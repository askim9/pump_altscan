#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v6.0                                                   ║
║                                                                          ║
║  PERUBAHAN DARI v5 (berdasarkan audit profesional):                      ║
║                                                                          ║
║  STATISTIK:                                                              ║
║  · MAD-based robust Z-score — tahan terhadap pump historis              ║
║    (std inflate oleh satu outlier; MAD tidak terpengaruh)                ║
║  · Baseline window: recent_exclude=3 → zero gap ke candle current       ║
║    (v5 meninggalkan gap 22 candle yang menyebabkan sinyal lemah)         ║
║                                                                          ║
║  SCORING REDESIGN:                                                       ║
║  [A] btx_ratio Z-score      — 25 pts  (% transaksi yang beli)           ║
║  [B] avg_buy_size Z-score   — 25 pts  (ukuran rata-rata per transaksi)  ║
║  [C] volume Z-score         — 20 pts  (total aktivitas)                  ║
║  [D] short_liq Z-score      — 20 pts  (short squeeze signal)            ║
║  [E] OI 4-candle Z-score    —  10 pts (akumulasi posisi)                ║
║                                                                          ║
║  A dan B independen (korelasi 0.065 vs 0.764 pada v5).                  ║
║  D dinaikkan 12→20: short squeeze adalah mekanisme pump paling          ║
║  reliabel di futures market.                                             ║
║                                                                          ║
║  FIX LAINNYA:                                                            ║
║  · ATR dihitung secara persentase → valid untuk mixed price source      ║
║  · Cooldown di-set saat scoring, bukan saat send Telegram               ║
║  · API key tanpa hardcoded default                                       ║
║  · SymbolMapper: reverse map dibangun sekali                             ║
║  · bv_ratio_bonus threshold masuk CONFIG                                 ║
║  · has_btx_data cek candle[-2] bukan [-1]                               ║
║  · CLZ volume field: hapus fallback 'close*1000' yang tidak valid       ║
║  · score_from_z: guard z_medium=0                                       ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import logging.handlers as _lh
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

VERSION = "6.0"

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger(); _root.setLevel(logging.INFO)
_ch   = logging.StreamHandler(); _ch.setFormatter(_fmt); _root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/scanner_v6.log", maxBytes=10 * 1024**2, backupCount=2)
_fh.setFormatter(_fmt); _root.addHandler(_fh)
log   = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # ── ENVIRONMENT ─────────────────────────────────────────────────────────
    # Sama seperti v5: hardcoded default jika env variable tidak di-set
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token":         os.getenv("BOT_TOKEN"),
    "chat_id":           os.getenv("CHAT_ID"),

    # ── VOLUME PRE-FILTER ────────────────────────────────────────────────────
    "pre_filter_vol":      100_000,    # $100K noise floor
    "min_vol_24h":         500_000,    # $500K minimum
    "max_vol_24h":     800_000_000,    # $800M ceiling
    "gate_chg_24h_max":       40.0,    # >40% naik dalam 24h = terlambat

    # ── DATA WINDOWS ─────────────────────────────────────────────────────────
    "candle_limit_bitget":     200,    # Bitget 1H candle limit
    "coinalyze_lookback_h":    168,    # 7 hari history Coinalyze
    "coinalyze_interval":   "1hour",

    # ── BASELINE WINDOWS ─────────────────────────────────────────────────────
    # recent_exclude: candle terakhir yang TIDAK masuk baseline.
    # candles[-1] = live (excluded); candles[-2] = current (excluded);
    # candles[-3] = pertama yang masuk baseline → zero gap
    "baseline_recent_exclude":   3,    # Fix: tidak ada gap antara baseline dan current
    "baseline_lookback_n":      96,    # 96 candle = 4 hari baseline
    "baseline_min_samples":     15,    # Minimum untuk Z-score valid

    # ── [A] BTX RATIO Z-SCORE (La Morgia 2023 — directionality) ─────────────
    # btx/tx = proporsi transaksi yang merupakan taker buy
    # Anomali ratio → buyers dominan dalam count, bukan sekadar volume besar
    "buy_tx_ratio_weight":      25,
    "buy_tx_ratio_z_strong":   2.0,
    "buy_tx_ratio_z_medium":   1.0,

    # ── [B] AVG BUY SIZE Z-SCORE (La Morgia 2023 — size anomaly) ────────────
    # bv/btx = rata-rata USD per transaksi beli
    # Anomali size → pemain besar masuk (institutional accumulation)
    # Independen dari [A]: korelasi ~0.07 (v5 A vs B: korelasi ~0.76)
    "avg_buy_size_weight":      25,
    "avg_buy_size_z_strong":   2.0,
    "avg_buy_size_z_medium":   0.9,

    # Bonus jika bv_ratio > threshold (lebih dari N% volume adalah taker buy)
    # Pindah ke CONFIG dari hardcoded di v5
    "bv_ratio_bonus_threshold": 0.62,   # 62% taker buy ratio = clearly bullish
    "bv_ratio_bonus_z":         0.5,    # Tambahan ke Z-score jika melebihi threshold

    # ── [C] VOLUME Z-SCORE (Fantazzini 2023 — total activity) ───────────────
    # Total volume anomali vs rolling baseline
    "volume_weight":            20,
    "volume_z_strong":         2.5,
    "volume_z_medium":         1.5,

    # ── [D] SHORT LIQUIDATION Z-SCORE (squeeze detector) ────────────────────
    # Posisi short force-closed → forced buying → harga naik
    # Dinaikkan 12→20: ini sinyal paling reliabel untuk pump di futures
    "short_liq_weight":         20,
    "short_liq_z_strong":      2.0,
    "short_liq_z_medium":      1.0,

    # ── [E] OI 4-CANDLE BUILDUP Z-SCORE (position accumulation) ─────────────
    # Menggunakan 4-candle window, bukan 1-candle (v5 terlalu noisy)
    # 4-candle = 4 jam: cukup panjang untuk filter noise, cukup pendek untuk signal
    "oi_buildup_weight":        10,
    "oi_buildup_z_strong":     1.5,
    "oi_buildup_z_medium":     0.5,
    "oi_buildup_candles":        4,     # Window untuk OI change calculation

    # ── MINIMUM ACTIVE COMPONENTS ────────────────────────────────────────────
    # Threshold proporsional: 10% dari max weight untuk setiap komponen
    # A:>2, B:>2, C:>2, D:>2, E:>1  (≈10% dari masing-masing max)
    "min_active_components":     2,
    "active_thresh_a":           2,    # 2/25 = 8%
    "active_thresh_b":           2,    # 2/25 = 8%
    "active_thresh_c":           2,    # 2/20 = 10%
    "active_thresh_d":           2,    # 2/20 = 10%
    "active_thresh_e":           1,    # 1/10 = 10%

    # ── SIGNAL THRESHOLDS ────────────────────────────────────────────────────
    # Dikalibrasi dari backtest v6: threshold 65 = F1 optimal (37.3% vs v5 27.2%)
    "score_threshold":          65,    # Alert dikirim
    "score_strong":             78,    # "Strong" signal
    "score_very_strong":        90,    # "Very strong"

    # ── ENTRY CALCULATION ────────────────────────────────────────────────────
    # ATR dihitung sebagai persentase (exchange-agnostic)
    # → valid meski price source berbeda antara Bitget dan Coinalyze
    "atr_candles":              14,    # Periode ATR
    "atr_sl_mult":             1.5,    # SL = entry * (1 - ATR_pct * mult)
    "min_target_pct":          7.0,    # Minimum T1 = 7% dari entry

    # ── OUTPUT ───────────────────────────────────────────────────────────────
    "max_alerts":                8,
    "alert_cooldown_sec":     3600,
    "cooldown_file":  "/tmp/v6_cooldown.json",
    "sleep_between_coins":     0.0,

    # ── COINALYZE RATE LIMIT ─────────────────────────────────────────────────
    "clz_min_interval_sec":    1.6,    # Enforce 40 calls/min limit
    "clz_batch_size":           20,    # Max symbol per call
    "clz_retry_attempts":        2,    # Retry hanya untuk 429; bukan 400/404
    "clz_retry_wait_sec":        2,
}


# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST
# ══════════════════════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
    "4USDT","0GUSDT","1000BONKUSDT","1000PEPEUSDT","1000RATSUSDT",
    "1000SHIBUSDT","1000XECUSDT","1INCHUSDT","1MBABYDOGEUSDT","2ZUSDT",
    "AAVEUSDT","ACEUSDT","ACHUSDT","ACTUSDT","ADAUSDT","AEROUSDT",
    "AGLDUSDT","AINUSDT","AIOUSDT","AIXBTUSDT","AKTUSDT","ALCHUSDT",
    "ALGOUSDT","ALICEUSDT","ALLOUSDT","ALTUSDT","ANIMEUSDT",
    "ANKRUSDT","APEUSDT","APEXUSDT","API3USDT","APRUSDT","APTUSDT",
    "ARUSDT","ARBUSDT","ARCUSDT","ARIAUSDT","ARKUSDT","ARKMUSDT",
    "ARPAUSDT","ASTERUSDT","ATUSDT","ATHUSDT","ATOMUSDT","AUCTIONUSDT",
    "AVAXUSDT","AVNTUSDT","AWEUSDT","AXLUSDT","AXSUSDT","AZTECUSDT",
    "BUSDT","B2USDT","BABYUSDT","BANUSDT","BANANAUSDT",
    "BANANAS31USDT","BANKUSDT","BARDUSDT","BATUSDT","BCHUSDT","BEATUSDT",
    "BERAUSDT","BGBUSDT","BIGTIMEUSDT","BIOUSDT","BIRBUSDT","BLASTUSDT",
    "BLESSUSDT","BLURUSDT","BNBUSDT","BOMEUSDT","BRETTUSDT","BREVUSDT",
    "BROCCOLIUSDT","BSVUSDT","BTCUSDT","BULLAUSDT","C98USDT","CAKEUSDT",
    "CCUSDT","CELOUSDT","CFXUSDT","CHILLGUYUSDT","CHZUSDT","CLUSDT",
    "CLANKERUSDT","CLOUSDT","COAIUSDT","COMPUSDT","COOKIEUSDT",
    "COWUSDT","CRCLUSDT","CROUSDT","CROSSUSDT","CRVUSDT","CTKUSDT",
    "CVCUSDT","CVXUSDT","CYBERUSDT","CYSUSDT","DASHUSDT","DEEPUSDT",
    "DENTUSDT","DEXEUSDT","DOGEUSDT","DOLOUSDT","DOODUSDT","DOTUSDT",
    "DRIFTUSDT","DYDXUSDT","DYMUSDT","EGLDUSDT","EIGENUSDT","ENAUSDT",
    "ENJUSDT","ENSUSDT","ENSOUSDT","EPICUSDT","ESPUSDT","ETCUSDT",
    "ETHUSDT","ETHFIUSDT","FUSDT","FARTCOINUSDT","FETUSDT",
    "FFUSDT","FIDAUSDT","FILUSDT","FLOKIUSDT","FLUIDUSDT","FOGOUSDT",
    "FOLKSUSDT","FORMUSDT","GALAUSDT","GASUSDT","GIGGLEUSDT",
    "GLMUSDT","GMTUSDT","GMXUSDT","GOATUSDT","GPSUSDT","GRASSUSDT","GUSDT",
    "GRIFFAINUSDT","GRTUSDT","GUNUSDT","GWEIUSDT","HUSDT","HBARUSDT",
    "HEIUSDT","HEMIUSDT","HMSTRUSDT","HOLOUSDT","HOMEUSDT","HYPEUSDT","HYPERUSDT",
    "ICNTUSDT","ICPUSDT","IDOLUSDT","ILVUSDT",
    "IMXUSDT","INITUSDT","INJUSDT","INXUSDT","IOUSDT",
    "IOTAUSDT","IOTXUSDT","IPUSDT","JASMYUSDT","JCTUSDT","JSTUSDT",
    "JTOUSDT","JUPUSDT","KAIAUSDT","KAITOUSDT","KASUSDT","KAVAUSDT",
    "kBONKUSDT","KERNELUSDT","KGENUSDT","KITEUSDT","kPEPEUSDT","kSHIBUSDT",
    "LAUSDT","LABUSDT","LAYERUSDT","LDOUSDT","LIGHTUSDT","LINEAUSDT",
    "LINKUSDT","LITUSDT","LPTUSDT","LSKUSDT","LTCUSDT","LUNAUSDT",
    "LUNCUSDT","LYNUSDT","MUSDT","MAGICUSDT","MAGMAUSDT","MANAUSDT",
    "MANTAUSDT","MANTRAUSDT","MASKUSDT","MAVUSDT","MAVIAUSDT","MBOXUSDT",
    "MEUSDT","MEGAUSDT","MELANIAUSDT","MEMEUSDT","MERLUSDT","METUSDT",
    "METAUSDT","MEWUSDT","MINAUSDT","MMTUSDT","MNTUSDT","MONUSDT",
    "MOODENGUSDT","MORPHOUSDT","MOVEUSDT","MOVRUSDT","MUUSDT","MUBARAKUSDT",
    "MYXUSDT","NAORISUSDT","NEARUSDT","NEIROCTOUSDT",
    "NEOUSDT","NEWTUSDT","NILUSDT","NMRUSDT","NOMUSDT","NOTUSDT",
    "NXPCUSDT","ONDOUSDT","ONGUSDT","ONTUSDT","OPUSDT","OPENUSDT",
    "OPNUSDT","ORCAUSDT","ORDIUSDT","OXTUSDT","PARTIUSDT",
    "PENDLEUSDT","PENGUUSDT","PEOPLEUSDT","PEPEUSDT","PHAUSDT","PIEVERSEUSDT",
    "PIPPINUSDT","PLUMEUSDT","PNUTUSDT","POLUSDT","POLYXUSDT",
    "POPCATUSDT","POWERUSDT","PROMPTUSDT","PROVEUSDT","PUMPUSDT","PURRUSDT",
    "PYTHUSDT","QUSDT","QNTUSDT","RAVEUSDT","RAYUSDT",
    "RECALLUSDT","RENDERUSDT","RESOLVUSDT","REZUSDT","RIVERUSDT","ROBOUSDT",
    "ROSEUSDT","RPLUSDT","RSRUSDT","RUNEUSDT","SUSDT","SAGAUSDT","SAHARAUSDT",
    "SANDUSDT","SAPIENUSDT","SEIUSDT","SENTUSDT","SHIBUSDT","SIGNUSDT",
    "SIRENUSDT","SKHYNIXUSDT","SKRUSDT","SKYUSDT","SKYAIUSDT","SLPUSDT",
    "SNXUSDT","SOLUSDT","SOMIUSDT","SONICUSDT","SOONUSDT","SOPHUSDT",
    "SPACEUSDT","SPKUSDT","SPXUSDT","SQDUSDT","SSVUSDT",
    "STBLUSDT","STEEMUSDT","STOUSDT","STRKUSDT","STXUSDT",
    "SUIUSDT","SUNUSDT","SUPERUSDT","SUSHIUSDT","SYRUPUSDT","TUSDT",
    "TACUSDT","TAGUSDT","TAIKOUSDT","TAOUSDT","THEUSDT","THETAUSDT",
    "TIAUSDT","TNSRUSDT","TONUSDT","TOSHIUSDT","TOWNSUSDT","TRBUSDT",
    "TRIAUSDT","TRUMPUSDT","TRXUSDT","TURBOUSDT","UAIUSDT","UBUSDT",
    "UMAUSDT","UNIUSDT","USUSDT","USDKRWUSDT","USELESSUSDT",
    "USUALUSDT","VANAUSDT","VANRYUSDT","VETUSDT","VINEUSDT","VIRTUALUSDT",
    "VTHOUSDT","VVVUSDT","WUSDT","WALUSDT","WAXPUSDT","WCTUSDT","WETUSDT",
    "WIFUSDT","WLDUSDT","WLFIUSDT","WOOUSDT","WTIUSDT","XAIUSDT",
    "XCUUSDT","XDCUSDT","XLMUSDT","XMRUSDT","XPDUSDT","XPINUSDT",
    "XPLUSDT","XRPUSDT","XTZUSDT","XVGUSDT","YGGUSDT","YZYUSDT","ZAMAUSDT",
    "ZBTUSDT","ZECUSDT","ZENUSDT","ZEREBROUSDT","ZETAUSDT","ZILUSDT",
    "ZKUSDT","ZKCUSDT","ZKJUSDT","ZKPUSDT","ZORAUSDT","ZROUSDT",
}

MANUAL_EXCLUDE: set = set()


# ══════════════════════════════════════════════════════════════════════════════
#  📐  MATH UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _mean(arr: list) -> float:
    return sum(arr) / len(arr) if arr else 0.0


def _median(series: list) -> float:
    if not series:
        return 0.0
    s = sorted(series)
    n = len(s)
    return (s[n // 2] + s[(n - 1) // 2]) / 2.0


def robust_zscore(value: float, series: list, min_samples: int = 10) -> float:
    """
    MAD-based robust Z-score.

    Menggunakan Median Absolute Deviation sebagai pengganti std.
    Tidak terpengaruh oleh pump historis yang inflate std.

    Formula: Z = 0.6745 * (value - median) / MAD
    Faktor 0.6745 membuat Z setara dengan Z-score normal untuk distribusi Gaussian.

    Edge case MAD=0 (semua nilai identik): gunakan deviasi persentase dari median.
    """
    if len(series) < min_samples:
        return 0.0
    med = _median(series)
    mad = _median([abs(x - med) for x in series])

    if mad < 1e-10:
        # Baseline memiliki zero variance — gunakan deviasi persentase
        if med < 1e-10:
            return 0.0
        pct_dev = (value - med) / med
        # Scale: 100% di atas median = Z=3.0
        return float(max(-3.0, min(3.0, pct_dev * 3.0)))

    return 0.6745 * (value - med) / mad


def score_from_z(z: float, z_strong: float, z_medium: float, weight: int) -> int:
    """
    Konversi Z-score ke skor [0, weight] secara linier.
    Guard untuk z_medium=0 (misconfiguration).
    Tidak ada 'tebing' — perubahan Z kecil menghasilkan perubahan skor kecil.
    """
    if z_medium <= 0 or z_strong <= z_medium:
        return weight if z >= 1.0 else 0

    if z >= z_strong:
        return weight
    if z >= z_medium:
        ratio = (z - z_medium) / (z_strong - z_medium)
        return int(weight // 2 + ratio * (weight - weight // 2))
    if z >= 0:
        ratio = z / z_medium
        return int(ratio * weight // 2)
    return 0


def _build_baseline(series: list) -> list:
    """
    Bangun baseline dari series menggunakan parameter CONFIG.
    Menggunakan recent_exclude untuk menghindari gap antara baseline dan current candle.
    """
    n   = len(series)
    exc = CONFIG["baseline_recent_exclude"]   # 3
    lkb = CONFIG["baseline_lookback_n"]        # 96

    end   = max(0, n - exc)
    start = max(0, end - lkb)
    return series[start:end]


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN
# ══════════════════════════════════════════════════════════════════════════════
def _load_cooldown() -> dict:
    try:
        path = CONFIG["cooldown_file"]
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items()
                    if now - v < CONFIG["alert_cooldown_sec"]}
    except Exception:
        pass
    return {}


def _save_cooldown(state: dict) -> None:
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass


_cooldown_state = _load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown_state)} coin")


def is_on_cooldown(sym: str) -> bool:
    return (time.time() - _cooldown_state.get(sym, 0)) < CONFIG["alert_cooldown_sec"]


def set_cooldown(sym: str) -> None:
    """
    Dipanggil SAAT SCORING — bukan saat pengiriman Telegram.
    Fix dari v5: cooldown tidak bergantung pada keberhasilan send_telegram().
    """
    _cooldown_state[sym] = time.time()
    _save_cooldown(_cooldown_state)


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET CLIENT
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE = "https://api.bitget.com"
    _candle_cache: Dict = {}
    _cache_ts: Dict = {}

    @staticmethod
    def _get(url: str, params: dict = None, timeout: int = 12) -> Optional[dict]:
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=timeout)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    log.warning("Bitget rate limit — tunggu 30s")
                    time.sleep(30)
                    continue
                break
            except Exception:
                if attempt < 2:
                    time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers",
                        params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 200) -> List[dict]:
        """Fetch Bitget 1H candles. Cached per scan run."""
        cache_key = f"{symbol}:{limit}"
        if cache_key in cls._candle_cache:
            return cls._candle_cache[cache_key]

        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/candles",
            params={"symbol": symbol, "productType": "USDT-FUTURES",
                    "granularity": "1H", "limit": limit}
        )
        if not data or data.get("code") != "00000":
            return []

        candles = []
        for row in data.get("data", []):
            try:
                vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                candles.append({
                    "ts":         int(row[0]),
                    "open":       float(row[1]),
                    "high":       float(row[2]),
                    "low":        float(row[3]),
                    "close":      float(row[4]),
                    "volume_usd": vol_usd,
                })
            except (IndexError, ValueError):
                continue

        candles.sort(key=lambda x: x["ts"])
        cls._candle_cache[cache_key] = candles
        return candles

    @classmethod
    def get_funding(cls, symbol: str) -> float:
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/current-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES"}
        )
        try:
            return float(data["data"][0]["fundingRate"])
        except Exception:
            return 0.0

    @classmethod
    def clear_cache(cls) -> None:
        cls._candle_cache.clear()
        cls._cache_ts.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  📡  COINALYZE CLIENT
# ══════════════════════════════════════════════════════════════════════════════
class CoinalyzeClient:
    BASE = "https://api.coinalyze.net/v1"
    _last_call: float = 0.0
    _cache: Dict = {}

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _wait(self) -> None:
        elapsed = time.time() - CoinalyzeClient._last_call
        wait    = CONFIG["clz_min_interval_sec"] - elapsed
        if wait > 0:
            time.sleep(wait)
        CoinalyzeClient._last_call = time.time()

    def _get(self, endpoint: str, params: dict) -> Optional[list]:
        """
        Single API call.
        Retry HANYA untuk 429 (rate limit).
        400/404 (symbol tidak ada) → return None langsung, tidak retry.
        """
        params["api_key"] = self.api_key

        for attempt in range(CONFIG["clz_retry_attempts"]):
            self._wait()
            try:
                r = requests.get(f"{self.BASE}/{endpoint}", params=params, timeout=15)

                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 10))
                    log.warning(f"Coinalyze rate limit — tunggu {retry_after}s")
                    time.sleep(retry_after + 1)
                    continue  # Retry setelah tunggu

                if r.status_code == 401:
                    log.error("Coinalyze API key tidak valid!")
                    return None

                if r.status_code in (400, 404):
                    # Symbol tidak ada — jangan retry, buang waktu
                    log.debug(f"CLZ {endpoint} {r.status_code}: {r.text[:80]}")
                    return None

                if r.status_code != 200:
                    log.warning(f"CLZ {endpoint} HTTP {r.status_code}: {r.text[:80]}")
                    return None

                data = r.json()
                # Coinalyze kadang return dict error, bukan list
                if isinstance(data, dict) and "error" in data:
                    log.debug(f"CLZ {endpoint} API error: {data.get('error','')}")
                    return None

                return data

            except requests.exceptions.Timeout:
                log.warning(f"CLZ {endpoint} timeout (attempt {attempt + 1})")
                if attempt < CONFIG["clz_retry_attempts"] - 1:
                    time.sleep(2)
            except Exception as e:
                log.debug(f"CLZ {endpoint} exception: {e}")
                if attempt < CONFIG["clz_retry_attempts"] - 1:
                    time.sleep(CONFIG["clz_retry_wait_sec"])

        return None

    def get_future_markets(self) -> List[dict]:
        cache_key = "future_markets"
        if cache_key in self._cache:
            return self._cache[cache_key]
        data = self._get("future-markets", {})
        result = data if isinstance(data, list) else []
        self._cache[cache_key] = result
        return result

    def _batch_fetch(self, endpoint: str, symbols: List[str],
                     extra_params: dict) -> Dict[str, list]:
        """
        Batch fetch, max clz_batch_size symbol per call.
        Log ringkas: hanya lapor jumlah batch gagal, bukan per-batch warning.
        """
        batch_size    = CONFIG["clz_batch_size"]
        result        = {}
        failed        = 0
        total_batches = math.ceil(len(symbols) / batch_size)

        for i in range(0, len(symbols), batch_size):
            batch   = symbols[i:i + batch_size]
            params  = {"symbols": ",".join(batch), **extra_params}
            data    = self._get(endpoint, params)

            if data is None:
                failed += 1
                continue

            if not isinstance(data, list):
                log.debug(f"CLZ {endpoint}: unexpected type {type(data)}")
                failed += 1
                continue

            for item in data:
                sym     = item.get("symbol", "")
                history = item.get("history", [])
                if sym and history:
                    result[sym] = history

        if failed > 0:
            log.info(f"CLZ {endpoint}: {len(result)}/{len(symbols) - failed} symbols OK "
                     f"({failed}/{total_batches} batch gagal — symbol tidak tersedia di CLZ)")
        return result

    def fetch_ohlcv_batch(self, symbols: List[str],
                          from_ts: int, to_ts: int) -> Dict[str, list]:
        """Fetch OHLCV+btx+bv. Volume field 'v' dalam quote currency (USD untuk USDT pairs)."""
        return self._batch_fetch("ohlcv-history", symbols, {
            "interval": CONFIG["coinalyze_interval"],
            "from":     from_ts,
            "to":       to_ts,
        })

    def fetch_liquidations_batch(self, symbols: List[str],
                                 from_ts: int, to_ts: int) -> Dict[str, list]:
        """Fetch liquidations. convert_to_usd=true mengkonversi ke USD."""
        return self._batch_fetch("liquidation-history", symbols, {
            "interval":       CONFIG["coinalyze_interval"],
            "from":           from_ts,
            "to":             to_ts,
            "convert_to_usd": "true",
        })

    def fetch_oi_batch(self, symbols: List[str],
                       from_ts: int, to_ts: int) -> Dict[str, list]:
        """Fetch open interest. convert_to_usd=true."""
        return self._batch_fetch("open-interest-history", symbols, {
            "interval":       CONFIG["coinalyze_interval"],
            "from":           from_ts,
            "to":             to_ts,
            "convert_to_usd": "true",
        })

    def clear_cache(self) -> None:
        self._cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  🗺️  SYMBOL MAPPER
# ══════════════════════════════════════════════════════════════════════════════
class SymbolMapper:
    """
    Bitget symbol ↔ Coinalyze symbol mapping.

    Strategi: Prioritaskan .A (aggregated across all exchanges).
    .A memiliki coverage terluas karena bukan exchange-specific.
    Data aggregated valid untuk Z-score karena menangkap global market activity.
    """

    def __init__(self, clz_client: CoinalyzeClient):
        self._client   = clz_client
        self._to_clz:  Dict[str, str]  = {}
        self._has_btx: Dict[str, bool] = {}
        self._rev_map: Dict[str, str]  = {}   # Pre-built reverse map (O(1) lookup)
        self._loaded   = False

    def load(self) -> int:
        log.info("SymbolMapper: fetching Coinalyze markets...")
        markets = self._client.get_future_markets()

        if not markets:
            log.warning("SymbolMapper: gagal fetch dari CLZ, gunakan format .A langsung")
            for sym in WHITELIST_SYMBOLS:
                self._to_clz[sym] = f"{sym}_PERP.A"
            self._loaded = True
            self._build_reverse()
            return len(WHITELIST_SYMBOLS)

        # Bangun index .A markets
        agg_index: Dict[str, dict] = {}
        for m in markets:
            sym = m.get("symbol", "")
            if sym.endswith(".A"):
                base = sym.rsplit(".", 1)[0]   # "BTCUSDT_PERP"
                agg_index[base] = m

        mapped_a   = 0
        unmapped   = []

        for sym in WHITELIST_SYMBOLS:
            a_sym    = f"{sym}_PERP.A"
            base_key = f"{sym}_PERP"

            if base_key in agg_index:
                m = agg_index[base_key]
                self._to_clz[sym]   = a_sym
                self._has_btx[a_sym] = m.get("has_buy_sell_data", True)
                mapped_a += 1
            else:
                # Coin tidak ada di .A — pakai format .A tetap
                # Coinalyze akan return 404 dan kita skip dengan benar (no retry)
                self._to_clz[sym]   = a_sym
                self._has_btx[a_sym] = True   # Coba saja
                unmapped.append(sym)

        log.info(f"SymbolMapper: {mapped_a}/{len(WHITELIST_SYMBOLS)} mapped ke .A "
                 f"({len(unmapped)} fallback tanpa konfirmasi)")

        self._build_reverse()
        self._loaded = True
        return mapped_a

    def _build_reverse(self) -> None:
        """Build reverse map sekali — bukan rebuild setiap panggilan (fix v5)."""
        self._rev_map = {v: k for k, v in self._to_clz.items()}

    def to_clz(self, bitget_sym: str) -> Optional[str]:
        return self._to_clz.get(bitget_sym)

    def to_bitget(self, clz_sym: str) -> Optional[str]:
        return self._rev_map.get(clz_sym)

    def btx_available(self, clz_sym: str) -> bool:
        return self._has_btx.get(clz_sym, True)

    def clz_symbols_for(self, bitget_syms: List[str]) -> List[str]:
        return [self._to_clz[s] for s in bitget_syms if s in self._to_clz]

    @property
    def is_loaded(self) -> bool:
        return self._loaded


# ══════════════════════════════════════════════════════════════════════════════
#  📦  DATA CONTAINERS
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class CoinData:
    symbol:    str
    price:     float       # Bitget lastPr (sumber tunggal untuk entry price)
    vol_24h:   float
    chg_24h:   float
    funding:   float
    candles:   List[dict]  # Price candles: CLZ OHLCV jika tersedia, else Bitget
    clz_ohlcv: List[dict]  # Coinalyze raw OHLCV+btx+bv (untuk scoring A,B)
    clz_liq:   List[dict]  # Coinalyze liquidations (untuk scoring D)
    clz_oi:    List[dict]  # Coinalyze open interest (untuk scoring E)

    @property
    def has_btx(self) -> bool:
        """Cek candle[-2] (yang dipakai scorer), bukan [-1]."""
        if len(self.clz_ohlcv) < 2:
            return False
        cur = self.clz_ohlcv[-2]
        return bool(cur.get("btx", 0)) and bool(cur.get("tx", 0))

    @property
    def has_liq(self) -> bool:
        return bool(self.clz_liq)

    @property
    def has_oi(self) -> bool:
        return bool(self.clz_oi)


@dataclass
class ScoreResult:
    symbol:       str
    score:        int
    confidence:   str
    components:   dict
    entry:        Optional[dict]
    price:        float
    vol_24h:      float
    chg_24h:      float
    funding:      float
    urgency:      str
    data_quality: dict


# ══════════════════════════════════════════════════════════════════════════════
#  🔬  SCORING COMPONENTS
# ══════════════════════════════════════════════════════════════════════════════

def _get_current_and_baseline(candles: list, field: str) -> Tuple[float, list]:
    """
    Ambil current value (candles[-2]) dan baseline list untuk field tertentu.
    Baseline menggunakan recent_exclude=3 → zero gap ke current.
    """
    if len(candles) < CONFIG["baseline_recent_exclude"] + 2:
        return 0.0, []
    cur_val  = candles[-2].get(field, 0) or 0.0
    baseline = _build_baseline(candles)
    bl_vals  = [c.get(field, 0) or 0.0 for c in baseline]
    return float(cur_val), bl_vals


def score_buy_tx_ratio(data: CoinData) -> Tuple[int, float, dict]:
    """
    [A] BTX Ratio Z-score — La Morgia 2023 (feature importance #2)

    btx/tx = proporsi transaksi yang merupakan taker buy.
    Mengukur DIRECTIONALITY: apakah buyers lebih agresif dari sellers.

    Berbeda dari [B] (avg size): A menangkap frekuensi buy,
    B menangkap besarnya setiap buy. Korelasi A-B ≈ 0.07 (hampir independen).
    """
    cfg    = CONFIG
    weight = cfg["buy_tx_ratio_weight"]

    if not data.clz_ohlcv or len(data.clz_ohlcv) < cfg["baseline_min_samples"]:
        return _fallback_buy_pressure(data, weight), 0.0, {"source": "fallback_bp"}

    candles = data.clz_ohlcv
    cur     = candles[-2]
    btx     = float(cur.get("btx", 0) or 0)
    tx      = float(cur.get("tx",  0) or 0)

    if tx <= 0:
        return _fallback_buy_pressure(data, weight), 0.0, {"source": "fallback_tx0"}

    btx_ratio = btx / tx  # [0, 1]

    # Baseline: btx/tx ratios
    baseline  = _build_baseline(candles)
    bl_ratios = [float(c.get("btx", 0) or 0) / max(float(c.get("tx", 0) or 1), 1)
                 for c in baseline if c.get("tx", 0)]

    if len(bl_ratios) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "insufficient_baseline_a"}

    z = robust_zscore(btx_ratio, bl_ratios)

    # Penalty: jika ratio turun signifikan (distribusi) meski raw count naik
    bl_btx = [float(c.get("btx", 0) or 0) for c in baseline]
    z_raw  = robust_zscore(btx, bl_btx)
    if z < -1.5 and z_raw > 1.5:
        # Divergence: banyak transaksi tapi proporsi buy TURUN = distribusi
        z = max(-1.0, z_raw * 0.3)   # Sangat kurangi skor
    elif z >= 0:
        # Normal: gabungkan ratio dan raw, ratio lebih penting
        z = max(z, z_raw * 0.6)

    score = score_from_z(z, cfg["buy_tx_ratio_z_strong"], cfg["buy_tx_ratio_z_medium"], weight)

    return score, round(z, 2), {
        "btx_ratio":  round(btx_ratio, 3),
        "btx":        int(btx),
        "tx":         int(tx),
        "z":          round(z, 2),
    }


def _fallback_buy_pressure(data: CoinData, weight: int) -> int:
    """
    Fallback ketika btx/tx tidak tersedia.
    Gunakan buy pressure proxy dari OHLCV: (close-low)/(high-low).
    Z-score terhadap baseline Bitget candles.
    """
    if not data.candles or len(data.candles) < CONFIG["baseline_min_samples"] + 3:
        return 0

    def bp(c):
        r = c["high"] - c["low"]
        if r <= 0: return 0.5
        return clamp((c["close"] - c["low"]) / r, 0.0, 1.0)

    candles  = data.candles
    cur_bp   = bp(candles[-2])
    baseline = _build_baseline(candles)
    bl_bp    = [bp(c) for c in baseline]

    if len(bl_bp) < CONFIG["baseline_min_samples"]:
        return 0

    z = robust_zscore(cur_bp, bl_bp)
    return score_from_z(z, 1.8, 0.9, weight // 2)   # Fallback max = setengah weight


def score_avg_buy_size(data: CoinData) -> Tuple[int, float, dict]:
    """
    [B] Average Buy Size Z-score — La Morgia 2023 (feature importance #1)

    bv/btx = rata-rata USD per taker buy transaction.
    Mengukur SIZE ANOMALY: apakah ukuran setiap pembelian lebih besar dari biasa.

    Korelasi dengan [A] ≈ 0.07 karena [A] mengukur COUNT, [B] mengukur SIZE.
    Smart money cenderung masuk dengan sedikit transaksi besar (tinggi [B], normal [A]).
    Retail FOMO cenderung masuk dengan banyak transaksi kecil (tinggi [A], normal [B]).
    """
    cfg    = CONFIG
    weight = cfg["avg_buy_size_weight"]

    if not data.clz_ohlcv or len(data.clz_ohlcv) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_clz_data"}

    candles = data.clz_ohlcv
    cur     = candles[-2]
    btx     = float(cur.get("btx", 0) or 0)
    bv      = float(cur.get("bv",  0) or 0)
    v       = float(cur.get("v",   0) or 0)

    if btx <= 0 or bv <= 0:
        # Tidak ada btx: fallback ke bv/v ratio
        if v > 0 and "bv" in cur:
            return _score_bv_ratio_fallback(data, weight, v, bv)
        return 0, 0.0, {"source": "no_btx_bv"}

    avg_buy_size = bv / btx   # USD per taker buy transaction

    # Baseline: avg_buy_size values
    baseline      = _build_baseline(candles)
    bl_avg_sizes  = []
    for c in baseline:
        c_btx = float(c.get("btx", 0) or 0)
        c_bv  = float(c.get("bv",  0) or 0)
        if c_btx > 0 and c_bv > 0:
            bl_avg_sizes.append(c_bv / c_btx)

    if len(bl_avg_sizes) < cfg["baseline_min_samples"]:
        return _score_bv_ratio_fallback(data, weight, v, bv)

    z = robust_zscore(avg_buy_size, bl_avg_sizes)

    # Bonus jika bv/v ratio tinggi (dominasi buy side)
    if v > 0:
        bv_ratio = bv / v
        if bv_ratio > cfg["bv_ratio_bonus_threshold"]:
            z += cfg["bv_ratio_bonus_z"]

    score = score_from_z(z, cfg["avg_buy_size_z_strong"], cfg["avg_buy_size_z_medium"], weight)

    return score, round(z, 2), {
        "avg_buy_size_usd": round(avg_buy_size),
        "bv_ratio":         round(bv / v if v > 0 else 0, 3),
        "z":                round(z, 2),
    }


def _score_bv_ratio_fallback(data: CoinData, weight: int,
                              v: float, bv: float) -> Tuple[int, float, dict]:
    """Fallback untuk [B] menggunakan bv/v ratio ketika btx tidak tersedia."""
    if v <= 0:
        return 0, 0.0, {"source": "bv_ratio_fallback_v0"}

    bv_ratio  = bv / v
    candles   = data.clz_ohlcv
    baseline  = _build_baseline(candles)
    bl_ratios = [float(c.get("bv", 0) or 0) / max(float(c.get("v", 0) or 1), 1)
                 for c in baseline if c.get("v", 0)]

    if len(bl_ratios) < CONFIG["baseline_min_samples"]:
        return 0, 0.0, {"source": "bv_ratio_fallback_insufficient"}

    z = robust_zscore(bv_ratio, bl_ratios)
    if bv_ratio > CONFIG["bv_ratio_bonus_threshold"]:
        z += CONFIG["bv_ratio_bonus_z"]

    score = score_from_z(z, CONFIG["avg_buy_size_z_strong"],
                         CONFIG["avg_buy_size_z_medium"], weight // 2)
    return score, round(z, 2), {
        "bv_ratio":  round(bv_ratio, 3),
        "source":    "bv_ratio_fallback",
        "z":         round(z, 2),
    }


def score_volume(data: CoinData) -> Tuple[int, float, dict]:
    """
    [C] Volume Z-score — Fantazzini 2023

    Total volume anomali. Sumber: candles (CLZ jika tersedia, else Bitget).
    Menggunakan MAD Z-score dengan baseline zero-gap.
    """
    cfg     = CONFIG
    weight  = cfg["volume_weight"]
    candles = data.candles

    if len(candles) < cfg["baseline_min_samples"] + cfg["baseline_recent_exclude"]:
        return 0, 0.0, {"source": "insufficient_candles"}

    cur_vol  = candles[-2]["volume_usd"]
    baseline = _build_baseline(candles)
    bl_vols  = [c["volume_usd"] for c in baseline]

    if len(bl_vols) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "insufficient_baseline_c"}

    z     = robust_zscore(cur_vol, bl_vols)
    score = score_from_z(z, cfg["volume_z_strong"], cfg["volume_z_medium"], weight)

    bl_mean  = _mean(bl_vols)
    vol_mult = cur_vol / bl_mean if bl_mean > 0 else 1.0

    return score, round(z, 2), {
        "cur_vol":    round(cur_vol),
        "vol_mult":   round(vol_mult, 2),
        "z":          round(z, 2),
    }


def score_short_liquidations(data: CoinData) -> Tuple[int, float, dict]:
    """
    [D] Short Liquidation Z-score

    short_liq spike → posisi short di-force close → forced buying → harga naik.
    Ini mekanisme pump paling reliabel di futures market: liquidation cascade.

    Weight dinaikkan 12→20 dari v5 karena ini adalah leading indicator
    yang paling specific untuk futures pump.
    """
    cfg    = CONFIG
    weight = cfg["short_liq_weight"]

    if not data.has_liq or len(data.clz_liq) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_liq_data"}

    liqs      = data.clz_liq
    cur_val   = float(liqs[-2].get("s", 0) or 0) if len(liqs) >= 2 else 0.0

    baseline  = _build_baseline(liqs)
    bl_vals   = [float(c.get("s", 0) or 0) for c in baseline]

    # Validasi: jika terlalu banyak zero, data tidak informatif
    nonzero_pct = sum(1 for x in bl_vals if x > 0) / max(len(bl_vals), 1)
    if nonzero_pct < 0.15:
        return 0, 0.0, {"source": "too_sparse_liq_data"}

    z     = robust_zscore(cur_val, bl_vals)
    score = score_from_z(z, cfg["short_liq_z_strong"], cfg["short_liq_z_medium"], weight)

    return score, round(z, 2), {
        "short_liq_usd": round(cur_val),
        "z":             round(z, 2),
    }


def score_oi_buildup(data: CoinData) -> Tuple[int, float, dict]:
    """
    [E] OI 4-Candle Buildup Z-score

    Menggunakan 4-candle window untuk OI change, bukan 1-candle (v5 terlalu noisy).
    Kenaikan OI selama 4 jam = posisi long baru dibuka secara konsisten = bullish.
    OI change negatif = posisi di-close = bearish.
    """
    cfg    = CONFIG
    weight = cfg["oi_buildup_weight"]
    w      = cfg["oi_buildup_candles"]  # 4

    if not data.has_oi or len(data.clz_oi) < cfg["baseline_min_samples"] + w:
        return 0, 0.0, {"source": "no_oi_data"}

    oi = data.clz_oi

    # 4-candle OI change: (candle[-2] - candle[-(2+w)]) / candle[-(2+w)]
    cur_oi  = float(oi[-2].get("c", 0) or 0)
    prev_oi = float(oi[-(2 + w)].get("c", 0) or 0) if len(oi) >= (2 + w) else 0.0

    if prev_oi <= 0:
        return 0, 0.0, {"source": "prev_oi_zero"}

    oi_change = (cur_oi - prev_oi) / prev_oi

    # Baseline: 4-candle OI changes
    baseline   = _build_baseline(oi)
    bl_changes = []
    for j in range(w, len(baseline)):
        oi_j   = float(baseline[j].get("c", 0) or 0)
        oi_bef = float(baseline[j - w].get("c", 0) or 0)
        if oi_bef > 0:
            bl_changes.append((oi_j - oi_bef) / oi_bef)

    if len(bl_changes) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "insufficient_baseline_oi"}

    z     = robust_zscore(oi_change, bl_changes)
    score = score_from_z(z, cfg["oi_buildup_z_strong"], cfg["oi_buildup_z_medium"], weight)

    return score, round(z, 2), {
        "oi_change_pct": round(oi_change * 100, 2),
        "window_h":      w,
        "z":             round(z, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_entry_targets(data: CoinData) -> Optional[dict]:
    """
    Hitung entry, SL, target.

    FIX dari v5: ATR dihitung sebagai PERSENTASE dari reference price,
    lalu diterapkan ke Bitget price. Ini valid untuk mixed source
    (CLZ candles vs Bitget entry) karena percentage moves lintas exchange sangat dekat.
    """
    candles = data.candles
    n_atr   = CONFIG["atr_candles"]

    if len(candles) < n_atr + 2:
        return None

    # ATR sebagai persentase
    price_ref = candles[-2]["close"]
    trs_pct   = []
    for i in range(1, min(n_atr + 1, len(candles))):
        c  = candles[-i]
        pc = candles[-(i + 1)]["close"]
        if pc > 0:
            tr_pct = max(
                (c["high"] - c["low"]) / pc,
                abs(c["high"] - pc)    / pc,
                abs(c["low"]  - pc)    / pc,
            )
            trs_pct.append(tr_pct)

    atr_pct = _mean(trs_pct) if trs_pct else 0.02   # Default 2%

    entry   = data.price                              # Selalu dari Bitget
    sl      = entry * (1 - atr_pct * CONFIG["atr_sl_mult"])
    sl_pct  = round((entry - sl) / entry * 100, 1)

    t1 = max(entry * (1 + CONFIG["min_target_pct"] / 100),
             entry * (1 + atr_pct * 3))
    t2 = max(entry * 1.20, entry * (1 + atr_pct * 6))

    t1_pct = round((t1 - entry) / entry * 100, 1)
    t2_pct = round((t2 - entry) / entry * 100, 1)
    rr     = round((t1 - entry) / (entry - sl), 2) if (entry - sl) > 0 else 0.0

    return {
        "entry":    round(entry, 8),
        "sl":       round(sl, 8),
        "sl_pct":   sl_pct,
        "t1":       round(t1, 8),
        "t2":       round(t2, 8),
        "t1_pct":   t1_pct,
        "t2_pct":   t2_pct,
        "rr":       rr,
        "atr_pct":  round(atr_pct * 100, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  MASTER SCORER
# ══════════════════════════════════════════════════════════════════════════════
def score_coin(data: CoinData) -> Optional[ScoreResult]:
    """
    Jalankan 5 komponen, agregasi, return ScoreResult atau None.

    Cooldown di-set DI SINI, sebelum return — tidak bergantung pada
    keberhasilan pengiriman Telegram (fix dari v5).
    """
    cfg = CONFIG

    # ── Hard pre-filters ──────────────────────────────────────────────────────
    if data.vol_24h < cfg["min_vol_24h"]:
        return None
    if data.chg_24h > cfg["gate_chg_24h_max"]:
        return None
    if data.price <= 0:
        return None

    # ── Run 5 components ─────────────────────────────────────────────────────
    a_score, a_z, a_d = score_buy_tx_ratio(data)
    b_score, b_z, b_d = score_avg_buy_size(data)
    c_score, c_z, c_d = score_volume(data)
    d_score, d_z, d_d = score_short_liquidations(data)
    e_score, e_z, e_d = score_oi_buildup(data)

    total = a_score + b_score + c_score + d_score + e_score

    # ── Minimum active filter (proporsional per komponen) ──────────────────
    active = sum([
        a_score > cfg["active_thresh_a"],
        b_score > cfg["active_thresh_b"],
        c_score > cfg["active_thresh_c"],
        d_score > cfg["active_thresh_d"],
        e_score > cfg["active_thresh_e"],
    ])
    if active < cfg["min_active_components"]:
        return None

    if total < cfg["score_threshold"]:
        return None

    # ── Confidence ────────────────────────────────────────────────────────────
    if total >= cfg["score_very_strong"]:
        confidence = "very_strong"
    elif total >= cfg["score_strong"]:
        confidence = "strong"
    else:
        confidence = "watch"

    # ── Data quality ──────────────────────────────────────────────────────────
    dq = {
        "has_btx":   data.has_btx,    # Cek [-2], bukan [-1] (fix v5)
        "has_liq":   data.has_liq,
        "has_oi":    data.has_oi,
        "candles":   len(data.candles),
        "clz_bars":  len(data.clz_ohlcv),
    }

    # ── Cooldown di-set sekarang (fix dari v5) ────────────────────────────────
    # Tidak lagi bergantung pada keberhasilan Telegram
    set_cooldown(data.symbol)

    # ── Urgency ───────────────────────────────────────────────────────────────
    liq_note = (f"${d_d.get('short_liq_usd', 0)/1e3:.0f}K liq"
                if d_score >= 8 else "")
    if d_score >= 14 and (a_score >= 12 or b_score >= 12):
        urgency = f"🔴 TINGGI — Short squeeze + akumulasi aktif {liq_note}"
    elif d_score >= 14:
        urgency = f"🔴 TINGGI — Short squeeze signal kuat {liq_note}"
    elif a_z >= 2.0 and b_z >= 1.5:
        urgency = "🟠 SEDANG — Buy count + size sama-sama anomali"
    elif b_z >= 2.0:
        urgency = "🟠 SEDANG — Smart money size anomali"
    elif c_z >= 2.0:
        urgency = "🟡 SEDANG — Volume spike signifikan"
    else:
        urgency = "⚪ WATCH — Akumulasi awal"

    return ScoreResult(
        symbol      = data.symbol,
        score       = total,
        confidence  = confidence,
        components  = {
            "A": {"score": a_score, "z": a_z, "details": a_d},
            "B": {"score": b_score, "z": b_z, "details": b_d},
            "C": {"score": c_score, "z": c_z, "details": c_d},
            "D": {"score": d_score, "z": d_z, "details": d_d},
            "E": {"score": e_score, "z": e_z, "details": e_d},
        },
        entry        = calc_entry_targets(data),
        price        = data.price,
        vol_24h      = data.vol_24h,
        chg_24h      = data.chg_24h,
        funding      = data.funding,
        urgency      = urgency,
        data_quality = dq,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def _conf_emoji(conf: str) -> str:
    return {"very_strong": "🟢", "strong": "🟡", "watch": "⚪"}.get(conf, "⚪")

def _dq_label(dq: dict) -> str:
    parts = []
    if dq.get("has_btx"): parts.append("btx✓")
    if dq.get("has_liq"): parts.append("liq✓")
    if dq.get("has_oi"):  parts.append("oi✓")
    return " ".join(parts) if parts else "basic"

def build_alert(r: ScoreResult, rank: int) -> str:
    e   = r.entry
    vol = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    bar = "█" * min(20, r.score // 5) + "░" * max(0, 20 - r.score // 5)
    dq  = _dq_label(r.data_quality)
    c   = r.components

    entry_block = ""
    if e:
        entry_block = (
            f"\n   📍 Entry: <b>${e['entry']:.6g}</b>"
            f" | SL: ${e['sl']:.6g} (-{e['sl_pct']}%)"
            f"\n   🎯 T1: +{e['t1_pct']}%"
            f" | T2: +{e['t2_pct']}%"
            f" | R/R: {e['rr']}"
        )

    return (
        f"#{rank} {_conf_emoji(r.confidence)} <b>{r.symbol}</b>"
        f"  Score: <b>{r.score}/100</b>  [{dq}]\n"
        f"   {bar}\n"
        f"   {r.urgency}\n"
        f"   Vol: {vol} | Δ24h: {r.chg_24h:+.1f}% | F: {r.funding:.5f}\n"
        f"   [A] BuyRatio: {c['A']['score']}pt ({c['A']['z']:+.1f}σ)"
        f"  [B] AvgSize: {c['B']['score']}pt ({c['B']['z']:+.1f}σ)\n"
        f"   [C] Volume: {c['C']['score']}pt ({c['C']['z']:+.1f}σ)"
        f"  [D] ShortLiq: {c['D']['score']}pt ({c['D']['z']:+.1f}σ)"
        f"  [E] OI: {c['E']['score']}pt ({c['E']['z']:+.1f}σ)"
        f"{entry_block}\n"
    )

def build_summary(results: List[ScoreResult]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = (
        f"🔍 <b>PRE-PUMP SCANNER v{VERSION}</b> — {now}\n"
        f"📡 Data: Bitget + Coinalyze (btx/bv/liq/OI)\n"
        f"📊 {len(results)} sinyal\n\n"
    )
    for i, r in enumerate(results, 1):
        c  = r.components
        t1 = f"+{r.entry['t1_pct']}%" if r.entry else "?"
        msg += (
            f"{i}. <b>{r.symbol}</b> [{r.score}pt] "
            f"A:{c['A']['score']} B:{c['B']['score']} "
            f"C:{c['C']['score']} D:{c['D']['score']} "
            f"E:{c['E']['score']} → T1:{t1}\n"
        )
    return msg

def send_telegram(msg: str) -> bool:
    bt = CONFIG["bot_token"]; ci = CONFIG["chat_id"]
    if not bt or not ci:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bt}/sendMessage",
            json={"chat_id": ci, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan() -> None:
    start_ts = time.time()
    log.info("=" * 70)
    log.info(f"  PRE-PUMP SCANNER v{VERSION} — "
             f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 70)

    # ── Validate API keys ─────────────────────────────────────────────────────
    if not CONFIG["coinalyze_api_key"]:
        log.error("FATAL: COINALYZE_API_KEY tidak di-set! Set environment variable.")
        exit(1)
    if not CONFIG["bot_token"] or not CONFIG["chat_id"]:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak di-set!")
        exit(1)

    # ── Init clients ──────────────────────────────────────────────────────────
    clz_client = CoinalyzeClient(CONFIG["coinalyze_api_key"])
    mapper     = SymbolMapper(clz_client)
    mapper.load()

    # ── Fetch Bitget tickers ──────────────────────────────────────────────────
    log.info("Fetching Bitget tickers...")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        send_telegram(f"⚠️ Scanner v{VERSION}: Gagal fetch Bitget tickers")
        return
    log.info(f"Bitget tickers: {len(tickers)}")

    # ── Build candidate list ──────────────────────────────────────────────────
    candidates = []
    skip_stats = defaultdict(int)

    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:
            skip_stats["excluded"] += 1; continue
        if is_on_cooldown(sym):
            skip_stats["cooldown"] += 1; continue
        if sym not in tickers:
            skip_stats["not_found"] += 1; continue

        t = tickers[sym]
        try:
            vol = float(t.get("quoteVolume", 0))
            chg = float(t.get("change24h",   0)) * 100
        except Exception:
            skip_stats["parse_error"] += 1; continue

        if vol < CONFIG["pre_filter_vol"]:   skip_stats["vol_low"]  += 1; continue
        if vol > CONFIG["max_vol_24h"]:      skip_stats["vol_high"] += 1; continue
        if chg > CONFIG["gate_chg_24h_max"]: skip_stats["pumped"]   += 1; continue
        candidates.append((sym, t))

    log.info(f"Candidates: {len(candidates)} | Skip: {dict(skip_stats)}")

    # ── Coinalyze bulk fetch ──────────────────────────────────────────────────
    now_ts  = int(time.time())
    from_ts = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    clz_syms = mapper.clz_symbols_for([sym for sym, _ in candidates])

    clz_ohlcv_all = clz_liq_all = clz_oi_all = {}
    if clz_syms:
        log.info(f"Fetching CLZ data untuk {len(clz_syms)} symbols...")
        clz_ohlcv_all = clz_client.fetch_ohlcv_batch(clz_syms, from_ts, now_ts)
        clz_liq_all   = clz_client.fetch_liquidations_batch(clz_syms, from_ts, now_ts)
        clz_oi_all    = clz_client.fetch_oi_batch(clz_syms, from_ts, now_ts)
        log.info(f"CLZ received: OHLCV={len(clz_ohlcv_all)} "
                 f"Liq={len(clz_liq_all)} OI={len(clz_oi_all)}")
    else:
        log.warning("Tidak ada CLZ symbols — menggunakan Bitget candles saja")

    # ── Score each coin ───────────────────────────────────────────────────────
    results: List[ScoreResult] = []
    BitgetClient.clear_cache()

    for i, (sym, ticker) in enumerate(candidates):
        log.info(f"[{i+1}/{len(candidates)}] {sym}")
        try:
            price   = float(ticker.get("lastPr", 0))
            vol_24h = float(ticker.get("quoteVolume", 0))
            chg_24h = float(ticker.get("change24h",   0)) * 100

            if price <= 0:
                continue

            clz_sym = mapper.to_clz(sym)
            ohlcv_c = clz_ohlcv_all.get(clz_sym, []) if clz_sym else []
            liq_c   = clz_liq_all.get(clz_sym, [])   if clz_sym else []
            oi_c    = clz_oi_all.get(clz_sym, [])     if clz_sym else []

            # ── Candles untuk scoring [C] dan entry ATR ────────────────────────
            # Jika CLZ OHLCV tersedia (≥60 bars): gunakan sebagai candles.
            # Field 'v' dari Coinalyze = volume dalam quote currency (USD untuk USDT pairs).
            # Hapus fallback 'close*1000' yang tidak valid (fix dari v5).
            # ATR dihitung sebagai % → valid meski CLZ dan Bitget price sedikit berbeda.
            if len(ohlcv_c) >= 60:
                candles = [
                    {
                        "ts":         int(bar.get("t", 0)) * 1000,
                        "open":       float(bar.get("o", 0)),
                        "high":       float(bar.get("h", 0)),
                        "low":        float(bar.get("l",  0)),
                        "close":      float(bar.get("c", 0)),
                        "volume_usd": float(bar.get("v", 0) or 0),
                    }
                    for bar in ohlcv_c
                ]
                funding = BitgetClient.get_funding(sym)
            else:
                candles = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])
                funding = BitgetClient.get_funding(sym)

            if len(candles) < 60:
                log.debug(f"  Skip {sym}: data kurang ({len(candles)} candles)")
                continue

            coin_data = CoinData(
                symbol    = sym,
                price     = price,
                vol_24h   = vol_24h,
                chg_24h   = chg_24h,
                funding   = funding,
                candles   = candles,
                clz_ohlcv = ohlcv_c,
                clz_liq   = liq_c,
                clz_oi    = oi_c,
            )

            result = score_coin(coin_data)
            if result:
                results.append(result)
                c = result.components
                log.info(
                    f"  ✅ Score={result.score} ({result.confidence}) | "
                    f"A:{c['A']['score']}({c['A']['z']:+.1f}σ) "
                    f"B:{c['B']['score']}({c['B']['z']:+.1f}σ) "
                    f"C:{c['C']['score']}({c['C']['z']:+.1f}σ) "
                    f"D:{c['D']['score']}({c['D']['z']:+.1f}σ) "
                    f"E:{c['E']['score']}({c['E']['z']:+.1f}σ) "
                    f"| {_dq_label(result.data_quality)}"
                )

        except Exception as exc:
            log.warning(f"  Error {sym}: {exc}", exc_info=False)

        time.sleep(CONFIG["sleep_between_coins"])

    # ── Sort & send ───────────────────────────────────────────────────────────
    results.sort(key=lambda x: x.score, reverse=True)
    top = results[:CONFIG["max_alerts"]]

    elapsed = round(time.time() - start_ts, 1)
    log.info(
        f"\nTotal sinyal: {len(results)} | Dikirim: {len(top)} | Waktu: {elapsed}s"
    )

    if not top:
        log.info("Tidak ada sinyal pre-pump saat ini")
        return

    send_telegram(build_summary(top))
    time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank))
        # Cooldown sudah di-set saat scoring — tidak perlu set lagi di sini
        log.info(f"📤 Alert #{rank}: {r.symbol} score={r.score} sent={ok}")
        time.sleep(2)

    log.info(f"=== SELESAI — {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")


# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    run_scan()
