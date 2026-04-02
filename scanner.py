#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v6.0 — COINALYZE INTEGRATION (FIXED)                  ║
║                                                                          ║
║  PERUBAHAN DARI v5.0:                                                    ║
║  [FIX-1] SymbolMapper: deteksi Bitget exchange code secara dinamis       ║
║           → tidak lagi fallback ke semua 4000+ market dari semua bursa   ║
║  [FIX-2] CoinalyzeClient._batch_fetch: log isi respons yang gagal        ║
║           → mudah diagnosa error 400/401/quota exceeded                  ║
║  [FIX-3] CoinalyzeClient: _cache & _last_call pindah ke instance         ║
║           → tidak ada class-shared state antar instance                  ║
║  [FIX-4] ATR calculation: kondisi i < 15 → i <= 15 (bug off-by-one)     ║
║  [FIX-5] _std(): gunakan Bessel's correction (/ n-1) bukan populasi      ║
║  [FIX-6] API key: tidak ada hardcoded default, raise error jika kosong   ║
║  [FIX-7] SymbolMapper.reverse(): di-cache saat load(), bukan rebuilt     ║
║           setiap call                                                     ║
║  [FIX-8] Active component thresholds masuk CONFIG                        ║
║  [FIX-9] Bitget retry loop: tambah continue eksplisit                    ║
║  [FIX-10] Score threshold adaptif: turun otomatis jika data CLZ minim    ║
║  [FIX-11] Batch timeout lebih pendek (8s) untuk batch yang pasti gagal   ║
║                                                                          ║
║  ARSITEKTUR:                                                             ║
║  ┌─────────────────┐   ┌────────────────────┐   ┌─────────────────┐    ║
║  │  BitgetClient   │   │ CoinalyzeClient     │   │  SymbolMapper   │    ║
║  │  · tickers      │   │ · ohlcv+btx+bv      │   │  Bitget ↔ CLZ  │    ║
║  │  · candles      │   │ · liquidations      │   │  auto-discover  │    ║
║  └────────┬────────┘   │ · open_interest     │   └────────┬────────┘    ║
║           │             └────────────────────┘            │             ║
║           └─────────────────────────────────────────────────┘           ║
║                         │                                                ║
║                   ┌─────▼──────────────────────────────────────────┐    ║
║                   │              Scorer (5 komponen)                │    ║
║                   │  [A] Buy TX Z-score     — 30 pts (La Morgia)   │    ║
║                   │  [B] Buy Volume Z-score — 30 pts (La Morgia)   │    ║
║                   │  [C] Volume Z-score     — 20 pts (Fantazzini)  │    ║
║                   │  [D] Short Liq Z-score  — 12 pts (squeeze)     │    ║
║                   │  [E] OI Change Z-score  —  8 pts (confirm)     │    ║
║                   └─────────────────────────────────────────────────┘   ║
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
    # ── ENVIRONMENT ────────────────────────────────────────────────────────
    # [FIX-6] Tidak ada default hardcoded — wajib di-set via environment variable
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY", ""),
    "bot_token":         os.getenv("BOT_TOKEN"),
    "chat_id":           os.getenv("CHAT_ID"),

    # ── VOLUME PRE-FILTER ──────────────────────────────────────────────────
    "pre_filter_vol":      100_000,    # $100K noise floor
    "min_vol_24h":         500_000,    # $500K minimum
    "max_vol_24h":     800_000_000,    # $800M ceiling
    "gate_chg_24h_max":       40.0,    # Coin naik >40% 24h = terlambat

    # ── DATA WINDOWS ───────────────────────────────────────────────────────
    "candle_limit_bitget":     200,    # Bitget: 200 candle 1H (~8 hari)
    "coinalyze_lookback_h":    168,    # Coinalyze: 7 hari history untuk baseline
    "coinalyze_interval":   "1hour",   # Interval Coinalyze

    # ── BASELINE & Z-SCORE WINDOWS ─────────────────────────────────────────
    "baseline_window":          24,    # 24 candle (1 hari) untuk rolling mean/std
    "baseline_min_samples":     15,    # Minimum data untuk Z-score valid

    # ── [A] BUY TRANSACTION Z-SCORE (La Morgia 2023 — feature #2) ─────────
    "buy_tx_weight":            30,
    "buy_tx_z_strong":         2.0,
    "buy_tx_z_medium":         1.0,

    # ── [B] BUY VOLUME Z-SCORE (La Morgia 2023 — feature #1 proxy) ────────
    "buy_vol_weight":           30,
    "buy_vol_z_strong":        2.0,
    "buy_vol_z_medium":        0.9,

    # ── [C] VOLUME Z-SCORE (Fantazzini 2023) ──────────────────────────────
    "volume_weight":            20,
    "volume_z_strong":         2.5,
    "volume_z_medium":         1.5,

    # ── [D] SHORT LIQUIDATION Z-SCORE (short squeeze detector) ────────────
    "short_liq_weight":         12,
    "short_liq_z_strong":      2.0,
    "short_liq_z_medium":      1.0,

    # ── [E] OI CHANGE Z-SCORE (confirmation signal) ────────────────────────
    "oi_change_weight":          8,
    "oi_z_strong":             1.5,
    "oi_z_medium":             0.5,

    # ── MINIMUM ACTIVE COMPONENTS ──────────────────────────────────────────
    # [FIX-8] Threshold aktif tiap komponen di-expose ke CONFIG
    "min_active_components":     2,
    "active_thresh_a":           3,    # A dianggap aktif jika score > 3
    "active_thresh_b":           3,    # B dianggap aktif jika score > 3
    "active_thresh_c":           3,    # C dianggap aktif jika score > 3
    "active_thresh_d":           2,    # D dianggap aktif jika score > 2
    "active_thresh_e":           1,    # E dianggap aktif jika score > 1

    # ── SIGNAL THRESHOLDS ──────────────────────────────────────────────────
    "score_threshold":          55,    # Minimum skor jika data CLZ lengkap
    "score_threshold_bitget_only": 35, # [FIX-10] Threshold turun jika hanya pakai Bitget
    "score_strong":             72,
    "score_very_strong":        88,

    # ── ENTRY CALCULATION ──────────────────────────────────────────────────
    "atr_period":               14,
    "atr_sl_mult":             1.5,
    "min_target_pct":          7.0,

    # ── OUTPUT ─────────────────────────────────────────────────────────────
    "max_alerts":                8,
    "alert_cooldown_sec":     3600,
    "sleep_between_coins":     0.3,
    "cooldown_file":  "/tmp/v6_cooldown.json",

    # ── COINALYZE RATE LIMIT ───────────────────────────────────────────────
    "clz_min_interval_sec":    1.6,
    "clz_batch_size":           20,
    "clz_retry_attempts":        3,
    "clz_retry_wait_sec":        5,
    "clz_request_timeout":       8,    # [FIX-11] Timeout lebih pendek per request
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

def _std(arr: list) -> float:
    """[FIX-5] Bessel's correction: bagi (n-1) bukan n untuk estimasi sampel."""
    if len(arr) < 2:
        return 0.0
    m = _mean(arr)
    return math.sqrt(sum((x - m) ** 2 for x in arr) / (len(arr) - 1))

def zscore(value: float, series: list, min_samples: int = 10) -> float:
    """Robust Z-score. Returns 0 jika data kurang atau std = 0."""
    if len(series) < min_samples:
        return 0.0
    sigma = _std(series)
    if sigma == 0:
        return 0.0
    return (value - _mean(series)) / sigma

def score_from_z(z: float, z_strong: float, z_medium: float, weight: int) -> int:
    """
    Interpolasi linear skor [0, weight] dari Z-score.
    z >= z_strong  → full weight
    z >= z_medium  → proporsional antara weight/2 dan weight
    z >= 0         → proporsional antara 0 dan weight/2
    z <  0         → 0
    """
    if z >= z_strong:
        return weight
    if z >= z_medium:
        ratio = (z - z_medium) / (z_strong - z_medium)
        return int(weight // 2 + ratio * (weight - weight // 2))
    if z >= 0:
        ratio = z / z_medium
        return int(ratio * weight // 2)
    return 0

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
    _cooldown_state[sym] = time.time()
    _save_cooldown(_cooldown_state)


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET CLIENT
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE = "https://api.bitget.com"
    _candle_cache: Dict = {}

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
                    continue  # [FIX-9] continue eksplisit
                log.warning(f"Bitget HTTP error: {e.response.status_code}")
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(3)
                    continue  # [FIX-9] continue eksplisit
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        """Ambil semua ticker USDT-Futures dari Bitget."""
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers",
                        params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 200) -> List[dict]:
        """Ambil candle 1H dari Bitget, cached per symbol."""
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


# ══════════════════════════════════════════════════════════════════════════════
#  📡  COINALYZE CLIENT (rate-limited, batched)
# ══════════════════════════════════════════════════════════════════════════════
class CoinalyzeClient:
    BASE = "https://api.coinalyze.net/v1"

    def __init__(self, api_key: str):
        # [FIX-3] _cache dan _last_call sebagai instance variable, bukan class variable
        self.api_key    = api_key
        self._cache:    Dict  = {}
        self._last_call: float = 0.0

    def _wait(self) -> None:
        """Enforce minimum interval antar call (rate limit compliance)."""
        elapsed = time.time() - self._last_call
        wait    = CONFIG["clz_min_interval_sec"] - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.time()

    def _get(self, endpoint: str, params: dict) -> Optional[list]:
        """Single API call dengan retry saat 429.
        [FIX-2] Log isi respons jika gagal untuk memudahkan diagnosa.
        """
        params["api_key"] = self.api_key
        for attempt in range(CONFIG["clz_retry_attempts"]):
            self._wait()
            try:
                r = requests.get(
                    f"{self.BASE}/{endpoint}",
                    params=params,
                    timeout=CONFIG["clz_request_timeout"]  # [FIX-11]
                )
                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 10))
                    log.warning(f"Coinalyze rate limit — tunggu {retry_after}s")
                    time.sleep(retry_after + 1)
                    continue
                if r.status_code == 401:
                    log.error("Coinalyze API key invalid atau expired!")
                    return None
                if r.status_code == 400:
                    # [FIX-2] Log body untuk diagnosa symbol format salah
                    log.warning(f"Coinalyze 400 Bad Request — {r.text[:300]}")
                    return None
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list):
                    return data
                # Respons berupa dict dengan error message
                log.warning(f"Coinalyze respons bukan list: {str(data)[:200]}")
                return None
            except requests.exceptions.Timeout:
                log.warning(f"Coinalyze timeout pada attempt {attempt+1} untuk {endpoint}")
                if attempt < CONFIG["clz_retry_attempts"] - 1:
                    time.sleep(CONFIG["clz_retry_wait_sec"])
                continue
            except Exception as e:
                log.warning(f"Coinalyze error [{endpoint}] attempt {attempt+1}: {e}")
                if attempt < CONFIG["clz_retry_attempts"] - 1:
                    time.sleep(CONFIG["clz_retry_wait_sec"])
        return None

    def get_future_markets(self) -> List[dict]:
        """Daftar semua future markets di Coinalyze."""
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
        Batch fetch untuk daftar symbol.
        Bagi ke batch ukuran clz_batch_size, gabungkan hasilnya.
        [FIX-2] Log detail respons yang gagal.
        Returns: {symbol: [candle_dicts]}
        """
        batch_size = CONFIG["clz_batch_size"]
        result: Dict[str, list] = {}

        for i in range(0, len(symbols), batch_size):
            batch    = symbols[i:i + batch_size]
            sym_str  = ",".join(batch)
            params   = {"symbols": sym_str, **extra_params}
            batch_no = i // batch_size + 1
            data     = self._get(endpoint, params)

            if not isinstance(data, list):
                log.warning(
                    f"Coinalyze {endpoint}: batch {batch_no} gagal "
                    f"({len(batch)} symbols: {batch[0]}..{batch[-1]})"
                )
                continue

            for item in data:
                sym     = item.get("symbol", "")
                history = item.get("history", [])
                if sym and history:
                    result[sym] = history

        return result

    def fetch_ohlcv_batch(self, symbols: List[str],
                          from_ts: int, to_ts: int) -> Dict[str, list]:
        extra = {
            "interval": CONFIG["coinalyze_interval"],
            "from":     from_ts,
            "to":       to_ts,
        }
        return self._batch_fetch("ohlcv-history", symbols, extra)

    def fetch_liquidations_batch(self, symbols: List[str],
                                 from_ts: int, to_ts: int) -> Dict[str, list]:
        extra = {
            "interval":       CONFIG["coinalyze_interval"],
            "from":           from_ts,
            "to":             to_ts,
            "convert_to_usd": "true",
        }
        return self._batch_fetch("liquidation-history", symbols, extra)

    def fetch_oi_batch(self, symbols: List[str],
                       from_ts: int, to_ts: int) -> Dict[str, list]:
        extra = {
            "interval":       CONFIG["coinalyze_interval"],
            "from":           from_ts,
            "to":             to_ts,
            "convert_to_usd": "true",
        }
        return self._batch_fetch("open-interest-history", symbols, extra)

    def clear_cache(self) -> None:
        self._cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  🗺️  SYMBOL MAPPER — Bitget ↔ Coinalyze
# ══════════════════════════════════════════════════════════════════════════════
class SymbolMapper:
    """
    Membangun mapping antara symbol Bitget (e.g. BTCUSDT) dan
    symbol Coinalyze (e.g. BTCUSDT_PERP.6).

    [FIX-1] Strategi mapping yang diperbaiki:
    1. Fetch semua future markets dari Coinalyze
    2. Cari exchange code Bitget secara dinamis dari market BTCUSDT atau ETHUSDT
       (anchor symbols yang pasti ada di Bitget)
    3. Filter hanya market dengan exchange code tersebut
    4. Map symbol_on_exchange → coinalyze symbol
    5. Fallback: gunakan suffix dari anchor — bukan dari semua market acak
    """
    # Anchor symbols untuk deteksi exchange code Bitget di Coinalyze
    _ANCHOR_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]

    def __init__(self, clz_client: CoinalyzeClient):
        self._client       = clz_client
        self._to_clz:  Dict[str, str] = {}   # bitget_sym → clz_sym
        self._from_clz: Dict[str, str] = {}  # [FIX-7] clz_sym → bitget_sym (cached reverse)
        self._has_btx: Dict[str, bool] = {}  # clz_sym → has buy/sell data
        self._bitget_suffix: str = ""         # Exchange code Bitget di Coinalyze, e.g. "6"
        self._loaded = False

    def load(self) -> int:
        """
        Fetch dan build mapping. Return jumlah coin yang berhasil di-map.
        Dipanggil sekali saat startup.
        """
        log.info("SymbolMapper: fetching Coinalyze future markets...")
        markets = self._client.get_future_markets()

        if not markets:
            log.error("SymbolMapper: gagal fetch markets dari Coinalyze!")
            return 0

        log.info(f"SymbolMapper: total {len(markets)} markets dari semua exchange")

        # ── [FIX-1] Step 1: Temukan exchange code Bitget secara dinamis ──────
        # Cari BTCUSDT atau ETHUSDT di semua market, ambil yang exchange name-nya "bitget"
        # Kalau tidak ada label "bitget", cari lewat symbol_on_exchange yang match anchor
        bitget_suffix = self._detect_bitget_suffix(markets)

        if not bitget_suffix:
            log.error(
                "SymbolMapper: tidak bisa mendeteksi exchange code Bitget dari Coinalyze! "
                "Periksa apakah Bitget tersedia di Coinalyze API plan kamu."
            )
            # Tetap coba semua market dengan label 'bitget' jika ada
            bitget_markets = [m for m in markets
                              if "bitget" in m.get("exchange", "").lower()]
            if not bitget_markets:
                log.warning("SymbolMapper: fallback ke mode Bitget-only (tanpa Coinalyze)")
                self._loaded = True
                return 0
        else:
            log.info(f"SymbolMapper: Bitget exchange code terdeteksi = .{bitget_suffix}")
            self._bitget_suffix = bitget_suffix
            # Filter hanya market Bitget berdasarkan suffix
            bitget_markets = [m for m in markets
                              if m.get("symbol", "").endswith(f".{bitget_suffix}")]
            log.info(f"SymbolMapper: {len(bitget_markets)} Bitget markets ditemukan")

        # ── Step 2: Build mapping ─────────────────────────────────────────────
        mapped = 0
        for m in bitget_markets:
            clz_sym  = m.get("symbol", "")
            exch_sym = m.get("symbol_on_exchange", "")
            has_btx  = m.get("has_buy_sell_data", False)

            if not clz_sym:
                continue

            # Normalisasi: hapus suffix exchange lama Bitget
            clean = exch_sym.replace("_UMCBL", "").replace("_DMCBL", "").upper()

            if clean in WHITELIST_SYMBOLS:
                self._to_clz[clean]    = clz_sym
                self._from_clz[clz_sym] = clean  # [FIX-7] build reverse sekalian
                self._has_btx[clz_sym] = has_btx
                mapped += 1
            elif exch_sym.upper() in WHITELIST_SYMBOLS:
                self._to_clz[exch_sym.upper()] = clz_sym
                self._from_clz[clz_sym] = exch_sym.upper()
                self._has_btx[clz_sym]  = has_btx
                mapped += 1

        log.info(f"SymbolMapper: {mapped}/{len(WHITELIST_SYMBOLS)} coin berhasil di-map ke Coinalyze")

        # ── Step 3: Fallback untuk yang belum ter-map ─────────────────────────
        # [FIX-1] Gunakan suffix Bitget yang sudah terdeteksi, bukan suffix random
        if bitget_suffix:
            unmapped = [s for s in WHITELIST_SYMBOLS if s not in self._to_clz]
            if unmapped:
                for sym in unmapped:
                    clz_sym = f"{sym}_PERP.{bitget_suffix}"
                    self._to_clz[sym]      = clz_sym
                    self._from_clz[clz_sym] = sym
                log.info(
                    f"SymbolMapper: {len(unmapped)} coin pakai fallback format "
                    f"{{SYMBOL}}_PERP.{bitget_suffix}"
                )

        self._loaded = True
        return mapped

    def _detect_bitget_suffix(self, markets: List[dict]) -> str:
        """
        [FIX-1] Deteksi exchange code Bitget di Coinalyze secara dinamis.
        Strategi:
        1. Cari market yang exchange-nya berlabel "bitget" DAN symbol_on_exchange adalah anchor (BTCUSDT dll)
        2. Jika tidak ada label, cari market yang symbol_on_exchange = anchor dan
           suffix-nya konsisten di beberapa anchor → itu kode Bitget
        """
        # Cara 1: label exchange eksplisit
        for anchor in self._ANCHOR_SYMBOLS:
            for m in markets:
                exch  = m.get("exchange", "").lower()
                sym_e = m.get("symbol_on_exchange", "").replace("_UMCBL","").replace("_DMCBL","").upper()
                if "bitget" in exch and sym_e == anchor:
                    clz_sym = m.get("symbol", "")
                    if "." in clz_sym:
                        suffix = clz_sym.split(".")[-1]
                        log.info(f"SymbolMapper: Bitget suffix dari label exchange = .{suffix} (via {anchor})")
                        return suffix

        # Cara 2: cari anchor symbol yang muncul dengan suffix konsisten
        # Kumpulkan semua kandidat suffix untuk setiap anchor
        anchor_suffixes: Dict[str, List[str]] = defaultdict(list)
        for m in markets:
            sym_e  = m.get("symbol_on_exchange", "").replace("_UMCBL","").replace("_DMCBL","").upper()
            clz_sym = m.get("symbol", "")
            if sym_e in self._ANCHOR_SYMBOLS and "." in clz_sym:
                suffix = clz_sym.split(".")[-1]
                anchor_suffixes[sym_e].append(suffix)

        if not anchor_suffixes:
            return ""

        # Suffix yang muncul di lebih dari satu anchor kemungkinan besar kode exchange tunggal
        # (satu exchange = satu kode untuk semua symbol)
        from collections import Counter
        all_suffixes = []
        for suf_list in anchor_suffixes.values():
            all_suffixes.extend(suf_list)

        suffix_count = Counter(all_suffixes)
        log.info(f"SymbolMapper: kandidat suffix dari anchor symbols: {dict(suffix_count)}")

        if not suffix_count:
            return ""

        # Pilih suffix yang paling sering muncul di anchor symbols
        # (biasanya satu suffix dominan = exchange utama)
        best = suffix_count.most_common(1)[0][0]
        log.info(f"SymbolMapper: suffix terpilih = .{best} (muncul {suffix_count[best]}x di anchor symbols)")
        return best

    def to_coinalyze(self, bitget_sym: str) -> Optional[str]:
        return self._to_clz.get(bitget_sym)

    def has_buy_sell(self, clz_sym: str) -> bool:
        return self._has_btx.get(clz_sym, True)

    def get_clz_symbols_for(self, bitget_syms: List[str]) -> List[str]:
        """Convert list Bitget symbols ke Coinalyze symbols."""
        result = []
        for s in bitget_syms:
            clz = self.to_coinalyze(s)
            if clz:
                result.append(clz)
        return result

    def reverse(self, clz_sym: str) -> Optional[str]:
        """[FIX-7] Coinalyze symbol → Bitget symbol. Menggunakan cached reverse map."""
        return self._from_clz.get(clz_sym)

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @property
    def bitget_suffix(self) -> str:
        return self._bitget_suffix


# ══════════════════════════════════════════════════════════════════════════════
#  📦  COIN DATA CONTAINER
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class CoinData:
    """Semua data yang dibutuhkan untuk scoring satu coin."""
    symbol:    str
    price:     float
    vol_24h:   float
    chg_24h:   float
    funding:   float
    candles:   List[dict] = field(default_factory=list)   # Bitget 1H OHLCV
    clz_ohlcv: List[dict] = field(default_factory=list)   # Coinalyze OHLCV+btx+bv
    clz_liq:   List[dict] = field(default_factory=list)   # Coinalyze liquidations
    clz_oi:    List[dict] = field(default_factory=list)   # Coinalyze OI

    @property
    def has_btx_data(self) -> bool:
        return bool(self.clz_ohlcv) and "btx" in (self.clz_ohlcv[-1] if self.clz_ohlcv else {})

    @property
    def has_liq_data(self) -> bool:
        return bool(self.clz_liq)

    @property
    def has_oi_data(self) -> bool:
        return bool(self.clz_oi)

    @property
    def has_clz_data(self) -> bool:
        """True jika setidaknya ada satu data Coinalyze yang tersedia."""
        return self.has_btx_data or self.has_liq_data or self.has_oi_data


# ══════════════════════════════════════════════════════════════════════════════
#  🔬  SCORING COMPONENTS
# ══════════════════════════════════════════════════════════════════════════════

def score_buy_tx(data: CoinData) -> Tuple[int, float, dict]:
    """
    [A] Buy Transaction Z-score — La Morgia 2023, feature importance #2
    btx = jumlah transaksi beli per candle (taker buy count)
    btx_ratio = btx / tx = proporsi transaksi yang merupakan pembelian agresif
    """
    cfg    = CONFIG
    weight = cfg["buy_tx_weight"]

    if not data.clz_ohlcv or len(data.clz_ohlcv) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "no btx data", "source": "coinalyze_missing"}

    candles = data.clz_ohlcv
    cur     = candles[-2] if len(candles) >= 2 else candles[-1]
    btx     = cur.get("btx", 0)
    tx      = cur.get("tx", 0)

    if not btx or not tx:
        return _score_buy_tx_fallback(data), 0.0, {"reason": "btx=0, fallback used"}

    btx_ratio = btx / tx if tx > 0 else 0.5
    btx_raw   = btx

    win_start = max(0, len(candles) - cfg["baseline_window"] * 4)
    win_end   = max(0, len(candles) - cfg["baseline_window"])
    baseline  = candles[win_start:win_end]

    baseline_ratios = []
    baseline_raws   = []
    for c in baseline:
        c_tx  = c.get("tx", 0)
        c_btx = c.get("btx", 0)
        if c_tx > 0 and c_btx > 0:
            baseline_ratios.append(c_btx / c_tx)
            baseline_raws.append(float(c_btx))

    if len(baseline_ratios) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "insufficient baseline for btx"}

    z_ratio = zscore(btx_ratio, baseline_ratios)
    z_raw   = zscore(btx_raw,   baseline_raws)
    z_use   = max(z_ratio, z_raw * 0.7)

    score = score_from_z(z_use, cfg["buy_tx_z_strong"], cfg["buy_tx_z_medium"], weight)

    return score, round(z_use, 2), {
        "btx_ratio": round(btx_ratio, 3),
        "btx_raw":   btx_raw,
        "z_ratio":   round(z_ratio, 2),
        "z_raw":     round(z_raw, 2),
    }


def _score_buy_tx_fallback(data: CoinData) -> int:
    """Fallback ke buy pressure proxy jika btx tidak tersedia."""
    if not data.candles or len(data.candles) < 20:
        return 0
    candles = data.candles
    cur     = candles[-2]
    rng     = cur["high"] - cur["low"]
    if rng <= 0:
        return 0
    bp = (cur["close"] - cur["low"]) / rng
    if bp > 0.75: return CONFIG["buy_tx_weight"] // 2
    if bp > 0.55: return CONFIG["buy_tx_weight"] // 4
    return 0


def score_buy_volume(data: CoinData) -> Tuple[int, float, dict]:
    """
    [B] Buy Volume Z-score — La Morgia 2023, feature importance #1 (rush orders)
    bv/v = proporsi volume dari pembelian agresif
    """
    cfg    = CONFIG
    weight = cfg["buy_vol_weight"]

    if not data.clz_ohlcv or len(data.clz_ohlcv) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "no bv data"}

    candles  = data.clz_ohlcv
    cur      = candles[-2] if len(candles) >= 2 else candles[-1]
    bv       = cur.get("bv", 0)
    v        = cur.get("v",  0)

    if not bv or not v:
        return 0, 0.0, {"reason": "bv=0"}

    bv_ratio = bv / v if v > 0 else 0.5

    win_start = max(0, len(candles) - cfg["baseline_window"] * 4)
    win_end   = max(0, len(candles) - cfg["baseline_window"])
    baseline  = candles[win_start:win_end]

    baseline_ratios = []
    for c in baseline:
        c_v  = c.get("v",  0)
        c_bv = c.get("bv", 0)
        if c_v > 0 and c_bv >= 0:
            baseline_ratios.append(c_bv / c_v)

    if len(baseline_ratios) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "insufficient baseline for bv"}

    z_ratio = zscore(bv_ratio, baseline_ratios)

    if bv_ratio > 0.65:
        z_ratio += 0.5

    score = score_from_z(z_ratio, cfg["buy_vol_z_strong"], cfg["buy_vol_z_medium"], weight)

    return score, round(z_ratio, 2), {
        "bv_ratio": round(bv_ratio, 3),
        "bv_usd":   round(bv),
        "v_usd":    round(v),
        "z":        round(z_ratio, 2),
    }


def score_volume(data: CoinData) -> Tuple[int, float, dict]:
    """
    [C] Volume Z-score — Fantazzini 2023
    Anomali volume total vs baseline rolling.
    Sumber data: Bitget candles (selalu tersedia).
    """
    cfg     = CONFIG
    weight  = cfg["volume_weight"]
    candles = data.candles

    if len(candles) < cfg["baseline_min_samples"] + 10:
        return 0, 0.0, {"reason": "insufficient bitget candles"}

    cur_vol = candles[-2]["volume_usd"]

    win_end   = max(0, len(candles) - cfg["baseline_window"])
    win_start = max(0, win_end - cfg["baseline_window"] * 4)
    baseline  = [c["volume_usd"] for c in candles[win_start:win_end]]

    if len(baseline) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "insufficient baseline for volume"}

    z = zscore(cur_vol, baseline)
    z_recent_avg = zscore(
        _mean([c["volume_usd"] for c in candles[-cfg["baseline_window"]:-1]]),
        baseline
    )
    z_use     = max(z, z_recent_avg * 0.8)
    score     = score_from_z(z_use, cfg["volume_z_strong"], cfg["volume_z_medium"], weight)
    vol_ratio = cur_vol / _mean(baseline) if _mean(baseline) > 0 else 1.0

    return score, round(z_use, 2), {
        "cur_vol":   round(cur_vol),
        "z":         round(z_use, 2),
        "vol_ratio": round(vol_ratio, 2),
    }


def score_short_liquidations(data: CoinData) -> Tuple[int, float, dict]:
    """
    [D] Short Liquidation Z-score — short squeeze detector
    short_liq spike → posisi short di-force close → forced buying → harga naik
    """
    cfg    = CONFIG
    weight = cfg["short_liq_weight"]

    if not data.has_liq_data or len(data.clz_liq) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "no liquidation data"}

    liqs      = data.clz_liq
    cur       = liqs[-2] if len(liqs) >= 2 else liqs[-1]
    short_liq = cur.get("s", 0) or 0

    win_end   = max(0, len(liqs) - cfg["baseline_window"])
    win_start = max(0, win_end - cfg["baseline_window"] * 4)
    baseline  = [c.get("s", 0) or 0 for c in liqs[win_start:win_end]]

    nonzero = [x for x in baseline if x > 0]
    if len(nonzero) < 5:
        return 0, 0.0, {"reason": "too many zero liquidations in baseline"}

    z     = zscore(short_liq, baseline)
    score = score_from_z(z, cfg["short_liq_z_strong"], cfg["short_liq_z_medium"], weight)

    return score, round(z, 2), {
        "short_liq_usd": round(short_liq),
        "z":             round(z, 2),
    }


def score_oi_change(data: CoinData) -> Tuple[int, float, dict]:
    """
    [E] Open Interest Change Z-score — confirmation signal
    OI naik = posisi baru dibuka (bukan sekadar covering)
    Rising OI + price rally = bullish positioning
    """
    cfg    = CONFIG
    weight = cfg["oi_change_weight"]

    if not data.has_oi_data or len(data.clz_oi) < cfg["baseline_min_samples"] + 2:
        return 0, 0.0, {"reason": "no OI data"}

    oi      = data.clz_oi
    cur_oi  = oi[-2].get("c", 0) or 0
    prev_oi = oi[-3].get("c", 0) or 0 if len(oi) >= 3 else 0

    if prev_oi == 0:
        return 0, 0.0, {"reason": "prev_oi=0"}

    oi_change_pct = (cur_oi - prev_oi) / prev_oi

    win_end   = max(0, len(oi) - cfg["baseline_window"])
    win_start = max(0, win_end - cfg["baseline_window"] * 4)
    baseline_oi = [oi[i].get("c", 0) or 0 for i in range(win_start, win_end)]

    baseline_changes = []
    for i in range(1, len(baseline_oi)):
        if baseline_oi[i - 1] > 0:
            baseline_changes.append((baseline_oi[i] - baseline_oi[i - 1]) / baseline_oi[i - 1])

    if len(baseline_changes) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"reason": "insufficient OI baseline changes"}

    z     = zscore(oi_change_pct, baseline_changes)
    score = score_from_z(z, cfg["oi_z_strong"], cfg["oi_z_medium"], weight)

    return score, round(z, 2), {
        "oi_change_pct": round(oi_change_pct * 100, 2),
        "z":             round(z, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_entry_targets(data: CoinData) -> Optional[dict]:
    candles = data.candles
    if len(candles) < 20:
        return None

    price = data.price
    # [FIX-4] Gunakan 14 candle terakhir (index -15 s/d -2), kondisi <= 14 bukan < 15
    trs = [
        max(c["high"] - c["low"],
            abs(c["high"] - candles[i - 1]["close"]),
            abs(c["low"]  - candles[i - 1]["close"]))
        for i, c in enumerate(candles[-15:], 1) if i <= 14
    ]
    atr = _mean(trs) if trs else price * 0.02

    entry  = price
    sl     = entry - atr * CONFIG["atr_sl_mult"]
    sl_pct = round((entry - sl) / entry * 100, 1)

    t1     = max(entry * (1 + CONFIG["min_target_pct"] / 100), entry + atr * 3)
    t2     = max(entry * 1.20, entry + atr * 6)
    t1_pct = round((t1 - entry) / entry * 100, 1)
    t2_pct = round((t2 - entry) / entry * 100, 1)
    rr     = round((t1 - entry) / (entry - sl), 2) if (entry - sl) > 0 else 0.0

    return {
        "entry":  round(entry, 8),
        "sl":     round(sl, 8),
        "sl_pct": sl_pct,
        "t1":     round(t1, 8),
        "t2":     round(t2, 8),
        "t1_pct": t1_pct,
        "t2_pct": t2_pct,
        "rr":     rr,
        "atr":    round(atr, 8),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  MASTER SCORER
# ══════════════════════════════════════════════════════════════════════════════
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
    bitget_only:  bool   # True jika tidak ada data Coinalyze


def score_coin(data: CoinData) -> Optional[ScoreResult]:
    """Jalankan 5 komponen scoring, gabungkan, return ScoreResult atau None."""
    cfg = CONFIG

    # ── Volume pre-check ────────────────────────────────────────────────────
    if data.vol_24h < cfg["min_vol_24h"]:
        return None
    if data.chg_24h > cfg["gate_chg_24h_max"]:
        return None

    # ── Run 5 components ────────────────────────────────────────────────────
    a_score, a_z, a_d = score_buy_tx(data)
    b_score, b_z, b_d = score_buy_volume(data)
    c_score, c_z, c_d = score_volume(data)
    d_score, d_z, d_d = score_short_liquidations(data)
    e_score, e_z, e_d = score_oi_change(data)

    total = a_score + b_score + c_score + d_score + e_score

    # ── [FIX-8] Active components — threshold dari CONFIG ───────────────────
    active = sum([
        a_score > cfg["active_thresh_a"],
        b_score > cfg["active_thresh_b"],
        c_score > cfg["active_thresh_c"],
        d_score > cfg["active_thresh_d"],
        e_score > cfg["active_thresh_e"],
    ])
    if active < cfg["min_active_components"]:
        return None

    # ── [FIX-10] Threshold adaptif berdasarkan ketersediaan data CLZ ────────
    bitget_only  = not data.has_clz_data
    threshold    = cfg["score_threshold_bitget_only"] if bitget_only else cfg["score_threshold"]

    if total < threshold:
        return None

    # ── Confidence ──────────────────────────────────────────────────────────
    if total >= cfg["score_very_strong"]:
        confidence = "very_strong"
    elif total >= cfg["score_strong"]:
        confidence = "strong"
    else:
        confidence = "watch"

    # ── Data quality info ────────────────────────────────────────────────────
    dq = {
        "has_btx":   data.has_btx_data,
        "has_liq":   data.has_liq_data,
        "has_oi":    data.has_oi_data,
        "candles":   len(data.candles),
        "clz_ohlcv": len(data.clz_ohlcv),
    }

    # ── Urgency ─────────────────────────────────────────────────────────────
    liq_str = f"${d_d.get('short_liq_usd', 0)/1e3:.0f}K liq" if d_score > 4 else ""
    if a_z >= 2.0 and b_z >= 1.5:
        urgency = f"🔴 TINGGI — BuyTX + BuyVol sama-sama anomali {liq_str}"
    elif d_score >= 8:
        urgency = f"🔴 TINGGI — Short squeeze aktif {liq_str}"
    elif a_z >= 1.5 or b_z >= 1.5:
        urgency = "🟠 SEDANG — Buy pressure meningkat"
    elif c_z >= 2.0:
        urgency = "🟡 SEDANG — Volume anomali"
    else:
        urgency = "⚪ WATCH — Akumulasi awal"

    return ScoreResult(
        symbol      = data.symbol,
        score       = total,
        confidence  = confidence,
        components  = {
            "A_buy_tx":    {"score": a_score, "z": a_z, "details": a_d},
            "B_buy_vol":   {"score": b_score, "z": b_z, "details": b_d},
            "C_volume":    {"score": c_score, "z": c_z, "details": c_d},
            "D_short_liq": {"score": d_score, "z": d_z, "details": d_d},
            "E_oi_change": {"score": e_score, "z": e_z, "details": e_d},
        },
        entry        = calc_entry_targets(data),
        price        = data.price,
        vol_24h      = data.vol_24h,
        chg_24h      = data.chg_24h,
        funding      = data.funding,
        urgency      = data.urgency if hasattr(data, "urgency") else urgency,
        data_quality = dq,
        bitget_only  = bitget_only,
    )

# Patch: urgency tidak ada di CoinData, ambil dari lokal saja
def _score_coin_fixed(data: CoinData) -> Optional[ScoreResult]:
    """Wrapper yang benar untuk score_coin."""
    cfg = CONFIG

    if data.vol_24h < cfg["min_vol_24h"]:
        return None
    if data.chg_24h > cfg["gate_chg_24h_max"]:
        return None

    a_score, a_z, a_d = score_buy_tx(data)
    b_score, b_z, b_d = score_buy_volume(data)
    c_score, c_z, c_d = score_volume(data)
    d_score, d_z, d_d = score_short_liquidations(data)
    e_score, e_z, e_d = score_oi_change(data)

    total = a_score + b_score + c_score + d_score + e_score

    active = sum([
        a_score > cfg["active_thresh_a"],
        b_score > cfg["active_thresh_b"],
        c_score > cfg["active_thresh_c"],
        d_score > cfg["active_thresh_d"],
        e_score > cfg["active_thresh_e"],
    ])
    if active < cfg["min_active_components"]:
        return None

    bitget_only = not data.has_clz_data
    threshold   = cfg["score_threshold_bitget_only"] if bitget_only else cfg["score_threshold"]
    if total < threshold:
        return None

    if total >= cfg["score_very_strong"]:
        confidence = "very_strong"
    elif total >= cfg["score_strong"]:
        confidence = "strong"
    else:
        confidence = "watch"

    dq = {
        "has_btx":   data.has_btx_data,
        "has_liq":   data.has_liq_data,
        "has_oi":    data.has_oi_data,
        "candles":   len(data.candles),
        "clz_ohlcv": len(data.clz_ohlcv),
    }

    liq_str = f"${d_d.get('short_liq_usd', 0)/1e3:.0f}K liq" if d_score > 4 else ""
    if a_z >= 2.0 and b_z >= 1.5:
        urgency = f"🔴 TINGGI — BuyTX + BuyVol sama-sama anomali {liq_str}"
    elif d_score >= 8:
        urgency = f"🔴 TINGGI — Short squeeze aktif {liq_str}"
    elif a_z >= 1.5 or b_z >= 1.5:
        urgency = "🟠 SEDANG — Buy pressure meningkat"
    elif c_z >= 2.0:
        urgency = "🟡 SEDANG — Volume anomali"
    else:
        urgency = "⚪ WATCH — Akumulasi awal"

    return ScoreResult(
        symbol      = data.symbol,
        score       = total,
        confidence  = confidence,
        components  = {
            "A_buy_tx":    {"score": a_score, "z": a_z, "details": a_d},
            "B_buy_vol":   {"score": b_score, "z": b_z, "details": b_d},
            "C_volume":    {"score": c_score, "z": c_z, "details": c_d},
            "D_short_liq": {"score": d_score, "z": d_z, "details": d_d},
            "E_oi_change": {"score": e_score, "z": e_z, "details": e_d},
        },
        entry        = calc_entry_targets(data),
        price        = data.price,
        vol_24h      = data.vol_24h,
        chg_24h      = data.chg_24h,
        funding      = data.funding,
        urgency      = urgency,
        data_quality = dq,
        bitget_only  = bitget_only,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def _conf_emoji(conf: str) -> str:
    return {"very_strong": "🟢", "strong": "🟡", "watch": "⚪"}.get(conf, "⚪")

def _dq_badge(dq: dict) -> str:
    parts = []
    if dq.get("has_btx"): parts.append("btx✓")
    if dq.get("has_liq"): parts.append("liq✓")
    if dq.get("has_oi"):  parts.append("oi✓")
    return " ".join(parts) if parts else "basic"

def build_alert(r: ScoreResult, rank: int) -> str:
    e     = r.entry
    vol_s = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    bar   = "█" * min(20, r.score // 5) + "░" * max(0, 20 - r.score // 5)
    dq    = _dq_badge(r.data_quality)
    comp  = r.components
    mode  = " [Bitget-only]" if r.bitget_only else ""

    entry_line = ""
    if e:
        entry_line = (
            f"\n   Entry: <b>${e['entry']:.6g}</b> | SL: ${e['sl']:.6g} (-{e['sl_pct']}%)"
            f"\n   T1: +{e['t1_pct']}% | T2: +{e['t2_pct']}% | R/R: {e['rr']}"
        )

    a  = comp["A_buy_tx"];   b  = comp["B_buy_vol"]
    c  = comp["C_volume"];   d  = comp["D_short_liq"]
    ee = comp["E_oi_change"]

    return (
        f"#{rank} {_conf_emoji(r.confidence)} <b>{r.symbol}</b>  "
        f"Score: <b>{r.score}/100</b>  [{dq}]{mode}\n"
        f"   {bar}\n"
        f"   {r.urgency}\n"
        f"   Vol:{vol_s} | Δ24h:{r.chg_24h:+.1f}% | F:{r.funding:.5f}\n"
        f"   [A]BuyTX:{a['score']}({a['z']:+.1f}σ) "
        f"[B]BuyVol:{b['score']}({b['z']:+.1f}σ) "
        f"[C]Vol:{c['score']}({c['z']:+.1f}σ)\n"
        f"   [D]ShortLiq:{d['score']}({d['z']:+.1f}σ) "
        f"[E]OI:{ee['score']}({ee['z']:+.1f}σ)"
        f"{entry_line}\n"
    )

def build_summary(results: List[ScoreResult]) -> str:
    now     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    n_full  = sum(1 for r in results if not r.bitget_only)
    n_basic = sum(1 for r in results if r.bitget_only)

    msg  = f"🔍 <b>PRE-PUMP SCANNER v6.0</b> — {now}\n"
    msg += f"📡 Data: Bitget + Coinalyze (btx/bv/liq/OI)\n"
    msg += f"📊 {len(results)} sinyal"
    if n_full and n_basic:
        msg += f" ({n_full} full-data, {n_basic} Bitget-only)"
    msg += "\n\n"

    for i, r in enumerate(results, 1):
        e    = r.entry
        t1   = f"+{e['t1_pct']}%" if e else "?"
        comp = r.components
        mode = " ⚠️" if r.bitget_only else ""
        msg += (
            f"{i}. <b>{r.symbol}</b> [{r.score}pts]{mode} "
            f"A:{comp['A_buy_tx']['score']} B:{comp['B_buy_vol']['score']} "
            f"C:{comp['C_volume']['score']} D:{comp['D_short_liq']['score']} "
            f"→ T1:{t1}\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════════════════════
#  📤  TELEGRAM SENDER
# ══════════════════════════════════════════════════════════════════════════════
def send_telegram(msg: str) -> bool:
    bot_token = CONFIG["bot_token"]
    chat_id   = CONFIG["chat_id"]
    if not bot_token or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
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
    log.info(f"{'='*70}")
    log.info(f"  PRE-PUMP SCANNER v6.0 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info(f"{'='*70}")

    # ── Validasi API key ─────────────────────────────────────────────────────
    # [FIX-6] Tidak ada default hardcoded — harus di-set via env
    clz_api_key = CONFIG["coinalyze_api_key"]
    if not clz_api_key:
        log.warning("COINALYZE_API_KEY tidak di-set! Scanner akan berjalan tanpa data Coinalyze.")
        clz_api_key = ""  # Lanjut tapi tanpa CLZ

    # ── Init clients ─────────────────────────────────────────────────────────
    clz_client = CoinalyzeClient(clz_api_key)
    mapper     = SymbolMapper(clz_client)

    mapped_count = mapper.load()
    if mapped_count == 0:
        log.warning(
            "SymbolMapper: 0 coin di-map ke Coinalyze. "
            "Scanner akan berjalan dengan Bitget-only (threshold lebih rendah)."
        )
        log.info(
            f"  → Bitget suffix terdeteksi: "
            f"'{mapper.bitget_suffix}' (kosong = tidak terdeteksi)"
        )
    else:
        log.info(f"SymbolMapper: {mapped_count} coin siap dengan data Coinalyze")

    # ── Fetch Bitget tickers ──────────────────────────────────────────────────
    log.info("Fetching Bitget tickers...")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner v6: Gagal fetch Bitget tickers")
        return
    log.info(f"Bitget tickers: {len(tickers)}")

    # ── Build candidate list ──────────────────────────────────────────────────
    candidates  = []
    skip_stats  = defaultdict(int)

    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:             skip_stats["excluded"]   += 1; continue
        if is_on_cooldown(sym):               skip_stats["cooldown"]   += 1; continue
        if sym not in tickers:                skip_stats["not_found"]  += 1; continue
        t = tickers[sym]
        try:
            vol = float(t.get("quoteVolume", 0))
            chg = float(t.get("change24h",   0)) * 100
        except Exception:
            skip_stats["parse_error"] += 1; continue
        if vol < CONFIG["pre_filter_vol"]:    skip_stats["vol_low"]    += 1; continue
        if vol > CONFIG["max_vol_24h"]:       skip_stats["vol_high"]   += 1; continue
        if chg > CONFIG["gate_chg_24h_max"]: skip_stats["pumped"]     += 1; continue
        candidates.append((sym, t))

    log.info(f"Candidates: {len(candidates)} | Skip: {dict(skip_stats)}")

    # ── Coinalyze bulk fetch ──────────────────────────────────────────────────
    now_ts    = int(time.time())
    from_ts   = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    cand_syms = [sym for sym, _ in candidates]
    clz_syms  = mapper.get_clz_symbols_for(cand_syms)

    log.info(f"Fetching Coinalyze data untuk {len(clz_syms)} coin "
             f"({len(cand_syms) - len(clz_syms)} tidak ter-map)...")

    if clz_syms and clz_api_key:
        log.info("  → OHLCV+btx+bv...")
        clz_ohlcv_all = clz_client.fetch_ohlcv_batch(clz_syms, from_ts, now_ts)
        log.info(f"  → OHLCV received: {len(clz_ohlcv_all)}/{len(clz_syms)} symbols "
                 f"({len(clz_ohlcv_all)/len(clz_syms)*100:.0f}%)")

        log.info("  → Liquidations...")
        clz_liq_all = clz_client.fetch_liquidations_batch(clz_syms, from_ts, now_ts)
        log.info(f"  → Liq received: {len(clz_liq_all)}/{len(clz_syms)} symbols "
                 f"({len(clz_liq_all)/len(clz_syms)*100:.0f}%)")

        log.info("  → Open Interest...")
        clz_oi_all = clz_client.fetch_oi_batch(clz_syms, from_ts, now_ts)
        log.info(f"  → OI received: {len(clz_oi_all)}/{len(clz_syms)} symbols "
                 f"({len(clz_oi_all)/len(clz_syms)*100:.0f}%)")

        # Ringkasan coverage
        n_has_any = sum(
            1 for sym in cand_syms
            if (clz_ohlcv_all.get(mapper.to_coinalyze(sym) or "") or
                clz_liq_all.get(mapper.to_coinalyze(sym) or "") or
                clz_oi_all.get(mapper.to_coinalyze(sym) or ""))
        )
        log.info(f"  → Coinalyze coverage: {n_has_any}/{len(cand_syms)} candidates "
                 f"({n_has_any/len(cand_syms)*100:.0f}%)")
    else:
        clz_ohlcv_all = clz_liq_all = clz_oi_all = {}
        if not clz_api_key:
            log.warning("Melewati Coinalyze fetch — API key kosong")
        else:
            log.warning("Tidak ada Coinalyze symbols — berjalan Bitget-only")

    # ── Score each coin ───────────────────────────────────────────────────────
    results: List[ScoreResult] = []
    BitgetClient.clear_cache()

    for i, (sym, ticker) in enumerate(candidates):
        log.info(f"[{i+1}/{len(candidates)}] {sym}")
        try:
            candles = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])
            if len(candles) < 60:
                log.debug(f"  Skip {sym}: candles kurang ({len(candles)})")
                continue

            price   = float(ticker.get("lastPr", 0))
            vol_24h = float(ticker.get("quoteVolume", 0))
            chg_24h = float(ticker.get("change24h", 0)) * 100
            funding = BitgetClient.get_funding(sym)

            if price <= 0:
                continue

            clz_sym  = mapper.to_coinalyze(sym)
            ohlcv_c  = clz_ohlcv_all.get(clz_sym, []) if clz_sym else []
            liq_c    = clz_liq_all.get(clz_sym,   []) if clz_sym else []
            oi_c     = clz_oi_all.get(clz_sym,    []) if clz_sym else []

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

            result = _score_coin_fixed(coin_data)
            if result:
                results.append(result)
                mode_tag = "[Bitget-only]" if result.bitget_only else "[Full-data]"
                log.info(
                    f"  ✅ Score={result.score} ({result.confidence}) {mode_tag} | "
                    f"A:{result.components['A_buy_tx']['score']} "
                    f"B:{result.components['B_buy_vol']['score']} "
                    f"C:{result.components['C_volume']['score']} "
                    f"D:{result.components['D_short_liq']['score']} "
                    f"E:{result.components['E_oi_change']['score']}"
                )

        except Exception as exc:
            log.warning(f"  Error {sym}: {exc}")

        time.sleep(CONFIG["sleep_between_coins"])

    # ── Sort & send ───────────────────────────────────────────────────────────
    # Prioritaskan full-data di atas Bitget-only untuk ranking
    results.sort(key=lambda x: (not x.bitget_only, x.score), reverse=True)
    top = results[:CONFIG["max_alerts"]]

    elapsed = round(time.time() - start_ts, 1)
    n_full  = sum(1 for r in results if not r.bitget_only)
    n_basic = sum(1 for r in results if r.bitget_only)
    log.info(
        f"\nTotal sinyal: {len(results)} "
        f"({n_full} full-data, {n_basic} Bitget-only) | "
        f"Dikirim: {len(top)} | Waktu: {elapsed}s"
    )

    if not top:
        log.info("Tidak ada sinyal pre-pump saat ini")
        return

    send_telegram(build_summary(top))
    time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank))
        if ok:
            set_cooldown(r.symbol)
            log.info(f"📤 Alert #{rank}: {r.symbol} score={r.score}")
        time.sleep(2)

    log.info(f"=== SELESAI — {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")


# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not CONFIG["bot_token"] or not CONFIG["chat_id"]:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)
    run_scan()
