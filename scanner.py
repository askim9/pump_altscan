#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v7.0 (QUANTITATIVE MICROSTRUCTURE EDITION)             ║
║                                                                          ║
║  UPGRADES DARI v6:                                                       ║
║  1. Market Regime Gate    — Ambang batas confidence adaptif via ADX      ║
║  2. Z-Score Bounding      — Koreksi matematis bv_ratio bonus ke space 0-1║
║  3. Squeeze Vol-Gate      — Sinyal short_liq wajib dikonfirmasi volume   ║
║  4. Capitulation Gate     — Filter pisau jatuh dgn "Recovery Mode"       ║
║  5. Dynamic Whitelist     — Auto-scan listing baru bervolume tinggi      ║
║  6. 15M Volatility Spike  — Lazy evaluation untuk akselerasi real-time   ║
║  7. Adaptive ATR Stop     — Stop Loss dinamis sesuai kompresi volatilitas║
║  8. Outcome Tracker       — Telemetri win-rate empiris (T1/SL)           ║
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

VERSION = "7.0"

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger(); _root.setLevel(logging.INFO)
_ch   = logging.StreamHandler(); _ch.setFormatter(_fmt); _root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/scanner_v7.log", maxBytes=10 * 1024**2, backupCount=2)
_fh.setFormatter(_fmt); _root.addHandler(_fh)
log   = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # ── ENVIRONMENT ─────────────────────────────────────────────────────────
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token":         os.getenv("BOT_TOKEN"),
    "chat_id":           os.getenv("CHAT_ID"),

    # ── VOLUME PRE-FILTER ────────────────────────────────────────────────────
    "pre_filter_vol":      100_000,    # $100K noise floor
    "min_vol_24h":         500_000,    # $500K minimum
    "max_vol_24h":     800_000_000,    # $800M ceiling
    "gate_chg_24h_max":       40.0,    # >40% naik = terlambat
    "gate_chg_24h_min":      -20.0,    # <-20% turun = pisau jatuh (v7)

    # ── DYNAMIC WHITELIST (v7) ───────────────────────────────────────────────
    "dynamic_whitelist_enabled": True,

    # ── DATA WINDOWS ─────────────────────────────────────────────────────────
    "candle_limit_bitget":     200,    
    "coinalyze_lookback_h":    168,    
    "coinalyze_interval":   "1hour",

    # ── BASELINE WINDOWS ─────────────────────────────────────────────────────
    "baseline_recent_exclude":   3,    
    "baseline_lookback_n":      96,    
    "baseline_min_samples":     15,    

    # ── SCORING WEIGHTS ──────────────────────────────────────────────────────
    "buy_tx_ratio_weight":      25,
    "buy_tx_ratio_z_strong":   2.0,
    "buy_tx_ratio_z_medium":   1.0,

    "avg_buy_size_weight":      25,
    "avg_buy_size_z_strong":   2.0,
    "avg_buy_size_z_medium":   0.9,
    "bv_ratio_bonus_threshold": 0.62,   

    "volume_weight":            20,
    "volume_z_strong":         2.5,
    "volume_z_medium":         1.5,

    "short_liq_weight":         20,
    "short_liq_z_strong":      2.0,
    "short_liq_z_medium":      1.0,
    "short_liq_requires_vol_confirm": True, # v7: Squeeze butuh vol taker

    "oi_buildup_weight":        10,
    "oi_buildup_z_strong":     1.5,
    "oi_buildup_z_medium":     0.5,
    "oi_buildup_candles":        4,     

    "min_active_components":     2,
    "active_thresh_a":           2,    
    "active_thresh_b":           2,    
    "active_thresh_c":           2,    
    "active_thresh_d":           2,    
    "active_thresh_e":           1,    

    # ── REGIME THRESHOLDS (v7) ───────────────────────────────────────────────
    "regime_thresholds": {
        "TRENDING_UP":          60,    # Continuation logic
        "RANGING":              65,    # Baseline 
        "HIGH_VOLATILITY":      72,    # Filter noise
        "TRENDING_DOWN":        80     # Hindari false bottom
    },
    "score_strong":             78,    
    "score_very_strong":        90,    

    # ── ENTRY CALCULATION ────────────────────────────────────────────────────
    "atr_candles":              14,    
    "atr_sl_mult":             1.5,    # Base multiplier (v7 adaptive)
    "min_target_pct":          7.0,    

    # ── OUTPUT ───────────────────────────────────────────────────────────────
    "max_alerts":                8,
    "alert_cooldown_sec":     3600,
    "cooldown_file":  "/tmp/v7_cooldown.json",
    "sleep_between_coins":     0.0,

    # ── COINALYZE RATE LIMIT ─────────────────────────────────────────────────
    "clz_min_interval_sec":    1.6,    
    "clz_batch_size":           20,    
    "clz_retry_attempts":        2,    
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
#  📐  MATH & REGIME UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _mean(arr: list) -> float:
    return sum(arr) / len(arr) if arr else 0.0

def _median(series: list) -> float:
    if not series: return 0.0
    s = sorted(series)
    n = len(s)
    return (s[n // 2] + s[(n - 1) // 2]) / 2.0

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def robust_zscore(value: float, series: list, min_samples: int = 10) -> float:
    if len(series) < min_samples: return 0.0
    med = _median(series)
    mad = _median([abs(x - med) for x in series])

    if mad < 1e-10:
        if med < 1e-10: return 0.0
        pct_dev = (value - med) / med
        return float(max(-3.0, min(3.0, pct_dev * 3.0)))
    return 0.6745 * (value - med) / mad

def score_from_z(z: float, z_strong: float, z_medium: float, weight: int) -> int:
    if z_medium <= 0 or z_strong <= z_medium:
        return weight if z >= 1.0 else 0
    if z >= z_strong: return weight
    if z >= z_medium:
        ratio = (z - z_medium) / (z_strong - z_medium)
        return int(weight // 2 + ratio * (weight - weight // 2))
    if z >= 0:
        ratio = z / z_medium
        return int(ratio * weight // 2)
    return 0

def _build_baseline(series: list) -> list:
    n   = len(series)
    exc = CONFIG["baseline_recent_exclude"]
    lkb = CONFIG["baseline_lookback_n"]
    end   = max(0, n - exc)
    start = max(0, end - lkb)
    return series[start:end]

def detect_market_regime(candles: list, period: int = 14) -> str:
    """
    [V7 UPGRADE] ADX & MA based regime classifier. 
    Execution time <50ms per coin.
    """
    if len(candles) < period * 2:
        return "RANGING"

    trs, p_dms, n_dms = [], [], []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i-1]
        tr = max(c['high'] - c['low'], abs(c['high'] - p['close']), abs(c['low'] - p['close']))
        up_m = c['high'] - p['high']
        dn_m = p['low'] - c['low']
        
        trs.append(tr)
        p_dms.append(up_m if up_m > dn_m and up_m > 0 else 0.0)
        n_dms.append(dn_m if dn_m > up_m and dn_m > 0 else 0.0)

    def _smooth(data, per):
        res = [sum(data[:per])]
        for val in data[per:]:
            res.append(res[-1] - (res[-1] / per) + val)
        return res

    sm_tr = _smooth(trs, period)
    sm_pdm = _smooth(p_dms, period)
    sm_ndm = _smooth(n_dms, period)

    adx_vals = []
    for tr, pdm, ndm in zip(sm_tr, sm_pdm, sm_ndm):
        if tr < 1e-8:
            adx_vals.append(0.0)
            continue
        p_di = 100 * pdm / tr
        n_di = 100 * ndm / tr
        dx = 100 * abs(p_di - n_di) / (p_di + n_di) if (p_di + n_di) > 0 else 0
        adx_vals.append(dx)

    adx = sum(adx_vals[-period:]) / period
    sma_fast = sum(c['close'] for c in candles[-period:]) / period
    cur_close = candles[-1]['close']

    if adx > 40: return "HIGH_VOLATILITY"
    if adx > 25 and cur_close > sma_fast: return "TRENDING_UP"
    if adx > 25 and cur_close < sma_fast: return "TRENDING_DOWN"
    return "RANGING"


# ══════════════════════════════════════════════════════════════════════════════
#  🔒  STATE MANAGEMENT (COOLDOWN & OUTCOMES)
# ══════════════════════════════════════════════════════════════════════════════
def _load_cooldown() -> dict:
    try:
        if os.path.exists(CONFIG["cooldown_file"]):
            with open(CONFIG["cooldown_file"]) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items() if now - v < CONFIG["alert_cooldown_sec"]}
    except Exception: pass
    return {}

_cooldown_state = _load_cooldown()

def is_on_cooldown(sym: str) -> bool:
    return (time.time() - _cooldown_state.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym: str) -> None:
    _cooldown_state[sym] = time.time()
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(_cooldown_state, f)
    except Exception: pass


def check_outcomes(tickers: dict) -> None:
    """[V7 UPGRADE] Tracks T1/SL hits locally using fresh ticker data (zero API cost)."""
    path = "/tmp/v7_outcomes.json"
    if not os.path.exists(path): return
    try:
        with open(path, "r") as f: state = json.load(f)
        closed = won = 0
        now = time.time()
        for k, v in list(state.items()):
            if v["status"] != "OPEN": continue
            if now - v["time"] > 86400:  # Expire after 24h
                v["status"] = "EXPIRED"; continue
                
            sym = v["symbol"]
            if sym not in tickers: continue
            cur_price = float(tickers[sym].get("lastPr", 0))
            
            if cur_price >= v["t1"]:
                v["status"] = "HIT_T1"
                closed += 1; won += 1
            elif cur_price <= v["sl"]:
                v["status"] = "HIT_SL"
                closed += 1
                
        if closed > 0:
            log.info(f"📊 TRACKER: {closed} trades closed. Win Rate: {(won/closed)*100:.1f}%")
        with open(path, "w") as f: json.dump(state, f)
    except Exception as e:
        log.error(f"Outcome tracker error: {e}")

def record_alert(r: ScoreResult) -> None:
    """[V7 UPGRADE] Saves new alerts into local JSON for outcome evaluation."""
    path = "/tmp/v7_outcomes.json"
    state = {}
    try:
        if os.path.exists(path):
            with open(path, "r") as f: state = json.load(f)
        if r.entry:
            state[f"{r.symbol}_{int(time.time())}"] = {
                "symbol": r.symbol, "time": int(time.time()),
                "entry": r.entry["entry"], "sl": r.entry["sl"], "t1": r.entry["t1"],
                "status": "OPEN"
            }
        with open(path, "w") as f: json.dump(state, f)
    except Exception: pass


def fetch_dynamic_whitelist(tickers: dict) -> set:
    """
    [V7 UPGRADE] Unions static whitelist with new high-volume pairs dynamically.
    """
    if not CONFIG.get("dynamic_whitelist_enabled", True):
        return set(WHITELIST_SYMBOLS)
        
    dynamic = set(WHITELIST_SYMBOLS)
    added = 0
    
    # Prioritize major new liquidity
    sorted_tickers = sorted(tickers.items(), key=lambda x: float(x[1].get('quoteVolume', 0)), reverse=True)
    for sym, t in sorted_tickers:
        if added >= 50: break
        if sym in dynamic or sym in MANUAL_EXCLUDE: continue
        try:
            if float(t.get("quoteVolume", 0)) > 1_000_000:
                dynamic.add(sym)
                added += 1
        except Exception: pass
            
    if added > 0:
        log.info(f"Dynamic whitelist added {added} new high-volume pairs.")
    return dynamic


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  API CLIENTS
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
                    time.sleep(30); continue
                break
            except Exception:
                if attempt < 2: time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers", params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000": return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 200) -> List[dict]:
        cache_key = f"{symbol}:{limit}"
        if cache_key in cls._candle_cache: return cls._candle_cache[cache_key]
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/candles",
            params={"symbol": symbol, "productType": "USDT-FUTURES", "granularity": "1H", "limit": limit}
        )
        if not data or data.get("code") != "00000": return []
        candles = []
        for row in data.get("data", []):
            try:
                vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                candles.append({
                    "ts": int(row[0]), "open": float(row[1]), "high": float(row[2]),
                    "low": float(row[3]), "close": float(row[4]), "volume_usd": vol_usd,
                })
            except Exception: continue
        candles.sort(key=lambda x: x["ts"])
        cls._candle_cache[cache_key] = candles
        return candles

    @classmethod
    def get_15m_vol_spike(cls, symbol: str) -> float:
        """[V7 UPGRADE] Lazy fetch 15m volume for acceleration validation."""
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/candles",
            params={"symbol": symbol, "productType": "USDT-FUTURES", "granularity": "15m", "limit": 8}
        )
        if not data or data.get("code") != "00000": return 0.0
        vols = []
        for row in data.get("data", []):
            try: vols.append(float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4]))
            except Exception: pass
        if len(vols) < 8: return 0.0
        cur_vol = vols[-1]
        med_vol = _median(vols[:-1])
        return cur_vol / med_vol if med_vol > 0 else 0.0

    @classmethod
    def get_funding(cls, symbol: str) -> float:
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/current-fund-rate", params={"symbol": symbol, "productType": "USDT-FUTURES"})
        try: return float(data["data"][0]["fundingRate"])
        except Exception: return 0.0

    @classmethod
    def clear_cache(cls) -> None:
        cls._candle_cache.clear()


class CoinalyzeClient:
    BASE = "https://api.coinalyze.net/v1"
    _last_call: float = 0.0

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._cache = {}

    def _wait(self) -> None:
        wait = CONFIG["clz_min_interval_sec"] - (time.time() - CoinalyzeClient._last_call)
        if wait > 0: time.sleep(wait)
        CoinalyzeClient._last_call = time.time()

    def _get(self, endpoint: str, params: dict) -> Optional[list]:
        params["api_key"] = self.api_key
        for attempt in range(CONFIG["clz_retry_attempts"]):
            self._wait()
            try:
                r = requests.get(f"{self.BASE}/{endpoint}", params=params, timeout=15)
                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", 10)) + 1); continue
                if r.status_code in (401, 400, 404): return None
                if r.status_code != 200: return None
                data = r.json()
                if isinstance(data, dict) and "error" in data: return None
                return data
            except Exception:
                if attempt < CONFIG["clz_retry_attempts"] - 1: time.sleep(CONFIG["clz_retry_wait_sec"])
        return None

    def get_future_markets(self) -> List[dict]:
        if "future_markets" in self._cache: return self._cache["future_markets"]
        data = self._get("future-markets", {})
        res = data if isinstance(data, list) else []
        self._cache["future_markets"] = res
        return res

    def _batch_fetch(self, endpoint: str, symbols: List[str], extra_params: dict) -> Dict[str, list]:
        bs = CONFIG["clz_batch_size"]
        res = {}
        for i in range(0, len(symbols), bs):
            batch = symbols[i:i + bs]
            data = self._get(endpoint, {"symbols": ",".join(batch), **extra_params})
            if data and isinstance(data, list):
                for item in data:
                    sym = item.get("symbol", "")
                    hist = item.get("history", [])
                    if sym and hist: res[sym] = hist
        return res

    def fetch_ohlcv_batch(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, list]:
        return self._batch_fetch("ohlcv-history", symbols, {"interval": CONFIG["coinalyze_interval"], "from": from_ts, "to": to_ts})

    def fetch_liquidations_batch(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, list]:
        return self._batch_fetch("liquidation-history", symbols, {"interval": CONFIG["coinalyze_interval"], "from": from_ts, "to": to_ts, "convert_to_usd": "true"})

    def fetch_oi_batch(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, list]:
        return self._batch_fetch("open-interest-history", symbols, {"interval": CONFIG["coinalyze_interval"], "from": from_ts, "to": to_ts, "convert_to_usd": "true"})


class SymbolMapper:
    def __init__(self, clz_client: CoinalyzeClient):
        self._client = clz_client
        self._to_clz, self._rev_map, self._has_btx = {}, {}, {}

    def load(self, active_symbols: set) -> None:
        markets = self._client.get_future_markets()
        if not markets:
            for sym in active_symbols: self._to_clz[sym] = f"{sym}_PERP.A"
        else:
            agg = {m.get("symbol", "").rsplit(".", 1)[0]: m for m in markets if m.get("symbol", "").endswith(".A")}
            for sym in active_symbols:
                a_sym = f"{sym}_PERP.A"; base = f"{sym}_PERP"
                self._to_clz[sym] = a_sym
                self._has_btx[a_sym] = agg[base].get("has_buy_sell_data", True) if base in agg else True
        self._rev_map = {v: k for k, v in self._to_clz.items()}

    def to_clz(self, bitget_sym: str) -> Optional[str]: return self._to_clz.get(bitget_sym)
    def clz_symbols_for(self, bitget_syms: List[str]) -> List[str]: return [self._to_clz[s] for s in bitget_syms if s in self._to_clz]


# ══════════════════════════════════════════════════════════════════════════════
#  📦  DATA CONTAINERS
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class CoinData:
    symbol:    str
    price:     float       
    vol_24h:   float
    chg_24h:   float
    funding:   float
    candles:   List[dict]  
    clz_ohlcv: List[dict]  
    clz_liq:   List[dict]  
    clz_oi:    List[dict]  

    @property
    def has_btx(self) -> bool:
        if len(self.clz_ohlcv) < 2: return False
        c = self.clz_ohlcv[-2]
        return bool(c.get("btx", 0)) and bool(c.get("tx", 0))

    @property
    def has_liq(self) -> bool: return bool(self.clz_liq)
    @property
    def has_oi(self) -> bool: return bool(self.clz_oi)

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
def score_buy_tx_ratio(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG; w = cfg["buy_tx_ratio_weight"]
    if not data.clz_ohlcv or len(data.clz_ohlcv) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_clz"}
    cur = data.clz_ohlcv[-2]
    btx = float(cur.get("btx", 0) or 0); tx = float(cur.get("tx", 0) or 0)
    if tx <= 0: return 0, 0.0, {"source": "no_tx"}
    
    ratio = btx / tx
    baseline = _build_baseline(data.clz_ohlcv)
    bl_ratios = [float(c.get("btx", 0) or 0) / max(float(c.get("tx", 0) or 1), 1) for c in baseline if c.get("tx", 0)]
    if len(bl_ratios) < cfg["baseline_min_samples"]: return 0, 0.0, {"source": "insufficient_bl"}
    
    z = robust_zscore(ratio, bl_ratios)
    score = score_from_z(z, cfg["buy_tx_ratio_z_strong"], cfg["buy_tx_ratio_z_medium"], w)
    return score, round(z, 2), {"btx_ratio": round(ratio, 3), "btx": int(btx), "z": round(z, 2)}

def score_avg_buy_size(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG; w = cfg["avg_buy_size_weight"]
    if not data.clz_ohlcv or len(data.clz_ohlcv) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_clz"}
    
    cur = data.clz_ohlcv[-2]
    btx = float(cur.get("btx", 0) or 0); bv = float(cur.get("bv", 0) or 0); v = float(cur.get("v", 0) or 0)
    
    if btx <= 0 or bv <= 0:
        return _score_bv_ratio_fallback(data, w, v, bv)
        
    avg_size = bv / btx
    baseline = _build_baseline(data.clz_ohlcv)
    bl_sizes = [float(c.get("bv", 0) or 0) / float(c.get("btx", 0) or 1) for c in baseline if float(c.get("btx", 0) or 0) > 0]
    if len(bl_sizes) < cfg["baseline_min_samples"]: return _score_bv_ratio_fallback(data, w, v, bv)
    
    z = robust_zscore(avg_size, bl_sizes)
    score = score_from_z(z, cfg["avg_buy_size_z_strong"], cfg["avg_buy_size_z_medium"], w)
    
    # [V7 UPGRADE] Mathematical correction: apply bonus linearly to score-space, capped at 20%
    bv_ratio = bv / v if v > 0 else 0.0
    if bv_ratio > cfg["bv_ratio_bonus_threshold"]:
        bonus = int(w * 0.2)
        score = min(w, score + bonus)
        
    return score, round(z, 2), {"avg_buy_usd": round(avg_size), "bv_ratio": round(bv_ratio, 3), "z": round(z, 2)}

def _score_bv_ratio_fallback(data: CoinData, w: int, v: float, bv: float) -> Tuple[int, float, dict]:
    if v <= 0: return 0, 0.0, {"source": "v0"}
    ratio = bv / v
    bl = _build_baseline(data.clz_ohlcv)
    bl_ratios = [float(c.get("bv", 0) or 0) / max(float(c.get("v", 0) or 1), 1) for c in bl if c.get("v", 0)]
    if len(bl_ratios) < CONFIG["baseline_min_samples"]: return 0, 0.0, {"source": "insufficient_bl"}
    
    z = robust_zscore(ratio, bl_ratios)
    score = score_from_z(z, CONFIG["avg_buy_size_z_strong"], CONFIG["avg_buy_size_z_medium"], w // 2)
    
    # [V7 UPGRADE] Bounded bonus execution
    if ratio > CONFIG["bv_ratio_bonus_threshold"]:
        bonus = int((w // 2) * 0.2)
        score = min(w // 2, score + bonus)
        
    return score, round(z, 2), {"bv_ratio": round(ratio, 3), "source": "bv_fallback", "z": round(z, 2)}

def score_volume(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG; w = cfg["volume_weight"]; cndls = data.candles
    if len(cndls) < cfg["baseline_min_samples"] + 3: return 0, 0.0, {"source": "no_candles"}
    cur = cndls[-2]["volume_usd"]
    bl = [c["volume_usd"] for c in _build_baseline(cndls)]
    z = robust_zscore(cur, bl)
    score = score_from_z(z, cfg["volume_z_strong"], cfg["volume_z_medium"], w)
    return score, round(z, 2), {"cur_vol": round(cur), "z": round(z, 2)}

def score_short_liquidations(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG; w = cfg["short_liq_weight"]
    if not data.has_liq or len(data.clz_liq) < cfg["baseline_min_samples"]: return 0, 0.0, {"source": "no_liq"}
    cur = float(data.clz_liq[-2].get("s", 0) or 0) if len(data.clz_liq) >= 2 else 0.0
    bl = [float(c.get("s", 0) or 0) for c in _build_baseline(data.clz_liq)]
    if (sum(1 for x in bl if x > 0) / max(len(bl), 1)) < 0.15: return 0, 0.0, {"source": "sparse_liq"}
    z = robust_zscore(cur, bl)
    score = score_from_z(z, cfg["short_liq_z_strong"], cfg["short_liq_z_medium"], w)
    return score, round(z, 2), {"short_liq_usd": round(cur), "z": round(z, 2)}

def score_oi_buildup(data: CoinData) -> Tuple[int, float, dict]:
    cfg = CONFIG; w = cfg["oi_buildup_weight"]; nw = cfg["oi_buildup_candles"]
    if not data.has_oi or len(data.clz_oi) < cfg["baseline_min_samples"] + nw: return 0, 0.0, {"source": "no_oi"}
    oi = data.clz_oi
    cur = float(oi[-2].get("c", 0) or 0); prv = float(oi[-(2+nw)].get("c", 0) or 0)
    if prv <= 0: return 0, 0.0, {"source": "prv_0"}
    chg = (cur - prv) / prv
    
    bl = _build_baseline(oi); bl_chgs = []
    for j in range(nw, len(bl)):
        oj = float(bl[j].get("c", 0) or 0); ob = float(bl[j-nw].get("c", 0) or 0)
        if ob > 0: bl_chgs.append((oj - ob) / ob)
        
    z = robust_zscore(chg, bl_chgs)
    score = score_from_z(z, cfg["oi_buildup_z_strong"], cfg["oi_buildup_z_medium"], w)
    return score, round(z, 2), {"oi_chg_pct": round(chg*100, 2), "z": round(z, 2)}


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_entry_targets(data: CoinData) -> Optional[dict]:
    candles = data.candles
    n_atr = CONFIG["atr_candles"]
    if len(candles) < n_atr + 2: return None

    price_ref = candles[-2]["close"]
    trs_pct = []
    for i in range(1, min(n_atr + 1, len(candles))):
        c = candles[-i]; pc = candles[-(i + 1)]["close"]
        if pc > 0: trs_pct.append(max((c["high"] - c["low"]) / pc, abs(c["high"] - pc) / pc, abs(c["low"] - pc) / pc))

    atr_pct = _mean(trs_pct) if trs_pct else 0.02
    entry = data.price

    # [V7 UPGRADE] Adaptive ATR Scaling 
    mult = CONFIG["atr_sl_mult"]
    if atr_pct > 0.04: mult = 1.2    # High volatility -> slightly tighter (relative) stop
    elif atr_pct < 0.015: mult = 2.0 # Compression -> looser stop to avoid hunting

    sl = entry * (1 - atr_pct * mult)
    t1 = max(entry * (1 + CONFIG["min_target_pct"] / 100), entry * (1 + atr_pct * 3))
    t2 = max(entry * 1.20, entry * (1 + atr_pct * 6))

    return {
        "entry": round(entry, 8), "sl": round(sl, 8), "sl_pct": round((entry - sl) / entry * 100, 1),
        "t1": round(t1, 8), "t2": round(t2, 8), "t1_pct": round((t1 - entry) / entry * 100, 1),
        "t2_pct": round((t2 - entry) / entry * 100, 1),
        "rr": round((t1 - entry) / (entry - sl), 2) if (entry - sl) > 0 else 0.0,
        "atr_pct": round(atr_pct * 100, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  MASTER SCORER
# ══════════════════════════════════════════════════════════════════════════════
def score_coin(data: CoinData) -> Optional[ScoreResult]:
    cfg = CONFIG

    if data.vol_24h < cfg["min_vol_24h"]: return None
    if data.chg_24h > cfg["gate_chg_24h_max"]: return None
    if data.price <= 0: return None

    a_sc, a_z, a_d = score_buy_tx_ratio(data)
    b_sc, b_z, b_d = score_avg_buy_size(data)
    c_sc, c_z, c_d = score_volume(data)
    d_sc, d_z, d_d = score_short_liquidations(data)
    e_sc, e_z, e_d = score_oi_buildup(data)

    # [V7 UPGRADE] Component D Vol-Gate: Short squeeze requires taker volume confirmation
    if cfg.get("short_liq_requires_vol_confirm", True):
        if d_sc >= 14 and c_sc < 4:
            d_sc = min(d_sc, 10)  # Demote false squeeze

    total = a_sc + b_sc + c_sc + d_sc + e_sc

    # [V7 UPGRADE] Recovery Mode (Downtrend Override)
    if -20.0 <= data.chg_24h < -10.0 and d_sc < 16:
        return None  # Drop falling knife unless massive squeeze detected

    act = sum([a_sc > cfg["active_thresh_a"], b_sc > cfg["active_thresh_b"], c_sc > cfg["active_thresh_c"], d_sc > cfg["active_thresh_d"], e_sc > cfg["active_thresh_e"]])
    if act < cfg["min_active_components"]: return None

    # [V7 UPGRADE] Contextual Regime Filtering
    regime = detect_market_regime(data.candles)
    dynamic_thresh = cfg["regime_thresholds"].get(regime, 65)

    if total < dynamic_thresh: return None

    conf = "very_strong" if total >= cfg["score_very_strong"] else "strong" if total >= cfg["score_strong"] else "watch"
    dq = {"has_btx": data.has_btx, "has_liq": data.has_liq, "has_oi": data.has_oi, "candles": len(data.candles)}
    set_cooldown(data.symbol)

    urg = "⚪ WATCH — Akumulasi awal"
    if d_sc >= 14 and (a_sc >= 12 or b_sc >= 12): urg = f"🔴 TINGGI — Short squeeze + Akumulasi"
    elif d_sc >= 14: urg = f"🔴 TINGGI — Short squeeze signal"
    elif a_z >= 2.0 and b_z >= 1.5: urg = "🟠 SEDANG — Buy count & size anomali"
    elif b_z >= 2.0: urg = "🟠 SEDANG — Smart money size anomali"
    
    urg = f"{urg} | [{regime}]"

    return ScoreResult(
        symbol=data.symbol, score=total, confidence=conf,
        components={"A": {"score": a_sc, "z": a_z, "details": a_d}, "B": {"score": b_sc, "z": b_z, "details": b_d},
                    "C": {"score": c_sc, "z": c_z, "details": c_d}, "D": {"score": d_sc, "z": d_z, "details": d_d},
                    "E": {"score": e_sc, "z": e_z, "details": e_d}},
        entry=calc_entry_targets(data), price=data.price, vol_24h=data.vol_24h, chg_24h=data.chg_24h, funding=data.funding,
        urgency=urg, data_quality=dq,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def build_alert(r: ScoreResult, rank: int) -> str:
    e = r.entry; vol = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    bar = "█" * min(20, r.score // 5) + "░" * max(0, 20 - r.score // 5)
    dq = " ".join([k.replace("has_", "")+"✓" for k, v in r.data_quality.items() if "has_" in k and v]) or "basic"
    c = r.components
    em = {"very_strong": "🟢", "strong": "🟡", "watch": "⚪"}.get(r.confidence, "⚪")

    entry_block = f"\n   📍 Entry: <b>${e['entry']:.6g}</b> | SL: ${e['sl']:.6g} (-{e['sl_pct']}%)\n   🎯 T1: +{e['t1_pct']}% | T2: +{e['t2_pct']}% | R/R: {e['rr']}" if e else ""
    return (
        f"#{rank} {em} <b>{r.symbol}</b>  Score: <b>{r.score}/100</b>  [{dq}]\n   {bar}\n   {r.urgency}\n"
        f"   Vol: {vol} | Δ24h: {r.chg_24h:+.1f}% | F: {r.funding:.5f}\n"
        f"   [A] BuyRatio: {c['A']['score']}pt ({c['A']['z']:+.1f}σ)  [B] AvgSize: {c['B']['score']}pt ({c['B']['z']:+.1f}σ)\n"
        f"   [C] Volume: {c['C']['score']}pt ({c['C']['z']:+.1f}σ)  [D] ShortLiq: {c['D']['score']}pt ({c['D']['z']:+.1f}σ)\n"
        f"   [E] OI: {c['E']['score']}pt ({c['E']['z']:+.1f}σ){entry_block}\n"
    )

def build_summary(results: List[ScoreResult]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = f"🔍 <b>PRE-PUMP SCANNER v{VERSION}</b> — {now}\n📊 {len(results)} sinyal (Microstructure Mode)\n\n"
    for i, r in enumerate(results, 1):
        t1 = f"+{r.entry['t1_pct']}%" if r.entry else "?"
        msg += f"{i}. <b>{r.symbol}</b> [{r.score}pt] A:{r.components['A']['score']} B:{r.components['B']['score']} C:{r.components['C']['score']} D:{r.components['D']['score']} → T1:{t1}\n"
    return msg

def send_telegram(msg: str) -> bool:
    if not CONFIG["bot_token"] or not CONFIG["chat_id"]: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{CONFIG['bot_token']}/sendMessage", json={"chat_id": CONFIG["chat_id"], "text": msg, "parse_mode": "HTML"}, timeout=10)
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan() -> None:
    start_ts = time.time()
    log.info("=" * 70); log.info(f"  PRE-PUMP SCANNER v{VERSION} — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"); log.info("=" * 70)

    clz_client = CoinalyzeClient(CONFIG["coinalyze_api_key"])
    mapper = SymbolMapper(clz_client)

    log.info("Fetching Bitget tickers...")
    tickers = BitgetClient.get_tickers()
    if not tickers: return log.error("Gagal fetch Bitget tickers")

    # [V7 UPGRADE] Process Outcomes & Dynamic Whitelist
    check_outcomes(tickers)
    active_whitelist = fetch_dynamic_whitelist(tickers)
    mapper.load(active_whitelist)

    candidates = []; skip_stats = defaultdict(int)
    for sym in active_whitelist:
        if sym in MANUAL_EXCLUDE: skip_stats["excluded"] += 1; continue
        if is_on_cooldown(sym): skip_stats["cooldown"] += 1; continue
        if sym not in tickers: skip_stats["not_found"] += 1; continue
        
        t = tickers[sym]
        try: vol = float(t.get("quoteVolume", 0)); chg = float(t.get("change24h", 0)) * 100
        except Exception: skip_stats["parse_error"] += 1; continue

        if vol < CONFIG["pre_filter_vol"]: skip_stats["vol_low"] += 1; continue
        if vol > CONFIG["max_vol_24h"]: skip_stats["vol_high"] += 1; continue
        if chg > CONFIG["gate_chg_24h_max"]: skip_stats["pumped"] += 1; continue
        # [V7 UPGRADE] Dump/Capitulation block (Recovery allowed up to -20% max)
        if chg < CONFIG["gate_chg_24h_min"]: skip_stats["dumped"] += 1; continue
        
        candidates.append((sym, t))

    log.info(f"Candidates: {len(candidates)} | Skip: {dict(skip_stats)}")

    now_ts = int(time.time()); from_ts = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    clz_syms = mapper.clz_symbols_for([s for s, _ in candidates])
    
    clz_ohlcv_all = clz_client.fetch_ohlcv_batch(clz_syms, from_ts, now_ts) if clz_syms else {}
    clz_liq_all   = clz_client.fetch_liquidations_batch(clz_syms, from_ts, now_ts) if clz_syms else {}
    clz_oi_all    = clz_client.fetch_oi_batch(clz_syms, from_ts, now_ts) if clz_syms else {}

    results: List[ScoreResult] = []
    BitgetClient.clear_cache()

    for i, (sym, ticker) in enumerate(candidates):
        try:
            price = float(ticker.get("lastPr", 0)); v24 = float(ticker.get("quoteVolume", 0)); c24 = float(ticker.get("change24h", 0)) * 100
            if price <= 0: continue
            
            csym = mapper.to_clz(sym)
            oc = clz_ohlcv_all.get(csym, []) if csym else []
            lc = clz_liq_all.get(csym, []) if csym else []
            ic = clz_oi_all.get(csym, []) if csym else []

            if len(oc) >= 60:
                cndls = [{"ts": int(b.get("t", 0))*1000, "open": float(b.get("o", 0)), "high": float(b.get("h", 0)), "low": float(b.get("l", 0)), "close": float(b.get("c", 0)), "volume_usd": float(b.get("v", 0) or 0)} for b in oc]
            else: cndls = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])

            if len(cndls) < 60: continue

            cdata = CoinData(symbol=sym, price=price, vol_24h=v24, chg_24h=c24, funding=BitgetClient.get_funding(sym), candles=cndls, clz_ohlcv=oc, clz_liq=lc, clz_oi=ic)
            res = score_coin(cdata)
            
            if res:
                # [V7 UPGRADE] Lazy-load 15M acceleration *only* for passing coins
                ratio_15m = BitgetClient.get_15m_vol_spike(sym)
                if ratio_15m > 2.5:
                    res.urgency += f" | ⚡ 15M VOL SPIKE ({ratio_15m:.1f}x)"
                    
                results.append(res)
                log.info(f"  ✅ {sym} Score={res.score} ({res.confidence})")
                
        except Exception as exc: pass
        time.sleep(CONFIG["sleep_between_coins"])

    results.sort(key=lambda x: x.score, reverse=True)
    top = results[:CONFIG["max_alerts"]]
    
    log.info(f"\nTotal sinyal: {len(results)} | Dikirim: {len(top)} | Waktu: {round(time.time() - start_ts, 1)}s")

    if not top: return log.info("Tidak ada sinyal pre-pump saat ini")

    send_telegram(build_summary(top)); time.sleep(2)
    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank))
        if ok: record_alert(r) # [V7 UPGRADE] Track successful alerts
        log.info(f"📤 Alert #{rank}: {r.symbol} score={r.score} sent={ok}")
        time.sleep(2)

if __name__ == "__main__":
    run_scan()
