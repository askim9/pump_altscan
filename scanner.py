"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v16 — REBUILD FROM SCRATCH                                 ║
║                                                                              ║
║  FILOSOFI BARU:                                                              ║
║  1. Deteksi AKUMULASI, bukan MOMENTUM                                        ║
║  2. Entry di SUPPORT/CONSOLIDATION, bukan chase price                        ║
║  3. Risk-first: SL harus jelas sebelum entry                                 ║
║  4. Filter agresif untuk menghindari late entry                              ║
║                                                                              ║
║  PERUBAHAN FUNDAMENTAL:                                                      ║
║  - Hapus semua indikator momentum (EMA gap, RSI tinggi, ATR tinggi)          ║
║  - Fokus: Volume Build-up + OI Build-up + Price Compression                  ║
║  - Entry: Di VWAP atau di bawah, NEVER chase                                ║
║  - SL: Berdasarkan struktur support, bukan ATR sembarangan                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests
import time
import os
import math
import json
import logging
import logging.handlers as _lh
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ── Logging ───────────────────────────────────────────────────────────────────
_log_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)

_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)

_fh = _lh.RotatingFileHandler("/tmp/scanner_v16.log", maxBytes=10*1024*1024, backupCount=3)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG — ULTRA CONSERVATIVE
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Alert Threshold ───────────────────────────────────────────────────────
    "min_score_alert": 12,           # Naik dari 10 — lebih selektif
    "max_alerts_per_run": 10,        # Turun dari 15 — kualitas > kuantitas
    
    # ── Volume Filters ────────────────────────────────────────────────────────
    "min_vol_24h": 50_000,           # Naik dari 10K — fokus liquid coins
    "max_vol_24h": 100_000_000,      # Naik dari 50M
    "pre_filter_vol": 25_000,        # Naik dari 5K
    
    # ── Open Interest ─────────────────────────────────────────────────────────
    "min_oi_usd": 500_000,           # Naik dari 100K — OI signifikan
    "oi_change_min_pct": 5.0,        # Naik dari 3% — OI harus naik signifikan
    
    # ── Price Change Gates ────────────────────────────────────────────────────
    # KRITIS: Hanya coin yang BELUM naik banyak
    "gate_chg_24h_max": 5.0,         # TURUN dari 12% — sudah naik 5% = too late
    "gate_chg_24h_max_1h": 2.0,      # BARU: tidak boleh naik >2% di 1h terakhir
    "gate_chg_24h_min": -10.0,       # Naik dari -15% — dump >10% = broken
    
    # ── VWAP Gate ─────────────────────────────────────────────────────────────
    # KRITIS: Entry harus di ATAS VWAP (bukan di bawah)
    # Coin di bawah VWAP = bearish, bukan accumulation
    "vwap_min_position": 0.995,      # Harga >= VWAP * 0.995 (0.5% di bawah max)
    "vwap_max_position": 1.02,       # BARU: harga <= VWAP * 1.02 (jangan terlalu jauh di atas)
    
    # ── Accumulation Detection ────────────────────────────────────────────────
    "accum_vol_ratio": 1.3,          # Volume 4h > 1.3x rata-rata
    "accum_price_range_max": 1.5,    # Range 12h < 1.5% (tight consolidation)
    "accum_candles_min": 36,         # Minimal 36 candle (1.5 hari) data
    
    # ── Compression Detection ─────────────────────────────────────────────────
    "compress_bbw_max": 0.04,        # BB Width < 4% (tight band)
    "compress_atr_max_pct": 0.8,     # ATR < 0.8% (low volatility)
    
    # ── Pre-Pump Momentum (Early Stage Only) ──────────────────────────────────
    "early_uptrend_max_hours": 3,    # Uptrend maksimal 3 jam = very early
    "early_bos_only": True,          # Hanya BOS yang BARU terjadi (< 3 candle)
    
    # ── Funding ───────────────────────────────────────────────────────────────
    "funding_max": 0.0005,           # Funding < 0.05% (tidak overbought)
    "funding_penalty_high": 0.001,   # Penalti jika funding > 0.1%
    
    # ── Risk Management ───────────────────────────────────────────────────────
    "max_risk_pct": 5.0,             # SL maksimal 5% dari entry
    "min_risk_pct": 1.0,             # SL minimal 1% (terlalu dekat = noise)
    "min_rr_ratio": 1.5,             # Minimal R:R 1:1.5
    
    # ── Candle Config ─────────────────────────────────────────────────────────
    "candle_1h": 168,
    "candle_4h": 48,
    
    # ── Timing ─────────────────────────────────────────────────────────────────
    "alert_cooldown_sec": 3600,      # Naik ke 1 jam — hindari spam
    "sleep_coins": 0.5,              # Lebih cepat
    "sleep_error": 2.0,
    "cooldown_file": "./cooldown_v16.json",
    "funding_snapshot_file": "./funding_v16.json",
    
    # ── Scoring Weights ───────────────────────────────────────────────────────
    "score_accumulation": 5,         # Volume up + price flat
    "score_compression": 4,          # BB squeeze + low ATR
    "score_oi_buildup": 4,           # OI naik signifikan
    "score_fresh_bos": 3,            # BOS baru (< 3 candle)
    "score_above_vwap": 2,           # Harga di atas VWAP
    "score_funding_favorable": 2,    # Funding negatif/netral
    "score_htf_accumulation": 3,     # 4H accumulation
    "score_early_uptrend": 2,        # Uptrend baru mulai
}

EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST", "WBTC", "STETH"]

# Whitelist: Top liquid pairs only
WHITELIST_SYMBOLS = {
    "DOGEUSDT", "ADAUSDT", "LINKUSDT", "AVAXUSDT", "SHIBUSDT", 
    "SUIUSDT", "UNIUSDT", "DOTUSDT", "TAOUSDT", "AAVEUSDT",
    "NEARUSDT", "ETCUSDT", "POLUSDT", "ATOMUSDT", "RENDERUSDT",
    "ARBUSDT", "OPUSDT", "INJUSDT", "FETUSDT", "GRTUSDT",
    "IMXUSDT", "DYDXUSDT", "SNXUSDT", "CRVUSDT", "RUNEUSDT",
    "PENDLEUSDT", "TIAUSDT", "STRKUSDT", "WLDUSDT", "SEIUSDT",
    "APTUSDT", "JUPUSDT", "PYTHUSDT", "ENSUSDT", "LDOUSDT",
}

BITGET_BASE = "https://api.bitget.com"
GRAN_MAP = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}

# ══════════════════════════════════════════════════════════════════════════════
#  🛡️  UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
class TTLCache:
    """Thread-safe cache dengan TTL"""
    def __init__(self, ttl: int = 90, max_size: int = 500):
        self.ttl = ttl
        self.max_size = max_size
        self._cache: Dict[str, Tuple[float, Any]] = {}
    
    def get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            ts, val = self._cache[key]
            if time.time() - ts < self.ttl:
                return val
            del self._cache[key]
        return None
    
    def set(self, key: str, value: Any) -> None:
        if len(self._cache) >= self.max_size:
            oldest = min(self._cache.keys(), key=lambda k: self._cache[k][0])
            del self._cache[oldest]
        self._cache[key] = (time.time(), value)

_cache = TTLCache()
_cooldown: Dict[str, float] = {}
_funding_snapshots: Dict[str, List[Dict]] = {}
_oi_snapshots: Dict[str, Dict] = {}

def safe_div(a: float, b: float, default: float = 0.0) -> float:
    """Safe division dengan protection lengkap"""
    try:
        if b is None or b == 0 or not isinstance(b, (int, float)):
            return default
        result = a / b
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ZeroDivisionError, TypeError, ValueError):
        return default

def safe_float(v: Any, default: float = 0.0) -> float:
    """Safe float conversion"""
    if v is None:
        return default
    try:
        result = float(v)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ValueError, TypeError):
        return default

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  API CLIENT
# ══════════════════════════════════════════════════════════════════════════════
def api_get(endpoint: str, params: Optional[Dict] = None, retries: int = 2) -> Optional[Dict]:
    """API client dengan exponential backoff"""
    url = f"{BITGET_BASE}{endpoint}"
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get("code") == "00000":
                return data
            log.warning(f"API error: {data.get('code')} — {data.get('msg', 'unknown')}")
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                sleep_time = min(5 * (2 ** attempt), 30)
                log.warning(f"Rate limit, sleep {sleep_time}s")
                time.sleep(sleep_time)
                continue
            log.warning(f"HTTP error: {e}")
        except Exception as e:
            log.warning(f"Request error: {e}")
        if attempt < retries - 1:
            time.sleep(2)
    return None

def get_all_tickers() -> Dict[str, Dict]:
    data = api_get("/api/v2/mix/market/tickers", {"productType": "usdt-futures"})
    if data:
        return {t["symbol"]: t for t in data.get("data", []) if "symbol" in t}
    return {}

def get_candles(symbol: str, gran: str, limit: int) -> List[Dict]:
    """Fetch candles dengan caching"""
    key = f"c_{symbol}_{gran}_{limit}"
    cached = _cache.get(key)
    if cached:
        return cached
    
    data = api_get("/api/v2/mix/market/candles", {
        "symbol": symbol,
        "granularity": GRAN_MAP.get(gran, "1H"),
        "limit": str(limit),
        "productType": "usdt-futures"
    })
    
    if not data:
        return []
    
    candles = []
    for c in data.get("data", []):
        if len(c) < 6:
            continue
        try:
            close = safe_float(c[4])
            vol = safe_float(c[5])
            vol_usd = safe_float(c[6]) if len(c) > 6 else vol * close
            
            candles.append({
                "ts": int(c[0]),
                "open": safe_float(c[1]),
                "high": safe_float(c[2]),
                "low": safe_float(c[3]),
                "close": close,
                "volume": vol,
                "volume_usd": vol_usd,
            })
        except Exception:
            continue
    
    candles.sort(key=lambda x: x["ts"])
    _cache.set(key, candles)
    return candles

def get_funding(symbol: str) -> float:
    """Get current funding rate"""
    data = api_get("/api/v2/mix/market/current-fund-rate", {
        "symbol": symbol, "productType": "usdt-futures"
    })
    if data:
        d_list = data.get("data", [])
        if d_list:
            return safe_float(d_list[0].get("fundingRate"), 0.0)
    return 0.0

def get_open_interest(symbol: str) -> float:
    """Get OI dalam USD"""
    data = api_get("/api/v2/mix/market/open-interest", {
        "symbol": symbol, "productType": "usdt-futures"
    })
    if data:
        d = data.get("data", [])
        if d and isinstance(d, list):
            d = d[0]
        if isinstance(d, dict):
            oi_list = d.get("openInterestList", [])
            if oi_list and isinstance(oi_list, list):
                oi = safe_float(oi_list[0].get("openInterest"), 0.0)
            else:
                oi = safe_float(d.get("openInterest", d.get("holdingAmount")), 0.0)
            price = safe_float(d.get("indexPrice", d.get("lastPr")), 0.0)
            if oi > 0 and price > 0:
                return oi * price
    return 0.0

# ══════════════════════════════════════════════════════════════════════════════
#  📊  CORE INDICATORS
# ══════════════════════════════════════════════════════════════════════════════
def calc_vwap(candles: List[Dict], lookback: int = 24) -> float:
    """Volume Weighted Average Price"""
    if not candles:
        return 0.0
    n = min(lookback, len(candles))
    recent = candles[-n:]
    
    cum_pv = sum(((c["high"] + c["low"] + c["close"]) / 3) * c["volume"] for c in recent)
    cum_v = sum(c["volume"] for c in recent)
    
    return safe_div(cum_pv, cum_v, candles[-1]["close"])

def calc_atr(candles: List[Dict], period: int = 14) -> float:
    """Average True Range (absolute)"""
    if len(candles) < period + 1:
        return candles[-1]["close"] * 0.01 if candles else 0.0
    
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1:
            break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    
    return sum(trs) / len(trs) if trs else 0.0

def calc_bb_width(candles: List[Dict], period: int = 20) -> float:
    """Bollinger Band Width sebagai % dari harga"""
    if len(candles) < period:
        return 1.0
    
    closes = [c["close"] for c in candles[-period:]]
    mean = sum(closes) / period
    variance = sum((x - mean) ** 2 for x in closes) / period
    std = math.sqrt(variance)
    
    return safe_div(2 * std * 2, mean, 0.0)  # (Upper - Lower) / Mean

def detect_accumulation(candles: List[Dict]) -> Dict:
    """
    Deteksi fase akumulasi yang VALID:
    - Volume naik (ada interest)
    - Harga flat (sedang dikumpulkan)
    - Bukan pump yang sudah terjadi
    """
    if len(candles) < 36:
        return {"is_accumulating": False, "score": 0, "details": "Insufficient data"}
    
    # Volume 4h vs 24h average
    vol_4h = sum(c["volume_usd"] for c in candles[-4:]) / 4
    vol_24h = sum(c["volume_usd"] for c in candles[-28:-4]) / 24 if len(candles) >= 28 else vol_4h
    vol_ratio = safe_div(vol_4h, vol_24h, 1.0)
    
    # Price range 12h — HARUS flat untuk accumulation
    recent_12h = candles[-12:]
    high_12h = max(c["high"] for c in recent_12h)
    low_12h = min(c["low"] for c in recent_12h)
    mid_12h = (high_12h + low_12h) / 2
    range_pct = safe_div(high_12h - low_12h, mid_12h, 0.0) * 100
    
    # ATR untuk volatility check
    atr = calc_atr(candles[-24:])
    atr_pct = safe_div(atr, candles[-1]["close"], 0.0) * 100
    
    # Kondisi accumulation: volume UP, price FLAT, volatility LOW
    vol_up = vol_ratio >= CONFIG["accum_vol_ratio"]
    price_flat = range_pct <= CONFIG["accum_price_range_max"]
    low_volatility = atr_pct <= CONFIG["compress_atr_max_pct"]
    
    is_accumulating = vol_up and price_flat and low_volatility
    
    score = 0
    if vol_up:
        score += 2
    if price_flat:
        score += 2
    if low_volatility:
        score += 1
    
    return {
        "is_accumulating": is_accumulating,
        "score": score,
        "vol_ratio": round(vol_ratio, 2),
        "range_pct": round(range_pct, 2),
        "atr_pct": round(atr_pct, 2),
        "details": f"Vol:{vol_ratio:.1f}x | Range:{range_pct:.1f}% | ATR:{atr_pct:.2f}%"
    }

def detect_compression(candles: List[Dict]) -> Dict:
    """Deteksi volatility compression (sebelum expansion)"""
    if len(candles) < 20:
        return {"is_compressed": False, "bbw": 0.0}
    
    bbw = calc_bb_width(candles)
    is_compressed = bbw <= CONFIG["compress_bbw_max"]
    
    return {
        "is_compressed": is_compressed,
        "bbw": round(bbw * 100, 2),  # dalam persen
    }

def detect_fresh_bos(candles: List[Dict]) -> Dict:
    """
    Deteksi Break of Structure yang BARU (max 3 candle).
    BOS lama = sudah terlambat, harga sudah naik banyak.
    """
    if len(candles) < 6:
        return {"is_fresh_bos": False, "candles_ago": 99}
    
    # Cari high tertinggi di 10 candle sebelum candle terakhir
    lookback = 10
    if len(candles) < lookback + 3:
        return {"is_fresh_bos": False, "candles_ago": 99}
    
    reference_highs = [c["high"] for c in candles[-(lookback+3):-3]]
    if not reference_highs:
        return {"is_fresh_bos": False, "candles_ago": 99}
    
    resistance = max(reference_highs)
    
    # Cek kapan break terjadi
    for i in range(1, min(4, len(candles))):  # Cek 3 candle terakhir
        if candles[-i]["close"] > resistance:
            return {
                "is_fresh_bos": True,
                "candles_ago": i,
                "resistance": resistance,
                "break_close": candles[-i]["close"]
            }
    
    return {"is_fresh_bos": False, "candles_ago": 99, "resistance": resistance}

def find_support_levels(candles: List[Dict], n_levels: int = 3) -> List[Dict]:
    """Cari level support dari recent lows"""
    if len(candles) < 20:
        return []
    
    # Ambil 20 candle terakhir, cari lows yang signifikan
    recent = candles[-20:]
    lows = [(i, c["low"]) for i, c in enumerate(recent)]
    lows_sorted = sorted(lows, key=lambda x: x[1])
    
    # Ambil 3 lowest yang tidak terlalu dekat
    supports = []
    for idx, low_val in lows_sorted[:n_levels]:
        # Cek apakah sudah ada support yang mirip
        too_close = any(abs(safe_div(low_val - s["level"], s["level"], 1), 0) < 0.005 for s in supports)
        if not too_close:
            supports.append({
                "level": low_val,
                "candle_ago": 20 - idx,
                "strength": 1
            })
    
    return sorted(supports, key=lambda x: -x["level"])  # Highest support first

def calculate_entry_and_sl(candles: List[Dict], vwap: float, price: float, 
                           supports: List[Dict], accumulation: Dict) -> Optional[Dict]:
    """
    Kalkulasi entry dan SL yang MASUK AKAL.
    
    Rules:
    1. Entry harus di ATAS support terdekat
    2. Entry tidak boleh terlalu jauh di atas VWAP (chase)
    3. SL di support yang valid (bukan arbitrary ATR)
    4. Risk maksimal 5%
    """
    if not supports:
        return None
    
    # Entry candidate 1: VWAP (pullback entry)
    # Entry candidate 2: Support terdekat (deep entry)
    # Entry candidate 3: Current price (jika sudah di atas VWAP tapi dekat)
    
    nearest_support = supports[0]["level"]
    
    # Jangan entry jika harga sudah terlalu jauh dari VWAP
    vwap_distance = safe_div(price - vwap, vwap, 0) * 100
    if vwap_distance > 2.0:  # Sudah >2% di atas VWAP = chasing
        return None
    
    # Entry logic
    if price > vwap:
        # Harga di atas VWAP — entry di VWAP (pullback)
        entry = vwap
        entry_type = "vwap_pullback"
    elif price > nearest_support * 1.005:
        # Harga di antara VWAP dan support — entry sekarang
        entry = price
        entry_type = "current"
    else:
        # Harga terlalu dekat support — risk terlalu tinggi
        return None
    
    # SL di support terdekat dengan buffer
    sl_buffer = 0.003  # 0.3% buffer di bawah support
    sl = nearest_support * (1 - sl_buffer)
    
    # Validasi risk
    risk_pct = safe_div(entry - sl, entry, 0) * 100
    if risk_pct > CONFIG["max_risk_pct"]:
        # Risk terlalu besar, cari support yang lebih dekat
        if len(supports) > 1:
            sl = supports[1]["level"] * (1 - sl_buffer)
            risk_pct = safe_div(entry - sl, entry, 0) * 100
        if risk_pct > CONFIG["max_risk_pct"]:
            return None
    
    if risk_pct < CONFIG["min_risk_pct"]:
        return None  # Terlalu dekat, noise
    
    # Target berdasarkan risk:reward
    rr = 2.0  # Target 1:2
    target_range = (entry - sl) * rr
    tp1 = entry + target_range
    tp2 = entry + target_range * 1.5
    
    return {
        "entry": round(entry, 8),
        "sl": round(sl, 8),
        "sl_pct": round(risk_pct, 2),
        "tp1": round(tp1, 8),
        "tp2": round(tp2, 8),
        "rr": rr,
        "entry_type": entry_type,
        "support_level": nearest_support,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  SCANNER ENGINE
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ScanResult:
    symbol: str
    score: int
    price: float
    chg_24h: float
    entry_data: Dict
    accumulation: Dict
    compression: Dict
    bos: Dict
    vwap_distance: float
    oi_change: float
    funding: float
    signals: List[str]

def scan_symbol(symbol: str, ticker: Dict) -> Optional[ScanResult]:
    """Main scan logic dengan filter agresif"""
    
    # ── Basic Data ────────────────────────────────────────────────────────────
    try:
        vol_24h = safe_float(ticker.get("quoteVolume"), 0)
        chg_24h = safe_float(ticker.get("change24h"), 0) * 100
        price = safe_float(ticker.get("lastPr"), 0)
    except Exception:
        return None
    
    if vol_24h < CONFIG["min_vol_24h"] or price <= 0:
        return None
    
    # ── Fetch Candles ─────────────────────────────────────────────────────────
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    if len(c1h) < 48:
        return None
    
    # ── Gate: Sudah naik terlalu banyak ────────────────────────────────────────
    if chg_24h > CONFIG["gate_chg_24h_max"]:
        log.debug(f"{symbol}: Already pumped {chg_24h:.1f}% — skipping")
        return None
    
    if chg_24h < CONFIG["gate_chg_24h_min"]:
        log.debug(f"{symbol}: Dumped {chg_24h:.1f}% — broken")
        return None
    
    # ── Gate: Naik terlalu banyak di 1h terakhir ───────────────────────────────
    if len(c1h) >= 2:
        chg_1h = safe_div(c1h[-1]["close"] - c1h[-2]["close"], c1h[-2]["close"], 0) * 100
        if chg_1h > CONFIG["gate_chg_24h_max_1h"]:
            log.debug(f"{symbol}: Pumping {chg_1h:.1f}% in last hour — too late")
            return None
    
    # ── VWAP Analysis ─────────────────────────────────────────────────────────
    vwap = calc_vwap(c1h)
    vwap_position = safe_div(price, vwap, 1.0)
    vwap_distance = (vwap_position - 1) * 100  # % di atas/bawah VWAP
    
    # Gate: Harus di atas VWAP (bullish) tapi tidak terlalu jauh (chase)
    if vwap_position < CONFIG["vwap_min_position"]:
        log.debug(f"{symbol}: Below VWAP {vwap_distance:.1f}% — bearish")
        return None
    
    if vwap_position > CONFIG["vwap_max_position"]:
        log.debug(f"{symbol}: Too far above VWAP {vwap_distance:.1f}% — chasing")
        return None
    
    # ── Accumulation Detection ────────────────────────────────────────────────
    accumulation = detect_accumulation(c1h)
    if not accumulation["is_accumulating"]:
        log.debug(f"{symbol}: No accumulation pattern")
        return None
    
    # ── Compression Detection ─────────────────────────────────────────────────
    compression = detect_compression(c1h)
    
    # ── Fresh BOS Detection ───────────────────────────────────────────────────
    bos = detect_fresh_bos(c1h)
    
    # ── OI Analysis ───────────────────────────────────────────────────────────
    oi_now = get_open_interest(symbol)
    if oi_now < CONFIG["min_oi_usd"]:
        log.debug(f"{symbol}: OI too low ${oi_now:,.0f}")
        return None
    
    oi_prev = _oi_snapshots.get(symbol, {}).get("oi", oi_now)
    oi_change = safe_div(oi_now - oi_prev, oi_prev, 0) * 100
    _oi_snapshots[symbol] = {"ts": time.time(), "oi": oi_now}
    
    oi_building = oi_change >= CONFIG["oi_change_min_pct"]
    
    # ── Funding Check ──────────────────────────────────────────────────────────
    funding = get_funding(symbol)
    funding_penalty = funding > CONFIG["funding_penalty_high"]
    
    if funding_penalty:
        log.debug(f"{symbol}: Funding too high {funding:.5f}")
        return None
    
    funding_favorable = funding <= 0
    
    # ── Support Levels ────────────────────────────────────────────────────────
    supports = find_support_levels(c1h)
    if not supports:
        log.debug(f"{symbol}: No clear support levels")
        return None
    
    # ── Entry & SL Calculation ────────────────────────────────────────────────
    entry_data = calculate_entry_and_sl(c1h, vwap, price, supports, accumulation)
    if not entry_data:
        log.debug(f"{symbol}: Cannot calculate valid entry/SL")
        return None
    
    # ── Scoring ───────────────────────────────────────────────────────────────
    score = 0
    signals = []
    
    # Core: Accumulation (wajib)
    score += accumulation["score"] * 2  # Weighted 2x
    signals.append(f"📦 Accumulation: {accumulation['details']}")
    
    # Compression
    if compression["is_compressed"]:
        score += CONFIG["score_compression"]
        signals.append(f"🗜️ Compression: BBW {compression['bbw']}%")
    
    # OI Buildup
    if oi_building:
        score += CONFIG["score_oi_buildup"]
        signals.append(f"📈 OI Building: +{oi_change:.1f}%")
    
    # Fresh BOS
    if bos["is_fresh_bos"] and bos["candles_ago"] <= 2:
        score += CONFIG["score_fresh_bos"]
        signals.append(f"🚀 Fresh BOS: {bos['candles_ago']} candle ago")
    
    # VWAP Position
    if 0 <= vwap_distance <= 1.0:
        score += CONFIG["score_above_vwap"]
        signals.append(f"✅ Above VWAP: {vwap_distance:.1f}%")
    
    # Funding
    if funding_favorable:
        score += CONFIG["score_funding_favorable"]
        signals.append(f"💚 Favorable funding: {funding:.5f}")
    
    # Check minimum score
    if score < CONFIG["min_score_alert"]:
        log.debug(f"{symbol}: Score {score} < {CONFIG['min_score_alert']}")
        return None
    
    return ScanResult(
        symbol=symbol,
        score=score,
        price=price,
        chg_24h=chg_24h,
        entry_data=entry_data,
        accumulation=accumulation,
        compression=compression,
        bos=bos,
        vwap_distance=vwap_distance,
        oi_change=oi_change,
        funding=funding,
        signals=signals
    )

# ══════════════════════════════════════════════════════════════════════════════
#  📱  OUTPUT FORMATTING
# ══════════════════════════════════════════════════════════════════════════════
def format_alert(result: ScanResult, rank: int) -> str:
    """Format alert untuk Telegram"""
    e = result.entry_data
    
    lines = [
        f"🔥 <b>{result.symbol}</b> — PRE-PUMP SETUP #{rank}",
        f"<b>Score:</b> {result.score}/20",
        f"<b>Time:</b> {utc_now()}",
        "",
        f"📊 <b>Price:</b> ${result.price:.6f} ({result.chg_24h:+.1f}% 24h)",
        f"📊 <b>VWAP:</b> {result.vwap_distance:+.1f}% vs price",
        "",
        f"🎯 <b>Entry:</b> <code>${e['entry']:.6f}</code> [{e['entry_type']}]",
        f"🛑 <b>SL:</b> <code>${e['sl']:.6f}</code> ({e['sl_pct']:.1f}%)",
        f"🎯 <b>TP1:</b> <code>${e['tp1']:.6f}</code> (1:{e['rr']:.1f})",
        f"🎯 <b>TP2:</b> <code>${e['tp2']:.6f}</code>",
        "",
        "<b>Signals:</b>",
    ]
    
    for sig in result.signals[:5]:
        lines.append(f"  • {sig}")
    
    lines.append("")
    lines.append(f"📈 OI Change: {result.oi_change:+.1f}% | Funding: {result.funding:.5f}")
    lines.append(f"<i>v16 REBUILD | Not financial advice</i>")
    
    return "\n".join(lines)

def format_summary(results: List[ScanResult]) -> str:
    """Format summary"""
    lines = [
        f"📋 <b>PRE-PUMP CANDIDATES — {utc_now()}</b>",
        "━" * 30,
    ]
    
    for i, r in enumerate(results[:10], 1):
        lines.append(f"{i}. <b>{r.symbol}</b> — Score:{r.score} | Entry:${r.entry_data['entry']:.4f}")
        lines.append(f"   SL:{r.entry_data['sl_pct']:.1f}% | RR:1:{r.entry_data['rr']:.1f}")
    
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def load_cooldown():
    global _cooldown
    try:
        path = CONFIG["cooldown_file"]
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
            now = time.time()
            _cooldown = {k: v for k, v in data.items() if now - v < CONFIG["alert_cooldown_sec"]}
    except Exception:
        _cooldown = {}

def save_cooldown():
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(_cooldown, f)
    except Exception:
        pass

def is_on_cooldown(symbol: str) -> bool:
    return (time.time() - _cooldown.get(symbol, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(symbol: str):
    _cooldown[symbol] = time.time()
    save_cooldown()

def send_telegram(msg: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        return False
    
    if len(msg) > 4000:
        msg = msg[:3900] + "\n\n<i>...</i>"
    
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        return r.status_code == 200
    except Exception:
        return False

def run_scan():
    log.info("=" * 60)
    log.info("PRE-PUMP SCANNER v16 — REBUILD")
    log.info("=" * 60)
    
    load_cooldown()
    
    tickers = get_all_tickers()
    if not tickers:
        log.error("Failed to fetch tickers")
        send_telegram("⚠️ Scanner error: Cannot fetch market data")
        return
    
    log.info(f"Fetched {len(tickers)} tickers")
    
    results = []
    scanned = 0
    
    for symbol in WHITELIST_SYMBOLS:
        if symbol not in tickers:
            continue
        if any(kw in symbol for kw in EXCLUDED_KEYWORDS):
            continue
        if is_on_cooldown(symbol):
            continue
        
        scanned += 1
        log.info(f"[{scanned}] Scanning {symbol}...")
        
        try:
            result = scan_symbol(symbol, tickers[symbol])
            if result:
                log.info(f"  ✅ {symbol}: Score {result.score}, Entry ${result.entry_data['entry']:.4f}")
                results.append(result)
            else:
                log.debug(f"  ❌ {symbol}: Filtered")
        except Exception as e:
            log.warning(f"  ⚠️ {symbol}: Error — {e}")
        
        time.sleep(CONFIG["sleep_coins"])
    
    log.info(f"\nScan complete: {len(results)} candidates from {scanned} scanned")
    
    if not results:
        log.info("No valid setups found")
        return
    
    # Sort by score
    results.sort(key=lambda x: x.score, reverse=True)
    top = results[:CONFIG["max_alerts_per_run"]]
    
    # Send summary
    if len(top) >= 2:
        send_telegram(format_summary(top))
        time.sleep(1)
    
    # Send individual alerts
    for i, result in enumerate(top, 1):
        if send_telegram(format_alert(result, i)):
            set_cooldown(result.symbol)
            log.info(f"Alert sent: {result.symbol}")
        time.sleep(1)
    
    log.info(f"Done — {len(top)} alerts sent")

if __name__ == "__main__":
    if not BOT_TOKEN or not CHAT_ID:
        log.error("BOT_TOKEN or CHAT_ID not set!")
        exit(1)
    
    run_scan()
