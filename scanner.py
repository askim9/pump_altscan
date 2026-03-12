"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              ALTCOIN PUMP SCANNER v34 — Bitget Futures                     ║
║              Detects coins BEFORE 15–50% pump moves                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

SCORING MODEL (4 structural phases):
  Phase 1 — COMPRESSION    : 20%  (volatility compression, ATR contraction)
  Phase 2 — ACCUMULATION   : 30%  (volume accumulation, funding, OI stability)
  Phase 3 — POSITION BUILD : 25%  (OI expansion + price stability)
  Phase 4 — IGNITION       : 25%  (breakout pressure, liquidity vacuum, energy)

  TOTAL = 100 points → Pump Probability mapped 0–100%
"""

import requests
import time
import os
import math
import json
import logging
import logging.handlers as _lh
import html as _html_mod
from datetime import datetime, timezone

# ─── Optional: load .env file if python-dotenv is installed ───────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # Telegram
    "bot_token":            os.getenv("BOT_TOKEN", ""),
    "chat_id":              os.getenv("CHAT_ID", ""),

    # Scan timing
    "scan_interval_sec":    300,          # Run full scan every 5 minutes
    "sleep_between_coins":  0.15,         # Pause between coin API calls (rate limit)
    "sleep_error":          3,            # Pause after API error

    # Alert filtering
    "min_score":            55,           # Minimum pump score to alert
    "top_n_alerts":         3,            # Max alerts per scan cycle
    "alert_cooldown_sec":   3600 * 3,     # 3h cooldown per coin

    # Filter thresholds
    "max_price_change_12h": 10.0,         # Reject if price pumped >10% in 12h
    "max_rsi":              80,           # Reject if RSI > 80 (already overbought)
    "min_volume_usd_24h":   500_000,      # Minimum 24h volume in USD

    # Persistence files
    "cooldown_file":        "./cooldown_v34.json",
    "oi_snapshot_file":     "./oi_snapshot_v34.json",
    "oi_history_file":      "./oi_history_v34.json",
    "funding_snapshot_file":"./funding_v34.json",
}

# ══════════════════════════════════════════════════════════════════════════════
#  📋  SYMBOL WHITELIST (Bitget Futures — active with sufficient OI/volume)
# ══════════════════════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
    # ── Tier 1: Large Cap ────────────────────────────────────────────────────
    "DOGEUSDT", "ADAUSDT", "XMRUSDT", "LINKUSDT", "XLMUSDT", "HBARUSDT",
    "LTCUSDT",  "AVAXUSDT", "SHIBUSDT", "SUIUSDT", "TONUSDT",
    "UNIUSDT",  "DOTUSDT",  "TAOUSDT",  "AAVEUSDT", "PEPEUSDT",
    "ETCUSDT",  "NEARUSDT", "ONDOUSDT", "POLUSDT",  "ICPUSDT", "ATOMUSDT",
    "ENAUSDT",  "KASUSDT",  "ALGOUSDT", "RENDERUSDT","FILUSDT", "APTUSDT",
    "ARBUSDT",  "JUPUSDT",  "SEIUSDT",  "STXUSDT",  "DYDXUSDT","VIRTUALUSDT",

    # ── Tier 2: Mid Cap ──────────────────────────────────────────────────────
    "FETUSDT",  "INJUSDT",  "PYTHUSDT", "GRTUSDT",  "TIAUSDT", "LDOUSDT",
    "OPUSDT",   "ENSUSDT",  "AXSUSDT",  "PENDLEUSDT","WIFUSDT", "SANDUSDT",
    "MANAUSDT", "COMPUSDT", "GALAUSDT", "RAYUSDT",   "RUNEUSDT","EGLDUSDT",
    "SNXUSDT",  "ARUSDT",   "CRVUSDT",  "IMXUSDT",  "EIGENUSDT","JTOUSDT",
    "CELOUSDT", "MASKUSDT", "APEUSDT",  "MOVEUSDT",  "MINAUSDT","SONICUSDT",
    "KAIAUSDT", "HYPEUSDT", "WLDUSDT",  "STRKUSDT",  "CFXUSDT", "BOMEUSDT",

    # ── Tier 3: Active / High Volatility ─────────────────────────────────────
    "FLOKIUSDT","CAKEUSDT","CHZUSDT","HNTUSDT","ROSEUSDT","IOTXUSDT",
    "ANKRUSDT", "ZILUSDT", "ONTUSDT","ENJUSDT","GMTUSDT", "NOTUSDT",
    "PEOPLEUSDT","METISUSDT","AIXBTUSDT","GOATUSDT","PNUTUSDT",
    "GRASSUSDT","POPCATUSDT","ORDIUSDT","MOODENGUSDT","BIOUSDT",
    "MAGICUSDT","REZUSDT","ARPAUSDT","ACTUSDT","USUALUSDT",
    "SLPUSDT","XAIUSDT","BLURUSDT","ARKMUSDT","API3USDT","AGLDUSDT",
    "TNSRUSDT","LAYERUSDT","ANIMEUSDT","YGGUSDT","THEUSDT",
}

# ══════════════════════════════════════════════════════════════════════════════
#  📝  LOGGING
# ══════════════════════════════════════════════════════════════════════════════
_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)

_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)

_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v34.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP SESSION
# ══════════════════════════════════════════════════════════════════════════════
BITGET_BASE  = "https://api.bitget.com"
GRAN_MAP     = {"5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
_http_session = requests.Session()
_http_session.headers.update({
    "User-Agent": "PumpScanner/34.0",
    "Accept-Encoding": "gzip",
})
_cache = {}  # in-memory candle/ticker cache

def safe_get(url, params=None, timeout=10):
    """HTTP GET with retry and 429 rate-limit handling."""
    for attempt in range(2):
        try:
            r = _http_session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                log.warning("Rate limit 429 — sleeping 15s")
                time.sleep(15)
                continue
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN — prevents re-alerting same coin within cooldown window
# ══════════════════════════════════════════════════════════════════════════════
def _load_cooldown():
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items()
                    if now - v < CONFIG["alert_cooldown_sec"]}
    except Exception:
        pass
    return {}

def _save_cooldown(state):
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass

_cooldown = _load_cooldown()

def is_cooldown(sym):
    return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym):
    _cooldown[sym] = time.time()
    _save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════════════════════
#  💾  OI SNAPSHOTS — persisted to disk to survive restarts
# ══════════════════════════════════════════════════════════════════════════════
_oi_snapshot = {}   # {symbol: {"ts": float, "oi": float}}
_oi_history  = {}   # {symbol: [{"ts": float, "oi": float}, ...]} rolling 40 entries

def _load_oi_snapshots():
    global _oi_snapshot
    try:
        p = CONFIG["oi_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            # Discard entries older than 2 hours
            _oi_snapshot = {s: v for s, v in data.items()
                            if now - v.get("ts", 0) < 7200}
            log.info(f"OI snapshots loaded: {len(_oi_snapshot)} coins")
    except Exception:
        _oi_snapshot = {}

def _save_oi_snapshots():
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
            json.dump(_oi_snapshot, f)
    except Exception:
        pass

def _load_oi_history():
    global _oi_history
    try:
        p = CONFIG["oi_history_file"]
        if os.path.exists(p):
            with open(p) as f:
                raw = json.load(f)
            now = time.time()
            loaded = {}
            for sym, entries in raw.items():
                fresh = [e for e in entries if now - e.get("ts", 0) < 1200]
                if fresh:
                    loaded[sym] = fresh[-40:]
            _oi_history = loaded
            log.info(f"OI history loaded: {len(_oi_history)} symbols")
    except Exception:
        _oi_history = {}

def _save_oi_history():
    try:
        with open(CONFIG["oi_history_file"], "w") as f:
            json.dump(_oi_history, f)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
#  💾  FUNDING SNAPSHOTS — track funding rate history per coin
# ══════════════════════════════════════════════════════════════════════════════
_funding_snapshots = {}  # {symbol: [{"ts": float, "funding": float}, ...]}

def _load_funding_snapshots():
    global _funding_snapshots
    try:
        p = CONFIG["funding_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                _funding_snapshots = json.load(f)
    except Exception:
        _funding_snapshots = {}

def _save_funding_snapshots():
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(_funding_snapshots, f)
    except Exception:
        pass

def _add_funding_snapshot(symbol, rate):
    if symbol not in _funding_snapshots:
        _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({"ts": time.time(), "funding": rate})
    # Keep last 48 snapshots (~24h at 30-min funding intervals)
    if len(_funding_snapshots[symbol]) > 48:
        _funding_snapshots[symbol] = _funding_snapshots[symbol][-48:]

# ══════════════════════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

def get_all_tickers():
    """Fetch all Bitget USDT-Futures tickers in a single call."""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/tickers",
        params={"productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        return {t["symbol"]: t for t in data.get("data", [])}
    return {}


def get_candles(symbol, gran="1h", limit=100):
    """
    Fetch OHLCV candles from Bitget.
    Cached in-memory for 90 seconds to reduce duplicate calls within one scan cycle.
    Returns list of dicts: {ts, open, high, low, close, volume, volume_usd}
    """
    g   = GRAN_MAP.get(gran, "1H")
    key = f"c_{symbol}_{g}_{limit}"
    if key in _cache:
        ts_cached, val = _cache[key]
        if time.time() - ts_cached < 90:
            return val
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/candles",
        params={
            "symbol":      symbol,
            "granularity": g,
            "limit":       str(limit),
            "productType": "usdt-futures",
        },
    )
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for c in data.get("data", []):
        try:
            vol_usd = float(c[6]) if len(c) > 6 else float(c[5]) * float(c[4])
            candles.append({
                "ts":         int(c[0]),
                "open":       float(c[1]),
                "high":       float(c[2]),
                "low":        float(c[3]),
                "close":      float(c[4]),
                "volume":     float(c[5]),
                "volume_usd": vol_usd,
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["ts"])
    _cache[key] = (time.time(), candles)
    return candles


def get_open_interest(symbol):
    """Fetch current OI from Bitget (returns value in USD)."""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/open-interest",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d = data["data"]
            if isinstance(d, list):
                d = d[0] if d else {}
            oi = float(d.get("openInterest", d.get("holdingAmount", 0)))
            price = float(d.get("indexPrice", d.get("lastPr", 0)) or 0)
            # If OI looks like raw contract units, multiply by price
            if 0 < oi < 1e9 and price > 0:
                return oi * price
            return oi
        except Exception:
            pass
    return 0.0


def get_oi_change(symbol):
    """
    Compare current OI to the snapshot saved at end of previous scan.
    Returns dict: {oi_now, oi_prev, change_pct, is_new}
    Also appends to rolling OI history for acceleration detection.
    """
    global _oi_snapshot, _oi_history
    oi_now = get_open_interest(symbol)
    now    = time.time()

    # Append to rolling history (used for OI acceleration signal)
    if symbol not in _oi_history:
        _oi_history[symbol] = []
    _oi_history[symbol].append({"ts": now, "oi": oi_now})
    if len(_oi_history[symbol]) > 40:
        _oi_history[symbol] = _oi_history[symbol][-40:]

    prev = _oi_snapshot.get(symbol)
    _oi_snapshot[symbol] = {"ts": now, "oi": oi_now}

    if prev is None or oi_now <= 0:
        return {"oi_now": oi_now, "oi_prev": 0.0, "change_pct": 0.0, "is_new": True}

    oi_prev    = prev["oi"]
    change_pct = ((oi_now - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0.0
    return {
        "oi_now":     round(oi_now, 2),
        "oi_prev":    round(oi_prev, 2),
        "change_pct": round(change_pct, 2),
        "is_new":     False,
    }


def get_funding(symbol):
    """Fetch current funding rate."""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d_list = data.get("data") or []
            if d_list:
                return float(d_list[0].get("fundingRate", 0))
        except Exception:
            pass
    return 0.0


def get_funding_stats(symbol):
    """Summarize the rolling funding snapshot for a symbol."""
    snaps = _funding_snapshots.get(symbol, [])
    if len(snaps) < 2:
        return None
    all_rates = [s["funding"] for s in snaps]
    last6     = all_rates[-6:]
    neg_pct   = sum(1 for f in last6 if f < 0) / len(last6) * 100
    streak    = 0
    for f in reversed(all_rates):
        if f < 0: streak += 1
        else:     break
    return {
        "avg":          sum(last6) / len(last6),
        "cumulative":   sum(last6),
        "neg_pct":      neg_pct,
        "streak":       streak,
        "current":      all_rates[-1],
        "sample_count": len(all_rates),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🧮  MATH / INDICATOR HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def calc_rsi(closes, period=14):
    """Wilder RSI. Returns float 0–100 or None if insufficient data."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[-period - 1 + i] - closes[-period - 2 + i]
        (gains if d >= 0 else losses).append(abs(d))
    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0
    # Smooth remaining candles
    for i in range(len(closes) - period, len(closes)):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            avg_gain = (avg_gain * (period - 1) + d) / period
            avg_loss = (avg_loss * (period - 1)) / period
        else:
            avg_gain = (avg_gain * (period - 1)) / period
            avg_loss = (avg_loss * (period - 1) + abs(d)) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_atr(candles, period=14):
    """Average True Range over `period` bars. Returns float or 0."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        c_prev = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - c_prev), abs(l - c_prev)))
    if len(trs) < period:
        return 0.0
    # Simple average of last `period` TR values (Wilder smoothing optional)
    return sum(trs[-period:]) / period


def _mean(values):
    return sum(values) / len(values) if values else 0.0

# ══════════════════════════════════════════════════════════════════════════════
#  🏗️  FEATURE EXTRACTION
#  All mathematical variables defined in the spec are computed here.
# ══════════════════════════════════════════════════════════════════════════════

def extract_features(symbol, ticker, candles_1h, candles_4h):
    """
    Compute all raw features needed for the four-phase scoring model.

    Returns a dict of feature values, or None if data is insufficient.
    """
    if not candles_1h or len(candles_1h) < 50:
        return None

    closes_1h = [c["close"] for c in candles_1h]
    highs_1h  = [c["high"]  for c in candles_1h]
    lows_1h   = [c["low"]   for c in candles_1h]
    vols_1h   = [c["volume_usd"] for c in candles_1h]

    # ── 1. RANGE COMPRESSION ─────────────────────────────────────────────────
    # Compare the current candle's high-low range to the 48h average range.
    # A ratio < 0.5 signals strong volatility compression (Bollinger squeeze).
    recent_range    = highs_1h[-1] - lows_1h[-1]                     # current bar range
    avg_range_48    = _mean([highs_1h[i] - lows_1h[i]
                             for i in range(-48, -1)])                 # last 48h avg range
    range_compression = (recent_range / avg_range_48) if avg_range_48 > 0 else 1.0

    # ── 2. ATR CONTRACTION ───────────────────────────────────────────────────
    # ATR14 / ATR100 — lower means volatility is contracting vs. longer history.
    atr14  = calc_atr(candles_1h[-15:],  period=14)
    atr100 = calc_atr(candles_1h[-101:], period=100) if len(candles_1h) >= 101 else atr14
    atr_contraction = (atr14 / atr100) if atr100 > 0 else 1.0

    # ── 3. VOLUME ACCUMULATION ───────────────────────────────────────────────
    # volume_24h / average_volume_7d — ratio > 1 means above-average volume today.
    vol_24h      = sum(vols_1h[-24:])
    vol_7d_avg   = sum(vols_1h[-168:]) / 7 if len(vols_1h) >= 168 else sum(vols_1h) / max(len(vols_1h) / 24, 1)
    vol_accum    = (vol_24h / vol_7d_avg) if vol_7d_avg > 0 else 1.0

    # ── 4. OI EXPANSION ──────────────────────────────────────────────────────
    oi_data = get_oi_change(symbol)
    oi_now  = oi_data["oi_now"]
    # Compute avg OI from history (last 24 OI readings)
    hist = _oi_history.get(symbol, [])
    if len(hist) >= 4:
        avg_oi_24h = _mean([e["oi"] for e in hist[-24:]])
    else:
        avg_oi_24h = oi_now  # not enough history → neutral
    oi_expansion = (oi_now / avg_oi_24h) if avg_oi_24h > 0 else 1.0

    # ── 5. PRICE STABILITY ───────────────────────────────────────────────────
    # abs(24h price change %) — LOW value + HIGH OI expansion = accumulation
    price_24h_ago    = closes_1h[-25] if len(closes_1h) >= 25 else closes_1h[0]
    price_now        = closes_1h[-1]
    price_change_24h = abs((price_now - price_24h_ago) / price_24h_ago * 100) if price_24h_ago > 0 else 0.0
    price_stability  = price_change_24h  # lower = more stable

    # ── 6. POSITION BUILD SCORE ──────────────────────────────────────────────
    # OI expansion × volume accumulation → both rising = new money entering
    position_score = oi_expansion * vol_accum

    # ── 7. BREAKOUT PRESSURE ─────────────────────────────────────────────────
    # vol_1h / avg_vol_24h — spike in last hour relative to the day average
    vol_1h_latest  = vols_1h[-1]
    avg_vol_hour   = _mean(vols_1h[-24:]) if len(vols_1h) >= 24 else vol_1h_latest
    breakout_pressure = (vol_1h_latest / avg_vol_hour) if avg_vol_hour > 0 else 1.0

    # ── 8. LIQUIDITY VACUUM ──────────────────────────────────────────────────
    # vol_accum / range_compression — high volume + low range = trapped liquidity
    liquidity_vacuum = (vol_accum / range_compression) if range_compression > 0 else vol_accum

    # ── 9. SHORT SQUEEZE POTENTIAL ───────────────────────────────────────────
    # OI rising + stable price = longs accumulating against shorts
    squeeze_score = (oi_expansion / (price_stability + 0.1))

    # ── 10. ENERGY BUILDUP ───────────────────────────────────────────────────
    # Combined structural energy: position build × liquidity vacuum
    energy = position_score * liquidity_vacuum

    # ── RSI (used for filter) ─────────────────────────────────────────────────
    rsi = calc_rsi(closes_1h)

    # ── 12h price change (used for filter) ───────────────────────────────────
    price_12h_ago    = closes_1h[-13] if len(closes_1h) >= 13 else closes_1h[0]
    price_change_12h = (price_now - price_12h_ago) / price_12h_ago * 100 if price_12h_ago > 0 else 0.0

    # ── Funding rate ──────────────────────────────────────────────────────────
    funding_rate = get_funding(symbol)
    _add_funding_snapshot(symbol, funding_rate)
    funding_stats = get_funding_stats(symbol)

    # ── Ticker 24h volume check ───────────────────────────────────────────────
    try:
        volume_usd_24h_ticker = float(ticker.get("quoteVolume", 0) or 0)
    except Exception:
        volume_usd_24h_ticker = vol_24h

    return {
        # Core metrics
        "price":               price_now,
        "price_change_24h":    (price_now - price_24h_ago) / price_24h_ago * 100 if price_24h_ago > 0 else 0.0,
        "price_change_12h":    price_change_12h,
        "price_stability":     price_stability,
        "rsi":                 rsi,
        "volume_usd_24h":      volume_usd_24h_ticker,

        # Phase 1 — Compression
        "range_compression":   range_compression,
        "atr_contraction":     atr_contraction,

        # Phase 2 — Accumulation
        "vol_accum":           vol_accum,
        "funding_rate":        funding_rate,
        "funding_stats":       funding_stats,

        # Phase 3 — Position Build
        "oi_expansion":        oi_expansion,
        "position_score":      position_score,
        "squeeze_score":       squeeze_score,
        "oi_change_pct":       oi_data["change_pct"],
        "oi_now":              oi_now,

        # Phase 4 — Ignition
        "breakout_pressure":   breakout_pressure,
        "liquidity_vacuum":    liquidity_vacuum,
        "energy":              energy,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🏆  SCORING MODEL
#  Four structural phases, weighted to 100 total points.
#  Each phase score is bounded 0–100 before weighting.
# ══════════════════════════════════════════════════════════════════════════════

def _clamp(val, lo=0.0, hi=100.0):
    return max(lo, min(hi, val))


def score_compression(f):
    """
    Phase 1 — COMPRESSION (weight 20%)

    Detects: volatility compression, ATR contraction.
    Score is HIGH when both range_compression and atr_contraction are LOW.
    Threshold tuned: range_compression < 0.6 and atr_contraction < 0.7 = ideal.
    """
    rc  = f["range_compression"]   # lower = more compressed
    atr = f["atr_contraction"]     # lower = more contracted

    # Map range_compression: 0.3 → 100, 1.0 → 0 (linear)
    rc_score  = _clamp((1.0 - rc)  / 0.7  * 100, 0, 100)
    # Map atr_contraction: 0.4 → 100, 1.0 → 0 (linear)
    atr_score = _clamp((1.0 - atr) / 0.6  * 100, 0, 100)

    # Bonus: both signals present simultaneously
    combo_bonus = 10.0 if rc < 0.5 and atr < 0.6 else 0.0

    raw = rc_score * 0.5 + atr_score * 0.5 + combo_bonus
    return _clamp(raw, 0, 100)


def score_accumulation(f):
    """
    Phase 2 — ACCUMULATION (weight 30%)

    Detects: above-average volume during sideways price action.
    A negative/low funding rate signals shorts are paying longs → potential squeeze.
    High vol_accum + low price movement = hidden accumulation by market makers.
    """
    va      = f["vol_accum"]          # > 1 = above average volume
    ps      = f["price_stability"]    # lower = more stable (range 0–100%)
    funding = f["funding_rate"]       # negative funding = shorts paying longs

    # Volume accumulation score: 1.5x avg vol → 50 pts, 2.5x → 100 pts
    vol_score = _clamp((va - 0.8) / 1.7 * 100, 0, 100)

    # Price stability bonus: volume with stable price is accumulation signal
    # ps < 3% is excellent, > 8% penalized
    stab_bonus = _clamp((5.0 - ps) / 5.0 * 30, 0, 30)

    # Funding score: negative funding up to -0.05% = short squeeze setup
    fund_score = 0.0
    if funding < 0:
        fund_score = _clamp(abs(funding) / 0.0005 * 25, 0, 25)
    elif funding > 0.001:
        # Extreme positive funding can mean over-leveraged longs (slight penalty)
        fund_score = -10.0

    # Funding streak bonus from historical snapshots
    fstat = f.get("funding_stats")
    streak_bonus = 0.0
    if fstat:
        if fstat["neg_pct"] > 60:
            streak_bonus = 10.0
        if fstat["streak"] >= 4:
            streak_bonus += 5.0

    raw = vol_score * 0.55 + stab_bonus + fund_score + streak_bonus
    return _clamp(raw, 0, 100)


def score_position_build(f):
    """
    Phase 3 — POSITION BUILD (weight 25%)

    Detects: open interest expanding while price stays flat.
    This is the hallmark of large players quietly building long positions.
    Also includes short-squeeze potential.
    """
    oi_exp  = f["oi_expansion"]     # > 1 = OI growing
    ps      = f["position_score"]   # oi_expansion × vol_accum
    sq      = f["squeeze_score"]    # OI / price_stability
    oi_chg  = f["oi_change_pct"]    # recent OI change %

    # OI expansion score: 1.05x → 30 pts, 1.2x → 100 pts
    oi_score = _clamp((oi_exp - 1.0) / 0.20 * 100, 0, 100)

    # Position score (compound signal)
    ps_score = _clamp((ps - 1.0) / 2.0 * 60, 0, 60)

    # Squeeze score: OI rising, price barely moving (short squeeze setup)
    sq_score = _clamp(sq / 20.0 * 40, 0, 40)

    # Recent OI change bonus: sudden spike in OI this interval
    oi_change_bonus = _clamp(oi_chg / 3.0 * 15, 0, 15) if oi_chg > 0 else 0.0

    raw = oi_score * 0.40 + ps_score * 0.30 + sq_score * 0.20 + oi_change_bonus
    return _clamp(raw, 0, 100)


def score_ignition(f):
    """
    Phase 4 — IGNITION (weight 25%)

    Detects: early signs of expansion beginning.
    Breakout pressure = volume in last hour vs. day average.
    Liquidity vacuum = buyers meeting thin supply above price.
    Energy buildup = compound structural tension.
    """
    bp  = f["breakout_pressure"]   # > 1.5 = above-average volume this hour
    lv  = f["liquidity_vacuum"]    # high vol / low range = trapped supply
    en  = f["energy"]              # position_score × liquidity_vacuum

    # Breakout pressure: 1.5x → 30 pts, 3x → 100 pts
    bp_score = _clamp((bp - 1.0) / 2.0 * 100, 0, 100)

    # Liquidity vacuum: ratio > 2 starts to matter, > 10 = maximum
    lv_score = _clamp((lv - 1.0) / 9.0 * 100, 0, 100)

    # Energy buildup: compound measure, > 4 = meaningful, > 15 = strong
    en_score = _clamp((en - 1.0) / 14.0 * 100, 0, 100)

    raw = bp_score * 0.35 + lv_score * 0.35 + en_score * 0.30
    return _clamp(raw, 0, 100)


def compute_pump_score(f):
    """
    Combine four phase scores with defined weights:
      Compression   20%
      Accumulation  30%
      Position      25%
      Ignition      25%

    Also computes pump_probability as a calibrated non-linear mapping
    so that borderline scores (55–65) show 50–65% and high scores (85+) show 85–95%.
    """
    s1 = score_compression(f)
    s2 = score_accumulation(f)
    s3 = score_position_build(f)
    s4 = score_ignition(f)

    total = (
        s1 * 0.20 +
        s2 * 0.30 +
        s3 * 0.25 +
        s4 * 0.25
    )
    total = _clamp(total, 0, 100)

    # Map raw score → pump probability (non-linear: conservative at low end)
    # Below 40 → < 30%, above 80 → > 85%
    if total < 40:
        prob = total * 0.6
    elif total < 60:
        prob = 24 + (total - 40) * 1.5
    elif total < 80:
        prob = 54 + (total - 60) * 1.2
    else:
        prob = 78 + (total - 80) * 0.85
    prob = _clamp(prob, 0, 98)

    return {
        "total":        round(total, 1),
        "probability":  round(prob, 1),
        "phase_scores": {
            "compression":   round(s1, 1),
            "accumulation":  round(s2, 1),
            "position_build":round(s3, 1),
            "ignition":      round(s4, 1),
        },
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🚫  FILTERS — Reject coins that have ALREADY pumped or are dangerous
# ══════════════════════════════════════════════════════════════════════════════

def passes_filters(symbol, f):
    """
    Returns (True, "") if the coin passes all filters.
    Returns (False, reason) if the coin should be rejected.

    Filters:
    1. Price already pumped >10% in last 12h → already moving, too late
    2. RSI > 80 → overbought, likely to reverse
    3. Volume 24h too low → insufficient liquidity
    4. Price change 24h > 15% → coin has already made a major move
    """
    # Filter 1: Recent pump detected
    if f["price_change_12h"] > CONFIG["max_price_change_12h"]:
        return False, f"already pumped {f['price_change_12h']:.1f}% in 12h"

    # Filter 2: RSI overbought
    if f["rsi"] is not None and f["rsi"] > CONFIG["max_rsi"]:
        return False, f"RSI overbought ({f['rsi']:.0f})"

    # Filter 3: Minimum liquidity
    if f["volume_usd_24h"] < CONFIG["min_volume_usd_24h"]:
        return False, f"low volume (${f['volume_usd_24h']:,.0f})"

    # Filter 4: Already had a 24h move
    if abs(f["price_change_24h"]) > 15.0:
        return False, f"24h move too large ({f['price_change_24h']:.1f}%)"

    return True, ""

# ══════════════════════════════════════════════════════════════════════════════
#  ⏱️  PUMP ESTIMATE
# ══════════════════════════════════════════════════════════════════════════════

def estimate_pump_time(score, f):
    """
    Estimate hours until pump initiates based on structural score.
    Higher score = more imminent breakout expected.
    Also considers ignition score as a timing accelerator.
    """
    ignition = f.get("phase_scores", {}).get("ignition", score)
    if score >= 80 and ignition >= 70:
        return "1–3h"
    elif score >= 70 and ignition >= 55:
        return "3–8h"
    elif score >= 60:
        return "8–24h"
    else:
        return "24–72h"

# ══════════════════════════════════════════════════════════════════════════════
#  📤  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

def _sanitize_telegram(msg):
    """Strip broken HTML tags and escape ampersands to avoid Telegram parse errors."""
    msg = msg.replace("&", "&amp;")
    return msg[:4000]


def send_telegram(msg):
    """Send message to Telegram. Tries HTML first, then plain text fallback."""
    bot_token = CONFIG["bot_token"]
    chat_id   = CONFIG["chat_id"]
    if not bot_token or not chat_id:
        log.warning("Telegram: BOT_TOKEN or CHAT_ID missing")
        return False

    msg = _sanitize_telegram(msg)

    for attempt in range(2):
        payload = {"chat_id": chat_id, "text": msg}
        if attempt == 0:
            payload["parse_mode"] = "HTML"
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                data=payload,
                timeout=15,
            )
            if r.status_code == 200:
                return True
            err = r.text[:200]
            if "can't parse" in err or "Bad Request" in err:
                # Strip HTML tags and retry as plain text
                msg = _html_mod.unescape(msg)
                for tag in ["<b>","</b>","<i>","</i>","<code>","</code>"]:
                    msg = msg.replace(tag, "")
                continue
            log.warning(f"Telegram HTTP {r.status_code}: {err}")
            return False
        except Exception as e:
            log.warning(f"Telegram exception: {e}")
            if attempt == 0:
                time.sleep(2)
    return False


def format_alert(rank, symbol, score_data, features):
    """
    Format the Telegram alert message.
    Follows the exact format specified in the system prompt.
    """
    score = score_data["total"]
    prob  = score_data["probability"]
    time_est = estimate_pump_time(score, score_data["phase_scores"])

    msg = (
        f"🚨 <b>POTENTIAL PUMP</b>\n"
        f"\n"
        f"Rank {rank}\n"
        f"Symbol: <b>{symbol}</b>\n"
        f"Score: {score:.0f}\n"
        f"Possible Pump: {prob:.0f}%\n"
        f"\n"
        f"Entry: 0\n"
        f"SL   : 0\n"
        f"TP   : 0\n"
        f"\n"
        f"Estimate pump: {time_est}\n"
    )
    return msg

# ══════════════════════════════════════════════════════════════════════════════
#  🔄  MAIN SCAN CYCLE
# ══════════════════════════════════════════════════════════════════════════════

def run_scan():
    """
    Full scan cycle:
    1. Fetch all tickers (one call)
    2. For each whitelisted symbol: fetch candles + OI + funding
    3. Extract features
    4. Apply filters
    5. Score
    6. Rank
    7. Alert top N if above threshold and not in cooldown
    """
    log.info("═" * 60)
    log.info(f"Scan started — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    # Step 1: Fetch all tickers in one API call
    tickers = get_all_tickers()
    if not tickers:
        log.warning("Failed to fetch tickers — skipping scan")
        return

    results = []
    symbols_scanned = 0

    for symbol in WHITELIST_SYMBOLS:
        ticker = tickers.get(symbol)
        if not ticker:
            continue  # Not listed on Bitget Futures

        try:
            # Step 2: Fetch candle data (1h, last 200 bars for ATR100 + history)
            candles_1h = get_candles(symbol, gran="1h", limit=200)
            candles_4h = get_candles(symbol, gran="4h", limit=50)  # reserved for future use
            time.sleep(CONFIG["sleep_between_coins"])

            # Step 3: Extract all features
            features = extract_features(symbol, ticker, candles_1h, candles_4h)
            if features is None:
                continue

            # Step 4: Apply filters
            ok, reason = passes_filters(symbol, features)
            if not ok:
                log.debug(f"  {symbol} filtered: {reason}")
                continue

            # Step 5: Compute score
            score_data = compute_pump_score(features)
            score_data["phase_scores"]["ignition"] = score_ignition(features)  # ensure present

            results.append({
                "symbol":     symbol,
                "score_data": score_data,
                "features":   features,
            })
            symbols_scanned += 1

        except Exception as e:
            log.error(f"Error processing {symbol}: {e}")
            continue

    # Step 6: Rank by total score (highest first)
    results.sort(key=lambda x: x["score_data"]["total"], reverse=True)

    log.info(f"Scanned {symbols_scanned} coins — {len(results)} passed filters")

    # Step 7: Print top 10 to log
    log.info("── TOP 10 ─────────────────────────────────────────────")
    for i, r in enumerate(results[:10], 1):
        s  = r["score_data"]
        ph = s["phase_scores"]
        log.info(
            f"  {i:2d}. {r['symbol']:<16} "
            f"Score={s['total']:5.1f}  Prob={s['probability']:.0f}%  "
            f"[C={ph['compression']:.0f} A={ph['accumulation']:.0f} "
            f"P={ph['position_build']:.0f} I={ph['ignition']:.0f}]"
        )

    # Step 8: Alert top N above threshold, skip cooldown
    alerts_sent = 0
    for rank, r in enumerate(results, 1):
        if alerts_sent >= CONFIG["top_n_alerts"]:
            break
        score = r["score_data"]["total"]
        sym   = r["symbol"]
        if score < CONFIG["min_score"]:
            break  # sorted descending — no point checking further
        if is_cooldown(sym):
            log.info(f"  {sym} in cooldown — skipped")
            continue

        msg = format_alert(rank, sym, r["score_data"], r["features"])
        ok  = send_telegram(msg)
        if ok:
            set_cooldown(sym)
            log.info(f"  ✅ Alert sent: {sym} (Score={score:.0f})")
            alerts_sent += 1
        else:
            log.warning(f"  ❌ Telegram failed for {sym}")

    if alerts_sent == 0:
        log.info("No alerts sent this cycle (score too low or all in cooldown)")

    # Persist state to disk
    _save_oi_snapshots()
    _save_oi_history()
    _save_funding_snapshots()

    log.info(f"Scan complete — {alerts_sent} alert(s) sent")


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("Scanner v34 starting…")
    log.info(f"Watching {len(WHITELIST_SYMBOLS)} symbols | "
             f"Min score: {CONFIG['min_score']} | "
             f"Cooldown: {CONFIG['alert_cooldown_sec']//3600}h | "
             f"Interval: {CONFIG['scan_interval_sec']}s")

    # Load persisted state from disk
    _load_oi_snapshots()
    _load_oi_history()
    _load_funding_snapshots()
    log.info(f"Cooldown active: {len(_cooldown)} coins")

    while True:
        try:
            run_scan()
        except KeyboardInterrupt:
            log.info("Interrupted — shutting down")
            break
        except Exception as e:
            log.error(f"Scan loop error: {e}")

        # Clear in-memory candle cache between cycles to avoid stale data
        _cache.clear()

        log.info(f"Sleeping {CONFIG['scan_interval_sec']}s until next scan…")
        time.sleep(CONFIG["scan_interval_sec"])


if __name__ == "__main__":
    main()
