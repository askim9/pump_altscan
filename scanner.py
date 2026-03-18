"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  SUPPORT HUNTER SCANNER — Enhanced Edition with MTF, Trend Filter & Adaptive ║
║  Based on "Support and Resistance (High Volume Boxes) [ChartPrime]"         ║
║  Game‑changing improvements:                                                 ║
║    • Multiple Timeframe Confirmation (1h + 4h)                               ║
║    • Trend filter using EMA50/EMA200 on 4h                                   ║
║    • Adaptive parameters based on BTC volatility                             ║
║    • Adaptive cooldown based on coin ATR                                     ║
║    • Better error handling & logging                                         ║
╚══════════════════════════════════════════════════════════════════════════════╝
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
from collections import defaultdict

# ── Persistent HTTP session ──────────────────────────────────────────────────
_http_session = requests.Session()
_http_session.headers.update({"User-Agent": "SupportHunter/2.0", "Accept-Encoding": "gzip"})

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ── Logging ───────────────────────────────────────────────────────────────────
_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)

_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)

_fh = _lh.RotatingFileHandler("/tmp/support_scanner.log", maxBytes=10*1024*1024, backupCount=3)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Support Hunter Scanner — Enhanced Edition — log: /tmp/support_scanner.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG — New parameters added for advanced features
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────────────────────
    "min_score_support":        30,
    "max_alerts_per_run":       15,

    # ── Volume 24h filter ────────────────────────────────────────────────────
    "min_vol_24h":          10_000,
    "max_vol_24h":      999_000_000,
    "pre_filter_vol":       10_000,

    # ── Open Interest minimum (optional) ──────────────────────────────────────
    "min_oi_usd":          100_000,

    # ── Candle limits ─────────────────────────────────────────────────────────
    "candle_1h":               200,   # enough for pivot detection
    "candle_4h":               100,   # for higher timeframe confirmation

    # ── Cooldown ──────────────────────────────────────────────────────────────
    "alert_cooldown_sec":     3600,   # base cooldown (will be adapted)
    "sleep_coins":             0.15,
    "sleep_error":             3.0,
    "cooldown_file":          "./cooldown_support.json",
    "funding_snapshot_file":  "./funding_support.json",
    "oi_snapshot_file":       "./oi_support.json",

    # ── Support Detection Parameters ──────────────────────────────────────────
    "support_lookback":        20,          # pivot lookback period (base)
    "support_vol_len":         2,           # window for volume extreme
    "support_box_width":       1.0,         # ATR multiplier for box height (base)
    "support_vol_mult":        1.5,         # min delta volume multiple
    "support_max_age":         50,          # max candles since pivot formation
    "support_bounce_candles":  3,           # look for bullish candles after touch

    # ── Scoring weights ───────────────────────────────────────────────────────
    "score_vol_strength":      10,
    "score_price_proximity":   15,
    "score_recent_bullish":    8,
    "score_flip_resistance":   12,
    "score_freshness":         5,
    "score_oi_confirmation":   6,
    "score_funding_neg":       5,
    "score_mtf_alignment":     10,          # new: support on higher timeframe
    "score_trend_alignment":   8,           # new: uptrend confirmation

    # ── Entry / SL / TP ───────────────────────────────────────────────────────
    "entry_buffer":            0.002,       # 0.2% above support level
    "sl_buffer":               0.005,       # 0.5% below box bottom
    "tp_atr_mult":             2.0,         # TP = entry + ATR * multiplier
    "tp2_atr_mult":            3.5,

    # ── Advanced Features ─────────────────────────────────────────────────────
    "use_mtf_confirmation":    True,        # check 4h support alignment
    "use_trend_filter":        True,        # only long if uptrend on 4h
    "use_dynamic_params":      True,        # adapt lookback/width to BTC volatility
    "btc_atr_period":          14,          # for volatility measurement
    "ema_short":               50,          # EMA period for trend filter
    "ema_long":                200,         # EMA period for trend filter
    "min_trend_score":         0,           # minimum score from trend alignment (0=disable)
}

# ── Whitelist (salin dari scanner_v28, di sini hanya contoh singkat) ─────────
WHITELIST_SYMBOLS = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT", "XRPUSDT",
    "DOGEUSDT", "DOTUSDT", "LINKUSDT", "MATICUSDT", "AVAXUSDT", "UNIUSDT",
    "ALGOUSDT", "ATOMUSDT", "VETUSDT", "FILUSDT", "ICPUSDT", "ETCUSDT",
    "FILUSDT", "AAVEUSDT", "SUSHIUSDT", "CAKEUSDT", "CRVUSDT", "SNXUSDT",
    "YGGUSDT", "GALAUSDT", "SANDUSDT", "MANAUSDT", "AXSUSDT", "ENJUSDT",
    "CHZUSDT", "BATUSDT", "ZILUSDT", "KSMUSDT", "RUNEUSDT", "NEARUSDT",
    "FTMUSDT", "ALPHAUSDT", "TLMUSDT", "C98USDT", "ROSEUSDT", "CELOUSDT",
    "KAVAUSDT", "IOSTUSDT", "ONTUSDT", "QTUMUSDT", "WAVESUSDT", "OMGUSDT",
    "LSKUSDT", "STORJUSDT", "SKLUSDT", "GRTUSDT", "1INCHUSDT", "ENSUSDT",
    "APEUSDT", "LDOUSDT", "OPUSDT", "ARBUSDT", "APTUSDT", "TIAUSDT",
    "SEIUSDT", "SUIUSDT", "WLDUSDT", "PEPEUSDT", "BONKUSDT", "ORDIUSDT",
    "INJUSDT", "BLURUSDT", "JTOUSDT", "JUPUSDT", "PYTHUSDT", "TNSRUSDT",
    "ENAUSDT", "ETHFIUSDT", "OMNIUSDT", "REZUSDT", "BBUSDT", "NOTUSDT",
    "ZKUSDT", "ZROUSDT", "DOGSUSDT", "CATIUSDT", "HMSTRUSDT", "NEIROUSDT",
    "EIGENUSDT", "GOATUSDT", "MOODENGUSDT", "PNUTUSDT", "ACTUSDT",
    "1000PEPEUSDT", "1000BONKUSDT", "1000SHIBUSDT", "1000RATSUSDT",
}

MANUAL_EXCLUDE = set()
EXCLUDED_KEYWORDS = ["XAU", "PAXG", "USDC", "DAI", "BUSD", "UST"]

GRAN_MAP    = {"5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
BITGET_BASE = "https://api.bitget.com"
_cache      = {}

# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN (adaptif)
# ══════════════════════════════════════════════════════════════════════════════
def load_cooldown():
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            # data format: {"SYM": {"timestamp": t, "atr": atr_value}}
            cleaned = {}
            for sym, entry in data.items():
                if isinstance(entry, dict):
                    ts = entry.get("timestamp", 0)
                    atr = entry.get("atr", 0.02)  # default 2%
                else:
                    # backward compatibility
                    ts = entry
                    atr = 0.02
                cooldown_sec = get_adaptive_cooldown_sec(atr)
                if now - ts < cooldown_sec:
                    cleaned[sym] = {"timestamp": ts, "atr": atr}
            return cleaned
    except Exception as e:
        log.warning(f"Load cooldown error: {e}")
    return {}

def save_cooldown(state):
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass

def get_adaptive_cooldown_sec(atr_ratio):
    """atr_ratio = ATR / price (as decimal). Base cooldown 3600s, adjust."""
    base = CONFIG["alert_cooldown_sec"]
    if atr_ratio <= 0:
        return base
    # Lebih volatile -> cooldown lebih pendek (min 1800s), kurang volatile -> lebih panjang (max 7200s)
    # atr_ratio 1% -> 3600s, 2% -> 2400s, 0.5% -> 4800s
    adaptive = base * (0.01 / atr_ratio)
    return max(1800, min(7200, adaptive))

def is_cooldown(sym):
    now = time.time()
    entry = _cooldown.get(sym)
    if not entry:
        return False
    if isinstance(entry, dict):
        ts = entry.get("timestamp", 0)
        atr = entry.get("atr", 0.02)
    else:
        ts = entry
        atr = 0.02
    return (now - ts) < get_adaptive_cooldown_sec(atr)

def set_cooldown(sym, atr_ratio=0.02):
    _cooldown[sym] = {"timestamp": time.time(), "atr": atr_ratio}
    save_cooldown(_cooldown)

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

# ══════════════════════════════════════════════════════════════════════════════
#  💾  FUNDING & OI SNAPSHOTS (sama seperti asli)
# ══════════════════════════════════════════════════════════════════════════════
_funding_snapshots = {}
_btc_candles_cache = {"ts": 0, "data": []}

def load_funding_snapshots():
    global _funding_snapshots
    try:
        p = CONFIG["funding_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                _funding_snapshots = json.load(f)
    except Exception:
        _funding_snapshots = {}

def save_all_funding_snapshots():
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(_funding_snapshots, f)
    except Exception:
        pass

def add_funding_snapshot(symbol, funding_rate):
    if symbol not in _funding_snapshots:
        _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({
        "ts":      time.time(),
        "funding": funding_rate,
    })
    if len(_funding_snapshots[symbol]) > 48:
        _funding_snapshots[symbol] = _funding_snapshots[symbol][-48:]

def get_funding_stats(symbol):
    snaps = _funding_snapshots.get(symbol, [])
    if len(snaps) < 2:
        return None
    all_rates = [s["funding"] for s in snaps]
    last6     = all_rates[-6:]
    avg6      = sum(last6) / len(last6)
    cumul     = sum(last6)
    neg_pct   = sum(1 for f in last6 if f < 0) / len(last6) * 100
    streak    = 0
    for f in reversed(all_rates):
        if f < 0:
            streak += 1
        else:
            break
    return {
        "avg":          avg6,
        "cumulative":   cumul,
        "neg_pct":      neg_pct,
        "streak":       streak,
        "basis":        all_rates[-1] * 100,
        "current":      all_rates[-1],
        "sample_count": len(all_rates),
    }

_oi_snapshot = {}

def load_oi_snapshots():
    global _oi_snapshot
    try:
        p = CONFIG["oi_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            _oi_snapshot = {
                sym: v for sym, v in data.items()
                if now - v.get("ts", 0) < 7200
            }
            log.info(f"OI snapshots loaded: {len(_oi_snapshot)} coins")
        else:
            _oi_snapshot = {}
    except Exception:
        _oi_snapshot = {}

def save_oi_snapshots():
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
            json.dump(_oi_snapshot, f)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP HELPERS (dengan sedikit perbaikan)
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=10, retries=2):
    for attempt in range(retries):
        try:
            r = _http_session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                sleep_time = 15 * (attempt + 1)
                log.warning(f"Rate limit — tunggu {sleep_time}s, lalu retry")
                time.sleep(sleep_time)
                continue
            log.warning(f"HTTP error {e}")
            break
        except requests.exceptions.RequestException as e:
            log.warning(f"Request exception: {e}")
            if attempt == retries - 1:
                break
            time.sleep(CONFIG["sleep_error"] * (attempt + 1))
        except Exception as e:
            log.warning(f"Unexpected error: {e}")
            break
    return None

def _safe_telegram_text(msg):
    import re
    msg = re.sub(r'&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)', '&amp;', msg)
    return msg

def send_telegram(msg, parse_mode="HTML"):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("send_telegram: BOT_TOKEN atau CHAT_ID tidak ada!")
        return False
    if len(msg) > 4000:
        msg = msg[:3900] + "\n\n<i>...[dipotong]</i>"
    msg = _safe_telegram_text(msg)
    for attempt in range(2):
        try:
            payload = {"chat_id": CHAT_ID, "text": msg}
            if attempt == 0:
                payload["parse_mode"] = "HTML"
            r = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data=payload,
                timeout=15,
            )
            if r.status_code == 200:
                return True
            err_text = r.text[:300]
            if "can't parse" in err_text or "Bad Request" in err_text:
                log.warning(f"Telegram parse error attempt {attempt} — retry plain text")
                msg = _html_mod.unescape(msg)
                msg = msg.replace("<b>","").replace("</b>","")
                msg = msg.replace("<i>","").replace("</i>","")
                continue
            log.warning(f"Telegram gagal: HTTP {r.status_code}")
            return False
        except Exception as e:
            log.warning(f"Telegram exception attempt {attempt}: {e}")
            if attempt == 0:
                time.sleep(2)
    return False

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ══════════════════════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS (ditingkatkan)
# ══════════════════════════════════════════════════════════════════════════════
def get_all_tickers():
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/tickers",
        params={"productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        return {t["symbol"]: t for t in data.get("data", [])}
    return {}

def get_candles(symbol, gran="1h", limit=168):
    g   = GRAN_MAP.get(gran, "1H")
    key = f"c_{symbol}_{g}_{limit}"
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < 90:
            return val
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/candles",
        params={
            "symbol":       symbol,
            "granularity":  g,
            "limit":        str(limit),
            "productType":  "usdt-futures",
        },
    )
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for c in data.get("data", []):
        try:
            # pastikan format candle: [ts, open, high, low, close, volume, volumeUsd?]
            # Bitget v2: [ts, open, high, low, close, volume, volumeUsd] untuk USDT futures
            if len(c) >= 7:
                vol_usd = float(c[6])
            else:
                vol_usd = float(c[5]) * float(c[4])
            candles.append({
                "ts":         int(c[0]),
                "open":       float(c[1]),
                "high":       float(c[2]),
                "low":        float(c[3]),
                "close":      float(c[4]),
                "volume":     float(c[5]),
                "volume_usd": vol_usd,
            })
        except Exception as e:
            log.debug(f"Parse candle error {symbol}: {e}")
            continue
    if not candles:
        return []
    candles.sort(key=lambda x: x["ts"])
    _cache[key] = (time.time(), candles)
    return candles

def get_funding(symbol):
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

def get_btc_candles_cached(limit=100):
    global _btc_candles_cache
    if time.time() - _btc_candles_cache["ts"] < 300 and _btc_candles_cache["data"]:
        return _btc_candles_cache["data"]
    candles = get_candles("BTCUSDT", "1h", limit)
    if candles:
        _btc_candles_cache = {"ts": time.time(), "data": candles}
    return candles

def get_open_interest(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/open-interest",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d = data["data"]
            if isinstance(d, list) and d:
                d = d[0]
            elif isinstance(d, list):
                return 0.0
            if "openInterestList" in d:
                oi_list = d.get("openInterestList") or []
                if oi_list:
                    oi = float(oi_list[0].get("openInterest", 0))
                else:
                    oi = float(d.get("openInterest", d.get("holdingAmount", 0)))
            else:
                oi = float(d.get("openInterest", d.get("holdingAmount", 0)))
            price = float(d.get("indexPrice", d.get("lastPr", 0)) or 0)
            if 0 < oi < 1e9 and price > 0:
                return oi * price
            return oi
        except Exception:
            pass
    return 0.0

def get_oi_change(symbol):
    global _oi_snapshot
    oi_now = get_open_interest(symbol)
    prev   = _oi_snapshot.get(symbol)
    if prev is None or oi_now <= 0:
        if oi_now > 0:
            _oi_snapshot[symbol] = {"ts": time.time(), "oi": oi_now}
        return {"oi_now": oi_now, "oi_prev": 0.0, "change_pct": 0.0, "is_new": True}
    oi_prev    = prev["oi"]
    change_pct = ((oi_now - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0.0
    _oi_snapshot[symbol] = {"ts": time.time(), "oi": oi_now}
    return {
        "oi_now":     round(oi_now, 2),
        "oi_prev":    round(oi_prev, 2),
        "change_pct": round(change_pct, 2),
        "is_new":     False,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📈  NEW HELPER FUNCTIONS (EMA, ATR, dynamic params, trend, MTF)
# ══════════════════════════════════════════════════════════════════════════════

def calc_ema(candles, period):
    """Simple EMA calculation from list of candles (each with 'close')."""
    if len(candles) < period:
        return None
    alpha = 2 / (period + 1)
    ema = candles[0]["close"]
    for i in range(1, len(candles)):
        ema = alpha * candles[i]["close"] + (1 - alpha) * ema
    return ema

def calc_atr(candles, period=14):
    """Calculate ATR from list of candles."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs)

def get_btc_atr_ratio():
    """Return (ATR/price) for BTCUSDT on 1h."""
    btc_candles = get_btc_candles_cached(limit=50)
    if len(btc_candles) < 20:
        return 0.01  # default 1%
    atr = calc_atr(btc_candles, CONFIG["btc_atr_period"])
    price = btc_candles[-1]["close"]
    if price <= 0:
        return 0.01
    return atr / price

def get_dynamic_params(btc_atr_ratio):
    """Adjust lookback, box_width, min_score based on BTC volatility."""
    base_lookback = CONFIG["support_lookback"]
    base_width = CONFIG["support_box_width"]
    base_min_score = CONFIG["min_score_support"]

    # Volatilitas tinggi -> lebih panjang lookback, lebih lebar box, skor minimal lebih tinggi
    if btc_atr_ratio > 0.02:  # >2%
        new_lookback = int(base_lookback * 1.5)
        new_width = base_width * 1.5
        new_min_score = base_min_score + 10
    elif btc_atr_ratio < 0.005:  # <0.5%
        new_lookback = int(base_lookback * 0.7)
        new_width = base_width * 0.8
        new_min_score = base_min_score - 5
    else:
        new_lookback = base_lookback
        new_width = base_width
        new_min_score = base_min_score

    # Batasan
    new_lookback = max(10, min(50, new_lookback))
    new_width = max(0.5, min(3.0, new_width))
    new_min_score = max(15, min(70, new_min_score))

    return {
        "lookback": new_lookback,
        "box_width": new_width,
        "min_score": new_min_score
    }

def check_higher_tf_support(symbol, current_price):
    """Cari support aktif di timeframe 4h, return level support teratas atau None."""
    c4h = get_candles(symbol, "4h", CONFIG["candle_4h"])
    if len(c4h) < 50:
        return None
    deltas_4h = calc_delta_volume(c4h)
    current_idx_4h = len(c4h) - 1
    # Gunakan parameter statis untuk 4h (atau bisa juga adaptif)
    boxes_4h = find_support_boxes(c4h, deltas_4h,
                                   lookback=15,  # lebih pendek untuk 4h
                                   vol_len=CONFIG["support_vol_len"],
                                   box_width=1.0)
    active_4h = []
    for b in boxes_4h:
        if is_box_broken(c4h, b, current_idx_4h):
            continue
        if b["zone_high"] > current_price:  # support di atas harga? tidak relevan
            continue
        active_4h.append(b)
    if not active_4h:
        return None
    # ambil support terdekat dengan harga
    active_4h.sort(key=lambda x: x["level"], reverse=True)
    return active_4h[0]["level"]

def check_trend(symbol):
    """Cek uptrend berdasarkan EMA50 > EMA200 pada timeframe 4h."""
    c4h = get_candles(symbol, "4h", CONFIG["candle_4h"])
    if len(c4h) < CONFIG["ema_long"] + 10:
        return False
    ema50 = calc_ema(c4h, CONFIG["ema_short"])
    ema200 = calc_ema(c4h, CONFIG["ema_long"])
    if ema50 is None or ema200 is None:
        return False
    last_close = c4h[-1]["close"]
    return last_close > ema50 and ema50 > ema200

# ══════════════════════════════════════════════════════════════════════════════
#  🆕  SUPPORT/RESISTANCE CORE FUNCTIONS (tetap sama)
# ══════════════════════════════════════════════════════════════════════════════

def calc_delta_volume(candles):
    deltas = []
    for c in candles:
        rng = c["high"] - c["low"]
        if rng > 0:
            clv = 2.0 * (c["close"] - c["low"]) / rng - 1.0
        else:
            clv = 0.0
        deltas.append(c["volume_usd"] * clv)
    return deltas

def find_pivot_highs(candles, lookback):
    pivots = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        left  = max(c["high"] for c in candles[i-lookback:i])
        right = max(c["high"] for c in candles[i+1:i+1+lookback])
        if candles[i]["high"] > left and candles[i]["high"] > right:
            pivots.append((i, candles[i]["high"]))
    return pivots

def find_pivot_lows(candles, lookback):
    pivots = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        left  = min(c["low"] for c in candles[i-lookback:i])
        right = min(c["low"] for c in candles[i+1:i+1+lookback])
        if candles[i]["low"] < left and candles[i]["low"] < right:
            pivots.append((i, candles[i]["low"]))
    return pivots

def calc_atr_at_index(candles, idx, period=14):
    if idx < period:
        return 0.0
    trs = []
    for i in range(idx - period + 1, idx + 1):
        if i < 1:
            continue
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else 0.0

def find_support_boxes(candles, deltas, lookback, vol_len, box_width):
    abs_deltas = [abs(d) for d in deltas]
    avg_abs_delta = []
    for i in range(len(deltas)):
        start = max(0, i - vol_len + 1)
        window = abs_deltas[start:i+1]
        avg_abs_delta.append(sum(window)/len(window) if window else 0)

    pivots = find_pivot_lows(candles, lookback)
    boxes = []
    for idx, level in pivots:
        delta = deltas[idx]
        avg_abs = avg_abs_delta[idx]
        if avg_abs > 0 and delta > avg_abs * CONFIG["support_vol_mult"]:
            atr = calc_atr_at_index(candles, idx, period=14)
            zone_low = level - atr * box_width
            boxes.append({
                "type": "support",
                "pivot_idx": idx,
                "level": level,
                "zone_low": zone_low,
                "zone_high": level,
                "delta_vol": delta,
                "atr": atr,
                "vol_ratio": delta / avg_abs if avg_abs else 0,
            })
    return boxes

def find_resistance_boxes(candles, deltas, lookback, vol_len, box_width):
    abs_deltas = [abs(d) for d in deltas]
    avg_abs_delta = []
    for i in range(len(deltas)):
        start = max(0, i - vol_len + 1)
        window = abs_deltas[start:i+1]
        avg_abs_delta.append(sum(window)/len(window) if window else 0)

    pivots = find_pivot_highs(candles, lookback)
    boxes = []
    for idx, level in pivots:
        delta = deltas[idx]
        avg_abs = avg_abs_delta[idx]
        if avg_abs > 0 and delta < -avg_abs * CONFIG["support_vol_mult"]:
            atr = calc_atr_at_index(candles, idx, period=14)
            zone_high = level + atr * box_width
            boxes.append({
                "type": "resistance",
                "pivot_idx": idx,
                "level": level,
                "zone_low": level,
                "zone_high": zone_high,
                "delta_vol": delta,
                "atr": atr,
                "vol_ratio": -delta / avg_abs if avg_abs else 0,
            })
    return boxes

def is_box_broken(candles, box, current_idx):
    for i in range(box["pivot_idx"], current_idx + 1):
        if candles[i]["close"] < box["zone_low"]:
            return True
    return False

def find_active_support(candles, deltas, current_price, current_idx, lookback, box_width):
    boxes = find_support_boxes(candles, deltas,
                               lookback=lookback,
                               vol_len=CONFIG["support_vol_len"],
                               box_width=box_width)
    active = []
    for b in boxes:
        age = current_idx - b["pivot_idx"]
        if age > CONFIG["support_max_age"]:
            continue
        if is_box_broken(candles, b, current_idx):
            continue
        if b["zone_high"] > current_price:
            continue
        active.append(b)
    if not active:
        return None
    active.sort(key=lambda x: x["level"], reverse=True)
    return active[0]

def score_support_box(box, candles, current_price, current_idx, deltas, oi_data, funding, extra_score=0):
    score = 0
    signals = []

    # Volume strength at pivot
    vol_score = min(box["vol_ratio"] * 5, 10)
    score += vol_score
    signals.append(f"📊 Delta Volume: {box['vol_ratio']:.1f}x avg → +{vol_score:.0f}")

    # Price proximity to support level
    dist_to_level = (current_price - box["level"]) / box["level"] * 100
    if dist_to_level < 1.0:
        prox_score = CONFIG["score_price_proximity"]
        score += prox_score
        signals.append(f"📍 Price within {dist_to_level:.2f}% of support → +{prox_score}")
    elif dist_to_level < 3.0:
        prox_score = CONFIG["score_price_proximity"] // 2
        score += prox_score
        signals.append(f"📍 Price {dist_to_level:.1f}% above support (moderate) → +{prox_score}")

    # Recent bullish candles after support touch
    touch_idx = None
    for i in range(current_idx, box["pivot_idx"], -1):
        if candles[i]["low"] <= box["zone_high"] and candles[i]["close"] > candles[i]["open"]:
            touch_idx = i
            break
    if touch_idx is not None:
        bullish_count = 0
        for i in range(touch_idx, current_idx + 1):
            if candles[i]["close"] > candles[i]["open"]:
                bullish_count += 1
        if bullish_count >= 2:
            score += CONFIG["score_recent_bullish"]
            signals.append(f"🟢 Bullish candles after support touch → +{CONFIG['score_recent_bullish']}")

    # Freshness
    age = current_idx - box["pivot_idx"]
    if age < 20:
        score += CONFIG["score_freshness"]
        signals.append(f"⏱️ Fresh support ({age} candles) → +{CONFIG['score_freshness']}")

    # OI confirmation
    if not oi_data.get("is_new") and oi_data["change_pct"] > 3.0:
        score += CONFIG["score_oi_confirmation"]
        signals.append(f"📈 OI +{oi_data['change_pct']:.1f}% → +{CONFIG['score_oi_confirmation']}")

    # Funding negative
    if funding < 0:
        score += CONFIG["score_funding_neg"]
        signals.append(f"💸 Funding {funding*100:.3f}% neg → +{CONFIG['score_funding_neg']}")

    # Extra score (from MTF/trend alignment)
    if extra_score:
        score += extra_score
        signals.append(f"🔷 Konfirmasi tambahan → +{extra_score}")

    return score, signals

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE (support version) dengan fitur baru
# ══════════════════════════════════════════════════════════════════════════════
def master_score_support(symbol, ticker):
    # 1. Ambil candles 1h
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    if len(c1h) < 50:
        log.debug(f"  {symbol}: Candle 1h tidak cukup ({len(c1h)} < 50)")
        return None

    # 2. Data dasar
    try:
        vol_24h   = float(ticker.get("quoteVolume", 0))
        chg_24h   = float(ticker.get("change24h", 0)) * 100
        price_now = float(ticker.get("lastPr", 0)) or c1h[-1]["close"]
    except Exception:
        return None

    if vol_24h < CONFIG["min_vol_24h"] or vol_24h > CONFIG["max_vol_24h"]:
        return None

    # 3. OI & funding
    oi_data = get_oi_change(symbol)
    funding = get_funding(symbol)
    add_funding_snapshot(symbol, funding)

    # 4. Parameter adaptif berdasarkan volatilitas BTC
    if CONFIG["use_dynamic_params"]:
        btc_atr_ratio = get_btc_atr_ratio()
        dyn = get_dynamic_params(btc_atr_ratio)
        lookback = dyn["lookback"]
        box_width = dyn["box_width"]
        min_score = dyn["min_score"]
    else:
        lookback = CONFIG["support_lookback"]
        box_width = CONFIG["support_box_width"]
        min_score = CONFIG["min_score_support"]

    # 5. Deteksi support aktif
    deltas = calc_delta_volume(c1h)
    current_idx = len(c1h) - 1
    box = find_active_support(c1h, deltas, price_now, current_idx, lookback, box_width)
    if not box:
        log.debug(f"  {symbol}: Tidak ada support aktif")
        return None

    # 6. Hitung skor dasar
    score, signals = score_support_box(box, c1h, price_now, current_idx, deltas, oi_data, funding)

    # 7. Filter / bonus dari fitur canggih
    extra_score = 0

    # --- Trend filter ---
    if CONFIG["use_trend_filter"]:
        uptrend = check_trend(symbol)
        if uptrend:
            extra_score += CONFIG["score_trend_alignment"]
            signals.append(f"📈 Uptrend 4h (EMA50>EMA200) → +{CONFIG['score_trend_alignment']}")
        else:
            # Opsional: jika tidak uptrend, bisa langsung reject
            if CONFIG["min_trend_score"] > 0:
                log.debug(f"  {symbol}: Gagal trend filter (downtrend)")
                return None

    # --- Multiple timeframe confirmation ---
    if CONFIG["use_mtf_confirmation"]:
        support_4h_level = check_higher_tf_support(symbol, price_now)
        if support_4h_level is not None:
            # Beri bonus jika support 1h dekat dengan support 4h (dalam 3%)
            dist_4h = abs(price_now - support_4h_level) / price_now * 100
            if dist_4h < 3.0:
                extra_score += CONFIG["score_mtf_alignment"]
                signals.append(f"📌 Support 4h di {support_4h_level:.4f} (selisih {dist_4h:.1f}%) → +{CONFIG['score_mtf_alignment']}")

    # Tambahkan extra score ke total
    score += extra_score

    # 8. Threshold
    if score < min_score:
        log.debug(f"  {symbol}: Support score {score} < {min_score}")
        return None

    # 9. Hitung ATR terbaru untuk SL/TP adaptif
    atr_now = calc_atr_at_index(c1h, current_idx, period=14)

    # Entry, SL, TP
    entry = box["level"] * (1.0 + CONFIG["entry_buffer"])
    sl    = box["zone_low"] * (1.0 - CONFIG["sl_buffer"])
    tp1   = entry + atr_now * CONFIG["tp_atr_mult"]
    tp2   = entry + atr_now * CONFIG["tp2_atr_mult"]

    # Alert level
    if score >= 60:
        alert_level = "STRONG ALERT"
    elif score >= 45:
        alert_level = "ALERT"
    else:
        alert_level = "WATCHLIST"

    # Simpan atr_ratio untuk cooldown adaptif
    atr_ratio = atr_now / price_now if price_now > 0 else 0.02

    return {
        "symbol": symbol,
        "score": score,
        "signals": signals,
        "price": price_now,
        "support_level": box["level"],
        "zone_low": box["zone_low"],
        "zone_high": box["zone_high"],
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "atr": atr_now,
        "atr_ratio": atr_ratio,
        "pivot_age": current_idx - box["pivot_idx"],
        "vol_24h": vol_24h,
        "chg_24h": chg_24h,
        "alert_level": alert_level,
        "oi_data": oi_data,
        "funding": funding,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER (dengan sedikit penyesuaian)
# ══════════════════════════════════════════════════════════════════════════════
def _fmt_price(p):
    if p == 0: return "0"
    if p >= 100: return f"{p:.2f}"
    if p >= 1: return f"{p:.4f}"
    if p >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"

def build_alert_support(r, rank=None):
    level_icon = {"STRONG ALERT": "🔥", "ALERT": "📡", "WATCHLIST": "👁"}.get(r["alert_level"], "👁")

    msg = f"{level_icon} <b>{r['symbol']} — {r['alert_level']}</b>  #{rank}\n"
    msg += f"<b>Score:</b> {r['score']}  |  <b>Support Age:</b> {r['pivot_age']} candles\n"
    msg += f"<b>Scan:</b> {utc_now()}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    msg += f"<b>Harga :</b> <code>{_fmt_price(r['price'])}</code> ({r['chg_24h']:+.1f}% 24h)\n"
    msg += f"<b>Support:</b> <code>{_fmt_price(r['support_level'])}</code>\n"
    msg += f"<b>Zone  :</b> <code>{_fmt_price(r['zone_low'])}</code> – <code>{_fmt_price(r['zone_high'])}</code>\n"
    msg += f"<b>ATR   :</b> {r['atr']/r['price']*100:.2f}%\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    msg += f"📍 <b>Entry :</b> <code>{_fmt_price(r['entry'])}</code> (buffer {CONFIG['entry_buffer']*100:.1f}%)\n"
    msg += f"🛑 <b>SL    :</b> <code>{_fmt_price(r['sl'])}</code> ({(r['entry']-r['sl'])/r['entry']*100:.2f}%)\n"
    msg += f"🎯 <b>TP1   :</b> <code>{_fmt_price(r['tp1'])}</code> (+{(r['tp1']-r['entry'])/r['entry']*100:.1f}%)\n"
    msg += f"🎯 <b>TP2   :</b> <code>{_fmt_price(r['tp2'])}</code> (+{(r['tp2']-r['entry'])/r['entry']*100:.1f}%)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    # Signals
    msg += "<b>Sinyal:</b>\n"
    for s in r["signals"][:7]:
        msg += f"• {s}\n"

    # OI / Funding
    if r["oi_data"]["oi_now"] > 0:
        ov = r["oi_data"]["oi_now"]
        os_str = f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K"
        cs = f"({r['oi_data']['change_pct']:+.1f}%)" if not r["oi_data"].get("is_new") else "(baseline)"
        msg += f"📈 OI: {os_str} {cs}\n"
    if r["funding"] != 0:
        msg += f"💸 Funding: {r['funding']*100:.3f}%\n"

    msg += f"\n<i>⚠️ Bukan financial advice</i>"
    return msg

def build_summary(results):
    top = results[:5]
    msg = f"📋 <b>TOP {len(top)} SUPPORT SETUPS — Support Hunter (Enhanced)</b>\n{utc_now()}\n{'━'*30}\n"
    for i, r in enumerate(top, 1):
        sym = r["symbol"].replace("USDT", "")
        msg += f"\n{i}. <b>{sym}</b> Score:{r['score']} | Age:{r['pivot_age']}c\n"
        msg += f"   Support: {_fmt_price(r['support_level'])} | Entry: {_fmt_price(r['entry'])}\n"
        msg += f"   SL: {_fmt_price(r['sl'])} | TP1: +{(r['tp1']-r['entry'])/r['entry']*100:.1f}%\n"
        if r["signals"]:
            msg += f"   • {r['signals'][0][:70]}\n"
    msg += f"\n{'━'*30}\n<i>⚠️ Bukan financial advice</i>"
    return msg

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST (sama, hanya log tambahan)
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    all_candidates = []
    not_found      = []
    filtered_stats = defaultdict(int)

    log.info("=" * 70)
    log.info("🔍 SCANNING WHITELIST (Enhanced Edition)")
    log.info("=" * 70)

    for sym in WHITELIST_SYMBOLS:
        if any(kw in sym for kw in EXCLUDED_KEYWORDS):
            filtered_stats["excluded_keyword"] += 1
            continue
        if sym in MANUAL_EXCLUDE:
            filtered_stats["manual_exclude"] += 1
            continue
        if is_cooldown(sym):
            filtered_stats["cooldown"] += 1
            continue
        if sym not in tickers:
            not_found.append(sym)
            continue
        ticker = tickers[sym]
        try:
            vol   = float(ticker.get("quoteVolume", 0))
            chg   = float(ticker.get("change24h",   0)) * 100
            price = float(ticker.get("lastPr",       0))
        except Exception:
            filtered_stats["parse_error"] += 1
            continue
        if vol < CONFIG["pre_filter_vol"]:
            filtered_stats["vol_too_low"] += 1
            continue
        if vol > CONFIG["max_vol_24h"]:
            filtered_stats["vol_too_high"] += 1
            continue
        if chg > 30.0:
            filtered_stats["change_too_high"] += 1
            continue
        if chg < -15.0:
            filtered_stats["dump_too_deep"] += 1
            continue
        if price <= 0:
            filtered_stats["invalid_price"] += 1
            continue
        all_candidates.append((sym, ticker))

    total = len(WHITELIST_SYMBOLS)
    will_scan = len(all_candidates)
    n_excluded = filtered_stats.get("excluded_keyword", 0) + filtered_stats.get("manual_exclude", 0)
    n_filtered = sum(v for k, v in filtered_stats.items() if k not in ("excluded_keyword", "manual_exclude"))
    accounted = will_scan + n_excluded + n_filtered + len(not_found)

    log.info(f"\n📊 SCAN SUMMARY:")
    log.info(f"   Whitelist total  : {total} coins")
    log.info(f"   ✅ Will scan     : {will_scan} ({will_scan/total*100:.1f}%)")
    log.info(f"   🚫 Excluded kw   : {n_excluded}")
    log.info(f"   ❌ Filtered      : {n_filtered}")
    log.info(f"   ⚠️  Not in Bitget : {len(not_found)}")
    log.info(f"   ✔️  Akuntabel     : {accounted}/{total}")
    log.info(f"\n📋 Filter breakdown:")
    for k, v in sorted(filtered_stats.items()):
        log.info(f"   {k:25s}: {v}")
    if not_found:
        sample = ", ".join(not_found[:10])
        log.info(f"\n   Missing sample   : {sample}{' ...' if len(not_found) > 10 else ''}")
    est_secs = will_scan * CONFIG["sleep_coins"]
    log.info(f"\n⏱️  Est. scan time: {est_secs:.0f}s (~{est_secs/60:.1f} min)")
    log.info("=" * 70 + "\n")
    return all_candidates

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== SUPPORT HUNTER SCANNER (ENHANCED) — {utc_now()} ===")

    load_funding_snapshots()
    load_oi_snapshots()

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return
    log.info(f"Total ticker dari Bitget: {len(tickers)}")

    candidates = build_candidate_list(tickers)
    results = []

    for i, (sym, t) in enumerate(candidates):
        if (i + 1) % 10 == 0:
            log.info(f"[{i+1}/{len(candidates)}] {sym}...")
        try:
            res = master_score_support(sym, t)
            if res:
                log.info(f"  ✅ Score={res['score']} | {res['alert_level']} | Support age:{res['pivot_age']}")
                results.append(res)
        except Exception as ex:
            log.warning(f"  ❌ Error {sym}: {ex}", exc_info=True)
            continue
        time.sleep(CONFIG["sleep_coins"])

    save_all_funding_snapshots()
    save_oi_snapshots()
    log.info("Funding dan OI snapshots disimpan ke disk.")

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"\nLolos threshold: {len(results)} coin")

    if not results:
        log.info("Tidak ada sinyal support saat ini.")
        return

    top = results[:CONFIG["max_alerts_per_run"]]

    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert_support(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"], r.get("atr_ratio", 0.02))
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║      SUPPORT HUNTER SCANNER — ENHANCED EDITION v2.0          ║")
    log.info("║   MTF · Trend Filter · Adaptive Parameters · Adaptive Cooldown ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")
    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)
    run_scan()
