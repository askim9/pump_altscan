"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v13.4-SUPPORT                                        ║
║                                                                          ║
║  FITUR BARU: FILTER SUPPORT KUAT                                        ║
║    • Hanya koin yang berada di area support kuat (skor ≥ 70)           ║
║    • Jarak dinamis berdasarkan ATR                                      ║
║    • Konfirmasi volume, pola candlestick, dan higher timeframe         ║
║    • Composite score: support (40%) + teknikal (35%) + funding (25%)   ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging
from datetime import datetime, timezone
from collections import defaultdict

# Library tambahan untuk analisis teknikal
import pandas as pd
import pandas_ta as ta
import numpy as np

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ── Logging ───────────────────────────────────────────────────────────────
import logging.handlers as _lh
_log_fmt    = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root   = logging.getLogger()
_log_root.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v13.log", maxBytes=10*1024*1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)
log = logging.getLogger(__name__)
log.info("Log file aktif: /tmp/scanner_v13.log (rotasi 10MB)")

# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIG (ditambahkan bagian support)
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────
    "min_score_alert":          10,          # untuk backward compatibility (tidak dipakai langsung)
    "max_alerts_per_run":        15,

    # ── Volume 24h TOTAL (USD) ─────────────────────────────────
    "min_vol_24h":            3_000,
    "max_vol_24h":       50_000_000,
    "pre_filter_vol":         1_000,

    # ── Gate perubahan harga (pre-filter) ──────────────────────
    "gate_chg_24h_max":          30.0,

    # ── Funding Gate (WAJIB) ───────────────────────────────────
    "funding_gate_avg":        -0.0001,
    "funding_gate_cumul":      -0.02,

    # ── Candle limits ─────────────────────────────────────────
    "candle_1h":                168,
    "candle_15m":                96,
    "candle_4h":                 42,
    "candle_1d":                 30,        # tambahan untuk 1D

    # ── Entry/exit ────────────────────────────────────────────
    "min_target_pct":             8.0,
    "max_sl_pct":                3.0,
    "entry_support_offset":       0.0,
    "entry_range_above":         0.003,

    # ── Operasional ───────────────────────────────────────────
    "alert_cooldown_sec":       1800,
    "sleep_coins":               0.8,
    "sleep_error":               3.0,
    "cooldown_file":    "./cooldown.json",
    "funding_snapshot_file":"./funding.json",

    # ── Bobot skor (utama) ────────────────────────────────────
    "score_bbw_12":              5,
    "score_bbw_10":              4,
    "score_bbw_8":               2,
    "score_price_2":             5,
    "score_price_1":             3,
    "score_price_05":            2,
    "score_above_vwap_bos":      4,
    "score_above_vwap":          2,
    "score_rsi_65":              3,
    "score_rsi_55":              2,
    "score_atr_15":              4,
    "score_atr_10":              2,
    "score_lowcap":              1,
    "score_ath_dist":            1,

    # ── Bobot tambahan ───────────────────────────────────────
    "score_vol_ratio_24h":       2,
    "score_vol_accel":           2,
    "score_macd_pos":            1,

    # ── Bobot funding (dipisahkan untuk funding score) ───────
    "score_funding_neg_pct":     3,
    "score_funding_streak":      3,
    "score_basis":               2,

    # ── Threshold tambahan ────────────────────────────────────
    "above_vwap_rate_min":       0.6,
    "squeeze_funding_cumul":    -0.05,
    "vol_ratio_threshold":       2.5,
    "vol_accel_threshold":       0.5,

    # ── Konfigurasi Support Strength (BARU) ───────────────────
    "support": {
        "min_touches": 2,
        "touch_tolerance": 0.003,        # 0.3%
        "max_distance_atr_multiplier": 1.5,
        "max_distance_cap": 0.02,        # 2% cap
    },
    "volume": {
        "min_touch_volume_ratio": 1.2,
        "confirmation_volume_ratio": 1.3,
    },
    "scoring": {
        "min_support_score": 70,          # minimal skor support untuk dianggap kuat
        "strong_support_threshold": 85,
        "composite_threshold": 75,        # minimal composite score untuk alert
        "support_weight": 0.4,
        "tech_weight": 0.35,
        "funding_weight": 0.25,
    },
    "timeframes": {
        "primary": "1h",
        "secondary": "4h",
        "structural": "1d",
    }
}

MANUAL_EXCLUDE = set()

# ══════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin pilihan (sama seperti sebelumnya)
# ══════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
    "DOGEUSDT", "BCHUSDT", "ADAUSDT", "HYPEUSDT", "XMRUSDT", "LINKUSDT", "XLMUSDT", "HBARUSDT",
    "LTCUSDT", "ZECUSDT", "AVAXUSDT", "SHIBUSDT", "SUIUSDT", "TONUSDT", "WLFIUSDT", "CROUSDT",
    "UNIUSDT", "DOTUSDT", "TAOUSDT", "MUSDT", "AAVEUSDT", "ASTERUSDT", "PEPEUSDT", "BGBUSDT",
    "SKYUSDT", "ETCUSDT", "NEARUSDT", "ONDOUSDT", "POLUSDT", "ICPUSDT", "WLDUSDT", "ATOMUSDT",
    "XDCUSDT", "COINUSDT", "NIGHTUSDT", "ENAUSDT", "PIPPINUSDT", "KASUSDT", "TRUMPUSDT", "QNTUSDT",
    "ALGOUSDT", "RENDERUSDT", "FILUSDT", "MORPHOUSDT", "APTUSDT", "SUPERUSDT", "VETUSDT", "PUMPUSDT",
    "1000SATSUSDT", "ARBUSDT", "1000BONKUSDT", "STABLEUSDT", "KITEUSDT", "JUPUSDT", "SEIUSDT", "ZROUSDT",
    "STXUSDT", "DYDXUSDT", "VIRTUALUSDT", "DASHUSDT", "PENGUUSDT", "CAKEUSDT", "JSTUSDT", "XTZUSDT",
    "ETHFIUSDT", "1MBABYDOGEUSDT", "IPUSDT", "LITUSDT", "HUSDT", "FETUSDT", "CHZUSDT", "CRVUSDT",
    "KAIAUSDT", "IMXUSDT", "BSVUSDT", "INJUSDT", "AEROUSDT", "PYTHUSDT", "IOTAUSDT", "EIGENUSDT",
    "GRTUSDT", "JASMYUSDT", "DEXEUSDT", "SPXUSDT", "TIAUSDT", "FLOKIUSDT", "HNTUSDT", "SIRENUSDT",
    "LDOUSDT", "CFXUSDT", "OPUSDT", "ENSUSDT", "STRKUSDT", "MONUSDT", "AXSUSDT", "SANDUSDT",
    "PENDLEUSDT", "WIFUSDT", "LUNCUSDT", "FFUSDT", "NEOUSDT", "THETAUSDT", "RIVERUSDT", "BATUSDT",
    "MANAUSDT", "CVXUSDT", "COMPUSDT", "BARDUSDT", "SENTUSDT", "GALAUSDT", "VVVUSDT", "RAYUSDT",
    "XPLUSDT", "FLUIDUSDT", "FARTCOINUSDT", "GLMUSDT", "RUNEUSDT", "0GUSDT", "POWERUSDT", "SKRUSDT",
    "EGLDUSDT", "BUSDT", "BERAUSDT", "SNXUSDT", "BANUSDT", "JTOUSDT", "ARUSDT", "COWUSDT",
    "DEEPUSDT", "SUSDT", "LPTUSDT", "MELANIAUSDT", "UBUSDT", "FOGOUSDT", "ARCUSDT", "WUSDT",
    "PIEVERSEUSDT", "AWEUSDT", "HOMEUSDT", "GASUSDT", "ICNTUSDT", "ZENUSDT", "XVGUSDT", "ROSEUSDT",
    "MYXUSDT", "KSMUSDT", "RSRUSDT", "ATHUSDT", "KMNOUSDT", "AKTUSDT", "ZORAUSDT", "ESPUSDT",
    "TOSHIUSDT", "STGUSDT", "ZILUSDT", "LYNUSDT", "APEUSDT", "KAITOUSDT", "FORMUSDT", "AZTECUSDT",
    "QUSDT", "MOVEUSDT", "MINAUSDT", "SOONUSDT", "TUSDT", "BRETTUSDT", "ACHUSDT", "TURBOUSDT",
    "NXPCUSDT", "ALCHUSDT", "ZETAUSDT", "MOCAUSDT", "CYSUSDT", "ASTRUSDT", "ENSOUSDT", "AXLUSDT",
    "UAIUSDT", "VTHOUSDT", "RAVEUSDT", "NMRUSDT", "COAIUSDT", "GWEIUSDT", "MEUSDT", "ORCAUSDT",
    "BLURUSDT", "MERLUSDT", "MOODENGUSDT", "BIOUSDT", "SOMIUSDT", "B2USDT", "ORDIUSDT", "SPKUSDT",
    "ZAMAUSDT", "PARTIUSDT", "1000RATSUSDT", "SSVUSDT", "BIRBUSDT", "POPCATUSDT", "GUNUSDT", "BEATUSDT",
    "BANANAS31USDT", "LAUSDT", "LINEAUSDT", "DRIFTUSDT", "AVNTUSDT", "GRASSUSDT", "GPSUSDT", "PNUTUSDT",
    "CELOUSDT", "LUNAUSDT", "VANAUSDT", "TRIAUSDT", "IOTXUSDT", "POLYXUSDT", "ANKRUSDT", "SAHARAUSDT",
    "RPLUSDT", "MASKUSDT", "UMAUSDT", "TAGUSDT", "USELESSUSDT", "MEMEUSDT", "ATUSDT", "KGENUSDT",
    "SKYAIUSDT", "ONTUSDT", "ENJUSDT", "SIGNUSDT", "CTKUSDT", "NOTUSDT", "CYBERUSDT", "GMTUSDT",
    "FIDAUSDT", "CROSSUSDT", "STEEMUSDT", "LABUSDT", "BREVUSDT", "AUCTIONUSDT", "HOLOUSDT", "PEOPLEUSDT",
    "CVCUSDT", "IOUSDT", "BROCCOLIUSDT", "SXTUSDT", "CLANKERUSDT", "BIGTIMEUSDT", "BLASTUSDT", "THEUSDT",
    "XPINUSDT", "MANTAUSDT", "YGGUSDT", "WAXPUSDT", "ONGUSDT", "LAYERUSDT", "ANIMEUSDT", "BOMEUSDT",
    "C98USDT", "API3USDT", "AGLDUSDT", "MMTUSDT", "INXUSDT", "GIGGLEUSDT", "IDOLUSDT", "ARKMUSDT",
    "RESOLVUSDT", "EULUSDT", "METISUSDT", "SONICUSDT", "TNSRUSDT", "PROMUSDT", "SAPIENUSDT", "VELVETUSDT",
    "FLOCKUSDT", "BANKUSDT", "ALLOUSDT", "USUALUSDT", "SLPUSDT", "ARIAUSDT", "MIRAUSDT", "MAGICUSDT",
    "ZKCUSDT", "INUSDT", "NAORISUSDT", "MAGMAUSDT", "REZUSDT", "WCTUSDT", "FUSDT", "ELSAUSDT",
    "SPACEUSDT", "APRUSDT", "AIXBTUSDT", "GOATUSDT", "DENTUSDT", "JCTUSDT", "XAIUSDT", "AIOUSDT",
    "ZKPUSDT", "VINEUSDT", "METAUSDT", "FIGHTUSDT", "INITUSDT", "BASUSDT", "NEWTUSDT", "FUNUSDT",
    "FOLKSUSDT", "ARPAUSDT", "MOVRUSDT", "MUBARAKUSDT", "NOMUSDT", "ACTUSDT", "ZKJUSDT", "VANRYUSDT",
    "AINUSDT", "RECALLUSDT", "MAVUSDT", "CLOUSDT", "LIGHTUSDT", "TOWNSUSDT", "BLESSUSDT", "HAEDALUSDT",
    "4USDT", "USUSDT", "HEIUSDT", "OGUSDT",
}

GRAN_MAP = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}

BITGET_BASE    = "https://api.bitget.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_cache         = {}

EXCLUDED_KEYWORDS = ["XAU","PAXG","BTC","ETH","USDC","DAI","BUSD","UST","LUNC","LUNA"]

# ══════════════════════════════════════════════════════════════
#  🔒  COOLDOWN & SNAPSHOTS (sama seperti sebelumnya)
# ══════════════════════════════════════════════════════════════
def load_cooldown():
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items()
                    if now - v < CONFIG["alert_cooldown_sec"]}
    except:
        pass
    return {}

def save_cooldown(state):
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except:
        pass

def load_funding_snapshots():
    try:
        if os.path.exists(CONFIG["funding_snapshot_file"]):
            with open(CONFIG["funding_snapshot_file"]) as f:
                return json.load(f)
    except:
        pass
    return {}

def save_funding_snapshot(symbol, funding_rate):
    snaps = load_funding_snapshots()
    now = time.time()
    if symbol not in snaps:
        snaps[symbol] = []
    snaps[symbol].append({"ts": now, "funding": funding_rate})
    snaps[symbol] = sorted(snaps[symbol], key=lambda x: x["ts"])[-20:]
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(snaps, f)
    except:
        pass

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym):
    return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym):
    _cooldown[sym] = time.time()
    save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════
#  🌐  HTTP UTILITIES (sama)
# ══════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=12):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log.warning("Rate limit — tunggu 15s")
                time.sleep(15)
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=15,
        )
        return r.status_code == 200
    except:
        return False

def utc_now():  return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
def utc_hour(): return datetime.now(timezone.utc).hour

# ══════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS (ditambah pengambilan candle 4h & 1d)
# ══════════════════════════════════════════════════════════════
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
        params={"symbol": symbol, "granularity": g,
                "limit": str(limit), "productType": "usdt-futures"},
    )
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for c in data.get("data", []):
        try:
            vol_usd = float(c[6]) if len(c) > 6 else float(c[5]) * float(c[4])
            candles.append({
                "ts":         int(c[0]),
                "open":     float(c[1]),
                "high":     float(c[2]),
                "low":      float(c[3]),
                "close":    float(c[4]),
                "volume":   float(c[5]),
                "volume_usd": vol_usd,
            })
        except:
            continue
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
            return float(data["data"][0].get("fundingRate", 0))
        except:
            pass
    return 0

def get_funding_stats(symbol, current_funding):
    snaps = load_funding_snapshots().get(symbol, [])
    all_rates = [s["funding"] for s in snaps] + [current_funding]
    if len(all_rates) < 2:
        return None
    last6 = all_rates[-6:]
    avg6 = sum(last6) / len(last6)
    cumul = sum(last6)
    neg_pct = sum(1 for f in last6 if f < 0) / len(last6) * 100
    streak = 0
    for f in reversed(last6):
        if f < 0:
            streak += 1
        else:
            break
    basis = current_funding * 100
    return {
        "avg": avg6,
        "cumulative": cumul,
        "neg_pct": neg_pct,
        "streak": streak,
        "basis": basis,
        "current": current_funding
    }

# ── Fungsi pendukung indikator (sama, tapi beberapa disesuaikan)
def calc_bbw(candles, period=20):
    if len(candles) < period:
        return 0, 0.5
    closes = [c["close"] for c in candles[-period:]]
    mean = sum(closes) / period
    std = math.sqrt(sum((x - mean)**2 for x in closes) / period)
    bb_upper = mean + 2*std
    bb_lower = mean - 2*std
    bbw = (bb_upper - bb_lower) / mean * 100 if mean > 0 else 0
    last = candles[-1]["close"]
    if bb_upper - bb_lower == 0:
        bb_pct = 0.5
    else:
        bb_pct = (last - bb_lower) / (bb_upper - bb_lower)
    return bbw, bb_pct

def calc_atr_pct(candles, period=14):
    if len(candles) < period + 1:
        return 0
    trs = []
    for i in range(1, period+1):
        h = candles[-i]["high"]
        l = candles[-i]["low"]
        pc = candles[-i-1]["close"] if i < len(candles) else candles[-i]["open"]
        tr = max(h-l, abs(h-pc), abs(l-pc))
        trs.append(tr)
    atr = sum(trs) / period
    cur = candles[-1]["close"]
    return atr / cur * 100 if cur > 0 else 0

def calc_vwap(candles):
    if len(candles) < 24:
        return candles[-1]["close"]
    cum_tv = 0
    cum_v = 0
    for c in candles[-24:]:
        tp = (c["high"] + c["low"] + c["close"]) / 3
        cum_tv += tp * c["volume"]
        cum_v += c["volume"]
    return cum_tv / cum_v if cum_v > 0 else candles[-1]["close"]

def detect_bos_up(candles):
    if len(candles) < 3:
        return False
    return candles[-1]["close"] > max(c["high"] for c in candles[-3:-1])

def higher_low_detected(candles):
    if len(candles) < 6:
        return False
    lows = [c["low"] for c in candles[-6:]]
    return lows[-1] > min(lows[:-1])

def get_rsi(candles, period=14):
    if len(candles) < period + 1:
        return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period-1) + gains[i]) / period
        avg_l = (avg_l * (period-1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))

def calc_macd(candles, fast=12, slow=26, signal=9):
    if len(candles) < slow + signal:
        return 0
    closes = [c["close"] for c in candles]
    # Sederhana: gunakan EMA
    def ema(period, index):
        if index < period - 1:
            return closes[index]
        alpha = 2 / (period + 1)
        ema_val = sum(closes[index-period+1:index+1]) / period
        for i in range(index - period + 1, index + 1):
            ema_val = alpha * closes[i] + (1 - alpha) * ema_val
        return ema_val
    macd_line = ema(fast, -1) - ema(slow, -1)
    # hitung signal line sebagai EMA dari MACD line selama signal periode
    macd_vals = [ema(fast, i) - ema(slow, i) for i in range(-signal, 0)]
    signal_line = sum(macd_vals) / signal
    hist = macd_line - signal_line
    return hist

def get_rank(symbol):
    # Placeholder
    return 0

def get_ath_distance(symbol, cur_price):
    # Placeholder
    return -95.0

# ==================== FUNGSI UNTUK FIBONACCI TARGET (sama) ====================
def find_swing_low_high(candles_1h, lookback=48):
    if len(candles_1h) < lookback:
        lookback = len(candles_1h)
    recent = candles_1h[-lookback:]
    low_idx = min(range(len(recent)), key=lambda i: recent[i]["low"])
    high_idx = max(range(len(recent)), key=lambda i: recent[i]["high"])
    if low_idx < high_idx:
        swing_low = recent[low_idx]["low"]
        swing_high = recent[high_idx]["high"]
    else:
        swing_low = min(c["low"] for c in recent)
        swing_high = max(c["high"] for c in recent)
    return swing_low, swing_high

def calc_fib_targets(entry, candles_1h):
    swing_low, swing_high = find_swing_low_high(candles_1h)
    fib_range = swing_high - swing_low
    if fib_range <= 0:
        return entry * 1.08, entry * 1.15
    t1 = swing_low + fib_range * 1.272
    t2 = swing_low + fib_range * 1.618
    if t1 < entry:
        t1 = entry * 1.08
    if t2 < t1:
        t2 = t1 * 1.08
    return round(t1, 8), round(t2, 8)

# ==================== FUNGSI ENTRY BARU (menerima support level dari luar) ====================
def calc_entry_from_support(current_price, support_level, candles_1h):
    """
    Menghitung entry, SL, dan target berdasarkan support level yang diberikan.
    """
    entry = support_level  # entry tepat di support
    entry_range = (support_level, support_level * (1 + CONFIG["entry_range_above"]))

    # Stop loss di bawah support terdekat (low 5h)
    low_5h = min(c["low"] for c in candles_1h[-5:])
    sl = min(entry * 0.98, low_5h * 0.995)

    # Target Fibonacci
    t1, t2 = calc_fib_targets(entry, candles_1h)

    risk = entry - sl
    reward = t1 - entry
    rr = round(reward / risk, 1) if risk > 0 else 0

    return {
        "cur": current_price,
        "entry": round(entry, 8),
        "entry_range": (round(entry_range[0], 8), round(entry_range[1], 8)),
        "sl": round(sl, 8),
        "sl_pct": round((entry - sl) / entry * 100, 1),
        "t1": t1,
        "t2": t2,
        "rr": rr,
        "liq_pct": round((t1 - current_price) / current_price * 100, 1),
        "support_used": round(support_level, 8),
    }

# ==================== FUNGSI BARU UNTUK SUPPORT STRENGTH ====================
def candles_to_df(candles):
    """Konversi list candle ke pandas DataFrame."""
    df = pd.DataFrame(candles)
    # Pastikan kolom yang diperlukan ada
    required = ['open', 'high', 'low', 'close', 'volume', 'volume_usd']
    for col in required:
        if col not in df.columns:
            df[col] = 0
    return df

def count_touches(df, level, tolerance=0.003, lookback=48):
    """
    Menghitung berapa kali level disentuh dalam lookback period.
    Hanya dihitung jika low berada dalam tolerance dan merupakan swing low.
    """
    lower_bound = level * (1 - tolerance)
    upper_bound = level * (1 + tolerance)
    touches = 0
    n = len(df)
    start = max(0, n - lookback)
    for i in range(start, n):
        low = df.iloc[i]['low']
        if lower_bound <= low <= upper_bound:
            # Cek apakah ini swing low (lebih rendah dari tetangga)
            if i > start and i < n-1:
                prev_low = df.iloc[i-1]['low']
                next_low = df.iloc[i+1]['low']
                if low <= prev_low and low <= next_low:
                    touches += 1
    return touches

def find_touch_indices(df, level, tolerance=0.003, lookback=48):
    """Mengembalikan indeks candle yang low-nya menyentuh level (dalam tolerance)."""
    lower_bound = level * (1 - tolerance)
    upper_bound = level * (1 + tolerance)
    indices = []
    n = len(df)
    start = max(0, n - lookback)
    for i in range(start, n):
        low = df.iloc[i]['low']
        if lower_bound <= low <= upper_bound:
            indices.append(i)
    return indices

def analyze_candle_patterns(df_recent, support_level):
    """
    Analisis pola candlestick di area support (5 candle terakhir).
    Mengembalikan skor 0-100.
    """
    score = 0
    for i in range(len(df_recent)):
        row = df_recent.iloc[i]
        body = abs(row['close'] - row['open'])
        lower_shadow = min(row['open'], row['close']) - row['low']
        upper_shadow = row['high'] - max(row['open'], row['close'])

        # Hammer pattern
        if lower_shadow > body * 2 and upper_shadow < body * 0.5:
            if abs(row['low'] - support_level) / support_level < 0.005:
                score = 100
                break

        # Bullish engulfing
        if i > 0:
            prev = df_recent.iloc[i-1]
            if (prev['close'] < prev['open'] and  # Prev bearish
                row['close'] > row['open'] and     # Current bullish
                row['open'] <= prev['close'] and
                row['close'] > prev['open']):
                score = max(score, 90)

        # Long lower shadow (rejection)
        if lower_shadow > body * 1.5:
            score = max(score, 70)
    return score

def analyze_trend_structure(df_1h, df_4h):
    """
    Analisis struktur trend berdasarkan higher lows di 1H dan EMA di 4H.
    Skor 0-100.
    """
    # Check higher lows in last 20 candles of 1H
    lows_1h = df_1h['low'].tail(20).values
    higher_lows = sum(1 for i in range(1, len(lows_1h)) if lows_1h[i] > lows_1h[i-1])

    # Check EMA alignment in 4H
    ema20_4h = ta.ema(df_4h['close'], length=20).iloc[-1] if len(df_4h) >= 20 else None
    ema50_4h = ta.ema(df_4h['close'], length=50).iloc[-1] if len(df_4h) >= 50 else None

    score = 0
    if higher_lows >= 10:  # More than 50% higher lows
        score += 50
    if ema20_4h and ema50_4h and ema20_4h > ema50_4h:
        score += 50
    return score

def check_htf_alignment(support_1h, df_4h, df_1d, atr_1h):
    """
    Cek apakah support 1h dekat dengan support di 4h dan 1d.
    Skor 0-100.
    """
    score = 0
    # Cari support level di 4H (menggunakan low terbaru sebagai proxy sederhana)
    if len(df_4h) >= 10:
        support_4h_candidates = df_4h['low'].tail(10).min()  # simple: low terendah 10 candle
        # Bisa juga menggunakan swing low, tapi sederhanakan dulu
        if abs(support_1h - support_4h_candidates) / support_1h < 0.005:
            score += 50

    if len(df_1d) >= 5:
        support_1d_candidates = df_1d['low'].tail(5).min()
        if abs(support_1h - support_1d_candidates) / support_1h < 0.01:
            score += 50
    return score

def calculate_support_strength(df_1h, df_4h, df_1d, current_price):
    """
    Menghitung kekuatan support (0-100) berdasarkan:
    - jarak harga ke support terdekat (ATR-based)
    - jumlah sentuhan
    - volume pada sentuhan
    - pola candlestick
    - struktur trend
    - alignment timeframe lebih tinggi
    """
    score = 0
    details = {}

    # 1. ATR untuk jarak maksimal
    atr_14 = ta.atr(df_1h['high'], df_1h['low'], df_1h['close'], length=14).iloc[-1]
    max_distance = min(CONFIG["support"]["max_distance_atr_multiplier"] * atr_14 / current_price,
                       CONFIG["support"]["max_distance_cap"])

    # 2. Cari support terdekat dari level yang ada (gunakan low 3h, VWAP, EMA20 seperti sebelumnya)
    # Kita gunakan fungsi get_support_levels yang sudah ada, tapi perlu diadaptasi untuk DataFrame
    # Sederhana: gunakan low 3h sebagai proxy
    if len(df_1h) >= 3:
        low_3h = df_1h['low'].tail(3).min()
    else:
        low_3h = current_price * 0.985
    support_candidates = [low_3h]
    if len(df_1h) >= 24:
        vwap = calc_vwap_from_df(df_1h)  # perlu fungsi
        if vwap < current_price:
            support_candidates.append(vwap)
    if len(df_1h) >= 20:
        ema20 = df_1h['close'].tail(20).mean()
        if ema20 < current_price:
            support_candidates.append(ema20)

    # Ambil support tertinggi di bawah harga
    valid_supports = [s for s in support_candidates if s < current_price]
    if valid_supports:
        nearest_support = max(valid_supports)
    else:
        nearest_support = current_price * 0.985

    min_distance = (current_price - nearest_support) / current_price

    # 3. Cek apakah dalam jarak yang diperbolehkan
    if min_distance > max_distance:
        return {'score': 0, 'valid': False, 'reason': f'Too far from support ({min_distance*100:.2f}% > {max_distance*100:.2f}%)'}

    # 4. Touch Count Analysis
    touch_count = count_touches(df_1h, nearest_support, tolerance=CONFIG["support"]["touch_tolerance"], lookback=48)
    touch_score = min(touch_count * 30, 100)  # 2x=60, 3x+=100
    score += touch_score * 0.25
    details['touch_count'] = touch_count

    # 5. Volume Analysis
    avg_volume = df_1h['volume'].rolling(20).mean().iloc[-1]
    recent_volume = df_1h['volume'].iloc[-5:].mean()
    touches_indices = find_touch_indices(df_1h, nearest_support, tolerance=CONFIG["support"]["touch_tolerance"], lookback=48)
    if touches_indices:
        volume_at_touches = df_1h.iloc[touches_indices]['volume'].mean()
    else:
        volume_at_touches = 0

    volume_score = 0
    if volume_at_touches > avg_volume * CONFIG["volume"]["min_touch_volume_ratio"]:
        volume_score = 80
    if volume_at_touches > avg_volume * CONFIG["volume"]["confirmation_volume_ratio"]:
        volume_score = 100
    if recent_volume > avg_volume * 1.3:
        volume_score += 10
    score += min(volume_score, 100) * 0.25
    details['volume_score'] = volume_score

    # 6. Candle Pattern Analysis
    pattern_score = analyze_candle_patterns(df_1h.tail(5), nearest_support)
    score += pattern_score * 0.15
    details['pattern_score'] = pattern_score

    # 7. Trend Structure
    trend_score = analyze_trend_structure(df_1h, df_4h)
    score += trend_score * 0.20
    details['trend_score'] = trend_score

    # 8. Higher TF Alignment
    alignment_score = check_htf_alignment(nearest_support, df_4h, df_1d, atr_14)
    score += alignment_score * 0.15
    details['htf_alignment'] = alignment_score

    # 9. OBV & CMF (bonus)
    try:
        obv = ta.obv(df_1h['close'], df_1h['volume'])
        obv_slope = (obv.iloc[-1] - obv.iloc[-10]) / obv.iloc[-10] * 100 if obv.iloc[-10] != 0 else 0
        if obv_slope > 0:
            score += 5
            details['obv_bullish'] = True
    except:
        pass

    try:
        cmf = ta.cmf(df_1h['high'], df_1h['low'], df_1h['close'], df_1h['volume'], length=20)
        if cmf.iloc[-1] > 0.05:
            score += 5
            details['cmf_positive'] = True
    except:
        pass

    final_score = min(score, 100)

    return {
        'score': final_score,
        'valid': final_score >= CONFIG["scoring"]["min_support_score"],
        'support_level': nearest_support,
        'distance_pct': min_distance * 100,
        'details': details
    }

def calc_vwap_from_df(df):
    """Menghitung VWAP dari DataFrame (24 periode terakhir)."""
    if len(df) < 24:
        return df['close'].iloc[-1]
    recent = df.tail(24)
    tp = (recent['high'] + recent['low'] + recent['close']) / 3
    cum_tv = (tp * recent['volume']).sum()
    cum_v = recent['volume'].sum()
    return cum_tv / cum_v if cum_v > 0 else recent['close'].iloc[-1]

# ==================== FUNGSI SKOR TEKNIKAL (TANPA FUNDING) ====================
def calculate_technical_score(candles_1h, ticker):
    """
    Menghitung skor teknikal (0-100) berdasarkan indikator selain funding.
    Menggunakan bobot yang sama seperti sebelumnya, tetapi dinormalisasi.
    """
    score = 0
    signals = []

    bbw, bb_pct = calc_bbw(candles_1h)
    if len(candles_1h) >= 2:
        price_chg = (candles_1h[-1]["close"] - candles_1h[-2]["close"]) / candles_1h[-2]["close"] * 100
    else:
        price_chg = 0
    atr_pct = calc_atr_pct(candles_1h)
    rsi = get_rsi(candles_1h[-48:])
    vwap = calc_vwap(candles_1h)
    above_vwap_rate = 0
    bos_up = False
    higher_low = False
    if len(candles_1h) >= 6:
        recent = candles_1h[-6:]
        above = sum(1 for c in recent if c["close"] > vwap)
        above_vwap_rate = above / len(recent)
        bos_up = detect_bos_up(candles_1h)
        higher_low = higher_low_detected(candles_1h)

    # Volume ratio 24h
    if len(candles_1h) >= 24:
        avg_vol_24h = sum(c["volume_usd"] for c in candles_1h[-24:]) / 24
        vol_ratio = candles_1h[-1]["volume_usd"] / avg_vol_24h if avg_vol_24h > 0 else 0
    else:
        vol_ratio = 0

    # Volume acceleration
    if len(candles_1h) >= 4:
        vol_1h = candles_1h[-1]["volume_usd"]
        vol_3h = sum(c["volume_usd"] for c in candles_1h[-4:-1]) / 3
        vol_accel = (vol_1h - vol_3h) / vol_3h if vol_3h > 0 else 0
    else:
        vol_accel = 0

    macd_hist = calc_macd(candles_1h)

    # Bobot (tanpa funding)
    if bbw >= 0.12:
        score += CONFIG["score_bbw_12"]
        signals.append(f"BBW {bbw:.2f}% (ekstrem)")
    elif bbw >= 0.10:
        score += CONFIG["score_bbw_10"]
        signals.append(f"BBW {bbw:.2f}% (tinggi)")
    elif bbw >= 0.08:
        score += CONFIG["score_bbw_8"]
        signals.append(f"BBW {bbw:.2f}% (sedang)")

    if price_chg >= 2.0:
        score += CONFIG["score_price_2"]
        signals.append(f"Price +{price_chg:.1f}% (spike)")
    elif price_chg >= 1.0:
        score += CONFIG["score_price_1"]
        signals.append(f"Price +{price_chg:.1f}% (naik)")
    elif price_chg >= 0.5:
        score += CONFIG["score_price_05"]
        signals.append(f"Price +{price_chg:.1f}% (sedang)")

    if above_vwap_rate > CONFIG["above_vwap_rate_min"] and bos_up:
        score += CONFIG["score_above_vwap_bos"]
        signals.append("Above VWAP + Break of Structure")
    elif above_vwap_rate > CONFIG["above_vwap_rate_min"]:
        score += CONFIG["score_above_vwap"]
        signals.append("Above VWAP dominan")

    if rsi >= 65:
        score += CONFIG["score_rsi_65"]
        signals.append(f"RSI {rsi:.1f} (overbought kuat)")
    elif rsi >= 55:
        score += CONFIG["score_rsi_55"]
        signals.append(f"RSI {rsi:.1f} (bullish)")

    if atr_pct >= 1.5:
        score += CONFIG["score_atr_15"]
        signals.append(f"ATR {atr_pct:.2f}% (volatilitas tinggi)")
    elif atr_pct >= 1.0:
        score += CONFIG["score_atr_10"]
        signals.append(f"ATR {atr_pct:.2f}% (volatilitas sedang)")

    # Rank & ATH (placeholder, tidak dihitung)
    # rank = get_rank(symbol)
    # if rank >= 200:
    #     score += CONFIG["score_lowcap"]
    #     signals.append("Low cap")
    # ath_dist = get_ath_distance(symbol, price_now)
    # if ath_dist <= -90:
    #     score += CONFIG["score_ath_dist"]
    #     signals.append("Deep from ATH")

    if vol_ratio > CONFIG["vol_ratio_threshold"]:
        score += CONFIG["score_vol_ratio_24h"]
        signals.append(f"Volume ratio {vol_ratio:.1f}x (tinggi)")

    if vol_accel > CONFIG["vol_accel_threshold"]:
        score += CONFIG["score_vol_accel"]
        signals.append(f"Volume acceleration {vol_accel*100:.0f}%")

    if macd_hist > 0:
        score += CONFIG["score_macd_pos"]
        signals.append("MACD histogram positif")

    # Normalisasi ke 0-100 berdasarkan maksimum teoritis (estimasi)
    max_tech_score = 50  # estimasi kasar, bisa dihitung lebih teliti
    normalized = min(score / max_tech_score * 100, 100)

    return normalized, signals

def calculate_funding_score(funding_stats):
    """Menghitung skor funding (0-100) berdasarkan stats."""
    if not funding_stats:
        return 0, []
    score = 0
    signals = []
    if funding_stats["neg_pct"] >= 70:
        score += CONFIG["score_funding_neg_pct"]
        signals.append(f"Funding negatif {funding_stats['neg_pct']:.0f}%")
    if funding_stats["streak"] >= 10:
        score += CONFIG["score_funding_streak"]
        signals.append(f"Funding streak negatif {funding_stats['streak']}")
    if funding_stats["basis"] <= -0.15:
        score += CONFIG["score_basis"]
        signals.append(f"Basis {funding_stats['basis']:.2f}% (diskonto)")
    max_funding_score = CONFIG["score_funding_neg_pct"] + CONFIG["score_funding_streak"] + CONFIG["score_basis"]
    normalized = min(score / max_funding_score * 100, 100) if max_funding_score > 0 else 0
    return normalized, signals

# ==================== MASTER SCORE YANG DIMODIFIKASI ====================
def master_score(symbol, ticker):
    # Ambil candle untuk berbagai timeframe
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    c4h = get_candles(symbol, "4h", CONFIG["candle_4h"])
    c1d = get_candles(symbol, "1d", CONFIG["candle_1d"])

    if len(c1h) < 48 or len(c4h) < 20 or len(c1d) < 5:
        log.info(f"  {symbol}: Data candle tidak mencukupi")
        return None

    try:
        vol_24h = float(ticker.get("quoteVolume", 0))
        chg_24h = float(ticker.get("change24h", 0)) * 100
        price_now = float(ticker.get("lastPr", 0)) or c1h[-1]["close"]
    except:
        return None

    if vol_24h < CONFIG["min_vol_24h"]:
        return None

    # Funding gate (tetap wajib)
    funding = get_funding(symbol)
    save_funding_snapshot(symbol, funding)
    fstats = get_funding_stats(symbol, funding)
    if not fstats:
        log.info(f"  {symbol}: Data funding belum cukup")
        return None
    if not (fstats["avg"] < CONFIG["funding_gate_avg"] or fstats["cumulative"] < CONFIG["funding_gate_cumul"]):
        log.info(f"  {symbol}: Funding tidak cukup negatif")
        return None

    # Konversi candle ke DataFrame
    df_1h = candles_to_df(c1h)
    df_4h = candles_to_df(c4h)
    df_1d = candles_to_df(c1d)

    # Analisis support strength
    support_analysis = calculate_support_strength(df_1h, df_4h, df_1d, price_now)
    if not support_analysis['valid']:
        log.info(f"  {symbol}: Support tidak kuat ({support_analysis.get('reason', 'skor < threshold')})")
        return None

    # Skor teknikal (tanpa funding)
    tech_score, tech_signals = calculate_technical_score(c1h, ticker)

    # Skor funding
    funding_score, funding_signals = calculate_funding_score(fstats)

    # Composite score
    composite = (support_analysis['score'] * CONFIG["scoring"]["support_weight"] +
                 tech_score * CONFIG["scoring"]["tech_weight"] +
                 funding_score * CONFIG["scoring"]["funding_weight"])

    if composite < CONFIG["scoring"]["composite_threshold"]:
        log.info(f"  {symbol}: Composite score {composite:.1f} < {CONFIG['scoring']['composite_threshold']}")
        return None

    # Hitung entry berdasarkan support level yang ditemukan
    entry_data = calc_entry_from_support(price_now, support_analysis['support_level'], c1h)

    # Gabungkan semua sinyal
    all_signals = tech_signals + funding_signals
    if support_analysis['score'] >= CONFIG["scoring"]["strong_support_threshold"]:
        all_signals.append(f"Support sangat kuat ({support_analysis['score']:.0f})")
    else:
        all_signals.append(f"Support kuat ({support_analysis['score']:.0f})")

    # Tipe pump (bisa disesuaikan)
    pump_type = "Support Bounce"

    # Potensi gain
    potential_gain_t1 = (entry_data["t1"] - price_now) / price_now * 100
    potential_gain_t2 = (entry_data["t2"] - price_now) / price_now * 100

    return {
        "symbol": symbol,
        "composite_score": round(composite, 1),
        "support_score": support_analysis['score'],
        "tech_score": round(tech_score, 1),
        "funding_score": round(funding_score, 1),
        "signals": all_signals,
        "entry": entry_data,
        "price": price_now,
        "chg_24h": chg_24h,
        "vol_24h": vol_24h,
        "rsi": round(get_rsi(c1h[-48:]), 1),
        "bbw": round(calc_bbw(c1h)[0], 2),
        "bb_pct": round(calc_bbw(c1h)[1], 2),
        "above_vwap_rate": round(sum(1 for c in c1h[-6:] if c["close"] > calc_vwap(c1h)) / 6 * 100, 1),
        "funding_stats": fstats,
        "pump_type": pump_type,
        "vol_ratio": round((c1h[-1]["volume_usd"] / (sum(c["volume_usd"] for c in c1h[-24:])/24)) if len(c1h)>=24 else 0, 2),
        "vol_accel": round((c1h[-1]["volume_usd"] - (sum(c["volume_usd"] for c in c1h[-4:-1])/3)) / (sum(c["volume_usd"] for c in c1h[-4:-1])/3) * 100 if len(c1h)>=4 else 0, 1),
        "macd_hist": round(calc_macd(c1h), 6),
        "potential_gain_t1": round(potential_gain_t1, 1),
        "potential_gain_t2": round(potential_gain_t2, 1),
        "support_analysis": support_analysis,
    }

# ==================== TELEGRAM FORMATTER (dimodifikasi) ====================
def build_alert(r, rank=None):
    msg = f"🚨 <b>PRE-PUMP SIGNAL {rank} — v13.4-SUPPORT</b>\n\n"
    msg += f"<b>Symbol    :</b> {r['symbol']}\n"
    msg += f"<b>Pump Type :</b> {r['pump_type']}\n"
    msg += f"<b>Composite Score:</b> {r['composite_score']} (S:{r['support_score']} T:{r['tech_score']} F:{r['funding_score']})\n"
    msg += f"<b>Harga     :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h)\n"
    msg += f"<b>RSI 14h   :</b> {r['rsi']}\n"
    msg += f"<b>BB Width  :</b> {r['bbw']}%\n"
    msg += f"<b>BB Position:</b> {r['bb_pct']*100:.0f}%\n"
    msg += f"<b>Above VWAP:</b> {r['above_vwap_rate']}% dalam 6h\n"
    msg += f"<b>Volume    :</b> ratio 24h={r['vol_ratio']}x, accel={r['vol_accel']}%\n"
    msg += f"<b>Funding   :</b> avg={r['funding_stats']['avg']:.6f}, cumul={r['funding_stats']['cumulative']:.4f}\n"
    msg += f"  streak={r['funding_stats']['streak']}, basis={r['funding_stats']['basis']:.2f}%\n"
    msg += f"<b>MACD hist :</b> {r['macd_hist']:.6f}\n"
    msg += f"<b>Potensi Gain:</b> T1 +{r['potential_gain_t1']}% | T2 +{r['potential_gain_t2']}%\n"
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📍 <b>SUPPORT AREA</b> (skor {r['support_score']})\n"
    e = r['entry']
    msg += f"  Support   : ${e['support_used']}\n"
    msg += f"  Jarak     : {r['support_analysis']['distance_pct']:.2f}% dari harga\n"
    msg += f"  Entry     : ${e['entry']} (tepat di support)\n"
    msg += f"  Rentang   : ${e['entry_range'][0]} - ${e['entry_range'][1]} (0 - {CONFIG['entry_range_above']*100:.1f}% di atas support)\n"
    msg += f"  SL        : ${e['sl']} (-{e['sl_pct']:.1f}%)\n"
    msg += f"  T1 (Fib 1.272): ${e['t1']} (+{e['liq_pct']:.1f}%)\n"
    msg += f"  T2 (Fib 1.618): ${e['t2']}\n"
    msg += f"  R/R       : 1:{e['rr']}\n"
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n📊 <b>SINYAL</b>\n"
    for s in r['signals']:
        msg += f"  • {s}\n"
    msg += f"\n📡 {utc_now()}\n<i>⚠️ Bukan financial advice.</i>"
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES v13.4 — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        vol = (f"${r['vol_24h']/1e6:.1f}M" if r['vol_24h'] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K")
        msg += f"{i}. <b>{r['symbol']}</b> [Comp:{r['composite_score']} | S:{r['support_score']} | Gain T1:{r['potential_gain_t1']}%]\n"
        msg += f"   {vol} | RSI:{r['rsi']} | BBW:{r['bbw']}% | AboveVWAP:{r['above_vwap_rate']}% | VolRatio:{r['vol_ratio']}x\n"
    return msg

# ==================== BUILD CANDIDATE LIST (sama) ====================
def build_candidate_list(tickers):
    # ... (kode sama seperti sebelumnya, tidak diubah)
    # Untuk menghemat ruang, saya tidak menyalin ulang, tetapi asumsikan tetap sama.
    # Dalam implementasi sebenarnya, gunakan kode build_candidate_list yang sudah ada.
    pass

# ==================== MAIN SCAN (disesuaikan) ====================
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v13.4-SUPPORT — {utc_now()} ===")
    log.info("=" * 70)
    log.info("FITUR BARU: Filter support kuat + composite score")
    log.info("=" * 70)
    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return
    log.info(f"Total ticker: {len(tickers)}")
    candidates = build_candidate_list(tickers)  # panggil fungsi yang sudah ada
    results = []
    for i, (sym, t) in enumerate(candidates):
        try:
            vol = float(t.get("quoteVolume", 0))
        except:
            vol = 0
        if vol < CONFIG["min_vol_24h"]:
            log.info(f"[{i+1}] {sym} — vol ${vol:,.0f} di bawah minimum")
            continue
        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K)...")
        try:
            res = master_score(sym, t)
            if res:
                log.info(f"  Composite={res['composite_score']} | support={res['support_score']} | gain T1={res['potential_gain_t1']}%")
                results.append(res)
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")
        time.sleep(CONFIG["sleep_coins"])
    results.sort(key=lambda x: x["composite_score"], reverse=True)
    log.info(f"Lolos threshold: {len(results)} coin")
    if not results:
        log.info("Tidak ada sinyal yang memenuhi syarat saat ini")
        return
    top = results[:CONFIG["max_alerts_per_run"]]
    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)
    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"✅ Alert #{rank}: {r['symbol']} Composite={r['composite_score']} Gain T1={r['potential_gain_t1']}%")
        time.sleep(2)
    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")

# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v13.4-SUPPORT                  ║")
    log.info("║  FOKUS: Support kuat + composite scoring         ║")
    log.info("╚═══════════════════════════════════════════════════╝")
    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)
    run_scan()
