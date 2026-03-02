"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v13.4-BETA                                            ║
║                                                                          ║
║  BERDASARKAN RISET + PERBAIKAN ENTRY + FILTER BTC:                      ║
║    • Funding rate sebagai GATE WAJIB (avg_6 < -0.0001 / cumul < -0.02) ║
║    • Variabel utama: BB width, price change, VWAP, RSI, ATR            ║
║    • Tambahan: Volume Ratio >2.5x, Volume Acceleration >50%            ║
║    • MACD Histogram positif (opsional)                                  ║
║    • Entry menggunakan RENTANG (support s/d support+0.3%)               ║
║    • Target Fibonacci (1.272 dan 1.618) berbasis swing low/high        ║
║    • Menampilkan potensi gain di summary                                 ║
║    • FILTER BTC: Beta & Alpha untuk menilai korelasi dan kekuatan      ║
║                                                                          ║
║  EXPECTED RESULT:                                                        ║
║    Entry lebih sering terisi, target lebih terstruktur                  ║
║    Terhindar dari kerugian saat BTC turun drastis                       ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging
from datetime import datetime, timezone
from collections import defaultdict
import numpy as np  # for linear regression

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
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────
    "min_score_alert":          10,
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

    # ── Entry/exit ────────────────────────────────────────────
    "min_target_pct":             8.0,      # fallback jika fib gagal
    "max_sl_pct":                3.0,
    "entry_support_offset":       0.0,      # entry di support (0%)
    "entry_range_above":         0.003,     # rentang 0.3% di atas support

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
    "score_funding_neg_pct":     3,
    "score_funding_streak":      3,
    "score_basis":               2,
    "score_lowcap":              1,
    "score_ath_dist":            1,

    # ── Bobot tambahan ───────────────────────────────────────
    "score_vol_ratio_24h":       2,
    "score_vol_accel":           2,
    "score_macd_pos":            1,

    # ── Threshold tambahan ────────────────────────────────────
    "above_vwap_rate_min":       0.6,
    "squeeze_funding_cumul":    -0.05,
    "vol_ratio_threshold":       2.5,
    "vol_accel_threshold":       0.5,

    # ── Parameter Beta/Alpha ──────────────────────────────────
    "beta_lookback_hours":       24,        # periode untuk regresi
    "beta_high_threshold":       1.5,       # beta di atas ini sensitif
    "beta_low_threshold":        0.5,       # beta di bawah ini kurang sensitif
    "alpha_positive_threshold":  0.5,       # alpha > 0.5% per jam dianggap kuat
    "btc_drop_threshold":        -1.5,      # BTC turun >1.5% dalam 3 jam
}

MANUAL_EXCLUDE = set()

# ══════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin pilihan
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
#  🔒  COOLDOWN & SNAPSHOTS (funding)
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
#  🌐  HTTP UTILITIES
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
        log.error("BOT_TOKEN atau CHAT_ID tidak diset")
        return False
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
        r = requests.post(url, data=payload, timeout=15)
        log.info(f"Telegram response: {r.status_code} - {r.text}")
        return r.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False

def utc_now():  return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
def utc_hour(): return datetime.now(timezone.utc).hour

# ══════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
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

# ── Fungsi pendukung indikator ─────────────────────────────────
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

# ==================== FUNGSI UNTUK FIBONACCI TARGET ====================
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

# ==================== FUNGSI ENTRY BARU ====================
def get_support_levels(candles_1h):
    cur = candles_1h[-1]["close"]
    supports = []
    low_3h = min(c["low"] for c in candles_1h[-3:])
    supports.append(low_3h)
    if len(candles_1h) >= 24:
        vwap, _ = calc_vwap_zone(candles_1h[-24:]) if 'calc_vwap_zone' in globals() else (calc_vwap(candles_1h), None)
        if isinstance(vwap, (int, float)) and vwap < cur:
            supports.append(vwap)
    if len(candles_1h) >= 20:
        closes = [c["close"] for c in candles_1h[-20:]]
        ema20 = sum(closes) / 20
        if ema20 < cur:
            supports.append(ema20)
    valid = [s for s in supports if s < cur]
    if valid:
        return max(valid)
    else:
        return cur * 0.985

def calc_entry(candles_1h, candles_15m):
    cur = candles_1h[-1]["close"]
    support = get_support_levels(candles_1h)
    entry = support
    entry_range = (support, support * (1 + CONFIG["entry_range_above"]))
    low_5h = min(c["low"] for c in candles_1h[-5:])
    sl = min(entry * 0.98, low_5h * 0.995)
    t1, t2 = calc_fib_targets(entry, candles_1h)
    risk = entry - sl
    reward = t1 - entry
    rr = round(reward / risk, 1) if risk > 0 else 0
    return {
        "cur": cur,
        "entry": round(entry, 8),
        "entry_range": (round(entry_range[0], 8), round(entry_range[1], 8)),
        "sl": round(sl, 8),
        "sl_pct": round((entry - sl) / entry * 100, 1),
        "t1": t1,
        "t2": t2,
        "rr": rr,
        "liq_pct": round((t1 - cur) / cur * 100, 1),
        "support_used": round(support, 8),
    }

def calc_vwap_zone(candles):
    vwap = calc_vwap(candles)
    return vwap, None

# ==================== FUNGSI BARU: BETA & ALPHA ====================
def get_btc_candles(gran="1h", limit=168):
    """Ambil candle BTCUSDT."""
    return get_candles("BTCUSDT", gran, limit)

def compute_beta_alpha(coin_candles, btc_candles, lookback_hours=24):
    """
    Menghitung beta dan alpha dari regresi return coin terhadap return BTC.
    lookback_hours menentukan jumlah candle 1h yang digunakan.
    Mengembalikan (beta, alpha, r_squared, last_btc_return)
    """
    if len(coin_candles) < lookback_hours or len(btc_candles) < lookback_hours:
        return 0, 0, 0, 0

    # Ambil harga close
    coin_closes = [c["close"] for c in coin_candles[-lookback_hours:]]
    btc_closes = [c["close"] for c in btc_candles[-lookback_hours:]]

    # Hitung return persen (1h)
    coin_returns = [(coin_closes[i] - coin_closes[i-1]) / coin_closes[i-1] * 100 for i in range(1, len(coin_closes))]
    btc_returns = [(btc_closes[i] - btc_closes[i-1]) / btc_closes[i-1] * 100 for i in range(1, len(btc_closes))]

    if len(coin_returns) < 2:
        return 0, 0, 0, 0

    # Regresi linear
    x = np.array(btc_returns)
    y = np.array(coin_returns)
    A = np.vstack([x, np.ones(len(x))]).T
    beta, alpha = np.linalg.lstsq(A, y, rcond=None)[0]

    # Hitung R-squared
    residuals = y - (beta * x + alpha)
    ss_res = np.sum(residuals**2)
    ss_tot = np.sum((y - np.mean(y))**2)
    r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

    # Return BTC terakhir
    last_btc_return = btc_returns[-1] if btc_returns else 0

    return beta, alpha, r_squared, last_btc_return

def get_btc_trend(btc_candles, hours=3):
    """Menentukan tren BTC berdasarkan perubahan harga dalam hours terakhir."""
    if len(btc_candles) < hours:
        return "neutral", 0
    start = btc_candles[-hours]["close"]
    end = btc_candles[-1]["close"]
    change = (end - start) / start * 100
    if change < CONFIG["btc_drop_threshold"]:
        return "bearish", change
    elif change > -CONFIG["btc_drop_threshold"]:
        return "bullish", change
    else:
        return "neutral", change

# ==================== MODIFIKASI MASTER SCORE ====================
def master_score(symbol, ticker):
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    if len(c1h) < 48:
        return None

    try:
        vol_24h = float(ticker.get("quoteVolume", 0))
        chg_24h = float(ticker.get("change24h", 0)) * 100
        price_now = float(ticker.get("lastPr", 0)) or c1h[-1]["close"]
    except:
        return None

    if vol_24h < CONFIG["min_vol_24h"]:
        return None

    # Funding gate
    funding = get_funding(symbol)
    save_funding_snapshot(symbol, funding)
    fstats = get_funding_stats(symbol, funding)
    if not fstats:
        log.info(f"  {symbol}: Data funding belum cukup")
        return None
    if not (fstats["avg"] < CONFIG["funding_gate_avg"] or fstats["cumulative"] < CONFIG["funding_gate_cumul"]):
        log.info(f"  {symbol}: Funding tidak cukup negatif")
        return None

    # Ambil data BTC
    btc_c1h = get_btc_candles("1h", CONFIG["candle_1h"])
    if len(btc_c1h) < 48:
        log.info("  Data BTC tidak cukup")
        return None

    # Hitung beta, alpha
    beta, alpha, r_squared, last_btc_return = compute_beta_alpha(c1h, btc_c1h, CONFIG["beta_lookback_hours"])
    btc_trend, btc_change = get_btc_trend(btc_c1h, hours=3)

    # Indikator teknikal
    bbw, bb_pct = calc_bbw(c1h)
    if len(c1h) >= 2:
        price_chg = (c1h[-1]["close"] - c1h[-2]["close"]) / c1h[-2]["close"] * 100
    else:
        price_chg = 0
    atr_pct = calc_atr_pct(c1h)
    rsi = get_rsi(c1h[-48:])
    vwap = calc_vwap(c1h)
    above_vwap_rate = 0
    bos_up = False
    higher_low = False
    if len(c1h) >= 6:
        recent = c1h[-6:]
        above = sum(1 for c in recent if c["close"] > vwap)
        above_vwap_rate = above / len(recent)
        bos_up = detect_bos_up(c1h)
        higher_low = higher_low_detected(c1h)

    if len(c1h) >= 24:
        avg_vol_24h = sum(c["volume_usd"] for c in c1h[-24:]) / 24
        vol_ratio = c1h[-1]["volume_usd"] / avg_vol_24h if avg_vol_24h > 0 else 0
    else:
        vol_ratio = 0

    if len(c1h) >= 4:
        vol_1h = c1h[-1]["volume_usd"]
        vol_3h = sum(c["volume_usd"] for c in c1h[-4:-1]) / 3
        vol_accel = (vol_1h - vol_3h) / vol_3h if vol_3h > 0 else 0
    else:
        vol_accel = 0

    macd_hist = calc_macd(c1h)

    score = 0
    signals = []

    # Utama
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

    # Funding tambahan
    if fstats["neg_pct"] >= 70:
        score += CONFIG["score_funding_neg_pct"]
        signals.append(f"Funding negatif {fstats['neg_pct']:.0f}%")
    if fstats["streak"] >= 10:
        score += CONFIG["score_funding_streak"]
        signals.append(f"Funding streak negatif {fstats['streak']}")
    if fstats["basis"] <= -0.15:
        score += CONFIG["score_basis"]
        signals.append(f"Basis {fstats['basis']:.2f}% (diskonto)")

    rank = get_rank(symbol)
    if rank >= 200:
        score += CONFIG["score_lowcap"]
        signals.append("Low cap")
    ath_dist = get_ath_distance(symbol, price_now)
    if ath_dist <= -90:
        score += CONFIG["score_ath_dist"]
        signals.append("Deep from ATH")

    if vol_ratio > CONFIG["vol_ratio_threshold"]:
        score += CONFIG["score_vol_ratio_24h"]
        signals.append(f"Volume ratio {vol_ratio:.1f}x (tinggi)")

    if vol_accel > CONFIG["vol_accel_threshold"]:
        score += CONFIG["score_vol_accel"]
        signals.append(f"Volume acceleration {vol_accel*100:.0f}%")

    if macd_hist > 0:
        score += CONFIG["score_macd_pos"]
        signals.append("MACD histogram positif")

    # ===== FILTER BTC =====
    # Berdasarkan beta dan alpha serta tren BTC
    btc_penalty = 0
    btc_bonus = 0
    if btc_trend == "bearish":
        if beta > CONFIG["beta_high_threshold"]:
            # Coin sangat sensitif, akan jatuh lebih dalam
            btc_penalty = -25
            signals.append(f"🚨 BTC turun {btc_change:.1f}% & beta {beta:.2f} (sensitif) - penalti besar!")
        elif beta > 1.0:
            btc_penalty = -15
            signals.append(f"⚠️ BTC turun {btc_change:.1f}% & beta {beta:.2f} - penalti sedang")
        else:
            btc_penalty = -5
            signals.append(f"📉 BTC turun {btc_change:.1f}% - penalti ringan")

        # Jika alpha positif, bisa mengurangi penalti
        if alpha > CONFIG["alpha_positive_threshold"]:
            btc_penalty = max(btc_penalty + 10, 0)  # kurangi penalti, maks 0
            signals.append(f"✅ Alpha {alpha:.2f}% positif - mengurangi dampak BTC")
    elif btc_trend == "bullish":
        if beta > CONFIG["beta_high_threshold"]:
            btc_bonus = 15
            signals.append(f"🚀 BTC naik {btc_change:.1f}% & beta {beta:.2f} - bonus!")
        elif beta > 1.0:
            btc_bonus = 8
            signals.append(f"📈 BTC naik {btc_change:.1f}% & beta {beta:.2f} - bonus kecil")
        else:
            btc_bonus = 3
            signals.append(f"✅ BTC naik {btc_change:.1f}% - bonus")

        # Jika alpha positif, tambah bonus
        if alpha > CONFIG["alpha_positive_threshold"]:
            btc_bonus += 5
            signals.append(f"⭐ Alpha {alpha:.2f}% positif - outperforming BTC")

    # Jika alpha sangat negatif, beri penalti terpisah
    if alpha < -CONFIG["alpha_positive_threshold"]:
        btc_penalty -= 10
        signals.append(f"⚠️ Alpha {alpha:.2f}% negatif - underperforming BTC")

    score += btc_penalty + btc_bonus

    # Simpan info beta/alpha untuk ditampilkan
    beta_alpha_info = {
        "beta": round(beta, 2),
        "alpha": round(alpha, 2),
        "r_squared": round(r_squared, 2),
        "btc_trend": btc_trend,
        "btc_change": round(btc_change, 1)
    }

    # Tipe pump
    pump_type = "unknown"
    if above_vwap_rate > CONFIG["above_vwap_rate_min"] and bb_pct > 0.4 and rsi > 45:
        pump_type = "Momentum Breakout (Tipe A)"
    elif above_vwap_rate < 0.2 and fstats["cumulative"] < CONFIG["squeeze_funding_cumul"] and higher_low:
        pump_type = "Short Squeeze (Tipe B)"

    entry_data = calc_entry(c1h, c15m)
    potential_gain_t1 = (entry_data["t1"] - price_now) / price_now * 100
    potential_gain_t2 = (entry_data["t2"] - price_now) / price_now * 100

    if score >= CONFIG["min_score_alert"]:
        return {
            "symbol": symbol,
            "score": score,
            "signals": signals,
            "entry": entry_data,
            "price": price_now,
            "chg_24h": chg_24h,
            "vol_24h": vol_24h,
            "rsi": round(rsi, 1),
            "bbw": round(bbw, 2),
            "bb_pct": round(bb_pct, 2),
            "above_vwap_rate": round(above_vwap_rate*100, 1),
            "funding_stats": fstats,
            "pump_type": pump_type,
            "vol_ratio": round(vol_ratio, 2),
            "vol_accel": round(vol_accel*100, 1),
            "macd_hist": round(macd_hist, 6),
            "potential_gain_t1": round(potential_gain_t1, 1),
            "potential_gain_t2": round(potential_gain_t2, 1),
            "beta_alpha": beta_alpha_info,
        }
    else:
        log.info(f"  {symbol}: Skor {score} < {CONFIG['min_score_alert']}")
        return None

# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER (modifikasi untuk menampilkan beta/alpha)
# ══════════════════════════════════════════════════════════════
def build_alert(r, rank=None):
    msg = f"🚨 <b>PRE-PUMP SIGNAL {rank} — v13.4-BETA</b>\n\n"
    msg += f"<b>Symbol    :</b> {r['symbol']}\n"
    msg += f"<b>Pump Type :</b> {r['pump_type']}\n"
    msg += f"<b>Score     :</b> {r['score']}\n"
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
    # Tambahkan info beta/alpha
    ba = r['beta_alpha']
    msg += f"<b>BTC      :</b> {ba['btc_trend'].upper()} {ba['btc_change']:+.1f}% (3h)\n"
    msg += f"<b>Beta     :</b> {ba['beta']} | <b>Alpha    :</b> {ba['alpha']:+.2f}% | R²={ba['r_squared']}\n"
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📍 <b>ENTRY ZONE (RENTANG)</b>\n"
    e = r['entry']
    msg += f"  Support  : ${e['support_used']}\n"
    msg += f"  Entry    : ${e['entry']} (tepat di support)\n"
    msg += f"  Rentang  : ${e['entry_range'][0]} - ${e['entry_range'][1]} (0 - {CONFIG['entry_range_above']*100:.1f}% di atas support)\n"
    msg += f"  SL       : ${e['sl']} (-{e['sl_pct']:.1f}%)\n"
    msg += f"  T1 (Fib 1.272): ${e['t1']} (+{e['liq_pct']:.1f}%)\n"
    msg += f"  T2 (Fib 1.618): ${e['t2']}\n"
    msg += f"  R/R      : 1:{e['rr']}\n"
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n📊 <b>SINYAL</b>\n"
    for s in r['signals']:
        msg += f"  • {s}\n"
    msg += f"\n📡 {utc_now()}\n<i>⚠️ Bukan financial advice.</i>"
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES v13.4 — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        vol = (f"${r['vol_24h']/1e6:.1f}M" if r['vol_24h'] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K")
        msg += f"{i}. <b>{r['symbol']}</b> [Score:{r['score']} | Gain T1:{r['potential_gain_t1']}% | Beta:{r['beta_alpha']['beta']} | Alpha:{r['beta_alpha']['alpha']:+.2f}]\n"
        msg += f"   {vol} | RSI:{r['rsi']} | BBW:{r['bbw']}% | AboveVWAP:{r['above_vwap_rate']}% | VolRatio:{r['vol_ratio']}x\n"
    return msg

# ══════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    all_candidates = []
    not_found = []
    filtered_stats = {
        "cooldown": 0,
        "manual_exclude": 0,
        "vol_too_low": 0,
        "vol_too_high": 0,
        "change_extreme": 0,
        "invalid_price": 0,
        "parse_error": 0,
    }
    log.info("=" * 70)
    log.info("🔍 SCANNING MODE: FULL WHITELIST (ALL 324 COINS)")
    log.info("=" * 70)
    for sym in WHITELIST_SYMBOLS:
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
            chg   = float(ticker.get("change24h", 0)) * 100
            price = float(ticker.get("lastPr", 0))
        except:
            filtered_stats["parse_error"] += 1
            continue
        if vol < CONFIG["pre_filter_vol"]:
            filtered_stats["vol_too_low"] += 1
            continue
        if vol > CONFIG["max_vol_24h"]:
            filtered_stats["vol_too_high"] += 1
            continue
        if abs(chg) > CONFIG["gate_chg_24h_max"]:
            filtered_stats["change_extreme"] += 1
            continue
        if price <= 0:
            filtered_stats["invalid_price"] += 1
            continue
        all_candidates.append((sym, ticker))
    total = len(WHITELIST_SYMBOLS)
    will_scan = len(all_candidates)
    filtered = total - will_scan
    log.info("")
    log.info("📊 SCAN SUMMARY:")
    log.info(f"   Whitelist total: {total} coins")
    log.info(f"   ✅ Will scan:     {will_scan} coins ({will_scan/total*100:.1f}%)")
    log.info(f"   ❌ Filtered:      {filtered} coins ({filtered/total*100:.1f}%)")
    log.info("")
    log.info("📋 Filter breakdown:")
    log.info(f"   Not in Bitget:  {len(not_found)}")
    log.info(f"   Cooldown:       {filtered_stats['cooldown']}")
    log.info(f"   Manual exclude: {filtered_stats['manual_exclude']}")
    log.info(f"   Vol < $1K:      {filtered_stats['vol_too_low']}")
    log.info(f"   Vol > $50M:     {filtered_stats['vol_too_high']}")
    log.info(f"   Chg > ±30%:     {filtered_stats['change_extreme']}")
    log.info(f"   Invalid price:  {filtered_stats['invalid_price']}")
    log.info(f"   Parse error:    {filtered_stats['parse_error']}")
    if not_found and len(not_found) <= 30:
        log.info(f"\n⚠️  Missing from Bitget: {', '.join(not_found)}")
    elif not_found:
        log.info(f"\n⚠️  {len(not_found)} coins missing from Bitget")
        log.info(f"     First 10: {', '.join(not_found[:10])}")
    log.info(f"\n⏱️  Est. scan time: {will_scan * CONFIG['sleep_coins']:.0f}s (~{will_scan * CONFIG['sleep_coins']/60:.1f} min)")
    log.info("=" * 70)
    log.info("")
    return all_candidates

# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v13.4-BETA — {utc_now()} ===")
    log.info("=" * 70)
    log.info("PERUBAHAN vs v13.3:")
    log.info("  • Menambahkan filter Beta & Alpha terhadap BTC")
    log.info("  • Penalti/bonus berdasarkan tren BTC dan sensitivitas coin")
    log.info("=" * 70)
    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return
    log.info(f"Total ticker: {len(tickers)}")
    candidates = build_candidate_list(tickers)
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
                log.info(f"  Score={res['score']} | sinyal: {len(res['signals'])} | tipe={res['pump_type']} | gain T1={res['potential_gain_t1']}% | Beta={res['beta_alpha']['beta']} Alpha={res['beta_alpha']['alpha']:+.2f}")
                results.append(res)
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")
        time.sleep(CONFIG["sleep_coins"])
    results.sort(key=lambda x: x["score"], reverse=True)
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
            log.info(f"✅ Alert #{rank}: {r['symbol']} Score={r['score']} Gain T1={r['potential_gain_t1']}% Beta={r['beta_alpha']['beta']}")
        time.sleep(2)
    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v13.4-BETA                      ║")
    log.info("║  FOKUS: Entry support + target Fibonacci + filter BTC ║")
    log.info("╚═══════════════════════════════════════════════════╝")
    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)
    run_scan()
