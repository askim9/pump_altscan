"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PIVOT LOW BOUNCE SCANNER v1.0                                          ║
║  (Based on v9.10 infrastructure)                                        ║
║                                                                          ║
║  Mencari setup pantulan kuat dari pivot low dengan volume tinggi,       ║
║  breakdown palsu, dan reversal cepat.                                   ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging
from datetime import datetime, timezone
from collections import defaultdict

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
    "/tmp/pivot_scanner.log", maxBytes=10*1024*1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIG (sederhanakan hanya parameter yang diperlukan)
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # Threshold sinyal
    "min_composite_alert":       50,   # bisa disesuaikan
    "max_alerts_per_run":         8,

    # Volume 24h (USD)
    "min_vol_24h":            3_000,
    "max_vol_24h":       50_000_000,
    "pre_filter_vol":         1_000,

    # Gate perubahan harga
    "gate_chg_24h_max":          30.0,
    "gate_chg_7d_max":           35.0,
    "gate_chg_7d_min":          -35.0,
    "gate_funding_extreme":      -0.002,

    # Candle limits
    "candle_1h":                200,   # cukup untuk pivot 20/20
    "candle_15m":                96,

    # Entry/exit
    "min_target_pct":             5.0,
    "max_sl_pct":                12.0,
    "atr_sl_mult":                1.5,
    "atr_t1_mult":                2.5,
    "box_width":                  1.0,   # lebar kotak support = ATR * box_width
    "recovery_bars":              3,     # maksimal candle setelah breakdown untuk reversal
    "volume_threshold":           1.8,   # volume pivot harus > avg_pos_vol * ini
    "reversal_vol_threshold":     1.5,   # volume reversal harus > avg_vol * ini

    # Cooldown
    "alert_cooldown_sec":       3600,
    "sleep_coins":               0.8,
    "sleep_error":               3.0,
    "cooldown_file":    "/tmp/pivot_cooldown.json",
    "oi_snapshot_file": "/tmp/pivot_oi.json",

    # Pivot parameters
    "left_bars":                  20,
    "right_bars":                 20,
    "breakdown_max_bars":         50,
}

# ── WHITELIST (sama seperti sebelumnya) ───────────────────────────────────
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

MANUAL_EXCLUDE = set()

# ══════════════════════════════════════════════════════════════
#  🔒  COOLDOWN & OI SNAPSHOT (sama seperti asli)
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

def load_oi_snapshots():
    try:
        if os.path.exists(CONFIG["oi_snapshot_file"]):
            with open(CONFIG["oi_snapshot_file"]) as f:
                return json.load(f)
    except:
        pass
    return {}

def save_oi_snapshot(symbol, oi_value):
    snaps = load_oi_snapshots()
    now   = time.time()
    if symbol not in snaps:
        snaps[symbol] = []
    snaps[symbol].append({"ts": now, "oi": oi_value})
    snaps[symbol] = sorted(snaps[symbol], key=lambda x: x["ts"])[-100:]
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
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
#  🌐  HTTP UTILITIES (sama persis)
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
#  📡  DATA FETCHERS (sama persis)
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

def get_open_interest(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/open-interest",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            oi = data.get("data", {})
            if "openInterestList" in oi and oi["openInterestList"]:
                return float(oi["openInterestList"][0].get("size", 0))
            return float(oi.get("size", 0))
        except:
            pass
    return 0

def get_long_short_ratio(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/account-long-short-ratio",
        params={"symbol": symbol, "period": "1H",
                "limit": "4", "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000" and data.get("data"):
        try:
            return float(data["data"][0].get("longShortRatio", 1.0))
        except:
            pass
    return None

def get_trades(symbol, limit=500):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/fills",
        params={"symbol": symbol, "productType": "usdt-futures", "limit": str(limit)},
    )
    if data and data.get("code") == "00000":
        trades = []
        for t in data.get("data", []):
            try:
                ts_ms = int(t.get("fillTime", t.get("cTime", t.get("ts", 0))))
                trades.append({
                    "price": float(t["price"]),
                    "size":  float(t["size"]),
                    "side":  t.get("side", "").lower(),
                    "ts_ms": ts_ms,
                })
            except:
                pass
        return trades
    return []

# ══════════════════════════════════════════════════════════════
#  📐  MATH HELPERS (yang diperlukan)
# ══════════════════════════════════════════════════════════════
def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr

def find_pivot_lows(candles, left=20, right=20):
    """Mengembalikan list index candle yang merupakan pivot low."""
    pivots = []
    for i in range(left, len(candles) - right):
        if candles[i]["low"] < candles[i-1]["low"] and candles[i]["low"] < candles[i+1]["low"]:
            # cek apakah low ini lebih rendah dari left sebelumnya dan right sesudahnya
            is_pivot = True
            for j in range(1, left+1):
                if candles[i]["low"] >= candles[i-j]["low"]:
                    is_pivot = False
                    break
            if is_pivot:
                for j in range(1, right+1):
                    if candles[i]["low"] >= candles[i+j]["low"]:
                        is_pivot = False
                        break
            if is_pivot:
                pivots.append(i)
    return pivots

def calc_vwap_zone(candles):
    cum_tv, cum_v = 0, 0
    vals = []
    for c in candles:
        tp     = (c["high"] + c["low"] + c["close"]) / 3
        cum_tv += tp * c["volume"]
        cum_v  += c["volume"]
        vals.append(cum_tv / cum_v if cum_v else tp)
    if not vals:
        return None, None
    vwap = vals[-1]
    devs = [abs(candles[i]["close"] - vals[i]) for i in range(len(candles))]
    std  = math.sqrt(sum(d ** 2 for d in devs) / len(devs)) if devs else 0
    z1   = vwap - 1.5 * std
    cur  = candles[-1]["close"]
    if z1 >= cur:
        z1 = cur * 0.97
    return vwap, z1

def calc_poc(candles):
    if not candles:
        return None
    pmin  = min(c["low"]  for c in candles)
    pmax  = max(c["high"] for c in candles)
    if pmax == pmin:
        return candles[-1]["close"]
    bsize   = (pmax - pmin) / 40
    vol_bkt = defaultdict(float)
    for c in candles:
        lo = int((c["low"]  - pmin) / bsize)
        hi = int((c["high"] - pmin) / bsize)
        nb = max(hi - lo + 1, 1)
        for b in range(lo, hi + 1):
            vol_bkt[b] += c["volume"] / nb
    poc_b = max(vol_bkt, key=vol_bkt.get) if vol_bkt else 20
    return pmin + (poc_b + 0.5) * bsize

def find_resistance_targets(candles_1h, cur):
    if len(candles_1h) < 24:
        return cur * 1.10, cur * 1.18

    recent = candles_1h[-168:]
    resistance_levels = []
    min_t = cur * (1 + CONFIG["min_target_pct"] / 100)

    for i in range(2, len(recent) - 2):
        h = recent[i]["high"]
        if h <= min_t:
            continue
        touches = sum(
            1 for c in recent
            if abs(c["high"] - h) / h < 0.015 or abs(c["low"] - h) / h < 0.015
        )
        if touches >= 2:
            resistance_levels.append((h, touches, recent[i]["volume_usd"]))

    if not resistance_levels:
        atr = calc_atr(candles_1h[-24:]) or cur * 0.02
        return round(cur * 1.10, 8), round(cur * 1.18, 8)

    resistance_levels.sort(key=lambda x: x[0])
    t1 = resistance_levels[0][0]
    t2 = resistance_levels[1][0] if len(resistance_levels) > 1 else t1 * 1.08
    return round(t1, 8), round(t2, 8)

def calc_entry(candles_1h):
    cur  = candles_1h[-1]["close"]
    atr  = calc_atr(candles_1h, 14) or cur * 0.02
    recent   = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    vwap, z1 = calc_vwap_zone(recent)
    poc_src  = candles_1h[-48:] if len(candles_1h) >= 48 else candles_1h
    z2       = calc_poc(poc_src)
    if not z2 or z2 >= cur:
        z2 = cur * 0.97
    support = max(z1 or cur * 0.97, z2)
    if support >= cur:
        support = cur * 0.96
    max_dist = CONFIG["max_sl_pct"] / 100
    if (cur - support) / cur > max_dist:
        support = cur * (1 - max_dist + 0.02)
    entry  = min(support * 1.002, cur * 0.998)
    sl     = max(entry - CONFIG["atr_sl_mult"] * atr, entry * 0.88)

    t1_res, t2_res = find_resistance_targets(candles_1h, cur)
    t1_atr         = entry + CONFIG["atr_t1_mult"] * atr
    t1             = t1_res if t1_res > cur * 1.05 else t1_atr
    if t1 <= cur * 1.05:
        t1 = cur * 1.10
    t2     = t2_res if t2_res > t1 * 1.02 else t1 * 1.08
    risk   = entry - sl
    reward = t1 - entry
    rr     = round(reward / risk, 1) if risk > 0 else 0
    t1_pct = round((t1 - cur) / cur * 100, 1)
    sl_pct = round((entry - sl) / entry * 100, 1)
    return {
        "cur":    cur,
        "atr":    round(atr, 8),
        "vwap":   round(vwap, 8) if vwap else 0,
        "z1":     round(z1, 8)   if z1   else 0,
        "z2":     round(z2, 8),
        "entry":  round(entry, 8),
        "sl":     round(sl, 8),
        "sl_pct": sl_pct,
        "t1":     round(t1, 8),
        "t2":     round(t2, 8),
        "rr":     rr,
        "liq_pct": t1_pct,
    }

# ══════════════════════════════════════════════════════════════
#  🔍  DETEKSI PIVOT LOW BOUNCE
# ══════════════════════════════════════════════════════════════
def detect_pivot_bounce(symbol, ticker, tickers_dict):
    # Ambil data candle
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    if len(c1h) < 50:  # minimal untuk pivot
        return None

    # Hitung ATR keseluruhan
    atr = calc_atr(c1h, 14)
    if not atr:
        atr = c1h[-1]["close"] * 0.02

    # Hitung rata-rata volume untuk konfirmasi
    volumes = [c["volume_usd"] for c in c1h]
    avg_vol = sum(volumes) / len(volumes) if volumes else 1

    # Hitung rata-rata volume positif (saat close > open)
    pos_vols = [c["volume_usd"] for c in c1h if c["close"] > c["open"]]
    avg_pos_vol = sum(pos_vols) / len(pos_vols) if pos_vols else avg_vol

    # Temukan semua pivot low
    pivot_indices = find_pivot_lows(c1h, left=CONFIG["left_bars"], right=CONFIG["right_bars"])

    signals_found = []

    for idx in pivot_indices:
        pivot_candle = c1h[idx]
        pivot_low = pivot_candle["low"]

        # Syarat 1: volume pivot harus signifikan
        if pivot_candle["volume_usd"] < avg_pos_vol * CONFIG["volume_threshold"]:
            continue

        # Hitung batas bawah support zone
        support_low = pivot_low - atr * CONFIG["box_width"]

        # Cari breakdown dalam jendela ke depan (maks 50 candle)
        for j in range(idx + 1, min(idx + CONFIG["breakdown_max_bars"], len(c1h))):
            if c1h[j]["low"] < support_low:
                # Breakdown terjadi di candle j
                # Cek reversal dalam recovery_bars setelahnya
                for k in range(j + 1, min(j + CONFIG["recovery_bars"] + 1, len(c1h))):
                    if c1h[k]["close"] > pivot_low:
                        # Reversal terjadi di candle k
                        # Syarat volume reversal tinggi
                        if c1h[k]["volume_usd"] > avg_vol * CONFIG["reversal_vol_threshold"]:
                            # Sinyal ditemukan
                            signals_found.append({
                                "pivot_idx": idx,
                                "pivot_low": pivot_low,
                                "support_low": support_low,
                                "break_idx": j,
                                "break_time": c1h[j]["ts"],
                                "reversal_idx": k,
                                "reversal_time": c1h[k]["ts"],
                                "reversal_price": c1h[k]["close"],
                                "pivot_vol": pivot_candle["volume_usd"],
                                "reversal_vol": c1h[k]["volume_usd"],
                            })
                        break  # hanya ambil reversal pertama setelah breakdown
                break  # hanya breakdown pertama yang diproses

    if not signals_found:
        return None

    # Ambil sinyal terbaru (reversal terakhir)
    latest_signal = max(signals_found, key=lambda x: x["reversal_idx"])

    # Hitung entry, SL, TP
    entry_data = calc_entry(c1h)
    if not entry_data or entry_data["liq_pct"] < CONFIG["min_target_pct"]:
        return None

    # Ambil data tambahan untuk konteks
    funding = get_funding(symbol)
    oi_value = get_open_interest(symbol)
    if oi_value > 0:
        save_oi_snapshot(symbol, oi_value)

    # Ambil perubahan harga 24h dari ticker
    try:
        chg_24h = float(ticker.get("change24h", 0)) * 100
        vol_24h = float(ticker.get("quoteVolume", 0))
    except:
        chg_24h, vol_24h = 0, 0

    # Hitung RVOL sederhana
    if len(c1h) >= 25:
        last_vol = c1h[-2]["volume_usd"]  # candle lengkap terakhir
        same_hour_vols = [c["volume_usd"] for c in c1h[:-2] if (c["ts"] // 3_600_000) % 24 == (c1h[-2]["ts"] // 3_600_000) % 24]
        avg_same_hour = sum(same_hour_vols) / len(same_hour_vols) if same_hour_vols else 1
        rvol = last_vol / avg_same_hour if avg_same_hour > 0 else 1
    else:
        rvol = 1

    # Hitung komposit sederhana (misal: skor berdasarkan jarak reversal dan volume)
    # Bisa juga pakai probabilitas sederhana
    composite_score = min(100, int(
        30 * (latest_signal["reversal_vol"] / avg_vol) +
        30 * (latest_signal["pivot_vol"] / avg_pos_vol) +
        20 * (1 if rvol > 2 else 0.5) +
        20
    ))

    # Susun hasil
    result = {
        "symbol": symbol,
        "score": composite_score,
        "composite_score": composite_score,
        "signals": [
            f"Pivot low ${latest_signal['pivot_low']:.4f} dengan volume {latest_signal['pivot_vol']/1e3:.0f}K",
            f"Breakdown di {datetime.fromtimestamp(latest_signal['break_time']/1000).strftime('%H:%M')}",
            f"Reversal di {datetime.fromtimestamp(latest_signal['reversal_time']/1000).strftime('%H:%M')} dengan volume {latest_signal['reversal_vol']/1e3:.0f}K",
        ],
        "entry": entry_data,
        "sector": "N/A",  # bisa diisi nanti
        "funding": funding,
        "price": c1h[-1]["close"],
        "chg_24h": chg_24h,
        "vol_24h": vol_24h,
        "rvol": round(rvol, 1),
        "ls_ratio": None,
        "chg_7d": 0,
        "avg_vol_6h": 0,
        "range_6h": 0,
        "coiling": 0,
        "bbw_val": 0,
        "oi_change_24h": 0,
        "oi_change_1h": 0,
        "prob_score": composite_score / 100,
        "prob_class": "Potential Bounce",
        "prob_metrics": {},
        "rsi_1h": 50,
        "long_liq": 0,
        "short_liq": 0,
        "linea_components": 0,
        "oi_accel_score": 0,
        "oi_accel_data": {},
        "nf_data": {},
        "nf_score": 0,
        "ws": 0,
        "wev": [],
        "bd": {"oi_valid": False},
    }

    return result

# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER (disederhanakan)
# ══════════════════════════════════════════════════════════════
def build_alert(r, rank=None):
    sc   = r["score"]
    comp = r.get("composite_score", sc)
    bar  = "█" * int(comp / 5) + "░" * (20 - int(comp / 5))
    e    = r["entry"]
    rk   = f"#{rank} " if rank else ""
    vol  = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
            else f"${r['vol_24h']/1e3:.0f}K")

    msg = (
        f"🚨 <b>PIVOT BOUNCE SIGNAL {rk}— v1.0</b>\n\n"
        f"<b>Symbol    :</b> {r['symbol']}\n"
        f"<b>Score     :</b> {comp}/100  {bar}\n"
        f"<b>Harga     :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h)\n"
        f"<b>Vol 24h   :</b> {vol} | RVOL: {r['rvol']:.1f}x\n"
        f"<b>Funding   :</b> {r['funding']:.5f}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
    )

    for s in r["signals"]:
        msg += f"  • {s}\n"

    if e:
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>ENTRY ZONES</b>\n"
            f"  🟢 VWAP  : ${e['z1']}\n"
            f"  🟢 POC   : ${e['z2']}\n"
            f"  📌 Entry : ${e['entry']}\n"
            f"  🛑 SL    : ${e['sl']}  (-{e['sl_pct']:.1f}%)\n\n"
            f"🎯 <b>TARGET</b>\n"
            f"  T1 : ${e['t1']}  (+{e['liq_pct']:.1f}%)\n"
            f"  T2 : ${e['t2']}\n"
            f"  R/R: 1:{e['rr']}  |  ATR: ${e['atr']}\n"
        )

    msg += f"\n🕐 {utc_now()}\n"
    msg += "<i>⚠️ Bukan financial advice. Manage risk ketat.</i>"
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP PIVOT BOUNCE CANDIDATES — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        comp     = r.get("composite_score", r["score"])
        bar      = "█" * int(comp / 10) + "░" * (10 - int(comp / 10))
        vol      = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                    else f"${r['vol_24h']/1e3:.0f}K")
        t1p      = r["entry"]["liq_pct"] if r.get("entry") else 0
        msg += (
            f"{i}. <b>{r['symbol']}</b> [C:{comp} {bar}]\n"
            f"   RVOL:{r['rvol']:.1f}x | {vol} | T1:+{t1p:.0f}%\n"
        )
    return msg

# ══════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST (sama seperti sebelumnya)
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
    log.info("🔍 SCANNING MODE: FULL WHITELIST (ALL COINS)")
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
    log.info(f"=== PIVOT LOW BOUNCE SCANNER v1.0 — {utc_now()} ===")

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
            res = detect_pivot_bounce(sym, t, tickers)
            if res:
                comp = res["composite_score"]
                log.info(
                    f"  Score={res['score']} Comp={comp} "
                    f"RVOL={res['rvol']:.1f}x T1=+{res['entry']['liq_pct']:.1f}%"
                )
                if comp >= CONFIG["min_composite_alert"]:
                    results.append(res)
                else:
                    log.info(f"  SKIP: comp={comp}<{CONFIG['min_composite_alert']}")
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
            log.info(f"✅ Alert #{rank}: {r['symbol']} C={r['composite_score']}")
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")


# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════════╗")
    log.info("║  PIVOT LOW BOUNCE SCANNER v1.0                   ║")
    log.info("║  Mencari setup pantulan dari pivot low dengan    ║")
    log.info("║  volume tinggi dan false breakdown.              ║")
    log.info("╚═══════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)

    run_scan()
