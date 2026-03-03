"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v10.0 — UNIFIED EDITION                               ║
║                                                                          ║
║  MERGE: v13.8-FINAL + v9.10                                              ║
║                                                                          ║

║             ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging
from datetime import datetime, timezone
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
_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v10.log", maxBytes=10*1024*1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)
log = logging.getLogger(__name__)
log.info("Log file aktif: /tmp/scanner_v10.log (rotasi 10MB)")

# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────
    "min_score_alert":          20,    # dinaikkan (scoring sekarang lebih kaya)
    "max_alerts_per_run":       15,

    # ── Volume 24h (USD) ──────────────────────────────────────
    "min_vol_24h":          50_000,    # FIX: dinaikkan dari $3K → $50K (realistis)
    "max_vol_24h":      50_000_000,
    "pre_filter_vol":       10_000,

    # ── Gate perubahan harga ──────────────────────────────────
    "gate_chg_24h_max":         30.0,

    # ── Dead Activity gate (v9.10) ────────────────────────────
    # Blok jika volume candle terakhir < threshold% dari avg 6 candle
    "dead_activity_threshold":  0.10,

    # ── Funding gate (v13.8, difix logikanya) ────────────────
    # Lolos jika: avg_funding < threshold ATAU cumulative < threshold
    "funding_gate_avg":      -0.0001,
    "funding_gate_cumul":      -0.02,

    # ── Candle limits ─────────────────────────────────────────
    "candle_1h":                168,   # 7 hari
    "candle_15m":                96,   # 24 jam
    "candle_4h":                 42,

    # ── Entry / Exit (v13.8) ──────────────────────────────────
    "max_risk_pct":              4.0,  # SL maks 4%
    "entry_range_above":         0.003,# entry range 0.3% di atas support
    "resistance_lookback_hours": 24,   # cari resistance dalam 24 jam
    "min_resistance_gap_pct":    0.5,  # FIX: target harus minimal 0.5% dari entry

    # ── Volume spike (v13.8) ──────────────────────────────────
    "vol_spike_threshold":       5.0,
    "vol_spike_bonus":           3,
    "min_vol_1h_ratio":          0.2,
    "vol_low_penalty":          -5,

    # ── BTC Beta/Alpha (v13.8) ────────────────────────────────
    "beta_lookback_hours":       24,
    "beta_high_threshold":       1.5,
    "btc_drop_threshold":       -1.5,
    "alpha_positive_threshold":  0.5,

    # ── Bobot skor teknikal (v13.8) ───────────────────────────
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
    "score_vol_ratio_24h":       2,
    "score_vol_accel":           2,
    "score_macd_pos":            1,
    "above_vwap_rate_min":       0.6,
    "squeeze_funding_cumul":    -0.05,
    "vol_ratio_threshold":       2.5,
    "vol_accel_threshold":       0.5,

    # ── OI layer (v9.10) ──────────────────────────────────────
    "oi_snapshot_file":    "./oi_snaps.json",
    "oi_penalty_24h":       -8,  # OI 24h turun signifikan
    "oi_penalty_1h":        -6,  # OI 1h turun (pendek)
    "oi_bonus_stealth":     +8,  # OI naik tapi harga flat (akumulasi)
    "oi_bonus_expand":      +5,  # OI naik moderan
    "oi_chg24h_penalty_thr": -8.0,
    "oi_chg1h_penalty_thr":  -3.0,
    "oi_stealth_oi_min":     +5.0,  # OI naik > 5%
    "oi_stealth_price_max":   2.0,  # harga flat < 2%

    # ── Net Flow (v9.10) ──────────────────────────────────────
    "max_netflow_score":        25,
    "nf_strong_buy":            12.0,
    "nf_buy":                    5.0,
    "nf_neutral_max":            5.0,
    "nf_sell":                  -5.0,
    "nf_strong_sell":          -15.0,
    "nf_gate_72h":             -12.0,
    "nf_gate_24h":              -8.0,
    "nf_gate_6h":               -5.0,
    "nf_whale_72h_max":          3.0,
    "nf_whale_72h_min":        -15.0,
    "nf_whale_24h_min":          3.0,
    "nf_whale_6h_min":           5.0,

    # ── Operasional ───────────────────────────────────────────
    "alert_cooldown_sec":      1800,
    "sleep_coins":              0.8,
    "sleep_error":              3.0,
    "cooldown_file":     "./cooldown.json",
    "funding_snapshot_file": "./funding.json",
}

MANUAL_EXCLUDE = set()

# ══════════════════════════════════════════════════════════════
#  📋  WHITELIST
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

GRAN_MAP       = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
BITGET_BASE    = "https://api.bitget.com"
_cache         = {}
EXCLUDED_KEYWORDS = ["XAU","PAXG","BTC","ETH","USDC","DAI","BUSD","UST","LUNC","LUNA"]

# ══════════════════════════════════════════════════════════════
#  🔒  COOLDOWN
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

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym):
    return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym):
    _cooldown[sym] = time.time()
    save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════
#  📦  FUNDING SNAPSHOTS (v13.8)
# ══════════════════════════════════════════════════════════════
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
    now   = time.time()
    if symbol not in snaps:
        snaps[symbol] = []
    snaps[symbol].append({"ts": now, "funding": funding_rate})
    snaps[symbol] = sorted(snaps[symbol], key=lambda x: x["ts"])[-20:]
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(snaps, f)
    except:
        pass

def get_funding_stats(symbol, current_funding):
    snaps    = load_funding_snapshots().get(symbol, [])
    all_rates = [s["funding"] for s in snaps] + [current_funding]
    if len(all_rates) < 2:
        return None
    last6    = all_rates[-6:]
    avg6     = sum(last6) / len(last6)
    cumul    = sum(last6)
    neg_pct  = sum(1 for f in last6 if f < 0) / len(last6) * 100
    streak   = 0
    for f in reversed(last6):
        if f < 0: streak += 1
        else:     break
    return {
        "avg":        avg6,
        "cumulative": cumul,
        "neg_pct":    neg_pct,
        "streak":     streak,
        "basis":      current_funding * 100,
        "current":    current_funding,
    }

# ══════════════════════════════════════════════════════════════
#  📦  OI SNAPSHOTS (v9.10 — v9.9 fix)
# ══════════════════════════════════════════════════════════════
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

def get_oi_changes(symbol, current_oi):
    """
    v9.9 FIX: OI-valid gate yang robust.
    - oi_valid = True jika ada >= 2 snapshot (ANY interval)
    - Toleransi nearest() = 1800s (30 menit)
    - Fallback ke snapshot tertua jika tidak ada snapshot ~1h
    """
    snaps = load_oi_snapshots()
    hist  = snaps.get(symbol, [])
    if len(hist) < 2:
        return 0, 0, False
    now = time.time()

    def nearest(target_ts, tolerance=1800):
        cands = [d for d in hist if abs(d["ts"] - target_ts) < tolerance]
        return min(cands, key=lambda d: abs(d["ts"] - target_ts)) if cands else None

    old1h  = nearest(now - 3600)
    old24h = nearest(now - 86400, tolerance=7200)

    # Fallback: jika tidak ada snapshot ~1h, gunakan tertua
    if not old1h:
        older = [d for d in hist if d["ts"] < now - 60]
        if older:
            old1h = min(older, key=lambda d: d["ts"])

    chg1h  = (current_oi - old1h["oi"])  / old1h["oi"]  * 100 if old1h  and old1h["oi"]  else 0
    chg24h = (current_oi - old24h["oi"]) / old24h["oi"] * 100 if old24h and old24h["oi"] else 0
    oi_valid = (old1h is not None)
    return chg1h, chg24h, oi_valid

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
        url     = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"}
        r       = requests.post(url, data=payload, timeout=15)
        log.info(f"Telegram response: {r.status_code}")
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

def get_trades(symbol, limit=500):
    """v9.10: menyertakan fillTime timestamp untuk windowed net flow."""
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

def get_orderbook(symbol, levels=50):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/merge-depth",
        params={"symbol": symbol, "productType": "usdt-futures",
                "precision": "scale0", "limit": str(levels)},
    )
    if data and data.get("code") == "00000":
        try:
            book    = data["data"]
            bid_vol = sum(float(b[1]) for b in book.get("bids", []))
            ask_vol = sum(float(a[1]) for a in book.get("asks", []))
            total   = bid_vol + ask_vol
            ratio   = bid_vol / total if total > 0 else 0.5
            return ratio, bid_vol, ask_vol
        except:
            pass
    return 0.5, 0, 0

def get_btc_candles(gran="1h", limit=168):
    return get_candles("BTCUSDT", gran, limit)

# ══════════════════════════════════════════════════════════════
#  📐  INDICATOR FUNCTIONS (v13.8 base)
# ══════════════════════════════════════════════════════════════
def calc_bbw(candles, period=20):
    if len(candles) < period:
        return 0, 0.5
    closes = [c["close"] for c in candles[-period:]]
    mean   = sum(closes) / period
    std    = math.sqrt(sum((x - mean)**2 for x in closes) / period)
    bb_upper = mean + 2*std
    bb_lower = mean - 2*std
    bbw    = (bb_upper - bb_lower) / mean * 100 if mean > 0 else 0
    last   = candles[-1]["close"]
    bb_pct = (last - bb_lower) / (bb_upper - bb_lower) if (bb_upper - bb_lower) > 0 else 0.5
    return bbw, bb_pct

def calc_atr_pct(candles, period=14):
    if len(candles) < period + 1:
        return 0
    trs = []
    for i in range(1, period + 1):
        h  = candles[-i]["high"]
        l  = candles[-i]["low"]
        pc = candles[-i-1]["close"] if i < len(candles) else candles[-i]["open"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs) / period
    cur = candles[-1]["close"]
    return atr / cur * 100 if cur > 0 else 0

def calc_vwap(candles, window=24):
    if len(candles) < window:
        return candles[-1]["close"]
    cum_tv = cum_v = 0
    for c in candles[-window:]:
        tp      = (c["high"] + c["low"] + c["close"]) / 3
        cum_tv += tp * c["volume"]
        cum_v  += c["volume"]
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
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return 100 - (100 / (1 + avg_g / avg_l))

def calc_macd(candles, fast=12, slow=26, signal=9):
    """FIX v10.0: EMA incremental yang benar (bukan recalculate dari scratch)."""
    if len(candles) < slow + signal + 5:
        return 0
    closes = [c["close"] for c in candles]

    def ema_series(period, data):
        alpha  = 2 / (period + 1)
        result = [sum(data[:period]) / period]
        for price in data[period:]:
            result.append(alpha * price + (1 - alpha) * result[-1])
        return result

    ema_fast   = ema_series(fast,   closes[-(slow + signal + 10):])
    ema_slow   = ema_series(slow,   closes[-(slow + signal + 10):])
    min_len    = min(len(ema_fast), len(ema_slow))
    macd_line  = [ema_fast[i + (len(ema_fast) - min_len)]
                  - ema_slow[i + (len(ema_slow) - min_len)]
                  for i in range(min_len)]
    if len(macd_line) < signal:
        return 0
    sig_line   = ema_series(signal, macd_line)
    return macd_line[-1] - sig_line[-1]

# ══════════════════════════════════════════════════════════════
#  🎯  ENTRY / SL / TARGET SYSTEM (v13.8)
# ══════════════════════════════════════════════════════════════
def find_swing_low_high(candles_1h, lookback=48):
    if len(candles_1h) < lookback:
        lookback = len(candles_1h)
    recent   = candles_1h[-lookback:]
    low_idx  = min(range(len(recent)), key=lambda i: recent[i]["low"])
    high_idx = max(range(len(recent)), key=lambda i: recent[i]["high"])
    if low_idx < high_idx:
        return recent[low_idx]["low"], recent[high_idx]["high"]
    return min(c["low"] for c in recent), max(c["high"] for c in recent)

def calc_fib_targets(entry, candles_1h):
    swing_low, swing_high = find_swing_low_high(candles_1h)
    fib_range = swing_high - swing_low
    if fib_range <= 0:
        return entry * 1.08, entry * 1.15
    t1 = swing_low + fib_range * 1.272
    t2 = swing_low + fib_range * 1.618
    if t1 < entry * 1.005:  t1 = entry * 1.08
    if t2 < t1 * 1.005:     t2 = t1 * 1.08
    return round(t1, 8), round(t2, 8)

def get_support_levels(candles_1h):
    cur      = candles_1h[-1]["close"]
    supports = [min(c["low"] for c in candles_1h[-3:])]
    if len(candles_1h) >= 24:
        vwap = calc_vwap(candles_1h[-24:])
        if vwap < cur:
            supports.append(vwap)
    if len(candles_1h) >= 20:
        closes = [c["close"] for c in candles_1h[-20:]]
        ema20  = sum(closes) / 20
        if ema20 < cur:
            supports.append(ema20)
    valid = [s for s in supports if s < cur]
    return max(valid) if valid else cur * 0.985

def get_resistance_levels(candles_1h, entry):
    """
    FIX v10.0: filter resistance yang terlalu dekat dari entry
    (minimal CONFIG['min_resistance_gap_pct'] gap).
    """
    lookback = CONFIG["resistance_lookback_hours"]
    min_gap  = CONFIG["min_resistance_gap_pct"] / 100
    if len(candles_1h) < lookback:
        return calc_fib_targets(entry, candles_1h)
    recent = candles_1h[-lookback:]
    # Filter: high harus di atas entry + min_gap
    highs  = sorted(set(
        round(c["high"], 8) for c in recent
        if c["high"] > entry * (1 + min_gap)
    ))
    if not highs:
        return calc_fib_targets(entry, candles_1h)
    t1    = highs[0]
    t2    = highs[1] if len(highs) >= 2 else calc_fib_targets(entry, candles_1h)[1]
    return t1, t2

def calc_entry(candles_1h):
    cur     = candles_1h[-1]["close"]
    support = get_support_levels(candles_1h)
    entry   = support
    entry_range = (support, support * (1 + CONFIG["entry_range_above"]))

    low_5h       = min(c["low"] for c in candles_1h[-5:])
    sl_candidate = min(low_5h, support * 0.99)
    risk_pct     = (entry - sl_candidate) / entry * 100
    if risk_pct > CONFIG["max_risk_pct"]:
        sl_candidate = entry * (1 - CONFIG["max_risk_pct"] / 100)
    sl = sl_candidate

    t1, t2       = get_resistance_levels(candles_1h, entry)
    fib_t1, fib_t2 = calc_fib_targets(entry, candles_1h)
    risk   = entry - sl
    reward = t1 - entry
    rr     = round(reward / risk, 1) if risk > 0 else 0

    return {
        "cur":          cur,
        "entry":        round(entry, 8),
        "entry_range":  (round(entry_range[0], 8), round(entry_range[1], 8)),
        "sl":           round(sl, 8),
        "sl_pct":       round((entry - sl) / entry * 100, 1),
        "t1":           round(t1, 8),
        "t2":           round(t2, 8),
        "fib_t1":       round(fib_t1, 8),
        "fib_t2":       round(fib_t2, 8),
        "rr":           rr,
        "support_used": round(support, 8),
    }

# ══════════════════════════════════════════════════════════════
#  📊  BETA / ALPHA vs BTC (v13.8)
# ══════════════════════════════════════════════════════════════
def compute_beta_alpha(coin_candles, btc_candles, lookback_hours=24):
    if len(coin_candles) < lookback_hours or len(btc_candles) < lookback_hours:
        return 0, 0, 0, 0
    coin_closes = [c["close"] for c in coin_candles[-lookback_hours:]]
    btc_closes  = [c["close"] for c in btc_candles[-lookback_hours:]]
    coin_ret    = [(coin_closes[i] - coin_closes[i-1]) / coin_closes[i-1] * 100
                   for i in range(1, len(coin_closes))]
    btc_ret     = [(btc_closes[i]  - btc_closes[i-1])  / btc_closes[i-1]  * 100
                   for i in range(1, len(btc_closes))]
    if len(coin_ret) < 2:
        return 0, 0, 0, 0
    x  = np.array(btc_ret)
    y  = np.array(coin_ret)
    A  = np.vstack([x, np.ones(len(x))]).T
    beta, alpha = np.linalg.lstsq(A, y, rcond=None)[0]
    residuals   = y - (beta * x + alpha)
    ss_res      = np.sum(residuals**2)
    ss_tot      = np.sum((y - np.mean(y))**2)
    r_squared   = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
    return beta, alpha, r_squared, btc_ret[-1] if btc_ret else 0

def get_btc_trend(btc_candles, hours=3):
    if len(btc_candles) < hours:
        return "neutral", 0
    start  = btc_candles[-hours]["close"]
    end    = btc_candles[-1]["close"]
    change = (end - start) / start * 100
    if change < CONFIG["btc_drop_threshold"]:
        return "bearish", change
    elif change > -CONFIG["btc_drop_threshold"]:
        return "bullish", change
    return "neutral", change

# ══════════════════════════════════════════════════════════════
#  🔴  NET FLOW MULTI-TF LAYER (v9.10)
# ══════════════════════════════════════════════════════════════
def _candle_net_flow(candles):
    buy_usd = sell_usd = 0.0
    for c in candles:
        rng       = c["high"] - c["low"]
        buy_ratio = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
        buy_usd  += buy_ratio * c["volume_usd"]
        sell_usd += (1.0 - buy_ratio) * c["volume_usd"]
    total   = buy_usd + sell_usd
    net     = buy_usd - sell_usd
    net_pct = net / total * 100 if total > 0 else 0.0
    return net, net_pct, buy_usd, sell_usd

def _tick_net_flow(trades, window_minutes=15):
    if not trades:
        return 0.0, 0.0, 0.0, 0.0, 0
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - window_minutes * 60 * 1000
    has_ts = any(t.get("ts_ms", 0) > 0 for t in trades)
    recent = [t for t in trades if t.get("ts_ms", 0) > cutoff] if has_ts else trades
    if not recent:
        recent = trades
    buy_usd  = sum(t["size"] * t["price"] for t in recent if "buy"  in t.get("side", ""))
    sell_usd = sum(t["size"] * t["price"] for t in recent if "sell" in t.get("side", ""))
    total    = buy_usd + sell_usd
    net_pct  = (buy_usd - sell_usd) / total * 100 if total > 0 else 0.0
    return buy_usd - sell_usd, net_pct, buy_usd, sell_usd, len(recent)

def _classify_flow(net_pct):
    if net_pct > CONFIG["nf_strong_buy"]:   return "STRONG_BUY"
    if net_pct > CONFIG["nf_buy"]:          return "BUY"
    if net_pct > -CONFIG["nf_neutral_max"]: return "NEUTRAL"
    if net_pct > CONFIG["nf_strong_sell"]:  return "SELL"
    return "STRONG_SELL"

def layer_net_flow(candles_1h, candles_15m, trades):
    """
    Multi-TF Net Flow: gate + scoring.
    Return: (score, signals, flow_data, should_block)
    """
    score, sigs = 0, []
    flow_data   = {
        "72h": {"net_pct": 0, "label": "NO_DATA"},
        "24h": {"net_pct": 0, "label": "NO_DATA"},
        "6h":  {"net_pct": 0, "label": "NO_DATA"},
        "15m": {"net_pct": 0, "label": "NO_DATA", "count": 0},
        "has_data": False,
    }

    pct_72h = pct_24h = pct_6h = pct_15m = None

    if len(candles_1h) >= 72:
        _, pct, buy, sell = _candle_net_flow(candles_1h[-72:])
        pct_72h = pct
        flow_data["72h"] = {"net_pct": round(pct, 1), "buy_usd": round(buy),
                            "sell_usd": round(sell), "label": _classify_flow(pct)}
    if len(candles_1h) >= 24:
        _, pct, buy, sell = _candle_net_flow(candles_1h[-24:])
        pct_24h = pct
        flow_data["24h"] = {"net_pct": round(pct, 1), "buy_usd": round(buy),
                            "sell_usd": round(sell), "label": _classify_flow(pct)}
    if len(candles_1h) >= 6:
        _, pct, buy, sell = _candle_net_flow(candles_1h[-6:])
        pct_6h = pct
        flow_data["6h"]  = {"net_pct": round(pct, 1), "buy_usd": round(buy),
                            "sell_usd": round(sell), "label": _classify_flow(pct)}
    if trades:
        _, pct, buy, sell, cnt = _tick_net_flow(trades, window_minutes=15)
        pct_15m = pct
        flow_data["15m"] = {"net_pct": round(pct, 1), "buy_usd": round(buy),
                            "sell_usd": round(sell), "label": _classify_flow(pct), "count": cnt}

    flow_data["has_data"] = (pct_24h is not None)
    if not flow_data["has_data"]:
        return 0, [], flow_data, False

    # ── Gate: distribusi sistematis ───────────────────────────
    if (pct_72h is not None
            and pct_72h < CONFIG["nf_gate_72h"]
            and pct_24h < CONFIG["nf_gate_24h"]
            and pct_6h  < CONFIG["nf_gate_6h"]):
        sigs.append(
            f"🚨 NET FLOW DISTRIBUSI: 72h={pct_72h:+.1f}% "
            f"24h={pct_24h:+.1f}% 6h={pct_6h:+.1f}%"
        )
        return 0, sigs, flow_data, True

    # ── Scoring ───────────────────────────────────────────────
    # 1. Whale Funnel (sinyal terkuat)
    if (pct_72h is not None
            and CONFIG["nf_whale_72h_min"] <= pct_72h <= CONFIG["nf_whale_72h_max"]
            and pct_24h >= CONFIG["nf_whale_24h_min"]
            and pct_6h  >= CONFIG["nf_whale_6h_min"]):
        score += 20
        sigs.append(f"🐋 WHALE FUNNEL: 72h={pct_72h:+.1f}% → 24h={pct_24h:+.1f}% → "
                    f"6h={pct_6h:+.1f}% — akumulasi 3 hari terkonfirmasi!")

    # 2. Full alignment bullish
    elif (pct_72h is not None and pct_72h > CONFIG["nf_buy"]
            and pct_24h > CONFIG["nf_buy"] and pct_6h > CONFIG["nf_buy"]):
        score += 15
        sigs.append(f"✅ NET FLOW BULLISH: 72h={pct_72h:+.1f}% 24h={pct_24h:+.1f}% "
                    f"6h={pct_6h:+.1f}%")

    # 3. Partial alignment
    else:
        if pct_24h > CONFIG["nf_buy"] and pct_6h > CONFIG["nf_buy"]:
            score += 10
            sigs.append(f"✅ Net Flow 24h={pct_24h:+.1f}% & 6h={pct_6h:+.1f}%")
        elif pct_6h is not None and pct_6h > CONFIG["nf_strong_buy"]:
            score += 7
            sigs.append(f"Net Flow 6h={pct_6h:+.1f}% — buying surge")
        elif pct_6h is not None and pct_6h > CONFIG["nf_buy"]:
            score += 4
            sigs.append(f"Net Flow 6h={pct_6h:+.1f}%")

        if (pct_72h is not None and CONFIG["nf_strong_sell"] < pct_72h < 0
                and pct_24h > CONFIG["nf_buy"]):
            score += 5
            sigs.append(f"📈 Flow shifting: 72h={pct_72h:+.1f}% → 24h={pct_24h:+.1f}%")

    # 4. Penalti TF negatif
    if pct_72h is not None:
        if pct_72h < CONFIG["nf_strong_sell"]:  score -= 12; sigs.append(f"⚠️ Net Flow 72h={pct_72h:+.1f}%")
        elif pct_72h < CONFIG["nf_sell"]:        score -= 6
    if pct_24h is not None:
        if pct_24h < CONFIG["nf_strong_sell"]:  score -= 10; sigs.append(f"⚠️ Net Flow 24h={pct_24h:+.1f}%")
        elif pct_24h < CONFIG["nf_sell"]:        score -= 5

    # 5. Flow acceleration
    if pct_6h is not None and pct_24h is not None:
        if pct_6h * 4 > pct_24h + 10 and pct_6h > 0:
            score += 3
            sigs.append(f"⚡ Flow akselerasi: 6h={pct_6h:+.1f}% >> 24h={pct_24h:+.1f}%")

    # 6. Real-time ticks
    if pct_15m is not None and flow_data["15m"]["count"] >= 10:
        if pct_15m > CONFIG["nf_strong_buy"]:   score += 4; sigs.append(f"✅ Ticks 15m={pct_15m:+.1f}%")
        elif pct_15m > CONFIG["nf_buy"]:         score += 2
        elif pct_15m < CONFIG["nf_strong_sell"]: score -= 5; sigs.append(f"⚠️ Ticks 15m={pct_15m:+.1f}%")
        elif pct_15m < CONFIG["nf_sell"]:        score -= 2

    return min(score, CONFIG["max_netflow_score"]), sigs, flow_data, False

# ══════════════════════════════════════════════════════════════
#  🔴  OI SCORING LAYER (v9.10, disederhanakan)
# ══════════════════════════════════════════════════════════════
def layer_oi(oi_chg1h, oi_chg24h, oi_valid, chg_24h):
    """
    Hitung skor dan sinyal dari perubahan OI.
    Return: (score, signals)
    """
    score, sigs = 0, []
    if not oi_valid:
        return 0, ["⚠️ OI baseline belum cukup (run ke-1)"]

    # Penalti: OI turun = posisi long ditutup / short buka = bearish
    if oi_chg24h < CONFIG["oi_chg24h_penalty_thr"]:
        score += CONFIG["oi_penalty_24h"]
        sigs.append(f"⚠️ OI 24h {oi_chg24h:+.1f}% — long ditutup masif")
    if oi_chg1h < CONFIG["oi_chg1h_penalty_thr"]:
        score += CONFIG["oi_penalty_1h"]
        sigs.append(f"⚠️ OI 1h {oi_chg1h:+.1f}% — posisi keluar tiba-tiba")

    # Bonus: OI naik tapi harga flat = stealth accumulation (sinyal kuat)
    if (oi_chg24h > CONFIG["oi_stealth_oi_min"]
            and abs(chg_24h) < CONFIG["oi_stealth_price_max"]):
        score += CONFIG["oi_bonus_stealth"]
        sigs.append(f"✅ OI +{oi_chg24h:.1f}% tapi harga flat — stealth akumulasi")
    elif oi_chg24h > CONFIG["oi_stealth_oi_min"]:
        score += CONFIG["oi_bonus_expand"]
        sigs.append(f"✅ OI 24h +{oi_chg24h:.1f}% — ekspansi posisi")

    return score, sigs

# ══════════════════════════════════════════════════════════════
#  🚦  GATE: SUDAH PUMP? (v9.10)
# ══════════════════════════════════════════════════════════════
def is_already_pumped(oi_chg24h, chg_24h, vol_ratio, oi_valid):
    """
    Return: (True/False, alasan)
    Blok coin yang sudah pump — terlambat masuk.
    """
    if not oi_valid:
        if chg_24h > 15 and vol_ratio > 5:
            return True, f"Harga +{chg_24h:.0f}% + Volume {vol_ratio:.1f}x — pump sudah terjadi"
        return False, ""
    if oi_chg24h > 35 and chg_24h > 3:
        return True, f"OI 24h +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}% — TERLAMBAT"
    if oi_chg24h > 25 and chg_24h > 5:
        return True, f"OI +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}% — pump berjalan"
    if oi_chg24h > 15 and chg_24h > 10:
        return True, f"OI +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}% — momentum habis"
    return False, ""

# ══════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    # ── 1. Ambil candle data ──────────────────────────────────
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    if len(c1h) < 48 or len(c15m) < 20:
        return None

    # ── 2. Data dasar dari ticker ──────────────────────────────
    try:
        vol_24h    = float(ticker.get("quoteVolume", 0))
        chg_24h    = float(ticker.get("change24h", 0)) * 100
        price_now  = float(ticker.get("lastPr", 0)) or c1h[-1]["close"]
    except:
        return None

    if vol_24h < CONFIG["min_vol_24h"]:
        return None

    # ── 3. Dead Activity gate (v9.10) ─────────────────────────
    if len(c1h) >= 7:
        avg_vol_6h_prev = sum(c["volume_usd"] for c in c1h[-7:-1]) / 6
        last_vol_1h     = c1h[-1]["volume_usd"]
        if avg_vol_6h_prev > 0 and last_vol_1h < avg_vol_6h_prev * CONFIG["dead_activity_threshold"]:
            log.info(f"  {symbol}: GATE dead activity (vol 1h {last_vol_1h:.0f} vs avg {avg_vol_6h_prev:.0f})")
            return None

    # ── 4. Funding gate (v13.8, logika difix) ─────────────────
    funding = get_funding(symbol)
    save_funding_snapshot(symbol, funding)
    fstats  = get_funding_stats(symbol, funding)
    if not fstats:
        log.info(f"  {symbol}: Data funding belum cukup (run pertama)")
        return None
    # FIX: sebelumnya `not (avg < thr OR cumul < thr)` → block semua yang tidak memenuhi salah satu
    # Sekarang: harus penuhi SALAH SATU kondisi untuk lolos
    funding_ok = (fstats["avg"] < CONFIG["funding_gate_avg"]
                  or fstats["cumulative"] < CONFIG["funding_gate_cumul"])
    if not funding_ok:
        log.info(f"  {symbol}: Funding tidak cukup negatif (avg={fstats['avg']:.6f})")
        return None

    # ── 5. OI data + gate sudah pump (v9.10) ──────────────────
    oi_value = get_open_interest(symbol)
    if oi_value > 0:
        save_oi_snapshot(symbol, oi_value)
    oi_chg1h, oi_chg24h, oi_valid = get_oi_changes(symbol, oi_value)

    # Volume ratio untuk gate
    avg_vol_24h_calc = sum(c["volume_usd"] for c in c1h[-24:]) / 24 if len(c1h) >= 24 else 0
    vol_ratio_gate   = c1h[-1]["volume_usd"] / avg_vol_24h_calc if avg_vol_24h_calc > 0 else 0

    pumped, pump_reason = is_already_pumped(oi_chg24h, chg_24h, vol_ratio_gate, oi_valid)
    if pumped:
        log.info(f"  {symbol}: GATE already pumped — {pump_reason}")
        return None

    # ── 6. BTC data + Beta/Alpha ───────────────────────────────
    btc_c1h = get_btc_candles("1h", CONFIG["candle_1h"])
    if len(btc_c1h) < 48:
        return None
    beta, alpha, r_squared, _ = compute_beta_alpha(c1h, btc_c1h, CONFIG["beta_lookback_hours"])
    btc_trend, btc_change     = get_btc_trend(btc_c1h, hours=3)

    # ── 7. Indikator teknikal ─────────────────────────────────
    bbw, bb_pct    = calc_bbw(c1h)
    price_chg      = (c1h[-1]["close"] - c1h[-2]["close"]) / c1h[-2]["close"] * 100 if len(c1h) >= 2 else 0
    atr_pct        = calc_atr_pct(c1h)
    rsi            = get_rsi(c1h[-48:])
    vwap           = calc_vwap(c1h)
    macd_hist      = calc_macd(c1h)

    above_vwap_rate = bos_up = higher_low = 0
    if len(c1h) >= 6:
        recent           = c1h[-6:]
        above_vwap_rate  = sum(1 for c in recent if c["close"] > vwap) / len(recent)
        bos_up           = detect_bos_up(c1h)
        higher_low       = higher_low_detected(c1h)

    # Volume indicators
    avg_vol_24h = sum(c["volume_usd"] for c in c1h[-24:]) / 24 if len(c1h) >= 24 else 0
    vol_ratio   = c1h[-1]["volume_usd"] / avg_vol_24h if avg_vol_24h > 0 else 0

    avg_vol_6h  = sum(c["volume_usd"] for c in c1h[-6:]) / 6 if len(c1h) >= 6 else 0
    vol_1h_last = c1h[-1]["volume_usd"]

    if len(c1h) >= 4:
        vol_3h   = sum(c["volume_usd"] for c in c1h[-4:-1]) / 3
        vol_accel = (vol_1h_last - vol_3h) / vol_3h if vol_3h > 0 else 0
    else:
        vol_accel = 0

    # ── 8. BASE SCORING (v13.8) ───────────────────────────────
    score, signals = 0, []

    # BBW (coiling)
    if bbw >= 0.12:
        score += CONFIG["score_bbw_12"]; signals.append(f"BBW {bbw:.2f}% (squeeze ekstrem)")
    elif bbw >= 0.10:
        score += CONFIG["score_bbw_10"]; signals.append(f"BBW {bbw:.2f}% (squeeze tinggi)")
    elif bbw >= 0.08:
        score += CONFIG["score_bbw_8"];  signals.append(f"BBW {bbw:.2f}% (coiling)")

    # Price change momentum
    if price_chg >= 2.0:
        score += CONFIG["score_price_2"];  signals.append(f"Price +{price_chg:.1f}% (spike)")
    elif price_chg >= 1.0:
        score += CONFIG["score_price_1"];  signals.append(f"Price +{price_chg:.1f}% (naik)")
    elif price_chg >= 0.5:
        score += CONFIG["score_price_05"]; signals.append(f"Price +{price_chg:.1f}%")

    # VWAP + BOS
    if above_vwap_rate > CONFIG["above_vwap_rate_min"] and bos_up:
        score += CONFIG["score_above_vwap_bos"]; signals.append("Above VWAP + Break of Structure")
    elif above_vwap_rate > CONFIG["above_vwap_rate_min"]:
        score += CONFIG["score_above_vwap"];     signals.append("Above VWAP dominan")

    # RSI
    if rsi >= 65:
        score += CONFIG["score_rsi_65"]; signals.append(f"RSI {rsi:.1f} (overbought)")
    elif rsi >= 55:
        score += CONFIG["score_rsi_55"]; signals.append(f"RSI {rsi:.1f} (bullish)")

    # ATR (volatilitas = bahan pump)
    if atr_pct >= 1.5:
        score += CONFIG["score_atr_15"]; signals.append(f"ATR {atr_pct:.2f}% (volatilitas tinggi)")
    elif atr_pct >= 1.0:
        score += CONFIG["score_atr_10"]; signals.append(f"ATR {atr_pct:.2f}%")

    # Funding stats
    if fstats["neg_pct"] >= 70:
        score += CONFIG["score_funding_neg_pct"]; signals.append(f"Funding negatif {fstats['neg_pct']:.0f}%")
    if fstats["streak"] >= 10:
        score += CONFIG["score_funding_streak"];  signals.append(f"Funding streak negatif {fstats['streak']}")
    if fstats["basis"] <= -0.15:
        score += CONFIG["score_basis"];            signals.append(f"Basis {fstats['basis']:.2f}%")

    # Volume indicators
    if vol_ratio > CONFIG["vol_ratio_threshold"]:
        score += CONFIG["score_vol_ratio_24h"]; signals.append(f"Volume ratio {vol_ratio:.1f}x")
    if vol_accel > CONFIG["vol_accel_threshold"]:
        score += CONFIG["score_vol_accel"];     signals.append(f"Volume accel {vol_accel*100:.0f}%")

    # MACD
    if macd_hist > 0:
        score += CONFIG["score_macd_pos"]; signals.append("MACD histogram positif")

    # Volume spike bonus
    if vol_ratio > CONFIG["vol_spike_threshold"]:
        score += CONFIG["vol_spike_bonus"]; signals.append(f"🔥 Volume spike {vol_ratio:.1f}x rata-rata 24h")

    # Volume rendah penalti
    vol_low_flag = (avg_vol_6h > 0 and vol_1h_last < avg_vol_6h * CONFIG["min_vol_1h_ratio"])
    if vol_low_flag:
        score += CONFIG["vol_low_penalty"]; signals.append(f"⚠️ Volume 1h rendah ({vol_1h_last/avg_vol_6h:.0%} avg 6h)")

    # ── 9. OI LAYER (v9.10) ───────────────────────────────────
    oi_sc, oi_sigs = layer_oi(oi_chg1h, oi_chg24h, oi_valid, chg_24h)
    score   += oi_sc
    signals += oi_sigs

    # ── 10. NET FLOW LAYER (v9.10) ────────────────────────────
    trades = get_trades(symbol, 500)
    nf_sc, nf_sigs, nf_data, nf_block = layer_net_flow(c1h, c15m, trades)
    if nf_block:
        log.info(f"  {symbol}: GATE net flow distribusi sistematis")
        return None
    score   += nf_sc
    signals += nf_sigs

    # ── 11. BTC FILTER — Beta/Alpha (v13.8) ──────────────────
    btc_penalty = btc_bonus = 0
    if btc_trend == "bearish":
        if beta > CONFIG["beta_high_threshold"]:
            btc_penalty = -25; signals.append(f"🚨 BTC turun {btc_change:.1f}% & beta {beta:.2f} — penalti besar")
        elif beta > 1.0:
            btc_penalty = -15; signals.append(f"⚠️ BTC turun {btc_change:.1f}% & beta {beta:.2f}")
        else:
            btc_penalty = -5;  signals.append(f"📉 BTC turun {btc_change:.1f}%")
        if alpha > CONFIG["alpha_positive_threshold"]:
            btc_penalty = max(btc_penalty + 10, 0)
            signals.append(f"✅ Alpha {alpha:.2f}% positif — kurangi dampak BTC")
    elif btc_trend == "bullish":
        if beta > CONFIG["beta_high_threshold"]:
            btc_bonus = 15; signals.append(f"🚀 BTC naik {btc_change:.1f}% & beta {beta:.2f}")
        elif beta > 1.0:
            btc_bonus = 8;  signals.append(f"📈 BTC naik {btc_change:.1f}% & beta {beta:.2f}")
        else:
            btc_bonus = 3;  signals.append(f"✅ BTC naik {btc_change:.1f}%")
        if alpha > CONFIG["alpha_positive_threshold"]:
            btc_bonus += 5; signals.append(f"⭐ Alpha {alpha:.2f}% positif — outperforming BTC")
    if alpha < -CONFIG["alpha_positive_threshold"]:
        btc_penalty -= 10; signals.append(f"⚠️ Alpha {alpha:.2f}% negatif — underperform BTC")
    score += btc_penalty + btc_bonus

    # ── 12. Pump type classification ──────────────────────────
    pump_type = "unknown"
    if above_vwap_rate > CONFIG["above_vwap_rate_min"] and bb_pct > 0.4 and rsi > 45:
        pump_type = "Momentum Breakout (Tipe A)"
    elif (above_vwap_rate < 0.2
            and fstats["cumulative"] < CONFIG["squeeze_funding_cumul"]
            and higher_low):
        pump_type = "Short Squeeze (Tipe B)"
    elif (nf_data.get("72h", {}).get("label") in ("NEUTRAL", "SELL")
            and nf_data.get("6h", {}).get("label") in ("BUY", "STRONG_BUY")
            and oi_chg24h > CONFIG["oi_stealth_oi_min"]):
        pump_type = "Whale Accumulation (Tipe C)"

    # ── 13. Entry calculation ─────────────────────────────────
    if score < CONFIG["min_score_alert"]:
        log.info(f"  {symbol}: Skor {score} < {CONFIG['min_score_alert']}")
        return None

    entry_data = calc_entry(c1h)
    pg_t1  = (entry_data["t1"]     - price_now) / price_now * 100
    pg_t2  = (entry_data["t2"]     - price_now) / price_now * 100
    pg_f1  = (entry_data["fib_t1"] - price_now) / price_now * 100
    pg_f2  = (entry_data["fib_t2"] - price_now) / price_now * 100

    return {
        "symbol":            symbol,
        "score":             score,
        "signals":           signals,
        "entry":             entry_data,
        "price":             price_now,
        "chg_24h":           chg_24h,
        "vol_24h":           vol_24h,
        "rsi":               round(rsi, 1),
        "bbw":               round(bbw, 2),
        "bb_pct":            round(bb_pct, 2),
        "above_vwap_rate":   round(above_vwap_rate * 100, 1),
        "funding_stats":     fstats,
        "oi_chg24h":         round(oi_chg24h, 1),
        "oi_chg1h":          round(oi_chg1h, 1),
        "oi_valid":          oi_valid,
        "nf_data":           nf_data,
        "nf_score":          nf_sc,
        "oi_score":          oi_sc,
        "pump_type":         pump_type,
        "vol_ratio":         round(vol_ratio, 2),
        "vol_accel":         round(vol_accel * 100, 1),
        "macd_hist":         round(macd_hist, 6),
        "potential_gain_t1": round(pg_t1, 1),
        "potential_gain_t2": round(pg_t2, 1),
        "potential_gain_f1": round(pg_f1, 1),
        "potential_gain_f2": round(pg_f2, 1),
        "beta_alpha":        {
            "beta": round(beta, 2), "alpha": round(alpha, 2),
            "r_squared": round(r_squared, 2), "btc_trend": btc_trend, "btc_change": round(btc_change, 1),
        },
        "vol_spike":         vol_ratio > CONFIG["vol_spike_threshold"],
        "vol_low":           vol_low_flag,
    }

# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════
def _flow_icon(label):
    return {"STRONG_BUY": "🟢🟢", "BUY": "🟢", "NEUTRAL": "⚪",
            "SELL": "🔴", "STRONG_SELL": "🔴🔴", "NO_DATA": "❓"}.get(label, "⚪")

def build_alert(r, rank=None):
    rk  = f"#{rank} " if rank else ""
    ba  = r["beta_alpha"]
    e   = r["entry"]
    nfd = r.get("nf_data", {})

    # OI line
    oi_str = ""
    if r.get("oi_valid"):
        oi_str = (f"<b>OI           :</b> 24h={r['oi_chg24h']:+.1f}%  "
                  f"1h={r['oi_chg1h']:+.1f}%  [score:{r['oi_score']:+d}]\n")
    else:
        oi_str = "<b>OI           :</b> ⚠️ baseline belum siap (run ke-1)\n"

    # Net Flow line
    nf_str = ""
    if nfd.get("has_data"):
        f72 = nfd.get("72h", {}); f24 = nfd.get("24h", {})
        f6  = nfd.get("6h",  {}); f15 = nfd.get("15m", {})
        nf_str = (
            f"<b>Net Flow     :</b> [score:{r['nf_score']:+d}]\n"
            f"  {_flow_icon(f72.get('label'))}72h:{f72.get('net_pct',0):+.1f}%  "
            f"{_flow_icon(f24.get('label'))}24h:{f24.get('net_pct',0):+.1f}%  "
            f"{_flow_icon(f6.get('label'))}6h:{f6.get('net_pct',0):+.1f}%  "
            f"{_flow_icon(f15.get('label'))}15m:{f15.get('net_pct',0):+.1f}%\n"
        )

    msg  = f"🚨 <b>PRE-PUMP SIGNAL {rk}— v10.0</b>\n\n"
    msg += f"<b>Symbol       :</b> {r['symbol']}\n"
    msg += f"<b>Pump Type    :</b> {r['pump_type']}\n"
    msg += f"<b>Score        :</b> {r['score']}\n"
    msg += f"<b>Harga        :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h)\n"
    msg += f"<b>RSI          :</b> {r['rsi']}  |  BB Width: {r['bbw']}%  BB Pos: {r['bb_pct']*100:.0f}%\n"
    msg += f"<b>Above VWAP   :</b> {r['above_vwap_rate']}% (6h)  |  Vol ratio: {r['vol_ratio']}x\n"
    msg += f"<b>Funding      :</b> avg={r['funding_stats']['avg']:.6f}  cumul={r['funding_stats']['cumulative']:.4f}  streak={r['funding_stats']['streak']}\n"
    msg += oi_str
    msg += nf_str
    msg += f"<b>BTC          :</b> {ba['btc_trend'].upper()} {ba['btc_change']:+.1f}% (3h)  |  Beta:{ba['beta']}  Alpha:{ba['alpha']:+.2f}  R²:{ba['r_squared']}\n"
    if r.get("vol_spike"):
        msg += f"<b>🔥 Volume Spike</b> terdeteksi!\n"
    if r.get("vol_low"):
        msg += f"<b>⚠️ Volume Rendah</b> — hati-hati aktivitas terbatas\n"

    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📍 <b>ENTRY ZONE</b>\n"
    msg += f"  Support  : ${e['support_used']}\n"
    msg += f"  Entry    : ${e['entry']}  →  ${e['entry_range'][1]}  (rentang 0.3%)\n"
    msg += f"  SL       : ${e['sl']}  (-{e['sl_pct']:.1f}%)\n"
    msg += f"  T1 (Demand): ${e['t1']}  (+{r['potential_gain_t1']}%)\n"
    msg += f"  T2 (Demand): ${e['t2']}  (+{r['potential_gain_t2']}%)\n"
    msg += f"  Fib 1.272 : ${e['fib_t1']}  (+{r['potential_gain_f1']}%)\n"
    msg += f"  Fib 1.618 : ${e['fib_t2']}  (+{r['potential_gain_f2']}%)\n"
    msg += f"  R/R (T1)  : 1:{e['rr']}\n"

    msg += "\n━━━━━━━━━━━━━━━━━━━━\n📊 <b>SINYAL</b>\n"
    for s in r["signals"]:
        msg += f"  • {s}\n"

    msg += f"\n📡 {utc_now()}\n<i>⚠️ Bukan financial advice. Manage risk ketat.</i>"
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES v10.0 — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        vol  = (f"${r['vol_24h']/1e6:.1f}M" if r['vol_24h'] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K")
        nfd  = r.get("nf_data", {})
        f6   = nfd.get("6h", {})
        nf6  = f"{f6.get('net_pct', 0):+.1f}%" if nfd.get("has_data") else "N/A"
        tags  = (" 🔥" if r.get("vol_spike") else "") + (" ⚠️" if r.get("vol_low") else "")
        oi_tag = f" OI24h:{r['oi_chg24h']:+.1f}%" if r.get("oi_valid") else ""
        msg += (f"{i}. <b>{r['symbol']}</b> "
                f"[Score:{r['score']} | T1:+{r['potential_gain_t1']}% | "
                f"Beta:{r['beta_alpha']['beta']} | Alpha:{r['beta_alpha']['alpha']:+.2f} | "
                f"Flow6h:{nf6}{oi_tag}]{tags}\n")
        msg += (f"   {vol} | RSI:{r['rsi']} | BBW:{r['bbw']}% | "
                f"VWAP:{r['above_vwap_rate']}% | VolRatio:{r['vol_ratio']}x | "
                f"Type:{r['pump_type']}\n")
    return msg

# ══════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    all_candidates = []
    not_found      = []
    fstats         = {"cooldown": 0, "manual": 0, "vol_low": 0, "vol_high": 0,
                      "chg_extreme": 0, "price_invalid": 0, "parse_err": 0}

    log.info("=" * 70)
    log.info("🔍 SCANNING: FULL WHITELIST")
    log.info("=" * 70)

    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:
            fstats["manual"] += 1; continue
        if is_cooldown(sym):
            fstats["cooldown"] += 1; continue
        if sym not in tickers:
            not_found.append(sym); continue
        ticker = tickers[sym]
        try:
            vol   = float(ticker.get("quoteVolume", 0))
            chg   = float(ticker.get("change24h", 0)) * 100
            price = float(ticker.get("lastPr", 0))
        except:
            fstats["parse_err"] += 1; continue
        if vol   < CONFIG["pre_filter_vol"]:  fstats["vol_low"]  += 1; continue
        if vol   > CONFIG["max_vol_24h"]:     fstats["vol_high"] += 1; continue
        if abs(chg) > CONFIG["gate_chg_24h_max"]: fstats["chg_extreme"] += 1; continue
        if price <= 0:                        fstats["price_invalid"] += 1; continue
        all_candidates.append((sym, ticker))

    total    = len(WHITELIST_SYMBOLS)
    will_scan = len(all_candidates)
    log.info(f"   Whitelist: {total}  |  Akan di-scan: {will_scan}  |  "
             f"Filter: {total - will_scan}")
    log.info(f"   Not in Bitget:{len(not_found)}  Cooldown:{fstats['cooldown']}  "
             f"Vol<:{fstats['vol_low']}  Vol>:{fstats['vol_high']}  "
             f"Chg>30%:{fstats['chg_extreme']}")
    log.info(f"   Est. scan time: {will_scan * CONFIG['sleep_coins']:.0f}s "
             f"(~{will_scan * CONFIG['sleep_coins'] / 60:.1f} menit)")
    log.info("=" * 70)
    return all_candidates

# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v10.0 — {utc_now()} ===")
    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return
    log.info(f"Total ticker Bitget: {len(tickers)}")
    candidates = build_candidate_list(tickers)
    results    = []

    for i, (sym, t) in enumerate(candidates):
        try:
            vol = float(t.get("quoteVolume", 0))
        except:
            vol = 0
        if vol < CONFIG["min_vol_24h"]:
            continue
        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K)...")
        try:
            res = master_score(sym, t)
            if res:
                ba = res["beta_alpha"]
                nfd = res.get("nf_data", {})
                f6  = nfd.get("6h", {})
                log.info(
                    f"  ✅ Score={res['score']} | OI24h={res['oi_chg24h']:+.1f}% "
                    f"[{'valid' if res['oi_valid'] else 'init'}] | "
                    f"Flow6h={f6.get('net_pct', 0):+.1f}% | "
                    f"Beta={ba['beta']} Alpha={ba['alpha']:+.2f} | "
                    f"T1=+{res['potential_gain_t1']}% | Type={res['pump_type']}"
                )
                results.append(res)
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")
        time.sleep(CONFIG["sleep_coins"])

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"Lolos semua gate: {len(results)} coin")

    if not results:
        log.info("Tidak ada sinyal memenuhi syarat")
        return

    top = results[:CONFIG["max_alerts_per_run"]]
    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)
    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"✅ Alert #{rank}: {r['symbol']} Score={r['score']}")
        time.sleep(2)
    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v10.0 — UNIFIED EDITION             ║")
    log.info("║  Merge: v13.8-FINAL + v9.10                           ║")
    log.info("║  Entry support-based | OI layer | Net Flow multi-TF   ║")
    log.info("║  Beta/Alpha vs BTC | Whale Funnel | Dist. Gate        ║")
    log.info("╚═══════════════════════════════════════════════════════╝")
    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)
    run_scan()
