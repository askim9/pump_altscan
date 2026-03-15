"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PIVOT LOW BOUNCE SCANNER v1.2 (FULL UPGRADE)                          ║
║                                                                          ║
║  UPGRADES:                                                               ║
║  - Support zone width = 2.5%                                            ║
║  - Pivot left/right = 8                                                  ║
║  - Volume ratio threshold = 0.75                                         ║
║  - Volume absorption detection                                          ║
║  - Volatility compression (ATR7/ATR30)                                  ║
║  - Sideways accumulation (range 24h < 8%)                               ║
║  - Liquidity trap wick detection                                        ║
║  - Micro consolidation (6-bar range < 3%)                               ║
║  - Scoring model (0-100) dengan filter ≥60                              ║
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
    "/tmp/scanner_v9.log", maxBytes=10*1024*1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)
log = logging.getLogger(__name__)
log.info("Log file aktif: /tmp/scanner_v9.log (rotasi 10MB)")


# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIG (dengan parameter upgrade)
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────
    "min_composite_alert":       52,        # tidak dipakai lagi, diganti score
    "max_alerts_per_run":         8,

    # ── Volume 24h TOTAL (USD) ─────────────────────────────────
    "min_vol_24h":            3_000,
    "max_vol_24h":       50_000_000,
    "pre_filter_vol":         1_000,

    # ── Gate perubahan harga ───────────────────────────────────
    "gate_chg_24h_max":          30.0,
    "gate_chg_7d_max":           35.0,
    "gate_chg_7d_min":          -35.0,
    "gate_funding_extreme":      -0.002,

    # ── Candle limits ─────────────────────────────────────────
    "candle_1h":                720,   # 30 hari
    "candle_15m":                96,

    # ── Entry/exit ────────────────────────────────────────────
    "min_target_pct":             5.0,
    "max_sl_pct":                12.0,
    "atr_sl_mult":                1.5,
    "atr_t1_mult":                2.5,

    # ── Operasional ───────────────────────────────────────────
    "alert_cooldown_sec":       3600,
    "sleep_coins":               0.8,
    "sleep_error":               3.0,
    "cooldown_file":    "/tmp/v9_cooldown.json",
    "oi_snapshot_file": "/tmp/v9_oi.json",

    # ── Dead Activity Gate ────────────────────────────────────
    "dead_activity_threshold":   0.10,

    # ── PIVOT LOW PARAMETERS (diubah ke 8) ────────────────────
    "pivot_left":                   8,
    "pivot_right":                  8,
    "box_width":                     1.0,
    "recovery_bars":                 3,
    "volume_threshold":              1.8,    # volume pivot > rata-rata positif * ini
    "reversal_vol_threshold":        1.5,    # volume reversal > rata-rata total * ini
    "breakdown_max_bars":            50,

    # ── FILTER VOLUME TERTINGGI (sebelumnya) ──────────────────
    "max_volume_ratio":             0.95,    # masih dipakai untuk filter awal? Kita akan gunakan untuk scoring juga

    # ── PARAMETER UPGRADE BARU ────────────────────────────────
    "support_zone_width":         0.025,      # 2.5% zona support
    "volume_ratio_threshold":     0.75,       # untuk high pivot volume (komponen 20)
    "volume_absorption_threshold": 1.8,       # volume spike > 1.8 * avg20
    "small_price_move_threshold": 0.01,       # perubahan harga < 1%
    "atr_short_period":           7,
    "atr_long_period":            30,
    "volatility_compression_ratio": 0.65,     # ATR7/ATR30 < 0.65
    "sideways_range_threshold":   0.08,       # range 24h < 8%
    "wick_threshold":             0.45,       # lower wick > 45% candle
    "micro_range_threshold":      0.03,       # range 6 candle < 3%
    "score_threshold":            60,         # minimal skor untuk alert
}

# ── STOCK_TICKERS (tidak diubah) ─────────────────────────────────────────
STOCK_TICKERS = {
    "CSCOUSDT","PEPUSDT","QQQUSDT","AAPLUSDT","MSFTUSDT","GOOGLUSDT",
    "INTCUSDT","AMDUSDT","NVDAUSDT","TSLAUSDT","AMZNUSDT","METAUSDT",
    "NFLXUSDT","ADBEUSDT","CRMUSDT","ORCLUSDT","IBMUSDT","SAPUSDT",
    "PYPLUSDT","UBERUSDT","LYFTUSDT","SPYUSDT","DIAUSDT","IWMUSDT",
    "MCDUSDT","KOLUSDT","DISUSDT","BRKUSDT","JPMCUSDT","BACHUSDT",
    "SBUXUSDT","NKEUSDT","WMTUSDT","COSTUSDT","HDUSTUSDT",
    "LLYUSDT","PFIZUSDT","JNJUSDT","ABBVUSDT","MRKUSDT","AMGNUSDT",
    "ASMLUSDT","TSMCUSDT",
    "HOODUSDT","COINUSDT",
    "GSUSDT","MSUSDT","BAMUSDT",
    "SNAPUSDT",
    "FUTUUSDT","TIGRUSDT","MUUSDT","MRVLUSDT","QCOMUSDT","TXNUSDT",
    "SMHUSDT","FOUSDT","GMUSDT","RIVUSDT","LCIDUSDT","NIOOUSDT",
    "RDTUSDT","SPOTUSDT","RBLXUSDT","SHOPUSDT","ETSYUSDT",
    "BABAUSDT","AVGOUSDT","BRKBUSDT","VISAUSDT","MAUSDT","ABNBUSDT","AIRBNBUSDT",
    "RDDTUSDT","RDDUSDT","PLTRUSDT","MSTRUSDT","SOFIUSDT","NUSDT",
    "AFRMUSDT","UPSTUSDT","CARVAUSDT","IONQUSDT","ARQITUSDT","ROBHUSDT",
}

MANUAL_EXCLUDE = set()

# ══════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin pilihan (sama)
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

SECTOR_MAP = {
    "DEFI": [
        "SNXUSDT","ENSOUSDT","SIRENUSDT","CRVUSDT","CVXUSDT","COMPUSDT",
        "AAVEUSDT","UNIUSDT","DYDXUSDT","COWUSDT","PENDLEUSDT","MORPHOUSDT",
        "FLUIDUSDT","SSVUSDT","LRCUSDT","RSRUSDT","NMRUSDT","UMAUSDT","BALUSDT",
        "LDOUSDT","ENSUSDT",
    ],
    "ZK_PRIVACY": ["AZTECUSDT","MINAUSDT","STRKUSDT","ZORAUSDT","ZRXUSDT","POLYXUSDT"],
    "DESCI":      ["BIOUSDT","ATHUSDT"],
    "AI_CRYPTO":  [
        "FETUSDT","RENDERUSDT","TAOUSDT","GRASSUSDT","AKTUSDT","VANAUSDT",
        "COAIUSDT","UAIUSDT","GRTUSDT","OCEANUSDT","AGIXUSDT",
    ],
    "SOLANA_ECO": [
        "ORCAUSDT","RAYUSDT","JTOUSDT","DRIFTUSDT","WIFUSDT","JUPUSDT",
        "1000BONKUSDT","PYTHUSDT","MEWUSDT",
    ],
    "LAYER1": [
        "APTUSDT","SUIUSDT","SEIUSDT","INJUSDT","KASUSDT","BERAUSDT",
        "MOVEUSDT","KAIAUSDT","TIAUSDT","EGLDUSDT","NEARUSDT","TONUSDT",
        "ALGOUSDT","HBARUSDT","STEEMUSDT","XTZUSDT","ZILUSDT","VETUSDT",
        "ESPUSDT","TRXUSDT",
    ],
    "LAYER2": ["ARBUSDT","OPUSDT","CELOUSDT","STRKUSDT","LDOUSDT","POLUSDT","LINEAUSDT"],
    "GAMING": [
        "AXSUSDT","GALAUSDT","IMXUSDT","SANDUSDT","APEUSDT","SUPERUSDT",
        "CHZUSDT","ENJUSDT","GLMUSDT",
    ],
    "LOW_CAP": [
        "VVVUSDT","POWERUSDT","ARCUSDT","AGLDUSDT","VIRTUALUSDT","SPXUSDT",
        "ONDOUSDT","ENAUSDT","EIGENUSDT","STXUSDT","RUNEUSDT","ORDIUSDT",
        "SKRUSDT","BRETTUSDT","AVNTUSDT","AEROUSDT",
    ],
    "MEME": [
        "PEPEUSDT","SHIBUSDT","FLOKIUSDT","BRETTUSDT","FARTCOINUSDT",
        "MEMEUSDT","TURBOUSDT","PNUTUSDT","POPCATUSDT","MOODENGUSDT",
        "1000BONKUSDT","TRUMPUSDT","WIFUSDT","TOSHIUSDT",
    ],
}
SECTOR_LOOKUP = {coin: sec for sec, coins in SECTOR_MAP.items() for coin in coins}

BITGET_BASE    = "https://api.bitget.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_cache         = {}

EXCLUDED_KEYWORDS = ["XAU","PAXG","BTC","ETH","USDC","DAI","BUSD","UST","LUNC","LUNA"]


# ══════════════════════════════════════════════════════════════
#  🔒  COOLDOWN & OI SNAPSHOT (sama)
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
#  📡  DATA FETCHERS (sama)
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

def get_liquidations(symbol):
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/liquidation-orders",
        params={"symbol": symbol, "productType": "usdt-futures",
                "pageSize": "100"},
    )
    if not data or data.get("code") != "00000":
        return 0, 0
    try:
        orders = data.get("data", {}).get("liquidationOrderList", [])
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - CONFIG.get("liq_window_min", 30) * 60 * 1000
        long_liq  = 0.0
        short_liq = 0.0
        for o in orders:
            ts   = int(o.get("cTime", 0))
            if ts < cutoff:
                continue
            usd  = float(o.get("size", 0)) * float(o.get("fillPrice", 0))
            side = o.get("side", "").lower()
            if "sell" in side:
                long_liq += usd
            else:
                short_liq += usd
        return long_liq, short_liq
    except:
        return 0, 0

def get_rsi(candles, period=14):
    if len(candles) < period + 1:
        return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))

def get_cg_trending():
    key = "cg_trend"
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < 600:
            return val
    data   = safe_get(f"{COINGECKO_BASE}/search/trending")
    result = [c["item"]["symbol"].upper() for c in (data or {}).get("coins", [])]
    _cache[key] = (time.time(), result)
    return result


# ══════════════════════════════════════════════════════════════
#  📐  MATH HELPERS (sama, termasuk find_pivot_lows)
# ══════════════════════════════════════════════════════════════
def bbw_percentile(candles, period=20):
    closes = [c["close"] for c in candles]
    if len(closes) < period + 10:
        return 0, 50
    bbws = []
    for i in range(period - 1, len(closes)):
        w    = closes[i - period + 1: i + 1]
        mean = sum(w) / period
        std  = math.sqrt(sum((x - mean) ** 2 for x in w) / period)
        bbws.append((4 * std / mean * 100) if mean else 0)
    if not bbws:
        return 0, 50
    cur = bbws[-1]
    pct = sum(1 for b in bbws[:-1] if b < cur) / max(len(bbws) - 1, 1) * 100
    return cur, pct

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

def find_pivot_lows(candles, left=20, right=20):
    pivots = []
    for i in range(left, len(candles) - right):
        if candles[i]["low"] < candles[i-1]["low"] and candles[i]["low"] < candles[i+1]["low"]:
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


# ══════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE (dengan upgrade scoring)
# ══════════════════════════════════════════════════════════════
def master_score(symbol, ticker, tickers_dict):
    # Ambil data candle 1h dengan limit 720 (30 hari)
    c1h = get_candles(symbol, "1h", CONFIG["candle_1h"])
    if len(c1h) < 50:
        return None

    try:
        chg_24h = float(ticker.get("change24h", 0)) * 100
        vol_24h = float(ticker.get("quoteVolume", 0))
    except:
        chg_24h, vol_24h = 0, 0

    # Dead activity gate
    if len(c1h) >= 7:
        last_vol = c1h[-1]["volume_usd"]
        avg_vol_6h = sum(c["volume_usd"] for c in c1h[-7:-1]) / 6
        if avg_vol_6h > 0 and last_vol / avg_vol_6h < CONFIG["dead_activity_threshold"]:
            log.info(f"  {symbol}: GATE dead activity")
            return None
    else:
        avg_vol_6h = 0

    # Hitung ATR dan rata-rata volume
    atr = calc_atr(c1h, 14) or c1h[-1]["close"] * 0.02
    avg_vol = sum(c["volume_usd"] for c in c1h) / len(c1h)
    pos_vols = [c["volume_usd"] for c in c1h if c["close"] > c["open"]]
    avg_pos_vol = sum(pos_vols) / len(pos_vols) if pos_vols else avg_vol

    # Cari semua pivot low dalam 30 hari (dengan left/right baru = 8)
    pivot_indices = find_pivot_lows(c1h, left=CONFIG["pivot_left"], right=CONFIG["pivot_right"])

    # Hitung volume maksimum dari semua pivot (untuk perbandingan)
    max_pivot_vol = 0
    for idx in pivot_indices:
        vol = c1h[idx]["volume_usd"]
        if vol > max_pivot_vol:
            max_pivot_vol = vol

    # Kumpulkan sinyal dari setiap pivot yang memenuhi syarat awal
    signals_found = []
    for idx in pivot_indices:
        pivot_candle = c1h[idx]
        pivot_low = pivot_candle["low"]

        # Syarat 1: volume pivot signifikan
        if pivot_candle["volume_usd"] < avg_pos_vol * CONFIG["volume_threshold"]:
            continue

        # Batas bawah support zone
        support_low = pivot_low - atr * CONFIG["box_width"]

        # Cari breakdown dalam jendela ke depan
        for j in range(idx + 1, min(idx + CONFIG["breakdown_max_bars"], len(c1h))):
            if c1h[j]["low"] < support_low:
                for k in range(j + 1, min(j + CONFIG["recovery_bars"] + 1, len(c1h))):
                    if c1h[k]["close"] > pivot_low:
                        if c1h[k]["volume_usd"] > avg_vol * CONFIG["reversal_vol_threshold"]:
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
                        break
                break

    if not signals_found:
        return None

    # Filter volume tertinggi (masih dipertahankan, tapi kita akan gunakan untuk scoring juga)
    min_required_vol = max_pivot_vol * CONFIG["max_volume_ratio"]   # 0.95
    signals_found = [s for s in signals_found if s["pivot_vol"] >= min_required_vol]
    if not signals_found:
        return None

    # Ambil sinyal terbaru (reversal terakhir)
    latest_signal = max(signals_found, key=lambda x: x["reversal_idx"])

    # Hitung entry, SL, TP
    entry_data = calc_entry(c1h)
    if not entry_data or entry_data["liq_pct"] < CONFIG["min_target_pct"]:
        return None

    # Hitung RVOL
    if len(c1h) >= 25:
        last_vol = c1h[-2]["volume_usd"]
        target_hour = (c1h[-2]["ts"] // 3_600_000) % 24
        same_hour_vols = [c["volume_usd"] for c in c1h[:-2] if (c["ts"] // 3_600_000) % 24 == target_hour]
        avg_same_hour = sum(same_hour_vols) / len(same_hour_vols) if same_hour_vols else 1
        rvol = last_vol / avg_same_hour if avg_same_hour > 0 else 1
    else:
        rvol = 1

    # ====================== METRIK TAMBAHAN UNTUK SCORING ======================
    price_now = c1h[-1]["close"]
    pivot_low = latest_signal["pivot_low"]
    distance_to_support = abs(price_now - pivot_low) / pivot_low

    # Pivot volume ratio (terhadap maksimum semua pivot)
    pivot_volume_ratio = latest_signal["pivot_vol"] / max_pivot_vol if max_pivot_vol > 0 else 0

    # Volatility ratio ATR7/ATR30
    atr_short = calc_atr(c1h, CONFIG["atr_short_period"]) or 0
    atr_long = calc_atr(c1h, CONFIG["atr_long_period"]) or 1
    volatility_ratio = atr_short / atr_long if atr_long != 0 else 0
    volatility_compression = volatility_ratio < CONFIG["volatility_compression_ratio"]

    # Range 24h
    if len(c1h) >= 24:
        high_24h = max(c["high"] for c in c1h[-24:])
        low_24h = min(c["low"] for c in c1h[-24:])
        range_24h = (high_24h - low_24h) / price_now
    else:
        range_24h = 999
    sideways = range_24h < CONFIG["sideways_range_threshold"]

    # Volume absorption (cek 20 candle terakhir)
    absorption_detected = False
    if len(c1h) >= 20:
        avg_vol_20 = sum(c["volume_usd"] for c in c1h[-20:]) / 20
        for c in c1h[-20:]:
            if c["volume_usd"] > CONFIG["volume_absorption_threshold"] * avg_vol_20:
                if abs(c["close"] - c["open"]) / c["close"] < CONFIG["small_price_move_threshold"]:
                    absorption_detected = True
                    break

    # Wick ratio pada candle reversal
    rev_idx = latest_signal["reversal_idx"]
    rev_candle = c1h[rev_idx]
    lower_wick = min(rev_candle["open"], rev_candle["close"]) - rev_candle["low"]
    candle_range = rev_candle["high"] - rev_candle["low"]
    wick_ratio = lower_wick / candle_range if candle_range > 0 else 0
    liquidity_trap = wick_ratio > CONFIG["wick_threshold"]

    # Micro range 6 candle terakhir
    if len(c1h) >= 6:
        high_6 = max(c["high"] for c in c1h[-6:])
        low_6 = min(c["low"] for c in c1h[-6:])
        micro_range = (high_6 - low_6) / price_now
    else:
        micro_range = 999
    micro_consolidation = micro_range < CONFIG["micro_range_threshold"]

    # ====================== PENGHITUNGAN SKOR ======================
    score = 0
    score += 40  # Support bounce detected (sinyal inti)

    if pivot_volume_ratio >= CONFIG["volume_ratio_threshold"]:
        score += 20  # High pivot volume

    if absorption_detected:
        score += 10

    if volatility_compression:
        score += 10

    if sideways:
        score += 10

    if liquidity_trap:
        score += 5

    if micro_consolidation:
        score += 5

    # Jika skor di bawah threshold, discard sinyal
    if score < CONFIG["score_threshold"]:
        return None

    # Sinyal teks (tambahkan informasi skor dan metrik)
    from datetime import datetime
    def ts_to_str(ts_ms):
        return datetime.fromtimestamp(ts_ms/1000).strftime("%d %H:%M")
    signal_texts = [
        f"Pivot low ${latest_signal['pivot_low']:.4f} (vol {latest_signal['pivot_vol']/1e3:.0f}K)",
        f"Breakdown di {ts_to_str(latest_signal['break_time'])}",
        f"Reversal di {ts_to_str(latest_signal['reversal_time'])} dengan vol {latest_signal['reversal_vol']/1e3:.0f}K",
        f"Skor: {score} | Vol ratio: {pivot_volume_ratio:.2f} | Wick: {wick_ratio:.2f}",
    ]

    funding = get_funding(symbol)

    # Range 6h (untuk log, tidak dipakai scoring)
    if len(c1h) >= 6:
        pre6 = c1h[-6:]
        high_6h = max(c["high"] for c in pre6)
        low_6h = min(c["low"] for c in pre6)
        range_6h = (high_6h - low_6h) / low_6h * 100 if low_6h > 0 else 0
    else:
        range_6h = 0

    # Susun hasil dengan field-field baru
    result = {
        "symbol": symbol,
        "score": score,                     # skor baru (0-100)
        "composite_score": score,           # untuk kompatibilitas dengan sorting dan alert
        "signals": signal_texts,
        "ws": 0,
        "wev": [],
        "entry": entry_data,
        "sector": SECTOR_LOOKUP.get(symbol, "N/A"),
        "funding": funding,
        "bd": {"oi_valid": False, "rsi_1h": 50},
        "price": price_now,
        "chg_24h": chg_24h,
        "vol_24h": vol_24h,
        "rvol": round(rvol, 1),
        "ls_ratio": None,
        "chg_7d": 0,
        "avg_vol_6h": avg_vol_6h,
        "range_6h": range_6h,
        "coiling": 0,
        "bbw_val": 0,
        "oi_change_24h": 0,
        "oi_change_1h": 0,
        "prob_score": score / 100,
        "prob_class": "Pivot Bounce",
        "prob_metrics": {},
        "rsi_1h": 50,
        "long_liq": 0,
        "short_liq": 0,
        "linea_components": 0,
        "oi_accel_score": 0,
        "oi_accel_data": {},
        "nf_data": {},
        "nf_score": 0,
        # Field tambahan untuk output
        "distance_to_support": distance_to_support,
        "pivot_volume_ratio": pivot_volume_ratio,
        "volatility_ratio": volatility_ratio,
        "range_24h": range_24h,
        "absorption_detected": absorption_detected,
        "wick_ratio": wick_ratio,
        "micro_range": micro_range,
    }
    return result


# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER (diperbarui untuk menampilkan skor dan metrik)
# ══════════════════════════════════════════════════════════════
def build_alert(r, rank=None):
    sc   = r["score"]
    comp = r.get("composite_score", sc)
    bar  = "█" * int(comp / 5) + "░" * (20 - int(comp / 5))
    e    = r["entry"]
    rk   = f"#{rank} " if rank else ""
    vol  = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K")

    msg = (
        f"🚨 <b>PIVOT BOUNCE {rk}— v1.2 (UPGRADE)</b>\n\n"
        f"<b>Symbol    :</b> {r['symbol']}\n"
        f"<b>Skor      :</b> {sc}/100  {bar}\n"
        f"<b>Harga     :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h)\n"
        f"<b>Vol 24h   :</b> {vol} | RVOL: {r['rvol']:.1f}x\n"
        f"<b>Funding   :</b> {r['funding']:.5f}\n"
        f"<b>Distance to support:</b> {r['distance_to_support']*100:.2f}%\n"
        f"<b>Pivot vol ratio:</b> {r['pivot_volume_ratio']:.2f}\n"
        f"<b>Volatility ratio:</b> {r['volatility_ratio']:.2f}\n"
        f"<b>Range 24h:</b> {r['range_24h']*100:.2f}%\n"
        f"<b>Absorption:</b> {'✅' if r['absorption_detected'] else '❌'}\n"
        f"<b>Wick ratio:</b> {r['wick_ratio']:.2f}\n"
        f"<b>Micro range:</b> {r['micro_range']*100:.2f}%\n\n"
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

    msg += f"\n🕐 {utc_now()}\n<i>⚠️ Bukan financial advice.</i>"
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP PIVOT BOUNCE — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        comp     = r.get("composite_score", r["score"])
        bar      = "█" * int(comp / 10) + "░" * (10 - int(comp / 10))
        vol      = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K")
        t1p      = r["entry"]["liq_pct"] if r.get("entry") else 0
        msg += (
            f"{i}. <b>{r['symbol']}</b> [C:{comp} {bar}]\n"
            f"   RVOL:{r['rvol']:.1f}x | {vol} | T1:+{t1p:.0f}%\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST (sama)
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
#  🚀  MAIN SCAN (sedikit modifikasi pada filter)
# ══════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PIVOT LOW BOUNCE SCANNER v1.2 (UPGRADE) — {utc_now()} ===")

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
            res = master_score(sym, t, tickers)
            if res:
                comp = res["composite_score"]
                log.info(f"  Score={res['score']} Comp={comp} RVOL={res['rvol']:.1f}x T1=+{res['entry']['liq_pct']:.1f}%")
                # Tidak perlu filter tambahan karena sudah di master_score
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
            log.info(f"✅ Alert #{rank}: {r['symbol']} C={r['composite_score']}")
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")


# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔═══════════════════════════════════════════════════╗")
    log.info("║  PIVOT LOW BOUNCE SCANNER v1.2 (UPGRADE)         ║")
    log.info("║  - Support zone, pivot=8, volume ratio           ║")
    log.info("║  - Absorption, volatility, sideways, wick, micro ║")
    log.info("║  - Scoring model dengan filter ≥60               ║")
    log.info("╚═══════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)

    run_scan()
