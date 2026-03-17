"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PIVOT BOUNCE SCANNER v3.2 — PRE-PUMP DETECTION + VELOCITY FILTER          ║
║                      (PERBAIKAN BUG & PENYELARASAN)                         ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging
import logging.handlers as _lh
from datetime import datetime, timezone
from collections import defaultdict

# ─── env ──────────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ─── logging ──────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch = logging.StreamHandler();  _ch.setFormatter(_fmt); _root.addHandler(_ch)
_fh = _lh.RotatingFileHandler("/tmp/scanner_v2.log", maxBytes=10*1024*1024, backupCount=3)
_fh.setFormatter(_fmt); _root.addHandler(_fh)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── volume 24h filter ─────────────────────────────────────────────────────
    "min_vol_24h":            500_000,
    "max_vol_24h":         800_000_000,
    "pre_filter_vol":         100_000,

    # ── price change gate ─────────────────────────────────────────────────────
    "gate_chg_24h_max":            40.0,

    # ── RVOL minimum gate ─────────────────────────────────────────────────────
    "min_rvol_gate":               1.5,

    # ── minimum R/R dan SL ────────────────────────────────────────────────────
    "min_rr":                      1.5,
    "min_sl_pct":                  2.5,

    # ── candle config ─────────────────────────────────────────────────────────
    "candle_limit_1h":            720,

    # ── COMPRESSION DETECTION ─────────────────────────────────────────────────
    "compression_min_candles":     16,
    "compression_max_candles":    672,
    "compression_range_pct":      0.11,
    "short_compression_range_pct": 0.08,
    "compression_lookback":       672,

    # ── ZONE PURITY CHECK (v3.2) ──────────────────────────────────────────────
    "zone_purity_vol_mult":        3.0,
    "zone_purity_spike_max":       1,

    # ── CHOPPY FILTER ─────────────────────────────────────────────────────────
    "compression_choppy_max":      0.02,

    # ── VOLUME AWAKENING ──────────────────────────────────────────────────────
    "awakening_vol_mult":          1.8,
    "awakening_lookback_candles":    3,
    "strong_awakening_mult":        3.0,
    "mega_awakening_mult":          6.0,

    # ── NOT TOO LATE GATE ─────────────────────────────────────────────────────
    "max_rise_from_low_pct":       0.12,
    "max_rise_warn_pct":           0.06,

    # ── SUPPORT PROXIMITY ─────────────────────────────────────────────────────
    "support_proximity_pct":       0.06,

    # ── TREND CONTEXT GATE ────────────────────────────────────────────────────
    "price_below_zone_max":        0.03,

    # ── POST-PUMP DETECTION GATE (BARU v2.6) ─────────────────────────────────
    "post_pump_vol_mult":            7.0,
    "post_pump_lookback_candles":      6,

    # ── POST-PUMP 48H GATE (v2.8) ─────────────────────────────────────────────
    "post_pump_lookback_48h":        48,
    "post_pump_vol_mult_48h":       4.0,

    # ── G3: BREAKOUT GATE (v2.6) ──────────────────────────────────────────────
    "price_above_zone_max":          0.03,

    # ── LIQUIDITY SWEEP BONUS ─────────────────────────────────────────────────
    "liq_sweep_lookback":           12,
    "liq_sweep_recover_bars":        4,

    # ── FUNDING ───────────────────────────────────────────────────────────────
    "funding_gate":              -0.003,

    # ── ENTRY / TARGET ────────────────────────────────────────────────────────
    "atr_sl_mult":                  1.2,
    "min_target_pct":               8.0,

    # ── SCORING THRESHOLD ─────────────────────────────────────────────────────
    "score_threshold":             45,

    # ── PUMP VELOCITY FILTER (BARU v3.0) ──────────────────────────────────────
    "velocity_fast_vol_mult":      50.0,
    "velocity_fast_body_pct":      0.40,
    "velocity_mid_vol_mult":       10.0,
    "velocity_mid_body_pct":       0.50,   # tidak dipakai langsung, lihat fungsi
    "velocity_highvol_override":  200.0,

    # ── OPERASIONAL ───────────────────────────────────────────────────────────
    "max_alerts_per_run":           8,
    "alert_cooldown_sec":        3600,
    "sleep_coins":                 0.7,
    "sleep_error":                 3.0,
    "cooldown_file":     "/tmp/v3_cooldown.json",
    "scan_interval_sec":          900,
}

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin (sama seperti asli, tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
      "4USDT", "0GUSDT", "1000BONKUSDT", "1000PEPEUSDT", "1000RATSUSDT",
    "1000SHIBUSDT", "1000XECUSDT", "1INCHUSDT", "1MBABYDOGEUSDT", "2ZUSDT",
    "AAVEUSDT", "ACEUSDT", "ACHUSDT", "ACTUSDT", "ADAUSDT", "AEROUSDT",
    "AGLDUSDT", "AINUSDT", "AIOUSDT", "AIXBTUSDT", "AKTUSDT", "ALCHUSDT",
    "ALGOUSDT", "ALICEUSDT", "ALLOUSDT", "ALTUSDT", "ANIMEUSDT",
    "ANKRUSDT", "APEUSDT", "APEXUSDT", "API3USDT", "APRUSDT", "APTUSDT",
    "ARUSDT", "ARBUSDT", "ARCUSDT", "ARIAUSDT", "ARKUSDT", "ARKMUSDT",
    "ARPAUSDT", "ASTERUSDT", "ATUSDT", "ATHUSDT", "ATOMUSDT", "AUCTIONUSDT",
    "AVAXUSDT", "AVNTUSDT", "AWEUSDT", "AXLUSDT", "AXSUSDT", "AZTECUSDT",
    "BUSDT", "B2USDT", "BABYUSDT", "BANUSDT", "BANANAUSDT",
    "BANANAS31USDT", "BANKUSDT", "BARDUSDT", "BATUSDT", "BCHUSDT", "BEATUSDT",
    "BERAUSDT", "BGBUSDT", "BIGTIMEUSDT", "BIOUSDT", "BIRBUSDT", "BLASTUSDT",
    "BLESSUSDT", "BLURUSDT", "BNBUSDT", "BOMEUSDT", "BRETTUSDT", "BREVUSDT",
    "BROCCOLIUSDT", "BSVUSDT", "BTCUSDT", "BULLAUSDT", "C98USDT", "CAKEUSDT",
    "CCUSDT", "CELOUSDT", "CFXUSDT", "CHILLGUYUSDT", "CHZUSDT", "CLUSDT",
    "CLANKERUSDT", "CLOUSDT", "COAIUSDT", "COMPUSDT", "COOKIEUSDT",
    "COWUSDT", "CRCLUSDT", "CROUSDT", "CROSSUSDT", "CRVUSDT", "CTKUSDT",
    "CVCUSDT", "CVXUSDT", "CYBERUSDT", "CYSUSDT", "DASHUSDT", "DEEPUSDT",
    "DENTUSDT", "DEXEUSDT", "DOGEUSDT", "DOLOUSDT", "DOODUSDT", "DOTUSDT",
    "DRIFTUSDT", "DYDXUSDT", "DYMUSDT", "EGLDUSDT", "EIGENUSDT", "ENAUSDT",
    "ENJUSDT", "ENSUSDT", "ENSOUSDT", "EPICUSDT", "ESPUSDT", "ETCUSDT",
    "ETHUSDT", "ETHFIUSDT", "FUSDT", "FARTCOINUSDT", "FETUSDT",
    "FFUSDT", "FIDAUSDT", "FILUSDT", "FLOKIUSDT", "FLUIDUSDT", "FOGOUSDT",
    "FOLKSUSDT", "FORMUSDT", "GALAUSDT", "GASUSDT", "GIGGLEUSDT",
    "GLMUSDT", "GMTUSDT", "GMXUSDT", "GOATUSDT", "GPSUSDT", "GRASSUSDT", "GUSDT",
    "GRIFFAINUSDT", "GRTUSDT", "GUNUSDT", "GWEIUSDT", "HUSDT", "HBARUSDT",
    "HEIUSDT", "HEMIUSDT", "HMSTRUSDT", "HOLOUSDT", "HOMEUSDT",     "HYPEUSDT", "HYPERUSDT", "ICNTUSDT", "ICPUSDT", "IDOLUSDT", "ILVUSDT",
    "IMXUSDT", "INITUSDT", "INJUSDT", "INXUSDT", "IOUSDT",
    "IOTAUSDT", "IOTXUSDT", "IPUSDT", "JASMYUSDT", "JCTUSDT", "JSTUSDT",
    "JTOUSDT", "JUPUSDT", "KAIAUSDT", "KAITOUSDT", "KASUSDT", "KAVAUSDT",
    "kBONKUSDT", "KERNELUSDT", "KGENUSDT", "KITEUSDT", "kPEPEUSDT", "kSHIBUSDT",
    "LAUSDT", "LABUSDT", "LAYERUSDT", "LDOUSDT", "LIGHTUSDT", "LINEAUSDT",
    "LINKUSDT", "LITUSDT", "LPTUSDT", "LSKUSDT", "LTCUSDT", "LUNAUSDT",
    "LUNCUSDT", "LYNUSDT", "MUSDT", "MAGICUSDT", "MAGMAUSDT", "MANAUSDT",
    "MANTAUSDT", "MANTRAUSDT", "MASKUSDT", "MAVUSDT", "MAVIAUSDT", "MBOXUSDT",
    "MEUSDT", "MEGAUSDT", "MELANIAUSDT", "MEMEUSDT", "MERLUSDT", "METUSDT",
    "METAUSDT", "MEWUSDT", "MINAUSDT", "MMTUSDT", "MNTUSDT", "MONUSDT",
    "MOODENGUSDT", "MORPHOUSDT", "MOVEUSDT", "MOVRUSDT",     "MUUSDT", "MUBARAKUSDT", "MYXUSDT", "NAORISUSDT", "NEARUSDT", "NEIROCTOUSDT",
    "NEOUSDT", "NEWTUSDT", "NILUSDT", "NMRUSDT", "NOMUSDT", "NOTUSDT",
    "NXPCUSDT", "ONDOUSDT", "ONGUSDT", "ONTUSDT", "OPUSDT", "OPENUSDT",
    "OPNUSDT", "ORCAUSDT", "ORDIUSDT", "OXTUSDT", "PARTIUSDT",     "PENDLEUSDT", "PENGUUSDT", "PEOPLEUSDT", "PEPEUSDT", "PHAUSDT", "PIEVERSEUSDT",
    "PIPPINUSDT", "PLUMEUSDT", "PNUTUSDT", "POLUSDT", "POLYXUSDT",
    "POPCATUSDT", "POWERUSDT", "PROMPTUSDT", "PROVEUSDT", "PUMPUSDT", "PURRUSDT",
    "PYTHUSDT", "QUSDT", "QNTUSDT", "RAVEUSDT", "RAYUSDT",     "RECALLUSDT", "RENDERUSDT", "RESOLVUSDT", "REZUSDT", "RIVERUSDT", "ROBOUSDT",
    "ROSEUSDT", "RPLUSDT", "RSRUSDT", "RUNEUSDT", "SUSDT", "SAGAUSDT", "SAHARAUSDT",
    "SANDUSDT", "SAPIENUSDT", "SEIUSDT", "SENTUSDT", "SHIBUSDT", "SIGNUSDT",
    "SIRENUSDT", "SKHYNIXUSDT", "SKRUSDT", "SKYUSDT", "SKYAIUSDT", "SLPUSDT",
    "SNXUSDT", "SOLUSDT", "SOMIUSDT", "SONICUSDT", "SOONUSDT", "SOPHUSDT",
    "SPACEUSDT", "SPKUSDT", "SPXUSDT", "SQDUSDT", "SSVUSDT",
    "STBLUSDT", "STEEMUSDT", "STOUSDT", "STRKUSDT", "STXUSDT",
    "SUIUSDT", "SUNUSDT", "SUPERUSDT", "SUSHIUSDT", "SYRUPUSDT", "TUSDT",
    "TACUSDT", "TAGUSDT", "TAIKOUSDT", "TAOUSDT", "THEUSDT", "THETAUSDT",
    "TIAUSDT", "TNSRUSDT", "TONUSDT", "TOSHIUSDT", "TOWNSUSDT", "TRBUSDT",
    "TRIAUSDT", "TRUMPUSDT", "TRXUSDT", "TURBOUSDT", "UAIUSDT", "UBUSDT",
    "UMAUSDT", "UNIUSDT", "USUSDT", "USDKRWUSDT", "USELESSUSDT",
    "USUALUSDT", "VANAUSDT", "VANRYUSDT", "VETUSDT", "VINEUSDT", "VIRTUALUSDT",
    "VTHOUSDT", "VVVUSDT", "WUSDT", "WALUSDT", "WAXPUSDT", "WCTUSDT", "WETUSDT",
    "WIFUSDT", "WLDUSDT", "WLFIUSDT", "WOOUSDT", "WTIUSDT", "XAIUSDT",
"XCUUSDT", "XDCUSDT", "XLMUSDT", "XMRUSDT", "XPDUSDT", "XPINUSDT",
    "XPLUSDT", "XRPUSDT", "XTZUSDT", "XVGUSDT", "YGGUSDT", "YZYUSDT", "ZAMAUSDT",
    "ZBTUSDT", "ZECUSDT", "ZENUSDT", "ZEREBROUSDT", "ZETAUSDT", "ZILUSDT",
    "ZKUSDT", "ZKCUSDT", "ZKJUSDT", "ZKPUSDT", "ZORAUSDT", "ZROUSDT",
}

MANUAL_EXCLUDE = set()

SECTOR_MAP = {
    "DEFI":      ["SNXUSDT","CRVUSDT","CVXUSDT","COMPUSDT","AAVEUSDT","UNIUSDT","DYDXUSDT",
                  "COWUSDT","PENDLEUSDT","MORPHOUSDT","FLUIDUSDT","SSVUSDT","LDOUSDT","ENSUSDT"],
    "AI_CRYPTO": ["FETUSDT","RENDERUSDT","TAOUSDT","GRASSUSDT","AKTUSDT","VANAUSDT",
                  "COAIUSDT","UAIUSDT","GRTUSDT"],
    "SOLANA_ECO":["ORCAUSDT","RAYUSDT","JTOUSDT","DRIFTUSDT","WIFUSDT","JUPUSDT",
                  "1000BONKUSDT","PYTHUSDT"],
    "LAYER1":    ["APTUSDT","SUIUSDT","SEIUSDT","INJUSDT","KASUSDT","BERAUSDT","MOVEUSDT",
                  "KAIAUSDT","TIAUSDT","EGLDUSDT","NEARUSDT","TONUSDT","ALGOUSDT","HBARUSDT"],
    "LAYER2":    ["ARBUSDT","OPUSDT","CELOUSDT","STRKUSDT","POLUSDT","LINEAUSDT"],
    "GAMING":    ["AXSUSDT","GALAUSDT","IMXUSDT","SANDUSDT","APEUSDT","SUPERUSDT","CHZUSDT","ENJUSDT"],
    "MEME":      ["PEPEUSDT","SHIBUSDT","FLOKIUSDT","BRETTUSDT","FARTCOINUSDT","MEMEUSDT",
                  "TURBOUSDT","PNUTUSDT","POPCATUSDT","MOODENGUSDT","1000BONKUSDT","TRUMPUSDT","WIFUSDT"],
}
SECTOR_LOOKUP = {coin: sec for sec, coins in SECTOR_MAP.items() for coin in coins}

BITGET_BASE = "https://api.bitget.com"
GRAN_MAP    = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
_cache      = {}

# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
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

def is_cooldown(sym):  return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]
def set_cooldown(sym): _cooldown[sym] = time.time(); save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=12):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log.warning("Rate limit — tunggu 20s")
                time.sleep(20)
                continue
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

def utc_now(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ══════════════════════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def get_all_tickers():
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/tickers",
                    params={"productType": "usdt-futures"})
    if data and data.get("code") == "00000":
        return {t["symbol"]: t for t in data.get("data", [])}
    return {}

def get_candles(symbol, gran="1h", limit=504):
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
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
                    params={"symbol": symbol, "productType": "usdt-futures"})
    if data and data.get("code") == "00000":
        try:
            return float(data["data"][0].get("fundingRate", 0))
        except:
            pass
    return 0.0

# ══════════════════════════════════════════════════════════════════════════════
#  📐  MATH HELPERS (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr

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
    return 100 - (100 / (1 + avg_g / avg_l))

def calc_poc(candles):
    """Point of Control — price level with highest traded volume."""
    if not candles:
        return None
    pmin = min(c["low"]  for c in candles)
    pmax = max(c["high"] for c in candles)
    if pmax == pmin:
        return candles[-1]["close"]
    bsize   = (pmax - pmin) / 40
    vol_bkt = defaultdict(float)
    for c in candles:
        lo = int((c["low"]  - pmin) / bsize)
        hi = int((c["high"] - pmin) / bsize)
        nb = max(hi - lo + 1, 1)
        for b in range(lo, hi + 1):
            vol_bkt[b] += c["volume_usd"] / nb
    poc_b = max(vol_bkt, key=vol_bkt.get) if vol_bkt else 20
    return pmin + (poc_b + 0.5) * bsize

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  COMPRESSION ZONE DETECTOR (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def find_compression_zone(candles):
    cfg        = CONFIG
    min_len    = cfg["compression_min_candles"]
    max_len    = cfg["compression_max_candles"]
    range_pct  = cfg["compression_range_pct"]
    short_rng  = cfg.get("short_compression_range_pct", 0.08)
    lookback   = min(cfg["compression_lookback"], len(candles))
    scan_slice = candles[-lookback:]
    n          = len(scan_slice)

    best = None

    for end in range(n - 1, min_len - 2, -1):
        zone_high = scan_slice[end]["high"]
        zone_low  = scan_slice[end]["low"]
        start     = end

        for start in range(end - 1, max(end - max_len, -1), -1):
            c = scan_slice[start]
            new_high = max(zone_high, c["high"])
            new_low  = min(zone_low,  c["low"])
            rng      = (new_high - new_low) / new_low if new_low > 0 else 999

            if rng > range_pct:
                start += 1
                break
            zone_high = new_high
            zone_low  = new_low

        length = end - start + 1
        if length < min_len:
            continue

        actual_range = (zone_high - zone_low) / zone_low if zone_low > 0 else 999
        if length < 24 and actual_range > short_rng:
            continue

        zone_candles = scan_slice[start:end+1]
        vols_zone    = sorted(c["volume_usd"] for c in zone_candles)
        mid          = length // 2
        median_vol   = (vols_zone[mid] + vols_zone[~mid]) / 2 if length > 1 else vols_zone[0]
        avg_vol      = sum(vols_zone) / length

        choppy_max = cfg.get("compression_choppy_max", 0.02)
        avg_candle_range = sum(
            (c["high"] - c["low"]) / c["low"]
            for c in zone_candles if c["low"] > 0
        ) / length
        if avg_candle_range > choppy_max:
            continue

        p75_idx   = int(length * 0.75)
        p75_vol   = vols_zone[min(p75_idx, length - 1)]
        purity_base = p75_vol if p75_vol > 0 else median_vol

        purity_mult = cfg.get("zone_purity_vol_mult", 3.0)
        purity_max  = cfg.get("zone_purity_spike_max", 1)
        spike_count = sum(
            1 for c in zone_candles
            if purity_base > 0 and c["volume_usd"] > purity_mult * purity_base
        )
        if spike_count > purity_max:
            continue

        age     = (n - 1) - end
        quality = length * math.exp(-age / 48)

        if best is None or quality > best["quality"]:
            best = {
                "start_idx":        start,
                "end_idx":          end,
                "low":              zone_low,
                "high":             zone_high,
                "length":           length,
                "avg_vol":          avg_vol,
                "age_candles":      age,
                "quality":          quality,
                "range_pct":        actual_range,
                "avg_candle_range": avg_candle_range,
                "spike_count":      spike_count,
            }

        end = start

    return best

# ══════════════════════════════════════════════════════════════════════════════
#  ⚡  VOLUME AWAKENING DETECTOR (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def detect_volume_awakening(candles, compression_avg_vol):
    if not candles or compression_avg_vol <= 0:
        return {"detected": False, "best_mult": 0, "spike_candle": -1,
                "is_green": False, "is_mega": False}

    lookback = CONFIG["awakening_lookback_candles"]
    thresh   = CONFIG["awakening_vol_mult"]

    best_mult    = 0.0
    spike_candle = -1
    is_green     = False

    for i in range(1, min(lookback + 1, len(candles) + 1)):
        c    = candles[-i]
        mult = c["volume_usd"] / compression_avg_vol if compression_avg_vol > 0 else 0
        if mult > best_mult:
            best_mult    = mult
            spike_candle = i
            is_green     = c["close"] > c["open"]

    detected = best_mult >= thresh - 1e-9
    is_mega  = best_mult >= CONFIG["mega_awakening_mult"]

    return {
        "detected":     detected,
        "best_mult":    round(best_mult, 2),
        "spike_candle": spike_candle,
        "is_green":     is_green,
        "is_mega":      is_mega,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  💧  LIQUIDITY SWEEP DETECTOR (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def detect_liquidity_sweep(candles, support_low):
    lookback    = CONFIG["liq_sweep_lookback"]
    recover_bars = CONFIG["liq_sweep_recover_bars"]
    recent      = candles[-lookback:]

    for i in range(len(recent) - 1):
        c = recent[i]
        if c["low"] < support_low * 0.99:
            for j in range(i + 1, min(i + recover_bars + 1, len(recent))):
                if recent[j]["close"] > support_low:
                    return True
    return False

# ══════════════════════════════════════════════════════════════════════════════
#  📊  CANDLE STRUCTURE ANALYZER (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def analyze_candle_structure(candle):
    body   = abs(candle["close"] - candle["open"])
    rng    = candle["high"] - candle["low"]
    if rng == 0:
        return 0, "doji"

    lower_wick = min(candle["open"], candle["close"]) - candle["low"]
    upper_wick = candle["high"] - max(candle["open"], candle["close"])
    body_pct   = body / rng
    lwick_pct  = lower_wick / rng

    if lwick_pct > 0.50 and body_pct < 0.35:
        return 15, "bullish rejection wick"
    if lwick_pct > 0.40:
        return 12, "hammer/pin bar"
    if body_pct < 0.15:
        return 8, "doji (indecision)"
    if candle["close"] > candle["open"]:
        return 5, "green candle"
    return 2, "red candle"

# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATOR (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def calc_entry_targets(candles, compression_zone):
    cur  = candles[-1]["close"]
    atr  = calc_atr(candles[-48:], 14) or cur * 0.025

    comp_mid = (compression_zone["high"] + compression_zone["low"]) / 2
    entry    = min(cur * 0.999, compression_zone["high"] * 1.005)

    sl = compression_zone["low"] - atr * CONFIG["atr_sl_mult"]
    sl = max(sl, entry * 0.85)

    min_sl_dist = entry * (CONFIG["min_sl_pct"] / 100)
    if (entry - sl) < min_sl_dist:
        sl = entry - min_sl_dist

    sl_pct = round((entry - sl) / entry * 100, 1)

    recent     = candles[-240:]
    res_levels = []
    min_target = cur * (1 + CONFIG["min_target_pct"] / 100)

    for i in range(3, len(recent) - 3):
        h = recent[i]["high"]
        if h <= min_target:
            continue
        touches = sum(
            1 for c in recent
            if abs(c["high"] - h) / h < 0.02 or abs(c["low"] - h) / h < 0.02
        )
        if touches >= 2:
            res_levels.append(h)

    if res_levels:
        res_levels.sort()
        t1 = res_levels[0]
        t2 = res_levels[1] if len(res_levels) > 1 else t1 * 1.15
    else:
        comp_len  = compression_zone["length"]
        atr_mult  = min(4.0 + comp_len / 48, 10.0)
        t1_atr    = entry + atr * atr_mult
        t1_min    = cur * 1.10
        t1        = max(t1_atr, t1_min)
        t2        = max(t1 * 1.20, cur * 1.22)

    if abs(t2 - t1) / t1 < 0.03:
        t2 = t1 * 1.15

    t1_pct = round((t1 - cur) / cur * 100, 1)
    t2_pct = round((t2 - cur) / cur * 100, 1)
    rr     = round((t1 - entry) / (entry - sl), 1) if (entry - sl) > 0 else 0

    return {
        "cur":    cur,
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
#  🔬  FORENSIC PATTERN DETECTORS (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def detect_higher_lows(candles, lookback=4):
    if len(candles) < lookback:
        return False
    recent = candles[-lookback:]
    lows   = [c["low"] for c in recent]
    for i in range(1, len(lows)):
        if lows[i] < lows[i-1] * 0.985:
            return False
    return True

def detect_price_acceleration(candles, lookback=6):
    if len(candles) < lookback:
        return False
    window = candles[-lookback:]
    half   = len(window) // 2
    if half < 1:
        return False
    late_start = window[half]["close"]
    late_end   = window[-1]["close"]
    if late_start <= 0:
        return False
    late_drift = (late_end - late_start) / late_start * 100
    return late_drift > 0.3

def detect_pre_pump_candle(candles):
    if len(candles) < 2:
        return 0, "insufficient data"
    c   = candles[-2]
    rng = c["high"] - c["low"]
    if rng == 0:
        return 0, "doji pre-spike"
    body     = c["close"] - c["open"]
    body_pct = abs(body) / rng
    if body > 0 and body_pct >= 0.30:
        return 5, f"pre-spike bull body {body_pct*100:.0f}%"
    elif c["close"] > (c["high"] + c["low"]) / 2:
        return 3, "pre-spike close above midpoint"
    else:
        return 0, "pre-spike bearish"

# ══════════════════════════════════════════════════════════════════════════════
#  🚦  PUMP VELOCITY CLASSIFIER (DIPERBAIKI)
# ══════════════════════════════════════════════════════════════════════════════
def classify_pump_velocity(spike_candle, comp_median_vol):
    """
    spike_candle : dict candle yang menjadi puncak volume (bisa bukan candle terbaru)
    comp_median_vol : median volume selama zona compression
    """
    if comp_median_vol <= 0:
        return "UNKNOWN", 0.0, 0

    vol_mult = spike_candle["volume_usd"] / comp_median_vol

    rng      = spike_candle["high"] - spike_candle["low"]
    body_abs = abs(spike_candle["close"] - spike_candle["open"])
    body_pct = body_abs / rng if rng > 0 else 0.0

    # Syarat wajib: candle spike harus hijau (fix 3 v3.2)
    if spike_candle["close"] <= spike_candle["open"]:
        return "LAMBAT", round(vol_mult, 1), round(body_pct * 100, 1)

    fast_vol   = CONFIG["velocity_fast_vol_mult"]
    fast_body  = CONFIG["velocity_fast_body_pct"]
    mid_vol    = CONFIG["velocity_mid_vol_mult"]
    hv_override = CONFIG["velocity_highvol_override"]

    # Volume ekstrem override
    if vol_mult >= hv_override:
        return "CEPAT", round(vol_mult, 1), round(body_pct * 100, 1)

    # CEPAT : vol tinggi + body cukup
    if vol_mult >= fast_vol and body_pct >= fast_body:
        return "CEPAT", round(vol_mult, 1), round(body_pct * 100, 1)

    # SEDANG : vol menengah + body ≥ 30% (ARIA type)
    if vol_mult >= mid_vol and body_pct >= 0.30:
        return "SEDANG", round(vol_mult, 1), round(body_pct * 100, 1)

    # SEDANG : body sangat dominan (≥70%) meskipun vol kecil (MYX type)
    if body_pct >= 0.70 and vol_mult >= 2.0:
        return "SEDANG", round(vol_mult, 1), round(body_pct * 100, 1)

    return "LAMBAT", round(vol_mult, 1), round(body_pct * 100, 1)

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE — DIPERBAIKI
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    c1h = get_candles(symbol, "1h", CONFIG["candle_limit_1h"])
    if len(c1h) < 72:
        return None

    try:
        chg_24h = float(ticker.get("change24h", 0)) * 100
        vol_24h = float(ticker.get("quoteVolume", 0))
        price   = float(ticker.get("lastPr", 0))
    except:
        return None

    if price <= 0:
        return None

    if chg_24h > 40.0:
        log.info(f"  {symbol}: SKIP chg_24h={chg_24h:.1f}% sudah naik duluan")
        return None

    compression = find_compression_zone(c1h)
    if compression is None:
        log.info(f"  {symbol}: SKIP tidak ada compression zone yang valid")
        return None

    comp_low    = compression["low"]
    comp_high   = compression["high"]
    comp_avg_vol = compression["avg_vol"]
    comp_length  = compression["length"]
    comp_age     = compression["age_candles"]

    # Median volume zona
    _lookback_actual = min(CONFIG["compression_lookback"], len(c1h))
    _scan_slice      = c1h[-_lookback_actual:]
    _zone_candles    = _scan_slice[compression["start_idx"]:compression["end_idx"] + 1]
    if _zone_candles:
        _zvols       = sorted(c["volume_usd"] for c in _zone_candles)
        _mid         = len(_zvols) // 2
        comp_median_vol = (_zvols[_mid] + _zvols[~_mid]) / 2 if len(_zvols) > 1 else _zvols[0]
    else:
        comp_median_vol = comp_avg_vol

    log.info(f"  {symbol}: Compression found len={comp_length} age={comp_age} "
             f"range={compression['range_pct']*100:.1f}% "
             f"candle_range={compression.get('avg_candle_range',0)*100:.2f}% "
             f"spikes={compression.get('spike_count',0)}")

    # PRE-ZONE PUMP CONTEXT
    lookback_ctx  = 48
    pre_zone_mult = 3.0
    pre_zone_max  = 5
    max_zone_age  = 168
    if comp_age <= max_zone_age:
        comp_start_abs = len(c1h) - min(CONFIG["compression_lookback"], len(c1h)) + compression["start_idx"]
        pre_start      = max(0, comp_start_abs - lookback_ctx)
        pre_candles    = c1h[pre_start:comp_start_abs]
        if pre_candles and comp_avg_vol > 0:
            pre_zone_spikes = sum(
                1 for c in pre_candles
                if c["volume_usd"] > pre_zone_mult * comp_avg_vol
            )
            if pre_zone_spikes > pre_zone_max:
                log.info(f"  {symbol}: SKIP pre-zone pump — {pre_zone_spikes} candle "
                         f"vol>{pre_zone_mult}×comp_avg dalam {len(pre_candles)}H sebelum zona")
                return None

    # Gate usia zona
    if comp_age > 168:
        log.info(f"  {symbol}: SKIP compression kadaluarsa (age={comp_age}h > 168h)")
        return None
    if comp_age > 72:
        _quick_awk = detect_volume_awakening(c1h, comp_avg_vol)
        if not _quick_awk["detected"]:
            log.info(f"  {symbol}: SKIP compression tua & tidak ada spike "
                     f"(age={comp_age}h, best_mult={_quick_awk['best_mult']:.1f}x)")
            return None

    # ── PERBAIKAN: gunakan harga ticker, bukan close candle ──────────────────
    price_now = price
    rise_from_low = (price_now - comp_low) / comp_low if comp_low > 0 else 999

    if rise_from_low > CONFIG["max_rise_from_low_pct"]:
        log.info(f"  {symbol}: SKIP sudah naik {rise_from_low*100:.1f}% dari low compression — terlambat")
        return None

    # Gate harga di bawah zona
    price_below_zone_pct = (comp_low - price_now) / comp_low if price_now < comp_low else 0
    if price_below_zone_pct > CONFIG["price_below_zone_max"]:
        log.info(f"  {symbol}: SKIP downtrend aktif — harga {price_below_zone_pct*100:.1f}% di bawah zona")
        return None

    # MA trend
    if len(c1h) >= 55:
        closes    = [c["close"] for c in c1h]
        ma20_now  = sum(closes[-20:])   / 20
        ma50_now  = sum(closes[-50:])   / 50
        ma20_ago  = sum(closes[-25:-5]) / 20
        ma50_ago  = sum(closes[-55:-5]) / 50
        ma_gap    = (ma50_now - ma20_now) / ma50_now if ma50_now > 0 else 0
        ma20_falling = ma20_now < ma20_ago
        ma50_falling = ma50_now < ma50_ago
        ma_bearish   = ma_gap > 0.025 and ma20_falling and ma50_falling
        if ma_bearish:
            log.info(f"  {symbol}: SKIP MA bearish — gap MA={ma_gap*100:.1f}%")
            return None

    # Post-pump 6H
    if len(c1h) >= CONFIG["post_pump_lookback_candles"]:
        lookback_n   = CONFIG["post_pump_lookback_candles"]
        avg_vol_last = sum(c["volume_usd"] for c in c1h[-lookback_n:]) / lookback_n
        post_pump_ratio = avg_vol_last / comp_avg_vol if comp_avg_vol > 0 else 0
        if post_pump_ratio > CONFIG["post_pump_vol_mult"]:
            log.info(f"  {symbol}: SKIP post-pump (6H) — avg_{lookback_n}h = {post_pump_ratio:.1f}x")
            return None

    # Post-pump 48H
    lookback_48 = CONFIG.get("post_pump_lookback_48h", 48)
    thresh_48   = CONFIG.get("post_pump_vol_mult_48h", 4.0)
    if len(c1h) >= lookback_48:
        avg_48h = sum(c["volume_usd"] for c in c1h[-lookback_48:]) / lookback_48
        ratio_48 = avg_48h / comp_avg_vol if comp_avg_vol > 0 else 0
        if ratio_48 > thresh_48:
            log.info(f"  {symbol}: SKIP post-pump (48H) — avg_48h = {ratio_48:.1f}x")
            return None

    # G3 breakout
    price_above_zone_pct = (price_now - comp_high) / comp_high if price_now > comp_high else 0
    if price_above_zone_pct > CONFIG.get("price_above_zone_max", 0.03):
        log.info(f"  {symbol}: SKIP G3 breakout — harga {price_above_zone_pct*100:.1f}% di atas zona")
        return None

    awakening = detect_volume_awakening(c1h, comp_avg_vol)
    if not awakening["detected"]:
        log.info(f"  {symbol}: SKIP volume belum bangun (best_mult={awakening['best_mult']:.1f}x)")
        return None

    # ── Gate selling climax ──────────────────────────────────────────────────
    spike_candle = c1h[-awakening["spike_candle"]] if awakening["spike_candle"] >= 1 else c1h[-1]
    spike_is_red = spike_candle["close"] < spike_candle["open"]
    price_below_zone = price_now < comp_low * 0.99
    if spike_is_red and price_below_zone:
        log.info(f"  {symbol}: SKIP selling climax — spike merah + harga di bawah zona")
        return None

    log.info(f"  {symbol}: Volume awakening! {awakening['best_mult']:.1f}x compression avg")

    funding = get_funding(symbol)
    if funding < CONFIG["funding_gate"]:
        log.info(f"  {symbol}: SKIP funding terlalu negatif ({funding:.5f})")
        return None

    # ── PERBAIKAN: RVOL menggunakan candle spike ─────────────────────────────
    if len(c1h) >= 25:
        spike_idx = -awakening["spike_candle"]  # indeks negatif
        last_vol       = c1h[spike_idx]["volume_usd"]
        target_hour    = (c1h[spike_idx]["ts"] // 3_600_000) % 24
        same_hour_vols = [c["volume_usd"] for c in c1h[:spike_idx]  # hanya candle sebelum spike
                          if (c["ts"] // 3_600_000) % 24 == target_hour]
        avg_same_hour  = sum(same_hour_vols) / len(same_hour_vols) if same_hour_vols else 1
        rvol           = last_vol / avg_same_hour if avg_same_hour > 0 else 1.0
    else:
        rvol = 1.0

    if rvol < CONFIG["min_rvol_gate"]:
        log.info(f"  {symbol}: SKIP RVOL={rvol:.2f}x terlalu rendah")
        return None

    # Metrik tambahan
    rsi          = get_rsi(c1h[-50:], 14)
    atr_7        = calc_atr(c1h[-10:],  7) or price_now * 0.02
    atr_30       = calc_atr(c1h[-33:], 30) or price_now * 0.02
    vol_compress = (atr_7 / atr_30) < 0.75 if atr_30 > 0 else False
    liq_sweep    = detect_liquidity_sweep(c1h, comp_low)
    candle_score, candle_label = analyze_candle_structure(c1h[-1])

    higher_lows        = detect_higher_lows(c1h, lookback=4)
    price_accel        = detect_price_acceleration(c1h, lookback=6)
    pre_spike_sc, pre_spike_label = detect_pre_pump_candle(c1h)

    # ── PERBAIKAN: velocity menggunakan candle spike ─────────────────────────
    velocity, vel_mult_med, vel_body_pct = classify_pump_velocity(spike_candle, comp_median_vol)

    if velocity == "LAMBAT":
        log.info(f"  {symbol}: SKIP pump velocity LAMBAT — vol={vel_mult_med}x median, body={vel_body_pct}%")
        return None

    # ── SCORING (sama seperti asli) ──────────────────────────────────────────
    score = 0
    score_breakdown = []

    comp_score = 0
    if comp_length >= 36:   comp_score += 10
    if comp_length >= 72:   comp_score += 8
    if comp_length >= 168:  comp_score += 7
    if comp_length >= 336:  comp_score += 5
    if compression["range_pct"] < 0.04:  comp_score += 5
    comp_score = min(comp_score, 30)
    score += comp_score
    score_breakdown.append(f"Compression: +{comp_score} (len={comp_length}h, range={compression['range_pct']*100:.1f}%)")

    vol_score = 0
    mult = awakening["best_mult"]
    if mult >= CONFIG["awakening_vol_mult"]:    vol_score += 10
    if mult >= CONFIG["strong_awakening_mult"]: vol_score += 8
    if mult >= CONFIG["mega_awakening_mult"]:   vol_score += 7
    if awakening["is_green"]:                   vol_score += 3
    if awakening["spike_candle"] == 1:          vol_score += 2
    vol_score = min(vol_score, 25)
    score += vol_score
    score_breakdown.append(f"Vol awakening: +{vol_score} ({mult:.1f}x, {'hijau' if awakening['is_green'] else 'merah'})")

    prox_score = 0
    if rise_from_low <= 0.02:   prox_score = 20
    elif rise_from_low <= 0.04: prox_score = 15
    elif rise_from_low <= 0.06: prox_score = 10
    elif rise_from_low <= 0.09: prox_score = 5
    else:                       prox_score = 2
    score += prox_score
    score_breakdown.append(f"Proximity: +{prox_score} ({rise_from_low*100:.1f}% dari low)")

    score += candle_score
    score_breakdown.append(f"Candle: +{candle_score} ({candle_label})")

    rsi_score = 0
    if rsi < 30:    rsi_score = 10
    elif rsi < 38:  rsi_score = 7
    elif rsi < 45:  rsi_score = 4
    else:           rsi_score = 2
    score += rsi_score
    score_breakdown.append(f"RSI: +{rsi_score} (RSI={rsi:.0f})")

    if liq_sweep:
        score += 8
        score_breakdown.append("Liq sweep: +8")
    if vol_compress:
        score += 5
        score_breakdown.append("Vol compress: +5")
    if higher_lows:
        score += 8
        score_breakdown.append("Higher lows: +8")
    if price_accel:
        score += 7
        score_breakdown.append("Price accel: +7")
    if pre_spike_sc > 0:
        score += pre_spike_sc
        score_breakdown.append(f"Pre-spike: +{pre_spike_sc} ({pre_spike_label})")
    if velocity == "CEPAT":
        score += 20
        score_breakdown.append(f"Velocity CEPAT: +20 (vol={vel_mult_med}x med, body={vel_body_pct}%)")
    elif velocity == "SEDANG":
        score += 10
        score_breakdown.append(f"Velocity SEDANG: +10 (vol={vel_mult_med}x med, body={vel_body_pct}%)")

    if funding < -0.001:
        score -= 5
        score_breakdown.append(f"Funding penalty: -5 ({funding:.5f})")
    if comp_age > 48:
        penalty = min((comp_age - 48) // 12, 10)
        score -= penalty
        score_breakdown.append(f"Age penalty: -{penalty} (zone berakhir {comp_age}h lalu)")

    score = min(score, 100)
    log.info(f"  {symbol}: Score={score} velocity={velocity} vel_mult={vel_mult_med}x breakdown={score_breakdown}")

    if score < CONFIG["score_threshold"]:
        return None

    entry_data = calc_entry_targets(c1h, compression)
    if not entry_data:
        log.info(f"  {symbol}: SKIP entry_data gagal dihitung")
        return None
    if entry_data["t1_pct"] < CONFIG["min_target_pct"]:
        log.info(f"  {symbol}: SKIP T1={entry_data['t1_pct']:.1f}% < min_target={CONFIG['min_target_pct']}%")
        return None
    if entry_data["rr"] < CONFIG["min_rr"]:
        log.info(f"  {symbol}: SKIP R/R={entry_data['rr']} terlalu kecil")
        return None

    if velocity == "CEPAT" and vel_mult_med >= 200:
        urgency = "🔴 SANGAT TINGGI — explosive pump, entry SEKARANG (H+1 biasanya ≥10%)"
    elif velocity == "CEPAT":
        urgency = "🟠 TINGGI — pump aktif & energetik, window entry 1-2 jam"
    elif velocity == "SEDANG":
        urgency = "🟡 SEDANG — pump mulai terbentuk, target TP 3-6 jam"
    else:
        urgency = "⚪ WATCH — sedang membangun momentum"

    return {
        "symbol":           symbol,
        "score":            score,
        "composite_score":  score,
        "compression":      compression,
        "awakening":        awakening,
        "entry":            entry_data,
        "liq_sweep":        liq_sweep,
        "candle_label":     candle_label,
        "spike_candle_green": awakening["is_green"],
        "pre_spike_label":  pre_spike_label,
        "higher_lows":      higher_lows,
        "price_accel":      price_accel,
        "rsi":              rsi,
        "vol_compress":     vol_compress,
        "funding":          funding,
        "rvol":             round(rvol, 1),
        "price":            price_now,
        "chg_24h":          chg_24h,
        "vol_24h":          vol_24h,
        "rise_from_low":    rise_from_low,
        "sector":           SECTOR_LOOKUP.get(symbol, "OTHER"),
        "urgency":          urgency,
        "score_breakdown":  score_breakdown,
        "velocity":         velocity,
        "vel_mult_med":     vel_mult_med,
        "vel_body_pct":     vel_body_pct,
        "comp_median_vol":  round(comp_median_vol, 2),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER (DIPERBAIKI, hapus vwap/z2)
# ══════════════════════════════════════════════════════════════════════════════
def build_alert(r, rank=None):
    sc   = min(r["score"], 100)
    bar  = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
    e    = r["entry"]
    comp = r["compression"]
    awk  = r["awakening"]
    rk   = f"#{rank} " if rank else ""
    vol  = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
            else f"${r['vol_24h']/1e3:.0f}K")
    rise_warn = (f"⚠️ Sudah naik {r['rise_from_low']*100:.1f}% dari low\n"
                 if r["rise_from_low"] > CONFIG["max_rise_warn_pct"] else "")

    comp_days = comp["length"] / 24
    comp_str  = (f"{comp_days:.0f} hari" if comp_days >= 1
                 else f"{comp['length']} jam")

    spike_candle_str = (
        f"{'Hijau ✅' if awk['is_green'] else 'Merah ⚠️'}"
        f"{'  🔥 MEGA SPIKE!' if awk['is_mega'] else ''}"
    )
    spike_is_current = awk["spike_candle"] == 1
    current_candle_str = r["candle_label"]

    forensic_checks = []
    if r.get("higher_lows"):  forensic_checks.append("Higher lows ✅")
    if r.get("price_accel"):  forensic_checks.append("Price accel ✅")
    pre_sc = r.get("pre_spike_label", "")
    if pre_sc and "bull" in pre_sc: forensic_checks.append("Pre-spike bull ✅")
    if r.get("liq_sweep"):    forensic_checks.append("Liq sweep ✅")
    forensic_str = "  " + " · ".join(forensic_checks) if forensic_checks else "  — tidak ada pola akumulasi"

    vel       = r.get("velocity", "UNKNOWN")
    vel_mult  = r.get("vel_mult_med", 0)
    vel_body  = r.get("vel_body_pct", 0)
    vel_emoji = {"CEPAT": "🚀", "SEDANG": "⚡", "LAMBAT": "🐌"}.get(vel, "❓")
    vel_str   = f"{vel_emoji} {vel} ({vel_mult}x median, body {vel_body}%)"

    msg = (
        f"🚀 <b>PRE-PUMP SIGNAL {rk}— v3.2</b>\n\n"
        f"<b>Symbol  :</b> {r['symbol']} [{r['sector']}]\n"
        f"<b>Skor    :</b> {sc}/100  {bar}\n"
        f"<b>Urgency :</b> {r['urgency']}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 <b>COMPRESSION ZONE</b>\n"
        f"  Durasi   : {comp_str} ({comp['length']} candle)\n"
        f"  Range    : {comp['range_pct']*100:.1f}% "
        f"(${comp['low']:.6g} – ${comp['high']:.6g})\n"
        f"  Harga kini: ${r['price']:.6g} "
        f"(+{r['rise_from_low']*100:.1f}% dari low)\n"
        f"{rise_warn}"
        f"\n⚡ <b>VOLUME AWAKENING</b>\n"
        f"  Spike    : {awk['best_mult']:.1f}x rata-rata compression\n"
        f"  Candle spike : {spike_candle_str}\n"
        f"  RVOL     : {r['rvol']:.1f}x\n"
        f"\n🏎️ <b>PUMP VELOCITY (v3.0)</b>\n"
        f"  {vel_str}\n"
        f"\n📊 <b>KONDISI TEKNIKAL</b>\n"
        f"  RSI 1H    : {r['rsi']:.0f} {'(oversold 🟢)' if r['rsi'] < 35 else '(netral)'}\n"
        f"  Candle kini: {current_candle_str}"
        f"{' (= spike candle)' if spike_is_current else ''}\n"
        f"  ATR comp  : {'✅' if r['vol_compress'] else '❌'}\n"
        f"  Funding   : {r['funding']:.5f}\n"
        f"  Vol 24H   : {vol}  |  Chg: {r['chg_24h']:+.1f}%\n"
        f"\n🔬 <b>AKUMULASI (forensik)</b>\n"
        f"{forensic_str}\n"
    )

    if e:
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>ENTRY &amp; TARGET</b>\n"
            f"  Entry : ${e['entry']}\n"
            f"  SL    : ${e['sl']}  (-{e['sl_pct']:.1f}%)\n"
            f"  T1    : ${e['t1']}  (+{e['t1_pct']:.1f}%)\n"
            f"  T2    : ${e['t2']}  (+{e['t2_pct']:.1f}%)\n"
            f"  R/R   : 1:{e['rr']}  |  ATR: ${e['atr']}\n"
        )

    msg += f"\n🕐 {utc_now()}\n<i>⚠️ Bukan financial advice. DYOR.</i>"
    return msg

def build_summary(results):
    msg  = f"📋 <b>PRE-PUMP WATCHLIST v3.2 — {utc_now()}</b>\n{'━'*30}\n"
    for i, r in enumerate(results, 1):
        comp  = r["compression"]
        awk   = r["awakening"]
        vol   = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                 else f"${r['vol_24h']/1e3:.0f}K")
        days  = comp["length"] / 24
        vel   = r.get("velocity", "?")
        vel_emoji = {"CEPAT": "🚀", "SEDANG": "⚡"}.get(vel, "❓")
        msg  += (
            f"{i}. <b>{r['symbol']}</b> [S:{r['score']}] {vel_emoji}{vel}\n"
            f"   Coil {days:.1f}d · Vol {awk['best_mult']:.1f}x · "
            f"T1:+{r['entry']['t1_pct']:.0f}% · {vol}\n"
        )
    return msg

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    candidates    = []
    not_found     = []
    stats         = defaultdict(int)

    log.info("=" * 70)
    log.info(f"🔍 SCANNING {len(WHITELIST_SYMBOLS)} coin — PRE-PUMP DETECTION v3.2")
    log.info("=" * 70)

    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:
            stats["manual_exclude"] += 1
            continue
        if is_cooldown(sym):
            stats["cooldown"] += 1
            continue
        if sym not in tickers:
            not_found.append(sym)
            continue

        t = tickers[sym]
        try:
            vol   = float(t.get("quoteVolume", 0))
            chg   = float(t.get("change24h",   0)) * 100
            price = float(t.get("lastPr",       0))
        except:
            stats["parse_error"] += 1
            continue

        if vol < CONFIG["pre_filter_vol"]:
            stats["vol_too_low"] += 1
            continue
        if vol > CONFIG["max_vol_24h"]:
            stats["vol_too_high"] += 1
            continue
        if abs(chg) > CONFIG["gate_chg_24h_max"]:
            stats["change_extreme"] += 1
            continue
        if price <= 0:
            stats["invalid_price"] += 1
            continue

        candidates.append((sym, t))

    total    = len(WHITELIST_SYMBOLS)
    will_scan = len(candidates)

    log.info(f"\n📊 Pre-filter: {will_scan}/{total} coin akan di-scan")
    log.info(f"   Cooldown: {stats['cooldown']} | Vol rendah: {stats['vol_too_low']} | "
             f"Vol tinggi: {stats['vol_too_high']} | Chg ekstrem: {stats['change_extreme']}")
    if not_found:
        log.info(f"   Tidak di Bitget: {len(not_found)} coin")
    log.info(f"   ⏱️  Est. waktu: ~{will_scan * CONFIG['sleep_coins'] / 60:.1f} menit")
    log.info("=" * 70)

    return candidates

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN (tidak diubah)
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v3.2 — {utc_now()} ===")

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
                log.info(f"  ✅ SIGNAL! Score={res['score']} "
                         f"Coil={res['compression']['length']}h "
                         f"VolSpike={res['awakening']['best_mult']:.1f}x "
                         f"Rise={res['rise_from_low']*100:.1f}%")
                results.append(res)
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}", exc_info=True)

        time.sleep(CONFIG["sleep_coins"])

    results.sort(key=lambda x: x["score"], reverse=True)

    log.info(f"\n{'='*70}")
    log.info(f"✅ Total sinyal lolos: {len(results)} coin")
    log.info(f"{'='*70}\n")

    if not results:
        log.info("Tidak ada sinyal pre-pump saat ini")
        return

    top = results[:CONFIG["max_alerts_per_run"]]

    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"📤 Alert #{rank}: {r['symbol']} Score={r['score']}")
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert dikirim — {utc_now()} ===")

if __name__ == "__main__":
    log.info("╔════════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v3.2 (FIXED)                    ║")
    log.info("║  Deteksi transisi Fase Tidur → Fase Bangun        ║")
    log.info("║  Target: pump energetik ≥8% dalam 1-3 jam        ║")
    log.info("╚════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)

    run_scan()
