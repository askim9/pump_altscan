#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v4.0 — EVIDENCE-BASED REDESIGN                    ║
║                                                                      ║
║  BASIS ILMIAH:                                                       ║
║  [A] Volume Z-score  — Fantazzini 2023 (351 events, Binance)        ║
║  [B] Taker Buy Proxy — La Morgia 2023 (ACM, feature importance #1)  ║
║  [C] Activity Proxy  — La Morgia 2023 (trade count anomaly #2)      ║
║                                                                      ║
║  YANG DIHAPUS (tidak ada bukti empiris sebagai pump predictor):     ║
║  - Compression zone / ATR tension scoring                            ║
║  - Candlestick patterns (hammer, pin bar, dll)                       ║
║  - Higher lows / failed breakdown detection                          ║
║  - Stealth accumulation scoring                                      ║
║  - Pre-breakout bias module                                          ║
║  - RSI (terbukti tidak predictive untuk pump)                       ║
║                                                                      ║
║  SCORING (total 100):                                                ║
║  [A] Volume Z-score:    40 pts  (bukti terkuat)                     ║
║  [B] Taker Buy Proxy:   35 pts  (rush orders proxy)                 ║
║  [C] Activity Proxy:    25 pts  (trade count proxy)                  ║
║                                                                      ║
║  THRESHOLD: score >= 50 → alert                                      ║
║  EXCHANGE : Bitget USDT-Futures                                      ║
║  INTERVAL : Setiap 1 jam                                             ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging
import logging.handlers as _lh
from datetime import datetime, timezone
from collections import defaultdict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ── Logging ────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch = logging.StreamHandler(); _ch.setFormatter(_fmt); _root.addHandler(_ch)
_fh = _lh.RotatingFileHandler("/tmp/scanner_v4.log", maxBytes=5*1024*1024, backupCount=2)
_fh.setFormatter(_fmt); _root.addHandler(_fh)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG — semua parameter di satu tempat
# ══════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── VOLUME PRE-FILTER (Charfeddine 2024: thresholds yang valid) ──────
    "pre_filter_vol":      100_000,    # $100K noise floor
    "min_vol_24h":         500_000,    # $500K minimum trading volume
    "max_vol_24h":     800_000_000,    # $800M ceiling (terlalu liquid, sulit pump)

    # ── ALREADY-PUMPED GATE ───────────────────────────────────────────────
    "gate_chg_24h_max":       40.0,    # Sudah naik >40% dalam 24h = terlambat

    # ── DATA ─────────────────────────────────────────────────────────────
    "candle_limit":            200,    # 200 jam = ~8.3 hari data 1H

    # ── [A] VOLUME Z-SCORE (Fantazzini 2023) ─────────────────────────────
    # Rolling Z-score: (current_vol - mean_N) / std_N
    # Sinyal muncul hingga 60 menit sebelum pump (window 1 jam = 1 candle)
    "vol_z_window":             24,    # Baseline window: 24 jam (1 hari)
    "vol_z_lookback":          168,    # History: 7 hari untuk stabil statistics
    "vol_z_strong":            2.5,    # Z-score sangat kuat → full 40 pts
    "vol_z_medium":            1.5,    # Z-score medium → ~20 pts

    # ── [B] TAKER BUY PRESSURE PROXY (La Morgia 2023) ────────────────────
    # Rush orders proxy: agressive buyers = close dekat high, body besar
    # Formula: buy_pressure = (close - low) / (high - low) * volume_usd
    # Normalized ke Z-score vs baseline
    "buy_z_window":             24,    # Baseline window
    "buy_z_strong":            1.8,    # Z-score kuat
    "buy_z_medium":            0.8,    # Z-score medium

    # ── [C] ACTIVITY PROXY (La Morgia 2023: trade count anomaly) ─────────
    # Proxy untuk "banyaknya transaksi": candle range + body ratio
    # Candle dengan banyak trades → range lebih besar, body lebih decisif
    "act_z_window":             24,    # Baseline window
    "act_z_strong":            1.5,    # Z-score kuat
    "act_z_medium":            0.6,    # Z-score medium

    # ── SIGNAL THRESHOLD ─────────────────────────────────────────────────
    "score_threshold":          50,    # Score >= 50 → alert dikirim
    "min_score_strong":         70,    # Score >= 70 → "STRONG" signal
    "min_score_very_strong":    85,    # Score >= 85 → "VERY STRONG" signal

    # ── ADDITIONAL CONTEXT (bukan gate, hanya info) ───────────────────────
    # Funding: digunakan sebagai CONTEXT saja, bukan hard gate
    # (Presto 2024: R²=12.5% only, near-zero for single asset T+1)
    "funding_context_threshold": -0.003,  # Note jika funding sangat negatif

    # ── OUTPUT ────────────────────────────────────────────────────────────
    "max_alerts":               8,
    "alert_cooldown_sec":    3600,
    "sleep_coins":             0.5,
    "sleep_error":             3.0,
    "cooldown_file":  "/tmp/v4_cooldown.json",
}

# ══════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin (dipertahankan dari v3)
# ══════════════════════════════════════════════════════════════════════════
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

MANUAL_EXCLUDE = set()

BITGET_BASE = "https://api.bitget.com"
_cache = {}

# ══════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN
# ══════════════════════════════════════════════════════════════════════════
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
def is_cooldown(sym): return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]
def set_cooldown(sym): _cooldown[sym] = time.time(); save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════════════════
#  🌐  HTTP & API
# ══════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=12):
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                log.warning("Rate limit — tunggu 30s")
                time.sleep(30)
                continue  # FIXED: continue bukan break
            log.warning(f"HTTP error {e.response.status_code}")
            break
        except Exception as ex:
            if attempt < 2:
                time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        return r.ok
    except Exception as ex:
        log.error(f"Telegram error: {ex}")
        return False

def get_all_tickers():
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/tickers",
                    params={"productType": "USDT-FUTURES"})
    if not data or data.get("code") != "00000":
        return {}
    return {item["symbol"]: item for item in data.get("data", [])}

def get_candles(symbol: str, limit: int = 200):
    ckey = f"{symbol}:1H:{limit}"
    if ckey in _cache:
        return _cache[ckey]
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/candles",
        params={"symbol": symbol, "productType": "USDT-FUTURES",
                "granularity": "1H", "limit": limit}
    )
    if not data or data.get("code") != "00000":
        return []
    raw = data.get("data", [])
    candles = []
    for row in raw:
        try:
            candles.append({
                "ts":         int(row[0]),
                "open":       float(row[1]),
                "high":       float(row[2]),
                "low":        float(row[3]),
                "close":      float(row[4]),
                "volume":     float(row[5]),
                "volume_usd": float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4]),
            })
        except (IndexError, ValueError):
            continue
    candles.sort(key=lambda x: x["ts"])
    _cache[ckey] = candles
    return candles

def get_funding(symbol: str) -> float:
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
        params={"symbol": symbol, "productType": "USDT-FUTURES"}
    )
    try:
        return float(data["data"][0]["fundingRate"])
    except:
        return 0.0

# ══════════════════════════════════════════════════════════════════════════
#  📐  MATH UTILITIES
# ══════════════════════════════════════════════════════════════════════════
def _mean(arr):
    return sum(arr) / len(arr) if arr else 0.0

def _std(arr):
    if len(arr) < 2:
        return 0.0
    m = _mean(arr)
    return math.sqrt(sum((x - m) ** 2 for x in arr) / len(arr))

def z_score(value: float, series: list) -> float:
    """Z-score of value vs series. Returns 0 if insufficient data."""
    if len(series) < 5:
        return 0.0
    mu = _mean(series)
    sigma = _std(series)
    if sigma == 0:
        return 0.0
    return (value - mu) / sigma

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

# ══════════════════════════════════════════════════════════════════════════
#  🔬  TIGA KOMPONEN SINYAL UTAMA
# ══════════════════════════════════════════════════════════════════════════

def compute_volume_z(candles: list) -> tuple:
    """
    [A] Volume Z-score — Fantazzini 2023
    Deteksi anomali volume vs rolling history.
    
    Kunci insight: unusual volume muncul hingga 60 menit sebelum pump.
    Window optimal: 1 jam (1 candle pada 1H chart).
    
    Returns: (score 0-40, z_score, details)
    """
    min_len = CONFIG["vol_z_lookback"] + 5
    if len(candles) < min_len:
        return 0, 0.0, {"reason": "insufficient data"}

    win   = CONFIG["vol_z_window"]
    lkbk  = CONFIG["vol_z_lookback"]

    # Volume candle terbaru (dikonfirmasi, bukan candle live)
    cur_vol = candles[-2]["volume_usd"]  # Pakai candle -2 (confirmed), bukan -1 (live)

    # Baseline: mean & std dari window_lookback candle sebelumnya
    baseline_vols = [c["volume_usd"] for c in candles[-(lkbk + win):-win]]
    if len(baseline_vols) < 20:
        return 0, 0.0, {"reason": "insufficient baseline"}

    # Z-score window terakhir vs baseline
    recent_avg = _mean([c["volume_usd"] for c in candles[-win:-1]])
    z = z_score(cur_vol, baseline_vols)
    z_recent = z_score(recent_avg, baseline_vols)

    # Score
    z_max  = CONFIG["vol_z_strong"]   # 2.5
    z_mid  = CONFIG["vol_z_medium"]   # 1.5
    z_use  = max(z, z_recent * 0.8)   # Ambil yang lebih kuat

    if z_use >= z_max:
        score = 40
    elif z_use >= z_mid:
        score = int(20 + (z_use - z_mid) / (z_max - z_mid) * 20)
    elif z_use >= 0.5:
        score = int((z_use - 0.5) / (z_mid - 0.5) * 20)
    else:
        score = 0

    vol_ratio = cur_vol / _mean(baseline_vols) if _mean(baseline_vols) > 0 else 1.0

    return score, z_use, {
        "cur_vol_usd":    round(cur_vol),
        "baseline_mean":  round(_mean(baseline_vols)),
        "z":              round(z, 2),
        "z_recent":       round(z_recent, 2),
        "vol_ratio":      round(vol_ratio, 2),
    }


def compute_buy_pressure_z(candles: list) -> tuple:
    """
    [B] Taker Buy Pressure Proxy — La Morgia 2023 (#1 feature)
    
    Proxy untuk rush orders (aggressive market buys):
    buy_pressure = (close - low) / (high - low)
    Menunjukkan seberapa "agresif" buyers: close dekat high = buyers agresif.
    
    Returns: (score 0-35, z_score, details)
    """
    win  = CONFIG["buy_z_window"]
    min_len = win * 4 + 5
    if len(candles) < min_len:
        return 0, 0.0, {"reason": "insufficient data"}

    def buy_pressure(c):
        rng = c["high"] - c["low"]
        if rng <= 0:
            return 0.5
        return clamp((c["close"] - c["low"]) / rng, 0.0, 1.0)

    # Pressure candle terbaru
    cur_bp = buy_pressure(candles[-2])

    # Weighted: pressure * volume (lebih dekat ke rush orders)
    cur_weighted = cur_bp * candles[-2]["volume_usd"]

    # Baseline: pressure * volume dari candle lookback
    baseline = [buy_pressure(c) * c["volume_usd"] for c in candles[-(win * 4):-win]]
    if len(baseline) < 15:
        return 0, 0.0, {"reason": "insufficient baseline"}

    z = z_score(cur_weighted, baseline)

    z_max = CONFIG["buy_z_strong"]  # 1.8
    z_mid = CONFIG["buy_z_medium"]  # 0.8

    if z >= z_max:
        score = 35
    elif z >= z_mid:
        score = int(15 + (z - z_mid) / (z_max - z_mid) * 20)
    elif z >= 0.2:
        score = int((z - 0.2) / (z_mid - 0.2) * 15)
    else:
        score = 0

    return score, z, {
        "cur_buy_pressure": round(cur_bp, 3),
        "z":                round(z, 2),
    }


def compute_activity_z(candles: list) -> tuple:
    """
    [C] Activity Proxy — La Morgia 2023 (#2 feature: trade count)
    
    Proxy untuk jumlah transaksi:
    activity = (body_size / range) * range_pct
    Candle dengan banyak trades → range lebih volatile, body lebih decisif.
    
    Returns: (score 0-25, z_score, details)
    """
    win  = CONFIG["act_z_window"]
    min_len = win * 3 + 5
    if len(candles) < min_len:
        return 0, 0.0, {"reason": "insufficient data"}

    def activity(c):
        rng = c["high"] - c["low"]
        if rng <= 0 or c["low"] <= 0:
            return 0.0
        body = abs(c["close"] - c["open"])
        range_pct = rng / c["low"]
        body_ratio = body / rng if rng > 0 else 0
        # Combined: range volatility * decisiveness
        return range_pct * (0.5 + body_ratio * 0.5)

    cur_act = activity(candles[-2])
    baseline = [activity(c) for c in candles[-(win * 3):-win]]
    if len(baseline) < 10:
        return 0, 0.0, {"reason": "insufficient baseline"}

    z = z_score(cur_act, baseline)

    z_max = CONFIG["act_z_strong"]  # 1.5
    z_mid = CONFIG["act_z_medium"]  # 0.6

    if z >= z_max:
        score = 25
    elif z >= z_mid:
        score = int(10 + (z - z_mid) / (z_max - z_mid) * 15)
    elif z >= 0.0:
        score = int(z / z_mid * 10)
    else:
        score = 0

    return score, z, {
        "cur_activity":  round(cur_act, 5),
        "z":             round(z, 2),
    }


# ══════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY CALCULATOR (sederhana)
# ══════════════════════════════════════════════════════════════════════════
def calc_entry(candles: list, price: float, funding: float) -> dict:
    """Hitung entry, SL, target berdasarkan ATR sederhana."""
    if len(candles) < 20:
        return None

    # ATR 14 sederhana
    trs = []
    for i in range(1, min(20, len(candles))):
        h, l, pc = candles[-i]["high"], candles[-i]["low"], candles[-i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = _mean(trs) if trs else price * 0.02

    entry  = price
    sl     = entry - atr * 1.5
    sl_pct = round((entry - sl) / entry * 100, 1)

    # Target T1: ATR * 3 (minimal 10%), T2: ATR * 6
    t1 = max(entry * 1.10, entry + atr * 3)
    t2 = max(entry * 1.20, entry + atr * 6)

    t1_pct = round((t1 - entry) / entry * 100, 1)
    t2_pct = round((t2 - entry) / entry * 100, 1)
    rr     = round((t1 - entry) / (entry - sl), 2) if (entry - sl) > 0 else 0

    return {
        "entry":  round(entry, 8),
        "sl":     round(sl, 8),
        "sl_pct": sl_pct,
        "t1":     round(t1, 8),
        "t2":     round(t2, 8),
        "t1_pct": t1_pct,
        "t2_pct": t2_pct,
        "rr":     rr,
        "atr":    round(atr, 8),
        "funding_note": "⚠️ Funding negatif" if funding < CONFIG["funding_context_threshold"] else "",
    }


# ══════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORER v4
# ══════════════════════════════════════════════════════════════════════════
def score_coin(symbol: str, ticker: dict) -> dict | None:
    """
    Scoring utama: 3 komponen berbasis empirical evidence.
    Returns result dict atau None jika data kurang/tidak lolos filter.
    """
    # ── Data quality check ───────────────────────────────────────────────
    candles = get_candles(symbol, CONFIG["candle_limit"])
    if len(candles) < 80:
        return None

    try:
        vol_24h = float(ticker.get("quoteVolume", 0))
        chg_24h = float(ticker.get("change24h",  0)) * 100
        price   = float(ticker.get("lastPr",      0))
    except:
        return None

    if price <= 0:
        return None

    # ── Hard filters (pre-filter sudah dilakukan di build_candidates) ────
    if vol_24h < CONFIG["min_vol_24h"]:
        return None
    if chg_24h > CONFIG["gate_chg_24h_max"]:
        return None

    # ── Compute 3 signals ───────────────────────────────────────────────
    vol_score,  vol_z,  vol_d  = compute_volume_z(candles)
    buy_score,  buy_z,  buy_d  = compute_buy_pressure_z(candles)
    act_score,  act_z,  act_d  = compute_activity_z(candles)

    total_score = vol_score + buy_score + act_score

    # ── Filter: semua komponen harus > 0 (tidak cukup hanya 1 sinyal kuat) ──
    # Minimal 2 dari 3 komponen harus aktif
    active_components = sum([vol_score > 5, buy_score > 5, act_score > 5])
    if active_components < 2:
        return None

    if total_score < CONFIG["score_threshold"]:
        return None

    # ── Context: funding (informational, bukan gate) ─────────────────────
    funding = get_funding(symbol)

    # ── Entry calculation ────────────────────────────────────────────────
    entry = calc_entry(candles, price, funding)

    # ── Confidence label ─────────────────────────────────────────────────
    if total_score >= CONFIG["min_score_very_strong"]:
        confidence = "very_strong"
    elif total_score >= CONFIG["min_score_strong"]:
        confidence = "strong"
    else:
        confidence = "watch"

    # ── Urgency ──────────────────────────────────────────────────────────
    if vol_z >= 2.5 and buy_z >= 1.5:
        urgency = "🔴 TINGGI — Volume + Buying pressure sama-sama anomali"
    elif vol_z >= 2.0:
        urgency = "🟠 SEDANG — Volume Z-score kuat"
    elif buy_z >= 1.5:
        urgency = "🟡 SEDANG — Buying pressure menonjol"
    else:
        urgency = "⚪ WATCH — Akumulasi awal"

    return {
        "symbol":     symbol,
        "score":      total_score,
        "confidence": confidence,
        "price":      price,
        "vol_24h":    vol_24h,
        "chg_24h":    chg_24h,
        "funding":    funding,
        "urgency":    urgency,
        "entry":      entry,
        # Component details
        "vol_score":  vol_score,
        "buy_score":  buy_score,
        "act_score":  act_score,
        "vol_z":      round(vol_z, 2),
        "buy_z":      round(buy_z, 2),
        "act_z":      round(act_z, 2),
        "vol_details": vol_d,
        "buy_details": buy_d,
        "act_details": act_d,
    }


# ══════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════
def build_alert(r: dict, rank: int) -> str:
    e = r["entry"]
    vol_str = f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6 else f"${r['vol_24h']/1e3:.0f}K"
    conf_emoji = {"very_strong": "🟢", "strong": "🟡", "watch": "⚪"}.get(r["confidence"], "⚪")

    entry_line = ""
    if e:
        entry_line = (
            f"\n   Entry: <b>${e['entry']:.6g}</b> | SL: ${e['sl']:.6g} (-{e['sl_pct']}%)"
            f"\n   T1: +{e['t1_pct']}% | T2: +{e['t2_pct']}% | R/R: {e['rr']}"
        )
        if e.get("funding_note"):
            entry_line += f"\n   {e['funding_note']}"

    score_bar = "█" * min(20, int(r['score'] / 5)) + "░" * max(0, 20 - int(r['score'] / 5))

    msg = (
        f"#{rank} {conf_emoji} <b>{r['symbol']}</b>  Score: {r['score']}/100\n"
        f"   {score_bar}\n"
        f"   {r['urgency']}\n"
        f"   Vol:{vol_str} | Δ24h:{r['chg_24h']:+.1f}% | Funding:{r['funding']:.5f}\n"
        f"   [A]Vol_Z:{r['vol_z']:.1f}({r['vol_score']}pts) "
        f"[B]Buy_Z:{r['buy_z']:.1f}({r['buy_score']}pts) "
        f"[C]Act_Z:{r['act_z']:.1f}({r['act_score']}pts)"
        f"{entry_line}\n"
    )
    return msg

def build_summary(results: list) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    msg = f"🔍 <b>PRE-PUMP SCANNER v4.0</b> — {now}\n"
    msg += f"📊 {len(results)} sinyal terdeteksi\n\n"
    for i, r in enumerate(results, 1):
        e = r.get("entry")
        t1 = f"+{e['t1_pct']}%" if e else "?"
        msg += (
            f"{i}. <b>{r['symbol']}</b> "
            f"[{r['score']} | Z:{r['vol_z']:.1f}/{r['buy_z']:.1f}/{r['act_z']:.1f}] "
            f"T1:{t1}\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v4.0 — {datetime.now(timezone.utc)} ===")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner v4.0 Error: Gagal ambil data Bitget")
        return

    log.info(f"Total ticker Bitget: {len(tickers)}")

    # ── Build candidate list ─────────────────────────────────────────────
    candidates = []
    stats = defaultdict(int)

    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:
            stats["excluded"] += 1; continue
        if is_cooldown(sym):
            stats["cooldown"] += 1; continue
        if sym not in tickers:
            stats["not_found"] += 1; continue

        t = tickers[sym]
        try:
            vol = float(t.get("quoteVolume", 0))
            chg = float(t.get("change24h",   0)) * 100
        except:
            stats["parse_error"] += 1; continue

        if vol < CONFIG["pre_filter_vol"]:
            stats["vol_low"] += 1; continue
        if vol > CONFIG["max_vol_24h"]:
            stats["vol_high"] += 1; continue
        if chg > CONFIG["gate_chg_24h_max"]:
            stats["already_pumped"] += 1; continue

        candidates.append((sym, t))

    log.info(f"Candidates: {len(candidates)} | "
             f"Cooldown:{stats['cooldown']} Vol↓:{stats['vol_low']} "
             f"Vol↑:{stats['vol_high']} Pumped:{stats['already_pumped']}")

    # ── Score each candidate ─────────────────────────────────────────────
    results = []
    for i, (sym, t) in enumerate(candidates):
        log.info(f"[{i+1}/{len(candidates)}] {sym}")
        try:
            result = score_coin(sym, t)
            if result:
                results.append(result)
                log.info(
                    f"  ✅ Score={result['score']} ({result['confidence']}) "
                    f"Z: vol={result['vol_z']} buy={result['buy_z']} act={result['act_z']}"
                )
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}", exc_info=False)
        time.sleep(CONFIG["sleep_coins"])

    # ── Sort by score desc ───────────────────────────────────────────────
    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:CONFIG["max_alerts"]]

    log.info(f"Total sinyal: {len(results)} | Dikirim: {len(top)}")

    if not top:
        log.info("Tidak ada sinyal pre-pump saat ini")
        return

    # ── Send Telegram ────────────────────────────────────────────────────
    if len(top) >= 1:
        send_telegram(build_summary(top))
        time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"📤 Alert #{rank}: {r['symbol']} score={r['score']}")
        time.sleep(2)

    log.info(f"=== SELESAI — {datetime.now(timezone.utc)} ===")


if __name__ == "__main__":
    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)
    run_scan()
