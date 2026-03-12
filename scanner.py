"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  ALTCOIN PUMP SCANNER v32 — BUG-FIXED STRUCTURAL ENGINE                    ║
║                                                                              ║
║  11 BUG DIPERBAIKI vs v31:                                                  ║
║  [FIX-01] change_24h konversi: coin pump >100% tidak lagi lolos gate       ║
║  [FIX-02] vol_accum baseline: exclude 24h terbaru, hindari post-pump bias  ║
║  [FIX-03] range_ratio: per-candle vs per-candle (apple-to-apple)           ║
║  [FIX-04] ATR contraction guard: skip jika range_72h > 15% (post-pump)    ║
║  [FIX-05] CVD weighted: volume × body_size / candle_range (lebih akurat)  ║
║  [FIX-06] BOS valid: hanya jika harga 0-12% di atas resistance (fresh)    ║
║  [FIX-07] Price pos gate: 0.80 (was 0.95 — terlalu longgar)               ║
║  [FIX-08] Pump history 72h: gate candle pump >8% atau range >25%          ║
║  [FIX-09] Higher Low: perlu 2 konfirmasi berturut-turut (was 1)            ║
║  [FIX-10] OI score: hanya jika harga flat <4% (bukan breakout)            ║
║  [FIX-11] Squeeze formula: oi_change% / abs(price_change) (intuitif)      ║
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

_http_session = requests.Session()
_http_session.headers.update({"User-Agent": "CryptoScanner/32.0", "Accept-Encoding": "gzip"})

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)
_ch = logging.StreamHandler(); _ch.setFormatter(_log_fmt); _log_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler("/tmp/scanner_v32.log", maxBytes=10*1024*1024, backupCount=3)
_fh.setFormatter(_log_fmt); _log_root.addHandler(_fh)
log = logging.getLogger(__name__)
log.info("Scanner v32 — Bug-Fixed Structural Engine aktif")

# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    "max_alerts_per_run":        12,
    "alert_cooldown_sec":        1800,
    "sleep_coins":               0.15,
    "sleep_error":               3.0,
    "min_vol_24h_usd":           500_000,
    "max_vol_24h_usd":           200_000_000,
    "min_oi_usd":                200_000,

    # HARD REJECT GATES
    "gate_already_pumped_pct":   8.0,    # reject >+8% change 24h
    "gate_dump_pct":             -15.0,  # reject <-15% change 24h
    "gate_price_pos_max":        0.80,   # [FIX-07] reject jika >80% range 48h (was 0.95)
    "gate_rsi_max":              68.0,   # reject RSI > 68

    # [FIX-08] PUMP HISTORY — ROOT CAUSE FIX
    "gate_pump_history_candle":  8.0,    # single candle >8% dalam 72h = reject
    "gate_pump_range_72h":       25.0,   # total range hi/lo 72h > 25% = reject

    "candles_1h":                210,
    "candles_4h":                60,
    "candles_15m":               80,

    # ── PHASE 1: COMPRESSION (max 20) ────────────────────────────────────────
    "p1_bbw_tight":              0.04,
    "p1_bbw_extreme":            0.025,
    "p1_score_bbw_tight":        8,
    "p1_score_bbw_extreme":      15,
    "p1_atr_contract_ratio":     0.70,
    "p1_atr_strong_ratio":       0.50,
    "p1_score_atr_contract":     8,
    "p1_score_atr_strong":       15,
    "p1_atr_pump_guard_pct":     15.0,   # [FIX-04] skip ATR jika range_72h > 15%
    "p1_range_compress_ratio":   0.65,   # [FIX-03] per-candle range < 65% avg
    "p1_score_range_compress":   7,
    "p1_max_score":              20,

    # ── PHASE 2: ACCUMULATION (max 30) ───────────────────────────────────────
    "p2_vol_baseline_start":     24,     # [FIX-02] baseline mulai dari candle ke-24
    "p2_vol_baseline_end":       168,    # baseline sampai candle ke-168
    "p2_vol_accum_mild":         1.3,
    "p2_vol_accum_strong":       1.8,
    "p2_vol_accum_extreme":      2.5,
    "p2_score_vol_mild":         4,
    "p2_score_vol_strong":       8,
    "p2_score_vol_extreme":      12,
    "p2_price_stable_pct":       2.0,
    "p2_price_ok_pct":           5.0,
    "p2_score_price_stable":     8,
    "p2_score_price_ok":         3,
    "p2_oi_expand_min":          3.0,
    "p2_oi_expand_strong":       8.0,
    "p2_score_oi_expand":        6,
    "p2_score_oi_strong":        10,
    "p2_oi_price_flat_max":      4.0,    # [FIX-10] OI score hanya jika price_change < 4%
    "p2_cvd_lookback":           12,
    "p2_cvd_price_flat_max":     1.5,
    "p2_cvd_ratio_min":          1.3,
    "p2_score_cvd_divergence":   10,
    "p2_max_score":              30,

    # ── PHASE 3: POSITION BUILD (max 25) ─────────────────────────────────────
    "p3_position_score_min":     1.5,
    "p3_position_score_strong":  3.5,
    "p3_score_pos_build":        8,
    "p3_score_pos_strong":       14,
    "p3_liq_vacuum_min":         1.8,
    "p3_liq_vacuum_strong":      4.0,
    "p3_score_liq_vacuum":       6,
    "p3_score_liq_strong":       11,
    "p3_squeeze_min":            3.0,    # [FIX-11] oi_change% / abs(price_change)
    "p3_squeeze_strong":         8.0,
    "p3_score_squeeze":          5,
    "p3_score_squeeze_strong":   9,
    "p3_energy_min":             2.5,
    "p3_energy_strong":          7.0,
    "p3_score_energy":           5,
    "p3_score_energy_strong":    10,
    "p3_funding_neg_threshold":  -0.0001,
    "p3_score_funding_neg":      5,
    "p3_max_score":              25,

    # ── PHASE 4: IGNITION PREP (max 25) ──────────────────────────────────────
    "p4_breakout_pressure_mild":   1.5,
    "p4_breakout_pressure_strong": 2.5,
    "p4_score_bp_mild":            6,
    "p4_score_bp_strong":          12,
    "p4_momentum_min_pct":         0.3,
    "p4_momentum_max_pct":         4.0,
    "p4_score_momentum_ok":        5,
    "p4_score_momentum_strong":    10,
    "p4_higher_low_lookback":      24,   # [FIX-09] 24 candle, perlu 3 segmen
    "p4_score_higher_low":         4,
    "p4_bos_lookback":             12,
    "p4_bos_accum_zone_max":       0.12, # [FIX-06] BOS valid 0-12% di atas resistance
    "p4_score_bos":                5,
    "p4_max_score":                25,

    # ── PROBABILITY MODEL ─────────────────────────────────────────────────────
    "prob_center":               55,
    "prob_scale":                10,
    "min_score_watchlist":       48,

    # ── ENTRY/SL/TP ───────────────────────────────────────────────────────────
    "tp1_atr_mult":              2.5,
    "tp2_atr_mult":              5.0,
    "tp3_atr_mult":              8.0,
    "sl_atr_mult":               1.8,

    # ── STORAGE ───────────────────────────────────────────────────────────────
    "cooldown_file":             "./cooldown_v32.json",
    "oi_snapshot_file":          "./oi_snapshot_v32.json",
    "funding_snapshot_file":     "./funding_v32.json",
}

WHITELIST_SYMBOLS = {
      "4USDT",
"0GUSDT",
"1000BONKUSDT",
"1000PEPEUSDT",
"1000RATSUSDT",
"1000SHIBUSDT",
"1000XECUSDT",
"1INCHUSDT",
"1MBABYDOGEUSDT",
"2ZUSDT",

"AAVEUSDT",
"ACEUSDT",
"ACHUSDT",
"ACTUSDT",
"ADAUSDT",
"AEROUSDT",
"AGLDUSDT",
"AINUSDT",
"AIOUSDT",
"AIXBTUSDT",
"AKTUSDT",
"ALCHUSDT",
"ALGOUSDT",
"ALICEUSDT",
"ALLOUSDT",
"ALTUSDT",

"ANIMEUSDT",
"ANKRUSDT",
"APEUSDT",
"APEXUSDT",
"API3USDT",
"APRUSDT",
"APTUSDT",
"ARUSDT",
"ARBUSDT",
"ARCUSDT",
"ARIAUSDT",
"ARKUSDT",
"ARKMUSDT",
"ARPAUSDT",
"ASTERUSDT",
"ATUSDT",
"ATHUSDT",
"ATOMUSDT",
"AUCTIONUSDT",
"AVAXUSDT",
"AVNTUSDT",
"AWEUSDT",
"AXLUSDT",
"AXSUSDT",
"AZTECUSDT",
"BUSDT",
"B2USDT",
"BABAUSDT",
"BABYUSDT",
"BANUSDT",
"BANANAUSDT",
"BANANAS31USDT",
"BANKUSDT",
"BARDUSDT",
"BATUSDT",
"BCHUSDT",
"BEATUSDT",
"BERAUSDT",
"BGBUSDT",
"BIGTIMEUSDT",
"BIOUSDT",
"BIRBUSDT",
"BLASTUSDT",
"BLESSUSDT",
"BLURUSDT",
"BNBUSDT",
"BOMEUSDT",
"BRETTUSDT",
"BREVUSDT",
"BROCCOLIUSDT",
"BSVUSDT",
"BTCUSDT",
"BULLAUSDT",
"C98USDT",
"CAKEUSDT",
"CCUSDT",
"CELOUSDT",
"CFXUSDT",
"CHILLGUYUSDT",
"CHZUSDT",
"CLUSDT",
"CLANKERUSDT",
"CLOUSDT",
"COAIUSDT",
"COINUSDT",
"COMPUSDT",
"COOKIEUSDT",
"COWUSDT",
"CRCLUSDT",
"CROUSDT",
"CROSSUSDT",
"CRVUSDT",
"CTKUSDT",
"CVCUSDT",
"CVXUSDT",
"CYBERUSDT",
"CYSUSDT",
"DASHUSDT",
"DEEPUSDT",
"DENTUSDT",
"DEXEUSDT",
"DOGEUSDT",
"DOLOUSDT",
"DOODUSDT",
"DOTUSDT",
"DRIFTUSDT",
"DYDXUSDT",
"DYMUSDT",
"EGLDUSDT",
"EIGENUSDT",
"ENAUSDT",
"ENJUSDT",
"ENSUSDT",
"ENSOUSDT",
"EPICUSDT",
"ESPUSDT",
"ETCUSDT",
"ETHUSDT",
"ETHFIUSDT",
"EURUSDUSDT",
"FUSDT",
"FARTCOINUSDT",
"FETUSDT",
"FFUSDT",
"FIDAUSDT",
"FILUSDT",
"FLOKIUSDT",
"FLUIDUSDT",
"FOGOUSDT",
"FOLKSUSDT",
"FORMUSDT",
"GALAUSDT",
"GASUSDT",
"GBPUSDUSDT",
"GIGGLEUSDT",
"GLMUSDT",
"GMTUSDT",
"GMXUSDT",
"GOATUSDT",

"GPSUSDT",
"GRASSUSDT",
"GRIFFAINUSDT",
"GRTUSDT",
"GUNUSDT",
"GWEIUSDT",
"HUSDT",
"HBARUSDT",
"HEIUSDT",
"HEMIUSDT",
"HMSTRUSDT",
"HOLOUSDT",
"HOMEUSDT",
"HOODUSDT",
"HYPEUSDT",
"HYPERUSDT",
"ICNTUSDT",
"ICPUSDT",
"IDOLUSDT",
"ILVUSDT",
"IMXUSDT",
"INITUSDT",
"INJUSDT",
"INTCUSDT",
"INXUSDT",
"IOUSDT",
"IOTAUSDT",
"IOTXUSDT",
"IPUSDT",
"JASMYUSDT",
"JCTUSDT",
"JSTUSDT",
"JTOUSDT",
"JUPUSDT",
"KAIAUSDT",
"KAITOUSDT",
"KASUSDT",
"KAVAUSDT",
"kBONKUSDT",
"KERNELUSDT",
"KGENUSDT",
"KITEUSDT",
"kPEPEUSDT",
"kSHIBUSDT",
"LAUSDT",
"LABUSDT",
"LAYERUSDT",
"LDOUSDT",
"LIGHTUSDT",
"LINEAUSDT",
"LINKUSDT",
"LITUSDT",
"LPTUSDT",
"LSKUSDT",
"LTCUSDT",
"LUNAUSDT",
"LUNCUSDT",
"LYNUSDT",
"MUSDT",
"MAGICUSDT",
"MAGMAUSDT",
"MANAUSDT",
"MANTAUSDT",
"MANTRAUSDT",
"MASKUSDT",
"MAVUSDT",
"MAVIAUSDT",
"MBOXUSDT",
"MEUSDT",
"MEGAUSDT",
"MELANIAUSDT",
"MEMEUSDT",
"MERLUSDT",
"METUSDT",
"METAUSDT",
"MEWUSDT",
"MINAUSDT",
"MMTUSDT",
"MNTUSDT",
"MONUSDT",
"MOODENGUSDT",
"MORPHOUSDT",
"MOVEUSDT",
"MOVRUSDT",
"MSFTUSDT",
"MSTRUSDT",
"MUUSDT",
"MUBARAKUSDT",
"MYXUSDT",
"NAORISUSDT",
"NEARUSDT",
"NEIROCTOUSDT",
"NEOUSDT",
"NEWTUSDT",
"NILUSDT",
"NMRUSDT",
"NOMUSDT",
"NOTUSDT",

"NXPCUSDT",
"ONDOUSDT",
"ONGUSDT",
"ONTUSDT",
"OPUSDT",
"OPENUSDT",
"OPNUSDT",
"ORCAUSDT",

"ORDIUSDT",
"OXTUSDT",
"PARTIUSDT",
"PAXGUSDT",
"PENDLEUSDT",
"PENGUUSDT",
"PEOPLEUSDT",
"PEPEUSDT",
"PHAUSDT",
"PIEVERSEUSDT",
"PIPPINUSDT",
"PLTRUSDT",
"PLUMEUSDT",
"PNUTUSDT",
"POLUSDT",
"POLYXUSDT",
"POPCATUSDT",
"POWERUSDT",
"PROMPTUSDT",
"PROVEUSDT",
"PUMPUSDT",
"PURRUSDT",
"PYTHUSDT",
"QUSDT",
"QNTUSDT",
"QQQUSDT",
"RAVEUSDT",
"RAYUSDT",

"RECALLUSDT",
"RENDERUSDT",
"RESOLVUSDT",
"REZUSDT",
"RIVERUSDT",
"ROBOUSDT",
"ROSEUSDT",
"RPLUSDT",
"RSRUSDT",
"RUNEUSDT",
"SUSDT",
"SAGAUSDT",
"SAHARAUSDT",
"SAMSUNGUSDT",
"SANDUSDT",
"SAPIENUSDT",
"SEIUSDT",
"SENTUSDT",
"SHIBUSDT",
"SIGNUSDT",
"SIRENUSDT",
"SKHYNIXUSDT",
"SKRUSDT",
"SKYUSDT",
"SKYAIUSDT",
"SLPUSDT",
"SNXUSDT",
"SOLUSDT",
"SOMIUSDT",
"SONICUSDT",
"SOONUSDT",
"SOPHUSDT",
"SPACEUSDT",
"SPKUSDT",
"SPXUSDT",
"SPYUSDT",
"SQDUSDT",
"SSVUSDT",
"STABLEUSDT",
"STBLUSDT",
"STEEMUSDT",
"STOUSDT",
"STRKUSDT",
"STXUSDT",
"SUIUSDT",
"SUNUSDT",
"SUPERUSDT",
"SUSHIUSDT",
"SYRUPUSDT",
"TUSDT",
"TACUSDT",
"TAGUSDT",
"TAIKOUSDT",
"TAOUSDT",
"THEUSDT",
"THETAUSDT",
"TIAUSDT",
"TNSRUSDT",
"TONUSDT",
"TOSHIUSDT",
"TOWNSUSDT",
"TRBUSDT",
"TRIAUSDT",
"TRUMPUSDT",
"TRXUSDT",

"TURBOUSDT",
"UAIUSDT",
"UBUSDT",
"UMAUSDT",
"UNIUSDT",
"USUSDT",
"USDCUSDT",
"USDKRWUSDT",
"USELESSUSDT",
"USUALUSDT",
"VANAUSDT",
"VANRYUSDT",
"VETUSDT",
"VINEUSDT",
"VIRTUALUSDT",
"VTHOUSDT",
"VVVUSDT",
"WUSDT",
"WALUSDT",
"WAXPUSDT",
"WCTUSDT",
"WETUSDT",
"WIFUSDT",
"WLDUSDT",
"WLFIUSDT",
"WOOUSDT",
"WTIUSDT",
"XAGUSDT",
"XAIUSDT",

"XAUTUSDT",
"XCUUSDT",
"XDCUSDT",
"XLMUSDT",
"XMRUSDT",
"XPDUSDT",
"XPINUSDT",
"XPLUSDT",

"XRPUSDT",
"XTZUSDT",
"XVGUSDT",
"YGGUSDT",
"YZYUSDT",
"ZAMAUSDT",
"ZBTUSDT",
"ZECUSDT",
"ZENUSDT",
"ZEREBROUSDT",
"ZETAUSDT",
"ZILUSDT",
"ZKUSDT",
"ZKCUSDT",
"ZKJUSDT",
"ZKPUSDT",
"ZORAUSDT",
"ZROUSDT",
}

EXCLUDED_KEYWORDS = ["XAU","PAXG","BTC","ETH","USDC","DAI","BUSD","UST"]
BITGET_BASE = "https://api.bitget.com"
GRAN_MAP    = {"5m":"5m","15m":"15m","1h":"1H","4h":"4H","1d":"1D"}
_cache      = {}

# ══════════════════════════════════════════════════════════════════════════════
#  COOLDOWN
# ══════════════════════════════════════════════════════════════════════════════
def load_cooldown():
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p) as f: data = json.load(f)
            now = time.time()
            return {k:v for k,v in data.items() if now-v < CONFIG["alert_cooldown_sec"]}
    except Exception: pass
    return {}

def save_cooldown(state):
    try:
        with open(CONFIG["cooldown_file"],"w") as f: json.dump(state, f)
    except Exception: pass

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym): return (time.time()-_cooldown.get(sym,0)) < CONFIG["alert_cooldown_sec"]
def set_cooldown(sym): _cooldown[sym]=time.time(); save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════════════════════
#  OI SNAPSHOTS
# ══════════════════════════════════════════════════════════════════════════════
_oi_snapshot = {}

def load_oi_snapshots():
    global _oi_snapshot
    try:
        p = CONFIG["oi_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f: data = json.load(f)
            now = time.time()
            _oi_snapshot = {sym:v for sym,v in data.items() if now-v.get("ts",0) < 7200}
            log.info(f"OI snapshots loaded: {len(_oi_snapshot)} coins")
        else: _oi_snapshot = {}
    except Exception: _oi_snapshot = {}

def save_oi_snapshots():
    try:
        with open(CONFIG["oi_snapshot_file"],"w") as f: json.dump(_oi_snapshot, f)
    except Exception: pass

# ══════════════════════════════════════════════════════════════════════════════
#  FUNDING SNAPSHOTS
# ══════════════════════════════════════════════════════════════════════════════
_funding_snapshots = {}

def load_funding_snapshots():
    global _funding_snapshots
    try:
        p = CONFIG["funding_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f: _funding_snapshots = json.load(f)
    except Exception: _funding_snapshots = {}

def save_funding_snapshots():
    try:
        with open(CONFIG["funding_snapshot_file"],"w") as f: json.dump(_funding_snapshots,f)
    except Exception: pass

def add_funding_snapshot(symbol, rate):
    if symbol not in _funding_snapshots: _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({"ts":time.time(),"funding":rate})
    if len(_funding_snapshots[symbol]) > 48: _funding_snapshots[symbol] = _funding_snapshots[symbol][-48:]

# ══════════════════════════════════════════════════════════════════════════════
#  HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=10):
    for attempt in range(2):
        try:
            r = _http_session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                log.warning("Rate limit 429 — tunggu 15s"); time.sleep(15); continue
            break
        except Exception:
            if attempt == 0: time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg, parse_mode="HTML"):
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("send_telegram: BOT_TOKEN/CHAT_ID tidak ada!"); return False
    if len(msg) > 4000: msg = msg[:3900] + "\n\n<i>...[dipotong]</i>"
    for attempt in range(2):
        try:
            payload = {"chat_id": CHAT_ID, "text": msg}
            if attempt == 0: payload["parse_mode"] = "HTML"
            r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                              data=payload, timeout=15)
            if r.status_code == 200: return True
            err_text = r.text[:300]
            if "can't parse" in err_text or "Bad Request" in err_text:
                msg = _html_mod.unescape(msg)
                for tag in ["<b>","</b>","<i>","</i>","<code>","</code>"]: msg = msg.replace(tag,"")
                continue
            log.warning(f"Telegram gagal: HTTP {r.status_code}"); return False
        except Exception as e:
            log.warning(f"Telegram exception attempt {attempt}: {e}")
            if attempt == 0: time.sleep(2)
    return False

def utc_now(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ══════════════════════════════════════════════════════════════════════════════
#  DATA FETCHERS (API unchanged)
# ══════════════════════════════════════════════════════════════════════════════
def get_all_tickers():
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", params={"productType":"usdt-futures"})
    if data and data.get("code") == "00000":
        return {t["symbol"]:t for t in data.get("data",[])}
    return {}

def get_candles(symbol, gran="1h", limit=210):
    g   = GRAN_MAP.get(gran,"1H")
    key = f"c_{symbol}_{g}_{limit}"
    if key in _cache:
        ts, val = _cache[key]
        if time.time()-ts < 90: return val
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/candles",
                    params={"symbol":symbol,"granularity":g,"limit":str(limit),"productType":"usdt-futures"})
    if not data or data.get("code") != "00000": return []
    candles = []
    for c in data.get("data",[]):
        try:
            vol_usd = float(c[6]) if len(c) > 6 else float(c[5])*float(c[4])
            candles.append({"ts":int(c[0]),"open":float(c[1]),"high":float(c[2]),
                            "low":float(c[3]),"close":float(c[4]),"volume":float(c[5]),"volume_usd":vol_usd})
        except Exception: continue
    candles.sort(key=lambda x: x["ts"])
    _cache[key] = (time.time(), candles)
    return candles

def get_funding_rate(symbol):
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
                    params={"symbol":symbol,"productType":"usdt-futures"})
    if data and data.get("code") == "00000":
        try:
            d_list = data.get("data") or []
            if d_list:
                rate = float(d_list[0].get("fundingRate",0))
                add_funding_snapshot(symbol, rate)
                return rate
        except Exception: pass
    return 0.0

def get_open_interest(symbol):
    data = safe_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest",
                    params={"symbol":symbol,"productType":"usdt-futures"})
    if data and data.get("code") == "00000":
        try:
            d = data["data"]
            if isinstance(d,list) and d: d = d[0]
            elif isinstance(d,list): return 0.0
            if "openInterestList" in d:
                oi_list = d.get("openInterestList") or []
                oi = float(oi_list[0].get("openInterest",0)) if oi_list else 0.0
            else:
                oi = float(d.get("openInterest", d.get("holdingAmount",0)))
            price = float(d.get("indexPrice", d.get("lastPr",0)) or 0)
            if 0 < oi < 1e9 and price > 0: return oi * price
            return oi
        except Exception: pass
    return 0.0

def get_oi_change(symbol):
    global _oi_snapshot
    oi_now = get_open_interest(symbol)
    prev   = _oi_snapshot.get(symbol)
    if prev is None or oi_now <= 0:
        if oi_now > 0: _oi_snapshot[symbol] = {"ts":time.time(),"oi":oi_now}
        return {"oi_now":oi_now,"oi_prev":0.0,"change_pct":0.0,"is_new":True}
    oi_prev    = prev["oi"]
    change_pct = ((oi_now-oi_prev)/oi_prev*100) if oi_prev > 0 else 0.0
    _oi_snapshot[symbol] = {"ts":time.time(),"oi":oi_now}
    return {"oi_now":round(oi_now,2),"oi_prev":round(oi_prev,2),"change_pct":round(change_pct,2),"is_new":False}

# ══════════════════════════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════════════════════════
def calc_rsi(candles, period=14):
    if len(candles) < period+1: return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1,len(closes)):
        d = closes[i]-closes[i-1]; gains.append(max(d,0.0)); losses.append(max(-d,0.0))
    avg_g = sum(gains[:period])/period; avg_l = sum(losses[:period])/period
    for i in range(period,len(gains)):
        avg_g = (avg_g*(period-1)+gains[i])/period; avg_l = (avg_l*(period-1)+losses[i])/period
    if avg_l == 0: return 100.0
    return round(100.0-100.0/(1.0+avg_g/avg_l), 2)

def calc_bbw(candles, period=20):
    if len(candles) < period: return 0.04, 0.5
    closes = [c["close"] for c in candles[-period:]]
    mean   = sum(closes)/period
    std    = math.sqrt(sum((x-mean)**2 for x in closes)/period)
    bb_u, bb_l = mean+2*std, mean-2*std
    bbw = (bb_u-bb_l)/mean if mean > 0 else 0.04
    bb_pct = ((candles[-1]["close"]-bb_l)/(bb_u-bb_l)) if bb_u != bb_l else 0.5
    return round(bbw,5), round(bb_pct,3)

def calc_atr(candles, period=14):
    if len(candles) < period+1: return candles[-1]["close"]*0.01 if candles else 0.0
    trs = []
    for i in range(1,period+1):
        idx = len(candles)-i
        if idx < 1: break
        h,l,pc = candles[idx]["high"],candles[idx]["low"],candles[idx-1]["close"]
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    return sum(trs)/len(trs) if trs else candles[-1]["close"]*0.01

def calc_atr_n(candles, n):
    trs = []
    for i in range(1,min(n+1,len(candles))):
        idx = len(candles)-i
        if idx < 1: break
        h,l,pc = candles[idx]["high"],candles[idx]["low"],candles[idx-1]["close"]
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    return sum(trs)/len(trs) if trs else 0.0

def calc_price_pos(candles, lookback=48):
    if len(candles) < 2: return 0.5
    recent = candles[-lookback:] if len(candles) >= lookback else candles
    hi, lo = max(c["high"] for c in recent), min(c["low"] for c in recent)
    cur = candles[-1]["close"]
    if hi == lo: return 0.5
    return round((cur-lo)/(hi-lo), 3)

def calc_avg_volume_window(candles, start_idx, end_idx):
    """
    [FIX-02] Rata-rata volume dari window yang ditentukan (dari belakang).
    start_idx=0, end_idx=24  → 24 candle terbaru (24h)
    start_idx=24, end_idx=168 → candle 24-168 dari belakang (hari 2-7 = baseline)
    """
    n = len(candles)
    actual_end   = min(end_idx, n)
    actual_start = min(start_idx, actual_end)
    if actual_start == 0:
        window = candles[-actual_end:]
    else:
        window = candles[-actual_end:-actual_start] if actual_start < actual_end else []
    if not window: return 0.0
    return sum(c["volume_usd"] for c in window) / len(window)

# ══════════════════════════════════════════════════════════════════════════════
#  [FIX-08] PUMP HISTORY DETECTION — ROOT CAUSE FIX
# ══════════════════════════════════════════════════════════════════════════════
def detect_pump_history(c1h):
    """
    Cek apakah coin SUDAH PUMP dalam 72 jam terakhir.
    Ini adalah guard paling penting — semua sinyal teknikal lain bisa
    memberikan false positive untuk coin yang sudah pump dan konsolidasi.

    Return: {already_pumped, max_single_candle_pct, range_72h_pct, reason}
    """
    window = 72  # 72 candle 1H = 3 hari
    n      = min(window, len(c1h))
    if n < 6:
        return {"already_pumped":False,"max_single_candle_pct":0.0,"range_72h_pct":0.0,"reason":"data tidak cukup"}

    recent = c1h[-n:]

    # Check 1: Ada single candle yang naik > 8%?
    max_single_pct = 0.0
    for c in recent:
        if c["open"] > 0:
            candle_move = (c["close"] - c["open"]) / c["open"] * 100
            max_single_pct = max(max_single_pct, candle_move)

    if max_single_pct >= CONFIG["gate_pump_history_candle"]:  # 8%
        return {"already_pumped":True, "max_single_candle_pct":round(max_single_pct,1),
                "range_72h_pct":0.0, "reason":f"single candle pump +{max_single_pct:.1f}% dalam 72h"}

    # Check 2: Total range 72h terlalu besar?
    hi_72h = max(c["high"] for c in recent)
    lo_72h = min(c["low"]  for c in recent)
    range_72h = (hi_72h/lo_72h - 1)*100 if lo_72h > 0 else 0.0

    if range_72h >= CONFIG["gate_pump_range_72h"]:  # 25%
        return {"already_pumped":True, "max_single_candle_pct":round(max_single_pct,1),
                "range_72h_pct":round(range_72h,1), "reason":f"range 72h = {range_72h:.1f}% (sudah pump besar)"}

    return {"already_pumped":False, "max_single_candle_pct":round(max_single_pct,1),
            "range_72h_pct":round(range_72h,1), "reason":"bersih — tidak ada pump besar dalam 72h"}

# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 1 — COMPRESSION (max 20)
# ══════════════════════════════════════════════════════════════════════════════
def analyze_phase1_compression(c1h, pump_history):
    score, signals = 0, []

    # 1. BB Squeeze
    bbw, bb_pct = calc_bbw(c1h, 20)
    if bbw < CONFIG["p1_bbw_extreme"]:
        s = CONFIG["p1_score_bbw_extreme"]; score += s
        signals.append(f"🗜️ BB Extreme Squeeze BBW={bbw*100:.2f}% +{s}")
    elif bbw < CONFIG["p1_bbw_tight"]:
        s = CONFIG["p1_score_bbw_tight"]; score += s
        signals.append(f"🗜️ BB Squeeze BBW={bbw*100:.2f}% +{s}")

    # 2. ATR Contraction — [FIX-04] dengan guard post-pump
    atr14  = calc_atr_n(c1h, 14)
    atr100 = calc_atr_n(c1h, min(100, len(c1h)))
    atr_ratio = (atr14/atr100) if atr100 > 0 else 1.0

    skip_atr = pump_history["range_72h_pct"] >= CONFIG["p1_atr_pump_guard_pct"]
    if skip_atr:
        # [FIX-04] Jangan score ATR jika coin sudah punya range besar 72h
        # Post-pump sideways (ATR14 rendah vs ATR100 tinggi) mirip dengan pre-pump compression
        # Guard ini mencegah false positive
        signals.append(f"⚠️ ATR dilewati: range_72h={pump_history['range_72h_pct']:.1f}% (post-pump consolidation)")
    else:
        # Normal path: score ATR contraction hanya untuk coin yang benar-benar fresh
        if atr_ratio < CONFIG["p1_atr_strong_ratio"]:
            s = CONFIG["p1_score_atr_strong"]; score += s
            signals.append(f"📉 ATR Strong Contraction ratio={atr_ratio:.2f} +{s}")
        elif atr_ratio < CONFIG["p1_atr_contract_ratio"]:
            s = CONFIG["p1_score_atr_contract"]; score += s
            signals.append(f"📉 ATR Contracting ratio={atr_ratio:.2f} +{s}")

    # 3. Range Compression — [FIX-03] per-candle apple-to-apple
    range_ratio = 1.0
    if len(c1h) >= 12:
        # Current: rata-rata range 4 candle terbaru (per candle)
        recent4   = c1h[-4:]
        cur_range = sum(c["high"]-c["low"] for c in recent4) / 4

        # Historical: rata-rata range candle 5-52 (exclude 4 terbaru, 48 sebelumnya)
        hist_end   = min(52, len(c1h))
        hist_start = 4
        hist_window = c1h[-hist_end:-hist_start] if hist_end > hist_start else c1h[:-hist_start]
        if hist_window:
            avg_range = sum(c["high"]-c["low"] for c in hist_window) / len(hist_window)
            range_ratio = (cur_range/avg_range) if avg_range > 0 else 1.0

        if range_ratio < CONFIG["p1_range_compress_ratio"]:
            s = CONFIG["p1_score_range_compress"]; score += s
            signals.append(f"📦 Range Compression ratio={range_ratio:.2f} +{s}")

    score = min(score, CONFIG["p1_max_score"])
    return {"score":score,"signals":signals,"bbw":bbw,"bb_pct":bb_pct,
            "atr_ratio":round(atr_ratio,3),"range_ratio":round(range_ratio,3),"skip_atr":skip_atr}

# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 2 — ACCUMULATION (max 30)
# ══════════════════════════════════════════════════════════════════════════════
def analyze_phase2_accumulation(c1h, c15m, oi_data, price_change_24h):
    """
    [FIX-02] vol_accum baseline terpisah dari 24h terbaru
    [FIX-05] CVD weighted delta
    [FIX-10] OI score hanya jika harga flat
    """
    score, signals = 0, []

    # 1. Volume Accumulation — [FIX-02]
    vol_accum = 1.0
    if len(c1h) >= 48:
        # Baseline: hari ke-2 sampai ke-7 (candle 24 sampai 168 dari belakang)
        vol_baseline = calc_avg_volume_window(c1h, CONFIG["p2_vol_baseline_start"], CONFIG["p2_vol_baseline_end"])
        # Terkini: 24 jam terakhir
        vol_recent   = calc_avg_volume_window(c1h, 0, 24)
        vol_accum    = (vol_recent/vol_baseline) if vol_baseline > 0 else 1.0

        if vol_accum >= CONFIG["p2_vol_accum_extreme"]:
            s = CONFIG["p2_score_vol_extreme"]; score += s
            signals.append(f"📊 Vol Extreme {vol_accum:.1f}x baseline(day2-7) +{s}")
        elif vol_accum >= CONFIG["p2_vol_accum_strong"]:
            s = CONFIG["p2_score_vol_strong"]; score += s
            signals.append(f"📊 Vol Strong {vol_accum:.1f}x baseline +{s}")
        elif vol_accum >= CONFIG["p2_vol_accum_mild"]:
            s = CONFIG["p2_score_vol_mild"]; score += s
            signals.append(f"📊 Vol Accumulating {vol_accum:.1f}x baseline +{s}")

    # 2. Price Stability
    abs_chg = abs(price_change_24h)
    if abs_chg < CONFIG["p2_price_stable_pct"]:
        s = CONFIG["p2_score_price_stable"]; score += s
        signals.append(f"⚖️ Price Stable |{price_change_24h:+.1f}%| +{s}")
    elif abs_chg < CONFIG["p2_price_ok_pct"]:
        s = CONFIG["p2_score_price_ok"]; score += s
        signals.append(f"⚖️ Price OK |{price_change_24h:+.1f}%| +{s}")

    # 3. OI Expansion — [FIX-10] HANYA jika harga flat
    oi_change   = oi_data.get("change_pct", 0.0)
    is_oi_accum = False
    if not oi_data.get("is_new") and oi_change > 0 and abs_chg <= CONFIG["p2_oi_price_flat_max"]:
        if oi_change >= CONFIG["p2_oi_expand_strong"]:
            s = CONFIG["p2_score_oi_strong"]; score += s; is_oi_accum = True
            signals.append(f"📈 OI Strong +{oi_change:.1f}% + harga flat → AKUMULASI MURNI +{s}")
        elif oi_change >= CONFIG["p2_oi_expand_min"]:
            s = CONFIG["p2_score_oi_expand"]; score += s; is_oi_accum = True
            signals.append(f"📈 OI +{oi_change:.1f}% + harga flat +{s}")
    elif not oi_data.get("is_new") and oi_change > 0 and abs_chg > CONFIG["p2_oi_price_flat_max"]:
        signals.append(f"ℹ️ OI +{oi_change:.1f}% tapi harga naik {price_change_24h:+.1f}% — tidak di-score (breakout)")

    # 4. CVD Divergence — [FIX-05] weighted delta
    is_cvd_divergence = False
    candles_for_cvd = c15m if (c15m and len(c15m) >= CONFIG["p2_cvd_lookback"]) else c1h
    lookback        = CONFIG["p2_cvd_lookback"]

    if len(candles_for_cvd) >= lookback:
        recent_cvd = candles_for_cvd[-lookback:]
        buy_delta = sell_delta = 0.0

        for c in recent_cvd:
            if c["open"] <= 0: continue
            candle_range = c["high"] - c["low"]
            body_size    = abs(c["close"] - c["open"])
            # [FIX-05] Weight = vol × (body/range) — candle besar lebih berpengaruh
            weight = c["volume_usd"] * (body_size/candle_range if candle_range > 0 else 0.5)
            if c["close"] >= c["open"]: buy_delta  += weight
            else:                       sell_delta += weight

        cvd_ratio        = (buy_delta/sell_delta) if sell_delta > 0 else 1.0
        price_start      = recent_cvd[0]["open"]
        price_end        = recent_cvd[-1]["close"]
        price_chg_window = abs((price_end-price_start)/price_start*100) if price_start > 0 else 0.0

        if price_chg_window <= CONFIG["p2_cvd_price_flat_max"] and cvd_ratio >= CONFIG["p2_cvd_ratio_min"]:
            s = CONFIG["p2_score_cvd_divergence"]; score += s; is_cvd_divergence = True
            signals.append(f"🔍 CVD Divergence: flat({price_chg_window:.1f}%) buy/sell={cvd_ratio:.1f}x HIDDEN BUYING +{s}")
        elif cvd_ratio < 0.7 and price_change_24h > 3:
            signals.append(f"⚠️ CVD Warning: harga naik tapi sell dominan (ratio={cvd_ratio:.1f}) — distribusi?")
            score -= 5

    score = min(max(score,0), CONFIG["p2_max_score"])
    return {"score":score,"signals":signals,"vol_accum":round(vol_accum,2),
            "is_cvd_divergence":is_cvd_divergence,"is_oi_accum":is_oi_accum,"oi_change":round(oi_change,2)}

# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 3 — POSITION BUILD-UP (max 25)
# ══════════════════════════════════════════════════════════════════════════════
def analyze_phase3_position_build(c1h, oi_data, vol_accum, range_ratio, price_change_24h, funding_rate):
    score, signals = 0, []

    oi_change = oi_data.get("change_pct", 0.0)
    is_new_oi = oi_data.get("is_new", True)

    oi_expansion   = (1.0 + oi_change/100.0) if (not is_new_oi and oi_change > 0) else 1.0
    abs_price_chg  = max(abs(price_change_24h), 0.5)
    range_compress = max(range_ratio, 0.1)

    # Formula 1: position_score = oi_expansion × vol_accum
    position_score = oi_expansion * vol_accum
    if position_score >= CONFIG["p3_position_score_strong"]:
        s = CONFIG["p3_score_pos_strong"]; score += s
        signals.append(f"🏗️ Position Build STRONG: {position_score:.1f} +{s}")
    elif position_score >= CONFIG["p3_position_score_min"]:
        s = CONFIG["p3_score_pos_build"]; score += s
        signals.append(f"🏗️ Position Build: {position_score:.1f} +{s}")

    # Formula 2: liq_vacuum = vol_accum / range_compression
    liq_vacuum = vol_accum / range_compress
    if liq_vacuum >= CONFIG["p3_liq_vacuum_strong"]:
        s = CONFIG["p3_score_liq_strong"]; score += s
        signals.append(f"💨 Liquidity Vacuum STRONG: {liq_vacuum:.1f} +{s}")
    elif liq_vacuum >= CONFIG["p3_liq_vacuum_min"]:
        s = CONFIG["p3_score_liq_vacuum"]; score += s
        signals.append(f"💨 Liquidity Vacuum: {liq_vacuum:.1f} +{s}")

    # Formula 3: squeeze = oi_change% / abs(price_change) — [FIX-11]
    squeeze_score = 0.0
    if not is_new_oi and oi_change > 0:
        squeeze_score = oi_change / abs_price_chg
        if squeeze_score >= CONFIG["p3_squeeze_strong"]:
            s = CONFIG["p3_score_squeeze_strong"]; score += s
            signals.append(f"🔫 Squeeze STRONG: OI/price={squeeze_score:.1f} +{s}")
        elif squeeze_score >= CONFIG["p3_squeeze_min"]:
            s = CONFIG["p3_score_squeeze"]; score += s
            signals.append(f"🔫 Squeeze Setup: OI/price={squeeze_score:.1f} +{s}")

    # Formula 4: energy = position × liq_vacuum
    energy = position_score * liq_vacuum
    if energy >= CONFIG["p3_energy_strong"]:
        s = CONFIG["p3_score_energy_strong"]; score += s
        signals.append(f"⚡ Energy STRONG: {energy:.1f} +{s}")
    elif energy >= CONFIG["p3_energy_min"]:
        s = CONFIG["p3_score_energy"]; score += s
        signals.append(f"⚡ Energy Building: {energy:.1f} +{s}")

    # Funding negatif
    if funding_rate <= CONFIG["p3_funding_neg_threshold"]:
        s = CONFIG["p3_score_funding_neg"]; score += s
        signals.append(f"💸 Funding Negatif {funding_rate*100:.4f}% (short trap) +{s}")

    score = min(max(score,0), CONFIG["p3_max_score"])
    return {"score":score,"signals":signals,"position_score":round(position_score,2),
            "liq_vacuum":round(liq_vacuum,2),"squeeze_score":round(squeeze_score,2),"energy":round(energy,2)}

# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 4 — IGNITION PREPARATION (max 25)
# ══════════════════════════════════════════════════════════════════════════════
def analyze_phase4_ignition(c1h, c15m, pump_history):
    """
    [FIX-06] BOS hanya valid jika harga 0-12% di atas resistance (fresh breakout)
    [FIX-09] Higher Low perlu 2 konfirmasi berturut-turut
    """
    score, signals = 0, []

    # 1. Breakout Pressure: vol 1h / baseline vol (day 2-7)
    bp = 1.0
    if len(c1h) >= 48:
        vol_1h       = c1h[-1]["volume_usd"]
        vol_baseline = calc_avg_volume_window(c1h, CONFIG["p2_vol_baseline_start"], CONFIG["p2_vol_baseline_end"])
        bp = (vol_1h/vol_baseline) if vol_baseline > 0 else 1.0
        if bp >= CONFIG["p4_breakout_pressure_strong"]:
            s = CONFIG["p4_score_bp_strong"]; score += s
            signals.append(f"🚀 Breakout Pressure STRONG: {bp:.1f}x baseline +{s}")
        elif bp >= CONFIG["p4_breakout_pressure_mild"]:
            s = CONFIG["p4_score_bp_mild"]; score += s
            signals.append(f"🚀 Breakout Pressure: {bp:.1f}x baseline +{s}")

    # 2. Early Momentum (0.3-4% dalam 4h)
    momentum_pct = 0.0
    if len(c1h) >= 4:
        price_now    = c1h[-1]["close"]
        price_4h     = c1h[-4]["close"]
        momentum_pct = ((price_now-price_4h)/price_4h*100) if price_4h > 0 else 0.0
        if CONFIG["p4_momentum_min_pct"] <= momentum_pct <= 2.5:
            s = CONFIG["p4_score_momentum_strong"]; score += s
            signals.append(f"⚡ Momentum Perfect: +{momentum_pct:.1f}% (4h) +{s}")
        elif CONFIG["p4_momentum_min_pct"] <= momentum_pct <= CONFIG["p4_momentum_max_pct"]:
            s = CONFIG["p4_score_momentum_ok"]; score += s
            signals.append(f"⚡ Momentum OK: +{momentum_pct:.1f}% (4h) +{s}")
        elif momentum_pct > CONFIG["p4_momentum_max_pct"]:
            signals.append(f"⚠️ Momentum terlalu besar +{momentum_pct:.1f}%"); score -= 4

    # 3. Higher Low — [FIX-09] 2 konfirmasi
    lookback = CONFIG["p4_higher_low_lookback"]  # 24
    if len(c1h) >= lookback:
        lows     = [c["low"] for c in c1h[-lookback:]]
        seg_size = lookback // 3
        lo1 = min(lows[:seg_size])
        lo2 = min(lows[seg_size:2*seg_size])
        lo3 = min(lows[2*seg_size:])
        if lo1 < lo2 < lo3:
            s = CONFIG["p4_score_higher_low"]; score += s
            signals.append(f"📐 Higher Low 2x terkonfirmasi +{s}")

    # 4. BOS — [FIX-06] hanya valid untuk fresh breakout
    bos_lb = CONFIG["p4_bos_lookback"]  # 12
    if len(c1h) >= bos_lb + 4:
        prior_high  = max(c["high"] for c in c1h[-(bos_lb+4):-4])
        current_cls = c1h[-1]["close"]
        pct_above   = (current_cls/prior_high-1) if prior_high > 0 else 0

        if 0 < pct_above <= CONFIG["p4_bos_accum_zone_max"]:
            s = CONFIG["p4_score_bos"]; score += s
            signals.append(f"🔔 BOS Fresh: +{pct_above*100:.1f}% di atas resistance +{s}")
        elif pct_above > CONFIG["p4_bos_accum_zone_max"]:
            signals.append(f"⚠️ BOS: harga {pct_above*100:.1f}% di atas resistance — terlalu jauh (post-pump)")

    score = min(max(score,0), CONFIG["p4_max_score"])
    return {"score":score,"signals":signals,"breakout_pressure":round(bp,2),"momentum_pct":round(momentum_pct,2)}

# ══════════════════════════════════════════════════════════════════════════════
#  PUMP PROBABILITY & UTILS
# ══════════════════════════════════════════════════════════════════════════════
def calc_pump_probability(total_score):
    prob = 1.0/(1.0+math.exp(-(total_score-CONFIG["prob_center"])/CONFIG["prob_scale"]))
    return round(prob*100, 1)

def calc_pump_eta(p1, p2, p3, p4):
    if p4 >= 15: return "30–90 menit"
    elif p3 >= 15 and p4 >= 8: return "1–3 jam"
    elif p3 >= 12: return "2–4 jam"
    elif p2 >= 20: return "3–6 jam"
    else: return "4–8 jam"

def calc_tp_sl(price, atr):
    tp1 = price + CONFIG["tp1_atr_mult"]*atr
    tp2 = price + CONFIG["tp2_atr_mult"]*atr
    tp3 = price + CONFIG["tp3_atr_mult"]*atr
    sl  = price - CONFIG["sl_atr_mult"] *atr
    def fmt(p):
        if p >= 10: return f"{p:.4f}"
        if p >= 1:  return f"{p:.5f}"
        if p >= 0.01: return f"{p:.6f}"
        return f"{p:.8f}"
    return {"entry":fmt(price),"sl":fmt(sl),"tp1":fmt(tp1),"tp2":fmt(tp2),"tp3":fmt(tp3),
            "sl_pct":round(abs(price-sl)/price*100,1),
            "tp1_pct":round(abs(tp1-price)/price*100,1),
            "tp3_pct":round(abs(tp3-price)/price*100,1)}

# ══════════════════════════════════════════════════════════════════════════════
#  MASTER SCORE
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker_data):
    try:
        price_now   = float(ticker_data.get("lastPr", ticker_data.get("last",0)))
        vol_24h_usd = float(ticker_data.get("quoteVolume",0))
        # [FIX-01] Konversi change_24h yang benar
        raw_chg    = float(ticker_data.get("change24h", ticker_data.get("priceChangePercent",0)))
        change_24h = raw_chg*100.0 if abs(raw_chg) <= 2.0 else raw_chg
    except Exception: return None

    if price_now <= 0: return None
    if vol_24h_usd < CONFIG["min_vol_24h_usd"] or vol_24h_usd > CONFIG["max_vol_24h_usd"]: return None
    if change_24h > CONFIG["gate_already_pumped_pct"]: return None
    if change_24h < CONFIG["gate_dump_pct"]: return None

    c1h  = get_candles(symbol, "1h",  CONFIG["candles_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candles_15m"])
    if len(c1h) < 50: return None

    # [FIX-08] PUMP HISTORY GATE — WAJIB
    pump_hist = detect_pump_history(c1h)
    if pump_hist["already_pumped"]:
        log.debug(f"  {symbol}: SKIP pump_history — {pump_hist['reason']}"); return None

    rsi = calc_rsi(c1h)
    if rsi >= CONFIG["gate_rsi_max"]: return None

    # [FIX-07] Price position gate ketat
    price_pos = calc_price_pos(c1h, 48)
    if price_pos > CONFIG["gate_price_pos_max"]: return None

    oi_data      = get_oi_change(symbol)
    funding_rate = get_funding_rate(symbol)
    if oi_data["oi_now"] > 0 and oi_data["oi_now"] < CONFIG["min_oi_usd"]: return None

    atr14 = calc_atr(c1h, 14)

    ph1 = analyze_phase1_compression(c1h, pump_hist)
    ph2 = analyze_phase2_accumulation(c1h, c15m, oi_data, change_24h)
    ph3 = analyze_phase3_position_build(c1h, oi_data, ph2["vol_accum"], ph1["range_ratio"], change_24h, funding_rate)
    ph4 = analyze_phase4_ignition(c1h, c15m, pump_hist)

    total_score = ph1["score"] + ph2["score"] + ph3["score"] + ph4["score"]
    if total_score < CONFIG["min_score_watchlist"]: return None

    pump_prob = calc_pump_probability(total_score)
    eta       = calc_pump_eta(ph1["score"], ph2["score"], ph3["score"], ph4["score"])
    levels    = calc_tp_sl(price_now, atr14)

    return {
        "symbol":symbol,"price":price_now,"score":total_score,"pump_prob":pump_prob,
        "rsi":rsi,"change_24h":round(change_24h,2),"price_pos":price_pos,
        "vol_24h_usd":vol_24h_usd,"oi_change":ph2["oi_change"],"funding_rate":funding_rate,
        "eta":eta,"ph1_score":ph1["score"],"ph2_score":ph2["score"],
        "ph3_score":ph3["score"],"ph4_score":ph4["score"],
        "bbw":ph1["bbw"],"atr_ratio":ph1["atr_ratio"],"vol_accum":ph2["vol_accum"],
        "is_cvd_div":ph2["is_cvd_divergence"],"liq_vacuum":ph3["liq_vacuum"],
        "energy":ph3["energy"],"bp_ratio":ph4["breakout_pressure"],"momentum_pct":ph4["momentum_pct"],
        "pump_hist":pump_hist,"levels":levels,
        "signals":ph1["signals"]+ph2["signals"]+ph3["signals"]+ph4["signals"],
        "rank_value":total_score*pump_prob,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM ALERT
# ══════════════════════════════════════════════════════════════════════════════
def build_alert(r, rank=1):
    lv = r["levels"]
    if r["ph4_score"] >= 15:   phase_label = "🔥 IGNITION IMMINENT"
    elif r["ph3_score"] >= 18: phase_label = "⚡ POSITION BUILD-UP"
    elif r["ph2_score"] >= 20: phase_label = "📦 LATE ACCUMULATION"
    else:                      phase_label = "🗜️ COMPRESSION"

    oi_str = f"+{r['oi_change']:.1f}%" if r["oi_change"] > 0 else f"{r['oi_change']:.1f}%"
    return (
        f"🚨 <b>POTENTIAL PUMP</b>\n\n"
        f"Symbol: <b>{r['symbol']}</b>\n"
        f"Score: <b>{r['score']}</b>\n"
        f"Possible Pump: <b>{r['pump_prob']}%</b>\n\n"
        f"Entry: <code>{lv['entry']}</code>\n"
        f"SL   : <code>{lv['sl']}</code> (-{lv['sl_pct']}%)\n"
        f"TP   : <code>{lv['tp3']}</code> (+{lv['tp3_pct']}%)\n\n"
        f"Estimate pump: {r['eta']}\n\n"
        f"Phase : {phase_label}\n"
        f"RSI   : {r['rsi']:.0f} | OI: {oi_str} | Vol: {r['vol_accum']:.1f}x\n"
        f"24h   : {r['change_24h']:+.1f}% | Pos: {r['price_pos']:.0%}\n"
        f"Rank #{rank} — {utc_now()}"
    )

def build_summary(results):
    lines = [f"📊 <b>PUMP SCANNER v32 — {utc_now()}</b>\n",
             f"<b>{len(results)} kandidat pump terdeteksi:</b>\n"]
    for i, r in enumerate(results[:10], 1):
        icon = "🔥" if r["ph4_score"] >= 15 else ("⚡" if r["ph3_score"] >= 15 else "📦")
        lines.append(f"{i}. {icon} <b>{r['symbol']}</b> Score:<b>{r['score']}</b> ({r['pump_prob']}%) ETA:{r['eta']}")
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════════════════
#  CANDIDATE BUILDER
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    candidates, filtered_stats, not_found = [], defaultdict(int), []
    for sym in WHITELIST_SYMBOLS:
        if any(kw in sym for kw in EXCLUDED_KEYWORDS): filtered_stats["excluded_keyword"] += 1; continue
        t = tickers.get(sym)
        if t is None: not_found.append(sym); continue
        try: vol = float(t.get("quoteVolume",0))
        except: vol = 0.0
        if vol < CONFIG["min_vol_24h_usd"]: filtered_stats["low_volume"] += 1; continue
        try:
            raw_chg = float(t.get("change24h",0))
            chg = raw_chg*100.0 if abs(raw_chg) <= 2.0 else raw_chg
        except: chg = 0.0
        if chg > CONFIG["gate_already_pumped_pct"]: filtered_stats["already_pumped"] += 1; continue
        if chg < CONFIG["gate_dump_pct"]: filtered_stats["dump_filter"] += 1; continue
        try:
            price = float(t.get("lastPr", t.get("last",0)))
            if price <= 0: filtered_stats["invalid_price"] += 1; continue
        except: filtered_stats["invalid_price"] += 1; continue
        if is_cooldown(sym): filtered_stats["cooldown"] += 1; continue
        candidates.append((sym, t))

    total = len(WHITELIST_SYMBOLS); will_scan = len(candidates)
    log.info(f"\n📊 SCAN SUMMARY v32:\n   Total: {total} | Scan: {will_scan}")
    for k,v in sorted(filtered_stats.items()): log.info(f"   ❌ {k:25s}: {v}")
    if not_found: log.info(f"   ⚠️  Tidak di Bitget: {len(not_found)}")
    est = will_scan*CONFIG["sleep_coins"]
    log.info(f"   ⏱️  Est: {est:.0f}s ({est/60:.1f} menit)\n")
    return candidates

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PUMP SCANNER v32 — {utc_now()} ===")
    load_funding_snapshots(); load_oi_snapshots()
    tickers = get_all_tickers()
    if not tickers: send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget"); return
    log.info(f"Total ticker: {len(tickers)}")
    candidates = build_candidate_list(tickers)
    results, t_start, n_err = [], time.time(), 0

    for i, (sym, t) in enumerate(candidates):
        try: vol = float(t.get("quoteVolume",0))
        except: vol = 0.0
        if (i+1) % 10 == 0 or i == 0:
            log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e6:.1f}M)...")
        try:
            res = master_score(sym, t)
            if res:
                log.info(f"  ✅ {sym}: Score={res['score']} Prob={res['pump_prob']}% "
                         f"Ph1:{res['ph1_score']} Ph2:{res['ph2_score']} "
                         f"Ph3:{res['ph3_score']} Ph4:{res['ph4_score']} ETA:{res['eta']}")
                results.append(res)
        except Exception as ex:
            import traceback as _tb
            log.warning(f"  ❌ Error {sym}: {type(ex).__name__}: {ex}")
            log.debug(_tb.format_exc().strip()); n_err += 1
        time.sleep(CONFIG["sleep_coins"])

    save_oi_snapshots(); save_funding_snapshots()
    results.sort(key=lambda x: x["rank_value"], reverse=True)
    t_total = time.time()-t_start
    log.info(f"\n📊 SCAN FUNNEL v32: {len(candidates)} scanned → {len(results)} lolos | ❌{n_err} errors | ⏱{t_total:.1f}s\n")

    if not results: log.info("Tidak ada sinyal memenuhi syarat."); return
    top = results[:CONFIG["max_alerts_per_run"]]

    if len(top) >= 2: send_telegram(build_summary(top)); time.sleep(2)
    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok: set_cooldown(r["symbol"]); log.info(f"✅ Alert #{rank}: {r['symbol']} Score={r['score']}")
        time.sleep(2)
    log.info(f"=== SELESAI v32 — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  ALTCOIN PUMP SCANNER v32 — 11 BUG DIPERBAIKI              ║")
    log.info("║  Deteksi pump 20-70% SEBELUM terjadi, bukan sesudah        ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")
    if not BOT_TOKEN or not CHAT_ID: log.error("FATAL: BOT_TOKEN/CHAT_ID tidak ada!"); exit(1)
    run_scan()
