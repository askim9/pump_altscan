
import requests
import time
import os
import math
import json
import logging
import logging.handlers as _lh
import html as _html_mod
import numpy as np
from datetime import datetime, timezone
from collections import defaultdict

# v26 PERF: Persistent HTTP session for connection reuse across ~342 symbols
# Eliminates TCP handshake overhead on every API call
_http_session = requests.Session()
_http_session.headers.update({"User-Agent": "CryptoScanner/34.0", "Accept-Encoding": "gzip"})
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

_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v34.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Scanner v34 — log aktif: /tmp/scanner_v34.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # File persistence paths
    "cooldown_file":          "./cooldown.json",
    "oi_snapshot_file":       "./oi_snapshot.json",
    "oi_history_file":        "./oi_history.json",
    "funding_snapshot_file":  "./funding_snapshots.json",

    # Timing
    "sleep_between_symbols":  0.12,   # seconds between per-symbol API calls
    "sleep_error":            2.0,    # seconds to wait after a non-429 error
    "scan_interval":          300,    # seconds between full scans (5 min)
    "alert_cooldown_sec":     3600,   # 1 hour cooldown per coin after alert

    # OI history
    "oi_history_max_entries": 40,

    # Ignition detection thresholds (WAJIB — JANGAN DIUBAH)
    "bbw_percentile_threshold":       20,     # ≤ persentil ke-20
    "atr_ratio_threshold":            0.75,   # ATR(6)/ATR(24) < 0.75
    "range_4h_threshold":             0.025,  # (high-low)/close < 2.5%
    "compression_score_bb":           15,
    "compression_score_atr":          10,
    "compression_score_range":        5,
    "oi_slope_threshold":             0.01,   # slope_normalized > 0.01
    "oi_burst_ratio_threshold":       1.5,    # burst ratio > 1.5
    "oi_conviction_formula_mult":     1000,   # min(100, slope * 1000)
    "orderflow_accum_imbalance":      1.2,    # weighted_imbalance > 1.2
    "orderflow_accum_buy_mult":       2.0,    # buy_vol > 2× sell_vol
    "supply_removal_velocity_pct":   -5.0,    # < -5% per menit
    "supply_removal_level_threshold": 2,      # > 2 level hilang
    "supply_removal_score_velocity":  20,
    "supply_removal_score_level":     10,
    "ignition_prior":                 0.15,
    "w_compression":                  1.5,
    "w_oi_conviction":                1.3,
    "w_orderflow":                    1.8,
    "w_supply_removal":               2.0,
    "w_total":                        6.6,
    "prob_watchlist":                 30,
    "prob_alert":                     50,
    "prob_strong_alert":              70,

    # Classify regime thresholds
    "regime_ignition_compression":    20,
    "regime_ignition_slope":          0.01,
    "regime_ignition_funding":       -0.0001,
    "regime_ignition_imbalance":      1.1,
    "regime_breakout_compression":    10,
    "regime_breakout_imbalance":      1.3,

    # Order book snapshot history limit
    "ob_snapshot_max":                5,

    # Order flow time decay
    "orderflow_half_life_sec":        30,
    "orderflow_window_sec":           60,
}

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST
# ══════════════════════════════════════════════════════════════════════════════
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
"AMZNUSDT",
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
"RDDTUSDT",
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

GRAN_MAP    = {"5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
BITGET_BASE = "https://api.bitget.com"
_cache      = {}

# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN
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
    except Exception:
        pass
    return {}

def save_cooldown(state):
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass

_cooldown = load_cooldown()
log.info(f"Cooldown aktif: {len(_cooldown)} coin")

def is_cooldown(sym):
    return (time.time() - _cooldown.get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym):
    _cooldown[sym] = time.time()
    save_cooldown(_cooldown)

# ══════════════════════════════════════════════════════════════════════════════
#  💾  FUNDING SNAPSHOTS
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
    # Simpan hanya 48 snapshot terakhir per coin
    if len(_funding_snapshots[symbol]) > 48:
        _funding_snapshots[symbol] = _funding_snapshots[symbol][-48:]

# ══════════════════════════════════════════════════════════════════════════════
#  💾  OI SNAPSHOTS — FIX v18: PERSISTEN KE DISK
# ══════════════════════════════════════════════════════════════════════════════
_oi_snapshot = {}
# PRE-PUMP ENGINE v28: ask-side liquidity snapshot for supply removal detection
# v34: diubah menjadi list per symbol (history maksimal 5 snapshot)
_ob_ask_snapshot = {}   # {symbol: [{"ts": float, "ask_vol": float, "ask_levels": [...]}]}

# v30 NEW: Rolling OI history buffer for OI Acceleration signal
# Structure: {symbol: [{"ts": float, "oi": float}, ...]}  (up to 40 entries)
_oi_history = {}  # {symbol: [{"ts": float, "oi": float}, ...]}

def load_oi_snapshots():
    """
    FIX v18: Load OI snapshot dari disk saat startup.
    Sebelumnya (v15.7) _oi_snapshot hanya in-memory → reset tiap restart
    → OI change selalu is_new=True → energy_buildup dan OI scoring tidak pernah
    aktif di run pertama setelah restart.
    """
    global _oi_snapshot
    try:
        p = CONFIG["oi_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            # Buang snapshot yang sudah lebih dari 2 jam (stale data)
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
    """FIX v18: Simpan OI snapshot ke disk setelah tiap scan."""
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
            json.dump(_oi_snapshot, f)
    except Exception:
        pass


# ── v30 TASK 4: OI history persistence ───────────────────────────────────────

def load_oi_history():
    """
    FIX v30 TASK 4: Load OI history buffer from disk on startup.
    Prevents the 10-minute cold-start period where OI Acceleration cannot fire
    after a process restart.  Stale entries (>20 min) are pruned on load.
    """
    global _oi_history
    try:
        p = CONFIG.get("oi_history_file", "./oi_history.json")
        if os.path.exists(p):
            with open(p) as f:
                raw = json.load(f)
            now = time.time()
            # Prune entries older than 20 minutes (1200s) — beyond useful lookback
            loaded = {}
            for sym, entries in raw.items():
                fresh = [e for e in entries if now - e.get("ts", 0) < 1200]
                if fresh:
                    loaded[sym] = fresh[-40:]   # enforce new cap
            _oi_history = loaded
            log.info(f"OI history loaded: {len(_oi_history)} symbols")
        else:
            _oi_history = {}
    except Exception:
        _oi_history = {}


def save_oi_history():
    """FIX v30 TASK 4: Persist OI history buffer to disk after each scan."""
    try:
        p = CONFIG.get("oi_history_file", "./oi_history.json")
        with open(p, "w") as f:
            json.dump(_oi_history, f)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=10):
    # v26 PERF: use persistent session for connection reuse
    for attempt in range(2):
        try:
            r = _http_session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                log.warning("Rate limit — tunggu 15s, lalu retry")
                time.sleep(15)
                continue   # retry setelah 429
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

def _safe_telegram_text_v22(msg: str) -> str:
    """
    v22: Sanitize pesan Telegram HTML.
    1. Escape & menjadi &amp; (kecuali yang sudah di dalam entity)
    2. Truncate ke 4050 karakter
    """
    import re
    # Escape bare ampersands yang bukan bagian dari HTML entity
    msg = re.sub(r'&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)', '&amp;', msg)
    if len(msg) > 4050:
        msg = msg[:4000] + "\n<i>...[dipotong]</i>"
    return msg

def _safe_telegram_text(msg):
    """
    FIX 13 v22 — Enhanced Telegram message sanitizer (delegates to v22 impl).
    Handles: & escaping, broken tags, truncation to 4050 chars.
    """
    return _safe_telegram_text_v22(msg)

def send_telegram(msg, parse_mode="HTML"):
    """
    STEP 17 v19 — Fixed Telegram sender dengan:
    1. html.escape fallback jika HTML parse mode gagal
    2. Retry tanpa parse_mode jika masih gagal
    3. Truncate aman dengan mempertahankan tag
    """
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
            # attempt 1: tanpa parse_mode (plain text fallback)
            r = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data=payload,
                timeout=15,
            )
            if r.status_code == 200:
                return True
            err_text = r.text[:300]
            if "can\'t parse" in err_text or "Bad Request" in err_text:
                log.warning(f"Telegram parse error attempt {attempt} — retry plain text")
                # Coba kirim ulang tanpa HTML
                msg = _html_mod.unescape(msg)
                msg = msg.replace("<b>","").replace("</b>","")
                msg = msg.replace("<i>","").replace("</i>","")
                msg = msg.replace("<code>","").replace("</code>","")
                msg = msg.replace("<pre>","").replace("</pre>","")
                continue
            log.warning(f"Telegram gagal: HTTP {r.status_code} — {err_text}")
            return False
        except Exception as e:
            log.warning(f"Telegram exception attempt {attempt}: {e}")
            if attempt == 0:
                time.sleep(2)
    return False

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

# ══════════════════════════════════════════════════════════════════════════════
#  📡  DATA FETCHERS
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

def get_funding(symbol):
    """Ambil funding rate terkini. Guard: cek data["data"] tidak kosong."""
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

def get_btc_candles_cached(limit=48):
    """Cache candle BTCUSDT 1h selama 5 menit — hemat ~100 API call per scan."""
    global _btc_candles_cache
    if time.time() - _btc_candles_cache["ts"] < 300 and _btc_candles_cache["data"]:
        return _btc_candles_cache["data"]
    candles = get_candles("BTCUSDT", "1h", limit)
    if candles:
        _btc_candles_cache = {"ts": time.time(), "data": candles}
    return candles

def get_funding_stats(symbol):
    """Hitung statistik funding dari snapshot in-memory."""
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

def get_open_interest(symbol):
    """Ambil Open Interest dari Bitget Futures API. Guard: cek list tidak kosong."""
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
    """
    FIX v18: Hitung % perubahan OI menggunakan snapshot yang sudah di-load dari disk.
    Sebelumnya (v15.7) _oi_snapshot hanya in-memory sehingga selalu is_new=True
    di setiap restart — menyebabkan energy_buildup dan OI scoring tidak pernah aktif.
    """
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
# 🆕 IGNITION DETECTION MODULES
# ══════════════════════════════════════════════════════════════════════════════

def _calc_atr(candles, period):
    """
    Hitung Average True Range (ATR) dengan periode tertentu.
    True Range = max(high-low, |high-prev_close|, |low-prev_close|)
    Return rata-rata TR dari `period` candle terakhir. Return 0.0 jika data kurang.
    """
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return 0.0
    return sum(trs[-period:]) / period


def _calc_bbw(candles, period=20):
    """
    Hitung Bollinger Band Width untuk setiap posisi dalam candles.
    BBW = (upper - lower) / middle  dimana middle = SMA(period), upper/lower = middle ± 2*std.
    Return list BBW dengan panjang sama dengan len(candles), nilai None untuk posisi awal
    yang tidak cukup data.
    """
    closes = [c["close"] for c in candles]
    result = [None] * len(closes)
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1: i + 1]
        mean = sum(window) / period
        variance = sum((x - mean) ** 2 for x in window) / period
        std = math.sqrt(variance) if variance > 0 else 0.0
        upper = mean + 2 * std
        lower = mean - 2 * std
        bbw = (upper - lower) / mean if mean != 0 else 0.0
        result[i] = bbw
    return result


def detect_compression_phase(c1h, c4h):
    """
    Deteksi fase kompresi harga sebagai indikator awal ignition.

    Parameter:
        c1h (list): list candle timeframe 1 jam
        c4h (list): list candle timeframe 4 jam

    Return:
        dict: {
            "compression_score": int (0-30),
            "bbw_tight": bool,
            "atr_tight": bool,
            "range_tight": bool
        }

    Logika:
        - BBW ≤ persentil ke-20 dari 50 candle terakhir → bbw_tight, skor +15
        - ATR(6)/ATR(24) < 0.75 → atr_tight, skor +10
        - (high-low)/close candle 4h terakhir < 2.5% → range_tight, skor +5
    """
    bbw_tight   = False
    atr_tight   = False
    range_tight = False

    # ── 1. BB Width persentil ─────────────────────────────────────────────────
    if len(c1h) >= 50:
        bbw_series = _calc_bbw(c1h, period=20)
        # Ambil 50 nilai BBW terakhir yang valid (tidak None)
        valid_bbw = [v for v in bbw_series if v is not None]
        if len(valid_bbw) >= 20:
            history_50 = valid_bbw[-50:]
            current_bbw = history_50[-1]
            sorted_hist = sorted(history_50)
            pct_20_idx  = max(0, int(len(sorted_hist) * 0.20) - 1)
            pct_20_val  = sorted_hist[pct_20_idx]
            bbw_tight   = current_bbw <= pct_20_val
            log.debug(f"BBW current={current_bbw:.4f} pct20={pct_20_val:.4f} tight={bbw_tight}")

    # ── 2. ATR ratio ──────────────────────────────────────────────────────────
    if len(c1h) >= 25:
        atr6  = _calc_atr(c1h, 6)
        atr24 = _calc_atr(c1h, 24)
        if atr24 > 0:
            ratio     = atr6 / atr24
            atr_tight = ratio < CONFIG["atr_ratio_threshold"]
            log.debug(f"ATR(6)/ATR(24) = {ratio:.3f} tight={atr_tight}")

    # ── 3. 4H candle range ────────────────────────────────────────────────────
    if c4h:
        last4h = c4h[-1]
        if last4h["close"] > 0:
            rng        = (last4h["high"] - last4h["low"]) / last4h["close"]
            range_tight = rng < CONFIG["range_4h_threshold"]
            log.debug(f"4H range={rng:.4f} tight={range_tight}")

    # ── Skor kompresi ─────────────────────────────────────────────────────────
    score = (
        (CONFIG["compression_score_bb"]    if bbw_tight   else 0) +
        (CONFIG["compression_score_atr"]   if atr_tight   else 0) +
        (CONFIG["compression_score_range"] if range_tight else 0)
    )

    return {
        "compression_score": score,
        "bbw_tight":         bbw_tight,
        "atr_tight":         atr_tight,
        "range_tight":       range_tight,
    }


def analyze_oi_trend(symbol):
    """
    Analisis tren Open Interest dari rolling history buffer.

    Parameter:
        symbol (str): simbol trading, e.g. "DOGEUSDT"

    Return:
        dict: {
            "slope_normalized": float,
            "is_burst": bool,
            "conviction_score": int (0-100)
        }

    Logika:
        - Ambil minimal 10 entry dari _oi_history[symbol]
        - Hitung slope OI vs waktu dengan regresi linear (np.polyfit)
        - Normalize slope dengan rata-rata OI
        - Burst ratio = mean(3 OI terakhir) / mean(7 OI sebelumnya) > 1.5 → is_burst
        - Jika is_burst → conviction_score = 0
        - Jika tidak → conviction_score = min(100, slope_normalized * 1000)
    """
    history = _oi_history.get(symbol, [])
    if len(history) < 10:
        log.debug(f"analyze_oi_trend {symbol}: data tidak cukup ({len(history)} entries)")
        return {"slope_normalized": 0.0, "is_burst": False, "conviction_score": 0}

    entries    = history[-40:]  # Gunakan maksimal 40 entry
    times      = np.array([e["ts"] for e in entries], dtype=float)
    oi_vals    = np.array([e["oi"] for e in entries], dtype=float)
    mean_oi    = float(np.mean(oi_vals)) if np.mean(oi_vals) != 0 else 1.0

    # Normalisasi waktu untuk numerik stability
    times_norm = times - times[0]

    # Regresi linear: OI = slope * t + intercept
    coeffs          = np.polyfit(times_norm, oi_vals, 1)
    raw_slope       = float(coeffs[0])
    slope_normalized = raw_slope / mean_oi if mean_oi != 0 else 0.0

    # Burst ratio: rata-rata 3 OI terakhir vs rata-rata 7 OI sebelumnya
    last3   = oi_vals[-3:]
    prev7   = oi_vals[-10:-3] if len(oi_vals) >= 10 else oi_vals[:-3]
    mean_last3 = float(np.mean(last3)) if len(last3) > 0 else 0.0
    mean_prev7 = float(np.mean(prev7)) if len(prev7) > 0 else 1.0
    burst_ratio = mean_last3 / mean_prev7 if mean_prev7 > 0 else 0.0
    is_burst    = burst_ratio > CONFIG["oi_burst_ratio_threshold"]

    if is_burst:
        conviction_score = 0
    else:
        conviction_score = int(min(100, slope_normalized * CONFIG["oi_conviction_formula_mult"]))
        conviction_score = max(0, conviction_score)

    log.debug(
        f"analyze_oi_trend {symbol}: slope_norm={slope_normalized:.5f} "
        f"burst={is_burst} conviction={conviction_score}"
    )
    return {
        "slope_normalized": round(slope_normalized, 6),
        "is_burst":         is_burst,
        "conviction_score": conviction_score,
    }


def get_order_flow_imbalance(symbol):
    """
    Hitung weighted order flow imbalance menggunakan data trades terkini.

    Parameter:
        symbol (str): simbol trading

    Return:
        dict: {
            "weighted_imbalance": float,
            "is_accumulation": bool
        }

    Logika:
        - Ambil 500 trades terbaru dari /api/v2/mix/market/fills
        - Hitung bobot waktu: exp(-(now - trade_time) / half_life) untuk 60 detik terakhir
        - Imbalance = total_buy_weighted / total_sell_weighted
        - is_accumulation jika imbalance > 1.2 AND buy_vol > 2 * sell_vol
        - Fallback ke proxy candle 15m jika endpoint gagal
    """
    half_life  = CONFIG["orderflow_half_life_sec"]
    window_sec = CONFIG["orderflow_window_sec"]
    now        = time.time()

    weighted_buy  = 0.0
    weighted_sell = 0.0
    raw_buy       = 0.0
    raw_sell      = 0.0
    use_fallback  = False

    # ── Coba ambil dari endpoint trades ──────────────────────────────────────
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/fills",
        params={"symbol": symbol, "limit": "500", "productType": "usdt-futures"},
    )

    if data and data.get("code") == "00000":
        trades = data.get("data", [])
        if not trades:
            use_fallback = True
        else:
            for t in trades:
                try:
                    # Bitget timestamp dalam ms
                    trade_ts  = int(t.get("ts", t.get("fillTime", 0))) / 1000.0
                    age       = now - trade_ts
                    if age < 0 or age > window_sec:
                        continue
                    size      = float(t.get("size", t.get("baseVolume", 0)))
                    side      = str(t.get("side", "")).lower()
                    weight    = math.exp(-age / half_life)
                    if side == "buy":
                        weighted_buy  += size * weight
                        raw_buy       += size
                    elif side == "sell":
                        weighted_sell += size * weight
                        raw_sell      += size
                except Exception:
                    continue
    else:
        use_fallback = True

    # ── Fallback: proxy dari candle 15m ───────────────────────────────────────
    if use_fallback:
        log.debug(f"get_order_flow_imbalance {symbol}: fallback ke candle 15m")
        c15 = get_candles(symbol, "15m", limit=4)  # 4 candle × 15m ≈ 1 jam
        for candle in c15:
            # Estimasi buy volume: jika close > open → bullish → close_ratio * volume
            o, c, h, l = candle["open"], candle["close"], candle["high"], candle["low"]
            rng = h - l if (h - l) > 0 else 1.0
            buy_fraction  = (c - l) / rng  # 0 (pure sell) to 1 (pure buy)
            sell_fraction = 1.0 - buy_fraction
            vol = candle["volume"]
            # Uniform decay karena tidak ada timestamp per-trade
            weighted_buy  += vol * buy_fraction
            weighted_sell += vol * sell_fraction
            raw_buy       += vol * buy_fraction
            raw_sell      += vol * sell_fraction

    # ── Hitung imbalance ──────────────────────────────────────────────────────
    total_weighted = weighted_buy + weighted_sell
    if total_weighted > 0:
        weighted_imbalance = weighted_buy / (weighted_sell if weighted_sell > 0 else 1e-9)
    else:
        weighted_imbalance = 1.0  # neutral

    is_accumulation = (
        weighted_imbalance > CONFIG["orderflow_accum_imbalance"]
        and raw_buy > CONFIG["orderflow_accum_buy_mult"] * raw_sell
    )

    log.debug(
        f"get_order_flow_imbalance {symbol}: imbalance={weighted_imbalance:.3f} "
        f"accum={is_accumulation}"
    )
    return {
        "weighted_imbalance": round(weighted_imbalance, 4),
        "is_accumulation":    is_accumulation,
    }


def get_orderbook_snapshot(symbol):
    """
    Ambil snapshot order book (sisi ask) dari endpoint merge-depth Bitget.

    Parameter:
        symbol (str): simbol trading

    Return:
        dict: {
            "ask_volume": float,  # total volume 5 level ask teratas
            "ask_levels": list    # list harga ask [level1, level2, ...]
        }

    Jika gagal, return dummy dengan ask_volume=0, ask_levels=[].
    """
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/merge-depth",
        params={"symbol": symbol, "productType": "usdt-futures", "limit": "5"},
    )
    if data and data.get("code") == "00000":
        try:
            asks = data["data"].get("asks", [])
            # Format: [[price, size], ...]
            top5       = asks[:5]
            ask_levels = [float(a[0]) for a in top5]
            ask_volume = sum(float(a[1]) for a in top5)
            return {"ask_volume": ask_volume, "ask_levels": ask_levels}
        except Exception as e:
            log.debug(f"get_orderbook_snapshot {symbol} parse error: {e}")
    return {"ask_volume": 0.0, "ask_levels": []}


def detect_supply_removal(symbol, current_ob):
    """
    Deteksi penghapusan supply (ask) pada order book — sinyal smart money buying.

    Parameter:
        symbol     (str):  simbol trading
        current_ob (dict): output dari get_orderbook_snapshot(symbol), mengandung
                           "ask_volume" (float) dan "ask_levels" (list harga ask)

    Return:
        dict: {
            "removal_score": int (0-20),
            "velocity": float,        # perubahan ask_volume per snapshot (persen/menit)
            "critical_removed": bool  # > 2 level ask hilang dari snapshot sebelumnya
        }

    Logika:
        - Simpan current_ob ke _ob_ask_snapshot[symbol] (list, maks 5 entry)
        - velocity = (ask_vol terbaru - ask_vol pertama) / jumlah_snapshot (persen/menit)
        - Jika velocity < -5% per menit → removal_score += 20
        - Jika > 2 level ask hilang dari snapshot sebelumnya → removal_score += 10 (maks 20)
    """
    global _ob_ask_snapshot

    # Inisialisasi list jika belum ada
    if symbol not in _ob_ask_snapshot or not isinstance(_ob_ask_snapshot[symbol], list):
        _ob_ask_snapshot[symbol] = []

    # Simpan snapshot baru
    snapshot_entry = {
        "ts":         time.time(),
        "ask_vol":    current_ob.get("ask_volume", 0.0),
        "ask_levels": current_ob.get("ask_levels", []),
    }
    _ob_ask_snapshot[symbol].append(snapshot_entry)

    # Pertahankan maksimal 5 snapshot
    max_snap = CONFIG["ob_snapshot_max"]
    if len(_ob_ask_snapshot[symbol]) > max_snap:
        _ob_ask_snapshot[symbol] = _ob_ask_snapshot[symbol][-max_snap:]

    history = _ob_ask_snapshot[symbol]

    removal_score    = 0
    velocity         = 0.0
    critical_removed = False

    if len(history) < 2:
        return {
            "removal_score":    removal_score,
            "velocity":         velocity,
            "critical_removed": critical_removed,
        }

    # ── Velocity perubahan ask volume ─────────────────────────────────────────
    first_vol = history[0]["ask_vol"]
    last_vol  = history[-1]["ask_vol"]
    n_snaps   = len(history)

    if first_vol > 0:
        # Perubahan persen total, lalu bagi jumlah interval untuk per-snapshot
        total_pct_change = (last_vol - first_vol) / first_vol * 100.0
        # Konversi ke per-menit: estimasi interval ~5 menit per scan
        time_elapsed_min = (history[-1]["ts"] - history[0]["ts"]) / 60.0
        if time_elapsed_min > 0:
            velocity = total_pct_change / time_elapsed_min
        else:
            velocity = total_pct_change / (n_snaps - 1) if n_snaps > 1 else 0.0
    else:
        velocity = 0.0

    if velocity < CONFIG["supply_removal_velocity_pct"]:
        removal_score += CONFIG["supply_removal_score_velocity"]

    # ── Deteksi penghapusan level kritis ─────────────────────────────────────
    prev_levels    = set(round(p, 6) for p in history[-2]["ask_levels"])
    current_levels = set(round(p, 6) for p in history[-1]["ask_levels"])
    removed_levels = prev_levels - current_levels

    if len(removed_levels) > CONFIG["supply_removal_level_threshold"]:
        critical_removed = True
        removal_score    = min(
            CONFIG["supply_removal_score_velocity"],
            removal_score + CONFIG["supply_removal_score_level"]
        )

    log.debug(
        f"detect_supply_removal {symbol}: velocity={velocity:.2f}%/min "
        f"score={removal_score} critical={critical_removed}"
    )
    return {
        "removal_score":    removal_score,
        "velocity":         round(velocity, 4),
        "critical_removed": critical_removed,
    }


def classify_regime(compression_score, oi_trend, funding, orderflow):
    """
    Klasifikasi regime pasar berdasarkan kombinasi sinyal.

    Parameter:
        compression_score (int):   skor dari detect_compression_phase
        oi_trend (dict):           output dari analyze_oi_trend
        funding (float):           funding rate terkini
        orderflow (dict):          output dari get_order_flow_imbalance

    Return:
        str: "IGNITION_PREPARATION" | "BREAKOUT_CONFIRMATION" | "NEUTRAL"

    Logika:
        - IGNITION_PREPARATION: kompresi kuat + OI naik + funding negatif + akumulasi
        - BREAKOUT_CONFIRMATION: tidak kompresi + burst OI + strong imbalance
        - NEUTRAL: kondisi lainnya
    """
    slope      = oi_trend.get("slope_normalized", 0.0)
    is_burst   = oi_trend.get("is_burst", False)
    imbalance  = orderflow.get("weighted_imbalance", 1.0)

    if (
        compression_score >= CONFIG["regime_ignition_compression"]
        and slope         >  CONFIG["regime_ignition_slope"]
        and funding       <  CONFIG["regime_ignition_funding"]
        and imbalance     >  CONFIG["regime_ignition_imbalance"]
    ):
        return "IGNITION_PREPARATION"

    if (
        compression_score < CONFIG["regime_breakout_compression"]
        and is_burst
        and imbalance     > CONFIG["regime_breakout_imbalance"]
    ):
        return "BREAKOUT_CONFIRMATION"

    return "NEUTRAL"


def calculate_ignition_probability(compression, oi_conviction, orderflow, supply_removal):
    """
    Hitung probabilitas ignition menggunakan weighted scoring langsung.

    Parameter:
        compression    (dict): output dari detect_compression_phase
        oi_conviction  (dict): output dari analyze_oi_trend
        orderflow      (dict): output dari get_order_flow_imbalance
        supply_removal (dict): output dari detect_supply_removal

    Return:
        float: probabilitas ignition dalam persen (0.0 – 100.0)

    Formula:
        L_comp    = compression_score / 30          (bobot 1.5)
        L_oi      = conviction_score / 100          (bobot 1.3)
        L_flow    = weighted_imbalance / 2.0        (bobot 1.8, capped 1.0)
        L_supply  = removal_score / 20              (bobot 2.0)
        raw_score = 1.5*L_comp + 1.3*L_oi + 1.8*L_flow + 2.0*L_supply
        prob      = min(100, (raw_score / 6.6) * 100)
    """
    L_comp   = compression["compression_score"] / 30.0
    L_oi     = oi_conviction["conviction_score"] / 100.0
    L_flow   = min(1.0, orderflow["weighted_imbalance"] / 2.0)
    L_supply = supply_removal["removal_score"] / 20.0

    w_comp   = CONFIG["w_compression"]
    w_oi     = CONFIG["w_oi_conviction"]
    w_flow   = CONFIG["w_orderflow"]
    w_supply = CONFIG["w_supply_removal"]
    w_total  = CONFIG["w_total"]

    raw_score = (
        w_comp   * L_comp  +
        w_oi     * L_oi    +
        w_flow   * L_flow  +
        w_supply * L_supply
    )

    prob = min(100.0, (raw_score / w_total) * 100.0)

    return round(prob, 1)


def master_score_v2(symbol, ticker):
    """
    Master scoring function v2 — deteksi fase ignition lengkap.

    Parameter:
        symbol (str):  simbol trading, e.g. "DOGEUSDT"
        ticker (dict): data ticker dari get_all_tickers()

    Return:
        dict lengkap berisi semua komponen deteksi, probabilitas, regime, dan alert level.
        Return None jika data fundamental tidak tersedia.
    """
    try:
        price = float(ticker.get("lastPr", ticker.get("last", 0)) or 0)
    except Exception:
        price = 0.0

    # ── 1. Ambil candles ──────────────────────────────────────────────────────
    c1h = get_candles(symbol, "1h",  limit=80)   # 80 candle: cukup untuk BBW(20)+history(50)+ATR(24)
    c4h = get_candles(symbol, "4h",  limit=10)

    if not c1h or not c4h:
        log.warning(f"master_score_v2 {symbol}: candle data tidak tersedia")
        return None

    # ── 2. Compression ────────────────────────────────────────────────────────
    compression = detect_compression_phase(c1h, c4h)

    # ── 3. OI Trend ───────────────────────────────────────────────────────────
    # Update _oi_history sebelum analyze
    oi_now = get_open_interest(symbol)
    if oi_now > 0:
        if symbol not in _oi_history:
            _oi_history[symbol] = []
        _oi_history[symbol].append({"ts": time.time(), "oi": oi_now})
        cap = CONFIG.get("oi_history_max_entries", 40)
        if len(_oi_history[symbol]) > cap:
            _oi_history[symbol] = _oi_history[symbol][-cap:]

    oi_trend = analyze_oi_trend(symbol)

    # ── 4. Order Flow ─────────────────────────────────────────────────────────
    orderflow = get_order_flow_imbalance(symbol)

    # ── 5. Order Book Snapshot → Supply Removal ───────────────────────────────
    current_ob = get_orderbook_snapshot(symbol)
    supply     = detect_supply_removal(symbol, current_ob)

    # ── 6. Funding ────────────────────────────────────────────────────────────
    funding = get_funding(symbol)
    add_funding_snapshot(symbol, funding)

    # ── 7. Regime ─────────────────────────────────────────────────────────────
    regime = classify_regime(
        compression["compression_score"], oi_trend, funding, orderflow
    )

    # ── 8. Probabilitas Ignition ──────────────────────────────────────────────
    prob = calculate_ignition_probability(compression, oi_trend, orderflow, supply)

    # ── 9. Alert Level ────────────────────────────────────────────────────────
    if prob >= CONFIG["prob_strong_alert"]:
        alert_level = "STRONG ALERT"
    elif prob >= CONFIG["prob_alert"]:
        alert_level = "ALERT"
    elif prob >= CONFIG["prob_watchlist"]:
        alert_level = "WATCHLIST"
    else:
        alert_level = "IGNORE"

    log.info(
        f"master_score_v2 {symbol}: prob={prob}% alert={alert_level} "
        f"regime={regime} comp={compression['compression_score']} "
        f"conviction={oi_trend['conviction_score']} "
        f"imbalance={orderflow['weighted_imbalance']} "
        f"supply={supply['removal_score']}"
    )

    return {
        # Identitas
        "symbol":              symbol,
        "price":               price,
        # Hasil utama
        "prob":                prob,
        "alert_level":         alert_level,
        "regime":              regime,
        # Komponen deteksi
        "compression":         compression,
        "oi_trend":            oi_trend,
        "orderflow":           orderflow,
        "supply_removal":      supply,
        # Data tambahan
        "funding":             round(funding, 6),
        "oi_now":              round(oi_now, 2),
        "timestamp":           utc_now(),
    }


def build_ignition_alert(r):
    """
    Buat pesan Telegram ringkas untuk sinyal ignition.

    Parameter:
        r (dict): output dari master_score_v2

    Return:
        str: pesan HTML untuk dikirim via send_telegram()
    """
    sym    = r["symbol"]
    prob   = r["prob"]
    level  = r["alert_level"]
    regime = r["regime"]
    price  = r["price"]
    comp   = r["compression"]
    oi     = r["oi_trend"]
    flow   = r["orderflow"]
    sup    = r["supply_removal"]
    fund   = r["funding"]

    # Emoji berdasarkan alert level
    level_emoji = {
        "STRONG ALERT": "🚨",
        "ALERT":        "⚠️",
        "WATCHLIST":    "👀",
        "IGNORE":       "⬜",
    }.get(level, "ℹ️")

    regime_emoji = {
        "IGNITION_PREPARATION":  "🔥",
        "BREAKOUT_CONFIRMATION": "🚀",
        "NEUTRAL":               "➖",
    }.get(regime, "❓")

    # Sinyal aktif
    signals = []
    if comp["bbw_tight"]:
        signals.append("• BBW sangat sempit (squeeze)")
    if comp["atr_tight"]:
        signals.append("• ATR(6)/ATR(24) rendah (volatilitas rendah)")
    if comp["range_tight"]:
        signals.append("• Range 4H kecil (konsolidasi ketat)")
    if oi["slope_normalized"] > CONFIG["oi_slope_threshold"]:
        signals.append("• OI trend naik (akumulasi posisi)")
    if oi["is_burst"]:
        signals.append("• OI burst terdeteksi")
    if flow["is_accumulation"]:
        signals.append("• Order flow: akumulasi aktif")
    if sup["critical_removed"]:
        signals.append("• Level ask kritis dihapus (supply removal)")
    if fund < CONFIG["regime_ignition_funding"]:
        signals.append(f"• Funding negatif ({fund:.4%})")

    signals_text = "\n".join(signals) if signals else "• Tidak ada sinyal dominan"

    msg = (
        f"{level_emoji} <b>[{level}] {sym}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>Prob Ignition:</b> <code>{prob}%</code>\n"
        f"{regime_emoji} <b>Regime:</b> {regime}\n"
        f"💰 <b>Harga:</b> <code>${price:,.6g}</code>\n"
        f"\n"
        f"📊 <b>Komponen Skor:</b>\n"
        f"  Compression:  <code>{comp['compression_score']}/30</code>\n"
        f"  OI Conviction:<code>{oi['conviction_score']}/100</code>\n"
        f"  Orderflow:    <code>{flow['weighted_imbalance']:.3f}×</code>\n"
        f"  Supply Rmvl:  <code>{sup['removal_score']}/20</code>\n"
        f"\n"
        f"📡 <b>Sinyal Aktif:</b>\n{signals_text}\n"
        f"\n"
        f"🕐 {utc_now()}"
    )
    return msg


# ══════════════════════════════════════════════════════════════════════════════
#  🔄  SCAN LOOP
# ══════════════════════════════════════════════════════════════════════════════

def run_scan():
    """
    Loop utama scanner. Jalankan scan untuk semua symbol di whitelist,
    kirim alert Telegram jika prob ignition memenuhi threshold.
    """
    log.info("=== run_scan START ===")

    # Load semua data persisten di awal
    load_oi_snapshots()
    load_oi_history()        # WAJIB: load history OI sebelum analyze_oi_trend
    load_funding_snapshots()

    tickers = get_all_tickers()
    if not tickers:
        log.warning("Tidak ada ticker — skip scan")
        return

    results  = []
    scanned  = 0

    for sym in sorted(WHITELIST_SYMBOLS):
        ticker = tickers.get(sym)
        if not ticker:
            log.debug(f"Ticker tidak ditemukan: {sym}")
            continue

        try:
            r = master_score_v2(sym, ticker)
            if r is None:
                continue
            results.append(r)
            scanned += 1

            # Kirim alert jika threshold terpenuhi dan tidak dalam cooldown
            if r["alert_level"] in ("STRONG ALERT", "ALERT", "WATCHLIST"):
                if not is_cooldown(sym):
                    msg = build_ignition_alert(r)
                    sent = send_telegram(msg)
                    if sent:
                        set_cooldown(sym)
                        log.info(f"Alert terkirim: {sym} [{r['alert_level']}] {r['prob']}%")
                else:
                    log.debug(f"Cooldown aktif: {sym} — skip alert")

        except Exception as e:
            log.error(f"Error scanning {sym}: {e}", exc_info=True)

        time.sleep(CONFIG["sleep_between_symbols"])

    # Simpan semua data persisten di akhir scan
    save_oi_snapshots()
    save_oi_history()        # WAJIB: simpan history OI setelah scan
    save_all_funding_snapshots()

    # Ringkasan
    alerts = [r for r in results if r["alert_level"] != "IGNORE"]
    log.info(
        f"=== run_scan SELESAI === "
        f"Scanned: {scanned} | "
        f"Alerts: {len(alerts)} | "
        f"(SA={sum(1 for r in alerts if r['alert_level']=='STRONG ALERT')} "
        f"A={sum(1 for r in alerts if r['alert_level']=='ALERT')} "
        f"W={sum(1 for r in alerts if r['alert_level']=='WATCHLIST')})"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("Scanner v34 dimulai — interval scan: %ds", CONFIG["scan_interval"])
    while True:
        try:
            run_scan()
        except KeyboardInterrupt:
            log.info("Dihentikan oleh user.")
            break
        except Exception as e:
            log.error(f"run_scan error (outer): {e}", exc_info=True)

        log.info(f"Menunggu {CONFIG['scan_interval']}s sebelum scan berikutnya...")
        time.sleep(CONFIG["scan_interval"])
