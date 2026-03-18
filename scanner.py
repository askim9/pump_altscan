"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER SR v1.0                                               ║
║                                                                          ║
║  BERDASARKAN: Forensik 4 chart pump (TRUMP +51%, PIXEL +228%,           ║
║               ORCA +79%, VVV +165%) menggunakan indikator               ║
║               "Support and Resistance (High Volume Boxes) [ChartPrime]" ║
║                                                                          ║
║  FITUR BARU vs v9.10:                                                   ║
║    SR ENGINE  : Replikasi Pine Script SR indicator                      ║
║    • Support box  = pivotLow  + volume positif spike                   ║
║    • Resistance box = pivotHigh + volume negatif spike                  ║
║    • Break Res  = harga crossover resistance → sinyal pump              ║
║    • Break Sup  = harga crossunder support → ANTI-BREAK SUPPORT        ║
║    • Res→Sup flip = resistance lama jadi support → konfirmasi kuat     ║
║                                                                          ║
║  ENTRY/SL/TP CANGGIH:                                                   ║
║    • Entry = tepat di atas support box ATAU di retest level             ║
║    • SL = di BAWAH support box (bukan % fixed)                         ║
║    • TP1 = resistance terdekat berikutnya                               ║
║    • TP2 = resistance ke-2 / proyeksi ATR                              ║
║    • Anti-Break Support Gate: blok sinyal jika support baru saja jebol ║
║                                                                          ║
║  DIPERTAHANKAN dari v9.10:                                              ║
║    • Semua 324 coin whitelist                                           ║
║    • Net Flow multi-TF (GC-6)                                          ║
║    • Whale scoring, OI acceleration, Linea signature                   ║
║    • Format Telegram HTML (kompatibel 100%)                            ║
║    • Cooldown, OI snapshot, log file                                   ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import requests, time, os, math, json, logging, logging.handlers as _lh
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
_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)
_ch = logging.StreamHandler(); _ch.setFormatter(_log_fmt); _log_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler("/tmp/scanner_SR.log", maxBytes=10*1024*1024, backupCount=3)
_fh.setFormatter(_log_fmt); _log_root.addHandler(_fh)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # ── Alert threshold ───────────────────────────────────────
    "min_composite_alert":    38,
    "min_prob_alert":        0.35,
    "min_score_alert":        25,
    "min_whale_score":        10,
    "max_alerts_per_run":     15,

    # Bobot composite
    "composite_w_layer":     0.55,
    "composite_w_prob":      0.45,

    # ── Volume filter ─────────────────────────────────────────
    "min_vol_24h":          3_000,
    "max_vol_24h":     50_000_000,
    "pre_filter_vol":       1_000,

    # ── Gate ──────────────────────────────────────────────────
    "gate_chg_24h_max":       30.0,
    "gate_chg_7d_max":        35.0,
    "gate_chg_7d_min":       -35.0,
    "gate_funding_extreme":  -0.002,
    "dead_activity_threshold": 0.10,

    # ── Candle limits ─────────────────────────────────────────
    "candle_1h":              200,
    "candle_15m":              96,
    "candle_4h":               42,

    # ── SR Engine (replikasi Pine Script) ─────────────────────
    "sr_lookback":             20,    # lookbackPeriod Pine Script
    "sr_vol_len":               2,    # vol_len Pine Script
    "sr_atr_period":          200,    # ATR untuk box width
    "sr_box_width":           1.0,    # box_withd Pine Script (multiplier ATR)
    "sr_vol_percentile":       75,    # vol spike threshold (>75th = strong)
    "sr_min_touches":           2,    # minimum touches untuk valid SR
    "sr_zone_tolerance":      0.015,  # ±1.5% untuk zone touch detection
    "sr_retest_window":        10,    # bar window untuk retest detection
    "sr_break_confirm_bars":    3,    # bar konfirmasi setelah break

    # ── Anti-Break Support Gate ───────────────────────────────
    "abs_break_lookback":      20,    # cek 20 bar terakhir
    "abs_min_support_vol":   0.70,    # support harus ≥70th percentile vol
    "abs_block_if_broken":   True,    # hard block jika support baru jebol

    # ── Entry/SL/TP canggih ───────────────────────────────────
    "entry_above_support_pct":  0.3,  # entry 0.3% di atas support box top
    "entry_retest_max_pct":     2.0,  # max jauh dari support untuk dianggap retest
    "sl_below_support_pct":     0.5,  # SL 0.5% di bawah support box bottom
    "sl_atr_multiplier":        1.2,  # SL = support_bottom - 1.2x ATR (ambil yang lebih rendah)
    "tp1_resistance_buffer":    0.3,  # TP1 = resistance - 0.3% (sebelum resistance)
    "tp2_multiplier":           1.8,  # TP2 = TP1 + (TP1-entry)*0.8 tambahan
    "min_rr_ratio":             1.5,  # minimum R:R untuk alert
    "min_tp1_pct":              5.0,  # minimum TP1 dari entry
    "max_sl_pct":               6.0,  # maximum SL dari entry

    # ── Stealth ───────────────────────────────────────────────
    "stealth_max_vol":        80_000,
    "stealth_min_coiling":         6,
    "stealth_max_range":         4.0,

    # ── Short squeeze ─────────────────────────────────────────
    "squeeze_funding_max":   -0.0001,
    "squeeze_oi_change_min":    3.0,

    # ── Layer max scores ──────────────────────────────────────
    "max_vol_score":             50,
    "max_flat_score":            20,
    "max_struct_score":          15,
    "max_pos_score":             15,
    "max_tf4h_score":             8,
    "max_ctx_score":             10,
    "max_whale_bonus":           20,
    "max_linea_score":           25,
    "max_sr_score":              40,   # SR layer (BARU)
    "max_netflow_score":         25,
    "max_oi_accel_score":        30,

    # ── GC-2: Liquidation ─────────────────────────────────────
    "liq_window_min":            30,
    "liq_long_block_usd":   100_000,
    "liq_short_bonus_usd":  150_000,

    # ── GC-3: Linea Signature ─────────────────────────────────
    "linea_oi_1h_min":          2.0,
    "linea_oi_24h_min":         3.0,
    "linea_rsi_max":           48.0,
    "linea_ls_max":             1.1,
    "linea_price_max_chg":      5.0,

    # ── GC-5: Micro-cap OI Acceleration ──────────────────────
    "oi_accel_micro_thresh":3_000_000,
    "oi_accel_dormant_vol":   500_000,
    "oi_accel_weak":           15.0,
    "oi_accel_medium":         35.0,
    "oi_accel_strong":         70.0,
    "oi_accel_extreme":       120.0,
    "oi_accel_div_price_max":   5.0,
    "oi_dormant_baseline_mult": 3.0,

    # ── GC-6: Multi-TF Net Flow ───────────────────────────────
    "nf_strong_buy":           12.0,
    "nf_buy":                   5.0,
    "nf_neutral_max":           5.0,
    "nf_sell":                 -5.0,
    "nf_strong_sell":         -15.0,
    "nf_gate_72h":            -12.0,
    "nf_gate_24h":             -8.0,
    "nf_gate_6h":              -5.0,
    "nf_whale_72h_max":         3.0,
    "nf_whale_72h_min":       -15.0,
    "nf_whale_24h_min":         3.0,
    "nf_whale_6h_min":          5.0,

    # ── Operasional ───────────────────────────────────────────
    "alert_cooldown_sec":      1800,
    "sleep_coins":              0.8,
    "sleep_error":              3.0,
    "cooldown_file":   "/tmp/sr_cooldown.json",
    "oi_snapshot_file":"/tmp/sr_oi.json",
}

MANUAL_EXCLUDE = set()

# ══════════════════════════════════════════════════════════════
#  📋  WHITELIST (324 coin dari v9.10)
# ══════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
    "DOGEUSDT","BCHUSDT","ADAUSDT","HYPEUSDT","XMRUSDT","LINKUSDT","XLMUSDT","HBARUSDT",
    "LTCUSDT","ZECUSDT","AVAXUSDT","SHIBUSDT","SUIUSDT","TONUSDT","WLFIUSDT","CROUSDT",
    "UNIUSDT","DOTUSDT","TAOUSDT","MUSDT","AAVEUSDT","ASTERUSDT","PEPEUSDT","BGBUSDT",
    "SKYUSDT","ETCUSDT","NEARUSDT","ONDOUSDT","POLUSDT","ICPUSDT","WLDUSDT","ATOMUSDT",
    "XDCUSDT","COINUSDT","NIGHTUSDT","ENAUSDT","PIPPINUSDT","KASUSDT","TRUMPUSDT","QNTUSDT",
    "ALGOUSDT","RENDERUSDT","FILUSDT","MORPHOUSDT","APTUSDT","SUPERUSDT","VETUSDT","PUMPUSDT",
    "1000SATSUSDT","ARBUSDT","1000BONKUSDT","STABLEUSDT","KITEUSDT","JUPUSDT","SEIUSDT","ZROUSDT",
    "STXUSDT","DYDXUSDT","VIRTUALUSDT","DASHUSDT","PENGUUSDT","CAKEUSDT","JSTUSDT","XTZUSDT",
    "ETHFIUSDT","1MBABYDOGEUSDT","IPUSDT","LITUSDT","HUSDT","FETUSDT","CHZUSDT","CRVUSDT",
    "KAIAUSDT","IMXUSDT","BSVUSDT","INJUSDT","AEROUSDT","PYTHUSDT","IOTAUSDT","EIGENUSDT",
    "GRTUSDT","JASMYUSDT","DEXEUSDT","SPXUSDT","TIAUSDT","FLOKIUSDT","HNTUSDT","SIRENUSDT",
    "LDOUSDT","CFXUSDT","OPUSDT","ENSUSDT","STRKUSDT","MONUSDT","AXSUSDT","SANDUSDT",
    "PENDLEUSDT","WIFUSDT","LUNCUSDT","FFUSDT","NEOUSDT","THETAUSDT","RIVERUSDT","BATUSDT",
    "MANAUSDT","CVXUSDT","COMPUSDT","BARDUSDT","SENTUSDT","GALAUSDT","VVVUSDT","RAYUSDT",
    "XPLUSDT","FLUIDUSDT","FARTCOINUSDT","GLMUSDT","RUNEUSDT","0GUSDT","POWERUSDT","SKRUSDT",
    "EGLDUSDT","BUSDT","BERAUSDT","SNXUSDT","BANUSDT","JTOUSDT","ARUSDT","COWUSDT",
    "DEEPUSDT","SUSDT","LPTUSDT","MELANIAUSDT","UBUSDT","FOGOUSDT","ARCUSDT","WUSDT",
    "PIEVERSEUSDT","AWEUSDT","HOMEUSDT","GASUSDT","ICNTUSDT","ZENUSDT","XVGUSDT","ROSEUSDT",
    "MYXUSDT","KSMUSDT","RSRUSDT","ATHUSDT","KMNOUSDT","AKTUSDT","ZORAUSDT","ESPUSDT",
    "TOSHIUSDT","STGUSDT","ZILUSDT","LYNUSDT","APEUSDT","KAITOUSDT","FORMUSDT","AZTECUSDT",
    "QUSDT","MOVEUSDT","MINAUSDT","SOONUSDT","TUSDT","BRETTUSDT","ACHUSDT","TURBOUSDT",
    "NXPCUSDT","ALCHUSDT","ZETAUSDT","MOCAUSDT","CYSUSDT","ASTRUSDT","ENSOUSDT","AXLUSDT",
    "UAIUSDT","VTHOUSDT","RAVEUSDT","NMRUSDT","COAIUSDT","GWEIUSDT","MEUSDT","ORCAUSDT",
    "BLURUSDT","MERLUSDT","MOODENGUSDT","BIOUSDT","SOMIUSDT","B2USDT","ORDIUSDT","SPKUSDT",
    "ZAMAUSDT","PARTIUSDT","1000RATSUSDT","SSVUSDT","BIRBUSDT","POPCATUSDT","GUNUSDT","BEATUSDT",
    "BANANAS31USDT","LAUSDT","LINEAUSDT","DRIFTUSDT","AVNTUSDT","GRASSUSDT","GPSUSDT","PNUTUSDT",
    "CELOUSDT","LUNAUSDT","VANAUSDT","TRIAUSDT","IOTXUSDT","POLYXUSDT","ANKRUSDT","SAHARAUSDT",
    "RPLUSDT","MASKUSDT","UMAUSDT","TAGUSDT","USELESSUSDT","MEMEUSDT","ATUSDT","KGENUSDT",
    "SKYAIUSDT","ONTUSDT","ENJUSDT","SIGNUSDT","CTKUSDT","NOTUSDT","CYBERUSDT","GMTUSDT",
    "FIDAUSDT","CROSSUSDT","STEEMUSDT","LABUSDT","BREVUSDT","AUCTIONUSDT","HOLOUSDT","PEOPLEUSDT",
    "CVCUSDT","IOUSDT","BROCCOLIUSDT","SXTUSDT","CLANKERUSDT","BIGTIMEUSDT","BLASTUSDT","THEUSDT",
    "XPINUSDT","MANTAUSDT","YGGUSDT","WAXPUSDT","ONGUSDT","LAYERUSDT","ANIMEUSDT","BOMEUSDT",
    "C98USDT","API3USDT","AGLDUSDT","MMTUSDT","INXUSDT","GIGGLEUSDT","IDOLUSDT","ARKMUSDT",
    "RESOLVUSDT","EULUSDT","METISUSDT","SONICUSDT","TNSRUSDT","PROMUSDT","SAPIENUSDT","VELVETUSDT",
    "FLOCKUSDT","BANKUSDT","ALLOUSDT","USUALUSDT","SLPUSDT","ARIAUSDT","MIRAUSDT","MAGICUSDT",
    "ZKCUSDT","INUSDT","NAORISUSDT","MAGMAUSDT","REZUSDT","WCTUSDT","FUSDT","ELSAUSDT",
    "SPACEUSDT","APRUSDT","AIXBTUSDT","GOATUSDT","DENTUSDT","JCTUSDT","XAIUSDT","AIOUSDT",
    "ZKPUSDT","VINEUSDT","METAUSDT","FIGHTUSDT","INITUSDT","BASUSDT","NEWTUSDT","FUNUSDT",
    "FOLKSUSDT","ARPAUSDT","MOVRUSDT","MUBARAKUSDT","NOMUSDT","ACTUSDT","ZKJUSDT","VANRYUSDT",
    "AINUSDT","RECALLUSDT","MAVUSDT","CLOUSDT","LIGHTUSDT","TOWNSUSDT","BLESSUSDT","HAEDALUSDT",
    "4USDT","USUSDT","HEIUSDT","OGUSDT","PIXELUSDT",
}

GRAN_MAP = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}

SECTOR_MAP = {
    "DEFI":      ["SNXUSDT","ENSOUSDT","SIRENUSDT","CRVUSDT","CVXUSDT","COMPUSDT","AAVEUSDT",
                  "UNIUSDT","DYDXUSDT","COWUSDT","PENDLEUSDT","MORPHOUSDT","FLUIDUSDT","SSVUSDT",
                  "RSRUSDT","NMRUSDT","UMAUSDT","LDOUSDT","ENSUSDT"],
    "ZK_PRIVACY":["AZTECUSDT","MINAUSDT","STRKUSDT","ZORAUSDT","POLYXUSDT"],
    "DESCI":     ["BIOUSDT","ATHUSDT"],
    "AI_CRYPTO": ["FETUSDT","RENDERUSDT","TAOUSDT","GRASSUSDT","AKTUSDT","VANAUSDT",
                  "COAIUSDT","UAIUSDT","GRTUSDT"],
    "SOLANA_ECO":["ORCAUSDT","RAYUSDT","JTOUSDT","DRIFTUSDT","WIFUSDT","JUPUSDT",
                  "1000BONKUSDT","PYTHUSDT"],
    "LAYER1":    ["APTUSDT","SUIUSDT","SEIUSDT","INJUSDT","KASUSDT","BERAUSDT","MOVEUSDT",
                  "KAIAUSDT","TIAUSDT","EGLDUSDT","NEARUSDT","TONUSDT","ALGOUSDT","HBARUSDT"],
    "LAYER2":    ["ARBUSDT","OPUSDT","CELOUSDT","STRKUSDT","LDOUSDT","POLUSDT","LINEAUSDT"],
    "GAMING":    ["AXSUSDT","GALAUSDT","IMXUSDT","SANDUSDT","APEUSDT","SUPERUSDT","CHZUSDT","ENJUSDT"],
    "LOW_CAP":   ["VVVUSDT","POWERUSDT","ARCUSDT","AGLDUSDT","VIRTUALUSDT","SPXUSDT","ONDOUSDT",
                  "ENAUSDT","EIGENUSDT","STXUSDT","RUNEUSDT","ORDIUSDT","SKRUSDT","AEROUSDT"],
    "MEME":      ["PEPEUSDT","SHIBUSDT","FLOKIUSDT","BRETTUSDT","FARTCOINUSDT","MEMEUSDT",
                  "TURBOUSDT","PNUTUSDT","POPCATUSDT","MOODENGUSDT","1000BONKUSDT","TRUMPUSDT",
                  "WIFUSDT","TOSHIUSDT"],
}
SECTOR_LOOKUP = {coin: sec for sec, coins in SECTOR_MAP.items() for coin in coins}

BITGET_BASE    = "https://api.bitget.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_cache         = {}

EXCLUDED_KEYWORDS = ["XAU","PAXG","BTC","ETH","USDC","DAI","BUSD","UST","LUNC","LUNA"]


# ══════════════════════════════════════════════════════════════
#  🔒  COOLDOWN & OI SNAPSHOT
# ══════════════════════════════════════════════════════════════
def load_cooldown():
    try:
        p = CONFIG["cooldown_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            return {k: v for k, v in data.items() if now - v < CONFIG["alert_cooldown_sec"]}
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

def get_oi_changes(symbol, current_oi):
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
    if not old1h:
        older = [d for d in hist if d["ts"] < now - 60]
        if older:
            old1h = min(older, key=lambda d: d["ts"])
    chg1h  = (current_oi - old1h["oi"])  / old1h["oi"]  * 100 if old1h  and old1h["oi"]  else 0
    chg24h = (current_oi - old24h["oi"]) / old24h["oi"] * 100 if old24h and old24h["oi"] else 0
    return chg1h, chg24h, (old1h is not None)

_cooldown = load_cooldown()

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

def get_candles(symbol, gran="1h", limit=200):
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
                "ts":       int(c[0]),
                "open":   float(c[1]),
                "high":   float(c[2]),
                "low":    float(c[3]),
                "close":  float(c[4]),
                "volume": float(c[5]),
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
        params={"symbol": symbol, "productType": "usdt-futures", "pageSize": "100"},
    )
    if not data or data.get("code") != "00000":
        return 0, 0
    try:
        orders  = data.get("data", {}).get("liquidationOrderList", [])
        now_ms  = int(time.time() * 1000)
        cutoff  = now_ms - CONFIG["liq_window_min"] * 60 * 1000
        long_liq  = 0.0
        short_liq = 0.0
        for o in orders:
            ts  = int(o.get("cTime", 0))
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


# ══════════════════════════════════════════════════════════════
#  📐  MATH HELPERS
# ══════════════════════════════════════════════════════════════
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

def calc_atr_long(candles, period=200):
    """ATR panjang untuk SR box width (replikasi Pine Script ta.atr(200))"""
    if len(candles) < period + 1:
        return calc_atr(candles, min(14, len(candles) - 1))
    return calc_atr(candles, period)

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

def calc_vwap(candles):
    cum_tv, cum_v = 0, 0
    for c in candles:
        tp      = (c["high"] + c["low"] + c["close"]) / 3
        cum_tv += tp * c["volume"]
        cum_v  += c["volume"]
    return cum_tv / cum_v if cum_v else (candles[-1]["close"] if candles else 0)

def percentile_val(values, pct):
    if not values:
        return 0
    s = sorted(values)
    idx = int(len(s) * pct / 100)
    return s[min(idx, len(s)-1)]


# ══════════════════════════════════════════════════════════════
#  🎯  SR ENGINE — Replikasi Pine Script ChartPrime
#     "Support and Resistance (High Volume Boxes)"
# ══════════════════════════════════════════════════════════════
def calc_delta_volume(candle):
    """
    Replikasi upAndDownVolume() dari Pine Script.
    Buy candle: +volume, Sell candle: -volume
    """
    if candle["close"] > candle["open"]:
        return candle["volume_usd"]   # positif = buy pressure
    else:
        return -candle["volume_usd"]  # negatif = sell pressure

def find_pivot_high(candles, lookback):
    """
    Replikasi ta.pivothigh(src, lookback, lookback)
    Pivot high di index i jika high[i] adalah max dari [i-lookback : i+lookback]
    Return list of (bar_index, price, delta_vol)
    """
    pivots = []
    for i in range(lookback, len(candles) - lookback):
        h = candles[i]["high"]
        window = [candles[j]["high"] for j in range(i - lookback, i + lookback + 1)]
        if h == max(window):
            dv = calc_delta_volume(candles[i])
            pivots.append({
                "idx":   i,
                "price": h,
                "delta_vol": dv,
                "volume_usd": candles[i]["volume_usd"],
                "ts":    candles[i]["ts"],
            })
    return pivots

def find_pivot_low(candles, lookback):
    """
    Replikasi ta.pivotlow(src, lookback, lookback)
    Pivot low di index i jika low[i] adalah min dari [i-lookback : i+lookback]
    """
    pivots = []
    for i in range(lookback, len(candles) - lookback):
        l = candles[i]["low"]
        window = [candles[j]["low"] for j in range(i - lookback, i + lookback + 1)]
        if l == min(window):
            dv = calc_delta_volume(candles[i])
            pivots.append({
                "idx":   i,
                "price": l,
                "delta_vol": dv,
                "volume_usd": candles[i]["volume_usd"],
                "ts":    candles[i]["ts"],
            })
    return pivots

def detect_sr_levels(candles):
    """
    CORE SR ENGINE:
    Replikasi calcSupportResistance() Pine Script.
    
    Support  = pivotLow  dengan Vol > vol_hi (volume positif/buy spike)
    Resistance = pivotHigh dengan Vol < vol_lo (volume negatif/sell spike)
    
    Returns:
        support_levels  : list of {price, box_top, box_bottom, vol, idx, strength}
        resist_levels   : list of {price, box_top, box_bottom, vol, idx, strength}
        sr_analysis     : dict dengan sinyal-sinyal SR
    """
    if len(candles) < CONFIG["sr_lookback"] * 2 + 5:
        return [], [], {"valid": False}

    lookback = CONFIG["sr_lookback"]
    vol_len  = CONFIG["sr_vol_len"]

    # Hitung delta volume per candle
    delta_vols = [calc_delta_volume(c) for c in candles]

    # vol_hi dan vol_lo (window vol_len) — replikasi Pine Script
    vol_hi_arr = []
    vol_lo_arr = []
    for i in range(len(candles)):
        window_start = max(0, i - vol_len + 1)
        window_dv    = [dv / 2.5 for dv in delta_vols[window_start:i+1]]
        vol_hi_arr.append(max(window_dv) if window_dv else 0)
        vol_lo_arr.append(min(window_dv) if window_dv else 0)

    # ATR untuk box width
    atr_long = calc_atr_long(candles, CONFIG["sr_atr_period"]) or (candles[-1]["close"] * 0.02)
    withd    = atr_long * CONFIG["sr_box_width"]

    # Temukan pivot high dan low
    pivot_highs = find_pivot_high(candles, lookback)
    pivot_lows  = find_pivot_low(candles, lookback)

    # All delta_vols untuk percentile
    all_dv = [abs(dv) for dv in delta_vols if dv != 0]

    # ── BUILD SUPPORT LEVELS ────────────────────────────────────
    support_levels = []
    for pv in pivot_lows:
        i  = pv["idx"]
        dv = delta_vols[i]
        # Support: pivotLow + Vol > vol_hi (buy pressure dominan)
        if dv > vol_hi_arr[i]:
            box_top    = pv["price"]
            box_bottom = pv["price"] - withd

            # Hitung kekuatan: berapa banyak touch pada zone ini
            touches = 0
            tol = CONFIG["sr_zone_tolerance"]
            for c in candles[i:]:
                in_zone = (box_bottom <= c["low"] <= box_top * (1 + tol) or
                           box_bottom * (1 - tol) <= c["high"] <= box_top)
                if in_zone:
                    touches += 1

            # Berapa besar volume relatif
            vol_rank = sum(1 for v in all_dv if v < abs(dv)) / max(len(all_dv), 1) * 100

            support_levels.append({
                "price":      pv["price"],
                "box_top":    box_top,
                "box_bottom": box_bottom,
                "delta_vol":  dv,
                "vol_usd":    pv["volume_usd"],
                "idx":        i,
                "touches":    touches,
                "vol_rank":   vol_rank,  # percentile kekuatan vol
                "age_bars":   len(candles) - 1 - i,
            })

    # ── BUILD RESISTANCE LEVELS ─────────────────────────────────
    resist_levels = []
    for pv in pivot_highs:
        i  = pv["idx"]
        dv = delta_vols[i]
        # Resistance: pivotHigh + Vol < vol_lo (sell pressure dominan)
        if dv < vol_lo_arr[i]:
            box_top    = pv["price"] + withd
            box_bottom = pv["price"]

            touches = 0
            tol = CONFIG["sr_zone_tolerance"]
            for c in candles[i:]:
                in_zone = (box_bottom * (1 - tol) <= c["high"] <= box_top or
                           box_bottom <= c["low"] <= box_top * (1 + tol))
                if in_zone:
                    touches += 1

            vol_rank = sum(1 for v in all_dv if v < abs(dv)) / max(len(all_dv), 1) * 100

            resist_levels.append({
                "price":      pv["price"],
                "box_top":    box_top,
                "box_bottom": box_bottom,
                "delta_vol":  dv,
                "vol_usd":    pv["volume_usd"],
                "idx":        i,
                "touches":    touches,
                "vol_rank":   vol_rank,
                "age_bars":   len(candles) - 1 - i,
            })

    # Sort: support dari bawah ke atas, resist dari bawah ke atas
    support_levels.sort(key=lambda x: x["price"])
    resist_levels.sort(key=lambda x: x["price"])

    # ── SR ANALYSIS ─────────────────────────────────────────────
    cur_price = candles[-1]["close"]
    cur_high  = candles[-1]["high"]
    cur_low   = candles[-1]["low"]
    n         = len(candles)

    sr_analysis = {
        "valid":         True,
        "cur_price":     cur_price,

        # Support terdekat di bawah harga
        "nearest_sup":   None,
        "is_at_support": False,
        "sup_vol_rank":  0,

        # Resistance terdekat di atas harga
        "nearest_res":   None,
        "is_at_resist":  False,

        # Break events
        "break_res_recent": False,   # Break Res baru terjadi (bull trigger!)
        "break_sup_recent": False,   # Break Sup baru terjadi (BAHAYA!)
        "res_became_sup":   False,   # Resistance flip jadi support
        "sup_became_res":   False,   # Support flip jadi resistance

        # Tren SR
        "sup_count":     len(support_levels),
        "res_count":     len(resist_levels),

        # Anti-break support gate
        "sup_broken":    False,
        "sup_broken_detail": "",
    }

    # Nearest support (di bawah atau sedikit di atas harga)
    below_sup = [s for s in support_levels if s["box_top"] <= cur_price * 1.03]
    if below_sup:
        sr_analysis["nearest_sup"] = below_sup[-1]  # yang paling dekat
        sup = below_sup[-1]
        dist_pct = (cur_price - sup["box_top"]) / cur_price * 100
        sr_analysis["is_at_support"] = dist_pct < CONFIG["entry_retest_max_pct"]
        sr_analysis["sup_vol_rank"]  = sup["vol_rank"]

    # Nearest resistance (di atas harga)
    above_res = [r for r in resist_levels if r["box_bottom"] >= cur_price * 0.97]
    if above_res:
        sr_analysis["nearest_res"] = above_res[0]  # yang paling dekat

    # ── DETEKSI BREAK RES (sinyal pump utama) ──────────────────
    # Break Res: harga baru saja melewati resistance dari bawah ke atas
    # Cek apakah ada resistance yang harganya sekarang di bawah close
    window_bars = CONFIG["sr_break_confirm_bars"]
    recent_candles = candles[-window_bars-5:]  # candle belakangan

    for res in resist_levels:
        res_level = res["box_bottom"]
        # Harga sekarang di atas resistance bottom (sudah break)
        if cur_price > res_level * 1.001:
            # Cek apakah sebelumnya (lookback bar lalu) harga di bawah resistance
            lookback_idx = max(0, res["idx"] - CONFIG["sr_break_confirm_bars"])
            # Resistance harus masih "fresh" (terbentuk dalam 50 bar terakhir)
            if res["age_bars"] <= 50:
                # Cek low candle recent masih di dekat resistance (retest potensial)
                if cur_price <= res["box_top"] * 1.05:
                    sr_analysis["break_res_recent"] = True
                    sr_analysis["res_became_sup"]   = True
                    # Resistance yang baru dibreak menjadi support baru
                    if sr_analysis["nearest_sup"] is None:
                        sr_analysis["nearest_sup"] = res
                    break

    # ── DETEKSI BREAK SUP (bahaya/anti-break) ──────────────────
    # Break Sup: harga turun menembus support ke bawah
    for sup in reversed(support_levels):  # dari atas ke bawah
        sup_level = sup["box_bottom"]
        # Harga sekarang di bawah support bottom (sudah break)
        if cur_price < sup_level * 0.999:
            if sup["age_bars"] <= CONFIG["abs_break_lookback"]:
                # Support baru saja jebol!
                sr_analysis["break_sup_recent"] = True
                sr_analysis["sup_broken"]        = True
                sr_analysis["sup_broken_detail"] = (
                    f"Support ${sup['price']:.6g} jebol! "
                    f"(vol_rank:{sup['vol_rank']:.0f}%ile, "
                    f"{sup['age_bars']} bar lalu)"
                )
                break

    return support_levels, resist_levels, sr_analysis


# ══════════════════════════════════════════════════════════════
#  🛡️  ANTI-BREAK SUPPORT GATE
# ══════════════════════════════════════════════════════════════
def anti_break_support_gate(sr_analysis):
    """
    Gate: blok sinyal jika support baru saja jebol.
    Berdasarkan forensik: setelah Break Sup, harga biasanya
    turun lebih jauh — bukan pump.
    Returns: (should_block: bool, reason: str)
    """
    if not sr_analysis.get("valid"):
        return False, ""

    if sr_analysis.get("sup_broken") and CONFIG["abs_block_if_broken"]:
        detail = sr_analysis.get("sup_broken_detail", "")
        return True, f"🛡️ ANTI-BREAK SUP: {detail}"

    return False, ""


# ══════════════════════════════════════════════════════════════
#  📍  ENTRY/SL/TP CANGGIH — Berbasis SR Level
# ══════════════════════════════════════════════════════════════
def calc_smart_entry(candles, sr_analysis, support_levels, resist_levels):
    """
    Entry/SL/TP berbasis SR level aktual (bukan % fixed).

    LOGIKA:
    1. Entry = di atas support box top (0.3% buffer)
       Jika Break Res baru terjadi: entry = di atas resistance lama (yang jadi support)
    2. SL = di bawah support box bottom + ATR multiplier (ambil yang lebih rendah)
    3. TP1 = resistance terdekat berikutnya - 0.3% buffer
    4. TP2 = resistance ke-2 atau proyeksi
    """
    cur      = candles[-1]["close"]
    atr_14   = calc_atr(candles[-50:] if len(candles) >= 50 else candles, 14) or cur * 0.02
    atr_long = calc_atr_long(candles, CONFIG["sr_atr_period"]) or cur * 0.02

    entry = None
    sl    = None
    entry_type = "market"

    nearest_sup = sr_analysis.get("nearest_sup")
    nearest_res = sr_analysis.get("nearest_res")

    # ── ENTRY ────────────────────────────────────────────────
    if sr_analysis.get("break_res_recent") and nearest_sup:
        # Pattern: Break Res → tunggu retest ke resistance lama (jadi support baru)
        # Entry = di atas box_top resistance yang sudah dibreak
        entry      = nearest_sup["box_top"] * (1 + CONFIG["entry_above_support_pct"] / 100)
        entry_type = "retest_break_res"

    elif sr_analysis.get("is_at_support") and nearest_sup:
        # Pattern: Harga retest support box → buy di zona support
        entry      = nearest_sup["box_top"] * (1 + CONFIG["entry_above_support_pct"] / 100)
        entry_type = "support_retest"

    elif nearest_sup:
        # Harga di atas support, entry saat ini (momentum entry)
        entry      = cur
        entry_type = "momentum"
    else:
        # Tidak ada support valid → skip
        return None

    # ── SL ───────────────────────────────────────────────────
    if nearest_sup:
        # SL di bawah box_bottom support
        sl_support = nearest_sup["box_bottom"] * (1 - CONFIG["sl_below_support_pct"] / 100)
        # SL berbasis ATR
        sl_atr     = entry - (atr_14 * CONFIG["sl_atr_multiplier"])
        # Ambil yang lebih rendah (lebih protektif)
        sl = min(sl_support, sl_atr)
    else:
        sl = entry * (1 - CONFIG["max_sl_pct"] / 100)

    # Pastikan SL tidak terlalu jauh (max 6% dari entry)
    sl_pct = (entry - sl) / entry * 100
    if sl_pct > CONFIG["max_sl_pct"]:
        sl     = entry * (1 - CONFIG["max_sl_pct"] / 100)
        sl_pct = CONFIG["max_sl_pct"]

    if sl >= entry:
        return None  # Invalid setup

    # ── TP1 — Resistance terdekat ────────────────────────────
    above_res_for_tp = [r for r in resist_levels
                        if r["box_bottom"] > entry * 1.03]  # min 3% di atas entry

    if above_res_for_tp:
        tp1_res = above_res_for_tp[0]  # resistance terdekat
        tp1     = tp1_res["box_bottom"] * (1 - CONFIG["tp1_resistance_buffer"] / 100)
    else:
        # Tidak ada resistance — proyeksi ATR
        tp1 = entry * (1 + max(CONFIG["min_tp1_pct"] / 100, atr_14 * 3 / entry))

    # Pastikan TP1 minimal CONFIG["min_tp1_pct"]% dari entry
    tp1_pct = (tp1 - entry) / entry * 100
    if tp1_pct < CONFIG["min_tp1_pct"]:
        tp1     = entry * (1 + CONFIG["min_tp1_pct"] / 100)
        tp1_pct = CONFIG["min_tp1_pct"]

    # ── TP2 — Resistance ke-2 atau proyeksi ─────────────────
    above_res_tp2 = [r for r in resist_levels
                     if r["box_bottom"] > tp1 * 1.03]

    if above_res_tp2:
        tp2 = above_res_tp2[0]["box_bottom"] * (1 - CONFIG["tp1_resistance_buffer"] / 100)
    else:
        # Proyeksi: TP2 = TP1 + (TP1-entry) * multiplier
        tp2 = tp1 + (tp1 - entry) * (CONFIG["tp2_multiplier"] - 1)

    # ── R/R RATIO ─────────────────────────────────────────────
    risk   = entry - sl
    reward = tp1 - entry
    rr     = round(reward / risk, 2) if risk > 0 else 0

    if rr < CONFIG["min_rr_ratio"]:
        # R/R terlalu rendah — coba adjust tp1
        tp1 = entry + risk * CONFIG["min_rr_ratio"]
        tp1_pct = (tp1 - entry) / entry * 100
        rr  = CONFIG["min_rr_ratio"]

    # ── LABEL SR KONTEKS ──────────────────────────────────────
    sup_label = f"${nearest_sup['price']:.6g}" if nearest_sup else "N/A"
    res_label = f"${nearest_res['box_bottom']:.6g}" if nearest_res else "N/A"

    return {
        "cur":         round(cur, 8),
        "entry":       round(entry, 8),
        "entry_type":  entry_type,
        "sl":          round(sl, 8),
        "sl_pct":      round(sl_pct, 2),
        "tp1":         round(tp1, 8),
        "tp1_pct":     round(tp1_pct, 2),
        "tp2":         round(tp2, 8),
        "tp2_pct":     round((tp2 - entry) / entry * 100, 1),
        "rr":          rr,
        "atr_14":      round(atr_14, 8),
        "sup_level":   sup_label,
        "res_level":   res_label,
        "sup_vol_rank": nearest_sup["vol_rank"] if nearest_sup else 0,
    }


# ══════════════════════════════════════════════════════════════
#  📊  SR LAYER SCORING
# ══════════════════════════════════════════════════════════════
def layer_sr(candles, sr_analysis, support_levels, resist_levels):
    """
    Scoring berbasis SR pattern — replika sinyal ChartPrime indicator.
    Max score: CONFIG['max_sr_score'] = 40
    """
    score, sigs = 0, []

    if not sr_analysis.get("valid"):
        return 0, sigs

    cur_price = sr_analysis["cur_price"]

    # 1. Support kuat di bawah harga (dasar dari semua pump yang dianalisis)
    nearest_sup = sr_analysis.get("nearest_sup")
    if nearest_sup:
        vol_rank = nearest_sup["vol_rank"]
        touches  = nearest_sup["touches"]
        age      = nearest_sup["age_bars"]

        if vol_rank >= 90:
            score += 12
            sigs.append(f"🟢 Support MASIF vol {vol_rank:.0f}%ile @ ${nearest_sup['price']:.6g} ({touches} touch)")
        elif vol_rank >= 75:
            score += 8
            sigs.append(f"🟢 Support kuat vol {vol_rank:.0f}%ile @ ${nearest_sup['price']:.6g}")
        elif vol_rank >= 60:
            score += 5
            sigs.append(f"🟢 Support vol {vol_rank:.0f}%ile @ ${nearest_sup['price']:.6g}")

        # Multi-touch = lebih kuat
        if touches >= 4:
            score += 5
            sigs.append(f"  ↳ {touches}x tested — support sangat kuat!")
        elif touches >= 3:
            score += 3

        # Support fresh (baru terbentuk) lebih valuable
        if age <= 10:
            score += 3
            sigs.append(f"  ↳ Support baru ({age} bar lalu) — akumulasi aktif")
        elif age <= 25:
            score += 1

    # 2. Break Resistance — SINYAL PUMP PALING KUAT
    if sr_analysis.get("break_res_recent"):
        score += 20
        sigs.append("🚨 BREAK RESISTANCE! Harga breakout resistance box — pump trigger!")

        if sr_analysis.get("res_became_sup"):
            score += 8
            sigs.append("  ↳ ✅ Resistance flip → Support (konfirmasi break kuat)")

    # 3. Harga di zone support (retest area — entry ideal)
    if sr_analysis.get("is_at_support") and not sr_analysis.get("break_res_recent"):
        score += 10
        sigs.append(f"📌 Harga retest support zone — zona entry ideal!")

    # 4. Jarak ke resistance (semakin dekat = potensial breakout lebih besar)
    nearest_res = sr_analysis.get("nearest_res")
    if nearest_res and nearest_sup:
        dist_to_res = (nearest_res["box_bottom"] - cur_price) / cur_price * 100
        dist_to_sup = (cur_price - nearest_sup["box_top"]) / cur_price * 100

        if 0 < dist_to_res <= 5:
            score += 6
            sigs.append(f"⚡ Resistance hanya {dist_to_res:.1f}% lagi — breakout imminent")
        elif dist_to_res <= 10:
            score += 3

        # Harga lebih dekat ke support daripada resistance = posisi ideal
        if dist_to_sup < dist_to_res:
            score += 3
            sigs.append(f"✅ Posisi ideal: dekat support ({dist_to_sup:.1f}%), jauh resistance ({dist_to_res:.1f}%)")

    # 5. Multiple support levels = struktur kuat
    strong_sups = [s for s in support_levels if s["vol_rank"] >= 70]
    if len(strong_sups) >= 3:
        score += 5
        sigs.append(f"🏗️ {len(strong_sups)} support kuat — struktur sangat kokoh")
    elif len(strong_sups) >= 2:
        score += 3

    # 6. Penalti jika resistance sangat banyak di atas (jalan berat)
    strong_res_above = [r for r in resist_levels
                        if r["box_bottom"] > cur_price and r["vol_rank"] >= 70]
    if len(strong_res_above) >= 4:
        score -= 5
        sigs.append(f"⚠️ {len(strong_res_above)} resistance kuat di atas — jalan berat")

    return min(score, CONFIG["max_sr_score"]), sigs


# ══════════════════════════════════════════════════════════════
#  📊  EXISTING LAYERS (dipertahankan dari v9.10)
# ══════════════════════════════════════════════════════════════
def calc_rvol(candles_1h):
    if len(candles_1h) < 25:
        return 1.0
    last_complete = candles_1h[-2]
    last_vol      = last_complete["volume_usd"]
    target_hour   = (last_complete["ts"] // 3_600_000) % 24
    same_hour_vols = [
        c["volume_usd"] for c in candles_1h[:-2]
        if (c["ts"] // 3_600_000) % 24 == target_hour
    ]
    if not same_hour_vols:
        return 1.0
    avg = sum(same_hour_vols) / len(same_hour_vols)
    return min(last_vol / avg, 30.0) if avg > 0 else 1.0

def calc_volume_spike_ratio(candles_1h):
    if len(candles_1h) < 24:
        return 1.0, 1.0
    vols     = [c["volume_usd"] for c in candles_1h]
    baseline = sorted(vols[:-6])[:int(len(vols) * 0.6)]
    base_avg = sum(baseline) / len(baseline) if baseline else 1
    if base_avg <= 0:
        return 1.0, 1.0
    recent_vols = vols[-6:]
    spikes      = [v / base_avg for v in recent_vols]
    return max(spikes) if spikes else 1.0, sum(spikes) / len(spikes) if spikes else 1.0

def calc_volume_irregularity(candles_1h):
    window = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    vols   = [c["volume_usd"] for c in window]
    if not vols:
        return 0.0
    mean = sum(vols) / len(vols)
    if mean <= 0:
        return 0.0
    std = math.sqrt(sum((v - mean) ** 2 for v in vols) / len(vols))
    return std / mean

def calc_cvd_signal(candles_1h):
    if len(candles_1h) < 12:
        return 0, ""
    window    = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    cvd, cvd_vals = 0, []
    for c in window:
        rng = c["high"] - c["low"]
        buy_ratio = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
        cvd += (buy_ratio * 2 - 1) * c["volume_usd"]
        cvd_vals.append(cvd)
    if len(cvd_vals) < 8:
        return 0, ""
    mid      = len(cvd_vals) // 2
    cvd_early = sum(cvd_vals[:mid]) / mid
    cvd_late  = sum(cvd_vals[mid:]) / (len(cvd_vals) - mid)
    cvd_rising = cvd_late > cvd_early
    p_start   = window[0]["close"]
    p_end     = window[-1]["close"]
    price_chg = (p_end - p_start) / p_start * 100 if p_start > 0 else 0
    if cvd_rising and price_chg < 1.5:
        if price_chg < -1.5:
            return 15, f"🔍 CVD Divergence KUAT: harga {price_chg:+.1f}% tapi buy pressure dominan"
        elif price_chg < 0:
            return 12, f"🔍 CVD naik saat harga turun — akumulasi tersembunyi"
        else:
            return 8,  f"🔍 CVD naik, harga flat — hidden accumulation"
    elif cvd_rising and 1.5 <= price_chg <= 5.0:
        return 5, f"CVD bullish, harga naik sehat ({price_chg:+.1f}%)"
    if not cvd_rising and price_chg > 1.5:
        return -12, f"⚠️ CVD turun saat harga naik {price_chg:+.1f}% — distribusi tersembunyi"
    elif not cvd_rising and -1.5 <= price_chg <= 1.5:
        return -8, f"⚠️ CVD turun, harga flat — tekanan jual tersembunyi"
    elif not cvd_rising and price_chg < -1.5:
        return -5, f"⚠️ CVD turun saat harga {price_chg:+.1f}% — tren jual berlanjut"
    return 0, ""

def calc_short_term_cvd(candles_1h):
    if len(candles_1h) < 12:
        return 0, ""
    recent = candles_1h[-6:]
    prev   = candles_1h[-12:-6]
    def cvd_delta(candles):
        delta = 0.0
        for c in candles:
            rng = c["high"] - c["low"]
            buy_ratio = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
            delta += (buy_ratio * 2 - 1) * c["volume_usd"]
        return delta
    recent_d = cvd_delta(recent)
    prev_d   = cvd_delta(prev)
    cur      = candles_1h[-1]["close"]
    p6h      = candles_1h[-6]["close"] if len(candles_1h) >= 6 else cur
    price_chg_6h = (cur - p6h) / p6h * 100 if p6h > 0 else 0
    if recent_d < 0 and recent_d < prev_d * 0.8:
        if price_chg_6h > -2:
            return -10, f"⚠️ CVD 6h memburuk — tekanan jual meningkat ({price_chg_6h:+.1f}% 6h)"
        return -5, f"⚠️ CVD 6h negatif — distribusi aktif"
    if recent_d > 0 and recent_d > prev_d * 1.2:
        if price_chg_6h < 2:
            return 8, f"✅ CVD 6h membaik — akumulasi baru ({price_chg_6h:+.1f}% 6h)"
        return 4, f"CVD 6h positif — buying momentum"
    return 0, ""

def detect_higher_lows(candles):
    if len(candles) < 6:
        return 0, ""
    lows       = [c["low"] for c in candles]
    local_lows = []
    for i in range(1, len(lows) - 1):
        if lows[i] <= lows[i-1] and lows[i] <= lows[i+1]:
            local_lows.append(lows[i])
    if len(local_lows) < 2:
        return 0, ""
    ascending = sum(
        1 for i in range(1, len(local_lows))
        if local_lows[i] > local_lows[i-1] * 1.001
    )
    if ascending >= 2:
        return 8, f"📐 {ascending + 1}x Higher Lows — ascending triangle"
    elif ascending >= 1:
        return 4, f"📐 Higher Low — struktur bullish mulai"
    return 0, ""

def layer_volume_intelligence(candles_1h):
    score, sigs = 0, []
    rvol = calc_rvol(candles_1h)
    if rvol >= 4.0:
        score += 16; sigs.append(f"🔥🔥 RVOL {rvol:.1f}x — volume MASIF vs historis!")
    elif rvol >= 2.8:
        score += 13; sigs.append(f"🔥 RVOL {rvol:.1f}x — volume spike signifikan")
    elif rvol >= 2.0:
        score += 10; sigs.append(f"RVOL {rvol:.1f}x — volume mulai bangun")
    elif rvol >= 1.4:
        score += 6;  sigs.append(f"RVOL {rvol:.1f}x — di atas normal")
    elif rvol >= 1.1:
        score += 3
    elif rvol < 0.4:
        score -= 4
    irr = calc_volume_irregularity(candles_1h)
    if irr >= 2.5:
        score += 10; sigs.append(f"📈 Vol Irregularity {irr:.2f} — whale masuk tidak merata")
    elif irr >= 1.8:
        score += 6;  sigs.append(f"Vol Irregularity {irr:.2f} — aktivitas whale")
    elif irr >= 1.3:
        score += 3
    cvd_s, cvd_sig = calc_cvd_signal(candles_1h)
    score += cvd_s
    if cvd_sig:
        sigs.append(cvd_sig)
    return min(score, CONFIG["max_vol_score"]), sigs, rvol

def layer_flat_accumulation(candles_1h):
    score, sigs = 0, []
    if len(candles_1h) < 4:
        return 0, sigs
    if len(candles_1h) >= 24:
        high24 = max(c["high"] for c in candles_1h[-24:])
        low24  = min(c["low"]  for c in candles_1h[-24:])
        range24_pct = (high24 - low24) / low24 * 100 if low24 > 0 else 99
    else:
        range24_pct = 99
    if range24_pct < 5:
        score += 15; sigs.append(f"🎯 Range 24h sangat sempit ({range24_pct:.1f}%)")
    elif range24_pct < 10:
        score += 10; sigs.append(f"🎯 Range 24h sempit ({range24_pct:.1f}%)")
    elif range24_pct < 15:
        score += 5
    elif range24_pct > 40:
        score -= 5
    # Higher lows
    hl_sc, hl_sig = detect_higher_lows(candles_1h[-16:] if len(candles_1h) >= 16 else candles_1h)
    score += hl_sc
    if hl_sig:
        sigs.append(hl_sig)
    return min(max(score, 0), CONFIG["max_flat_score"]), sigs

def layer_structure(candles_1h):
    score, sigs = 0, []
    bbw_val, bbw_pct = bbw_percentile(candles_1h)
    if bbw_pct < 10:
        score += 10; sigs.append(f"BBW Squeeze Ekstrem ({bbw_pct:.0f}%ile) — siap meledak")
    elif bbw_pct < 25:
        score += 7;  sigs.append(f"BBW Squeeze Kuat ({bbw_pct:.0f}%ile)")
    elif bbw_pct < 45:
        score += 4;  sigs.append(f"BBW Menyempit ({bbw_pct:.0f}%ile)")
    elif bbw_pct > 85:
        score -= 5;  sigs.append(f"⚠️ BBW Melebar ({bbw_pct:.0f}%ile) — volatilitas sudah terjadi")
    coiling = 0
    for c in reversed(candles_1h[-72:]):
        body = abs(c["close"] - c["open"]) / c["open"] * 100 if c["open"] else 99
        if body < 1.0:
            coiling += 1
        else:
            break
    if coiling >= 18:
        score += 5; sigs.append(f"Coiling {coiling}h — energi terkumpul lama")
    elif coiling >= 10:
        score += 3; sigs.append(f"Coiling {coiling}h")
    elif coiling >= 5:
        score += 1
    return min(score, CONFIG["max_struct_score"]), sigs, bbw_val, bbw_pct, coiling

def layer_positioning(symbol, funding, oi_chg1h):
    score, sigs = 0, []
    ls_block = False
    if funding <= -0.0004:
        score += 8;  sigs.append(f"💰 Funding {funding:.5f} — short squeeze setup KUAT!")
    elif -0.0004 < funding <= -0.00001:
        score += 6;  sigs.append(f"💰 Funding {funding:.5f} — short squeeze setup")
    elif abs(funding) < 0.00001:
        score += 4;  sigs.append(f"Funding {funding:.5f} — netral")
    elif funding > 0.0003:
        score -= 5;  sigs.append(f"⚠️ Funding {funding:.5f} — long overcrowded, risiko dump")
    if funding <= CONFIG["squeeze_funding_max"] and oi_chg1h > CONFIG["squeeze_oi_change_min"]:
        score += 10
        sigs.append(f"🔥 SHORT SQUEEZE: funding negatif, OI 1h +{oi_chg1h:.1f}%")
    ls       = get_long_short_ratio(symbol)
    ls_score = 0
    if ls is not None:
        if ls < 0.6:
            ls_score = 10; sigs.append(f"🎯 L/S {ls:.2f} — short dominan, squeeze fuel besar!")
        elif ls < 0.75:
            ls_score = 8;  sigs.append(f"🎯 L/S {ls:.2f} — short dominan")
        elif ls < 0.9:
            ls_score = 5;  sigs.append(f"L/S {ls:.2f} — lebih banyak short")
        elif ls <= 1.15:
            ls_score = 2
        elif 1.15 < ls <= 1.3:
            ls_score = -5
        elif 1.3 < ls <= 1.6:
            ls_score = -10; sigs.append(f"⚠️ L/S {ls:.2f} — longs dominan")
        elif 1.6 < ls <= 2.0:
            ls_score = -16; sigs.append(f"⚠️⚠️ L/S {ls:.2f} — longs sangat dominan")
        elif ls > 2.0:
            ls_score  = -25
            ls_block  = True
            sigs.append(f"🚨 L/S {ls:.2f} — long overcrowded KRITIS, hard block")
    return min(score + ls_score, CONFIG["max_pos_score"]), sigs, ls, ls_block

def calc_4h_confluence(candles_4h):
    if len(candles_4h) < 6:
        return 0, ""
    closes    = [c["close"] for c in candles_4h]
    p_now     = closes[-1]
    p_7d      = closes[0]
    p_48h     = closes[-12] if len(closes) >= 12 else closes[0]
    trend_7d  = (p_now - p_7d)  / p_7d  * 100 if p_7d  > 0 else 0
    trend_48h = (p_now - p_48h) / p_48h * 100 if p_48h > 0 else 0
    if trend_48h > 2 and -10 <= trend_7d <= 15:
        return 6, f"📊 4H: reversal bullish 48h +{trend_48h:.1f}%"
    elif trend_48h > 0 and trend_7d > -15:
        return 3, f"📊 4H upward bias ({trend_48h:+.1f}% 48h)"
    elif trend_48h < -8:
        return -5, f"⚠️ 4H masih downtrend ({trend_48h:+.1f}% 48h)"
    return 0, ""

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

def layer_context(symbol, tickers_dict):
    sector = SECTOR_LOOKUP.get(symbol, "MISC")
    peers  = SECTOR_MAP.get(sector, [])
    pumped = []
    for p in peers:
        if p == symbol or p not in tickers_dict:
            continue
        try:
            chg = float(tickers_dict[p].get("change24h", 0)) * 100
            if chg > 8:
                pumped.append((p.replace("USDT", ""), chg))
        except:
            continue
    pumped.sort(key=lambda x: x[1], reverse=True)
    sec_score, sec_sig = 0, ""
    if pumped:
        top       = pumped[0]
        sec_score = 5 if top[1] > 20 else 3 if top[1] > 12 else 1
        sec_sig   = f"🔄 {sector}: {top[0]} +{top[1]:.0f}% — rotasi potensial"
    name       = symbol.replace("USDT", "").replace("1000", "").upper()
    soc_score, soc_sig = 0, ""
    if name in get_cg_trending():
        soc_score = 3
        soc_sig   = f"🔥 {name} trending CoinGecko"
    sigs = [s for s in [sec_sig, soc_sig] if s]
    return min(sec_score + soc_score, CONFIG["max_ctx_score"]), sigs, sector

def calc_whale(symbol, candles_15m, funding):
    ws, ev = 0, []
    cur = candles_15m[-1]["close"] if candles_15m else 0
    trades = get_trades(symbol, 500)
    if trades:
        buy_v  = sum(t["size"] for t in trades if t["side"] == "buy")
        tot_v  = sum(t["size"] for t in trades)
        tr     = buy_v / tot_v if tot_v > 0 else 0.5
        if tr > 0.70:
            ws += 30; ev.append(f"✅ Taker Buy {tr:.0%} — pembeli sangat dominan")
        elif tr > 0.62:
            ws += 15; ev.append(f"🔶 Taker Buy {tr:.0%} — bias beli")
        total_usd = sum(t["size"] * t["price"] for t in trades)
        avg_trade = total_usd / len(trades) if trades else 1
        thr       = max(avg_trade * 5, 3_000)
        lbuy_usd  = sum(
            t["size"] * t["price"] for t in trades
            if t["side"] == "buy" and t["size"] * t["price"] > thr
        )
        if total_usd > 0 and lbuy_usd / total_usd > 0.28:
            ws += 25; ev.append(f"✅ Smart money {lbuy_usd/total_usd:.0%} vol")
    if candles_15m and len(candles_15m) >= 16:
        p4h  = candles_15m[-16]["close"]
        pchg = abs((cur - p4h) / p4h * 100) if p4h else 99
        if pchg < 1.5:
            ws += 15; ev.append("✅ Harga flat 4h — stealth positioning")
        elif pchg < 3.0:
            ws += 7;  ev.append("🔶 Harga relatif flat 4h")
    if -0.0004 <= funding <= -0.00002:
        ws += 10; ev.append(f"✅ Funding {funding:.5f} — short squeeze fuel")
    ob_ratio, bid_vol, ask_vol = get_orderbook(symbol, 50)
    if ob_ratio > 0.65:
        ws += 15; ev.append(f"✅ OB Bid {ob_ratio:.0%} — tekanan beli di book")
    elif ob_ratio > 0.55:
        ws += 7;  ev.append(f"🔶 OB Bid {ob_ratio:.0%}")
    elif ob_ratio < 0.35:
        ws -= 10; ev.append(f"⚠️ OB Ask dominan — tekanan jual lebih besar")
    return min(ws, 100), min(ws, 100) // 5, ev

def layer_liquidation(symbol, candles_1h):
    long_liq, short_liq = get_liquidations(symbol)
    score, sigs = 0, []
    should_block = False
    if long_liq > CONFIG["liq_long_block_usd"] * 3:
        should_block = True
        sigs.append(f"🚨 Long liq masif ${long_liq/1e3:.0f}K — pump aborted!")
    elif long_liq > CONFIG["liq_long_block_usd"]:
        score -= 15
        sigs.append(f"⚠️ Long liq ${long_liq/1e3:.0f}K — longs baru dihancurkan")
    if short_liq > CONFIG["liq_short_bonus_usd"] * 2:
        score += 20; sigs.append(f"🔥🔥 Short liq ${short_liq/1e3:.0f}K — SHORT SQUEEZE AKTIF!")
    elif short_liq > CONFIG["liq_short_bonus_usd"]:
        score += 12; sigs.append(f"🔥 Short liq ${short_liq/1e3:.0f}K — squeeze meningkat")
    elif short_liq > CONFIG["liq_short_bonus_usd"] * 0.5:
        score += 6;  sigs.append(f"Short liq ${short_liq/1e3:.0f}K — short mulai kena")
    return score, sigs, long_liq, short_liq, should_block

def layer_linea_signature(candles_1h, oi_chg1h, oi_chg24h, oi_valid, ls_ratio, funding, chg_24h):
    score, sigs, components = 0, [], 0
    if ls_ratio is not None:
        if ls_ratio < 0.75:
            score += 6; components += 1
            sigs.append(f"✅ [Linea-LS] L/S {ls_ratio:.2f} — short sangat dominan")
        elif ls_ratio <= CONFIG["linea_ls_max"]:
            score += 3; components += 1
    if chg_24h < -3:
        score += 5; components += 1
        sigs.append(f"✅ [Linea-P] Harga {chg_24h:+.1f}% — tertekan, siap reversal")
    elif chg_24h <= CONFIG["linea_price_max_chg"]:
        score += 2; components += 1
    stcvd_sc, stcvd_sig = calc_short_term_cvd(candles_1h)
    if stcvd_sc >= 8:
        score += 5; components += 1
        sigs.append(f"✅ [Linea-CVD] {stcvd_sig}")
    elif stcvd_sc > 0:
        score += 2
    if oi_valid:
        oi_1h_ok  = oi_chg1h  >= CONFIG["linea_oi_1h_min"]
        oi_24h_ok = oi_chg24h >= CONFIG["linea_oi_24h_min"]
        if oi_chg1h >= 8.0:
            score += 8; components += 1
            sigs.append(f"✅ [Linea-OI1h] OI 1h +{oi_chg1h:.1f}% — posisi baru masuk MASIF")
        elif oi_1h_ok:
            score += 3; components += 1
        if oi_24h_ok:
            score += 3; components += 1
        if oi_1h_ok and oi_24h_ok and chg_24h < 0:
            score += 8; components += 1
            sigs.append(f"⭐ [Linea-DIV] OI naik + Harga {chg_24h:+.1f}% — DIVERGENCE BULLISH!")
    if components >= 4:
        score += 5; sigs.append(f"⭐ FULL LINEA SIGNATURE ({components}/5) — pre-pump template!")
    elif components >= 3:
        sigs.append(f"[Linea] {components} komponen aktif")
    return min(score, CONFIG["max_linea_score"]), sigs, components

def layer_oi_acceleration(symbol, oi_value, chg_24h, vol_24h):
    score, sigs, accel = 0, [], {}
    if oi_value <= 0:
        return 0, [], accel
    is_micro  = oi_value < CONFIG["oi_accel_micro_thresh"]
    is_dormant = vol_24h < CONFIG["oi_accel_dormant_vol"]
    snaps = load_oi_snapshots()
    hist  = snaps.get(symbol, [])
    if len(hist) < 2:
        return 0, [], accel
    now = time.time()
    def oi_at(target_ts, tolerance=900):
        cands = [d for d in hist if abs(d["ts"] - target_ts) < tolerance]
        return min(cands, key=lambda d: abs(d["ts"] - target_ts)) if cands else None
    def growth_pct(old_snap):
        if not old_snap or old_snap["oi"] <= 0:
            return None
        return (oi_value - old_snap["oi"]) / old_snap["oi"] * 100
    snap_1h = oi_at(now - 3600)
    snap_3h = oi_at(now - 10800)
    snap_6h = oi_at(now - 21600)
    gr_1h = growth_pct(snap_1h)
    gr_3h = growth_pct(snap_3h)
    gr_6h = growth_pct(snap_6h)
    if gr_1h is not None: accel["growth_rate_1h"] = round(gr_1h, 2)
    if gr_3h is not None: accel["growth_rate_3h"] = round(gr_3h, 2)
    if gr_6h is not None: accel["growth_rate_6h"] = round(gr_6h, 2)
    accel["is_micro_cap"] = is_micro
    multiplier = 1.5 if is_micro else 1.0
    extra_tag  = " [MICRO]" if is_micro else ""
    primary_gr = gr_1h if gr_1h is not None else gr_3h
    if primary_gr is not None:
        if primary_gr >= CONFIG["oi_accel_extreme"] * multiplier:
            score += 20; sigs.append(f"🚀 OI tumbuh +{primary_gr:.0f}%/1h{extra_tag} — EKSTREM!")
        elif primary_gr >= CONFIG["oi_accel_strong"] * multiplier:
            score += 14; sigs.append(f"🔥 OI tumbuh +{primary_gr:.0f}%/1h{extra_tag} — sangat kuat")
        elif primary_gr >= CONFIG["oi_accel_medium"]:
            score += 9;  sigs.append(f"OI tumbuh +{primary_gr:.0f}%/1h{extra_tag}")
        elif primary_gr >= CONFIG["oi_accel_weak"]:
            score += 4;  sigs.append(f"OI tumbuh +{primary_gr:.0f}%/1h — awal akumulasi")
        elif primary_gr < -CONFIG["oi_accel_weak"]:
            score -= 8;  sigs.append(f"⚠️ OI turun {primary_gr:.0f}%/1h — distribusi cepat")
    price_flat = abs(chg_24h) <= CONFIG["oi_accel_div_price_max"]
    oi_growing = (primary_gr or 0) >= CONFIG["oi_accel_weak"]
    if oi_growing and price_flat and chg_24h < 0 and (primary_gr or 0) >= CONFIG["oi_accel_medium"]:
        accel["divergence"] = True
        score += 10
        sigs.append(f"⭐ OI DIVERGENCE: OI +{primary_gr:.0f}% saat harga {chg_24h:+.1f}%")
    return min(score, CONFIG["max_oi_accel_score"]), sigs, accel

def _candle_net_flow(candles):
    buy_usd = sell_usd = 0.0
    for c in candles:
        rng = c["high"] - c["low"]
        buy_ratio = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
        b = buy_ratio * c["volume_usd"]
        s = (1.0 - buy_ratio) * c["volume_usd"]
        buy_usd  += b
        sell_usd += s
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
    net      = buy_usd - sell_usd
    net_pct  = net / total * 100 if total > 0 else 0.0
    return net, net_pct, buy_usd, sell_usd, len(recent)

def _classify_flow(net_pct):
    if net_pct > CONFIG["nf_strong_buy"]:   return "STRONG_BUY"
    if net_pct > CONFIG["nf_buy"]:          return "BUY"
    if net_pct > -CONFIG["nf_neutral_max"]: return "NEUTRAL"
    if net_pct > CONFIG["nf_strong_sell"]:  return "SELL"
    return "STRONG_SELL"

def layer_net_flow(candles_1h, candles_15m, trades):
    score, sigs = 0, []
    flow_data   = {
        "72h": {"net_pct": 0, "label": "NO_DATA"},
        "24h": {"net_pct": 0, "label": "NO_DATA"},
        "6h":  {"net_pct": 0, "label": "NO_DATA"},
        "15m": {"net_pct": 0, "label": "NO_DATA", "count": 0},
        "has_data": False,
    }
    should_block = False
    pct_72h = pct_24h = pct_6h = pct_15m = None

    if len(candles_1h) >= 72:
        _, pct, buy, sell = _candle_net_flow(candles_1h[-72:])
        pct_72h = pct
        flow_data["72h"] = {"net_pct": round(pct,1), "label": _classify_flow(pct)}
    if len(candles_1h) >= 24:
        _, pct, buy, sell = _candle_net_flow(candles_1h[-24:])
        pct_24h = pct
        flow_data["24h"] = {"net_pct": round(pct,1), "label": _classify_flow(pct)}
    if len(candles_1h) >= 6:
        _, pct, buy, sell = _candle_net_flow(candles_1h[-6:])
        pct_6h = pct
        flow_data["6h"] = {"net_pct": round(pct,1), "label": _classify_flow(pct)}
    if trades:
        _, pct, _, _, cnt = _tick_net_flow(trades, 15)
        pct_15m = pct
        flow_data["15m"] = {"net_pct": round(pct,1), "label": _classify_flow(pct), "count": cnt}

    flow_data["has_data"] = (pct_24h is not None)
    if not flow_data["has_data"]:
        return 0, [], flow_data, False

    # Gate distribusi sistematis
    if (pct_72h is not None
            and pct_72h < CONFIG["nf_gate_72h"]
            and pct_24h < CONFIG["nf_gate_24h"]
            and pct_6h  < CONFIG["nf_gate_6h"]):
        should_block = True
        sigs.append(
            f"🚨 NET FLOW DISTRIBUSI: 72h={pct_72h:+.1f}% "
            f"24h={pct_24h:+.1f}% 6h={pct_6h:+.1f}% — whale keluar semua TF"
        )
        return score, sigs, flow_data, should_block

    # Whale accumulation funnel pattern
    if (pct_72h is not None
            and CONFIG["nf_whale_72h_min"] <= pct_72h <= CONFIG["nf_whale_72h_max"]
            and pct_24h is not None and pct_24h >= CONFIG["nf_whale_24h_min"]
            and pct_6h  is not None and pct_6h  >= CONFIG["nf_whale_6h_min"]):
        score += 20
        sigs.append(
            f"🐋 WHALE FUNNEL: 72h={pct_72h:+.1f}% → 24h={pct_24h:+.1f}% → "
            f"6h={pct_6h:+.1f}% — akumulasi 3 hari!"
        )
    elif (pct_72h is not None and pct_72h > CONFIG["nf_buy"]
            and pct_24h is not None and pct_24h > CONFIG["nf_buy"]
            and pct_6h  is not None and pct_6h  > CONFIG["nf_buy"]):
        score += 15
        sigs.append(f"✅ NET FLOW BULLISH semua TF: 72h={pct_72h:+.1f}% 24h={pct_24h:+.1f}% 6h={pct_6h:+.1f}%")
    else:
        if pct_24h is not None and pct_24h > CONFIG["nf_buy"] and pct_6h is not None and pct_6h > CONFIG["nf_buy"]:
            score += 10
        elif pct_6h is not None and pct_6h > CONFIG["nf_strong_buy"]:
            score += 7
        if pct_72h is not None and CONFIG["nf_strong_sell"] < pct_72h < 0 and pct_24h is not None and pct_24h > CONFIG["nf_buy"]:
            score += 5; sigs.append(f"📈 Flow shifting: 72h={pct_72h:+.1f}% → 24h={pct_24h:+.1f}%")

    if pct_72h is not None:
        if pct_72h < CONFIG["nf_strong_sell"]:
            score -= 12; sigs.append(f"⚠️ Net Flow 72h={pct_72h:+.1f}% — distribusi besar 3 hari")
        elif pct_72h < CONFIG["nf_sell"]:
            score -= 6
    if pct_24h is not None:
        if pct_24h < CONFIG["nf_strong_sell"]:
            score -= 10; sigs.append(f"⚠️ Net Flow 24h={pct_24h:+.1f}% — distribusi aktif")
        elif pct_24h < CONFIG["nf_sell"]:
            score -= 5

    if pct_15m is not None and flow_data["15m"]["count"] >= 10:
        if pct_15m > CONFIG["nf_strong_buy"]:
            score += 4; sigs.append(f"✅ Ticks 15m={pct_15m:+.1f}% — beli dominan real-time")
        elif pct_15m < CONFIG["nf_strong_sell"]:
            score -= 5; sigs.append(f"⚠️ Ticks 15m={pct_15m:+.1f}% — jual dominan sekarang")

    return min(score, CONFIG["max_netflow_score"]), sigs, flow_data, should_block

def compute_pump_probability(candles_1h, whale_score=0):
    if len(candles_1h) < 24:
        return {"probability_score": 0.3, "classification": "Data Kurang", "metrics": {}}
    max_spike, avg_spike = calc_volume_spike_ratio(candles_1h)
    irr      = calc_volume_irregularity(candles_1h)
    atr_14   = calc_atr(candles_1h[-24:], 14) or 0
    cur      = candles_1h[-1]["close"] or 1
    norm_atr = (atr_14 / cur) * 100
    def clamp(v, lo, hi):
        return max(0.0, min(1.0, (v - lo) / (hi - lo))) if hi > lo else 0.5
    n_mvs   = clamp(max_spike,  1.0, 10.0)
    n_irr   = clamp(irr,        0.5,  3.5)
    n_avs   = clamp(avg_spike,  0.5,  2.0)
    n_atr   = 1.0 - clamp(norm_atr, 0.05, 3.0)
    n_whale = whale_score / 100.0
    score = max(0.0, min(1.0,
        n_mvs * 0.30 + n_irr * 0.20 + n_avs * 0.12 + n_atr * 0.25 + n_whale * 0.13
    ))
    if score < 0.30:   cls = "Noise"
    elif score < 0.45: cls = "Sideways"
    elif score < 0.60: cls = "Accumulation"
    elif score < 0.75: cls = "Pre-Pump"
    else:               cls = "Imminent Pump"
    return {
        "probability_score": score,
        "classification":    cls,
        "metrics": {
            "max_vol_spike":    round(max_spike, 2),
            "vol_irregularity": round(irr, 3),
            "norm_atr_pct":     round(norm_atr, 4),
        },
    }

def is_already_pumped(oi_chg24h, chg_24h, oi_valid):
    if not oi_valid:
        if chg_24h > 15:
            return True, f"Harga +{chg_24h:.0f}% — pump sudah terjadi"
        return False, ""
    if oi_chg24h > 35 and chg_24h > 3:
        return True, f"OI 24h +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}% — TERLAMBAT"
    if oi_chg24h > 15 and chg_24h > 10:
        return True, f"OI +{oi_chg24h:.0f}% + Harga +{chg_24h:.0f}% — momentum sudah habis"
    return False, ""

def get_time_mult():
    h = utc_hour()
    if h in [5, 6, 7, 8, 11, 12, 13, 19, 20, 21]:
        return 1.15, f"⏰ High-prob window ({h}:00 UTC)"
    if h in [1, 2, 3, 4]:
        return 0.85, "Low-prob window"
    return 1.0, ""


# ══════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════
def master_score(symbol, ticker, tickers_dict):
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])
    c4h  = get_candles(symbol, "4h",  CONFIG["candle_4h"])

    if len(c1h) < 48 or len(c15m) < 20:
        return None

    # Dead activity gate
    if len(c1h) >= 7:
        last_vol   = c1h[-1]["volume_usd"]
        avg_vol_6h = sum(c["volume_usd"] for c in c1h[-7:-1]) / 6
        if avg_vol_6h > 0 and last_vol / avg_vol_6h < CONFIG["dead_activity_threshold"]:
            log.info(f"  {symbol}: GATE dead activity")
            return None

    funding = get_funding(symbol)

    try:
        p7d_ago = c1h[-168]["close"] if len(c1h) >= 168 else c1h[0]["close"]
        chg_7d  = (c1h[-1]["close"] - p7d_ago) / p7d_ago * 100 if p7d_ago > 0 else 0
    except:
        chg_7d = 0

    try:
        chg_24h = float(ticker.get("change24h", 0)) * 100
        vol_24h = float(ticker.get("quoteVolume", 0))
    except:
        chg_24h, vol_24h = 0, 0

    # Gates
    if chg_7d > CONFIG["gate_chg_7d_max"]:
        oi_v = get_open_interest(symbol)
        oi_c1h, _, oi_vld = get_oi_changes(symbol, oi_v) if oi_v > 0 else (0, 0, False)
        if not (funding <= CONFIG["squeeze_funding_max"] and oi_vld and oi_c1h > CONFIG["squeeze_oi_change_min"]):
            log.info(f"  {symbol}: GATE overbought ({chg_7d:.1f}%)")
            return None
    if chg_7d < CONFIG["gate_chg_7d_min"]:
        log.info(f"  {symbol}: GATE downtrend ({chg_7d:.1f}%)")
        return None
    if funding < CONFIG["gate_funding_extreme"]:
        log.info(f"  {symbol}: GATE funding ekstrem ({funding:.5f})")
        return None

    # ── SR ENGINE ────────────────────────────────────────────────────────────
    support_levels, resist_levels, sr_analysis = detect_sr_levels(c1h)

    # ── ANTI-BREAK SUPPORT GATE ─────────────────────────────────────────────
    abs_block, abs_reason = anti_break_support_gate(sr_analysis)
    if abs_block:
        log.info(f"  {symbol}: {abs_reason}")
        return None

    score, sigs, bd = 0, [], {}

    # SR Layer (BARU — paling penting)
    sr_sc, sr_sigs = layer_sr(c1h, sr_analysis, support_levels, resist_levels)
    score += sr_sc; sigs += sr_sigs; bd["sr"] = sr_sc

    # Volume layer
    v_sc, v_sigs, rvol = layer_volume_intelligence(c1h)
    score += v_sc; sigs += v_sigs; bd["vol"] = v_sc

    # Short-term CVD
    stcvd_sc, stcvd_sig = calc_short_term_cvd(c1h)
    score += stcvd_sc
    if stcvd_sig:
        sigs.append(stcvd_sig)
    bd["stcvd"] = stcvd_sc

    # Flat accumulation
    fa_sc, fa_sigs = layer_flat_accumulation(c1h)
    score += fa_sc; sigs += fa_sigs; bd["flat"] = fa_sc

    # Structure
    st_sc, st_sigs, bbw_val, bbw_pct, coiling = layer_structure(c1h)
    score += st_sc; sigs += st_sigs; bd["struct"] = st_sc

    # Stealth bonus
    if len(c1h) >= 6:
        pre6       = c1h[-6:]
        avg_vol_6h = sum(c["volume_usd"] for c in pre6) / 6
        high_6h    = max(c["high"] for c in pre6)
        low_6h     = min(c["low"]  for c in pre6)
        range_6h   = (high_6h - low_6h) / low_6h * 100 if low_6h > 0 else 0
    else:
        avg_vol_6h, range_6h = 0, 0

    stealth_bonus = 0
    if (avg_vol_6h < CONFIG["stealth_max_vol"]
            and coiling > CONFIG["stealth_min_coiling"]
            and range_6h < CONFIG["stealth_max_range"]):
        stealth_bonus = 25
        sigs.append(f"🕵️ STEALTH PATTERN: vol ${avg_vol_6h:.0f}/h coiling {coiling}h")
    score += stealth_bonus; bd["stealth"] = stealth_bonus

    # OI
    oi_value  = get_open_interest(symbol)
    oi_chg1h  = oi_chg24h = 0
    oi_valid  = False
    if oi_value > 0:
        save_oi_snapshot(symbol, oi_value)
        oi_chg1h, oi_chg24h, oi_valid = get_oi_changes(symbol, oi_value)

    pumped, pump_reason = is_already_pumped(oi_chg24h, chg_24h, oi_valid)
    if pumped:
        log.info(f"  {symbol}: GATE already pumped — {pump_reason}")
        return None

    # Positioning
    pos_sc, pos_sigs, ls_ratio, ls_block = layer_positioning(symbol, funding, oi_chg1h)
    if ls_block:
        log.info(f"  {symbol}: GATE L/S overcrowded (L/S={ls_ratio:.2f})")
        return None
    score += pos_sc; sigs += pos_sigs; bd["pos"] = pos_sc

    # 4H confluence
    tf4h_sc = 0
    if c4h:
        tf4h_sc, tf4h_sig = calc_4h_confluence(c4h)
        if tf4h_sig:
            sigs.append(tf4h_sig)
    score += tf4h_sc; bd["tf4h"] = tf4h_sc

    # Context
    ctx_sc, ctx_sigs, sector = layer_context(symbol, tickers_dict)
    score += ctx_sc; sigs += ctx_sigs; bd["ctx"] = ctx_sc

    # Whale
    ws, whale_bonus, wev = calc_whale(symbol, c15m, funding)
    score += whale_bonus; bd["whale"] = whale_bonus

    # Liquidation
    liq_sc, liq_sigs, long_liq, short_liq, liq_block = layer_liquidation(symbol, c1h)
    if liq_block:
        log.info(f"  {symbol}: GATE liquidation")
        return None
    score += liq_sc; sigs += liq_sigs; bd["liq"] = liq_sc

    # RSI
    rsi_1h = get_rsi(c1h[-48:] if len(c1h) >= 48 else c1h)

    # Linea signature
    linea_sc, linea_sigs, linea_components = layer_linea_signature(
        c1h, oi_chg1h, oi_chg24h, oi_valid, ls_ratio, funding, chg_24h
    )
    if linea_components >= 2 and rsi_1h < CONFIG["linea_rsi_max"]:
        linea_sc += 5; linea_sigs.append(f"✅ [Linea-RSI] {rsi_1h:.1f} — oversold, siap reversal")
    score += linea_sc; sigs += linea_sigs; bd["linea"] = linea_sc

    # OI acceleration
    oi_accel_sc, oi_accel_sigs, oi_accel_data = layer_oi_acceleration(symbol, oi_value, chg_24h, vol_24h)
    score += oi_accel_sc; sigs += oi_accel_sigs; bd["oi_accel"] = oi_accel_sc

    # Net flow
    trades_for_flow = get_trades(symbol, 500)
    nf_sc, nf_sigs, nf_data, nf_block = layer_net_flow(c1h, c15m, trades_for_flow)
    if nf_block:
        log.info(f"  {symbol}: GATE net flow distribusi sistematis")
        return None
    score += nf_sc; sigs += nf_sigs; bd["netflow"] = nf_sc

    # OI decline penalties
    if oi_value > 0 and oi_valid:
        if oi_chg24h > 35 and chg_24h > 3:
            return None
        if oi_chg24h < -20:
            score -= 25; sigs.append(f"⚠️ OI 24h turun {oi_chg24h:.1f}% — distribusi masif")
        elif oi_chg24h < -10:
            score -= 15
        elif oi_chg24h < -5:
            score -= 10
        if oi_chg1h < -8:
            score -= 20; sigs.append(f"🚨 OI 1h turun {oi_chg1h:.1f}% — distribusi CEPAT!")
        elif oi_chg1h < -5:
            score -= 12
        elif oi_chg1h > 5:
            score += 5; sigs.append(f"✅ OI 1h naik {oi_chg1h:.1f}% — posisi baru masuk")
        if oi_chg24h < -3 and oi_chg1h < -2:
            log.info(f"  {symbol}: GATE multi-TF OI decline")
            return None
        if rvol > 1.5 and oi_chg24h > 5:
            score += 8; sigs.append(f"✅ Vol naik + OI naik — akumulasi kuat")

    bd["oi_change"]    = round(oi_chg24h, 1)
    bd["oi_change_1h"] = round(oi_chg1h, 1)
    bd["oi_valid"]     = oi_valid

    tmult, tsig = get_time_mult()
    raw_score   = int(score * tmult)
    if tsig:
        sigs.append(tsig)

    prob = compute_pump_probability(c1h, ws)
    bd["prob_score"] = round(prob["probability_score"] * 100, 1)
    bd["prob_class"] = prob["classification"]

    composite = int(
        min(raw_score, 100) * CONFIG["composite_w_layer"]
        + prob["probability_score"] * 100 * CONFIG["composite_w_prob"]
    )
    composite = min(composite, 100)
    bd["composite"] = composite
    bd["rsi_1h"]    = round(rsi_1h, 1)
    bd["linea_comp"] = linea_components

    # ── SMART ENTRY/SL/TP (berbasis SR) ─────────────────────────────────────
    entry = calc_smart_entry(c1h, sr_analysis, support_levels, resist_levels)
    if not entry:
        log.info(f"  {symbol}: SKIP — tidak ada setup SR valid untuk entry")
        return None

    price_now = float(ticker.get("lastPr", 0)) or c1h[-1]["close"]

    return {
        "symbol":           symbol,
        "score":            raw_score,
        "composite_score":  composite,
        "signals":          sigs,
        "ws":               ws,
        "wev":              wev,
        "entry":            entry,
        "sr_analysis":      sr_analysis,
        "sector":           sector,
        "funding":          funding,
        "bd":               bd,
        "price":            price_now,
        "chg_24h":          chg_24h,
        "vol_24h":          vol_24h,
        "rvol":             rvol,
        "ls_ratio":         ls_ratio,
        "chg_7d":           chg_7d,
        "avg_vol_6h":       avg_vol_6h,
        "range_6h":         range_6h,
        "coiling":          coiling,
        "bbw_val":          bbw_val,
        "oi_change_24h":    bd.get("oi_change", 0),
        "oi_change_1h":     bd.get("oi_change_1h", 0),
        "prob_score":       prob["probability_score"],
        "prob_class":       prob["classification"],
        "prob_metrics":     prob.get("metrics", {}),
        "rsi_1h":           rsi_1h,
        "long_liq":         long_liq,
        "short_liq":        short_liq,
        "linea_components": linea_components,
        "oi_accel_score":   oi_accel_sc,
        "oi_accel_data":    oi_accel_data,
        "nf_data":          nf_data,
        "nf_score":         nf_sc,
        "sup_levels":       len(support_levels),
        "res_levels":       len(resist_levels),
        "break_res_recent": sr_analysis.get("break_res_recent", False),
        "is_at_support":    sr_analysis.get("is_at_support", False),
    }


# ══════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════
def _entry_type_label(et):
    return {
        "retest_break_res": "🚨 Retest Break Res (IDEAL)",
        "support_retest":   "📌 Support Retest",
        "momentum":         "⚡ Momentum Entry",
    }.get(et, et)

def build_alert(r, rank=None):
    sc   = r["score"]
    comp = r.get("composite_score", sc)
    bar  = "█" * int(comp / 5) + "░" * (20 - int(comp / 5))
    e    = r["entry"]
    rk   = f"#{rank} " if rank else ""
    vol  = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
            else f"${r['vol_24h']/1e3:.0f}K")
    ls   = f" | L/S:{r['ls_ratio']:.2f}" if r.get("ls_ratio") else ""

    prob_pct = r.get("prob_score", 0) * 100
    prob_cls = r.get("prob_class", "?")
    pm       = r.get("prob_metrics", {})
    bd       = r.get("bd", {})

    linea_str = ""
    if r.get("linea_components", 0) >= 3:
        linea_str = f"<b>Linea Sig :</b> ⭐ {r['linea_components']}/5 komponen!\n"

    accel_str = ""
    ad = r.get("oi_accel_data", {})
    if r.get("oi_accel_score", 0) > 0:
        div_tag   = " 📈DIV" if ad.get("divergence") else ""
        micro_tag = " [MICRO]" if ad.get("is_micro_cap") else ""
        accel_str = (
            f"<b>OI Accel  :</b> +{r['oi_accel_score']}pt{micro_tag}{div_tag} | "
            f"1h:{ad.get('growth_rate_1h',0):+.1f}% "
            f"3h:{ad.get('growth_rate_3h',0):+.1f}% "
            f"6h:{ad.get('growth_rate_6h',0):+.1f}%\n"
        )

    liq_str = ""
    if r.get("long_liq", 0) > 0 or r.get("short_liq", 0) > 0:
        liq_str = (f"<b>Liquidation:</b> Long ${r.get('long_liq',0)/1e3:.0f}K | "
                   f"Short ${r.get('short_liq',0)/1e3:.0f}K (30m)\n")

    nf_str = ""
    nfd = r.get("nf_data", {})
    if nfd.get("has_data"):
        def _fi(label):
            return {"STRONG_BUY": "🟢🟢", "BUY": "🟢", "NEUTRAL": "⚪",
                    "SELL": "🔴", "STRONG_SELL": "🔴🔴"}.get(label, "⚪")
        f72 = nfd.get("72h", {}); f24 = nfd.get("24h", {})
        f6  = nfd.get("6h",  {}); f15 = nfd.get("15m", {})
        nf_str = (
            f"<b>Net Flow   :</b> [{r.get('nf_score',0):+d}pt]\n"
            f"  {_fi(f72.get('label',''))}72h:{f72.get('net_pct',0):+.1f}%  "
            f"{_fi(f24.get('label',''))}24h:{f24.get('net_pct',0):+.1f}%  "
            f"{_fi(f6.get('label',''))}6h:{f6.get('net_pct',0):+.1f}%  "
            f"{_fi(f15.get('label',''))}15m:{f15.get('net_pct',0):+.1f}%\n"
        )

    # SR status
    sr  = r.get("sr_analysis", {})
    sr_status = ""
    if r.get("break_res_recent"):
        sr_status = "🚨 BREAK RES — resistance tertembus ke atas!\n"
    elif r.get("is_at_support"):
        sr_status = f"📌 AT SUPPORT — zona entry ideal\n"
    nearest_sup = sr.get("nearest_sup")
    nearest_res = sr.get("nearest_res")
    sr_levels_str = ""
    if nearest_sup:
        sr_levels_str += f"  🟢 Support: ${nearest_sup['price']:.6g} (vol rank:{nearest_sup['vol_rank']:.0f}%ile, {nearest_sup['touches']}x touch)\n"
    if nearest_res:
        sr_levels_str += f"  🔴 Resist : ${nearest_res['price']:.6g} (vol rank:{nearest_res['vol_rank']:.0f}%ile)\n"

    msg = (
        f"🚨 <b>PRE-PUMP SIGNAL {rk}— SR v1.0</b>\n\n"
        f"<b>Symbol    :</b> {r['symbol']}\n"
        f"<b>Composite :</b> {comp}/100  {bar}\n"
        f"<b>Layer Score:</b> {sc}/100  |  SR Score: {bd.get('sr',0)}pt\n"
        f"<b>Prob Model :</b> {prob_pct:.1f}% ({prob_cls})\n"
        f"<b>RSI 1h     :</b> {r.get('rsi_1h',0):.1f}\n"
        f"{linea_str}"
        f"{accel_str}"
        f"<b>Sektor     :</b> {r['sector']}\n"
        f"<b>Harga      :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h | {r['chg_7d']:+.1f}% 7d)\n"
        f"<b>Vol 24h    :</b> {vol} | RVOL: {r['rvol']:.1f}x{ls}\n"
        f"<b>OI 24h/1h  :</b> {r['oi_change_24h']:+.1f}% / {r.get('oi_change_1h',0):+.1f}%\n"
        f"{nf_str}"
        f"{liq_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>SR ANALYSIS</b>\n"
        f"{sr_status}"
        f"{sr_levels_str}"
        f"  Support total: {r.get('sup_levels',0)} | Resist total: {r.get('res_levels',0)}\n"
        f"\n"
        f"🐋 <b>WHALE SCORE: {r['ws']}/100</b>\n"
    )
    for ev in r["wev"]:
        msg += f"  {ev}\n"

    if e:
        entry_type_lbl = _entry_type_label(e.get("entry_type", ""))
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>ENTRY SETUP ({entry_type_lbl})</b>\n"
            f"  🎯 Cur Price : ${e['cur']:.6g}\n"
            f"  📌 Entry     : ${e['entry']:.6g}\n"
            f"  🛑 Stop Loss : ${e['sl']:.6g}  (-{e['sl_pct']:.2f}%)\n"
            f"     ↳ Di bawah support ${e['sup_level']}\n\n"
            f"🎯 <b>TARGET (SR-BASED)</b>\n"
            f"  T1 : ${e['tp1']:.6g}  (+{e['tp1_pct']:.1f}%)\n"
            f"     ↳ Sebelum resistance ${e['res_level']}\n"
            f"  T2 : ${e['tp2']:.6g}  (+{e['tp2_pct']:.1f}%)\n"
            f"  R/R: 1:{e['rr']}  |  ATR 14: ${e['atr_14']:.6g}\n\n"
            f"  💡 <i>Tips:</i>\n"
            f"  • Entry di zona support — jangan chase breakout\n"
            f"  • SL di bawah support box (jika support jebol = invalidasi)\n"
            f"  • Take 50-60% di T1, trail sisa ke T2\n"
            f"  • Invalidasi jika tutup di bawah SL\n"
        )

    msg += f"\n━━━━━━━━━━━━━━━━━━━━\n📊 <b>SINYAL AKTIF</b>\n"
    for s in r["signals"][:12]:
        msg += f"  • {s}\n"

    msg += (
        f"\n📐 <b>BREAKDOWN</b>\n"
        f"  SR:{bd.get('sr',0)} Vol:{bd.get('vol',0)} StCVD:{bd.get('stcvd',0)} "
        f"Flat:{bd.get('flat',0)} Struct:{bd.get('struct',0)} Pos:{bd.get('pos',0)} "
        f"4H:{bd.get('tf4h',0)} Ctx:{bd.get('ctx',0)} Whale:{bd.get('whale',0)} "
        f"Liq:{bd.get('liq',0)} Linea:{bd.get('linea',0)} "
        f"Accel:{bd.get('oi_accel',0)} Flow:{bd.get('netflow',0)}\n"
        f"  OI valid:{bd.get('oi_valid','?')} RSI:{bd.get('rsi_1h','?')} "
        f"MVS:{pm.get('max_vol_spike','?')}x Irr:{pm.get('vol_irregularity','?')}\n\n"
        f"📡 Funding:{r['funding']:.5f}  🕐 {utc_now()}\n"
        f"<i>⚠️ Bukan financial advice. Manage risk ketat.</i>"
    )
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES SR v1.0 — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        comp     = r.get("composite_score", r["score"])
        bar      = "█" * int(comp / 10) + "░" * (10 - int(comp / 10))
        vol      = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                    else f"${r['vol_24h']/1e3:.0f}K")
        e        = r.get("entry", {})
        tp1_pct  = e.get("tp1_pct", 0) if e else 0
        rr       = e.get("rr", 0)    if e else 0
        prob     = r.get("prob_score", 0) * 100
        rsi      = r.get("rsi_1h", 0)
        sr_sc    = r.get("bd", {}).get("sr", 0)
        linea    = f" ⭐L{r.get('linea_components',0)}" if r.get("linea_components", 0) >= 2 else ""
        break_tag = " 🚨BR" if r.get("break_res_recent") else ""
        sup_tag   = " 📌SUP" if r.get("is_at_support") else ""
        msg += (
            f"{i}. <b>{r['symbol']}</b> [C:{comp} SR:{sr_sc} {bar}]{linea}{break_tag}{sup_tag}\n"
            f"   🐋{r['ws']} | RVOL:{r['rvol']:.1f}x | {vol} | "
            f"T1:+{tp1_pct:.1f}% R/R:1:{rr} | {prob:.0f}% {r.get('prob_class','?')} | RSI:{rsi:.0f}\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    all_candidates = []
    not_found      = []
    stats = defaultdict(int)

    log.info("=" * 70)
    log.info("🔍 SR SCANNER v1.0 — FULL WHITELIST")
    log.info("=" * 70)

    for sym in WHITELIST_SYMBOLS:
        if sym in MANUAL_EXCLUDE:
            stats["manual_exclude"] += 1; continue
        if is_cooldown(sym):
            stats["cooldown"] += 1; continue
        if sym not in tickers:
            not_found.append(sym); continue

        ticker = tickers[sym]
        try:
            vol   = float(ticker.get("quoteVolume", 0))
            chg   = float(ticker.get("change24h", 0)) * 100
            price = float(ticker.get("lastPr", 0))
        except:
            stats["parse_error"] += 1; continue

        if vol < CONFIG["pre_filter_vol"]:
            stats["vol_too_low"] += 1; continue
        if vol > CONFIG["max_vol_24h"]:
            stats["vol_too_high"] += 1; continue
        if abs(chg) > CONFIG["gate_chg_24h_max"]:
            stats["change_extreme"] += 1; continue
        if price <= 0:
            stats["invalid_price"] += 1; continue

        all_candidates.append((sym, ticker))

    total    = len(WHITELIST_SYMBOLS)
    will_scan = len(all_candidates)
    log.info(f"✅ Will scan: {will_scan}/{total} coins")
    log.info(f"⏱️  Est. time: ~{will_scan * CONFIG['sleep_coins'] / 60:.1f} menit")
    log.info("=" * 70)
    return all_candidates


# ══════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== SR PRE-PUMP SCANNER v1.0 — {utc_now()} ===")
    log.info("SR ENGINE: Replikasi Pine Script ChartPrime SR Boxes")
    log.info("FITUR BARU: SR scoring + Smart Entry/SL/TP + Anti-Break-Support gate")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner SR Error: Gagal ambil data Bitget")
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
            continue

        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K)...")
        try:
            res = master_score(sym, t, tickers)
            if res:
                comp     = res["composite_score"]
                prob     = res["prob_score"] * 100
                prob_cls = res["prob_class"]
                sr_sc    = res["bd"].get("sr", 0)
                rr       = res["entry"]["rr"] if res.get("entry") else 0
                br_tag   = " 🚨BREAK_RES" if res.get("break_res_recent") else ""
                sup_tag  = " 📌AT_SUP"    if res.get("is_at_support") else ""
                log.info(
                    f"  Score={res['score']} Comp={comp} W={res['ws']} "
                    f"RVOL={res['rvol']:.1f}x SR={sr_sc} "
                    f"Prob={prob:.0f}% ({prob_cls}) "
                    f"T1=+{res['entry']['tp1_pct']:.1f}% R/R=1:{rr}"
                    f"{br_tag}{sup_tag}"
                )
                if (comp >= CONFIG["min_composite_alert"]
                        and res["prob_score"] >= CONFIG["min_prob_alert"]):
                    results.append(res)
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    # Sort: prioritaskan Break Res > Composite > Whale
    results.sort(
        key=lambda x: (
            int(x.get("break_res_recent", False)) * 100
            + x["composite_score"]
            + x.get("linea_components", 0) * 2,
            x["ws"]
        ),
        reverse=True,
    )
    log.info(f"Lolos threshold: {len(results)} coin")

    qualified = [
        r for r in results
        if (r["ws"] >= CONFIG["min_whale_score"]
            or r["composite_score"] >= 62
            or r["prob_score"] >= 0.75
            or r.get("linea_components", 0) >= 3
            or r.get("break_res_recent", False))
    ]

    if not qualified:
        log.info("Tidak ada sinyal yang memenuhi syarat saat ini")
        return

    top = qualified[:CONFIG["max_alerts_per_run"]]

    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(
                f"✅ Alert #{rank}: {r['symbol']} "
                f"S={r['score']} C={r['composite_score']} W={r['ws']} "
                f"SR={r['bd'].get('sr',0)} "
                f"Prob={r['prob_score']*100:.0f}% "
                f"T1=+{r['entry']['tp1_pct']:.1f}% R/R=1:{r['entry']['rr']}"
            )
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")


# ══════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔════════════════════════════════════════════════════════╗")
    log.info("║  SR PRE-PUMP SCANNER v1.0                             ║")
    log.info("║  Support & Resistance Engine (ChartPrime Pine Script) ║")
    log.info("║  Smart Entry/SL/TP + Anti-Break-Support Gate          ║")
    log.info("╚════════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)

    run_scan()
