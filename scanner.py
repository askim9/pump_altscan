"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v3.1 — AGGRESSIVE EARLY DETECTION                         ║
║                                                                              ║
║  FILOSOFI CORE:                                                              ║
║  Deteksi SEBELUM breakout. Bukan setelah. Bukan saat.                        ║
║  Noise lebih tinggi diterima — miss lebih mahal dari false positive.         ║
║                                                                              ║
║  PERUBAHAN v3.1 vs v3.0:                                                     ║
║  1. score_min_output: 50 → 42  (lebih banyak coin lolos output)             ║
║  2. compression_min_candles: 30 → 20 (deteksi zona lebih cepat)             ║
║  3. compression_range_pct: 0.12 → 0.15 (toleransi lebih lebar)             ║
║  4. zone_purity_spike_max: 2 → 3 (zona lebih toleran)                       ║
║  5. tension_volatility_ratio_max: 0.75 → 0.90 (tension lebih mudah terdeteksi) ║
║  6. tension_breakout_pct: 0.03 → 0.05 (toleransi micro-breakout)           ║
║  7. Volume: early accumulation range diperluas, low vol bonus ditambah      ║
║  8. dist_penalty di-halving: max 40 → 20 (penalty dikurangi 50%)           ║
║  9. slow_trend_penalty: max 20 → 10                                         ║
║  10. late_entry_penalty: 30 → 15 (lebih toleran jika ada konsolidasi)      ║
║  11. PRE-BREAKOUT BIAS MODULE baru: deteksi squeeze + micro-accumulation    ║
║  12. Stealth threshold: 70 → 60 (lebih sensitif)                            ║
║  13. Zone age decay diperlambat: exp(-age/72) → exp(-age/120)               ║
║  14. max_alerts_per_run: 8 → 10                                              ║
║                                                                              ║
║  KOMPONEN SCORING:                                                           ║
║  [A] Compression + Tension      — HIGH (max 35)                             ║
║  [B] Volume Intelligence        — MEDIUM (max 25)                            ║
║  [C] Structure                  — MEDIUM (max 20)                            ║
║  [D] Continuation               — MEDIUM (max 15)                            ║
║  [E] Stealth Accumulation       — BONUS (max 15)                             ║
║  [F] Pre-Breakout Bias (NEW)    — BONUS (max 12)                             ║
║  [G] Distribution Penalty       — capped at −20 (dari −40)                  ║
║  [H] Slow-trend Penalty         — capped at −10 (dari −20)                  ║
║  [I] Late-entry Penalty         — capped at −15 (dari −30)                  ║
║                                                                              ║
║  Confidence:                                                                 ║
║    < 42  → ignore                                                            ║
║    42–60 → early (sinyal awal, noise lebih tinggi)                          ║
║    60–78 → strong pre-pump candidate (PRIORITAS)                            ║
║    > 78  → possibly late stage                                               ║
║                                                                              ║
║  INTERVAL : Setiap 1 jam | EXCHANGE : Bitget USDT-Futures                  ║
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
_fh = _lh.RotatingFileHandler("/tmp/scanner_v3.log", maxBytes=10*1024*1024, backupCount=3)
_fh.setFormatter(_fmt); _root.addHandler(_fh)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── VOLUME SAFETY ─────────────────────────────────────────────────────────
    # Hanya pre-filter kasar — bukan hard filter berbasis sinyal
    "pre_filter_vol":          100_000,   # $100K minimum (noise filter)
    "min_vol_24h":             500_000,   # $500K minimum untuk trading
    "max_vol_24h":         800_000_000,   # $800M ceiling (sudah terlalu ramai)
    "gate_chg_24h_max":          50.0,   # coin sudah naik >50% 24h = definitely late

    # ── CANDLE CONFIG ─────────────────────────────────────────────────────────
    "candle_limit_1h":           720,    # 30 hari data 1H

    # ── REFERENCE WINDOWS (konsisten di semua modul) ──────────────────────────
    "win_short":                  10,    # short-term reference
    "win_mid":                    20,    # mid-term reference
    "win_structure":              50,    # structure window
    "win_compression":           100,    # compression history window (percentile)

    # ── COMPRESSION DETECTION ─────────────────────────────────────────────────
    # v3.1: min_candles 30→20, range 0.12→0.15 — catch zones earlier & wider
    "compression_min_candles":    20,    # 20 jam minimum (dari 30) — deteksi lebih awal
    "compression_max_candles":   720,
    "compression_range_pct":    0.15,   # 15% range (dari 12%) — toleransi lebih lebar
    "compression_lookback":      720,

    # ── ZONE PURITY (anti false compression) ──────────────────────────────────
    # v3.1: spike_max 2→3, choppy_max 0.025→0.03 — lebih toleran
    "zone_purity_vol_mult":       3.0,
    "zone_purity_spike_max":        3,   # toleransi 3 spike (dari 2)
    "compression_choppy_max":    0.030,  # avg candle range < 3% (dari 2.5%)

    # ── ZONE AGE DECAY ────────────────────────────────────────────────────────
    # v3.1: decay diperlambat — zona lama tetap valid lebih lama
    # formula: quality = length * exp(-age / decay_tau)
    "zone_age_decay_tau":         120,   # dari 72 → 120 (zona bertahan lebih lama)

    # ── TENSION SCORING ───────────────────────────────────────────────────────
    # v3.1: rasio ATR lebih longgar + toleransi micro-breakout lebih besar
    "tension_volatility_ratio_max": 0.90,  # dari 0.75 — tension lebih mudah terpenuhi
    "tension_range_percentile_max": 50,    # dari 40 — range < median historis sudah cukup
    "tension_vol_acc_max_ratio":    4.0,   # dari 3.0 — toleransi volume naik lebih tinggi
    "tension_breakout_pct":         0.05,  # dari 0.03 — toleransi micro-breakout 5%
    "tension_breakout_lookback":      6,

    # ── DISTRIBUTION FILTER ────────────────────────────────────────────────────
    # v3.1: penalty di-halving — distribusi masih bisa lolos jika sinyal lain kuat
    "dist_upper_wick_ratio":       0.65,   # dari 0.60 — sedikit lebih toleran
    "dist_vol_spike_mult":         3.0,    # dari 2.5 — butuh spike lebih besar untuk penalty
    "dist_high_vol_no_prog_pct":   0.010,  # dari 0.015 — lebih ketat cek no-progress
    "dist_penalty_cap":             25,    # v3.2: raised to 25 (was 20). Track A alone can hit 25.

    # ── ANTI-LATE-ENTRY ────────────────────────────────────────────────────────
    # v3.1: penalty dikurangi 50% — lebih toleran terhadap sinyal yang sedikit terlambat
    "anti_late_move_pct":          0.10,   # dari 0.08 — harus naik >10% baru penalty
    "anti_late_lookback":          10,
    "anti_late_penalty_cap":        15,    # BARU: cap penalty late di 15 (dari 30)

    # ── CONTINUATION LOGIC ────────────────────────────────────────────────────
    "continuation_move_min":       0.08,   # dari 0.10 — deteksi leg 2 lebih awal
    "continuation_pullback_max":   0.618,  # dari 0.50 — toleransi pullback Fibonacci
    "continuation_lookback":       48,

    # ── VOLUME INTELLIGENCE WINDOWS ──────────────────────────────────────────
    "vol_avg_short":               10,
    "vol_avg_long":                20,
    "vol_acc_window":               5,

    # ── PRE-BREAKOUT BIAS MODULE (NEW v3.1) ───────────────────────────────────
    # Deteksi micro-accumulation patterns SEBELUM volume spike terjadi
    # Window: 5-8 candle terakhir (lebih pendek = lebih early)
    "pbb_window":                   6,    # 6 candle terakhir untuk analisa micro
    "pbb_squeeze_atr_ratio":       0.60,  # ATR6/ATR30 < 0.60 = squeeze kuat
    "pbb_vol_creep_ratio":         0.15,  # volume naik perlahan: avg_last3/avg_prev3 > 1.15
    "pbb_wick_reject_min":         0.40,  # lower wick > 40% range = buyer di bawah
    "pbb_close_top_half":          0.55,  # close di atas 55% range candle = bullish bias

    # ── SCORING THRESHOLDS (OUTPUT FILTER) ────────────────────────────────────
    # v3.1: turunkan floor dari 50 → 42 — lebih banyak early signals keluar
    "score_min_output":            42,    # dari 50 — lebih agresif
    "score_target_low":            60,    # dari 65 — early zona mulai dari 60
    "score_target_high":           78,    # dari 80

    # ── STEALTH ACCUMULATION ──────────────────────────────────────────────────
    # v3.1: threshold 70→60 — stealth terdeteksi lebih mudah
    "stealth_window_candles":      30,
    "stealth_range_pct_max":     0.05,   # dari 0.04 — lebih longgar
    "stealth_atr_period":          14,
    "stealth_atr_ma_period":       20,
    "stealth_std_threshold":    0.013,   # dari 0.010 — lebih toleran
    "stealth_ema_period":          20,
    "stealth_ma_dist_max":       0.008,  # dari 0.005 — lebih longgar
    "stealth_slope_window":         5,
    "stealth_slope_max":         0.005,  # dari 0.003 — lebih longgar
    "stealth_wick_ratio_max":      3.0,  # dari 2.5
    "stealth_vol_spike_reject":    3.5,  # dari 3.0
    "stealth_upper_wick_max":     0.65,  # dari 0.60
    "stealth_vol_cv_max":         0.70,  # dari 0.60
    "stealth_breakdown_tolerance":0.007, # dari 0.005
    "stealth_score_threshold":     60,   # dari 70 — KUNCI: stealth lebih mudah terdeteksi

    # ── ENTRY / TARGET ────────────────────────────────────────────────────────
    "atr_sl_mult":                 1.2,
    "min_target_pct":              7.0,  # dari 8.0 — target minimum lebih rendah
    "min_sl_pct":                  2.0,  # dari 2.5 — SL minimum lebih longgar
    "min_rr":                      1.1,  # dari 1.3 — R/R minimum lebih rendah

    # ── FUNDING ───────────────────────────────────────────────────────────────
    "funding_gate":             -0.005,  # dari -0.003 — lebih toleran terhadap funding negatif

    # ── LIQUIDITY SWEEP BONUS ──────────────────────────────────────────────────
    "liq_sweep_lookback":          16,   # dari 12 — window lebih panjang
    "liq_sweep_recover_bars":       6,   # dari 4 — recovery lebih longgar

    # ── OUTPUT ────────────────────────────────────────────────────────────────
    "max_alerts_per_run":          10,   # dari 8 — lebih banyak coin dikirim
    "alert_cooldown_sec":        3600,
    "sleep_coins":                 0.7,
    "sleep_error":                 3.0,
    "cooldown_file":   "/tmp/v3_cooldown.json",
}

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin
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
    "HEIUSDT", "HEMIUSDT", "HMSTRUSDT", "HOLOUSDT", "HOMEUSDT", "HYPEUSDT", "HYPERUSDT",
    "ICNTUSDT", "ICPUSDT", "IDOLUSDT", "ILVUSDT",
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
    "MOODENGUSDT", "MORPHOUSDT", "MOVEUSDT", "MOVRUSDT", "MUUSDT", "MUBARAKUSDT",
    "MYXUSDT", "NAORISUSDT", "NEARUSDT", "NEIROCTOUSDT",
    "NEOUSDT", "NEWTUSDT", "NILUSDT", "NMRUSDT", "NOMUSDT", "NOTUSDT",
    "NXPCUSDT", "ONDOUSDT", "ONGUSDT", "ONTUSDT", "OPUSDT", "OPENUSDT",
    "OPNUSDT", "ORCAUSDT", "ORDIUSDT", "OXTUSDT", "PARTIUSDT",
    "PENDLEUSDT", "PENGUUSDT", "PEOPLEUSDT", "PEPEUSDT", "PHAUSDT", "PIEVERSEUSDT",
    "PIPPINUSDT", "PLUMEUSDT", "PNUTUSDT", "POLUSDT", "POLYXUSDT",
    "POPCATUSDT", "POWERUSDT", "PROMPTUSDT", "PROVEUSDT", "PUMPUSDT", "PURRUSDT",
    "PYTHUSDT", "QUSDT", "QNTUSDT", "RAVEUSDT", "RAYUSDT",
    "RECALLUSDT", "RENDERUSDT", "RESOLVUSDT", "REZUSDT", "RIVERUSDT", "ROBOUSDT",
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
#  🌐  HTTP
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
#  📡  DATA FETCHERS
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
#  📐  MATH HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _mean(arr):
    return sum(arr) / len(arr) if arr else 0.0

def _std(arr):
    if len(arr) < 2:
        return 0.0
    m = _mean(arr)
    return math.sqrt(sum((x - m) ** 2 for x in arr) / len(arr))

def _percentile(arr, p):
    """Return p-th percentile (0-100) of arr."""
    if not arr:
        return 0.0
    s = sorted(arr)
    k = (len(s) - 1) * p / 100
    f = int(k)
    c = f + 1
    if c >= len(s):
        return s[-1]
    return s[f] + (k - f) * (s[c] - s[f])

def _median(arr):
    if not arr:
        return 0.0
    s = sorted(arr)
    n = len(s)
    return (s[n//2] + s[~(n//2)]) / 2

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

def calc_ema(values, period):
    """Calculate EMA of a list of values."""
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  COMPRESSION ZONE DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
def find_compression_zone(candles):
    """
    Cari zona compression terbaik dari candle history.
    Return zona atau None jika tidak ada yang cukup valid.
    Lebih toleran dari v2.8 — boleh range sedikit lebih lebar,
    karena hard filter sudah digantikan scoring.
    """
    cfg        = CONFIG
    min_len    = cfg["compression_min_candles"]
    max_len    = cfg["compression_max_candles"]
    range_pct  = cfg["compression_range_pct"]
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

        zone_candles = scan_slice[start:end+1]
        vols_zone    = sorted(c["volume_usd"] for c in zone_candles)
        mid          = length // 2
        median_vol   = (vols_zone[mid] + vols_zone[~mid]) / 2 if length > 1 else vols_zone[0]
        avg_vol      = sum(vols_zone) / length

        choppy_max = cfg.get("compression_choppy_max", 0.025)
        avg_candle_range = sum(
            (c["high"] - c["low"]) / c["low"]
            for c in zone_candles if c["low"] > 0
        ) / length
        if avg_candle_range > choppy_max:
            continue

        purity_mult = cfg.get("zone_purity_vol_mult", 3.0)
        purity_max  = cfg.get("zone_purity_spike_max", 2)
        spike_count = sum(
            1 for c in zone_candles
            if median_vol > 0 and c["volume_usd"] > purity_mult * median_vol
        )
        if spike_count > purity_max:
            continue

        age     = (n - 1) - end
        tau     = CONFIG.get("zone_age_decay_tau", 120)   # v3.1: slower decay
        quality = length * math.exp(-age / tau)

        if best is None or quality > best["quality"]:
            best = {
                "start_idx":        start,
                "end_idx":          end,
                "low":              zone_low,
                "high":             zone_high,
                "length":           length,
                "avg_vol":          avg_vol,
                "median_vol":       median_vol,
                "age_candles":      age,
                "quality":          quality,
                "range_pct":        (zone_high - zone_low) / zone_low,
                "avg_candle_range": avg_candle_range,
                "spike_count":      spike_count,
            }

        end = start

    return best

# ══════════════════════════════════════════════════════════════════════════════
#  💧  LIQUIDITY SWEEP DETECTOR
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
#  📊  CANDLE STRUCTURE ANALYZER
# ══════════════════════════════════════════════════════════════════════════════
def analyze_candle_structure(candle):
    body   = abs(candle["close"] - candle["open"])
    rng    = candle["high"] - candle["low"]
    if rng == 0:
        return 0, "doji"

    lower_wick = min(candle["open"], candle["close"]) - candle["low"]
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
#  🎯  ENTRY & TARGET CALCULATOR
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
        comp_len = compression_zone["length"]
        atr_mult = min(4.0 + comp_len / 48, 10.0)
        t1_atr   = entry + atr * atr_mult
        t1_min   = cur * 1.10
        t1       = max(t1_atr, t1_min)
        t2       = max(t1 * 1.20, cur * 1.22)

    if abs(t2 - t1) / t1 < 0.03:
        t2 = t1 * 1.15

    t1_pct = round((t1 - cur) / cur * 100, 1)
    t2_pct = round((t2 - cur) / cur * 100, 1)
    rr     = round((t1 - entry) / (entry - sl), 1) if (entry - sl) > 0 else 0

    poc    = calc_poc(candles[-168:])

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
        "z2":     round(poc, 8) if poc else None,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🔬  FORENSIC PATTERN DETECTORS
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
    return (late_end - late_start) / late_start * 100 > 0.3

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

def detect_failed_breakdown(candles, comp_low, lookback=20):
    """
    Cek apakah ada failed breakdown (break support lalu recover).
    Lebih longgar dari liq_sweep: cek dalam 20 candle terakhir.
    """
    recent = candles[-lookback:] if len(candles) >= lookback else candles
    for i in range(len(recent) - 2):
        c = recent[i]
        if c["low"] < comp_low * 0.98:
            for j in range(i + 1, min(i + 6, len(recent))):
                if recent[j]["close"] > comp_low:
                    return True
    return False

# ══════════════════════════════════════════════════════════════════════════════
#  🟦  STEALTH ACCUMULATION DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
def detect_stealth_accumulation(candles):
    cfg = CONFIG
    N   = cfg.get("stealth_window_candles", 30)

    _null = {
        "detected": False, "score": 0.0, "status": "NONE",
        "range_pct": 0.0, "atr_trend": "unknown",
        "ma_distance": 0.0, "volume_stability": 1.0,
        "structure_quality": "insufficient data", "entry_trigger": False,
        "_score_vol_suppress": 0, "_score_price_compress": 0,
        "_score_ma_hug": 0, "_score_low_noise": 0,
        "_score_no_dist": 0, "_score_vol_stable": 0,
        "_structure_penalty": 0, "_distribution_flags": 0,
    }

    atr_ma_p    = cfg.get("stealth_atr_ma_period", 20)
    atr_period  = cfg.get("stealth_atr_period", 14)
    min_candles = N + atr_ma_p + atr_period + 5

    if not candles or len(candles) < min_candles:
        return _null

    confirmed = candles[:-1]
    current   = candles[-1]
    window    = confirmed[-N:]

    if len(window) < N:
        return _null

    highs  = [c["high"]  for c in window]
    lows   = [c["low"]   for c in window]
    closes = [c["close"] for c in window]
    opens  = [c["open"]  for c in window]
    vols   = [c.get("volume_usd", c.get("volume", 0)) for c in window]
    n_win  = len(window)

    avg_price      = _mean(closes)
    avg_vol_window = _mean(vols)

    # [1] Volatility suppression (+25)
    max_h     = max(highs)
    min_l     = min(lows)
    range_pct = (max_h - min_l) / min_l if min_l > 0 else 1.0

    score_vol_suppress = 0.0
    rng_limit = cfg.get("stealth_range_pct_max", 0.04)
    if range_pct < rng_limit:
        score_vol_suppress = 25.0
    elif range_pct < rng_limit * 1.5:
        score_vol_suppress = 15.0
    elif range_pct < rng_limit * 2.0:
        score_vol_suppress = 7.0

    # ATR trend
    atr_p     = cfg.get("stealth_atr_period", 14)
    atr_ma_p_ = cfg.get("stealth_atr_ma_period", 20)
    all_cl    = [c["close"] for c in confirmed]
    all_hi    = [c["high"]  for c in confirmed]
    all_lo    = [c["low"]   for c in confirmed]

    atr_values = []
    if len(confirmed) > atr_p + atr_ma_p_ + 5:
        for i in range(atr_p, len(confirmed)):
            chunk = confirmed[max(0, i - atr_p): i + 1]
            v = calc_atr(chunk, min(atr_p, len(chunk) - 1))
            if v is not None:
                atr_values.append(v)

    atr_trend_label = "unknown"
    if len(atr_values) >= atr_ma_p_:
        recent_atr = _mean(atr_values[-5:])
        older_atr  = _mean(atr_values[-atr_ma_p_: -5]) if atr_ma_p_ > 10 else _mean(atr_values[:5])
        if older_atr > 0:
            ratio = recent_atr / older_atr
            if ratio < 0.85:
                atr_trend_label = "decreasing"
                score_vol_suppress = min(25.0, score_vol_suppress + 5.0)
            elif ratio < 1.05:
                atr_trend_label = "flat"
            else:
                atr_trend_label = "increasing"
                score_vol_suppress = max(0.0, score_vol_suppress - 5.0)

    score_vol_suppress = round(max(0.0, min(25.0, score_vol_suppress)), 2)

    # [2] Price compression (+20)
    std_close = _std(closes)
    score_price_compress = 0.0
    std_limit = avg_price * cfg.get("stealth_std_threshold", 0.010)
    if avg_price > 0 and std_close < std_limit:
        score_price_compress = 20.0
    elif avg_price > 0 and std_close < std_limit * 1.5:
        score_price_compress = 12.0
    elif avg_price > 0 and std_close < std_limit * 2.0:
        score_price_compress = 5.0
    score_price_compress = round(max(0.0, min(20.0, score_price_compress)), 2)

    # [3] MA hugging (+20)
    ema_p          = cfg.get("stealth_ema_period", 20)
    slope_win      = cfg.get("stealth_slope_window", 5)
    ma_dist_max    = cfg.get("stealth_ma_dist_max", 0.005)
    slope_max      = cfg.get("stealth_slope_max", 0.003)
    score_ma_hug   = 0.0
    ma_distance    = 0.0

    all_closes_ext = [c["close"] for c in confirmed]
    if len(all_closes_ext) >= ema_p:
        ema_now = calc_ema(all_closes_ext, ema_p)
        if ema_now and ema_now > 0:
            ma_distance = abs(closes[-1] - ema_now) / ema_now

            if ma_distance < ma_dist_max:
                score_ma_hug += 15.0
            elif ma_distance < ma_dist_max * 2:
                score_ma_hug += 8.0
            elif ma_distance < ma_dist_max * 3:
                score_ma_hug += 3.0

            if len(all_closes_ext) >= ema_p + slope_win:
                ema_prev = calc_ema(all_closes_ext[:-slope_win], ema_p)
                if ema_prev and ema_prev > 0:
                    slope = abs(ema_now - ema_prev) / ema_prev
                    if slope < slope_max:
                        score_ma_hug += 5.0
                    elif slope < slope_max * 2:
                        score_ma_hug += 2.0

    ma_distance_pct = ma_distance
    score_ma_hug = round(max(0.0, min(20.0, score_ma_hug)), 2)

    # [4] Low noise structure (+15)
    score_low_noise = 0.0
    wick_ratios = []
    for i in range(n_win):
        o, c_, h, l = opens[i], closes[i], highs[i], lows[i]
        body_ = abs(c_ - o)
        if body_ > 0:
            upper = h - max(o, c_)
            lower = min(o, c_) - l
            wick_ratios.append((upper + lower) / body_)

    if wick_ratios:
        avg_wick_ratio = _mean(wick_ratios)
        wick_limit     = cfg.get("stealth_wick_ratio_max", 2.5)
        if avg_wick_ratio < wick_limit * 0.5:
            score_low_noise = 15.0
        elif avg_wick_ratio < wick_limit:
            score_low_noise = 10.0
        elif avg_wick_ratio < wick_limit * 1.5:
            score_low_noise = 4.0
    score_low_noise = round(max(0.0, min(15.0, score_low_noise)), 2)

    # [5] No distribution (+10)
    score_no_dist      = 10.0
    distribution_flags = 0
    vol_spike_limit    = cfg.get("stealth_vol_spike_reject", 3.0)
    upper_wick_limit   = cfg.get("stealth_upper_wick_max", 0.60)
    vol_median         = _median(vols) if vols else 0

    for i in range(n_win):
        o, c_, h, l = opens[i], closes[i], highs[i], lows[i]
        v   = vols[i]
        rng = h - l
        is_red    = c_ < o
        vol_spike = vol_median > 0 and v > vol_spike_limit * vol_median

        if is_red and vol_spike:
            distribution_flags += 1
        if rng > 0:
            upper_wick_pct = (h - max(o, c_)) / rng
            if upper_wick_pct > upper_wick_limit:
                distribution_flags += 1
        if vol_spike:
            distribution_flags += 1

    if distribution_flags >= 5:
        score_no_dist = 0.0
    elif distribution_flags >= 3:
        score_no_dist = 3.0
    elif distribution_flags >= 1:
        score_no_dist = 7.0
    score_no_dist = round(max(0.0, min(10.0, score_no_dist)), 2)

    # [6] Volume stability (+10)
    score_vol_stable = 0.0
    volume_cv        = 1.0
    if vols and avg_vol_window > 0:
        std_vol   = _std(vols)
        volume_cv = std_vol / avg_vol_window
        cv_max    = cfg.get("stealth_vol_cv_max", 0.60)
        if volume_cv < cv_max:
            ratio = volume_cv / cv_max
            score_vol_stable = 10.0 * (1.0 - ratio)
            half       = n_win // 2
            avg_first  = _mean(vols[:half])  if half > 0 else 0.0
            avg_second = _mean(vols[half:])  if half > 0 else 0.0
            if avg_first > 0 and 1.0 <= avg_second / avg_first <= 1.5:
                score_vol_stable = min(10.0, score_vol_stable + 2.0)
    score_vol_stable = round(max(0.0, min(10.0, score_vol_stable)), 2)

    # [7] Structure stability (penalty)
    structure_quality = "stable"
    structure_penalty = 0.0
    breakdown_tol     = cfg.get("stealth_breakdown_tolerance", 0.005)
    lower_low_count   = 0
    for i in range(1, n_win):
        if lows[i] < lows[i-1] * (1.0 - breakdown_tol):
            lower_low_count += 1
    selloff_count = 0
    for i in range(n_win):
        rng_ = highs[i] - lows[i]
        if rng_ > 0:
            body_ = opens[i] - closes[i]
            if body_ > 0 and body_ / rng_ > 0.70:
                selloff_count += 1

    if lower_low_count > n_win * 0.30:
        structure_quality = "unstable (lower lows)"
        structure_penalty = 10.0
    elif selloff_count > n_win * 0.20:
        structure_quality = "unstable (sell-off)"
        structure_penalty = 8.0
    elif lower_low_count > n_win * 0.15:
        structure_quality = "weak (minor lower lows)"
        structure_penalty = 4.0

    stealth_score = (
        score_vol_suppress + score_price_compress + score_ma_hug +
        score_low_noise    + score_no_dist        + score_vol_stable
    )
    stealth_score = max(0.0, stealth_score - structure_penalty)
    stealth_score = round(min(100.0, stealth_score), 2)

    threshold = cfg.get("stealth_score_threshold", 70)
    detected  = stealth_score >= threshold
    status    = "STEALTH_ACCUMULATION_DETECTED" if detected else "NONE"

    # Entry trigger
    breakout_vol_mult = cfg.get("stealth_breakout_vol_mult", 1.5) if hasattr(cfg, "get") else 1.5
    breakout_vol_mult = 1.5
    recent_high       = max(highs)
    cur_close         = current["close"]
    cur_vol           = current.get("volume_usd", current.get("volume", 0))
    entry_trigger     = (
        cur_close > recent_high and avg_vol_window > 0
        and cur_vol > avg_vol_window * breakout_vol_mult
    )

    return {
        "detected": detected, "score": stealth_score, "status": status,
        "range_pct": round(range_pct, 6), "atr_trend": atr_trend_label,
        "ma_distance": round(ma_distance_pct, 6),
        "volume_stability": round(volume_cv, 4),
        "structure_quality": structure_quality,
        "entry_trigger": entry_trigger,
        "_score_vol_suppress":   score_vol_suppress,
        "_score_price_compress": score_price_compress,
        "_score_ma_hug":         score_ma_hug,
        "_score_low_noise":      score_low_noise,
        "_score_no_dist":        score_no_dist,
        "_score_vol_stable":     score_vol_stable,
        "_structure_penalty":    structure_penalty,
        "_lower_low_count":      lower_low_count,
        "_distribution_flags":   distribution_flags,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📊  ADAPTIVE VOLUME INTELLIGENCE
#  Relative scoring — semua vs rolling history coin itu sendiri
# ══════════════════════════════════════════════════════════════════════════════
def score_volume_intelligence(candles):
    """
    Adaptive volume scoring: SEMUA metrics relatif terhadap rolling history.
    Tidak ada fixed threshold.
    Return (score 0-25, breakdown_dict)
    """
    if len(candles) < 25:
        return 0, {"reason": "insufficient data"}

    cur_vol    = candles[-1]["volume_usd"]
    vols_short = [c["volume_usd"] for c in candles[-CONFIG["vol_avg_short"]:]]
    vols_long  = [c["volume_usd"] for c in candles[-CONFIG["vol_avg_long"]:]]

    avg_short  = _mean(vols_short)
    avg_long   = _mean(vols_long)

    if avg_long <= 0:
        return 0, {"reason": "no volume history"}

    vol_ratio = cur_vol / avg_long

    # Volume acceleration (vs avg_short to detect recent uptick)
    vol_acceleration = (cur_vol - avg_short) / avg_short if avg_short > 0 else 0

    # Scoring based on vol_ratio — adaptive classification
    # v3.1: early range widened, low-vol gets meaningful score (not penalized)
    if vol_ratio < 0.3:
        # Very suppressed — deep stealth accumulation, smart money not showing hand
        vol_score = 12
        vol_label = f"deep stealth ({vol_ratio:.2f}x) — possible smart money"
    elif 0.3 <= vol_ratio < 0.7:
        # Low but building — early accumulation, NOT a negative
        vol_score = 16
        vol_label = f"quiet accum ({vol_ratio:.2f}x)"
    elif 0.7 <= vol_ratio <= 1.5:
        # Normal-to-slightly-elevated — classic pre-pump zone
        vol_score = 20
        vol_label = f"pre-pump zone ({vol_ratio:.2f}x)"
    elif 1.5 < vol_ratio <= 3.0:
        # Clear volume expansion — strong early signal
        vol_score = 25
        vol_label = f"vol expansion ({vol_ratio:.2f}x)"
    elif 3.0 < vol_ratio <= 5.0:
        # Large spike — still useful but may be later stage
        vol_score = 15
        vol_label = f"large spike ({vol_ratio:.2f}x)"
    else:
        # Extreme — distribution or already-pumped territory
        vol_score = 5
        vol_label = f"extreme ({vol_ratio:.2f}x) — caution"

    # Volume acceleration bonus/penalty — v3.1: reward positive more, penalize less
    if vol_acceleration > 0.3:
        vol_score = min(25, vol_score + 10)
        vol_label += " +acc↑↑"
    elif vol_acceleration > 0.05:
        # Even tiny positive acceleration is a pre-pump signal
        vol_score = min(25, vol_score + 5)
        vol_label += " +acc↑"
    elif vol_acceleration < -0.5:
        # Only penalize if strongly declining
        vol_score = max(0, vol_score - 3)
        vol_label += " -acc"

    # RVOL vs same hour
    rvol = 1.0
    if len(candles) >= 25:
        last_vol       = candles[-2]["volume_usd"]
        target_hour    = (candles[-2]["ts"] // 3_600_000) % 24
        same_hour_vols = [c["volume_usd"] for c in candles[:-2]
                          if (c["ts"] // 3_600_000) % 24 == target_hour]
        if same_hour_vols:
            avg_sh = _mean(same_hour_vols)
            rvol   = last_vol / avg_sh if avg_sh > 0 else 1.0

    # ── RVOL scoring — dedicated component (v3.2) ────────────────────────────
    # RVOL vs same-hour is more honest than vol_ratio alone.
    # Elevated RVOL = real abnormal activity, not just a high-volume coin.
    rvol_score = 0
    if rvol >= 3.0:
        rvol_score = 10   # strong abnormal activity
        vol_label += f" RVOL:{rvol:.1f}x↑↑"
    elif rvol >= 2.0:
        rvol_score = 7
        vol_label += f" RVOL:{rvol:.1f}x↑"
    elif rvol >= 1.5:
        rvol_score = 4
        vol_label += f" RVOL:{rvol:.1f}x"
    elif rvol >= 1.0:
        rvol_score = 2
    # RVOL < 1.0 = no bonus, no penalty (could be accumulation before RVOL rises)

    vol_score = min(vol_score + rvol_score, 30)   # v3.2: ceiling raised 25→30

    return vol_score, {
        "vol_ratio":        round(vol_ratio, 3),
        "vol_acceleration": round(vol_acceleration, 3),
        "rvol":             round(rvol, 2),
        "rvol_score":       rvol_score,
        "label":            vol_label,
        "avg_long":         avg_long,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📦  ADAPTIVE COMPRESSION + TENSION SCORE
#  HIGH PRIORITY — core of pre-pump detection
# ══════════════════════════════════════════════════════════════════════════════
def score_compression_tension(candles, compression):
    """
    Adaptive compression + tension scoring.
    Uses percentile of range_pct vs rolling 100-candle history.
    Tension exists when ALL 4 conditions met (numerically defined).
    Return (score 0-35, breakdown_dict)
    """
    if compression is None:
        return 0, {"reason": "no compression zone"}

    comp_length = compression["length"]
    range_pct   = compression["range_pct"]

    # ── Compression score based on length ─────────────────────────────────────
    comp_score = 0
    if comp_length >= 30:  comp_score += 8
    if comp_length >= 72:  comp_score += 7   # 3+ hari
    if comp_length >= 168: comp_score += 6   # 7+ hari
    if comp_length >= 336: comp_score += 5   # 14+ hari

    # Range quality bonus — tighter is better
    if range_pct < 0.04:   comp_score += 5   # sangat ketat
    elif range_pct < 0.07: comp_score += 3
    elif range_pct < 0.10: comp_score += 1

    comp_score = min(comp_score, 22)   # v3.2: raised from 20

    # ── Tension detection (numerik, bukan binary) ─────────────────────────────
    tension_score = 0
    tension_details = []

    # Condition 1: volatility_ratio (ATR short vs ATR long)
    win_short = CONFIG["win_short"]
    win_long  = CONFIG["win_structure"]
    atr_short = calc_atr(candles[-(win_short + 5):], win_short)
    atr_long  = calc_atr(candles[-(win_long + 5):],  win_long)

    vol_ratio = (atr_short / atr_long) if (atr_short and atr_long and atr_long > 0) else 1.0
    tension_max_ratio = CONFIG["tension_volatility_ratio_max"]
    if vol_ratio < tension_max_ratio:
        r = max(0, tension_max_ratio - vol_ratio) / tension_max_ratio
        tension_score += int(r * 12)   # v3.2: raised from 10 → 12
        tension_details.append(f"ATR_ratio={vol_ratio:.2f}")

    # Condition 2: range_pct < percentile_40 of last 100 candles
    history_ranges = []
    win_history = min(CONFIG["win_compression"], len(candles) - comp_length)
    if win_history > 20:
        # compute rolling range for chunks of comp_length size
        step = max(1, comp_length // 4)
        for i in range(0, win_history - comp_length, step):
            chunk = candles[-(win_history + comp_length - i): -(comp_length - i) or None]
            if len(chunk) >= comp_length:
                sub = chunk[:comp_length]
                h   = max(c["high"] for c in sub)
                l   = min(c["low"]  for c in sub)
                if l > 0:
                    history_ranges.append((h - l) / l)

    if history_ranges:
        p40 = _percentile(history_ranges, CONFIG["tension_range_percentile_max"])
        if range_pct < p40:
            tension_score += 10
            tension_details.append(f"range<p40({p40*100:.1f}%)")
        elif range_pct < _percentile(history_ranges, 60):
            tension_score += 5
            tension_details.append(f"range<p60")

    # Condition 3: volume acceleration > 0 but not extreme
    vols_recent = [c["volume_usd"] for c in candles[-10:]]
    vols_older  = [c["volume_usd"] for c in candles[-20:-10]]
    avg_recent  = _mean(vols_recent)
    avg_older   = _mean(vols_older)
    vol_acc_ratio = (avg_recent / avg_older) if avg_older > 0 else 1.0
    if 1.0 < vol_acc_ratio < CONFIG["tension_vol_acc_max_ratio"]:
        tension_score += 5
        tension_details.append(f"vol_acc={vol_acc_ratio:.2f}x")

    # Condition 4: no breakout >3% in last 6 candles
    lookback_bt = CONFIG["tension_breakout_lookback"]
    recent_moves = [
        abs(candles[-i]["close"] - candles[-i]["open"]) / candles[-i]["open"]
        for i in range(1, min(lookback_bt + 1, len(candles)))
        if candles[-i]["open"] > 0
    ]
    max_recent_move = max(recent_moves) if recent_moves else 0
    if max_recent_move < CONFIG["tension_breakout_pct"]:
        tension_score += 5
        tension_details.append("no_breakout")

    tension_score = min(tension_score, 18)   # v3.2: raised from 15 → 18

    total = comp_score + tension_score
    total = min(total, 40)   # v3.2: raised from 35 → 40

    return total, {
        "comp_score":    comp_score,
        "tension_score": tension_score,
        "vol_ratio":     round(vol_ratio, 3),
        "tension_details": tension_details,
        "comp_length":   comp_length,
        "range_pct":     round(range_pct, 4),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🏗  STRUCTURE SCORE (Smart Money Footprint)
# ══════════════════════════════════════════════════════════════════════════════
def score_structure(candles, compression):
    """
    Score structure signals. Max 20 pts.
    """
    score = 0
    breakdown = []

    # Higher lows
    higher_lows = detect_higher_lows(candles, lookback=4)
    if higher_lows:
        score += 10
        breakdown.append("higher_lows +10")

    # Flat base (compression stability already in compression score)
    # Check if price acceleration is present
    if detect_price_acceleration(candles, lookback=6):
        score += 5
        breakdown.append("price_accel +5")

    # Failed breakdown bonus — v3.2: raised 12→18 (strongest smart-money signal)
    if compression:
        failed_bd = detect_failed_breakdown(candles, compression["low"], lookback=20)
        if failed_bd:
            score += 18
            breakdown.append("failed_breakdown +18")

    # Candle structure — v3.2: ceiling reduced 15→8 (noisy signal, easily faked)
    candle_score, candle_label = analyze_candle_structure(candles[-1])
    candle_score = min(candle_score, 8)   # hard cap
    score += candle_score
    breakdown.append(f"candle:{candle_label} +{candle_score}")

    # Pre-spike candle
    pre_spike_sc, pre_spike_label = detect_pre_pump_candle(candles)
    score += pre_spike_sc
    if pre_spike_sc > 0:
        breakdown.append(f"pre_spike:{pre_spike_label} +{pre_spike_sc}")

    # Lower highs penalty
    if len(candles) >= 6:
        highs = [candles[-i]["high"] for i in range(1, 7)]
        lower_high_count = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1] * 0.98)
        if lower_high_count >= 4:
            score -= 10
            breakdown.append("lower_highs_pattern -10")

    score = max(-20, min(score, 20))

    return score, {
        "score":          score,
        "breakdown":      breakdown,
        "higher_lows":    higher_lows,
        "candle_label":   candle_label,
        "pre_spike_label": pre_spike_label,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🔄  CONTINUATION SCORE (Second Leg Logic)
# ══════════════════════════════════════════════════════════════════════════════
def score_continuation(candles):
    """
    If coin moved >10% recently, check for high-quality pullback and tight base.
    Return (score 0-15, details)
    """
    if len(candles) < CONFIG["continuation_lookback"]:
        return 0, {}

    lb = CONFIG["continuation_lookback"]
    recent = candles[-lb:]

    price_start = recent[0]["close"]
    price_max   = max(c["high"] for c in recent)
    price_now   = recent[-1]["close"]

    if price_start <= 0:
        return 0, {}

    move_pct = (price_max - price_start) / price_start

    if move_pct < CONFIG["continuation_move_min"]:
        return 0, {"reason": "no prior move"}

    # Pullback from peak
    pullback_pct = (price_max - price_now) / price_max if price_max > 0 else 1.0

    if pullback_pct > CONFIG["continuation_pullback_max"]:
        return 0, {"reason": f"pullback too deep ({pullback_pct*100:.1f}%)"}

    # Check tight consolidation post-pullback (last 10 candles)
    last10 = candles[-10:]
    h10    = max(c["high"] for c in last10)
    l10    = min(c["low"]  for c in last10)
    rng10  = (h10 - l10) / l10 if l10 > 0 else 1.0

    # Higher low in post-pullback zone
    is_higher_low = price_now > price_start * 1.02

    score = 0
    details = {"move_pct": round(move_pct, 3), "pullback_pct": round(pullback_pct, 3),
               "rng10": round(rng10, 4)}

    if pullback_pct < 0.382:
        score += 10
        details["pullback_quality"] = "shallow (<38.2%)"
    elif pullback_pct < 0.50:
        score += 6
        details["pullback_quality"] = "moderate (38-50%)"

    if rng10 < 0.04:
        score += 5
        details["consolidation"] = "tight"
    elif rng10 < 0.07:
        score += 2
        details["consolidation"] = "moderate"

    if is_higher_low:
        score += 3
        details["higher_low"] = True

    return min(score, 15), details

# ══════════════════════════════════════════════════════════════════════════════
#  🚨  DISTRIBUTION PENALTY
#  Anti-trap: aggressively penalize fake strength
# ══════════════════════════════════════════════════════════════════════════════
def calc_distribution_penalty(candles):
    """
    v3.2 — Two-track distribution filter:
      Track A (heavy): high volume + no price progress — strongest false-positive signal.
        Max 25pts. This is the dominant distribution pattern before dumps.
      Track B (soft): upper wick rejection + lower highs structure.
        Max 15pts. Meaningful but less reliable alone.
    Total capped at CONFIG["dist_penalty_cap"] (default 25).
    """
    if len(candles) < 10:
        return 0, {}

    recent     = candles[-10:]
    price_now  = candles[-1]["close"]
    price_10a  = candles[-10]["close"]
    vols       = [c["volume_usd"] for c in recent]
    vol_median = _median(vols)
    dist_thresh = CONFIG["dist_vol_spike_mult"]      # 3.0x
    uw_thresh   = CONFIG["dist_upper_wick_ratio"]    # 0.65
    prog_thresh = CONFIG["dist_high_vol_no_prog_pct"] # 0.010

    penalty = 0
    flags   = []

    # ── TRACK A: High volume + no price progress (heaviest signal) ─────────────
    # This is what distribution looks like: smart money dumps into volume
    # while retail sees the volume and thinks it's a buy signal.
    high_vol_no_prog  = 0
    high_vol_red      = 0   # v3.2: high-vol red candle = additional confirmation
    for c in recent:
        v        = c["volume_usd"]
        high_vol = vol_median > 0 and v > dist_thresh * vol_median
        if high_vol:
            progress = abs(c["close"] - c["open"]) / max(c["open"], 1e-10)
            if progress < prog_thresh:
                high_vol_no_prog += 1
            if c["close"] < c["open"]:   # red + high volume
                high_vol_red += 1

    if high_vol_no_prog >= 3:
        penalty += 25
        flags.append(f"hvnp:{high_vol_no_prog}x (severe)")
    elif high_vol_no_prog >= 2:
        penalty += 18
        flags.append(f"hvnp:{high_vol_no_prog}x")
    elif high_vol_no_prog >= 1:
        penalty += 8
        flags.append(f"hvnp:1x")

    # High volume + red candles compound the penalty
    if high_vol_red >= 2 and high_vol_no_prog >= 1:
        penalty += 7
        flags.append(f"hvred:{high_vol_red}x")

    # ── TRACK B: Structural distribution signals (softer) ─────────────────────
    track_b = 0

    # Upper wick rejections (price pushed up then sold down = supply overhead)
    upper_wick_count = sum(
        1 for c in recent
        if (c["high"] - c["low"]) > 0 and
        (c["high"] - max(c["open"], c["close"])) / (c["high"] - c["low"]) > uw_thresh
    )
    if upper_wick_count >= 4:
        track_b += 12
        flags.append(f"upper_wicks:{upper_wick_count}")
    elif upper_wick_count >= 3:
        track_b += 7
        flags.append(f"upper_wicks:{upper_wick_count}")
    elif upper_wick_count >= 2:
        track_b += 3

    # Lower highs (bearish structure)
    highs = [c["high"] for c in recent]
    lower_high_seq = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1] * 0.99)
    if lower_high_seq >= 6:
        track_b += 12
        flags.append(f"lower_highs:{lower_high_seq}")
    elif lower_high_seq >= 4:
        track_b += 5

    # Price down on elevated volume (overall window)
    avg_vol      = _mean(vols)
    comp_vol_ref = candles[-20:-10]
    avg_comp_vol = _mean([c["volume_usd"] for c in comp_vol_ref]) if comp_vol_ref else avg_vol
    overall_move = (price_now - price_10a) / price_10a if price_10a > 0 else 0
    if avg_comp_vol > 0 and avg_vol > 1.5 * avg_comp_vol and overall_move < -0.02:
        track_b += 8
        flags.append("vol_up_price_down")

    penalty += min(track_b, 15)   # cap track B contribution

    cap = CONFIG.get("dist_penalty_cap", 25)   # v3.2: raised cap 20→25
    penalty = min(penalty, cap)
    return penalty, {"penalty": penalty, "flags": flags,
                     "high_vol_no_prog": high_vol_no_prog, "upper_wick_count": upper_wick_count}

# ══════════════════════════════════════════════════════════════════════════════
#  🐢  SLOW TREND PENALTY
#  Anti-drift: avoid smooth trends without compression
# ══════════════════════════════════════════════════════════════════════════════
def calc_slow_trend_penalty(candles, compression):
    """
    Penalize coins that are just drifting up/down without tension.
    Return (penalty 0-20, details)
    """
    if len(candles) < 30:
        return 0, {}

    closes = [c["close"] for c in candles[-30:]]
    ma5    = _mean(closes[-5:])
    ma20   = _mean(closes[-20:])
    ma30   = _mean(closes)

    # Smooth uptrend without compression = slow trend
    trending = (ma5 > ma20 > ma30 * 1.005) or (ma5 < ma20 < ma30 * 0.995)

    # ATR compression check
    atr_s = calc_atr(candles[-12:], 10)
    atr_l = calc_atr(candles[-32:], 30)
    no_compression = (atr_s is not None and atr_l is not None and atr_l > 0
                      and atr_s / atr_l > 0.90)

    # Compression zone exists — if yes, no slow-trend penalty
    has_compression = compression is not None

    if trending and no_compression and not has_compression:
        return min(10, 10), {"reason": "smooth trend without compression"}   # v3.1: capped at 10
    elif trending and no_compression and has_compression and compression["length"] < 30:
        return min(5, 10), {"reason": "short compression + trending"}
    return 0, {}

# ══════════════════════════════════════════════════════════════════════════════
#  ⏰  ANTI-LATE-ENTRY PENALTY
# ══════════════════════════════════════════════════════════════════════════════
def calc_late_entry_penalty(candles):
    """
    If price moved >8% in last 6-12 candles without consolidation → heavy penalty.
    Return (penalty 0-30, details)
    """
    lb = CONFIG["anti_late_lookback"]
    if len(candles) < lb + 2:
        return 0, {}

    price_now   = candles[-1]["close"]
    price_start = candles[-(lb + 1)]["close"]

    if price_start <= 0:
        return 0, {}

    move_pct = (price_now - price_start) / price_start

    if abs(move_pct) < CONFIG["anti_late_move_pct"]:
        return 0, {}

    # Check if there's consolidation after the move
    recent5 = candles[-5:]
    h5 = max(c["high"] for c in recent5)
    l5 = min(c["low"]  for c in recent5)
    rng5 = (h5 - l5) / l5 if l5 > 0 else 1.0

    has_consolidation = rng5 < 0.04   # tight last 5 = consolidating

    if not has_consolidation:
        cap     = CONFIG.get("anti_late_penalty_cap", 15)   # v3.1: capped at 15
        penalty = cap if abs(move_pct) > 0.15 else cap // 2
        return penalty, {
            "move_pct": round(move_pct, 3),
            "rng5": round(rng5, 4),
            "reason": "late_no_consolidation",
        }
    else:
        # Move happened but now consolidating = maybe second leg (handled in continuation)
        return 0, {"move_pct": round(move_pct, 3), "consolidating": True}


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  PRE-BREAKOUT BIAS MODULE (NEW v3.1)
#
#  Mendeteksi micro-accumulation dalam 6 candle terakhir SEBELUM volume spike.
#  Fokus pada 4 sinyal yang konsisten terjadi 1-3 candle SEBELUM pump:
#    [1] ATR squeeze — volatility menyempit tajam dalam window pendek
#    [2] Volume creep — volume naik perlahan tanpa spike (smart money masuk diam)
#    [3] Wick rejection bawah — buyer mempertahankan harga di support
#    [4] Close bias atas — candle-candle terakhir tutup di bagian atas range
#
#  CRITICAL: Modul ini sengaja tidak bergantung pada compression zone.
#  Bisa detect sinyal bahkan jika compression belum teridentifikasi.
#  Max bonus: +12 pts
# ══════════════════════════════════════════════════════════════════════════════
def score_pre_breakout_bias(candles):
    """
    Deteksi micro-accumulation pattern dalam window pendek (6 candle).
    Return (score 0-12, details_dict)
    """
    win  = CONFIG.get("pbb_window", 6)
    if len(candles) < win + 15:   # butuh context untuk ATR long
        return 0, {}

    recent = candles[-win:]
    score  = 0
    flags  = []

    # ── [1] ATR SQUEEZE — volatility collapse in short window ─────────────────
    atr_short = calc_atr(candles[-(win + 2):], win)
    atr_long  = calc_atr(candles[-32:], 30)
    squeeze_ratio = CONFIG.get("pbb_squeeze_atr_ratio", 0.60)

    if atr_short and atr_long and atr_long > 0:
        ratio = atr_short / atr_long
        if ratio < squeeze_ratio:
            score += 4
            flags.append(f"squeeze({ratio:.2f})")
        elif ratio < squeeze_ratio * 1.2:
            score += 2
            flags.append(f"mild_squeeze({ratio:.2f})")

    # ── [2] VOLUME CREEP — slow silent increase, no spike ─────────────────────
    vols     = [c["volume_usd"] for c in recent]
    half     = max(1, win // 2)
    avg_prev = _mean(vols[:half])
    avg_last = _mean(vols[half:])
    creep_ratio = CONFIG.get("pbb_vol_creep_ratio", 0.15)

    if avg_prev > 0:
        vol_change = (avg_last - avg_prev) / avg_prev
        vol_max    = max(vols)
        vol_median = _median(vols)
        no_spike   = vol_median > 0 and vol_max < vol_median * 2.5   # no single spike

        if vol_change > creep_ratio and no_spike:
            score += 4
            flags.append(f"vol_creep(+{vol_change*100:.0f}%)")
        elif vol_change > 0.05 and no_spike:
            score += 2
            flags.append(f"vol_uptick(+{vol_change*100:.0f}%)")

    # ── [3] WICK REJECTION — consistent lower wick buying pressure ─────────────
    wick_min   = CONFIG.get("pbb_wick_reject_min", 0.40)
    wick_count = 0
    for c in recent:
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        lower_wick = (min(c["open"], c["close"]) - c["low"]) / rng
        if lower_wick > wick_min:
            wick_count += 1

    if wick_count >= win * 0.5:      # ≥50% candle punya rejection wick bawah
        score += 3
        flags.append(f"wick_reject({wick_count}/{win})")
    elif wick_count >= win * 0.33:
        score += 1
        flags.append(f"wick_reject({wick_count}/{win})")

    # ── [4] CLOSE BIAS TOP — candles consistently closing in upper half ────────
    top_close_thresh = CONFIG.get("pbb_close_top_half", 0.55)
    top_count = 0
    for c in recent:
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        close_pos = (c["close"] - c["low"]) / rng
        if close_pos > top_close_thresh:
            top_count += 1

    if top_count >= win * 0.67:      # ≥67% candle tutup di bagian atas
        score += 3
        flags.append(f"close_bias_top({top_count}/{win})")
    elif top_count >= win * 0.50:
        score += 1
        flags.append(f"close_bias_top({top_count}/{win})")

    score = min(score, 12)
    return score, {"pbb_score": score, "flags": flags, "window": win}


# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE v3.1
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    """
    v3.1 — Aggressive early detection.
    Scores every coin probabilistically. NO hard skips on signals.
    Prioritizes pre-breakout conditions over confirmed ones.
    """
    # ── Ambil candle 1H ──────────────────────────────────────────────────────
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

    # ── SAFETY GATE 1: Volume 24H ─────────────────────────────────────────────
    # This is a liquidity gate, not a signal gate
    if vol_24h < CONFIG["min_vol_24h"]:
        return None
    if vol_24h > CONFIG["max_vol_24h"]:
        return None

    # ── SAFETY GATE 2: Already pumped 50%+ today ─────────────────────────────
    if chg_24h > CONFIG["gate_chg_24h_max"]:
        log.info(f"  {symbol}: SKIP chg_24h={chg_24h:.1f}% — already pumped (safety gate)")
        return None

    # ── SAFETY GATE 3: Funding rate (risk management) ─────────────────────────
    funding = get_funding(symbol)
    if funding < CONFIG["funding_gate"]:
        log.info(f"  {symbol}: SKIP funding={funding:.5f} < gate={CONFIG['funding_gate']}")
        return None

    price_now = c1h[-1]["close"]

    # ── Cari compression zone ─────────────────────────────────────────────────
    compression = find_compression_zone(c1h)

    # ── [A] COMPRESSION + TENSION SCORE (HIGH PRIORITY, max 35) ──────────────
    comp_tension_score, comp_tension_details = score_compression_tension(c1h, compression)

    # ── [B] VOLUME INTELLIGENCE (MEDIUM, max 25) ──────────────────────────────
    vol_score, vol_details = score_volume_intelligence(c1h)
    rvol = vol_details.get("rvol", 1.0)

    # ── [C] STRUCTURE (MEDIUM, max 20) ───────────────────────────────────────
    struct_score, struct_details = score_structure(c1h, compression)

    # ── [D] CONTINUATION (MEDIUM, max 15) ─────────────────────────────────────
    cont_score, cont_details = score_continuation(c1h)

    # ── [E] STEALTH ACCUMULATION (BONUS, max 15) ──────────────────────────────
    stealth      = detect_stealth_accumulation(c1h)
    stealth_score_bonus = 0
    if stealth["detected"]:
        st = stealth["score"]
        # Interpolate: 70 → +10, 100 → +15
        t    = CONFIG["stealth_score_threshold"]
        ratio = max(0.0, min(1.0, (st - t) / (100.0 - t))) if (100.0 - t) > 0 else 1.0
        stealth_score_bonus = round(10 + ratio * 5)
    elif stealth["score"] >= 55:
        stealth_score_bonus = 5   # partial stealth = small bonus

    # ── [F] PRE-BREAKOUT BIAS (BONUS, max 12) — NEW v3.1 ─────────────────────
    pbb_score, pbb_details = score_pre_breakout_bias(c1h)

    # ── [G] DISTRIBUTION PENALTY (capped at 20 in v3.1) ──────────────────────
    dist_penalty, dist_details = calc_distribution_penalty(c1h)

    # ── [H] SLOW TREND PENALTY (capped at 10 in v3.1) ───────────────────────
    slow_penalty, slow_details = calc_slow_trend_penalty(c1h, compression)

    # ── [I] ANTI-LATE-ENTRY PENALTY (capped at 15 in v3.1) ───────────────────
    late_penalty, late_details = calc_late_entry_penalty(c1h)

    # ── RSI (v3.2: reduced to 1pt max — informational only, weak predictor) ───
    rsi       = get_rsi(c1h[-50:], 14)
    rsi_bonus = 1 if rsi < 35 else 0   # v3.2: was 3/2/1, now max 1

    # ── Liquidity sweep bonus ─────────────────────────────────────────────────
    comp_low    = compression["low"] if compression else price_now * 0.95
    liq_sweep   = detect_liquidity_sweep(c1h, comp_low)
    liq_bonus   = 8 if liq_sweep else 0

    # ── FINAL SCORE ───────────────────────────────────────────────────────────
    raw_score = (
        comp_tension_score   # +40 max (v3.2: raised from 35)
        + vol_score          # +30 max (v3.2: raised from 25, RVOL now included)
        + struct_score       # +20 max (failed_breakdown 18pts is the key driver)
        + cont_score         # +15 max
        + stealth_score_bonus# +15 max bonus
        + pbb_score          # +12 max bonus
        + rsi_bonus          # +1 max (v3.2: reduced from 3)
        + liq_bonus          # +8 max
        - dist_penalty       # -25 max (v3.2: two-track, HVNP is dominant)
        - slow_penalty       # -10 max
        - late_penalty       # -15 max
    )

    # Funding penalty
    if funding < -0.001:
        raw_score -= 5

    score = max(0, min(100, raw_score))

    # ── Confidence band ───────────────────────────────────────────────────────
    if score < CONFIG["score_min_output"]:
        confidence = "ignore"
    elif score < CONFIG["score_target_low"]:
        confidence = "early"       # v3.1: 42–60 = early signal (was "weak")
    elif score <= CONFIG["score_target_high"]:
        confidence = "strong"      # 60–78 = strong pre-pump (zona ideal)
    else:
        confidence = "possibly_late"

    # ── Score breakdown ────────────────────────────────────────────────────────
    score_breakdown = [
        f"Compression+Tension: +{comp_tension_score} ({comp_tension_details.get('comp_length','?')}h, range={comp_tension_details.get('range_pct',0)*100:.1f}%, tension={comp_tension_details.get('tension_score',0)})",
        f"Volume: +{vol_score} ({vol_details.get('label','')})",
        f"Structure: {struct_score:+d} ({', '.join(struct_details.get('breakdown',[]))[:50]})",
        f"Continuation: +{cont_score} ({cont_details.get('pullback_quality', 'no_prior_move')})" if cont_score else "Continuation: 0",
        f"PreBreakout: +{pbb_score} ({', '.join(pbb_details.get('flags',[])[:3])})" if pbb_score else "PreBreakout: 0",
        f"Stealth: +{stealth_score_bonus} (stealth_score={stealth['score']:.0f})" if stealth_score_bonus else "Stealth: 0",
        f"RSI: +{rsi_bonus} (RSI={rsi:.0f})",
        f"LiqSweep: +{liq_bonus}" if liq_bonus else "",
        f"DistPenalty: -{dist_penalty} ({', '.join(dist_details.get('flags',[])[:3])})" if dist_penalty else "",
        f"SlowTrend: -{slow_penalty}" if slow_penalty else "",
        f"LateEntry: -{late_penalty} ({late_details.get('reason','')})" if late_penalty else "",
    ]
    score_breakdown = [s for s in score_breakdown if s]

    log.info(f"  {symbol}: Score={score} ({confidence}) | CT={comp_tension_score} V={vol_score} S={struct_score} C={cont_score} PBB={pbb_score} St={stealth_score_bonus} | D=-{dist_penalty} Sl=-{slow_penalty} La=-{late_penalty}")

    # ── Filter output ─────────────────────────────────────────────────────────
    if score < CONFIG["score_min_output"]:
        return None

    # ── Entry & targets ────────────────────────────────────────────────────────
    comp_zone_for_entry = compression if compression else {
        "low": price_now * 0.95,
        "high": price_now,
        "length": 0,
        "avg_vol": 0,
    }
    entry_data = calc_entry_targets(c1h, comp_zone_for_entry)
    if not entry_data:
        return None
    if entry_data["t1_pct"] < CONFIG["min_target_pct"]:
        log.info(f"  {symbol}: T1={entry_data['t1_pct']:.1f}% < min — still included at lower score")
        score = max(0, score - 10)
    if entry_data["rr"] < CONFIG["min_rr"]:
        score = max(0, score - 8)

    # ── Urgency label ──────────────────────────────────────────────────────────
    vol_ratio = vol_details.get("vol_ratio", 1.0)
    comp_len  = compression["length"] if compression else 0
    if vol_ratio >= 4.0 and comp_len >= 168:
        urgency = "🔴 SANGAT TINGGI — mega spike + coil panjang, pump bisa dalam 1-3 jam"
    elif vol_ratio >= 2.5 or comp_len >= 168:
        urgency = "🟠 TINGGI — potensi pump dalam 6-24 jam"
    elif vol_ratio >= 1.2 and comp_len >= 72:
        urgency = "🟡 SEDANG — potensi pump dalam 12-48 jam"
    elif stealth["detected"]:
        urgency = "🔵 STEALTH — fase akumulasi diam-diam, breakout belum terjadi"
    else:
        urgency = "⚪ WATCH — sinyal awal, perlu konfirmasi"

    rise_from_low = 0.0
    if compression:
        rise_from_low = (price_now - compression["low"]) / compression["low"] if compression["low"] > 0 else 0

    return {
        "symbol":          symbol,
        "score":           score,
        "confidence":      confidence,
        "compression":     compression,
        "vol_details":     vol_details,
        "struct_details":  struct_details,
        "cont_details":    cont_details,
        "stealth":         stealth,
        "entry":           entry_data,
        "liq_sweep":       liq_sweep,
        "rsi":             rsi,
        "funding":         funding,
        "rvol":            round(rvol, 1),
        "price":           price_now,
        "chg_24h":         chg_24h,
        "vol_24h":         vol_24h,
        "rise_from_low":   rise_from_low,
        "sector":          SECTOR_LOOKUP.get(symbol, "OTHER"),
        "urgency":         urgency,
        "score_breakdown": score_breakdown,
        # Penalty details
        "dist_penalty":    dist_penalty,
        "slow_penalty":    slow_penalty,
        "late_penalty":    late_penalty,
        # Sub-scores for transparency
        "comp_tension_score": comp_tension_score,
        "vol_score":          vol_score,
        "struct_score":       struct_score,
        "cont_score":         cont_score,
        "pbb_score":           pbb_score,
        "pbb_details":         pbb_details,
        "stealth_bonus":      stealth_score_bonus,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER v3.0
# ══════════════════════════════════════════════════════════════════════════════
def _confidence_emoji(conf):
    return {"ignore": "⚫", "early": "🟡", "strong": "🟢", "possibly_late": "🟠"}.get(conf, "⚪")

def build_alert(r, rank=None):
    sc   = r["score"]
    conf = r["confidence"]
    bar  = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
    e    = r["entry"]
    comp = r["compression"]
    rk   = f"#{rank} " if rank else ""
    vol  = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
            else f"${r['vol_24h']/1e3:.0f}K")

    comp_str = "N/A"
    if comp:
        comp_days = comp["length"] / 24
        comp_str  = f"{comp_days:.0f} hari" if comp_days >= 1 else f"{comp['length']} jam"

    # Sub-scores summary
    sub = (
        f"  CT:{r['comp_tension_score']} V:{r['vol_score']} "
        f"S:{r['struct_score']:+d} C:{r['cont_score']} "
        f"PBB:{r.get('pbb_score',0)} St:{r['stealth_bonus']} | "
        f"D:-{r['dist_penalty']} Sl:-{r['slow_penalty']} La:-{r['late_penalty']}"
    )

    # Stealth section
    stealth     = r.get("stealth", {})
    stealth_str = ""
    if stealth.get("detected"):
        ss = stealth["score"]
        stealth_str = (
            f"\n🟦 <b>STEALTH MODE</b> (Blue Box)\n"
            f"  Score:{ss:.0f}  ATR:{stealth.get('atr_trend','?')}  "
            f"CV:{stealth.get('volume_stability',0):.2f}  "
            f"Struct:{stealth.get('structure_quality','?')}\n"
            f"  Trigger: {'✅ BREAKOUT' if stealth.get('entry_trigger') else '⏳ waiting'}\n"
        )
    elif stealth.get("score", 0) >= 55:
        stealth_str = f"\n🔷 <b>Stealth Watch</b> ({stealth.get('score',0):.0f}/100)\n"

    # Forensic
    forensic_items = []
    sd = r.get("struct_details", {})
    if sd.get("higher_lows"):   forensic_items.append("Higher lows ✅")
    if r.get("liq_sweep"):      forensic_items.append("Liq sweep ✅")
    pbb_flags = r.get("pbb_details", {}).get("flags", [])
    if pbb_flags:
        forensic_items.append(f"PreBreakout: {', '.join(pbb_flags[:3])} 🔵")
    cd = r.get("cont_details", {})
    if r.get("cont_score", 0) > 0:
        forensic_items.append(f"Continuation ✅ ({cd.get('pullback_quality','?')})")
    forensic_str = "  " + " · ".join(forensic_items) if forensic_items else "  — sinyal masih awal"

    # Distribution warning
    dist_warn = ""
    df = r.get("dist_details", {})
    if r.get("dist_penalty", 0) >= 20:
        dist_str = ", ".join(r.get("dist_details", {}).get("flags", ["?"])[:3])
        dist_warn = f"\n⚠️ <b>DISTRIBUSI TERDETEKSI:</b> {dist_str}\n"

    msg = (
        f"🚀 <b>PRE-PUMP SIGNAL {rk}— v3.0</b>\n\n"
        f"<b>Symbol    :</b> {r['symbol']} [{r['sector']}]\n"
        f"<b>Skor      :</b> {sc}/100  {_confidence_emoji(conf)} {conf.upper()}\n"
        f"<b>Bar       :</b> {bar}\n"
        f"<b>Urgency   :</b> {r['urgency']}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>SUB-SCORES</b>\n"
        f"{sub}\n"
        f"  CT=Compression+Tension  V=Volume  S=Structure\n"
        f"  C=Continuation  PBB=Pre-Breakout  St=Stealth\n"
        f"  D=Dist  Sl=Slow  La=Late\n"
        f"\n📦 <b>COMPRESSION ZONE</b>\n"
        f"  Durasi : {comp_str}\n"
    )
    if comp:
        msg += (
            f"  Range  : {comp['range_pct']*100:.1f}% "
            f"(${comp['low']:.6g}–${comp['high']:.6g})\n"
            f"  Harga  : ${r['price']:.6g} (+{r['rise_from_low']*100:.1f}% dari low)\n"
        )

    vd = r.get("vol_details", {})
    msg += (
        f"\n⚡ <b>VOLUME</b>\n"
        f"  Ratio  : {vd.get('vol_ratio',0):.2f}x avg20  |  RVOL: {r['rvol']:.1f}x\n"
        f"  Acc    : {vd.get('vol_acceleration',0):+.2f}  |  Vol24H: {vol}\n"
        f"\n📊 <b>TEKNIKAL</b>\n"
        f"  RSI    : {r['rsi']:.0f}  |  Chg24H: {r['chg_24h']:+.1f}%\n"
        f"  Candle : {sd.get('candle_label','?')}\n"
        f"  Funding: {r['funding']:.5f}\n"
        f"\n🔬 <b>SINYAL AKUMULASI</b>\n"
        f"{forensic_str}\n"
        f"{stealth_str}"
        f"{dist_warn}"
    )

    if e:
        poc_line  = f"  POC   : ${e['z2']}\n" if e.get("z2") else ""
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>ENTRY &amp; TARGET</b>\n"
            f"{poc_line}"
            f"  Entry : ${e['entry']}\n"
            f"  SL    : ${e['sl']}  (-{e['sl_pct']:.1f}%)\n"
            f"  T1    : ${e['t1']}  (+{e['t1_pct']:.1f}%)\n"
            f"  T2    : ${e['t2']}  (+{e['t2_pct']:.1f}%)\n"
            f"  R/R   : 1:{e['rr']}  |  ATR: ${e['atr']}\n"
        )

    # Score breakdown (collapsed)
    msg += (
        f"\n🔢 <b>BREAKDOWN:</b>\n"
        + "\n".join(f"  {s}" for s in r["score_breakdown"][:6])
        + f"\n\n🕐 {utc_now()}\n<i>⚠️ Bukan financial advice. DYOR.</i>"
    )
    return msg


def build_summary(results):
    msg  = f"📋 <b>PRE-PUMP WATCHLIST v3.0 — {utc_now()}</b>\n{'━'*30}\n"
    for i, r in enumerate(results, 1):
        comp  = r["compression"]
        vol   = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                 else f"${r['vol_24h']/1e3:.0f}K")
        comp_str = f"{comp['length']}h" if comp else "?"
        conf_emoji = _confidence_emoji(r["confidence"])
        vd = r.get("vol_details", {})
        msg += (
            f"{i}. <b>{r['symbol']}</b> [S:{r['score']} {conf_emoji}]\n"
            f"   Coil:{comp_str} · Vol:{vd.get('vol_ratio',0):.1f}x · "
            f"T1:+{r['entry']['t1_pct']:.0f}% · {vol}\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    candidates = []
    not_found  = []
    stats      = defaultdict(int)

    log.info("=" * 70)
    log.info(f"🔍 SCANNING {len(WHITELIST_SYMBOLS)} coin — PRE-PUMP v3.0 (PROBABILISTIC)")
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

        # Only hard pre-filter: noise coins and extreme already-moved coins
        if vol < CONFIG["pre_filter_vol"]:
            stats["vol_too_low"] += 1
            continue
        if vol > CONFIG["max_vol_24h"]:
            stats["vol_too_high"] += 1
            continue
        if chg > CONFIG["gate_chg_24h_max"]:
            stats["change_extreme"] += 1
            continue
        if price <= 0:
            stats["invalid_price"] += 1
            continue

        candidates.append((sym, t))

    log.info(f"\n📊 Pre-filter: {len(candidates)}/{len(WHITELIST_SYMBOLS)} coin akan di-scan")
    log.info(f"   Cooldown: {stats['cooldown']} | Vol rendah: {stats['vol_too_low']} | "
             f"Vol tinggi: {stats['vol_too_high']} | Chg ekstrem: {stats['change_extreme']}")
    if not_found:
        log.info(f"   Tidak di Bitget: {len(not_found)} coin")
    log.info(f"   ⏱️  Est. waktu: ~{len(candidates) * CONFIG['sleep_coins'] / 60:.1f} menit")
    log.info("=" * 70)

    return candidates


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v3.0 — {utc_now()} ===")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return

    log.info(f"Total ticker Bitget: {len(tickers)}")

    candidates = build_candidate_list(tickers)
    all_results = []   # semua coin yang punya score >= min_output (termasuk weak)

    for i, (sym, t) in enumerate(candidates):
        log.info(f"[{i+1}/{len(candidates)}] {sym} ...")
        try:
            res = master_score(sym, t)
            if res:
                all_results.append(res)
                log.info(
                    f"  ✅ SCORED! Score={res['score']} ({res['confidence']}) "
                    f"CT={res['comp_tension_score']} V={res['vol_score']} "
                    f"S={res['struct_score']} C={res['cont_score']}"
                )
        except Exception as ex:
            log.warning(f"  Error {sym}: {ex}", exc_info=True)

        time.sleep(CONFIG["sleep_coins"])

    # Sort by score descending
    all_results.sort(key=lambda x: x["score"], reverse=True)

    log.info(f"\n{'='*70}")
    log.info(f"✅ Total coin scored: {len(all_results)}")
    conf_counts = defaultdict(int)
    for r in all_results:
        conf_counts[r["confidence"]] += 1
    log.info(f"   Strong: {conf_counts['strong']} | Early: {conf_counts['early']} | Late: {conf_counts['possibly_late']}")
    log.info(f"{'='*70}\n")

    if not all_results:
        log.info("Tidak ada sinyal pre-pump saat ini")
        return

    # Prioritize 'strong' (60-78) first, then 'early' (42-60), then others
    strong   = [r for r in all_results if r["confidence"] == "strong"]
    early    = [r for r in all_results if r["confidence"] == "early"]
    others   = [r for r in all_results if r["confidence"] not in ("strong", "early")]
    top_list = (strong + early + others)[:CONFIG["max_alerts_per_run"]]

    if len(top_list) >= 2:
        send_telegram(build_summary(top_list))
        time.sleep(2)

    for rank, r in enumerate(top_list, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"📤 Alert #{rank}: {r['symbol']} Score={r['score']} ({r['confidence']})")
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top_list)} alert dikirim — {utc_now()} ===")


# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔════════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v3.2 — OPTIMIZED WEIGHTS        ║")
    log.info("║  CT:40 Vol+RVOL:30 Struct:20 | Dist(HVNP):25     ║")
    log.info("║  RSI demoted · Failed breakdown promoted          ║")
    log.info("╚════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)

    run_scan()
