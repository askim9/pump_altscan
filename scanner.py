"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  ALTCOIN PUMP SCANNER v31 — STRUCTURAL ACCUMULATION ENGINE                 ║
║                                                                              ║
║  REBUILT FROM SCRATCH — Target: Detect 20–70% pumps BEFORE ignition        ║
║                                                                              ║
║  CORE PROBLEMS FIXED vs v30:                                                 ║
║    [FIX-1] Momentum counted multiple times → ONE score per category        ║
║    [FIX-2] Volume spike dominated scoring → volume is supporting signal    ║
║    [FIX-3] Accumulation weight was tiny → Phase 2 now 30% of total         ║
║    [FIX-4] No accumulation vs distribution distinction → CVD + OI filter   ║
║    [FIX-5] Small 1-3% pump detection → filter: reject RSI>72, price>10%   ║
║    [FIX-6] Late detection → compression + OI buildup scored FIRST          ║
║    [FIX-7] Volatility compression ignored → Phase 1 BB/ATR squeeze added  ║
║    [FIX-8] Liquidity vacuum ignored → detect thin ask-side supply          ║
║                                                                              ║
║  4-PHASE SCORING MODEL:                                                      ║
║    Phase 1 — Compression Score   : 20%  (BB squeeze + ATR contraction)    ║
║    Phase 2 — Accumulation Score  : 30%  (OI + volume + CVD + price flat)  ║
║    Phase 3 — Position Build Score: 25%  (OI accel + liq vacuum + squeeze) ║
║    Phase 4 — Ignition Score      : 25%  (breakout pressure + momentum)    ║
║                                                                              ║
║  TOTAL PUMP SCORE = 100 (weighted blend of 4 phases)                        ║
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

# ── Persistent HTTP session (connection reuse) ────────────────────────────────
_http_session = requests.Session()
_http_session.headers.update({
    "User-Agent": "CryptoScanner/31.0",
    "Accept-Encoding": "gzip",
})

# ── .env loader ───────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

# ── Logging setup ─────────────────────────────────────────────────────────────
_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)

_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)

_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v31.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Scanner v31 — Structural Accumulation Engine aktif")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── API & Run Settings ────────────────────────────────────────────────────
    "max_alerts_per_run":       12,
    "alert_cooldown_sec":       1800,     # 30 menit cooldown per coin
    "sleep_coins":              0.15,     # delay antar coin (detik)
    "sleep_error":              3.0,

    # ── Pre-filter (cepat, sebelum API call berat) ────────────────────────────
    "min_vol_24h_usd":          500_000,   # minimal $500K volume 24h
    "max_vol_24h_usd":          200_000_000, # skip mega-cap outlier
    "min_oi_usd":               200_000,   # minimal $200K Open Interest

    # ── Hard Reject Gates (WAJIB — tidak boleh lolos) ─────────────────────────
    # FIX-5: reject harga naik >10% dalam 12 jam = sudah pump
    "gate_already_pumped_pct":  10.0,
    # FIX-5: reject RSI overbought = distribusi
    "gate_rsi_max":             72.0,
    # FIX-4: reject jika harga sudah di atas 95% range 48h = zona distribusi
    "gate_price_pos_max":       0.95,
    # Reject drop besar (dump)
    "gate_dump_pct":            -20.0,

    # ── Candle limits ─────────────────────────────────────────────────────────
    "candles_1h":               210,      # untuk EMA200 butuh 200+
    "candles_4h":               60,
    "candles_15m":              80,
    "candles_5m":               50,

    # ══════════════════════════════════════════════════════════════════════════
    #  PHASE 1 — COMPRESSION (bobot 20%)
    #  Deteksi: volatilitas menyempit sebelum ekspansi
    # ══════════════════════════════════════════════════════════════════════════

    # BB Width Squeeze: BBW < threshold = energi terkompresi
    "p1_bbw_tight":             0.04,     # BBW < 4% = squeeze
    "p1_bbw_extreme":           0.025,    # BBW < 2.5% = extreme squeeze
    "p1_score_bbw_tight":       8,        # poin jika BBW < 4%
    "p1_score_bbw_extreme":     15,       # poin jika BBW < 2.5%

    # ATR Contraction: ATR14 / ATR100 < threshold = kontraksi
    "p1_atr_contract_ratio":    0.70,     # ATR14 < 70% dari ATR100
    "p1_atr_strong_ratio":      0.50,     # ATR14 < 50% dari ATR100
    "p1_score_atr_contract":    8,
    "p1_score_atr_strong":      15,

    # Range Compression: current range / avg_range_48 < threshold
    "p1_range_compress_ratio":  0.60,     # range saat ini < 60% rata-rata
    "p1_score_range_compress":  7,

    # Compression phase cap (Phase 1 max 20 poin dari 100)
    "p1_max_score":             20,

    # ══════════════════════════════════════════════════════════════════════════
    #  PHASE 2 — ACCUMULATION (bobot 30%)
    #  Deteksi: volume naik + harga stabil + OI naik = akumulasi
    #  KUNCI: membedakan akumulasi vs distribusi
    # ══════════════════════════════════════════════════════════════════════════

    # Volume Accumulation: volume 24h / avg volume 7d
    "p2_vol_accum_mild":        1.3,      # vol 30% di atas rata2
    "p2_vol_accum_strong":      1.8,      # vol 80% di atas rata2
    "p2_vol_accum_extreme":     2.5,      # vol 2.5x di atas rata2
    "p2_score_vol_mild":        4,
    "p2_score_vol_strong":      8,
    "p2_score_vol_extreme":     12,

    # Price Stability: harga tidak bergerak banyak = akumulasi murni
    # FIX-4: jika OI naik tapi harga diam = position build (bukan distribusi)
    "p2_price_stable_pct":      2.0,      # abs(price_change_24h) < 2% = stabil
    "p2_price_ok_pct":          5.0,      # abs(price_change_24h) < 5% = oke
    "p2_score_price_stable":    8,        # bonus besar jika harga flat + volume naik
    "p2_score_price_ok":        3,

    # OI Expansion dalam konteks akumulasi
    # OI naik + harga flat = position build (BULLISH)
    # OI naik + harga naik = breakout sedang berlangsung
    # OI turun + harga naik = SHORT COVER (bisa pump tapi risiko tinggi)
    "p2_oi_expand_min":         3.0,      # OI naik minimal 3%
    "p2_oi_expand_strong":      8.0,      # OI naik 8%+ = kuat
    "p2_score_oi_expand":       6,
    "p2_score_oi_strong":       10,

    # CVD (Cumulative Volume Delta) Divergence
    # Harga flat/turun + CVD naik = hidden buying = ACCUMULATION SIGNAL
    # FIX-4: ini yang membedakan akumulasi vs distribusi
    "p2_cvd_lookback":          12,       # 12 candle untuk CVD
    "p2_cvd_price_flat_max":    1.5,      # harga bergerak < 1.5%
    "p2_cvd_ratio_min":         1.4,      # buy volume / sell volume >= 1.4
    "p2_score_cvd_divergence":  10,       # sinyal kuat akumulasi tersembunyi

    # Accumulation phase cap (Phase 2 max 30 poin)
    "p2_max_score":             30,

    # ══════════════════════════════════════════════════════════════════════════
    #  PHASE 3 — POSITION BUILD-UP (bobot 25%)
    #  Deteksi: market maker sedang build posisi sebelum pump
    # ══════════════════════════════════════════════════════════════════════════

    # Position Build Score: OI expansion × volume accumulation
    # Formula: oi_expansion * vol_accum (dari prompt)
    "p3_position_score_min":    2.0,      # oi_exp * vol_accum >= 2.0
    "p3_position_score_strong": 4.0,      # oi_exp * vol_accum >= 4.0
    "p3_score_pos_build":       8,
    "p3_score_pos_strong":      14,

    # Liquidity Vacuum: vol naik + range compression = liquidity trap sebelum ekspansi
    # Formula: vol_accum / range_compression (dari prompt)
    "p3_liq_vacuum_min":        2.5,      # ratio minimal
    "p3_liq_vacuum_strong":     5.0,      # ratio kuat
    "p3_score_liq_vacuum":      6,
    "p3_score_liq_strong":      11,

    # Short Squeeze Potential: OI expansion / price_stability
    # OI naik besar + harga bergerak kecil = squeeze setup
    "p3_squeeze_min":           2.0,      # oi_expand% / abs_price_change >= 2
    "p3_squeeze_strong":        5.0,      # squeeze sangat kuat
    "p3_score_squeeze":         5,
    "p3_score_squeeze_strong":  9,

    # Energy Buildup: position_score × liquidity_vacuum
    "p3_energy_min":            4.0,
    "p3_energy_strong":         10.0,
    "p3_score_energy":          5,
    "p3_score_energy_strong":   10,

    # Funding Rate negatif = short-side dominan = squeeze kandidat kuat
    "p3_funding_neg_threshold": -0.0001,
    "p3_score_funding_neg":     5,

    # Position build-up phase cap
    "p3_max_score":             25,

    # ══════════════════════════════════════════════════════════════════════════
    #  PHASE 4 — IGNITION PREPARATION (bobot 25%)
    #  Deteksi: sinyal awal ignition — BUKAN setelah pump dimulai
    # ══════════════════════════════════════════════════════════════════════════

    # Breakout Pressure: vol 1h / avg vol 24h
    # Meningkat tapi belum meledak = ignition sedang dimulai
    "p4_breakout_pressure_mild":   1.5,   # vol 1h = 1.5x rata-rata 24h
    "p4_breakout_pressure_strong": 2.5,   # vol 1h = 2.5x rata-rata 24h
    "p4_score_bp_mild":            6,
    "p4_score_bp_strong":          12,

    # Momentum Buildup: harga mulai naik tapi belum parabolic
    # Kita INGIN sedikit momentum untuk konfirmasi, tapi BUKAN sudah pump besar
    "p4_momentum_min_pct":         0.5,   # naik minimal 0.5% dari 3h lalu
    "p4_momentum_max_pct":         5.0,   # belum naik >5% (masih awal)
    "p4_score_momentum_ok":        5,
    "p4_score_momentum_strong":    10,    # jika 1-3% — perfect zone

    # Higher Low Structure: tanda akhir akumulasi
    "p4_higher_low_lookback":      16,
    "p4_score_higher_low":         4,

    # BOS (Break of Structure): breakout dari range akumulasi
    "p4_bos_lookback":             8,
    "p4_score_bos":                4,

    # Ignition phase cap
    "p4_max_score":                25,

    # ══════════════════════════════════════════════════════════════════════════
    #  PUMP PROBABILITY MODEL
    # ══════════════════════════════════════════════════════════════════════════

    # Total pump score → probability via logistic function
    "prob_center":              55,       # score 55 → 50% probability
    "prob_scale":               10,       # steepness

    # Minimum score untuk masuk watchlist
    "min_score_watchlist":      45,

    # TP/SL multipliers (ATR-based)
    "tp1_atr_mult":             2.0,      # TP1 = entry + 2×ATR
    "tp2_atr_mult":             4.0,      # TP2 = entry + 4×ATR
    "tp3_atr_mult":             7.0,      # TP3 = entry + 7×ATR (15-50% target)
    "sl_atr_mult":              2.0,      # SL  = entry - 2×ATR

    # ── Storage files ─────────────────────────────────────────────────────────
    "cooldown_file":            "./cooldown_v31.json",
    "oi_snapshot_file":         "./oi_snapshot_v31.json",
    "funding_snapshot_file":    "./funding_v31.json",
}

# ── Whitelist altcoins yang dipantau ──────────────────────────────────────────
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
"ORCLUSDT",
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
"TSLAUSDT",
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

# Keyword yang selalu di-skip (stablecoin, BTC, ETH)
EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST"]

BITGET_BASE = "https://api.bitget.com"
GRAN_MAP    = {"5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
_cache      = {}  # API response cache (90 detik TTL)

# ══════════════════════════════════════════════════════════════════════════════
#  🔒  COOLDOWN SYSTEM
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
#  💾  OI SNAPSHOT (untuk hitung OI change antar run)
# ══════════════════════════════════════════════════════════════════════════════
_oi_snapshot = {}

def load_oi_snapshots():
    global _oi_snapshot
    try:
        p = CONFIG["oi_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            _oi_snapshot = {
                sym: v for sym, v in data.items()
                if now - v.get("ts", 0) < 7200   # buang data > 2 jam
            }
            log.info(f"OI snapshots loaded: {len(_oi_snapshot)} coins")
        else:
            _oi_snapshot = {}
    except Exception:
        _oi_snapshot = {}

def save_oi_snapshots():
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
            json.dump(_oi_snapshot, f)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
#  💾  FUNDING SNAPSHOTS (untuk hitung trend funding)
# ══════════════════════════════════════════════════════════════════════════════
_funding_snapshots = {}

def load_funding_snapshots():
    global _funding_snapshots
    try:
        p = CONFIG["funding_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                _funding_snapshots = json.load(f)
    except Exception:
        _funding_snapshots = {}

def save_funding_snapshots():
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(_funding_snapshots, f)
    except Exception:
        pass

def add_funding_snapshot(symbol, rate):
    if symbol not in _funding_snapshots:
        _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({"ts": time.time(), "funding": rate})
    if len(_funding_snapshots[symbol]) > 48:
        _funding_snapshots[symbol] = _funding_snapshots[symbol][-48:]

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=10):
    """HTTP GET dengan retry dan rate-limit handling."""
    for attempt in range(2):
        try:
            r = _http_session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                log.warning("Rate limit 429 — tunggu 15s")
                time.sleep(15)
                continue
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

def send_telegram(msg, parse_mode="HTML"):
    """Kirim pesan ke Telegram dengan fallback plain text."""
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("send_telegram: BOT_TOKEN atau CHAT_ID tidak ada!")
        return False
    if len(msg) > 4000:
        msg = msg[:3900] + "\n\n<i>...[dipotong]</i>"
    for attempt in range(2):
        try:
            payload = {"chat_id": CHAT_ID, "text": msg}
            if attempt == 0:
                payload["parse_mode"] = "HTML"
            r = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data=payload, timeout=15,
            )
            if r.status_code == 200:
                return True
            err_text = r.text[:300]
            if "can't parse" in err_text or "Bad Request" in err_text:
                # Fallback: hapus HTML tags
                msg = _html_mod.unescape(msg)
                for tag in ["<b>", "</b>", "<i>", "</i>", "<code>", "</code>"]:
                    msg = msg.replace(tag, "")
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
#  📡  DATA FETCHERS (API — tidak diubah dari v30)
# ══════════════════════════════════════════════════════════════════════════════
def get_all_tickers():
    """Ambil semua ticker Bitget USDT Futures."""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/tickers",
        params={"productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        return {t["symbol"]: t for t in data.get("data", [])}
    return {}

def get_candles(symbol, gran="1h", limit=210):
    """Ambil candle dengan cache 90 detik."""
    g   = GRAN_MAP.get(gran, "1H")
    key = f"c_{symbol}_{g}_{limit}"
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < 90:
            return val
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/candles",
        params={
            "symbol":      symbol,
            "granularity": g,
            "limit":       str(limit),
            "productType": "usdt-futures",
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

def get_funding_rate(symbol):
    """Ambil funding rate terkini."""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d_list = data.get("data") or []
            if d_list:
                rate = float(d_list[0].get("fundingRate", 0))
                add_funding_snapshot(symbol, rate)
                return rate
        except Exception:
            pass
    return 0.0

def get_open_interest(symbol):
    """Ambil Open Interest dari Bitget."""
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
                oi = float(oi_list[0].get("openInterest", 0)) if oi_list else 0.0
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
    Hitung % perubahan OI antar run menggunakan snapshot persisten.
    Return dict: {oi_now, oi_prev, change_pct, is_new}
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
#  📊  TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════════════════════════

def calc_ema(values, period):
    """Hitung EMA untuk series nilai."""
    if len(values) < period:
        return None
    alpha = 2.0 / (period + 1)
    ema   = sum(values[:period]) / period
    for v in values[period:]:
        ema = alpha * v + (1.0 - alpha) * ema
    return ema

def calc_rsi(candles, period=14):
    """RSI Wilder smoothing."""
    if len(candles) < period + 1:
        return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return round(100.0 - 100.0 / (1.0 + rs), 2)

def calc_bbw(candles, period=20):
    """
    Bollinger Band Width (desimal) dan posisi harga (0=bawah, 1=atas).
    Return: (bbw, bb_pct)
    """
    if len(candles) < period:
        return 0.04, 0.5
    closes   = [c["close"] for c in candles[-period:]]
    mean     = sum(closes) / period
    variance = sum((x - mean) ** 2 for x in closes) / period
    std      = math.sqrt(variance)
    bb_upper = mean + 2 * std
    bb_lower = mean - 2 * std
    bbw      = (bb_upper - bb_lower) / mean if mean > 0 else 0.04
    if bb_upper == bb_lower:
        bb_pct = 0.5
    else:
        bb_pct = (candles[-1]["close"] - bb_lower) / (bb_upper - bb_lower)
    return round(bbw, 5), round(bb_pct, 3)

def calc_atr(candles, period=14):
    """ATR absolut."""
    if len(candles) < period + 1:
        return candles[-1]["close"] * 0.01 if candles else 0.0
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1:
            break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else candles[-1]["close"] * 0.01

def calc_atr_n(candles, n):
    """ATR untuk n candle terakhir (helper)."""
    trs = []
    for i in range(1, min(n + 1, len(candles))):
        idx = len(candles) - i
        if idx < 1:
            break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else 0.0

def calc_vwap(candles, lookback=24):
    """VWAP rolling N candle."""
    n      = min(lookback, len(candles))
    recent = candles[-n:]
    cum_tv = sum((c["high"] + c["low"] + c["close"]) / 3 * c["volume"] for c in recent)
    cum_v  = sum(c["volume"] for c in recent)
    return (cum_tv / cum_v) if cum_v > 0 else candles[-1]["close"]

def calc_price_pos_48(candles):
    """
    Posisi harga saat ini dalam range 48 candle terakhir.
    0 = di bawah, 1 = di atas. > 0.95 = zona distribusi.
    """
    if len(candles) < 2:
        return 0.5
    recent = candles[-48:] if len(candles) >= 48 else candles
    hi     = max(c["high"] for c in recent)
    lo     = min(c["low"]  for c in recent)
    cur    = candles[-1]["close"]
    if hi == lo:
        return 0.5
    return round((cur - lo) / (hi - lo), 3)

def calc_avg_volume(candles, lookback=24):
    """Rata-rata volume N candle terakhir."""
    if not candles:
        return 0.0
    n      = min(lookback, len(candles))
    recent = candles[-n:]
    return sum(c["volume_usd"] for c in recent) / n

# ══════════════════════════════════════════════════════════════════════════════
#  📐  PHASE 1 — COMPRESSION ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
def analyze_phase1_compression(c1h, c4h):
    """
    Deteksi volatilitas compression sebagai setup pre-pump.

    Signals:
    1. BB Width Squeeze (1H) — energi terkompresi
    2. ATR Contraction — ATR14 vs ATR100
    3. Range Compression — current range vs avg range 48h

    Return: {score, signals, bbw, atr_ratio, range_ratio}
    """
    score   = 0
    signals = []

    # ── 1. BB Width Squeeze ───────────────────────────────────────────────────
    bbw, bb_pct = calc_bbw(c1h, 20)

    if bbw < CONFIG["p1_bbw_extreme"]:
        s = CONFIG["p1_score_bbw_extreme"]
        score += s
        signals.append(f"🗜️ BB Extreme Squeeze BBW={bbw*100:.2f}% (<{CONFIG['p1_bbw_extreme']*100:.1f}%) +{s}")
    elif bbw < CONFIG["p1_bbw_tight"]:
        s = CONFIG["p1_score_bbw_tight"]
        score += s
        signals.append(f"🗜️ BB Squeeze BBW={bbw*100:.2f}% (<{CONFIG['p1_bbw_tight']*100:.1f}%) +{s}")

    # ── 2. ATR Contraction: ATR14 / ATR100 ────────────────────────────────────
    atr14  = calc_atr_n(c1h, 14)
    atr100 = calc_atr_n(c1h, min(100, len(c1h)))
    atr_ratio = (atr14 / atr100) if atr100 > 0 else 1.0

    if atr_ratio < CONFIG["p1_atr_strong_ratio"]:
        s = CONFIG["p1_score_atr_strong"]
        score += s
        signals.append(f"📉 ATR Strong Contraction ratio={atr_ratio:.2f} (<{CONFIG['p1_atr_strong_ratio']}) +{s}")
    elif atr_ratio < CONFIG["p1_atr_contract_ratio"]:
        s = CONFIG["p1_score_atr_contract"]
        score += s
        signals.append(f"📉 ATR Contracting ratio={atr_ratio:.2f} (<{CONFIG['p1_atr_contract_ratio']}) +{s}")

    # ── 3. Range Compression: current_range / avg_range_48 ───────────────────
    range_ratio = 1.0
    if len(c1h) >= 10:
        # Range saat ini (4 candle terakhir)
        recent4    = c1h[-4:]
        cur_range  = max(c["high"] for c in recent4) - min(c["low"] for c in recent4)
        # Rata-rata range 48 candle
        lookback   = min(48, len(c1h))
        avg_ranges = []
        for i in range(lookback):
            c = c1h[-(i+1)]
            avg_ranges.append(c["high"] - c["low"])
        avg_range  = sum(avg_ranges) / len(avg_ranges) if avg_ranges else cur_range

        price_cur  = c1h[-1]["close"]
        # Normalisasi range sebagai % harga
        if price_cur > 0 and avg_range > 0:
            cur_range_pct = cur_range / price_cur
            avg_range_pct = avg_range / price_cur
            range_ratio   = cur_range_pct / avg_range_pct if avg_range_pct > 0 else 1.0

        if range_ratio < CONFIG["p1_range_compress_ratio"]:
            s = CONFIG["p1_score_range_compress"]
            score += s
            signals.append(f"📦 Range Compression ratio={range_ratio:.2f} (<{CONFIG['p1_range_compress_ratio']}) +{s}")

    # ── Cap Phase 1 di max ────────────────────────────────────────────────────
    score = min(score, CONFIG["p1_max_score"])

    return {
        "score":       score,
        "signals":     signals,
        "bbw":         bbw,
        "bb_pct":      bb_pct,
        "atr_ratio":   round(atr_ratio, 3),
        "range_ratio": round(range_ratio, 3),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📐  PHASE 2 — ACCUMULATION ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
def analyze_phase2_accumulation(c1h, c15m, oi_data, price_change_24h):
    """
    Deteksi akumulasi tersembunyi — membedakan AKUMULASI vs DISTRIBUSI.

    FIX-4: Kunci pembeda adalah CVD Divergence + OI + Volume pattern
    Akumulasi  = vol naik + harga flat + OI naik + CVD positif (hidden buying)
    Distribusi = vol naik + harga naik dengan cepat + CVD negatif (selling into strength)

    Return: {score, signals, vol_accum, is_cvd_divergence, is_oi_accum}
    """
    score   = 0
    signals = []

    # ── 1. Volume Accumulation: vol_24h / avg_vol_7d ─────────────────────────
    # Hitung dari candle: avg 168 candle vs 24 candle terakhir
    vol_accum = 1.0
    if len(c1h) >= 24:
        avg_vol_7d  = calc_avg_volume(c1h, min(168, len(c1h)))
        avg_vol_24h = calc_avg_volume(c1h, 24)
        vol_accum   = (avg_vol_24h / avg_vol_7d) if avg_vol_7d > 0 else 1.0

        if vol_accum >= CONFIG["p2_vol_accum_extreme"]:
            s = CONFIG["p2_score_vol_extreme"]
            score += s
            signals.append(f"📊 Vol Extreme Accum {vol_accum:.1f}x vs 7d avg +{s}")
        elif vol_accum >= CONFIG["p2_vol_accum_strong"]:
            s = CONFIG["p2_score_vol_strong"]
            score += s
            signals.append(f"📊 Vol Strong Accum {vol_accum:.1f}x vs 7d avg +{s}")
        elif vol_accum >= CONFIG["p2_vol_accum_mild"]:
            s = CONFIG["p2_score_vol_mild"]
            score += s
            signals.append(f"📊 Vol Accumulating {vol_accum:.1f}x vs 7d avg +{s}")

    # ── 2. Price Stability (OI naik + harga flat = position build) ────────────
    abs_chg = abs(price_change_24h)

    if abs_chg < CONFIG["p2_price_stable_pct"]:
        s = CONFIG["p2_score_price_stable"]
        score += s
        signals.append(f"⚖️ Price Stable |{price_change_24h:+.1f}%| (flat = akumulasi murni) +{s}")
    elif abs_chg < CONFIG["p2_price_ok_pct"]:
        s = CONFIG["p2_score_price_ok"]
        score += s
        signals.append(f"⚖️ Price OK |{price_change_24h:+.1f}%| (masih dalam range) +{s}")

    # ── 3. OI Expansion dalam konteks akumulasi ───────────────────────────────
    oi_change   = oi_data.get("change_pct", 0.0)
    is_oi_accum = False

    if not oi_data.get("is_new") and oi_change > 0:
        if oi_change >= CONFIG["p2_oi_expand_strong"]:
            s = CONFIG["p2_score_oi_strong"]
            score += s
            is_oi_accum = True
            signals.append(f"📈 OI Strong Expansion +{oi_change:.1f}% (posisi besar dibangun) +{s}")
        elif oi_change >= CONFIG["p2_oi_expand_min"]:
            s = CONFIG["p2_score_oi_expand"]
            score += s
            is_oi_accum = True
            signals.append(f"📈 OI Expanding +{oi_change:.1f}% (akumulasi posisi) +{s}")

    # ── 4. CVD Divergence — KUNCI pembeda akumulasi vs distribusi ─────────────
    # Harga flat/turun + buy volume > sell volume = hidden accumulation
    # Ini adalah sinyal PALING PENTING untuk membedakan akumulasi vs distribusi
    is_cvd_divergence = False

    candles_for_cvd = c15m if (c15m and len(c15m) >= CONFIG["p2_cvd_lookback"]) else c1h
    lookback        = CONFIG["p2_cvd_lookback"]

    if len(candles_for_cvd) >= lookback:
        recent_cvd = candles_for_cvd[-lookback:]

        # Hitung buy vs sell volume dari arah candle
        buy_vol  = sum(c["volume_usd"] for c in recent_cvd if c["close"] >= c["open"])
        sell_vol = sum(c["volume_usd"] for c in recent_cvd if c["close"] < c["open"])

        # Hitung price change dalam window CVD
        price_start = recent_cvd[0]["open"]
        price_end   = recent_cvd[-1]["close"]
        price_chg_window = abs((price_end - price_start) / price_start * 100) if price_start > 0 else 0.0

        cvd_ratio = (buy_vol / sell_vol) if sell_vol > 0 else 1.0

        # CVD Divergence: harga flat/sideways TAPI buy vol dominan = hidden buying
        if (price_chg_window <= CONFIG["p2_cvd_price_flat_max"]
                and cvd_ratio >= CONFIG["p2_cvd_ratio_min"]):
            s = CONFIG["p2_score_cvd_divergence"]
            score += s
            is_cvd_divergence = True
            signals.append(
                f"🔍 CVD Divergence: harga flat ({price_chg_window:.1f}%) "
                f"tapi buy/sell={cvd_ratio:.1f}x → HIDDEN ACCUMULATION +{s}"
            )
        elif cvd_ratio < 0.7 and price_change_24h > 3:
            # Distribusi: harga naik tapi sell vol dominan = WASPADA
            signals.append(f"⚠️ CVD Warning: harga naik tapi sell dominan (ratio={cvd_ratio:.1f}) — bisa distribusi")
            score -= 5  # penalti distribusi

    # ── Cap Phase 2 di max ────────────────────────────────────────────────────
    score = min(max(score, 0), CONFIG["p2_max_score"])

    return {
        "score":              score,
        "signals":            signals,
        "vol_accum":          round(vol_accum, 2),
        "is_cvd_divergence":  is_cvd_divergence,
        "is_oi_accum":        is_oi_accum,
        "oi_change":          round(oi_change, 2),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📐  PHASE 3 — POSITION BUILD-UP ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
def analyze_phase3_position_build(c1h, oi_data, vol_accum, range_ratio, price_change_24h, funding_rate):
    """
    Deteksi market maker sedang build posisi besar sebelum pump.

    Menggunakan formula dari prompt:
    - position_score    = oi_expansion × vol_accum
    - liquidity_vacuum  = vol_accum / range_compression
    - squeeze_score     = oi_expansion / price_stability
    - energy            = position_score × liquidity_vacuum

    Return: {score, signals, position_score, liq_vacuum, squeeze_score, energy}
    """
    score   = 0
    signals = []

    oi_change    = oi_data.get("change_pct", 0.0)
    is_new_oi    = oi_data.get("is_new", True)

    # Jika OI belum ada snapshot, gunakan estimasi dari vol
    oi_expansion = max(1.0, oi_change / 10.0 + 1.0) if not is_new_oi else 1.0
    if not is_new_oi and oi_change > 0:
        oi_expansion = 1.0 + (oi_change / 100.0)   # contoh: OI +5% → oi_expansion = 1.05

    # Price stability sebagai denominator (hindari div/0)
    abs_price_chg   = max(abs(price_change_24h), 0.1)
    # Range compression (< 1 = terkompresi, gunakan inverse untuk formula)
    range_compress  = max(range_ratio, 0.1)  # range_ratio dari Phase 1

    # ── Formula 1: Position Build Score = oi_expansion × vol_accum ────────────
    position_score = oi_expansion * vol_accum

    if position_score >= CONFIG["p3_position_score_strong"]:
        s = CONFIG["p3_score_pos_strong"]
        score += s
        signals.append(f"🏗️ Position Build STRONG: OI_exp×vol={position_score:.1f} +{s}")
    elif position_score >= CONFIG["p3_position_score_min"]:
        s = CONFIG["p3_score_pos_build"]
        score += s
        signals.append(f"🏗️ Position Build: OI_exp×vol={position_score:.1f} +{s}")

    # ── Formula 2: Liquidity Vacuum = vol_accum / range_compression ───────────
    # Range terkompresi (range_ratio < 1) → range_compress < 1 → liq_vacuum besar
    liq_vacuum = vol_accum / range_compress

    if liq_vacuum >= CONFIG["p3_liq_vacuum_strong"]:
        s = CONFIG["p3_score_liq_strong"]
        score += s
        signals.append(f"💨 Liquidity Vacuum STRONG: {liq_vacuum:.1f} +{s}")
    elif liq_vacuum >= CONFIG["p3_liq_vacuum_min"]:
        s = CONFIG["p3_score_liq_vacuum"]
        score += s
        signals.append(f"💨 Liquidity Vacuum: {liq_vacuum:.1f} +{s}")

    # ── Formula 3: Short Squeeze Potential = oi_expansion / price_stability ───
    # OI expansion besar + harga hampir tidak bergerak = squeeze setup
    squeeze_score = oi_expansion / (abs_price_chg / 10.0)  # normalize price

    if not is_new_oi and oi_change > 0:
        if squeeze_score >= CONFIG["p3_squeeze_strong"]:
            s = CONFIG["p3_score_squeeze_strong"]
            score += s
            signals.append(f"🔫 Short Squeeze STRONG: OI/price={squeeze_score:.1f} +{s}")
        elif squeeze_score >= CONFIG["p3_squeeze_min"]:
            s = CONFIG["p3_score_squeeze"]
            score += s
            signals.append(f"🔫 Short Squeeze Setup: OI/price={squeeze_score:.1f} +{s}")

    # ── Formula 4: Energy Buildup = position_score × liquidity_vacuum ─────────
    energy = position_score * liq_vacuum

    if energy >= CONFIG["p3_energy_strong"]:
        s = CONFIG["p3_score_energy_strong"]
        score += s
        signals.append(f"⚡ Energy STRONG: pos_score×liq_vac={energy:.1f} +{s}")
    elif energy >= CONFIG["p3_energy_min"]:
        s = CONFIG["p3_score_energy"]
        score += s
        signals.append(f"⚡ Energy Building: pos_score×liq_vac={energy:.1f} +{s}")

    # ── Bonus: Funding Rate negatif = short trap = squeeze ready ─────────────
    if funding_rate <= CONFIG["p3_funding_neg_threshold"]:
        s = CONFIG["p3_score_funding_neg"]
        score += s
        signals.append(f"💸 Funding Negatif {funding_rate*100:.4f}% (short trap aktif) +{s}")

    # ── Cap Phase 3 ───────────────────────────────────────────────────────────
    score = min(max(score, 0), CONFIG["p3_max_score"])

    return {
        "score":          score,
        "signals":        signals,
        "position_score": round(position_score, 2),
        "liq_vacuum":     round(liq_vacuum, 2),
        "squeeze_score":  round(squeeze_score, 2),
        "energy":         round(energy, 2),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📐  PHASE 4 — IGNITION PREPARATION ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
def analyze_phase4_ignition(c1h, c15m, price_change_24h):
    """
    Deteksi sinyal awal ignition — BUKAN setelah pump sudah berjalan.

    Kita ingin SEDIKIT momentum untuk konfirmasi, tapi BELUM parabolic.
    Target zone: harga sudah mulai naik 0.5-5% dari akumulasi.

    Signals:
    1. Breakout Pressure (vol 1h vs avg 24h)
    2. Early Momentum (naik sedikit dari accumulation zone)
    3. Higher Low structure
    4. BOS (Break of Structure)

    Return: {score, signals, breakout_pressure, momentum_pct}
    """
    score   = 0
    signals = []

    # ── 1. Breakout Pressure: vol 1h / avg vol 24h ────────────────────────────
    bp = 1.0
    if len(c1h) >= 24:
        vol_1h      = c1h[-1]["volume_usd"]
        avg_vol_24h = calc_avg_volume(c1h, 24)
        bp          = (vol_1h / avg_vol_24h) if avg_vol_24h > 0 else 1.0

        if bp >= CONFIG["p4_breakout_pressure_strong"]:
            s = CONFIG["p4_score_bp_strong"]
            score += s
            signals.append(f"🚀 Breakout Pressure STRONG: vol 1h={bp:.1f}x avg +{s}")
        elif bp >= CONFIG["p4_breakout_pressure_mild"]:
            s = CONFIG["p4_score_bp_mild"]
            score += s
            signals.append(f"🚀 Breakout Pressure: vol 1h={bp:.1f}x avg +{s}")

    # ── 2. Early Momentum (zona ideal: naik 0.5–5%) ───────────────────────────
    momentum_pct = 0.0
    if len(c1h) >= 4:
        # Momentum 4h: close sekarang vs close 4 candle lalu
        price_now   = c1h[-1]["close"]
        price_4h    = c1h[-4]["close"]
        momentum_pct = ((price_now - price_4h) / price_4h * 100) if price_4h > 0 else 0.0

        if CONFIG["p4_momentum_min_pct"] <= momentum_pct <= 3.0:
            # Perfect zone: naik 0.5-3% = awal ignition
            s = CONFIG["p4_score_momentum_strong"]
            score += s
            signals.append(f"⚡ Early Momentum Perfect: +{momentum_pct:.1f}% (4h) +{s}")
        elif CONFIG["p4_momentum_min_pct"] <= momentum_pct <= CONFIG["p4_momentum_max_pct"]:
            # Masih ok: naik 3-5%
            s = CONFIG["p4_score_momentum_ok"]
            score += s
            signals.append(f"⚡ Early Momentum OK: +{momentum_pct:.1f}% (4h) +{s}")
        elif momentum_pct > CONFIG["p4_momentum_max_pct"]:
            # Sudah terlalu naik = bukan awal lagi
            signals.append(f"⚠️ Momentum terlalu besar +{momentum_pct:.1f}% — mungkin sudah pump")
            score -= 3

    # ── 3. Higher Low Structure ────────────────────────────────────────────────
    lookback = CONFIG["p4_higher_low_lookback"]
    if len(c1h) >= lookback:
        lows = [c["low"] for c in c1h[-lookback:]]
        # Cek apakah ada tren higher low (low sekarang > low beberapa candle lalu)
        mid_low = min(lows[:lookback//2])
        rec_low = min(lows[lookback//2:])
        if rec_low > mid_low:
            s = CONFIG["p4_score_higher_low"]
            score += s
            signals.append(f"📐 Higher Low Structure terkonfirmasi +{s}")

    # ── 4. BOS (Break of Structure) ───────────────────────────────────────────
    bos_lookback = CONFIG["p4_bos_lookback"]
    if len(c1h) >= bos_lookback + 2:
        # BOS = close sekarang > high tertinggi dari N candle sebelumnya
        # (harga breakout dari range akumulasi)
        prior_high  = max(c["high"] for c in c1h[-(bos_lookback+2):-2])
        current_cls = c1h[-1]["close"]
        if current_cls > prior_high:
            s = CONFIG["p4_score_bos"]
            score += s
            signals.append(f"🔔 BOS (Break of Structure) terkonfirmasi +{s}")

    # ── Cap Phase 4 ───────────────────────────────────────────────────────────
    score = min(max(score, 0), CONFIG["p4_max_score"])

    return {
        "score":              score,
        "signals":            signals,
        "breakout_pressure":  round(bp, 2),
        "momentum_pct":       round(momentum_pct, 2),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🧮  PUMP PROBABILITY & SCORE CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_pump_probability(total_score):
    """
    Logistic function: score → probability (%)
    score = prob_center → 50%
    steepness dikontrol prob_scale
    """
    center = CONFIG["prob_center"]
    scale  = CONFIG["prob_scale"]
    prob   = 1.0 / (1.0 + math.exp(-(total_score - center) / scale))
    return round(prob * 100, 1)

def calc_pump_eta(p1_score, p2_score, p3_score, p4_score):
    """
    Estimasi waktu pump berdasarkan fase aktif.
    Phase 1+2 aktif = masih akumulasi = 4-8 jam
    Phase 1+2+3 = position build = 2-4 jam
    Phase 4 aktif = ignition imminent = 30-90 menit
    """
    if p4_score >= 15:
        return "30–90 menit"
    elif p3_score >= 15 and p4_score >= 8:
        return "1–3 jam"
    elif p3_score >= 12:
        return "2–4 jam"
    elif p2_score >= 20:
        return "3–6 jam"
    else:
        return "4–8 jam"

def calc_tp_sl(price, atr):
    """Hitung Entry, SL, TP1, TP2, TP3 berdasarkan ATR."""
    tp1 = price * (1 + CONFIG["tp1_atr_mult"] * atr / price)
    tp2 = price * (1 + CONFIG["tp2_atr_mult"] * atr / price)
    tp3 = price * (1 + CONFIG["tp3_atr_mult"] * atr / price)
    sl  = price * (1 - CONFIG["sl_atr_mult"]  * atr / price)

    def fmt(p):
        if p >= 10:   return f"{p:.4f}"
        if p >= 1:    return f"{p:.5f}"
        if p >= 0.01: return f"{p:.6f}"
        return f"{p:.8f}"

    sl_pct  = abs(price - sl) / price * 100
    tp1_pct = abs(tp1 - price) / price * 100
    tp3_pct = abs(tp3 - price) / price * 100

    return {
        "entry":   fmt(price),
        "sl":      fmt(sl),
        "tp1":     fmt(tp1),
        "tp2":     fmt(tp2),
        "tp3":     fmt(tp3),
        "sl_pct":  round(sl_pct, 1),
        "tp1_pct": round(tp1_pct, 1),
        "tp3_pct": round(tp3_pct, 1),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🏆  MASTER SCORE — CORE SCORING ENGINE
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker_data):
    """
    Scoring utama — menggabungkan 4 fase menjadi pump probability score.

    Return dict dengan semua hasil, atau None jika tidak lolos gate.
    """
    # ── Extract ticker data ───────────────────────────────────────────────────
    try:
        price_now     = float(ticker_data.get("lastPr", ticker_data.get("last", 0)))
        vol_24h_usd   = float(ticker_data.get("quoteVolume", 0))
        change_24h    = float(ticker_data.get("change24h", ticker_data.get("priceChangePercent", 0)))
        # Bitget biasanya memberikan change24h dalam desimal (0.05 = 5%)
        if abs(change_24h) < 1.0:
            change_24h = change_24h * 100.0
    except Exception:
        return None

    if price_now <= 0:
        return None

    # ── Pre-filter cepat ──────────────────────────────────────────────────────
    if vol_24h_usd < CONFIG["min_vol_24h_usd"]:
        return None
    if vol_24h_usd > CONFIG["max_vol_24h_usd"]:
        return None

    # ── Gate 1: Sudah pump (reject) ──────────────────────────────────────────
    if change_24h > CONFIG["gate_already_pumped_pct"]:
        log.debug(f"  {symbol}: SKIP — sudah pump {change_24h:+.1f}%")
        return None

    # ── Gate 2: Dump besar (reject) ──────────────────────────────────────────
    if change_24h < CONFIG["gate_dump_pct"]:
        log.debug(f"  {symbol}: SKIP — dump {change_24h:+.1f}%")
        return None

    # ── Fetch candle data ─────────────────────────────────────────────────────
    c1h  = get_candles(symbol, "1h",  CONFIG["candles_1h"])
    c4h  = get_candles(symbol, "4h",  CONFIG["candles_4h"])
    c15m = get_candles(symbol, "15m", CONFIG["candles_15m"])

    if len(c1h) < 20:
        return None

    # ── Gate 3: RSI overbought (reject) ──────────────────────────────────────
    rsi = calc_rsi(c1h)
    if rsi >= CONFIG["gate_rsi_max"]:
        log.debug(f"  {symbol}: SKIP — RSI overbought {rsi:.1f}")
        return None

    # ── Gate 4: Zona distribusi 48h (reject) ─────────────────────────────────
    price_pos = calc_price_pos_48(c1h)
    if price_pos > CONFIG["gate_price_pos_max"]:
        log.debug(f"  {symbol}: SKIP — harga di zona distribusi {price_pos:.0%}")
        return None

    # ── Fetch OI & Funding ────────────────────────────────────────────────────
    oi_data      = get_oi_change(symbol)
    funding_rate = get_funding_rate(symbol)

    # ── Gate 5: OI minimum ────────────────────────────────────────────────────
    if oi_data["oi_now"] > 0 and oi_data["oi_now"] < CONFIG["min_oi_usd"]:
        return None

    # ── Hitung ATR untuk Entry/SL/TP ─────────────────────────────────────────
    atr14 = calc_atr(c1h, 14)

    # ══════════════════════════════════════════════════════════════════════════
    #  RUN 4-PHASE ANALYSIS
    # ══════════════════════════════════════════════════════════════════════════
    ph1 = analyze_phase1_compression(c1h, c4h)
    ph2 = analyze_phase2_accumulation(c1h, c15m, oi_data, change_24h)
    ph3 = analyze_phase3_position_build(
        c1h, oi_data,
        ph2["vol_accum"],
        ph1["range_ratio"],
        change_24h,
        funding_rate
    )
    ph4 = analyze_phase4_ignition(c1h, c15m, change_24h)

    # ── Total Score (100 poin) ────────────────────────────────────────────────
    total_score = ph1["score"] + ph2["score"] + ph3["score"] + ph4["score"]

    # Minimum score filter
    if total_score < CONFIG["min_score_watchlist"]:
        return None

    # ── Pump Probability ──────────────────────────────────────────────────────
    pump_prob = calc_pump_probability(total_score)

    # ── ETA estimasi ─────────────────────────────────────────────────────────
    eta = calc_pump_eta(ph1["score"], ph2["score"], ph3["score"], ph4["score"])

    # ── Entry / SL / TP ───────────────────────────────────────────────────────
    levels = calc_tp_sl(price_now, atr14)

    # ── Compile signals list ──────────────────────────────────────────────────
    all_signals = []
    all_signals.extend(ph1["signals"])
    all_signals.extend(ph2["signals"])
    all_signals.extend(ph3["signals"])
    all_signals.extend(ph4["signals"])

    return {
        "symbol":           symbol,
        "price":            price_now,
        "score":            total_score,
        "pump_prob":        pump_prob,
        "rsi":              rsi,
        "change_24h":       round(change_24h, 2),
        "price_pos":        price_pos,
        "vol_24h_usd":      vol_24h_usd,
        "oi_change":        ph2["oi_change"],
        "funding_rate":     funding_rate,
        "eta":              eta,
        "ph1_score":        ph1["score"],
        "ph2_score":        ph2["score"],
        "ph3_score":        ph3["score"],
        "ph4_score":        ph4["score"],
        "bbw":              ph1["bbw"],
        "atr_ratio":        ph1["atr_ratio"],
        "vol_accum":        ph2["vol_accum"],
        "is_cvd_div":       ph2["is_cvd_divergence"],
        "liq_vacuum":       ph3["liq_vacuum"],
        "energy":           ph3["energy"],
        "bp_ratio":         ph4["breakout_pressure"],
        "momentum_pct":     ph4["momentum_pct"],
        "levels":           levels,
        "signals":          all_signals,
        # Untuk ranking
        "rank_value":       total_score * pump_prob,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📨  TELEGRAM ALERT BUILDER
# ══════════════════════════════════════════════════════════════════════════════
def build_alert(r, rank=1):
    """
    Format alert Telegram sesuai spesifikasi prompt.
    Simple, clean, informatif.
    """
    sym   = r["symbol"].replace("USDT", "")
    score = r["score"]
    prob  = r["pump_prob"]
    lv    = r["levels"]

    # Tentukan phase label
    if r["ph4_score"] >= 15:
        phase_label = "🔥 IGNITION IMMINENT"
    elif r["ph3_score"] >= 18:
        phase_label = "⚡ POSITION BUILD-UP"
    elif r["ph2_score"] >= 20:
        phase_label = "📦 LATE ACCUMULATION"
    else:
        phase_label = "🗜️ COMPRESSION"

    # Format OI info
    oi_str = f"+{r['oi_change']:.1f}%" if r['oi_change'] > 0 else f"{r['oi_change']:.1f}%"

    msg = (
        f"🚨 <b>POTENTIAL PUMP</b>\n\n"
        f"Symbol: <b>{r['symbol']}</b>\n"
        f"Score: <b>{score}</b>\n"
        f"Possible Pump: <b>{prob}%</b>\n\n"
        f"Entry: <code>{lv['entry']}</code>\n"
        f"SL   : <code>{lv['sl']}</code> (-{lv['sl_pct']}%)\n"
        f"TP   : <code>{lv['tp3']}</code> (+{lv['tp3_pct']}%)\n\n"
        f"Estimate pump: {r['eta']}\n\n"
        f"Phase : {phase_label}\n"
        f"RSI   : {r['rsi']:.0f} | OI: {oi_str} | Vol: {r['vol_accum']:.1f}x\n"
        f"Rank #{rank} dari {utc_now()}"
    )
    return msg

def build_summary(results):
    """Ringkasan semua kandidat pump dalam satu pesan."""
    lines = [f"📊 <b>PUMP SCANNER v31 — {utc_now()}</b>\n"]
    lines.append(f"<b>{len(results)} kandidat pump terdeteksi:</b>\n")

    for i, r in enumerate(results[:10], 1):
        phase_icon = "🔥" if r["ph4_score"] >= 15 else ("⚡" if r["ph3_score"] >= 15 else "📦")
        lines.append(
            f"{i}. {phase_icon} <b>{r['symbol']}</b> "
            f"Score: <b>{r['score']}</b> "
            f"({r['pump_prob']}%) "
            f"ETA: {r['eta']}"
        )

    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  CANDIDATE BUILDER (pre-filter sebelum scan berat)
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    """
    Filter cepat sebelum scan penuh.
    Hanya kirim coin yang layak untuk analisis detail.
    """
    candidates    = []
    filtered_stats = defaultdict(int)
    not_found     = []

    for sym in WHITELIST_SYMBOLS:
        # Skip keyword yang dilarang
        if any(kw in sym for kw in EXCLUDED_KEYWORDS):
            filtered_stats["excluded_keyword"] += 1
            continue

        # Cari di data ticker
        t = tickers.get(sym)
        if t is None:
            not_found.append(sym)
            continue

        # Pre-filter volume
        try:
            vol = float(t.get("quoteVolume", 0))
        except Exception:
            vol = 0.0

        if vol < CONFIG["min_vol_24h_usd"]:
            filtered_stats["low_volume"] += 1
            continue

        # Pre-filter perubahan harga
        try:
            chg = float(t.get("change24h", 0))
            if abs(chg) < 1.0:
                chg = chg * 100.0
        except Exception:
            chg = 0.0

        if chg > CONFIG["gate_already_pumped_pct"]:
            filtered_stats["already_pumped"] += 1
            continue

        if chg < CONFIG["gate_dump_pct"]:
            filtered_stats["dump_filter"] += 1
            continue

        # Harga valid
        try:
            price = float(t.get("lastPr", t.get("last", 0)))
            if price <= 0:
                filtered_stats["invalid_price"] += 1
                continue
        except Exception:
            filtered_stats["invalid_price"] += 1
            continue

        # Skip jika cooldown aktif
        if is_cooldown(sym):
            filtered_stats["cooldown"] += 1
            continue

        candidates.append((sym, t))

    total     = len(WHITELIST_SYMBOLS)
    will_scan = len(candidates)
    log.info(f"\n📊 SCAN SUMMARY v31:")
    log.info(f"   Total whitelist : {total}")
    log.info(f"   ✅ Akan di-scan : {will_scan}")
    for k, v in sorted(filtered_stats.items()):
        log.info(f"   ❌ {k:25s}: {v}")
    if not_found:
        log.info(f"   ⚠️  Tidak di Bitget: {len(not_found)} coin")
    est = will_scan * CONFIG["sleep_coins"]
    log.info(f"   ⏱️  Est. scan time: {est:.0f}s ({est/60:.1f} menit)\n")

    return candidates

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== ALTCOIN PUMP SCANNER v31 — {utc_now()} ===")

    # Load persistent data
    load_funding_snapshots()
    load_oi_snapshots()

    # Ambil semua ticker
    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return
    log.info(f"Total ticker dari Bitget: {len(tickers)}")

    # Build candidate list (pre-filtered)
    candidates = build_candidate_list(tickers)

    results    = []
    t_start    = time.time()
    n_err      = 0

    for i, (sym, t) in enumerate(candidates):
        try:
            vol = float(t.get("quoteVolume", 0))
        except Exception:
            vol = 0.0

        if (i + 1) % 10 == 0 or i == 0:
            log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e6:.1f}M)...")

        t_sym = time.time()
        try:
            res = master_score(sym, t)
            if res:
                elapsed = time.time() - t_sym
                log.info(
                    f"  ✅ {sym}: Score={res['score']} Prob={res['pump_prob']}% "
                    f"Ph1:{res['ph1_score']} Ph2:{res['ph2_score']} "
                    f"Ph3:{res['ph3_score']} Ph4:{res['ph4_score']} "
                    f"ETA:{res['eta']} ({elapsed:.2f}s)"
                )
                results.append(res)
        except Exception as ex:
            import traceback as _tb
            log.warning(f"  ❌ Error {sym}: {type(ex).__name__}: {ex} — skip")
            log.debug(_tb.format_exc().strip())
            n_err += 1

        time.sleep(CONFIG["sleep_coins"])

    # Save snapshots
    save_oi_snapshots()
    save_funding_snapshots()
    log.info("OI & Funding snapshots disimpan.")

    # Ranking: rank_value = score × probability
    results.sort(key=lambda x: x["rank_value"], reverse=True)

    t_total   = time.time() - t_start
    n_scanned = len(candidates)
    n_pass    = len(results)
    log.info(
        f"\n📊 SCAN FUNNEL v31:\n"
        f"   {n_scanned} scanned → {n_pass} lolos threshold\n"
        f"   ❌ Errors: {n_err} | ⏱ Total: {t_total:.1f}s\n"
    )

    if not results:
        log.info("Tidak ada sinyal memenuhi syarat saat ini.")
        return

    top = results[:CONFIG["max_alerts_per_run"]]

    # Kirim summary
    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    # Kirim alert per coin
    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"✅ Alert #{rank}: {r['symbol']} Score={r['score']} Prob={r['pump_prob']}%")
        time.sleep(2)

    log.info(f"=== SELESAI v31 — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  ALTCOIN PUMP SCANNER v31 — STRUCTURAL ACCUMULATION ENGINE  ║")
    log.info("║  Target: Deteksi pump 20-70% SEBELUM ignition              ║")
    log.info("║  4-Phase: Compression → Accumulation → Build → Ignition    ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)

    run_scan()
