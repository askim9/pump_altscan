"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PIVOT BOUNCE SCANNER v2.6 — PRE-PUMP DETECTION                            ║
║                                                                              ║
║  FILOSOFI CORE:                                                              ║
║  Deteksi TRANSISI dari Fase Tidur → Fase Bangun, SEBELUM harga lari.        ║
║                                                                              ║
║  3 kondisi wajib terpenuhi bersamaan:                                       ║
║  [1] TIDUR PULAS   — coin compression di support ≥ 36 jam                  ║
║  [2] MULAI BANGUN  — volume spike ≥ 1.8x avg compression DAN RVOL ≥ 1.5x  ║
║  [3] POSISI TEPAT  — harga masih dalam 12% dari low, belum lari             ║
║                                                                              ║
║  SCORING (0-100):                                                            ║
║  [30] Compression quality — seberapa "padat" dan panjang fase tidur         ║
║  [25] Volume awakening   — seberapa besar volume spike vs baseline          ║
║  [20] Support proximity  — seberapa dekat harga ke support historis         ║
║  [15] Candle structure   — candle terbaru hijau / wick panjang / doji       ║
║  [10] RSI momentum       — oversold = siap balik                            ║
║                                                                              ║
║  HARD GATES (salah satu gagal → skip coin):                                 ║
║  - Compression ≥ 36 candle 1H dengan range < 10%                           ║
║  - Volume candle terbaru ≥ 1.8x avg compression                            ║
║  - RVOL ≥ 1.5x (vs jam yang sama historis) — konfirmasi ganda              ║
║  - Spike candle bukan selling climax (merah + harga di bawah zona)         ║
║  - Harga belum naik > 12% dari low compression                             ║
║  - Volume 24H: $500K – $80M                                                ║
║  - R/R minimum 1:1.5, SL minimum 2.5% dari entry                          ║
║  - Funding rate > -0.003                                                   ║
║                                                                              ║
║  PATCH v2.1 (dari data nyata CAKE/THETA/IOTX):                             ║
║  + Gate selling climax: spike merah + harga di bawah zona = SKIP           ║
║  + Gate RVOL minimum 1.5x                                                  ║
║  + Gate R/R minimum 1:1.5, SL minimum 2.5%                                ║
║  + min_vol_24h dinaikkan $3K → $500K                                       ║
║                                                                              ║
║  PATCH v2.2 (dari data nyata ALCH/BANK):                                   ║
║  + Gate trend konteks: harga > 3% di bawah zona compression = SKIP         ║
║  + Gate MA bearish: MA20-MA50 gap > 2.5% + keduanya turun = SKIP          ║
║    (dengan gap threshold agar false positive pada compression dihindari)    ║
║                                                                              ║
║  TARGET   : Entry sekarang, TP dalam 1-2 hari (+10% s/d +100%)             ║
║  INTERVAL : Setiap 1 jam                                                    ║
║  EXCHANGE : Bitget USDT-Futures                                             ║
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
    # Dinaikkan dari $3K → $500K berdasarkan data nyata:
    # IOTX $117K, THETA $310K terlalu kecil untuk pump 10%+ dalam 24H
    # Referensi chart (TRUMP/PIXEL/ORCA/VVV): semua vol ≥ $5M saat pump
    "min_vol_24h":            500_000,
    "max_vol_24h":         800_000_000,
    "pre_filter_vol":         100_000,   # pre-filter dinaikkan ke $100K

    # ── price change gate ─────────────────────────────────────────────────────
    "gate_chg_24h_max":            40.0,   # coin yang sudah naik >40% 24h pasti terlambat

    # ── RVOL minimum gate ─────────────────────────────────────────────────────
    # Data nyata: THETA RVOL=1.2x lolos tapi bukan awakening sejati
    # RVOL dihitung vs jam yang sama minggu lalu — lebih jujur dari vol_mult
    "min_rvol_gate":               1.5,   # RVOL < 1.5x = SKIP

    # ── minimum R/R dan SL ────────────────────────────────────────────────────
    # IOTX R/R=0 karena SL hampir sama dengan entry — tidak layak trade
    "min_rr":                      1.5,   # R/R minimum 1:1.5
    "min_sl_pct":                  2.5,   # minimum SL distance 2.5% dari entry

    # ── candle config ─────────────────────────────────────────────────────────
    "candle_limit_1h":            720,     # 30 hari data 1H — dinaikkan agar XAN-type (flat 40 hari) terdeteksi

    # ── COMPRESSION DETECTION ─────────────────────────────────────────────────
    "compression_min_candles":     36,
    "compression_max_candles":    672,
    # 11% — dinaikkan dari 10% berdasarkan forensik BLAST (range 10.23%)
    "compression_range_pct":      0.11,
    "compression_lookback":       672,

    # ── ZONE PURITY CHECK (v2.8) ──────────────────────────────────────────────
    # Masalah dari 2ZUSDT: pump terjadi DALAM zona compression (13-15 Mar),
    # lalu koreksi, lalu scanner kirim sinyal seolah-olah ini pre-pump baru.
    # Gate sebelumnya (post_pump 6H) tidak menangkap karena pump sudah > 6 jam.
    #
    # Fix: zona compression tidak boleh mengandung banyak candle volume ekstrim.
    # Candle vol > 5× avg_zone = ada aksi besar yang sudah terjadi dalam zona.
    # Jika ada ≥ 3 candle seperti ini → zona "terkontaminasi" → bukan zona tidur → SKIP
    #
    # Kalibrasi dari data forensik:
    #   2Z (false positive): ~6 candle pump dalam zona → SKIP ✅
    #   XAN (valid): 0 candle vol>5x avg dalam zona → LOLOS ✅
    #   BLAST (valid): 1 candle outlier → LOLOS ✅
    "zone_purity_vol_mult":        5.0,   # candle vol > 5× avg_zone = spike besar
    "zone_purity_spike_max":       1,     # toleransi 1 spike; ke-2 = terkontaminasi

    # ── CHOPPY FILTER ─────────────────────────────────────────────────────────
    # avg (high-low)/low per candle dalam zona < 2% → zona benar-benar flat
    # Kalibrasi: BEAT 3.64% (false) vs IOTA 0.74% / ARPA 0.84% / NOT 1.23% (valid)
    "compression_choppy_max":      0.02,  # avg candle range < 2% dalam zona

    # ── VOLUME AWAKENING ──────────────────────────────────────────────────────
    # Fase 2: volume mulai "bangun" — ini trigger utama
    "awakening_vol_mult":          1.8,    # volume candle terbaru ≥ 1.8x avg volume selama compression
    "awakening_lookback_candles":    3,    # cek 3 candle terakhir (salah satu harus spike)
    "strong_awakening_mult":        3.0,   # ≥ 3x = awakening kuat, +bonus score
    "mega_awakening_mult":          6.0,   # ≥ 6x = mega spike (seperti PIXEL), +bonus besar

    # ── NOT TOO LATE GATE ─────────────────────────────────────────────────────
    # Harga belum boleh naik terlalu jauh dari low compression
    # Dinaikkan 8% → 12%: spike candle pertama saja bisa +6-10% dari low,
    # sehingga alert yang dikirim saat spike candle baru tutup tetap valid
    "max_rise_from_low_pct":       0.12,   # maksimal sudah naik 12% dari low compression
    "max_rise_warn_pct":           0.06,   # > 6% dari low = kasih warning di alert

    # ── SUPPORT PROXIMITY ─────────────────────────────────────────────────────
    "support_proximity_pct":       0.06,   # harga dalam 6% dari support historis

    # ── TREND CONTEXT GATE ────────────────────────────────────────────────────
    # Bug dari data nyata (ALCH/BANK): coin downtrend bisa punya compression
    # historis + spike hijau tapi harga masih di bawah zona → false signal.
    # Solusi: cek apakah harga sekarang MASIH di dalam atau di atas zona compression.
    # Jika harga di bawah comp_low lebih dari threshold ini → downtrend aktif, skip.
    "price_below_zone_max":        0.03,   # toleransi harga di bawah zona: max 3%
                                           # > 3% di bawah comp_low = downtrend sejati, skip
                                           # ≤ 3% = bisa jadi liq sweep sebelum bounce

    # ── POST-PUMP DETECTION GATE (BARU v2.6) ────────────────────────────────
    # Masalah: GAS dan 1MBABYDOGE lolos karena pump sudah terjadi tapi harga
    # kembali ke zona compression (post-pump retracement). Scanner tidak tahu
    # bahwa volume 6H terakhir sudah jauh di atas baseline compression.
    #
    # Solusi: jika rata-rata volume 6H terakhir > comp_avg_vol × 7,
    # berarti coin sedang di fase aktif/post-pump, bukan pre-pump.
    #
    # Dikalibrasi dari data forensik nyata:
    #   XAN window alert  : avg_vol_6h = 3.4x comp_avg → LOLOS ✅
    #   MYX window alert  : avg_vol_6h = 1.2x comp_avg → LOLOS ✅
    #   GAS post-pump     : avg_vol_6h = 150x comp_avg → SKIP  ✅
    #   BABYDOGE post-pump: avg_vol_6h = 8.0x comp_avg → SKIP  ✅
    #   Threshold 7x memberikan margin aman di antara keduanya
    "post_pump_vol_mult":            7.0,   # avg_vol_6h > comp_avg * 7 → skip (pump baru)
    "post_pump_lookback_candles":      6,   # window 6H untuk pump sangat baru

    # ── POST-PUMP 48H GATE (v2.8) ─────────────────────────────────────────────
    # Menangkap pump yang terjadi 6-48 jam lalu (2Z-type false positive).
    # Threshold lebih rendah dari gate 6H karena window lebih panjang.
    # Kalibrasi: 2Z avg_48H/comp_avg ≈ 4-5x, XAN/MYX ≈ 1-3x.
    "post_pump_lookback_48h":        48,   # window 48H
    "post_pump_vol_mult_48h":       4.0,   # avg_48h > comp_avg * 4 → skip

    # ── G3: BREAKOUT GATE (v2.6) ──────────────────────────────────────────────
    # Jika harga sekarang sudah > comp_high × 1.03 → coin sedang/sudah breakout.
    # Berbeda dari post_pump (dimensi volume), ini cek posisi harga vs zona.
    "price_above_zone_max":          0.03,  # price_now max 3% di atas comp_high

    # ── LIQUIDITY SWEEP BONUS ─────────────────────────────────────────────────
    # Bonus score jika ada false breakdown sebelum recovery (pola ORCA/PIXEL)
    "liq_sweep_lookback":           12,    # cek 12 candle terakhir
    "liq_sweep_recover_bars":        4,    # recovery dalam 4 candle setelah breakdown

    # ── FUNDING ───────────────────────────────────────────────────────────────
    "funding_gate":              -0.003,   # buang jika funding < -0.003

    # ── ENTRY / TARGET ────────────────────────────────────────────────────────
    "atr_sl_mult":                  1.2,
    "min_target_pct":               8.0,

    # ── SCORING THRESHOLD ─────────────────────────────────────────────────────
    # Dikalibrasi dari 4 chart nyata (TRUMP, PIXEL, ORCA, VVV):
    # - Setup kuat (TRUMP/PIXEL/ORCA): skor 62-64
    # - Setup moderat (VVV-type): skor 55-60 di kondisi real market
    # - Threshold 52 menangkap semua 4 tipe tanpa terlalu banyak false positive
    "score_threshold":             52,     # minimal skor untuk alert

    # ── OPERASIONAL ───────────────────────────────────────────────────────────
    "max_alerts_per_run":           8,   # dinaikkan v2.8: VET/CRO score 82 tidak masuk karena limit 6
    "alert_cooldown_sec":        3600,
    "sleep_coins":                 0.7,
    "sleep_error":                 3.0,
    "cooldown_file":     "/tmp/v2_cooldown.json",
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
#  🔍  COMPRESSION ZONE DETECTOR
#  Cari zona tidur terpanjang dan terbaru yang berakhir dengan volume spike
# ══════════════════════════════════════════════════════════════════════════════
def find_compression_zone(candles):
    """
    Scan dari kanan ke kiri (terbaru ke terlama).
    Cari rentang candle di mana high-low range < compression_range_pct.
    Return zona terbaik beserta metriknya.

    Return dict:
      start_idx, end_idx   — index candle zona compression
      low, high            — batas zona
      length               — jumlah candle dalam zona
      avg_vol              — rata-rata volume selama compression
      age_candles          — berapa candle lalu zona ini berakhir (0 = masih aktif)
    """
    cfg        = CONFIG
    min_len    = cfg["compression_min_candles"]
    max_len    = cfg["compression_max_candles"]
    range_pct  = cfg["compression_range_pct"]
    lookback   = min(cfg["compression_lookback"], len(candles))
    scan_slice = candles[-lookback:]
    n          = len(scan_slice)

    best = None

    # Geser window dari kanan (terbaru)
    # Kita cari zona paling baru yang cukup panjang
    for end in range(n - 1, min_len - 2, -1):
        # Ekspansi ke kiri selama range masih dalam batas
        zone_high = scan_slice[end]["high"]
        zone_low  = scan_slice[end]["low"]
        start     = end

        for start in range(end - 1, max(end - max_len, -1), -1):
            c = scan_slice[start]
            new_high = max(zone_high, c["high"])
            new_low  = min(zone_low,  c["low"])
            rng      = (new_high - new_low) / new_low if new_low > 0 else 999

            if rng > range_pct:
                # Range sudah terlalu lebar, hentikan ekspansi
                start += 1  # step back satu agar valid
                break
            zone_high = new_high
            zone_low  = new_low

        length = end - start + 1
        if length < min_len:
            continue

        # Zona valid ditemukan
        zone_candles = scan_slice[start:end+1]
        avg_vol = sum(c["volume_usd"] for c in zone_candles) / length

        # ── CHOPPY FILTER (v2.8) ─────────────────────────────────────────────
        # Zona dengan range < 11% tapi candle-candle individualnya besar = choppy.
        # Kalibrasi: BEAT avg_candle_range=3.64% (false), valid coins < 1.5%.
        choppy_max = cfg.get("compression_choppy_max", 0.02)
        avg_candle_range = sum(
            (c["high"] - c["low"]) / c["low"]
            for c in zone_candles if c["low"] > 0
        ) / length
        if avg_candle_range > choppy_max:
            continue

        # ── ZONE PURITY CHECK (v2.8) ─────────────────────────────────────────
        # Zona tidak boleh mengandung banyak candle volume ekstrim.
        # Candle vol >> avg_zone = ada aksi besar yang sudah terjadi dalam zona.
        # Kalibrasi: spike_max=1 memberi toleransi 1 spike outlier (seperti BLAST),
        # tapi 2Z dengan 5-6 pump candles dalam zona pasti terfilter.
        purity_mult = cfg.get("zone_purity_vol_mult", 5.0)
        purity_max  = cfg.get("zone_purity_spike_max", 1)
        spike_count = sum(
            1 for c in zone_candles
            if avg_vol > 0 and c["volume_usd"] > purity_mult * avg_vol
        )
        if spike_count > purity_max:
            continue  # zona terkontaminasi

        # Hitung "age" — berapa candle dari akhir zona ke candle terkini
        age = (n - 1) - end  # 0 = zona berakhir di candle terbaru

        # Skor kualitas: lebih panjang lebih baik, lebih baru lebih baik
        quality = length * math.exp(-age / 48)  # decay jika sudah lama

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
                "range_pct":        (zone_high - zone_low) / zone_low,
                "avg_candle_range": avg_candle_range,
                "spike_count":      spike_count,
            }

        # Setelah menemukan zona valid, geser end ke awal zona untuk efisiensi
        end = start

    return best


# ══════════════════════════════════════════════════════════════════════════════
#  ⚡  VOLUME AWAKENING DETECTOR
#  Apakah volume sudah mulai "bangun" dari tidurnya?
# ══════════════════════════════════════════════════════════════════════════════
def detect_volume_awakening(candles, compression_avg_vol):
    """
    Cek 3 candle terbaru — apakah ada yang volumenya spike signifikan
    dibanding rata-rata selama compression?

    Return dict:
      detected       — bool
      best_mult      — multiplier volume terbaik dari 3 candle terbaru
      spike_candle   — index candle yang spike (dari akhir array)
      is_green       — apakah candle spike hijau
      is_mega        — volume ≥ 6x (seperti PIXEL)
    """
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
            spike_candle = i   # 1 = terbaru
            is_green     = c["close"] > c["open"]

    detected = best_mult >= thresh
    is_mega  = best_mult >= CONFIG["mega_awakening_mult"]

    return {
        "detected":     detected,
        "best_mult":    round(best_mult, 2),
        "spike_candle": spike_candle,
        "is_green":     is_green,
        "is_mega":      is_mega,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  💧  LIQUIDITY SWEEP DETECTOR
#  Cek apakah ada false breakdown (dip bawah support lalu recovery cepat)
#  — pola yang sering terjadi sebelum pump besar (ORCA, PIXEL)
# ══════════════════════════════════════════════════════════════════════════════
def detect_liquidity_sweep(candles, support_low):
    """
    False breakdown = harga dip di bawah support_low tapi langsung recovery
    dalam beberapa candle. Ini pertanda smart money ambil likuiditas.
    """
    lookback    = CONFIG["liq_sweep_lookback"]
    recover_bars = CONFIG["liq_sweep_recover_bars"]
    recent      = candles[-lookback:]

    for i in range(len(recent) - 1):
        c = recent[i]
        # Candle ini breakdown di bawah support
        if c["low"] < support_low * 0.99:  # minimal 1% di bawah support
            # Cek apakah recovery dalam recover_bars candle berikutnya
            for j in range(i + 1, min(i + recover_bars + 1, len(recent))):
                if recent[j]["close"] > support_low:
                    return True  # ada liquidity sweep
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  📊  CANDLE STRUCTURE ANALYZER
#  Analisa struktur candle terbaru — rejection / doji / engulfing
# ══════════════════════════════════════════════════════════════════════════════
def analyze_candle_structure(candle):
    """
    Return skor 0-15 berdasarkan struktur candle terbaru.
    Bullish rejection (wick panjang bawah) = +15
    Doji / spinning top = +8
    Candle hijau biasa = +5
    Candle merah = 0
    """
    body   = abs(candle["close"] - candle["open"])
    rng    = candle["high"] - candle["low"]
    if rng == 0:
        return 0, "doji"

    lower_wick = min(candle["open"], candle["close"]) - candle["low"]
    upper_wick = candle["high"] - max(candle["open"], candle["close"])
    body_pct   = body / rng
    lwick_pct  = lower_wick / rng

    # Bullish rejection: lower wick > 50% candle range, body kecil
    if lwick_pct > 0.50 and body_pct < 0.35:
        return 15, "bullish rejection wick"

    # Hammer/pin bar: lower wick > 40%
    if lwick_pct > 0.40:
        return 12, "hammer/pin bar"

    # Doji: body sangat kecil
    if body_pct < 0.15:
        return 8, "doji (indecision)"

    # Green candle biasa
    if candle["close"] > candle["open"]:
        return 5, "green candle"

    # Red candle — bearish, nilai minimal
    return 2, "red candle"


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_entry_targets(candles, compression_zone):
    cur  = candles[-1]["close"]
    atr  = calc_atr(candles[-48:], 14) or cur * 0.025

    # Entry: dalam zona compression atau sedikit di atasnya
    comp_mid = (compression_zone["high"] + compression_zone["low"]) / 2
    entry    = min(cur * 0.999, compression_zone["high"] * 1.005)

    # Stop loss: di bawah low compression dengan buffer ATR
    sl = compression_zone["low"] - atr * CONFIG["atr_sl_mult"]
    sl = max(sl, entry * 0.85)  # batas atas: SL maksimal 15% dari entry

    # ── FIX: enforce minimum SL distance ─────────────────────────────────────
    # Untuk coin harga sangat rendah, ATR tiny → SL bisa 0.01% dari entry
    # yang tidak masuk akal. Minimum 2.5% agar ada ruang gerak yang wajar.
    min_sl_dist = entry * (CONFIG["min_sl_pct"] / 100)
    if (entry - sl) < min_sl_dist:
        sl = entry - min_sl_dist

    sl_pct = round((entry - sl) / entry * 100, 1)

    # Target: cari resistance historis di atas harga
    recent     = candles[-240:]  # 10 hari
    res_levels = []
    min_target = cur * (1 + CONFIG["min_target_pct"] / 100)

    for i in range(3, len(recent) - 3):
        h = recent[i]["high"]
        if h <= min_target:
            continue
        # Minimal 2 touches dalam 10 hari
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
        # Fallback: ATR multiplier berbasis panjang compression.
        # Coin yang compression sangat panjang (IOTA 256H, BCH 288H) punya ATR
        # sangat kecil karena sudah flat lama → atr * mult bisa tetap kecil.
        # Solusi: T1 fallback dijamin minimal 10% dari harga sekarang,
        # dan T2 minimal 20% — mencerminkan potensi breakout dari compression panjang.
        comp_len  = compression_zone["length"]
        atr_mult  = min(4.0 + comp_len / 48, 10.0)
        t1_atr    = entry + atr * atr_mult
        t1_min    = cur * 1.10   # minimum 10% dari harga sekarang
        t1        = max(t1_atr, t1_min)
        t2        = max(t1 * 1.20, cur * 1.22)  # minimum 22% dari harga sekarang

    # ── FIX: pastikan T1 dan T2 berbeda secara meaningful ────────────────────
    if abs(t2 - t1) / t1 < 0.03:   # jika T1 dan T2 terlalu dekat (< 3% beda)
        t2 = t1 * 1.15             # paksa T2 = T1 + 15%

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
#  🔬  FORENSIC PATTERN DETECTORS (v2.6)
#  Tiga pola yang TERBUKTI konsisten di XAN/MYX/CUS sebelum pump besar.
#  Dibuktikan dari analisa candle-by-candle data forensik nyata.
# ══════════════════════════════════════════════════════════════════════════════

def detect_higher_lows(candles, lookback=4):
    """
    Higher lows = harga tidak mau turun lebih rendah dari low sebelumnya.
    Ini tanda smart money akumulasi — ada buyer yang masuk setiap dip.

    Forensik:
      XAN: lows [0.00713, 0.00730, 0.00720, 0.00736] → YES (toleransi 1.5%) ✅
      CUS: lows [0.05431, 0.05423, 0.05426] → YES ✅
      MYX: lows [0.34190, 0.34330, 0.34820, 0.35440] → YES ✅ (naik terus)

    Toleransi 1.5%: coin low-price bergerak 0.3-0.5% per candle, noise lebih
    besar secara proporsional. Toleransi ketat 0.5% terlalu sensitif.
    """
    if len(candles) < lookback:
        return False
    recent = candles[-lookback:]
    lows   = [c["low"] for c in recent]
    # Higher low dengan toleransi 1.5% untuk absorb noise candle pendek
    for i in range(1, len(lows)):
        if lows[i] < lows[i-1] * 0.985:   # toleransi 1.5%
            return False
    return True


def detect_price_acceleration(candles, lookback=6):
    """
    Price acceleration = separuh terakhir window menunjukkan drift positif.
    Forensik: XAN +2.16%, MYX +4.25%, CUS +1.10% di separuh terakhir.

    Versi relaxed: cukup late drift > 0.3% (positif minimal).
    Strict (late > early) terlalu ketat karena awal window bisa sudah naik duluan.
    """
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
    # Separuh terakhir naik minimal 0.3% — price building momentum
    return late_drift > 0.3


def detect_pre_pump_candle(candles):
    """
    Candle terakhir sebelum spike menunjukkan bull body signifikan.
    Terbukti di semua 3 coin: XAN 48% body, CUS 32% body, MYX 57% body.

    Berbeda dari analyze_candle_structure (cek candle terbaru = spike candle),
    fungsi ini cek candle ke-2 dari belakang = candle SEBELUM spike.
    Tujuan: konfirmasi bahwa accumulation candle terakhir bullish.

    Return (score, label):
      +5 jika body bullish ≥ 30% dari range
      +3 jika close di atas midpoint candle (bullish bias)
      +0 jika bearish
    """
    if len(candles) < 2:
        return 0, "insufficient data"

    c   = candles[-2]  # candle SEBELUM terbaru (pre-spike candle)
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
#  🧠  MASTER SCORE — INTI SCANNER v2.0
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    # ── Ambil candle 1H ──────────────────────────────────────────────────────
    c1h = get_candles(symbol, "1h", CONFIG["candle_limit_1h"])
    if len(c1h) < 72:  # minimal 3 hari data
        return None

    try:
        chg_24h = float(ticker.get("change24h", 0)) * 100
        vol_24h = float(ticker.get("quoteVolume", 0))
        price   = float(ticker.get("lastPr", 0))
    except:
        return None

    if price <= 0:
        return None

    # ── Gate: harga tidak sedang pompa duluan ────────────────────────────────
    # Jika coin sudah naik >40% dalam 24 jam, kemungkinan sudah terlambat
    if chg_24h > 40.0:
        log.info(f"  {symbol}: SKIP chg_24h={chg_24h:.1f}% sudah naik duluan")
        return None

    # ── FASE 1: Cari Compression Zone ────────────────────────────────────────
    compression = find_compression_zone(c1h)
    if compression is None:
        log.info(f"  {symbol}: SKIP tidak ada compression zone yang valid")
        return None

    comp_low    = compression["low"]
    comp_high   = compression["high"]
    comp_avg_vol = compression["avg_vol"]
    comp_length  = compression["length"]
    comp_age     = compression["age_candles"]

    log.info(f"  {symbol}: Compression found len={comp_length} age={comp_age} "
             f"range={compression['range_pct']*100:.1f}% "
             f"candle_range={compression.get('avg_candle_range',0)*100:.2f}% "
             f"spikes={compression.get('spike_count',0)}")

    # ── Gate: compression tidak boleh terlalu tua (zona kadaluarsa) ──────────
    # Jika zona compression berakhir > 72 jam lalu dan volume belum spike, skip
    if comp_age > 72 and compression["quality"] < 50:
        log.info(f"  {symbol}: SKIP compression terlalu tua (age={comp_age}h)")
        return None

    # ── Gate: harga masih di dekat zona compression ──────────────────────────
    price_now = c1h[-1]["close"]
    rise_from_low = (price_now - comp_low) / comp_low if comp_low > 0 else 999

    if rise_from_low > CONFIG["max_rise_from_low_pct"]:
        log.info(f"  {symbol}: SKIP sudah naik {rise_from_low*100:.1f}% dari low compression — terlambat")
        return None

    # ── Gate: TREND KONTEKS — harga tidak boleh terlalu jauh di bawah zona ───
    # Bug dari data nyata (ALCH/BANK): harga di bawah zona compression tapi
    # spike candle hijau → scanner lolos. Ini adalah bounce kecil dalam downtrend.
    #
    # Logika:
    #   - Harga DALAM zona (comp_low ≤ price ≤ comp_high): ideal, coin di support
    #   - Harga SEDIKIT di bawah zona (< 3%): toleransi liq sweep, masih valid
    #   - Harga JAUH di bawah zona (> 3%): downtrend aktif, zona sudah tidak relevan
    #
    # ALCH: comp_low $0.0724, harga $0.0692 → 4.4% di bawah → downtrend, SKIP
    # BANK: comp_low $0.0365, harga $0.0380 → +4.1% di atas → VALID (ini bounce)
    price_below_zone_pct = (comp_low - price_now) / comp_low if price_now < comp_low else 0

    if price_below_zone_pct > CONFIG["price_below_zone_max"]:
        log.info(f"  {symbol}: SKIP downtrend aktif — harga {price_below_zone_pct*100:.1f}% "
                 f"di bawah zona compression (max={CONFIG['price_below_zone_max']*100:.0f}%)")
        return None

    # ── Gate: MA trend — konfirmasi downtrend dengan gap MA signifikan ────────
    # Revisi penting: gate lama (MA20 < MA50 + keduanya turun) terlalu sensitif.
    # Selama COMPRESSION, MA20 dan MA50 hampir sama dan bisa saling mendahului
    # karena oscillasi kecil → false positive pada TRUMP/PIXEL/VVV type.
    #
    # Fix: bearish HANYA jika gap MA20-MA50 > 2.5% (downtrend sejati punya gap besar)
    # ALCH gap = 3.0% → SKIP ✅ | TRUMP gap = 1.2% → LOLOS ✅
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
            log.info(f"  {symbol}: SKIP MA bearish — gap MA={ma_gap*100:.1f}% > 2.5%, "
                     f"MA20={ma20_now:.6g} < MA50={ma50_now:.6g}, keduanya turun")
            return None

    # ── Gate: POST-PUMP DETECTION (diperbarui v2.8) ─────────────────────────
    # v2.6 original: window 6H, threshold 7x — menangkap GAS/BABYDOGE (pump baru)
    # v2.8 extended: tambah window 48H, threshold 4x — menangkap 2ZUSDT (pump 2 hari lalu)
    #
    # Kalibrasi 48H window dari data forensik:
    #   XAN pre-pump  : avg_48H ≈ 2-3x comp_avg → LOLOS ✅
    #   MYX pre-pump  : avg_48H ≈ 1-2x comp_avg → LOLOS ✅
    #   2Z post-pump  : avg_48H ≈ 4-5x comp_avg → SKIP  ✅  (pump 48-72 jam lalu)
    #   GAS post-pump : avg_6H  = 150x           → SKIP  ✅  (sudah tangkap di 6H gate)
    #
    # Dua gate terpisah: 6H untuk pump sangat baru, 48H untuk pump beberapa hari lalu.
    if len(c1h) >= CONFIG["post_pump_lookback_candles"]:
        lookback_n   = CONFIG["post_pump_lookback_candles"]
        avg_vol_last = sum(c["volume_usd"] for c in c1h[-lookback_n:]) / lookback_n
        post_pump_ratio = avg_vol_last / comp_avg_vol if comp_avg_vol > 0 else 0

        if post_pump_ratio > CONFIG["post_pump_vol_mult"]:
            log.info(f"  {symbol}: SKIP post-pump (6H) — avg_{lookback_n}h = "
                     f"{post_pump_ratio:.1f}x comp_avg "
                     f"(threshold={CONFIG['post_pump_vol_mult']}x)")
            return None

    # Gate 48H: deteksi pump yang terjadi 6-48 jam yang lalu (2Z-type)
    lookback_48 = CONFIG.get("post_pump_lookback_48h", 48)
    thresh_48   = CONFIG.get("post_pump_vol_mult_48h", 4.0)
    if len(c1h) >= lookback_48:
        avg_48h = sum(c["volume_usd"] for c in c1h[-lookback_48:]) / lookback_48
        ratio_48 = avg_48h / comp_avg_vol if comp_avg_vol > 0 else 0
        if ratio_48 > thresh_48:
            log.info(f"  {symbol}: SKIP post-pump (48H) — avg_48h = "
                     f"{ratio_48:.1f}x comp_avg (threshold={thresh_48}x) "
                     f"— kemungkinan pump beberapa hari lalu")
            return None

    # ── Gate: G3 BREAKOUT — harga belum keluar dari zona ke atas ─────────────
    # Forensik XAN/MYX: alert harus keluar saat harga masih di dalam zona.
    # Jika price_now sudah > comp_high × 1.03 → breakout sedang/sudah terjadi.
    # Tidak bertabrakan dengan post_pump gate karena:
    #   - post_pump cek VOLUME historis 6H (dimensi volume)
    #   - G3 cek HARGA sekarang vs batas atas zona (dimensi harga/posisi)
    # Coin yang baru mulai breakout (1 candle) akan di-skip G3.
    # Coin yang sudah pump lama lalu balik ke zona: lolos G3, di-skip post_pump.
    price_above_zone_pct = (price_now - comp_high) / comp_high if price_now > comp_high else 0
    if price_above_zone_pct > CONFIG.get("price_above_zone_max", 0.03):
        log.info(f"  {symbol}: SKIP G3 breakout — harga {price_above_zone_pct*100:.1f}% "
                 f"di atas comp_high ${comp_high:.6g}")
        return None
    awakening = detect_volume_awakening(c1h, comp_avg_vol)

    if not awakening["detected"]:
        log.info(f"  {symbol}: SKIP volume belum bangun (best_mult={awakening['best_mult']:.1f}x)")
        return None

    # ── Gate: SELLING CLIMAX — spike merah + harga di bawah zona ────────────
    # CAKE case: spike 12.1x tapi candle merah + harga di bawah zona = dump.
    spike_candle     = c1h[-awakening["spike_candle"]] if awakening["spike_candle"] >= 1 else c1h[-1]
    spike_is_red     = spike_candle["close"] < spike_candle["open"]
    price_below_zone = price_now < comp_low * 0.99

    if spike_is_red and price_below_zone:
        log.info(f"  {symbol}: SKIP selling climax — spike merah + harga di bawah zona compression")
        return None

    log.info(f"  {symbol}: Volume awakening! {awakening['best_mult']:.1f}x compression avg")

    # ── Funding gate ─────────────────────────────────────────────────────────
    funding = get_funding(symbol)
    if funding < CONFIG["funding_gate"]:
        log.info(f"  {symbol}: SKIP funding terlalu negatif ({funding:.5f})")
        return None

    # ── Hitung RVOL (relatif vs jam yang sama) ────────────────────────────────
    # Dilakukan di sini agar bisa dipakai sebagai gate sebelum scoring penuh
    if len(c1h) >= 25:
        last_vol       = c1h[-2]["volume_usd"]
        target_hour    = (c1h[-2]["ts"] // 3_600_000) % 24
        same_hour_vols = [c["volume_usd"] for c in c1h[:-2]
                          if (c["ts"] // 3_600_000) % 24 == target_hour]
        avg_same_hour  = sum(same_hour_vols) / len(same_hour_vols) if same_hour_vols else 1
        rvol           = last_vol / avg_same_hour if avg_same_hour > 0 else 1.0
    else:
        rvol = 1.0

    # ── Gate: RVOL minimum ────────────────────────────────────────────────────
    # Data nyata: THETA RVOL=1.2x lolos scoring tapi tidak ada awakening nyata.
    # RVOL dihitung vs jam yang sama → lebih jujur dari vol_mult (yang vs avg compression).
    # Kedua sinyal harus konfirmasi bersamaan.
    if rvol < CONFIG["min_rvol_gate"]:
        log.info(f"  {symbol}: SKIP RVOL={rvol:.2f}x terlalu rendah (min={CONFIG['min_rvol_gate']}x)")
        return None

    # ── Metrik tambahan ───────────────────────────────────────────────────────
    rsi          = get_rsi(c1h[-50:], 14)
    atr_7        = calc_atr(c1h[-10:],  7) or price_now * 0.02
    atr_30       = calc_atr(c1h[-33:], 30) or price_now * 0.02
    vol_compress = (atr_7 / atr_30) < 0.75 if atr_30 > 0 else False
    liq_sweep    = detect_liquidity_sweep(c1h, comp_low)
    candle_score, candle_label = analyze_candle_structure(c1h[-1])

    # ── Forensic pattern detectors (v2.6) ────────────────────────────────────
    # Tiga pola yang terbukti konsisten sebelum pump besar dari data forensik
    # XAN/CUS/MYX — lebih reliable dari RSI atau candle structure saja.
    higher_lows        = detect_higher_lows(c1h, lookback=4)
    price_accel        = detect_price_acceleration(c1h, lookback=6)
    pre_spike_sc, pre_spike_label = detect_pre_pump_candle(c1h)

    # ── SCORING ───────────────────────────────────────────────────────────────
    score = 0
    score_breakdown = []

    # [30] Compression quality
    # Skor berdasarkan panjang zona dan ketatnya range
    comp_score = 0
    if comp_length >= 36:   comp_score += 10
    if comp_length >= 72:   comp_score += 8    # 3+ hari
    if comp_length >= 168:  comp_score += 7    # 7+ hari (seperti VVV)
    if comp_length >= 336:  comp_score += 5    # 14+ hari (seperti TRUMP)
    # Bonus range sangat ketat
    if compression["range_pct"] < 0.04:  comp_score += 5  # range < 4%
    comp_score = min(comp_score, 30)
    score += comp_score
    score_breakdown.append(f"Compression: +{comp_score} (len={comp_length}h, range={compression['range_pct']*100:.1f}%)")

    # [25] Volume awakening
    vol_score = 0
    mult = awakening["best_mult"]
    if mult >= CONFIG["awakening_vol_mult"]:    vol_score += 10  # ≥ 1.8x
    if mult >= CONFIG["strong_awakening_mult"]: vol_score += 8   # ≥ 3x
    if mult >= CONFIG["mega_awakening_mult"]:   vol_score += 7   # ≥ 6x (PIXEL-level)
    if awakening["is_green"]:                   vol_score += 3   # spike candle hijau
    if awakening["spike_candle"] == 1:          vol_score += 2   # spike di candle TERBARU
    vol_score = min(vol_score, 25)
    score += vol_score
    score_breakdown.append(f"Vol awakening: +{vol_score} ({mult:.1f}x, {'hijau' if awakening['is_green'] else 'merah'})")

    # [20] Support proximity
    # Seberapa dekat harga ke support (bawah zona compression)
    prox_score = 0
    if rise_from_low <= 0.02:   prox_score = 20  # dalam 2% dari low — ideal
    elif rise_from_low <= 0.04: prox_score = 15  # dalam 4%
    elif rise_from_low <= 0.06: prox_score = 10  # dalam 6%
    elif rise_from_low <= 0.09: prox_score = 5   # dalam 9%
    else:                       prox_score = 2   # 9-12% — masih valid, spike candle
    score += prox_score
    score_breakdown.append(f"Proximity: +{prox_score} ({rise_from_low*100:.1f}% dari low)")

    # [15] Candle structure
    score += candle_score
    score_breakdown.append(f"Candle: +{candle_score} ({candle_label})")

    # [10] RSI momentum
    rsi_score = 0
    if rsi < 30:    rsi_score = 10  # oversold kuat
    elif rsi < 38:  rsi_score = 7   # oversold sedang
    elif rsi < 45:  rsi_score = 4   # mendekati netral
    else:           rsi_score = 2   # netral — tetap dapat poin minimal
                                    # (RSI tinggi saat compression bukan masalah
                                    # jika volume spike baru terjadi)
    score += rsi_score
    score_breakdown.append(f"RSI: +{rsi_score} (RSI={rsi:.0f})")

    # Bonus: liquidity sweep (pola ORCA/PIXEL)
    if liq_sweep:
        score += 8
        score_breakdown.append("Liq sweep: +8 (false breakdown terdeteksi)")

    # Bonus: volatility compression (coil makin ketat)
    if vol_compress:
        score += 5
        score_breakdown.append("Vol compress: +5 (ATR7/ATR30 < 0.75)")

    # ── Bonus forensik v2.6 — terbukti dari data nyata XAN/CUS/MYX ──────────

    # Higher lows (+8): smart money akumulasi, tidak mau biarkan harga turun
    # Terbukti di semua 3 coin sebelum pump besar
    if higher_lows:
        score += 8
        score_breakdown.append("Higher lows: +8 (akumulasi terdeteksi)")

    # Price acceleration (+7): momentum membangun, harga naik lebih cepat
    # CUS: -2.8% → +1.1%, MYX: -1.5% → +4.3% sebelum pump
    if price_accel:
        score += 7
        score_breakdown.append("Price accel: +7 (momentum membangun)")

    # Pre-spike candle bullish (+3 atau +5): candle sebelum spike bull body
    # XAN: 48%, CUS: 32%, MYX: 57% — semua hijau sebelum meledak
    if pre_spike_sc > 0:
        score += pre_spike_sc
        score_breakdown.append(f"Pre-spike: +{pre_spike_sc} ({pre_spike_label})")

    # Penalti: funding sangat negatif
    if funding < -0.001:
        score -= 5
        score_breakdown.append(f"Funding penalty: -5 ({funding:.5f})")

    # Penalti: zone terlalu tua
    if comp_age > 48:
        penalty = min((comp_age - 48) // 12, 10)
        score -= penalty
        score_breakdown.append(f"Age penalty: -{penalty} (zone berakhir {comp_age}h lalu)")

    log.info(f"  {symbol}: Score={score} breakdown={score_breakdown}")

    # ── Gate skor minimum ─────────────────────────────────────────────────────
    if score < CONFIG["score_threshold"]:
        return None

    # ── Hitung entry & target ─────────────────────────────────────────────────
    entry_data = calc_entry_targets(c1h, compression)
    if not entry_data:
        log.info(f"  {symbol}: SKIP entry_data gagal dihitung")
        return None
    if entry_data["t1_pct"] < CONFIG["min_target_pct"]:
        log.info(f"  {symbol}: SKIP T1={entry_data['t1_pct']:.1f}% < min_target={CONFIG['min_target_pct']}%")
        return None

    # ── Gate: minimum R/R ────────────────────────────────────────────────────
    # Data nyata IOTX: R/R=0 karena SL terlalu dekat — tidak layak di-trade
    if entry_data["rr"] < CONFIG["min_rr"]:
        log.info(f"  {symbol}: SKIP R/R={entry_data['rr']} terlalu kecil (min={CONFIG['min_rr']})")
        return None

    # ── Estimasi urgency: seberapa cepat koin ini mungkin bergerak ────────────
    # Berdasarkan panjang compression dan kekuatan volume awakening
    if awakening["best_mult"] >= 6.0 and comp_length >= 168:
        urgency = "🔴 SANGAT TINGGI — mega spike, bisa pump dalam 1-3 jam"
    elif awakening["best_mult"] >= 3.0 or comp_length >= 168:
        urgency = "🟠 TINGGI — potensi pump dalam 6-24 jam"
    elif awakening["best_mult"] >= 1.8 and comp_length >= 72:
        urgency = "🟡 SEDANG — potensi pump dalam 12-48 jam"
    else:
        urgency = "⚪ WATCH — sedang membangun momentum"

    return {
        "symbol":          symbol,
        "score":           score,
        "composite_score": score,
        "compression":     compression,
        "awakening":       awakening,
        "entry":           entry_data,
        "liq_sweep":       liq_sweep,
        "candle_label":    candle_label,        # struktur candle TERBARU
        "spike_candle_green": awakening["is_green"],  # warna candle SPIKE (berbeda!)
        "pre_spike_label": pre_spike_label,     # struktur candle sebelum spike
        "higher_lows":     higher_lows,
        "price_accel":     price_accel,
        "rsi":             rsi,
        "vol_compress":    vol_compress,
        "funding":         funding,
        "rvol":            round(rvol, 1),
        "price":           price_now,
        "chg_24h":         chg_24h,
        "vol_24h":         vol_24h,
        "rise_from_low":   rise_from_low,
        "sector":          SECTOR_LOOKUP.get(symbol, "OTHER"),
        "urgency":         urgency,
        "score_breakdown": score_breakdown,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def build_alert(r, rank=None):
    sc   = r["score"]
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

    # ── FIX: pisahkan label spike candle vs candle terbaru ────────────────────
    # Bug lama: baris "Candle" muncul dua kali dengan makna berbeda — membingungkan.
    # Sekarang:
    #   "Candle spike" = kondisi candle yang volume-nya spike (is_green dari awakening)
    #   "Candle kini"  = struktur candle paling terbaru (candle_label dari analyze_candle_structure)
    # Keduanya informatif tapi harus jelas konteksnya.
    spike_candle_str = (
        f"{'Hijau ✅' if awk['is_green'] else 'Merah ⚠️'}"
        f"{'  🔥 MEGA SPIKE!' if awk['is_mega'] else ''}"
    )
    # Jika spike terjadi di candle terbaru, "candle kini" dan "candle spike" adalah sama
    spike_is_current = awk["spike_candle"] == 1
    current_candle_str = r["candle_label"]

    # ── Forensik bonus summary ─────────────────────────────────────────────────
    forensic_checks = []
    if r.get("higher_lows"):  forensic_checks.append("Higher lows ✅")
    if r.get("price_accel"):  forensic_checks.append("Price accel ✅")
    pre_sc = r.get("pre_spike_label", "")
    if pre_sc and "bull" in pre_sc: forensic_checks.append("Pre-spike bull ✅")
    if r.get("liq_sweep"):    forensic_checks.append("Liq sweep ✅")
    forensic_str = "  " + " · ".join(forensic_checks) if forensic_checks else "  — tidak ada pola akumulasi"

    msg = (
        f"🚀 <b>PRE-PUMP SIGNAL {rk}— v2.6</b>\n\n"
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
        vwap_line = f"  VWAP  : ${e['vwap']}\n" if e.get("vwap") else ""
        poc_line  = f"  POC   : ${e['z2']}\n"   if e.get("z2")   else ""
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>ENTRY &amp; TARGET</b>\n"
            f"{vwap_line}"
            f"{poc_line}"
            f"  Entry : ${e['entry']}\n"
            f"  SL    : ${e['sl']}  (-{e['sl_pct']:.1f}%)\n"
            f"  T1    : ${e['t1']}  (+{e['t1_pct']:.1f}%)\n"
            f"  T2    : ${e['t2']}  (+{e['t2_pct']:.1f}%)\n"
            f"  R/R   : 1:{e['rr']}  |  ATR: ${e['atr']}\n"
        )

    msg += f"\n🕐 {utc_now()}\n<i>⚠️ Bukan financial advice. DYOR.</i>"
    return msg


def build_summary(results):
    msg  = f"📋 <b>PRE-PUMP WATCHLIST — {utc_now()}</b>\n{'━'*30}\n"
    for i, r in enumerate(results, 1):
        comp  = r["compression"]
        awk   = r["awakening"]
        vol   = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                 else f"${r['vol_24h']/1e3:.0f}K")
        days  = comp["length"] / 24
        msg  += (
            f"{i}. <b>{r['symbol']}</b> [S:{r['score']}]\n"
            f"   Coil {days:.1f}d · Vol {awk['best_mult']:.1f}x · "
            f"T1:+{r['entry']['t1_pct']:.0f}% · {vol}\n"
        )
    return msg


# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    candidates    = []
    not_found     = []
    stats         = defaultdict(int)

    log.info("=" * 70)
    log.info(f"🔍 SCANNING {len(WHITELIST_SYMBOLS)} coin — PRE-PUMP DETECTION v2.6")
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
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v2.8 — {utc_now()} ===")

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

        # Final volume check
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

    # Sort: utamakan score tinggi, tapi juga pertimbangkan rise_from_low rendah
    results.sort(key=lambda x: x["score"], reverse=True)

    log.info(f"\n{'='*70}")
    log.info(f"✅ Total sinyal lolos: {len(results)} coin")
    log.info(f"{'='*70}\n")

    if not results:
        log.info("Tidak ada sinyal pre-pump saat ini")
        return

    top = results[:CONFIG["max_alerts_per_run"]]

    # Kirim summary dulu
    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    # Kirim detail per coin
    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(f"📤 Alert #{rank}: {r['symbol']} Score={r['score']}")
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert dikirim — {utc_now()} ===")


# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔════════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v2.8                            ║")
    log.info("║  Deteksi transisi Fase Tidur → Fase Bangun        ║")
    log.info("║  Target: entry sekarang, TP 1-2 hari (+10-100%)  ║")
    log.info("╚════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)

    run_scan()
