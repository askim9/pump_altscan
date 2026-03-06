"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v14.0                                                      ║
║                                                                              ║
║  REWRITE TOTAL — Berbasis riset analisis_pump_vs_nonpump_v2                 ║
║                                                                              ║
║  PERUBAHAN UTAMA dari v13.3:                                                 ║
║  1. above_vwap jadi GATE WAJIB (riset: 95.74% pre-pump di atas VWAP)        ║
║  2. ema_gap DIIMPLEMENTASIKAN (lift 3.73x — sinyal terkuat di riset!)        ║
║  3. Funding gate DIPERKETAT: avg < -0.0005, cumul < -0.05                   ║
║  4. BBW threshold DIPERBAIKI ke format desimal (0.06/0.10)                  ║
║  5. Funding streak dihitung dari FULL HISTORY bukan hanya last6              ║
║  6. Funding streak threshold: >= 5 (bukan >= 10 yang mustahil dicapai)      ║
║  7. Entry logic DIBALIK: HIGH → di atas bos_level, MEDIUM → di atas VWAP   ║
║  8. SL diperketat: max 1% dari entry (riset: 0.5-1%)                        ║
║  9. Filter manipulasi: volume harus konsisten >= 2 candle, bukan 1 spike    ║
║  10. MACD DIHAPUS (perhitungan di v13 salah, tidak ada di riset)             ║
║  11. Funding I/O menjadi BATCH (load sekali, save sekali di akhir scan)     ║
║  12. EXCLUDED_KEYWORDS sekarang AKTIF digunakan                              ║
║  13. Gain dihitung dari ENTRY, bukan dari harga sekarang                     ║
║  14. get_rank & get_ath_distance DIHAPUS (placeholder, distorsi skor)       ║
║                                                                              ║
║  BOBOT SKOR — sesuai urutan lift riset:                                     ║
║    ema_gap >= 1.0    : +5 (lift 3.73x — tertinggi)                          ║
║    atr_pct >= 1.5%   : +4 (lift 3.31x)                                      ║
║    bbw >= 0.10       : +4 (lift 2.88x)                                       ║
║    bos+vwap          : +4 (lift 3.72x kombinasi)                             ║
║    bos_up saja       : +3 (lift 2.81x)                                       ║
║    atr_pct >= 1.0%   : +3                                                    ║
║    bbw >= 0.06       : +3                                                    ║
║    funding_neg_pct   : +3                                                    ║
║    funding_streak    : +3                                                    ║
║    higher_low        : +2 (lift 1.73x)                                       ║
║    bb_squeeze        : +2 (lift 1.34x)                                       ║
║    rsi >= 65         : +2 (lift 2.46x, bobot turun sesuai riset)             ║
║    funding_cumul     : +2                                                    ║
║    vol_ratio         : +2                                                    ║
║    vol_accel         : +2                                                    ║
║    rsi >= 55         : +1                                                    ║
║    price_chg >= 0.5% : +1                                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import requests
import time
import os
import math
import json
import logging
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

# ── Logging ───────────────────────────────────────────────────────────────────
_log_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_root = logging.getLogger()
_log_root.setLevel(logging.INFO)

_ch = logging.StreamHandler()
_ch.setFormatter(_log_fmt)
_log_root.addHandler(_ch)

_fh = _lh.RotatingFileHandler(
    "/tmp/scanner_v14.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Scanner v14.0 — log aktif: /tmp/scanner_v14.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────────────────────
    "min_score_alert":          10,
    "max_alerts_per_run":       15,

    # ── Volume 24h total (USD) ────────────────────────────────────────────────
    "min_vol_24h":           3_000,
    "max_vol_24h":      50_000_000,
    "pre_filter_vol":        1_000,

    # ── Gate perubahan harga ──────────────────────────────────────────────────
    "gate_chg_24h_max":         30.0,

    # ── Funding Gate WAJIB (diperketat sesuai riset) ──────────────────────────
    # Riset: pump coins avg = -0.0006, threshold riset = -0.0005
    # Sebelumnya: -0.0001 (terlalu longgar)
    "funding_gate_avg":        -0.0005,
    # Riset: threshold cumulative = -0.05
    # Sebelumnya: -0.02 (terlalu longgar)
    "funding_gate_cumul":      -0.05,

    # ── Candle limits ─────────────────────────────────────────────────────────
    "candle_1h":                168,
    "candle_15m":                96,

    # ── Entry / SL (berbasis riset) ───────────────────────────────────────────
    # HIGH alert: entry 0.1% di atas bos_level
    "entry_bos_buffer":         0.001,
    # MEDIUM alert: entry 0.1% di atas VWAP
    "entry_vwap_buffer":        0.001,
    # SL: 0.5% di bawah VWAP (riset: primary SL)
    "sl_vwap_offset":           0.005,
    # SL: 1% di bawah low terbaru (riset: secondary SL)
    "sl_low_offset":            0.010,
    # SL maksimum dari entry (tidak boleh lebih dari 1%)
    "max_sl_pct":               1.0,

    # ── Operasional ───────────────────────────────────────────────────────────
    "alert_cooldown_sec":      1800,
    "sleep_coins":              0.8,
    "sleep_error":              3.0,
    "cooldown_file":           "./cooldown.json",
    "funding_snapshot_file":   "./funding.json",

    # ── Bobot skor (urutan lift dari riset) ───────────────────────────────────
    "score_ema_gap":            5,   # ema_gap >= 1.0 → lift 3.73x (TERTINGGI)
    "score_atr_15":             4,   # atr >= 1.5%   → lift 3.31x
    "score_bbw_10":             4,   # bbw >= 0.10   → lift 2.88x
    "score_above_vwap_bos":     4,   # bos+vwap      → lift 3.72x (kombinasi)
    "score_bos_up":             3,   # bos_up saja   → lift 2.81x
    "score_atr_10":             3,   # atr >= 1.0%
    "score_bbw_6":              3,   # bbw >= 0.06
    "score_funding_neg_pct":    3,   # funding_neg_pct >= 70%
    "score_funding_streak":     3,   # funding_streak >= 5
    "score_higher_low":         2,   # higher_low    → lift 1.73x
    "score_bb_squeeze":         2,   # bb_squeeze    → lift 1.34x
    "score_rsi_65":             2,   # rsi >= 65     → lift 2.46x (bobot turun)
    "score_funding_cumul":      2,   # funding_cumul <= -0.05
    "score_vol_ratio":          2,   # vol_ratio > 2.5x (hanya jika konsisten)
    "score_vol_accel":          2,   # vol_accel > 50%
    "score_rsi_55":             1,   # rsi >= 55
    "score_price_chg":          1,   # price_chg >= 0.5% dalam 1h

    # ── Threshold indikator ───────────────────────────────────────────────────
    "above_vwap_rate_min":      0.6,   # 60% dari 6 candle terakhir di atas VWAP
    "ema_gap_threshold":        1.0,   # close / EMA20 >= 1.0 (harga di atas EMA)
    # BBW format desimal (0.06 = 6%, threshold dari riset)
    "bbw_threshold_high":       0.10,
    "bbw_threshold_mid":        0.06,
    # BB Squeeze: band sempit < 4% (konsolidasi sebelum breakout)
    "bb_squeeze_threshold":     0.04,
    "vol_ratio_threshold":      2.5,
    "vol_accel_threshold":      0.5,   # 50%
    # Funding streak minimum (riset: >= 5 mencakup 50% pump coins)
    "funding_streak_min":       5,
}

MANUAL_EXCLUDE = set()

# ── Keyword yang dikecualikan (sekarang AKTIF dipakai) ───────────────────────
EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST"]

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin pilihan
# ══════════════════════════════════════════════════════════════════════════════
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

GRAN_MAP    = {"15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
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
#  💾  FUNDING SNAPSHOTS — batch I/O (load sekali, save sekali)
# ══════════════════════════════════════════════════════════════════════════════
# Semua snapshot disimpan di memori selama scan, baru ditulis ke disk di akhir.
# Sebelumnya: baca+tulis disk untuk setiap coin = 648 disk I/O per scan.
_funding_snapshots = {}

def load_funding_snapshots():
    """Load semua snapshot ke memori di awal scan."""
    global _funding_snapshots
    try:
        p = CONFIG["funding_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                _funding_snapshots = json.load(f)
    except Exception:
        _funding_snapshots = {}

def save_all_funding_snapshots():
    """Tulis semua snapshot ke disk sekaligus di akhir scan."""
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(_funding_snapshots, f)
    except Exception as e:
        log.warning(f"Gagal simpan funding snapshot: {e}")

def add_funding_snapshot(symbol, funding_rate):
    """Tambahkan satu data point ke snapshot in-memory."""
    now = time.time()
    if symbol not in _funding_snapshots:
        _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({"ts": now, "funding": funding_rate})
    # Simpan maksimum 20 data terakhir per coin
    _funding_snapshots[symbol] = sorted(
        _funding_snapshots[symbol], key=lambda x: x["ts"]
    )[-20:]

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=12):
    for attempt in range(2):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
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
    except Exception:
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
            "symbol": symbol,
            "granularity": g,
            "limit": str(limit),
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
                "open":     float(c[1]),
                "high":     float(c[2]),
                "low":      float(c[3]),
                "close":    float(c[4]),
                "volume":   float(c[5]),
                "volume_usd": vol_usd,
            })
        except Exception:
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
        except Exception:
            pass
    return 0.0

def get_funding_stats(symbol):
    """
    Hitung statistik funding dari snapshot in-memory.
    - avg6: rata-rata 6 periode terakhir
    - cumulative: kumulatif 6 periode terakhir
    - neg_pct: % periode negatif dari 6 terakhir
    - streak: streak negatif berturut dari FULL HISTORY (bukan hanya last6)
              Sebelumnya dihitung dari last6 saja sehingga max=6, threshold 10 mustahil.
    """
    snaps = _funding_snapshots.get(symbol, [])
    if len(snaps) < 2:
        return None

    all_rates = [s["funding"] for s in snaps]

    last6      = all_rates[-6:]
    avg6       = sum(last6) / len(last6)
    cumul      = sum(last6)
    neg_pct    = sum(1 for f in last6 if f < 0) / len(last6) * 100

    # Streak dihitung dari seluruh history (max 20 data)
    streak = 0
    for f in reversed(all_rates):
        if f < 0:
            streak += 1
        else:
            break

    basis = all_rates[-1] * 100   # basis dalam persen dari funding terkini

    return {
        "avg":          avg6,
        "cumulative":   cumul,
        "neg_pct":      neg_pct,
        "streak":       streak,
        "basis":        basis,
        "current":      all_rates[-1],
        "sample_count": len(all_rates),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  📊  INDIKATOR TEKNIKAL
# ══════════════════════════════════════════════════════════════════════════════

def _calc_ema_series(values, period):
    """
    Menghitung nilai EMA yang benar:
    seed awal = SMA(period), lalu setiap nilai berikutnya pakai smoothing alpha.
    Mengembalikan nilai EMA terakhir.
    """
    if len(values) < period:
        return None
    alpha   = 2.0 / (period + 1)
    ema_val = sum(values[:period]) / period   # seed = SMA
    for v in values[period:]:
        ema_val = alpha * v + (1.0 - alpha) * ema_val
    return ema_val

def calc_ema_gap(candles, period=20):
    """
    ema_gap = close_terakhir / EMA(period)

    Interpretasi:
      > 1.0  → harga di atas EMA → momentum bullish
      < 1.0  → harga di bawah EMA → momentum bearish

    Riset: ema_gap >= 1.0 memiliki lift 3.73x (SINYAL TERKUAT)
    Semakin jauh di atas 1.0, semakin kuat momentum.
    """
    if len(candles) < period + 1:
        return 0.0
    closes  = [c["close"] for c in candles]
    ema_val = _calc_ema_series(closes, period)
    if ema_val is None or ema_val == 0:
        return 0.0
    return candles[-1]["close"] / ema_val

def calc_bbw(candles, period=20):
    """
    BB Width dalam FORMAT DESIMAL (bukan persen).
    0.06 = 6%  → threshold riset untuk sinyal pre-pump
    0.10 = 10% → threshold lebih kuat

    PERBAIKAN dari v13:
    v13 menghitung bbw * 100 (dalam persen) tapi membandingkan dengan 0.12
    sehingga 8% >= 0.12 selalu True → semua coin dapat skor BBW penuh (salah).

    Sekarang konsisten: bbw desimal dibandingkan threshold desimal.
    """
    if len(candles) < period:
        return 0.0, 0.5
    closes   = [c["close"] for c in candles[-period:]]
    mean     = sum(closes) / period
    variance = sum((x - mean) ** 2 for x in closes) / period
    std      = math.sqrt(variance)
    bb_upper = mean + 2 * std
    bb_lower = mean - 2 * std
    bbw      = (bb_upper - bb_lower) / mean if mean > 0 else 0.0
    last     = candles[-1]["close"]
    if bb_upper - bb_lower == 0:
        bb_pct = 0.5
    else:
        bb_pct = (last - bb_lower) / (bb_upper - bb_lower)
    return bbw, bb_pct

def calc_bb_squeeze(candles, period=20):
    """
    BB Squeeze: band sangat sempit (< 4%) → konsolidasi sebelum breakout.
    Riset: lift 1.34x (sinyal lemah tapi tetap bermakna secara statistik).
    """
    bbw, _ = calc_bbw(candles, period)
    return bbw < CONFIG["bb_squeeze_threshold"]

def calc_atr_pct(candles, period=14):
    """
    ATR sebagai % dari harga close terakhir.
    Riset: atr_pct >= 1.0 memiliki lift 3.31x.
    """
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1:
            break
        h  = candles[idx]["high"]
        l  = candles[idx]["low"]
        pc = candles[idx - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if not trs:
        return 0.0
    atr = sum(trs) / len(trs)
    cur = candles[-1]["close"]
    return (atr / cur * 100) if cur > 0 else 0.0

def calc_vwap(candles, lookback=24):
    """VWAP rolling: rata-rata harga tertimbang volume untuk sejumlah candle terakhir."""
    n = min(lookback, len(candles))
    if n == 0:
        return candles[-1]["close"] if candles else 0.0
    recent  = candles[-n:]
    cum_tv  = sum((c["high"] + c["low"] + c["close"]) / 3 * c["volume"] for c in recent)
    cum_v   = sum(c["volume"] for c in recent)
    return (cum_tv / cum_v) if cum_v > 0 else candles[-1]["close"]

def get_rsi(candles, period=14):
    """
    RSI Wilder (smoothed moving average).
    Riset: rsi14 >= 65 memiliki lift 2.46x — lebih rendah dari ema_gap dan atr.
    Bobot skor diturunkan sesuai urutan lift di riset.
    """
    if len(candles) < period + 1:
        return 50.0
    closes = [c["close"] for c in candles]
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
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
    return 100 - (100 / (1 + rs))

def detect_bos_up(candles, lookback=3):
    """
    Break of Structure ke atas: close candle terakhir > high tertinggi
    dari N candle sebelumnya.

    Riset: bos_up memiliki lift 2.81x.
    Kombinasi bos_up + above_vwap: lift 3.72x (tertinggi kombinasi).

    Return: (is_bos: bool, bos_level: float)
    bos_level = level yang di-break, digunakan sebagai referensi entry HIGH.
    """
    if len(candles) < lookback + 1:
        return False, 0.0
    prev_highs = [c["high"] for c in candles[-(lookback + 1):-1]]
    bos_level  = max(prev_highs)
    is_bos     = candles[-1]["close"] > bos_level
    return is_bos, bos_level

def higher_low_detected(candles):
    """
    Higher Low: low candle terakhir lebih tinggi dari semua low dalam 5 candle sebelumnya.
    Riset: lift 1.73x.
    """
    if len(candles) < 6:
        return False
    lows = [c["low"] for c in candles[-6:]]
    return lows[-1] > min(lows[:-1])

def calc_volume_ratio(candles, lookback=24):
    """
    Rasio volume candle terakhir vs rata-rata lookback candle sebelumnya.
    Threshold: > 2.5x
    """
    if len(candles) < lookback + 1:
        return 0.0
    avg_vol = sum(c["volume_usd"] for c in candles[-(lookback + 1):-1]) / lookback
    if avg_vol <= 0:
        return 0.0
    return candles[-1]["volume_usd"] / avg_vol

def calc_volume_acceleration(candles):
    """
    Volume acceleration: volume 1h terakhir vs rata-rata 3h sebelumnya.
    Threshold: > 50% (CONFIG vol_accel_threshold = 0.5)
    """
    if len(candles) < 4:
        return 0.0
    vol_1h  = candles[-1]["volume_usd"]
    vol_3h  = sum(c["volume_usd"] for c in candles[-4:-1]) / 3
    if vol_3h <= 0:
        return 0.0
    return (vol_1h - vol_3h) / vol_3h

def check_volume_consistent(candles, lookback=3, min_ratio=1.5):
    """
    Filter anti-manipulasi: volume tinggi harus konsisten di >= 2 candle terakhir,
    bukan hanya spike 1 candle (tanda pump palsu / manipulasi).

    Logika: dari lookback candle terakhir, minimal separuhnya harus di atas
    rata-rata * min_ratio.
    """
    if len(candles) < 24:
        return False
    avg_vol = sum(c["volume_usd"] for c in candles[-24:]) / 24
    if avg_vol <= 0:
        return False
    recent    = candles[-lookback:]
    above_avg = sum(1 for c in recent if c["volume_usd"] > avg_vol * min_ratio)
    return above_avg >= max(1, lookback // 2)

# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATION
# ══════════════════════════════════════════════════════════════════════════════

def calc_fib_targets(entry, candles):
    """
    Target Fibonacci extension 1.272 dan 1.618 dari swing terdekat.
    Hanya valid jika swing low terjadi sebelum swing high (pola uptrend).
    Fallback ke +8% dan +15% jika pola tidak valid.
    """
    lookback = min(48, len(candles))
    recent   = candles[-lookback:]

    lows  = [(i, c["low"])  for i, c in enumerate(recent)]
    highs = [(i, c["high"]) for i, c in enumerate(recent)]

    low_idx,  swing_low  = min(lows,  key=lambda x: x[1])
    high_idx, swing_high = max(highs, key=lambda x: x[1])

    # Fibonacci extension valid hanya jika low terjadi sebelum high (uptrend)
    if low_idx >= high_idx or (swing_high - swing_low) <= 0:
        return round(entry * 1.08, 8), round(entry * 1.15, 8)

    fib_range = swing_high - swing_low
    t1 = swing_low + fib_range * 1.272
    t2 = swing_low + fib_range * 1.618

    # Pastikan target selalu di atas entry
    if t1 <= entry:
        t1 = entry * 1.08
    if t2 <= t1:
        t2 = t1 * 1.08

    return round(t1, 8), round(t2, 8)

def calc_entry(candles, bos_level, alert_level, vwap, price_now):
    """
    Entry logic berbasis riset (DIBALIK dari v13):

    v13 (salah): entry di support → DI BAWAH harga → tidak pernah terkejar
    v14 (benar): entry mengikuti arah momentum

    HIGH alert (bos_up = True):
      Entry 0.1% di atas bos_level (konfirmasi breakout sudah terjadi)
      Riset: HIGH alert entry di atas bos_up level

    MEDIUM alert:
      Entry 0.1% di atas VWAP (konfirmasi harga di atas equilibrium)
      Riset: MEDIUM alert entry di atas VWAP

    SL (riset: primary = di bawah VWAP 0.5%, secondary = di bawah low 1%):
      Ambil SL yang lebih tinggi dari kedua opsi (tighter = lebih aman).
      Hard limit: SL tidak lebih dari max_sl_pct (1%) di bawah entry.
    """
    # Hitung entry
    if alert_level == "HIGH":
        entry = bos_level * (1.0 + CONFIG["entry_bos_buffer"])
    else:
        entry = vwap * (1.0 + CONFIG["entry_vwap_buffer"])

    # Jika entry masih di bawah harga sekarang (misalnya vwap jauh di bawah cur),
    # gunakan harga sekarang + 0.1% sebagai minimum entry
    if entry < price_now:
        entry = price_now * 1.001

    # Hitung SL
    sl_from_vwap = vwap    * (1.0 - CONFIG["sl_vwap_offset"])
    low_5h       = min(c["low"] for c in candles[-5:]) if len(candles) >= 5 else entry * 0.99
    sl_from_low  = low_5h  * (1.0 - CONFIG["sl_low_offset"])
    # Ambil SL tertinggi (lebih dekat ke entry = lebih konservatif / ketat)
    sl           = max(sl_from_vwap, sl_from_low)
    # Hard limit: tidak lebih dari max_sl_pct di bawah entry
    sl_hard_min  = entry * (1.0 - CONFIG["max_sl_pct"] / 100.0)
    sl           = max(sl, sl_hard_min)

    # Pastikan SL tidak di atas entry (kasus edge case)
    if sl >= entry:
        sl = entry * 0.99

    # Hitung target Fibonacci
    t1, t2 = calc_fib_targets(entry, candles)

    risk   = entry - sl
    reward = t1 - entry
    rr     = round(reward / risk, 1) if risk > 0 else 0.0

    return {
        "entry":          round(entry, 8),
        "sl":             round(sl, 8),
        "sl_pct":         round((entry - sl) / entry * 100, 2),
        "t1":             t1,
        "t2":             t2,
        "rr":             rr,
        "vwap":           round(vwap, 8),
        "bos_level":      round(bos_level, 8),
        "alert_level":    alert_level,
        # Gain dihitung dari ENTRY (bukan dari harga sekarang seperti di v13)
        "gain_t1_pct":    round((t1 - entry) / entry * 100, 1),
        "gain_t2_pct":    round((t2 - entry) / entry * 100, 1),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    c1h  = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c15m = get_candles(symbol, "15m", CONFIG["candle_15m"])

    if len(c1h) < 48:
        log.info(f"  {symbol}: Candle 1h tidak cukup ({len(c1h)} < 48)")
        return None

    # Parse data ticker
    try:
        vol_24h   = float(ticker.get("quoteVolume", 0))
        chg_24h   = float(ticker.get("change24h",  0)) * 100
        price_now = float(ticker.get("lastPr",      0)) or c1h[-1]["close"]
    except Exception:
        return None

    if vol_24h <= 0 or price_now <= 0:
        return None

    # ── GATE 1: Funding (WAJIB, diperketat) ─────────────────────────────────
    funding = get_funding(symbol)
    add_funding_snapshot(symbol, funding)
    fstats  = get_funding_stats(symbol)

    if fstats is None:
        log.info(f"  {symbol}: Funding snapshot belum cukup (min 2 data — "
                 f"normal di run pertama)")
        return None

    gate_funding = (
        fstats["avg"] < CONFIG["funding_gate_avg"] or
        fstats["cumulative"] < CONFIG["funding_gate_cumul"]
    )
    if not gate_funding:
        log.info(
            f"  {symbol}: Funding tidak cukup negatif "
            f"(avg={fstats['avg']:.6f}, cumul={fstats['cumulative']:.5f})"
        )
        return None

    # ── GATE 2: above_vwap (WAJIB) ──────────────────────────────────────────
    # Riset: 95.74% pre-pump candles ada di atas VWAP — ini prasyarat mutlak.
    # Di v13 ini hanya skor opsional, sehingga coin di bawah VWAP bisa lolos.
    vwap = calc_vwap(c1h, lookback=24)
    if price_now < vwap:
        log.info(
            f"  {symbol}: Harga di bawah VWAP — GATE GAGAL "
            f"(${price_now:.6g} < ${vwap:.6g})"
        )
        return None

    # ── Indikator teknikal ───────────────────────────────────────────────────
    ema_gap      = calc_ema_gap(c1h, period=20)
    bbw, bb_pct  = calc_bbw(c1h)
    bb_squeeze   = calc_bb_squeeze(c1h)
    atr_pct      = calc_atr_pct(c1h)
    rsi          = get_rsi(c1h[-48:])
    bos_up, bos_level = detect_bos_up(c1h)
    higher_low   = higher_low_detected(c1h)
    vol_ratio    = calc_volume_ratio(c1h)
    vol_accel    = calc_volume_acceleration(c1h)
    vol_consistent = check_volume_consistent(c1h)

    # Rate above VWAP dalam 6 candle terakhir
    above_vwap_rate = 0.0
    if len(c1h) >= 6:
        recent_6        = c1h[-6:]
        above           = sum(1 for c in recent_6 if c["close"] > vwap)
        above_vwap_rate = above / len(recent_6)

    # Price change 1h
    price_chg = 0.0
    if len(c1h) >= 2 and c1h[-2]["close"] > 0:
        price_chg = (c1h[-1]["close"] - c1h[-2]["close"]) / c1h[-2]["close"] * 100

    # ── Hitung skor ─────────────────────────────────────────────────────────
    score   = 0
    signals = []

    # 1. EMA Gap (lift 3.73x — TERTINGGI, bobot 5)
    if ema_gap >= CONFIG["ema_gap_threshold"]:
        score += CONFIG["score_ema_gap"]
        signals.append(f"EMA Gap {ema_gap:.3f} ≥ 1.0 — harga di atas EMA20 (lift 3.73x)")

    # 2. ATR (lift 3.31x)
    if atr_pct >= 1.5:
        score += CONFIG["score_atr_15"]
        signals.append(f"ATR {atr_pct:.2f}% ≥ 1.5% — volatilitas tinggi")
    elif atr_pct >= 1.0:
        score += CONFIG["score_atr_10"]
        signals.append(f"ATR {atr_pct:.2f}% ≥ 1.0% — volatilitas sedang")

    # 3. BB Width (lift 2.88x, format desimal sesuai riset)
    if bbw >= CONFIG["bbw_threshold_high"]:
        score += CONFIG["score_bbw_10"]
        signals.append(f"BB Width {bbw*100:.2f}% ≥ 10% — band lebar kuat")
    elif bbw >= CONFIG["bbw_threshold_mid"]:
        score += CONFIG["score_bbw_6"]
        signals.append(f"BB Width {bbw*100:.2f}% ≥ 6% — band mulai lebar")

    # 4. BOS Up + Above VWAP (kombinasi terbaik, lift 3.72x)
    if bos_up and above_vwap_rate >= CONFIG["above_vwap_rate_min"]:
        score += CONFIG["score_above_vwap_bos"]
        signals.append(
            f"BOS Up + Above VWAP {above_vwap_rate*100:.0f}% — kombinasi terkuat (lift 3.72x)"
        )
    elif bos_up:
        score += CONFIG["score_bos_up"]
        signals.append(f"Break of Structure ke atas (lift 2.81x)")

    # 5. Higher Low (lift 1.73x)
    if higher_low:
        score += CONFIG["score_higher_low"]
        signals.append("Higher Low terdeteksi — struktur bullish")

    # 6. BB Squeeze (lift 1.34x)
    if bb_squeeze:
        score += CONFIG["score_bb_squeeze"]
        signals.append(f"BB Squeeze aktif (band < 4%) — konsolidasi pre-breakout")

    # 7. RSI (lift 2.46x, bobot lebih rendah dari ema_gap dan atr sesuai riset)
    if rsi >= 65:
        score += CONFIG["score_rsi_65"]
        signals.append(f"RSI {rsi:.1f} ≥ 65 — momentum kuat (lift 2.46x)")
    elif rsi >= 55:
        score += CONFIG["score_rsi_55"]
        signals.append(f"RSI {rsi:.1f} ≥ 55 — bullish")

    # 8. Funding bonus
    if fstats["neg_pct"] >= 70:
        score += CONFIG["score_funding_neg_pct"]
        signals.append(f"Funding negatif {fstats['neg_pct']:.0f}% dari 6 periode terakhir")

    if fstats["streak"] >= CONFIG["funding_streak_min"]:
        score += CONFIG["score_funding_streak"]
        signals.append(
            f"Funding streak negatif {fstats['streak']} periode berturut "
            f"(dari {fstats['sample_count']} total data)"
        )

    if fstats["cumulative"] <= -0.05:
        score += CONFIG["score_funding_cumul"]
        signals.append(f"Funding kumulatif {fstats['cumulative']:.4f} — ekstrem negatif")

    # 9. Volume — hanya dihitung jika konsisten (anti-manipulasi)
    if vol_ratio > CONFIG["vol_ratio_threshold"]:
        if vol_consistent:
            score += CONFIG["score_vol_ratio"]
            signals.append(f"Volume {vol_ratio:.1f}x rata-rata (konsisten ≥ 2 candle)")
        else:
            # Catat sebagai peringatan di sinyal, tapi TIDAK tambah skor
            signals.append(
                f"⚠️ Volume spike {vol_ratio:.1f}x tapi tidak konsisten "
                f"— kemungkinan manipulasi, skor TIDAK ditambah"
            )

    if vol_accel > CONFIG["vol_accel_threshold"] and vol_consistent:
        score += CONFIG["score_vol_accel"]
        signals.append(f"Volume acceleration {vol_accel*100:.0f}% dalam 1h terakhir")

    # 10. Price change
    if price_chg >= 0.5:
        score += CONFIG["score_price_chg"]
        signals.append(f"Price +{price_chg:.2f}% dalam 1h terakhir")

    # ── Tentukan tipe pump dan level alert ──────────────────────────────────
    # HIGH: bos_up terjadi DAN ema_gap positif (konfirmasi momentum + struktur)
    # MEDIUM: di atas VWAP tapi belum BOS
    alert_level = "MEDIUM"
    pump_type   = "VWAP Momentum"

    if bos_up and ema_gap >= CONFIG["ema_gap_threshold"] and above_vwap_rate >= CONFIG["above_vwap_rate_min"]:
        alert_level = "HIGH"
        pump_type   = "Momentum Breakout"
    elif bos_up and ema_gap >= CONFIG["ema_gap_threshold"]:
        alert_level = "HIGH"
        pump_type   = "BOS + EMA Breakout"
    elif above_vwap_rate >= CONFIG["above_vwap_rate_min"] and fstats["cumulative"] <= -0.05 and higher_low:
        alert_level = "MEDIUM"
        pump_type   = "Short Squeeze Setup"
    elif above_vwap_rate >= CONFIG["above_vwap_rate_min"]:
        alert_level = "MEDIUM"
        pump_type   = "VWAP Momentum"

    # ── Hitung entry & target ────────────────────────────────────────────────
    entry_data = calc_entry(c1h, bos_level, alert_level, vwap, price_now)

    if score >= CONFIG["min_score_alert"]:
        return {
            "symbol":         symbol,
            "score":          score,
            "signals":        signals,
            "entry":          entry_data,
            "price":          price_now,
            "chg_24h":        chg_24h,
            "vol_24h":        vol_24h,
            "rsi":            round(rsi, 1),
            "ema_gap":        round(ema_gap, 3),
            "bbw":            round(bbw * 100, 2),   # simpan dalam persen untuk tampilan
            "bb_pct":         round(bb_pct, 2),
            "bb_squeeze":     bb_squeeze,
            "atr_pct":        round(atr_pct, 2),
            "above_vwap_rate": round(above_vwap_rate * 100, 1),
            "vwap":           round(vwap, 8),
            "bos_up":         bos_up,
            "bos_level":      round(bos_level, 8),
            "higher_low":     higher_low,
            "funding_stats":  fstats,
            "pump_type":      pump_type,
            "alert_level":    alert_level,
            "vol_ratio":      round(vol_ratio, 2),
            "vol_accel":      round(vol_accel * 100, 1),
            "vol_consistent": vol_consistent,
        }
    else:
        log.info(f"  {symbol}: Skor {score} < {CONFIG['min_score_alert']} — dilewati")
        return None

# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def build_alert(r, rank=None):
    level_icon = "🔥" if r["alert_level"] == "HIGH" else "📡"
    e = r["entry"]

    msg  = f"{level_icon} <b>PRE-PUMP SIGNAL #{rank} — v14.0</b>\n\n"
    msg += f"<b>Symbol    :</b> {r['symbol']}\n"
    msg += f"<b>Alert     :</b> {r['alert_level']} — {r['pump_type']}\n"
    msg += f"<b>Score     :</b> {r['score']}\n"
    msg += f"<b>Harga     :</b> ${r['price']:.6g}  ({r['chg_24h']:+.1f}% 24h)\n"
    msg += f"<b>VWAP      :</b> ${r['vwap']:.6g}\n"
    msg += "\n"
    msg += f"<b>EMA Gap   :</b> {r['ema_gap']:.3f} {'✅ ≥1.0' if r['ema_gap'] >= 1.0 else '❌ <1.0'}\n"
    msg += f"<b>RSI 14    :</b> {r['rsi']}\n"
    msg += f"<b>ATR %     :</b> {r['atr_pct']:.2f}%\n"
    msg += f"<b>BB Width  :</b> {r['bbw']:.2f}%\n"
    msg += f"<b>BB Squeeze:</b> {'Ya ⚡' if r['bb_squeeze'] else 'Tidak'}\n"
    msg += f"<b>BB Pos    :</b> {r['bb_pct']*100:.0f}%\n"
    msg += f"<b>BOS Up    :</b> {'✅' if r['bos_up'] else '❌'} (level ${r['bos_level']:.6g})\n"
    msg += f"<b>Above VWAP:</b> {r['above_vwap_rate']}% dari 6h terakhir\n"
    msg += f"<b>Higher Low:</b> {'✅' if r['higher_low'] else '❌'}\n"
    msg += f"<b>Volume    :</b> {r['vol_ratio']}x | accel {r['vol_accel']}% | konsisten {'✅' if r['vol_consistent'] else '⚠️'}\n"
    msg += f"<b>Funding   :</b> avg={r['funding_stats']['avg']:.6f} | cumul={r['funding_stats']['cumulative']:.4f}\n"
    msg += (
        f"  streak={r['funding_stats']['streak']} | "
        f"neg%={r['funding_stats']['neg_pct']:.0f}% | "
        f"basis={r['funding_stats']['basis']:.3f}%\n"
    )
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📍 <b>ENTRY ({e['alert_level']})</b>\n"
    msg += f"  Entry      : ${e['entry']}\n"
    msg += f"  SL         : ${e['sl']} (-{e['sl_pct']:.2f}%)\n"
    msg += f"  T1 (1.272) : ${e['t1']} (+{e['gain_t1_pct']:.1f}% dari entry)\n"
    msg += f"  T2 (1.618) : ${e['t2']} (+{e['gain_t2_pct']:.1f}% dari entry)\n"
    msg += f"  R/R        : 1:{e['rr']}\n"
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n📊 <b>SINYAL AKTIF</b>\n"
    for s in r["signals"]:
        msg += f"  • {s}\n"
    msg += f"\n📡 {utc_now()}\n<i>⚠️ Bukan financial advice.</i>"
    return msg

def build_summary(results):
    msg = f"📋 <b>TOP CANDIDATES v14.0 — {utc_now()}</b>\n{'━'*28}\n"
    for i, r in enumerate(results, 1):
        vol_str    = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                      else f"${r['vol_24h']/1e3:.0f}K")
        level_icon = "🔥" if r["alert_level"] == "HIGH" else "📡"
        msg += f"{i}. {level_icon} <b>{r['symbol']}</b> [Score:{r['score']} | {r['alert_level']}]\n"
        msg += (
            f"   {vol_str} | RSI:{r['rsi']} | EMAGap:{r['ema_gap']} | "
            f"T1:+{r['entry']['gain_t1_pct']}%\n"
        )
    return msg

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    all_candidates = []
    not_found      = []
    filtered_stats = defaultdict(int)

    log.info("=" * 70)
    log.info("🔍 SCANNING MODE: FULL WHITELIST")
    log.info("=" * 70)

    for sym in WHITELIST_SYMBOLS:

        # Filter keyword yang dikecualikan (sekarang AKTIF)
        if any(kw in sym for kw in EXCLUDED_KEYWORDS):
            filtered_stats["excluded_keyword"] += 1
            continue

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
            chg   = float(ticker.get("change24h",   0)) * 100
            price = float(ticker.get("lastPr",       0))
        except Exception:
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

    total     = len(WHITELIST_SYMBOLS)
    will_scan = len(all_candidates)
    filtered  = total - will_scan - len(not_found)

    log.info(f"\n📊 SCAN SUMMARY:")
    log.info(f"   Whitelist total  : {total} coins")
    log.info(f"   ✅ Will scan     : {will_scan} ({will_scan/total*100:.1f}%)")
    log.info(f"   ❌ Filtered      : {filtered}")
    log.info(f"   ⚠️  Not in Bitget : {len(not_found)}")
    log.info(f"\n📋 Filter breakdown:")
    for k, v in sorted(filtered_stats.items()):
        log.info(f"   {k:20s}: {v}")
    if not_found:
        sample = ", ".join(not_found[:10])
        log.info(f"\n   Missing sample   : {sample}"
                 f"{' ...' if len(not_found) > 10 else ''}")
    est_secs = will_scan * CONFIG["sleep_coins"]
    log.info(f"\n⏱️  Est. scan time: {est_secs:.0f}s (~{est_secs/60:.1f} min)")
    log.info("=" * 70 + "\n")
    return all_candidates

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    log.info(f"=== PRE-PUMP SCANNER v14.0 — {utc_now()} ===")

    # Load semua funding snapshot ke memori sebelum scan dimulai
    load_funding_snapshots()
    log.info(f"Funding snapshots loaded: {len(_funding_snapshots)} coins di memori")

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return
    log.info(f"Total ticker dari Bitget: {len(tickers)}")

    candidates = build_candidate_list(tickers)
    results    = []

    for i, (sym, t) in enumerate(candidates):
        try:
            vol = float(t.get("quoteVolume", 0))
        except Exception:
            vol = 0.0

        if vol < CONFIG["min_vol_24h"]:
            log.info(f"[{i+1}] {sym} — vol ${vol:,.0f} di bawah minimum, skip")
            continue

        log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K)...")

        try:
            res = master_score(sym, t)
            if res:
                log.info(
                    f"  ✅ Score={res['score']} | {res['alert_level']} | "
                    f"{res['pump_type']} | T1:+{res['entry']['gain_t1_pct']}%"
                )
                results.append(res)
        except Exception as ex:
            log.warning(f"  ❌ Error {sym}: {ex}")

        time.sleep(CONFIG["sleep_coins"])

    # Simpan semua funding snapshot ke disk sekaligus (batch I/O)
    save_all_funding_snapshots()
    log.info("Funding snapshots disimpan ke disk.")

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"\nLolos threshold: {len(results)} coin")

    if not results:
        log.info("Tidak ada sinyal yang memenuhi syarat saat ini.")
        return

    top = results[:CONFIG["max_alerts_per_run"]]

    if len(top) >= 2:
        send_telegram(build_summary(top))
        time.sleep(2)

    for rank, r in enumerate(top, 1):
        ok = send_telegram(build_alert(r, rank=rank))
        if ok:
            set_cooldown(r["symbol"])
            log.info(
                f"✅ Alert #{rank}: {r['symbol']} Score={r['score']} "
                f"Level={r['alert_level']}"
            )
        time.sleep(2)

    log.info(f"=== SELESAI — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  PRE-PUMP SCANNER v14.0                             ║")
    log.info("║  Rewrite total berbasis riset + perbaikan logika    ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)

    run_scan()
