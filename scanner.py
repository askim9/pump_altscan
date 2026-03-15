"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PIVOT BOUNCE SCANNER v2.1 — PRE-PUMP DETECTION                            ║
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
    "max_vol_24h":         80_000_000,
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
    "candle_limit_1h":            504,     # 21 hari data 1H

    # ── COMPRESSION DETECTION ─────────────────────────────────────────────────
    # Fase 1: coin harus "tidur pulas" minimal 36 jam di range sempit
    "compression_min_candles":     36,     # minimal 36 candle 1H = 1.5 hari
    "compression_max_candles":    480,     # maksimal 480 candle = 20 hari (lebih dari itu terlalu tua)
    "compression_range_pct":      0.10,    # range high-low selama compression < 10%
                                           # Dinaikkan dari 7% → 10% agar menangkap PIXEL-type
                                           # (low-price coin terlihat "flat" di chart tapi range
                                           # nominalnya bisa 15-20% — 10% adalah sweet spot)
    "compression_lookback":       480,     # cari zona compression dalam 480 candle terakhir

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
    "max_alerts_per_run":           6,
    "alert_cooldown_sec":        3600,
    "sleep_coins":                 0.7,
    "sleep_error":                 3.0,
    "cooldown_file":     "/tmp/v2_cooldown.json",
}

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST — 324 coin
# ══════════════════════════════════════════════════════════════════════════════
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

        # Hitung "age" — berapa candle dari akhir zona ke candle terkini
        age = (n - 1) - end  # 0 = zona berakhir di candle terbaru

        # Skor kualitas: lebih panjang lebih baik, lebih baru lebih baik
        quality = length * math.exp(-age / 48)  # decay jika sudah lama

        if best is None or quality > best["quality"]:
            best = {
                "start_idx":  start,
                "end_idx":    end,
                "low":        zone_low,
                "high":       zone_high,
                "length":     length,
                "avg_vol":    avg_vol,
                "age_candles": age,
                "quality":    quality,
                "range_pct":  (zone_high - zone_low) / zone_low,
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
        # Fallback: ATR multiplier berbasis panjang compression
        comp_len  = compression_zone["length"]
        atr_mult  = min(4.0 + comp_len / 48, 10.0)
        t1 = entry + atr * atr_mult
        t2 = t1 * 1.20

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
             f"range={compression['range_pct']*100:.1f}%")

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

    # ── FASE 2: Deteksi Volume Awakening ─────────────────────────────────────
    awakening = detect_volume_awakening(c1h, comp_avg_vol)

    if not awakening["detected"]:
        log.info(f"  {symbol}: SKIP volume belum bangun (best_mult={awakening['best_mult']:.1f}x)")
        return None

    # ── Gate: SELLING CLIMAX — spike merah di BAWAH zona compression ─────────
    # Bug dari data nyata (CAKE): volume spike 12.1x tapi candle MERAH dan
    # harga SUDAH di bawah zona compression = ini selling climax / dump, BUKAN awakening.
    # Bedakan: awakening sejati = spike terjadi saat harga MASIH di zona atau baru breakout atas.
    price_now    = c1h[-1]["close"]
    spike_candle = c1h[-awakening["spike_candle"]] if awakening["spike_candle"] >= 1 else c1h[-1]
    spike_is_red = spike_candle["close"] < spike_candle["open"]
    price_below_zone = price_now < comp_low * 0.99   # harga > 1% di bawah zona

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
    if not entry_data or entry_data["t1_pct"] < CONFIG["min_target_pct"]:
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
        "candle_label":    candle_label,
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

    # Format compression info
    comp_days = comp["length"] / 24
    comp_str  = (f"{comp_days:.0f} hari" if comp_days >= 1
                 else f"{comp['length']} jam")

    msg = (
        f"🚀 <b>PRE-PUMP SIGNAL {rk}— v2.0</b>\n\n"
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
        f"  Candle   : {'Hijau ✅' if awk['is_green'] else 'Merah ⚠️'}"
        f"{'  🔥 MEGA SPIKE!' if awk['is_mega'] else ''}\n"
        f"  RVOL     : {r['rvol']:.1f}x\n"
        f"\n📊 <b>KONDISI TEKNIKAL</b>\n"
        f"  RSI 1H   : {r['rsi']:.0f} {'(oversold 🟢)' if r['rsi'] < 35 else '(netral)'}\n"
        f"  Candle   : {r['candle_label']}\n"
        f"  Liq sweep: {'✅ terdeteksi' if r['liq_sweep'] else '❌ tidak ada'}\n"
        f"  ATR comp : {'✅' if r['vol_compress'] else '❌'}\n"
        f"  Funding  : {r['funding']:.5f}\n"
        f"  Vol 24H  : {vol}  |  Chg: {r['chg_24h']:+.1f}%\n"
    )

    if e:
        msg += (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>ENTRY &amp; TARGET</b>\n"
            f"  Entry : ${e['entry']}\n"
            f"  SL    : ${e['sl']}  (-{e['sl_pct']:.1f}%)\n"
            f"  T1    : ${e['t1']}  (+{e['t1_pct']:.1f}%)\n"
            f"  T2    : ${e['t2']}  (+{e['t2_pct']:.1f}%)\n"
            f"  R/R   : 1:{e['rr']}\n"
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
    log.info(f"🔍 SCANNING {len(WHITELIST_SYMBOLS)} coin — PRE-PUMP DETECTION v2.0")
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
    log.info(f"=== PRE-PUMP SCANNER v2.0 — {utc_now()} ===")

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
    results.sort(key=lambda x: (x["score"] - x["rise_from_low"] * 50), reverse=True)

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
    log.info("║  PRE-PUMP SCANNER v2.0                            ║")
    log.info("║  Deteksi transisi Fase Tidur → Fase Bangun        ║")
    log.info("║  Target: entry sekarang, TP 1-2 hari (+10-100%)  ║")
    log.info("╚════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di environment!")
        exit(1)

    run_scan()
