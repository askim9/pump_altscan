#!/usr/bin/env python3
"""
nexus_pb.py — NEXUS Pre-Breakout Scanner
Versi: 3.0.0-AUDIT | Bitget USDT-Futures

Trigger: EMA20 cross EMA50 + BBW squeeze + OI buildup (Coinalyze proxy)
Entry  : Akhir fase konsolidasi, sebelum breakout
DB     : nexus_pb_history.db (terpisah dari scanner_history.db v16)
Interval: 30 menit (GitHub Actions cron: */30 * * * *)

BERBEDA dari scanner_v16.py (momentum continuation):
  - Trigger : EMA cross (bukan ATR/momentum)
  - BBW     : squeeze dulu → expand (bukan wide = positif)
  - Entry   : bottom konsolidasi (bukan post-breakout +10-20%)
  - SL      : 5-8% (bukan 7-12%)
  - Lead time: 10-24 jam (bukan 0-12 jam)
  - Threshold: 55/100 (bukan 95-110 range v16)

PERUBAHAN dari v2.0.0:
  BUG-06 FIX : hit_sl pakai min_return <= -sl_pct (bukan new_min <= sl_price)
               Type mismatch % vs harga dollar di v2 menyebabkan hit_sl
               overcounting — True untuk setiap return negatif sekecil apapun.

  BUG-07 FIX : calc_ema di check_ema200_condition dan detect_ema_cross
               sekarang pakai closed=candles[:-1] sebagai referensi "sekarang".
               candles[-1] adalah candle belum closed (LOOKAHEAD — bisa flip).
               Estimasi dampak: cross detection 10-20% terlalu dini di v2.

  BUG-08 FIX : Pre-filter BBW pakai calc_bbw(candles[:-1]) bukan calc_bbw(candles).

  BUG-09 FIX : Bybit fallback di Coinalyze mapping.
               v2 hanya map Binance — coin yang hanya ada di Bybit di-REJECT
               secara diam-diam. Sekarang eksplisit: Binance → Bybit → REJECT.

Basis data: 1.362 pump events (130 symbols) + 51 sinyal scanner v16.
JANGAN ubah threshold CONFIG sebelum 50 sinyal outcomes terkumpul.
"""

import hashlib
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

# ══════════════════════════════════════════════════════════════════════
#  VERSION
# ══════════════════════════════════════════════════════════════════════
VERSION = "3.0.0-AUDIT"

# ══════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════
def setup_logging() -> logging.Logger:
    log = logging.getLogger("nexus_pb")
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    sh  = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(sh)
    fh  = logging.FileHandler("nexus_pb.log")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    return log

log = setup_logging()

# ══════════════════════════════════════════════════════════════════════
#  KONFIGURASI
#  SEMUA ANGKA BERBASIS DATA EMPIRIS — JANGAN UBAH TANPA JUSTIFIKASI
#  Rule: butuh 50+ signal outcomes per segment sebelum ubah angka apapun.
# ══════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # ── Identity ───────────────────────────────────────────────────────
    "version":                VERSION,
    "db_path":                "nexus_pb_history.db",
    "cooldown_hours":         24,

    # ── API Keys (environment variables) ──────────────────────────────
    "telegram_token":         os.getenv("TELEGRAM_TOKEN", ""),
    "telegram_chat_id":       os.getenv("TELEGRAM_CHAT_ID", ""),
    "coinalyze_key":          os.getenv("COINALYZE_API_KEY", "57f01115-bc40-4b82-8aef-ed4bcc5c64eb"),

    # ── Phase 1: Bitget pre-filter ─────────────────────────────────────
    # Basis: riset empiris 1.362 pump events + 51 sinyal
    "min_vol_24h_usd":        2_000_000,   # T6: $2M min — illiquid di bawah ini
    "chg_24h_min":            3.0,          # T3: momentum pertama harus sudah ada
    "chg_24h_max":            20.0,         # T3: belum terlambat
    "chg_1h_max":             8.0,          # T3: tidak sedang pump cepat

    # ── BBW Squeeze ────────────────────────────────────────────────────
    # Basis: T1 median konsolidasi 9.8 jam, sweet spot 4-24 jam
    "bbw_squeeze_threshold":  0.08,         # BBW < 8% = squeeze aktif
    "bbw_squeeze_min_candles": 6,           # minimum 6 jam (dari riset T1)
    "bbw_squeeze_max_candles": 48,          # max 48 jam (lebih = stale setup)
    # Toleransi: 1 candle spike diizinkan agar squeeze panjang tidak terpotong

    # ── EMA Conditions ─────────────────────────────────────────────────
    # Basis: 5 kondisi valid dari riset (lihat SKILL — 5 Kondisi EMA Cross)
    "ema200_slope_min":       -0.30,        # EMA200 tidak boleh turun > 0.3%/candle
    "dist_to_ema200_min":     8.0,          # min 8% di bawah EMA200
    "dist_to_ema200_max":     30.0,         # max 30% di bawah EMA200
    "cross_max_candles_ago":  2,            # cross baru (max 2 closed candle lalu)
    "cross_min_vol_ratio":    1.5,          # volume cross min 1.5x avg 10 candle

    # ── OI Validation ──────────────────────────────────────────────────
    # Basis: T5 — T2>=20 wajib di 100% sinyal HIT terbaik
    # Hard rule: tidak tersedia = REJECT (bukan skip/default lolos)
    "oi_change_6h_min":       5.0,          # OI naik min 5% dalam 6 jam

    # ── BTC Regime ─────────────────────────────────────────────────────
    # Basis: pump internal strength tidak butuh BTC rally
    "btc_4h_max":             3.0,          # BTC > +3% = coin ikut-ikutan
    "btc_4h_min":            -1.0,          # BTC < -1% = tekanan jual

    # ── Funding ────────────────────────────────────────────────────────
    "funding_min":           -0.0005,       # > -0.05%: extreme shorts = reject

    # ── Scoring & Alert ────────────────────────────────────────────────
    # JANGAN ubah weights sebelum 50 sinyal outcomes terkumpul di DB
    "alert_threshold":        55,           # /100 — belum dikalibrasi, review Sprint 2
    "score_squeeze_24h":      30,           # squeeze >= 24 jam
    "score_squeeze_12h":      25,           # squeeze 12-24 jam
    "score_squeeze_8h":       20,           # squeeze 8-12 jam
    "score_squeeze_6h":       15,           # squeeze 6-8 jam (minimum valid)
    "score_oi_high":          25,           # OI >= 20%
    "score_oi_mid":           18,           # OI 10-20%
    "score_oi_low":           10,           # OI 5-10% (minimum)
    "score_vol_high":         25,           # volume cross >= 3x
    "score_vol_mid":          18,           # volume cross 2-3x
    "score_vol_low":          10,           # volume cross 1.5-2x (minimum)
    "score_supp_4":           20,           # support >= 4 test
    "score_supp_3":           14,           # support 3 test
    "score_supp_2":            8,           # support 2 test

    # ── Outcome Tracking ───────────────────────────────────────────────
    # Basis: T2 lead time — 46% pump terjadi di 12-24 jam
    "outcome_window_hours":   24,           # lebih panjang dari v16 (12h)

    # ── Whitelist (coin yang dipindai) ────────────────────────────────
    # 383 symbols dari scanner v16 — sudah tervalidasi ada di Bitget
    # NEXUS-PB scan hanya dari whitelist ini (bukan semua 535 symbols)
    "whitelist": {
        "4USDT", "0GUSDT", "1000BONKUSDT", "1000PEPEUSDT", "1000RATSUSDT", "1000SHIBUSDT", "1000XECUSDT", "1INCHUSDT",
        "1MBABYDOGEUSDT", "2ZUSDT", "AAVEUSDT", "ACEUSDT", "ACHUSDT", "ACTUSDT", "ADAUSDT", "AEROUSDT",
        "AGLDUSDT", "AINUSDT", "AIOUSDT", "AIXBTUSDT", "AKTUSDT", "ALCHUSDT", "ALGOUSDT", "ALICEUSDT",
        "ALLOUSDT", "ALTUSDT", "ANIMEUSDT", "ANKRUSDT", "APEUSDT", "APEXUSDT", "API3USDT", "APRUSDT",
        "APTUSDT", "ARUSDT", "ARBUSDT", "ARCUSDT", "ARIAUSDT", "ARKUSDT", "ARKMUSDT", "ARPAUSDT",
        "ASTERUSDT", "ATUSDT", "ATHUSDT", "ATOMUSDT", "AUCTIONUSDT", "AVAXUSDT", "AVNTUSDT", "AWEUSDT",
        "AXLUSDT", "AXSUSDT", "AZTECUSDT", "BUSDT", "B2USDT", "BABYUSDT", "BANUSDT", "BANANAUSDT",
        "BANANAS31USDT", "BANKUSDT", "BARDUSDT", "BATUSDT", "BCHUSDT", "BEATUSDT", "BERAUSDT", "BGBUSDT",
        "BIGTIMEUSDT", "BIOUSDT", "BIRBUSDT", "BLASTUSDT", "BLESSUSDT", "BLURUSDT", "BNBUSDT", "BOMEUSDT",
        "BRETTUSDT", "BREVUSDT", "BROCCOLIUSDT", "BSVUSDT", "BTCUSDT", "BULLAUSDT", "C98USDT", "CAKEUSDT",
        "CCUSDT", "CELOUSDT", "CFXUSDT", "CHILLGUYUSDT", "CHZUSDT", "CLUSDT", "CLANKERUSDT", "CLOUSDT",
        "COAIUSDT", "COMPUSDT", "COOKIEUSDT", "COWUSDT", "CRCLUSDT", "CROUSDT", "CROSSUSDT", "CRVUSDT",
        "CTKUSDT", "CVCUSDT", "CVXUSDT", "CYBERUSDT", "CYSUSDT", "DASHUSDT", "DEEPUSDT", "DENTUSDT",
        "DEXEUSDT", "DOGEUSDT", "DOLOUSDT", "DOODUSDT", "DOTUSDT", "DRIFTUSDT", "DYDXUSDT", "DYMUSDT",
        "EGLDUSDT", "EIGENUSDT", "ENAUSDT", "ENJUSDT", "ENSUSDT", "ENSOUSDT", "EPICUSDT", "ESPUSDT",
        "ETCUSDT", "ETHUSDT", "ETHFIUSDT", "FUSDT", "FARTCOINUSDT", "FETUSDT", "FFUSDT", "FIDAUSDT",
        "FILUSDT", "FLOKIUSDT", "FLUIDUSDT", "FOGOUSDT", "FOLKSUSDT", "FORMUSDT", "GALAUSDT", "GASUSDT",
        "GIGGLEUSDT", "GLMUSDT", "GMTUSDT", "GMXUSDT", "GOATUSDT", "GPSUSDT", "GRASSUSDT", "GUSDT",
        "GRIFFAINUSDT", "GRTUSDT", "GUNUSDT", "GWEIUSDT", "HUSDT", "HBARUSDT", "HEIUSDT", "HEMIUSDT",
        "HMSTRUSDT", "HOLOUSDT", "HOMEUSDT", "HYPEUSDT", "HYPERUSDT", "ICNTUSDT", "ICPUSDT", "IDOLUSDT",
        "ILVUSDT", "IMXUSDT", "INITUSDT", "INJUSDT", "INXUSDT", "IOUSDT", "IOTAUSDT", "IOTXUSDT",
        "IPUSDT", "JASMYUSDT", "JCTUSDT", "JSTUSDT", "JTOUSDT", "JUPUSDT", "KAIAUSDT", "KAITOUSDT",
        "KASUSDT", "KAVAUSDT", "kBONKUSDT", "KERNELUSDT", "KGENUSDT", "KITEUSDT", "kPEPEUSDT", "kSHIBUSDT",
        "LAUSDT", "LABUSDT", "LAYERUSDT", "LDOUSDT", "LIGHTUSDT", "LINEAUSDT", "LINKUSDT", "LITUSDT",
        "LPTUSDT", "LSKUSDT", "LTCUSDT", "LUNAUSDT", "LUNCUSDT", "LYNUSDT", "MUSDT", "MAGICUSDT",
        "MAGMAUSDT", "MANAUSDT", "MANTAUSDT", "MANTRAUSDT", "MASKUSDT", "MAVUSDT", "MAVIAUSDT", "MBOXUSDT",
        "MEUSDT", "MEGAUSDT", "MELANIAUSDT", "MEMEUSDT", "MERLUSDT", "METUSDT", "METAUSDT", "MEWUSDT",
        "MINAUSDT", "MMTUSDT", "MNTUSDT", "MONUSDT", "MOODENGUSDT", "MORPHOUSDT", "MOVEUSDT", "MOVRUSDT",
        "MUUSDT", "MUBARAKUSDT", "MYXUSDT", "NAORISUSDT", "NEARUSDT", "NEIROCTOUSDT", "NEOUSDT", "NEWTUSDT",
        "NILUSDT", "NMRUSDT", "NOMUSDT", "NOTUSDT", "NXPCUSDT", "ONDOUSDT", "ONGUSDT", "ONTUSDT",
        "OPUSDT", "OPENUSDT", "OPNUSDT", "ORCAUSDT", "ORDIUSDT", "OXTUSDT", "PARTIUSDT", "PENDLEUSDT",
        "PENGUUSDT", "PEOPLEUSDT", "PEPEUSDT", "PHAUSDT", "PIEVERSEUSDT", "PIPPINUSDT", "PLUMEUSDT", "PNUTUSDT",
        "POLUSDT", "POLYXUSDT", "POPCATUSDT", "POWERUSDT", "PROMPTUSDT", "PROVEUSDT", "PUMPUSDT", "PURRUSDT",
        "PYTHUSDT", "QUSDT", "QNTUSDT", "RAVEUSDT", "RAYUSDT", "RECALLUSDT", "RENDERUSDT", "RESOLVUSDT",
        "REZUSDT", "RIVERUSDT", "ROBOUSDT", "ROSEUSDT", "RPLUSDT", "RSRUSDT", "RUNEUSDT", "SUSDT",
        "SAGAUSDT", "SAHARAUSDT", "SANDUSDT", "SAPIENUSDT", "SEIUSDT", "SENTUSDT", "SHIBUSDT", "SIGNUSDT",
        "SIRENUSDT", "SKHYNIXUSDT", "SKRUSDT", "SKYUSDT", "SKYAIUSDT", "SLPUSDT", "SNXUSDT", "SOLUSDT",
        "SOMIUSDT", "SONICUSDT", "SOONUSDT", "SOPHUSDT", "SPACEUSDT", "SPKUSDT", "SPXUSDT", "SQDUSDT",
        "SSVUSDT", "STBLUSDT", "STEEMUSDT", "STOUSDT", "STRKUSDT", "STXUSDT", "SUIUSDT", "SUNUSDT",
        "SUPERUSDT", "SUSHIUSDT", "SYRUPUSDT", "TUSDT", "TACUSDT", "TAGUSDT", "TAIKOUSDT", "TAOUSDT",
        "THEUSDT", "THETAUSDT", "TIAUSDT", "TNSRUSDT", "TONUSDT", "TOSHIUSDT", "TOWNSUSDT", "TRBUSDT",
        "TRIAUSDT", "TRUMPUSDT", "TRXUSDT", "TURBOUSDT", "UAIUSDT", "UBUSDT", "UMAUSDT", "UNIUSDT",
        "USUSDT", "USDKRWUSDT", "USELESSUSDT", "USUALUSDT", "VANAUSDT", "VANRYUSDT", "VETUSDT", "VINEUSDT",
        "VIRTUALUSDT", "VTHOUSDT", "VVVUSDT", "WUSDT", "WALUSDT", "WAXPUSDT", "WCTUSDT", "WETUSDT",
        "WIFUSDT", "WLDUSDT", "WLFIUSDT", "WOOUSDT", "WTIUSDT", "XAIUSDT", "XCUUSDT", "XDCUSDT",
        "XLMUSDT", "XMRUSDT", "XPDUSDT", "XPINUSDT", "XPLUSDT", "XRPUSDT", "XTZUSDT", "XVGUSDT",
        "YGGUSDT", "YZYUSDT", "ZAMAUSDT", "ZBTUSDT", "ZECUSDT", "ZENUSDT", "ZEREBROUSDT", "ZETAUSDT",
        "ZILUSDT", "ZKUSDT", "ZKCUSDT", "ZKJUSDT", "ZKPUSDT", "ZORAUSDT", "ZROUSDT",
    },

    # ── Stock Token Blacklist ──────────────────────────────────────────
    # Token saham tokenized Bitget — tidak ada di Binance/Bybit futures
    # (diambil dari working scanner v16 + tambahan dari log run pertama)
    "stock_token_blacklist": {
        "HOODUSDT","COINUSDT","MSTRUSDT","NVDAUSDT","AAPLUSDT",
        "GOOGLUSDT","AMZNUSDT","METAUSDT","QQQUSDT","BZUSDT",
        "MCDUSDT","JCTUSDT","NOMUSDT","ASTERUSDT","POLYXUSDT",
        "TSLAUSDT","CRCLUSDT","SPYUSDT","GLDUSDT","MSFTUSDT",
        "PLTRUSDT","INTCUSDT","XAUSDT","USDCUSDT","SKHYNIXUSDT",
        "XAGUSDT","WTIUSDT","XPDUSDT","USDKRWUSDT",
    },
}

# ══════════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════
@dataclass
class ClzData:
    """Container data Coinalyze per symbol."""
    ohlcv:                  List[dict] = field(default_factory=list)
    oi:                     List[dict] = field(default_factory=list)
    liq:                    List[dict] = field(default_factory=list)
    funding_hist:           List[dict] = field(default_factory=list)
    predicted_funding_hist: List[dict] = field(default_factory=list)
    ls_ratio:               List[dict] = field(default_factory=list)
    proxy_exchange:         str = ""    # "binance" atau "bybit" — untuk log & DB

    @property
    def has_ohlcv(self) -> bool:   return len(self.ohlcv) >= 6
    @property
    def has_oi(self) -> bool:      return len(self.oi) >= 9  # butuh -8 index
    @property
    def has_funding(self) -> bool: return len(self.funding_hist) >= 3


@dataclass
class CoinData:
    symbol:     str
    price:      float
    vol_24h:    float
    chg_24h:    float
    chg_1h:     float
    chg_4h:     float
    funding:    float
    candles:    List[dict]
    btc_chg_1h: float = 0.0
    btc_chg_4h: float = 0.0
    clz:        ClzData = field(default_factory=ClzData)


@dataclass
class NexusResult:
    """Hasil scoring NEXUS-PB untuk satu coin."""
    symbol:             str
    score:              int
    entry_price:        float
    sl_price:           float
    sl_pct:             float
    tp1_price:          float
    tp1_pct:            float

    # Kondisi saat sinyal — semua disimpan ke DB untuk kalibrasi Sprint 2
    squeeze_duration_h:     float
    bbw_at_cross:           float
    ema20:                  float
    ema50:                  float
    ema200:                 float
    ema200_slope:           float
    dist_to_ema200_pct:     float
    cross_volume_ratio:     float
    oi_change_6h_pct:       float
    oi_proxy:               str       # "binance" atau "bybit" — untuk analisis
    funding:                float
    btc_chg_4h:             float
    chg_1h:                 float
    chg_4h:                 float
    chg_24h:                float
    vol_24h:                float
    fingerprint:            str = ""


# ══════════════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════════════
def init_db() -> None:
    """Inisialisasi DB. Aman dijalankan berulang (CREATE IF NOT EXISTS)."""
    conn = sqlite3.connect(CONFIG["db_path"])
    c    = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS nexus_signals (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol              TEXT NOT NULL,
            alerted_at          INTEGER NOT NULL,
            score               INTEGER,
            entry_price         REAL,
            sl_price            REAL,
            sl_pct              REAL,
            tp1_price           REAL,
            tp1_pct             REAL,

            -- Kondisi entry (untuk kalibrasi Sprint 2)
            squeeze_duration_h  REAL,
            bbw_at_cross        REAL,
            ema20_at_cross      REAL,
            ema50_at_cross      REAL,
            ema200_at_cross     REAL,
            ema200_slope        REAL,
            dist_to_ema200_pct  REAL,
            cross_volume_ratio  REAL,
            oi_change_6h_pct    REAL,
            oi_proxy            TEXT,
            funding_at_signal   REAL,
            btc_chg_4h          REAL,
            chg_1h_signal       REAL,
            chg_4h_signal       REAL,
            chg_24h_signal      REAL,
            vol_24h_signal      REAL,

            -- Outcome (diisi bertahap, window 24 jam)
            return_1h           REAL DEFAULT NULL,
            return_3h           REAL DEFAULT NULL,
            return_6h           REAL DEFAULT NULL,
            return_12h          REAL DEFAULT NULL,
            return_24h          REAL DEFAULT NULL,
            max_return          REAL DEFAULT NULL,
            min_return          REAL DEFAULT NULL,
            hit_15pct           INTEGER DEFAULT NULL,
            hit_10pct           INTEGER DEFAULT NULL,
            hit_sl              INTEGER DEFAULT NULL,
            checked             INTEGER DEFAULT 0,
            fingerprint         TEXT,
            data_version        TEXT DEFAULT 'v3'
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_ns_sym "
              "ON nexus_signals(symbol, alerted_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ns_checked "
              "ON nexus_signals(checked, alerted_at)")

    c.execute("""
        CREATE TABLE IF NOT EXISTS nexus_pump_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT NOT NULL,
            detected_at     INTEGER NOT NULL,
            chg_24h         REAL,
            vol_24h         REAL,
            scanner_alerted INTEGER DEFAULT 0,
            signal_id       INTEGER DEFAULT NULL
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_npe_sym "
              "ON nexus_pump_events(symbol, detected_at DESC)")

    conn.commit()
    conn.close()
    log.info("  DB initialized: %s", CONFIG["db_path"])


def is_on_cooldown(symbol: str) -> bool:
    """Return True jika symbol sudah di-alert dalam cooldown_hours terakhir."""
    try:
        conn = sqlite3.connect(CONFIG["db_path"])
        c    = conn.cursor()
        c.execute("SELECT MAX(alerted_at) FROM nexus_signals WHERE symbol=?", (symbol,))
        row  = c.fetchone()
        conn.close()
        if row and row[0]:
            return (time.time() - row[0]) < (CONFIG["cooldown_hours"] * 3600)
        return False
    except Exception:
        return False


def save_signal(result: NexusResult) -> int:
    """Simpan sinyal ke DB. Return rowid yang diinsert, -1 jika gagal."""
    try:
        conn = sqlite3.connect(CONFIG["db_path"])
        c    = conn.cursor()
        c.execute("""
            INSERT INTO nexus_signals (
                symbol, alerted_at, score, entry_price, sl_price, sl_pct,
                tp1_price, tp1_pct,
                squeeze_duration_h, bbw_at_cross, ema20_at_cross, ema50_at_cross,
                ema200_at_cross, ema200_slope, dist_to_ema200_pct,
                cross_volume_ratio, oi_change_6h_pct, oi_proxy,
                funding_at_signal, btc_chg_4h,
                chg_1h_signal, chg_4h_signal, chg_24h_signal, vol_24h_signal,
                fingerprint
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            result.symbol, int(time.time()), result.score,
            result.entry_price, result.sl_price, result.sl_pct,
            result.tp1_price, result.tp1_pct,
            result.squeeze_duration_h, result.bbw_at_cross,
            result.ema20, result.ema50, result.ema200, result.ema200_slope,
            result.dist_to_ema200_pct, result.cross_volume_ratio,
            result.oi_change_6h_pct, result.oi_proxy,
            result.funding, result.btc_chg_4h,
            result.chg_1h, result.chg_4h, result.chg_24h, result.vol_24h,
            result.fingerprint,
        ))
        row_id = c.lastrowid
        conn.commit()
        conn.close()
        return row_id
    except Exception as e:
        log.warning("save_signal failed: %s", e)
        return -1


def check_and_update_outcomes(tickers: Dict[str, dict]) -> None:
    """
    Update return_Xh, max_return, min_return untuk sinyal belum checked.
    Dipanggil tiap run scanner (resolusi 30 menit).
    Window: 24 jam.

    BUG-06 FIX: hit_sl = 1 jika min_return <= -sl_pct
    (v2 pakai new_min <= sl_price — type mismatch % vs harga dollar,
    menyebabkan hit_sl=1 untuk SEMUA return negatif sekecil apapun)

    Catatan resolusi SL: min_return diupdate dari ticker price saat polling.
    Intracandle low tidak tertangkap di sini.
    nexus_outcome_analyzer.py melakukan update lebih akurat berbasis candle low.
    """
    try:
        conn = sqlite3.connect(CONFIG["db_path"])
        c    = conn.cursor()
        now  = int(time.time())

        c.execute("""
            SELECT id, symbol, alerted_at, entry_price, sl_pct,
                   return_1h, return_3h, return_6h, return_12h, return_24h,
                   max_return, min_return
            FROM nexus_signals
            WHERE checked=0 AND alerted_at <= ?
        """, (now - 3600,))
        rows = c.fetchall()

        updated = 0
        for row in rows:
            (row_id, symbol, alerted_at, entry_price, sl_pct,
             r1h, r3h, r6h, r12h, r24h, max_ret, min_ret) = row

            if not entry_price or entry_price <= 0:
                continue

            ticker = tickers.get(symbol)
            if not ticker:
                continue

            cur = float(ticker.get("lastPr", 0) or 0)
            if cur <= 0:
                continue

            elapsed = now - alerted_at
            ret     = round((cur - entry_price) / entry_price * 100, 2)

            # max_return dan min_return: update setiap run
            new_max = max(v for v in [max_ret, ret] if v is not None)
            new_min = min(v for v in [min_ret, ret] if v is not None)
            new_max = round(new_max, 2)
            new_min = round(new_min, 2)

            c.execute(
                "UPDATE nexus_signals SET max_return=?, min_return=? WHERE id=?",
                (new_max, new_min, row_id),
            )

            # return_Xh: set sekali, tidak dioverwrite setelah terisi
            if elapsed >= 3600      and r1h  is None:
                c.execute("UPDATE nexus_signals SET return_1h=?  WHERE id=?", (ret, row_id))
            if elapsed >= 3 * 3600  and r3h  is None:
                c.execute("UPDATE nexus_signals SET return_3h=?  WHERE id=?", (ret, row_id))
            if elapsed >= 6 * 3600  and r6h  is None:
                c.execute("UPDATE nexus_signals SET return_6h=?  WHERE id=?", (ret, row_id))
            if elapsed >= 12 * 3600 and r12h is None:
                c.execute("UPDATE nexus_signals SET return_12h=? WHERE id=?", (ret, row_id))

            # Close sinyal di 24 jam
            if elapsed >= CONFIG["outcome_window_hours"] * 3600 and r24h is None:
                hit_15 = 1 if new_max >= 15.0 else 0
                hit_10 = 1 if new_max >= 10.0 else 0

                # BUG-06 FIX: keduanya persentase
                # min_return adalah return % (misal -7.1)
                # sl_pct adalah SL persentase positif (misal 8.0)
                # SL kena jika harga pernah turun >= sl_pct% dari entry
                if sl_pct and sl_pct > 0:
                    hit_sl = 1 if new_min <= -sl_pct else 0
                else:
                    hit_sl = 0  # sl_pct tidak valid — konservatif, jangan overclaim

                c.execute("""
                    UPDATE nexus_signals
                    SET return_24h=?, hit_15pct=?, hit_10pct=?, hit_sl=?, checked=1
                    WHERE id=?
                """, (ret, hit_15, hit_10, hit_sl, row_id))
                updated += 1
                log.info(
                    "  CLOSED %s: max=%+.1f%% min=%+.1f%% "
                    "hit15=%d hit10=%d hitSL=%d (sl_pct=%.1f%%)",
                    symbol, new_max, new_min, hit_15, hit_10, hit_sl, sl_pct or 0,
                )

        if updated:
            log.info("  Outcome tracking: %d sinyal closed (24h window)", updated)
        conn.commit()
        conn.close()

    except Exception as e:
        log.warning("check_and_update_outcomes error: %s", e)


# ══════════════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════
def _mean(vals: List[float]) -> float:
    v = [x for x in vals if x is not None]
    return sum(v) / len(v) if v else 0.0


def calc_ema(candles: List[dict], period: int) -> float:
    """
    Hitung EMA untuk period tertentu dari list candle yang diberikan.

    PENTING — LOOKAHEAD CONTRACT:
    Caller wajib pass candles[:-1] jika ingin EMA "sekarang" = last closed candle.
    Fungsi ini menghitung EMA dari semua elemen yang diberikan, termasuk elemen
    terakhir. Tidak ada validasi lookahead di dalam fungsi ini.
    """
    if len(candles) < period:
        return 0.0
    closes = [float(c["close"]) for c in candles if c.get("close")]
    if not closes:
        return 0.0
    k   = 2.0 / (period + 1)
    ema = closes[0]
    for price in closes[1:]:
        ema = price * k + ema * (1 - k)
    return ema


def calc_bbw(candles: List[dict]) -> float:
    """
    Hitung Bollinger Band Width dari 20 candle terakhir daftar yang diberikan.

    PENTING — LOOKAHEAD CONTRACT:
    Caller wajib pass candles[:-1] jika tidak ingin candle yang belum closed
    masuk kalkulasi.
    """
    if len(candles) < 22:
        return 0.0
    closes = [float(c["close"]) for c in candles[-20:] if c.get("close")]
    if len(closes) < 20:
        return 0.0
    avg      = sum(closes) / len(closes)
    variance = sum((x - avg) ** 2 for x in closes) / len(closes)
    std      = variance ** 0.5
    if avg == 0:
        return 0.0
    return (4 * std) / avg


def get_chg_from_candles(candles: List[dict], n_hours: int) -> float:
    """
    Hitung perubahan harga n_hours terakhir.
    NO-LOOKAHEAD: candles[-2] = last closed candle (candles[-1] belum closed).
    """
    if len(candles) < n_hours + 2:
        return 0.0
    cur  = float(candles[-2].get("close", 0) or 0)   # NO-LOOKAHEAD: last closed
    prev = float(candles[-(n_hours + 2)].get("close", 0) or 0)
    return (cur - prev) / prev * 100 if prev > 0 else 0.0


def make_fingerprint(components: dict) -> str:
    data = json.dumps(components, sort_keys=True)
    return hashlib.md5(data.encode()).hexdigest()[:8]


# ══════════════════════════════════════════════════════════════════════
#  BITGET CLIENT
# ══════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE_URL  = "https://api.bitget.com"
    _cache:   Dict[str, Tuple[float, list]] = {}
    CACHE_TTL = 55  # detik — refresh sebelum candle 1H baru

    @classmethod
    def _get(cls, path: str, params: dict = None) -> dict:
        try:
            resp = requests.get(
                f"{cls.BASE_URL}/{path}",
                params=params,
                headers={"User-Agent": f"NexusPB/{VERSION}"},
                timeout=15,
            )
            return resp.json()
        except Exception as e:
            log.warning("Bitget API error [%s]: %s", path, e)
            return {}

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        """Fetch semua USDT-Futures tickers. Return dict {symbol: ticker}."""
        data   = cls._get("api/v2/mix/market/tickers",
                          params={"productType": "USDT-FUTURES"})
        result = {}
        for t in data.get("data", []):
            sym = t.get("symbol", "")
            if sym.endswith("USDT"):
                result[sym] = t
        return result

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 210) -> List[dict]:
        """
        Fetch candle 1H. Limit 210 cukup untuk EMA200 + buffer.

        Return: list candle ascending (oldest first).
        candles[-1] = candle yang SEDANG BERJALAN (belum closed).
        candles[-2] = last closed candle. ← ini "sekarang" untuk kalkulasi.
        """
        cache = cls._cache.get(symbol)
        if cache and time.time() - cache[0] < cls.CACHE_TTL:
            return cache[1]

        data = cls._get("api/v2/mix/market/candles", params={
            "symbol":      symbol,
            "productType": "USDT-FUTURES",
            "granularity": "1H",
            "limit":       str(limit),
        })

        candles = []
        for row in data.get("data", []):
            try:
                # Bitget v2: [ts_ms, open, high, low, close, vol, vol_usd]
                vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                candles.append({
                    "ts":         int(row[0]) // 1000,
                    "open":       float(row[1]),
                    "high":       float(row[2]),
                    "low":        float(row[3]),
                    "close":      float(row[4]),
                    "volume_usd": vol_usd,
                })
            except Exception:
                continue

        candles.sort(key=lambda x: x["ts"])
        if candles:
            cls._cache[symbol] = (time.time(), candles)
        return candles

    @classmethod
    def get_funding(cls, symbol: str) -> float:
        """Fetch current funding rate. Return 0.0 jika gagal."""
        data = cls._get("api/v2/mix/market/current-fund-rate", params={
            "symbol":      symbol,
            "productType": "USDT-FUTURES",
        })
        try:
            return float(data["data"][0]["fundingRate"])
        except Exception:
            return 0.0


# ══════════════════════════════════════════════════════════════════════
#  COINALYZE CLIENT
# ══════════════════════════════════════════════════════════════════════
class CoinalyzeClient:
    """
    Client Coinalyze dengan API yang sudah terverifikasi dari scanner v16.

    Perbedaan kritis dari versi sebelumnya (semuanya menyebabkan 0 markets):
    - Endpoint: future-markets (bukan markets)
    - Exchange kode: "A"=Binance, "6"=Bybit (bukan string "binance_futures")
    - Field mapping: symbol_on_exchange → bitget_symbol (bukan field "symbol")
    - OI params: from/to timestamps + convert_to_usd=true (bukan limit)
    - OI interval: 1hour (bukan 1H)
    """
    BASE_URL      = "https://api.coinalyze.net/v1"
    _last_call    = 0.0
    BATCH_SIZE    = 5   # max symbols per request
    RATE_WAIT     = 1.2 # detik antar request

    def __init__(self, api_key: str):
        self.api_key       = api_key
        self._bn_map:  Dict[str, str] = {}  # bitget_sym → clz_sym (Binance)
        self._by_map:  Dict[str, str] = {}  # bitget_sym → clz_sym (Bybit)

    def _wait(self) -> None:
        elapsed = time.time() - CoinalyzeClient._last_call
        if elapsed < self.RATE_WAIT:
            time.sleep(self.RATE_WAIT - elapsed)
        CoinalyzeClient._last_call = time.time()

    def _get(self, endpoint: str, params: dict = None) -> Optional[Any]:
        """Rate-limited GET dengan retry 3x dan Retry-After handling."""
        p = {"api_key": self.api_key, **(params or {})}
        headers = {"User-Agent": f"NexusPB/{VERSION}"}
        for attempt in range(3):
            self._wait()
            try:
                resp = requests.get(
                    f"{self.BASE_URL}/{endpoint}", params=p,
                    headers=headers, timeout=20,
                )
                if resp.status_code == 429:
                    ra = resp.headers.get("Retry-After", "5")
                    try:    wait = int(float(ra)) + 1
                    except: wait = 6
                    log.warning("  Coinalyze rate limit — wait %ds (attempt %d/3)", wait, attempt+1)
                    time.sleep(wait + 1.5)
                    continue
                if resp.status_code != 200:
                    log.warning("  Coinalyze %s HTTP %d", endpoint, resp.status_code)
                    return None
                data = resp.json()
                if isinstance(data, dict) and "error" in data:
                    log.warning("  Coinalyze error: %s", data["error"])
                    return None
                return data
            except Exception as e:
                log.warning("  Coinalyze request error (attempt %d): %s", attempt+1, e)
                if attempt < 2:
                    time.sleep(3)
        return None

    def build_symbol_maps(self, bitget_symbols: List[str]) -> None:
        """
        Buat mapping bitget_symbol → coinalyze_symbol dari future-markets.

        Exchange kode (terverifikasi dari API):
          "A" = Binance Futures (data OI, funding, liquidation)
          "6" = Bybit (data L/S ratio)

        Symbol normalization: handles 1000BONKUSDT → BONKUSDT di Coinalyze.
        """
        log.info("  Loading Coinalyze future-markets...")
        data = self._get("future-markets", {})
        markets = data if isinstance(data, list) else []
        log.info("  Got %d Coinalyze future-markets", len(markets))

        if not markets:
            if isinstance(data, dict):
                log.warning("  future-markets error: %s", str(data)[:200])
            return

        # Build lookup: symbol_on_exchange → clz_symbol per exchange
        bn_lookup: Dict[str, str] = {}  # Binance: symbol_on_exchange → clz_sym
        by_lookup: Dict[str, str] = {}  # Bybit: symbol_on_exchange → clz_sym

        for m in markets:
            exc     = m.get("exchange", "")
            sym_exc = m.get("symbol_on_exchange", "")  # e.g. "BTCUSDT"
            clz_sym = m.get("symbol", "")              # e.g. "BTCUSDT.6" or "BTCUSDT.A"
            is_perp = m.get("is_perpetual", False)
            quote   = m.get("quote_asset", "").upper()

            if not (is_perp and quote == "USDT" and clz_sym and sym_exc):
                continue
            if exc == "A":   # Binance Futures
                bn_lookup[sym_exc] = clz_sym
            elif exc == "6": # Bybit
                by_lookup[sym_exc] = clz_sym

        def _candidates(sym: str) -> List[str]:
            """Generate variasi nama untuk matching (termasuk 1000X prefix)."""
            base = sym.replace("USDT", "")
            cands = [sym]
            if base.startswith("1000"):
                cands.append(base[4:] + "USDT")   # 1000BONKUSDT → BONKUSDT
            elif base.startswith("10000"):
                cands.append(base[5:] + "USDT")
            return cands

        mapped_bn = mapped_by = 0
        for sym in bitget_symbols:
            for cand in _candidates(sym):
                if cand in bn_lookup and sym not in self._bn_map:
                    self._bn_map[sym] = bn_lookup[cand]
                    mapped_bn += 1
                if cand in by_lookup and sym not in self._by_map:
                    self._by_map[sym] = by_lookup[cand]
                    mapped_by += 1

        n_reject = len(bitget_symbols) - len(self._bn_map)
        log.info(
            "  Mapping: %d/%d Binance | %d/%d Bybit | %d no proxy → REJECT",
            mapped_bn, len(bitget_symbols),
            mapped_by, len(bitget_symbols),
            n_reject,
        )

    def _batch_fetch(self, endpoint: str, symbols: List[str], params: dict) -> Dict[str, list]:
        """Fetch endpoint dalam batch BATCH_SIZE. Return {clz_symbol: history_list}."""
        result: Dict[str, list] = {}
        for i in range(0, len(symbols), self.BATCH_SIZE):
            batch = symbols[i: i + self.BATCH_SIZE]
            try:
                p = dict(params)
                p["symbols"] = ",".join(batch)
                data = self._get(endpoint, p)
                if data and isinstance(data, list):
                    for item in data:
                        sym  = item.get("symbol", "")
                        hist = item.get("history", [])
                        if hist:
                            hist = sorted(hist, key=lambda x: x.get("t", 0))
                        if sym and hist:
                            result[sym] = hist
            except Exception as e:
                log.warning("  _batch_fetch %s batch %d error: %s", endpoint, i//self.BATCH_SIZE+1, e)
        return result

    def fetch_oi_and_funding(self, bitget_symbols: List[str]) -> Dict[str, ClzData]:
        """
        Fetch OI history dan funding rate.

        Strategi dua lapis:
        1. Coinalyze (lebih kaya data) — bisa gagal jika IP di-block (403)
        2. Binance Futures API langsung (free, no key, no IP restriction) — fallback

        Coinalyze params yang benar:
          interval = 1hour | from/to = unix timestamps | convert_to_usd = true
        """
        result  = {sym: ClzData() for sym in bitget_symbols}
        now_ts  = int(time.time())
        from_ts = now_ts - 12 * 3600

        bn_syms = [self._bn_map[s] for s in bitget_symbols if s in self._bn_map]
        bn_rev  = {v: k for k, v in self._bn_map.items()}
        coinalyze_ok = 0

        if bn_syms:
            log.info("  Fetching OI via Coinalyze (%d syms)...", len(bn_syms))
            oi_raw = self._batch_fetch(
                "open-interest-history", bn_syms,
                {"interval": "1hour", "from": from_ts, "to": now_ts, "convert_to_usd": "true"},
            )
            for clz_sym, hist in oi_raw.items():
                bsym = bn_rev.get(clz_sym)
                if bsym:
                    result[bsym].oi             = hist
                    result[bsym].proxy_exchange = "coinalyze_binance"
                    coinalyze_ok += 1

            fund_raw = self._batch_fetch(
                "funding-rate-history", bn_syms,
                {"interval": "1hour", "from": from_ts, "to": now_ts},
            )
            for clz_sym, hist in fund_raw.items():
                bsym = bn_rev.get(clz_sym)
                if bsym:
                    result[bsym].funding_hist = hist

        # Fallback: Binance Futures API langsung untuk coin yang belum dapat OI
        # Aktif jika Coinalyze gagal (IP allowlist block = HTTP 403)
        need_fallback = [s for s in bitget_symbols if not result[s].oi]
        if need_fallback:
            log.info(
                "  Coinalyze OI: %d/%d OK | Binance direct fallback untuk %d sym...",
                coinalyze_ok, len(bitget_symbols), len(need_fallback),
            )
            fallback_ok = self._fetch_oi_binance_direct(need_fallback, result)
            log.info("  Binance direct OI: %d/%d OK", fallback_ok, len(need_fallback))

        total_oi = sum(1 for s in bitget_symbols if result[s].oi)
        log.info("  Total symbol dengan OI: %d/%d", total_oi, len(bitget_symbols))
        return result

    def _fetch_oi_binance_direct(
        self,
        bitget_symbols: List[str],
        result: Dict[str, ClzData],
    ) -> int:
        """
        Fallback: OI history dari Binance Futures public API.
        Tidak butuh API key, tidak ada IP restriction.

        Endpoint: fapi.binance.com/futures/data/openInterestHist
        Format output disamakan dengan Coinalyze: {"t": ts_sec, "c": oi_value}
        agar validate_oi_buildup() tidak perlu diubah.
        """
        BINANCE_BASE = "https://fapi.binance.com"
        ok_count     = 0

        for sym in bitget_symbols:
            # kBONKUSDT (Bitget prefix) → 1000BONKUSDT (Binance format)
            bn_sym = "1000" + sym[1:] if (sym.startswith("k") and len(sym) > 1 and sym[1].isupper()) else sym
            try:
                resp = requests.get(
                    f"{BINANCE_BASE}/futures/data/openInterestHist",
                    params={"symbol": bn_sym, "period": "1h", "limit": 12},
                    headers={"User-Agent": f"NexusPB/{VERSION}"},
                    timeout=10,
                )
                if resp.status_code != 200:
                    log.debug("  Binance OI %s: HTTP %d", sym, resp.status_code)
                    continue
                data = resp.json()
                if not isinstance(data, list) or not data:
                    continue
                hist = []
                for item in data:
                    try:
                        hist.append({
                            "t": int(item["timestamp"]) // 1000,
                            "c": float(item["sumOpenInterest"]),
                        })
                    except (KeyError, ValueError, TypeError):
                        continue
                if len(hist) >= 9:
                    result[sym].oi             = hist
                    result[sym].proxy_exchange = "binance_direct"
                    ok_count += 1
                time.sleep(0.1)
            except Exception as e:
                log.debug("  Binance direct OI %s: %s", sym, e)
        return ok_count


# ══════════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════════
def send_telegram(message: str) -> bool:
    token   = CONFIG["telegram_token"]
    chat_id = CONFIG["telegram_chat_id"]
    if not token or not chat_id:
        log.warning("Telegram tidak dikonfigurasi (TELEGRAM_TOKEN/TELEGRAM_CHAT_ID kosong)")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=15,
        )
        return resp.status_code == 200
    except Exception as e:
        log.warning("Telegram error: %s", e)
        return False


def format_alert(result: NexusResult, rank: int) -> str:
    now_wib    = datetime.fromtimestamp(time.time() + 7 * 3600, tz=timezone.utc)
    dt_str     = now_wib.strftime("%d/%m %H:%M WIB")
    proxy_note = f" via {result.oi_proxy.capitalize()} proxy"
    rr_ratio   = round(result.tp1_pct / result.sl_pct, 1) if result.sl_pct > 0 else 0

    return (
        f"🔵 <b>NEXUS-PB #{rank}</b> | {dt_str}\n"
        f"──────────────────────────\n"
        f"<b>{result.symbol}</b>  [PRE-BREAKOUT]\n"
        f"Score: {result.score}/100\n\n"
        f"🕐 <b>Squeeze</b>: {result.squeeze_duration_h:.1f} jam\n"
        f"📈 <b>EMA Cross</b>: Vol {result.cross_volume_ratio:.1f}x avg\n"
        f"📊 <b>OI Buildup</b>: {result.oi_change_6h_pct:+.1f}% (6h){proxy_note}\n"
        f"📉 <b>Jarak EMA200</b>: -{result.dist_to_ema200_pct:.1f}% "
        f"| slope: {result.ema200_slope:+.2f}%/candle\n"
        f"💹 <b>BTC 4h</b>: {result.btc_chg_4h:+.1f}%\n\n"
        f"💰 <b>ENTRY ZONE</b>\n"
        f"   Mid:  ${result.entry_price:.6g}\n"
        f"   SL:   ${result.sl_price:.6g} (-{result.sl_pct:.1f}%)\n"
        f"   TP1:  ${result.tp1_price:.6g} (+{result.tp1_pct:.1f}%) [RR {rr_ratio}:1]\n\n"
        f"📍 Vol: ${result.vol_24h/1e6:.1f}M | "
        f"Δ1h: {result.chg_1h:+.1f}% | "
        f"Δ4h: {result.chg_4h:+.1f}% | "
        f"Δ24h: {result.chg_24h:+.1f}%\n"
        f"   Fund: {result.funding*100:.4f}%\n"
        f"⏰ Est. lead time: 10-20 jam\n"
        f"🔑 {result.fingerprint}"
    )


# ══════════════════════════════════════════════════════════════════════
#  DETECTION FUNCTIONS
# ══════════════════════════════════════════════════════════════════════

def detect_bbw_squeeze_duration(candles: List[dict]) -> Tuple[bool, int, dict]:
    """
    Deteksi BBW squeeze dan durasinya.
    Toleransi 1 candle spike agar squeeze panjang tidak terpotong.

    NO-LOOKAHEAD: iterasi dimulai dari candles[-2] (last closed) ke belakang.
    candles[-1] (belum closed) tidak pernah masuk window BBW manapun.

    Return: (is_valid, duration_candles, detail_dict)
    """
    if len(candles) < 25:
        return False, 0, {"reason": "insufficient_candles"}

    threshold = CONFIG["bbw_squeeze_threshold"]
    min_dur   = CONFIG["bbw_squeeze_min_candles"]
    max_dur   = CONFIG["bbw_squeeze_max_candles"]
    TOLERANCE = 1  # max 1 spike candle sebelum squeeze dianggap berakhir

    squeeze_count = 0
    spike_streak  = 0
    bbw_values    = []

    # NO-LOOKAHEAD: start_idx = len-2 (last closed), bukan len-1
    start_idx = len(candles) - 2
    stop_idx  = max(start_idx - max_dur - 5, 20)

    for i in range(start_idx, stop_idx, -1):
        window = candles[max(0, i - 20):i]  # window[-1] = candles[i-1], tidak include i
        if len(window) < 20:
            break
        bbw = calc_bbw(window)
        bbw_values.append(bbw)

        if bbw < threshold:
            squeeze_count += 1
            spike_streak   = 0
        else:
            spike_streak += 1
            if spike_streak > TOLERANCE:
                break  # squeeze benar-benar berakhir

    current_bbw = bbw_values[0] if bbw_values else 0.0
    is_valid    = min_dur <= squeeze_count <= max_dur

    return is_valid, squeeze_count, {
        "squeeze_candles": squeeze_count,
        "current_bbw":     round(current_bbw, 4),
        "threshold":       threshold,
        "is_valid":        is_valid,
    }


def check_ema200_condition(candles: List[dict], price: float) -> Tuple[bool, dict]:
    """
    Validasi kondisi EMA200:
    1. EMA200 flat atau curl up (slope > ema200_slope_min % per candle)
    2. Harga berada 8-30% di bawah EMA200

    BUG-07 FIX: v2 pakai calc_ema(candles, 200) yang include candles[-1].
    Sekarang: closed = candles[:-1] untuk eksklusi candle belum closed.

    slope dihitung sebagai rata-rata perubahan per candle selama 3 candle
    terakhir: (EMA200_t0 - EMA200_t3) / EMA200_t3 / 3
    """
    if len(candles) < 207:  # 200 EMA period + 7 buffer
        return False, {"reason": "insufficient_for_ema200"}

    # NO-LOOKAHEAD: exclude candle[-1] yang belum closed
    closed = candles[:-1]

    ema200_now  = calc_ema(closed,        200)  # EMA200 at last closed (t=0)
    ema200_prev = calc_ema(closed[:-3],   200)  # EMA200 3 closed candles ago (t=-3)

    if ema200_now <= 0 or ema200_prev <= 0:
        return False, {"reason": "ema200_calc_error"}

    # Slope = perubahan % per candle (dibagi 3 karena span 3 candle)
    slope_pct = (ema200_now - ema200_prev) / ema200_prev * 100 / 3.0

    # Jarak harga ke EMA200 (positif = harga di bawah EMA200)
    # price sudah dari candles[-2].close (last closed) — dipanggil dari main
    dist_pct = (ema200_now - price) / ema200_now * 100

    slope_ok = slope_pct > CONFIG["ema200_slope_min"]
    dist_ok  = CONFIG["dist_to_ema200_min"] <= dist_pct <= CONFIG["dist_to_ema200_max"]

    return (slope_ok and dist_ok), {
        "ema200":    round(ema200_now, 6),
        "slope_pct": round(slope_pct, 3),
        "dist_pct":  round(dist_pct, 1),
        "slope_ok":  slope_ok,
        "dist_ok":   dist_ok,
    }


def detect_ema_cross(candles: List[dict]) -> Tuple[bool, dict]:
    """
    Deteksi EMA20 cross EMA50 dari bawah ke atas.

    Cross valid jika:
    - Terjadi dalam cross_max_candles_ago closed candle terakhir (max 2)
    - Volume candle cross >= cross_min_vol_ratio × avg 10 candle sebelumnya

    BUG-07 FIX: v2 pakai calc_ema(candles, 20/50) yang include candles[-1].
    Sekarang semua EMA dihitung relatif ke closed = candles[:-1].

    Terminologi setelah fix:
      closed[-1] = candles[-2] = last closed candle     (t=0)
      closed[-2] = candles[-3] = 1 closed candle lalu   (t=-1)
      closed[-3] = candles[-4] = 2 closed candles lalu  (t=-2)

    Cross "1 candle ago" = EMA cross terjadi di t=0 (last closed).
    Cross "2 candles ago" = EMA cross terjadi di t=-1.
    """
    if len(candles) < 57:  # 50 + 7 buffer
        return False, {"reason": "insufficient_candles"}

    # NO-LOOKAHEAD: semua EMA dari closed candles
    closed = candles[:-1]

    # EMA di 3 titik: t=0 (last closed), t=-1, t=-2
    ema20_t0 = calc_ema(closed,        20)
    ema50_t0 = calc_ema(closed,        50)
    ema20_t1 = calc_ema(closed[:-1],   20)
    ema50_t1 = calc_ema(closed[:-1],   50)
    ema20_t2 = calc_ema(closed[:-2],   20)
    ema50_t2 = calc_ema(closed[:-2],   50)

    # Cross dari bawah ke atas
    crossed_at_t0 = (ema20_t1 <= ema50_t1) and (ema20_t0 > ema50_t0)
    crossed_at_t1 = (ema20_t2 <= ema50_t2) and (ema20_t1 > ema50_t1)

    if not (crossed_at_t0 or crossed_at_t1):
        return False, {
            "reason": "no_fresh_cross",
            "ema20":  round(ema20_t0, 6),
            "ema50":  round(ema50_t0, 6),
        }

    # Tentukan candle cross dan window volume 10 candle sebelumnya
    if crossed_at_t0:
        cross_candle    = closed[-1]           # = candles[-2], last closed
        pre_vol_candles = closed[-11:-1]       # 10 candle sebelum cross candle
        candles_ago     = 1
    else:
        cross_candle    = closed[-2]           # = candles[-3]
        pre_vol_candles = closed[-12:-2]       # 10 candle sebelum cross candle
        candles_ago     = 2

    vols      = [c.get("volume_usd", 0) for c in pre_vol_candles if c.get("volume_usd")]
    avg_vol   = _mean(vols) if vols else 0
    cross_vol = cross_candle.get("volume_usd", 0)
    vol_ratio = round(cross_vol / avg_vol, 2) if avg_vol > 0 else 0.0

    vol_ok = vol_ratio >= CONFIG["cross_min_vol_ratio"]

    return vol_ok, {
        "crossed_candles_ago": candles_ago,
        "ema20":               round(ema20_t0, 6),
        "ema50":               round(ema50_t0, 6),
        "vol_ratio":           vol_ratio,
        "vol_ok":              vol_ok,
        "reason": (
            "" if vol_ok
            else f"cross_vol_low ({vol_ratio:.2f}x < {CONFIG['cross_min_vol_ratio']}x)"
        ),
    }


def validate_oi_buildup(clz: ClzData) -> Tuple[bool, dict]:
    """
    Validasi OI naik selama konsolidasi (dari Coinalyze Binance/Bybit proxy).

    OI naik + harga flat = smart money akumulasi.
    Ini pembeda utama akumulasi vs distribusi tanpa order flow data.

    Hard rule: OI data tidak tersedia → REJECT (bukan skip, bukan lolos default).
    Basis: T5 — T2>=20 wajib di 100% dari 10 sinyal HIT terbaik.
    """
    if not clz.has_oi:
        return False, {
            "reason": "no_oi_data",
            "proxy":  clz.proxy_exchange or "none",
        }

    oi_data = clz.oi
    if len(oi_data) < 9:
        return False, {
            "reason": "insufficient_oi_history",
            "proxy":  clz.proxy_exchange,
        }

    try:
        # NO-LOOKAHEAD: pakai [-2] karena bar terbaru mungkin belum complete
        # (mengikuti pattern working scanner v16)
        oi_now = float(oi_data[-2].get("c", 0) or 0)
        oi_6h  = float(oi_data[-8].get("c", 0) or 0)
    except (ValueError, TypeError):
        return False, {"reason": "oi_parse_error", "proxy": clz.proxy_exchange}

    if oi_6h <= 0:
        return False, {"reason": "zero_oi_baseline", "proxy": clz.proxy_exchange}

    oi_change_pct = (oi_now - oi_6h) / oi_6h * 100
    is_building   = oi_change_pct >= CONFIG["oi_change_6h_min"]

    return is_building, {
        "oi_change_6h_pct": round(oi_change_pct, 2),
        "oi_now":           round(oi_now),
        "oi_6h_ago":        round(oi_6h),
        "proxy":            clz.proxy_exchange,
    }


def check_btc_regime(btc_chg_4h: float) -> Tuple[bool, str]:
    """
    BTC harus flat atau mild uptrend untuk sinyal valid.
    BTC rally terlalu kuat = coin naik ikut-ikutan (bukan internal strength).
    BTC bearish = tekanan jual akan reverse cross.
    """
    mn = CONFIG["btc_4h_min"]
    mx = CONFIG["btc_4h_max"]
    if mn <= btc_chg_4h <= mx:
        return True, "BTC_FAVORABLE"
    elif btc_chg_4h > mx:
        return False, f"BTC_RALLY_TOO_STRONG ({btc_chg_4h:+.1f}%)"
    else:
        return False, f"BTC_BEARISH ({btc_chg_4h:+.1f}%)"


def detect_support_for_sl(candles: List[dict], price: float) -> Tuple[float, int, dict]:
    """
    Deteksi support level terdekat di bawah harga untuk SL placement.

    Support valid = level yang ditest 2+ kali tanpa breakdown (cluster lows).
    SL = support_low - 1% buffer.
    SL hard floor: 5% | soft cap: 12%.
    Basis: T4 data — avg drawdown sebelum HIT = -3.6%, max = -7.1%.

    NO-LOOKAHEAD: pakai lows dari closed candles (candles[:-1]).

    Return: (sl_price, support_test_count, detail_dict)
    """
    if len(candles) < 22:
        sl = round(price * 0.92, 8)
        return sl, 0, {"reason": "insufficient_candles", "sl_pct": 8.0}

    # NO-LOOKAHEAD: hanya closed candles
    closed = candles[:-1]
    lows   = [c["low"] for c in closed[-50:] if c.get("low") and c["low"] < price]

    if not lows:
        sl = round(price * 0.92, 8)
        return sl, 0, {"reason": "no_lows_below_price", "sl_pct": 8.0}

    # Cluster lows yang berdekatan (dalam 1.5% dari center)
    clusters = []
    for low in sorted(lows):
        placed = False
        for cluster in clusters:
            if abs(low - cluster["center"]) / cluster["center"] < 0.015:
                cluster["lows"].append(low)
                cluster["center"] = _mean(cluster["lows"])
                placed = True
                break
        if not placed:
            clusters.append({"center": low, "lows": [low]})

    # Cari cluster valid (>= 2 test) paling dekat di bawah harga
    best_support = None
    best_tests   = 0
    for cluster in sorted(clusters, key=lambda x: x["center"], reverse=True):
        if cluster["center"] < price * 0.98 and len(cluster["lows"]) >= 2:
            best_support = min(cluster["lows"])
            best_tests   = len(cluster["lows"])
            break

    if best_support:
        sl     = best_support * 0.99  # 1% buffer di bawah support
        sl_pct = (price - sl) / price * 100
        # Hard floor 5%: data T4 — 44% sinyal HIT butuh ruang > 3.6%
        if sl_pct < 5.0:
            sl     = price * 0.95
            sl_pct = 5.0
        elif sl_pct > 12.0:
            sl     = price * 0.88
            sl_pct = 12.0
    else:
        sl       = price * 0.92  # fallback 8%
        sl_pct   = 8.0
        best_tests = 0

    return round(sl, 8), best_tests, {
        "support_price": round(best_support, 8) if best_support else None,
        "support_tests": best_tests,
        "sl_pct":        round(sl_pct, 1),
    }


def score_nexus_pb(
    squeeze_h:     float,
    oi_change:     float,
    vol_ratio:     float,
    support_tests: int,
) -> int:
    """
    Scoring NEXUS-PB. Semua weight dari CONFIG (bukan hardcode di fungsi).
    Threshold: alert_threshold = 55/100.

    Komponen (total max 100):
      Squeeze duration  : max 30 — setup lebih matang = lebih reliable
      OI change 6h      : max 25 — OI buildup = smart money proxy
      Cross volume ratio: max 25 — konfirmasi real buying pressure
      Support quality   : max 20 — SL placement lebih valid

    JANGAN ubah weights sebelum 50 sinyal outcomes terkumpul.
    """
    score = 0
    c     = CONFIG

    if squeeze_h >= 24:   score += c["score_squeeze_24h"]
    elif squeeze_h >= 12: score += c["score_squeeze_12h"]
    elif squeeze_h >= 8:  score += c["score_squeeze_8h"]
    else:                 score += c["score_squeeze_6h"]

    if oi_change >= 20:   score += c["score_oi_high"]
    elif oi_change >= 10: score += c["score_oi_mid"]
    elif oi_change >= 5:  score += c["score_oi_low"]

    if vol_ratio >= 3.0:   score += c["score_vol_high"]
    elif vol_ratio >= 2.0: score += c["score_vol_mid"]
    elif vol_ratio >= 1.5: score += c["score_vol_low"]

    if support_tests >= 4:   score += c["score_supp_4"]
    elif support_tests >= 3: score += c["score_supp_3"]
    elif support_tests >= 2: score += c["score_supp_2"]

    return min(score, 100)


# ══════════════════════════════════════════════════════════════════════
#  MAIN SCORING FUNCTION
# ══════════════════════════════════════════════════════════════════════
def analyze_coin(coin_data: CoinData) -> Optional[NexusResult]:
    """
    Analisis satu coin melalui semua filter NEXUS-PB.

    Filter order: murah/cepat dulu untuk short-circuit lebih awal.
      1. Funding hard reject    (1 number check)
      2. BTC regime             (1 number check)
      3. BBW squeeze duration   (O(n) candle scan)
      4. EMA200 condition       (O(n) EMA calc × 2)
      5. EMA20 cross EMA50      (O(n) EMA calc × 6)
      6. OI buildup             (data sudah ada, hanya parse)

    Return: NexusResult jika semua lolos dan score >= threshold.
    Return: None jika di-reject di filter manapun.
    """
    sym = coin_data.symbol

    # ── Filter 1: Funding anomali ──────────────────────────────────────
    if coin_data.funding < CONFIG["funding_min"]:
        log.info("  ✗ %s REJECT: funding=%.4f%% (< %.4f%%)",
                 sym, coin_data.funding * 100, CONFIG["funding_min"] * 100)
        return None

    # ── Filter 2: BTC regime ───────────────────────────────────────────
    btc_ok, btc_reason = check_btc_regime(coin_data.btc_chg_4h)
    if not btc_ok:
        log.info("  ✗ %s REJECT: %s", sym, btc_reason)
        return None

    # ── Filter 3: BBW Squeeze duration ────────────────────────────────
    squeeze_ok, squeeze_candles, squeeze_d = detect_bbw_squeeze_duration(coin_data.candles)
    if not squeeze_ok:
        log.info(
            "  ✗ %s REJECT: squeeze=%dh bbw=%.3f (need %d-%dh)",
            sym, squeeze_candles, squeeze_d.get("current_bbw", 0),
            CONFIG["bbw_squeeze_min_candles"], CONFIG["bbw_squeeze_max_candles"],
        )
        return None
    squeeze_h = float(squeeze_candles)  # 1H candle = 1 jam

    # ── Filter 4: EMA200 condition ─────────────────────────────────────
    ema200_ok, ema200_d = check_ema200_condition(coin_data.candles, coin_data.price)
    if not ema200_ok:
        log.info(
            "  ✗ %s REJECT: EMA200 slope=%.3f%% dist=%.1f%% (slope_ok=%s dist_ok=%s)",
            sym, ema200_d.get("slope_pct", 0), ema200_d.get("dist_pct", 0),
            ema200_d.get("slope_ok"), ema200_d.get("dist_ok"),
        )
        return None

    # ── Filter 5: EMA Cross ────────────────────────────────────────────
    cross_ok, cross_d = detect_ema_cross(coin_data.candles)
    if not cross_ok:
        log.info(
            "  ✗ %s REJECT: %s vol_ratio=%.2fx",
            sym, cross_d.get("reason", "no_cross"), cross_d.get("vol_ratio", 0),
        )
        return None

    # ── Filter 6: OI Buildup ───────────────────────────────────────────
    oi_ok, oi_d = validate_oi_buildup(coin_data.clz)
    if not oi_ok:
        log.info(
            "  ✗ %s REJECT: OI %s (proxy=%s, need >=%.1f%%)",
            sym, oi_d.get("reason"), oi_d.get("proxy"), CONFIG["oi_change_6h_min"],
        )
        return None

    # ── Support detection & SL ─────────────────────────────────────────
    sl_price, support_tests, supp_d = detect_support_for_sl(
        coin_data.candles, coin_data.price
    )
    sl_pct = supp_d.get("sl_pct", 8.0)

    # ── Scoring ────────────────────────────────────────────────────────
    score = score_nexus_pb(
        squeeze_h     = squeeze_h,
        oi_change     = oi_d.get("oi_change_6h_pct", 0),
        vol_ratio     = cross_d.get("vol_ratio", 0),
        support_tests = support_tests,
    )

    log.info(
        "  ~ %s score=%d/%d | "
        "squeeze=%.0fh bbw=%.3f | "
        "ema200 dist=%.1f%% slope=%.3f%% | "
        "cross %.2fx (%d candle ago) | "
        "oi %.1f%% (%s) | "
        "supp=%d sl=%.1f%%",
        sym, score, CONFIG["alert_threshold"],
        squeeze_h, squeeze_d["current_bbw"],
        ema200_d["dist_pct"], ema200_d["slope_pct"],
        cross_d.get("vol_ratio", 0), cross_d.get("crossed_candles_ago", 0),
        oi_d.get("oi_change_6h_pct", 0), oi_d.get("proxy", "?"),
        support_tests, sl_pct,
    )

    if score < CONFIG["alert_threshold"]:
        log.info("  ✗ %s REJECT: score=%d < threshold=%d", sym, score, CONFIG["alert_threshold"])
        return None

    # ── TP Calculation (RR 2:1 berbasis SL distance) ──────────────────
    risk_pct  = sl_pct / 100
    tp1_price = coin_data.price * (1 + risk_pct * 2)
    tp1_pct   = (tp1_price - coin_data.price) / coin_data.price * 100

    # ── Fingerprint ────────────────────────────────────────────────────
    fp = make_fingerprint({
        "sym":     sym,
        "sqz_h":   round(squeeze_h),
        "ema200d": round(ema200_d["dist_pct"]),
        "oi":      round(oi_d.get("oi_change_6h_pct", 0)),
    })

    return NexusResult(
        symbol             = sym,
        score              = score,
        entry_price        = coin_data.price,
        sl_price           = sl_price,
        sl_pct             = round(sl_pct, 1),
        tp1_price          = round(tp1_price, 8),
        tp1_pct            = round(tp1_pct, 1),
        squeeze_duration_h = round(squeeze_h, 1),
        bbw_at_cross       = squeeze_d["current_bbw"],
        ema20              = cross_d.get("ema20", 0),
        ema50              = cross_d.get("ema50", 0),
        ema200             = ema200_d["ema200"],
        ema200_slope       = ema200_d["slope_pct"],
        dist_to_ema200_pct = ema200_d["dist_pct"],
        cross_volume_ratio = cross_d.get("vol_ratio", 0),
        oi_change_6h_pct   = oi_d.get("oi_change_6h_pct", 0),
        oi_proxy           = oi_d.get("proxy", "unknown"),
        funding            = coin_data.funding,
        btc_chg_4h         = coin_data.btc_chg_4h,
        chg_1h             = coin_data.chg_1h,
        chg_4h             = coin_data.chg_4h,
        chg_24h            = coin_data.chg_24h,
        vol_24h            = coin_data.vol_24h,
        fingerprint        = fp,
    )


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════
def main() -> None:
    log.info("══════════════════════════════════════════════════════════")
    log.info("  NEXUS-PB Pre-Breakout Scanner %s", VERSION)
    log.info("  Trigger : EMA20 cross EMA50 + BBW squeeze + OI buildup")
    log.info("  Interval: 30 menit | DB: %s", CONFIG["db_path"])
    log.info("══════════════════════════════════════════════════════════")

    init_db()

    # ── Step 1: Fetch Bitget tickers ───────────────────────────────────
    log.info("📡 Fetching Bitget tickers...")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        log.error("Tidak ada ticker dari Bitget — abort.")
        return
    log.info("  Bitget: %d USDT-Futures symbols", len(tickers))

    # ── Step 2: Update outcomes sinyal sebelumnya ──────────────────────
    log.info("📊 Updating previous signal outcomes...")
    check_and_update_outcomes(tickers)

    # ── Step 3: BTC context ────────────────────────────────────────────
    btc_candles = BitgetClient.get_candles("BTCUSDT", limit=15)
    btc_chg_1h  = get_chg_from_candles(btc_candles, 1) if btc_candles else 0.0
    btc_chg_4h  = get_chg_from_candles(btc_candles, 4) if btc_candles else 0.0
    btc_price   = float(tickers.get("BTCUSDT", {}).get("lastPr", 0) or 0)
    log.info("  BTC: $%s | 1h: %+.2f%% | 4h: %+.2f%%",
             f"{btc_price:,.0f}", btc_chg_1h, btc_chg_4h)

    # ── Step 4: Phase 1 — Pre-filter ──────────────────────────────────
    log.info("🔍 Phase 1: Pre-filtering symbols...")
    candidates = []

    for sym, ticker in tickers.items():
        if sym not in CONFIG["whitelist"]:
            continue
        if sym == "BTCUSDT":
            continue
        if sym in CONFIG["stock_token_blacklist"]:
            continue
        if is_on_cooldown(sym):
            continue

        try:
            vol_24h = float(ticker.get("quoteVolume", 0) or 0)
            price   = float(ticker.get("lastPr", 0) or 0)
            raw_chg = float(ticker.get("change24h", 0) or 0)
            # Bitget v2 API: change24h = desimal (0.084 = 8.4%)
            chg_24h = raw_chg * 100 if abs(raw_chg) <= 10 else raw_chg

            if vol_24h < CONFIG["min_vol_24h_usd"]:  continue
            if not (CONFIG["chg_24h_min"] <= chg_24h <= CONFIG["chg_24h_max"]): continue
            if price <= 0:  continue

            # Fetch candles: 210 untuk EMA200 + buffer
            candles = BitgetClient.get_candles(sym)
            if len(candles) < 207:
                continue

            # NO-LOOKAHEAD: chg dari candles[-2]
            chg_1h = get_chg_from_candles(candles, 1)
            chg_4h = get_chg_from_candles(candles, 4)

            if chg_1h > CONFIG["chg_1h_max"]:
                continue

            # Quick BBW pre-check — BUG-08 FIX: pakai candles[:-1]
            bbw = calc_bbw(candles[:-1])
            if bbw >= CONFIG["bbw_squeeze_threshold"] * 1.2:
                continue  # Jelas tidak squeeze, skip

            candidates.append((sym, price, vol_24h, chg_1h, chg_4h, chg_24h, candles))

        except Exception as e:
            log.debug("  Pre-filter error [%s]: %s", sym, e)

    log.info("  Phase 1 passed: %d candidates", len(candidates))

    if not candidates:
        log.info("  No candidates — done.")
        log.info("══════════════════════════════════════════════════════════")
        return

    # ── Step 5: Build Coinalyze symbol map ───────────────────────────
    log.info("🗺️  Building Coinalyze symbol maps...")
    clz_client = CoinalyzeClient(CONFIG["coinalyze_key"])
    cand_syms  = [s for s, *_ in candidates]
    clz_client.build_symbol_maps(cand_syms)

    # ── Step 6: Fetch OI + Funding ────────────────────────────────────
    log.info("📈 Fetching Coinalyze OI & funding...")
    clz_map: Dict[str, ClzData] = clz_client.fetch_oi_and_funding(cand_syms)

    # ── Step 7: Phase 2 — Full analysis ───────────────────────────────
    log.info("🎯 Phase 2: Full NEXUS-PB analysis (%d candidates)...", len(candidates))
    final_results = []

    for sym, price, vol_24h, chg_1h, chg_4h, chg_24h, candles in candidates:
        # Tidak ada proxy → hard reject (PERINGATAN #5)
        if sym not in clz_map:
            log.info("  ✗ %s REJECT: tidak ada OI proxy (tidak di Binance maupun Bybit)", sym)
            continue

        try:
            funding   = BitgetClient.get_funding(sym)

            log.info(
                "  → %s: chg24h=%+.1f%% chg1h=%+.1f%% chg4h=%+.1f%% "
                "vol=$%.1fM fund=%.4f%% proxy=%s",
                sym, chg_24h, chg_1h, chg_4h,
                vol_24h / 1e6, funding * 100,
                clz_map[sym].proxy_exchange,
            )

            coin_data = CoinData(
                symbol     = sym,
                price      = price,
                vol_24h    = vol_24h,
                chg_24h    = chg_24h,
                chg_1h     = chg_1h,
                chg_4h     = chg_4h,
                funding    = funding,
                candles    = candles,
                btc_chg_1h = btc_chg_1h,
                btc_chg_4h = btc_chg_4h,
                clz        = clz_map[sym],
            )

            result = analyze_coin(coin_data)
            if result:
                final_results.append(result)
                log.info(
                    "  ✅ %s: score=%d squeeze=%.0fh oi=%.1f%% "
                    "cross=%.2fx sl=%.1f%% proxy=%s",
                    sym, result.score, result.squeeze_duration_h,
                    result.oi_change_6h_pct, result.cross_volume_ratio,
                    result.sl_pct, result.oi_proxy,
                )

        except Exception as e:
            log.warning("  %s analysis error: %s", sym, e)

    # ── Step 8: Sort, save, send ───────────────────────────────────────
    final_results.sort(key=lambda x: -x.score)

    log.info("")
    log.info("══════════════════════════════════════════════════════════")
    log.info("  DONE: %d signal(s) this cycle", len(final_results))
    log.info("══════════════════════════════════════════════════════════")

    for i, result in enumerate(final_results[:3], 1):  # max 3 alert per run
        log.info(
            "  #%d %s score=%d squeeze=%.0fh oi=%.1f%% cross=%.2fx",
            i, result.symbol, result.score,
            result.squeeze_duration_h, result.oi_change_6h_pct,
            result.cross_volume_ratio,
        )
        save_signal(result)
        msg = format_alert(result, i)
        if send_telegram(msg):
            log.info("  📱 Alert sent: %s", result.symbol)
        else:
            log.warning("  ⚠️  Alert failed (Telegram): %s", result.symbol)

    if not final_results:
        log.info("  No signals this cycle.")

    log.info("══════════════════════════════════════════════════════════")


if __name__ == "__main__":
    main()
