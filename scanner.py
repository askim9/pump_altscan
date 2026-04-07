#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v14.9 — DATA-AUDITED (104 PUMP EVENTS, 120 SIMBOL)        ║
║                                                                              ║
║  PERBAIKAN DARI AUDIT RAW DATA v2 (104 pump events, 120 simbol, 46 fitur):  ║
║                                                                              ║
║  [BUG KRITIS DIPERBAIKI]                                                     ║
║  • detect_price_stability DIINVERT: reward range LEBAR (pump_med=3.45%),    ║
║    bukan sempit. COILING (range<1.5%) sekarang dapat skor 0.                ║
║  • rs_24h threshold OUTPERFORM: 0.5 → 0.3 (median aktual=0.366, bukan      ║
║    0.728 di JSON — delta 0.362). Threshold lama hanya cover 37% pump events.║
║  • Type D trigger: hapus stab_sc>=6 (anti-pump). inside_compression=0 pada  ║
║    63.5% pump events. Syarat baru: bbw_sc>=8 + dry_sc>=5 + CLZ confirmed.  ║
║                                                                              ║
║  [WEIGHT DISESUAIKAN]                                                        ║
║  • momentum_decel_weight: 12 → 8 (signal melemah di v2 universe 120 simbol, ║
║    median -0.364 → -0.062, 53% pump events negatif vs 60% di v1)           ║
║                                                                              ║
║  [FITUR BARU]                                                                ║
║  • detect_lower_wick + upper wick comparison: reward lower>upper             ║
║    (net buying pressure), penalti upper>>lower (net selling pressure).       ║
║    last_wick_up STRONG rank 5 (v2) — pump/rang 4.6x, dump/rang 7.2x.       ║
║                                                                              ║
║  [DIPERTAHANKAN DARI v14.8]                                                  ║
║  • BBW lebar = bullish (diinvert dari v14.6)                                 ║
║  • ATR absolut tinggi = fitur #1 (komponen ganda)                            ║
║  • RS 1h negatif = catchup pending (diinvert)                                ║
║  • L/S threshold relaksasi | DistSupport 96c | threshold EARLY 95           ║
║  • Type D konfirmasi Coinalyze wajib                                         ║
║  • Blacklist case-insensitive + simbol saham                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import logging
import logging.handlers as _lh
import os
import time
import sqlite3
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

VERSION = "14.9-DATA-AUDITED"

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch   = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/scanner_v14.log", maxBytes=10 * 1024**2, backupCount=3)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG v14.8
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    "coinalyze_api_key":  os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token":          os.getenv("BOT_TOKEN"),
    "chat_id":            os.getenv("CHAT_ID"),

    # ── Universe filter ───────────────────────────────────────────────────────
    "pre_filter_vol_min":     300_000,
    "pre_filter_vol_max":   100_000_000,
    "max_symbols_per_scan":         150,

    # ── Velocity gates ────────────────────────────────────────────────────────
    "velocity_gates": {
        "chg_1h_max":                4.0,
        "chg_4h_max":                8.0,
        "chg_24h_max_early":        12.0,
        "chg_24h_max_continuation": 30.0,
        "chg_24h_min":              -8.0,
    },

    # ── Bitget candle settings ────────────────────────────────────────────────
    "candle_limit_bitget": 100,

    # ── Coinalyze settings ────────────────────────────────────────────────────
    "coinalyze_lookback_h":          72,
    "coinalyze_funding_lookback_h": 168,
    "coinalyze_interval":        "1hour",
    "coinalyze_funding_interval":  "daily",
    "coinalyze_funding_interval_alt": "1hour",
    "coinalyze_batch_size":           10,
    "coinalyze_rate_limit_wait":     1.2,

    # ── TIER 1 weights (Coinalyze) ─────────────────────────────────────────────
    "ls_ratio_weight":           35,
    "buy_vol_ratio_weight":      30,
    "funding_trend_weight":      25,
    "funding_snapshot_weight":   15,
    "predicted_funding_weight":  20,
    "oi_buildup_weight":         20,
    "short_liq_weight":          20,
    "liq_cascade_weight":        15,

    # ── TIER 3 weights (Bitget candles — dari riset 269 pump events) ──────────
    "bbw_squeeze_weight":        5,    # BBW LEBAR = bullish (diinvert v14.7)
    "accumulation_weight":       15,
    "price_stability_weight":    7,
    "volume_dryup_weight":       5,
    "volatility_return_weight":  22,   # ATR absolut = fitur #1 (disc=117)
    "rs_btc_weight":             8,    # RS 1h negatif = catchup pending

    # ── FITUR BARU v14.7 (dipertahankan) ──────────────────────────────────────
    "lower_wick_weight":         15,   # disc=90.75, lift=2.56x
    "momentum_decel_weight":     8,    # disc=46.85, v14.9: turun 12→8 (signal melemah di v2 universe 120 simbol, median -0.364→-0.062)
    "dist_to_support_weight":    10,   # pump median 1% dari support
    "rs_24h_weight":             10,   # outperform BTC 24h sebelum pump

    # ── Alert thresholds (v14.8: EARLY naik 90→95 karena signal rate 24.7%) ──
    "multiwave_bonus":           30,
    "alert_threshold_early":     95,   # ← NAIK dari 90 (signal rate terlalu tinggi)
    "alert_threshold_continuation": 100,
    "alert_threshold_reversal":  80,

    # ── Entry/SL/TP ───────────────────────────────────────────────────────────
    "min_rr_ratio":         2.0,
    "max_alerts_per_scan":    5,
    "atr_candles":           14,
    "sl_mult_volatile":     2.5,
    "sl_mult_normal":       2.0,
    "sl_mult_quiet":        1.5,
    "tp1_pct":             15.0,
    "tp2_pct":             30.0,
    "tp3_pct":             50.0,

    # ── Position sizing ───────────────────────────────────────────────────────
    "account_balance":       10000.0,
    "risk_per_trade_pct":      1.0,
    "max_position_pct":        5.0,
    "max_leverage":           10,

    # ── History DB ────────────────────────────────────────────────────────────
    "pump_history_db":    "/tmp/scanner_v14_history.db",
    "pump_threshold_pct":    15,
    "pump_max_duration_h":   24,
    "multiwave_lookback_days": 30,

    # ── Circuit breaker ───────────────────────────────────────────────────────
    "btc_dump_threshold":  -3.0,

    # ── L/S ratio thresholds (v14.8: DIRELAKSASI sesuai data aktual Bybit) ───
    # Bug: semua sinyal L/S=0 karena threshold terlalu ketat vs data nyata.
    # Data Bybit aktual: long ratio cenderung di 0.43–0.57 (bukan di bawah 0.38).
    "ls_long_extreme_low":  0.42,   # ← NAIK dari 0.38 (lebih realistis)
    "ls_long_low":          0.47,   # ← NAIK dari 0.44 (mencakup lebih banyak)
    "ls_long_normal":       0.50,
    "ls_long_high":         0.58,

    # ── Buy volume thresholds ─────────────────────────────────────────────────
    "bv_ratio_strong":      0.62,
    "bv_ratio_moderate":    0.55,

    # ── Pump type thresholds ──────────────────────────────────────────────────
    "short_squeeze_ls_min":    8,    # ← TURUN dari 10 (sesuai relaksasi L/S)
    "short_squeeze_liq_min":   6,
    "short_squeeze_fund_min":  7,
    "whale_accum_bv_min":      8,
    "whale_accum_accum_min":   5,

    # ── squeeze_alt threshold (v14.8: diperketat — mencegah false positive) ──
    # Bug: squeeze_alt terlalu mudah terpenuhi (fund>=20 dan liq>=15 sebelumnya).
    "squeeze_alt_fund_liq_fund":   20,  # fund threshold
    "squeeze_alt_fund_liq_liq":    18,  # ← NAIK dari 15 (lebih ketat)
    "squeeze_alt_fund_pred_fund":  15,
    "squeeze_alt_fund_pred_pred":  10,
    "squeeze_alt_fund_pred_liq":    8,  # ← NAIK dari 6

    # ── Type D konfirmasi Coinalyze ────────────────────────────────────────────
    "type_d_min_oi_sc":         6,
    "type_d_min_liq_sc":        6,
    "type_d_min_fund_sc":      10,

    # ── DistToSupport (v14.8: window diperluas) ────────────────────────────────
    # Bug: fungsi tidak pernah aktif karena window 48 candle tidak cukup.
    "support_candle_window":   96,   # ← NAIK dari 48 (gunakan 4 hari)
    "support_cluster_tol":   0.020,  # ← NAIK dari 0.015 (toleransi cluster 2%)
    "support_bounce_min":        2,  # minimum bounce (sudah di v14.7, dipertahankan)
    "support_bounce_max":        5,  # bounce 5+ = ranging (diperketat dari 4)

    # ── Blacklist ─────────────────────────────────────────────────────────────
    "stock_token_blacklist": [
        "HOODUSDT", "COINUSDT", "MSTRUSDT", "NVDAUSDT", "AAPLUSDT",
        "GOOGLUSDT", "AMZNUSDT", "METAUSDT", "QQQUSDT", "BZUSDT",
        "MCDUSDT", "NIGHTUSDT", "JCTUSDT", "NOMUSDT", "ASTERUSDT",
        "POLYXUSDT", "PIUSDT", "WMTUSDT", "BGBUSDT", "MEUSDT",
        "TSLAUSDT", "CRCLUSDT", "SPYUSDT", "GLDUSDT", "MSFTUSDT",
        "PLTRUSDT", "INTCUSDT", "XAUSDT", "USDCUSDT", "TRXUSDT",
    ],

    # ── Pre-filter Bitget opsional ────────────────────────────────────────────
    "prefilter_bitget_top_n": 0,   # 0=nonaktif, contoh: 60
}


# ══════════════════════════════════════════════════════════════════════════════
#  📊  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ClzData:
    ohlcv:                  List[dict] = field(default_factory=list)
    oi:                     List[dict] = field(default_factory=list)
    liq:                    List[dict] = field(default_factory=list)
    funding_hist:           List[dict] = field(default_factory=list)
    predicted_funding_hist: List[dict] = field(default_factory=list)
    ls_ratio:               List[dict] = field(default_factory=list)

    @property
    def has_ohlcv(self) -> bool:             return len(self.ohlcv) >= 10
    @property
    def has_oi(self) -> bool:                return len(self.oi) >= 4
    @property
    def has_liq(self) -> bool:               return len(self.liq) >= 4
    @property
    def has_funding_hist(self) -> bool:      return len(self.funding_hist) >= 3
    @property
    def has_predicted_funding(self) -> bool: return len(self.predicted_funding_hist) >= 3
    @property
    def has_ls(self) -> bool:                return len(self.ls_ratio) >= 4

    @property
    def last_buy_ratio(self) -> float:
        if not self.has_ohlcv:
            return 0.0
        for c in reversed(self.ohlcv[:-1]):
            v  = float(c.get("v",  0) or 0)
            bv = float(c.get("bv", 0) or 0)
            if v > 0:
                return bv / v
        return 0.0

    @property
    def last_ls_long(self) -> float:
        if not self.has_ls:
            return 0.5
        return float(self.ls_ratio[-2].get("l", 0.5) or 0.5)

    @property
    def last_ls_ratio(self) -> float:
        if not self.has_ls:
            return 1.0
        return float(self.ls_ratio[-2].get("r", 1.0) or 1.0)


@dataclass
class CoinData:
    symbol:      str
    price:       float
    vol_24h:     float
    chg_24h:     float
    chg_1h:      float
    chg_4h:      float
    funding:     float
    candles:     List[dict]
    btc_chg_1h:  float   = 0.0
    btc_chg_24h: float   = 0.0
    clz:         ClzData = field(default_factory=ClzData)


@dataclass
class PhaseInfo:
    phase:       str
    base_score:  int
    description: str
    risk_level:  str


@dataclass
class PumpType:
    type_code:  str
    type_name:  str
    confidence: int
    signals:    List[str]


@dataclass
class PumpEvent:
    symbol:         str
    timestamp:      datetime
    magnitude_pct:  float
    duration_hours: float
    type:           str


@dataclass
class ScoreResult:
    symbol:        str
    score:         int
    phase:         str
    pump_types:    List[PumpType]
    confidence:    str
    components:    Dict[str, Any]
    catalysts:     List[str]
    entry:         Optional[dict]
    price:         float
    vol_24h:       float
    chg_24h:       float
    chg_1h:        float
    funding:       float
    urgency:       str
    risk_warnings: List[str]  = field(default_factory=list)
    position:      Optional[dict] = None


# ══════════════════════════════════════════════════════════════════════════════
#  🗄️  DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def init_db():
    db   = CONFIG["pump_history_db"]
    conn = sqlite3.connect(db)
    c    = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS pump_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL, timestamp INTEGER NOT NULL,
            magnitude_pct REAL NOT NULL, duration_hours REAL NOT NULL,
            event_type TEXT NOT NULL,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL, alerted_at INTEGER NOT NULL,
            score INTEGER, phase TEXT, entry_price REAL,
            outcome_pct REAL, outcome_checked INTEGER DEFAULT 0
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_sym_ts    ON pump_events(symbol, timestamp DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_alert_sym ON alerts(symbol, alerted_at DESC)")
    conn.commit()
    conn.close()


def is_on_cooldown(symbol: str, cooldown_hours: int = 6) -> bool:
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        c    = conn.cursor()
        c.execute("SELECT MAX(alerted_at) FROM alerts WHERE symbol = ?", (symbol,))
        row  = c.fetchone()
        conn.close()
        if row and row[0]:
            return (time.time() - row[0]) < (cooldown_hours * 3600)
    except Exception:
        pass
    return False


def set_alert(symbol: str, score: int, phase: str, entry_price: float):
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        c    = conn.cursor()
        c.execute(
            "INSERT INTO alerts (symbol, alerted_at, score, phase, entry_price) VALUES (?,?,?,?,?)",
            (symbol, int(time.time()), score, phase, entry_price),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"set_alert failed: {e}")


def get_pump_history(symbol: str, days: int = 30) -> List[PumpEvent]:
    try:
        conn   = sqlite3.connect(CONFIG["pump_history_db"])
        c      = conn.cursor()
        cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        c.execute("""
            SELECT timestamp, magnitude_pct, duration_hours, event_type
            FROM pump_events WHERE symbol = ? AND timestamp >= ?
            ORDER BY timestamp DESC
        """, (symbol, cutoff))
        events = [
            PumpEvent(symbol, datetime.fromtimestamp(r[0], tz=timezone.utc), r[1], r[2], r[3])
            for r in c.fetchall()
        ]
        conn.close()
        return events
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  🔧  HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def robust_zscore(val: float, baseline: List[float]) -> float:
    if not baseline or len(baseline) < 2:
        return 0.0
    med        = sorted(baseline)[len(baseline) // 2]
    deviations = [abs(x - med) for x in baseline]
    mad        = sorted(deviations)[len(deviations) // 2]
    if mad < 1e-9:
        return 0.0
    return (val - med) / (mad * 1.4826)


def get_chg_from_candles(candles: List[dict], n_hours: int) -> float:
    if len(candles) < n_hours + 2:
        return 0.0
    now_price  = candles[-2]["close"]
    prev_price = candles[-(n_hours + 2)]["close"]
    if prev_price <= 0:
        return 0.0
    return (now_price - prev_price) / prev_price * 100


def get_hour_utc() -> int:
    return datetime.now(timezone.utc).hour


def volume_tod_mult(hour: int) -> float:
    if 2 <= hour <= 8:
        return 1.35
    elif 13 <= hour <= 21:
        return 0.88
    return 1.0


def is_stock_token(symbol: str) -> bool:
    blacklist = {s.strip().upper() for s in CONFIG.get("stock_token_blacklist", [])}
    return symbol.strip().upper() in blacklist


# ══════════════════════════════════════════════════════════════════════════════
#  📐  ATR & ENTRY / SL / TP
# ══════════════════════════════════════════════════════════════════════════════
def calc_atr(candles: List[dict], n: int = 14) -> float:
    trs = []
    for i in range(2, min(n + 2, len(candles))):
        c  = candles[-i]
        pc = candles[-(i + 1)]["close"]
        if pc > 0:
            tr = max(
                (c["high"] - c["low"]) / pc,
                abs(c["high"] - pc) / pc,
                abs(c["low"]  - pc) / pc,
            )
            trs.append(tr)
    return _mean(trs) if trs else 0.02


def calc_entry_targets(data: CoinData) -> Optional[dict]:
    if len(data.candles) < 16:
        return None
    atr   = calc_atr(data.candles, CONFIG["atr_candles"])
    entry = data.price
    if atr > 0.04:
        sl_mult = CONFIG["sl_mult_volatile"]
    elif atr > 0.02:
        sl_mult = CONFIG["sl_mult_normal"]
    else:
        sl_mult = CONFIG["sl_mult_quiet"]
    sl     = entry * (1 - atr * sl_mult)
    sl_pct = (entry - sl) / entry * 100
    tp1    = entry * (1 + CONFIG["tp1_pct"] / 100)
    tp2    = entry * (1 + CONFIG["tp2_pct"] / 100)
    tp3    = entry * (1 + CONFIG["tp3_pct"] / 100)
    risk   = entry - sl
    if risk <= 0:
        return None
    rr1 = (tp1 - entry) / risk
    if rr1 < CONFIG["min_rr_ratio"]:
        return None
    return {
        "entry":            round(entry, 8),
        "entry_zone_low":   round(entry * (1 - atr * 0.3), 8),
        "entry_zone_high":  round(entry * (1 + atr * 0.2), 8),
        "sl":               round(sl, 8),
        "sl_pct":           round(sl_pct, 1),
        "tp1":              round(tp1, 8), "tp1_pct": CONFIG["tp1_pct"],
        "tp2":              round(tp2, 8), "tp2_pct": CONFIG["tp2_pct"],
        "tp3":              round(tp3, 8), "tp3_pct": CONFIG["tp3_pct"],
        "rr1":              round(rr1, 2),
        "rr2":              round((tp2 - entry) / risk, 2),
        "atr_pct":          round(atr * 100, 2),
        "atr_decimal":      atr,
        "sl_mult":          sl_mult,
    }


def calc_position_size(entry: float, sl: float, atr: float) -> dict:
    bal           = CONFIG["account_balance"]
    risk_usd      = bal * CONFIG["risk_per_trade_pct"] / 100
    risk_per_unit = (entry - sl) / entry
    if risk_per_unit <= 0:
        risk_per_unit = atr * CONFIG["sl_mult_normal"]
    pos_needed = risk_usd / risk_per_unit
    pos_cap    = bal * CONFIG["max_position_pct"] / 100
    pos_val    = min(pos_needed, pos_cap)
    leverage   = min(pos_val / bal, CONFIG["max_leverage"]) if pos_val > bal else 1.0
    pos_val    = min(pos_val, bal * max(leverage, 1))
    return {
        "position_size":  round(pos_val / entry, 6) if entry > 0 else 0,
        "leverage":       round(leverage, 2),
        "risk_usd":       round(risk_usd, 2),
        "position_value": round(pos_val, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  PHASE CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════
def classify_phase(chg_24h: float) -> PhaseInfo:
    if chg_24h < -8.0:
        return PhaseInfo("DOWNTREND", 5, "Deep downtrend", "HIGH")
    elif chg_24h < -3.0:
        return PhaseInfo("WEAK", 15, "Weak / pemulihan awal", "MEDIUM-HIGH")
    elif chg_24h > 25.0:
        return PhaseInfo("PARABOLIC", 10, "Parabolic", "EXTREME")
    elif chg_24h > 12.0:
        base = max(20, 40 - int(chg_24h - 12) * 2)
        return PhaseInfo("CONTINUATION", base, "Momentum continuation", "MEDIUM")
    else:
        if abs(chg_24h) <= 3.0:
            base = 45
        elif chg_24h <= 8.0:
            base = 40
        else:
            base = 35
        return PhaseInfo("EARLY", base, "Early — prime zone", "LOW")


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  TIER 1 SIGNALS — COINALYZE MULTI-EXCHANGE
# ══════════════════════════════════════════════════════════════════════════════
def score_long_short_ratio(clz: ClzData) -> Tuple[int, dict]:
    """
    v14.8 FIX: Bug — L/S score selalu 0 karena threshold terlalu ketat.
    Data aktual Bybit: long ratio di 0.43–0.57 (bukan ekstrem di bawah 0.38).
    Perbaikan:
    - ls_long_extreme_low: 0.38 → 0.42
    - ls_long_low: 0.44 → 0.47
    - Tambah tier menengah: 0.47–0.50 = partial score
    - Relaksasi trend threshold: -0.03 → -0.02
    - Selalu log nilai aktual untuk monitoring
    """
    if not clz.has_ls:
        return 0, {"source": "no_ls_data"}
    hist = clz.ls_ratio
    if len(hist) < 4:
        return 0, {"source": "insufficient_ls"}

    current_long  = float(hist[-2].get("l", 0.5) or 0.5)
    current_short = float(hist[-2].get("s", 0.5) or 0.5)
    ls_ratio_val  = float(hist[-2].get("r", 1.0) or 1.0)
    long_4h_ago   = float(hist[-5].get("l", 0.5) or 0.5) if len(hist) >= 5 else current_long
    long_trend    = current_long - long_4h_ago

    score, signals = 0, []
    cfg = CONFIG

    # Tier posisi shorts dominan
    if current_long < cfg["ls_long_extreme_low"]:       # < 0.42
        score += 30
        signals.append(f"EXTREME_SHORT_DOM longs={current_long:.1%}")
    elif current_long < cfg["ls_long_low"]:             # 0.42–0.47
        score += 20
        signals.append(f"SHORT_DOM longs={current_long:.1%}")
    elif current_long < cfg["ls_long_normal"]:          # 0.47–0.50 — tier baru v14.8
        score += 10
        signals.append(f"SLIGHT_SHORT_DOM longs={current_long:.1%}")

    # Tier trend: shorts sedang bertambah
    if long_trend < -0.02:                              # ← RELAKSASI dari -0.03
        score += 12
        signals.append(f"SHORTS_ADDING Δ={long_trend:.2%}")
    elif long_trend < -0.010:                           # ← RELAKSASI dari -0.015
        score += 6
        signals.append(f"LONGS_REDUCING Δ={long_trend:.2%}")
    elif long_trend < -0.005:                           # tier baru v14.8
        score += 3
        signals.append(f"SLIGHT_LONGS_REDUCING Δ={long_trend:.2%}")

    # Penalti jika terlalu banyak long
    if current_long > cfg["ls_long_high"]:
        score = max(0, score - 15)
        signals.append(f"⚠️ LONG_HEAVY={current_long:.1%}")

    # Diagnostik log: selalu tampilkan nilai aktual (bug monitor)
    log.debug(f"    L/S: long={current_long:.3f} trend={long_trend:+.3f} score={score}")

    return min(score, cfg["ls_ratio_weight"]), {
        "long_ratio":    round(current_long, 4),
        "short_ratio":   round(current_short, 4),
        "ls_ratio_val":  round(ls_ratio_val, 4),
        "long_trend_4h": round(long_trend, 4),
        "signals":       signals,
    }


def score_buy_volume_ratio(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_ohlcv:
        return 0, {"source": "no_ohlcv"}
    hist   = clz.ohlcv
    cfg    = CONFIG
    recent = [c for c in hist[-7:-1] if float(c.get("v", 0) or 0) > 0]
    if len(recent) < 3:
        return 0, {"source": "insufficient_ohlcv"}
    bv_ratios = []
    for c in recent:
        v  = float(c.get("v",  0) or 0)
        bv = float(c.get("bv", 0) or 0)
        if v > 0:
            bv_ratios.append(bv / v)
    if not bv_ratios:
        return 0, {"source": "no_bv_data"}
    avg_bv_ratio  = _mean(bv_ratios)
    last_bv_ratio = bv_ratios[-1]
    if len(bv_ratios) >= 3:
        early_avg = _mean(bv_ratios[:len(bv_ratios) // 2])
        late_avg  = _mean(bv_ratios[len(bv_ratios) // 2:])
        bv_trend  = late_avg - early_avg
    else:
        bv_trend = 0
    tx_total      = [float(c.get("tx",  0) or 0) for c in recent]
    btx_count     = [float(c.get("btx", 0) or 0) for c in recent]
    avg_btx_ratio = _mean([b / t for b, t in zip(btx_count, tx_total) if t > 0])
    score, signals = 0, []
    if avg_bv_ratio >= cfg["bv_ratio_strong"]:
        score += 25
        signals.append(f"STRONG_BUY bv/v={avg_bv_ratio:.1%}")
    elif avg_bv_ratio >= cfg["bv_ratio_moderate"]:
        score += 15
        signals.append(f"NET_BUYING bv/v={avg_bv_ratio:.1%}")
    if bv_trend > 0.05:
        score += 8
        signals.append(f"BUYING_ACCEL Δ={bv_trend:.2%}")
    elif bv_trend > 0.02:
        score += 4
        signals.append(f"BUYING_RISING Δ={bv_trend:.2%}")
    if avg_btx_ratio >= 0.60:
        score += 5
        signals.append(f"BUY_TX_DOM btx_ratio={avg_btx_ratio:.1%}")
    return min(score, cfg["buy_vol_ratio_weight"]), {
        "avg_bv_ratio":  round(avg_bv_ratio, 4),
        "last_bv_ratio": round(last_bv_ratio, 4),
        "bv_trend":      round(bv_trend, 4),
        "avg_btx_ratio": round(avg_btx_ratio, 4),
        "signals":       signals,
    }


def score_funding_trend(clz: ClzData, current_funding: float) -> Tuple[int, dict]:
    cfg    = CONFIG
    score, signals = 0, []
    if current_funding < -0.0010:
        score += cfg["funding_snapshot_weight"]
        signals.append(f"EXTREME_FUNDING={current_funding*100:.4f}%")
    elif current_funding < -0.0005:
        score += int(cfg["funding_snapshot_weight"] * 0.7)
        signals.append(f"STRONG_NEG_FUNDING={current_funding*100:.4f}%")
    elif current_funding < -0.0002:
        score += int(cfg["funding_snapshot_weight"] * 0.4)
        signals.append(f"NEG_FUNDING={current_funding*100:.4f}%")
    if not clz.has_funding_hist:
        return min(score, cfg["funding_trend_weight"] + cfg["funding_snapshot_weight"]), {
            "signals": signals, "trend": "no_history",
        }
    hist  = clz.funding_hist
    rates = [float(c.get("c", 0) or 0) for c in hist if c.get("c") is not None]
    if len(rates) < 3:
        return score, {"signals": signals}
    recent_24h    = rates[-3:]
    prev_24h      = rates[-6:-3] if len(rates) >= 6 else rates[:3]
    avg_recent    = _mean(recent_24h)
    avg_prev      = _mean(prev_24h)
    funding_drift = avg_recent - avg_prev
    if funding_drift < -0.0003:
        score += cfg["funding_trend_weight"]
        signals.append(f"FUNDING_TRENDING_NEG Δ={funding_drift*100:.4f}%")
    elif funding_drift < -0.0001:
        score += int(cfg["funding_trend_weight"] * 0.6)
        signals.append(f"FUNDING_DRIFTING_NEG Δ={funding_drift*100:.4f}%")
    if avg_prev > 0.0001 and avg_recent < -0.0001:
        score += 8
        signals.append(f"FUNDING_FLIPPED_NEG (was +{avg_prev*100:.4f}%)")
    neg_count = sum(1 for r in rates[-9:] if r < -0.0001)
    if neg_count >= 7:
        score += 6
        signals.append(f"PERSISTENT_NEG {neg_count}/9 negative")
    total_weight = cfg["funding_trend_weight"] + cfg["funding_snapshot_weight"]
    return min(score, total_weight), {
        "current":      round(current_funding * 100, 5),
        "avg_24h":      round(avg_recent * 100, 5),
        "avg_prev_24h": round(avg_prev * 100, 5),
        "drift":        round(funding_drift * 100, 5),
        "signals":      signals,
    }


def score_predicted_funding(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_predicted_funding:
        return 0, {"source": "no_predicted_funding"}
    hist  = clz.predicted_funding_hist
    rates = [float(c.get("c", 0) or 0) for c in hist if c.get("c") is not None]
    if len(rates) < 3:
        return 0, {"source": "insufficient"}
    recent     = rates[-3:]
    prev       = rates[-6:-3] if len(rates) >= 6 else rates[:3]
    avg_recent = _mean(recent)
    avg_prev   = _mean(prev)
    drift      = avg_recent - avg_prev
    score, signals = 0, []
    if drift < -0.0002:
        score = CONFIG["predicted_funding_weight"]
        signals.append(f"PRED_FUNDING_BEARISH Δ={drift*100:.4f}%")
    elif drift < -0.0001:
        score = int(CONFIG["predicted_funding_weight"] * 0.6)
        signals.append(f"PRED_FUNDING_NEG Δ={drift*100:.4f}%")
    return score, {"drift": round(drift * 100, 5), "signals": signals}


def score_oi_buildup(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_oi:
        return 0, {"source": "no_oi"}
    hist = clz.oi
    if len(hist) < 6:
        return 0, {"source": "insufficient_oi"}
    oi_now  = float(hist[-2].get("c", 0) or 0)
    oi_4h   = float(hist[-5].get("c", 0) or 0)
    oi_12h  = float(hist[-13].get("c", 0) or 0) if len(hist) >= 13 else oi_4h
    if oi_4h <= 0:
        return 0, {"source": "oi_zero"}
    oi_chg_4h  = (oi_now - oi_4h)  / oi_4h  * 100
    oi_chg_12h = (oi_now - oi_12h) / oi_12h * 100 if oi_12h > 0 else 0
    oi_vals        = [float(c.get("c", 0) or 0) for c in hist[-12:-1]]
    oi_trend_up    = sum(1 for i in range(1, len(oi_vals)) if oi_vals[i] > oi_vals[i - 1])
    oi_consistency = oi_trend_up / max(len(oi_vals) - 1, 1)
    score, signals = 0, []
    w = CONFIG["oi_buildup_weight"]
    # v14.8 FIX — display string OI: gunakan format +/- eksplisit agar tidak ambigu
    # Bug sebelumnya: f"OI +{oi_chg_4h:.1f}%" selalu cetak "+" meski nilai negatif.
    if oi_chg_4h > 5.0 and oi_consistency >= 0.6:
        score += w
        signals.append(f"STRONG_OI_BUILDUP OI4h={oi_chg_4h:+.1f}%")
    elif oi_chg_4h > 2.5:
        score += int(w * 0.6)
        signals.append(f"OI_BUILDUP OI4h={oi_chg_4h:+.1f}%")
    elif oi_chg_4h > 1.0:
        score += int(w * 0.3)
        signals.append(f"OI_RISING OI4h={oi_chg_4h:+.1f}%")
    elif oi_chg_4h < -10.0:
        # v14.8: OI negatif besar = peringatan — posisi ditutup massal
        signals.append(f"⚠️ OI_DUMP OI4h={oi_chg_4h:+.1f}% (posisi ditutup)")
    if oi_chg_12h > 8.0:
        score += 5
        signals.append(f"OI_BUILDUP_12H OI12h={oi_chg_12h:+.1f}%")
    return min(score, w + 5), {
        "oi_chg_4h_pct":  round(oi_chg_4h, 2),
        "oi_chg_12h_pct": round(oi_chg_12h, 2),
        "oi_consistency": round(oi_consistency, 2),
        "signals":        signals,
    }


def score_liquidations(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_liq:
        return 0, {"source": "no_liq"}
    hist = clz.liq
    if len(hist) < 6:
        return 0, {"source": "insufficient_liq"}
    baseline_short   = [float(c.get("s", 0) or 0) for c in hist[-24:-3]]
    baseline_long    = [float(c.get("l", 0) or 0) for c in hist[-24:-3]]
    current_short    = float(hist[-2].get("s", 0) or 0)
    current_long     = float(hist[-2].get("l", 0) or 0)
    if not baseline_short:
        return 0, {"source": "no_baseline"}
    baseline_avg_short = _mean(baseline_short) + 1
    baseline_avg_long  = _mean(baseline_long) + 1
    short_liq_z        = robust_zscore(current_short, baseline_short)
    short_ratio        = current_short / baseline_avg_short
    recent_4h_short    = [float(c.get("s", 0) or 0) for c in hist[-5:-1]]
    cascade_score      = 0
    if len(recent_4h_short) >= 3:
        increases = sum(
            1 for i in range(1, len(recent_4h_short))
            if recent_4h_short[i] > recent_4h_short[i - 1] * 1.1
        )
        cascade_score = increases
    score, signals = 0, []
    w = CONFIG["short_liq_weight"]
    if short_liq_z >= 2.5:
        score += w
        signals.append(f"SHORT_LIQ_SPIKE z={short_liq_z:.1f}")
    elif short_liq_z >= 1.5:
        score += int(w * 0.6)
        signals.append(f"SHORT_LIQ_ELEVATED z={short_liq_z:.1f}")
    if cascade_score >= 2:
        score += CONFIG["liq_cascade_weight"]
        signals.append(f"SHORT_LIQ_CASCADE {cascade_score}/3")
    elif cascade_score == 1:
        score += int(CONFIG["liq_cascade_weight"] * 0.5)
    if current_long > current_short * 2 and current_long > baseline_avg_long * 2:
        score = max(0, score - 10)
        signals.append("⚠️ LONG_LIQ_DOM")
    return min(score, w + CONFIG["liq_cascade_weight"]), {
        "short_liq_z":   round(short_liq_z, 2),
        "short_ratio":   round(short_ratio, 2),
        "cascade_score": cascade_score,
        "short_usd":     round(current_short),
        "long_usd":      round(current_long),
        "signals":       signals,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  TIER 3 SIGNALS — BITGET CANDLES
# ══════════════════════════════════════════════════════════════════════════════
def detect_bbw_squeeze(candles: List[dict]) -> Tuple[int, dict]:
    """
    v14.7+ DIINVERT: BBW LEBAR = pump-ready, bukan sempit.
    Pre-pump BBW = 0.207 vs ranging BBW = 0.039.
    """
    if len(candles) < 22:
        return 0, {}
    closes = [c["close"] for c in candles[-20:]]
    sma    = _mean(closes)
    if sma <= 0:
        return 0, {}
    var   = sum((x - sma) ** 2 for x in closes) / 20
    std   = var ** 0.5
    bb_w  = (sma + 2 * std - (sma - 2 * std)) / sma
    getting_wider = False
    if len(candles) >= 44:
        prev_closes = [c["close"] for c in candles[-44:-24]]
        if prev_closes:
            p_sma = _mean(prev_closes)
            p_var = sum((x - p_sma) ** 2 for x in prev_closes) / len(prev_closes)
            p_std = p_var ** 0.5
            p_bbw = (p_sma + 2 * p_std - (p_sma - 2 * p_std)) / p_sma if p_sma > 0 else 0
            getting_wider = bb_w > p_bbw
    w = CONFIG["bbw_squeeze_weight"]
    if bb_w > 0.15:
        score, pat = w, "WIDE_EXPANSION"
    elif bb_w > 0.10:
        score, pat = int(w * 0.8), "EXPANDING"
    elif bb_w > 0.06:
        score, pat = int(w * 0.4), "MODERATE"
    else:
        score, pat = 0, "TIGHT_SQUEEZE"
    if getting_wider and score > 0:
        score = min(score + 3, w + 3)
        pat  += "+WIDENING"
    return score, {"bb_w": round(bb_w, 4), "pattern": pat, "wider": getting_wider}


def detect_price_stability(candles: List[dict]) -> Tuple[int, dict]:
    """
    v14.9 DIINVERT: range_pct LEBAR = bullish, bukan sempit.
    Audit raw data v2: pump_med range_pct=3.45%, ranging_med=0.91%.
    Pump terjadi saat range LEBAR — reward range lebar, penalti range sempit.
    (v14.8 reward range sempit/COILING = kontradiksi langsung dengan data.)
    """
    if len(candles) < 10:
        return 0, {}
    recent    = candles[-9:-1]
    closes    = [c["close"] for c in recent]
    lo, hi    = min(closes), max(closes)
    ref       = (lo + hi) / 2
    if ref <= 0:
        return 0, {}
    range_pct = (hi - lo) / ref * 100
    w = CONFIG["price_stability_weight"]
    if range_pct >= 4.0:
        return w, {"range_pct": round(range_pct, 2), "pattern": "WIDE_EXPANSION"}
    elif range_pct >= 2.5:
        return int(w * 0.67), {"range_pct": round(range_pct, 2), "pattern": "RANGING"}
    elif range_pct >= 1.5:
        return int(w * 0.33), {"range_pct": round(range_pct, 2), "pattern": "CONSOLIDATING"}
    return 0, {"range_pct": round(range_pct, 2), "pattern": "COILING"}


def detect_volume_dryup(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 26:
        return 0, {}
    cur_vol   = candles[-2].get("volume_usd", 0)
    avg_vol   = _mean([c.get("volume_usd", 0) for c in candles[-26:-2]])
    if avg_vol <= 0:
        return 0, {}
    tod       = volume_tod_mult(get_hour_utc())
    adj_ratio = (cur_vol * tod) / avg_vol
    w = CONFIG["volume_dryup_weight"]
    if adj_ratio < 0.35:
        return w, {"ratio": round(adj_ratio, 2), "pattern": "EXTREME_DRY"}
    elif adj_ratio < 0.50:
        return int(w * 0.7), {"ratio": round(adj_ratio, 2), "pattern": "VERY_DRY"}
    elif adj_ratio < 0.65:
        return int(w * 0.4), {"ratio": round(adj_ratio, 2), "pattern": "DRY"}
    return 0, {"ratio": round(adj_ratio, 2), "pattern": "NORMAL"}


def detect_accumulation(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 26:
        return 0, {}
    cur_vol  = _mean([c.get("volume_usd", 0) for c in candles[-7:-1]])
    base_vol = _mean([c.get("volume_usd", 0) for c in candles[-25:-7]])
    if base_vol <= 0:
        return 0, {}
    ratio = cur_vol / base_vol
    if candles[-7]["close"] <= 0:
        return 0, {}
    p_chg = (candles[-2]["close"] - candles[-7]["close"]) / candles[-7]["close"] * 100
    w     = CONFIG["accumulation_weight"]
    if ratio >= 3.0 and -2 < p_chg < 4:
        return w, {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "STRONG_ACCUM"}
    elif ratio >= 2.5 and -2 < p_chg < 5:
        return int(w * 0.75), {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "ACCUM"}
    elif ratio >= 2.0 and -1 < p_chg < 4:
        return int(w * 0.5), {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "LIGHT_ACCUM"}
    return 0, {"vol_ratio": round(ratio, 2), "pattern": "NO_ACCUM"}


def detect_volatility_return(candles: List[dict]) -> Tuple[int, dict]:
    """
    v14.7+: ATR absolut tinggi = fitur #1 (disc=117, lift=3.2x).
    Pre-pump ATR = 4.99% vs ranging ATR = 1.00%.
    Komponen ganda: ATR absolut + volatility return ratio.
    """
    if len(candles) < 50:
        return 0, {}
    atr_now  = calc_atr(candles[-22:], 14)
    atr_hist = calc_atr(candles[-72:-24], 14) if len(candles) >= 74 else calc_atr(candles[:-24], 14)
    if atr_hist <= 0:
        return 0, {}
    ratio       = atr_now / atr_hist
    atr_now_pct = atr_now * 100
    w = CONFIG["volatility_return_weight"]

    # Komponen 1: ATR absolut tinggi
    if atr_now_pct >= 5.0:
        abs_score, abs_pat = w, "HIGH_ABSOLUTE_ATR"
    elif atr_now_pct >= 3.5:
        abs_score, abs_pat = int(w * 0.8), "ELEVATED_ATR"
    elif atr_now_pct >= 2.5:
        abs_score, abs_pat = int(w * 0.5), "MODERATE_ATR"
    else:
        abs_score, abs_pat = 0, ""

    # Komponen 2: Volatility return dari kondisi quiet
    if ratio < 0.40:
        ratio_score, ratio_pat = int(w * 0.5), "EXTREME_QUIET_RETURN"
    elif ratio < 0.60:
        ratio_score, ratio_pat = int(w * 0.35), "VERY_QUIET_RETURN"
    elif ratio < 0.75:
        ratio_score, ratio_pat = int(w * 0.2), "QUIET_RETURN"
    else:
        ratio_score, ratio_pat = 0, "NORMAL_VOL"

    if abs_score > 0 and ratio_score > 0:
        score   = min(abs_score + ratio_score // 2, w)
        pattern = f"{abs_pat}+{ratio_pat}"
    else:
        score   = max(abs_score, ratio_score)
        pattern = abs_pat or ratio_pat or "NORMAL_VOL"

    return score, {
        "atr_ratio":   round(ratio, 3),
        "atr_now_pct": round(atr_now_pct, 2),
        "pattern":     pattern,
    }


def detect_rs_btc(coin_chg_1h: float, btc_chg_1h: float) -> Tuple[int, dict]:
    """
    v14.7+ DIINVERT: RS 1h negatif saat BTC naik = catchup pending = bullish.
    Pre-pump rs_1h = -0.287. Bobot turun 16 → 8.
    """
    if btc_chg_1h == 0:
        return 0, {"rs": 0}
    rs = coin_chg_1h - btc_chg_1h
    w  = CONFIG["rs_btc_weight"]
    if rs < -0.2 and btc_chg_1h > 0.3:
        return w, {"rs": round(rs, 2), "pattern": "BTC_LEADING_CATCHUP_PENDING"}
    elif rs < -0.1 and btc_chg_1h > 0:
        return int(w * 0.6), {"rs": round(rs, 2), "pattern": "SLIGHT_LAG_VS_BTC"}
    elif rs > 3.0 and btc_chg_1h <= 0.5:
        return int(w * 0.5), {"rs": round(rs, 2), "pattern": "STRONG_DECOUPLE"}
    elif rs > 2.0:
        return int(w * 0.3), {"rs": round(rs, 2), "pattern": "OUTPERFORMING"}
    return 0, {"rs": round(rs, 2), "pattern": "INLINE"}


def detect_lower_wick(candles: List[dict]) -> Tuple[int, dict]:
    """
    v14.9 UPDATE: Tambah perbandingan last_wick_dn vs last_wick_up (net buying pressure).
    Audit raw data v2: last_wick_up pump/rang ratio = 4.6x (STRONG rank 5), tapi
    DUMP/rang ratio = 7.2x — upper wick tinggi ambiguous. Implementasi yang tepat:
    reward jika lower_wick > upper_wick (net buying pressure dominan),
    penalti jika upper_wick >> lower_wick (net selling pressure).
    last_wick_dn tetap sinyal utama (disc=90.75, lift=2.56x, ratio 3.37x).
    """
    if len(candles) < 5:
        return 0, {}
    wick_pcts       = []
    upper_wick_pcts = []
    for c in candles[-4:-1]:
        lo       = c["low"]
        hi       = c["high"]
        op       = c.get("open", 0)
        cl       = c["close"]
        body_low  = min(op, cl) if op > 0 else cl
        body_high = max(op, cl) if op > 0 else cl
        if body_low > 0:
            lower_w = (body_low - lo) / body_low * 100
            wick_pcts.append(max(0.0, lower_w))
        if body_high > 0:
            upper_w = (hi - body_high) / body_high * 100
            upper_wick_pcts.append(max(0.0, upper_w))
    if not wick_pcts:
        return 0, {}
    avg_wick       = _mean(wick_pcts)
    max_wick       = max(wick_pcts)
    avg_upper_wick = _mean(upper_wick_pcts) if upper_wick_pcts else 0.0
    w = CONFIG["lower_wick_weight"]
    if avg_wick >= 1.0:
        score, pat = w, "STRONG_REJECTION_WICK"
    elif avg_wick >= 0.65:
        score, pat = int(w * 0.75), "REJECTION_WICK"
    elif avg_wick >= 0.40:
        score, pat = int(w * 0.45), "LIGHT_WICK"
    else:
        score, pat = 0, "NO_WICK"
    if max_wick >= 1.5 and score > 0:
        score = min(score + 3, w + 3)
        pat  += "+MAX_WICK"
    # Net buying pressure: reward jika lower > upper (buying dominan)
    if avg_wick > avg_upper_wick and score > 0:
        score = min(score + 2, w + 3)
        pat  += "+NET_BUY_PRESSURE"
    # Net selling pressure: penalti jika upper >> lower (selling dominan)
    elif avg_upper_wick > avg_wick * 1.5 and avg_upper_wick > 0.5:
        score = max(0, score - 3)
        pat  += "+NET_SELL_PRESSURE"
    return score, {
        "avg_wick_pct":       round(avg_wick, 3),
        "max_wick_pct":       round(max_wick, 3),
        "avg_upper_wick_pct": round(avg_upper_wick, 3),
        "pattern":            pat,
    }


def detect_momentum_decel(candles: List[dict]) -> Tuple[int, dict]:
    """
    v14.7+: Momentum melambat 1-3 jam sebelum pump.
    disc=46.85. Pre-pump accel = -0.364 (vs ranging +0.014).
    Pola klasik: coin berhenti sesaat → lalu meledak.
    """
    if len(candles) < 8:
        return 0, {}
    chgs = []
    for i in range(-5, -1):
        c  = candles[i]
        pc = candles[i - 1]
        if pc["close"] > 0:
            chg = (c["close"] - pc["close"]) / pc["close"] * 100
            chgs.append(chg)
    if len(chgs) < 4:
        return 0, {}
    recent_mom  = _mean(chgs[-2:])
    earlier_mom = _mean(chgs[:2])
    accel = recent_mom - earlier_mom
    w = CONFIG["momentum_decel_weight"]
    if accel <= -0.30:
        score, pat = w, "STRONG_DECEL"
    elif accel <= -0.15:
        score, pat = int(w * 0.70), "DECEL"
    elif accel <= -0.05:
        score, pat = int(w * 0.35), "SLIGHT_DECEL"
    else:
        score, pat = 0, "NO_DECEL"
    if accel >= 0.50:
        score = 0
        pat   = "DUMP_ACCEL"
    return score, {
        "recent_mom":  round(recent_mom, 3),
        "earlier_mom": round(earlier_mom, 3),
        "accel":       round(accel, 3),
        "pattern":     pat,
    }


def detect_dist_to_support(candles: List[dict], price: float) -> Tuple[int, dict]:
    """
    v14.8 FIX: Window diperluas 48 → 96 candle, toleransi cluster 1.5% → 2.0%.
    Bug: fungsi tidak pernah aktif karena 48 candle (2 hari) tidak cukup
    membentuk cluster yang valid. Sekarang menggunakan 96 candle (4 hari).

    Logika: pump median terjadi 1% dari support (bounce 2–5x, bukan 6+).
    """
    cfg = CONFIG
    window  = cfg["support_candle_window"]   # 96 candle
    tol     = cfg["support_cluster_tol"]     # 2.0%
    b_min   = cfg["support_bounce_min"]      # 2
    b_max   = cfg["support_bounce_max"]      # 5

    if len(candles) < 10 or price <= 0:
        return 0, {}

    # Ambil lows dari window yang tersedia (max 96 candle)
    window_candles = candles[-window:] if len(candles) >= window else candles
    lows = [c["low"] for c in window_candles if c["low"] > 0]
    if len(lows) < 4:
        return 0, {}

    # Clustering lows dengan toleransi 2%
    clusters: Dict[float, int] = {}
    for low in lows:
        matched = False
        for cp in list(clusters.keys()):
            if abs(low - cp) / cp < tol:
                clusters[cp] += 1
                matched = True
                break
        if not matched:
            clusters[low] = 1

    if not clusters:
        return 0, {}

    # Support valid: bounce b_min–b_max kali, di bawah harga saat ini
    valid_supports = [
        (lvl, cnt) for lvl, cnt in clusters.items()
        if b_min <= cnt <= b_max and lvl < price
    ]
    if not valid_supports:
        # Fallback: coba support dengan bounce >= 2 tanpa batas atas
        fallback = [(lvl, cnt) for lvl, cnt in clusters.items()
                    if cnt >= b_min and lvl < price]
        if not fallback:
            return 0, {}
        valid_supports = fallback

    # Ambil support terkuat (bounce terbanyak) yang terdekat dari harga
    support_level, bounce_count = max(valid_supports, key=lambda x: x[1])
    dist_pct = (price - support_level) / support_level * 100

    w = cfg["dist_to_support_weight"]
    if 0.3 <= dist_pct <= 1.5:
        score, pat = w, "JUST_BOUNCED"         # sweet spot pre-pump
    elif 1.5 < dist_pct <= 3.0:
        score, pat = int(w * 0.6), "NEAR_SUPPORT"
    elif dist_pct < 0.3:
        score, pat = int(w * 0.4), "AT_SUPPORT"
    elif 3.0 < dist_pct <= 5.0:
        score, pat = int(w * 0.2), "EXTENDED_FROM_SUPPORT"
    else:
        score, pat = 0, "FAR_FROM_SUPPORT"

    return score, {
        "support_level": round(support_level, 8),
        "dist_pct":      round(dist_pct, 2),
        "bounce_count":  bounce_count,
        "candles_used":  len(window_candles),
        "pattern":       pat,
    }


def detect_rs_24h(candles: List[dict], btc_chg_24h: float) -> Tuple[int, dict]:
    """
    v14.9 FIX: Threshold OUTPERFORM diturunkan 0.5 → 0.3.
    Audit raw data v2: pump_median rs_24h = 0.366 (bukan 0.728 di JSON — delta 0.362).
    Threshold lama >= 0.5 hanya mencakup 37% pump events.
    Threshold baru >= 0.3 mencakup ~50% pump events sesuai median aktual.
    """
    coin_chg_24h = get_chg_from_candles(candles, 24) if len(candles) >= 26 else 0.0
    if btc_chg_24h == 0:
        return 0, {"rs_24h": 0}
    rs_24h = coin_chg_24h - btc_chg_24h
    w = CONFIG["rs_24h_weight"]
    if rs_24h >= 0.3:
        score, pat = w, "OUTPERFORM_BTC_24H"
    elif rs_24h >= 0.1:
        score, pat = int(w * 0.6), "SLIGHT_OUTPERFORM_24H"
    elif rs_24h >= -0.1:
        score, pat = int(w * 0.2), "INLINE_BTC_24H"
    else:
        score, pat = 0, "UNDERPERFORM_BTC_24H"
    return score, {
        "rs_24h":       round(rs_24h, 2),
        "coin_chg_24h": round(coin_chg_24h, 2),
        "pattern":      pat,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🔄  CONTINUATION & REVERSAL
# ══════════════════════════════════════════════════════════════════════════════
def check_multiwave_history(symbol: str) -> Tuple[int, dict]:
    history = get_pump_history(symbol, CONFIG["multiwave_lookback_days"])
    pumps   = [e for e in history if e.type == "PUMP" and e.magnitude_pct >= CONFIG["pump_threshold_pct"]]
    if len(pumps) < 2:
        return 0, {}
    gaps        = [(pumps[i].timestamp - pumps[i + 1].timestamp).total_seconds() / 3600
                   for i in range(len(pumps) - 1)]
    avg_gap     = _mean(gaps)
    hours_since = (datetime.now(timezone.utc) - pumps[0].timestamp).total_seconds() / 3600
    in_window   = False
    if avg_gap > 0:
        if avg_gap * 0.5 <= hours_since <= avg_gap * 1.5:
            score, pattern, in_window = 30, "IN_WINDOW", True
        elif hours_since < avg_gap * 0.5:
            score, pattern = 10, "TOO_EARLY"
        elif hours_since <= avg_gap * 2:
            score, pattern = 15, "NEAR_WINDOW"
        else:
            score, pattern = 0, "TOO_LATE"
    else:
        score, pattern = 0, "NO_GAP_DATA"
    return score, {
        "num_pumps":   len(pumps),
        "avg_gap_h":   round(avg_gap, 1),
        "hours_since": round(hours_since, 1),
        "pattern":     pattern,
        "in_window":   in_window,
    }


def check_reversal_pattern(data: CoinData) -> Tuple[int, dict]:
    candles = data.candles
    score, signals = 0, []
    distance = None
    if len(candles) >= 48:
        lows     = [c["low"] for c in candles[-48:]]
        clusters: Dict[float, int] = {}
        for low in lows:
            matched = False
            for cp in list(clusters.keys()):
                if abs(low - cp) / cp < 0.015:
                    clusters[cp] += 1
                    matched = True
                    break
            if not matched:
                clusters[low] = 1
        if clusters:
            support = max(clusters, key=clusters.get)
            if clusters[support] >= 3:
                distance = abs(data.price - support) / support
                if distance < 0.015:
                    score += 22
                    signals.append("AT_SUPPORT")
                elif distance < 0.04:
                    score += 12
                    signals.append("NEAR_SUPPORT")
    if len(candles) >= 24:
        cv = candles[-2].get("volume_usd", 0)
        av = _mean([c.get("volume_usd", 0) for c in candles[-24:-2]])
        if av > 0 and cv > av * 3:
            score += 15
            signals.append("CAPITULATION_VOL")
    last = candles[-2]
    cr   = last["high"] - last["low"]
    if cr > 0 and (last["close"] - last["low"]) / cr > 0.55:
        score += 10
        signals.append("REJECTION_WICK")
    if data.funding < -0.0005:
        score += 12
        signals.append("FUNDING_EXTREME")
    return score, {"signals": signals, "distance_pct": round(distance * 100, 2) if distance else None}


# ══════════════════════════════════════════════════════════════════════════════
#  🚪  VELOCITY GATES
# ══════════════════════════════════════════════════════════════════════════════
def check_velocity_gates(chg_24h: float, chg_1h: float, chg_4h: float,
                          is_continuation: bool = False) -> Tuple[bool, str]:
    cfg = CONFIG["velocity_gates"]
    if chg_24h < cfg["chg_24h_min"]:
        return True, f"⛔ DUMP Δ24h={chg_24h:+.1f}%"
    max_24h = cfg["chg_24h_max_continuation"] if is_continuation else cfg["chg_24h_max_early"]
    if chg_24h > max_24h:
        return True, f"⛔ LATE Δ24h={chg_24h:+.1f}%"
    if chg_1h > cfg["chg_1h_max"]:
        return True, f"⛔ PUMPING Δ1h={chg_1h:+.1f}%"
    if chg_4h > cfg["chg_4h_max"]:
        return True, f"⛔ LATE 4h Δ4h={chg_4h:+.1f}%"
    return False, ""


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  MASTER SCORING v14.8
# ══════════════════════════════════════════════════════════════════════════════
def score_coin_v14(data: CoinData) -> Optional[ScoreResult]:
    if data.vol_24h < CONFIG["pre_filter_vol_min"] or data.price <= 0:
        return None

    phase   = classify_phase(data.chg_24h)
    is_cont = phase.phase == "CONTINUATION"
    if phase.phase not in ["DOWNTREND", "WEAK"]:
        blocked, reason = check_velocity_gates(data.chg_24h, data.chg_1h, data.chg_4h, is_cont)
        if blocked:
            return None

    clz           = data.clz
    phase_score   = phase.base_score
    catalysts:     List[str] = []
    risk_warnings: List[str] = []
    pump_types:    List[PumpType] = []

    # ── Tier 1: Coinalyze derivatif ───────────────────────────────────────────
    ls_sc,   ls_d   = score_long_short_ratio(clz)
    bv_sc,   bv_d   = score_buy_volume_ratio(clz)
    fund_sc, fund_d = score_funding_trend(clz, data.funding)
    pred_sc, pred_d = score_predicted_funding(clz)
    tier1_score     = ls_sc + bv_sc + fund_sc + pred_sc

    if ls_sc > 0:
        catalysts.append(f"📊 L/S={ls_d.get('long_ratio',0):.1%}longs {' | '.join(ls_d.get('signals',[])[:2])}")
    if bv_sc > 0:
        catalysts.append(f"💚 BuyVol={bv_d.get('avg_bv_ratio',0):.1%} {' | '.join(bv_d.get('signals',[])[:2])}")
    if fund_sc > 0:
        catalysts.append(f"💰 Fund={fund_d.get('current',0):.4f}% {' | '.join(fund_d.get('signals',[])[:2])}")
    if pred_sc > 0:
        catalysts.append(f"🔮 PredFund Δ={pred_d.get('drift',0):.4f}% {' | '.join(pred_d.get('signals',[]))}")

    # ── Tier 2: Coinalyze OI & Liquidations ───────────────────────────────────
    oi_sc,  oi_d  = score_oi_buildup(clz)
    liq_sc, liq_d = score_liquidations(clz)
    tier2_score   = oi_sc + liq_sc

    if oi_sc > 0:
        # v14.8 FIX: format OI dengan tanda +/- eksplisit dari dalam oi_d signals
        oi_signals_str = " | ".join(oi_d.get("signals", [])[:1])
        catalysts.append(f"📈 {oi_signals_str}")
    # Tambah warning jika OI sangat negatif
    oi_warn_signals = [s for s in oi_d.get("signals", []) if "DUMP" in s]
    if oi_warn_signals:
        risk_warnings.append(f"⚠️ {oi_warn_signals[0]}")
    if liq_sc > 0:
        catalysts.append(f"💥 ShortLiq z={liq_d.get('short_liq_z',0):.1f} {' | '.join(liq_d.get('signals',[])[:2])}")

    # ── Tier 3: Bitget candles ─────────────────────────────────────────────────
    bbw_sc,   bbw_d   = detect_bbw_squeeze(data.candles)
    stab_sc,  stab_d  = detect_price_stability(data.candles)
    dry_sc,   dry_d   = detect_volume_dryup(data.candles)
    accum_sc, accum_d = detect_accumulation(data.candles)
    vret_sc,  vret_d  = detect_volatility_return(data.candles)
    rs_sc,    rs_d    = detect_rs_btc(data.chg_1h, data.btc_chg_1h)
    wick_sc,  wick_d  = detect_lower_wick(data.candles)
    decel_sc, decel_d = detect_momentum_decel(data.candles)
    supp_sc,  supp_d  = detect_dist_to_support(data.candles, data.price)
    rs24_sc,  rs24_d  = detect_rs_24h(data.candles, data.btc_chg_24h)
    tier3_score = (bbw_sc + stab_sc + dry_sc + accum_sc + vret_sc + rs_sc
                   + wick_sc + decel_sc + supp_sc + rs24_sc)

    if bbw_sc > 0:
        catalysts.append(f"📐 BBW={bbw_d.get('bb_w',0):.3f} [{bbw_d.get('pattern','')}]")
    if accum_sc > 0:
        catalysts.append(f"🐋 Accum x{accum_d.get('vol_ratio',0):.1f} Δp={accum_d.get('price_chg',0):+.1f}%")
    if vret_sc > 0:
        catalysts.append(f"⚡ {vret_d.get('pattern','')} ATR={vret_d.get('atr_now_pct',0):.2f}% ratio={vret_d.get('atr_ratio',0):.2f}")
    if rs_sc > 0:
        catalysts.append(f"📊 RS1h={rs_d.get('rs',0):+.2f}% vs BTC [{rs_d.get('pattern','')}]")
    if wick_sc > 0:
        catalysts.append(f"🕯️ LowerWick={wick_d.get('avg_wick_pct',0):.2f}% [{wick_d.get('pattern','')}]")
    if decel_sc > 0:
        catalysts.append(f"🔻 MomDecel accel={decel_d.get('accel',0):+.3f} [{decel_d.get('pattern','')}]")
    if supp_sc > 0:
        catalysts.append(f"🎯 Support dist={supp_d.get('dist_pct',0):.1f}% bounce={supp_d.get('bounce_count',0)}x [{supp_d.get('pattern','')}]")
    if rs24_sc > 0:
        catalysts.append(f"📈 RS24h={rs24_d.get('rs_24h',0):+.2f}% vs BTC [{rs24_d.get('pattern','')}]")

    # ══════════════════════════════════════════════════════════════════════════
    #  PUMP TYPE CLASSIFICATION v14.8
    # ══════════════════════════════════════════════════════════════════════════
    cfg = CONFIG

    # ── Type E: Short Squeeze ─────────────────────────────────────────────────
    squeeze_via_ls = (
        ls_sc >= cfg["short_squeeze_ls_min"] and
        (liq_sc >= cfg["short_squeeze_liq_min"] or fund_sc >= cfg["short_squeeze_fund_min"])
    )
    # v14.8 FIX: squeeze_alt diperketat — liq threshold naik 15 → 18
    squeeze_alt = (
        (fund_sc >= cfg["squeeze_alt_fund_liq_fund"] and liq_sc >= cfg["squeeze_alt_fund_liq_liq"]) or
        (fund_sc >= cfg["squeeze_alt_fund_pred_fund"] and pred_sc >= cfg["squeeze_alt_fund_pred_pred"]
         and liq_sc >= cfg["squeeze_alt_fund_pred_liq"])
    )
    if squeeze_via_ls or squeeze_alt:
        pump_types.append(PumpType(
            "E", "Short Squeeze",
            min((ls_sc + liq_sc + fund_sc + pred_sc) * 2, 100),
            ls_d.get("signals", []) + liq_d.get("signals", []) + pred_d.get("signals", []),
        ))
        log.debug(f"  {data.symbol}: Type E (ls={ls_sc} liq={liq_sc} fund={fund_sc} pred={pred_sc} via_ls={squeeze_via_ls} alt={squeeze_alt})")

    # ── Type B: Whale Accumulation ────────────────────────────────────────────
    if bv_sc >= cfg["whale_accum_bv_min"] and accum_sc >= cfg["whale_accum_accum_min"]:
        pump_types.append(PumpType(
            "B", "Whale Accumulation",
            min((bv_sc + accum_sc) * 3, 100),
            bv_d.get("signals", []) + [accum_d.get("pattern", "")],
        ))

    # ── Type D: Technical Breakout + konfirmasi Coinalyze ─────────────────────
    # v14.9 FIX: Hapus syarat stab_sc >= 6 (anti-pump).
    # Audit raw data: 63.5% pump events memiliki inside_compression=0 (sudah keluar compression).
    # stab_sc >= 6 identik dengan kondisi COILING/CONSOLIDATING = inside_compression=1,
    # yang justru anti-pump (ranging_med=1, 77.5% ranging dalam compression).
    # Type D sekarang hanya mensyaratkan BBW lebar + dry volume + konfirmasi Coinalyze.
    type_d_coinalyze_confirmed = (
        oi_sc   >= cfg["type_d_min_oi_sc"]  or
        liq_sc  >= cfg["type_d_min_liq_sc"] or
        fund_sc >= cfg["type_d_min_fund_sc"]
    )
    if bbw_sc >= 8 and dry_sc >= 5 and type_d_coinalyze_confirmed:
        pump_types.append(PumpType(
            "D", "Technical Breakout",
            min((bbw_sc + dry_sc) * 3, 100),
            [bbw_d.get("pattern", ""), dry_d.get("pattern", "")],
        ))
        log.debug(f"  {data.symbol}: Type D confirmed (bbw={bbw_sc} dry={dry_sc} oi={oi_sc} liq={liq_sc} fund={fund_sc})")
    elif bbw_sc >= 8 and dry_sc >= 5 and not type_d_coinalyze_confirmed:
        risk_warnings.append("⚠️ BBW expansion tanpa konfirmasi derivatif — Type D diabaikan")

    # ── Type F: Volatility Return / High ATR ──────────────────────────────────
    if vret_sc >= 10:
        pump_types.append(PumpType(
            "F", "Volatility Return",
            min(vret_sc * 5, 100),
            [vret_d.get("pattern", "")],
        ))

    # ── Type G: Multi-wave Continuation ──────────────────────────────────────
    mw_bonus = 0
    if phase.phase in ["EARLY", "CONTINUATION", "PARABOLIC"]:
        mw_sc, mw_d = check_multiwave_history(data.symbol)
        if mw_sc > 0:
            mw_bonus = mw_sc
            catalysts.append(f"🔄 Multi-wave {mw_d['num_pumps']}x, gap {mw_d['avg_gap_h']:.0f}h [{mw_d['pattern']}]")
            pump_types.append(PumpType("G", "Multi-wave", mw_sc, [mw_d.get("pattern", "")]))

    # ── Type R: Reversal Bounce ───────────────────────────────────────────────
    reversal_score = 0
    if phase.phase in ["DOWNTREND", "WEAK"]:
        rev_sc, rev_d = check_reversal_pattern(data)
        reversal_score = rev_sc
        if rev_sc >= 35:
            catalysts.append(f"↩️ Reversal: {', '.join(rev_d.get('signals', []))}")
            pump_types.append(PumpType("R", "Reversal Bounce", min(rev_sc, 100), rev_d.get("signals", [])))
        else:
            risk_warnings.append(f"⚠️ Reversal weak ({rev_sc}pts)")

    # ── Total score & threshold check ─────────────────────────────────────────
    total = phase_score + tier1_score + tier2_score + tier3_score + mw_bonus + reversal_score

    has_any_clz = clz.has_ohlcv or clz.has_oi or clz.has_liq or clz.has_ls or clz.has_funding_hist
    if not has_any_clz:
        risk_warnings.append("⚠️ No Coinalyze data — score based on Bitget candles only")

    if phase.phase == "EARLY":
        threshold = cfg["alert_threshold_early"]          # 95 (v14.8: naik dari 90)
    elif phase.phase == "CONTINUATION":
        threshold = cfg["alert_threshold_continuation"]   # 100
    elif phase.phase in ["DOWNTREND", "WEAK"]:
        threshold = cfg["alert_threshold_reversal"]       # 80
    else:
        threshold = 110

    if total < threshold:
        return None
    if not pump_types:
        return None

    entry_data = calc_entry_targets(data)
    if entry_data is None:
        return None

    position = calc_position_size(entry_data["entry"], entry_data["sl"], entry_data["atr_decimal"])

    type_labels = {
        "E": "💰 SHORT SQUEEZE",
        "B": "🐋 WHALE ACCUM",
        "D": "📐 BREAKOUT",
        "F": "⚡ VOL RETURN",
        "G": "🔄 CONTINUATION",
        "R": "↩️ REVERSAL",
    }
    top       = pump_types[0]
    all_types = "/".join([pt.type_code for pt in pump_types])
    urg       = f"{type_labels.get(top.type_code, '🎯')} [{all_types}]"
    confidence= "very_strong" if total >= 130 else "strong" if total >= 95 else "watch"

    data_sources = []
    if clz.has_ls:                data_sources.append("L/S✅")
    if clz.has_ohlcv:             data_sources.append("BV✅")
    if clz.has_funding_hist:      data_sources.append("Fund✅")
    if clz.has_predicted_funding: data_sources.append("Pred✅")
    if clz.has_oi:                data_sources.append("OI✅")
    if clz.has_liq:               data_sources.append("Liq✅")

    return ScoreResult(
        symbol=data.symbol, score=min(total, 250), phase=phase.phase,
        pump_types=pump_types, confidence=confidence,
        components={
            "phase":           phase_score,
            "tier1_clz":       tier1_score,
            "tier2_clz":       tier2_score,
            "tier3_technical": tier3_score,
            "multiwave":       mw_bonus,
            "reversal":        reversal_score,
            "detail": {
                "ls":    ls_sc,   "bv":    bv_sc,   "fund": fund_sc, "pred": pred_sc,
                "oi":    oi_sc,   "liq":   liq_sc,
                "bbw":   bbw_sc,  "stab":  stab_sc, "dry":  dry_sc,
                "accum": accum_sc,"vret":  vret_sc, "rs":   rs_sc,
                "wick":  wick_sc, "decel": decel_sc,"supp": supp_sc, "rs24": rs24_sc,
                # Diagnostik L/S untuk monitoring (v14.8)
                "ls_long_actual": round(ls_d.get("long_ratio", 0), 4),
                "ls_trend_actual": round(ls_d.get("long_trend_4h", 0), 4),
            },
            "data_sources": " ".join(data_sources) if data_sources else "Bitget-only",
        },
        catalysts=catalysts, entry=entry_data, price=data.price,
        vol_24h=data.vol_24h, chg_24h=data.chg_24h, chg_1h=data.chg_1h,
        funding=data.funding, urgency=urg,
        risk_warnings=risk_warnings, position=position,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  📤  ALERT FORMATTER v14.8
# ══════════════════════════════════════════════════════════════════════════════
def build_alert_v14(r: ScoreResult, rank: int) -> str:
    vol    = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    em     = {"very_strong": "🟢", "strong": "🟡", "watch": "⚪"}.get(r.confidence, "⚪")
    bar_len= min(20, r.score * 20 // 200)
    bar    = "█" * bar_len + "░" * (20 - bar_len)
    comp   = r.components
    d      = comp.get("detail", {})
    lines  = [
        f"{'─'*58}",
        f"#{rank}  {r.symbol}  {em}  Score: {r.score}  [{r.phase}]",
        f"   {bar}",
        f"   {r.urgency}",
        f"   Data: {comp.get('data_sources', 'N/A')}",
        f"",
    ]
    if r.catalysts:
        lines.append("   📊 Signals:")
        for c in r.catalysts[:8]:
            lines.append(f"      {c}")
        lines.append("")
    if r.risk_warnings:
        lines.append("   ⚠️ Risks:")
        for w in r.risk_warnings[:3]:
            lines.append(f"      {w}")
        lines.append("")
    lines.append(f"   Vol: {vol} | Δ1h: {r.chg_1h:+.1f}% | Δ24h: {r.chg_24h:+.1f}% | F: {r.funding*100:.4f}%")
    lines.append(f"   Phase:{comp['phase']} T1:{comp['tier1_clz']} T2:{comp['tier2_clz']} T3:{comp['tier3_technical']}")
    lines.append(f"   L/S:{d.get('ls',0)} BV:{d.get('bv',0)} Fund:{d.get('fund',0)} Pred:{d.get('pred',0)} OI:{d.get('oi',0)} Liq:{d.get('liq',0)}")
    lines.append(f"   BBW:{d.get('bbw',0)} Accum:{d.get('accum',0)} VRet:{d.get('vret',0)} RS1h:{d.get('rs',0)}")
    lines.append(f"   Wick:{d.get('wick',0)} Decel:{d.get('decel',0)} Supp:{d.get('supp',0)} RS24h:{d.get('rs24',0)}")
    # v14.8: tampilkan nilai L/S aktual untuk monitoring
    ls_actual = d.get("ls_long_actual", 0)
    ls_trend  = d.get("ls_trend_actual", 0)
    if ls_actual > 0:
        lines.append(f"   [diag] L/S actual: long={ls_actual:.3f} trend4h={ls_trend:+.3f}")
    if r.entry:
        e = r.entry
        lines += [
            f"",
            f"   💰 ENTRY ZONE:",
            f"      Low:  ${e['entry_zone_low']:.8f}  ← pullback entry",
            f"      Mid:  ${e['entry']:.8f}  ← ideal",
            f"      High: ${e['entry_zone_high']:.8f}  ← breakout confirm",
            f"      SL:   ${e['sl']:.8f}  (-{e['sl_pct']:.1f}%)  [ATR×{e['sl_mult']:.1f}]",
            f"      TP1:  ${e['tp1']:.8f}  (+{e['tp1_pct']:.0f}%)  R/R {e['rr1']:.1f}x",
            f"      TP2:  ${e['tp2']:.8f}  (+{e['tp2_pct']:.0f}%)  R/R {e['rr2']:.1f}x",
            f"      TP3:  ${e['tp3']:.8f}  (+{e['tp3_pct']:.0f}%)  [trailing]",
        ]
    if r.position:
        p = r.position
        lines.append(f"      Size: {p['position_size']:.4f} | Lev: {p['leverage']:.1f}x | Risk: ${p['risk_usd']:.0f}")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  API CLIENTS
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE      = "https://api.bitget.com"
    _cache:    Dict = {}
    _cache_ts: Dict = {}
    CACHE_TTL = 55 * 60

    @staticmethod
    def _get(url: str, params: dict = None, timeout: int = 12) -> Optional[dict]:
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=timeout)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    time.sleep(10)
                    continue
                break
            except Exception:
                if attempt < 2:
                    time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers",
                        params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 100) -> List[dict]:
        key = f"{symbol}:{limit}"
        if key in cls._cache and time.time() - cls._cache_ts.get(key, 0) < cls.CACHE_TTL:
            return cls._cache[key]
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/candles",
            params={"symbol": symbol, "productType": "USDT-FUTURES",
                    "granularity": "1H", "limit": limit},
        )
        if not data or data.get("code") != "00000":
            return []
        candles = []
        for row in data.get("data", []):
            try:
                vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                candles.append({
                    "ts": int(row[0]), "open": float(row[1]),
                    "high": float(row[2]), "low": float(row[3]),
                    "close": float(row[4]), "volume_usd": vol_usd,
                })
            except Exception:
                continue
        candles.sort(key=lambda x: x["ts"])
        cls._cache[key]    = candles
        cls._cache_ts[key] = time.time()
        return candles

    @classmethod
    def get_funding(cls, symbol: str) -> float:
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/current-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES"},
        )
        try:
            return float(data["data"][0]["fundingRate"])
        except Exception:
            return 0.0

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()
        cls._cache_ts.clear()


class CoinalyzeClient:
    BASE              = "https://api.coinalyze.net/v1"
    _class_last_call: float = 0.0

    def __init__(self, api_key: str):
        self.api_key          = api_key
        self._markets_cache: Optional[List[dict]] = None
        self._bn_map:         Dict[str, str] = {}
        self._by_map:         Dict[str, str] = {}

    def _wait(self):
        elapsed = time.time() - CoinalyzeClient._class_last_call
        wait    = CONFIG["coinalyze_rate_limit_wait"] - elapsed
        if wait > 0:
            time.sleep(wait)
        CoinalyzeClient._class_last_call = time.time()

    def _get(self, endpoint: str, params: dict) -> Optional[Any]:
        p = dict(params)
        p["api_key"] = self.api_key
        headers = {"User-Agent": f"PrePumpScanner/{VERSION}"}
        for attempt in range(3):
            self._wait()
            try:
                r = requests.get(f"{self.BASE}/{endpoint}", params=p,
                                 headers=headers, timeout=15)
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    try:
                        wait = int(float(retry_after)) + 1 if retry_after else 11
                    except (ValueError, TypeError):
                        wait = 11
                    jitter = random.uniform(0.5, 2.0)
                    log.warning(f"  Coinalyze rate limit — wait {wait}s + {jitter:.1f}s jitter")
                    time.sleep(wait + jitter)
                    continue
                if r.status_code != 200:
                    log.warning(f"  Coinalyze {endpoint} HTTP {r.status_code}: {r.text[:150]}")
                    return None
                data = r.json()
                if isinstance(data, dict) and "error" in data:
                    log.warning(f"  Coinalyze error: {data['error']}")
                    return None
                return data
            except requests.exceptions.Timeout:
                if attempt < 2:
                    time.sleep(3)
            except Exception as e:
                log.warning(f"  Coinalyze request error: {e}")
                if attempt < 2:
                    time.sleep(3)
        return None

    def build_symbol_maps(self, bitget_symbols: List[str]) -> None:
        if self._markets_cache is None:
            log.info("  Loading Coinalyze markets...")
            data = self._get("future-markets", {})
            self._markets_cache = data if isinstance(data, list) else []
            log.info(f"  Got {len(self._markets_cache)} Coinalyze markets")
        markets       = self._markets_cache
        bn_lookup:    Dict[str, str] = {}
        by_ls_lookup: Dict[str, str] = {}
        for m in markets:
            exc        = m.get("exchange", "")
            sym_on_exc = m.get("symbol_on_exchange", "")
            clz_sym    = m.get("symbol", "")
            is_perp    = m.get("is_perpetual", False)
            quote      = m.get("quote_asset", "").upper()
            if not (is_perp and quote == "USDT" and clz_sym):
                continue
            if exc == "A":
                bn_lookup[sym_on_exc] = clz_sym
            elif exc == "6" and m.get("has_long_short_ratio_data"):
                by_ls_lookup[sym_on_exc] = clz_sym

        def _normalize(s: str) -> str:
            if s.startswith("1000"):
                s = s[4:]
            return s.upper()

        def _candidates(sym: str) -> List[str]:
            base = sym.replace("USDT", "")
            cand = [sym, f"{base}/USDT", f"{base}-USDT",
                    f"1000{base}USDT", f"10000{base}USDT"]
            if base.startswith("1000"):
                cand.append(base[4:] + "USDT")
            return list(set(cand))

        mapped_bn, mapped_by = 0, 0
        for sym in bitget_symbols:
            norm_sym = _normalize(sym)
            for cand in _candidates(norm_sym):
                if cand in bn_lookup:
                    self._bn_map[sym] = bn_lookup[cand]
                    mapped_bn += 1
                    break
            for cand in _candidates(norm_sym):
                if cand in by_ls_lookup:
                    self._by_map[sym] = by_ls_lookup[cand]
                    mapped_by += 1
                    break
        log.info(f"  Fuzzy mapping: {mapped_bn}/{len(bitget_symbols)} Binance, {mapped_by}/{len(bitget_symbols)} Bybit")

    def _batch_fetch(self, endpoint: str, symbols: List[str], params: dict) -> Dict[str, list]:
        batch_size = CONFIG["coinalyze_batch_size"]
        result: Dict[str, list] = {}
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            try:
                p = dict(params)
                p["symbols"] = ",".join(batch)
                data = self._get(endpoint, p)
                if data and isinstance(data, list):
                    for item in data:
                        sym  = item.get("symbol", "")
                        hist = item.get("history", [])
                        if sym and hist:
                            result[sym] = hist
                elif data and isinstance(data, dict) and "error" in data:
                    log.warning(f"  API error batch {batch[:3]}...: {data['error']}")
            except Exception as e:
                log.warning(f"  Batch {i//batch_size+1} failed: {e}")
        return result

    def fetch_all_data(self, bitget_symbols: List[str],
                       from_ts: int, to_ts: int) -> Dict[str, ClzData]:
        result  = {sym: ClzData() for sym in bitget_symbols}
        bn_syms = [self._bn_map[s] for s in bitget_symbols if s in self._bn_map]
        by_syms = [self._by_map[s] for s in bitget_symbols if s in self._by_map]
        bn_rev  = {v: k for k, v in self._bn_map.items()}
        by_rev  = {v: k for k, v in self._by_map.items()}
        interval          = CONFIG["coinalyze_interval"]
        fund_interval     = CONFIG["coinalyze_funding_interval"]
        fund_interval_alt = CONFIG["coinalyze_funding_interval_alt"]
        fund_from         = to_ts - CONFIG["coinalyze_funding_lookback_h"] * 3600

        if bn_syms:
            log.info(f"  Fetching Binance OHLCV ({len(bn_syms)} syms)...")
            ohlcv_data = self._batch_fetch("ohlcv-history", bn_syms,
                                           {"interval": interval, "from": from_ts, "to": to_ts})
            for clz_sym, hist in ohlcv_data.items():
                bitget_sym = bn_rev.get(clz_sym)
                if bitget_sym:
                    result[bitget_sym].ohlcv = hist
            log.info(f"    Got {len(ohlcv_data)} OHLCV")

        if bn_syms:
            log.info(f"  Fetching OI history...")
            oi_data = self._batch_fetch("open-interest-history", bn_syms,
                                        {"interval": interval, "from": from_ts, "to": to_ts,
                                         "convert_to_usd": "true"})
            for clz_sym, hist in oi_data.items():
                bitget_sym = bn_rev.get(clz_sym)
                if bitget_sym:
                    result[bitget_sym].oi = hist
            log.info(f"    Got {len(oi_data)} OI series")

        if bn_syms:
            log.info(f"  Fetching Liquidations...")
            liq_data = self._batch_fetch("liquidation-history", bn_syms,
                                         {"interval": interval, "from": from_ts, "to": to_ts,
                                          "convert_to_usd": "true"})
            for clz_sym, hist in liq_data.items():
                bitget_sym = bn_rev.get(clz_sym)
                if bitget_sym:
                    result[bitget_sym].liq = hist
            log.info(f"    Got {len(liq_data)} Liq series")

        if bn_syms:
            log.info(f"  Fetching Funding rate history (7d)...")
            for interval_try in [fund_interval, fund_interval_alt]:
                fund_data = self._batch_fetch("funding-rate-history", bn_syms,
                                              {"interval": interval_try,
                                               "from": fund_from, "to": to_ts})
                if fund_data:
                    log.info(f"    Funding OK using interval '{interval_try}'")
                    for clz_sym, hist in fund_data.items():
                        bitget_sym = bn_rev.get(clz_sym)
                        if bitget_sym:
                            result[bitget_sym].funding_hist = hist
                    break
                else:
                    log.warning(f"    Funding interval '{interval_try}' empty, trying next...")

        if bn_syms:
            log.info(f"  Fetching Predicted funding history...")
            pred_data = self._batch_fetch("predicted-funding-rate-history", bn_syms,
                                          {"interval": "daily", "from": fund_from, "to": to_ts})
            for clz_sym, hist in pred_data.items():
                bitget_sym = bn_rev.get(clz_sym)
                if bitget_sym:
                    result[bitget_sym].predicted_funding_hist = hist
            log.info(f"    Got {len(pred_data)} predicted funding series")

        if by_syms:
            log.info(f"  Fetching Bybit L/S ratio ({len(by_syms)} syms)...")
            ls_data = self._batch_fetch("long-short-ratio-history", by_syms,
                                        {"interval": interval, "from": from_ts, "to": to_ts})
            for clz_sym, hist in ls_data.items():
                bitget_sym = by_rev.get(clz_sym)
                if bitget_sym:
                    result[bitget_sym].ls_ratio = hist
            log.info(f"    Got {len(ls_data)} L/S ratio series")
        else:
            log.warning("  No Bybit symbols mapped — L/S ratio not available")

        return result


# ══════════════════════════════════════════════════════════════════════════════
#  📤  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════
def send_telegram(message: str) -> bool:
    bot_token = CONFIG.get("bot_token")
    chat_id   = CONFIG.get("chat_id")
    if not bot_token or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCANNER LOOP
# ══════════════════════════════════════════════════════════════════════════════
def prefilter_by_bitget(symbols: List[str], tickers: Dict, top_n: int = 60) -> List[str]:
    """
    Pre-filter menggunakan Tier 1 reliable features dari riset 269 pump events:
    ATR absolut (fitur #1), lower wick (fitur #4), momentum decel.
    """
    scored = []
    for sym in symbols:
        try:
            candles = BitgetClient.get_candles(sym, 50)
            if len(candles) < 26:
                continue
            vret_sc,  _ = detect_volatility_return(candles)
            wick_sc,  _ = detect_lower_wick(candles)
            decel_sc, _ = detect_momentum_decel(candles)
            combined    = vret_sc + wick_sc + decel_sc
            if combined >= 5:
                scored.append((sym, combined))
        except Exception:
            pass
    scored.sort(key=lambda x: x[1], reverse=True)
    top        = [s for s, _ in scored[:top_n]]
    rest_count = top_n - len(top)
    if rest_count > 0:
        rest = [s for s in symbols if s not in set(top)]
        random.shuffle(rest)
        top += rest[:rest_count]
    log.info(f"  Pre-filter Bitget (ATR+Wick+Decel): {len(top)}/{len(symbols)} selected")
    return top


def select_universe(tickers: Dict) -> List[str]:
    vol_min    = CONFIG["pre_filter_vol_min"]
    vol_max    = CONFIG["pre_filter_vol_max"]
    candidates = []
    for sym, t in tickers.items():
        if is_stock_token(sym):
            continue
        try:
            vol = float(t.get("quoteVolume", 0))
            if vol_min <= vol <= vol_max:
                candidates.append((sym, vol))
        except Exception:
            pass
    candidates.sort(key=lambda x: x[1])
    n = len(candidates)
    if n > 20:
        lo, hi     = n // 10, n * 9 // 10
        candidates = candidates[lo:hi]
    if len(candidates) > CONFIG["max_symbols_per_scan"]:
        random.shuffle(candidates)
        candidates = candidates[:CONFIG["max_symbols_per_scan"]]
    syms = [s for s, _ in candidates]
    log.info(f"  Universe: {len(syms)} symbols (${vol_min/1e6:.0f}M–${vol_max/1e6:.0f}M)")
    return syms


def main():
    log.info(f"{'═'*70}")
    log.info(f"  PRE-PUMP SCANNER v{VERSION}")
    log.info(f"  Target: Pump ≥15% / 24h | Signal 1-3h sebelumnya")
    log.info(f"  Data:   Bitget(price/candles) + Binance+Bybit via Coinalyze")
    log.info(f"  Riset:  104 pump events | 120 simbol | 46 fitur | v2 dataset")
    log.info(f"  Fix:    price_stability INVERT (range lebar=bullish, pump_med=3.45%)")
    log.info(f"  Fix:    rs_24h threshold 0.5→0.3 (median aktual=0.366, cover 50% pump)")
    log.info(f"  Fix:    Type D hapus stab_sc (anti-pump) — syarat bbw+dry+CLZ")
    log.info(f"  Fix:    momentum_decel weight 12→8 (signal melemah di v2 universe)")
    log.info(f"  Stock token blacklist: {len(CONFIG['stock_token_blacklist'])} symbols")
    log.info(f"{'═'*70}")

    if not CONFIG.get("coinalyze_api_key"):
        log.error("❌ COINALYZE_API_KEY tidak di-set!")
        return 1

    init_db()
    clz = CoinalyzeClient(CONFIG["coinalyze_api_key"])

    log.info("📊 Step 1: Fetching Bitget tickers...")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        log.error("❌ No tickers from Bitget")
        return 1
    log.info(f"  Got {len(tickers)} tickers")

    btc_candles = BitgetClient.get_candles("BTCUSDT", 30)
    btc_chg_1h  = 0.0
    btc_chg_24h = 0.0
    if len(btc_candles) >= 3:
        btc_chg_1h = (btc_candles[-2]["close"] - btc_candles[-3]["close"]) / btc_candles[-3]["close"] * 100
    if len(btc_candles) >= 26:
        btc_chg_24h = get_chg_from_candles(btc_candles, 24)
    log.info(f"  BTC 1h: {btc_chg_1h:+.2f}% | BTC 24h: {btc_chg_24h:+.2f}%")
    if btc_chg_1h < CONFIG["btc_dump_threshold"]:
        log.warning(f"⛔ BTC CIRCUIT BREAKER: {btc_chg_1h:+.1f}% — scan paused")
        return 0

    log.info("🔍 Step 2: Selecting scan universe...")
    active = select_universe(tickers)
    if not active:
        log.error("❌ No symbols passed universe filter")
        return 1

    prefilter_n = CONFIG.get("prefilter_bitget_top_n", 0)
    if prefilter_n > 0 and len(active) > prefilter_n:
        log.info(f"⚡ Step 2b: Pre-filtering (top {prefilter_n})...")
        active = prefilter_by_bitget(active, tickers, top_n=prefilter_n)

    log.info("🗺️  Step 3: Building Coinalyze symbol maps...")
    clz.build_symbol_maps(active)

    log.info("📈 Step 4: Fetching Coinalyze multi-exchange data...")
    now_ts   = int(time.time())
    from_ts  = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    clz_data = clz.fetch_all_data(active, from_ts, now_ts)

    has_ohlcv = sum(1 for d in clz_data.values() if d.has_ohlcv)
    has_oi    = sum(1 for d in clz_data.values() if d.has_oi)
    has_liq   = sum(1 for d in clz_data.values() if d.has_liq)
    has_fund  = sum(1 for d in clz_data.values() if d.has_funding_hist)
    has_pred  = sum(1 for d in clz_data.values() if d.has_predicted_funding)
    has_ls    = sum(1 for d in clz_data.values() if d.has_ls)
    log.info(f"  Coverage: OHLCV={has_ohlcv} OI={has_oi} Liq={has_liq} "
             f"Fund={has_fund} Pred={has_pred} L/S={has_ls}")

    log.info("🎯 Step 5: Scoring...")
    results = []
    for sym in active:
        if is_on_cooldown(sym):
            continue
        try:
            ticker  = tickers.get(sym, {})
            price   = float(ticker.get("lastPr", 0))
            vol_24h = float(ticker.get("quoteVolume", 0))
            if price <= 0:
                continue
            candles = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])
            if len(candles) < 30:
                continue
            chg_24h = get_chg_from_candles(candles, 24)
            chg_1h  = get_chg_from_candles(candles, 1)
            chg_4h  = get_chg_from_candles(candles, 4)
            funding = BitgetClient.get_funding(sym)
            coin_data = CoinData(
                symbol=sym, price=price, vol_24h=vol_24h,
                chg_24h=chg_24h, chg_1h=chg_1h, chg_4h=chg_4h,
                funding=funding, candles=candles,
                btc_chg_1h=btc_chg_1h,
                btc_chg_24h=btc_chg_24h,
                clz=clz_data.get(sym, ClzData()),
            )
            result = score_coin_v14(coin_data)
            if result:
                results.append(result)
                types = "/".join([pt.type_code for pt in result.pump_types])
                src   = result.components.get("data_sources", "")
                d     = result.components.get("detail", {})
                log.info(f"  ✅ {sym}: {result.score} [{result.phase}] [{types}] "
                         f"L/S={d.get('ls',0)} {src}")
        except Exception as e:
            log.warning(f"  ⚠️ {sym}: {e}")

    results.sort(key=lambda x: x.score, reverse=True)
    max_alerts = CONFIG["max_alerts_per_scan"]
    log.info(f"\n{'═'*70}")
    log.info(f"  📊 DONE: {len(results)} signals | Sending top {min(max_alerts, len(results))}")
    log.info(f"{'═'*70}\n")

    sent = 0
    for rank, r in enumerate(results[:10], 1):
        msg = build_alert_v14(r, rank)
        print(msg)
        if sent < max_alerts:
            if send_telegram(msg):
                sent += 1
            entry_price = r.entry["entry"] if r.entry else r.price
            set_alert(r.symbol, r.score, r.phase, entry_price)

    if not results:
        log.info("  No signals this cycle")

    BitgetClient.clear_cache()
    return 0


if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        log.info("\n⚠️ Stopped by user")
        exit(0)
    except Exception as e:
        log.error(f"❌ Fatal: {e}", exc_info=True)
        exit(1)
