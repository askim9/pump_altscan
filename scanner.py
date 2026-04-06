#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v14.3 — FINAL WITH STOCK TOKEN BLOCK                       ║
║                                                                              ║
║  NEW: Blacklist for stock tokens (HOOD, COIN, MSTR, NVDA, AAPL, etc.)       ║
║  FIXES: Retry-After parsing, optimized rate limits, adjusted thresholds     ║
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

VERSION = "14.3-STOCK-BLOCK"

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler("/tmp/scanner_v14.log", maxBytes=10 * 1024**2, backupCount=3)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)

# Set to DEBUG for more details (including short squeeze threshold checks)
# log.setLevel(logging.DEBUG)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG v14.3
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token":         os.getenv("BOT_TOKEN"),
    "chat_id":           os.getenv("CHAT_ID"),
    "clz_binance_suffix": "_PERP.A",
    "clz_bybit_suffix":   ".6",
    "pre_filter_vol_min": 1_000_000,
    "pre_filter_vol_max": 100_000_000,
    "max_symbols_per_scan": 150,
    "velocity_gates": {
        "chg_1h_max": 4.0,
        "chg_4h_max": 8.0,
        "chg_24h_max_early": 12.0,
        "chg_24h_max_continuation": 30.0,
        "chg_24h_min": -8.0,
    },
    "candle_limit_bitget": 100,
    "coinalyze_lookback_h": 72,
    "coinalyze_funding_lookback_h": 168,
    "coinalyze_interval": "1hour",
    "coinalyze_funding_interval": "8hour",
    "coinalyze_batch_size": 10,               # Reduced from 20 to avoid rate limits
    "coinalyze_rate_limit_wait": 0.6,         # Increased from 0.5 for safety
    "baseline_recent_exclude": 3,
    "baseline_lookback_n": 72,
    "baseline_min_samples": 10,
    "ls_ratio_weight": 35,
    "buy_vol_ratio_weight": 30,
    "funding_trend_weight": 25,
    "funding_snapshot_weight": 15,
    "oi_buildup_weight": 20,
    "short_liq_weight": 20,
    "liq_cascade_weight": 15,
    "bbw_squeeze_weight": 25,
    "price_stability_weight": 12,
    "volume_dryup_weight": 10,
    "accumulation_weight": 20,
    "volatility_return_weight": 15,
    "rs_btc_weight": 12,
    "multiwave_bonus": 30,
    "alert_threshold_early": 85,
    "alert_threshold_continuation": 100,
    "alert_threshold_reversal": 80,
    "min_rr_ratio": 2.0,
    "max_alerts_per_scan": 5,
    "atr_candles": 14,
    "sl_mult_volatile": 2.5,
    "sl_mult_normal": 2.0,
    "sl_mult_quiet": 1.5,
    "tp1_pct": 15.0,
    "tp2_pct": 30.0,
    "tp3_pct": 50.0,
    "account_balance": 10000.0,
    "risk_per_trade_pct": 1.0,
    "max_position_pct": 5.0,
    "max_leverage": 10,
    "pump_history_db": "/tmp/scanner_v14_history.db",
    "pump_threshold_pct": 15,
    "pump_max_duration_h": 24,
    "multiwave_lookback_days": 30,
    "btc_dump_threshold": -3.0,
    "ls_long_extreme_low": 0.38,
    "ls_long_low": 0.44,
    "ls_long_normal": 0.50,
    "ls_long_high": 0.58,
    "bv_ratio_strong": 0.62,
    "bv_ratio_moderate": 0.55,
    # Adjusted thresholds for short squeeze detection (more sensitive)
    "short_squeeze_ls_min": 15,
    "short_squeeze_liq_min": 8,
    "short_squeeze_fund_min": 10,
    "whale_accum_bv_min": 15,
    "whale_accum_accum_min": 10,
    # BLACKLIST: stock tokens to exclude (mencegah false signal dari saham)
    "stock_token_blacklist": [
        "TSLAUSDT","CRCLUSDT", "SPYUSDT","GOOGLUSDT","COINUSDT","NVDAUSDT","METAUSDT","QQQUSDT","GLDUSDT",
"MSFTUSDT","AAPLUSDT","MSTRUSDT","PLTRUSDT","INTCUSDT","XAUSDT", "BZUSDT", "TONUSDT", "BGBUSDT",  "BNBUSDT", "TRXUSDT", 
         "MCDUSDT", "XRPUSDT" # tambahan jika perlu
    ],
}


# ══════════════════════════════════════════════════════════════════════════════
#  📊  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class ClzData:
    ohlcv: List[dict] = field(default_factory=list)
    oi: List[dict] = field(default_factory=list)
    liq: List[dict] = field(default_factory=list)
    funding_hist: List[dict] = field(default_factory=list)
    ls_ratio: List[dict] = field(default_factory=list)

    @property
    def has_ohlcv(self) -> bool:
        return len(self.ohlcv) >= 10
    @property
    def has_oi(self) -> bool:
        return len(self.oi) >= 4
    @property
    def has_liq(self) -> bool:
        return len(self.liq) >= 4
    @property
    def has_funding_hist(self) -> bool:
        return len(self.funding_hist) >= 3
    @property
    def has_ls(self) -> bool:
        return len(self.ls_ratio) >= 4
    @property
    def last_buy_ratio(self) -> float:
        if not self.has_ohlcv:
            return 0.0
        for c in reversed(self.ohlcv[:-1]):
            v = float(c.get("v", 0) or 0)
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
    symbol: str
    price: float
    vol_24h: float
    chg_24h: float
    chg_1h: float
    chg_4h: float
    funding: float
    candles: List[dict]
    btc_chg_1h: float = 0.0
    clz: ClzData = field(default_factory=ClzData)


@dataclass
class PhaseInfo:
    phase: str
    base_score: int
    description: str
    risk_level: str


@dataclass
class PumpType:
    type_code: str
    type_name: str
    confidence: int
    signals: List[str]


@dataclass
class PumpEvent:
    symbol: str
    timestamp: datetime
    magnitude_pct: float
    duration_hours: float
    type: str


@dataclass
class ScoreResult:
    symbol: str
    score: int
    phase: str
    pump_types: List[PumpType]
    confidence: str
    components: Dict[str, Any]
    catalysts: List[str]
    entry: Optional[dict]
    price: float
    vol_24h: float
    chg_24h: float
    chg_1h: float
    funding: float
    urgency: str
    risk_warnings: List[str] = field(default_factory=list)
    position: Optional[dict] = None


# ══════════════════════════════════════════════════════════════════════════════
#  🗄️  DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def init_db():
    db = CONFIG["pump_history_db"]
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS pump_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL, timestamp INTEGER NOT NULL,
            magnitude_pct REAL NOT NULL, duration_hours REAL NOT NULL,
            event_type TEXT NOT NULL, created_at INTEGER DEFAULT (strftime('%s','now'))
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
    c.execute("CREATE INDEX IF NOT EXISTS idx_sym_ts ON pump_events(symbol, timestamp DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_alert_sym ON alerts(symbol, alerted_at DESC)")
    conn.commit()
    conn.close()


def is_on_cooldown(symbol: str, cooldown_hours: int = 6) -> bool:
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        c = conn.cursor()
        c.execute("SELECT MAX(alerted_at) FROM alerts WHERE symbol = ?", (symbol,))
        row = c.fetchone()
        conn.close()
        if row and row[0]:
            return (time.time() - row[0]) < (cooldown_hours * 3600)
    except Exception:
        pass
    return False


def set_alert(symbol: str, score: int, phase: str, entry_price: float):
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        c = conn.cursor()
        c.execute(
            "INSERT INTO alerts (symbol, alerted_at, score, phase, entry_price) VALUES (?,?,?,?,?)",
            (symbol, int(time.time()), score, phase, entry_price)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"set_alert failed: {e}")


def get_pump_history(symbol: str, days: int = 30) -> List[PumpEvent]:
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        c = conn.cursor()
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
    med = sorted(baseline)[len(baseline) // 2]
    deviations = [abs(x - med) for x in baseline]
    mad = sorted(deviations)[len(deviations) // 2]
    if mad < 1e-9:
        return 0.0
    return (val - med) / (mad * 1.4826)

def get_chg_from_candles(candles: List[dict], n_hours: int) -> float:
    if len(candles) < n_hours + 2:
        return 0.0
    now_price = candles[-2]["close"]
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
    """Cek apakah simbol termasuk dalam blacklist stock token"""
    blacklist = CONFIG.get("stock_token_blacklist", [])
    return symbol in blacklist


# ══════════════════════════════════════════════════════════════════════════════
#  📐  ATR & ENTRY/SL/TP
# ══════════════════════════════════════════════════════════════════════════════
def calc_atr(candles: List[dict], n: int = 14) -> float:
    trs = []
    for i in range(2, min(n + 2, len(candles))):
        c = candles[-i]
        pc = candles[-(i + 1)]["close"]
        if pc > 0:
            tr = max(
                (c["high"] - c["low"]) / pc,
                abs(c["high"] - pc) / pc,
                abs(c["low"] - pc) / pc,
            )
            trs.append(tr)
    return _mean(trs) if trs else 0.02

def calc_entry_targets(data: CoinData) -> Optional[dict]:
    if len(data.candles) < 16:
        return None
    atr = calc_atr(data.candles, CONFIG["atr_candles"])
    entry = data.price
    if atr > 0.04:
        sl_mult = CONFIG["sl_mult_volatile"]
    elif atr > 0.02:
        sl_mult = CONFIG["sl_mult_normal"]
    else:
        sl_mult = CONFIG["sl_mult_quiet"]
    sl = entry * (1 - atr * sl_mult)
    sl_pct = (entry - sl) / entry * 100
    tp1 = entry * (1 + CONFIG["tp1_pct"] / 100)
    tp2 = entry * (1 + CONFIG["tp2_pct"] / 100)
    tp3 = entry * (1 + CONFIG["tp3_pct"] / 100)
    risk = entry - sl
    if risk <= 0:
        return None
    rr1 = (tp1 - entry) / risk
    if rr1 < CONFIG["min_rr_ratio"]:
        return None
    return {
        "entry": round(entry, 8),
        "entry_zone_low": round(entry * (1 - atr * 0.3), 8),
        "entry_zone_high": round(entry * (1 + atr * 0.2), 8),
        "sl": round(sl, 8),
        "sl_pct": round(sl_pct, 1),
        "tp1": round(tp1, 8), "tp1_pct": CONFIG["tp1_pct"],
        "tp2": round(tp2, 8), "tp2_pct": CONFIG["tp2_pct"],
        "tp3": round(tp3, 8), "tp3_pct": CONFIG["tp3_pct"],
        "rr1": round(rr1, 2), "rr2": round((tp2 - entry) / risk, 2),
        "atr_pct": round(atr * 100, 2),
        "atr_decimal": atr,
        "sl_mult": sl_mult,
    }

def calc_position_size(entry: float, sl: float, atr: float) -> dict:
    bal = CONFIG["account_balance"]
    risk_usd = bal * CONFIG["risk_per_trade_pct"] / 100
    risk_per_unit = (entry - sl) / entry
    if risk_per_unit <= 0:
        risk_per_unit = atr * CONFIG["sl_mult_normal"]
    pos_needed = risk_usd / risk_per_unit
    pos_cap = bal * CONFIG["max_position_pct"] / 100
    pos_val = min(pos_needed, pos_cap)
    leverage = min(pos_val / bal, CONFIG["max_leverage"]) if pos_val > bal else 1.0
    pos_val = min(pos_val, bal * max(leverage, 1))
    return {
        "position_size": round(pos_val / entry, 6) if entry > 0 else 0,
        "leverage": round(leverage, 2),
        "risk_usd": round(risk_usd, 2),
        "position_value": round(pos_val, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  PHASE CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════
def classify_phase(chg_24h: float) -> PhaseInfo:
    if chg_24h < -8.0:
        return PhaseInfo("DOWNTREND", 5, "Deep downtrend", "HIGH")
    elif chg_24h < -3.0:
        return PhaseInfo("WEAK", 15, "Weak", "MEDIUM-HIGH")
    elif chg_24h > 25.0:
        return PhaseInfo("PARABOLIC", 10, "Parabolic", "EXTREME")
    elif chg_24h > 12.0:
        base = max(20, 40 - int(chg_24h - 12) * 2)
        return PhaseInfo("CONTINUATION", base, "Momentum", "MEDIUM")
    else:
        if abs(chg_24h) <= 3.0:
            base = 60
        elif chg_24h <= 8.0:
            base = 50
        else:
            base = 40
        return PhaseInfo("EARLY", base, "Early — prime zone", "LOW")


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  TIER 1 SIGNALS — COINALYZE MULTI-EXCHANGE
# ══════════════════════════════════════════════════════════════════════════════
def score_long_short_ratio(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_ls:
        return 0, {"source": "no_ls_data"}
    hist = clz.ls_ratio
    if len(hist) < 4:
        return 0, {"source": "insufficient_ls"}
    current_long = float(hist[-2].get("l", 0.5) or 0.5)
    current_short = float(hist[-2].get("s", 0.5) or 0.5)
    ls_ratio = float(hist[-2].get("r", 1.0) or 1.0)
    long_4h_ago = float(hist[-5].get("l", 0.5) or 0.5) if len(hist) >= 5 else current_long
    long_trend = current_long - long_4h_ago
    score = 0
    signals = []
    cfg = CONFIG
    if current_long < cfg["ls_long_extreme_low"]:
        score += 30
        signals.append(f"EXTREME_SHORT_DOM longs={current_long:.1%}")
    elif current_long < cfg["ls_long_low"]:
        score += 20
        signals.append(f"SHORT_DOM longs={current_long:.1%}")
    if long_trend < -0.03:
        score += 12
        signals.append(f"SHORTS_ADDING Δ={long_trend:.2%}")
    elif long_trend < -0.015:
        score += 6
        signals.append(f"LONGS_REDUCING Δ={long_trend:.2%}")
    if current_long > cfg["ls_long_high"]:
        score = max(0, score - 15)
        signals.append(f"⚠️ LONG_HEAVY={current_long:.1%}")
    return min(score, cfg["ls_ratio_weight"]), {
        "long_ratio": round(current_long, 4),
        "short_ratio": round(current_short, 4),
        "ls_ratio_val": round(ls_ratio, 4),
        "long_trend_4h": round(long_trend, 4),
        "signals": signals,
    }

def score_buy_volume_ratio(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_ohlcv:
        return 0, {"source": "no_ohlcv"}
    hist = clz.ohlcv
    cfg = CONFIG
    recent = [c for c in hist[-7:-1] if float(c.get("v", 0) or 0) > 0]
    if len(recent) < 3:
        return 0, {"source": "insufficient_ohlcv"}
    bv_ratios = []
    for c in recent:
        v = float(c.get("v", 0) or 0)
        bv = float(c.get("bv", 0) or 0)
        if v > 0:
            bv_ratios.append(bv / v)
    if not bv_ratios:
        return 0, {"source": "no_bv_data"}
    avg_bv_ratio = _mean(bv_ratios)
    last_bv_ratio = bv_ratios[-1] if bv_ratios else 0
    if len(bv_ratios) >= 3:
        early_avg = _mean(bv_ratios[:len(bv_ratios)//2])
        late_avg = _mean(bv_ratios[len(bv_ratios)//2:])
        bv_trend = late_avg - early_avg
    else:
        bv_trend = 0
    tx_total = [float(c.get("tx", 0) or 0) for c in recent]
    btx_count = [float(c.get("btx", 0) or 0) for c in recent]
    avg_btx_ratio = _mean([b/t for b,t in zip(btx_count, tx_total) if t > 0])
    score = 0
    signals = []
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
        "avg_bv_ratio": round(avg_bv_ratio, 4),
        "last_bv_ratio": round(last_bv_ratio, 4),
        "bv_trend": round(bv_trend, 4),
        "avg_btx_ratio": round(avg_btx_ratio, 4),
        "signals": signals,
    }

def score_funding_trend(clz: ClzData, current_funding: float) -> Tuple[int, dict]:
    cfg = CONFIG
    score = 0
    signals = []
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
            "signals": signals, "trend": "no_history"
        }
    hist = clz.funding_hist
    rates = [float(c.get("c", 0) or 0) for c in hist if c.get("c") is not None]
    if len(rates) < 3:
        return score, {"signals": signals}
    recent_24h = rates[-3:]
    prev_24h = rates[-6:-3] if len(rates) >= 6 else rates[:3]
    avg_recent = _mean(recent_24h)
    avg_prev = _mean(prev_24h)
    funding_drift = avg_recent - avg_prev
    if funding_drift < -0.0003:
        score += cfg["funding_trend_weight"]
        signals.append(f"FUNDING_TRENDING_NEGATIVE Δ={funding_drift*100:.4f}%")
    elif funding_drift < -0.0001:
        score += int(cfg["funding_trend_weight"] * 0.6)
        signals.append(f"FUNDING_DRIFTING_NEG Δ={funding_drift*100:.4f}%")
    if avg_prev > 0.0001 and avg_recent < -0.0001:
        score += 8
        signals.append(f"FUNDING_FLIPPED_NEG (was +{avg_prev*100:.4f}%)")
    neg_count = sum(1 for r in rates[-9:] if r < -0.0001)
    if neg_count >= 7:
        score += 6
        signals.append(f"PERSISTENT_NEG {neg_count}/9 entries negative")
    total_weight = cfg["funding_trend_weight"] + cfg["funding_snapshot_weight"]
    return min(score, total_weight), {
        "current": round(current_funding * 100, 5),
        "avg_24h": round(avg_recent * 100, 5),
        "avg_prev_24h": round(avg_prev * 100, 5),
        "drift": round(funding_drift * 100, 5),
        "signals": signals,
    }

def score_oi_buildup(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_oi:
        return 0, {"source": "no_oi"}
    hist = clz.oi
    if len(hist) < 6:
        return 0, {"source": "insufficient_oi"}
    oi_now = float(hist[-2].get("c", 0) or 0)
    oi_4h = float(hist[-5].get("c", 0) or 0)
    oi_12h = float(hist[-13].get("c", 0) or 0) if len(hist) >= 13 else oi_4h
    if oi_4h <= 0:
        return 0, {"source": "oi_zero"}
    oi_chg_4h = (oi_now - oi_4h) / oi_4h * 100
    oi_chg_12h = (oi_now - oi_12h) / oi_12h * 100 if oi_12h > 0 else 0
    oi_vals = [float(c.get("c", 0) or 0) for c in hist[-12:-1]]
    oi_trend_up = sum(1 for i in range(1, len(oi_vals)) if oi_vals[i] > oi_vals[i-1])
    oi_consistency = oi_trend_up / max(len(oi_vals) - 1, 1)
    score = 0
    signals = []
    w = CONFIG["oi_buildup_weight"]
    if oi_chg_4h > 5.0 and oi_consistency >= 0.6:
        score += w
        signals.append(f"STRONG_OI_BUILDUP +{oi_chg_4h:.1f}%")
    elif oi_chg_4h > 2.5:
        score += int(w * 0.6)
        signals.append(f"OI_BUILDUP +{oi_chg_4h:.1f}%")
    elif oi_chg_4h > 1.0:
        score += int(w * 0.3)
        signals.append(f"OI_RISING +{oi_chg_4h:.1f}%")
    if oi_chg_12h > 8.0:
        score += 5
        signals.append(f"OI_BUILDUP_12H +{oi_chg_12h:.1f}%")
    return min(score, w + 5), {
        "oi_chg_4h_pct": round(oi_chg_4h, 2),
        "oi_chg_12h_pct": round(oi_chg_12h, 2),
        "oi_consistency": round(oi_consistency, 2),
        "signals": signals,
    }

def score_liquidations(clz: ClzData) -> Tuple[int, dict]:
    if not clz.has_liq:
        return 0, {"source": "no_liq"}
    hist = clz.liq
    if len(hist) < 6:
        return 0, {"source": "insufficient_liq"}
    baseline_short = [float(c.get("s", 0) or 0) for c in hist[-24:-3]]
    baseline_long = [float(c.get("l", 0) or 0) for c in hist[-24:-3]]
    current_short = float(hist[-2].get("s", 0) or 0)
    current_long = float(hist[-2].get("l", 0) or 0)
    if not baseline_short:
        return 0, {"source": "no_baseline"}
    baseline_avg_short = _mean(baseline_short) + 1
    baseline_avg_long = _mean(baseline_long) + 1
    short_liq_z = robust_zscore(current_short, baseline_short)
    short_ratio = current_short / baseline_avg_short
    recent_4h_short = [float(c.get("s", 0) or 0) for c in hist[-5:-1]]
    cascade_score = 0
    if len(recent_4h_short) >= 3:
        increases = sum(1 for i in range(1, len(recent_4h_short)) if recent_4h_short[i] > recent_4h_short[i-1] * 1.1)
        cascade_score = increases
    score = 0
    signals = []
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
        "short_liq_z": round(short_liq_z, 2),
        "short_ratio": round(short_ratio, 2),
        "cascade_score": cascade_score,
        "short_usd": round(current_short),
        "long_usd": round(current_long),
        "signals": signals,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  TIER 3 SIGNALS — BITGET CANDLES
# ══════════════════════════════════════════════════════════════════════════════
def detect_bbw_squeeze(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 22:
        return 0, {}
    closes = [c["close"] for c in candles[-20:]]
    sma = _mean(closes)
    if sma <= 0:
        return 0, {}
    var = sum((x - sma) ** 2 for x in closes) / 20
    std = var ** 0.5
    bb_w = (sma + 2*std - (sma - 2*std)) / sma
    getting_tighter = False
    if len(candles) >= 44:
        prev_closes = [c["close"] for c in candles[-44:-24]]
        if prev_closes:
            p_sma = _mean(prev_closes)
            p_var = sum((x - p_sma) ** 2 for x in prev_closes) / len(prev_closes)
            p_std = p_var ** 0.5
            p_bbw = (p_sma + 2*p_std - (p_sma - 2*p_std)) / p_sma if p_sma > 0 else 0
            getting_tighter = bb_w < p_bbw
    w = CONFIG["bbw_squeeze_weight"]
    if bb_w < 0.04:
        score, pat = w, "EXTREME_SQUEEZE"
    elif bb_w < 0.06:
        score, pat = int(w * 0.8), "TIGHT_SQUEEZE"
    elif bb_w < 0.08:
        score, pat = int(w * 0.55), "MODERATE_SQUEEZE"
    elif bb_w < 0.10:
        score, pat = int(w * 0.28), "LIGHT_SQUEEZE"
    else:
        score, pat = 0, "NO_SQUEEZE"
    if getting_tighter and score > 0:
        score = min(score + 5, w + 5)
        pat += "+SQUEEZING"
    return score, {"bb_w": round(bb_w, 4), "pattern": pat, "tighter": getting_tighter}

def detect_price_stability(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 10:
        return 0, {}
    recent = candles[-9:-1]
    closes = [c["close"] for c in recent]
    lo, hi = min(closes), max(closes)
    ref = (lo + hi) / 2
    if ref <= 0:
        return 0, {}
    range_pct = (hi - lo) / ref * 100
    last = candles[-2]
    curr_chg = (last["close"] - last["open"]) / last["open"] * 100 if last.get("open", 0) > 0 else 0
    w = CONFIG["price_stability_weight"]
    if range_pct < 1.5 and abs(curr_chg) < 0.5:
        return w, {"range_pct": round(range_pct, 2), "pattern": "COILING"}
    elif range_pct < 2.5:
        return int(w * 0.67), {"range_pct": round(range_pct, 2), "pattern": "CONSOLIDATING"}
    elif range_pct < 4.0:
        return int(w * 0.33), {"range_pct": round(range_pct, 2), "pattern": "RANGING"}
    return 0, {"range_pct": round(range_pct, 2), "pattern": "WIDE"}

def detect_volume_dryup(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 26:
        return 0, {}
    cur_vol = candles[-2].get("volume_usd", 0)
    avg_vol = _mean([c.get("volume_usd", 0) for c in candles[-26:-2]])
    if avg_vol <= 0:
        return 0, {}
    tod = volume_tod_mult(get_hour_utc())
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
    cur_vol = _mean([c.get("volume_usd", 0) for c in candles[-7:-1]])
    base_vol = _mean([c.get("volume_usd", 0) for c in candles[-25:-7]])
    if base_vol <= 0:
        return 0, {}
    ratio = cur_vol / base_vol
    if candles[-7]["close"] > 0:
        p_chg = (candles[-2]["close"] - candles[-7]["close"]) / candles[-7]["close"] * 100
    else:
        return 0, {}
    w = CONFIG["accumulation_weight"]
    if ratio >= 3.0 and -2 < p_chg < 4:
        return w, {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "STRONG_ACCUM"}
    elif ratio >= 2.5 and -2 < p_chg < 5:
        return int(w * 0.75), {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "ACCUM"}
    elif ratio >= 2.0 and -1 < p_chg < 4:
        return int(w * 0.5), {"vol_ratio": round(ratio, 2), "price_chg": round(p_chg, 2), "pattern": "LIGHT_ACCUM"}
    return 0, {"vol_ratio": round(ratio, 2), "pattern": "NO_ACCUM"}

def detect_volatility_return(candles: List[dict]) -> Tuple[int, dict]:
    if len(candles) < 50:
        return 0, {}
    atr_now = calc_atr(candles[-22:], 14)
    atr_hist = calc_atr(candles[-72:-24], 14) if len(candles) >= 74 else calc_atr(candles[:-24], 14)
    if atr_hist <= 0:
        return 0, {}
    ratio = atr_now / atr_hist
    w = CONFIG["volatility_return_weight"]
    if ratio < 0.40:
        return w, {"atr_ratio": round(ratio, 3), "pattern": "EXTREME_QUIET"}
    elif ratio < 0.60:
        return int(w * 0.7), {"atr_ratio": round(ratio, 3), "pattern": "VERY_QUIET"}
    elif ratio < 0.75:
        return int(w * 0.4), {"atr_ratio": round(ratio, 3), "pattern": "QUIET"}
    return 0, {"atr_ratio": round(ratio, 3), "pattern": "NORMAL_VOL"}

def detect_rs_btc(coin_chg_1h: float, btc_chg_1h: float) -> Tuple[int, dict]:
    if btc_chg_1h == 0:
        return 0, {"rs": 0}
    rs = coin_chg_1h - btc_chg_1h
    w = CONFIG["rs_btc_weight"]
    if rs > 3.0 and btc_chg_1h <= 0.5:
        return w, {"rs": round(rs, 2), "pattern": "STRONG_DECOUPLE"}
    elif rs > 2.0:
        return int(w * 0.67), {"rs": round(rs, 2), "pattern": "OUTPERFORMING"}
    elif rs > 1.0:
        return int(w * 0.33), {"rs": round(rs, 2), "pattern": "SLIGHTLY_BETTER"}
    return 0, {"rs": round(rs, 2), "pattern": "INLINE"}


# ══════════════════════════════════════════════════════════════════════════════
#  🔄  CONTINUATION & REVERSAL
# ══════════════════════════════════════════════════════════════════════════════
def check_multiwave_history(symbol: str) -> Tuple[int, dict]:
    history = get_pump_history(symbol, CONFIG["multiwave_lookback_days"])
    pumps = [e for e in history if e.type == "PUMP" and e.magnitude_pct >= CONFIG["pump_threshold_pct"]]
    if len(pumps) < 2:
        return 0, {}
    gaps = [(pumps[i].timestamp - pumps[i+1].timestamp).total_seconds() / 3600 for i in range(len(pumps)-1)]
    avg_gap = _mean(gaps)
    hours_since = (datetime.now(timezone.utc) - pumps[0].timestamp).total_seconds() / 3600
    in_window = False
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
        "num_pumps": len(pumps), "avg_gap_h": round(avg_gap, 1),
        "hours_since": round(hours_since, 1), "pattern": pattern, "in_window": in_window,
    }

def check_reversal_pattern(data: CoinData) -> Tuple[int, dict]:
    candles = data.candles
    score, signals = 0, []
    distance = None
    if len(candles) >= 48:
        lows = [c["low"] for c in candles[-48:]]
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
                    score += 22; signals.append("AT_SUPPORT")
                elif distance < 0.04:
                    score += 12; signals.append("NEAR_SUPPORT")
    if len(candles) >= 24:
        cv = candles[-2].get("volume_usd", 0)
        av = _mean([c.get("volume_usd", 0) for c in candles[-24:-2]])
        if av > 0 and cv > av * 3:
            score += 15; signals.append("CAPITULATION_VOL")
    last = candles[-2]
    cr = last["high"] - last["low"]
    if cr > 0 and (last["close"] - last["low"]) / cr > 0.55:
        score += 10; signals.append("REJECTION_WICK")
    if data.funding < -0.0005:
        score += 12; signals.append("FUNDING_EXTREME")
    return score, {"signals": signals, "distance_pct": round(distance*100, 2) if distance else None}


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
#  🏆  MASTER SCORING v14.3
# ══════════════════════════════════════════════════════════════════════════════
def score_coin_v14(data: CoinData) -> Optional[ScoreResult]:
    if data.vol_24h < CONFIG["pre_filter_vol_min"] or data.price <= 0:
        return None
    phase = classify_phase(data.chg_24h)
    is_cont = phase.phase == "CONTINUATION"
    if phase.phase not in ["DOWNTREND", "WEAK"]:
        blocked, reason = check_velocity_gates(data.chg_24h, data.chg_1h, data.chg_4h, is_cont)
        if blocked:
            return None
    clz = data.clz
    phase_score = phase.base_score
    catalysts = []
    risk_warnings = []
    pump_types: List[PumpType] = []
    # Tier 1
    ls_sc, ls_d = score_long_short_ratio(clz)
    bv_sc, bv_d = score_buy_volume_ratio(clz)
    fund_sc, fund_d = score_funding_trend(clz, data.funding)
    tier1_score = ls_sc + bv_sc + fund_sc
    if ls_sc > 0:
        catalysts.append(f"📊 L/S={ls_d.get('long_ratio',0):.1%}longs {' | '.join(ls_d.get('signals',[])[:2])}")
    if bv_sc > 0:
        catalysts.append(f"💚 BuyVol={bv_d.get('avg_bv_ratio',0):.1%} {' | '.join(bv_d.get('signals',[])[:2])}")
    if fund_sc > 0:
        catalysts.append(f"💰 Fund={fund_d.get('current',0):.4f}% {' | '.join(fund_d.get('signals',[])[:2])}")
    # Tier 2
    oi_sc, oi_d = score_oi_buildup(clz)
    liq_sc, liq_d = score_liquidations(clz)
    tier2_score = oi_sc + liq_sc
    if oi_sc > 0:
        catalysts.append(f"📈 OI +{oi_d.get('oi_chg_4h_pct',0):.1f}%4h {' | '.join(oi_d.get('signals',[])[:1])}")
    if liq_sc > 0:
        catalysts.append(f"💥 ShortLiq z={liq_d.get('short_liq_z',0):.1f} {' | '.join(liq_d.get('signals',[])[:2])}")
    # Tier 3
    bbw_sc, bbw_d = detect_bbw_squeeze(data.candles)
    stab_sc, stab_d = detect_price_stability(data.candles)
    dry_sc, dry_d = detect_volume_dryup(data.candles)
    accum_sc, accum_d = detect_accumulation(data.candles)
    vret_sc, vret_d = detect_volatility_return(data.candles)
    rs_sc, rs_d = detect_rs_btc(data.chg_1h, data.btc_chg_1h)
    tier3_score = bbw_sc + stab_sc + dry_sc + accum_sc + vret_sc + rs_sc
    if bbw_sc > 0:
        catalysts.append(f"📐 BBW={bbw_d.get('bb_w',0):.3f} {bbw_d.get('pattern','')}")
    if accum_sc > 0:
        catalysts.append(f"🐋 Accum x{accum_d.get('vol_ratio',0):.1f} Δp={accum_d.get('price_chg',0):+.1f}%")
    if vret_sc > 0:
        catalysts.append(f"⚡ {vret_d.get('pattern','')} atr_ratio={vret_d.get('atr_ratio',0):.2f}")
    if rs_sc > 0:
        catalysts.append(f"📊 RS={rs_d.get('rs',0):+.1f}% vs BTC")
    # Pump types with adjusted thresholds
    cfg = CONFIG
    # Short squeeze (Type E)
    if ls_sc >= cfg["short_squeeze_ls_min"] and (liq_sc >= cfg["short_squeeze_liq_min"] or fund_sc >= cfg["short_squeeze_fund_min"]):
        pump_types.append(PumpType("E", "Short Squeeze", min((ls_sc+liq_sc+fund_sc)*2, 100), ls_d.get("signals",[])+liq_d.get("signals",[])))
        log.debug(f"  {data.symbol}: Short squeeze candidate (ls={ls_sc}, liq={liq_sc}, fund={fund_sc})")
    # Whale accumulation (Type B)
    if bv_sc >= cfg["whale_accum_bv_min"] and accum_sc >= cfg["whale_accum_accum_min"]:
        pump_types.append(PumpType("B", "Whale Accumulation", min((bv_sc+accum_sc)*3, 100), bv_d.get("signals",[])+[accum_d.get("pattern","")]))
    # Technical breakout (Type D)
    if bbw_sc >= 15 and (stab_sc >= 8 or dry_sc >= 6):
        pump_types.append(PumpType("D", "Technical Breakout", min((bbw_sc+stab_sc)*3, 100), [bbw_d.get("pattern",""), stab_d.get("pattern","")]))
    # Volatility return (Type F)
    if vret_sc >= 10:
        pump_types.append(PumpType("F", "Volatility Return", min(vret_sc*5, 100), [vret_d.get("pattern","")]))
    # Multi-wave (Type G)
    mw_bonus = 0
    if phase.phase in ["EARLY", "CONTINUATION", "PARABOLIC"]:
        mw_sc, mw_d = check_multiwave_history(data.symbol)
        if mw_sc > 0:
            mw_bonus = mw_sc
            catalysts.append(f"🔄 Multi-wave {mw_d['num_pumps']}x, gap {mw_d['avg_gap_h']:.0f}h [{mw_d['pattern']}]")
            pump_types.append(PumpType("G", "Multi-wave", mw_sc, [mw_d.get("pattern","")]))
    # Reversal (Type R)
    reversal_score = 0
    if phase.phase in ["DOWNTREND", "WEAK"]:
        rev_sc, rev_d = check_reversal_pattern(data)
        reversal_score = rev_sc
        if rev_sc >= 35:
            catalysts.append(f"↩️ Reversal: {', '.join(rev_d.get('signals', []))}")
            pump_types.append(PumpType("R", "Reversal Bounce", min(rev_sc, 100), rev_d.get("signals", [])))
        else:
            risk_warnings.append(f"⚠️ Reversal weak ({rev_sc}pts)")
    total = phase_score + tier1_score + tier2_score + tier3_score + mw_bonus + reversal_score
    has_any_clz = clz.has_ohlcv or clz.has_oi or clz.has_liq or clz.has_ls or clz.has_funding_hist
    if not has_any_clz:
        risk_warnings.append("⚠️ No Coinalyze data — score based on Bitget candles only")
    # Thresholds
    if phase.phase == "EARLY":
        threshold = CONFIG["alert_threshold_early"]
    elif phase.phase == "CONTINUATION":
        threshold = CONFIG["alert_threshold_continuation"]
    elif phase.phase in ["DOWNTREND", "WEAK"]:
        threshold = CONFIG["alert_threshold_reversal"]
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
    type_labels = {"E":"💰 SHORT SQUEEZE","B":"🐋 WHALE ACCUM","D":"📐 BREAKOUT","F":"⚡ VOL RETURN","G":"🔄 CONTINUATION","R":"↩️ REVERSAL"}
    top = pump_types[0]
    all_types = "/".join([pt.type_code for pt in pump_types])
    urg = f"{type_labels.get(top.type_code, '🎯')} [{all_types}]"
    confidence = "very_strong" if total >= 120 else "strong" if total >= 90 else "watch"
    data_sources = []
    if clz.has_ls: data_sources.append("L/S✅")
    if clz.has_ohlcv: data_sources.append("BV✅")
    if clz.has_funding_hist: data_sources.append("Fund✅")
    if clz.has_oi: data_sources.append("OI✅")
    if clz.has_liq: data_sources.append("Liq✅")
    return ScoreResult(
        symbol=data.symbol, score=min(total,250), phase=phase.phase, pump_types=pump_types, confidence=confidence,
        components={"phase":phase_score, "tier1_clz":tier1_score, "tier2_clz":tier2_score, "tier3_technical":tier3_score,
                    "multiwave":mw_bonus, "reversal":reversal_score,
                    "detail":{"ls":ls_sc, "bv":bv_sc, "fund":fund_sc, "oi":oi_sc, "liq":liq_sc,
                              "bbw":bbw_sc, "stab":stab_sc, "dry":dry_sc, "accum":accum_sc, "vret":vret_sc, "rs":rs_sc},
                    "data_sources": " ".join(data_sources) if data_sources else "Bitget-only"},
        catalysts=catalysts, entry=entry_data, price=data.price, vol_24h=data.vol_24h,
        chg_24h=data.chg_24h, chg_1h=data.chg_1h, funding=data.funding, urgency=urg,
        risk_warnings=risk_warnings, position=position,
    )

def build_alert_v14(r: ScoreResult, rank: int) -> str:
    vol = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    em = {"very_strong":"🟢","strong":"🟡","watch":"⚪"}.get(r.confidence,"⚪")
    bar_len = min(20, r.score * 20 // 200)
    bar = "█" * bar_len + "░" * (20 - bar_len)
    comp = r.components
    d = comp.get("detail", {})
    lines = [
        f"{'─'*58}",
        f"#{rank}  {r.symbol}  {em}  Score: {r.score}  [{r.phase}]",
        f"   {bar}",
        f"   {r.urgency}",
        f"   Data: {comp.get('data_sources', 'N/A')}",
        f"",
    ]
    if r.catalysts:
        lines.append("   📊 Signals:")
        for c in r.catalysts[:7]:
            lines.append(f"      {c}")
        lines.append("")
    if r.risk_warnings:
        lines.append("   ⚠️ Risks:")
        for w in r.risk_warnings[:3]:
            lines.append(f"      {w}")
        lines.append("")
    lines.append(f"   Vol: {vol} | Δ1h: {r.chg_1h:+.1f}% | Δ24h: {r.chg_24h:+.1f}% | F: {r.funding*100:.4f}%")
    lines.append(f"   Phase:{comp['phase']} T1:{comp['tier1_clz']} T2:{comp['tier2_clz']} T3:{comp['tier3_technical']}")
    lines.append(f"   L/S:{d.get('ls',0)} BV:{d.get('bv',0)} Fund:{d.get('fund',0)} OI:{d.get('oi',0)} Liq:{d.get('liq',0)}")
    lines.append(f"   BBW:{d.get('bbw',0)} Accum:{d.get('accum',0)} VRet:{d.get('vret',0)} RS:{d.get('rs',0)}")
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
#  🌐  API CLIENTS — FINAL FIXED
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE = "https://api.bitget.com"
    _cache: Dict = {}
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
                if attempt < 2: time.sleep(3)
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
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/candles",
                        params={"symbol": symbol, "productType": "USDT-FUTURES",
                                "granularity": "1H", "limit": limit})
        if not data or data.get("code") != "00000":
            return []
        candles = []
        for row in data.get("data", []):
            try:
                vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                candles.append({"ts": int(row[0]), "open": float(row[1]),
                                 "high": float(row[2]), "low": float(row[3]),
                                 "close": float(row[4]), "volume_usd": vol_usd})
            except Exception:
                continue
        candles.sort(key=lambda x: x["ts"])
        cls._cache[key] = candles
        cls._cache_ts[key] = time.time()
        return candles

    @classmethod
    def get_funding(cls, symbol: str) -> float:
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/current-fund-rate",
                        params={"symbol": symbol, "productType": "USDT-FUTURES"})
        try:
            return float(data["data"][0]["fundingRate"])
        except Exception:
            return 0.0

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()
        cls._cache_ts.clear()


class CoinalyzeClient:
    BASE = "https://api.coinalyze.net/v1"
    _class_last_call: float = 0.0

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._markets_cache: Optional[List[dict]] = None
        self._bn_map: Dict[str, str] = {}
        self._by_map: Dict[str, str] = {}

    def _wait(self):
        elapsed = time.time() - CoinalyzeClient._class_last_call
        wait = CONFIG["coinalyze_rate_limit_wait"] - elapsed
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
                r = requests.get(f"{self.BASE}/{endpoint}", params=p, headers=headers, timeout=15)
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait = int(float(retry_after)) + 1
                        except ValueError:
                            wait = 11
                    else:
                        wait = 11
                    log.warning(f"  Coinalyze rate limit — wait {wait}s")
                    time.sleep(wait)
                    continue
                if r.status_code != 200:
                    log.debug(f"  Coinalyze {endpoint} HTTP {r.status_code}")
                    return None
                data = r.json()
                if isinstance(data, dict) and "error" in data:
                    log.debug(f"  Coinalyze error: {data['error']}")
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
        markets = self._markets_cache
        bn_lookup: Dict[str, str] = {}
        by_ls_lookup: Dict[str, str] = {}
        for m in markets:
            exc = m.get("exchange", "")
            sym_on_exc = m.get("symbol_on_exchange", "")
            clz_sym = m.get("symbol", "")
            is_perp = m.get("is_perpetual", False)
            quote = m.get("quote_asset", "").upper()
            if not (is_perp and quote == "USDT" and clz_sym):
                continue
            if exc == "A":
                bn_lookup[sym_on_exc] = clz_sym
            elif exc == "6" and m.get("has_long_short_ratio_data"):
                by_ls_lookup[sym_on_exc] = clz_sym
        mapped_bn, mapped_by = 0, 0
        for sym in bitget_symbols:
            if sym in bn_lookup:
                self._bn_map[sym] = bn_lookup[sym]
                mapped_bn += 1
            if sym in by_ls_lookup:
                self._by_map[sym] = by_ls_lookup[sym]
                mapped_by += 1
        log.info(f"  Symbol mapping: {mapped_bn}/{len(bitget_symbols)} Binance, "
                 f"{mapped_by}/{len(bitget_symbols)} Bybit L/S")

    def _batch_fetch(self, endpoint: str, symbols: List[str], params: dict) -> Dict[str, list]:
        batch_size = CONFIG["coinalyze_batch_size"]
        result: Dict[str, list] = {}
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            try:
                p = dict(params)
                p["symbols"] = ",".join(batch)
                data = self._get(endpoint, p)
                if data and isinstance(data, list):
                    for item in data:
                        sym = item.get("symbol", "")
                        hist = item.get("history", [])
                        if sym and hist:
                            result[sym] = hist
                elif data and isinstance(data, dict) and "error" in data:
                    log.warning(f"  API error for batch {batch[:3]}...: {data['error']}")
            except Exception as e:
                log.warning(f"  Batch {i//batch_size+1} failed: {e}")
        return result

    def fetch_all_data(self, bitget_symbols: List[str],
                       from_ts: int, to_ts: int) -> Dict[str, ClzData]:
        result: Dict[str, ClzData] = {sym: ClzData() for sym in bitget_symbols}
        bn_syms = [self._bn_map[s] for s in bitget_symbols if s in self._bn_map]
        by_syms = [self._by_map[s] for s in bitget_symbols if s in self._by_map]
        bn_rev = {v: k for k, v in self._bn_map.items()}
        by_rev = {v: k for k, v in self._by_map.items()}
        interval = CONFIG["coinalyze_interval"]
        fund_interval = CONFIG["coinalyze_funding_interval"]
        fund_from = to_ts - CONFIG["coinalyze_funding_lookback_h"] * 3600

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
            # Try alternative interval if 8hour fails
            fund_data = self._batch_fetch("funding-rate-history", bn_syms,
                                          {"interval": fund_interval,
                                           "from": fund_from, "to": to_ts})
            if not fund_data:
                log.warning("  Funding rate history empty, trying '1day' interval...")
                fund_data = self._batch_fetch("funding-rate-history", bn_syms,
                                              {"interval": "1day",
                                               "from": fund_from, "to": to_ts})
            for clz_sym, hist in fund_data.items():
                bitget_sym = bn_rev.get(clz_sym)
                if bitget_sym:
                    result[bitget_sym].funding_hist = hist
            log.info(f"    Got {len(fund_data)} funding histories")
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
    chat_id = CONFIG.get("chat_id")
    if not bot_token or not chat_id:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCANNER LOOP
# ══════════════════════════════════════════════════════════════════════════════
def select_universe(tickers: Dict) -> List[str]:
    vol_min = CONFIG["pre_filter_vol_min"]
    vol_max = CONFIG["pre_filter_vol_max"]
    candidates = []
    for sym, t in tickers.items():
        # Skip stock tokens
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
        lo, hi = n // 10, n * 9 // 10
        candidates = candidates[lo:hi]
    if len(candidates) > CONFIG["max_symbols_per_scan"]:
        random.shuffle(candidates)
        candidates = candidates[:CONFIG["max_symbols_per_scan"]]
    syms = [s for s, _ in candidates]
    log.info(f"  Universe: {len(syms)} symbols (${vol_min/1e6:.0f}M–${vol_max/1e6:.0f}M) [stock tokens blocked]")
    return syms


def main():
    log.info(f"{'═'*70}")
    log.info(f"  PRE-PUMP SCANNER v{VERSION}")
    log.info(f"  Target: Pump ≥15% / 24h | Signal 1-3h sebelumnya")
    log.info(f"  Data: Bitget(price) + Binance+Bybit via Coinalyze (FINAL OPTIMIZED)")
    log.info(f"  Stock token blacklist enabled: {len(CONFIG['stock_token_blacklist'])} symbols blocked")
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

    btc_candles = BitgetClient.get_candles("BTCUSDT", 5)
    btc_chg_1h = 0.0
    if len(btc_candles) >= 3:
        btc_chg_1h = (btc_candles[-2]["close"] - btc_candles[-3]["close"]) / btc_candles[-3]["close"] * 100
    log.info(f"  BTC 1h: {btc_chg_1h:+.2f}%")
    if btc_chg_1h < CONFIG["btc_dump_threshold"]:
        log.warning(f"⛔ BTC CIRCUIT BREAKER: {btc_chg_1h:+.1f}% — scan paused")
        return 0

    log.info("🔍 Step 3: Selecting scan universe...")
    active = select_universe(tickers)
    if not active:
        log.error("❌ No symbols passed universe filter")
        return 1

    log.info("🗺️  Step 4: Building Coinalyze symbol maps...")
    clz.build_symbol_maps(active)

    log.info("📈 Step 5: Fetching Coinalyze multi-exchange data...")
    now_ts = int(time.time())
    from_ts = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
    clz_data = clz.fetch_all_data(active, from_ts, now_ts)

    has_ohlcv = sum(1 for d in clz_data.values() if d.has_ohlcv)
    has_oi    = sum(1 for d in clz_data.values() if d.has_oi)
    has_liq   = sum(1 for d in clz_data.values() if d.has_liq)
    has_fund  = sum(1 for d in clz_data.values() if d.has_funding_hist)
    has_ls    = sum(1 for d in clz_data.values() if d.has_ls)
    log.info(f"  Coverage: OHLCV={has_ohlcv} OI={has_oi} Liq={has_liq} Fund={has_fund} L/S={has_ls}")

    log.info("🎯 Step 6: Scoring...")
    results = []
    for sym in active:
        if is_on_cooldown(sym):
            continue
        try:
            ticker = tickers.get(sym, {})
            price = float(ticker.get("lastPr", 0))
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
                clz=clz_data.get(sym, ClzData()),
            )
            result = score_coin_v14(coin_data)
            if result:
                results.append(result)
                types = "/".join([pt.type_code for pt in result.pump_types])
                src = result.components.get("data_sources", "")
                log.info(f"  ✅ {sym}: {result.score} [{result.phase}] [{types}] {src}")
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
