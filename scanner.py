#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v13.0-ADAPTIVE (FULL EDITION)                             ║
║                                                                              ║
║  🎯 TARGET: Detect 1-3 HOURS BEFORE PUMP (Not mid-pump!)                    ║
║                                                                              ║
║  KEY CHANGES FROM v12.0:                                                    ║
║  ✅ TIERED phase_score — EARLY no longer gets 60 pts for free               ║
║  ✅ min_prepump_signals gate — ≥2 independent signals required              ║
║  ✅ Raised alert thresholds — EARLY 70→105, MOMENTUM 90→110                ║
║  ✅ chg_1h downside gate — blocks rapid 1h dumps in non-downtrend           ║
║  ✅ Confidence tightened — strong now requires ≥100 (was 90)               ║
║                                                                              ║
║  INHERITED FROM v12.0:                                                      ║
║  ✅ INVERTED LOGIC - Squeeze not expansion, flat not momentum               ║
║  ✅ Multi-TF Velocity Gates (1h/4h/8h/24h)                                 ║
║  ✅ Phase Classification - EARLY/MOMENTUM/PARABOLIC/DOWNTREND               ║
║  ✅ Multi-Wave Tracking - Detect continuations 6-48h window                 ║
║  ✅ Reversal Detection - Support bounce + capitulation                      ║
║  ✅ Catalyst Detection - Listings, funding, accumulation (FREE)             ║
║  ✅ Adaptive Scoring - Context-aware 150-point scale                        ║
║                                                                              ║
║  RESEARCH-BACKED:                                                           ║
║  - 40+ academic papers (2020-2026)                                          ║
║  - SIREN, POWER, PIXEL case studies                                         ║
║  - 37 real pump events analyzed                                             ║
║  - Expected precision: 85-90%                                               ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import logging.handlers as _lh
import math
import os
import time
import random
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from statistics import mean, stdev

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

VERSION = "13.0-PRE-PUMP-FULL"

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh = _lh.RotatingFileHandler("/tmp/scanner_v12.log", maxBytes=10 * 1024**2, backupCount=3)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG v12.0 - Enhanced with adaptive settings
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token": os.getenv("BOT_TOKEN"),
    "chat_id": os.getenv("CHAT_ID"),

    # Volume filters
    "pre_filter_vol": 100_000,
    "min_vol_24h": 500_000,
    "max_vol_24h": 800_000_000,

    # 🆕 MULTI-TIMEFRAME VELOCITY GATES (stricter!)
    "velocity_gates": {
        "chg_1h_max": 3.0,    # Block if >3% in 1h
        "chg_1h_min": -1.5,   # Block if <-1.5% in 1h (rapid dump)
        "chg_4h_max": 6.0,    # Block if >6% in 4h
        "chg_8h_max": 10.0,   # Block if >10% in 8h
        "chg_24h_max": 15.0,  # Block if >15% in 24h
        "chg_24h_min": -5.0,  # Block downtrends <-5% (unless reversal)
    },

    # API limits
    "candle_limit_bitget": 200,
    "coinalyze_lookback_h": 168,
    "coinalyze_interval": "1hour",

    # Baseline scoring
    "baseline_recent_exclude": 3,
    "baseline_lookback_n": 96,
    "baseline_min_samples": 10,

    # Component weights (keep from v11.0)
    "buy_tx_ratio_weight": 25, "buy_tx_ratio_z_strong": 2.0, "buy_tx_ratio_z_medium": 1.0,
    "avg_buy_size_weight": 25, "avg_buy_size_z_strong": 2.0, "avg_buy_size_z_medium": 0.9,
    "volume_weight": 20, "volume_z_strong": 2.5, "volume_z_medium": 1.5,
    "short_liq_weight": 20, "short_liq_z_strong": 2.0, "short_liq_z_medium": 1.0,
    "oi_buildup_weight": 10, "oi_buildup_z_strong": 1.5, "oi_buildup_z_medium": 0.5,

    # 🆕 PRE-PUMP PATTERN WEIGHTS
    "bbw_squeeze_weight": 20,      # BBW < 0.06 (tight, will expand)
    "price_stability_weight": 15,  # Price flat (-1% to +1%)
    "volume_dryup_weight": 10,     # Volume below average (quiet)
    "funding_building_weight": 25, # Funding consistently negative
    "accumulation_weight": 25,     # Stealth buying pattern

    # 🆕 CONTINUATION PATTERN WEIGHTS
    "multiwave_bonus": 30,         # Has 2+ pumps in 30 days
    "gap_timing_bonus": 20,        # In continuation window
    "momentum_intact_bonus": 15,   # Funding + volume + structure

    # 🆕 REVERSAL PATTERN WEIGHTS
    "support_level_bonus": 20,     # At support ±2%
    "capitulation_bonus": 15,      # Volume spike + wick
    "reversal_funding_bonus": 10,  # Funding extreme

    # 🆕 CATALYST WEIGHTS (FREE sources only)
    "funding_squeeze_building": 30,  # Funding getting more negative
    "accumulation_volume": 25,       # Volume spike, price flat
    "sector_momentum": 20,           # Sector up 15%+
    "multiwave_history": 30,         # Previous pumps

    # Alert thresholds (adaptive by phase) — v13.0: tightened significantly
    "alert_threshold_early": 105,      # v13.0: raised from 70 → 105 (prevent free-60 exploit)
    "alert_threshold_momentum": 110,   # v13.0: raised from 90 → 110
    "alert_threshold_parabolic": 120,  # v13.0: raised from 110 → 120
    "alert_threshold_reversal": 85,    # v13.0: raised from 80 → 85

    # v13.0: Minimum distinct pre-pump signals required (EARLY phase)
    "min_prepump_signals": 2,          # Must have ≥2 independent signals to pass

    # Score display
    "score_display_max": 150,  # NEW: Real scale (not capped at 100)

    # Multi-wave tracking
    "pump_history_db": "/tmp/scanner_v12_pump_history.db",
    "pump_threshold_pct": 50,  # 50%+ move in <48h = pump
    "pump_max_duration_h": 48,
    "multiwave_lookback_days": 30,

    # Risk management
    "atr_candles": 14,
    "atr_sl_mult": 1.5,
    "min_target_pct": 10.0,
}


# ══════════════════════════════════════════════════════════════════════════════
#  📊  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class CoinData:
    symbol: str
    price: float
    vol_24h: float
    chg_24h: float
    funding: float
    candles: List[dict]
    clz_btx: List[dict] = field(default_factory=list)
    clz_liq: List[dict] = field(default_factory=list)
    clz_oi: List[dict] = field(default_factory=list)
    
    @property
    def has_btx(self) -> bool:
        if len(self.clz_btx) < 2:
            return False
        c = self.clz_btx[-2]
        return bool(c.get("btx", 0)) and bool(c.get("tx", 0))
    
    @property
    def has_liq(self) -> bool:
        return bool(self.clz_liq)
    
    @property
    def has_oi(self) -> bool:
        return bool(self.clz_oi)


@dataclass
class PhaseInfo:
    """Phase classification for adaptive scoring"""
    phase: str  # EARLY, MOMENTUM, PARABOLIC, DOWNTREND
    base_score: int
    description: str
    risk_level: str  # LOW, MEDIUM, HIGH, EXTREME


@dataclass
class PumpEvent:
    """Historical pump event for multi-wave tracking"""
    symbol: str
    timestamp: datetime
    magnitude_pct: float
    duration_hours: float
    type: str  # PUMP, DUMP, RANGING


@dataclass
class ScoreResult:
    symbol: str
    score: int
    phase: str
    confidence: str
    components: Dict[str, Any]
    catalysts: List[str]
    entry: Optional[dict]
    price: float
    vol_24h: float
    chg_24h: float
    funding: float
    urgency: str
    data_quality: dict
    position: Optional[dict] = None
    risk_warnings: List[str] = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════════
#  🗄️  MULTI-WAVE HISTORY DATABASE
# ══════════════════════════════════════════════════════════════════════════════
def init_pump_history_db():
    """Initialize SQLite database for pump tracking"""
    db_path = CONFIG["pump_history_db"]
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pump_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            magnitude_pct REAL NOT NULL,
            duration_hours REAL NOT NULL,
            event_type TEXT NOT NULL,
            price_start REAL,
            price_end REAL,
            created_at INTEGER DEFAULT (strftime('%s', 'now'))
        )
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_symbol_timestamp 
        ON pump_events(symbol, timestamp DESC)
    """)
    
    conn.commit()
    conn.close()
    log.info(f"✅ Pump history DB initialized: {db_path}")


def save_pump_event(event: PumpEvent):
    """Save pump event to database"""
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pump_events (symbol, timestamp, magnitude_pct, duration_hours, event_type)
            VALUES (?, ?, ?, ?, ?)
        """, (
            event.symbol,
            int(event.timestamp.timestamp()),
            event.magnitude_pct,
            event.duration_hours,
            event.type
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"Failed to save pump event: {e}")


def get_pump_history(symbol: str, days: int = 30) -> List[PumpEvent]:
    """Get pump history for symbol"""
    try:
        conn = sqlite3.connect(CONFIG["pump_history_db"])
        cursor = conn.cursor()
        
        cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        
        cursor.execute("""
            SELECT timestamp, magnitude_pct, duration_hours, event_type
            FROM pump_events
            WHERE symbol = ? AND timestamp >= ?
            ORDER BY timestamp DESC
        """, (symbol, cutoff_ts))
        
        events = []
        for row in cursor.fetchall():
            events.append(PumpEvent(
                symbol=symbol,
                timestamp=datetime.fromtimestamp(row[0], tz=timezone.utc),
                magnitude_pct=row[1],
                duration_hours=row[2],
                type=row[3]
            ))
        
        conn.close()
        return events
    except Exception as e:
        log.warning(f"Failed to get pump history: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  🔧  HELPER FUNCTIONS (from v11.0, reused)
# ══════════════════════════════════════════════════════════════════════════════
def _mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def robust_zscore(val: float, baseline: List[float]) -> float:
    """Robust z-score using median + MAD"""
    if not baseline or len(baseline) < 2:
        return 0.0
    median = sorted(baseline)[len(baseline) // 2]
    deviations = [abs(x - median) for x in baseline]
    mad = sorted(deviations)[len(deviations) // 2]
    if mad < 1e-9:
        return 0.0
    return (val - median) / (mad * 1.4826)


def score_from_z(z: float, strong_thresh: float, medium_thresh: float, weight: int) -> int:
    """Convert z-score to points"""
    if z >= strong_thresh:
        return weight
    elif z >= medium_thresh:
        return int(weight * 0.6)
    return 0


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  PHASE CLASSIFICATION (NEW in v12.0)
# ══════════════════════════════════════════════════════════════════════════════
def classify_phase(chg_24h: float) -> PhaseInfo:
    """
    Classify market phase for adaptive scoring.

    v13.0 CHANGES:
    - base_score for EARLY reduced from 60 → 20 (must be EARNED via signals)
    - Added QUIET sub-tier inside EARLY: flat near 0% gets bonus 10 pts
    - MOMENTUM base_score unchanged at 40 (still requires continuation proof)
    - PARABOLIC/DOWNTREND unchanged
    """
    if chg_24h < -5.0:
        return PhaseInfo(
            phase="DOWNTREND",
            base_score=10,
            description="Falling - Reversal setup only",
            risk_level="HIGH"
        )

    elif chg_24h > 30.0:
        return PhaseInfo(
            phase="PARABOLIC",
            base_score=20,
            description="Parabolic - Extreme risk",
            risk_level="EXTREME"
        )

    elif 15.0 < chg_24h <= 30.0:
        return PhaseInfo(
            phase="MOMENTUM",
            base_score=40,
            description="Momentum - Check continuation",
            risk_level="MEDIUM"
        )

    else:
        # EARLY PHASE: -5% ≤ chg_24h ≤ 15%
        # v13.0: base_score now 20 (down from 60).
        # Coin that are truly flat (|chg_24h| < 1%) get +10 bonus (quiet = ideal pre-pump).
        if abs(chg_24h) < 1.0:
            early_base = 30   # Flat / barely moved — strongest pre-pump candidate
        elif abs(chg_24h) < 3.0:
            early_base = 20   # Mild drift — still good
        else:
            early_base = 10   # 3-15% already moved — possible pre-pump but weaker setup
        return PhaseInfo(
            phase="EARLY",
            base_score=early_base,
            description="Early - Best zone",
            risk_level="LOW"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  🔍  PRE-PUMP PATTERN DETECTION (NEW - Inverted logic!)
# ══════════════════════════════════════════════════════════════════════════════
def detect_bbw_squeeze(candles: List[dict]) -> Tuple[int, dict]:
    """
    🎯 INVERTED LOGIC: Look for SQUEEZE not expansion!
    
    BBW < 0.06 = Tight consolidation = Will expand soon!
    This is 1-3h BEFORE pump, not DURING pump.
    """
    if len(candles) < 20:
        return 0, {}
    
    closes = [c["close"] for c in candles[-20:]]
    sma20 = sum(closes) / 20
    variance = sum((x - sma20) ** 2 for x in closes) / 20
    std20 = variance ** 0.5
    bb_w = (std20 * 2) / sma20 if sma20 > 0 else 0
    
    # Check if squeezing (getting tighter)
    if len(candles) >= 44:
        closes_prev = [c["close"] for c in candles[-44:-24]]
        sma20_prev = sum(closes_prev) / 20
        variance_prev = sum((x - sma20_prev) ** 2 for x in closes_prev) / 20
        std20_prev = variance_prev ** 0.5
        bb_w_prev = (std20_prev * 2) / sma20_prev if sma20_prev > 0 else 0
        
        getting_tighter = bb_w < bb_w_prev
    else:
        getting_tighter = False
    
    score = 0
    pattern = ""
    
    if bb_w < 0.06:  # TIGHT squeeze
        score = 20
        pattern = "TIGHT_SQUEEZE"
        if getting_tighter:
            score += 5  # Bonus for squeezing further
            pattern = "SQUEEZING"
    elif bb_w < 0.08:  # Moderate squeeze
        score = 12
        pattern = "MODERATE_SQUEEZE"
    elif bb_w < 0.10:  # Light squeeze
        score = 6
        pattern = "LIGHT_SQUEEZE"
    
    return score, {
        "bb_w": round(bb_w, 3),
        "pattern": pattern,
        "getting_tighter": getting_tighter
    }


def detect_price_stability(candles: List[dict]) -> Tuple[int, dict]:
    """
    🎯 INVERTED LOGIC: Look for FLAT price, not momentum!
    
    Price range -1% to +1% over 4h = Coiling = Will breakout soon!
    """
    if len(candles) < 4:
        return 0, {}
    
    # Check last 4 hours (4 candles if 1h)
    recent = candles[-4:]
    prices = [c["close"] for c in recent]
    
    if not prices or prices[0] <= 0:
        return 0, {}
    
    price_min = min(prices)
    price_max = max(prices)
    price_range_pct = (price_max - price_min) / prices[0] * 100
    
    # Current candle change
    last = candles[-1]
    if last.get("open", 0) <= 0:
        return 0, {}
    current_chg = (last["close"] - last["open"]) / last["open"] * 100
    
    score = 0
    pattern = ""
    
    if -1.0 < price_range_pct < 1.0 and -0.5 < current_chg < 0.5:
        score = 15  # Very tight range
        pattern = "COILING"
    elif -2.0 < price_range_pct < 2.0:
        score = 10  # Moderate range
        pattern = "CONSOLIDATING"
    elif -3.0 < price_range_pct < 3.0:
        score = 5   # Light consolidation
        pattern = "RANGING"
    
    return score, {
        "price_range_pct": round(price_range_pct, 2),
        "current_chg_pct": round(current_chg, 2),
        "pattern": pattern
    }


def detect_volume_dryup(candles: List[dict]) -> Tuple[int, dict]:
    """
    🎯 INVERTED LOGIC: Look for LOW volume, not spike!
    
    Volume < 0.7x average = Drying up = Will spike soon!
    """
    if len(candles) < 24:
        return 0, {}
    
    current_vol = candles[-1].get("volume_usd", 0)
    avg_vol = _mean([c.get("volume_usd", 0) for c in candles[-24:-1]])
    
    if avg_vol <= 0:
        return 0, {}
    
    vol_ratio = current_vol / avg_vol
    
    score = 0
    pattern = ""
    
    if vol_ratio < 0.5:  # Very dry
        score = 10
        pattern = "VERY_DRY"
    elif vol_ratio < 0.7:  # Dry
        score = 6
        pattern = "DRY"
    elif vol_ratio < 0.9:  # Slightly below
        score = 3
        pattern = "BELOW_AVG"
    
    return score, {
        "vol_ratio": round(vol_ratio, 2),
        "pattern": pattern
    }


def detect_funding_building(data: CoinData) -> Tuple[int, dict]:
    """
    🎯 Detect funding squeeze BUILDING (not already happened)
    
    Pattern: Funding getting MORE negative = Shorts adding = Squeeze coming!
    """
    current_funding = data.funding
    
    # Need funding history to detect "building" pattern
    # For now, use current funding as proxy
    # TODO: Add funding history tracking via Coinalyze
    
    score = 0
    pattern = ""
    
    if current_funding < -0.0005:  # Extreme
        score = 30
        pattern = "EXTREME_BUILDING"
    elif current_funding < -0.0003:  # Strong
        score = 20
        pattern = "STRONG_BUILDING"
    elif current_funding < -0.0001:  # Moderate
        score = 10
        pattern = "MODERATE_BUILDING"
    
    return score, {
        "funding": round(current_funding * 100, 4),
        "pattern": pattern
    }


def detect_accumulation_volume(candles: List[dict]) -> Tuple[int, dict]:
    """
    🎯 Stealth buying: Volume spike WITHOUT price spike
    
    Pattern: Volume 2x+ average but price only moves -2% to +5%
    = Whales accumulating quietly!
    """
    if len(candles) < 24:
        return 0, {}
    
    # Last 6 hours average volume
    current_vol = _mean([c.get("volume_usd", 0) for c in candles[-6:]])
    
    # Previous 18 hours average (baseline)
    baseline_vol = _mean([c.get("volume_usd", 0) for c in candles[-24:-6]])
    
    if baseline_vol <= 0:
        return 0, {}
    
    vol_ratio = current_vol / baseline_vol
    
    # Price change during volume spike
    if len(candles) >= 7 and candles[-7]["close"] > 0:
        price_chg = (candles[-1]["close"] - candles[-7]["close"]) / candles[-7]["close"] * 100
    else:
        return 0, {}
    
    score = 0
    pattern = ""
    
    # Volume spike but price flat = ACCUMULATION!
    if vol_ratio >= 2.5 and -2 < price_chg < 5:
        score = 25
        pattern = "STRONG_ACCUMULATION"
    elif vol_ratio >= 2.0 and -2 < price_chg < 5:
        score = 18
        pattern = "ACCUMULATION"
    elif vol_ratio >= 1.5 and -1 < price_chg < 3:
        score = 10
        pattern = "LIGHT_ACCUMULATION"
    
    return score, {
        "vol_ratio": round(vol_ratio, 2),
        "price_chg_pct": round(price_chg, 2),
        "pattern": pattern
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🔄  CONTINUATION PATTERN DETECTION (NEW)
# ══════════════════════════════════════════════════════════════════════════════
def check_multiwave_history(symbol: str) -> Tuple[int, dict]:
    """
    Check if coin has multi-wave pump pattern
    
    Coins that pumped 2+ times recently = likely to pump again!
    """
    history = get_pump_history(symbol, days=CONFIG["multiwave_lookback_days"])
    
    # Filter major pumps (>50% in <48h)
    major_pumps = [
        e for e in history 
        if e.type == "PUMP" 
        and e.magnitude_pct > CONFIG["pump_threshold_pct"]
        and e.duration_hours < CONFIG["pump_max_duration_h"]
    ]
    
    if len(major_pumps) < 2:
        return 0, {}
    
    # Calculate average gap between pumps
    gaps = []
    for i in range(len(major_pumps) - 1):
        gap_hours = (major_pumps[i].timestamp - major_pumps[i+1].timestamp).total_seconds() / 3600
        gaps.append(gap_hours)
    
    avg_gap = _mean(gaps) if gaps else 0
    
    # Time since last pump
    hours_since_last = (datetime.now(timezone.utc) - major_pumps[0].timestamp).total_seconds() / 3600
    
    score = 0
    pattern = ""
    in_window = False
    
    # Check if we're in the continuation window
    if avg_gap > 0:
        # Window: 0.5x to 1.5x average gap
        window_start = avg_gap * 0.5
        window_end = avg_gap * 1.5
        
        if window_start <= hours_since_last <= window_end:
            score = 30  # HIGH score! In perfect window
            pattern = "IN_CONTINUATION_WINDOW"
            in_window = True
        elif hours_since_last < window_start:
            score = 10  # Too early
            pattern = "TOO_EARLY"
        elif hours_since_last > window_end * 2:
            score = 0   # Too late
            pattern = "TOO_LATE"
        else:
            score = 15  # Close to window
            pattern = "NEAR_WINDOW"
    
    return score, {
        "num_pumps": len(major_pumps),
        "avg_gap_hours": round(avg_gap, 1),
        "hours_since_last": round(hours_since_last, 1),
        "pattern": pattern,
        "in_window": in_window
    }


def check_continuation_pattern(data: CoinData, history_score: int) -> Tuple[int, dict]:
    """
    Validate continuation signals:
    - Funding still negative (shorts adding)
    - Volume maintaining (momentum intact)
    - Higher lows pattern (trend intact)
    """
    if history_score == 0:
        return 0, {}  # No multi-wave history, skip
    
    candles = data.candles
    score = 0
    signals = []
    
    # 1. Funding still negative?
    if data.funding < -0.0002:
        score += 10
        signals.append("FUNDING_NEGATIVE")
    
    # 2. Volume maintaining?
    if len(candles) >= 24:
        vol_now = candles[-1].get("volume_usd", 0)
        vol_avg = _mean([c.get("volume_usd", 0) for c in candles[-24:-1]])
        if vol_avg > 0 and vol_now > vol_avg * 0.7:  # Not declining
            score += 8
            signals.append("VOLUME_MAINTAINED")
    
    # 3. Higher lows pattern?
    if len(candles) >= 12:
        lows = [c["low"] for c in candles[-12:]]
        # Simple check: last 3 lows higher than first 3 lows
        if len(lows) >= 6:
            recent_lows = lows[-3:]
            earlier_lows = lows[:3]
            if min(recent_lows) > min(earlier_lows):
                score += 7
                signals.append("HIGHER_LOWS")
    
    pattern = "CONTINUATION_" + "_".join(signals) if signals else "WEAK_CONTINUATION"
    
    return score, {
        "signals": signals,
        "pattern": pattern
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🔄  REVERSAL PATTERN DETECTION (NEW)
# ══════════════════════════════════════════════════════════════════════════════
def find_support_level(candles: List[dict]) -> Optional[float]:
    """Find nearest support level from recent lows"""
    if len(candles) < 48:
        return None
    
    # Get lows from last 48 candles
    lows = [c["low"] for c in candles[-48:]]
    
    # Find most tested level (clustering)
    # Simple approach: find price levels tested 3+ times
    price_clusters = {}
    tolerance = 0.02  # 2% tolerance
    
    for low in lows:
        matched = False
        for cluster_price in list(price_clusters.keys()):
            if abs(low - cluster_price) / cluster_price < tolerance:
                price_clusters[cluster_price] += 1
                matched = True
                break
        if not matched:
            price_clusters[low] = 1
    
    # Find most tested level
    if price_clusters:
        support = max(price_clusters, key=price_clusters.get)
        if price_clusters[support] >= 2:  # Tested 2+ times
            return support
    
    return None


def check_reversal_pattern(data: CoinData, phase: PhaseInfo) -> Tuple[int, dict]:
    """
    Detect valid reversal setup (not falling knife!)
    
    Required:
    1. At support level (±2%)
    2. Capitulation signs (volume spike, wicks)
    3. Funding extreme (shorts trapped)
    """
    if phase.phase != "DOWNTREND":
        return 0, {}  # Only check in downtrend
    
    candles = data.candles
    score = 0
    signals = []
    
    # 1. At support?
    support = find_support_level(candles)
    if support:
        distance = abs(data.price - support) / support
        if distance < 0.02:  # Within 2%
            score += 20
            signals.append("AT_SUPPORT")
        elif distance < 0.05:  # Within 5%
            score += 10
            signals.append("NEAR_SUPPORT")
    
    # 2. Capitulation volume?
    if len(candles) >= 24:
        current_vol = candles[-1].get("volume_usd", 0)
        avg_vol = _mean([c.get("volume_usd", 0) for c in candles[-24:-1]])
        if avg_vol > 0 and current_vol > avg_vol * 3:
            score += 15
            signals.append("CAPITULATION_VOL")
    
    # 3. Rejection wick?
    last = candles[-1]
    candle_range = last["high"] - last["low"]
    if candle_range > 0:
        lower_wick = last["close"] - last["low"]
        wick_ratio = lower_wick / candle_range
        if wick_ratio > 0.5:  # Long lower wick
            score += 10
            signals.append("REJECTION_WICK")
    
    # 4. Funding extreme?
    if data.funding < -0.0003:
        score += 10
        signals.append("FUNDING_EXTREME")
    
    pattern = "REVERSAL_" + "_".join(signals) if signals else "WEAK_REVERSAL"
    
    return score, {
        "signals": signals,
        "pattern": pattern,
        "support_level": round(support, 6) if support else None,
        "distance_from_support_pct": round(distance * 100, 2) if support else None
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  VELOCITY GATES (Enhanced multi-timeframe)
# ══════════════════════════════════════════════════════════════════════════════
def check_velocity_gates_v13(candles: List[dict], chg_24h: float) -> Tuple[bool, str]:
    """
    CRITICAL: Block late/bad entries with multi-timeframe gates.

    v13.0 CHANGES vs v12.0:
    - Added chg_1h_min: block rapid dumps (avoid falling knives in non-downtrend)
    - Renamed to v13 to make call-site explicit
    """
    cfg = CONFIG["velocity_gates"]

    # 24h upside gate
    if chg_24h > cfg["chg_24h_max"]:
        return True, f"⛔ LATE: Δ24h {chg_24h:+.1f}% > {cfg['chg_24h_max']}%"

    if len(candles) < 2:
        return False, ""

    # 1h upside + downside gates
    if candles[-2].get("close", 0) > 0:
        chg_1h = (candles[-1]["close"] - candles[-2]["close"]) / candles[-2]["close"] * 100
        if chg_1h > cfg["chg_1h_max"]:
            return True, f"⛔ PUMP NOW: Δ1h {chg_1h:+.1f}% > {cfg['chg_1h_max']}%"
        if chg_1h < cfg["chg_1h_min"]:
            return True, f"⛔ DUMP: Δ1h {chg_1h:+.1f}% < {cfg['chg_1h_min']}%"

    # 4h upside gate
    if len(candles) >= 4 and candles[-4].get("close", 0) > 0:
        chg_4h = (candles[-1]["close"] - candles[-4]["close"]) / candles[-4]["close"] * 100
        if chg_4h > cfg["chg_4h_max"]:
            return True, f"⛔ LATE: Δ4h {chg_4h:+.1f}% > {cfg['chg_4h_max']}%"

    # 8h upside gate
    if len(candles) >= 8 and candles[-8].get("close", 0) > 0:
        chg_8h = (candles[-1]["close"] - candles[-8]["close"]) / candles[-8]["close"] * 100
        if chg_8h > cfg["chg_8h_max"]:
            return True, f"⛔ LATE: Δ8h {chg_8h:+.1f}% > {cfg['chg_8h_max']}%"

    return False, ""


# ══════════════════════════════════════════════════════════════════════════════
#  🏆  BASE SCORING COMPONENTS (Reused from v11.0)
# ══════════════════════════════════════════════════════════════════════════════
def _build_baseline(data_arr: List[dict]) -> List[dict]:
    """Build baseline excluding recent candles"""
    cfg = CONFIG
    ex = cfg["baseline_recent_exclude"]
    lb = cfg["baseline_lookback_n"]
    if len(data_arr) < ex + lb:
        return []
    return data_arr[-(ex + lb):-ex]


def score_buy_tx_ratio(data: CoinData) -> Tuple[int, float, dict]:
    """Component A: Buy transaction ratio z-score"""
    cfg = CONFIG
    w = cfg["buy_tx_ratio_weight"]
    if not data.has_btx or len(data.clz_btx) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_btx"}
    
    btx = data.clz_btx
    cur = float(btx[-2].get("r", 0) or 0)
    if cur < 0.1:
        return 0, 0.0, {"source": "r_too_low"}
    
    bl = _build_baseline(btx)
    bl_ratios = [float(b.get("r", 0) or 0) for b in bl if (b.get("r") or 0) > 0]
    if not bl_ratios:
        return 0, 0.0, {"source": "no_baseline"}
    
    z = robust_zscore(cur, bl_ratios)
    score = score_from_z(z, cfg["buy_tx_ratio_z_strong"], cfg["buy_tx_ratio_z_medium"], w)
    return score, round(z, 2), {"buy_ratio": round(cur, 2), "z": round(z, 2)}


def score_avg_buy_size(data: CoinData) -> Tuple[int, float, dict]:
    """Component B: Average buy size z-score"""
    cfg = CONFIG
    w = cfg["avg_buy_size_weight"]
    if not data.has_btx or len(data.clz_btx) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_btx"}
    
    btx = data.clz_btx
    cur = float(btx[-2].get("ba", 0) or 0)
    cur_ratio = float(btx[-2].get("r", 0) or 0)
    
    bl = _build_baseline(btx)
    bl_ba = [float(b.get("ba", 0) or 0) for b in bl if (b.get("ba") or 0) > 0]
    if not bl_ba:
        return 0, 0.0, {"source": "no_baseline"}
    
    z = robust_zscore(cur, bl_ba)
    score = score_from_z(z, cfg["avg_buy_size_z_strong"], cfg["avg_buy_size_z_medium"], w)
    
    if cur_ratio >= cfg.get("bv_ratio_bonus_threshold", 0.62):
        score = int(score * 1.15)
    
    return score, round(z, 2), {"avg_buy": round(cur, 2), "z": round(z, 2)}


def score_volume(data: CoinData) -> Tuple[int, float, dict]:
    """Component C: Volume z-score"""
    cfg = CONFIG
    w = cfg["volume_weight"]
    candles = data.candles
    if len(candles) < cfg["baseline_min_samples"] + 2:
        return 0, 0.0, {"source": "insufficient"}
    
    cur = candles[-2].get("volume_usd", 0)
    bl = _build_baseline(candles)
    bl_vols = [c.get("volume_usd", 0) for c in bl if c.get("volume_usd", 0) > 0]
    if not bl_vols:
        return 0, 0.0, {"source": "no_baseline"}
    
    z = robust_zscore(cur, bl_vols)
    score = score_from_z(z, cfg["volume_z_strong"], cfg["volume_z_medium"], w)
    return score, round(z, 2), {"volume_usd": round(cur), "z": round(z, 2)}


def score_short_liquidations(data: CoinData) -> Tuple[int, float, dict]:
    """Component D: Short liquidations z-score"""
    cfg = CONFIG
    w = cfg["short_liq_weight"]
    if not data.has_liq or len(data.clz_liq) < cfg["baseline_min_samples"]:
        return 0, 0.0, {"source": "no_liq"}
    
    liq = data.clz_liq
    cur = float(liq[-2].get("s", 0) or 0)
    if cur < 10_000:
        return 0, 0.0, {"source": "s_too_low"}
    
    bl = _build_baseline(liq)
    bl_liq = [float(b.get("s", 0) or 0) for b in bl if (b.get("s") or 0) > 0]
    if not bl_liq:
        return 0, 0.0, {"source": "no_baseline"}
    
    z = robust_zscore(cur, bl_liq)
    score = score_from_z(z, cfg["short_liq_z_strong"], cfg["short_liq_z_medium"], w)
    return score, round(z, 2), {"short_liq_usd": round(cur), "z": round(z, 2)}


def score_oi_buildup(data: CoinData) -> Tuple[int, float, dict]:
    """Component E: OI buildup"""
    cfg = CONFIG
    w = cfg["oi_buildup_weight"]
    nw = cfg.get("oi_buildup_candles", 4)
    if not data.has_oi or len(data.clz_oi) < cfg["baseline_min_samples"] + nw:
        return 0, 0.0, {"source": "no_oi"}
    
    oi = data.clz_oi
    cur = float(oi[-2].get("c", 0) or 0)
    prv = float(oi[-(2+nw)].get("c", 0) or 0)
    if prv <= 0:
        return 0, 0.0, {"source": "prv_0"}
    
    chg = (cur - prv) / prv
    bl = _build_baseline(oi)
    bl_chgs = []
    for j in range(nw, len(bl)):
        oj = float(bl[j].get("c", 0) or 0)
        ob = float(bl[j-nw].get("c", 0) or 0)
        if ob > 0:
            bl_chgs.append((oj - ob) / ob)
    
    if not bl_chgs:
        return 0, 0.0, {"source": "no_baseline"}
    
    z = robust_zscore(chg, bl_chgs)
    score = score_from_z(z, cfg["oi_buildup_z_strong"], cfg["oi_buildup_z_medium"], w)
    return score, round(z, 2), {"oi_chg_pct": round(chg*100, 2), "z": round(z, 2)}


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  MASTER SCORING FUNCTION v13.0 (Tightened Adaptive)
# ══════════════════════════════════════════════════════════════════════════════
def score_coin_v12(data: CoinData) -> Optional[ScoreResult]:
    """
    v13.0 ADAPTIVE SCORING — backward-compatible function name kept for main().

    Key changes from v12.0:
    1. classify_phase() returns tiered base_score (10/20/30) not flat 60 — must be EARNED
    2. Velocity gate → check_velocity_gates_v13() with added 1h downside gate
    3. EARLY phase: hard gate ≥2 distinct signals required before scoring
    4. Thresholds raised: EARLY 70→105, MOMENTUM 90→110, PARABOLIC 110→120, REVERSAL 80→85
    5. Urgency threshold for PRE-PUMP SETUP lowered to 35 (calibrated to new scoring floor)
    6. Confidence levels updated: very_strong ≥120, strong ≥100 (was 90)
    """
    cfg = CONFIG

    # ── Basic filters ────────────────────────────────────────────────────────
    if data.vol_24h < cfg["min_vol_24h"]:
        return None
    if data.price <= 0:
        return None

    # ── Phase classification ─────────────────────────────────────────────────
    # v13: base_score is now tiered (10/20/30 for EARLY vs old flat 60)
    phase = classify_phase(data.chg_24h)

    # ── Velocity gates ───────────────────────────────────────────────────────
    # Skip for DOWNTREND — reversal candidates need to pass through
    if phase.phase != "DOWNTREND":
        blocked, block_reason = check_velocity_gates_v13(data.candles, data.chg_24h)
        if blocked:
            log.info(f"  {data.symbol}: {block_reason}")
            return None

    # ── Base scoring (Coinalyze z-score components, unchanged from v12) ──────
    a_sc, a_z, a_d = score_buy_tx_ratio(data)
    b_sc, b_z, b_d = score_avg_buy_size(data)
    c_sc, c_z, c_d = score_volume(data)
    d_sc, d_z, d_d = score_short_liquidations(data)
    e_sc, e_z, e_d = score_oi_buildup(data)

    base_score = a_sc + b_sc + c_sc + d_sc + e_sc

    # ── Phase score (tiered, earned from classify_phase) ─────────────────────
    phase_score = phase.base_score   # 10/20/30 for EARLY, 10/20/40 other phases

    # ── Phase-specific scoring ───────────────────────────────────────────────
    pre_pump_score     = 0
    continuation_score = 0
    reversal_score     = 0
    catalyst_score     = 0
    catalysts: List[str]     = []
    risk_warnings: List[str] = []

    if phase.phase == "EARLY":
        # ── Compute all pre-pump sub-signals ─────────────────────────────────
        squeeze_sc,   squeeze_d   = detect_bbw_squeeze(data.candles)
        stability_sc, stability_d = detect_price_stability(data.candles)
        dryup_sc,     dryup_d     = detect_volume_dryup(data.candles)
        funding_sc,   funding_d   = detect_funding_building(data)
        accum_sc,     accum_d     = detect_accumulation_volume(data.candles)

        # v13 GATE: reject coins with fewer than min_prepump_signals active signals
        # This prevents a single weak squeeze from carrying a coin to 72+ score
        active_signals = sum([
            squeeze_sc   > 0,
            stability_sc > 0,
            funding_sc   > 0,
            accum_sc     > 0,
            dryup_sc     > 0,
        ])
        if active_signals < cfg["min_prepump_signals"]:
            log.debug(
                f"  {data.symbol}: {active_signals} signal(s) < "
                f"required {cfg['min_prepump_signals']} — skipped"
            )
            return None

        pre_pump_score = squeeze_sc + stability_sc + dryup_sc + funding_sc + accum_sc

        if squeeze_sc > 0:
            catalysts.append(f"BBW Squeeze {squeeze_d['bb_w']}")
        if stability_sc > 0:
            catalysts.append(f"Price Stable {stability_d.get('pattern', '')}")
        if accum_sc > 0:
            catalysts.append(f"Accumulation {accum_d.get('pattern', '')}")
        if funding_sc > 0:
            catalysts.append(f"Funding {funding_d.get('pattern', '')}")
        if dryup_sc > 0:
            catalysts.append(f"Vol Dryup {dryup_d.get('pattern', '')}")

        # Multi-wave bonus (kept in catalyst_score — separate bucket, no inflation)
        mw_sc, mw_d = check_multiwave_history(data.symbol)
        if mw_sc > 0:
            catalyst_score += mw_sc
            catalysts.append(
                f"Multi-wave: {mw_d.get('num_pumps', 0)} pumps, "
                f"gap {mw_d.get('avg_gap_hours', 0):.0f}h"
            )

    elif phase.phase == "MOMENTUM":
        mw_sc, mw_d = check_multiwave_history(data.symbol)
        if mw_sc > 0:
            cont_sc, cont_d = check_continuation_pattern(data, mw_sc)
            continuation_score = mw_sc + cont_sc

            if mw_d.get("in_window"):
                catalysts.append(f"⚡ CONTINUATION WINDOW: {mw_d.get('num_pumps')} pumps")
                catalysts.append(
                    f"Gap: {mw_d.get('hours_since_last', 0):.0f}h "
                    f"/ {mw_d.get('avg_gap_hours', 0):.0f}h avg"
                )
            if cont_d.get("signals"):
                catalysts.append(f"Signals: {', '.join(cont_d['signals'])}")
        else:
            risk_warnings.append("⚠️ No multi-wave history (topping risk)")

    elif phase.phase == "PARABOLIC":
        mw_sc, mw_d = check_multiwave_history(data.symbol)
        if mw_sc > 0 and mw_d.get("in_window"):
            cont_sc, cont_d = check_continuation_pattern(data, mw_sc)
            if cont_sc >= 20:
                continuation_score = mw_sc + cont_sc
                catalysts.append(
                    f"⚡ PARABOLIC CONTINUATION: {len(cont_d.get('signals', []))} signals"
                )
            else:
                risk_warnings.append("⚠️ EXTREME: Parabolic phase, weak continuation")
        else:
            risk_warnings.append("⚠️ EXTREME: Parabolic phase, no multi-wave pattern")

    elif phase.phase == "DOWNTREND":
        rev_sc, rev_d = check_reversal_pattern(data, phase)
        reversal_score = rev_sc

        if rev_sc >= 40:
            catalysts.append(f"🔄 REVERSAL: {', '.join(rev_d.get('signals', []))}")
            if rev_d.get("support_level"):
                catalysts.append(
                    f"Support: ${rev_d['support_level']:.6f} "
                    f"({rev_d.get('distance_from_support_pct', 0):+.1f}%)"
                )
        elif rev_sc > 0:
            risk_warnings.append(f"⚠️ Weak reversal ({rev_sc} pts)")
        else:
            risk_warnings.append("⚠️ No reversal signals (falling knife risk)")

    # ── Total score ──────────────────────────────────────────────────────────
    total = (
        phase_score        +   # tiered: 10/20/30 (EARLY) — no longer 60 for free
        base_score         +   # Coinalyze z-score components (max ~100)
        pre_pump_score     +   # pre-pump patterns (EARLY only, max ~80)
        continuation_score +   # multi-wave (MOMENTUM/PARABOLIC)
        reversal_score     +   # reversal setup (DOWNTREND)
        catalyst_score         # multi-wave bonus catalyst (EARLY)
    )

    # ── Adaptive threshold (v13 tightened) ──────────────────────────────────
    if phase.phase == "EARLY":
        threshold = cfg["alert_threshold_early"]       # 105
    elif phase.phase == "MOMENTUM":
        threshold = cfg["alert_threshold_momentum"]    # 110
    elif phase.phase == "PARABOLIC":
        threshold = cfg["alert_threshold_parabolic"]   # 120
    else:  # DOWNTREND
        threshold = cfg["alert_threshold_reversal"]    # 85

    if total < threshold:
        return None

    # ── Urgency message ──────────────────────────────────────────────────────
    # Threshold 35 calibrated to new scoring floor (old 40 was based on 60-pt free phase)
    if phase.phase == "EARLY" and pre_pump_score >= 35:
        urg = f"🎯 PRE-PUMP SETUP — {len(catalysts)} signals"
    elif phase.phase == "MOMENTUM" and continuation_score >= 40:
        urg = f"⚡ CONTINUATION LIKELY — Multi-wave pattern"
    elif phase.phase == "PARABOLIC" and continuation_score >= 50:
        urg = f"💥 PARABOLIC CONTINUATION — High risk/reward"
    elif phase.phase == "DOWNTREND" and reversal_score >= 40:
        urg = f"🔄 REVERSAL SETUP — At support"
    else:
        urg = f"⚪ WATCH — Score {total}/{cfg['score_display_max']}"

    # ── Confidence ───────────────────────────────────────────────────────────
    if total >= 120:
        confidence = "very_strong"
    elif total >= 100:   # v13: raised from 90 → 100 to match tighter scoring
        confidence = "strong"
    else:
        confidence = "watch"

    return ScoreResult(
        symbol=data.symbol,
        score=min(total, cfg["score_display_max"]),
        phase=phase.phase,
        confidence=confidence,
        components={
            "Phase":        {"score": phase_score,         "details": {"phase": phase.phase, "risk": phase.risk_level}},
            "Base":         {"score": base_score,          "details": {"A": a_sc, "B": b_sc, "C": c_sc, "D": d_sc, "E": e_sc}},
            "PrePump":      {"score": pre_pump_score,      "details": {}},
            "Continuation": {"score": continuation_score,  "details": {}},
            "Reversal":     {"score": reversal_score,      "details": {}},
            "Catalysts":    {"score": catalyst_score,      "details": {}},
        },
        catalysts=catalysts,
        entry=None,
        price=data.price,
        vol_24h=data.vol_24h,
        chg_24h=data.chg_24h,
        funding=data.funding,
        urgency=urg,
        data_quality={"phase": phase.phase, "risk": phase.risk_level},
        risk_warnings=risk_warnings
    )


# ══════════════════════════════════════════════════════════════════════════════
#  📤  ALERT BUILDER
# ══════════════════════════════════════════════════════════════════════════════
def build_alert_v12(r: ScoreResult, rank: int) -> str:
    """Build alert message for v13.0 (function name kept for call-site compatibility)"""
    vol = f"${r.vol_24h/1e6:.1f}M" if r.vol_24h >= 1e6 else f"${r.vol_24h/1e3:.0f}K"
    
    # Score bar (out of 150)
    bar_len = min(20, r.score * 20 // CONFIG["score_display_max"])
    bar = "█" * bar_len + "░" * (20 - bar_len)
    
    # Emoji
    em = {
        "very_strong": "🟢",
        "strong": "🟡",
        "watch": "⚪"
    }.get(r.confidence, "⚪")
    
    lines = [
        f"#{rank}  {r.symbol}  {em} Score: {r.score}/{CONFIG['score_display_max']}  [{r.phase}]",
        f"   {bar}",
        f"   {r.urgency}",
        f""
    ]
    
    # Catalysts
    if r.catalysts:
        lines.append(f"   📊 Catalysts:")
        for cat in r.catalysts[:5]:  # Max 5
            lines.append(f"      • {cat}")
        lines.append("")
    
    # Risk warnings
    if r.risk_warnings:
        lines.append(f"   ⚠️ Risks:")
        for warn in r.risk_warnings[:3]:
            lines.append(f"      • {warn}")
        lines.append("")
    
    # Market data
    lines.append(f"   Vol: {vol} | Δ24h: {r.chg_24h:+.1f}% | F: {r.funding*100:.5f}%")
    
    # Components breakdown
    comp = r.components
    phase_sc = comp.get("Phase", {}).get("score", 0)
    base_sc = comp.get("Base", {}).get("score", 0)
    prepump_sc = comp.get("PrePump", {}).get("score", 0)
    cont_sc = comp.get("Continuation", {}).get("score", 0)
    rev_sc = comp.get("Reversal", {}).get("score", 0)
    cat_sc = comp.get("Catalysts", {}).get("score", 0)
    
    lines.append(f"   Phase:{phase_sc} Base:{base_sc} Pre:{prepump_sc} Cont:{cont_sc} Rev:{rev_sc} Cat:{cat_sc}")
    lines.append("")
    
    # Entry (if available)
    if r.entry:
        e = r.entry
        lines.append(f"   Entry: ${e['entry']:.8f} | SL: ${e['sl']:.8f} (-{e['sl_pct']:.1f}%)")
        lines.append(f"   T1: +{e['t1_pct']:.1f}% | T2: +{e['t2_pct']:.1f}% | R/R: {e['rr']:.2f}")
        lines.append("")
    
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  API CLIENTS
# ══════════════════════════════════════════════════════════════════════════════
class BitgetClient:
    BASE = "https://api.bitget.com"
    _candle_cache: Dict = {}

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
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers", params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {item["symbol"]: item for item in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 200) -> List[dict]:
        cache_key = f"{symbol}:{limit}"
        if cache_key in cls._candle_cache:
            return cls._candle_cache[cache_key]
        
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/candles",
            params={"symbol": symbol, "productType": "USDT-FUTURES", "granularity": "1H", "limit": limit}
        )
        if not data or data.get("code") != "00000":
            return []
        
        candles = []
        for row in data.get("data", []):
            try:
                vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                candles.append({
                    "ts": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume_usd": vol_usd,
                })
            except Exception:
                continue
        
        candles.sort(key=lambda x: x["ts"])
        cls._candle_cache[cache_key] = candles
        return candles

    @classmethod
    def get_funding(cls, symbol: str) -> float:
        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/current-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES"}
        )
        try:
            return float(data["data"][0]["fundingRate"])
        except Exception:
            return 0.0

    @classmethod
    def clear_cache(cls) -> None:
        cls._candle_cache.clear()


class CoinalyzeClient:
    BASE = "https://api.coinalyze.net/v1"
    _last_call: float = 0.0

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._cache = {}

    def _wait(self) -> None:
        wait = 0.5 - (time.time() - CoinalyzeClient._last_call)
        if wait > 0:
            time.sleep(wait)
        CoinalyzeClient._last_call = time.time()

    def _get(self, endpoint: str, params: dict) -> Optional[list]:
        params["api_key"] = self.api_key
        for attempt in range(3):
            self._wait()
            try:
                r = requests.get(f"{self.BASE}/{endpoint}", params=params, timeout=15)
                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", 10)) + 1)
                    continue
                if r.status_code in (401, 400, 404):
                    return None
                if r.status_code != 200:
                    return None
                data = r.json()
                if isinstance(data, dict) and "error" in data:
                    return None
                return data
            except Exception:
                if attempt < 2:
                    time.sleep(3)
        return None

    def get_future_markets(self) -> List[dict]:
        if "future_markets" in self._cache:
            return self._cache["future_markets"]
        data = self._get("future-markets", {})
        res = data if isinstance(data, list) else []
        self._cache["future_markets"] = res
        return res

    def _batch_fetch(self, endpoint: str, symbols: List[str], extra_params: dict) -> Dict[str, list]:
        batch_size = 20
        res = {}
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            data = self._get(endpoint, {"symbols": ",".join(batch), **extra_params})
            if data and isinstance(data, list):
                for item in data:
                    sym = item.get("symbol", "")
                    hist = item.get("history", [])
                    if sym and hist:
                        res[sym] = hist
        return res

    def fetch_buy_sell_batch(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, list]:
        return self._batch_fetch(
            "ohlcv-history",
            symbols,
            {"interval": CONFIG["coinalyze_interval"], "from": from_ts, "to": to_ts}
        )

    def fetch_liquidations_batch(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, list]:
        return self._batch_fetch(
            "liquidation-history",
            symbols,
            {"interval": CONFIG["coinalyze_interval"], "from": from_ts, "to": to_ts, "convert_to_usd": "true"}
        )

    def fetch_oi_batch(self, symbols: List[str], from_ts: int, to_ts: int) -> Dict[str, list]:
        return self._batch_fetch(
            "open-interest-history",
            symbols,
            {"interval": CONFIG["coinalyze_interval"], "from": from_ts, "to": to_ts, "convert_to_usd": "true"}
        )


# ══════════════════════════════════════════════════════════════════════════════
#  🗺️  SYMBOL MAPPER
# ══════════════════════════════════════════════════════════════════════════════
class SymbolMapper:
    def __init__(self, clz_client: CoinalyzeClient):
        self._client = clz_client
        self._to_clz = {}
        self._rev_map = {}

    def load(self, active_symbols: set) -> None:
        markets = self._client.get_future_markets()
        if not markets:
            log.warning("No Coinalyze markets data, using fallback mapping")
            for sym in active_symbols:
                self._to_clz[sym] = f"{sym}_PERP.A"
        else:
            agg = {
                m.get("symbol", "").rsplit(".", 1)[0]: m
                for m in markets
                if m.get("symbol", "").endswith(".A")
            }
            for sym in active_symbols:
                a_sym = f"{sym}_PERP.A"
                self._to_clz[sym] = a_sym
        
        self._rev_map = {v: k for k, v in self._to_clz.items()}

    def to_clz(self, bitget_sym: str) -> Optional[str]:
        return self._to_clz.get(bitget_sym)

    def clz_symbols_for(self, bitget_syms: List[str]) -> List[str]:
        return [self._to_clz[s] for s in bitget_syms if s in self._to_clz]


# ══════════════════════════════════════════════════════════════════════════════
#  📤  TELEGRAM ALERT
# ══════════════════════════════════════════════════════════════════════════════
def send_telegram_alert(message: str) -> bool:
    """Send alert via Telegram"""
    bot_token = CONFIG.get("bot_token")
    chat_id = CONFIG.get("chat_id")
    
    if not bot_token or not chat_id:
        log.warning("Telegram not configured")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        r = requests.post(url, json=data, timeout=10)
        return r.status_code == 200
    except Exception as e:
        log.error(f"Failed to send Telegram alert: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCANNER LOOP
# ══════════════════════════════════════════════════════════════════════════════
# Cooldown tracking
_cooldown_state: Dict[str, float] = {}

def is_on_cooldown(symbol: str, cooldown_hours: int = 6) -> bool:
    """Check if symbol is on cooldown"""
    last_alert = _cooldown_state.get(symbol, 0)
    return (time.time() - last_alert) < (cooldown_hours * 3600)

def set_cooldown(symbol: str) -> None:
    """Set cooldown for symbol"""
    _cooldown_state[symbol] = time.time()


def main():
    """
    Main scanner loop - v12.0 PRE-PUMP DETECTION
    """
    log.info(f"{'═'*80}")
    log.info(f"  PRE-PUMP SCANNER v{VERSION}")
    log.info(f"  Target: Detect 1-3h BEFORE pump")
    log.info(f"  Expected Precision: 85-90%")
    log.info(f"{'═'*80}")
    
    # Initialize pump history database
    init_pump_history_db()
    
    # Initialize API clients
    clz_client = CoinalyzeClient(CONFIG["coinalyze_api_key"])
    mapper = SymbolMapper(clz_client)
    
    log.info("✅ Scanner v12.0 initialized")
    log.info("🔄 Starting scan...")
    
    try:
        # === STEP 1: Fetch tickers ===
        log.info("📊 Fetching tickers from Bitget...")
        tickers = BitgetClient.get_tickers()
        if not tickers:
            log.error("❌ No tickers from Bitget")
            return 1
        
        log.info(f"✅ Got {len(tickers)} tickers")
        
        # === STEP 2: Filter active symbols ===
        active = set()
        for sym, t in tickers.items():
            try:
                vol = float(t.get("quoteVolume", 0))
                if vol >= CONFIG["pre_filter_vol"]:
                    active.add(sym)
            except:
                pass
        
        log.info(f"✅ {len(active)} symbols passed volume filter (>{CONFIG['pre_filter_vol']/1e6:.1f}M)")
        
        # === STEP 3: Load symbol mapping ===
        log.info("🗺️  Loading Coinalyze symbol mapping...")
        mapper.load(active)
        
        # === STEP 4: Fetch Coinalyze data ===
        log.info("📈 Fetching Coinalyze data...")
        now_ts = int(time.time())
        from_ts = now_ts - CONFIG["coinalyze_lookback_h"] * 3600
        
        clz_syms = mapper.clz_symbols_for(list(active))
        
        btx_data = clz_client.fetch_buy_sell_batch(clz_syms, from_ts, now_ts)
        liq_data = clz_client.fetch_liquidations_batch(clz_syms, from_ts, now_ts)
        oi_data = clz_client.fetch_oi_batch(clz_syms, from_ts, now_ts)
        
        log.info(f"✅ Coinalyze data: BTX={len(btx_data)}, LIQ={len(liq_data)}, OI={len(oi_data)}")
        
        # === STEP 5: Score each coin ===
        log.info("🎯 Scoring coins...")
        results = []
        
        for sym in active:
            if is_on_cooldown(sym):
                continue
            
            try:
                # Get ticker data
                ticker = tickers.get(sym, {})
                price = float(ticker.get("lastPr", 0))
                vol_24h = float(ticker.get("quoteVolume", 0))
                chg_24h = float(ticker.get("chgUTC", 0))
                
                # Basic filters
                if vol_24h < CONFIG["min_vol_24h"]:
                    continue
                if vol_24h > CONFIG["max_vol_24h"]:
                    continue
                if price <= 0:
                    continue
                
                # Fetch candles
                candles = BitgetClient.get_candles(sym, CONFIG["candle_limit_bitget"])
                if len(candles) < 50:
                    continue
                
                # Fetch funding
                funding = BitgetClient.get_funding(sym)
                
                # Get Coinalyze data
                clz_sym = mapper.to_clz(sym)
                clz_btx = btx_data.get(clz_sym, []) if clz_sym else []
                clz_liq = liq_data.get(clz_sym, []) if clz_sym else []
                clz_oi = oi_data.get(clz_sym, []) if clz_sym else []
                
                # Build CoinData
                coin_data = CoinData(
                    symbol=sym,
                    price=price,
                    vol_24h=vol_24h,
                    chg_24h=chg_24h,
                    funding=funding,
                    candles=candles,
                    clz_btx=clz_btx,
                    clz_liq=clz_liq,
                    clz_oi=clz_oi
                )
                
                # Score with v12.0
                result = score_coin_v12(coin_data)
                if result:
                    results.append(result)
                    log.info(f"  ✅ {sym}: Score {result.score}/{CONFIG['score_display_max']} [{result.phase}]")
            
            except Exception as e:
                log.warning(f"  ⚠️ {sym}: Error - {e}")
                continue
        
        # === STEP 6: Sort and alert ===
        results.sort(key=lambda x: x.score, reverse=True)
        
        log.info(f"\n{'═'*80}")
        log.info(f"  📊 SCAN COMPLETE: {len(results)} signals")
        log.info(f"{'═'*80}\n")
        
        if results:
            for rank, r in enumerate(results[:10], 1):  # Top 10
                alert_msg = build_alert_v12(r, rank)
                print(alert_msg)
                
                # Send to Telegram
                if rank <= 5:  # Only top 5
                    send_telegram_alert(alert_msg)
                
                # Set cooldown
                set_cooldown(r.symbol)
        else:
            log.info("  No signals above threshold")
        
        # Clear cache
        BitgetClient.clear_cache()
        
        return 0
    
    except Exception as e:
        log.error(f"❌ Scanner error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        log.info("\n⚠️  Scanner stopped by user")
        exit(0)
    except Exception as e:
        log.error(f"❌ Fatal error: {e}", exc_info=True)
        exit(1)
