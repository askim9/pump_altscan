"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  QUANTITATIVE PUMP DETECTION SCANNER v27 — PRE-PUMP INTELLIGENCE EDITION    ║
║                                                                              ║
║  INHERITED FROM v26 (all bug fixes + performance confirmed working):        ║
║    pump_prob_v22 ✅ | vol log ✅ | crash protection ✅ | HTTP session ✅     ║
║    sleep 0.15s ✅ | EMA/vol cache reuse ✅ | TOP 5 output ✅                 ║
║                                                                              ║
║  NEW v27 — 7 Pre-Pump Intelligence Modules:                                 ║
║    Module 1 — Momentum Acceleration MTF (5m vs 1h)          → +10           ║
║    Module 2 — Volume Expansion Extreme (>4× baseline)        → +20           ║
║    Module 3 — OI Expansion (>5% growth = new money in)       → +20           ║
║    Module 4 — ATR Breakout (price > EMA20 + 1.5×ATR)        → +10           ║
║    Module 5 — Real L2 Orderbook (Bitget merge-depth, live)   → +15/+7        ║
║    Module 6 — Whale Accumulation (large buy candles ≥$15K)   → +15           ║
║    Module 7 — Upward Liquidity Sweep (short stop-hunt >1%)   → +10           ║
║                                                                              ║
║  pump_probability_score (0-90): aggregated v27 composite score              ║
║  Boost: pump_prob += pump_probability_score × 0.15 (max +13.5pp, cap 95%)  ║
║  Ranking: pump_score_v27 → rank_value → mm_score → vol_z → ob_imbalance    ║
║                                                                              ║
║  TARGET: detect 8–30% pumps vs current 2–4% sensitivity                    ║
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

# v26 PERF: Persistent HTTP session for connection reuse across ~342 symbols
# Eliminates TCP handshake overhead on every API call
_http_session = requests.Session()
_http_session.headers.update({"User-Agent": "CryptoScanner/26.0", "Accept-Encoding": "gzip"})
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
    "/tmp/scanner_v27.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Scanner v27 — log aktif: /tmp/scanner_v27.log")

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG = {
    # ── Threshold alert ───────────────────────────────────────────────────────
    "min_score_alert":          8,
    "max_alerts_per_run":       15,

    # ── Volume 24h total (USD) ────────────────────────────────────────────────
    "min_vol_24h":          10_000,
    "max_vol_24h":      50_000_000,
    "pre_filter_vol":       10_000,

    # ── Open Interest minimum filter ──────────────────────────────────────────
    "min_oi_usd":          100_000,   # minimal $100K OI

    # ── Gate perubahan harga 24h ──────────────────────────────────────────────
    # FIX v18: dilonggarkan dari 5% → 8%.
    # 5% terlalu ketat di pasar bullish — coin yang baru mulai pump +5-7%
    # masih bisa pre-pump, belum tentu distribusi.
    # 8% masih memfilter coin yang sudah pump besar.
    "gate_chg_24h_max":          8.0,
    "gate_chg_24h_min":        -15.0,   # hanya skip dump besar

    # ── VWAP Gate Tolerance ───────────────────────────────────────────────────
    "vwap_gate_tolerance":      0.97,   # price > vwap * 0.97

    # ── Gate uptrend usia ─────────────────────────────────────────────────────
    "gate_uptrend_max_hours":   10,

    # ── Gate RSI overbought ───────────────────────────────────────────────────
    "gate_rsi_max":             72.0,

    # ── Gate BB Position ──────────────────────────────────────────────────────
    "gate_bb_pos_max":          1.05,

    # ── Funding rate scoring ──────────────────────────────────────────────────
    "funding_penalty_avg":     0.0003,   # > +0.03% → penalti -2
    "funding_bonus_avg":      -0.0002,   # < -0.02% → bonus +2
    "funding_bonus_cumul":    -0.001,    # cumul < -0.1% → bonus +1
    "funding_streak_min":       5,

    # ── Candle limits ─────────────────────────────────────────────────────────
    "candle_1h":               168,
    "candle_4h":                48,

    # ── Entry / SL ────────────────────────────────────────────────────────────
    "entry_bos_buffer":        0.0005,
    "sl_swing_lookback":       12,
    "sl_swing_buffer":         0.003,
    "sl_atr_mult_min":         1.0,
    "sl_atr_mult_max":         3.0,
    "max_sl_pct":              8.0,
    "min_sl_pct":              0.5,

    # ── Operasional ───────────────────────────────────────────────────────────
    "alert_cooldown_sec":     1800,
    # v26 PERF: reduced 0.8 → 0.15s per symbol (~342 coins → ~51s total vs ~274s)
    "sleep_coins":             0.15,
    "sleep_error":             3.0,
    "cooldown_file":          "./cooldown.json",
    "funding_snapshot_file":  "./funding.json",
    # FIX v18: OI snapshot sekarang persisten ke disk
    "oi_snapshot_file":       "./oi_snapshot.json",

    # ══════════════════════════════════════════════════════════════════════════
    #  BOBOT SKOR v18
    # ══════════════════════════════════════════════════════════════════════════

    # ── BB Squeeze ────────────────────────────────────────────────────────────
    "bb_squeeze_threshold":    0.04,
    "score_bb_squeeze":        4,

    # ── ATR Contracting ───────────────────────────────────────────────────────
    "atr_contract_ratio":      0.75,
    "score_atr_contracting":   3,

    # ── Energy Build-Up ───────────────────────────────────────────────────────
    "energy_oi_change_min":    5.0,
    "energy_vol_ratio_min":    1.5,
    "energy_range_max_pct":    2.5,
    "score_energy_buildup":    4,

    # ── Smart Money Accumulation ──────────────────────────────────────────────
    "accum_vol_ratio":         1.5,
    "accum_price_range_max":   2.0,
    "accum_atr_lookback_long": 24,
    "accum_atr_lookback_short": 6,
    "accum_atr_contract_ratio": 0.75,
    "accum_max_pos_in_range":  0.70,
    "score_accumulation":      4,
    # FIX v18: score_vol_compression hanya aktif jika is_accumulating=False
    "score_vol_compression":   4,

    # ── HTF Accumulation 4H ───────────────────────────────────────────────────
    "htf_atr_contract_ratio":  0.85,
    "htf_vol_ratio_min":       1.3,
    "htf_range_max_pct":       3.0,
    "htf_max_pos_in_range":    0.75,
    "score_htf_accumulation":  3,

    # ── Liquidity Sweep ───────────────────────────────────────────────────────
    "liq_sweep_lookback":      20,
    "liq_sweep_wick_min_pct":  0.3,
    "score_liquidity_sweep":   3,

    # ── OI Expansion ─────────────────────────────────────────────────────────
    "oi_change_min_pct":       3.0,
    "oi_strong_pct":          10.0,
    "score_oi_expansion":      3,
    "score_oi_strong":         5,

    # ── Volume dengan konteks arah ────────────────────────────────────────────
    "vol_ratio_threshold":     1.5,
    "vol_bullish_min_ratio":   0.6,
    "score_vol_bullish":       2,

    # ── Volume Acceleration ───────────────────────────────────────────────────
    "vol_accel_threshold":     0.5,
    "score_vol_accel":         2,

    # ── RSI ideal pre-pump = 40–60 ────────────────────────────────────────────
    "rsi_ideal_min":           40.0,
    "rsi_ideal_max":           60.0,
    "score_rsi_ideal":         2,

    # ── Higher Low ────────────────────────────────────────────────────────────
    # FIX v18: lookback dinaikkan 6 → 16 candle (lebih bermakna)
    "higher_low_lookback":     16,
    "score_higher_low":        2,

    # ── BOS Up ───────────────────────────────────────────────────────────────
    # FIX v18: lookback dinaikkan 3 → 8 candle (BOS lebih bermakna)
    "bos_lookback":            8,
    "score_bos_up":            1,

    # ── Funding scoring ───────────────────────────────────────────────────────
    "score_funding_avg_neg":   2,
    "score_funding_cumul":     2,
    "score_funding_neg_pct":   3,
    "score_funding_streak":    3,

    # ── BTC Outperformance ────────────────────────────────────────────────────
    "btc_bearish_threshold":  -3.0,
    "btc_bullish_threshold":   3.0,
    "outperform_min_delta":    2.0,
    "score_outperform":        3,

    # ── Threshold lainnya ─────────────────────────────────────────────────────
    "above_vwap_rate_min":     0.6,
    "ema_gap_threshold":       1.0,

    # ══════════════════════════════════════════════════════════════════════════
    #  UPGRADE v18: NEW FEATURES
    # ══════════════════════════════════════════════════════════════════════════

    # ── Volume Spike Detection (Phase 1 pre-pump) ─────────────────────────────
    # Formula: current_volume_20 / avg_volume_20
    "vol_spike_window":        20,       # candle baseline window
    "vol_spike_low":           1.5,      # threshold low  → +8
    "vol_spike_mid":           2.0,      # threshold mid  → +15
    "vol_spike_high":          3.0,      # threshold high → +22
    "score_vol_spike_low":     8,
    "score_vol_spike_mid":     15,
    "score_vol_spike_high":    22,

    # ── Momentum Acceleration (Phase 3 pre-pump) ──────────────────────────────
    # acceleration = (price_now-price_3h) - (price_3h-price_6h)
    "accel_strong_threshold":  0.005,    # 0.5% percepatan → strong
    "score_accel_positive":    10,       # acceleration > 0
    "score_accel_strong":      18,       # acceleration > strong_threshold

    # ── Buy Pressure (Phase 2 pre-pump) ──────────────────────────────────────
    # buy_ratio = buy_volume / total_volume (dari 15m candles)
    "buy_pressure_window":     8,        # 8 candle 15m = 2 jam
    "buy_pressure_low":        0.55,     # >55% accumulation → +6
    "buy_pressure_mid":        0.65,     # >65% whale activity → +12
    "buy_pressure_high":       0.75,     # >75% pump phase → +20
    "score_buy_pressure_low":  6,
    "score_buy_pressure_mid":  12,
    "score_buy_pressure_high": 20,

    # ── Whale Order Detection ─────────────────────────────────────────────────
    # Deteksi via volume spike besar pada 15m candle terbaru
    "whale_vol_mult":          5.0,      # 5x avg 15m volume = whale
    "score_whale_order":       8,

    # ── Fake Pump Filter ──────────────────────────────────────────────────────
    # Harga naik tapi buy_ratio < 50% → spoof / distribusi
    "fake_pump_price_min":     0.3,      # price naik minimal 0.3% dalam 3h
    "fake_pump_buy_max":       0.50,     # buy_ratio < 50%
    "penalty_fake_pump":       -10,

    # ── Logistic Probability Model ────────────────────────────────────────────
    # prob = 1 / (1 + exp(-(score - center) / scale))
    "prob_center":             50,       # score 50 → prob 50%
    "prob_scale":              8,        # steepness

    # ── Signal Threshold v18 ──────────────────────────────────────────────────
    "score_watchlist":         40,       # 40–55 → WATCHLIST
    "score_alert":             55,       # 55–70 → ALERT
    "score_strong_alert":      70,       # 70+   → STRONG ALERT

    # ── Entry Regime Detection ────────────────────────────────────────────────
    "breakout_buy_ratio_min":  0.60,     # buy_ratio > 60% untuk breakout mode
    "breakout_vol_ratio_min":  1.80,     # vol_ratio > 1.8x untuk breakout mode
    "mean_rev_rsi_max":        45.0,     # RSI < 45 untuk mean reversion mode
    "mean_rev_range_max":      0.20,     # range_pos < 20% untuk mean reversion
    "entry_breakout_atr_mult": 0.15,     # entry = resistance + 0.15×ATR
    "entry_mean_rev_atr_mult": 0.20,     # entry = support + 0.20×ATR
    "sl_atr_base":             2.5,      # SL = entry - ATR × 2.5
    "sl_atr_volatile":         3.0,      # SL untuk coin volatile
    "tp1_atr_mult":            1.5,      # v18: TP1 = entry + ATR × 1.5
    "tp2_atr_mult":            3.0,      # v18: TP2 = entry + ATR × 3
    "tp3_atr_mult":            5.0,      # v18: TP3 = entry + ATR × 5

    # ── v18 Entry model ───────────────────────────────────────────────────────
    "entry_pullback_atr_mult": 0.30,     # PULLBACK: entry = VWAP - 0.3×ATR
    "entry_sweep_atr_mult":    0.20,     # SWEEP: entry = sweep_low + 0.2×ATR
    "entry_retest_buffer":     0.005,    # BREAKOUT retest: +0.5% di atas breakout

    # ── v18 Micro Momentum (5m candles) ──────────────────────────────────────
    "micro_mom_candles":       12,       # berapa 5m candle untuk rata2 1h
    "micro_accel_strong":      0.003,    # 0.3% acceleration = strong
    "score_micro_accel":       15,       # strong micro accel score
    "score_micro_accel_pos":   8,        # positive micro accel score

    # ── v18 Whale detection upgrade ──────────────────────────────────────────
    "whale_vol_mult_v18":      3.0,      # vol > 3× (bukan 5×)
    "whale_buy_ratio_min":     0.65,     # buy_ratio > 65%
    "whale_oi_change_min":     2.0,      # OI harus naik minimal 2%
    "score_whale_v18":         10,       # skor whale yang lebih ketat tapi akurat

    # ── v18 Pump Probability feature weights ─────────────────────────────────
    "prob_w_vol_spike":        0.9,
    "prob_w_buy_press":        1.2,
    "prob_w_mom_accel":        1.5,
    "prob_w_oi_change":        0.7,
    "prob_w_bb_squeeze":       0.6,
    "prob_w_atr_contract":     0.5,
    "prob_w_fake_pump":       -1.0,

    # ── v18 Pump Timing ETA ───────────────────────────────────────────────────
    "timing_w_vol_accel":      0.4,
    "timing_w_buy_ratio":      0.3,
    "timing_w_oi_accel":       0.2,
    "timing_w_momentum":       0.1,
    "timing_eta_5min":         0.75,     # timing > 0.75 → ETA 5 min
    "timing_eta_10min":        0.55,     # timing > 0.55 → ETA 10 min
    "timing_eta_30min":        0.35,     # timing > 0.35 → ETA 30 min
    "timing_eta_60min":        0.20,     # timing > 0.20 → ETA 60 min

    # ── v18 Fake Pump upgrade ─────────────────────────────────────────────────
    "fake_price_spike_min":    0.005,    # harga naik > 0.5% dalam 3h
    "fake_oi_flat_max":        1.5,      # OI change < 1.5% = OI flat
    "fake_sell_press_max":     0.48,     # buy_ratio < 48% = sell pressure
    "fake_penalty_mild":      -5,        # 2 kondisi fake
    "fake_penalty_strong":    -12,       # 3 kondisi fake
    "fake_penalty_severe":    -20,       # 4 kondisi fake (semua terpenuhi)

    # ══════════════════════════════════════════════════════════════════════════
    #  UPGRADE v19: NEW CONFIG
    # ══════════════════════════════════════════════════════════════════════════

    # STEP 3 — Dynamic TP multipliers (v19)
    "tp1_v19_mult":            2.0,      # TP1 = entry + ATR × 2
    "tp2_v19_mult":            3.5,      # TP2 = entry + ATR × 3.5
    "tp3_v19_mult":            5.0,      # TP3 = entry + ATR × 5

    # STEP 4 — AI Weighted scoring weights
    "wscore_volume":           0.30,
    "wscore_accel":            0.20,
    "wscore_momentum":         0.20,
    "wscore_liquidity":        0.15,
    "wscore_breakout":         0.10,
    "wscore_rsi":              0.05,

    # STEP 5 — Logistic probability params
    "logistic_k":              0.08,
    "logistic_threshold":      55.0,

    # STEP 6 — Trend filter EMAs
    "ema_fast":                20,
    "ema_slow":                50,
    "score_penalty_bearish":  -8,        # EMA20 < EMA50 → penalti

    # STEP 8 — Momentum validation (5m price change)
    # FIX v23: dilonggarkan 0.0 → -0.005 (-0.5%) — flat coins tidak dibuang
    "momentum_val_reject":    -0.005,    # reject only if price_chg_5m < -0.5%

    # STEP 9 — Wick ratio filter
    "wick_ratio_max":          0.4,      # reject if (high-close)/(high-low) > 0.4

    # STEP 10 — Volume Z-score
    "vol_zscore_boost":        3.0,      # z > 3 → boost volume score
    "vol_zscore_window":       24,       # lookback untuk mean/std

    # STEP 11 — Micro breakout
    "micro_breakout_lookback": 20,       # highest high N candle
    "score_micro_breakout":    6,

    # STEP 13 — Noise filter
    # FIX v23: min vol 20M → 2M — filter too strict, removing valid pumps
    # FIX v24: further relaxed 2M → 500k — allow smaller cap altcoins
    "noise_min_vol_24h":      500_000,   # min $500K volume 24h (was $2M)
    "noise_min_atr_pct":       0.3,      # min 0.3% ATR

    # STEP 14 — Early pump detection
    "early_pump_range_pos_max": 0.40,    # range_pos < 40%
    "score_early_pump":         8,

    # STEP 15 — Rank by: score × probability
    "rank_use_combined":        True,

    # STEP 16 — Whale accumulation (sideways + vol rising + ATR falling)
    "whale_accum_vol_min":      1.3,     # vol ratio min
    "whale_accum_atr_max":      0.90,    # ATR contract ratio max
    "whale_accum_range_max":    2.5,     # price range pct max
    "score_whale_accum":        7,

    # STEP 18 — Threshold naik 40→55
    "score_watchlist":          55,      # was 40 in v18
    "score_alert":              65,      # was 55
    "score_strong_alert":       78,      # was 70

    # ══════════════════════════════════════════════════════════════════════════
    #  UPGRADE v20: NEW CONFIG
    # ══════════════════════════════════════════════════════════════════════════

    # PART 1 — EMA Slope
    "ema20_slope_lookback":     3,       # candles ago for slope reference
    "score_ema20_slope":        4,       # bonus score if slope > 0

    # PART 1 — Volume Z-Score v20 (stricter threshold vs v19 z>3)
    "vol_zscore_v20_min":       1.5,     # z > 1.5 for reversal confirmation
    "vol_zscore_v20_strong":    2.0,     # z > 2 → extra bonus + dump filter
    "score_vol_zscore_v20":     10,      # z > 2 → +10

    # PART 1 — Micro Breakout bonus v20
    "score_micro_breakout_v20": 8,       # price > high_last_20 → +8

    # PART 1 — EMA200 Distance
    "ema200_distance_max":      0.06,    # < 6% away from EMA200
    "score_ema200_close":       5,       # bonus for near EMA200

    # PART 1 — Orderbook Imbalance (bid/ask ratio from candle proxy)
    "bid_ask_ratio_min":        1.2,     # bid_ask_ratio > 1.2 minimum
    "bid_ask_ratio_strong":     1.3,     # bid_ask_ratio > 1.3 → extra bonus
    "score_bid_ask_v20":        6,       # bid_ask_ratio > 1.3 → +6

    # PART 2 — Fake Reversal Filter
    "fake_reversal_penalty":   -12,      # penalty if EMA cross reversal fails

    # PART 3 — Dump Filter thresholds
    "dump_filter_5m_pct":      -4.0,     # price_change_5m < -4% → reject
    "dump_filter_15m_pct":     -6.0,     # price_change_15m < -6% → reject

    # PART 4 — Weighted score caps (FIX v24: these were used but never defined)
    "v20_momentum_cap":         20,      # max bonus from momentum component
    "v20_volume_cap":           15,      # max bonus from volume component
    "v20_breakout_cap":         15,      # max bonus from breakout component
    "v20_reversal_cap":         15,      # max bonus from reversal component
    "v20_orderbook_cap":        10,      # max bonus from orderbook component
    "v20_trend_cap":            10,      # max bonus from trend component

    # PART 7 — Multi-key ranking
    "rank_v20_multi":           True,    # sort by score, zscore, bid_ask_ratio

    # ══════════════════════════════════════════════════════════════════════════
    #  UPGRADE v22: INSTITUTIONAL PUMP HUNTER CONFIG
    # ══════════════════════════════════════════════════════════════════════════

    # FIX 01 — EMA50 Reversal Override
    "ema50_override_slope_min":  0.0,    # slope > 0 overrides EMA50 reject gate
    # FIX v23: z-score threshold 1.5 → -0.5 so early reversals are not blocked
    # FIX v24: further relaxed -0.5 → -2.0 — allow even very early reversals
    "ema50_override_zscore_min": -2.0,   # was -0.5 (v23) → now -2.0 (v24)

    # FIX 02 — Smart Money Accumulation
    "sma_range_max":             0.03,   # price range contraction < 3%
    "sma_vol_trend_min":         1.2,    # rising volume ratio
    "score_smart_money_accum":   12,     # bonus score

    # FIX 03 — Liquidity Trap (stop-sweep)
    "liq_trap_lookback":         30,     # 30 candles for prior low
    "liq_trap_zscore_min":       1.5,    # z-score condition
    "score_liq_trap":            10,     # bonus score

    # FIX 04 — Whale Footprint
    "whale_fp_vol_mult":         3.0,    # vol > 3× mean
    "whale_fp_price_max_pct":    0.5,    # price change < 0.5%
    "score_whale_footprint":     8,      # bonus score

    # FIX 05 — Pre-Breakout Pressure (BB percentile)
    "bb_percentile_lookback":    50,     # candles for BB percentile
    "bb_percentile_threshold":   20,     # below 20th percentile
    "score_prebreakout":         8,      # bonus score

    # FIX 06 — Momentum Ignition
    "mom_ignition_highs":        3,      # 3 consecutive higher highs
    "mom_ignition_zscore":       1.8,    # z-score requirement
    "score_mom_ignition":        10,     # bonus score

    # FIX 07 — Dump Trap Filter
    "dump_trap_ema50_slope_max": 0.0,    # ema50_slope < 0
    "dump_trap_ask_bid_min":     1.3,    # ask pressure > 1.3× bid

    # FIX 08 — Improved Reversal
    "rev_vwap_tolerance":        0.02,   # price within 2% of VWAP

    # FIX 09 — Institutional Scoring weights
    "inst_w_accumulation":       0.20,
    "inst_w_breakout":           0.20,
    "inst_w_volume":             0.20,
    "inst_w_orderbook":          0.15,
    "inst_w_momentum":           0.15,
    "inst_w_liq_trap":           0.10,

    # FIX 12 — Dynamic TP/SL v22
    "tp1_v22_mult":              1.3,    # TP1 = entry + ATR × 1.3
    "tp2_v22_mult":              2.2,    # TP2 = entry + ATR × 2.2
    "sl_v22_mult":               0.9,    # SL  = entry − ATR × 0.9

    # ══════════════════════════════════════════════════════════════════════════
    #  UPGRADE v24: ANTI-OVERFILTERING PATCHES
    # ══════════════════════════════════════════════════════════════════════════

    # FIX v24: Penalties for soft-gates (replace hard rejects)
    "penalty_below_vwap_v24":   -3,     # VWAP gate breach → -3 score
    "penalty_below_ema50_v24":  -5,     # EMA50 downtrend  → -5 score
    "penalty_dump_trap_v24":   -10,     # Dump trap active  → -10 score

    # FIX v24: Watchlist threshold further lowered for more candidates
    "score_watchlist_v24":       30,    # was 40 in v23 (target: 20-40 alerts)

    # ══════════════════════════════════════════════════════════════════════════
    #  UPGRADE v25: STABILITY + SMART FILTER CONFIG
    # ══════════════════════════════════════════════════════════════════════════

    # Wick trap soft penalty (replaces hard reject at 0.55)
    "penalty_wick_v25":         -3,     # wick > 0.55 → -3 (hard reject only at 0.75+ask>2)

    # EMA50 triple-condition hard reject threshold
    "ema50_hard_reject_ask_bid": 1.8,   # ask/bid > 1.8 required for triple dump reject

    # Early Reversal Setup bonus
    "score_reversal_setup_v25":  8,     # EMA20 slope↑ + z>1.5 + mom↑ = +8

    # ══════════════════════════════════════════════════════════════════════════
    #  UPGRADE v26: CALIBRATED SCORING WEIGHTS + PERFORMANCE
    # ══════════════════════════════════════════════════════════════════════════

    # Calibrated signal scores (spec: vol spike +6, BB breakout +5, micro +7, reversal +8, OB +5)
    # v26: aligns vol_spike_low with spec floor (+6); upper tiers kept proportional
    "score_vol_spike_low_v26":   6,     # was 8 — aligns with spec minimum
    "score_micro_breakout_v26":  7,     # was 8 — micro breakout calibrated
    "score_ob_imbalance_v26":    5,     # orderbook imbalance standalone bonus

    # Performance config
    # sleep_coins already set to 0.15 above
    "log_progress_interval":    10,     # log coin progress every N symbols

    # --- Filter relaxations (6 critical overfiltering fixes) ---
    # (see inline where applied: momentum_val_reject, noise_min_vol_24h,
    #  ema50_override_zscore_min, gate_bb_pos_max, price_pos_48 gate, reversal)

    # BB Breakout bonus (replaces hard rejection)
    "score_bb_breakout_v23":     5,      # price above upper BB → +5 breakout score

    # Spoofing detection
    "spoofing_vol_mult":         4.0,    # large order = vol > 4× mean
    "spoofing_reversal_candles": 2,      # disappears within 2 candles
    "spoofing_price_max_pct":    0.3,    # price move < 0.3% despite big vol
    "spoofing_penalty":         -6,      # score penalty if spoofing detected

    # Orderbook pressure model
    "ob_imbalance_bullish":      1.2,    # bid/ask > 1.2 = bullish
    "ob_imbalance_bearish":      0.8,    # bid/ask < 0.8 = bearish
    "ob_imbalance_score_max":    10,     # max bonus from orderbook pressure

    # Volatility compression (BB percentile — reuses v22 thresholds)
    # already: bb_percentile_lookback, bb_percentile_threshold, score_prebreakout

    # v23 institutional scoring blend ratio
    "v23_heuristic_weight":      0.45,   # was 0.50
    "v23_ai_weight":             0.30,   # unchanged
    "v23_inst_weight":           0.25,   # was 0.20

    # Watchlist threshold — FIX v23: lowered 55 → 40 to pass more candidates
    # (raised too aggressively in v19; 344 coins 0 passed)
    "score_watchlist_v23":       40,     # effective threshold for v23

    # ══════════════════════════════════════════════════════════════════════════
    #  v27 — PRE-PUMP INTELLIGENCE CONFIG
    # ══════════════════════════════════════════════════════════════════════════

    # Module 1 — Momentum Acceleration (multi-timeframe: 5m feeds 1h)
    "v27_mom_accel_5m_ratio":    1.5,    # 5m change must be 1.5× the 30m change
    "v27_mom_accel_score":       10,     # +10 if multi-TF momentum accelerating

    # Module 2 — Volume Expansion (extreme tier >4× baseline)
    "v27_vol_expansion_thresh":  4.0,    # >4× = extreme pump-grade expansion
    "v27_vol_expansion_score":   20,     # +20 for >4× (adds to existing vol_spike)

    # Module 3 — OI Expansion scoring
    "v27_oi_expansion_pct":      5.0,    # OI growth > 5% = confirmed new money
    "v27_oi_expansion_score":    20,     # +20 when OI expands > 5%

    # Module 4 — ATR Breakout (price > EMA20 + 1.5×ATR)
    "v27_atr_breakout_mult":     1.5,    # multiplier above EMA20
    "v27_atr_breakout_score":    10,     # +10 for ATR breakout confirmation

    # Module 5 — Real L2 Orderbook Imbalance (Bitget merge-depth)
    "v27_ob_depth_limit":        20,     # top-N levels to sum bids/asks
    "v27_ob_ratio_strong":       1.6,    # bid/ask ≥ 1.6 → strong buy wall
    "v27_ob_ratio_mild":         1.2,    # bid/ask ≥ 1.2 → mild imbalance
    "v27_ob_score_strong":       15,     # +15 for strong imbalance
    "v27_ob_score_mild":         7,      # +7 for mild imbalance

    # Module 6 — Whale Accumulation (large single trades via volume candles)
    "v27_whale_trade_usd":       15000,  # single trade > $15K = whale
    "v27_whale_trade_count":     3,      # need ≥ 3 such candles in window
    "v27_whale_trade_window":    12,     # look back N candles
    "v27_whale_accum_score":     15,     # +15 when whale accumulation detected

    # Module 7 — Liquidity Sweep (high breakout — price > recent high × 1.01)
    "v27_liq_sweep_upbreak_pct": 0.01,   # price must exceed prior high by 1%
    "v27_liq_sweep_lookback":    20,     # lookback for prior high
    "v27_liq_sweep_score":       10,     # +10 for upward sweep

    # pump_probability_score cap
    "v27_pump_score_cap":        90,     # max pump_probability_score (0-100)
}

MANUAL_EXCLUDE = set()

EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST"]

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST
# ══════════════════════════════════════════════════════════════════════════════
WHITELIST_SYMBOLS = {
    # ── Tier 1: Large Cap Altcoin (OI & volume tertinggi) ────────────────────
    "DOGEUSDT", "ADAUSDT", "XMRUSDT", "LINKUSDT", "XLMUSDT", "HBARUSDT",
    "LTCUSDT", "AVAXUSDT", "SHIBUSDT", "SUIUSDT", "TONUSDT",
    "UNIUSDT", "DOTUSDT", "TAOUSDT", "AAVEUSDT", "PEPEUSDT",
    "ETCUSDT", "NEARUSDT", "ONDOUSDT", "POLUSDT", "ICPUSDT", "ATOMUSDT",
    "ENAUSDT", "KASUSDT", "ALGOUSDT", "RENDERUSDT", "FILUSDT", "APTUSDT",
    "ARBUSDT", "JUPUSDT", "SEIUSDT", "STXUSDT", "DYDXUSDT", "VIRTUALUSDT",

    # ── Tier 2: Mid Cap (OI signifikan, aktif di futures) ────────────────────
    "FETUSDT", "INJUSDT", "PYTHUSDT", "GRTUSDT", "TIAUSDT", "LDOUSDT",
    "OPUSDT", "ENSUSDT", "AXSUSDT", "PENDLEUSDT", "WIFUSDT", "SANDUSDT",
    "MANAUSDT", "COMPUSDT", "GALAUSDT", "RAYUSDT", "RUNEUSDT", "EGLDUSDT",
    "SNXUSDT", "ARUSDT", "CRVUSDT", "IMXUSDT", "EIGENUSDT", "JTOUSDT",
    "CELOUSDT", "MASKUSDT", "APEUSDT", "MOVEUSDT", "MINAUSDT", "SONICUSDT",
    "KAIAUSDT", "HYPEUSDT", "WLDUSDT", "STRKUSDT", "CFXUSDT", "BOMEUSDT",

    # ── Tier 3: Aktif trading, OI > threshold ────────────────────────────────
    "FLOKIUSDT", "CAKEUSDT", "CHZUSDT", "HNTUSDT", "ROSEUSDT", "IOTXUSDT",
    "ANKRUSDT", "ZILUSDT", "ONTUSDT", "ENJUSDT", "GMTUSDT", "NOTUSDT",
    "PEOPLEUSDT", "METISUSDT", "AIXBTUSDT", "GOATUSDT", "PNUTUSDT",
    "GRASSUSDT", "POPCATUSDT", "ORDIUSDT", "MOODENGUSDT", "BIOUSDT",
    "MAGICUSDT", "REZUSDT", "ARPAUSDT", "ACTUSDT", "USUALUSDT",
    "SLPUSDT", "XAIUSDT", "BLURUSDT", "ARKMUSDT", "API3USDT", "AGLDUSDT",
    "TNSRUSDT", "LAYERUSDT", "ANIMEUSDT", "YGGUSDT", "THEUSDT",
}

GRAN_MAP    = {"5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H", "1d": "1D"}
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
#  💾  FUNDING SNAPSHOTS
# ══════════════════════════════════════════════════════════════════════════════
_funding_snapshots = {}
_btc_candles_cache = {"ts": 0, "data": []}

def load_funding_snapshots():
    global _funding_snapshots
    try:
        p = CONFIG["funding_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                _funding_snapshots = json.load(f)
    except Exception:
        _funding_snapshots = {}

def save_all_funding_snapshots():
    try:
        with open(CONFIG["funding_snapshot_file"], "w") as f:
            json.dump(_funding_snapshots, f)
    except Exception:
        pass

def add_funding_snapshot(symbol, funding_rate):
    if symbol not in _funding_snapshots:
        _funding_snapshots[symbol] = []
    _funding_snapshots[symbol].append({
        "ts":      time.time(),
        "funding": funding_rate,
    })
    # Simpan hanya 48 snapshot terakhir per coin
    if len(_funding_snapshots[symbol]) > 48:
        _funding_snapshots[symbol] = _funding_snapshots[symbol][-48:]

# ══════════════════════════════════════════════════════════════════════════════
#  💾  OI SNAPSHOTS — FIX v18: PERSISTEN KE DISK
# ══════════════════════════════════════════════════════════════════════════════
_oi_snapshot = {}

def load_oi_snapshots():
    """
    FIX v18: Load OI snapshot dari disk saat startup.
    Sebelumnya (v15.7) _oi_snapshot hanya in-memory → reset tiap restart
    → OI change selalu is_new=True → energy_buildup dan OI scoring tidak pernah
    aktif di run pertama setelah restart.
    """
    global _oi_snapshot
    try:
        p = CONFIG["oi_snapshot_file"]
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            # Buang snapshot yang sudah lebih dari 2 jam (stale data)
            now = time.time()
            _oi_snapshot = {
                sym: v for sym, v in data.items()
                if now - v.get("ts", 0) < 7200
            }
            log.info(f"OI snapshots loaded: {len(_oi_snapshot)} coins")
        else:
            _oi_snapshot = {}
    except Exception:
        _oi_snapshot = {}

def save_oi_snapshots():
    """FIX v18: Simpan OI snapshot ke disk setelah tiap scan."""
    try:
        with open(CONFIG["oi_snapshot_file"], "w") as f:
            json.dump(_oi_snapshot, f)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def safe_get(url, params=None, timeout=10):
    # v26 PERF: use persistent session for connection reuse
    for attempt in range(2):
        try:
            r = _http_session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                log.warning("Rate limit — tunggu 15s, lalu retry")
                time.sleep(15)
                continue   # retry setelah 429
            break
        except Exception:
            if attempt == 0:
                time.sleep(CONFIG["sleep_error"])
    return None

def _safe_telegram_text(msg):
    """
    FIX 13 v22 — Enhanced Telegram message sanitizer (delegates to v22 impl).
    Handles: & escaping, broken tags, truncation to 4050 chars.
    """
    return _safe_telegram_text_v22(msg)

def send_telegram(msg, parse_mode="HTML"):
    """
    STEP 17 v19 — Fixed Telegram sender dengan:
    1. html.escape fallback jika HTML parse mode gagal
    2. Retry tanpa parse_mode jika masih gagal
    3. Truncate aman dengan mempertahankan tag
    """
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("send_telegram: BOT_TOKEN atau CHAT_ID tidak ada!")
        return False
    if len(msg) > 4000:
        msg = msg[:3900] + "\n\n<i>...[dipotong]</i>"

    msg = _safe_telegram_text(msg)

    for attempt in range(2):
        try:
            payload = {"chat_id": CHAT_ID, "text": msg}
            if attempt == 0:
                payload["parse_mode"] = "HTML"
            # attempt 1: tanpa parse_mode (plain text fallback)
            r = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data=payload,
                timeout=15,
            )
            if r.status_code == 200:
                return True
            err_text = r.text[:300]
            if "can\'t parse" in err_text or "Bad Request" in err_text:
                log.warning(f"Telegram parse error attempt {attempt} — retry plain text")
                # Coba kirim ulang tanpa HTML
                msg = _html_mod.unescape(msg)
                msg = msg.replace("<b>","").replace("</b>","")
                msg = msg.replace("<i>","").replace("</i>","")
                msg = msg.replace("<code>","").replace("</code>","")
                msg = msg.replace("<pre>","").replace("</pre>","")
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
            "symbol":       symbol,
            "granularity":  g,
            "limit":        str(limit),
            "productType":  "usdt-futures",
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

def get_funding(symbol):
    """Ambil funding rate terkini. Guard: cek data["data"] tidak kosong."""
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/current-fund-rate",
        params={"symbol": symbol, "productType": "usdt-futures"},
    )
    if data and data.get("code") == "00000":
        try:
            d_list = data.get("data") or []
            if d_list:
                return float(d_list[0].get("fundingRate", 0))
        except Exception:
            pass
    return 0.0

def get_btc_candles_cached(limit=48):
    """Cache candle BTCUSDT 1h selama 5 menit — hemat ~100 API call per scan."""
    global _btc_candles_cache
    if time.time() - _btc_candles_cache["ts"] < 300 and _btc_candles_cache["data"]:
        return _btc_candles_cache["data"]
    candles = get_candles("BTCUSDT", "1h", limit)
    if candles:
        _btc_candles_cache = {"ts": time.time(), "data": candles}
    return candles

def get_funding_stats(symbol):
    """Hitung statistik funding dari snapshot in-memory."""
    snaps = _funding_snapshots.get(symbol, [])
    if len(snaps) < 2:
        return None
    all_rates = [s["funding"] for s in snaps]
    last6     = all_rates[-6:]
    avg6      = sum(last6) / len(last6)
    cumul     = sum(last6)
    neg_pct   = sum(1 for f in last6 if f < 0) / len(last6) * 100
    streak    = 0
    for f in reversed(all_rates):
        if f < 0:
            streak += 1
        else:
            break
    return {
        "avg":          avg6,
        "cumulative":   cumul,
        "neg_pct":      neg_pct,
        "streak":       streak,
        "basis":        all_rates[-1] * 100,
        "current":      all_rates[-1],
        "sample_count": len(all_rates),
    }

def get_open_interest(symbol):
    """Ambil Open Interest dari Bitget Futures API. Guard: cek list tidak kosong."""
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
                if oi_list:
                    oi = float(oi_list[0].get("openInterest", 0))
                else:
                    oi = float(d.get("openInterest", d.get("holdingAmount", 0)))
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
    FIX v18: Hitung % perubahan OI menggunakan snapshot yang sudah di-load dari disk.
    Sebelumnya (v15.7) _oi_snapshot hanya in-memory sehingga selalu is_new=True
    di setiap restart — menyebabkan energy_buildup dan OI scoring tidak pernah aktif.
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
#  📊  INDIKATOR TEKNIKAL
# ══════════════════════════════════════════════════════════════════════════════

def _calc_ema_series(values, period):
    if len(values) < period:
        return None
    alpha   = 2.0 / (period + 1)
    ema_val = sum(values[:period]) / period
    for v in values[period:]:
        ema_val = alpha * v + (1.0 - alpha) * ema_val
    return ema_val

def calc_ema_gap(candles, period=20):
    """EMA gap = close / EMA(period)."""
    if len(candles) < period + 1:
        return 1.0
    closes  = [c["close"] for c in candles]
    ema_val = _calc_ema_series(closes, period)
    if ema_val is None or ema_val == 0:
        return 1.0
    return candles[-1]["close"] / ema_val

def calc_bbw(candles, period=20):
    """BB Width (desimal) dan posisi harga dalam band (0=bawah, 1=atas)."""
    if len(candles) < period:
        return 0.0, 0.5
    closes   = [c["close"] for c in candles[-period:]]
    mean     = sum(closes) / period
    variance = sum((x - mean) ** 2 for x in closes) / period
    std      = math.sqrt(variance)
    bb_upper = mean + 2 * std
    bb_lower = mean - 2 * std
    bbw      = (bb_upper - bb_lower) / mean if mean > 0 else 0.0
    if bb_upper == bb_lower:
        bb_pct = 0.5
    else:
        bb_pct = (candles[-1]["close"] - bb_lower) / (bb_upper - bb_lower)
    return bbw, bb_pct

def calc_atr_pct(candles, period=14):
    """ATR sebagai % dari harga close terakhir."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1:
            break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return 0.0
    atr = sum(trs) / len(trs)
    cur = candles[-1]["close"]
    return (atr / cur * 100) if cur > 0 else 0.0

def calc_atr_abs(candles, period=14):
    """ATR dalam nilai absolut untuk kalkulasi entry/SL."""
    if len(candles) < period + 1:
        return candles[-1]["close"] * 0.01
    trs = []
    for i in range(1, period + 1):
        idx = len(candles) - i
        if idx < 1:
            break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else candles[-1]["close"] * 0.01

def _atr_n(candles, n):
    """Helper: hitung ATR untuk n candle terakhir."""
    trs = []
    for i in range(1, min(n + 1, len(candles))):
        idx = len(candles) - i
        if idx < 1:
            break
        h, l, pc = candles[idx]["high"], candles[idx]["low"], candles[idx-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / len(trs) if trs else 0.0

def calc_atr_contracting(candles):
    """
    Deteksi kompresi volatilitas: ATR jangka pendek < ATR jangka panjang.
    ATR_short (6c) / ATR_long (24c) < threshold = energi menumpuk sebelum ekspansi.
    """
    atr_s = _atr_n(candles, CONFIG["accum_atr_lookback_short"])   # 6 candle
    atr_l = _atr_n(candles, CONFIG["accum_atr_lookback_long"])    # 24 candle
    if atr_l <= 0:
        return {"is_contracting": False, "ratio": 1.0}
    ratio = atr_s / atr_l
    return {
        "is_contracting": ratio <= CONFIG["atr_contract_ratio"],
        "ratio":          round(ratio, 3),
    }

def calc_vwap(candles, lookback=24):
    """VWAP rolling 24 candle."""
    n = min(lookback, len(candles))
    if n == 0:
        return candles[-1]["close"] if candles else 0.0
    recent = candles[-n:]
    cum_tv = sum((c["high"] + c["low"] + c["close"]) / 3 * c["volume"] for c in recent)
    cum_v  = sum(c["volume"] for c in recent)
    return (cum_tv / cum_v) if cum_v > 0 else candles[-1]["close"]

def get_rsi(candles, period=14):
    """RSI Wilder."""
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
    return 100 - (100 / (1 + avg_g / avg_l))

def detect_bos_up(candles, lookback=None):
    """
    Break of Structure ke atas.
    FIX v18: lookback default dinaikkan 3 → 8 candle.
    Lookback 3 candle = 3 jam saja → BOS trivial, hampir selalu True.
    Lookback 8 candle = lebih bermakna secara struktur.
    """
    if lookback is None:
        lookback = CONFIG["bos_lookback"]
    if len(candles) < lookback + 1:
        return False, 0.0
    prev_highs = [c["high"] for c in candles[-(lookback + 1):-1]]
    bos_level  = max(prev_highs)
    return candles[-1]["close"] > bos_level, bos_level

def higher_low_detected(candles):
    """
    Higher Low: low candle terakhir > min(low N candle sebelumnya).
    FIX v18: lookback dinaikkan 6 → 16 candle.
    Lookback 6 candle rentan noise. 16 candle lebih bermakna secara teknikal.
    """
    lookback = CONFIG["higher_low_lookback"]
    if len(candles) < lookback + 1:
        return False
    lows = [c["low"] for c in candles[-(lookback + 1):]    ]
    return lows[-1] > min(lows[:-1])

def calc_swing_range_position(candles, lookback=48):
    """
    Hitung posisi harga saat ini dalam swing range lookback candle.
    Return 0.0 (bawah = accumulation zone) hingga 1.0 (atas = distribusi zone).
    """
    n      = min(lookback, len(candles))
    recent = candles[-n:]
    if not recent:
        return 0.5
    swing_low  = min(c["low"]  for c in recent)
    swing_high = max(c["high"] for c in recent)
    swing_range = swing_high - swing_low
    if swing_range <= 0:
        return 0.5
    price = candles[-1]["close"]
    pos   = (price - swing_low) / swing_range
    return round(min(max(pos, 0.0), 1.0), 3)

def calc_candle_direction_ratio(candles, lookback=6):
    """
    Rasio candle bullish dalam lookback candle terakhir (0.0–1.0).
    Candle bullish = close > open.
    """
    if len(candles) < lookback:
        return 0.5
    recent   = candles[-lookback:]
    bullish  = sum(1 for c in recent if c["close"] >= c["open"])
    return bullish / len(recent)

# ══════════════════════════════════════════════════════════════════════════════
#  📊  NEW v18 INDICATORS
# ══════════════════════════════════════════════════════════════════════════════

def calc_volume_spike(candles):
    """
    v18 — Volume Spike Detection (Phase 1 pre-pump).
    Baseline = avg 20 candle, current = avg 3 candle terbaru.
    Tier: 1.5x→+8 | 2x→+15 | 3x→+22
    """
    window = CONFIG["vol_spike_window"]
    if len(candles) < window + 3:
        return {"ratio": 0.0, "score": 0, "label": "Data kurang", "tier": 0,
                "current_vol": 0.0, "avg_vol": 0.0}
    current_vol      = sum(c["volume_usd"] for c in candles[-3:]) / 3
    baseline_candles = candles[-(window + 3):-3]
    avg_vol          = sum(c["volume_usd"] for c in baseline_candles) / len(baseline_candles) if baseline_candles else 0.0
    if avg_vol <= 0:
        return {"ratio": 0.0, "score": 0, "label": "Vol baseline 0", "tier": 0,
                "current_vol": current_vol, "avg_vol": 0.0}
    ratio = current_vol / avg_vol
    if ratio >= CONFIG["vol_spike_high"]:
        return {"ratio": round(ratio,2), "score": CONFIG["score_vol_spike_high"],
                "label": f"🚀 Volume Spike {ratio:.1f}x — PUMP LIKELY (Phase 1 kuat)", "tier": 3,
                "current_vol": current_vol, "avg_vol": avg_vol}
    elif ratio >= CONFIG["vol_spike_mid"]:
        return {"ratio": round(ratio,2), "score": CONFIG["score_vol_spike_mid"],
                "label": f"📈 Volume Spike {ratio:.1f}x — Akumulasi kuat (Phase 1)", "tier": 2,
                "current_vol": current_vol, "avg_vol": avg_vol}
    elif ratio >= CONFIG["vol_spike_low"]:
        # v26: use calibrated score (6) aligned with spec
        return {"ratio": round(ratio,2),
                "score": CONFIG.get("score_vol_spike_low_v26", CONFIG["score_vol_spike_low"]),
                "label": f"📊 Volume Spike {ratio:.1f}x — Early interest (Phase 1)", "tier": 1,
                "current_vol": current_vol, "avg_vol": avg_vol}
    return {"ratio": round(ratio,2), "score": 0, "label": f"Volume normal {ratio:.1f}x", "tier": 0,
            "current_vol": current_vol, "avg_vol": avg_vol}


def calc_micro_momentum(candles_5m):
    """
    FIX v18 #3 — Micro Momentum Engine (5m/15m/1h).

    Pump altcoin sering terjadi dalam 5−20 menit.
    Menggunakan candle 5m untuk deteksi momentum jauh lebih responsif:

      mom_5m  = % change candle 5m terbaru vs 1 candle sebelumnya
      mom_15m = % change rata-rata 3 candle 5m terakhir (= 15 menit)
      mom_1h  = % change rata-rata 12 candle 5m terakhir (= 1 jam)

    Formula percepatan:
      accel = mom_5m × 2 + mom_15m − mom_1h

    Jika accel > 0 → momentum sedang menguat (pre-pump signal)
    """
    if len(candles_5m) < 13:
        return {
            "mom_5m": 0.0, "mom_15m": 0.0, "mom_1h": 0.0,
            "accel": 0.0, "score": 0, "label": "Data 5m kurang",
            "is_accelerating": False, "is_strong": False,
        }

    closes = [c["close"] for c in candles_5m]

    # mom_5m: % change 1 candle 5m terbaru
    mom_5m = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] > 0 else 0.0

    # mom_15m: avg % change 3 candle terakhir
    c3 = closes[-4] if len(closes) >= 4 else closes[0]
    mom_15m = (closes[-1] - c3) / c3 / 3 if c3 > 0 else 0.0

    # mom_1h: avg % change 12 candle terakhir
    c12 = closes[-13] if len(closes) >= 13 else closes[0]
    mom_1h = (closes[-1] - c12) / c12 / 12 if c12 > 0 else 0.0

    # acceleration formula
    accel = mom_5m * 2 + mom_15m - mom_1h

    strong_threshold = CONFIG["micro_accel_strong"]

    if accel > strong_threshold:
        score = CONFIG["score_micro_accel"]
        label = (f"⚡ Micro Momentum KUAT — 5m:{mom_5m*100:+.3f}% "
                 f"15m:{mom_15m*100:+.3f}% 1h:{mom_1h*100:+.3f}% "
                 f"accel:{accel*100:+.3f}%")
        is_accel = True
        is_strong = True
    elif accel > 0:
        score = CONFIG["score_micro_accel_pos"]
        label = (f"📈 Micro Momentum positif — accel:{accel*100:+.3f}% "
                 f"(5m:{mom_5m*100:+.3f}%)")
        is_accel = True
        is_strong = False
    else:
        score = 0
        label = f"Micro momentum flat/negatif ({accel*100:+.3f}%)"
        is_accel = False
        is_strong = False

    return {
        "mom_5m":          round(mom_5m, 6),
        "mom_15m":         round(mom_15m, 6),
        "mom_1h":          round(mom_1h, 6),
        "accel":           round(accel, 6),
        "score":           score,
        "label":           label,
        "is_accelerating": is_accel,
        "is_strong":       is_strong,
    }


def calc_momentum_acceleration(candles):
    """
    1h momentum acceleration (dipertahankan sebagai secondary signal).
    Digunakan sebagai input pump probability dan timing model.
    """
    if len(candles) < 7:
        return {"acceleration": 0.0, "score": 0, "label": "Data kurang",
                "is_accelerating": False, "is_strong": False,
                "momentum1": 0.0, "momentum2": 0.0}
    price_now = candles[-1]["close"]
    price_3h  = candles[-4]["close"]
    price_6h  = candles[-7]["close"]
    if price_3h <= 0 or price_6h <= 0:
        return {"acceleration": 0.0, "score": 0, "label": "Data invalid",
                "is_accelerating": False, "is_strong": False,
                "momentum1": 0.0, "momentum2": 0.0}
    momentum1    = (price_now - price_3h) / price_3h
    momentum2    = (price_3h  - price_6h) / price_6h
    acceleration = momentum1 - momentum2
    strong_th    = CONFIG["accel_strong_threshold"]
    if acceleration > strong_th:
        score, is_accel, is_strong = CONFIG["score_accel_strong"], True, True
        label = (f"⚡ 1h Momentum Accel KUAT {acceleration*100:+.2f}% — "
                 f"m1={momentum1*100:+.2f}% > m2={momentum2*100:+.2f}%")
    elif acceleration > 0:
        score, is_accel, is_strong = CONFIG["score_accel_positive"], True, False
        label = f"📈 1h Momentum Accel {acceleration*100:+.2f}%"
    else:
        score, is_accel, is_strong = 0, False, False
        label = f"Momentum 1h melambat {acceleration*100:+.2f}%"
    return {
        "acceleration":    round(acceleration, 5),
        "momentum1":       round(momentum1, 5),
        "momentum2":       round(momentum2, 5),
        "score":           score,
        "label":           label,
        "is_accelerating": is_accel,
        "is_strong":       is_strong,
    }


def calc_buy_pressure(candles_15m):
    """
    v18 — Buy Pressure dari 15m candles.
    Estimasi buy_volume dari posisi close dalam candle range.
    Tier: >55%→+6 | >65%→+12 | >75%→+20
    """
    window = CONFIG["buy_pressure_window"]
    if len(candles_15m) < window:
        return {"buy_ratio": 0.5, "buy_pct": 50.0, "score": 0,
                "label": "Data 15m kurang", "is_bullish": False, "tier": 0}
    recent    = candles_15m[-window:]
    total_vol = sum(c["volume"] for c in recent)
    if total_vol <= 0:
        return {"buy_ratio": 0.5, "buy_pct": 50.0, "score": 0,
                "label": "Volume 15m nol", "is_bullish": False, "tier": 0}
    buy_vol = 0.0
    for c in recent:
        rng = c["high"] - c["low"]
        frac = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
        buy_vol += c["volume"] * frac
    buy_ratio = buy_vol / total_vol
    if buy_ratio >= CONFIG["buy_pressure_high"]:
        return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                "score": CONFIG["score_buy_pressure_high"],
                "label": f"🐳 Buy Pressure {buy_ratio*100:.0f}% — PUMP PHASE", "is_bullish": True, "tier": 3}
    elif buy_ratio >= CONFIG["buy_pressure_mid"]:
        return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                "score": CONFIG["score_buy_pressure_mid"],
                "label": f"💰 Buy Pressure {buy_ratio*100:.0f}% — Whale activity", "is_bullish": True, "tier": 2}
    elif buy_ratio >= CONFIG["buy_pressure_low"]:
        return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                "score": CONFIG["score_buy_pressure_low"],
                "label": f"📦 Buy Pressure {buy_ratio*100:.0f}% — Accumulation", "is_bullish": True, "tier": 1}
    return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
            "score": 0, "label": f"Buy Pressure rendah {buy_ratio*100:.0f}%",
            "is_bullish": False, "tier": 0}


def detect_whale_order(candles_15m, oi_data):
    """
    FIX v18 #2 — Whale Detection Diperkuat.

    v18: volume > 5× avg (terlalu kasar)
    v18: whale = vol > 3× AND buy_ratio > 65% AND OI naik
    Ini filter market maker, liquidation spike, dan fake pump.
    """
    window = 10
    if len(candles_15m) < window + 1:
        return {"is_whale": False, "mult": 0.0, "score": 0,
                "label": "Data kurang", "confidence": "LOW"}

    recent_vol = candles_15m[-1]["volume"]
    avg_vol    = sum(c["volume"] for c in candles_15m[-(window+1):-1]) / window
    if avg_vol <= 0:
        return {"is_whale": False, "mult": 0.0, "score": 0,
                "label": "Avg vol 0", "confidence": "LOW"}

    mult = recent_vol / avg_vol

    # Hitung buy ratio candle terakhir
    c = candles_15m[-1]
    rng = c["high"] - c["low"]
    buy_frac = (c["close"] - c["low"]) / rng if rng > 0 else 0.5

    # Cek OI naik
    oi_rising = (not oi_data.get("is_new", True) and
                 oi_data.get("change_pct", 0) >= CONFIG["whale_oi_change_min"])

    cond_vol      = mult >= CONFIG["whale_vol_mult_v18"]       # vol > 3x
    cond_buy      = buy_frac >= CONFIG["whale_buy_ratio_min"]  # buy > 65%
    cond_oi       = oi_rising                                   # OI naik

    n_cond = sum([cond_vol, cond_buy, cond_oi])

    if n_cond >= 3:
        # Semua 3 kondisi: whale confirmed
        score      = CONFIG["score_whale_v18"]
        confidence = "HIGH"
        label      = (f"🐳 WHALE CONFIRMED {mult:.1f}x vol + buy {buy_frac*100:.0f}% "
                      f"+ OI naik — big player masuk!")
        is_whale   = True
    elif n_cond == 2 and cond_vol and cond_buy:
        # Vol + Buy (tanpa OI data): probable whale
        score      = CONFIG["score_whale_v18"] // 2
        confidence = "MEDIUM"
        label      = (f"🐳 Whale Probable {mult:.1f}x vol + buy {buy_frac*100:.0f}% "
                      f"(OI belum konfirmasi)")
        is_whale   = True
    else:
        score, is_whale, confidence = 0, False, "LOW"
        label = f"Order biasa {mult:.1f}x (vol={cond_vol}, buy={cond_buy}, OI={cond_oi})"

    return {
        "is_whale":   is_whale,
        "mult":       round(mult, 1),
        "buy_frac":   round(buy_frac, 3),
        "oi_rising":  oi_rising,
        "score":      score,
        "label":      label,
        "confidence": confidence,
        "n_cond":     n_cond,
    }


def detect_fake_pump(candles, buy_pressure_ratio, oi_data):
    """
    FIX v18 #7 — Fake Pump Filter Diperkuat.

    v18: price_up AND buy<50% → fake (terlalu sederhana)
    v18: fake_score berbasis 4 kondisi bertingkat:
      1. price_spike: harga naik > 0.5% dalam 3h
      2. vol_spike: volume naik tapi buy rendah
      3. OI_flat: OI tidak naik (tidak ada posisi baru)
      4. sell_pressure: buy_ratio < 48%

    Penalti proporsional: 2 kondisi→−5, 3 kondisi→−12, 4 kondisi→−20
    """
    if len(candles) < 4:
        return {"is_fake": False, "penalty": 0, "label": "", "n_cond": 0}

    price_now = candles[-1]["close"]
    price_3h  = candles[-4]["close"]
    if price_3h <= 0:
        return {"is_fake": False, "penalty": 0, "label": "", "n_cond": 0}

    price_chg_3h = (price_now - price_3h) / price_3h

    # Kondisi 1: Price spike
    cond_price_spike = price_chg_3h > CONFIG["fake_price_spike_min"]

    # Kondisi 2: Volume anomali (vol naik tapi bukan buy)
    vol_anomaly = False
    if len(candles) >= 8:
        avg_vol = sum(c["volume_usd"] for c in candles[-8:-3]) / 5
        cur_vol = sum(c["volume_usd"] for c in candles[-3:]) / 3
        vol_anomaly = cur_vol > avg_vol * 1.5 and buy_pressure_ratio < 0.55

    # Kondisi 3: OI flat (tidak ada posisi baru = tidak ada institutional)
    oi_flat = (oi_data.get("is_new", True) or
               abs(oi_data.get("change_pct", 0)) < CONFIG["fake_oi_flat_max"])

    # Kondisi 4: Sell pressure dominan
    sell_pressure = buy_pressure_ratio < CONFIG["fake_sell_press_max"]

    # Hitung n kondisi yang aktif
    conditions = [cond_price_spike, vol_anomaly, oi_flat, sell_pressure]
    n_cond = sum(conditions)

    # Hanya flag sebagai fake jika minimal price_spike + 1 kondisi lain
    if not cond_price_spike or n_cond < 2:
        return {"is_fake": False, "penalty": 0, "label": "", "n_cond": n_cond}

    if n_cond >= 4:
        penalty = CONFIG["fake_penalty_severe"]
        severity = "SEVERE"
    elif n_cond == 3:
        penalty = CONFIG["fake_penalty_strong"]
        severity = "STRONG"
    else:
        penalty = CONFIG["fake_penalty_mild"]
        severity = "MILD"

    label = (
        f"⚠️ FAKE PUMP {severity} ({n_cond}/4 cond) — "
        f"harga +{price_chg_3h*100:.2f}% tapi: "
        f"vol_anomaly={vol_anomaly}, OI_flat={oi_flat}, "
        f"sell_press={sell_pressure} (buy={buy_pressure_ratio*100:.0f}%)"
    )

    return {
        "is_fake":           n_cond >= 2,
        "penalty":           penalty,
        "label":             label,
        "n_cond":            n_cond,
        "severity":          severity,
        "price_chg_3h":      round(price_chg_3h * 100, 2),
        "cond_price_spike":  cond_price_spike,
        "cond_vol_anomaly":  vol_anomaly,
        "cond_oi_flat":      oi_flat,
        "cond_sell_pressure": sell_pressure,
    }


def calc_pump_probability_v18(vol_spike, buy_press, micro_mom, oi_data, bb_squeeze,
                               atr_contracting, fake_pump):
    """
    FIX v18 #1 — Probability Model Berbasis Feature (bukan raw score).

    v18: prob = logistic(score) — score adalah penjumlahan heuristik,
         bisa tinggi tanpa pump nyata.

    v18: z = Σ(bobot × feature_normalized)
         prob = 1 / (1 + exp(-z))

    Feature normalization:
      - vol_spike_ratio: [0,3+] → normalized ke [0,1] max=3
      - buy_pressure: [0,1] → already normalized
      - micro_accel: [-∞,+∞] → clamp ke [-0.01, 0.01] → scale ke [-1,1]
      - OI_change: [-∞,+∞] → clamp ke [-10,10] → scale
      - bb_squeeze: boolean [0,1]
      - atr_contract: boolean [0,1]
      - fake_pump: boolean [0,1]
    """
    # Normalize features ke [0, 1] atau [-1, 1]
    vs_norm   = min(vol_spike.get("ratio", 0) / 3.0, 1.0)
    bp_norm   = buy_press.get("buy_ratio", 0.5)
    ma_raw    = micro_mom.get("accel", 0)
    ma_norm   = max(-1.0, min(1.0, ma_raw / 0.01))   # scale: 1% accel = max
    oi_raw    = oi_data.get("change_pct", 0) if not oi_data.get("is_new", True) else 0
    oi_norm   = max(-1.0, min(1.0, oi_raw / 10.0))   # scale: 10% OI = max
    bb_norm   = 1.0 if bb_squeeze else 0.0
    atr_norm  = 1.0 if atr_contracting else 0.0
    fake_norm = 1.0 if fake_pump.get("is_fake", False) else 0.0

    w = CONFIG
    z = (w["prob_w_vol_spike"]  * vs_norm
       + w["prob_w_buy_press"]  * bp_norm
       + w["prob_w_mom_accel"]  * ma_norm
       + w["prob_w_oi_change"]  * oi_norm
       + w["prob_w_bb_squeeze"] * bb_norm
       + w["prob_w_atr_contract"] * atr_norm
       + w["prob_w_fake_pump"]  * fake_norm)

    # Logistic transform — z adalah raw feature sum, bukan score
    # Center sekitar z=1.0 (threshold decision boundary)
    prob = 1.0 / (1.0 + math.exp(-(z - 1.0) * 2.5))
    return round(prob * 100, 1)


def calc_pump_timing_eta(vol_accel, buy_ratio, oi_data, micro_mom):
    """
    FIX v18 #6 — Pump Timing Model / ETA.

    timing_score = 0.4×vol_accel + 0.3×buy_ratio + 0.2×OI_accel + 0.1×mom_micro

    Output ETA berdasarkan timing_score:
      > 0.75 → 5 menit
      > 0.55 → 10 menit
      > 0.35 → 30 menit
      > 0.20 → 60 menit
      else   → > 1 jam (setup fase awal)
    """
    # Normalize inputs ke [0, 1]
    va_norm  = min(max(vol_accel, 0) / 2.0, 1.0)   # vol accel 0-200%
    bp_norm  = min(max(buy_ratio, 0), 1.0)
    oi_raw   = oi_data.get("change_pct", 0) if not oi_data.get("is_new", True) else 0
    oi_norm  = min(max(oi_raw, 0) / 10.0, 1.0)     # OI accel 0-10%
    ma_raw   = micro_mom.get("accel", 0)
    ma_norm  = min(max(ma_raw, 0) / 0.01, 1.0)     # micro accel 0-1%

    w = CONFIG
    timing = (w["timing_w_vol_accel"] * va_norm
            + w["timing_w_buy_ratio"] * bp_norm
            + w["timing_w_oi_accel"]  * oi_norm
            + w["timing_w_momentum"]  * ma_norm)

    if timing >= CONFIG["timing_eta_5min"]:
        eta = "~5 menit"
        eta_emoji = "🔥🔥🔥"
        urgency = "IMMINENT"
    elif timing >= CONFIG["timing_eta_10min"]:
        eta = "~10 menit"
        eta_emoji = "🔥🔥"
        urgency = "VERY SOON"
    elif timing >= CONFIG["timing_eta_30min"]:
        eta = "~30 menit"
        eta_emoji = "🔥"
        urgency = "SOON"
    elif timing >= CONFIG["timing_eta_60min"]:
        eta = "~60 menit"
        eta_emoji = "⏳"
        urgency = "BUILDING"
    else:
        eta = "> 1 jam"
        eta_emoji = "📦"
        urgency = "EARLY SETUP"

    return {
        "timing_score": round(timing, 3),
        "eta":          eta,
        "eta_emoji":    eta_emoji,
        "urgency":      urgency,
        "va_norm":      round(va_norm, 3),
        "bp_norm":      round(bp_norm, 3),
        "oi_norm":      round(oi_norm, 3),
        "ma_norm":      round(ma_norm, 3),
    }


def get_alert_level_v19(score):
    """v18: threshold sama dengan v18."""
    if score >= CONFIG["score_strong_alert"]:
        return "STRONG ALERT"
    elif score >= CONFIG["score_alert"]:
        return "ALERT"
    elif score >= CONFIG["score_watchlist"]:
        return "WATCHLIST"
    return "IGNORE"


def calc_market_regime(candles, vwap, buy_ratio, vol_ratio, rsi, price_pos):
    """
    v18 — Market Regime: PULLBACK / SWEEP / BREAKOUT / NEUTRAL.
    Extends v18 dengan mode PULLBACK dan SWEEP.
    """
    price_now = candles[-1]["close"]

    above_vwap   = price_now > vwap
    buy_strong   = buy_ratio >= CONFIG["breakout_buy_ratio_min"]
    vol_strong   = vol_ratio >= CONFIG["breakout_vol_ratio_min"]
    below_vwap   = price_now < vwap
    oversold     = rsi < CONFIG["mean_rev_rsi_max"]
    low_range    = price_pos < CONFIG["mean_rev_range_max"]

    if above_vwap and buy_strong and vol_strong:
        return "BREAKOUT"
    elif below_vwap and oversold and low_range:
        return "PULLBACK"
    elif below_vwap and low_range:
        return "SWEEP"
    return "NEUTRAL"


def calc_entry_v18(candles, vwap, price_now, atr_abs_val, market_regime, sr,
                   rsi, buy_ratio, vol_ratio, price_pos, alert_level, bos_level,
                   liq_sweep):
    """
    FIX v18 #4 & #5 — Entry Engine Diperbaiki + TP Diperlebar.

    Entry modes:
      PULLBACK  → entry = VWAP − 0.3×ATR  (tunggu pullback)
      SWEEP     → entry = liq_sweep_low + 0.2×ATR
      BREAKOUT  → entry = breakout_retest + 0.15×ATR
      NEUTRAL   → entry = support + 0.2×ATR

    TP v18 (diperlebar untuk altcoin pump 5−20%):
      TP1 = 1.5×ATR | TP2 = 3×ATR | TP3 = 5×ATR
      + TP3 cek liquidity void (area gap resistance)
    """
    atr = atr_abs_val
    atr_pct_now = (atr / price_now * 100) if price_now > 0 else 2.0

    # ── Entry berbasis regime ─────────────────────────────────────────────────
    if market_regime == "PULLBACK":
        entry        = vwap - atr * CONFIG["entry_pullback_atr_mult"]
        entry_reason = f"PULLBACK — VWAP {_fmt_price(vwap)} − 0.3×ATR"
        if entry >= price_now:          # jika entry > harga sekarang, pakai harga
            entry = price_now * 0.998

    elif market_regime == "SWEEP" and liq_sweep and liq_sweep.get("is_sweep"):
        sweep_low    = liq_sweep.get("sweep_low", price_now * 0.98)
        entry        = sweep_low + atr * CONFIG["entry_sweep_atr_mult"]
        entry_reason = f"SWEEP — Low {_fmt_price(sweep_low)} + 0.2×ATR"
        if entry > price_now:
            entry = price_now * 0.998

    elif market_regime == "BREAKOUT":
        res_levels = []
        if sr and sr.get("resistance"):
            res_levels = [rv["level"] for rv in sr["resistance"]
                          if rv["level"] > price_now * 0.998]
        if res_levels:
            breakout_lvl = min(res_levels)
            entry        = breakout_lvl * (1.0 + CONFIG["entry_retest_buffer"])
            entry_reason = f"BREAKOUT retest — R1 {_fmt_price(breakout_lvl)} + buffer"
        elif bos_level > 0 and bos_level < price_now * 1.05:
            entry        = bos_level * (1.0 + CONFIG["entry_bos_buffer"])
            entry_reason = "BREAKOUT — BOS retest"
        else:
            entry        = price_now * 1.001
            entry_reason = "BREAKOUT — market"

    else:  # NEUTRAL
        sup_levels = []
        if sr and sr.get("support"):
            sup_levels = [sv["level"] for sv in sr["support"]
                          if sv["level"] < price_now]
        if sup_levels:
            support      = max(sup_levels)
            entry        = support + atr * CONFIG["entry_mean_rev_atr_mult"]
            entry_reason = f"NEUTRAL — S1 {_fmt_price(support)} + 0.2×ATR"
        else:
            entry        = vwap - atr * 0.1   # slight below vwap
            entry_reason = "NEUTRAL — VWAP basis"
        if entry > price_now:
            entry = price_now * 0.999

    # ── SL ────────────────────────────────────────────────────────────────────
    sl_mult = CONFIG["sl_atr_volatile"] if atr_pct_now > 3.0 else CONFIG["sl_atr_base"]
    sl      = entry - atr * sl_mult
    sl      = max(sl, entry * (1.0 - CONFIG["max_sl_pct"] / 100.0))
    sl      = min(sl, entry * (1.0 - CONFIG["min_sl_pct"] / 100.0))
    if sl >= entry:
        sl = entry * 0.975

    # ── TP v18: 1.5× / 3× / 5× ATR (diperlebar) ──────────────────────────────
    tp1 = entry + atr * CONFIG["tp1_atr_mult"]   # 1.5×
    tp2 = entry + atr * CONFIG["tp2_atr_mult"]   # 3.0×
    tp3 = entry + atr * CONFIG["tp3_atr_mult"]   # 5.0×

    # TP3 cek liquidity void: area tanpa resistance signifikan
    if sr and sr.get("resistance"):
        res_above = sorted([rv["level"] for rv in sr["resistance"] if rv["level"] > entry])
        # Jika ada gap besar antar resistance, pakai batas atas gap sebagai TP3
        if len(res_above) >= 2:
            gap = res_above[1] - res_above[0]
            if gap / res_above[0] > 0.05:   # gap > 5% = liquidity void
                tp3 = max(tp3, res_above[1])
        if res_above and res_above[0] > tp1:
            tp1 = max(tp1, res_above[0])
        if len(res_above) >= 2 and res_above[1] > tp2:
            tp2 = max(tp2, res_above[1])

    tp1 = max(tp1, entry * 1.008)
    tp2 = max(tp2, tp1   * 1.01)
    tp3 = max(tp3, tp2   * 1.02)

    risk = entry - sl
    rr1  = round((tp1 - entry) / risk, 1) if risk > 0 else 0.0
    rr2  = round((tp2 - entry) / risk, 1) if risk > 0 else 0.0
    rr3  = round((tp3 - entry) / risk, 1) if risk > 0 else 0.0

    return {
        "entry":         round(entry, 8),
        "sl":            round(sl, 8),
        "sl_pct":        round((entry - sl) / entry * 100, 2),
        "t1":            round(tp1, 8),
        "t2":            round(tp2, 8),
        "t3":            round(tp3, 8),
        "rr":            rr1,
        "rr2":           rr2,
        "rr3":           rr3,
        "rr_str":        f"{rr1:.1f}",
        "rr2_str":       f"{rr2:.1f}",
        "rr3_str":       f"{rr3:.1f}",
        "vwap":          round(vwap, 8),
        "bos_level":     round(bos_level, 8),
        "alert_level":   alert_level,
        "gain_t1_pct":   round((tp1 - entry) / entry * 100, 1),
        "gain_t2_pct":   round((tp2 - entry) / entry * 100, 1),
        "gain_t3_pct":   round((tp3 - entry) / entry * 100, 1),
        "atr_abs":       round(atr, 8),
        "atr_pct":       round(atr_pct_now, 2),
        "sl_method":     entry_reason,
        "market_regime": market_regime,
        "trail_note":    "Trailing: TP1→SL=Entry | TP2→SL=TP1 | TP3 free run",
        "used_resistance": bool(sr and sr.get("resistance")),
        "t1_source":     f"ATR×{CONFIG['tp1_atr_mult']}",
        "t2_source":     f"ATR×{CONFIG['tp2_atr_mult']}",
        "t3_source":     f"ATR×{CONFIG['tp3_atr_mult']} / Liq Void",
        "atr_pct_abs":   round(atr / entry * 100, 2) if entry > 0 else 0.0,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📊  NEW v19 INDICATORS
# ══════════════════════════════════════════════════════════════════════════════

def calc_ema(candles, period):
    """
    STEP 6 v19 — Exponential Moving Average.
    Digunakan untuk trend filter EMA20/EMA50.
    """
    if len(candles) < period:
        return None
    closes = [c["close"] for c in candles]
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def calc_ema_trend(candles):
    """
    STEP 6 v19 — Trend Context Filter via EMA20/EMA50.

    Rules:
      - EMA20 < EMA50 → bearish bias → penalti score −8
      - price < EMA50 → strong downtrend → REJECT gate
        FIX 01 v22: OVERRIDE reject if EMA20 slope > 0 AND vol z > 1.5
                    (reversal pump setup — price below EMA50 temporarily)
    Returns dict dengan ema20, ema50, trend, should_reject, score_penalty.
    """
    ema20 = calc_ema(candles, CONFIG["ema_fast"])
    ema50 = calc_ema(candles, CONFIG["ema_slow"])
    price = candles[-1]["close"]

    if ema20 is None or ema50 is None:
        return {"ema20": None, "ema50": None, "trend": "UNKNOWN",
                "should_reject": False, "score_penalty": 0, "label": "EMA data kurang",
                "reversal_override": False}

    # FIX 01 v22: compute EMA20 slope for override check
    # Build minimal EMA20 series to get slope (3-candle lookback)
    if len(candles) >= 24:
        closes  = [c["close"] for c in candles]
        alpha20 = 2.0 / 21.0
        ema20_s = sum(closes[:20]) / 20
        ema20_series = [ema20_s]
        for v in closes[20:]:
            ema20_s = alpha20 * v + (1.0 - alpha20) * ema20_s
            ema20_series.append(ema20_s)
        ema20_slope_val = ema20_series[-1] - ema20_series[-4] if len(ema20_series) >= 4 else 0.0
    else:
        ema20_slope_val = 0.0

    if price < ema50:
        # FIX 01 v22: check if reversal override applies
        # Override: slope rising + we'll check z-score in master_score (flagged here)
        reversal_override = ema20_slope_val > CONFIG["ema50_override_slope_min"]
        if reversal_override:
            trend         = "REVERSAL_SETUP"
            should_reject = False    # override: do NOT reject
            penalty       = CONFIG["score_penalty_bearish"] // 2   # reduced penalty
            label         = (f"🔄 REVERSAL SETUP: price < EMA50 BUT slope↑ {ema20_slope_val:.6g} "
                             f"— override aktif (v22)")
        else:
            trend         = "DOWNTREND"
            should_reject = True
            penalty       = CONFIG["score_penalty_bearish"]
            label         = (f"📉 DOWNTREND: harga ${price:.4g} < EMA50 {ema50:.4g} "
                             f"— GATE GAGAL (strong downtrend)")
    elif ema20 < ema50:
        trend         = "BEARISH"
        should_reject = False
        penalty       = CONFIG["score_penalty_bearish"]
        reversal_override = False
        label         = (f"📉 Bearish bias: EMA20 {ema20:.4g} < EMA50 {ema50:.4g} "
                         f"— penalti {penalty}")
    elif ema20 > ema50 and price > ema20:
        trend         = "UPTREND"
        should_reject = False
        penalty       = 0
        reversal_override = False
        label         = f"📈 Uptrend: price > EMA20 {ema20:.4g} > EMA50 {ema50:.4g}"
    else:
        trend         = "NEUTRAL"
        should_reject = False
        penalty       = 0
        reversal_override = False
        label         = f"〰️ Neutral: EMA20 {ema20:.4g} / EMA50 {ema50:.4g}"

    return {
        "ema20":            round(ema20, 8),
        "ema50":            round(ema50, 8),
        "trend":            trend,
        "should_reject":    should_reject,
        "score_penalty":    penalty,
        "label":            label,
        "price_vs_ema50":   round((price - ema50) / ema50 * 100, 2) if ema50 > 0 else 0,
        "reversal_override": reversal_override if 'reversal_override' in dir() else False,
        "ema20_slope_val":  round(ema20_slope_val, 8),
    }


def calc_vol_zscore(candles):
    """
    STEP 10 v19 — Volume Z-Score.

    z = (vol_current − mean_vol) / std_vol

    Jika z > 3 → volume anomali ekstrem → boost volume_score.
    """
    window = CONFIG["vol_zscore_window"]
    if len(candles) < window + 1:
        return {"z": 0.0, "is_anomaly": False, "label": "Data kurang", "boost": 0}

    recent_vols  = [c["volume_usd"] for c in candles[-(window + 1):-1]]
    current_vol  = candles[-1]["volume_usd"]

    mean_v = sum(recent_vols) / len(recent_vols)
    var_v  = sum((v - mean_v) ** 2 for v in recent_vols) / len(recent_vols)
    std_v  = var_v ** 0.5

    if std_v <= 0:
        return {"z": 0.0, "is_anomaly": False, "label": "Std=0", "boost": 0}

    z = (current_vol - mean_v) / std_v
    is_anomaly = z > CONFIG["vol_zscore_boost"]

    label = (
        f"🔺 Volume Z-Score {z:.1f}σ — aktivitas tidak normal (anomaly)" if is_anomaly
        else f"Volume Z-Score {z:.1f}σ — normal"
    )
    return {
        "z":          round(z, 2),
        "is_anomaly": is_anomaly,
        "label":      label,
        "boost":      8 if z > 5 else (5 if is_anomaly else 0),
        "mean_vol":   mean_v,
        "std_vol":    std_v,
    }


def calc_micro_breakout(candles):
    """
    STEP 11 v19 — Micro Breakout Detection.

    Jika harga saat ini > highest_high dari N candle terakhir
    → breakout baru → skor tinggi.
    """
    lookback = CONFIG["micro_breakout_lookback"]
    if len(candles) < lookback + 1:
        return {"is_breakout": False, "highest_high": 0.0, "score": 0,
                "label": "Data kurang"}

    prev_candles  = candles[-(lookback + 1):-1]
    highest_high  = max(c["high"] for c in prev_candles)
    price_now     = candles[-1]["close"]
    is_breakout   = price_now > highest_high

    score = CONFIG["score_micro_breakout"] if is_breakout else 0
    label = (
        f"🚀 MICRO BREAKOUT: {price_now:.6g} > Highest {highest_high:.6g} "
        f"({lookback} candle)"
        if is_breakout else
        f"No breakout: {price_now:.6g} vs High {highest_high:.6g}"
    )
    return {
        "is_breakout":  is_breakout,
        "highest_high": round(highest_high, 8),
        "score":        score,
        "label":        label,
        "gap_pct":      round((price_now - highest_high) / highest_high * 100, 2),
    }


def calc_candle_imbalance(candles_15m):
    """
    STEP 12 v19 — Orderbook Imbalance Proxy.

    Proxy menggunakan candle body + wick ratio sebagai estimasi bid/ask pressure.
    imbalance = buy_vol_est / sell_vol_est
    Jika imbalance > 1.5 → bullish imbalance → boost score.
    """
    window = min(8, len(candles_15m))
    if window < 3:
        return {"imbalance": 1.0, "is_bullish": False, "score": 0,
                "label": "Data kurang"}

    recent = candles_15m[-window:]
    buy_vol = 0.0
    sell_vol = 0.0

    for c in recent:
        rng = c["high"] - c["low"]
        if rng <= 0:
            buy_vol += c["volume"] * 0.5
            sell_vol += c["volume"] * 0.5
            continue
        buy_frac  = (c["close"] - c["low"]) / rng
        sell_frac = 1.0 - buy_frac
        buy_vol  += c["volume"] * buy_frac
        sell_vol += c["volume"] * sell_frac

    if sell_vol <= 0:
        return {"imbalance": 2.0, "is_bullish": True, "score": 5,
                "label": "Full buy imbalance"}

    imbalance  = buy_vol / sell_vol
    is_bullish = imbalance > 1.5

    score = 5 if imbalance > 2.0 else (3 if is_bullish else 0)
    label = (
        f"📊 OB Imbalance {imbalance:.2f}x — buy dominan (proxy bullish)"
        if is_bullish else
        f"OB Imbalance {imbalance:.2f}x — balanced/bearish"
    )
    return {
        "imbalance":  round(imbalance, 3),
        "is_bullish": is_bullish,
        "score":      score,
        "label":      label,
    }


def detect_whale_accumulation(candles):
    """
    STEP 16 v19 — Whale Accumulation Detection.

    Pattern: vol naik + harga sideways + ATR menyempit
    → smart money akumulasi diam-diam sebelum pump.
    Berbeda dari whale_order (single candle) — ini deteksi pola multi-candle.
    """
    window = 12
    if len(candles) < window + 12:
        return {"is_accum": False, "score": 0, "label": "Data kurang"}

    recent   = candles[-window:]
    baseline = candles[-(window + 12):-window]

    # Vol naik
    avg_recent   = sum(c["volume_usd"] for c in recent)   / len(recent)
    avg_baseline = sum(c["volume_usd"] for c in baseline) / len(baseline)
    vol_rising   = avg_recent >= avg_baseline * CONFIG["whale_accum_vol_min"] if avg_baseline > 0 else False

    # Price sideways
    hi  = max(c["high"]  for c in recent)
    lo  = min(c["low"]   for c in recent)
    mid = (hi + lo) / 2
    range_pct    = (hi - lo) / mid * 100 if mid > 0 else 99.0
    price_sideways = range_pct <= CONFIG["whale_accum_range_max"]

    # ATR menyempit (kompresi)
    atr_r  = _atr_n(candles, window)
    atr_b  = _atr_n(candles[:-window], window) if len(candles) > window * 2 else atr_r
    atr_contracting = (atr_r / atr_b) <= CONFIG["whale_accum_atr_max"] if atr_b > 0 else False

    n_cond = sum([vol_rising, price_sideways, atr_contracting])
    is_accum = n_cond >= 2  # min 2 dari 3 kondisi

    score = CONFIG["score_whale_accum"] if n_cond >= 3 else (4 if n_cond == 2 else 0)

    if n_cond >= 3:
        label = (f"🐳 Whale Accumulation CONFIRMED — vol {avg_recent/avg_baseline:.1f}x, "
                 f"range {range_pct:.1f}%, ATR menyempit") if avg_baseline > 0 else "🐳 Whale Accum"
    elif n_cond == 2:
        label = f"🐳 Whale Accum PROBABLE ({n_cond}/3 kondisi)"
    else:
        label = f"No whale accum ({n_cond}/3)"

    return {
        "is_accum":        is_accum,
        "n_cond":          n_cond,
        "vol_rising":      vol_rising,
        "price_sideways":  price_sideways,
        "atr_contracting": atr_contracting,
        "range_pct":       round(range_pct, 2),
        "score":           score,
        "label":           label,
    }


def calc_ai_weighted_score(vol_spike, micro_mom, mom_accel, buy_press, liq_sweep,
                            micro_breakout, rsi, bb_squeeze, vol_zscore, energy,
                            candle_imbalance, whale_accum):
    """
    STEP 4 v19 — AI-Style Weighted Score Engine.

    Setiap komponen dinormalisasi ke [0,100] lalu dikalikan bobot:

    score = 0.30 * volume_score
          + 0.20 * acceleration_score
          + 0.20 * momentum_score
          + 0.15 * liquidity_score
          + 0.10 * breakout_score
          + 0.05 * rsi_score

    Return: weighted_score (0-100) dan breakdown per komponen.
    """
    # ── Volume score [0-100] ───────────────────────────────────────────────────
    vs_ratio = vol_spike.get("ratio", 0)
    vol_base = min(vs_ratio / 3.0, 1.0) * 60                  # vol spike max 60
    zboost   = min(vol_zscore.get("z", 0) / 5.0, 1.0) * 40   # z-score max 40
    volume_score = min(vol_base + zboost, 100.0)

    # ── Acceleration score [0-100] (5m micro + 1h secondary) ──────────────────
    ma_raw  = micro_mom.get("accel", 0)
    accel_5m = min(max(ma_raw / 0.01, 0), 1.0) * 70           # 5m micro max 70
    ma1h     = mom_accel.get("acceleration", 0)
    accel_1h = min(max(ma1h / 0.005, 0), 1.0) * 30            # 1h secondary max 30
    acceleration_score = min(accel_5m + accel_1h, 100.0)

    # ── Momentum score [0-100] (buy pressure + imbalance) ─────────────────────
    bp_ratio  = buy_press.get("buy_ratio", 0.5)
    bp_score  = max(bp_ratio - 0.5, 0) / 0.25 * 70            # 0.5→0, 0.75→70
    ob_score  = min(candle_imbalance.get("imbalance", 1.0) / 2.0, 1.0) * 30
    momentum_score = min(bp_score + ob_score, 100.0)

    # ── Liquidity score [0-100] (liq sweep + whale + energy) ──────────────────
    lq_sweep  = 30 if liq_sweep.get("is_sweep") else 0
    lq_whale  = 35 if whale_accum.get("is_accum") else 0
    lq_energy = 35 if energy.get("is_buildup") else 0
    liquidity_score = min(lq_sweep + lq_whale + lq_energy, 100.0)

    # ── Breakout score [0-100] ─────────────────────────────────────────────────
    bo_micro = 50 if micro_breakout.get("is_breakout") else 0
    bo_bb    = 30 if bb_squeeze else 0
    bo_gap   = min(abs(micro_breakout.get("gap_pct", 0)) / 2.0, 1.0) * 20
    breakout_score = min(bo_micro + bo_bb + bo_gap, 100.0)

    # ── RSI score [0-100] (ideal zone = highest) ──────────────────────────────
    if 45 <= rsi <= 58:
        rsi_score = 100.0          # perfect pre-pump zone
    elif 40 <= rsi < 45 or 58 < rsi <= 62:
        rsi_score = 70.0
    elif 35 <= rsi < 40:
        rsi_score = 40.0           # oversold — might bounce but not ideal
    elif rsi > 62:
        rsi_score = max(0.0, 100.0 - (rsi - 62) * 5)
    else:
        rsi_score = 20.0

    # ── Weighted sum ──────────────────────────────────────────────────────────
    w = CONFIG
    weighted = (
        w["wscore_volume"]    * volume_score
      + w["wscore_accel"]     * acceleration_score
      + w["wscore_momentum"]  * momentum_score
      + w["wscore_liquidity"] * liquidity_score
      + w["wscore_breakout"]  * breakout_score
      + w["wscore_rsi"]       * rsi_score
    )

    return {
        "weighted_score":      round(weighted, 1),
        "volume_score":        round(volume_score, 1),
        "acceleration_score":  round(acceleration_score, 1),
        "momentum_score":      round(momentum_score, 1),
        "liquidity_score":     round(liquidity_score, 1),
        "breakout_score":      round(breakout_score, 1),
        "rsi_score":           round(rsi_score, 1),
    }


def calc_pump_probability_v19(score_raw):
    """
    STEP 5 v19 — Logistic Probability Model.

    P(pump) = 1 / (1 + exp(-k * (score - threshold)))

    Parameters (dari audit):
      k = 0.08
      threshold = 55
    """
    k   = CONFIG["logistic_k"]
    thr = CONFIG["logistic_threshold"]
    prob = 1.0 / (1.0 + math.exp(-k * (score_raw - thr)))
    return round(prob * 100, 1)


def calc_wick_ratio(candles, lookback=3):
    """
    STEP 9 v19 — Wick Ratio Filter (Whale Trap Detection).

    wick_ratio = (high - close) / (high - low)

    Jika wick_ratio besar → ada distribusi/rejection di atas harga
    → kemungkinan whale trap → penalti / reject.
    """
    if len(candles) < lookback:
        return {"ratio": 0.0, "is_trap": False, "penalty": 0, "label": ""}

    recent = candles[-lookback:]
    ratios = []
    for c in recent:
        rng = c["high"] - c["low"]
        if rng > 0:
            ratios.append((c["high"] - c["close"]) / rng)
        else:
            ratios.append(0.0)

    avg_wick = sum(ratios) / len(ratios)
    is_trap  = avg_wick > CONFIG["wick_ratio_max"]
    penalty  = -8 if avg_wick > 0.55 else (-4 if is_trap else 0)
    label    = (
        f"⚠️ Wick Trap {avg_wick:.2f} — distribusi/rejection di atas harga"
        if is_trap else ""
    )
    return {
        "ratio":    round(avg_wick, 3),
        "is_trap":  is_trap,
        "penalty":  penalty,
        "label":    label,
        "ratios":   [round(r, 3) for r in ratios],
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🆕  NEW INDICATORS v20
# ══════════════════════════════════════════════════════════════════════════════

def calc_ema20_slope(candles):
    """
    PART 1 v20 — EMA20 Slope.
    Ensures upward momentum exists by measuring EMA20 direction.

    ema20_slope = ema20_current - ema20_3_candles_ago
    Condition: ema20_slope > 0

    Uses vectorized EMA calculation over the full series for accuracy.
    Returns slope, current/past EMA20, and boolean is_rising.
    """
    if len(candles) < 24:
        return {"slope": 0.0, "ema20_now": 0.0, "ema20_prev": 0.0,
                "is_rising": False, "score": 0, "label": "Data kurang untuk EMA20 slope"}

    closes = [c["close"] for c in candles]
    alpha  = 2.0 / 21.0
    # Build full EMA20 series (vectorized)
    ema_series = [sum(closes[:20]) / 20]
    for v in closes[20:]:
        ema_series.append(alpha * v + (1.0 - alpha) * ema_series[-1])

    ema_now  = ema_series[-1]
    lookback = CONFIG["ema20_slope_lookback"]
    ema_prev = ema_series[-1 - lookback] if len(ema_series) > lookback else ema_series[0]

    slope     = ema_now - ema_prev
    is_rising = slope > 0
    score     = CONFIG["score_ema20_slope"] if is_rising else 0
    label     = (f"📈 EMA20 Slope naik +{slope:.6g} ({lookback}c ago)"
                 if is_rising else f"📉 EMA20 Slope turun {slope:.6g}")

    return {
        "slope":     round(slope, 8),
        "ema20_now": round(ema_now, 8),
        "ema20_prev":round(ema_prev, 8),
        "is_rising": is_rising,
        "score":     score,
        "label":     label,
    }


def calc_vol_zscore_v20(candles, window=20):
    """
    PART 1 v20 — Volume Z-Score (stricter threshold: z > 1.5).
    Formula: z = (current_volume - mean_vol_20) / std_vol_20

    Condition: z > 1.5 for reversal confirmation
               z > 2.0 triggers dump filter (close < ema20 AND z > 2 → reject)

    Also computes bid_ask_ratio proxy from candle close/range position.
    """
    if len(candles) < window + 1:
        return {
            "z": 0.0, "mean": 0.0, "std": 0.0,
            "current_vol": 0.0, "is_spike": False,
            "bid_ask_ratio": 1.0, "label": "Data volume kurang",
        }

    vols     = [c["volume_usd"] for c in candles]
    cur_vol  = vols[-1]
    baseline = vols[-(window + 1):-1]   # last N before current (vectorized slice)
    mean_v   = sum(baseline) / window
    variance = sum((v - mean_v) ** 2 for v in baseline) / window
    std_v    = math.sqrt(variance) if variance > 0 else 1.0
    z        = (cur_vol - mean_v) / std_v

    # Bid/Ask ratio proxy from buy-fraction of recent candles (3-candle window)
    recent3 = candles[-3:]
    buy_vol  = 0.0
    sell_vol = 0.0
    for c in recent3:
        rng  = c["high"] - c["low"]
        frac = (c["close"] - c["low"]) / rng if rng > 0 else 0.5
        buy_vol  += c["volume_usd"] * frac
        sell_vol += c["volume_usd"] * (1.0 - frac)
    bid_ask_ratio = buy_vol / sell_vol if sell_vol > 0 else 1.0

    is_spike = z > CONFIG["vol_zscore_v20_min"]
    if z > CONFIG["vol_zscore_v20_strong"]:
        label = f"🔥 Vol Z-Score {z:.2f} (KUAT > {CONFIG['vol_zscore_v20_strong']}) — spike anomali"
    elif is_spike:
        label = f"📊 Vol Z-Score {z:.2f} > {CONFIG['vol_zscore_v20_min']} — volume di atas normal"
    else:
        label = f"Vol Z-Score {z:.2f} — normal"

    return {
        "z":             round(z, 3),
        "mean":          round(mean_v, 2),
        "std":           round(std_v, 2),
        "current_vol":   round(cur_vol, 2),
        "is_spike":      is_spike,
        "bid_ask_ratio": round(bid_ask_ratio, 3),
        "label":         label,
    }


def calc_ema200_distance(candles):
    """
    PART 1 v20 — Distance from EMA200.
    Prevents catching deep downtrend bounces.

    distance_ema200 = abs(price - ema200) / ema200
    Condition: distance_ema200 < 0.06 (within 6%)

    Returns ema200 value, distance, and whether price is near EMA200.
    """
    if len(candles) < 200:
        return {
            "ema200": 0.0, "distance": 1.0, "is_near": False,
            "above_ema200": False, "score": 0,
            "label": "Data < 200 candle (EMA200 tidak tersedia)",
        }

    closes = [c["close"] for c in candles]
    alpha  = 2.0 / 201.0
    ema200 = sum(closes[:200]) / 200
    for v in closes[200:]:
        ema200 = alpha * v + (1.0 - alpha) * ema200

    price    = closes[-1]
    dist     = abs(price - ema200) / ema200 if ema200 > 0 else 1.0
    is_near  = dist < CONFIG["ema200_distance_max"]
    above    = price > ema200
    score    = CONFIG["score_ema200_close"] if is_near else 0
    label    = (
        f"{'✅' if is_near else '⚠️'} EMA200: {ema200:.6g} | dist {dist*100:.1f}% "
        f"({'above' if above else 'below'}) — {'near' if is_near else 'too far'}"
    )

    return {
        "ema200":       round(ema200, 8),
        "distance":     round(dist, 4),
        "is_near":      is_near,
        "above_ema200": above,
        "score":        score,
        "label":        label,
    }


def calc_higher_low_v20(candles, lookback=3):
    """
    PART 1 v20 — Higher Low Structure.
    Verifies reversal structure: recent low > prior low.

    low_current  = min low of last 2 candles
    low_previous = min low of candles [-(lookback+2) : -2]
    Condition: low_current > low_previous
    """
    if len(candles) < lookback + 2:
        return {"is_higher_low": False, "low_now": 0.0, "low_prev": 0.0,
                "label": "Data kurang untuk higher low v20"}

    low_now  = min(c["low"] for c in candles[-2:])
    low_prev = min(c["low"] for c in candles[-(lookback + 2):-2])

    is_hl = low_now > low_prev
    label = (f"🔼 Higher Low v20: {low_now:.6g} > {low_prev:.6g}" if is_hl
             else f"Lower Low: {low_now:.6g} ≤ {low_prev:.6g}")

    return {
        "is_higher_low": is_hl,
        "low_now":       round(low_now, 8),
        "low_prev":      round(low_prev, 8),
        "label":         label,
    }


def check_dump_filter_v20(candles_5m, candles_15m, price_now, ema20_slope_data):
    """
    PART 3 v20 — Dump Filter.
    Rejects coins experiencing heavy selling pressure.

    Reject conditions:
      1. price_change_5m  < -4%
      2. price_change_15m < -6%
      3. close < ema20 AND vol_zscore > 2

    Returns (should_reject: bool, reason: str)
    """
    # Condition 1: 5m price drop
    if candles_5m and len(candles_5m) >= 2:
        c5_prev = candles_5m[-2]["close"]
        if c5_prev > 0:
            chg_5m = (candles_5m[-1]["close"] - c5_prev) / c5_prev * 100
            if chg_5m < CONFIG["dump_filter_5m_pct"]:
                return True, f"🚨 DUMP FILTER: 5m drop {chg_5m:.2f}% < {CONFIG['dump_filter_5m_pct']}%"

    # Condition 2: 15m price drop
    if candles_15m and len(candles_15m) >= 4:
        c15_prev = candles_15m[-4]["close"]
        if c15_prev > 0:
            chg_15m = (candles_15m[-1]["close"] - c15_prev) / c15_prev * 100
            if chg_15m < CONFIG["dump_filter_15m_pct"]:
                return True, f"🚨 DUMP FILTER: 15m drop {chg_15m:.2f}% < {CONFIG['dump_filter_15m_pct']}%"

    # Condition 3: price < ema20 AND z-score > 2 (heavy selling on down candle)
    ema20_now = ema20_slope_data.get("ema20_now", 0)
    if ema20_now > 0 and price_now < ema20_now:
        # recompute zscore from caller context — use slope data flag
        # flag set by master_score when vol_zscore_v20["z"] > strong threshold
        # passed as extra field in ema20_slope_data for convenience
        z = ema20_slope_data.get("vol_z_for_dump", 0.0)
        if z > CONFIG["vol_zscore_v20_strong"]:
            return True, (f"🚨 DUMP FILTER: close {price_now:.6g} < EMA20 {ema20_now:.6g} "
                          f"AND z-score {z:.2f} > 2 — dump aktif")

    return False, ""


def validate_reversal_v20(ema_trend, price_now, vol_zscore_v20, ema20_slope,
                           micro_breakout, candle_imbal):
    """
    PART 2 v20 — Fake Reversal Filter.
    Multi-condition validation for EMA20 cross EMA50 signals.

    Valid reversal requires ALL of:
      1. EMA20 crossed above EMA50
      2. price > EMA20
      3. volume_zscore > 1.5
      4. ema20_slope > 0
      5. price > high_last_20
      6. bid_ask_ratio > 1.2

    If not all met → downgrade score by fake_reversal_penalty.
    Returns (is_valid_reversal, conditions_met, penalty_if_invalid).
    """
    ema_cross = ema_trend.get("cross_up", False)
    ema20_val = ema_trend.get("ema20", 0)

    cond_cross    = ema_cross
    cond_price    = price_now > ema20_val if ema20_val > 0 else False
    cond_zscore   = vol_zscore_v20.get("z", 0) > CONFIG["vol_zscore_v20_min"]
    cond_slope    = ema20_slope.get("is_rising", False)
    cond_breakout = micro_breakout.get("is_breakout", False)
    cond_bidask   = candle_imbal.get("imbalance", 0) > CONFIG["bid_ask_ratio_min"]

    conditions = {
        "ema20_cross_ema50": cond_cross,
        "price_above_ema20": cond_price,
        "vol_zscore_ok":     cond_zscore,
        "ema20_slope_ok":    cond_slope,
        "micro_breakout_ok": cond_breakout,
        "bid_ask_ok":        cond_bidask,
    }
    n_met   = sum(conditions.values())
    n_total = len(conditions)

    # Only apply penalty if EMA cross was detected but conditions not fully met
    if cond_cross and n_met < n_total:
        penalty  = CONFIG["fake_reversal_penalty"]
        is_valid = False
        label    = (f"⚠️ Fake Reversal Risk ({n_met}/{n_total} cond) — "
                    f"EMA cross tapi {n_total - n_met} kondisi gagal: "
                    + ", ".join(k for k, v in conditions.items() if not v))
    elif cond_cross and n_met == n_total:
        penalty  = 0
        is_valid = True
        label    = f"✅ Reversal Valid ({n_met}/{n_total} — semua kondisi terpenuhi)"
    else:
        # No cross detected — not applicable
        penalty  = 0
        is_valid = True   # neutral: no cross = no fake reversal concern
        label    = ""

    return {
        "is_valid":    is_valid,
        "n_met":       n_met,
        "n_total":     n_total,
        "conditions":  conditions,
        "penalty":     penalty,
        "label":       label,
    }


def calc_weighted_score_v20(score_heuristic, ema20_slope, vol_zscore_v20,
                             micro_breakout, reversal_valid, candle_imbal,
                             ema_trend, ema200_dist, higher_low_v20):
    """
    PART 4 v20 — Weighted Scoring Model.
    Adds structured bonus points on top of existing heuristic score.

    Caps per category:
      momentum  (EMA slope + higher low)    : 0–20
      volume    (z-score bonus)              : 0–15
      breakout  (micro breakout v20)         : 0–15
      reversal  (valid reversal multi-cond)  : 0–15
      orderbook (bid_ask_ratio)              : 0–10
      trend     (EMA200 distance + EMA slope): 0–10

    Returns bonus_score (additive) and breakdown dict.
    """
    # ── Momentum component (0-20) ─────────────────────────────────────────────
    mom = 0
    if ema20_slope.get("is_rising"):
        mom += CONFIG["score_ema20_slope"]          # +4
    if higher_low_v20.get("is_higher_low"):
        mom += 6                                     # +6
    if ema_trend.get("trend") == "UPTREND":
        mom += 6                                     # +6 uptrend confirmed
    if ema_trend.get("cross_up"):
        mom += 4                                     # +4 EMA cross up
    momentum_bonus = min(mom, CONFIG["v20_momentum_cap"])

    # ── Volume component (0-15) ───────────────────────────────────────────────
    vol = 0
    z = vol_zscore_v20.get("z", 0)
    if z > CONFIG["vol_zscore_v20_strong"]:
        vol += CONFIG["score_vol_zscore_v20"]        # +10
    elif z > CONFIG["vol_zscore_v20_min"]:
        vol += 5                                      # +5
    volume_bonus = min(vol, CONFIG["v20_volume_cap"])

    # ── Breakout component (0-15) ─────────────────────────────────────────────
    bo = 0
    if micro_breakout.get("is_breakout"):
        bo += CONFIG["score_micro_breakout_v20"]     # +8
        if micro_breakout.get("gap_pct", 0) > 1.0:
            bo += 4                                   # extra if >1% above high
    if ema_trend.get("cross_up"):
        bo += 3                                       # +3 EMA cross adds breakout conf
    breakout_bonus = min(bo, CONFIG["v20_breakout_cap"])

    # ── Reversal component (0-15) ─────────────────────────────────────────────
    rev = 0
    if reversal_valid.get("is_valid") and reversal_valid.get("n_met", 0) >= 4:
        rev += 10
    elif reversal_valid.get("n_met", 0) >= 3:
        rev += 5
    reversal_bonus = min(rev, CONFIG["v20_reversal_cap"])

    # ── Orderbook component (0-10) ─────────────────────────────────────────────
    ob  = 0
    bar = vol_zscore_v20.get("bid_ask_ratio", candle_imbal.get("imbalance", 1.0))
    if bar > CONFIG["bid_ask_ratio_strong"]:
        ob += CONFIG["score_bid_ask_v20"]            # +6
    elif bar > CONFIG["bid_ask_ratio_min"]:
        ob += 3                                       # +3
    orderbook_bonus = min(ob, CONFIG["v20_orderbook_cap"])

    # ── Trend component (0-10) ────────────────────────────────────────────────
    tr = 0
    if ema200_dist.get("is_near") and ema200_dist.get("above_ema200"):
        tr += CONFIG["score_ema200_close"]           # +5
    elif ema200_dist.get("is_near"):
        tr += 3                                       # near but below EMA200
    trend_bonus = min(tr, CONFIG["v20_trend_cap"])

    total_bonus = (momentum_bonus + volume_bonus + breakout_bonus
                   + reversal_bonus + orderbook_bonus + trend_bonus)

    return {
        "total_bonus":     total_bonus,
        "momentum_bonus":  momentum_bonus,
        "volume_bonus":    volume_bonus,
        "breakout_bonus":  breakout_bonus,
        "reversal_bonus":  reversal_bonus,
        "orderbook_bonus": orderbook_bonus,
        "trend_bonus":     trend_bonus,
        "bid_ask_ratio":   round(bar, 3),
        "vol_z_v20":       round(z, 3),
    }


def calc_ema20_cross_up(candles, period_fast=20, period_slow=50):
    """
    PART 2 v20 — Detect EMA20 crossed above EMA50 in recent candles.
    Checks if previous candle had EMA20 < EMA50 and current has EMA20 > EMA50.
    Used by validate_reversal_v20. Injects cross_up flag into ema_trend dict.
    """
    if len(candles) < period_slow + 2:
        return False, 0.0, 0.0

    closes = [c["close"] for c in candles]
    a20    = 2.0 / (period_fast + 1)
    a50    = 2.0 / (period_slow + 1)

    # Build EMA series for last few points only (cache-friendly)
    ema20 = sum(closes[:period_fast]) / period_fast
    ema50 = sum(closes[:period_slow]) / period_slow
    for v in closes[period_fast:]:
        ema20 = a20 * v + (1.0 - a20) * ema20
    for v in closes[period_slow:]:
        ema50 = a50 * v + (1.0 - a50) * ema50

    # Prev EMA20/50 (one candle ago) — approximate via slope
    ema20_prev = ema20 - (closes[-1] - closes[-2]) * a20
    ema50_prev = ema50 - (closes[-1] - closes[-2]) * a50

    cross_up = (ema20_prev <= ema50_prev) and (ema20 > ema50)
    return cross_up, round(ema20, 8), round(ema50, 8)


# ══════════════════════════════════════════════════════════════════════════════
#  🏦  NEW INSTITUTIONAL DETECTORS v22
# ══════════════════════════════════════════════════════════════════════════════

def detect_smart_money_accumulation_v22(candles):
    """
    FIX 02 v22 — Smart Money Accumulation Detector.

    Hidden accumulation signal:
      • price range contraction over last 20 candles < 3%
      • volume trend rising (recent 5c vs baseline 15c)
      • bid pressure > ask pressure (candle close-position proxy)

    Returns is_accumulating, score, label.
    """
    if len(candles) < 25:
        return {"is_accumulating": False, "score": 0,
                "range_ratio": 1.0, "vol_trend": 1.0, "bid_pressure": 0.5,
                "label": "Data kurang"}

    # Price range contraction
    last20     = candles[-20:]
    hi20       = max(c["high"] for c in last20)
    lo20       = min(c["low"]  for c in last20)
    mid_price  = (hi20 + lo20) / 2
    range_ratio = (hi20 - lo20) / mid_price if mid_price > 0 else 1.0

    # Volume trend: recent 5 vs prior 15
    vol_recent  = sum(c["volume_usd"] for c in candles[-5:]) / 5
    vol_prior   = sum(c["volume_usd"] for c in candles[-20:-5]) / 15
    vol_trend   = (vol_recent / vol_prior) if vol_prior > 0 else 1.0

    # Bid pressure proxy (close position in range)
    buy_fracs = []
    for c in last20:
        rng = c["high"] - c["low"]
        buy_fracs.append((c["close"] - c["low"]) / rng if rng > 0 else 0.5)
    bid_pressure = sum(buy_fracs) / len(buy_fracs)

    cond_range = range_ratio < CONFIG["sma_range_max"]
    cond_vol   = vol_trend   >= CONFIG["sma_vol_trend_min"]
    cond_bid   = bid_pressure > 0.52   # slight buy bias

    n_cond         = sum([cond_range, cond_vol, cond_bid])
    is_accumulating = n_cond >= 2

    if n_cond == 3:
        score = CONFIG["score_smart_money_accum"]
        label = (f"🏦 Smart Money Accum CONFIRMED — range {range_ratio*100:.1f}%, "
                 f"vol {vol_trend:.1f}x, bid {bid_pressure*100:.0f}%")
    elif n_cond == 2:
        score = CONFIG["score_smart_money_accum"] // 2
        label = (f"🏦 Smart Money Accum PROBABLE ({n_cond}/3) — "
                 f"range {range_ratio*100:.1f}%, vol {vol_trend:.1f}x")
    else:
        score = 0
        label = f"No smart money signal ({n_cond}/3)"

    return {
        "is_accumulating": is_accumulating,
        "score":           score,
        "range_ratio":     round(range_ratio, 4),
        "vol_trend":       round(vol_trend, 3),
        "bid_pressure":    round(bid_pressure, 3),
        "n_cond":          n_cond,
        "label":           label,
    }


def detect_liquidity_trap_v22(candles, vol_zscore_z):
    """
    FIX 03 v22 — Liquidity Trap Detector (Stop-Hunt before Pump).

    Market makers sweep stops below 30-candle low, then price reverses.

    Conditions:
      candle_low < lowest_low_last_30
      AND close > open  (bullish reversal candle)
      AND volume_zscore > 1.5

    Returns is_trap, score, label.
    """
    lookback = CONFIG["liq_trap_lookback"]
    if len(candles) < lookback + 1:
        return {"is_trap": False, "score": 0, "sweep_low": 0.0, "label": "Data kurang"}

    prior_low = min(c["low"] for c in candles[-(lookback + 1):-1])
    c_last    = candles[-1]
    cond_sweep   = c_last["low"]   < prior_low
    cond_bullish = c_last["close"] > c_last["open"]
    cond_z       = vol_zscore_z    > CONFIG["liq_trap_zscore_min"]

    is_trap = cond_sweep and cond_bullish and cond_z

    if is_trap:
        score = CONFIG["score_liq_trap"]
        label = (f"🪤 Liquidity Trap! Low swept {c_last['low']:.6g} < prior {prior_low:.6g}, "
                 f"bullish reversal, z={vol_zscore_z:.2f}")
    else:
        score = 0
        label = ""

    return {
        "is_trap":   is_trap,
        "score":     score,
        "sweep_low": round(prior_low, 8),
        "label":     label,
    }


def detect_whale_footprint_v22(candles):
    """
    FIX 04 v22 — Whale Footprint Detector.

    Large volume spike WITHOUT large price movement = hidden accumulation.
    Whales absorb supply without moving price (stealth buy).

    Conditions:
      volume > mean_volume * 3
      AND abs(price_change) < 0.5%

    Returns is_footprint, score, label.
    """
    window = 20
    if len(candles) < window + 1:
        return {"is_footprint": False, "score": 0, "vol_mult": 0.0, "label": "Data kurang"}

    vols      = [c["volume_usd"] for c in candles]
    cur_vol   = vols[-1]
    mean_vol  = sum(vols[-(window + 1):-1]) / window if window > 0 else cur_vol
    vol_mult  = (cur_vol / mean_vol) if mean_vol > 0 else 0.0

    c         = candles[-1]
    price_chg = abs(c["close"] - c["open"]) / c["open"] * 100 if c["open"] > 0 else 99.0

    cond_vol   = vol_mult  > CONFIG["whale_fp_vol_mult"]
    cond_price = price_chg < CONFIG["whale_fp_price_max_pct"]

    is_footprint = cond_vol and cond_price

    if is_footprint:
        score = CONFIG["score_whale_footprint"]
        label = (f"🐋 Whale Footprint: {vol_mult:.1f}x vol, price only {price_chg:.2f}% "
                 f"— stealth accumulation")
    else:
        score = 0
        label = ""

    return {
        "is_footprint": is_footprint,
        "score":        score,
        "vol_mult":     round(vol_mult, 2),
        "price_chg":    round(price_chg, 3),
        "label":        label,
    }


def detect_prebreakout_pressure_v22(candles):
    """
    FIX 05 v22 — Pre-Breakout Pressure Detector (BB Compression).

    Detects volatility compression before explosive move.
    Uses BB width relative to its historical percentile.

    Conditions:
      current_bbw < percentile_20(bbw_history)
      AND recent volume spike (vol_ratio > 1.3x)

    Returns is_compressed, score, label.
    """
    lookback = CONFIG["bb_percentile_lookback"]
    period   = 20
    if len(candles) < lookback + period:
        return {"is_compressed": False, "score": 0, "percentile": 50.0, "label": "Data kurang"}

    # Build BBW history
    bbw_history = []
    for i in range(lookback, 0, -1):
        window = candles[-(i + period):-i] if i > 0 else candles[-period:]
        if len(window) < period:
            continue
        closes   = [c["close"] for c in window]
        mean_c   = sum(closes) / period
        std_c    = math.sqrt(sum((x - mean_c) ** 2 for x in closes) / period)
        mid_c    = mean_c if mean_c > 0 else 1.0
        bbw_history.append((2 * std_c * 2) / mid_c)   # BB width = 4σ / price

    if not bbw_history:
        return {"is_compressed": False, "score": 0, "percentile": 50.0, "label": "BBW history kurang"}

    current_bbw = bbw_history[-1]
    sorted_bbw  = sorted(bbw_history)
    rank        = sum(1 for v in sorted_bbw if v <= current_bbw)
    percentile  = rank / len(sorted_bbw) * 100

    # Volume spike check
    vol_recent = sum(c["volume_usd"] for c in candles[-3:]) / 3
    vol_prior  = sum(c["volume_usd"] for c in candles[-15:-3]) / 12
    vol_ratio  = (vol_recent / vol_prior) if vol_prior > 0 else 1.0

    cond_bb  = percentile <= CONFIG["bb_percentile_threshold"]
    cond_vol = vol_ratio  >= 1.3

    is_compressed = cond_bb and cond_vol

    if is_compressed:
        score = CONFIG["score_prebreakout"]
        label = (f"💥 Pre-Breakout: BB width p{percentile:.0f} "
                 f"(compressed), vol {vol_ratio:.1f}x — explosion incoming")
    elif cond_bb:
        score = CONFIG["score_prebreakout"] // 2
        is_compressed = True
        label = (f"💥 Pre-Breakout (BB only): p{percentile:.0f} — no vol confirm yet")
    else:
        score = 0
        label = ""

    return {
        "is_compressed": is_compressed,
        "score":         score,
        "percentile":    round(percentile, 1),
        "current_bbw":   round(current_bbw, 5),
        "vol_ratio":     round(vol_ratio, 3),
        "label":         label,
    }


def detect_momentum_ignition_v22(candles, vol_zscore_z):
    """
    FIX 06 v22 — Momentum Ignition Detector.

    Early pump stage signal: price is making consecutive higher highs
    while volume confirms with a spike.

    Conditions:
      3 consecutive higher highs in last 5 candles
      AND volume_zscore > 1.8

    Returns is_ignition, score, label.
    """
    n_highs = CONFIG["mom_ignition_highs"]
    if len(candles) < n_highs + 2:
        return {"is_ignition": False, "score": 0, "label": "Data kurang"}

    highs  = [c["high"] for c in candles[-(n_highs + 1):]]
    consec = sum(1 for i in range(1, len(highs)) if highs[i] > highs[i - 1])

    cond_highs = consec >= n_highs
    cond_z     = vol_zscore_z > CONFIG["mom_ignition_zscore"]

    is_ignition = cond_highs and cond_z

    if is_ignition:
        score = CONFIG["score_mom_ignition"]
        label = (f"🚀 Momentum Ignition! {consec} higher highs + "
                 f"vol z={vol_zscore_z:.2f} — EARLY PUMP STAGE")
    else:
        score = 0
        label = ""

    return {
        "is_ignition": is_ignition,
        "score":       score,
        "consec_highs": consec,
        "label":       label,
    }


def check_dump_trap_v22(candles, ema200_dist_data, vol_zscore_v20_data):
    """
    FIX 07 v22 — Dump Trap Filter.
    Reject coins in active institutional sell pressure.

    Conditions (ALL must be true to reject):
      price < EMA200
      AND ema50_slope < 0  (EMA50 downward)
      AND ask pressure >> bid pressure (imbalance ratio > 1.3)

    Returns (should_reject: bool, reason: str)
    """
    if len(candles) < 55:
        return False, ""

    # EMA200 check (reuse from ema200_dist_data)
    above_ema200 = ema200_dist_data.get("above_ema200", True)
    if above_ema200:
        return False, ""   # price above EMA200 — not a dump trap

    # EMA50 slope: compare current EMA50 vs 5 candles ago
    closes = [c["close"] for c in candles]
    alpha50 = 2.0 / 51.0
    ema50 = sum(closes[:50]) / 50
    ema50_series = [ema50]
    for v in closes[50:]:
        ema50 = alpha50 * v + (1.0 - alpha50) * ema50
        ema50_series.append(ema50)

    ema50_now  = ema50_series[-1]
    ema50_prev = ema50_series[-6] if len(ema50_series) > 6 else ema50_series[0]
    ema50_slope = ema50_now - ema50_prev

    if ema50_slope >= CONFIG["dump_trap_ema50_slope_max"]:
        return False, ""   # EMA50 still rising

    # Ask pressure check from candle imbalance proxy
    ask_bid_ratio = 1.0 / vol_zscore_v20_data.get("bid_ask_ratio", 1.0)   # invert bid/ask
    if ask_bid_ratio < CONFIG["dump_trap_ask_bid_min"]:
        return False, ""

    return True, (
        f"🚨 DUMP TRAP: price < EMA200, EMA50 slope {ema50_slope:.6g} < 0, "
        f"ask/bid={ask_bid_ratio:.2f} — institutional selling active"
    )


def calc_improved_reversal_v22(price_now, vwap, ema20_slope_data, vol_zscore_v20_data):
    """
    FIX 08 v22 — Improved Reversal Filter.
    Allows reversal pumps ONLY when conditions confirm genuine reversal.

    Required:
      ema20_slope > 0        (momentum building)
      vol_zscore_z > 1.5     (volume confirms)
      price near VWAP (within ±2%)  (fair value area)

    Returns (is_valid_reversal: bool, confidence: str, label: str)
    """
    slope_ok  = ema20_slope_data.get("is_rising", False)
    z         = vol_zscore_v20_data.get("z", 0)
    z_ok      = z > CONFIG["vol_zscore_v20_min"]

    vwap_dist = abs(price_now - vwap) / vwap if vwap > 0 else 1.0
    vwap_ok   = vwap_dist <= CONFIG["rev_vwap_tolerance"]

    n_cond = sum([slope_ok, z_ok, vwap_ok])

    if n_cond == 3:
        return True, "HIGH", (
            f"✅ Reversal VALID (3/3): slope↑ + z={z:.2f} + "
            f"price {vwap_dist*100:.1f}% from VWAP"
        )
    elif n_cond == 2:
        return True, "MEDIUM", (
            f"✅ Reversal PROBABLE (2/3): {n_cond}/3 conditions met"
        )
    elif slope_ok and z_ok:
        return True, "LOW", "Reversal: slope + volume OK (VWAP dist mismatch)"
    else:
        return False, "NONE", (
            f"❌ Reversal WEAK ({n_cond}/3) — slope:{slope_ok} z:{z:.2f} vwap:{vwap_ok}"
        )


def calc_institutional_score_v22(smart_money, liq_trap, whale_fp,
                                  prebreakout, mom_ignition,
                                  vol_zscore_v20, candle_imbal,
                                  micro_breakout, accum, energy):
    """
    FIX 09 v22 — Institutional Scoring Model (0-100, normalized weighted).

    score = accumulation  × 0.20
          + breakout      × 0.20
          + volume        × 0.20
          + orderbook     × 0.15
          + momentum      × 0.15
          + liq_trap      × 0.10

    Returns inst_score (0-100) and breakdown dict.
    """
    # Accumulation component [0-100]
    accum_raw = 0
    if smart_money.get("is_accumulating"):
        accum_raw += 50 + (smart_money.get("n_cond", 0) - 2) * 25
    if whale_fp.get("is_footprint"):
        accum_raw += 30
    if accum.get("is_accumulating"):
        accum_raw += 20
    accum_score = min(accum_raw, 100.0)

    # Breakout component [0-100]
    bo_raw = 0
    if prebreakout.get("is_compressed"):
        bo_raw += 40 + max(0, 20 - prebreakout.get("percentile", 20))
    if micro_breakout.get("is_breakout"):
        bo_raw += 40
    if energy.get("is_buildup"):
        bo_raw += 20
    breakout_score = min(bo_raw, 100.0)

    # Volume component [0-100]
    z = vol_zscore_v20.get("z", 0)
    volume_score = min(max(z / 4.0, 0.0) * 100, 100.0)   # z=4 → 100%

    # Orderbook component [0-100]
    bar = vol_zscore_v20.get("bid_ask_ratio", candle_imbal.get("imbalance", 1.0))
    ob_raw = min(max((bar - 1.0) / 0.5, 0.0) * 100, 100.0)   # bar=1.5 → 100%
    orderbook_score = ob_raw

    # Momentum component [0-100]
    mom_raw = 0
    if mom_ignition.get("is_ignition"):
        mom_raw += 60
    mom_raw += min(mom_ignition.get("consec_highs", 0) * 15, 40)
    momentum_score = min(mom_raw, 100.0)

    # Liquidity trap component [0-100]
    liq_trap_score = 100.0 if liq_trap.get("is_trap") else 0.0

    w = CONFIG
    inst_score = (
        w["inst_w_accumulation"] * accum_score
      + w["inst_w_breakout"]     * breakout_score
      + w["inst_w_volume"]       * volume_score
      + w["inst_w_orderbook"]    * orderbook_score
      + w["inst_w_momentum"]     * momentum_score
      + w["inst_w_liq_trap"]     * liq_trap_score
    )

    return {
        "inst_score":      round(inst_score, 1),
        "accum_score":     round(accum_score, 1),
        "breakout_score":  round(breakout_score, 1),
        "volume_score":    round(volume_score, 1),
        "orderbook_score": round(orderbook_score, 1),
        "momentum_score":  round(momentum_score, 1),
        "liq_trap_score":  round(liq_trap_score, 1),
    }


def calc_pump_probability_v22(inst_score):
    """
    FIX 10 v22 — Advanced Pump Probability Model.

    Uses sigmoid function centered on score=50 with scale=8:
      probability = 1 / (1 + exp(-inst_score / 8))

    Maps directly to 0-100%.
    Replaces linear logistic — this gives stronger signal separation.
    """
    try:
        prob = 1.0 / (1.0 + math.exp(-inst_score / 8.0))
    except OverflowError:
        prob = 0.0 if inst_score < 0 else 1.0
    return round(prob * 100, 1)


def _safe_telegram_text_v22(msg):
    """
    FIX 13 v22 — Enhanced Telegram message sanitizer.

    Strips / replaces problematic characters that cause
    'Bad Request: can't parse entities' in HTML parse mode:
      • & → &amp; (if not already entity)
      • Unmatched < > that look like broken tags
      • Non-ASCII chars that can confuse parser
      • Ensures message <= 4096 chars

    Returns sanitized message string.
    """
    import re as _re
    # Escape & that aren't already HTML entities
    msg = _re.sub(r'&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)', '&amp;', msg)
    # Remove any bare < or > that aren't part of known tags
    allowed_tags = r'(?:</?(?:b|i|code|pre|a|s|u)(?:\s[^>]*)?>)'
    def _fix_angle(m):
        return m.group(0) if _re.match(allowed_tags, m.group(0), _re.I) else ''
    msg = _re.sub(r'<[^>]*>', _fix_angle, msg)
    # Truncate safely
    if len(msg) > 4050:
        msg = msg[:3950] + "\n<i>...[truncated]</i>"
    return msg


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  LEVEL 7 — MARKET MAKER DETECTION v23
# ══════════════════════════════════════════════════════════════════════════════

def detect_liquidity_sweep_v23(candles, vol_zscore_z):
    """
    v23 — Liquidity Sweep Detector (MM stop-hunt before pump).

    Detects when market makers sweep stop-losses below a prior support
    low, then reverse. Classic pattern before institutional pump.

    Conditions:
      candle_low < lowest_low_last_30
      AND close > open  (bullish reversal)
      AND volume_zscore > 1.2
    """
    lookback = CONFIG.get("liq_trap_lookback", 30)
    if len(candles) < lookback + 1:
        return {"is_sweep": False, "score": 0, "label": ""}

    prior_low = min(c["low"] for c in candles[-(lookback + 1):-1])
    c_last    = candles[-1]

    cond_sweep   = c_last["low"]   < prior_low
    cond_bull    = c_last["close"] > c_last["open"]
    cond_z       = vol_zscore_z    > 1.2

    if cond_sweep and cond_bull and cond_z:
        score = CONFIG.get("score_liq_trap", 10)
        label = (
            f"🪤 Liq Sweep v23: swept {c_last['low']:.6g} < "
            f"prior {prior_low:.6g}, bull reversal, z={vol_zscore_z:.2f}"
        )
        return {"is_sweep": True, "score": score, "label": label,
                "swept_low": round(prior_low, 8)}

    return {"is_sweep": False, "score": 0, "label": "", "swept_low": 0.0}


def detect_whale_absorption_v23(candles):
    """
    v23 — Whale Absorption Detector.

    Large volume spike with minimal price movement = whales absorbing
    sell-side supply. Classic accumulation footprint.

    Conditions:
      volume > mean_volume * 3
      AND abs(price_change_pct) < 0.5%
    """
    window = 20
    if len(candles) < window + 2:
        return {"is_absorption": False, "score": 0, "vol_mult": 0.0, "label": ""}

    vols     = [c["volume_usd"] for c in candles]
    cur_vol  = vols[-1]
    mean_vol = sum(vols[-(window + 1):-1]) / window if window > 0 else cur_vol
    vol_mult = cur_vol / mean_vol if mean_vol > 0 else 0.0

    c         = candles[-1]
    pch       = abs(c["close"] - c["open"]) / c["open"] * 100 if c["open"] > 0 else 99.0

    if vol_mult > CONFIG["whale_fp_vol_mult"] and pch < CONFIG["whale_fp_price_max_pct"]:
        score = CONFIG.get("score_whale_footprint", 8)
        label = (
            f"🐋 Whale Absorption v23: {vol_mult:.1f}× vol, "
            f"price only {pch:.2f}% — stealth buy"
        )
        return {"is_absorption": True, "score": score,
                "vol_mult": round(vol_mult, 2), "price_chg": round(pch, 3),
                "label": label}

    return {"is_absorption": False, "score": 0, "vol_mult": round(vol_mult, 2),
            "price_chg": round(pch, 3), "label": ""}


def detect_volatility_compression_v23(candles):
    """
    v23 — Volatility Compression Detector.

    Detects pre-breakout BB compression using historical percentile.
    Low percentile = coil spring ready to explode.

    Computes BB width (4σ / mid_price) and checks if current value
    is below the 20th historical percentile over last 50 candles.
    """
    lookback = CONFIG.get("bb_percentile_lookback", 50)
    period   = 20
    if len(candles) < lookback + period:
        return {"is_compressed": False, "score": 0, "percentile": 50.0, "label": ""}

    bbw_history = []
    for i in range(lookback, 0, -1):
        seg    = candles[-(i + period):-i] if i > 0 else candles[-period:]
        if len(seg) < period:
            continue
        cls    = [c["close"] for c in seg]
        mean_c = sum(cls) / period
        var    = sum((x - mean_c) ** 2 for x in cls) / period
        std_c  = math.sqrt(var)
        mid_c  = mean_c if mean_c > 0 else 1.0
        bbw_history.append(4.0 * std_c / mid_c)

    if not bbw_history:
        return {"is_compressed": False, "score": 0, "percentile": 50.0, "label": ""}

    cur_bbw    = bbw_history[-1]
    rank       = sum(1 for v in bbw_history if v <= cur_bbw)
    pctile     = rank / len(bbw_history) * 100

    # Volume confirmation: recent 3c vs prior 12c
    vol_r  = sum(c["volume_usd"] for c in candles[-3:]) / 3
    vol_p  = sum(c["volume_usd"] for c in candles[-15:-3]) / 12
    vol_ok = (vol_r / vol_p) >= 1.3 if vol_p > 0 else False

    thresh = CONFIG.get("bb_percentile_threshold", 20)
    if pctile <= thresh and vol_ok:
        score = CONFIG.get("score_prebreakout", 8)
        label = (
            f"💥 Volatility Compression v23: BB p{pctile:.0f}, "
            f"vol confirm — EXPLOSION INCOMING"
        )
        return {"is_compressed": True, "score": score,
                "percentile": round(pctile, 1), "bbw": round(cur_bbw, 5), "label": label}
    elif pctile <= thresh:
        score = CONFIG.get("score_prebreakout", 8) // 2
        label = f"💥 Vol Compression (BB only): p{pctile:.0f}"
        return {"is_compressed": True, "score": score,
                "percentile": round(pctile, 1), "bbw": round(cur_bbw, 5), "label": label}

    return {"is_compressed": False, "score": 0,
            "percentile": round(pctile, 1), "bbw": round(cur_bbw, 5), "label": ""}


def detect_momentum_ignition_v23(candles, vol_zscore_z):
    """
    v23 — Momentum Ignition Detector.

    Early pump trigger: consecutive higher highs + volume confirms.
    Distinct from v22: uses z>1.5 (relaxed from 1.8) for more hits.

    Conditions:
      3 consecutive higher highs in last N+1 candles
      AND volume_zscore > 1.5
    """
    n_highs = CONFIG.get("mom_ignition_highs", 3)
    if len(candles) < n_highs + 2:
        return {"is_ignition": False, "score": 0, "label": ""}

    highs  = [c["high"] for c in candles[-(n_highs + 1):]]
    consec = sum(1 for i in range(1, len(highs)) if highs[i] > highs[i - 1])

    cond_highs = consec >= n_highs
    cond_z     = vol_zscore_z > 1.5   # v23: relaxed from 1.8

    if cond_highs and cond_z:
        score = CONFIG.get("score_mom_ignition", 10)
        label = (
            f"🚀 Momentum Ignition v23: {consec} higher highs + "
            f"z={vol_zscore_z:.2f} — EARLY PUMP"
        )
        return {"is_ignition": True, "score": score,
                "consec_highs": consec, "label": label}

    return {"is_ignition": False, "score": 0, "consec_highs": consec, "label": ""}


def calc_orderbook_pressure_v23(vol_zscore_v20_data, candle_imbal_data):
    """
    v23 — Orderbook Pressure Model.

    Computes bid/ask imbalance from available proxies:
      1. bid_ask_ratio from vol_zscore_v20 (candle volume proxy)
      2. candle imbalance from candle_imbal

    imbalance > 1.2 = bullish pressure  → score bonus
    imbalance < 0.8 = bearish pressure  → score penalty
    """
    bar      = vol_zscore_v20_data.get("bid_ask_ratio", 1.0)
    ci_bull  = candle_imbal_data.get("is_bullish", False)
    ci_ratio = candle_imbal_data.get("imbalance", 1.0)

    # Combine both proxies
    imbalance = (bar + ci_ratio) / 2.0

    bull_thr = CONFIG.get("ob_imbalance_bullish", 1.2)
    bear_thr = CONFIG.get("ob_imbalance_bearish", 0.8)
    max_sc   = CONFIG.get("ob_imbalance_score_max", 10)

    if imbalance >= bull_thr:
        raw_score = min((imbalance - 1.0) / 0.5 * max_sc, max_sc)
        label = (
            f"📊 OB Pressure v23: imbalance {imbalance:.2f} "
            f"(bid/ask {bar:.2f}, candle {ci_ratio:.2f}) — BUY DOMINANT"
        )
        return {"imbalance": round(imbalance, 3), "score": round(raw_score, 1),
                "is_bullish": True, "is_bearish": False, "label": label}
    elif imbalance <= bear_thr:
        penalty = -round(min((1.0 - imbalance) / 0.3 * 5, 5), 1)
        label = (
            f"📊 OB Pressure v23: imbalance {imbalance:.2f} — SELL DOMINANT"
        )
        return {"imbalance": round(imbalance, 3), "score": penalty,
                "is_bullish": False, "is_bearish": True, "label": label}

    return {"imbalance": round(imbalance, 3), "score": 0,
            "is_bullish": False, "is_bearish": False, "label": ""}


def detect_spoofing_v23(candles):
    """
    v23 — Spoofing / Fake Wall Detector.

    Detects large volume candles that produce no real price movement.
    These signal fake buy/sell walls (market maker spoofing).

    Conditions for spoofing:
      - Volume spike > 4× mean in last candle
      - Price change < 0.3% despite huge volume
      - FOLLOWED by opposing candle within 2 candles (reversal)

    Applies score penalty to filter out manipulated setups.
    """
    window = 15
    if len(candles) < window + 3:
        return {"is_spoofing": False, "penalty": 0, "label": ""}

    vols     = [c["volume_usd"] for c in candles]
    mean_vol = sum(vols[-(window + 1):-1]) / window if window > 0 else vols[-1]
    mult     = vols[-1] / mean_vol if mean_vol > 0 else 0.0

    c_last   = candles[-1]
    pch      = abs(c_last["close"] - c_last["open"]) / c_last["open"] * 100 \
               if c_last["open"] > 0 else 99.0

    if mult < CONFIG.get("spoofing_vol_mult", 4.0):
        return {"is_spoofing": False, "penalty": 0, "label": ""}
    if pch >= CONFIG.get("spoofing_price_max_pct", 0.3):
        return {"is_spoofing": False, "penalty": 0, "label": ""}

    # Check if next candle (candles[-1] spike, candles prev) reversal
    rev_n = CONFIG.get("spoofing_reversal_candles", 2)
    is_bull_last = c_last["close"] > c_last["open"]
    reversal_found = False
    for prev in candles[-(rev_n + 2):-1]:
        is_bear = prev["close"] < prev["open"]
        if is_bull_last and is_bear:
            reversal_found = True
        elif not is_bull_last and prev["close"] > prev["open"]:
            reversal_found = True

    if reversal_found:
        penalty = CONFIG.get("spoofing_penalty", -6)
        label = (
            f"⚠️ Spoofing v23: {mult:.1f}× vol, price {pch:.2f}%, "
            f"direction reversed — fake wall detected"
        )
        return {"is_spoofing": True, "penalty": penalty,
                "vol_mult": round(mult, 2), "label": label}

    return {"is_spoofing": False, "penalty": 0, "label": ""}


def calc_mm_score_v23(liq_sweep_v23, whale_abs_v23, vol_comp_v23,
                      mom_ign_v23, ob_press_v23, spoofing_v23,
                      smart_money_v22, accum, energy):
    """
    v23 — Market Maker Scoring Model (0-100, weighted).

    Replaces/augments institutional score with MM-specific detectors.

    Weights:
      accumulation  × 0.20
      breakout      × 0.20
      volume        × 0.20
      orderbook     × 0.15
      momentum      × 0.15
      liquidity_sweep × 0.10
    """
    # Accumulation component [0-100]
    acc_raw = 0
    if smart_money_v22.get("is_accumulating"):
        acc_raw += 50 + (smart_money_v22.get("n_cond", 0) - 2) * 25
    if whale_abs_v23.get("is_absorption"):
        acc_raw += 35
    if accum.get("is_accumulating"):
        acc_raw += 20
    acc_score = min(acc_raw, 100.0)

    # Breakout component [0-100]
    bo_raw = 0
    if vol_comp_v23.get("is_compressed"):
        bo_raw += 40 + max(0, 20 - vol_comp_v23.get("percentile", 20))
    if energy.get("is_buildup"):
        bo_raw += 25
    bo_score = min(bo_raw, 100.0)

    # Volume component [0-100]
    # Will be passed in as z-score; proxy via whale absorption mult
    vol_raw = min(whale_abs_v23.get("vol_mult", 0) / 5.0 * 100, 100.0)
    vol_score = vol_raw

    # Orderbook component [0-100]
    ob_score = max(
        min(ob_press_v23.get("score", 0) / CONFIG.get("ob_imbalance_score_max", 10) * 100, 100.0),
        0.0
    )

    # Momentum component [0-100]
    mom_raw = 60 if mom_ign_v23.get("is_ignition") else 0
    mom_raw += min(mom_ign_v23.get("consec_highs", 0) * 10, 30)
    mom_score = min(mom_raw, 100.0)

    # Liquidity sweep component [0-100]
    ls_score = 100.0 if liq_sweep_v23.get("is_sweep") else 0.0

    # Spoofing penalty — directly reduce components
    spoof_pen = spoofing_v23.get("penalty", 0)

    w = CONFIG
    mm_score = (
        w["inst_w_accumulation"] * acc_score
      + w["inst_w_breakout"]     * bo_score
      + w["inst_w_volume"]       * vol_score
      + w["inst_w_orderbook"]    * ob_score
      + w["inst_w_momentum"]     * mom_score
      + w["inst_w_liq_trap"]     * ls_score
    )
    mm_score = max(0.0, mm_score + spoof_pen)  # apply spoofing penalty

    return {
        "mm_score":      round(mm_score, 1),
        "acc_score":     round(acc_score, 1),
        "bo_score":      round(bo_score, 1),
        "vol_score":     round(vol_score, 1),
        "ob_score":      round(ob_score, 1),
        "mom_score":     round(mom_score, 1),
        "ls_score":      round(ls_score, 1),
        "spoof_pen":     spoof_pen,
    }


def calc_pump_probability_v23(mm_score):
    """
    v23 — Pump Probability via Sigmoid function.

    probability = 1 / (1 + exp(-mm_score / 8)) * 100

    Gives strong separation: mm_score=50 → ~73%, mm_score=30 → ~48%
    """
    try:
        prob = 1.0 / (1.0 + math.exp(-mm_score / 8.0))
    except OverflowError:
        prob = 0.0 if mm_score < 0 else 1.0
    return round(prob * 100, 1)

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  v27 — PRE-PUMP INTELLIGENCE MODULES (7 modules)
#  All functions are additive — score bonuses only, no hard rejects.
#  Integration: calc_pump_probability_score_v27() aggregates all signals.
# ══════════════════════════════════════════════════════════════════════════════


def detect_momentum_acceleration_v27(candles_5m, candles_1h):
    """
    Module 1 — Multi-Timeframe Momentum Acceleration.

    Detects accelerating momentum by confirming that short-term (5m)
    price change is growing faster than medium-term (1h).
    Spec: change_5m > change_1h * 1.5 → acceleration confirmed.

    Returns dict with is_accelerating, score, label, ratio.
    """
    null = {"is_accelerating": False, "score": 0,
            "label": "—", "ratio": 0.0, "chg_5m": 0.0, "chg_1h": 0.0}
    if len(candles_5m) < 6 or len(candles_1h) < 4:
        return null

    p5_now  = candles_5m[-1]["close"]
    p5_prev = candles_5m[-4]["close"]   # ~15 min ago
    if p5_prev <= 0:
        return null
    chg_5m = (p5_now - p5_prev) / p5_prev

    p1_now  = candles_1h[-1]["close"]
    p1_prev = candles_1h[-4]["close"]   # 3h ago
    if p1_prev <= 0:
        return null
    chg_1h = (p1_now - p1_prev) / p1_prev

    threshold = CONFIG.get("v27_mom_accel_5m_ratio", 1.5)
    if chg_1h <= 0:
        ratio = 99.0 if chg_5m > 0.002 else 0.0
    elif chg_5m <= 0:
        ratio = 0.0
    else:
        ratio = chg_5m / chg_1h

    is_accelerating = ratio >= threshold
    score = CONFIG.get("v27_mom_accel_score", 10) if is_accelerating else 0

    label = (
        f"⚡ MTF Momentum Accel v27: 5m {chg_5m*100:+.2f}% "
        f"vs 1h {chg_1h*100:+.2f}% (ratio {ratio:.1f}×) — +{score}"
        if is_accelerating else
        f"Momentum accel v27: ratio {ratio:.1f}× (need ≥{threshold}×)"
    )
    return {
        "is_accelerating": is_accelerating,
        "score":           score,
        "label":           label,
        "ratio":           round(ratio, 2),
        "chg_5m":          round(chg_5m * 100, 3),
        "chg_1h":          round(chg_1h * 100, 3),
    }


def detect_volume_expansion_v27(candles):
    """
    Module 2 — Extreme Volume Expansion (>4× baseline).

    Adds a dedicated extreme tier at >4× to identify pump-grade volume.
    Complements existing calc_volume_spike (1.5/2/3× tiers).
    Large pumps ALWAYS have >4× volume expansion before the move.

    Returns dict with is_extreme, ratio, score, label.
    """
    window = CONFIG.get("vol_spike_window", 20)
    if len(candles) < window + 3:
        return {"is_extreme": False, "ratio": 0.0, "score": 0, "label": "Data kurang"}

    current_vol      = sum(c["volume_usd"] for c in candles[-3:]) / 3
    baseline_candles = candles[-(window + 3):-3]
    avg_vol = (sum(c["volume_usd"] for c in baseline_candles) /
               len(baseline_candles)) if baseline_candles else 0.0

    if avg_vol <= 0:
        return {"is_extreme": False, "ratio": 0.0, "score": 0, "label": "Baseline vol 0"}

    ratio      = current_vol / avg_vol
    threshold  = CONFIG.get("v27_vol_expansion_thresh", 4.0)
    is_extreme = ratio >= threshold
    score      = CONFIG.get("v27_vol_expansion_score", 20) if is_extreme else 0

    label = (
        f"🔥 Vol Expansion EXTREME v27: {ratio:.1f}× baseline — PUMP GRADE (+{score})"
        if is_extreme else
        f"Vol expansion v27: {ratio:.1f}× (threshold {threshold:.0f}×)"
    )
    return {
        "is_extreme":  is_extreme,
        "ratio":       round(ratio, 2),
        "current_vol": round(current_vol, 0),
        "avg_vol":     round(avg_vol, 0),
        "score":       score,
        "label":       label,
    }


def detect_oi_expansion_v27(oi_data):
    """
    Module 3 — Open Interest Expansion (>5% growth).

    Real futures pumps require new leveraged money entering.
    OI growth >5% = new participants opening longs = fuel for pump.
    Reuses the existing get_oi_change() result — zero extra API calls.

    Returns dict with is_expanding, change_pct, score, label.
    """
    if oi_data.get("is_new", True):
        return {"is_expanding": False, "change_pct": 0.0, "score": 0,
                "label": "OI: first scan (no prev snapshot)"}

    change_pct   = oi_data.get("change_pct", 0.0)
    threshold    = CONFIG.get("v27_oi_expansion_pct", 5.0)
    is_expanding = change_pct >= threshold
    score        = CONFIG.get("v27_oi_expansion_score", 20) if is_expanding else 0

    label = (
        f"📈 OI Expansion v27: +{change_pct:.1f}% ≥ {threshold:.0f}% "
        f"— new leveraged money (+{score})"
        if is_expanding else
        f"OI change v27: {change_pct:+.1f}% (need >{threshold:.0f}%)"
    )
    return {
        "is_expanding": is_expanding,
        "change_pct":   round(change_pct, 2),
        "score":        score,
        "label":        label,
    }


def detect_atr_breakout_v27(price_now, ema20_val, atr_abs_val):
    """
    Module 4 — ATR Breakout (price > EMA20 + 1.5×ATR).

    Confirms price has broken out beyond normal volatility range.
    ema20_val comes from ema_trend["ema20"] — no recomputation needed.
    atr_abs_val comes from calc_atr_abs(c1h) already in master_score.

    Returns dict with is_breakout, breakout_level, score, label.
    """
    if ema20_val is None or ema20_val <= 0 or atr_abs_val <= 0:
        return {"is_breakout": False, "breakout_level": 0.0,
                "excess_pct": 0.0, "score": 0, "label": "ATR breakout: data kurang"}

    mult           = CONFIG.get("v27_atr_breakout_mult", 1.5)
    breakout_level = ema20_val + mult * atr_abs_val
    is_breakout    = price_now > breakout_level
    score          = CONFIG.get("v27_atr_breakout_score", 10) if is_breakout else 0

    excess_pct = (price_now - breakout_level) / breakout_level * 100 if is_breakout else 0.0
    label = (
        f"🎯 ATR Breakout v27: {price_now:.6g} > EMA20+{mult}×ATR "
        f"{breakout_level:.6g} (+{excess_pct:.2f}%) — +{score}"
        if is_breakout else
        f"No ATR breakout v27: {price_now:.6g} vs {breakout_level:.6g}"
    )
    return {
        "is_breakout":    is_breakout,
        "breakout_level": round(breakout_level, 8),
        "excess_pct":     round(excess_pct, 2),
        "score":          score,
        "label":          label,
    }


def get_orderbook_imbalance_v27(symbol):
    """
    Module 5 — Real L2 Orderbook Imbalance (Bitget merge-depth).

    Fetches top-N bids/asks from Bitget futures depth endpoint.
    Computes dollar-weighted bid_vol / ask_vol ratio.
    Falls back to neutral on any API failure — never crashes scanner.

    Endpoint: /api/v2/mix/market/merge-depth
    Cost: 1 extra API call per coin (only when activated in master_score).

    Returns dict with ratio, is_bullish, score, label, source.
    """
    null = {"bid_vol": 0.0, "ask_vol": 0.0, "ratio": 1.0,
            "is_bullish": False, "score": 0,
            "label": "OB depth: no data", "source": "fallback"}
    try:
        limit = CONFIG.get("v27_ob_depth_limit", 20)
        data  = safe_get(
            f"{BITGET_BASE}/api/v2/mix/market/merge-depth",
            params={
                "symbol":      symbol,
                "productType": "usdt-futures",
                "precision":   "scale0",
                "limit":       str(limit),
            },
        )
        if not data or data.get("code") != "00000":
            return null

        depth = data.get("data", {})
        bids  = depth.get("bids", [])   # [[price_str, qty_str], ...]
        asks  = depth.get("asks", [])
        if not bids or not asks:
            return null

        # Dollar-weighted volume: price × qty for top-N levels
        bid_vol = sum(float(b[0]) * float(b[1])
                      for b in bids[:limit] if len(b) >= 2)
        ask_vol = sum(float(a[0]) * float(a[1])
                      for a in asks[:limit] if len(a) >= 2)

        if ask_vol <= 0:
            return {**null, "bid_vol": bid_vol, "ratio": 2.0,
                    "is_bullish": True,
                    "score": CONFIG.get("v27_ob_score_strong", 15),
                    "label": "OB depth v27: full bid wall",
                    "source": "live"}

        ratio         = bid_vol / ask_vol
        thresh_strong = CONFIG.get("v27_ob_ratio_strong", 1.6)
        thresh_mild   = CONFIG.get("v27_ob_ratio_mild",   1.2)

        if ratio >= thresh_strong:
            score      = CONFIG.get("v27_ob_score_strong", 15)
            is_bullish = True
            label      = (f"📚 L2 OB v27: bid/ask={ratio:.2f} ≥ {thresh_strong} "
                          f"— STRONG buy wall (+{score})")
        elif ratio >= thresh_mild:
            score      = CONFIG.get("v27_ob_score_mild", 7)
            is_bullish = True
            label      = (f"📊 L2 OB v27: bid/ask={ratio:.2f} ≥ {thresh_mild} "
                          f"— mild imbalance (+{score})")
        else:
            score      = 0
            is_bullish = False
            label      = f"L2 OB v27: bid/ask={ratio:.2f} — balanced/bearish"

        return {
            "bid_vol":    round(bid_vol, 0),
            "ask_vol":    round(ask_vol, 0),
            "ratio":      round(ratio, 3),
            "is_bullish": is_bullish,
            "score":      score,
            "label":      label,
            "source":     "live",
        }
    except Exception as e:
        log.debug(f"  OB depth v27 fetch failed: {e}")
        return null


def detect_whale_accumulation_v27(candles, window=None):
    """
    Module 6 — Whale Accumulation via Large-Volume Buy Candles.

    Detects repeated large buy candles as a proxy for large trades.
    A candle qualifies when: volume_usd >= threshold AND close >= open.
    Needs >= 3 such candles in the last N-hour window.

    Complements detect_whale_accumulation() which uses range compression.
    This module targets single-candle large-trade signatures.

    Returns dict with is_whale_accum, count, score, label.
    """
    if window is None:
        window = CONFIG.get("v27_whale_trade_window", 12)

    null = {"is_whale_accum": False, "count": 0, "score": 0,
            "label": "Whale accum v27: data kurang"}
    if len(candles) < window:
        return null

    usd_thresh = CONFIG.get("v27_whale_trade_usd",   15_000)
    min_count  = CONFIG.get("v27_whale_trade_count",  3)
    recent     = candles[-window:]

    whale_count = sum(
        1 for c in recent
        if c.get("volume_usd", 0) >= usd_thresh and c["close"] >= c["open"]
    )
    is_whale_accum = whale_count >= min_count
    score          = CONFIG.get("v27_whale_accum_score", 15) if is_whale_accum else 0

    label = (
        f"🐳 Whale Accum v27: {whale_count} large buy candles "
        f"≥ ${usd_thresh/1e3:.0f}K in {window}h — +{score}"
        if is_whale_accum else
        f"Whale accum v27: {whale_count}/{min_count} candles (${usd_thresh/1e3:.0f}K)"
    )
    return {
        "is_whale_accum": is_whale_accum,
        "count":          whale_count,
        "threshold_usd":  usd_thresh,
        "score":          score,
        "label":          label,
    }


def detect_liquidity_sweep_up_v27(candles):
    """
    Module 7 — Upward Liquidity Sweep (stop-hunt of shorts).

    Price breaks above the prior N-candle high by >1% → triggers
    short stop-losses → momentum surge → precursor to large pumps.

    Spec: if prices[-1] > max(prices[:-1]) * 1.01 → True.
    Complements detect_liquidity_sweep() (downward sweep).

    Returns dict with is_sweep_up, sweep_high, prior_high, score, label.
    """
    lookback = CONFIG.get("v27_liq_sweep_lookback", 20)
    null = {"is_sweep_up": False, "sweep_high": 0.0, "prior_high": 0.0,
            "excess_pct": 0.0, "score": 0, "label": "—"}

    if len(candles) < lookback + 1:
        return null

    prior_candles = candles[-(lookback + 1):-1]
    current       = candles[-1]
    if not prior_candles:
        return null

    prior_high   = max(c["high"] for c in prior_candles)
    current_high = current["high"]
    threshold    = CONFIG.get("v27_liq_sweep_upbreak_pct", 0.01)
    is_sweep_up  = prior_high > 0 and current_high > prior_high * (1.0 + threshold)
    score        = CONFIG.get("v27_liq_sweep_score", 10) if is_sweep_up else 0

    excess_pct = ((current_high - prior_high) / prior_high * 100
                  if (is_sweep_up and prior_high > 0) else 0.0)
    label = (
        f"🎯 Liq Sweep UP v27: {current_high:.6g} > prior {prior_high:.6g} "
        f"(+{excess_pct:.2f}%) — short stop-hunt (+{score})"
        if is_sweep_up else
        f"No upward sweep v27: {current_high:.6g} vs {prior_high:.6g}"
    )
    return {
        "is_sweep_up": is_sweep_up,
        "sweep_high":  round(current_high, 8),
        "prior_high":  round(prior_high, 8),
        "excess_pct":  round(excess_pct, 2),
        "score":       score,
        "label":       label,
    }


def calc_pump_probability_score_v27(
        mom_accel_v27, vol_exp_v27, oi_exp_v27,
        atr_break_v27, ob_imbal_v27, whale_accum_v27, liq_sweep_up_v27):
    """
    v27 — Composite Pump Probability Score (0–90 capped).

    Aggregates all 7 pre-pump intelligence modules.
    Each module contributes its score only when its signal is active.
    This score is additive to blended_score — it does NOT replace it.

    Spec weights:
        Volume expansion  +20  |  OI expansion      +20
        Orderbook (strong)+15  |  Whale accumulation+15
        Momentum accel    +10  |  ATR breakout      +10
        Liq sweep up      +10
        ──────────────────────────────────────────────
        Max raw           100  (capped at 90)

    Returns dict with pump_probability_score, components, active_modules, label.
    """
    components = {
        "vol_exp":      vol_exp_v27.get("score",       0),
        "oi_exp":       oi_exp_v27.get("score",        0),
        "ob_imbal":     ob_imbal_v27.get("score",      0),
        "whale_accum":  whale_accum_v27.get("score",   0),
        "mom_accel":    mom_accel_v27.get("score",     0),
        "atr_break":    atr_break_v27.get("score",     0),
        "liq_sweep_up": liq_sweep_up_v27.get("score",  0),
    }

    raw_total  = sum(components.values())
    cap        = CONFIG.get("v27_pump_score_cap", 90)
    pump_score = min(int(raw_total), cap)

    active = []
    if vol_exp_v27.get("is_extreme"):       active.append("VOL4x")
    if oi_exp_v27.get("is_expanding"):      active.append("OI+")
    if ob_imbal_v27.get("is_bullish"):      active.append("OB")
    if whale_accum_v27.get("is_whale_accum"): active.append("WHALE")
    if mom_accel_v27.get("is_accelerating"): active.append("ACCEL")
    if atr_break_v27.get("is_breakout"):    active.append("ATR")
    if liq_sweep_up_v27.get("is_sweep_up"): active.append("SWEEP↑")

    n_active = len(active)
    tag = " + ".join(active) if active else "none"

    if pump_score >= 70:
        label = f"🚀 PUMP PROB HIGH v27: {pump_score}/100 [{tag}]"
    elif pump_score >= 40:
        label = f"📈 Pump Prob MODERATE v27: {pump_score}/100 [{tag}]"
    elif n_active > 0:
        label = f"⚠️ Pump Prob LOW v27: {pump_score}/100 [{tag}]"
    else:
        label = f"Pump Prob v27: {pump_score}/100 — no modules triggered"

    return {
        "pump_probability_score": pump_score,
        "components":             components,
        "active_modules":         active,
        "n_active":               n_active,
        "label":                  label,
    }



def calc_entry_v19(candles, vwap, price_now, atr_abs_val, market_regime, sr,
                   rsi, buy_ratio, vol_ratio, price_pos, alert_level, bos_level,
                   liq_sweep):
    """
    STEP 1, 2, 3 v19 — Adaptive Entry + Liquidity-Aware SL + Dynamic TP.

    STEP 1 — Adaptive Entry (always near current price):
      entry = min(VWAP, price + ATR * 0.25)
      Override per regime untuk RR yang lebih baik.

    STEP 2 — Liquidity-Aware SL:
      swing_low = lowest low last 20 candles
      SL = min(swing_low − ATR*0.5, entry − ATR*1.5)

    STEP 3 — Dynamic TP:
      TP1 = entry + ATR * 2
      TP2 = entry + ATR * 3.5
      TP3 = entry + ATR * 5
    """
    atr = atr_abs_val
    atr_pct_now = (atr / price_now * 100) if price_now > 0 else 2.0

    # ── STEP 1: Adaptive Entry ────────────────────────────────────────────────
    # Base: min(VWAP, price + ATR*0.25) — selalu dekat harga
    base_entry = min(vwap, price_now + atr * 0.25)

    # Regime override untuk RR lebih baik
    if market_regime == "PULLBACK":
        # Tunggu pullback ke VWAP − 0.3×ATR
        pullback_entry = vwap - atr * CONFIG["entry_pullback_atr_mult"]
        entry = min(base_entry, pullback_entry)
        if entry < price_now * 0.985:   # jangan terlalu jauh
            entry = base_entry
        entry_reason = f"PULLBACK adaptive — min(VWAP,price+ATR*0.25) vs VWAP−0.3ATR"

    elif market_regime == "SWEEP" and liq_sweep and liq_sweep.get("is_sweep"):
        sweep_low    = liq_sweep.get("sweep_low", price_now * 0.98)
        sweep_entry  = sweep_low + atr * CONFIG["entry_sweep_atr_mult"]
        entry        = min(base_entry, sweep_entry)
        entry_reason = f"SWEEP adaptive — sweep_low {sweep_low:.6g} + 0.2ATR"

    elif market_regime == "BREAKOUT":
        # Di atas harga tapi tidak jauh
        res_levels = []
        if sr and sr.get("resistance"):
            res_levels = [rv["level"] for rv in sr["resistance"]
                          if rv["level"] > price_now * 0.998]
        if res_levels:
            breakout_lvl = min(res_levels)
            entry        = min(breakout_lvl * 1.005, price_now + atr * 0.25)
            entry_reason = f"BREAKOUT adaptive — near R1 {breakout_lvl:.6g}"
        else:
            entry        = base_entry
            entry_reason = "BREAKOUT adaptive — base"

    else:  # NEUTRAL
        entry        = base_entry
        entry_reason = f"NEUTRAL adaptive — min(VWAP,price+ATR*0.25)"

    # Pastikan entry masuk akal
    if entry <= 0 or entry > price_now * 1.05:
        entry = price_now * 1.001

    # ── STEP 2: Liquidity-Aware SL ────────────────────────────────────────────
    # swing_low = lowest low 20 candle terakhir
    lookback_sl = min(20, len(candles) - 1)
    swing_low   = min(c["low"] for c in candles[-lookback_sl:]) if lookback_sl > 0 else entry * 0.95

    # SL = min(swing_low − ATR*0.5, entry − ATR*1.5)
    sl_swing    = swing_low - atr * 0.5
    sl_atr      = entry - atr * 1.5
    sl          = min(sl_swing, sl_atr)

    # Clamp SL: tidak terlalu dekat maupun terlalu jauh
    sl = max(sl, entry * (1.0 - CONFIG["max_sl_pct"] / 100.0))
    sl = min(sl, entry * (1.0 - CONFIG["min_sl_pct"] / 100.0))
    if sl >= entry:
        sl = entry * 0.975

    # ── STEP 3: Dynamic TP v22 — ATR × 1.3 / × 2.2 / × 3.5 ──────────────────
    # FIX 12 v22: tighter TP1/TP2 for higher win-rate, ATR×0.9 SL
    tp1 = entry + atr * CONFIG["tp1_v22_mult"]    # 1.3× ATR (tighter, higher hit rate)
    tp2 = entry + atr * CONFIG["tp2_v22_mult"]    # 2.2× ATR
    tp3 = entry + atr * CONFIG["tp3_v19_mult"]    # 5.0× ATR (keep v19 for extended target)
    # v22 tighter SL = entry - ATR × 0.9
    sl_v22 = entry - atr * CONFIG["sl_v22_mult"]
    sl = max(sl, sl_v22)   # use whichever is less risky (higher of the two SL levels)
    sl = max(sl, entry * (1.0 - CONFIG["max_sl_pct"] / 100.0))
    sl = min(sl, entry * (1.0 - CONFIG["min_sl_pct"] / 100.0))
    if sl >= entry:
        sl = entry * 0.975

    # Boost TP3 jika ada liquidity void (gap resistance > 5%)
    if sr and sr.get("resistance"):
        res_above = sorted([rv["level"] for rv in sr["resistance"] if rv["level"] > entry])
        if len(res_above) >= 2:
            gap = res_above[1] - res_above[0]
            if gap / res_above[0] > 0.05:
                tp3 = max(tp3, res_above[1])
        if res_above and res_above[0] > tp1:
            tp1 = max(tp1, res_above[0])
        if len(res_above) >= 2 and res_above[1] > tp2:
            tp2 = max(tp2, res_above[1])

    tp1 = max(tp1, entry * 1.005)   # v22: relaxed minimum (was 1.008)
    tp2 = max(tp2, tp1   * 1.01)
    tp3 = max(tp3, tp2   * 1.02)

    risk = entry - sl
    rr1  = round((tp1 - entry) / risk, 1) if risk > 0 else 0.0
    rr2  = round((tp2 - entry) / risk, 1) if risk > 0 else 0.0
    rr3  = round((tp3 - entry) / risk, 1) if risk > 0 else 0.0

    return {
        "entry":         round(entry, 8),
        "sl":            round(sl, 8),
        "sl_pct":        round((entry - sl) / entry * 100, 2),
        "t1":            round(tp1, 8),
        "t2":            round(tp2, 8),
        "t3":            round(tp3, 8),
        "rr":            rr1,
        "rr2":           rr2,
        "rr3":           rr3,
        "rr_str":        f"{rr1:.1f}",
        "rr2_str":       f"{rr2:.1f}",
        "rr3_str":       f"{rr3:.1f}",
        "vwap":          round(vwap, 8),
        "bos_level":     round(bos_level, 8),
        "alert_level":   alert_level,
        "gain_t1_pct":   round((tp1 - entry) / entry * 100, 1),
        "gain_t2_pct":   round((tp2 - entry) / entry * 100, 1),
        "gain_t3_pct":   round((tp3 - entry) / entry * 100, 1),
        "atr_abs":       round(atr, 8),
        "atr_pct":       round(atr_pct_now, 2),
        "sl_method":     entry_reason,
        "market_regime": market_regime,
        "trail_note":    "Trailing v19: TP1→SL=Entry | TP2→SL=TP1 | TP3 free run",
        "swing_low":     round(swing_low, 8),
        "used_resistance": bool(sr and sr.get("resistance")),
        "t1_source":     f"ATR×{CONFIG['tp1_v19_mult']}",
        "t2_source":     f"ATR×{CONFIG['tp2_v19_mult']}",
        "t3_source":     f"ATR×{CONFIG['tp3_v19_mult']} / Liq Void",
        "atr_pct_abs":   round(atr / entry * 100, 2) if entry > 0 else 0.0,
    }


def calc_accumulation_phase(candles):
    """
    Deteksi fase akumulasi smart money.

    Kondisi akumulasi VALID (v15.7/15.8):
      1. Volume 4 candle terbaru > 1.5x baseline 24 candle sebelumnya
      2. Price range 12 candle < 2% (harga sideways)
      3. ATR short < 75% ATR long (volatilitas menyempit)
      4. BB Width menyempit dibanding 12 candle lalu
      5. Posisi harga dalam swing range < 70% (bukan distribusi atas)

    FIX v18: is_vol_compress hanya aktif secara scoring jika is_accumulating=False
    (isolasi scoring di master_score — lihat bagian scoring).
    """
    if len(candles) < 36:
        return {
            "is_accumulating": False, "is_vol_compress": False,
            "vol_ratio_4h": 0.0, "price_range_pct": 0.0,
            "atr_contract": 1.0, "bbw_contracting": False,
            "price_pos": 0.5, "phase_label": "Data kurang",
        }

    # Volume: rata-rata 4 candle terbaru vs baseline 24 candle sebelumnya
    vol_4h  = sum(c["volume_usd"] for c in candles[-4:]) / 4
    vol_24h = sum(c["volume_usd"] for c in candles[-28:-4]) / 24 if len(candles) >= 28 else vol_4h
    vol_ratio_4h = (vol_4h / vol_24h) if vol_24h > 0 else 1.0

    # Price range 12 candle
    r12             = candles[-12:]
    hi12            = max(c["high"] for c in r12)
    lo12            = min(c["low"]  for c in r12)
    mid12           = (hi12 + lo12) / 2
    price_range_pct = ((hi12 - lo12) / mid12 * 100) if mid12 > 0 else 99.0

    # ATR contracting
    atr_s        = _atr_n(candles, CONFIG["accum_atr_lookback_short"])
    atr_l        = _atr_n(candles, CONFIG["accum_atr_lookback_long"])
    atr_contract = (atr_s / atr_l) if atr_l > 0 else 1.0

    # BB contracting
    bbw_now, _  = calc_bbw(candles)
    bbw_12h, _  = calc_bbw(candles[:-12]) if len(candles) > 32 else (bbw_now, 0.0)
    bbw_contracting = (bbw_now < bbw_12h * 0.85) if bbw_12h > 0 else False

    # Posisi harga dalam swing range 48 candle
    price_pos = calc_swing_range_position(candles, lookback=48)

    vol_rising      = vol_ratio_4h    >= CONFIG["accum_vol_ratio"]
    price_sideways  = price_range_pct <= CONFIG["accum_price_range_max"]
    atr_shrinking   = atr_contract    <= CONFIG["accum_atr_contract_ratio"]
    is_vol_compress = atr_shrinking and bbw_contracting
    price_in_zone   = price_pos < CONFIG["accum_max_pos_in_range"]
    is_accumulating = vol_rising and price_sideways and price_in_zone

    if is_accumulating and is_vol_compress:
        label = (
            f"🏦 AKUMULASI + COMPRESSION — vol {vol_ratio_4h:.1f}x, "
            f"range {price_range_pct:.1f}%, pos {price_pos:.0%} dari range"
        )
    elif is_accumulating:
        label = (
            f"📦 AKUMULASI — vol {vol_ratio_4h:.1f}x, "
            f"sideways {price_range_pct:.1f}%, pos {price_pos:.0%}"
        )
    elif is_vol_compress:
        label = f"🗜️ VOLATILITY COMPRESSION — ATR {atr_contract:.2f}x dari baseline"
    else:
        label = "—"

    return {
        "is_accumulating":  is_accumulating,
        "is_vol_compress":  is_vol_compress,
        "vol_ratio_4h":     round(vol_ratio_4h, 2),
        "price_range_pct":  round(price_range_pct, 2),
        "atr_contract":     round(atr_contract, 3),
        "bbw_contracting":  bbw_contracting,
        "price_pos":        price_pos,
        "phase_label":      label,
    }

def calc_htf_accumulation(candles_4h):
    """
    HTF Accumulation Filter — deteksi akumulasi di timeframe 4H.

    Kondisi:
      1. ATR 4H terkini < 85% ATR rata-rata (kompresi volatilitas TF besar)
      2. Volume 4H terbaru > 1.3x rata-rata
      3. Range 8 candle 4H < 3%
      4. Posisi harga 4H < 75% swing range 4H (bukan distribusi)
    """
    if len(candles_4h) < 16:
        return {
            "is_htf_accum": False, "atr_ratio": 1.0,
            "vol_ratio": 1.0, "range_pct": 99.0,
            "price_pos": 0.5, "label": "Data 4H tidak cukup",
        }

    atr_recent = _atr_n(candles_4h, 4)
    atr_avg    = _atr_n(candles_4h, 12)
    atr_ratio  = (atr_recent / atr_avg) if atr_avg > 0 else 1.0

    vol_recent = sum(c["volume_usd"] for c in candles_4h[-2:]) / 2
    vol_avg    = (sum(c["volume_usd"] for c in candles_4h[-10:-2]) / 8
                  if len(candles_4h) >= 10 else vol_recent)
    vol_ratio  = (vol_recent / vol_avg) if vol_avg > 0 else 1.0

    r8        = candles_4h[-8:]
    hi8       = max(c["high"] for c in r8)
    lo8       = min(c["low"]  for c in r8)
    mid8      = (hi8 + lo8) / 2
    range_pct = ((hi8 - lo8) / mid8 * 100) if mid8 > 0 else 99.0

    price_pos = calc_swing_range_position(candles_4h, lookback=32)

    atr_compressed = atr_ratio  <= CONFIG["htf_atr_contract_ratio"]
    vol_building   = vol_ratio  >= CONFIG["htf_vol_ratio_min"]
    price_sideways = range_pct  <= CONFIG["htf_range_max_pct"]
    price_in_zone  = price_pos  <  CONFIG["htf_max_pos_in_range"]

    is_htf_accum = atr_compressed and vol_building and price_sideways and price_in_zone

    if is_htf_accum:
        label = (
            f"🕯️ 4H HTF Akumulasi — ATR {atr_ratio:.2f}, "
            f"vol {vol_ratio:.1f}x, range {range_pct:.1f}%, pos {price_pos:.0%}"
        )
    elif atr_compressed and price_sideways and price_in_zone:
        label = f"🕯️ 4H Konsolidasi (vol belum naik) — range {range_pct:.1f}%"
    else:
        label = "—"

    return {
        "is_htf_accum": is_htf_accum,
        "atr_ratio":    round(atr_ratio, 3),
        "vol_ratio":    round(vol_ratio, 2),
        "range_pct":    round(range_pct, 2),
        "price_pos":    price_pos,
        "label":        label,
    }

def detect_liquidity_sweep(candles, lookback=None):
    """
    Liquidity Sweep Detection — stop hunt sebelum reversal/pump.
    Pola: harga turun di bawah support, lalu candle close kembali di atas
    support dengan wick panjang → market maker sudah selesai ambil likuiditas.
    """
    if lookback is None:
        lookback = CONFIG["liq_sweep_lookback"]
    if len(candles) < lookback + 3:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0, "label": "Data kurang"}

    reference_candles = candles[-(lookback + 3):-3]
    if not reference_candles:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0, "label": "—"}

    lows_sorted   = sorted(c["low"] for c in reference_candles)
    support_level = sum(lows_sorted[:3]) / 3

    sweep_detected = False
    sweep_candle   = None
    sweep_low_val  = 0.0

    for candle in candles[-3:]:
        candle_range = candle["high"] - candle["low"]
        if candle_range <= 0:
            continue
        wick_bottom = (candle["open"] - candle["low"]
                       if candle["close"] > candle["open"]
                       else candle["close"] - candle["low"])
        wick_pct   = wick_bottom / candle_range

        went_below   = candle["low"]   < support_level
        closed_above = candle["close"] > support_level
        has_wick     = wick_pct >= CONFIG["liq_sweep_wick_min_pct"]

        if went_below and closed_above and has_wick:
            sweep_detected = True
            sweep_candle   = candle
            sweep_low_val  = candle["low"]
            break

    if sweep_detected and sweep_candle is not None:
        depth_pct = (support_level - sweep_low_val) / support_level * 100
        label = (
            f"🎯 Liquidity Sweep — low ${sweep_low_val:.6g} tembus support "
            f"${support_level:.6g} ({depth_pct:.2f}%), close kembali di atas"
        )
    else:
        label = "—"

    return {
        "is_sweep":  sweep_detected,
        "sweep_low": round(sweep_low_val, 8),
        "support":   round(support_level, 8),
        "label":     label,
    }

def detect_energy_buildup(candles_1h, oi_data):
    """
    Energy Build-Up Detector — "OI Build + Volume Bullish + Price Stuck".

    Pola absorption: market maker menyerap order sambil membangun posisi.
    Harga DITAHAN (sideways) meski volume dan OI naik = klasik pre-pump.

    Kondisi deteksi:
      1. OI naik > 5% (posisi baru dibangun) — FIX v18: sekarang bisa aktif
         karena OI snapshot di-load dari disk antar-run.
      2. Vol rata-rata 3h terbaru > 1.5x baseline 24h
      3. Price range 3h < 2.5% (harga tidak bergerak)
      4. Minimal 2 dari 3 candle terbaru bullish (buying pressure)
    """
    if len(candles_1h) < 24:
        return {
            "is_buildup": False, "is_strong": False,
            "oi_change": 0.0, "vol_ratio": 0.0, "range_pct": 0.0,
            "label": "Data tidak cukup",
        }

    # Kondisi 1: OI naik (sekarang bisa berfungsi karena snapshot persisten)
    oi_change = oi_data.get("change_pct", 0.0)
    oi_rising = (not oi_data.get("is_new", True)) and oi_change >= CONFIG["energy_oi_change_min"]

    # Kondisi 2: volume rata-rata 3 candle terbaru vs baseline
    recent_3 = candles_1h[-3:]
    vol_3h_avg = sum(c["volume_usd"] for c in recent_3) / 3
    baseline   = candles_1h[-24:-3]
    avg_vol    = sum(c["volume_usd"] for c in baseline) / len(baseline) if baseline else vol_3h_avg
    vol_ratio  = (vol_3h_avg / avg_vol) if avg_vol > 0 else 1.0
    vol_rising = vol_ratio >= CONFIG["energy_vol_ratio_min"]

    # Kondisi 3: harga tidak bergerak
    hi3       = max(c["high"]  for c in recent_3)
    lo3       = min(c["low"]   for c in recent_3)
    mid3      = (hi3 + lo3) / 2
    range_pct = ((hi3 - lo3) / mid3 * 100) if mid3 > 0 else 99.0
    price_stuck = range_pct <= CONFIG["energy_range_max_pct"]

    # Kondisi 4: mayoritas candle terbaru bullish
    bullish_count   = sum(1 for c in recent_3 if c["close"] >= c["open"])
    candles_bullish = bullish_count >= 2

    is_buildup = oi_rising and vol_rising and price_stuck and candles_bullish
    is_strong  = False   # akan di-set dari master_score jika funding <= 0

    if is_buildup:
        label = (
            f"⚡ ENERGY BUILD-UP — OI +{oi_change:.1f}%, vol {vol_ratio:.1f}x "
            f"(3h avg), range {range_pct:.1f}%, candle {bullish_count}/3 hijau"
        )
    else:
        conds = sum([oi_rising, vol_rising, price_stuck, candles_bullish])
        label = (
            f"— ({conds}/4 kondisi: OI={oi_rising}, vol={vol_rising}, "
            f"stuck={price_stuck}, bullish={candles_bullish})"
        )

    return {
        "is_buildup":       is_buildup,
        "is_strong":        is_strong,
        "oi_change":        round(oi_change, 2),
        "vol_ratio":        round(vol_ratio, 2),
        "range_pct":        round(range_pct, 2),
        "candles_bullish":  candles_bullish,
        "oi_rising":        oi_rising,
        "vol_rising":       vol_rising,
        "price_stuck":      price_stuck,
        "label":            label,
    }

def calc_uptrend_age(candles):
    """Berapa jam harga naik berturut-turut. Pre-pump ideal = streak pendek atau 0."""
    if len(candles) < 4:
        return {"age_hours": 0, "is_fresh": False, "is_late": False}
    streak = 0
    for i in range(len(candles) - 1, 0, -1):
        if candles[i]["close"] > candles[i-1]["close"]:
            streak += 1
        else:
            break
    return {
        "age_hours": streak,
        "is_fresh":  1 <= streak <= 8,
        "is_late":   streak > CONFIG["gate_uptrend_max_hours"],
    }

def calc_support_resistance(candles, lookback=48, n_levels=3):
    """Level S/R dari pivot point 48 candle terakhir."""
    if len(candles) < 10:
        return {"resistance": [], "support": [], "nearest_res": None, "nearest_sup": None}
    n      = min(lookback, len(candles))
    recent = candles[-n:]
    price  = candles[-1]["close"]

    pivots_high, pivots_low = [], []
    for i in range(1, len(recent) - 1):
        h, l = recent[i]["high"], recent[i]["low"]
        if h > recent[i-1]["high"] and h > recent[i+1]["high"]:
            pivots_high.append(h)
        if l < recent[i-1]["low"]  and l < recent[i+1]["low"]:
            pivots_low.append(l)

    def cluster_levels(levels, cluster_pct=0.005):
        if not levels:
            return []
        levels   = sorted(levels)
        clusters = []
        current  = [levels[0]]
        for lv in levels[1:]:
            if (lv - current[-1]) / current[-1] < cluster_pct:
                current.append(lv)
            else:
                clusters.append((sum(current) / len(current), len(current)))
                current = [lv]
        clusters.append((sum(current) / len(current), len(current)))
        clusters.sort(key=lambda x: -x[1])
        return [round(lv, 8) for lv, _ in clusters[:n_levels]]

    res_all  = cluster_levels(pivots_high)
    sup_all  = cluster_levels(pivots_low)
    resistance = sorted([r for r in res_all if r > price * 1.001])[:n_levels]
    support    = sorted([s for s in sup_all if s < price * 0.999], reverse=True)[:n_levels]

    def fmt(lv, ref):
        return {"level": round(lv, 8), "gap_pct": round((lv - ref) / ref * 100, 1)}

    return {
        "resistance":  [fmt(r, price) for r in resistance],
        "support":     [fmt(s, price) for s in support],
        "nearest_res": fmt(resistance[0], price) if resistance else None,
        "nearest_sup": fmt(support[0], price)    if support    else None,
    }

def calc_volume_ratio(candles, lookback=24):
    """Rasio volume candle terakhir vs rata-rata lookback candle sebelumnya."""
    if len(candles) < lookback + 1:
        return 0.0
    avg_vol = sum(c["volume_usd"] for c in candles[-(lookback + 1):-1]) / lookback
    if avg_vol <= 0:
        return 0.0
    return candles[-1]["volume_usd"] / avg_vol

def calc_volume_acceleration(candles):
    """Volume acceleration: vol 1h terbaru vs rata-rata 3h sebelumnya."""
    if len(candles) < 4:
        return 0.0
    vol_1h = candles[-1]["volume_usd"]
    vol_3h = sum(c["volume_usd"] for c in candles[-4:-1]) / 3
    if vol_3h <= 0:
        return 0.0
    return (vol_1h - vol_3h) / vol_3h

def check_volume_consistent(candles, lookback=3, min_ratio=1.5):
    """Volume tinggi harus konsisten ≥ 2 candle, bukan hanya 1 spike."""
    if len(candles) < 24:
        return False
    avg_vol   = sum(c["volume_usd"] for c in candles[-24:]) / 24
    if avg_vol <= 0:
        return False
    recent    = candles[-lookback:]
    above_avg = sum(1 for c in recent if c["volume_usd"] > avg_vol * min_ratio)
    return above_avg >= max(1, lookback // 2)

def calc_btc_correlation(coin_candles, btc_candles, lookback=24):
    """Pearson correlation coin vs BTC untuk mendeteksi pergerakan independen."""
    if not coin_candles or not btc_candles or len(coin_candles) < 5:
        return {"correlation": None, "label": "UNKNOWN", "emoji": "❓",
                "lookback": 0, "risk_note": "Data tidak cukup"}

    n   = min(lookback, len(coin_candles), len(btc_candles))
    c_c = coin_candles[-n:]
    c_b = btc_candles[-n:]

    def pct_changes(candles):
        return [(candles[i]["close"] - candles[i-1]["close"]) / candles[i-1]["close"]
                for i in range(1, len(candles)) if candles[i-1]["close"] > 0]

    cc = pct_changes(c_c)
    cb = pct_changes(c_b)
    mn = min(len(cc), len(cb))
    if mn < 5:
        return {"correlation": None, "label": "UNKNOWN", "emoji": "❓",
                "lookback": mn, "risk_note": "Data tidak cukup"}

    cc, cb = cc[-mn:], cb[-mn:]
    mc, mb = sum(cc) / mn, sum(cb) / mn
    num    = sum((x - mc) * (y - mb) for x, y in zip(cc, cb))
    sd_c   = (sum((x - mc)**2 for x in cc)) ** 0.5
    sd_b   = (sum((y - mb)**2 for y in cb)) ** 0.5

    corr = 0.0 if sd_c < 1e-10 or sd_b < 1e-10 else max(-1.0, min(1.0, num / (sd_c * sd_b)))

    if corr >= 0.75:
        label, emoji = "CORRELATED",  "🔗"
        risk_note    = "⚠️ Ikuti BTC! Jika BTC dump → exit cepat"
    elif corr >= 0.40:
        label, emoji = "MODERATE",    "〰️"
        risk_note    = "🔶 Sebagian ikuti BTC — pantau jika BTC turun"
    else:
        label, emoji = "INDEPENDENT", "🚀"
        risk_note    = "✅ Pergerakan independen — lebih tahan dump BTC"

    btc_chg  = ((c_b[-1]["close"] - c_b[0]["close"]) / c_b[0]["close"] * 100
                if len(c_b) >= 2 and c_b[0]["close"] > 0 else 0.0)
    coin_chg = ((c_c[-1]["close"] - c_c[0]["close"]) / c_c[0]["close"] * 100
                if len(c_c) >= 2 and c_c[0]["close"] > 0 else 0.0)

    if btc_chg <= CONFIG["btc_bearish_threshold"]:
        btc_regime, btc_re = "BEARISH", "🔻"
        btc_rn = f"⚠️ BTC bearish ({btc_chg:+.1f}%/{mn}h) — risiko tinggi"
    elif btc_chg >= CONFIG["btc_bullish_threshold"]:
        btc_regime, btc_re = "BULLISH", "🟢"
        btc_rn = f"✅ BTC bullish ({btc_chg:+.1f}%/{mn}h) — kondisi favorable"
    else:
        btc_regime, btc_re = "SIDEWAYS", "⬜"
        btc_rn = f"BTC sideways ({btc_chg:+.1f}%/{mn}h) — altcoin bisa independen"

    delta = coin_chg - btc_chg
    if delta >= CONFIG["outperform_min_delta"] and coin_chg > 0:
        op_label, op_emoji = "OUTPERFORM",   "🚀"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% (+{delta:.1f}%)"
    elif delta <= -CONFIG["outperform_min_delta"]:
        op_label, op_emoji = "UNDERPERFORM", "📉"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}% ({delta:.1f}%)"
    else:
        op_label, op_emoji = "IN-LINE",      "〰️"
        op_note = f"Coin {coin_chg:+.1f}% vs BTC {btc_chg:+.1f}%"

    return {
        "correlation":      round(corr, 3),
        "label":            label,
        "emoji":            emoji,
        "lookback":         mn,
        "risk_note":        risk_note,
        "btc_regime":       btc_regime,
        "btc_regime_emoji": btc_re,
        "btc_regime_note":  btc_rn,
        "btc_period_chg":   round(btc_chg, 2),
        "coin_period_chg":  round(coin_chg, 2),
        "outperform_label": op_label,
        "outperform_emoji": op_emoji,
        "outperform_note":  op_note,
        "delta_vs_btc":     round(delta, 2),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🎯  ENTRY & TARGET CALCULATION
# ══════════════════════════════════════════════════════════════════════════════

def find_swing_low_sl(candles, lookback=None):
    """Cari swing low terbaru dalam lookback candle sebagai dasar SL."""
    if lookback is None:
        lookback = CONFIG["sl_swing_lookback"]
    n = min(lookback, len(candles) - 1)
    if n < 2:
        return None
    recent_lows = [c["low"] for c in candles[-(n + 1):-1]]
    return min(recent_lows) * (1.0 - CONFIG["sl_swing_buffer"])

def calc_entry(candles, bos_level, alert_level, vwap, price_now, atr_abs_val=None, sr=None):
    """
    Entry / SL / Target — v18

    Entry:
      HIGH  → di atas BOS level + buffer kecil
      MEDIUM → VWAP atau market price

    SL:
      Swing low 12 candle, clamp [1x–3x ATR] dan [0.5%–8%]

    Target (3-tier, per-coin dinamis):
      Tier 1: Resistance pivot 48–168 candle
      Tier 2: ATR projection per-coin
      Tier 3: Fibonacci swing projection (fallback)
    """
    if atr_abs_val is None:
        atr_abs_val = calc_atr_abs(candles)

    # ── Entry ─────────────────────────────────────────────────────────────────
    gap_to_vwap_pct = (price_now - vwap) / vwap * 100 if vwap > 0 else 0

    if alert_level == "HIGH" and bos_level > 0 and bos_level < price_now * 1.05:
        entry        = bos_level * (1.0 + CONFIG["entry_bos_buffer"])
        entry_reason = "BOS breakout"
    elif gap_to_vwap_pct <= 2.0:
        entry        = max(vwap, price_now)
        entry_reason = "VWAP pullback"
    else:
        entry        = price_now * 1.001
        entry_reason = "market price"

    if entry < price_now:
        entry = price_now * 1.001

    # ── SL ────────────────────────────────────────────────────────────────────
    sl_swing = find_swing_low_sl(candles, lookback=12)
    if sl_swing is None or sl_swing >= entry:
        sl_swing = entry - atr_abs_val * 2.0

    sl_floor = entry - atr_abs_val * CONFIG["sl_atr_mult_max"]
    sl_ceil  = entry - atr_abs_val * CONFIG["sl_atr_mult_min"]
    sl       = max(sl_swing, sl_floor)
    sl       = min(sl, sl_ceil)
    sl       = max(sl, entry * (1.0 - CONFIG["max_sl_pct"] / 100.0))
    sl       = min(sl, entry * (1.0 - CONFIG["min_sl_pct"] / 100.0))
    if sl >= entry:
        sl = entry * 0.98

    # ── Target — kumpulkan resistance pivot ───────────────────────────────────
    res_levels = []

    if sr and sr.get("resistance"):
        for rv in sr["resistance"]:
            if rv["level"] > entry * 1.005:
                res_levels.append(rv["level"])

    lookback_long = min(168, len(candles))
    recent_long   = candles[-lookback_long:]
    pivot_highs   = []
    for i in range(2, len(recent_long) - 2):
        h = recent_long[i]["high"]
        if (h > recent_long[i-1]["high"] and h > recent_long[i-2]["high"] and
                h > recent_long[i+1]["high"] and h > recent_long[i+2]["high"]):
            pivot_highs.append(h)

    if pivot_highs:
        pivot_highs = sorted(set(pivot_highs))
        clusters, cur = [], [pivot_highs[0]]
        for ph in pivot_highs[1:]:
            if (ph - cur[-1]) / cur[-1] < 0.015:
                cur.append(ph)
            else:
                clusters.append(sum(cur) / len(cur))
                cur = [ph]
        clusters.append(sum(cur) / len(cur))
        for c_lv in clusters:
            if c_lv > entry * 1.005 and c_lv not in res_levels:
                res_levels.append(c_lv)

    res_levels = sorted(set(res_levels))

    # Swing range untuk proyeksi Fibonacci
    swing_low_val  = min(c["low"]  for c in recent_long)
    swing_high_val = max(c["high"] for c in recent_long)
    swing_range    = swing_high_val - swing_low_val
    price_pos_pct  = ((entry - swing_low_val) / swing_range) if swing_range > 0 else 0.5

    # ATR floor adaptif berdasarkan posisi harga dalam range
    if price_pos_pct < 0.4:
        atr_mult_t1, atr_mult_t2 = 3.5, 6.5
    elif price_pos_pct < 0.6:
        atr_mult_t1, atr_mult_t2 = 2.5, 5.0
    else:
        atr_mult_t1, atr_mult_t2 = 1.5, 3.0

    atr_floor_t1 = entry + atr_abs_val * atr_mult_t1
    atr_floor_t2 = entry + atr_abs_val * atr_mult_t2

    if res_levels:
        t1, t1_source = res_levels[0], "R1 pivot"
        if len(res_levels) >= 2:
            t2, t2_source = res_levels[1], "R2 pivot"
        else:
            t2, t2_source = t1 * 1.272, "R1 × 1.272"
        if t1 < atr_floor_t1:
            t1, t1_source = atr_floor_t1, f"ATR×{atr_mult_t1:.1f} (R1 terlalu dekat)"
        if t2 < atr_floor_t2:
            t2, t2_source = atr_floor_t2, f"ATR×{atr_mult_t2:.1f}"
    else:
        swing_valid = swing_range > atr_abs_val * 2 and swing_low_val < entry
        if swing_valid:
            t1, t1_source = entry + swing_range * 0.382, "Fib 38.2% swing"
            t2, t2_source = entry + swing_range * 0.618, "Fib 61.8% swing"
        else:
            t1, t1_source = atr_floor_t1, f"ATR×{atr_mult_t1:.1f}"
            t2, t2_source = atr_floor_t2, f"ATR×{atr_mult_t2:.1f}"
        if t1 < atr_floor_t1:
            t1 = atr_floor_t1
        if t2 < atr_floor_t2:
            t2 = atr_floor_t2

    if t2 <= t1:
        t2        = t1 * (1 + (atr_abs_val / entry) * atr_mult_t1)
        t2_source = "T1 + ATR ext"

    t1 = max(t1, entry * 1.005)
    t2 = max(t2, t1   * 1.005)

    risk   = entry - sl
    rr_val = round((t1 - entry) / risk, 1) if risk > 0 else 0.0

    return {
        "entry":           round(entry, 8),
        "sl":              round(sl, 8),
        "sl_pct":          round((entry - sl) / entry * 100, 2),
        "t1":              round(t1, 8),
        "t2":              round(t2, 8),
        "rr":              rr_val,
        "rr_str":          f"{rr_val:.1f}",
        "vwap":            round(vwap, 8),
        "bos_level":       round(bos_level, 8),
        "alert_level":     alert_level,
        "gain_t1_pct":     round((t1 - entry) / entry * 100, 1),
        "gain_t2_pct":     round((t2 - entry) / entry * 100, 1),
        "atr_abs":         round(atr_abs_val, 8),
        "sl_method":       entry_reason,
        "used_resistance": len(res_levels) > 0,
        "n_res_levels":    len(res_levels),
        "t1_source":       t1_source,
        "t2_source":       t2_source,
        "atr_pct_abs":     round(atr_abs_val / entry * 100, 2),
        "swing_range_pct": round(swing_range / entry * 100, 1) if entry > 0 else 0.0,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  🧠  MASTER SCORE
# ══════════════════════════════════════════════════════════════════════════════
def master_score(symbol, ticker):
    c1h   = get_candles(symbol, "1h",  CONFIG["candle_1h"])
    c4h   = get_candles(symbol, "4h",  CONFIG["candle_4h"])
    c15m  = get_candles(symbol, "15m", 30)   # 15m candles untuk buy pressure
    c5m   = get_candles(symbol, "5m",  36)   # NEW v18: 5m candles untuk micro momentum

    if len(c1h) < 48:
        log.info(f"  {symbol}: Candle 1h tidak cukup ({len(c1h)} < 48)")
        return None

    try:
        vol_24h   = float(ticker.get("quoteVolume", 0))
        chg_24h   = float(ticker.get("change24h",  0)) * 100
        price_now = float(ticker.get("lastPr",      0)) or c1h[-1]["close"]
    except Exception:
        return None

    if vol_24h <= 0 or price_now <= 0:
        return None

    # ── GATE 0: Open Interest minimum ────────────────────────────────────────
    oi_data = get_oi_change(symbol)
    if oi_data["oi_now"] > 0 and oi_data["oi_now"] < CONFIG["min_oi_usd"]:
        log.info(
            f"  {symbol}: OI ${oi_data['oi_now']:,.0f} < ${CONFIG['min_oi_usd']:,} "
            f"— GATE GAGAL (coin illiquid)"
        )
        return None

    # ── GATE 1: Funding — ambil dan simpan snapshot ───────────────────────────
    funding = get_funding(symbol)
    add_funding_snapshot(symbol, funding)
    fstats  = get_funding_stats(symbol)
    if fstats is None:
        fstats = {
            "avg": funding, "cumulative": funding, "neg_pct": 0.0,
            "streak": 0, "basis": funding * 100, "current": funding,
            "sample_count": 1,
        }
        log.info(f"  {symbol}: Funding snapshot baru (1 data) — lanjut scan")

    # ── GATE 2: VWAP dengan toleransi ────────────────────────────────────────
    vwap            = calc_vwap(c1h, lookback=24)
    vwap_gate_level = vwap * CONFIG["vwap_gate_tolerance"]
    # FIX v24: no longer hard-reject on VWAP — apply score penalty instead
    # Coins below VWAP can still be valid reversal setups
    _vwap_penalty = 0
    if price_now < vwap_gate_level:
        _vwap_penalty = CONFIG.get("penalty_below_vwap_v24", -3)
        log.info(
            f"  {symbol}: Harga ${price_now:.6g} < VWAP gate ${vwap_gate_level:.6g} "
            f"— penalti {_vwap_penalty} (tidak reject)"
        )

    # ── Hitung semua indikator ────────────────────────────────────────────────
    bbw, bb_pct      = calc_bbw(c1h)
    atr_pct          = calc_atr_pct(c1h)
    atr_abs_val      = calc_atr_abs(c1h)
    atr_contr        = calc_atr_contracting(c1h)
    rsi              = get_rsi(c1h[-48:])
    bos_up, bos_level = detect_bos_up(c1h)
    higher_low       = higher_low_detected(c1h)
    vol_ratio        = calc_volume_ratio(c1h)
    vol_accel        = calc_volume_acceleration(c1h)
    vol_consistent   = check_volume_consistent(c1h)
    uptrend          = calc_uptrend_age(c1h)
    sr               = calc_support_resistance(c1h)
    btc_candles      = get_btc_candles_cached(48)
    btc_corr         = calc_btc_correlation(c1h, btc_candles, lookback=24)
    accum            = calc_accumulation_phase(c1h)
    htf_accum        = calc_htf_accumulation(c4h)
    liq_sweep        = detect_liquidity_sweep(c1h)
    energy           = detect_energy_buildup(c1h, oi_data)
    price_pos_48     = calc_swing_range_position(c1h, lookback=48)
    candle_dir_ratio = calc_candle_direction_ratio(c1h, lookback=6)

    # ── NEW v18/v19: Phase 1-2-3 + Micro Momentum + Timing + v19 signals ────
    vol_spike    = calc_volume_spike(c1h)
    micro_mom    = calc_micro_momentum(c5m)          # v18: 5m micro momentum
    mom_accel    = calc_momentum_acceleration(c1h)   # 1h secondary signal
    buy_press    = calc_buy_pressure(c15m)
    whale_order  = detect_whale_order(c15m, oi_data) # v18: upgraded whale
    fake_pump    = detect_fake_pump(c1h, buy_press["buy_ratio"], oi_data)  # v18: upgraded fake

    # ── NEW v19 indicators ────────────────────────────────────────────────────
    ema_trend      = calc_ema_trend(c1h)             # STEP 6: EMA20/EMA50 trend filter
    # v26 PERF: compute vol_zscore_v20 first (window=20), then derive legacy
    # vol_zscore from it — avoids iterating the same volume array twice
    vol_zscore_v20 = calc_vol_zscore_v20(c1h, window=20)    # shared base z-score
    _z19 = vol_zscore_v20["z"]
    _z19_anomaly = _z19 > CONFIG["vol_zscore_boost"]   # vol_zscore_boost = 3.0
    vol_zscore = {
        "z":          _z19,
        "is_anomaly": _z19_anomaly,
        "mean_vol":   vol_zscore_v20["mean"],
        "std_vol":    vol_zscore_v20["std"],
        "boost":      8 if _z19 > 5 else (5 if _z19_anomaly else 0),
        "label":      (f"🔺 Volume Z-Score {_z19:.1f}σ — aktivitas tidak normal (anomaly)"
                       if _z19_anomaly else f"Volume Z-Score {_z19:.1f}σ — normal"),
    }
    micro_breakout = calc_micro_breakout(c1h)        # STEP 11: highest_high breakout
    candle_imbal   = calc_candle_imbalance(c15m)     # STEP 12: orderbook proxy
    whale_accum    = detect_whale_accumulation(c1h)  # STEP 16: whale accumulation
    wick_filter    = calc_wick_ratio(c1h)            # STEP 9: wick trap detection

    # ── NEW v20 indicators ────────────────────────────────────────────────────
    # v26 PERF: derive ema20_slope from already-computed ema_trend — avoids
    # rebuilding the full EMA20 series a second time over the same c1h data
    _et_slope = ema_trend.get("ema20_slope_val", 0.0)
    _et_ema20 = ema_trend.get("ema20", 0.0)
    _is_rising = _et_slope > 0
    ema20_slope = {
        "slope":      _et_slope,
        "ema20_now":  _et_ema20,
        "ema20_prev": _et_ema20 - _et_slope,
        "is_rising":  _is_rising,
        "score":      CONFIG["score_ema20_slope"] if _is_rising else 0,
        "label":      (f"📈 EMA20 Slope naik +{_et_slope:.6g} ({CONFIG['ema20_slope_lookback']}c ago)"
                       if _is_rising else f"📉 EMA20 Slope turun {_et_slope:.6g}"),
    }
    ema200_dist   = calc_ema200_distance(c1h)               # PART 1: distance from EMA200
    higher_low_v20 = calc_higher_low_v20(c1h)               # PART 1: higher low structure
    cross_up, ema20_val_v20, ema50_val_v20 = calc_ema20_cross_up(c1h)  # PART 2

    # Inject cross_up into ema_trend for use by reversal validator
    ema_trend["cross_up"] = cross_up

    # FIX 01 v22: also validate z-score for reversal override
    # If REVERSAL_SETUP but z < threshold → still reject
    if (ema_trend.get("trend") == "REVERSAL_SETUP"
            and vol_zscore_v20["z"] < CONFIG["ema50_override_zscore_min"]):
        ema_trend["should_reject"] = True
        ema_trend["label"] += f" [z={vol_zscore_v20['z']:.2f} < {CONFIG['ema50_override_zscore_min']} — override cancelled]"

    # Inject vol_z for dump filter (passed via ema20_slope data dict)
    ema20_slope["vol_z_for_dump"] = vol_zscore_v20["z"]

    # PART 3: Dump filter — early rejection before scoring
    dump_reject, dump_reason = check_dump_filter_v20(c5m, c15m, price_now, ema20_slope)
    if dump_reject:
        log.info(f"  {symbol}: {dump_reason} — GATE GAGAL (dump filter v20)")
        return None

    # ── NEW v22 indicators ────────────────────────────────────────────────────
    smart_money_v22  = detect_smart_money_accumulation_v22(c1h)
    liq_trap_v22     = detect_liquidity_trap_v22(c1h, vol_zscore_v20["z"])
    whale_fp_v22     = detect_whale_footprint_v22(c1h)
    prebreakout_v22  = detect_prebreakout_pressure_v22(c1h)
    mom_ignition_v22 = detect_momentum_ignition_v22(c1h, vol_zscore_v20["z"])

    # FIX 07: Dump Trap Filter — FIX v24: penalise instead of hard-reject
    # FIX v25: hard-reject only if score goes negative (coin is net negative signal)
    _dump_trap_penalty = 0
    dump_trap_reject, dump_trap_reason = check_dump_trap_v22(c1h, ema200_dist, vol_zscore_v20)
    if dump_trap_reject:
        _dump_trap_penalty = CONFIG.get("penalty_dump_trap_v24", -10)
        log.info(
            f"  {symbol}: {dump_trap_reason} — penalti {_dump_trap_penalty}"
        )

    # FIX 08: Improved reversal check (informational, used in scoring)
    rev_valid_v22, rev_conf_v22, rev_label_v22 = calc_improved_reversal_v22(
        price_now, vwap, ema20_slope, vol_zscore_v20
    )

    # PART 2: Reversal validation (v20)
    reversal_valid = validate_reversal_v20(
        ema_trend, price_now, vol_zscore_v20, ema20_slope,
        micro_breakout, candle_imbal
    )

    # ── NEW v23 — Market Maker Detection ─────────────────────────────────────
    liq_sweep_v23  = detect_liquidity_sweep_v23(c1h, vol_zscore_v20["z"])
    whale_abs_v23  = detect_whale_absorption_v23(c1h)
    vol_comp_v23   = detect_volatility_compression_v23(c1h)
    mom_ign_v23    = detect_momentum_ignition_v23(c1h, vol_zscore_v20["z"])
    ob_press_v23   = calc_orderbook_pressure_v23(vol_zscore_v20, candle_imbal)
    spoofing_v23   = detect_spoofing_v23(c1h)

    # PART 4: Weighted scoring bonus (computed later in scoring section)

    # ── NEW v27 — Pre-Pump Intelligence Modules ───────────────────────────────
    # All 7 modules are additive. No hard rejects. Falls back gracefully.
    # Modules reuse already-computed variables — minimal extra computation.
    _v27_mom_accel   = detect_momentum_acceleration_v27(c5m, c1h)
    _v27_vol_exp     = detect_volume_expansion_v27(c1h)
    _v27_oi_exp      = detect_oi_expansion_v27(oi_data)
    _v27_atr_break   = detect_atr_breakout_v27(
                            price_now,
                            ema_trend.get("ema20") or 0.0,
                            atr_abs_val)
    _v27_ob_imbal    = get_orderbook_imbalance_v27(symbol)   # 1 extra API call
    _v27_whale_accum = detect_whale_accumulation_v27(c1h)
    _v27_liq_sweep_up = detect_liquidity_sweep_up_v27(c1h)

    # Aggregate into pump_probability_score_v27
    _v27_pump_score_data = calc_pump_probability_score_v27(
        _v27_mom_accel, _v27_vol_exp, _v27_oi_exp,
        _v27_atr_break, _v27_ob_imbal, _v27_whale_accum, _v27_liq_sweep_up
    )


    # Set energy.is_strong jika funding negatif
    if energy["is_buildup"] and fstats.get("current", 1) <= 0:
        energy["is_strong"] = True
        energy["label"]     = energy["label"] + " 🔥 + funding negatif (squeeze)"

    # Rate candle di atas VWAP (6 candle terbaru)
    above_vwap_rate = 0.0
    if len(c1h) >= 6:
        above           = sum(1 for c in c1h[-6:] if c["close"] > vwap)
        above_vwap_rate = above / 6

    # Price change 1h
    price_chg = 0.0
    if len(c1h) >= 2 and c1h[-2]["close"] > 0:
        price_chg = (c1h[-1]["close"] - c1h[-2]["close"]) / c1h[-2]["close"] * 100

    # ── GATE 3: Uptrend tidak terlalu tua ────────────────────────────────────
    if uptrend["is_late"]:
        log.info(
            f"  {symbol}: Uptrend sudah {uptrend['age_hours']}h — "
            f"terlalu tua, kemungkinan distribusi (GATE GAGAL)"
        )
        return None

    # ── GATE 4: RSI tidak overbought ─────────────────────────────────────────
    if rsi >= CONFIG["gate_rsi_max"]:
        log.info(
            f"  {symbol}: RSI {rsi:.1f} ≥ {CONFIG['gate_rsi_max']} — "
            f"overbought (GATE GAGAL)"
        )
        return None

    # ── GATE 5: BB Position — FIX v23: convert hard reject to breakout bonus ──
    # Old: bb_pct >= 1.05 → reject  (blocked real breakout pumps)
    # New: bb_pct >= 1.10 → add breakout bonus; soft warn only
    _bb_breakout_bonus = 0
    if bb_pct >= CONFIG["gate_bb_pos_max"]:
        # No longer rejected — price above upper BB = confirmed breakout
        _bb_breakout_bonus = CONFIG.get("score_bb_breakout_v23", 5)
        log.info(
            f"  {symbol}: BB pos {bb_pct*100:.0f}% ≥ upper — "
            f"BB breakout (bonus +{_bb_breakout_bonus})"
        )

    # ── GATE 6: Harga tidak di zona distribusi atas ───────────────────────────
    # FIX v23: 85% → 97% — previous threshold blocked too many breakout setups
    if price_pos_48 > 0.97:
        log.info(
            f"  {symbol}: Posisi harga {price_pos_48:.0%} dari swing range — "
            f"zona distribusi atas (GATE GAGAL)"
        )
        return None

    # ── GATE 7: STEP 6 v19 — EMA Trend Filter
    # FIX v24: price < EMA50 → penalty only
    # FIX v25: hard-reject ONLY when all 3 severe conditions met simultaneously
    _ema50_penalty = 0
    if ema_trend["should_reject"]:
        # Check if this is a true dump (price<EMA200 + slope<0 + ask/bid>1.8)
        _above200     = ema200_dist.get("above_ema200", True)
        _slope_rising = ema_trend.get("ema20_slope_val", 0) >= 0
        _bar_g7       = vol_zscore_v20.get("bid_ask_ratio", 1.0)
        _ask_bid_g7   = 1.0 / _bar_g7 if _bar_g7 > 0 else 1.0
        if (not _above200
                and not _slope_rising
                and _ask_bid_g7 > CONFIG.get("ema50_hard_reject_ask_bid", 1.8)):
            log.info(
                f"  {symbol}: {ema_trend['label']} + price<EMA200 + "
                f"ask/bid {_ask_bid_g7:.2f}>1.8 — GATE GAGAL v25 (triple dump)"
            )
            return None
        _ema50_penalty = CONFIG.get("penalty_below_ema50_v24", -5)
        log.info(
            f"  {symbol}: {ema_trend['label']} — penalti {_ema50_penalty}"
        )

    # ── GATE 8: STEP 7 v19 — VWAP Bias (pump setup mulai di atas VWAP)
    # Sudah ada vwap_gate_tolerance (0.97) di GATE 2, tapi STEP 7 lebih ketat
    # Gunakan sebagai soft gate: tambah penalti, bukan reject penuh
    # (karena GATE 2 sudah handle reject di bawah 97% VWAP)

    # ── GATE 9: STEP 8 v19 — Momentum Validation (5m price change)
    # FIX v23: only reject if price dropping > 0.5%, flat is fine
    if c5m and len(c5m) >= 2:
        price_chg_5m = (c5m[-1]["close"] - c5m[-2]["close"]) / c5m[-2]["close"]
        if price_chg_5m <= CONFIG["momentum_val_reject"]:
            log.info(
                f"  {symbol}: 5m price change {price_chg_5m*100:+.3f}% < "
                f"{CONFIG['momentum_val_reject']*100:.1f}% "
                f"— momentum negatif kuat (GATE GAGAL)"
            )
            return None
    else:
        price_chg_5m = 0.001  # fallback jika tidak ada data 5m

    # ── GATE 10: STEP 13 v19 — Noise Filter (volume & ATR minimum)
    if vol_24h < CONFIG["noise_min_vol_24h"]:
        # BUG FIX v25: use adaptive K/M format so $500K doesn't log as "$0M"
        _thr     = CONFIG["noise_min_vol_24h"]
        _thr_str = f"${_thr/1e6:.1f}M" if _thr >= 1_000_000 else f"${_thr/1e3:.0f}K"
        _vol_str = f"${vol_24h/1e6:.1f}M" if vol_24h >= 1_000_000 else f"${vol_24h/1e3:.0f}K"
        log.info(
            f"  {symbol}: Vol 24h {_vol_str} < {_thr_str} "
            f"— coin terlalu illiquid (GATE GAGAL)"
        )
        return None
    if atr_pct < CONFIG["noise_min_atr_pct"]:
        log.info(
            f"  {symbol}: ATR {atr_pct:.3f}% < {CONFIG['noise_min_atr_pct']}% "
            f"— volatilitas terlalu rendah (GATE GAGAL)"
        )
        return None

    # ── GATE 11: STEP 9 v19 — Wick Trap Filter
    # FIX v25: soft penalty at 0.55, hard reject only at 0.75 + ask/bid > 2.0
    _wick_penalty = 0
    if wick_filter["is_trap"] and wick_filter["ratio"] > 0.55:
        _bar_for_wick = vol_zscore_v20.get("bid_ask_ratio", 1.0)
        _ask_bid_wick = 1.0 / _bar_for_wick if _bar_for_wick > 0 else 1.0
        if wick_filter["ratio"] > 0.75 and _ask_bid_wick > 2.0:
            # Still hard-reject on extreme wick + confirmed sell pressure
            log.info(
                f"  {symbol}: Wick {wick_filter['ratio']:.2f} > 0.75 + "
                f"ask/bid {_ask_bid_wick:.2f} > 2.0 — GATE GAGAL v25"
            )
            return None
        else:
            _wick_penalty = CONFIG.get("penalty_wick_v25", -3)
            log.info(
                f"  {symbol}: Wick {wick_filter['ratio']:.2f} — penalti {_wick_penalty}"
            )

    # ══════════════════════════════════════════════════════════════════════════
    #  SCORING v19 — AI Weighted + Traditional heuristic (hybrid)
    # ══════════════════════════════════════════════════════════════════════════
    score   = 0
    signals = []

    # ── v24: Apply deferred soft penalties (VWAP, EMA50, dump trap) ──────────
    # These were converted from hard-rejects to score deductions so that
    # candidates are ranked low rather than silently dropped.
    if _vwap_penalty < 0:
        score += _vwap_penalty
        signals.append(
            f"⚠️ Below VWAP gate — penalti {_vwap_penalty}"
        )
    if _ema50_penalty < 0:
        score += _ema50_penalty
        signals.append(
            f"⚠️ {ema_trend['label']} — penalti {_ema50_penalty}"
        )
    if _dump_trap_penalty < 0:
        score += _dump_trap_penalty
        signals.append(
            f"⚠️ Dump trap aktif — penalti {_dump_trap_penalty}"
        )
    # FIX v25: wick penalty (soft, from updated GATE 11)
    if _wick_penalty < 0:
        score += _wick_penalty
        signals.append(
            f"⚠️ Wick trap {wick_filter['ratio']:.2f} — penalti {_wick_penalty}"
        )

    # ── 0a. Volume Spike (Phase 1) ────────────────────────────────────────────
    if vol_spike["tier"] > 0:
        score += vol_spike["score"]
        signals.append(vol_spike["label"])

    # ── 0a+. FIX v23: BB Breakout bonus (replaces hard reject from GATE 5) ───
    if _bb_breakout_bonus > 0:
        score += _bb_breakout_bonus
        signals.append(f"📈 BB Breakout: price above upper band — confirmed breakout (+{_bb_breakout_bonus})")

    # ── 0b. Buy Pressure (Phase 2) ────────────────────────────────────────────
    if buy_press["tier"] > 0:
        score += buy_press["score"]
        signals.append(buy_press["label"])

    # ── 0c. NEW v18: Micro Momentum (Phase 3 — 5m responsif) ─────────────────
    if micro_mom["is_accelerating"]:
        score += micro_mom["score"]
        signals.append(micro_mom["label"])

    # ── 0d. 1h Momentum Acceleration (secondary signal) ──────────────────────
    if mom_accel["is_accelerating"] and not micro_mom["is_accelerating"]:
        # Hanya aktif jika micro_mom tidak aktif (hindari double count)
        score += mom_accel["score"]
        signals.append(mom_accel["label"])

    # ── 0e. Whale Order Detection (v18 upgraded) ──────────────────────────────
    if whale_order["is_whale"]:
        score += whale_order["score"]
        signals.append(whale_order["label"])

    # ── 0f. Fake Pump Penalty (v18 upgraded, bertingkat) ─────────────────────
    if fake_pump["is_fake"]:
        score += fake_pump["penalty"]   # nilai negatif −5/−12/−20
        signals.append(fake_pump["label"])

    # ── 0g. NEW v19: EMA Trend Penalty (STEP 6) ───────────────────────────────
    if ema_trend["score_penalty"] < 0:
        score += ema_trend["score_penalty"]
        signals.append(ema_trend["label"])
    elif ema_trend["trend"] in ("UPTREND",):
        score += 3   # bonus untuk uptrend konfirmasi
        signals.append(ema_trend["label"])

    # ── 0h. NEW v19: Volume Z-Score boost (STEP 10) ───────────────────────────
    if vol_zscore["is_anomaly"]:
        score += vol_zscore["boost"]
        signals.append(vol_zscore["label"])

    # ── 0i. NEW v19: Micro Breakout (STEP 11) ─────────────────────────────────
    if micro_breakout["is_breakout"]:
        score += micro_breakout["score"]
        signals.append(micro_breakout["label"])

    # ── 0j. NEW v19: Orderbook Imbalance proxy (STEP 12) ─────────────────────
    if candle_imbal["is_bullish"]:
        score += candle_imbal["score"]
        signals.append(candle_imbal["label"])
    # v26: additional calibrated OB bonus from bid/ask ratio (spec: +5 for bullish OB)
    _ob_bar = vol_zscore_v20.get("bid_ask_ratio", 1.0)
    if _ob_bar >= CONFIG.get("bid_ask_ratio_strong", 1.3):
        _ob_bonus = CONFIG.get("score_ob_imbalance_v26", 5)
        score += _ob_bonus
        signals.append(
            f"📊 OB Imbalance v26: bid/ask {_ob_bar:.2f} ≥ {CONFIG.get('bid_ask_ratio_strong', 1.3)} "
            f"— buy dominant (+{_ob_bonus})"
        )

    # ── 0k. NEW v19: Whale Accumulation (STEP 16) ────────────────────────────
    if whale_accum["is_accum"]:
        score += whale_accum["score"]
        signals.append(whale_accum["label"])

    # ── 0l. NEW v19: Wick Trap penalty (STEP 9 — soft, severe already gated) ─
    if wick_filter["is_trap"] and wick_filter["penalty"] < 0:
        score += wick_filter["penalty"]
        signals.append(wick_filter["label"])

    # ── 0m. NEW v19: Early Pump Detection (STEP 14) ──────────────────────────
    early_pump_detected = (
        vol_accel > 0.3
        and price_now > vwap
        and price_pos_48 < CONFIG["early_pump_range_pos_max"]
        and micro_mom.get("is_accelerating", False)
    )
    if early_pump_detected:
        score += CONFIG["score_early_pump"]
        signals.append(
            f"⚡ EARLY PUMP SIGNAL — vol_accel {vol_accel*100:.0f}%, "
            f"price>VWAP, pos {price_pos_48:.0%}<40%, micro_mom aktif"
        )

    # ── 1. BB Squeeze ─────────────────────────────────────────────────────────
    bb_squeeze = bbw < CONFIG["bb_squeeze_threshold"]
    if bb_squeeze:
        score += CONFIG["score_bb_squeeze"]
        signals.append(
            f"🗜️ BB Squeeze aktif (BBW {bbw*100:.2f}% < {CONFIG['bb_squeeze_threshold']*100:.0f}%) "
            f"— kompresi energi sebelum breakout"
        )

    # ── 2. ATR Contracting ────────────────────────────────────────────────────
    if atr_contr["is_contracting"]:
        score += CONFIG["score_atr_contracting"]
        signals.append(
            f"📉 ATR Menyempit — rasio {atr_contr['ratio']:.2f} "
            f"(ATR 6c = {atr_contr['ratio']*100:.0f}% dari ATR 24c) — energi menumpuk"
        )

    # ── 3. Energy Build-Up ────────────────────────────────────────────────────
    if energy["is_buildup"]:
        score += CONFIG["score_energy_buildup"]
        signals.append(energy["label"])
        if energy["is_strong"]:
            score   += 2
            signals.append("⭐ Energy Build-Up + Funding Negatif = squeeze probability tinggi")

    # ── 4. Smart Money Accumulation + Volatility Compression ─────────────────
    # FIX v18: isolasi scoring — vol_compression TIDAK dapat skor jika
    # is_accumulating sudah aktif (mencegah double counting sinyal ATR).
    if accum["is_accumulating"] and accum["is_vol_compress"]:
        score += CONFIG["score_accumulation"] + CONFIG["score_vol_compression"]
        signals.append(
            f"🏦 AKUMULASI + VOL COMPRESSION — vol {accum['vol_ratio_4h']:.1f}x, "
            f"range {accum['price_range_pct']:.1f}%, pos {accum['price_pos']:.0%}"
        )
    elif accum["is_accumulating"]:
        score += CONFIG["score_accumulation"]
        signals.append(
            f"📦 Smart Money Accumulation — vol {accum['vol_ratio_4h']:.1f}x, "
            f"sideways {accum['price_range_pct']:.1f}%, pos {accum['price_pos']:.0%}"
        )
    elif accum["is_vol_compress"]:
        # FIX v18: hanya aktif jika is_accumulating=False
        score += CONFIG["score_vol_compression"]
        signals.append(
            f"🗜️ Volatility Compression — ATR {accum['atr_contract']:.2f}x dari baseline"
        )

    # ── 5. HTF Accumulation 4H ────────────────────────────────────────────────
    if htf_accum["is_htf_accum"]:
        score += CONFIG["score_htf_accumulation"]
        signals.append(htf_accum["label"])

    # ── 6. Liquidity Sweep ────────────────────────────────────────────────────
    if liq_sweep["is_sweep"]:
        score += CONFIG["score_liquidity_sweep"]
        signals.append(liq_sweep["label"])

    # ── 7. OI Expansion ───────────────────────────────────────────────────────
    # Guard: skip jika energy_buildup aktif (OI sudah dihitung di sana).
    if not energy["is_buildup"]:
        if not oi_data["is_new"] and oi_data["oi_now"] > 0:
            chg = oi_data["change_pct"]
            if chg >= CONFIG["oi_strong_pct"]:
                score += CONFIG["score_oi_strong"]
                signals.append(f"📈 OI Expansion KUAT +{chg:.1f}% — posisi leverage besar dibangun")
            elif chg >= CONFIG["oi_change_min_pct"]:
                score += CONFIG["score_oi_expansion"]
                signals.append(f"📊 OI Expansion +{chg:.1f}% — akumulasi posisi futures")
        elif oi_data["is_new"] and oi_data["oi_now"] > 0:
            signals.append(
                f"📊 OI baseline ${oi_data['oi_now']/1e6:.2f}M (snapshot pertama)"
            )
    else:
        if oi_data["oi_now"] > 0:
            ov     = oi_data["oi_now"]
            os_str = (f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K")
            chg_str = (f"+{oi_data['change_pct']:.1f}%" if not oi_data.get("is_new")
                       else "baseline")
            signals.append(f"📊 OI: {os_str} ({chg_str}) — sudah termasuk dalam Energy Build-Up")

    # ── 8. Volume dengan konteks arah harga ──────────────────────────────────
    if vol_ratio > CONFIG["vol_ratio_threshold"] and vol_consistent:
        if candle_dir_ratio >= CONFIG["vol_bullish_min_ratio"]:
            score += CONFIG["score_vol_bullish"]
            signals.append(
                f"🟢 Volume {vol_ratio:.1f}x rata-rata + {candle_dir_ratio*100:.0f}% candle "
                f"bullish — buying pressure konsisten"
            )
        else:
            signals.append(
                f"⚠️ Volume {vol_ratio:.1f}x tapi {candle_dir_ratio*100:.0f}% candle "
                f"bullish — kemungkinan distribusi/short, skor TIDAK ditambah"
            )

    # ── 9. Volume Acceleration dengan konteks arah ────────────────────────────
    if vol_accel > CONFIG["vol_accel_threshold"] and vol_consistent:
        last_candle_bullish = c1h[-1]["close"] >= c1h[-1]["open"]
        if last_candle_bullish:
            score += CONFIG["score_vol_accel"]
            signals.append(
                f"📈 Volume acceleration {vol_accel*100:.0f}% — candle terbaru bullish"
            )
        else:
            signals.append(
                f"⚠️ Volume acceleration {vol_accel*100:.0f}% tapi candle terbaru merah "
                f"— kemungkinan distribusi agresif"
            )

    # ── 10. RSI ideal pre-pump = 40–60 ────────────────────────────────────────
    rsi_in_ideal_zone = CONFIG["rsi_ideal_min"] <= rsi <= CONFIG["rsi_ideal_max"]
    if rsi_in_ideal_zone:
        score += CONFIG["score_rsi_ideal"]
        signals.append(
            f"📊 RSI {rsi:.1f} — zona ideal pre-pump (40–60): "
            f"belum overbought, momentum mulai terbentuk"
        )
    elif rsi < CONFIG["rsi_ideal_min"]:
        signals.append(f"📊 RSI {rsi:.1f} — oversold (bisa reversal, tapi belum konfirmasi)")
    else:
        signals.append(f"📊 RSI {rsi:.1f} — di atas zona ideal, momentum sudah berjalan")

    # ── 11. Higher Low ────────────────────────────────────────────────────────
    if higher_low:
        score += CONFIG["score_higher_low"]
        signals.append("🔼 Higher Low terdeteksi — struktur bullish awal mulai terbentuk")

    # ── 12. BOS Up ────────────────────────────────────────────────────────────
    if bos_up:
        score += CONFIG["score_bos_up"]
        signals.append(
            f"🔺 BOS Up (level {_fmt_price(bos_level)}) — breakout minor, "
            f"konfirmasi struktur berbalik (skor rendah: idealnya deteksi sebelum BOS)"
        )

    # ── 13. Funding rate ──────────────────────────────────────────────────────
    # FIX v18: mutual exclusion guard — funding_neg_pct dan funding_streak
    # hanya aktif jika funding_avg_neg TIDAK aktif, mencegah double scoring
    # dari sumber yang sama (funding negatif) hingga +8 poin.
    f_avg = fstats["avg"]
    funding_avg_neg_active = False

    if f_avg <= CONFIG["funding_bonus_avg"]:
        score += CONFIG["score_funding_avg_neg"]
        funding_avg_neg_active = True
        signals.append(f"⭐ Funding avg {f_avg:.6f} — sangat negatif (short squeeze setup)")
    elif fstats["cumulative"] <= CONFIG["funding_bonus_cumul"]:
        score += CONFIG["score_funding_cumul"]
        signals.append(f"Funding kumulatif {fstats['cumulative']:.5f} — akumulasi negatif")
    elif f_avg < 0:
        signals.append(f"Funding avg {f_avg:.6f} — negatif ringan (favorable)")
    elif f_avg >= CONFIG["funding_penalty_avg"]:
        score -= 2
        signals.append(
            f"⚠️ Funding avg {f_avg:.6f} — sangat positif (penalti: overbought)"
        )
    else:
        signals.append(f"Funding avg {f_avg:.6f} — netral")

    # FIX v18: neg_pct dan streak hanya aktif jika avg_neg TIDAK aktif
    if not funding_avg_neg_active:
        if fstats["neg_pct"] >= 70 and fstats["sample_count"] >= 3:
            score += CONFIG["score_funding_neg_pct"]
            signals.append(
                f"Funding negatif {fstats['neg_pct']:.0f}% dari {fstats['sample_count']} periode"
            )
        if fstats["streak"] >= CONFIG["funding_streak_min"]:
            score += CONFIG["score_funding_streak"]
            signals.append(
                f"Funding streak negatif {fstats['streak']}x berturut "
                f"({fstats['sample_count']} total data)"
            )
    else:
        # Jika avg_neg sudah aktif, tetap tampilkan info tapi tanpa skor tambahan
        if fstats["neg_pct"] >= 70 and fstats["sample_count"] >= 3:
            signals.append(
                f"Funding negatif {fstats['neg_pct']:.0f}% "
                f"(sudah dihitung dalam avg_neg — tidak ditambah lagi)"
            )
        if fstats["streak"] >= CONFIG["funding_streak_min"]:
            signals.append(
                f"Funding streak {fstats['streak']}x "
                f"(sudah dihitung dalam avg_neg — tidak ditambah lagi)"
            )

    # ── 14. BTC Outperformance ────────────────────────────────────────────────
    if btc_corr.get("outperform_label") == "OUTPERFORM":
        score += CONFIG["score_outperform"]
        signals.append(
            f"🚀 OUTPERFORM BTC — coin {btc_corr['coin_period_chg']:+.1f}% vs BTC "
            f"{btc_corr['btc_period_chg']:+.1f}% ({btc_corr['delta_vs_btc']:+.1f}%)"
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  SCORING v20 — PART 4: WEIGHTED BONUS + NEW INDICATORS
    # ══════════════════════════════════════════════════════════════════════════

    # Compute v20 weighted bonus
    v20_bonus = calc_weighted_score_v20(
        score, ema20_slope, vol_zscore_v20, micro_breakout,
        reversal_valid, candle_imbal, ema_trend, ema200_dist, higher_low_v20
    )

    # Apply weighted bonus additively to heuristic score
    score += v20_bonus["total_bonus"]

    # EMA20 Slope bonus
    if ema20_slope.get("is_rising"):
        signals.append(ema20_slope["label"])

    # Volume Z-Score v20 bonus (stricter z > 1.5 threshold)
    if vol_zscore_v20.get("is_spike"):
        signals.append(vol_zscore_v20["label"])

    # EMA200 Distance bonus
    if ema200_dist.get("is_near"):
        signals.append(ema200_dist["label"])

    # Higher Low v20
    if higher_low_v20.get("is_higher_low"):
        signals.append(higher_low_v20["label"])

    # Bid/Ask Ratio proxy (from candle volume)
    bar = vol_zscore_v20.get("bid_ask_ratio", 1.0)
    if bar > CONFIG["bid_ask_ratio_min"]:
        signals.append(
            f"📊 Bid/Ask Ratio {bar:.2f} > {CONFIG['bid_ask_ratio_min']} "
            f"{'(STRONG)' if bar > CONFIG['bid_ask_ratio_strong'] else ''} — buy pressure dominan"
        )

    # PART 2: Fake Reversal penalty
    if reversal_valid.get("penalty", 0) < 0:
        score += reversal_valid["penalty"]
        signals.append(reversal_valid["label"])
    elif reversal_valid.get("label"):
        signals.append(reversal_valid["label"])

    # ══════════════════════════════════════════════════════════════════════════
    #  SCORING v22 — INSTITUTIONAL DETECTOR BONUSES
    # ══════════════════════════════════════════════════════════════════════════

    # FIX 02: Smart Money Accumulation
    if smart_money_v22.get("is_accumulating"):
        score += smart_money_v22["score"]
        signals.append(smart_money_v22["label"])

    # FIX 03: Liquidity Trap (stop-sweep reversal)
    if liq_trap_v22.get("is_trap"):
        score += liq_trap_v22["score"]
        signals.append(liq_trap_v22["label"])

    # FIX 04: Whale Footprint
    if whale_fp_v22.get("is_footprint"):
        score += whale_fp_v22["score"]
        signals.append(whale_fp_v22["label"])

    # FIX 05: Pre-Breakout Pressure
    if prebreakout_v22.get("is_compressed"):
        score += prebreakout_v22["score"]
        signals.append(prebreakout_v22["label"])

    # FIX 06: Momentum Ignition
    if mom_ignition_v22.get("is_ignition"):
        score += mom_ignition_v22["score"]
        signals.append(mom_ignition_v22["label"])

    # FIX 08: Improved Reversal — add signal but no extra score (it's informational)
    if rev_label_v22:
        signals.append(rev_label_v22)

    # ── v25: Early Trend Reversal Bonus ──────────────────────────────────────
    # Detect EMA20 rising toward EMA50 with volume confirmation
    _rev_bonus_v25 = 0
    _ema20_v   = ema_trend.get("ema20", 0)
    _ema50_v   = ema_trend.get("ema50", 0)
    _slope_pos = ema20_slope.get("is_rising", False)
    _z_ok      = vol_zscore_v20.get("z", 0) > 1.5
    _mom_ok    = price_chg > 0   # 1h price change positive
    # "approaching": EMA20 within 3% of EMA50 from below
    _approaching = (
        _ema20_v > 0 and _ema50_v > 0
        and _ema20_v < _ema50_v
        and (_ema50_v - _ema20_v) / _ema50_v < 0.03
    )
    if _slope_pos and _z_ok and _mom_ok and (_approaching or ema_trend.get("trend") == "NEUTRAL"):
        _rev_bonus_v25 = CONFIG.get("score_reversal_setup_v25", 8)
        score += _rev_bonus_v25
        signals.append(
            f"🔄 Early Reversal Setup v25: EMA20 slope↑, z={vol_zscore_v20['z']:.2f}, "
            f"mom↑ — bonus +{_rev_bonus_v25}"
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  SCORING v23 — MARKET MAKER DETECTION BONUSES
    # ══════════════════════════════════════════════════════════════════════════

    # Liquidity Sweep v23
    if liq_sweep_v23.get("is_sweep"):
        score += liq_sweep_v23["score"]
        signals.append(liq_sweep_v23["label"])

    # Whale Absorption v23
    if whale_abs_v23.get("is_absorption"):
        score += whale_abs_v23["score"]
        signals.append(whale_abs_v23["label"])

    # Volatility Compression v23
    if vol_comp_v23.get("is_compressed"):
        score += vol_comp_v23["score"]
        signals.append(vol_comp_v23["label"])

    # Momentum Ignition v23
    if mom_ign_v23.get("is_ignition"):
        score += mom_ign_v23["score"]
        signals.append(mom_ign_v23["label"])

    # Orderbook Pressure v23
    ob_sc = ob_press_v23.get("score", 0)
    if ob_sc != 0:
        score += int(ob_sc)
        if ob_press_v23.get("label"):
            signals.append(ob_press_v23["label"])

    # Spoofing penalty v23
    spoof_pen = spoofing_v23.get("penalty", 0)
    if spoof_pen < 0:
        score += spoof_pen
        signals.append(spoofing_v23["label"])

    # ══════════════════════════════════════════════════════════════════════════
    #  SCORING v27 — PRE-PUMP INTELLIGENCE BONUSES
    #  All bonuses are additive to heuristic score before blending.
    #  Modules fire independently — failing one doesn't block others.
    # ══════════════════════════════════════════════════════════════════════════

    # Module 1 — Multi-TF Momentum Acceleration
    if _v27_mom_accel.get("is_accelerating"):
        score += _v27_mom_accel["score"]
        signals.append(_v27_mom_accel["label"])

    # Module 2 — Extreme Volume Expansion (>4×)
    if _v27_vol_exp.get("is_extreme"):
        score += _v27_vol_exp["score"]
        signals.append(_v27_vol_exp["label"])

    # Module 3 — OI Expansion (>5%)
    if _v27_oi_exp.get("is_expanding"):
        score += _v27_oi_exp["score"]
        signals.append(_v27_oi_exp["label"])

    # Module 4 — ATR Breakout
    if _v27_atr_break.get("is_breakout"):
        score += _v27_atr_break["score"]
        signals.append(_v27_atr_break["label"])

    # Module 5 — Real L2 Orderbook Imbalance
    if _v27_ob_imbal.get("is_bullish"):
        score += _v27_ob_imbal["score"]
        signals.append(_v27_ob_imbal["label"])

    # Module 6 — Whale Accumulation (large buy candles)
    if _v27_whale_accum.get("is_whale_accum"):
        score += _v27_whale_accum["score"]
        signals.append(_v27_whale_accum["label"])

    # Module 7 — Upward Liquidity Sweep
    if _v27_liq_sweep_up.get("is_sweep_up"):
        score += _v27_liq_sweep_up["score"]
        signals.append(_v27_liq_sweep_up["label"])

    # pump_probability_score summary signal (always append if any module active)
    if _v27_pump_score_data.get("n_active", 0) > 0:
        signals.append(_v27_pump_score_data["label"])

    # ══════════════════════════════════════════════════════════════════════════
    #  ALERT LEVEL v18 — feature-based probability + timing ETA
    # ══════════════════════════════════════════════════════════════════════════

    # STEP 4 v19: AI Weighted Score (hybrid dengan heuristic score)
    ai_score_data = calc_ai_weighted_score(
        vol_spike, micro_mom, mom_accel, buy_press, liq_sweep,
        micro_breakout, rsi, bb_squeeze, vol_zscore, energy,
        candle_imbal, whale_accum
    )

    # FIX 09 v22: Institutional Score (0-100 normalized)
    inst_score_data = calc_institutional_score_v22(
        smart_money_v22, liq_trap_v22, whale_fp_v22,
        prebreakout_v22, mom_ignition_v22,
        vol_zscore_v20, candle_imbal, micro_breakout, accum, energy
    )

    # v23 Market Maker Score (0-100)
    mm_score_data = calc_mm_score_v23(
        liq_sweep_v23, whale_abs_v23, vol_comp_v23,
        mom_ign_v23, ob_press_v23, spoofing_v23,
        smart_money_v22, accum, energy
    )

    # v23: Blended score — 45% heuristic + 30% AI + 25% MM institutional
    _hw = CONFIG.get("v23_heuristic_weight", 0.45)
    _aw = CONFIG.get("v23_ai_weight",        0.30)
    _mw = CONFIG.get("v23_inst_weight",      0.25)
    blended_score = round(
        score                          * _hw
        + ai_score_data["weighted_score"]  * _aw
        + mm_score_data["mm_score"]        * _mw
    )

    # Pump probability: blend sigmoid from MM score (70%) + legacy logistic (30%)
    pump_prob_v23  = calc_pump_probability_v23(mm_score_data["mm_score"])
    pump_prob_leg  = calc_pump_probability_v19(blended_score)
    pump_prob_base = round(pump_prob_v23 * 0.70 + pump_prob_leg * 0.30, 1)
    # v27: incorporate pump_probability_score — if v27 signals are strong,
    # they boost the final pump_prob by up to 15 percentage points (capped at 95)
    _v27_ps        = _v27_pump_score_data["pump_probability_score"]
    _v27_boost     = round(_v27_ps * 0.15, 1)   # max +13.5pp at score=90
    pump_prob      = min(round(pump_prob_base + _v27_boost, 1), 95.0)
    # BUG FIX v25: pump_prob_v22 was referenced in result dict but never assigned
    # Alias to pump_prob_v23 for backward compatibility with build_alert
    pump_prob_v22  = pump_prob_v23

    # v18 timing ETA (retained)
    pump_timing = calc_pump_timing_eta(
        vol_accel, buy_press["buy_ratio"], oi_data, micro_mom
    )

    # Alert level dari threshold v19 (55/65/78)
    alert_level_v19 = get_alert_level_v19(blended_score)

    # Expose blended_score untuk ranking

    score = blended_score   # reuse score variable for downstream

    # Pump type detection (Phase-based)
    if vol_spike["tier"] >= 2 and buy_press["tier"] >= 2 and mom_accel["is_strong"]:
        pump_type = "Phase 1+2+3 Full — PUMP IMMINENT"
    elif vol_spike["tier"] >= 2 and buy_press["tier"] >= 2:
        pump_type = "Phase 1+2 — Volume + Buy Pressure"
    elif vol_spike["tier"] >= 3:
        pump_type = "Phase 1 STRONG — Volume Spike 3x+"
    elif whale_order["is_whale"] and buy_press["tier"] >= 2:
        pump_type = "Whale Entry + Buy Pressure"
    elif mom_accel["is_strong"] and vol_spike["tier"] >= 1:
        pump_type = "Momentum Acceleration + Vol Spike"
    elif energy["is_buildup"] and energy["is_strong"]:
        pump_type = "Energy Build-Up + Short Squeeze"
    elif liq_sweep["is_sweep"] and htf_accum["is_htf_accum"]:
        pump_type = "Liquidity Sweep + HTF Accumulation"
    elif accum["is_accumulating"] and accum["is_vol_compress"] and atr_contr["is_contracting"]:
        pump_type = "Smart Money Accumulation + ATR Compression"
    elif bb_squeeze and energy["is_buildup"]:
        pump_type = "BB Squeeze + Energy Build-Up"
    elif energy["is_buildup"]:
        pump_type = "Energy Build-Up (OI+Vol+Price Stuck)"
    elif vol_spike["tier"] >= 1:
        pump_type = "Volume Spike — Early Interest"
    elif accum["is_accumulating"]:
        pump_type = "Smart Money Accumulation"
    elif htf_accum["is_htf_accum"]:
        pump_type = "HTF Accumulation Build-Up"
    elif liq_sweep["is_sweep"]:
        pump_type = "Liquidity Sweep Reversal"
    elif bb_squeeze and atr_contr["is_contracting"]:
        pump_type = "BB Squeeze + ATR Compression"
    elif (not oi_data["is_new"] and oi_data["change_pct"] >= CONFIG["oi_strong_pct"]
          and accum["is_vol_compress"]):
        pump_type = "OI Expansion Kuat + Vol Compression"
    else:
        pump_type = "Accumulation Setup"

    # Map alert_level_v19 ke format lama (HIGH/MEDIUM) untuk kompatibilitas
    if alert_level_v19 == "STRONG ALERT":
        alert_level = "HIGH"
    elif alert_level_v19 in ("ALERT", "WATCHLIST"):
        alert_level = "MEDIUM"
    else:
        alert_level = "LOW"

    # ── Entry & Target v19 — Adaptive Entry + Liquidity SL + Dynamic TP ─────
    market_regime = calc_market_regime(
        c1h, vwap, buy_press["buy_ratio"], vol_ratio, rsi, price_pos_48
    )
    entry_data = calc_entry_v19(
        c1h, vwap, price_now, atr_abs_val, market_regime, sr,
        rsi, buy_press["buy_ratio"], vol_ratio, price_pos_48,
        alert_level_v19, bos_level, liq_sweep
    )

    # v18: gunakan threshold WATCHLIST (40) sebagai minimum
    # FIX v24: use lower threshold to produce 20-40 candidates
    min_score = CONFIG.get("score_watchlist_v24", CONFIG.get("score_watchlist_v23", CONFIG["score_watchlist"]))

    # FIX v25: hard-reject dump trap ONLY if score fell to or below zero
    # (net-negative signal = no bullish case remains after all bonuses)
    if dump_trap_reject and score <= 0:
        log.info(
            f"  {symbol}: Dump trap + score {score} ≤ 0 — GATE GAGAL v25 (net negative)"
        )
        return None
    if score >= min_score:
        return {
            "symbol":          symbol,
            "score":           score,
            "signals":         signals,
            "entry":           entry_data,
            "price":           price_now,
            "chg_24h":         chg_24h,
            "vol_24h":         vol_24h,
            "rsi":             round(rsi, 1),
            "bbw":             round(bbw * 100, 2),
            "bb_pct":          round(bb_pct, 2),
            "bb_squeeze":      bb_squeeze,
            "atr_pct":         round(atr_pct, 2),
            "atr_contracting": atr_contr["is_contracting"],
            "atr_ratio":       atr_contr["ratio"],
            "above_vwap_rate": round(above_vwap_rate * 100, 1),
            "vwap":            round(vwap, 8),
            "bos_up":          bos_up,
            "bos_level":       round(bos_level, 8),
            "higher_low":      higher_low,
            "funding_stats":   fstats,
            "pump_type":       pump_type,
            "alert_level":     alert_level,
            "alert_level_v19": alert_level_v19,   # NEW v18
            "pump_prob":       pump_prob,          # NEW v18: logistic probability
            "vol_ratio":       round(vol_ratio, 2),
            "vol_accel":       round(vol_accel * 100, 1),
            "vol_consistent":  vol_consistent,
            "candle_dir_ratio": round(candle_dir_ratio * 100, 1),
            "price_pos_48":    price_pos_48,
            "uptrend_age":     uptrend["age_hours"],
            "sr":              sr,
            "btc_corr":        btc_corr,
            "accum":           accum,
            "htf_accum":       htf_accum,
            "liq_sweep":       liq_sweep,
            "energy":          energy,
            "oi_data":         oi_data,
            # v18 phase signals + timing
            "vol_spike":       vol_spike,
            "buy_press":       buy_press,
            "micro_mom":       micro_mom,      # v18: 5m micro momentum
            "mom_accel":       mom_accel,
            "whale_order":     whale_order,
            "fake_pump":       fake_pump,
            "market_regime":   market_regime,
            "pump_timing":     pump_timing,    # v18: ETA model
            # v19 new fields
            "ema_trend":       ema_trend,
            "vol_zscore":      vol_zscore,
            "micro_breakout":  micro_breakout,
            "candle_imbal":    candle_imbal,
            "whale_accum":     whale_accum,
            "wick_filter":     wick_filter,
            "ai_score":        ai_score_data,
            "early_pump":      early_pump_detected,
            "rank_value":      round(blended_score * pump_prob / 100, 2),
            # v20 new fields
            "ema20_slope":     ema20_slope,
            "vol_zscore_v20":  vol_zscore_v20,
            "ema200_dist":     ema200_dist,
            "higher_low_v20":  higher_low_v20,
            "reversal_valid":  reversal_valid,
            "v20_bonus":       v20_bonus,
            # v20 ranking keys (used for multi-key sort in PART 7)
            "rank_vol_z_v20":  vol_zscore_v20["z"],
            "rank_bid_ask":    vol_zscore_v20["bid_ask_ratio"],
            # v22 institutional fields
            "smart_money_v22":  smart_money_v22,
            "liq_trap_v22":     liq_trap_v22,
            "whale_fp_v22":     whale_fp_v22,
            "prebreakout_v22":  prebreakout_v22,
            "mom_ignition_v22": mom_ignition_v22,
            "inst_score":       inst_score_data,
            "pump_prob_v22":    pump_prob_v22,
            "rev_conf_v22":     rev_conf_v22,
            # FIX 11 v22 ranking keys
            "rank_inst_score":  inst_score_data["inst_score"],
            "rank_accum_score": inst_score_data["accum_score"],
            # v23 market maker detection fields
            "liq_sweep_v23":    liq_sweep_v23,
            "whale_abs_v23":    whale_abs_v23,
            "vol_comp_v23":     vol_comp_v23,
            "mom_ign_v23":      mom_ign_v23,
            "ob_press_v23":     ob_press_v23,
            "spoofing_v23":     spoofing_v23,
            "mm_score":         mm_score_data,
            "pump_prob_v23":    pump_prob_v23,
            # v23 ranking keys (4-key sort)
            "rank_mm_score":    mm_score_data["mm_score"],
            "rank_ob_score":    ob_press_v23.get("imbalance", 1.0),
            # v27 pre-pump intelligence fields
            "v27_mom_accel":    _v27_mom_accel,
            "v27_vol_exp":      _v27_vol_exp,
            "v27_oi_exp":       _v27_oi_exp,
            "v27_atr_break":    _v27_atr_break,
            "v27_ob_imbal":     _v27_ob_imbal,
            "v27_whale_accum":  _v27_whale_accum,
            "v27_liq_sweep_up": _v27_liq_sweep_up,
            "pump_probability_score": _v27_pump_score_data["pump_probability_score"],
            "v27_active_modules": _v27_pump_score_data["active_modules"],
            "rank_pump_score_v27": _v27_pump_score_data["pump_probability_score"],
        }
    else:
        log.info(f"  {symbol}: Skor {score} < {min_score} (WATCHLIST threshold) — dilewati")
        return None

# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════════════════
def _fmt_price(p):
    """Format harga otomatis sesuai magnitudo."""
    if p == 0:
        return "0"
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.5f}"
    return f"{p:.8f}"

def build_alert(r, rank=None):
    """Pesan Telegram v18 — format diperkaya dengan Phase signals & logistic prob."""
    level_v18 = r.get("alert_level_v19", "ALERT")
    if level_v18 == "STRONG ALERT":
        level_icon = "🔥"
    elif level_v18 == "ALERT":
        level_icon = "📡"
    else:
        level_icon = "👁"

    e   = r["entry"]
    bc  = r.get("btc_corr", {})
    sr  = r.get("sr", {})
    oi  = r.get("oi_data", {})
    vs  = r.get("vol_spike", {})
    bp  = r.get("buy_press", {})
    ma  = r.get("mom_accel", {})
    wo  = r.get("whale_order", {})
    fp  = r.get("fake_pump", {})

    p     = r["price"]
    entry = e["entry"]
    sl    = e["sl"]
    t1    = e["t1"]
    t2    = e["t2"]
    t3    = e.get("t3", t2)

    pump_prob    = r.get("pump_prob", 0)
    market_regime = e.get("market_regime", r.get("market_regime", "NEUTRAL"))

    # Header
    pt    = r.get("pump_timing", {})
    eta   = pt.get("eta", "?")
    eta_e = pt.get("eta_emoji", "")
    urg   = pt.get("urgency", "")

    msg  = f"{level_icon} <b>{r['symbol']} — {level_v18}</b>  #{rank}\n"
    msg += f"<b>Score :</b> {r['score']}  |  <b>Pump Prob:</b> {pump_prob}%\n"
    msg += f"<b>ETA   :</b> {eta_e} {eta} ({urg})\n"
    msg += f"<b>Type  :</b> {r['pump_type']}\n"
    msg += f"<b>Regime:</b> {market_regime}  |  <b>Scan:</b> {utc_now()}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n"

    # Harga & kondisi pasar
    msg += f"<b>Harga :</b> <code>{_fmt_price(p)}</code>  ({r['chg_24h']:+.1f}% 24h)\n"
    msg += f"<b>VWAP  :</b> <code>{_fmt_price(r['vwap'])}</code>"
    gap_vwap = (p - r['vwap']) / r['vwap'] * 100 if r['vwap'] > 0 else 0
    msg += f"  ({gap_vwap:+.1f}% vs harga)\n"
    msg += (
        f"<b>Posisi:</b> {r['price_pos_48']:.0%} range  |  "
        f"RSI: {r['rsi']}  |  "
        f"{r['candle_dir_ratio']:.0f}% candle hijau\n"
    )

    # Phase Signals v18
    mm   = r.get("micro_mom", {})

    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>📊 Phase Signals v18:</b>\n"
    vol_str   = f"{vs.get('ratio', 0):.1f}x"   if vs else "—"
    buy_str   = f"{bp.get('buy_pct', 0):.0f}%" if bp else "—"
    # Micro momentum (5m)
    if mm and mm.get("is_accelerating"):
        m5   = mm.get("mom_5m", 0) * 100
        accel_val = mm.get("accel", 0) * 100
        micro_str = f"⚡{accel_val:+.3f}% (5m:{m5:+.3f}%)"
    else:
        micro_str = "—"
    # 1h momentum fallback
    accel_1h = (f"+{ma.get('acceleration',0)*100:.2f}%"
                if ma and ma.get("is_accelerating") else "—")
    # Whale confidence
    wh_conf   = wo.get("confidence","") if wo else ""
    whale_str = (f"✅ {wo.get('mult',0):.1f}x [{wh_conf}]"
                 if wo and wo.get("is_whale") else "—")
    # Fake severity
    fake_sev = fp.get("severity","") if fp and fp.get("is_fake") else ""
    fake_str = (f"⚠️ FAKE [{fake_sev}] ({fp.get('n_cond',0)}/4)"
                if fp and fp.get("is_fake") else "✅ Clean")
    # Timing
    timing_score = pt.get("timing_score", 0) if pt else 0
    msg += (
        f"  Vol Spike   : {vol_str}\n"
        f"  Buy Pressure: {buy_str}\n"
        f"  Micro Mom5m : {micro_str}\n"
        f"  Mom 1h      : {accel_1h}\n"
        f"  Whale       : {whale_str}\n"
        f"  Fake Filter : {fake_str}\n"
        f"  Timing Score: {timing_score:.2f}\n"
    )

    # Entry / SL / TP v18
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📍 <b>Entry :</b> <code>{_fmt_price(entry)}</code>  [{e.get('sl_method','')}]\n"
    msg += f"🛑 <b>SL    :</b> <code>{_fmt_price(sl)}</code>  (-{e['sl_pct']:.2f}%)\n"
    rr3_str = e.get("rr3_str", e.get("rr2_str", "?"))
    msg += f"🎯 <b>TP1   :</b> <code>{_fmt_price(t1)}</code>  (+{e['gain_t1_pct']:.1f}%)  RR:{e['rr_str']}x\n"
    msg += f"🎯 <b>TP2   :</b> <code>{_fmt_price(t2)}</code>  (+{e['gain_t2_pct']:.1f}%)  RR:{e.get('rr2_str','?')}x\n"
    msg += f"🎯 <b>TP3   :</b> <code>{_fmt_price(t3)}</code>  (+{e.get('gain_t3_pct', 0):.1f}%)  RR:{rr3_str}x\n"
    msg += f"⚖️ <b>ATR   :</b> {e.get('atr_pct', r.get('atr_pct',0)):.2f}%  |  SL: -{e['sl_pct']:.2f}%\n"
    msg += f"📌 <i>{e.get('trail_note','')}</i>\n"

    # BTC correlation
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    if bc.get("correlation") is not None:
        msg += (
            f"{bc.get('btc_regime_emoji','❓')} <b>BTC:</b> {bc.get('btc_regime','?')}"
            f"  ({bc.get('btc_period_chg',0):+.1f}%/{bc.get('lookback',24)}h)\n"
        )
        msg += (
            f"{bc.get('outperform_emoji','〰️')} <b>vs BTC:</b> {bc.get('outperform_label','?')} "
            f"| Coin {bc.get('coin_period_chg',0):+.1f}% vs BTC {bc.get('btc_period_chg',0):+.1f}%\n"
        )
    else:
        msg += "📊 <b>vs BTC:</b> data tidak tersedia\n"

    # Support & Resistance
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    res_list = sr.get("resistance", []) if sr else []
    sup_list = sr.get("support",    []) if sr else []
    if res_list:
        for rv in res_list[:2]:
            msg += f"🔴 R <code>{_fmt_price(rv['level'])}</code>  ({rv['gap_pct']:+.1f}%)\n"
    msg += f"▶ NOW <code>{_fmt_price(p)}</code>\n"
    if sup_list:
        for sv in sup_list[:2]:
            msg += f"🟢 S <code>{_fmt_price(sv['level'])}</code>  ({sv['gap_pct']:+.1f}%)\n"

    # OI
    if oi.get("oi_now", 0) > 0:
        ov     = oi["oi_now"]
        os_str = f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K"
        cs     = f"({oi['change_pct']:+.1f}%)" if not oi.get("is_new") else "(baseline)"
        msg += f"📈 <b>OI:</b> {os_str} {cs}\n"

    # Sinyal teknikal prioritas
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>Sinyal:</b>\n"
    priority_signals = []
    keywords = [
        "Phase", "Volume Spike", "Buy Pressure", "Acceleration",
        "Whale", "FAKE", "AKUMULASI", "BUILD-UP", "Squeeze", "Sweep",
        "BOS", "Funding", "HTF", "OI", "ATR", "BB"
    ]
    for s in r["signals"]:
        if any(kw in s for kw in keywords):
            priority_signals.append(s)
        if len(priority_signals) >= 7:
            break
    for s in priority_signals:
        s_short = s[:88] + "…" if len(s) > 88 else s
        msg += f"• {s_short}\n"

    # AI Score breakdown
    ai  = r.get("ai_score", {})
    et  = r.get("ema_trend", {})
    ep  = r.get("early_pump", False)
    wca = r.get("whale_accum", {})
    mbo = r.get("micro_breakout", {})

    if ai:
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += "<b>🤖 AI Score v19:</b>\n"
        msg += (
            f"  Weighted Score : {ai.get('weighted_score', 0):.1f}/100\n"
            f"  Volume         : {ai.get('volume_score', 0):.0f}  "
            f"Accel: {ai.get('acceleration_score', 0):.0f}  "
            f"Mom: {ai.get('momentum_score', 0):.0f}\n"
            f"  Liquidity      : {ai.get('liquidity_score', 0):.0f}  "
            f"Breakout: {ai.get('breakout_score', 0):.0f}  "
            f"RSI: {ai.get('rsi_score', 0):.0f}\n"
        )
    if et:
        trend_str = et.get("trend", "?")
        ema20_str = f"{et.get('ema20', 0):.4g}" if et.get("ema20") else "?"
        ema50_str = f"{et.get('ema50', 0):.4g}" if et.get("ema50") else "?"
        msg += f"📊 EMA Trend: {trend_str}  EMA20:{ema20_str}  EMA50:{ema50_str}\n"
    if ep:
        msg += "⚡ <b>EARLY PUMP SIGNAL AKTIF</b>\n"
    if wca and wca.get("is_accum"):
        msg += f"🐳 Whale Accum: {wca.get('label', '')}\n"
    if mbo and mbo.get("is_breakout"):
        msg += f"🚀 Micro Breakout: {mbo.get('gap_pct', 0):+.2f}% di atas high {mbo.get('period', 20)}c\n"

    # v20 signal summary
    v20b  = r.get("v20_bonus", {})
    rv20  = r.get("reversal_valid", {})
    es20  = r.get("ema20_slope", {})
    ed200 = r.get("ema200_dist", {})
    hlv20 = r.get("higher_low_v20", {})
    zvv20 = r.get("vol_zscore_v20", {})
    if v20b and v20b.get("total_bonus", 0) > 0:
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"<b>🆕 v20 Signals:</b>\n"
        bar = v20b.get("bid_ask_ratio", 1.0)
        zv  = v20b.get("vol_z_v20", 0)
        msg += (
            f"  Bonus Total   : +{v20b['total_bonus']}\n"
            f"  Momentum      : +{v20b['momentum_bonus']}  "
            f"Volume: +{v20b['volume_bonus']}  "
            f"Breakout: +{v20b['breakout_bonus']}\n"
            f"  Reversal      : +{v20b['reversal_bonus']}  "
            f"Orderbook: +{v20b['orderbook_bonus']}  "
            f"Trend: +{v20b['trend_bonus']}\n"
            f"  Vol Z v20     : {zv:.2f}  |  Bid/Ask: {bar:.2f}\n"
        )
    if rv20 and rv20.get("label"):
        safe_rv = rv20["label"][:100]
        msg += f"  {safe_rv}\n"
    if es20 and es20.get("is_rising"):
        msg += f"  {es20.get('label', '')[:80]}\n"
    if ed200 and ed200.get("is_near"):
        msg += f"  {ed200.get('label', '')[:80]}\n"
    if hlv20 and hlv20.get("is_higher_low"):
        msg += f"  {hlv20.get('label', '')[:80]}\n"

    # v22 institutional signals
    sm22  = r.get("smart_money_v22", {})
    lt22  = r.get("liq_trap_v22", {})
    wf22  = r.get("whale_fp_v22", {})
    pb22  = r.get("prebreakout_v22", {})
    mi22  = r.get("mom_ignition_v22", {})
    is22  = r.get("inst_score", {})
    pv22  = r.get("pump_prob_v22", 0)
    rc22  = r.get("rev_conf_v22", "")

    if is22 and is22.get("inst_score", 0) > 0:
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += "<b>🏦 Institutional Score v22:</b>\n"
        msg += (
            f"  Score: {is22.get('inst_score',0):.1f}/100 | "
            f"Prob: {pv22:.1f}% | Rev: {rc22}\n"
            f"  Accum:{is22.get('accum_score',0):.0f} "
            f"BO:{is22.get('breakout_score',0):.0f} "
            f"Vol:{is22.get('volume_score',0):.0f} "
            f"OB:{is22.get('orderbook_score',0):.0f} "
            f"Mom:{is22.get('momentum_score',0):.0f} "
            f"Trap:{is22.get('liq_trap_score',0):.0f}\n"
        )
    for det, label_key in [(sm22,"label"),(lt22,"label"),(wf22,"label"),(pb22,"label"),(mi22,"label")]:
        lbl = det.get(label_key, "") if det else ""
        if lbl:
            msg += f"  {lbl[:90]}\n"

    # v23 Market Maker signals
    mm23   = r.get("mm_score", {})
    pv23   = r.get("pump_prob_v23", 0)
    ls23   = r.get("liq_sweep_v23", {})
    wa23   = r.get("whale_abs_v23", {})
    vc23   = r.get("vol_comp_v23", {})
    mi23   = r.get("mom_ign_v23", {})
    ob23   = r.get("ob_press_v23", {})
    sp23   = r.get("spoofing_v23", {})

    if mm23 and mm23.get("mm_score", 0) > 0:
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += "<b>🎯 Market Maker Score v23:</b>\n"
        msg += (
            f"  MM:{mm23.get('mm_score',0):.1f}/100 | Prob:{pv23:.1f}%\n"
            f"  Acc:{mm23.get('acc_score',0):.0f} "
            f"BO:{mm23.get('bo_score',0):.0f} "
            f"Vol:{mm23.get('vol_score',0):.0f} "
            f"OB:{mm23.get('ob_score',0):.0f} "
            f"Mom:{mm23.get('mom_score',0):.0f} "
            f"LS:{mm23.get('ls_score',0):.0f}"
        )
        if mm23.get("spoof_pen", 0) < 0:
            msg += f" Spoof:{mm23.get('spoof_pen',0)}"
        msg += "\n"
    for det_23 in [ls23, wa23, vc23, mi23, ob23, sp23]:
        lbl = det_23.get("label", "") if det_23 else ""
        if lbl:
            msg += f"  {lbl[:90]}\n"

    rank_val = r.get("rank_value", r.get("score", 0))
    z_rank   = r.get("rank_vol_z_v20", 0)
    mm_rank  = r.get("rank_mm_score", 0)
    ob_rank  = r.get("rank_ob_score", 1.0)
    msg += (f"\n<i>Scanner v27 | Rank:{rank_val:.1f} | "
            f"MM:{mm_rank:.0f} | Z:{z_rank:.2f} | OB:{ob_rank:.2f} | ⚠️ Bukan financial advice</i>")
    return msg

def build_summary(results):
    """
    v25 — Enhanced TOP 5 summary with full institutional signal breakdown.
    Shows: symbol, score, pump prob, vol z-score, EMA structure, OB imbalance.
    """
    top5 = results[:5]
    msg  = (
        f"📋 <b>TOP {len(top5)} PUMP CANDIDATES — Scanner v27</b>\n"
        f"{utc_now()}\n"
        f"{'━'*30}\n"
    )
    for i, r in enumerate(top5, 1):
        sym      = r["symbol"].replace("USDT", "")
        score    = r["score"]
        prob     = r.get("pump_prob", 0)
        lv       = r.get("alert_level_v19", "ALERT")
        level_ic = "🔥" if lv == "STRONG ALERT" else ("📡" if lv == "ALERT" else "👁")

        # Volume Z-score
        z_v20    = r.get("rank_vol_z_v20", r.get("vol_zscore_v20", {}).get("z", 0))
        vol_str  = (f"${r['vol_24h']/1e6:.1f}M" if r["vol_24h"] >= 1e6
                    else f"${r['vol_24h']/1e3:.0f}K")

        # EMA structure
        ema_t    = r.get("ema_trend", {}).get("trend", "?")
        ema_ic   = {"UPTREND": "📈", "DOWNTREND": "📉",
                    "REVERSAL_SETUP": "🔄", "BEARISH": "⬇️",
                    "NEUTRAL": "〰️"}.get(ema_t, "〰️")

        # Orderbook imbalance
        ob_imb   = r.get("rank_ob_score", r.get("ob_press_v23", {}).get("imbalance", 1.0))
        ob_str   = f"{ob_imb:.2f}"
        ob_ic    = "🟢" if ob_imb >= 1.2 else ("🔴" if ob_imb <= 0.8 else "🟡")

        # MM score
        mm_sc    = r.get("rank_mm_score", r.get("mm_score", {}).get("mm_score", 0))

        # TP targets
        t1_pct   = r.get("entry", {}).get("gain_t1_pct", 0)

        msg += (
            f"\n{i}. {level_ic} <b>{sym}</b>\n"
            f"   Score:<b>{score}</b> | Prob:<b>{prob}%</b> | MM:{mm_sc:.0f}\n"
            f"   Vol Z:{z_v20:.2f} ({vol_str}) | {ema_ic}EMA:{ema_t} | {ob_ic}OB:{ob_str}\n"
            f"   TP1:+{t1_pct}%\n"
        )

        # Highlight top signals (max 2)
        sigs = r.get("signals", [])
        for s in sigs[:2]:
            if s:
                msg += f"   • {str(s)[:70]}\n"

    msg += f"\n{'━'*30}\n<i>⚠️ Bukan financial advice</i>"
    return msg

# ══════════════════════════════════════════════════════════════════════════════
#  🔍  BUILD CANDIDATE LIST
# ══════════════════════════════════════════════════════════════════════════════
def build_candidate_list(tickers):
    all_candidates = []
    not_found      = []
    filtered_stats = defaultdict(int)

    log.info("=" * 70)
    log.info("🔍 SCANNING MODE: WHITELIST (top OI & volume pairs)")
    log.info("=" * 70)

    for sym in WHITELIST_SYMBOLS:
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

        # FIX v18: dilonggarkan dari 5% → 8%
        if chg > CONFIG["gate_chg_24h_max"]:
            filtered_stats["change_too_high"] += 1
            continue

        if chg < CONFIG["gate_chg_24h_min"]:
            filtered_stats["dump_too_deep"] += 1
            continue

        if price <= 0:
            filtered_stats["invalid_price"] += 1
            continue

        all_candidates.append((sym, ticker))

    total      = len(WHITELIST_SYMBOLS)
    will_scan  = len(all_candidates)
    n_excluded = (filtered_stats.get("excluded_keyword", 0)
                  + filtered_stats.get("manual_exclude", 0))
    n_filtered = sum(v for k, v in filtered_stats.items()
                     if k not in ("excluded_keyword", "manual_exclude"))
    accounted  = will_scan + n_excluded + n_filtered + len(not_found)

    log.info(f"\n📊 SCAN SUMMARY Scanner v27:")
    log.info(f"   Whitelist total  : {total} coins")
    log.info(f"   ✅ Will scan     : {will_scan} ({will_scan/total*100:.1f}%)")
    log.info(f"   🚫 Excluded kw   : {n_excluded}")
    log.info(f"   ❌ Filtered      : {n_filtered}")
    log.info(f"   ⚠️  Not in Bitget : {len(not_found)}")
    log.info(f"   ✔️  Akuntabel     : {accounted}/{total}")
    log.info(f"\n📋 Filter breakdown:")
    for k, v in sorted(filtered_stats.items()):
        log.info(f"   {k:25s}: {v}")
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
    log.info(f"=== QUANTITATIVE PUMP DETECTION SCANNER v27 — {utc_now()} ===")

    load_funding_snapshots()
    log.info(f"Funding snapshots loaded: {len(_funding_snapshots)} coins di memori")

    # FIX v18: load OI snapshots dari disk
    load_oi_snapshots()

    tickers = get_all_tickers()
    if not tickers:
        send_telegram("⚠️ Scanner Error: Gagal ambil data Bitget")
        return
    log.info(f"Total ticker dari Bitget: {len(tickers)}")

    candidates = build_candidate_list(tickers)
    results    = []
    _t_scan_start = time.time()   # v25: scan speed tracking
    _n_err = 0                    # v25: error counter

    for i, (sym, t) in enumerate(candidates):
        try:
            vol = float(t.get("quoteVolume", 0))
        except Exception:
            vol = 0.0

        # v26 PERF: reduce log noise — debug every coin, info every 10 + all errors/hits
        if (i + 1) % 10 == 0 or i == 0:
            log.info(f"[{i+1}/{len(candidates)}] {sym} (vol ${vol/1e3:.0f}K)...")
        else:
            log.debug(f"[{i+1}/{len(candidates)}] {sym}")
        _t_sym = time.time()   # v25: per-symbol timer

        try:
            res = master_score(sym, t)
            if res:
                _elapsed_sym = time.time() - _t_sym
                log.info(
                    f"  ✅ Score={res['score']} | {res['alert_level']} | "
                    f"{res['pump_type']} | pos:{res['price_pos_48']:.0%} | "
                    f"T1:+{res['entry']['gain_t1_pct']}% | {_elapsed_sym:.2f}s"
                )
                results.append(res)
        except Exception as ex:
            # BUG FIX v25: log full exception type + message; scanner NEVER stops
            import traceback as _tb
            log.warning(
                f"  ❌ Error {sym}: {type(ex).__name__}: {ex} — "
                f"skipped, scan continues"
            )
            log.debug(f"  ❌ {sym} traceback: {_tb.format_exc().strip()}")
            _n_err += 1
            continue

        time.sleep(CONFIG["sleep_coins"])

    save_all_funding_snapshots()
    log.info("Funding snapshots disimpan ke disk.")

    # FIX v18: simpan OI snapshots ke disk
    save_oi_snapshots()
    log.info("OI snapshots disimpan ke disk.")

    # v23 — Advanced 4-key Ranking: prob → MM score → vol_zscore → OB imbalance
    if CONFIG.get("rank_v20_multi", True):
        results.sort(
            key=lambda x: (
                x.get("rank_pump_score_v27", 0),          # 1: v27 pump score (NEW)
                x.get("rank_value",      x["score"]),     # 2: score × prob
                x.get("rank_mm_score",   0),              # 3: MM score (v23)
                x.get("rank_vol_z_v20",  0),              # 4: vol z-score
                x.get("rank_ob_score",   1.0),            # 5: OB imbalance
            ),
            reverse=True,
        )
    elif CONFIG.get("rank_use_combined", True):
        results.sort(key=lambda x: x.get("rank_value", x["score"]), reverse=True)
    else:
        results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"\nLolos threshold: {len(results)} coin")

    # v25: Post-scan funnel report
    _t_total = time.time() - _t_scan_start
    _n_scanned = len(candidates)
    _n_pass    = len(results)
    _n_strong  = sum(1 for r in results if r.get("alert_level_v19") == "STRONG ALERT")
    _n_alert   = sum(1 for r in results if r.get("alert_level_v19") == "ALERT")
    _n_watch   = _n_pass - _n_strong - _n_alert
    log.info(
        f"\n📊 SCAN FUNNEL v25:\n"
        f"   {_n_scanned} scanned → "
        f"{_n_scanned - _n_pass - _n_err} filtered → "
        f"{_n_pass} watchlist → "
        f"{_n_alert} alert → "
        f"{_n_strong} strong\n"
        f"   ❌ Errors: {_n_err} | ⏱ Total: {_t_total:.1f}s "
        f"({_t_total/_n_scanned:.2f}s/coin)\n"
    )

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

    log.info(f"=== SELESAI Scanner v27 — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  QUANTITATIVE PUMP DETECTION SCANNER v27                   ║")
    log.info("║  PRE-PUMP INTELLIGENCE EDITION — 7 new modules             ║")
    log.info("║  Vol4x+20 | OI5%+20 | L2-OB+15 | Whale+15                ║")
    log.info("║  MomAccel+10 | ATRbreak+10 | SweepUP+10 | prob_score 0-90 ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)

    run_scan()
