"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  QUANTITATIVE PUMP DETECTION SCANNER v32 — ChartPrime SR Integration      ║
║  Berdasarkan forensik: PIXEL+228%, TRUMP+51%, ORCA+79%, VVV+165%          ║
║                                                                              ║
║  ROOT CAUSE v31: SR logic tidak menggunakan volume-weighted pivot          ║
║  (ChartPrime), sup_holds pattern tidak di-score, BOS scoring tidak         ║
║  proporsional, alert terlalu verbose.                                       ║
║                                                                              ║
║  FIX-v32-1  calc_chartprime_sr: fungsi SR baru berbasis delta volume       ║
║             (pivot + vol filter = kotak hijau/merah ChartPrime)            ║
║  FIX-v32-2  detect_sup_holds: deteksi pola green ◆ berulang di support     ║
║             (buyer defensif = pre-pump akumulasi terkuat)                  ║
║  FIX-v32-3  score_bos_up: 18 → 12 (cegah false positive dominan)         ║
║  FIX-v32-4  score_chartprime_break_res: +20 (sinyal paling prediktif)     ║
║  FIX-v32-5  score_sup_holds_streak: bertingkat 1–3x / 4–6x / 7x+         ║
║  FIX-v32-6  Telegram alert: tambah ChartPrime SR section, lebih ringkas   ║
║  FIX-v32-7  build_summary: tampilkan ChartPrime signal di TOP list         ║
║                                                                              ║
║  FORENSIK 4-FASE (dari 4 chart nyata):                                     ║
║  Fase 1: Akumulasi di green box, sup_holds berulang, vol negatif melemah   ║
║  Fase 2: Break Res muncul, vol positif ledak > vol_hi threshold            ║
║  Fase 3: Pump (entry 1-3 bar setelah Break Res, retest ke bekas res)       ║
║  Fase 4: Exit saat res_holds/Break Sup + vol negatif masif di puncak       ║
║                                                                              ║
║  TARGET: 342 scanned → ~200 filtered → ~40 watchlist → ~10 alert → ~2 top ║
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
_http_session.headers.update({"User-Agent": "CryptoScanner/31.0", "Accept-Encoding": "gzip"})
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
    "/tmp/scanner_v32.log", maxBytes=10 * 1024 * 1024, backupCount=3
)
_fh.setFormatter(_log_fmt)
_log_root.addHandler(_fh)

log = logging.getLogger(__name__)
log.info("Scanner v32 — log aktif: /tmp/scanner_v32.log")

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
    # FIX v27: dilonggarkan dari 8% → 25%.
    # FIX v30 FIX-2: gate_chg_24h_max KONTEKSTUAL — tidak flat 25%.
    # PIXEL +280%, TRUMP +60%, HUS +56% — semua diblokir oleh gate 25%.
    # Gate ini diterapkan di build_candidate_list SEBELUM BBW diketahui,
    # sehingga nilai ini adalah fallback untuk coin tanpa squeeze.
    # Untuk coin dengan squeeze aktif, gate di-override di build_candidate_list.
    # 50% = cukup untuk menangkap pump fase awal (10-40% dari dasar).
    "gate_chg_24h_max":          50.0,
    # Gate kontekstual squeeze: jika BBW percentile <= 25 → pakai nilai ini.
    # Nilai 200% = tidak ada batas praktis untuk coin dalam squeeze panjang.
    "gate_chg_24h_max_squeeze":  200.0,
    "gate_chg_24h_min":        -15.0,   # hanya skip dump besar

    # ── VWAP Gate Tolerance ───────────────────────────────────────────────────
    "vwap_gate_tolerance":      0.97,   # price > vwap * 0.97

    # ── Gate uptrend usia ─────────────────────────────────────────────────────
    # FIX v27: dinaikkan dari 10 jam → 24 jam.
    # Pump 30-70% sering didahului trend naik 12-18 jam (fase akumulasi terlihat).
    # 10 jam terlalu pendek — membuang coin yang justru mau pump besar.
    "gate_uptrend_max_hours":   24,

    # ── Gate RSI overbought ───────────────────────────────────────────────────
    # FIX v27: dinaikkan dari 72 → 85.
    # RSI 72 TERLALU KETAT untuk altcoin pump detector.
    # Pump besar sering baru mulai dari RSI 60-75 dan berhenti di RSI 85-92.
    # Dengan batas 72, scanner membuang coin yang baru masuk fase 2 dari pump 5 fase!
    "gate_rsi_max":             85.0,

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
    # FIX v28 BUG#8: 1800→3600. Pump 20-70% butuh 2-6 jam terbentuk.
    # Cooldown 30 menit menyebabkan sinyal update terlewat di tengah pump.
    "alert_cooldown_sec":     3600,
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
    "energy_buildup_window":   8,
    "energy_buildup_bullish_min": 5,
    "score_energy_buildup":    4,

    # FIX v30 FIX-6: Volume compression DALAM SQUEEZE = sinyal akumulasi.
    # Ketiga coin pump (PIXEL, HUS, TRUMP) punya vol_spike 0.3-0.8x baseline.
    # Scanner tidak memberi poin apapun untuk pola ini.
    # Kondisi: vol_spike ratio 0.2-0.85x DAN BBW percentile <= 30 (squeeze aktif).
    # Ini membedakan "volume rendah = tidak ada minat" vs
    # "volume rendah = market maker menekan harga sambil akumulasi".
    "score_vol_compression_squeeze": 10,   # vol 0.2-0.85x dalam squeeze → +10
    "vol_compression_max_ratio":    0.85,  # volume harus < 85% baseline
    "vol_compression_min_ratio":    0.10,  # tapi masih ada activity (> 10%)

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
    # FIX v29 BUG-A: Sweep dianggap valid HANYA dalam N candle setelah terjadi.
    # Sweep jam 19:30 tidak boleh masih aktif 17 jam kemudian.
    # Default 6 = 6 candle 1h = 6 jam window validitas.
    "liq_sweep_expiry_candles": 6,
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

    # ── RSI ideal pre-pump — FIX v28 BUG#5 ──────────────────────────────────
    # Pump 30-70% sering mulai dari RSI 55-72, bukan hanya 40-60.
    # Zone diperluas dan dibuat bertingkat agar tidak melewatkan entry optimal.
    "rsi_ideal_min":           40.0,
    "rsi_ideal_max":           60.0,   # zona optimal (score tertinggi)
    "rsi_valid_max":           72.0,   # zona valid (score lebih rendah)
    "score_rsi_ideal":         4,      # RSI 45-60 → +4 (was +2)
    "score_rsi_valid":         2,      # RSI 60-72 → +2 (baru)
    "score_rsi_oversold":      1,      # RSI 35-45 → +1 (reversal setup)

    # ── Fresh Breakout Start bonus — FIX v28 BUG#9 ───────────────────────────
    # Coin yang baru mulai naik setelah akumulasi (streak 1-3 candle) adalah
    # titik entry terbaik untuk pump 20-70%. Sebelumnya tidak ada bonus.
    "score_fresh_breakout_start": 3,   # streak 1-3 jam = fresh start → +3

    # ── Higher Low ────────────────────────────────────────────────────────────
    # FIX v18: lookback dinaikkan 6 → 16 candle (lebih bermakna)
    "higher_low_lookback":     16,
    "score_higher_low":        2,

    # ── BOS Up ───────────────────────────────────────────────────────────────
    # FIX v18: lookback dinaikkan 3 → 8 candle (BOS lebih bermakna)
    "bos_lookback":            8,
    # FIX v32: score_bos_up: 18 → 12. Score 18 terlalu dominan → false positive.
    # ChartPrime "Break Res" sekarang di-score terpisah via calc_chartprime_sr (+20).
    # BOS Up tetap sinyal kuat tapi tidak monopoli scoring.
    "score_bos_up":            12,

    # ── ChartPrime SR (v32 NEW) ───────────────────────────────────────────────
    # Implementasi logika: pivot high/low + delta volume filter (seperti indikator
    # ChartPrime SR Breaks and Retests). Ini adalah sinyal paling prediktif
    # berdasarkan forensik 4 pump nyata (PIXEL, TRUMP, ORCA, VVV).
    #
    # vol_len=2: window filter vol (vol_hi = highest(delta_vol/2.5, 2))
    # cp_lookback=30: pivot lookback (naik dari 20 — tangkap pivot lebih historis)
    # cp_atr_mult=1.0: lebar box (sama dengan ChartPrime box_width=1)
    "cp_lookback":              30,   # FIX-CP-4: 20→30 pivot lookback
    "cp_vol_len":                4,   # FIX-CP-2: delta volume filter window (2→4)
    "cp_atr_mult":             1.0,   # box width multiplier
    "cp_sup_holds_lookback":    30,   # FIX: candle terakhir cek sup_holds (20→30)
    "cp_brekout_window":         3,   # FIX-CP-6: brekout valid dalam N candle
    # ── Support Health System (v32b NEW) ──────────────────────────────────────
    "sup_health_block_score":  -20,   # kurangi score jika risk CRITICAL
    "sup_health_warn_score":   -10,   # kurangi score jika risk HIGH
    "sup_health_at_support_bonus": 8, # bonus jika harga dalam 3% dari support
    "sup_health_at_support_pct": 0.03,# threshold "dekat support" = 3%
    "sup_health_critical_block": True,# hard-reject saat CRITICAL + squeeze
    # Score untuk ChartPrime signals — berdasarkan forensik:
    # "Break Res" = trigger utama semua pump, harus score tertinggi
    "score_cp_break_res":       20,   # Break Res = resistance patah → pump
    "score_cp_sup_holds_1":      4,   # 1-3x sup_holds (buyer defensif ada)
    "score_cp_sup_holds_2":      8,   # 4-6x sup_holds (akumulasi kuat)
    "score_cp_sup_holds_3":     14,   # 7x+ sup_holds (akumulasi sangat kuat)
    "score_cp_res_holds":       -6,   # res_holds = resistance bertahan → penalti
    "score_cp_break_sup":      -15,   # Break Sup = support patah → penalti besar
    # Proximity bonus: harga dekat resistance (< 2%) = siap break
    "cp_proximity_pct":        0.02,  # 2% dari resistance = "dekat"
    "score_cp_proximity":        6,   # bonus proximity menuju resistance

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

    # ── Signal Threshold — SATU SUMBER KEBENARAN (FIX v28 BUG#6) ────────────
    # v18 mendefinisikan ini, lalu v19 mendefinisikan ulang. Python dict dengan
    # key duplikat → nilai terakhir menang → tidak konsisten antar lookup.
    # DIHAPUS dari sini: definisi final ada di blok v19 di bawah.
    # (baris ini sengaja dikosongkan untuk menjaga urutan konfigurasi)

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
    # FIX v28 BUG#2: -0.005 → -0.02 (-2%).
    # Coin akumulasi punya 5m flat/sedikit negatif — ini justru CIRI pre-pump!
    # -0.5% threshold membuang ribuan coin akumulasi valid setiap scan.
    # Hanya tolak jika terjadi dump aktif nyata (>-2%).
    "momentum_val_reject":    -0.02,

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
    # FIX v30 FIX-3: noise_min_vol_24h: $500K → $100K.
    # PIXEL dan HUS volume sangat rendah selama akumulasi panjang.
    # $100K cukup untuk futures yang dapat diperdagangkan (spread manageable).
    # Likuiditas rendah = slippage lebih tinggi, tapi pump 50-280% > biaya itu.
    "noise_min_vol_24h":      100_000,
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

    # STEP 18 — Threshold — DEFINISI FINAL (FIX v28 BUG#6)
    # Duplikat dari v18 dihapus. Nilai ini yang berlaku.
    "score_watchlist":          55,      # WATCHLIST threshold
    "score_alert":              65,      # ALERT threshold
    "score_strong_alert":       78,      # STRONG ALERT threshold

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

    # FIX v30 FIX-5: BB Squeeze scoring bertingkat.
    # Score lama +8 (atau +4 tanpa vol confirm) tidak cukup untuk LAS coin.
    # Squeeze persentil rendah adalah sinyal paling prediktif big pump.
    # PIXEL/HUS: BBW sangat rendah selama 2-4 hari sebelum pump.
    # Sistem baru bertingkat berdasarkan seberapa ekstrem squeeze-nya:
    #   p <= 10 → +20 (squeeze ekstrem, langka, sangat prediktif)
    #   p <= 20 → +12 (squeeze kuat)
    #   p <= 30 → +6  (squeeze moderate)
    # Parameter ini digunakan di detect_volatility_compression_v23 dan
    # detect_prebreakout_pressure_v22 yang keduanya membaca score_prebreakout.
    "bb_percentile_lookback":    50,
    "bb_percentile_threshold":   30,     # diperluas dari 20 → 30
    "score_prebreakout":         20,     # p<=10: skor penuh
    "score_prebreakout_mid":     12,     # p<=20: skor medium
    "score_prebreakout_mild":     6,     # p<=30: skor ringan

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
    # FIX v28 BUG#1: normalisasi heuristic ke 0-100 sebelum blend
    # FIX v31 FIX-3: ref_max 150 → 80.
    # BOS+squeeze+vol_compress = ~48 raw → 48/150 = 32% norm → blended ~18 (terlalu rendah).
    # Dengan ref_max=80: 48/80 = 60% → blended ~35 → di atas threshold 30.
    # Nilai 80 = score heuristic "kuat tapi belum extreme" yang seharusnya map ke 100%.
    "heuristic_ref_max":          80.0,

    # Bobot blend — ketiga komponen sekarang di skala 0-100 (setelah normalisasi)
    "v23_heuristic_weight":      0.50,   # heuristic: 50%
    "v23_ai_weight":             0.25,   # AI: 25%
    "v23_inst_weight":           0.25,   # MM institutional: 25%

    # Watchlist threshold — FIX v23: lowered 55 → 40 to pass more candidates
    # (raised too aggressively in v19; 344 coins 0 passed)
    "score_watchlist_v23":       40,     # effective threshold for v23

    # ══════════════════════════════════════════════════════════════════════════
    #  NEW v27: OI ACCELERATION
    # ══════════════════════════════════════════════════════════════════════════
    # OI acceleration = OI naik makin cepat (second derivative positif)
    # Muncul 1-3 jam sebelum pump 30%+. Institusi membangun posisi dgn urgensi.
    "oi_accel_window":           6,      # simpan 6 snapshot OI terakhir per coin
    "oi_accel_min_snapshots":    3,      # butuh minimal 3 data point untuk hitung acceleration
    "oi_accel_threshold":        0.5,    # rate OI harus >0.5%/jam untuk dianggap aktif
    "oi_accel_strong":           1.5,    # acceleration kuat = OI naik >1.5%/jam
    "score_oi_accel":            8,      # bonus score OI acceleration normal
    "score_oi_accel_strong":    18,      # bonus score OI acceleration sangat kuat
    "oi_accel_snapshot_file":   "./oi_accel.json",

    # ══════════════════════════════════════════════════════════════════════════
    #  NEW v27: ORDERBOOK LIQUIDITY VACUUM
    # ══════════════════════════════════════════════════════════════════════════
    # Vacuum = area kosong di orderbook (gap besar antar level ask).
    # Harga "terbang" melewati vacuum karena tidak ada hambatan sell order.
    # Gap >0.3% = vacuum valid; >0.8% = pump highway!
    "ob_vacuum_depth":           20,     # ambil 20 level bid/ask
    "ob_vacuum_gap_min_pct":     0.3,    # gap minimal 0.3% untuk dianggap vacuum
    "ob_ask_vacuum_gap_strong":  0.8,    # gap kuat = >0.8%
    "ob_vacuum_vol_confirm_min": 1.3,    # butuh vol z-score >1.3 untuk konfirmasi
    "score_ob_vacuum":          12,      # bonus vacuum moderat (0.3-0.8%)
    "score_ob_vacuum_strong":   20,      # bonus vacuum kuat (>0.8%) + vol confirmed

    # ══════════════════════════════════════════════════════════════════════════
    #  NEW v27: CVD DIVERGENCE
    # ══════════════════════════════════════════════════════════════════════════
    # CVD = Cumulative Volume Delta (akumulasi buy_vol - sell_vol)
    # Membedakan akumulasi nyata vs distribusi tersembunyi dengan akurat.
    # Bullish Div: harga turun tapi CVD naik = whale akumulasi diam-diam
    # Bearish Div: harga naik tapi CVD turun = distribusi di kenaikan (JEBAKAN!)
    "cvd_lookback":              12,     # 12 candle untuk hitung CVD
    "cvd_divergence_threshold":  0.4,   # threshold divergence signifikan
    "score_cvd_bullish_div":    15,     # akumulasi tersembunyi = signal kuat
    "score_cvd_bearish_div":   -10,     # distribusi = penalti
    "score_cvd_momentum":        8,     # CVD dan harga sama naik = konfirmasi

    # ══════════════════════════════════════════════════════════════════════════
    #  NEW v31: SQUEEZE MODE CONFIG
    # ══════════════════════════════════════════════════════════════════════════
    # Squeeze mode aktif jika BBW percentile (dari candle 1h) <= threshold ini.
    # Saat aktif: gate OI, uptrend, 5m momentum, ATR minimum di-bypass.
    # Scoring: vol rendah dianggap bullish, BOS pre-close diakui, fresh streak diperluas.
    "squeeze_mode_bbw_percentile": 30,   # BBW percentile <= 30 → squeeze mode aktif
    "squeeze_mode_lookback":       50,   # lookback candle untuk hitung percentile BBW
    # Pre-close BOS: high candle terbaru tembus bos_level meski close belum konfirmasi
    "bos_preclose_bonus":          10,   # bonus score untuk pre-close BOS signal
    # Low-vol accumulation: vol 0.1-0.6x baseline + squeeze = akumulasi tersembunyi
    "accum_low_vol_min_ratio":     0.08, # minimal ada aktivitas (> 8% baseline)
    "accum_low_vol_max_ratio":     0.65, # tapi di bawah 65% baseline
    "score_low_vol_accum":          8,   # bonus untuk low-vol accumulation dalam squeeze
    # Fresh breakout diperluas ke 5 candle (v30: hanya 3)
    "score_fresh_breakout_start":   5,   # v31: streak 1-5 jam setelah akumulasi panjang
    "fresh_breakout_max_streak":    5,   # v31: diperluas dari 3 → 5
}

MANUAL_EXCLUDE = set()

EXCLUDED_KEYWORDS = ["XAU", "PAXG", "BTC", "ETH", "USDC", "DAI", "BUSD", "UST"]

# ══════════════════════════════════════════════════════════════════════════════
#  📋  WHITELIST
# ══════════════════════════════════════════════════════════════════════════════
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
"GBPUSDUSDT",
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

# ══════════════════════════════════════════════════════════════════════════════
#  💾  OI ACCELERATION HISTORY — NEW v27
# ══════════════════════════════════════════════════════════════════════════════
# Menyimpan 6+ OI snapshots per coin untuk deteksi second-derivative (acceleration).
# _oi_snapshot (v18) hanya 2-point; _oi_accel_history menyimpan time-series penuh.
_oi_accel_history = {}

def load_oi_accel_history():
    """v27: Load histori OI multi-snapshot dari disk."""
    global _oi_accel_history
    try:
        p = CONFIG.get("oi_accel_snapshot_file", "./oi_accel.json")
        if os.path.exists(p):
            with open(p) as f:
                data = json.load(f)
            now = time.time()
            _oi_accel_history = {
                sym: [snap for snap in snaps if now - snap["ts"] < 21600]
                for sym, snaps in data.items()
                if any(now - s["ts"] < 21600 for s in snaps)
            }
            log.info(f"OI Accel history loaded: {len(_oi_accel_history)} coins")
        else:
            _oi_accel_history = {}
    except Exception:
        _oi_accel_history = {}

def save_oi_accel_history():
    """v27: Simpan histori OI ke disk setelah setiap scan."""
    try:
        p = CONFIG.get("oi_accel_snapshot_file", "./oi_accel.json")
        with open(p, "w") as f:
            json.dump(_oi_accel_history, f)
    except Exception:
        pass

def update_oi_accel_snapshot(symbol, oi_now):
    """v27: Tambah snapshot OI baru ke histori ring-buffer per coin."""
    global _oi_accel_history
    if oi_now <= 0:
        return
    if symbol not in _oi_accel_history:
        _oi_accel_history[symbol] = []
    _oi_accel_history[symbol].append({"ts": time.time(), "oi": oi_now})
    window = CONFIG.get("oi_accel_window", 6)
    if len(_oi_accel_history[symbol]) > window * 2:
        _oi_accel_history[symbol] = _oi_accel_history[symbol][-(window * 2):]

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


def calc_bbw_percentile(candles, lookback=None, period=20):
    """
    v31 — Hitung persentil BBW saat ini vs historis.
    Digunakan untuk mendeteksi squeeze mode secara akurat dari candle.
    Return: percentile (0-100), current_bbw, is_squeeze_active
    """
    if lookback is None:
        lookback = CONFIG.get("squeeze_mode_lookback", 50)
    if len(candles) < lookback + period:
        return 50.0, 0.0, False
    bbw_history = []
    for i in range(lookback, 0, -1):
        seg = candles[-(i + period):-i] if i > 0 else candles[-period:]
        if len(seg) < period:
            continue
        cls    = [c["close"] for c in seg]
        mean_c = sum(cls) / period
        if mean_c <= 0:
            continue
        var    = sum((x - mean_c) ** 2 for x in cls) / period
        std_c  = math.sqrt(var)
        bbw_history.append(4.0 * std_c / mean_c)
    if not bbw_history:
        return 50.0, 0.0, False
    cur_bbw   = bbw_history[-1]
    rank      = sum(1 for v in bbw_history if v <= cur_bbw)
    percentile = rank / len(bbw_history) * 100
    threshold  = CONFIG.get("squeeze_mode_bbw_percentile", 30)
    return round(percentile, 1), round(cur_bbw, 6), percentile <= threshold

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
    Break of Structure ke atas — v31 UPGRADE.

    v30: hanya cek close[-1] > max(high[-9:-1])
         Masalah: aktif 1 jam terlambat karena harus tunggu candle tutup.

    v31 FIX-v31-2: Tiga lapis deteksi:
    1. STANDARD BOS: close terbaru > max(high N candle sebelumnya) — konfirmasi penuh
    2. PRE-CLOSE BOS: high candle terbaru > bos_level meski close belum melewati
       → early signal, candle sedang break tapi belum tutup (+10 bonus di scoring)
    3. HORIZONTAL RESISTANCE BREAK: harga melewati level resistance pivot historis
       yang muncul 2+ kali dalam 48 candle (level nyata, bukan hanya swing high lokal)

    Return: (bos_up, bos_level, pre_close_bos, horiz_break)
    """
    if lookback is None:
        lookback = CONFIG["bos_lookback"]
    if len(candles) < lookback + 1:
        return False, 0.0, False, False

    # --- Layer 1: Standard BOS ---
    prev_highs = [c["high"] for c in candles[-(lookback + 1):-1]]
    bos_level  = max(prev_highs)
    close_now  = candles[-1]["close"]
    high_now   = candles[-1]["high"]
    standard_bos = close_now > bos_level

    # --- Layer 2: Pre-close BOS (high sudah tembus tapi close belum) ---
    pre_close_bos = (not standard_bos) and (high_now > bos_level)

    # --- Layer 3: Horizontal resistance break (pivot count >= 2 dalam 48c) ---
    horiz_break   = False
    horiz_level   = 0.0
    n_hist        = min(48, len(candles) - 1)
    hist_candles  = candles[-(n_hist + 1):-1]
    price_now     = close_now

    if len(hist_candles) >= 5:
        pivot_highs = []
        for i in range(1, len(hist_candles) - 1):
            h = hist_candles[i]["high"]
            if h > hist_candles[i-1]["high"] and h > hist_candles[i+1]["high"]:
                pivot_highs.append(h)
        # Cluster pivot highs dalam 0.5% radius
        if pivot_highs:
            pivot_highs.sort()
            for ph in pivot_highs:
                count = sum(1 for p in pivot_highs if abs(p - ph) / max(ph, 1e-10) < 0.005)
                if count >= 2 and ph < price_now * 1.02:
                    # Level yang ditembus (price sekarang di atas level ini)
                    if price_now > ph > price_now * 0.97:
                        horiz_break = True
                        horiz_level = ph
                        break

    # Pilih bos_level terbaik
    if horiz_break and horiz_level > 0:
        bos_level = max(bos_level, horiz_level)

    bos_up = standard_bos or (pre_close_bos and horiz_break)

    return bos_up, bos_level, pre_close_bos, horiz_break

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
    FIX v28 BUG#3 — Micro Momentum Engine (5m/15m/1h).

    AUDIT FIX: Formula mom_15m dan mom_1h sebelumnya SALAH — dibagi 3 dan 12
    padahal nilai sudah merupakan perubahan TOTAL (bukan per-candle).
    Membaginya membuat nilainya sangat kecil → accel mendekati 0 → skor
    hampir tidak pernah signifikan meski pump akan terjadi.

    Formula yang benar:
      mom_5m  = % change 1 candle 5m terbaru (paling baru)
      mom_15m = % change total 3 candle 5m terakhir (= 15 menit total)
      mom_1h  = % change total 12 candle 5m terakhir (= 1 jam total)

    acceleration = mom_5m * 2 + mom_15m - mom_1h
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

    # mom_15m: % change TOTAL 3 candle terakhir (= 15 menit)
    # FIX v28 BUG#3: TIDAK dibagi 3 — ini adalah total change, bukan per-candle
    c3 = closes[-4] if len(closes) >= 4 else closes[0]
    mom_15m = (closes[-1] - c3) / c3 if c3 > 0 else 0.0

    # mom_1h: % change TOTAL 12 candle terakhir (= 1 jam)
    # FIX v28 BUG#3: TIDAK dibagi 12 — ini adalah total change, bukan per-candle
    c12 = closes[-13] if len(closes) >= 13 else closes[0]
    mom_1h = (closes[-1] - c12) / c12 if c12 > 0 else 0.0

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

    FIX v29 BUG-B: Tambah upward close confirmation.
    Masalah sebelumnya: (close-low)/(high-low) menghasilkan buy_ratio tinggi
    saat retracement karena candle menutup di atas low dengan range sempit.
    Ini menghasilkan false positive "76% buy pressure" saat harga sebenarnya
    sedang turun setelah pump (case MELANIA).

    Fix: Tier hanya naik ke >=2 jika harga candle terbaru JUGA lebih tinggi
    dari candle sebelumnya. Saat retracement, harga turun → tier max 1.
    Ini memaksa buy_pressure tier tinggi hanya muncul saat harga benar naik.

    Tier: >55%→+6 | >65%→+12 | >75%→+20
    """
    window = CONFIG["buy_pressure_window"]
    if len(candles_15m) < window + 1:
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

    # FIX v29 BUG-B: Cek apakah harga sedang naik (upward close confirmation).
    # Bandingkan close candle terbaru vs rata-rata close 3 candle sebelumnya.
    # Jika harga turun, tier dikap di 1 (tidak bisa jadi "PUMP PHASE").
    close_now  = candles_15m[-1]["close"]
    close_prev = sum(c["close"] for c in candles_15m[-4:-1]) / 3 if len(candles_15m) >= 4 else close_now
    price_rising = close_now > close_prev

    if buy_ratio >= CONFIG["buy_pressure_high"]:
        if price_rising:
            # Normal: buy pressure tinggi + harga naik = valid pump signal
            return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                    "score": CONFIG["score_buy_pressure_high"],
                    "label": f"🐳 Buy Pressure {buy_ratio*100:.0f}% — PUMP PHASE",
                    "is_bullish": True, "tier": 3}
        else:
            # BUG-B FIX: buy_ratio tinggi tapi harga turun = retracement artefak
            # Turunkan ke tier 1 (akumulasi), bukan "PUMP PHASE"
            return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                    "score": CONFIG["score_buy_pressure_low"],
                    "label": f"📦 Buy Pressure {buy_ratio*100:.0f}% tapi harga turun — kemungkinan retracement (tier diturunkan)",
                    "is_bullish": True, "tier": 1}
    elif buy_ratio >= CONFIG["buy_pressure_mid"]:
        if price_rising:
            return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                    "score": CONFIG["score_buy_pressure_mid"],
                    "label": f"💰 Buy Pressure {buy_ratio*100:.0f}% — Whale activity",
                    "is_bullish": True, "tier": 2}
        else:
            return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                    "score": CONFIG["score_buy_pressure_low"],
                    "label": f"📦 Buy Pressure {buy_ratio*100:.0f}% tapi harga turun — akumulasi atau retracement",
                    "is_bullish": True, "tier": 1}
    elif buy_ratio >= CONFIG["buy_pressure_low"]:
        return {"buy_ratio": round(buy_ratio,3), "buy_pct": round(buy_ratio*100,1),
                "score": CONFIG["score_buy_pressure_low"],
                "label": f"📦 Buy Pressure {buy_ratio*100:.0f}% — Accumulation",
                "is_bullish": True, "tier": 1}
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
    FIX v28 BUG#4 — Kondisi oi_flat sekarang TIDAK aktif jika is_new=True.

    Sebelumnya: oi_flat = (oi_data.get("is_new", True) or ...)
    Bug: is_new=True di run pertama → oi_flat=True untuk SEMUA coin →
         fake pump lebih mudah terpicu → penalti -5 sampai -20 tidak adil.

    Fix: Jika is_new=True (belum ada baseline OI), lewati kondisi ini.
         Hanya anggap oi_flat jika ada data OI sebelumnya dan OI memang flat.

    v18: price_up AND buy<50% → fake (terlalu sederhana)
    v18: fake_score berbasis 4 kondisi bertingkat:
      1. price_spike: harga naik > 0.5% dalam 3h
      2. vol_spike: volume naik tapi buy rendah
      3. OI_flat: OI tidak naik (tidak ada posisi baru) — SKIP jika is_new
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

    # Kondisi 3: OI flat — FIX v28 BUG#4: SKIP jika is_new=True
    # Jika belum ada baseline OI, kita tidak tahu apakah OI flat atau tidak.
    # Jangan langsung anggap flat = fake hanya karena run pertama.
    is_oi_new = oi_data.get("is_new", True)
    if is_oi_new:
        oi_flat = False  # FIX: tidak bisa menilai OI flat tanpa baseline
    else:
        oi_flat = abs(oi_data.get("change_pct", 0)) < CONFIG["fake_oi_flat_max"]

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


def detect_whale_accumulation(candles, squeeze_active=False):
    """
    STEP 16 v19 — Whale Accumulation Detection.

    v31 FIX-v31-6: Squeeze bypass untuk vol requirement.
    v30: vol_rising = vol > 1.3x WAJIB → akumulasi PIXEL/HUS tidak terdeteksi.
    Akumulasi nyata justru punya volume rendah karena market maker menekan
    harga dan volume sambil akumulasi secara diam-diam.

    Saat squeeze_active=True:
      - vol_rising diganti: vol 0.08-0.65x baseline JUGA dianggap "akumulasi"
      - Hanya perlu price_sideways + atr_contracting (2/2 non-vol cond)
      - Ini mendeteksi "stealth accumulation" yang tidak terlihat dari volume

    Pattern normal: vol naik + harga sideways + ATR menyempit
    Pattern squeeze: vol sangat rendah + harga sideways + ATR menyempit
    """
    window = 12
    if len(candles) < window + 12:
        return {"is_accum": False, "score": 0, "label": "Data kurang"}

    recent   = candles[-window:]
    baseline = candles[-(window + 12):-window]

    avg_recent   = sum(c["volume_usd"] for c in recent)   / len(recent)
    avg_baseline = sum(c["volume_usd"] for c in baseline) / len(baseline)
    vol_ratio    = (avg_recent / avg_baseline) if avg_baseline > 0 else 1.0

    # Vol naik (mode normal)
    vol_rising = vol_ratio >= CONFIG["whale_accum_vol_min"] if avg_baseline > 0 else False

    # v31: Vol rendah dalam squeeze = stealth accumulation mode
    lv_min = CONFIG.get("accum_low_vol_min_ratio", 0.08)
    lv_max = CONFIG.get("accum_low_vol_max_ratio", 0.65)
    vol_stealth = squeeze_active and (lv_min <= vol_ratio <= lv_max)

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

    # Normal mode: butuh min 2 dari 3 kondisi (termasuk vol)
    n_cond_normal = sum([vol_rising, price_sideways, atr_contracting])
    # Stealth mode: butuh price_sideways + atr_contracting (vol diabaikan)
    n_cond_stealth = sum([price_sideways, atr_contracting])

    is_accum_normal  = n_cond_normal  >= 2
    is_accum_stealth = vol_stealth and n_cond_stealth >= 2
    is_accum = is_accum_normal or is_accum_stealth

    if is_accum_stealth and not is_accum_normal:
        score = CONFIG["score_whale_accum"] - 2   # sedikit lebih rendah karena vol tidak konfirmasi
        label = (f"🐳 Whale Stealth Accum — vol sangat rendah {vol_ratio:.2f}x "
                 f"(squeeze), range {range_pct:.1f}%, ATR menyempit — pre-pump signal")
    elif n_cond_normal >= 3:
        score = CONFIG["score_whale_accum"]
        label = (f"🐳 Whale Accumulation CONFIRMED — vol {vol_ratio:.1f}x, "
                 f"range {range_pct:.1f}%, ATR menyempit") if avg_baseline > 0 else "🐳 Whale Accum"
    elif n_cond_normal == 2:
        score = 4
        label = f"🐳 Whale Accum PROBABLE ({n_cond_normal}/3 kondisi)"
    else:
        score = 0
        label = f"No whale accum ({n_cond_normal}/3)"

    return {
        "is_accum":        is_accum,
        "n_cond":          n_cond_normal,
        "vol_rising":      vol_rising,
        "vol_stealth":     vol_stealth,
        "vol_ratio":       round(vol_ratio, 3),
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

def detect_smart_money_accumulation_v22(candles, squeeze_active=False):
    """
    FIX 02 v22 — Smart Money Accumulation Detector.

    v31 FIX-v31-8: Squeeze bypass untuk vol threshold.
    v30: sma_vol_trend_min = 1.2 → akumulasi low-vol tidak terdeteksi.
    Saat squeeze_active=True, threshold diturunkan ke 0.5 (vol boleh rendah).
    Dengan demikian range_contraction + bid_pressure sudah cukup jadi sinyal.

    Hidden accumulation signal:
      • price range contraction over last 20 candles < 3%
      • volume trend (normal: >1.2x; squeeze mode: >0.5x)
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
    # v31: saat squeeze aktif, vol threshold diturunkan ke 0.5 (bukan 1.2)
    vol_threshold = 0.5 if squeeze_active else CONFIG["sma_vol_trend_min"]
    cond_vol   = vol_trend   >= vol_threshold
    cond_bid   = bid_pressure > 0.52   # slight buy bias

    n_cond         = sum([cond_range, cond_vol, cond_bid])
    is_accumulating = n_cond >= 2

    if n_cond == 3:
        score = CONFIG["score_smart_money_accum"]
        sq_tag = " [squeeze mode]" if squeeze_active and vol_trend < 1.0 else ""
        label = (f"🏦 Smart Money Accum CONFIRMED{sq_tag} — range {range_ratio*100:.1f}%, "
                 f"vol {vol_trend:.2f}x, bid {bid_pressure*100:.0f}%")
    elif n_cond == 2:
        score = CONFIG["score_smart_money_accum"] // 2
        label = (f"🏦 Smart Money Accum PROBABLE ({n_cond}/3) — "
                 f"range {range_ratio*100:.1f}%, vol {vol_trend:.2f}x")
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
    vol_bonus = 4 if cond_vol else 0

    # FIX v30 FIX-5: tiered scoring (sama dengan detect_volatility_compression_v23)
    thresh_mild = CONFIG.get("bb_percentile_threshold", 30)
    thresh_mid  = min(thresh_mild, 20)
    thresh_str  = min(thresh_mid, 10)
    s_full = CONFIG.get("score_prebreakout",      20)
    s_mid  = CONFIG.get("score_prebreakout_mid",  12)
    s_mild = CONFIG.get("score_prebreakout_mild",  6)

    is_compressed = cond_bb

    if percentile <= thresh_str:
        score = s_full + vol_bonus
        label = (f"💥 Pre-Breakout SQUEEZE EKSTREM p{percentile:.0f}"
                 f"{' + vol confirm' if cond_vol else ''} (+{score})")
    elif percentile <= thresh_mid:
        score = s_mid + vol_bonus
        label = (f"💥 Pre-Breakout BB kuat p{percentile:.0f}"
                 f"{' + vol confirm' if cond_vol else ''} (+{score})")
    elif cond_bb:
        score = s_mild + vol_bonus
        label = (f"💥 Pre-Breakout (BB only): p{percentile:.0f}"
                 f"{' + vol' if cond_vol else ''} (+{score})")
    else:
        score = 0
        is_compressed = False
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


# ══════════════════════════════════════════════════════════════════════════════
#  🛡️  v32b: SUPPORT HEALTH SYSTEM — Deteksi Preventif Support Jebol
# ══════════════════════════════════════════════════════════════════════════════
def calc_support_health(candles, cp_sr, vol_zscore_v20, oi_data):
    """
    v32b — Support Health System: 7 lapis deteksi preventif SEBELUM support jebol.

    Berbeda dari dump_filter (deteksi dump AKTIF) dan dump_trap (butuh price<EMA200),
    fungsi ini mendeteksi tanda-tanda PELEMAHAN support secara dini saat harga
    masih di atas support — sehingga scanner bisa menolak atau memberi peringatan
    SEBELUM trader masuk posisi.

    7 Indikator:
    1. Support stress test: harga menyentuh support berulang (3x+) tanpa BOS ke atas
       → setiap sentuhan memperlemah support (distribusi tersembunyi)
    2. Negative volume spike mendekati support: seller agresif di dekat support
       → tanda institusi sedang menembus support
    3. Consecutive bearish candles menuju support: momentum turun yang konsisten
       → buyer tidak ada tenaga untuk mempertahankan support
    4. SL gap terlalu kecil: jarak entry ke support < 1.5%
       → tidak ada ruang breathing, noise biasa bisa trigger stop
    5. OI naik + harga turun menuju support: short buildup aktif
       → institusi membangun posisi short, bukan akumulasi long
    6. CVD negatif konsisten di dekat support: seller dominan di zona support
       → kontradiksi langsung dengan asumsi support kuat
    7. Failed bounce pattern: 2x menyentuh support, bounce makin lemah
       → exhausted support, siap jebol

    Return: dict dengan health_score (0-100), risk_level, signals, dan
            should_warn / should_block flags.
    """
    signals     = []
    penalties   = []
    health      = 100   # mulai dari sehat, kurangi per indikator negatif

    price_now   = candles[-1]["close"] if candles else 0
    if price_now <= 0 or len(candles) < 20:
        return _support_health_empty()

    cp_sup_level  = cp_sr["support"]["level"]   if (cp_sr and cp_sr.get("support"))  else 0.0
    cp_sup_level1 = cp_sr["support"]["level_1"] if (cp_sr and cp_sr.get("support"))  else 0.0

    # Fallback ke swing_low jika tidak ada CP support
    if cp_sup_level <= 0:
        cp_sup_level = min(c["low"] for c in candles[-20:])

    if cp_sup_level <= 0:
        return _support_health_empty()

    dist_to_sup = (price_now - cp_sup_level) / price_now   # fraksi jarak

    # ── Indikator 1: Support stress test (sentuhan berulang) ─────────────────
    # Hitung berapa kali low candle menyentuh zona support (dalam 2% dari level)
    touch_zone  = cp_sup_level * 1.02
    n_touches   = sum(1 for c in candles[-30:] if c["low"] <= touch_zone)
    # Cek apakah ada BOS ke atas setelah sentuhan (bounce valid)
    bos_after   = False
    for i in range(len(candles) - 10, len(candles)):
        if i >= 1 and candles[i]["high"] > max(c["high"] for c in candles[max(0,i-8):i]):
            bos_after = True
            break

    if n_touches >= 3 and not bos_after:
        # Makin banyak sentuhan tanpa BOS = makin lemah
        pen = min(25, n_touches * 6)
        health -= pen
        penalties.append(pen)
        signals.append(
            f"⚠️ Support stress: {n_touches}x disentuh tanpa BOS ke atas "
            f"— distribusi tersembunyi (penalti -{pen})"
        )

    # ── Indikator 2: Volume negatif spike mendekati support ──────────────────
    # Cek 5 candle terbaru: apakah ada vol negatif besar (>2x baseline) saat
    # harga dalam 5% dari support?
    if len(candles) >= 25:
        vol_baseline = sum(abs(c["volume_usd"]) for c in candles[-25:-5]) / 20
        for c in candles[-5:]:
            c_dist = (c["low"] - cp_sup_level) / price_now
            if c_dist < 0.05:   # dalam 5% dari support
                dv  = _delta_volume(c)
                if dv < 0 and abs(dv) > vol_baseline * 2.0:
                    pen = 20
                    health -= pen
                    penalties.append(pen)
                    signals.append(
                        f"🚨 Negative vol spike {abs(dv)/1e3:.0f}K ({abs(dv)/vol_baseline:.1f}x baseline) "
                        f"dekat support — seller agresif (penalti -{pen})"
                    )
                    break

    # ── Indikator 3: Consecutive bearish menuju support ───────────────────────
    # 3+ candle merah berturut-turut, setiap close lebih rendah
    recent6 = candles[-6:]
    bearish_streak = 0
    for i in range(1, len(recent6)):
        if recent6[i]["close"] < recent6[i]["open"] and recent6[i]["close"] < recent6[i-1]["close"]:
            bearish_streak += 1
        else:
            bearish_streak = 0
    if bearish_streak >= 3:
        pen = 15
        health -= pen
        penalties.append(pen)
        signals.append(
            f"📉 {bearish_streak} candle merah berturut-turut menuju support "
            f"— momentum turun (penalti -{pen})"
        )

    # ── Indikator 4: SL gap terlalu kecil ────────────────────────────────────
    # Jika jarak dari entry ke support < 1.5%, noise bisa trigger SL
    if dist_to_sup < 0.015 and dist_to_sup > 0:
        pen = 10
        health -= pen
        penalties.append(pen)
        signals.append(
            f"⚠️ Jarak ke support hanya {dist_to_sup*100:.2f}% "
            f"— SL gap terlalu sempit, rawan noise (penalti -{pen})"
        )

    # ── Indikator 5: OI naik + harga turun ke support ────────────────────────
    # OI bertambah sementara harga turun = short building
    oi_chg  = oi_data.get("change_pct", 0) if oi_data else 0
    harga_turun = len(candles) >= 4 and candles[-1]["close"] < candles[-4]["close"]
    if oi_chg > 3.0 and harga_turun and dist_to_sup < 0.08:
        pen = 15
        health -= pen
        penalties.append(pen)
        signals.append(
            f"🚨 OI +{oi_chg:.1f}% + harga turun menuju support "
            f"— short buildup aktif (penalti -{pen})"
        )

    # ── Indikator 6: CVD negatif konsisten di dekat support ──────────────────
    # CVD = cumulative delta vol 8 candle terakhir, saat harga dekat support
    if len(candles) >= 10 and dist_to_sup < 0.05:
        dv_recent = [_delta_volume(c) for c in candles[-8:]]
        cvd_sum   = sum(dv_recent)
        cvd_neg   = sum(1 for d in dv_recent if d < 0)
        if cvd_neg >= 6 and cvd_sum < 0:
            pen = 18
            health -= pen
            penalties.append(pen)
            signals.append(
                f"📊 CVD negatif: {cvd_neg}/8 candle seller dominan di dekat support "
                f"— support lemah (penalti -{pen})"
            )

    # ── Indikator 7: Failed bounce pattern ───────────────────────────────────
    # Cari 2 titik dimana low menyentuh support zone, lalu cek apakah
    # bounce setelahnya makin lemah (each bounce high < prev bounce high)
    if len(candles) >= 20:
        touch_indices = [
            i for i, c in enumerate(candles[-20:])
            if c["low"] <= cp_sup_level * 1.015
        ]
        if len(touch_indices) >= 2:
            # Ambil high setelah masing-masing touch
            bounce_highs = []
            for ti in touch_indices[-2:]:
                real_idx = len(candles) - 20 + ti
                # High maksimum dalam 3 candle setelah touch
                window = candles[real_idx: min(real_idx + 4, len(candles))]
                bounce_highs.append(max(c["high"] for c in window) if window else 0)
            if len(bounce_highs) == 2 and bounce_highs[1] < bounce_highs[0] * 0.995:
                pen = 12
                health -= pen
                penalties.append(pen)
                signals.append(
                    f"⚠️ Failed bounce: bounce ke-2 ({bounce_highs[1]:.6g}) "
                    f"< bounce ke-1 ({bounce_highs[0]:.6g}) — support melemah (penalti -{pen})"
                )

    # ── Tentukan risk level dan flags ─────────────────────────────────────────
    health      = max(0, health)
    total_pen   = sum(penalties)
    n_warnings  = len(penalties)

    if health <= 30 or total_pen >= 40:
        risk_level   = "CRITICAL"
        should_block = True
        should_warn  = True
    elif health <= 55 or total_pen >= 25:
        risk_level   = "HIGH"
        should_block = False
        should_warn  = True
    elif health <= 75 or total_pen >= 15:
        risk_level   = "MEDIUM"
        should_block = False
        should_warn  = True
    else:
        risk_level   = "LOW"
        should_block = False
        should_warn  = False

    return {
        "ok":            True,
        "health":        health,
        "risk_level":    risk_level,
        "total_penalty": total_pen,
        "n_warnings":    n_warnings,
        "signals":       signals,
        "should_block":  should_block,
        "should_warn":   should_warn,
        "dist_to_sup":   round(dist_to_sup * 100, 2),
        "cp_sup_level":  cp_sup_level,
        "n_touches":     n_touches,
    }


def _support_health_empty():
    """Return default jika tidak ada data support."""
    return {
        "ok": False, "health": 100, "risk_level": "UNKNOWN",
        "total_penalty": 0, "n_warnings": 0,
        "signals": [], "should_block": False, "should_warn": False,
        "dist_to_sup": 0.0, "cp_sup_level": 0.0, "n_touches": 0,
    }
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

    FIX v30 FIX-5: Scoring bertingkat berdasarkan seberapa ekstrem squeeze.
    Squeeze persentil rendah adalah sinyal paling prediktif untuk big pump.
    PIXEL BBW sangat rendah 2 hari, HUS 4 hari sebelum pump.
      p <= 10 → +20 (squeeze ekstrem — langka dan sangat prediktif)
      p <= 20 → +12 (squeeze kuat)
      p <= 30 → +6  (squeeze moderat)
    Volume confirmation memberikan bonus tambahan +4 di setiap tier.
    Sebelumnya: +8 atau +4. Sekarang: +6 sampai +24 tergantung squeeze level.
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
    vol_r     = sum(c["volume_usd"] for c in candles[-3:]) / 3
    vol_p     = sum(c["volume_usd"] for c in candles[-15:-3]) / 12
    vol_ok    = (vol_r / vol_p) >= 1.3 if vol_p > 0 else False
    vol_bonus = 4 if vol_ok else 0

    # FIX v30 FIX-5: tiered scoring
    thresh_mild = CONFIG.get("bb_percentile_threshold", 30)
    thresh_mid  = min(thresh_mild, 20)
    thresh_str  = min(thresh_mid, 10)

    s_full = CONFIG.get("score_prebreakout",      20)
    s_mid  = CONFIG.get("score_prebreakout_mid",  12)
    s_mild = CONFIG.get("score_prebreakout_mild",  6)

    if pctile <= thresh_str:
        score = s_full + vol_bonus
        label = (f"💥 BB SQUEEZE EKSTREM p{pctile:.0f} — energi sangat terkompresi"
                 f"{' + vol confirm' if vol_ok else ''} (+{score})")
        return {"is_compressed": True, "score": score,
                "percentile": round(pctile, 1), "bbw": round(cur_bbw, 5), "label": label}
    elif pctile <= thresh_mid:
        score = s_mid + vol_bonus
        label = (f"💥 BB Squeeze kuat p{pctile:.0f}"
                 f"{' + vol confirm' if vol_ok else ''} (+{score})")
        return {"is_compressed": True, "score": score,
                "percentile": round(pctile, 1), "bbw": round(cur_bbw, 5), "label": label}
    elif pctile <= thresh_mild:
        score = s_mild + vol_bonus
        label = (f"💥 BB Squeeze moderat p{pctile:.0f}"
                 f"{' + vol confirm' if vol_ok else ''} (+{score})")
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


# ══════════════════════════════════════════════════════════════════════════════
#  🆕  v27 FEATURE 1: OI ACCELERATION DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_oi_acceleration_v27(symbol, oi_now):
    """
    v27 — OI Acceleration Detector.

    Pump besar (30-70%) sering didahului OI yang MAKIN CEPAT bertambah,
    bukan hanya bertambah (second derivative positif dari time-series OI).

    Logika:
    - Simpan 6 snapshot OI per coin (tiap run scan ≈ tiap 15-30 menit)
    - Hitung rate of change OI (% per jam) antar snapshot berturutan
    - Bandingkan rate terbaru vs rata-rata rate sebelumnya
    - Jika OI naik makin cepat = ACCELERATION = institusi masuk dengan urgensi tinggi

    Contoh nyata pre-pump 50%:
      t-3h: OI +1.2%/h   ← akumulasi normal
      t-2h: OI +2.8%/h   ← mulai akselerasi
      t-1h: OI +5.4%/h   ← akselerasi kuat → ALERT!
      t-0h: pump +50%

    Returns: is_accelerating, is_strong, acceleration_rate, score, label.
    """
    global _oi_accel_history

    # Tambah snapshot terbaru ke histori
    update_oi_accel_snapshot(symbol, oi_now)

    snaps     = _oi_accel_history.get(symbol, [])
    min_snaps = CONFIG.get("oi_accel_min_snapshots", 3)

    if len(snaps) < min_snaps or oi_now <= 0:
        return {
            "is_accelerating": False, "is_strong": False,
            "acceleration_rate": 0.0, "current_rate": 0.0,
            "avg_prior_rate": 0.0, "rates": [],
            "score": 0, "label": "OI Accel: data belum cukup (butuh ≥3 scan)",
        }

    # Hitung rate of change per interval (dinormalisasi ke %/jam)
    rates = []
    for i in range(1, len(snaps)):
        prev_oi = snaps[i-1]["oi"]
        curr_oi = snaps[i]["oi"]
        dt      = snaps[i]["ts"] - snaps[i-1]["ts"]
        if prev_oi > 0 and dt > 60:   # minimal 1 menit antar snapshot
            rate_pct = (curr_oi - prev_oi) / prev_oi * 100 * (3600.0 / dt)
            rates.append(rate_pct)

    if len(rates) < 2:
        return {
            "is_accelerating": False, "is_strong": False,
            "acceleration_rate": 0.0, "current_rate": 0.0,
            "avg_prior_rate": 0.0, "rates": rates,
            "score": 0, "label": "OI Accel: rate belum cukup",
        }

    current_rate     = rates[-1]
    prior_rates      = rates[:-1]
    avg_prior        = sum(prior_rates) / len(prior_rates)
    acceleration_rate = current_rate - avg_prior   # second derivative OI

    accel_threshold = CONFIG.get("oi_accel_threshold", 0.5)
    accel_strong    = CONFIG.get("oi_accel_strong", 1.5)

    # Semua rate dalam 3 snapshot terakhir positif = OI naik konsisten
    all_positive = all(r > 0 for r in rates[-3:]) if len(rates) >= 3 else current_rate > 0

    is_accelerating = (
        current_rate > accel_threshold
        and acceleration_rate > accel_threshold
        and all_positive
    )
    is_strong = (
        is_accelerating
        and current_rate    > accel_strong
        and acceleration_rate > accel_strong
    )

    if is_strong:
        score = CONFIG.get("score_oi_accel_strong", 18)
        label = (
            f"🚀 OI ACCELERATION KUAT! Rate: +{current_rate:.2f}%/h "
            f"(sebelumnya avg +{avg_prior:.2f}%/h, akselerasi +{acceleration_rate:.2f}%/h) "
            f"— INSTITUSI MASUK URGENSI TINGGI, pump 1-3 jam lagi!"
        )
    elif is_accelerating:
        score = CONFIG.get("score_oi_accel", 8)
        label = (
            f"⚡ OI Acceleration: +{current_rate:.2f}%/h vs avg +{avg_prior:.2f}%/h "
            f"(akselerasi +{acceleration_rate:.2f}%/h) — OI makin cepat, pre-pump signal"
        )
    elif current_rate > accel_threshold and all_positive:
        score = CONFIG.get("score_oi_accel", 8) // 2
        is_accelerating = True
        label = f"📊 OI Rising Steady: +{current_rate:.2f}%/h — naik stabil, belum akselerasi"
    elif current_rate < -accel_threshold:
        score = -(CONFIG.get("score_oi_accel", 8) // 2)
        label = f"⚠️ OI Menurun: {current_rate:.2f}%/h — posisi ditutup (distribusi/profit taking)"
    else:
        score = 0
        label = f"OI Stabil: rate {current_rate:+.2f}%/h (n={len(snaps)} snapshots)"

    return {
        "is_accelerating":   is_accelerating,
        "is_strong":         is_strong,
        "acceleration_rate": round(acceleration_rate, 3),
        "current_rate":      round(current_rate, 3),
        "avg_prior_rate":    round(avg_prior, 3),
        "rates":             [round(r, 3) for r in rates],
        "n_snapshots":       len(snaps),
        "score":             score,
        "label":             label,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🆕  v27 FEATURE 2: ORDERBOOK LIQUIDITY VACUUM DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
def get_orderbook_v27(symbol, limit=20):
    """
    v27 — Ambil real orderbook dari Bitget Futures API.
    Menggunakan endpoint merge-depth; fallback ke books jika gagal.
    """
    data = safe_get(
        f"{BITGET_BASE}/api/v2/mix/market/merge-depth",
        params={
            "symbol":      symbol,
            "productType": "usdt-futures",
            "precision":   "scale1",
            "limit":       str(limit),
        },
    )
    if not data or data.get("code") != "00000":
        data = safe_get(
            f"{BITGET_BASE}/api/v2/mix/market/books",
            params={
                "symbol":      symbol,
                "productType": "usdt-futures",
                "limit":       str(limit),
            },
        )
    if not data or data.get("code") != "00000":
        return None
    try:
        d        = data.get("data", {})
        asks_raw = d.get("asks", [])
        bids_raw = d.get("bids", [])
        asks = [[float(x[0]), float(x[1])] for x in asks_raw if len(x) >= 2]
        bids = [[float(x[0]), float(x[1])] for x in bids_raw if len(x) >= 2]
        asks.sort(key=lambda x: x[0])           # ascending (lowest ask first)
        bids.sort(key=lambda x: x[0], reverse=True)  # descending (highest bid first)
        return {"asks": asks, "bids": bids}
    except Exception:
        return None


def detect_liquidity_vacuum_v27(symbol, price_now, vol_zscore_z):
    """
    v27 — Orderbook Liquidity Vacuum Detector.

    Liquidity Vacuum = gap besar antar level ask di orderbook = area kosong
    tanpa hambatan sell order. Ketika harga masuk area ini, harga bisa
    meluncur sangat cepat karena tidak ada penjual (pump 20-70%!).

    Prinsip: Market bergerak dari satu cluster likuiditas ke cluster berikutnya.
    Jika antara cluster A dan cluster B ada gap 0.8% tanpa order = harga akan
    "teleport" melewati gap itu dalam hitungan menit begitu breakout terjadi.

    Juga menghitung real bid/ask depth ratio (bukan proxy candle seperti v23).
    """
    ob = get_orderbook_v27(symbol, limit=CONFIG.get("ob_vacuum_depth", 20))

    # Fallback jika orderbook tidak tersedia
    if not ob or not ob.get("asks") or not ob.get("bids"):
        return {
            "is_vacuum": False, "vacuum_pct": 0.0, "vacuum_bid_pct": 0.0,
            "vacuum_side": "none", "vacuum_price_start": 0.0,
            "vacuum_price_end": 0.0, "bid_depth_usd": 0.0, "ask_depth_usd": 0.0,
            "bid_ask_real_ratio": 1.0,
            "score": 0, "label": "OB Vacuum: data orderbook tidak tersedia (API fallback)",
        }

    asks = ob["asks"]
    bids = ob["bids"]

    gap_min_pct  = CONFIG.get("ob_vacuum_gap_min_pct", 0.3) / 100.0
    gap_strong   = CONFIG.get("ob_ask_vacuum_gap_strong", 0.8) / 100.0
    vol_min      = CONFIG.get("ob_vacuum_vol_confirm_min", 1.3)
    vol_ok       = vol_zscore_z > vol_min

    # ── Real liquidity depth (USD value of top 5 levels) ─────────────────────
    ask_depth_usd = sum(p * s for p, s in asks[:5]) if len(asks) >= 5 else sum(p * s for p, s in asks)
    bid_depth_usd = sum(p * s for p, s in bids[:5]) if len(bids) >= 5 else sum(p * s for p, s in bids)
    real_ratio    = bid_depth_usd / ask_depth_usd if ask_depth_usd > 0 else 1.0

    # ── Deteksi vacuum ASK (hambatan jual kosong = bullish) ───────────────────
    ask_vacuum_pct   = 0.0
    ask_vac_start    = 0.0
    ask_vac_end      = 0.0
    for i in range(1, min(len(asks), 20)):
        gap = (asks[i][0] - asks[i-1][0]) / asks[i-1][0]
        if gap > ask_vacuum_pct:
            ask_vacuum_pct = gap
            ask_vac_start  = asks[i-1][0]
            ask_vac_end    = asks[i][0]

    # ── Deteksi vacuum BID (support kosong = bearish / dump risk) ─────────────
    bid_vacuum_pct   = 0.0
    bid_vac_start    = 0.0
    bid_vac_end      = 0.0
    for i in range(1, min(len(bids), 20)):
        gap = (bids[i-1][0] - bids[i][0]) / bids[i][0]
        if gap > bid_vacuum_pct:
            bid_vacuum_pct = gap
            bid_vac_start  = bids[i-1][0]
            bid_vac_end    = bids[i][0]

    # ── Scoring ───────────────────────────────────────────────────────────────
    is_vacuum   = False
    vacuum_side = "none"

    if ask_vacuum_pct >= gap_strong and vol_ok:
        score       = CONFIG.get("score_ob_vacuum_strong", 20)
        is_vacuum   = True
        vacuum_side = "ask_strong"
        label = (
            f"🚀 LIQUIDITY VACUUM KUAT! Gap ASK {ask_vacuum_pct*100:.2f}% "
            f"({_fmt_price(ask_vac_start)}→{_fmt_price(ask_vac_end)}) "
            f"+ vol z={vol_zscore_z:.2f}. "
            f"PUMP HIGHWAY — tidak ada hambatan sell sampai {_fmt_price(ask_vac_end)}!"
        )
    elif ask_vacuum_pct >= gap_strong:
        score       = CONFIG.get("score_ob_vacuum", 12)
        is_vacuum   = True
        vacuum_side = "ask_strong"
        label = (
            f"🕳️ Vacuum ASK Kuat {ask_vacuum_pct*100:.2f}% "
            f"({_fmt_price(ask_vac_start)}→{_fmt_price(ask_vac_end)}) "
            f"— area terbuka, vol belum konfirmasi"
        )
    elif ask_vacuum_pct >= gap_min_pct and ask_vacuum_pct >= bid_vacuum_pct:
        score       = CONFIG.get("score_ob_vacuum", 12) if vol_ok else CONFIG.get("score_ob_vacuum", 12) // 2
        is_vacuum   = True
        vacuum_side = "ask"
        label = (
            f"⚡ Liquidity Vacuum ASK: {ask_vacuum_pct*100:.2f}% "
            f"di {_fmt_price(ask_vac_start)}-{_fmt_price(ask_vac_end)}"
            + (f" ✓ vol z={vol_zscore_z:.2f}" if vol_ok else " (vol belum konfirm)")
        )
    elif bid_vacuum_pct > ask_vacuum_pct and bid_vacuum_pct >= gap_min_pct:
        score       = -(CONFIG.get("score_ob_vacuum", 12) // 3)
        is_vacuum   = True
        vacuum_side = "bid"
        label = (
            f"⚠️ Vacuum BID: gap {bid_vacuum_pct*100:.2f}% "
            f"di {_fmt_price(bid_vac_end)}-{_fmt_price(bid_vac_start)} "
            f"— support tipis, risiko dump"
        )
    else:
        score = 0
        label = (
            f"OB: likuiditas normal (ask gap {ask_vacuum_pct*100:.2f}%, "
            f"bid/ask {real_ratio:.2f}x)"
        )

    # Bonus: real bid depth jauh lebih besar dari ask = akumulasi nyata
    if real_ratio >= 2.0 and score >= 0:
        score += 5
        label += f" | 📊 Bid depth {real_ratio:.1f}× ask (akumulasi nyata)"

    return {
        "is_vacuum":          is_vacuum,
        "vacuum_pct":         round(ask_vacuum_pct * 100, 3),
        "vacuum_bid_pct":     round(bid_vacuum_pct * 100, 3),
        "vacuum_side":        vacuum_side,
        "vacuum_price_start": round(ask_vac_start, 8),
        "vacuum_price_end":   round(ask_vac_end, 8),
        "bid_depth_usd":      round(bid_depth_usd, 2),
        "ask_depth_usd":      round(ask_depth_usd, 2),
        "bid_ask_real_ratio": round(real_ratio, 3),
        "score":              score,
        "label":              label,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🆕  v27 FEATURE 3: CVD DIVERGENCE DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
def calc_cvd_divergence_v27(candles_1h, candles_15m=None):
    """
    v27 — Cumulative Volume Delta (CVD) Divergence Detector.

    CVD = akumulasi (buy_volume - sell_volume) sepanjang waktu.
    Dihitung dari OHLCV candle menggunakan Close Location Value:
      delta per candle = volume × (2 × close_position - 1)
      dimana close_position = (close - low) / (high - low)

    Empat sinyal utama:

    A. BULLISH DIVERGENCE (score +15):
       Harga turun tapi CVD naik = meski chart terlihat merah, lebih banyak
       pembelian di bawah (akumulasi tersembunyi). Ini sinyal pre-pump PALING KUAT.
       Contoh: harga -3% dalam 12 jam tapi CVD +40% = whale beli diam-diam.

    B. BEARISH DIVERGENCE / DISTRIBUSI (score -10):
       Harga naik tapi CVD turun = jual di kenaikan harga (distribusi).
       JANGAN MASUK! Ini adalah false pump yang akan berbalik.
       Contoh: harga +5% tapi CVD -30% = pump palsu, segera turun.

    C. CVD MOMENTUM (score +8):
       Harga naik DAN CVD naik, CVD lebih cepat = buying pressure nyata.
       Pump berlanjut, konfirmasi akumulasi = distribusi.

    D. CVD EXHAUSTION (score -5):
       Harga masih naik tapi delta CVD melambat >70% di paruh akhir.
       Pump hampir habis, jangan masuk terlambat.

    Menggunakan candle 15m jika tersedia (lebih sensitif), fallback ke 1h.
    """
    lookback = CONFIG.get("cvd_lookback", 12)

    # Gunakan 15m untuk resolusi lebih tinggi (multiply lookback 4x untuk 1h equivalent)
    if candles_15m and len(candles_15m) >= lookback * 4:
        candles    = candles_15m
        use_15m    = True
        effective_lb = lookback * 4
    elif candles_1h and len(candles_1h) >= lookback + 2:
        candles    = candles_1h
        use_15m    = False
        effective_lb = lookback
    else:
        return {
            "cvd_now": 0.0, "cvd_trend_norm": 0.0, "price_trend_pct": 0.0,
            "is_bullish_div": False, "is_bearish_div": False,
            "is_momentum": False, "is_exhaustion": False,
            "score": 0, "label": "CVD: data tidak cukup",
        }

    recent = candles[-effective_lb:]

    # ── Hitung delta per candle (Close Location Value method) ─────────────────
    deltas = []
    for c in recent:
        rng = c["high"] - c["low"]
        if rng > 0:
            close_pos  = (c["close"] - c["low"]) / rng
        else:
            close_pos  = 0.5
        delta_frac = 2.0 * close_pos - 1.0    # [-1, +1]
        delta      = c["volume_usd"] * delta_frac
        deltas.append(delta)

    # ── CVD kumulatif ─────────────────────────────────────────────────────────
    cvd_series = []
    cum = 0.0
    for d in deltas:
        cum += d
        cvd_series.append(cum)

    cvd_now   = cvd_series[-1]
    cvd_start = cvd_series[0]
    mid_idx   = len(cvd_series) // 2
    cvd_mid   = cvd_series[mid_idx]

    # Trend CVD (normalisasi oleh rata-rata volume untuk scale-invariant)
    avg_vol  = sum(c["volume_usd"] for c in recent) / len(recent)
    if avg_vol > 0 and effective_lb > 0:
        cvd_trend_norm = (cvd_now - cvd_start) / (avg_vol * effective_lb)
    else:
        cvd_trend_norm = 0.0
    cvd_trend_norm = max(-1.0, min(1.0, cvd_trend_norm))

    # Trend harga dalam lookback yang sama
    price_start = recent[0]["close"]
    price_now_c = recent[-1]["close"]
    price_trend = (price_now_c - price_start) / price_start if price_start > 0 else 0.0

    # Normalisasi harga trend ke [-1, +1] (scale: 10% = max)
    price_trend_norm = max(-1.0, min(1.0, price_trend / 0.10))

    div_threshold = CONFIG.get("cvd_divergence_threshold", 0.4)

    # ── Deteksi sinyal ────────────────────────────────────────────────────────
    is_bullish_div = (
        price_trend < -0.005          # harga turun minimal 0.5%
        and cvd_trend_norm > div_threshold    # CVD naik signifikan
    )

    is_bearish_div = (
        price_trend > 0.005           # harga naik minimal 0.5%
        and cvd_trend_norm < -div_threshold   # CVD turun signifikan (distribusi)
    )

    is_momentum = (
        price_trend > 0.002
        and cvd_trend_norm > 0.15
        and cvd_trend_norm > price_trend_norm * 1.2   # CVD lebih kuat dari harga
        and not is_bearish_div
    )

    # Exhaustion: rata-rata delta CVD di paruh akhir vs paruh awal
    first_half_delta  = (cvd_mid - cvd_start)
    second_half_delta = (cvd_now  - cvd_mid)
    is_exhaustion = (
        price_trend > 0.005
        and first_half_delta > 0
        and second_half_delta < first_half_delta * 0.3   # CVD melambat >70%
    )

    # ── Scoring ───────────────────────────────────────────────────────────────
    if is_bullish_div:
        score = CONFIG.get("score_cvd_bullish_div", 15)
        label = (
            f"📉→📈 CVD BULLISH DIVERGENCE! "
            f"Harga {price_trend*100:+.2f}% turun tapi CVD {cvd_trend_norm*100:+.0f}% naik "
            f"({'15m' if use_15m else '1h'} data) — "
            f"AKUMULASI TERSEMBUNYI! Whale beli diam-diam. Pre-pump signal KUAT."
        )
    elif is_bearish_div:
        score = CONFIG.get("score_cvd_bearish_div", -10)
        label = (
            f"📈→📉 CVD BEARISH DIVERGENCE (DISTRIBUSI)! "
            f"Harga {price_trend*100:+.2f}% naik tapi CVD {cvd_trend_norm*100:+.0f}% turun "
            f"— JUAL DI KENAIKAN! Ini false pump, jangan masuk!"
        )
    elif is_exhaustion:
        score = -(CONFIG.get("score_cvd_bullish_div", 15) // 3)
        ratio_pct = second_half_delta / max(abs(first_half_delta), 1) * 100
        label = (
            f"⚠️ CVD Exhaustion: harga {price_trend*100:+.2f}% tapi delta "
            f"CVD melambat {ratio_pct:.0f}% dari paruh pertama — pump hampir habis"
        )
    elif is_momentum:
        score = CONFIG.get("score_cvd_momentum", 8)
        label = (
            f"⚡ CVD Momentum: harga {price_trend*100:+.2f}% + CVD {cvd_trend_norm*100:+.0f}% "
            f"({'15m' if use_15m else '1h'}) — buying pressure nyata terkonfirmasi"
        )
    else:
        score = 0
        label = (
            f"CVD: harga {price_trend*100:+.2f}%, delta CVD {cvd_trend_norm*100:+.0f}% "
            f"({'15m' if use_15m else '1h'}) — tidak ada divergensi"
        )

    return {
        "cvd_now":          round(cvd_now, 2),
        "cvd_start":        round(cvd_start, 2),
        "cvd_trend_norm":   round(cvd_trend_norm, 4),
        "price_trend":      round(price_trend, 5),
        "price_trend_pct":  round(price_trend * 100, 2),
        "is_bullish_div":   is_bullish_div,
        "is_bearish_div":   is_bearish_div,
        "is_momentum":      is_momentum,
        "is_exhaustion":    is_exhaustion,
        "use_15m":          use_15m,
        "score":            score,
        "label":            label,
    }


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


def calc_entry_v19(candles, vwap, price_now, atr_abs_val, market_regime, sr,
                   rsi, buy_ratio, vol_ratio, price_pos, alert_level, bos_level,
                   liq_sweep, cp_sr=None):
    """
    v32 — Entry Engine (rewrite dari v19/v22).

    FIX-v32-E1  SL paradoks: sl_v22 (ATR×0.9) selalu menang karena max().
                Diganti: SL = swing_low − buffer adaptif, clamp min/max pct.
    FIX-v32-E2  t1_source/t2_source salah label (v19 mult vs v22 mult).
    FIX-v32-E3  Entry sadar ChartPrime SR: saat CP Break Res aktif,
                entry = cp_res_level + buffer (retest ke bekas resistance).
    FIX-v32-E4  Regime BREAKOUT override saat cp_brekout_res aktif —
                tidak perlu tunggu above_vwap.
    FIX-v32-E5  Squeeze mode: SL diperluas ke cp_support box bawah,
                bukan hanya swing_low yang mungkin terlalu sempit.
    FIX-v32-E6  TP3 adaptif: saat CP Break Res, TP3 = cp_res × fib 1.618
                atau ATR×8 (ambil yang lebih besar) untuk tangkap pump besar.

    TP multiplier final:
      TP1 = entry + ATR × 1.5   (quick scalp — lebih mudah tercapai)
      TP2 = entry + ATR × 3.0   (swing target)
      TP3 = entry + ATR × 6.0   (extended / pump besar)
      Override jika ada resistance pivot di atas entry.
    """
    atr         = atr_abs_val
    atr_pct_now = (atr / price_now * 100) if price_now > 0 else 2.0

    # --- Deteksi apakah CP Break Res aktif ---
    cp_break_res  = cp_sr.get("brekout_res", False)  if cp_sr else False
    cp_res_level  = cp_sr["resistance"]["level"]       if (cp_sr and cp_sr.get("resistance")) else 0.0
    cp_sup_level  = cp_sr["support"]["level"]          if (cp_sr and cp_sr.get("support"))    else 0.0
    cp_sup_level1 = cp_sr["support"]["level_1"]        if (cp_sr and cp_sr.get("support"))    else 0.0
    squeeze_now   = cp_sr.get("ok", False) and (
        cp_sr.get("sup_holds_count", 0) >= 2 or
        cp_sr.get("sup_holds", False)
    ) if cp_sr else False

    # FIX-v32-E4: override regime ke BREAKOUT jika CP Break Res aktif
    if cp_break_res:
        market_regime = "BREAKOUT"

    # ── STEP 1: Adaptive Entry ────────────────────────────────────────────────
    if market_regime == "BREAKOUT":
        # FIX-v32-E3: entry = bekas resistance (retest) jika CP Break Res
        if cp_break_res and cp_res_level > 0:
            # Retest ke bekas resistance = level terbaik untuk entry
            retest_entry = cp_res_level * (1.0 + CONFIG.get("entry_bos_buffer", 0.0005))
            # Jika harga sudah melewati jauh, pakai harga saat ini
            if retest_entry > price_now * 1.02:
                entry        = price_now * 1.001
                entry_reason = f"BREAKOUT — CP res {_fmt_price(cp_res_level)} (market entry, sudah lewat)"
            else:
                entry        = retest_entry
                entry_reason = f"BREAKOUT — CP retest {_fmt_price(cp_res_level)} + buffer"
        else:
            # Fallback ke resistance pivot biasa
            res_levels = []
            if sr and sr.get("resistance"):
                res_levels = [rv["level"] for rv in sr["resistance"]
                              if rv["level"] > price_now * 0.998]
            if res_levels:
                breakout_lvl = min(res_levels)
                entry        = min(breakout_lvl * 1.005, price_now + atr * 0.25)
                entry_reason = f"BREAKOUT — near R1 {_fmt_price(breakout_lvl)}"
            elif bos_level > 0 and bos_level < price_now * 1.03:
                entry        = bos_level * (1.0 + CONFIG.get("entry_bos_buffer", 0.0005))
                entry_reason = "BREAKOUT — BOS retest"
            else:
                entry        = min(vwap, price_now + atr * 0.25)
                entry_reason = "BREAKOUT — base"

    elif market_regime == "PULLBACK":
        pullback_entry = vwap - atr * CONFIG["entry_pullback_atr_mult"]
        base_entry     = min(vwap, price_now + atr * 0.25)
        entry          = min(base_entry, pullback_entry)
        if entry < price_now * 0.985:
            entry = base_entry
        entry_reason   = f"PULLBACK — min(VWAP, price+ATR×0.25) vs VWAP−0.3ATR"

    elif market_regime == "SWEEP" and liq_sweep and liq_sweep.get("is_sweep"):
        sweep_low    = liq_sweep.get("sweep_low", price_now * 0.98)
        entry        = min(sweep_low + atr * CONFIG["entry_sweep_atr_mult"],
                           price_now + atr * 0.25)
        entry_reason = f"SWEEP — low {_fmt_price(sweep_low)} + 0.2ATR"

    else:  # NEUTRAL
        # Jika ada CP support aktif, entry sedikit di atas support
        if cp_sup_level > 0 and cp_sup_level < price_now:
            raw_entry    = cp_sup_level + atr * 0.3
            # FIX-CP-8: clamp — jika support terlalu jauh (>3%), pakai VWAP basis
            if raw_entry < price_now * 0.97:
                entry        = min(vwap, price_now + atr * 0.1)
                entry_reason = f"NEUTRAL — VWAP (CP sup {_fmt_price(cp_sup_level)} terlalu jauh)"
            else:
                entry        = raw_entry
                entry_reason = f"NEUTRAL — CP sup {_fmt_price(cp_sup_level)} + 0.3ATR"
        else:
            sup_levels = [sv["level"] for sv in sr.get("support", [])
                          if sv["level"] < price_now] if sr else []
            if sup_levels:
                entry        = max(sup_levels) + atr * 0.2
                entry_reason = f"NEUTRAL — S1 {_fmt_price(max(sup_levels))} + 0.2ATR"
            else:
                entry        = min(vwap, price_now + atr * 0.25)
                entry_reason = "NEUTRAL — VWAP basis"

    # Sanity clamp: entry tidak boleh > 5% di atas harga, dan > 0
    if entry <= 0 or entry > price_now * 1.05:
        entry        = price_now * 1.001
        entry_reason += " [clamped]"

    # ── STEP 2: SL — FIX-v32-E1 + E5 ────────────────────────────────────────
    # Logika baru: SL berbasis swing_low dengan buffer adaptif.
    # Tidak ada sl_v22 override yang mematikan swing_low logic.
    lookback_sl = min(20, len(candles) - 1)
    swing_low   = min(c["low"] for c in candles[-lookback_sl:]) if lookback_sl > 0 else entry * 0.95

    # FIX-v32-E5: saat squeeze/CP support aktif, gunakan cp_support_box bawah
    # sebagai anchor SL (lebih logis dari swing_low yang bisa terlalu sempit)
    if squeeze_now and cp_sup_level1 > 0 and cp_sup_level1 < swing_low:
        sl_anchor    = cp_sup_level1
        sl_src       = "CP sup box"
    else:
        sl_anchor    = swing_low
        sl_src       = "swing low"

    # Buffer di bawah anchor: lebih lebar saat volatile, lebih sempit saat squeeze
    if atr_pct_now > 3.0:
        sl_buffer_mult = 0.8     # volatile: buffer 0.8×ATR di bawah anchor
    elif squeeze_now:
        sl_buffer_mult = 0.3     # squeeze: buffer sempit, ATR kecil sudah cukup
    else:
        sl_buffer_mult = 0.5     # normal: 0.5×ATR buffer

    sl = sl_anchor - atr * sl_buffer_mult

    # Clamp: tidak terlalu dekat (min_sl_pct) maupun terlalu jauh (max_sl_pct)
    sl = max(sl, entry * (1.0 - CONFIG["max_sl_pct"] / 100.0))
    sl = min(sl, entry * (1.0 - CONFIG["min_sl_pct"] / 100.0))
    if sl >= entry:
        sl = entry * 0.975

    # ── STEP 3: TP — FIX-v32-E2 + E6 ────────────────────────────────────────
    # Multiplier final yang benar (v32):
    tp1_mult = 1.5   # scalp — lebih mudah tercapai dari 1.3
    tp2_mult = 3.0   # swing
    tp3_mult = 6.0   # extended (dinaikkan dari 5.0 — pump altcoin sering >10%)

    tp1 = entry + atr * tp1_mult
    tp2 = entry + atr * tp2_mult
    tp3 = entry + atr * tp3_mult

    # FIX-v32-E6: TP3 agresif saat CP Break Res (pump besar diharapkan)
    if cp_break_res and cp_res_level > 0:
        # Fibonacci extension dari CP resistance: 1.618× jarak entry ke res
        dist_to_res = abs(cp_res_level - entry)
        tp3_fib     = cp_res_level + dist_to_res * 1.618
        tp3         = max(tp3, tp3_fib)

    # Override TP dengan resistance pivot di atas entry (tetap dipertahankan)
    if sr and sr.get("resistance"):
        res_above = sorted([rv["level"] for rv in sr["resistance"] if rv["level"] > entry])
        if res_above:
            if res_above[0] > tp1:
                tp1 = max(tp1, res_above[0])
            if len(res_above) >= 2:
                if res_above[1] > tp2:
                    tp2 = max(tp2, res_above[1])
                gap = res_above[1] - res_above[0]
                if gap / res_above[0] > 0.05:   # liquidity void > 5%
                    tp3 = max(tp3, res_above[1] * 1.1)

    # Floor checks
    tp1 = max(tp1, entry * 1.005)
    tp2 = max(tp2, tp1   * 1.01)
    tp3 = max(tp3, tp2   * 1.02)

    risk = entry - sl
    rr1  = round((tp1 - entry) / risk, 1) if risk > 0 else 0.0
    rr2  = round((tp2 - entry) / risk, 1) if risk > 0 else 0.0
    rr3  = round((tp3 - entry) / risk, 1) if risk > 0 else 0.0

    sl_pct_val = (entry - sl) / entry * 100 if entry > 0 else 0.0

    # FIX-v32-E2: label sesuai multiplier yang benar
    t1_source = f"ATR×{tp1_mult}"
    t2_source = f"ATR×{tp2_mult}"
    t3_source = f"ATR×{tp3_mult}" + (" + Fib1.618" if cp_break_res else " / Liq Void")
    if sr and sr.get("resistance") and len(sr["resistance"]) > 0:
        res_above_check = [rv["level"] for rv in sr["resistance"] if rv["level"] > entry]
        if res_above_check and min(res_above_check) >= tp1 * 0.99:
            t1_source = f"R1 pivot ({_fmt_price(min(res_above_check))})"

    return {
        "entry":           round(entry, 8),
        "sl":              round(sl, 8),
        "sl_pct":          round(sl_pct_val, 2),
        "t1":              round(tp1, 8),
        "t2":              round(tp2, 8),
        "t3":              round(tp3, 8),
        "rr":              rr1,
        "rr2":             rr2,
        "rr3":             rr3,
        "rr_str":          f"{rr1:.1f}",
        "rr2_str":         f"{rr2:.1f}",
        "rr3_str":         f"{rr3:.1f}",
        "vwap":            round(vwap, 8),
        "bos_level":       round(bos_level, 8),
        "alert_level":     alert_level,
        "gain_t1_pct":     round((tp1 - entry) / entry * 100, 1),
        "gain_t2_pct":     round((tp2 - entry) / entry * 100, 1),
        "gain_t3_pct":     round((tp3 - entry) / entry * 100, 1),
        "atr_abs":         round(atr, 8),
        "atr_pct":         round(atr_pct_now, 2),
        "sl_method":       entry_reason,
        "sl_anchor":       sl_src,
        "market_regime":   market_regime,
        "trail_note":      "v32: TP1→SL=Entry | TP2→SL=TP1 | TP3 bebas",
        "swing_low":       round(swing_low, 8),
        "cp_break_res":    cp_break_res,
        "used_resistance": bool(sr and sr.get("resistance")),
        "t1_source":       t1_source,
        "t2_source":       t2_source,
        "t3_source":       t3_source,
        "atr_pct_abs":     round(atr / entry * 100, 2) if entry > 0 else 0.0,
    }


def calc_accumulation_phase(candles, squeeze_active=False):
    """
    Deteksi fase akumulasi smart money.

    v31 FIX-v31-5: Tambah low-vol accumulation mode.
    v30 mensyaratkan vol > 1.5x WAJIB, padahal akumulasi PIXEL/HUS justru
    vol 0.2-0.5x baseline. Akumulasi sejati = harga ditekan, volume rendah,
    market maker menyerap supply diam-diam.

    Saat squeeze_active=True dan vol sangat rendah (0.08-0.65x):
      - Jika harga sideways + ATR menyempit → is_low_vol_accum = True
      - Mendapat skor sama dengan akumulasi normal

    FIX v18: is_vol_compress hanya aktif secara scoring jika is_accumulating=False
    (isolasi scoring di master_score).
    """
    if len(candles) < 36:
        return {
            "is_accumulating": False, "is_vol_compress": False,
            "is_low_vol_accum": False,
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

    # v31 FIX-v31-5: Low-vol accumulation mode
    lv_min = CONFIG.get("accum_low_vol_min_ratio", 0.08)
    lv_max = CONFIG.get("accum_low_vol_max_ratio", 0.65)
    is_low_vol_accum = (
        squeeze_active
        and (lv_min <= vol_ratio_4h <= lv_max)
        and price_sideways
        and atr_shrinking
        and price_in_zone
    )

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
    elif is_low_vol_accum:
        label = (
            f"🕵️ AKUMULASI TERSEMBUNYI — vol {vol_ratio_4h:.2f}x (rendah), "
            f"squeeze aktif, range {price_range_pct:.1f}% — pre-pump kuat"
        )
    elif is_vol_compress:
        label = f"🗜️ VOLATILITY COMPRESSION — ATR {atr_contract:.2f}x dari baseline"
    else:
        label = "—"

    return {
        "is_accumulating":  is_accumulating or is_low_vol_accum,
        "is_vol_compress":  is_vol_compress,
        "is_low_vol_accum": is_low_vol_accum,
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

    FIX v29 BUG-A: Tambah expiry check.
    Sweep hanya dianggap valid dalam CONFIG["liq_sweep_expiry_candles"] candle
    terakhir (default: 6 jam). Sweep yang terjadi 17 jam lalu tidak boleh
    masih mengaktifkan sinyal dan skip VWAP penalty.
    """
    if lookback is None:
        lookback = CONFIG["liq_sweep_lookback"]
    if len(candles) < lookback + 3:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0,
                "label": "Data kurang", "candles_ago": 99}

    reference_candles = candles[-(lookback + 3):-3]
    if not reference_candles:
        return {"is_sweep": False, "sweep_low": 0.0, "support": 0.0,
                "label": "—", "candles_ago": 99}

    lows_sorted   = sorted(c["low"] for c in reference_candles)
    support_level = sum(lows_sorted[:3]) / 3

    sweep_detected  = False
    sweep_candle    = None
    sweep_low_val   = 0.0
    sweep_candles_ago = 99  # berapa candle lalu sweep terjadi

    # FIX v29 BUG-A: Perluas pencarian ke expiry window (bukan hanya 3 candle)
    # agar kita tahu KAPAN sweep terjadi dan bisa menentukan apakah masih valid.
    expiry = CONFIG.get("liq_sweep_expiry_candles", 6)
    search_window = min(expiry, len(candles))

    for idx, candle in enumerate(reversed(candles[-search_window:])):
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
            sweep_detected    = True
            sweep_candle      = candle
            sweep_low_val     = candle["low"]
            sweep_candles_ago = idx + 1  # 1 = candle terakhir, 6 = 6 jam lalu
            break

    # FIX v29 BUG-A: Jika sweep ditemukan tapi sudah lebih dari expiry candle,
    # tandai sebagai expired (is_sweep=False agar tidak skip VWAP, tidak naik skor).
    is_valid = sweep_detected and sweep_candles_ago <= expiry

    if is_valid and sweep_candle is not None:
        depth_pct = (support_level - sweep_low_val) / support_level * 100
        label = (
            f"🎯 Liquidity Sweep — low ${sweep_low_val:.6g} tembus support "
            f"${support_level:.6g} ({depth_pct:.2f}%), close kembali di atas"
            f" [{sweep_candles_ago}h lalu]"
        )
    elif sweep_detected and not is_valid:
        depth_pct = (support_level - sweep_low_val) / support_level * 100
        label = (
            f"⏰ Sweep EXPIRED ({sweep_candles_ago}h lalu > {expiry}h max) — "
            f"low ${sweep_low_val:.6g} support ${support_level:.6g} "
            f"({depth_pct:.2f}%) — tidak lagi valid sebagai sinyal"
        )
    else:
        label = "—"

    return {
        "is_sweep":      is_valid,
        "sweep_low":     round(sweep_low_val, 8),
        "support":       round(support_level, 8),
        "label":         label,
        "candles_ago":   sweep_candles_ago,
        "is_expired":    sweep_detected and not is_valid,
    }

def detect_energy_buildup(candles_1h, oi_data):
    """
    Energy Build-Up Detector — "OI Build + Volume Bullish + Price Stuck".

    Pola absorption: market maker menyerap order sambil membangun posisi.
    Harga DITAHAN (sideways) meski volume dan OI naik = klasik pre-pump.

    FIX v29 BUG-D: Window diperluas dari 3 candle (3 jam) ke 8 candle (8 jam).
    Akumulasi real berlangsung 8-18 jam. Dengan window 3h:
      - Fase akumulasi MELANIA (15 jam) tidak pernah terdeteksi
      - Energy buildup hanya aktif tepat sebelum pump (terlambat)
    Dengan window 8h, pola absorption dapat terdeteksi 6-12 jam sebelum pump.

    Kondisi deteksi (v29):
      1. OI naik > 5% (posisi baru dibangun)
      2. Vol rata-rata 8h terbaru > 1.5x baseline 24h
      3. Price range 8h < 2.5% (harga tidak bergerak)
      4. Minimal 5 dari 8 candle terbaru bullish (buying pressure konsisten)
    """
    window = CONFIG.get("energy_buildup_window", 8)
    bullish_min = CONFIG.get("energy_buildup_bullish_min", 5)

    if len(candles_1h) < window + 4:
        return {
            "is_buildup": False, "is_strong": False,
            "oi_change": 0.0, "vol_ratio": 0.0, "range_pct": 0.0,
            "label": "Data tidak cukup",
        }

    # Kondisi 1: OI naik
    oi_change = oi_data.get("change_pct", 0.0)
    oi_rising = (not oi_data.get("is_new", True)) and oi_change >= CONFIG["energy_oi_change_min"]

    # Kondisi 2: volume rata-rata 8 candle terbaru vs baseline 24h
    recent_n   = candles_1h[-window:]
    vol_n_avg  = sum(c["volume_usd"] for c in recent_n) / window
    baseline   = candles_1h[-(window + 24):-window]
    avg_vol    = sum(c["volume_usd"] for c in baseline) / len(baseline) if baseline else vol_n_avg
    vol_ratio  = (vol_n_avg / avg_vol) if avg_vol > 0 else 1.0
    vol_rising = vol_ratio >= CONFIG["energy_vol_ratio_min"]

    # Kondisi 3: harga tidak bergerak dalam 8h
    hi_n       = max(c["high"]  for c in recent_n)
    lo_n       = min(c["low"]   for c in recent_n)
    mid_n      = (hi_n + lo_n) / 2
    range_pct  = ((hi_n - lo_n) / mid_n * 100) if mid_n > 0 else 99.0
    price_stuck = range_pct <= CONFIG["energy_range_max_pct"]

    # Kondisi 4: mayoritas candle terbaru bullish (5 dari 8)
    bullish_count   = sum(1 for c in recent_n if c["close"] >= c["open"])
    candles_bullish = bullish_count >= bullish_min

    is_buildup = oi_rising and vol_rising and price_stuck and candles_bullish
    is_strong  = False   # akan di-set dari master_score jika funding <= 0

    if is_buildup:
        label = (
            f"⚡ ENERGY BUILD-UP ({window}h) — OI +{oi_change:.1f}%, "
            f"vol {vol_ratio:.1f}x baseline, range {range_pct:.1f}%, "
            f"candle {bullish_count}/{window} hijau"
        )
    else:
        conds = sum([oi_rising, vol_rising, price_stuck, candles_bullish])
        label = (
            f"— ({conds}/4 kondisi: OI={oi_rising}, vol={vol_rising}, "
            f"stuck={price_stuck}, bullish={candles_bullish} [{bullish_count}/{window}])"
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

def calc_uptrend_age(candles, squeeze_active=False):
    """
    Berapa jam harga naik berturut-turut. Pre-pump ideal = streak pendek atau 0.

    v31 FIX-v31-4: Bedakan akumulasi dari late uptrend.
    PIXEL/HUS punya higher low structure → candle hijau kecil terus → streak 20-30+.
    Di v30, ini menyebabkan is_late=True → GATE 3 gagal meski itu bukan distribusi.

    Fix: Jika squeeze_active=True → is_late selalu False.
    Logika: akumulasi dalam squeeze dengan higher low bukan "late uptrend" —
    itu adalah momen terbaik sebelum pump. Gate ini tidak relevan di kondisi squeeze.
    """
    if len(candles) < 4:
        return {"age_hours": 0, "is_fresh": False, "is_late": False}
    streak = 0
    for i in range(len(candles) - 1, 0, -1):
        if candles[i]["close"] > candles[i-1]["close"]:
            streak += 1
        else:
            break
    # FIX v31: is_late hanya True jika streak > 24 DAN tidak dalam squeeze mode.
    # Akumulasi panjang dengan higher low (streak tinggi) ≠ late uptrend.
    is_late = (streak > CONFIG["gate_uptrend_max_hours"]) and (not squeeze_active)
    return {
        "age_hours": streak,
        "is_fresh":  1 <= streak <= 8,
        "is_late":   is_late,
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


# ══════════════════════════════════════════════════════════════════════════════
#  🆕  v32: ChartPrime SR — Volume-Weighted Pivot S/R
# ══════════════════════════════════════════════════════════════════════════════
def _delta_volume(candle):
    """
    Replika upAndDownVolume() dari ChartPrime Pine Script.

    v32b FIX-7: Upgrade dari binary (close>open → +vol) ke wick-weighted fraction.
    Alasan: candle doji (open≈close) dan candle dengan wick panjang menghasilkan
    delta_vol=0 atau salah arah dengan metode binary.

    Formula: buy_fraction = (close - low) / (high - low)
    → close di atas mid range → lebih banyak buy pressure
    → close di bawah mid range → lebih banyak sell pressure
    → delta_vol = (2*buy_fraction - 1) * volume_usd
    → Range: [-volume_usd, +volume_usd]

    Fallback ke binary jika range=0 (candle doji sempurna).
    """
    rng = candle["high"] - candle["low"]
    if rng > 0:
        buy_frac  = (candle["close"] - candle["low"]) / rng
        return (2.0 * buy_frac - 1.0) * candle["volume_usd"]
    # Doji sempurna: fallback ke binary
    if candle["close"] > candle["open"]:
        return candle["volume_usd"]
    elif candle["close"] < candle["open"]:
        return -candle["volume_usd"]
    return 0.0


def calc_chartprime_sr(candles):
    """
    v32b — ChartPrime SR Breaks and Retests (perbaikan dari v32).

    FIX-CP-1  PIVOT: strict left/right terpisah (Pine: pivothigh/pivotlow)
              Sebelumnya: ph == max(window) → miss jika ada high sama
              Sekarang:   ph > max(kiri) DAN ph > max(kanan) — strict

    FIX-CP-2  vol_len default dinaikkan 2→4 agar lebih stabil terhadap
              outlier delta_vol dari candle pump sebelumnya.

    FIX-CP-3  SORT BUG: active_res diurutkan ascending abs(delta_vol)
              → pilih resistance TERLEMAH. Fix: descending (terkuat dulu).
              Ditambah: kembalikan top-3 levels, bukan hanya best_1.

    FIX-CP-4  LOOKBACK RIGHT: candle terbaru tidak bisa pivot karena
              butuh konfirmasi lb candle di kanan. Untuk deteksi Break Res
              pada candle terbaru, tambahkan cek "partial pivot" (3c kanan).

    FIX-CP-5  FILTER active_sup: tambahkan batas atas proximity.
              Support valid hanya jika price_now dalam 15% di atas level.

    FIX-CP-6  BREKOUT_RES window diperluas: valid jika terjadi dalam
              N candle terakhir (bukan hanya 1 transisi candle).
    """
    lb  = CONFIG.get("cp_lookback", 20)
    vl  = CONFIG.get("cp_vol_len",   4)    # FIX-CP-2: 2 → 4

    if len(candles) < lb * 2 + vl + 5:
        return _cp_sr_empty()

    dv_series = [_delta_volume(c) for c in candles]

    # FIX-CP-7: ATR 20 untuk box width — lebih responsif dari ATR 200
    # ATR 200 saat squeeze masih tinggi karena mencakup periode pre-squeeze
    atr_val   = _atr_n(candles, min(20, len(candles) - 1))
    withd     = atr_val * CONFIG.get("cp_atr_mult", 1.0)

    support_levels    = []
    resistance_levels = []
    n = len(candles)

    for i in range(lb, n - lb):
        ph = candles[i]["high"]
        pl = candles[i]["low"]

        # FIX-CP-1: strict pivot — kiri DAN kanan terpisah
        left_highs  = [c["high"] for c in candles[i - lb: i]]
        right_highs = [c["high"] for c in candles[i + 1: i + lb + 1]]
        left_lows   = [c["low"]  for c in candles[i - lb: i]]
        right_lows  = [c["low"]  for c in candles[i + 1: i + lb + 1]]

        is_pivot_high = (left_highs and right_highs and
                         ph > max(left_highs) and ph > max(right_highs))
        is_pivot_low  = (left_lows and right_lows and
                         pl < min(left_lows) and pl < min(right_lows))

        dv = dv_series[i]

        # vol_hi / vol_lo dari window vl candle (FIX-CP-2: vl=4)
        dv_slice = [dv_series[j] / 2.5 for j in range(max(0, i - vl + 1), i + 1)]
        if not dv_slice:
            continue
        vol_hi = max(dv_slice)
        vol_lo = min(dv_slice)

        if is_pivot_low and dv > vol_hi:
            support_levels.append({
                "level":     pl,
                "level_1":   pl - withd,
                "delta_vol": dv,
                "bar_idx":   i,
            })

        if is_pivot_high and dv < vol_lo:
            resistance_levels.append({
                "level":     ph,
                "level_1":   ph + withd,
                "delta_vol": dv,
                "bar_idx":   i,
            })

    # FIX-CP-4: partial pivot untuk lb candle terbaru (belum ada konfirmasi kanan)
    # Cek 3c kanan saja (lebih responsif untuk sinyal baru)
    partial_lb = 3
    for i in range(n - lb, n - partial_lb):
        ph = candles[i]["high"]
        pl = candles[i]["low"]
        left_highs  = [c["high"] for c in candles[max(0, i - lb): i]]
        right_highs = [c["high"] for c in candles[i + 1: min(n, i + partial_lb + 1)]]
        left_lows   = [c["low"]  for c in candles[max(0, i - lb): i]]
        right_lows  = [c["low"]  for c in candles[i + 1: min(n, i + partial_lb + 1)]]

        is_partial_high = (left_highs and right_highs and
                           ph > max(left_highs) and ph > max(right_highs))
        is_partial_low  = (left_lows and right_lows and
                           pl < min(left_lows) and pl < min(right_lows))

        dv = dv_series[i]
        dv_slice = [dv_series[j] / 2.5 for j in range(max(0, i - vl + 1), i + 1)]
        if not dv_slice:
            continue
        vol_hi = max(dv_slice)
        vol_lo = min(dv_slice)

        if is_partial_low and dv > vol_hi:
            support_levels.append({
                "level":     pl,
                "level_1":   pl - withd,
                "delta_vol": dv,
                "bar_idx":   i,
                "partial":   True,
            })
        if is_partial_high and dv < vol_lo:
            resistance_levels.append({
                "level":     ph,
                "level_1":   ph + withd,
                "delta_vol": dv,
                "bar_idx":   i,
                "partial":   True,
            })

    if not support_levels and not resistance_levels:
        return _cp_sr_empty()

    price_now = candles[-1]["close"]
    high_now  = candles[-1]["high"]
    low_now   = candles[-1]["low"]

    # FIX-CP-5: active_sup proximity ≤ 15% di atas level (bukan tak terbatas)
    prox_sup_max = 0.15
    active_sup = [
        s for s in support_levels
        if s["level_1"] <= price_now <= s["level"] * (1 + prox_sup_max)
    ]
    # FIX-CP-3: active_res — resistance di atas harga, sort DESCENDING abs(delta_vol)
    active_res = [r for r in resistance_levels if price_now <= r["level_1"]]

    # Sort support: terbesar delta_vol dulu (terkuat), tie-break: terdekat ke harga
    if active_sup:
        active_sup.sort(key=lambda s: (
            -abs(s["delta_vol"]),
            abs(price_now - s["level"]) / max(price_now, 1e-10)
        ))
        best_sup = active_sup[0]
        # Simpan top-3 untuk multi-level awareness
        top_sup  = active_sup[:3]
    else:
        best_sup = None
        top_sup  = []

    # FIX-CP-3: sort DESCENDING abs(delta_vol) → pilih resistance TERKUAT
    if active_res:
        active_res.sort(key=lambda r: (
            -abs(r["delta_vol"]),                          # DESCENDING (terbesar abs dulu)
            abs(price_now - r["level"]) / max(price_now, 1e-10)
        ))
        best_res = active_res[0]
        top_res  = active_res[:3]
    else:
        best_res = None
        top_res  = []

    # FIX-CP-6: brekout_res diperluas — valid dalam N_BREAK candle terakhir
    N_BREAK     = CONFIG.get("cp_brekout_window", 3)   # default 3 candle
    brekout_res = False
    res_holds   = False
    if best_res:
        res_lv  = best_res["level"]
        res_lv1 = best_res["level_1"]
        # Cek apakah ada transisi crossover dalam N_BREAK candle terakhir
        for k in range(1, min(N_BREAK + 1, len(candles))):
            c_cur  = candles[-k]
            c_prev = candles[-k - 1] if k + 1 <= len(candles) else candles[-k]
            if c_cur["low"] > res_lv1 and c_prev["low"] <= res_lv1:
                brekout_res = True
                break
        # res_holds: high candle saat ini < res_lv setelah sebelumnya menyentuh
        if len(candles) >= 2:
            res_holds = (high_now < res_lv) and (candles[-2]["high"] >= res_lv)

    sup_holds   = False
    brekout_sup = False
    if best_sup:
        sup_lv  = best_sup["level"]
        sup_lv1 = best_sup["level_1"]
        if len(candles) >= 2:
            prev_low  = candles[-2]["low"]
            prev_high = candles[-2]["high"]
            sup_holds   = (low_now  > sup_lv)  and (prev_low  <= sup_lv)
            brekout_sup = (high_now < sup_lv1) and (prev_high >= sup_lv1)

    prox_pct     = CONFIG.get("cp_proximity_pct", 0.02)
    near_res     = False
    near_res_pct = 0.0
    if best_res and best_res["level"] > 0:
        gap          = (best_res["level"] - price_now) / best_res["level"]
        near_res_pct = gap
        near_res     = 0 < gap < prox_pct

    sup_holds_count = _count_sup_holds_streak(
        candles,
        best_sup["level"]   if best_sup else 0.0,
        best_sup["level_1"] if best_sup else 0.0,
        lookback=CONFIG.get("cp_sup_holds_lookback", 20),
    )

    return {
        "ok":               True,
        "support":          best_sup,
        "resistance":       best_res,
        "top_sup":          top_sup,          # FIX-CP-3: top-3 support
        "top_res":          top_res,          # FIX-CP-3: top-3 resistance
        "brekout_res":      brekout_res,
        "res_holds":        res_holds,
        "sup_holds":        sup_holds,
        "brekout_sup":      brekout_sup,
        "near_res":         near_res,
        "near_res_pct":     round(near_res_pct * 100, 2),
        "sup_holds_count":  sup_holds_count,
        "n_support":        len(support_levels),
        "n_resistance":     len(resistance_levels),
        "price_now":        price_now,
        "withd":            withd,
    }


def _cp_sr_empty():
    """Return kosong jika ChartPrime SR tidak bisa dihitung."""
    return {
        "ok": False, "support": None, "resistance": None,
        "top_sup": [], "top_res": [],
        "brekout_res": False, "res_holds": False,
        "sup_holds": False, "brekout_sup": False,
        "near_res": False, "near_res_pct": 0.0,
        "sup_holds_count": 0, "n_support": 0, "n_resistance": 0,
        "price_now": 0.0, "withd": 0.0,
    }


def _count_sup_holds_streak(candles, sup_level, sup_level_1, lookback=20):
    """
    Hitung berapa kali sup_holds terjadi dalam lookback candle terakhir.
    sup_holds = low[i] > sup_level DAN low[i-1] <= sup_level
    (price bounce dari support = green ◆ di ChartPrime)
    """
    if sup_level <= 0 or len(candles) < 2:
        return 0
    count = 0
    n     = min(lookback, len(candles) - 1)
    window = candles[-n - 1:]
    for i in range(1, len(window)):
        curr_low = window[i]["low"]
        prev_low = window[i - 1]["low"]
        if curr_low > sup_level and prev_low <= sup_level:
            count += 1
    return count


def score_chartprime_sr(cp_sr):
    """
    v32 — Scoring berdasarkan hasil calc_chartprime_sr().

    Skala prioritas (dari forensik 4 pump):
    1. brekout_res (+20)  — "Break Res" label = trigger pump terkuat
    2. sup_holds 7x+ (+14) — akumulasi sangat kuat (PIXEL/ORCA pattern)
    3. sup_holds 4-6x (+8) — akumulasi kuat
    4. near_res (+6)       — harga dekat resistance, siap break
    5. sup_holds 1-3x (+4) — buyer defensif awal
    6. res_holds (-6)      — resistance masih menahan, belum siap
    7. brekout_sup (-15)   — support jebol, trend balik

    Return: (score_delta, signals_list, cp_label)
    """
    if not cp_sr or not cp_sr.get("ok"):
        return 0, [], "ChartPrime SR: data tidak cukup"

    score   = 0
    signals = []
    label   = ""
    sup     = cp_sr.get("support")
    res     = cp_sr.get("resistance")
    holds_n = cp_sr.get("sup_holds_count", 0)

    # --- Break Res: sinyal paling prediktif ---
    if cp_sr.get("brekout_res"):
        pts = CONFIG.get("score_cp_break_res", 20)
        score += pts
        res_lv = res["level"] if res else 0.0
        dv     = res["delta_vol"] if res else 0.0
        signals.append(
            f"🚀 ChartPrime BREAK RES! Resistance {_fmt_price(res_lv)} "
            f"ditembus (Vol: {dv/1e3:.0f}K) — SINYAL PUMP FASE 2 (+{pts})"
        )
        label = "BREAK_RES"

    # --- Break Sup: support patah, bahaya ---
    elif cp_sr.get("brekout_sup"):
        pts = CONFIG.get("score_cp_break_sup", -15)
        score += pts
        sup_lv = sup["level"] if sup else 0.0
        signals.append(
            f"💥 ChartPrime BREAK SUP! Support {_fmt_price(sup_lv)} jebol "
            f"— trend balik bearish ({pts})"
        )
        label = "BREAK_SUP"

    else:
        # --- Sup Holds streak: akumulasi di support ---
        if holds_n >= 7:
            pts = CONFIG.get("score_cp_sup_holds_3", 14)
            score += pts
            signals.append(
                f"🟢 ChartPrime SUP HOLDS {holds_n}x — akumulasi SANGAT KUAT "
                f"(buyer defensif berulang) (+{pts})"
            )
            label = f"SUP_HOLDS_{holds_n}x"
        elif holds_n >= 4:
            pts = CONFIG.get("score_cp_sup_holds_2", 8)
            score += pts
            signals.append(
                f"🟢 ChartPrime SUP HOLDS {holds_n}x — akumulasi kuat (+{pts})"
            )
            label = f"SUP_HOLDS_{holds_n}x"
        elif holds_n >= 1:
            pts = CONFIG.get("score_cp_sup_holds_1", 4)
            score += pts
            signals.append(
                f"🟢 ChartPrime SUP HOLDS {holds_n}x — buyer defensif awal (+{pts})"
            )
            label = f"SUP_HOLDS_{holds_n}x"

        # --- Proximity bonus: dekat resistance ---
        if cp_sr.get("near_res"):
            pts = CONFIG.get("score_cp_proximity", 6)
            score += pts
            signals.append(
                f"📍 ChartPrime: Harga {cp_sr['near_res_pct']:.2f}% dari resistance "
                f"— siap break (+{pts})"
            )
            if not label:
                label = "NEAR_RES"

        # --- Res Holds: resistance masih kuat ---
        if cp_sr.get("res_holds"):
            pts = CONFIG.get("score_cp_res_holds", -6)
            score += pts
            signals.append(
                f"🔴 ChartPrime RES HOLDS — resistance masih menahan ({pts})"
            )
            if not label:
                label = "RES_HOLDS"

        # --- Sup Holds candle saat ini ---
        if cp_sr.get("sup_holds") and holds_n == 0:
            # Baru pertama kali bounce (belum masuk streak counter)
            pts = CONFIG.get("score_cp_sup_holds_1", 4)
            score += pts
            signals.append(
                f"🟢 ChartPrime SUP HOLDS (baru) — bounce dari support (+{pts})"
            )
            if not label:
                label = "SUP_HOLDS_NEW"

    if not label:
        # Informasi saja tanpa sinyal aktif
        if sup:
            signals.append(
                f"📊 ChartPrime SR: sup {_fmt_price(sup['level'])} "
                f"| res {_fmt_price(res['level']) if res else 'N/A'}"
            )
        label = "NEUTRAL"

    return score, signals, label

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

    # ── v31 FIX-v31-1: SQUEEZE MODE DETECTION (harus sebelum semua gate) ────
    # Deteksi squeeze aktif dari candle 1h nyata (BBW percentile ≤ 30).
    # Squeeze mode = coin dalam akumulasi panjang sebelum pump besar.
    # Saat aktif: gate OI, uptrend age, 5m momentum, ATR minimum di-bypass.
    # Ini adalah perbaikan root cause utama v31.
    _bbw_pct, _bbw_val, squeeze_active = calc_bbw_percentile(c1h)
    log.debug(
        f"  {symbol}: BBW percentile={_bbw_pct:.1f}% val={_bbw_val:.5f} "
        f"squeeze_mode={'ON' if squeeze_active else 'off'}"
    )

    # ── GATE 0: Open Interest minimum ────────────────────────────────────────
    # FIX v31: skip gate ini jika squeeze aktif — akumulasi wajar punya OI kecil
    oi_data = get_oi_change(symbol)
    if oi_data["oi_now"] > 0 and oi_data["oi_now"] < CONFIG["min_oi_usd"]:
        if squeeze_active:
            log.info(
                f"  {symbol}: OI ${oi_data['oi_now']:,.0f} < ${CONFIG['min_oi_usd']:,} "
                f"— GATE 0 di-SKIP (squeeze mode aktif, akumulasi wajar OI kecil)"
            )
        else:
            log.info(
                f"  {symbol}: OI ${oi_data['oi_now']:,.0f} < ${CONFIG['min_oi_usd']:,} "
                f"— GATE GAGAL (coin illiquid)"
            )
            return None

    # v27: OI Acceleration — update snapshot histori segera setelah OI didapat
    oi_accel_v27 = calc_oi_acceleration_v27(symbol, oi_data["oi_now"])
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
    bos_up, bos_level, pre_close_bos, horiz_break = detect_bos_up(c1h)
    higher_low       = higher_low_detected(c1h)
    vol_ratio        = calc_volume_ratio(c1h)
    vol_accel        = calc_volume_acceleration(c1h)
    vol_consistent   = check_volume_consistent(c1h)
    uptrend          = calc_uptrend_age(c1h, squeeze_active=squeeze_active)
    sr               = calc_support_resistance(c1h)
    cp_sr            = calc_chartprime_sr(c1h)      # v32: ChartPrime volume-weighted SR
    sup_health       = calc_support_health(         # v32b: support break prevention
        c1h, cp_sr, None, oi_data               # vol_zscore passed later via scoring
    )
    btc_candles      = get_btc_candles_cached(48)
    btc_corr         = calc_btc_correlation(c1h, btc_candles, lookback=24)
    accum            = calc_accumulation_phase(c1h, squeeze_active=squeeze_active)
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
    whale_accum    = detect_whale_accumulation(c1h, squeeze_active=squeeze_active)  # STEP 16: whale accumulation
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
    smart_money_v22  = detect_smart_money_accumulation_v22(c1h, squeeze_active=squeeze_active)
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

    # ── NEW v27 indicators ────────────────────────────────────────────────────
    # Orderbook Liquidity Vacuum (real API data — bukan proxy candle)
    ob_vacuum_v27  = detect_liquidity_vacuum_v27(symbol, c1h[-1]["close"], vol_zscore_v20["z"])
    # CVD Divergence (membedakan akumulasi vs distribusi dengan akurat)
    cvd_div_v27    = calc_cvd_divergence_v27(c1h, c15m)

    # PART 4: Weighted scoring bonus (computed later in scoring section)


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
    # FIX v31: calc_uptrend_age sudah menerima squeeze_active — is_late=False
    # saat squeeze aktif. Gate ini hanya efektif untuk non-squeeze coin.
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
    # FIX v31: di-bypass saat squeeze aktif.
    # Di area kotak hijau, harga 5m masih flat/sedikit negatif karena candle
    # breakout sedang terbentuk. Gate ini tidak relevan untuk pre-pump squeeze.
    if c5m and len(c5m) >= 2:
        price_chg_5m = (c5m[-1]["close"] - c5m[-2]["close"]) / c5m[-2]["close"]
        if price_chg_5m <= CONFIG["momentum_val_reject"]:
            if squeeze_active:
                log.info(
                    f"  {symbol}: 5m price change {price_chg_5m*100:+.3f}% — "
                    f"GATE 9 di-SKIP (squeeze mode aktif, flat 5m wajar saat akumulasi)"
                )
            else:
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
        _thr     = CONFIG["noise_min_vol_24h"]
        _thr_str = f"${_thr/1e6:.1f}M" if _thr >= 1_000_000 else f"${_thr/1e3:.0f}K"
        _vol_str = f"${vol_24h/1e6:.1f}M" if vol_24h >= 1_000_000 else f"${vol_24h/1e3:.0f}K"
        log.info(
            f"  {symbol}: Vol 24h {_vol_str} < {_thr_str} "
            f"— coin terlalu illiquid (GATE GAGAL)"
        )
        return None
    # FIX v31: ATR minimum di-bypass saat squeeze aktif.
    # Squeeze = ATR sangat kecil. Ini BUKAN noise, ini kompresi volatilitas sebelum pump.
    # PIXEL saat akumulasi: harga $0.005, candle range $0.00001 → ATR < 0.3%.
    if atr_pct < CONFIG["noise_min_atr_pct"]:
        if squeeze_active:
            log.info(
                f"  {symbol}: ATR {atr_pct:.3f}% < {CONFIG['noise_min_atr_pct']}% "
                f"— GATE 10 ATR di-SKIP (squeeze mode aktif, ATR kecil = kompresi normal)"
            )
        else:
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

    # FIX v28 BUG#10: Skip VWAP penalty jika liquidity sweep aktif.
    # Logika: sweep (stop-hunt) biasanya mendorong harga SEMENTARA di bawah VWAP
    # sebelum pump besar dimulai. Ini adalah pre-pump terkuat — menambah penalti
    # VWAP di sini akan mendiskualifikasi setup terbaik yang ada.
    _liq_sweep_active = liq_sweep.get("is_sweep", False) or liq_sweep_v23.get("is_sweep", False)
    if _vwap_penalty < 0:
        if _liq_sweep_active:
            signals.append(
                f"ℹ️ Below VWAP tapi Liquidity Sweep AKTIF — "
                f"penalti VWAP {_vwap_penalty} di-SKIP (sweep biasanya pre-pump)"
            )
        else:
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

    # ── 0a+. FIX v30 FIX-6 / v31: Vol Compression dalam Squeeze = akumulasi ──
    # v31: _squeeze_active kini menggunakan BBW percentile nyata dari candle
    # (calc_bbw_percentile dipanggil di awal master_score) — lebih akurat dari
    # pendekatan v30 yang menggabungkan beberapa proxy tidak konsisten.
    _squeeze_active = (
        squeeze_active or   # v31: dari BBW percentile candle nyata (utama)
        vol_comp_v23.get("is_compressed") or
        prebreakout_v22.get("is_compressed") or
        bbw < CONFIG.get("bb_squeeze_threshold", 0.05)
    )
    _vol_compression_ratio = vol_spike.get("ratio", 1.0)
    _vcmin = CONFIG.get("vol_compression_min_ratio", 0.10)
    _vcmax = CONFIG.get("vol_compression_max_ratio", 0.85)
    if (_squeeze_active and
            vol_spike["tier"] == 0 and
            _vcmin <= _vol_compression_ratio <= _vcmax):
        _vc_bonus = CONFIG.get("score_vol_compression_squeeze", 10)
        score += _vc_bonus
        signals.append(
            f"🗜️ Vol Compression dalam Squeeze {_vol_compression_ratio:.2f}x — "
            f"akumulasi tersembunyi (vol rendah + squeeze = pre-pump kuat) (+{_vc_bonus})"
        )

    # ── 0a+++. v31: Low-vol accumulation bonus ───────────────────────────────
    # FIX-v31-5: calc_accumulation_phase kini mendeteksi akumulasi low-volume.
    # Jika is_low_vol_accum aktif, beri bonus tambahan karena ini kondisi langka
    # dan sangat prediktif (pola PIXEL/HUS sebelum pump besar).
    if accum.get("is_low_vol_accum"):
        _lva_bonus = CONFIG.get("score_low_vol_accum", 8)
        score += _lva_bonus
        signals.append(
            f"🕵️ Low-Vol Accumulation aktif — vol {accum['vol_ratio_4h']:.2f}x "
            f"dalam squeeze (akumulasi tersembunyi paling prediktif) (+{_lva_bonus})"
        )

    # ── 0a++. FIX v23: BB Breakout bonus (replaces hard reject from GATE 5) ──
    if _bb_breakout_bonus > 0:
        score += _bb_breakout_bonus
        signals.append(f"📈 BB Breakout: price above upper band — confirmed breakout (+{_bb_breakout_bonus})")

    # ── 0b. Buy Pressure (Phase 2) ────────────────────────────────────────────
    # FIX v30 FIX-7: Saat squeeze aktif, jangan turunkan tier buy_press hanya
    # karena harga tidak naik. Dalam akumulasi, buy_ratio tinggi + vol rendah
    # adalah kondisi VALID — ini pola absorption, bukan retracement.
    # Override: jika squeeze aktif → pakai tier asli dari buy_ratio saja,
    # abaikan pengecekan price_rising yang diperkenalkan di BUG-B fix v29.
    if _squeeze_active and "tier" in buy_press:
        # Re-evaluate tier murni dari buy_ratio tanpa price_rising dampak
        _br = buy_press["buy_ratio"]
        if _br >= CONFIG["buy_pressure_high"] and buy_press["tier"] < 3:
            buy_press = dict(buy_press)
            buy_press["tier"]  = 3
            buy_press["score"] = CONFIG["score_buy_pressure_high"]
            buy_press["label"] = (
                f"🐳 Buy Pressure {_br*100:.0f}% — PUMP PHASE "
                f"[squeeze override, buy valid meski harga flat]"
            )
        elif _br >= CONFIG["buy_pressure_mid"] and buy_press["tier"] < 2:
            buy_press = dict(buy_press)
            buy_press["tier"]  = 2
            buy_press["score"] = CONFIG["score_buy_pressure_mid"]
            buy_press["label"] = (
                f"💰 Buy Pressure {_br*100:.0f}% — Whale activity "
                f"[squeeze override]"
            )

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
            # FIX v28 BUG#7: Jika OI baseline baru (is_new=True) tapi nilainya
            # substansial (>2x min_oi), beri partial bonus +1.
            # Ini menandakan coin aktif diperdagangkan di futures.
            _oi_sub = oi_data["oi_now"] > CONFIG["min_oi_usd"] * 2
            if _oi_sub:
                score += 1
                signals.append(
                    f"📊 OI baseline ${oi_data['oi_now']/1e6:.2f}M (snapshot pertama) "
                    f"— substansial, coin aktif di futures (+1)"
                )
            else:
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

    # ── 10. RSI — FIX v28 BUG#5: multi-tier scoring ─────────────────────────
    # Pump 30-70% sering mulai dari RSI 55-72, bukan hanya 40-60.
    # Zone diperluas: 45-60 optimal (+4), 60-72 valid (+2), 35-45 reversal (+1)
    rsi_in_ideal_zone = 45.0 <= rsi <= 60.0
    rsi_in_valid_zone = 60.0 < rsi <= CONFIG.get("rsi_valid_max", 72.0)
    rsi_in_oversold   = 35.0 <= rsi < 45.0

    if rsi_in_ideal_zone:
        score += CONFIG["score_rsi_ideal"]
        signals.append(
            f"📊 RSI {rsi:.1f} — zona OPTIMAL pre-pump (45–60): "
            f"momentum mulai, belum overbought (+{CONFIG['score_rsi_ideal']})"
        )
    elif rsi_in_valid_zone:
        score += CONFIG.get("score_rsi_valid", 2)
        signals.append(
            f"📊 RSI {rsi:.1f} — zona valid pre-pump (60–72): "
            f"pump sedang berjalan, masih bisa entry (+{CONFIG.get('score_rsi_valid', 2)})"
        )
    elif rsi_in_oversold:
        score += CONFIG.get("score_rsi_oversold", 1)
        signals.append(
            f"📊 RSI {rsi:.1f} — oversold reversal zone (35–45): "
            f"potensi bounce (+{CONFIG.get('score_rsi_oversold', 1)})"
        )
    elif rsi < 35.0:
        signals.append(f"📊 RSI {rsi:.1f} — sangat oversold (bisa reversal tapi butuh konfirmasi)")
    else:
        signals.append(f"📊 RSI {rsi:.1f} — di atas zona valid, momentum sudah lanjut")

    # ── 11. Higher Low ────────────────────────────────────────────────────────
    if higher_low:
        score += CONFIG["score_higher_low"]
        signals.append("🔼 Higher Low terdeteksi — struktur bullish awal mulai terbentuk")

    # ── 12. BOS Up ────────────────────────────────────────────────────────────
    # v32 FIX: Mutual exclusion dengan ChartPrime Break Res.
    # BOS Up dan CP Break Res keduanya mendeteksi "resistance ditembus",
    # hanya dari sudut yang sedikit berbeda (close vs low crossover kotak).
    # Jika CP Break Res aktif → BOS Up sudah tercakup, skip (+12) untuk hindari
    # double count. BOS Up tetap aktif jika CP tidak mendeteksi break.
    _cp_break_res_active = cp_sr.get("brekout_res", False) if cp_sr else False

    if bos_up and not _cp_break_res_active:
        score += CONFIG["score_bos_up"]
        if horiz_break:
            signals.append(
                f"🔺 BOS Up CONFIRMED — level horizontal {_fmt_price(bos_level)} "
                f"tertembus (pivot resistance multi-candle) (+{CONFIG['score_bos_up']})"
            )
        else:
            signals.append(
                f"🔺 BOS Up — high {_fmt_price(bos_level)} tertembus (+{CONFIG['score_bos_up']})"
            )
    elif bos_up and _cp_break_res_active:
        # BOS terkonfirmasi tapi CP Break Res sudah di-score → log saja
        signals.append(
            f"🔺 BOS Up terkonfirmasi (skor ditahan — CP Break Res sudah aktif, "
            f"hindari double count)"
        )
    elif pre_close_bos and not _cp_break_res_active:
        # Pre-close BOS: candle sedang tembus tapi belum tutup — early signal
        _pcs = CONFIG.get("bos_preclose_bonus", 10)
        score += _pcs
        signals.append(
            f"⚡ PRE-CLOSE BOS: high candle tembus {_fmt_price(bos_level)} "
            f"tapi belum close — sinyal awal breakout (+{_pcs})"
        )
    elif horiz_break and not _cp_break_res_active:
        # Horizontal break tanpa standard BOS — partial signal
        _hbs = CONFIG.get("bos_preclose_bonus", 10) // 2
        score += _hbs
        signals.append(
            f"📈 Horizontal Resistance Break: level pivot {_fmt_price(bos_level)} "
            f"dilewati (+{_hbs})"
        )

    # ── 12b. Fresh Breakout Start — v31 FIX-v31-11 ──────────────────────────
    # v30: hanya streak 1-3 candle. v31: diperluas ke 5 candle.
    # Coin baru naik 1-5 jam setelah akumulasi panjang = titik entry terbaik.
    _streak = uptrend.get("age_hours", 0)
    _fresh_max = CONFIG.get("fresh_breakout_max_streak", 5)
    if 1 <= _streak <= _fresh_max:
        _fresh_bonus = CONFIG.get("score_fresh_breakout_start", 5)
        score += _fresh_bonus
        signals.append(
            f"🟢 Fresh Breakout Start: uptrend baru {_streak} candle "
            f"— titik entry optimal setelah akumulasi (+{_fresh_bonus})"
        )

    # ── 12c. v32: ChartPrime SR Volume-Weighted Scoring ──────────────────────
    # Ini adalah sinyal paling prediktif berdasarkan forensik 4 pump nyata.
    # Terintegrasi langsung dari logika Pine Script ChartPrime SR indicator.
    _cp_score, _cp_signals, _cp_label = score_chartprime_sr(cp_sr)
    if _cp_score != 0 or _cp_signals:
        score += _cp_score
        signals.extend(_cp_signals)
        log.debug(
            f"  {symbol}: ChartPrime SR → {_cp_label} | score_delta={_cp_score:+d}"
        )

    # ── 12d. v32b: Support Health System — 7 lapis preventif ────────────────
    # Hard-block jika CRITICAL + squeeze (false signal paling berbahaya).
    # Penalty bertingkat untuk HIGH/MEDIUM. Bonus jika harga di support.
    if sup_health.get("ok"):
        sh_risk  = sup_health.get("risk_level", "LOW")
        sh_dist  = sup_health.get("dist_to_sup", 99.0)
        sh_block = sup_health.get("should_block", False)

        # Hard-block: CRITICAL + squeeze = sinyal palsu paling berbahaya
        if sh_block and squeeze_active and CONFIG.get("sup_health_critical_block", True):
            log.info(
                f"  {symbol}: Support Health CRITICAL ({sup_health['health']}/100) "
                f"+ squeeze aktif — GATE GAGAL (support melemah, squeeze = false signal)"
            )
            return None

        # Penalty bertingkat
        if sh_risk == "CRITICAL":
            _sh_pen = CONFIG.get("sup_health_block_score", -20)
            score  += _sh_pen
            for sig in sup_health["signals"][:3]:
                signals.append(sig)
            signals.append(
                f"🚨 Support Health CRITICAL ({sup_health['health']}/100) "
                f"— {sup_health['n_warnings']} peringatan aktif ({_sh_pen})"
            )
        elif sh_risk == "HIGH":
            _sh_pen = CONFIG.get("sup_health_warn_score", -10)
            score  += _sh_pen
            for sig in sup_health["signals"][:2]:
                signals.append(sig)
            signals.append(
                f"⚠️ Support Health HIGH ({sup_health['health']}/100) ({_sh_pen})"
            )
        elif sh_risk == "MEDIUM":
            for sig in sup_health["signals"][:2]:
                signals.append(sig)

        # Bonus setup ideal: harga dekat support
        _sup_prox = CONFIG.get("sup_health_at_support_pct", 0.03)
        if 0 < sh_dist / 100 <= _sup_prox:
            _sh_bonus = CONFIG.get("sup_health_at_support_bonus", 8)
            score    += _sh_bonus
            signals.append(
                f"✅ Harga dekat support ({sh_dist:.2f}%) — entry ideal (+{_sh_bonus})"
            )

        log.debug(
            f"  {symbol}: SuppHealth={sup_health['health']}/100 "
            f"risk={sh_risk} dist={sh_dist:.1f}% warns={sup_health['n_warnings']}"
        )
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

    # FIX 02: Smart Money Accumulation v22
    # v32: mutual exclusion dengan accum (v18) — keduanya deteksi range contraction
    # + vol trend + bid pressure (logika hampir identik, threshold sedikit berbeda).
    # Jika accum v18 sudah aktif (is_accumulating=True), skip smart_money_v22
    # untuk menghindari double count +4 + +12 = +16 dari sinyal yang sama.
    _accum_already_scored = accum.get("is_accumulating", False)
    if smart_money_v22.get("is_accumulating"):
        if _accum_already_scored:
            signals.append(
                f"ℹ️ Smart Money Accum v22 terkonfirmasi "
                f"(skor ditahan — accum v18 sudah aktif, hindari double count)"
            )
        else:
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
    # v32: mutual exclusion dengan vol_comp_v23 — keduanya deteksi BB percentile
    # squeeze dengan parameter yang hampir sama. Hanya satu yang di-score,
    # pilih yang lebih tinggi. Jika squeeze_active (v31 BBW percentile) sudah
    # aktif, prioritaskan vol_comp_v23 karena lebih lengkap (tiered + vol confirm).
    if prebreakout_v22.get("is_compressed"):
        _pb_score  = prebreakout_v22["score"]
        _vc_score  = vol_comp_v23.get("score", 0)
        if _vc_score > 0:
            # vol_comp_v23 sudah aktif dan akan di-score di bawah → skip prebreakout_v22
            signals.append(
                f"ℹ️ Pre-Breakout v22 p{prebreakout_v22.get('percentile',0):.0f} "
                f"(skor ditahan — vol_comp_v23 lebih kuat, hindari double count)"
            )
        else:
            # vol_comp_v23 tidak aktif → score prebreakout_v22
            score += _pb_score
            signals.append(prebreakout_v22["label"])

    # FIX 06: Momentum Ignition v22
    # v32: mutual exclusion dengan mom_ign_v23 — keduanya deteksi consecutive
    # higher highs + vol spike. Hanya score yang lebih tinggi.
    if mom_ignition_v22.get("is_ignition"):
        _mi22_score = mom_ignition_v22["score"]
        _mi23_score = mom_ign_v23.get("score", 0) if mom_ign_v23.get("is_ignition") else 0
        if _mi23_score >= _mi22_score:
            signals.append(
                f"ℹ️ Mom Ignition v22 (skor ditahan — v23 sama/lebih kuat, "
                f"hindari double count)"
            )
        else:
            score += _mi22_score
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
    # v32: mutual exclusion dengan score_vol_compression_squeeze (v31).
    # Keduanya aktif saat squeeze: vol_comp_v23 (BBW percentile) dan
    # _squeeze_active + vol_compression_ratio (v31). Hanya score yang lebih besar.
    if vol_comp_v23.get("is_compressed"):
        _vc23_score = vol_comp_v23["score"]
        # Cek apakah vol_compression_squeeze sudah di-score di section 0a+
        _vc_squeeze_already = (
            _squeeze_active and
            vol_spike["tier"] == 0 and
            _vcmin <= _vol_compression_ratio <= _vcmax
        )
        if _vc_squeeze_already:
            _vc_squeeze_pts = CONFIG.get("score_vol_compression_squeeze", 10)
            if _vc23_score > _vc_squeeze_pts:
                # vol_comp_v23 lebih kuat → refund squeeze score, pakai v23
                score -= _vc_squeeze_pts
                score += _vc23_score
                signals.append(vol_comp_v23["label"])
                signals.append(
                    f"ℹ️ Vol Compression Squeeze (-{_vc_squeeze_pts}) diganti "
                    f"vol_comp_v23 (+{_vc23_score}) — ambil yang lebih kuat"
                )
            else:
                # squeeze score sama atau lebih kuat → skip v23
                signals.append(
                    f"ℹ️ Vol Comp v23 p{vol_comp_v23.get('percentile',0):.0f} "
                    f"(skor ditahan — vol_compression_squeeze sudah aktif)"
                )
        else:
            score += _vc23_score
            signals.append(vol_comp_v23["label"])

    # Momentum Ignition v23
    # v32: mutual exclusion dengan mom_ignition_v22 (sudah dihandle di atas).
    # Hanya score jika v22 tidak aktif, atau jika v23 lebih kuat.
    if mom_ign_v23.get("is_ignition"):
        _mi23_score = mom_ign_v23["score"]
        _mi22_was_scored = (
            mom_ignition_v22.get("is_ignition") and
            mom_ignition_v22.get("score", 0) > 0 and
            not (mom_ign_v23.get("score", 0) >= mom_ignition_v22.get("score", 0))
        )
        if _mi22_was_scored:
            signals.append(
                f"ℹ️ Mom Ignition v23 (skor ditahan — v22 sudah di-score)"
            )
        else:
            score += _mi23_score
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
    #  SCORING v27 — OI ACCELERATION + LIQUIDITY VACUUM + CVD DIVERGENCE
    # ══════════════════════════════════════════════════════════════════════════

    # 1. OI Acceleration (second derivative — OI makin cepat bertambah)
    oi_accel_sc = oi_accel_v27.get("score", 0)
    if oi_accel_sc != 0:
        score += oi_accel_sc
        if oi_accel_v27.get("label"):
            signals.append(oi_accel_v27["label"])

    # 2. Orderbook Liquidity Vacuum (real orderbook gap detector)
    ob_vac_sc = ob_vacuum_v27.get("score", 0)
    if ob_vac_sc != 0:
        score += ob_vac_sc
        if ob_vacuum_v27.get("label"):
            signals.append(ob_vacuum_v27["label"])

    # 3. CVD Divergence (akumulasi vs distribusi real)
    cvd_sc = cvd_div_v27.get("score", 0)
    if cvd_sc != 0:
        score += cvd_sc
        if cvd_div_v27.get("label"):
            signals.append(cvd_div_v27["label"])

    # Guard distribusi: CVD bearish + OB vacuum bid = distribusi kuat → label warning
    if cvd_div_v27.get("is_bearish_div") and ob_vacuum_v27.get("vacuum_side") == "bid":
        signals.append("🚨 DISTRIBUSI DOUBLE KONFIRMASI: CVD + OB Vacuum Bid — HINDARI ENTRY!")



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

    # FIX v28 BUG#1 — Normalisasi heuristic score ke 0-100 sebelum blending.
    #
    # ROOT CAUSE: `score` (heuristic) berskala 0-200+ tanpa batas atas, sedangkan
    # ai_score dan mm_score selalu 0-100. Sebelumnya:
    #   blended = score*0.50 + ai*0.25 + mm*0.25
    # → Coin dengan heuristic=150 (spike kuat) mendapat blended ~90+
    # → Coin pump lambat heuristic=60 tapi ai=80, mm=80 mendapat blended ~85
    # Perbedaan hanya 5 poin padahal spike coin jauh lebih bullish → ranking salah.
    #
    # Fix: normalisasi heuristic ke 0-100 menggunakan ref_max=150 sebagai nilai
    # referensi (spike signal sangat kuat ≈ 150 poin heuristic).
    # Jika score > 150 → normalized = 100 (cap, tidak hilang).
    # Semua tiga komponen sekarang berada di skala yang sama sebelum blend.
    _ref_max  = CONFIG.get("heuristic_ref_max", 150.0)
    _heuristic_norm = min(score / _ref_max, 1.0) * 100.0   # 0.0 – 100.0

    _hw = CONFIG.get("v23_heuristic_weight", 0.50)   # heuristic: 50%
    _aw = CONFIG.get("v23_ai_weight",        0.25)   # AI: 25%
    _mw = CONFIG.get("v23_inst_weight",      0.25)   # MM institutional: 25%
    blended_score = round(
        _heuristic_norm                    * _hw
        + ai_score_data["weighted_score"]  * _aw
        + mm_score_data["mm_score"]        * _mw
    )

    # Debug log: bantu kalibrasi ref_max di live environment
    log.debug(
        f"  Blended score [{symbol}]: heuristic_raw={score:.1f} "
        f"→ norm={_heuristic_norm:.1f} | ai={ai_score_data['weighted_score']:.1f} "
        f"| mm={mm_score_data['mm_score']:.1f} → blended={blended_score}"
    )

    # Pump probability: blend sigmoid from MM score (70%) + legacy logistic (30%)
    pump_prob_v23  = calc_pump_probability_v23(mm_score_data["mm_score"])
    pump_prob_leg  = calc_pump_probability_v19(blended_score)
    pump_prob_raw  = round(pump_prob_v23 * 0.70 + pump_prob_leg * 0.30, 1)

    # FIX v29 BUG-C: Cap pump_prob berdasarkan alert_level untuk eliminasi
    # inkonsistensi fatal "IGNORE #2 | Pump Prob: 75.4%".
    # Masalah: pump_prob_v23 menggunakan mm_score (bukan blended_score),
    # sehingga bisa 75%+ meski score masuk IGNORE.
    # Fix: pump_prob di-cap sesuai alert level yang dikembalikan ke user.
    # IGNORE    → max 40%  (ada sinyal tapi tidak signifikan)
    # WATCHLIST → max 60%  (menarik tapi belum cukup kuat)
    # ALERT     → max 80%  (sinyal kuat)
    # STRONG    → 100%     (tidak di-cap)
    alert_level_v19 = get_alert_level_v19(blended_score)
    _prob_caps = {
        "IGNORE":       40.0,
        "WATCHLIST":    60.0,
        "ALERT":        80.0,
        "STRONG ALERT": 100.0,
    }
    _prob_cap  = _prob_caps.get(alert_level_v19, 60.0)
    pump_prob  = round(min(pump_prob_raw, _prob_cap), 1)

    if pump_prob < pump_prob_raw:
        log.debug(
            f"  {symbol}: pump_prob capped {pump_prob_raw}% → {pump_prob}% "
            f"(alert={alert_level_v19}, cap={_prob_cap}%)"
        )

    # BUG FIX v25: pump_prob_v22 was referenced in result dict but never assigned
    # Alias to pump_prob_v23 for backward compatibility with build_alert
    pump_prob_v22  = pump_prob_v23

    # v18 timing ETA (retained)
    pump_timing = calc_pump_timing_eta(
        vol_accel, buy_press["buy_ratio"], oi_data, micro_mom
    )

    # alert_level_v19 sudah dihitung di atas (FIX v29 BUG-C)

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
        # FIX v29 BUG-E: "Whale Entry" hanya valid jika OI juga dikonfirmasi.
        # Jika OI not confirmed (is_new atau change kecil), downgrade label
        # agar user tidak salah mengira ini whale entry yang kuat.
        _oi_confirmed = (not oi_data.get("is_new", True) and
                         oi_data.get("change_pct", 0) >= CONFIG.get("oi_change_min_pct", 3.0))
        if _oi_confirmed:
            pump_type = "Whale Entry + Buy Pressure"
        else:
            pump_type = "Volume Spike + Buy Pressure (OI unconfirmed)"
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
        alert_level_v19, bos_level, liq_sweep,
        cp_sr=cp_sr   # v32: ChartPrime SR untuk entry/SL/TP presisi
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
            # v27 new fields
            "oi_accel_v27":     oi_accel_v27,
            "ob_vacuum_v27":    ob_vacuum_v27,
            "cvd_div_v27":      cvd_div_v27,
            # v31 new fields
            "squeeze_active":   squeeze_active,
            "bbw_percentile":   round(_bbw_pct, 1),
            "pre_close_bos":    pre_close_bos,
            "horiz_break":      horiz_break,
            "low_vol_accum":    accum.get("is_low_vol_accum", False),
            # v32 new fields — ChartPrime SR
            "cp_sr":            cp_sr,
            "cp_label":         _cp_label,
            "cp_score":         _cp_score,
            # v32b new fields — Support Health
            "sup_health":       sup_health,
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

    # v31: Squeeze mode indicator
    sq_active    = r.get("squeeze_active", False)
    bbw_pct      = r.get("bbw_percentile", 50.0)
    pre_close    = r.get("pre_close_bos", False)
    low_vol_acc  = r.get("low_vol_accum", False)
    sq_tag       = f" 🗜️SQ{bbw_pct:.0f}%" if sq_active else ""
    pcs_tag      = " ⚡PRE-BOS" if pre_close else ""
    lva_tag      = " 🕵️LVA" if low_vol_acc else ""

    msg  = f"{level_icon} <b>{r['symbol']} — {level_v18}</b>  #{rank}{sq_tag}{pcs_tag}{lva_tag}\n"
    msg += f"<b>Score :</b> {r['score']}  |  <b>Pump Prob:</b> {pump_prob}%\n"
    msg += f"<b>ETA   :</b> {eta_e} {eta} ({urg})\n"
    msg += f"<b>Type  :</b> {r['pump_type']}\n"
    msg += f"<b>Regime:</b> {market_regime}  |  <b>Scan:</b> {utc_now()}\n"
    if sq_active:
        msg += f"<b>🗜️ SQUEEZE MODE</b> — BBW percentile {bbw_pct:.0f}% (akumulasi pra-pump terdeteksi)\n"
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

    # Entry / SL / TP v32
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    sl_anchor_str = e.get("sl_anchor", "")
    sl_src_tag    = f" [{sl_anchor_str}]" if sl_anchor_str else ""
    cp_entry_tag  = " 🚀CP" if e.get("cp_break_res") else ""
    msg += f"📍 <b>Entry :</b> <code>{_fmt_price(entry)}</code>  [{e.get('sl_method','')}]{cp_entry_tag}\n"
    msg += f"🛑 <b>SL    :</b> <code>{_fmt_price(sl)}</code>  (-{e['sl_pct']:.2f}%){sl_src_tag}\n"
    rr3_str = e.get("rr3_str", e.get("rr2_str", "?"))
    msg += f"🎯 <b>TP1   :</b> <code>{_fmt_price(t1)}</code>  (+{e['gain_t1_pct']:.1f}%)  RR:{e['rr_str']}x  [{e.get('t1_source','')}]\n"
    msg += f"🎯 <b>TP2   :</b> <code>{_fmt_price(t2)}</code>  (+{e['gain_t2_pct']:.1f}%)  RR:{e.get('rr2_str','?')}x  [{e.get('t2_source','')}]\n"
    msg += f"🎯 <b>TP3   :</b> <code>{_fmt_price(t3)}</code>  (+{e.get('gain_t3_pct', 0):.1f}%)  RR:{rr3_str}x  [{e.get('t3_source','')}]\n"
    msg += f"⚖️ <b>ATR   :</b> {e.get('atr_pct', r.get('atr_pct',0)):.2f}%  |  SL: -{e['sl_pct']:.2f}%  |  Regime: {e.get('market_regime','?')}\n"
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

    # Support & Resistance (standard pivot levels)
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

    # v32b: ChartPrime SR Section — inti sinyal pre-pump
    cp   = r.get("cp_sr", {})
    cp_l = r.get("cp_label", "")
    if cp and cp.get("ok"):
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        cp_icon = {
            "BREAK_RES":    "🚀",
            "BREAK_SUP":    "💥",
            "RES_HOLDS":    "🔴",
            "NEAR_RES":     "📍",
            "NEUTRAL":      "📊",
        }.get(cp_l.split("_")[0] + "_" + cp_l.split("_")[1] if "_" in cp_l else cp_l, "📊")
        if cp_l.startswith("SUP_HOLDS"):
            cp_icon = "🟢"
        cp_sup   = cp.get("support")
        cp_res   = cp.get("resistance")
        top_res  = cp.get("top_res", [])
        holds_n  = cp.get("sup_holds_count", 0)
        msg += f"<b>{cp_icon} ChartPrime SR [v32b]:</b>  {cp_l}\n"
        # Tampilkan top-3 resistance jika ada (FIX-CP-3)
        if top_res:
            for idx, rv in enumerate(top_res[:3]):
                gap_r = (rv["level"] - p) / p * 100
                tag   = " ← strongest" if idx == 0 else ""
                msg  += (f"  🔴 R{idx+1}: <code>{_fmt_price(rv['level'])}</code>"
                         f"  ({gap_r:+.1f}%)  dVol:{rv['delta_vol']/1e3:.0f}K{tag}\n")
        elif cp_res:
            gap_r = (cp_res["level"] - p) / p * 100
            msg += (f"  🔴 CP-Res: <code>{_fmt_price(cp_res['level'])}</code>"
                    f"  ({gap_r:+.1f}%)  Vol:{cp_res['delta_vol']/1e3:.0f}K\n")
        if cp_sup:
            gap_s = (cp_sup["level"] - p) / p * 100
            msg += (f"  🟢 CP-Sup: <code>{_fmt_price(cp_sup['level'])}</code>"
                    f"  ({gap_s:+.1f}%)  Vol:{cp_sup['delta_vol']/1e3:.0f}K"
                    f"  | Holds:{holds_n}x\n")
        if cp.get("brekout_res"):
            msg += "  ⚡ <b>BREAK RES AKTIF — sinyal pump fase 2!</b>\n"
        elif cp.get("brekout_sup"):
            msg += "  ⚠️ <b>BREAK SUP — support patah, hati-hati!</b>\n"
        elif cp.get("near_res"):
            msg += f"  📍 Jarak ke resistance: {cp['near_res_pct']:.2f}% (siap break)\n"

    # v32b: Support Health section
    sh = r.get("sup_health", {})
    if sh and sh.get("ok") and sh.get("should_warn"):
        sh_risk  = sh.get("risk_level", "LOW")
        sh_score = sh.get("health", 100)
        sh_dist  = sh.get("dist_to_sup", 0)
        sh_icon  = "🚨" if sh_risk == "CRITICAL" else "⚠️"
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"{sh_icon} <b>Support Health [{sh_risk}]:</b> {sh_score}/100\n"
        msg += f"  Jarak ke support: {sh_dist:.2f}%\n"
        for sig in sh.get("signals", [])[:3]:
            msg += f"  • {sig[:85]}\n"
        if sh_risk == "CRITICAL":
            msg += "  <b>⛔ PERINGATAN: Support rentan jebol — pertimbangkan skip</b>\n"
        # Tampilkan jika ada SR flip (resistance lama jadi support / sebaliknya)
        n_flip_r = cp.get("n_flipped_res", 0)
        n_flip_s = cp.get("n_flipped_sup", 0)
        if n_flip_r > 0 or n_flip_s > 0:
            msg += f"  🔄 SR Flip: {n_flip_r} res→sup, {n_flip_s} sup→res\n"

    # OI
    if oi.get("oi_now", 0) > 0:
        ov     = oi["oi_now"]
        os_str = f"${ov/1e6:.2f}M" if ov >= 1e6 else f"${ov/1e3:.0f}K"
        cs     = f"({oi['change_pct']:+.1f}%)" if not oi.get("is_new") else "(baseline)"
        msg += f"📈 <b>OI:</b> {os_str} {cs}\n"

    # Sinyal teknikal prioritas — v32: tambahkan ChartPrime ke keyword
    msg += "━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>Sinyal:</b>\n"
    priority_signals = []
    keywords = [
        "ChartPrime", "Phase", "Volume Spike", "Buy Pressure", "Acceleration",
        "Whale", "FAKE", "AKUMULASI", "BUILD-UP", "Squeeze", "Sweep",
        "BOS", "Funding", "HTF", "OI", "ATR", "BB"
    ]
    for s in r["signals"]:
        if any(kw in s for kw in keywords):
            priority_signals.append(s)
        if len(priority_signals) >= 8:
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

    # ── v27 NEW: OI Acceleration + Liquidity Vacuum + CVD Divergence ──────────
    oi_acc  = r.get("oi_accel_v27", {})
    ob_vac  = r.get("ob_vacuum_v27", {})
    cvd_div = r.get("cvd_div_v27", {})

    has_v27 = (
        oi_acc.get("is_accelerating")
        or ob_vac.get("is_vacuum")
        or cvd_div.get("is_bullish_div")
        or cvd_div.get("is_bearish_div")
        or cvd_div.get("is_momentum")
    )
    if has_v27:
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += "<b>🆕 v27 Advanced Signals:</b>\n"

        # OI Acceleration
        if oi_acc.get("is_accelerating") or oi_acc.get("score", 0) != 0:
            rate      = oi_acc.get("current_rate", 0)
            acc_rate  = oi_acc.get("acceleration_rate", 0)
            is_strong = oi_acc.get("is_strong", False)
            n_snaps   = oi_acc.get("n_snapshots", 0)
            emoji     = "🚀" if is_strong else "⚡"
            msg += (
                f"  {emoji} <b>OI Accel:</b> rate {rate:+.2f}%/h "
                f"[akselerasi {acc_rate:+.2f}%/h | n={n_snaps}]"
                + (" — KUAT!" if is_strong else "") + "\n"
            )

        # Orderbook Liquidity Vacuum
        if ob_vac.get("is_vacuum") or ob_vac.get("score", 0) != 0:
            gap      = ob_vac.get("vacuum_pct", 0)
            bid_gap  = ob_vac.get("vacuum_bid_pct", 0)
            side     = ob_vac.get("vacuum_side", "none")
            real_rat = ob_vac.get("bid_ask_real_ratio", 1.0)
            bid_d    = ob_vac.get("bid_depth_usd", 0)
            ask_d    = ob_vac.get("ask_depth_usd", 0)
            bid_str  = f"${bid_d/1e3:.0f}K" if bid_d > 0 else "?"
            ask_str  = f"${ask_d/1e3:.0f}K" if ask_d > 0 else "?"
            vac_emoji = "🚀" if side == "ask_strong" else ("🕳️" if "ask" in side else "⚠️")
            msg += (
                f"  {vac_emoji} <b>OB Vacuum:</b> ASK gap {gap:.2f}% / BID gap {bid_gap:.2f}% "
                f"[{side.upper()}] | bid/ask {real_rat:.2f}× "
                f"({bid_str}/{ask_str})\n"
            )

        # CVD Divergence
        if cvd_div.get("is_bullish_div"):
            p_pct  = cvd_div.get("price_trend_pct", 0)
            c_norm = cvd_div.get("cvd_trend_norm", 0) * 100
            res    = "15m" if cvd_div.get("use_15m") else "1h"
            msg += (
                f"  📉→📈 <b>CVD Bullish Div!</b> Harga {p_pct:+.2f}% vs "
                f"CVD {c_norm:+.0f}% ({res}) — AKUMULASI TERSEMBUNYI\n"
            )
        elif cvd_div.get("is_bearish_div"):
            p_pct  = cvd_div.get("price_trend_pct", 0)
            c_norm = cvd_div.get("cvd_trend_norm", 0) * 100
            res    = "15m" if cvd_div.get("use_15m") else "1h"
            msg += (
                f"  🚨 <b>CVD Distribusi!</b> Harga {p_pct:+.2f}% vs "
                f"CVD {c_norm:+.0f}% ({res}) — JUAL DI KENAIKAN!\n"
            )
        elif cvd_div.get("is_momentum"):
            p_pct  = cvd_div.get("price_trend_pct", 0)
            res    = "15m" if cvd_div.get("use_15m") else "1h"
            msg += f"  ⚡ <b>CVD Momentum</b> ({res}): harga {p_pct:+.2f}% terkonfirmasi\n"
        elif cvd_div.get("is_exhaustion"):
            msg += "  ⚠️ <b>CVD Exhaustion</b> — pump hampir habis\n"

    rank_val = r.get("rank_value", r.get("score", 0))
    z_rank   = r.get("rank_vol_z_v20", 0)
    mm_rank  = r.get("rank_mm_score", 0)
    ob_rank  = r.get("rank_ob_score", 1.0)
    cp_lbl   = r.get("cp_label", "")
    cp_sc    = r.get("cp_score", 0)
    cp_tag   = f" | CP:{cp_lbl}({cp_sc:+d})" if cp_lbl and cp_lbl != "NEUTRAL" else ""
    msg += (f"\n<i>Scanner v32 | Rank:{rank_val:.1f} | "
            f"MM:{mm_rank:.0f} | Z:{z_rank:.2f} | OB:{ob_rank:.2f}"
            f"{cp_tag} | ⚠️ Bukan financial advice</i>")
    return msg

def build_summary(results):
    """
    v32 — TOP N summary dengan ChartPrime SR signal highlight.
    """
    top5 = results[:5]
    msg  = (
        f"📋 <b>TOP {len(top5)} PUMP CANDIDATES — Scanner v32</b>\n"
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
        ob_ic    = "🟢" if ob_imb >= 1.2 else ("🔴" if ob_imb <= 0.8 else "🟡")

        # MM score
        mm_sc    = r.get("rank_mm_score", r.get("mm_score", {}).get("mm_score", 0))

        # TP targets
        t1_pct   = r.get("entry", {}).get("gain_t1_pct", 0)

        # v32: ChartPrime label
        cp_lbl   = r.get("cp_label", "")
        cp_sc_v  = r.get("cp_score", 0)
        if cp_lbl and cp_lbl not in ("NEUTRAL", ""):
            cp_icon = "🚀" if cp_lbl == "BREAK_RES" else (
                      "🟢" if cp_lbl.startswith("SUP_HOLDS") else (
                      "💥" if cp_lbl == "BREAK_SUP" else "📍"))
            cp_str  = f"\n   {cp_icon}CP: {cp_lbl} ({cp_sc_v:+d}pt)"
        else:
            cp_str  = ""

        # Squeeze indicator
        sq_tag = " 🗜️SQ" if r.get("squeeze_active") else ""

        msg += (
            f"\n{i}. {level_ic} <b>{sym}</b>{sq_tag}\n"
            f"   Score:<b>{score}</b> | Prob:<b>{prob}%</b> | MM:{mm_sc:.0f}\n"
            f"   Vol Z:{z_v20:.2f} ({vol_str}) | {ema_ic}EMA:{ema_t} | {ob_ic}OB:{ob_imb:.2f}\n"
            f"   TP1:+{t1_pct}%{cp_str}\n"
        )

        # Highlight top signals (max 2, prioritaskan ChartPrime)
        sigs = r.get("signals", [])
        cp_sigs = [s for s in sigs if "ChartPrime" in s]
        other_sigs = [s for s in sigs if "ChartPrime" not in s]
        shown = (cp_sigs[:1] + other_sigs)[:2]
        for s in shown:
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

        # FIX v31 FIX-v31-7: BBW estimasi dari candle 1h BUKAN ticker high/low 24h.
        # v30 memakai high24h/low24h sebagai proxy squeeze — ini salah.
        # Setelah pump +280%, high24h sangat tinggi → range 24h besar →
        # tidak dianggap squeeze → gate 50% → coin diblokir.
        # v31: pakai candle dari cache (_cache), hitung std/mean (BBW nyata).
        # Fallback ke ticker jika candle belum di-cache.
        _effective_gate = CONFIG["gate_chg_24h_max"]  # default 50%
        try:
            _cache_key_cl = f"c_{sym}_1H_168"
            _cached_cl    = _cache.get(_cache_key_cl)
            if _cached_cl and (time.time() - _cached_cl[0] < 300) and len(_cached_cl[1]) >= 22:
                _c1h_cand = _cached_cl[1]
                _closes_c = [c["close"] for c in _c1h_cand[-20:]]
                _mean_c   = sum(_closes_c) / 20
                if _mean_c > 0:
                    _var_c  = sum((x - _mean_c)**2 for x in _closes_c) / 20
                    _bbw_c  = 4.0 * (_var_c**0.5) / _mean_c
                    # Bandingkan vs historis 50 candle untuk percentile
                    _hist_c = []
                    for _ii in range(50, 0, -1):
                        _sg = _c1h_cand[-(_ii+20):-_ii] if _ii > 0 else _c1h_cand[-20:]
                        if len(_sg) < 20:
                            continue
                        _mc = sum(c["close"] for c in _sg) / 20
                        if _mc <= 0:
                            continue
                        _vc = sum((c["close"]-_mc)**2 for c in _sg) / 20
                        _hist_c.append(4.0 * (_vc**0.5) / _mc)
                    if _hist_c:
                        _rk = sum(1 for v in _hist_c if v <= _bbw_c)
                        _pt = _rk / len(_hist_c) * 100
                        if _pt <= CONFIG.get("squeeze_mode_bbw_percentile", 30):
                            _effective_gate = CONFIG.get("gate_chg_24h_max_squeeze", 200.0)
                    elif _bbw_c < 0.04:
                        _effective_gate = CONFIG.get("gate_chg_24h_max_squeeze", 200.0)
            else:
                # Fallback: ticker range 24h
                _h24 = float(ticker.get("high24h", price * 1.05) or price * 1.05)
                _l24 = float(ticker.get("low24h",  price * 0.95) or price * 0.95)
                _mid = (_h24 + _l24) / 2 if (_h24 + _l24) > 0 else price
                if (_mid > 0) and ((_h24 - _l24) / _mid * 100) < 6.0:
                    _effective_gate = CONFIG.get("gate_chg_24h_max_squeeze", 200.0)
        except Exception:
            pass  # fallback ke gate default

        if chg > _effective_gate:
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

    log.info(f"\n📊 SCAN SUMMARY Scanner v31:")
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
    log.info(f"=== QUANTITATIVE PUMP DETECTION SCANNER v32 — {utc_now()} ===")

    load_funding_snapshots()
    log.info(f"Funding snapshots loaded: {len(_funding_snapshots)} coins di memori")

    # FIX v18: load OI snapshots dari disk
    load_oi_snapshots()

    # v27: load OI acceleration history dari disk
    load_oi_accel_history()

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

    # v27: simpan OI acceleration history ke disk
    save_oi_accel_history()
    log.info("OI Accel history disimpan ke disk.")

    # v23 — Advanced 4-key Ranking: prob → MM score → vol_zscore → OB imbalance
    if CONFIG.get("rank_v20_multi", True):
        results.sort(
            key=lambda x: (
                x.get("rank_value",      x["score"]),   # 1: score × prob
                x.get("rank_mm_score",   0),             # 2: MM score (v23)
                x.get("rank_vol_z_v20",  0),             # 3: vol z-score
                x.get("rank_ob_score",   1.0),           # 4: OB imbalance
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
                f"Level={r['alert_level']} CP={r.get('cp_label','?')}"
            )
        time.sleep(2)

    log.info(f"=== SELESAI Scanner v32 — {len(top)} alert terkirim ===")

# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════════════════════════╗")
    log.info("║  QUANTITATIVE PUMP DETECTION SCANNER v32                   ║")
    log.info("║  ChartPrime SR Integration | Break Res +20 | Sup Holds     ║")
    log.info("║  score_bos_up:18→12 | Delta Vol Pivot | 4-fase forensik   ║")
    log.info("╚══════════════════════════════════════════════════════════════╝")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan di .env!")
        exit(1)

    SCAN_INTERVAL_SEC = 900  # 15 menit

    scan_count = 0
    while True:
        scan_count += 1
        log.info(f"\n{'='*60}")
        log.info(f"🔄 SCAN #{scan_count} — {utc_now()}")
        log.info(f"{'='*60}")
        try:
            run_scan()
        except KeyboardInterrupt:
            log.info("⛔ Scanner dihentikan oleh user (Ctrl+C).")
            break
        except Exception as e:
            import traceback
            log.error(f"❌ Error fatal di run_scan: {e}")
            log.debug(traceback.format_exc())
            log.info(f"⏳ Retry dalam 60 detik...")
            time.sleep(60)
            continue

        log.info(f"✅ Scan #{scan_count} selesai. Next scan dalam {SCAN_INTERVAL_SEC//60} menit...")
        log.info(f"   (Tekan Ctrl+C untuk berhenti)")
        time.sleep(SCAN_INTERVAL_SEC)
