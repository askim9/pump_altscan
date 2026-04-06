#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NEXUS-SR v3.2 — Empirically Validated + Coinalyze Derivatives Data         ║
║                                                                              ║
║  PERUBAHAN v3.2:                                                             ║
║  Integrasi data Coinalyze berdasarkan hasil probe lapangan (Apr 2026):       ║
║  · Bitget TIDAK ada di Coinalyze → pakai Binance (code A) + Bybit (code 6)  ║
║    sebagai proxy. Coin yang sama bergerak hampir identik karena arbitrase.   ║
║                                                                              ║
║  DATA BARU dari Coinalyze (semua divalidasi dari probe):                     ║
║  · OI Change     : /open-interest-history  → Binance, field 'c' (USD)       ║
║  · Funding Rate  : /funding-rate (current) → Binance, field 'value'         ║
║  · Funding Hist  : /funding-rate-history   → interval 1hour (bukan 8hour!)  ║
║  · L/S Ratio     : /long-short-ratio-history → Bybit 542 markets            ║
║                    field l=long%, s=short%, r=ratio                          ║
║  · Liquidations  : /liquidation-history    → Binance, l=long, s=short (USD) ║
║  · Taker Buy     : /ohlcv-history          → Binance, btx/tx/bv 0% null     ║
║                                                                              ║
║  SYMBOL FORMAT (dari probe):                                                 ║
║  · Binance: {BASE}USDT_PERP.A  → BTCUSDT_PERP.A                            ║
║  · Bybit  : {BASE}USDT.6       → BTCUSDT.6                                  ║
║                                                                              ║
║  ARSITEKTUR SCORING (berbasis data empiris 5.214 events):                   ║
║  LAYER 1 — ZONE GATE   : TESTING state, break<3, vol not outlier            ║
║  LAYER 2 — VOLATILITY  : BBW ≥ 0.050, ATR% ≥ 1.20%                         ║
║  LAYER 3 — SCORE 0-100 :                                                    ║
║    A. Volatility   0-50 pts → BBW + ATR_pct                                 ║
║    B. Volume       0-30 pts → VolCompression + VolZ_4H                      ║
║    C. Momentum     0-20 pts → bear_streak + VolRatio                        ║
║  LAYER 4 — DERIVATIVES : Coinalyze data sebagai CONTEXT INFO                ║
║    · Ditampilkan di output dan Telegram                                      ║
║    · Tidak masuk scoring (data cross-exchange, bukan Bitget langsung)        ║
║    · Berfungsi sebagai filter manual / konfirmasi tambahan                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import logging.handlers as _lh
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Logging ───────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_ch   = logging.StreamHandler()
_ch.setFormatter(_fmt)
_root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/nexus_sr_v32.log", maxBytes=10 * 1024**2, backupCount=2)
_fh.setFormatter(_fmt)
_root.addHandler(_fh)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG — semua threshold dari data empiris
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    # ── CREDENTIALS ────────────────────────────────────────────────────────
    "bot_token": os.getenv("BOT_TOKEN"),
    "chat_id":   os.getenv("CHAT_ID"),

    # ── UNIVERSE ───────────────────────────────────────────────────────────
    "min_vol_24h":       50_000,    # $50K floor — singkirkan ghost coins saja
    "min_vol_signal":   500_000,    # $500K minimum untuk sinyal yang dikirim ke Telegram
    "max_vol_24h":  2_000_000_000,
    "gate_chg_24h_max":    60.0,   # skip jika sudah pump >60%

    # ── PINE SCRIPT PARAMS ─────────────────────────────────────────────────
    "lookback_period":       20,   # Pine: lookbackPeriod
    "vol_len":                2,   # Pine: vol_len
    "box_width_multiplier": 1.0,   # Pine: box_withd
    "candle_limit_1h":      200,   # 1H candles (~8 hari)
    "candle_limit_1d":      100,   # FIX: Bitget 1D candles max ~90 dalam praktik
                                   # 100 cukup untuk SMA50 (50 candles needed)
                                   # Regime check pakai SMA50 bukan EMA200
                                   # (EMA200 butuh 200 candles, tapi Bitget max ~90 1D)

    # ── ZONE GATE ──────────────────────────────────────────────────────────
    "atr_period":           200,   # Pine: ta.atr(200)
    "max_break_count":        3,
    "vol_outlier_mult":     5.0,   # vol > 5× avg = outlier → skip
    "vol_outlier_lookback":  20,   # FIX: dipakai di candles[-2], bukan candles[-1]
    "max_zone_width_pct":    8.0,  # zone width max 8% dari harga — filter low-liquidity

    # ── PUMP REJECTION GATE ────────────────────────────────────────────────
    # Dari audit: 4USDT lolos sebagai STRONG karena volume spike 10.66×
    # padahal bear_streak=0 → ini bukan bounce setup, tapi pump event
    # Pattern: vol_ratio tinggi + tidak ada tekanan jual = pump SEDANG terjadi
    # Sinyal yang benar: ada tekanan jual (bear streak) LALU volume spike
    "pump_reject_vol_ratio":  5.0,  # vol_ratio > ini = potensi pump
    "pump_reject_bear_min":   1,    # bear_streak harus >= ini jika vol tinggi

    # ── MINIMUM COMPONENT SCORE ────────────────────────────────────────────
    # FIX: Turunkan dari 10 ke 5 berdasarkan hasil run nyata.
    # min_score_B=10 memblok 5/5 kandidat karena crash market membuat volume flat.
    # Prioritas perbaikan dari backtest: naikkan threshold ke 70 (score 60-64 EV negatif),
    # bukan block semua sinyal via min B.
    # Score 5 = B1+B2 minimal ada sedikit konfirmasi, bukan zero sama sekali.
    "min_score_B":            5,   # require B ≥ 5 (dari 30 max)

    # ── VOLATILITY GATE (G4 + G5) — dari data empiris ──────────────────────
    # BBW threshold: top quintile dari large dataset = 0.078
    # Turunkan ke 0.050 untuk meningkatkan recall (capture lebih banyak bounce)
    # Tradeoff: precision sedikit turun tapi tidak kehilangan terlalu banyak hits
    "gate_bbw_min":        0.050,  # Bollinger Band Width minimum
    "gate_atr_pct_min":    1.20,   # ATR sebagai % dari harga minimum

    # ── SCORING PARAMS (dari data empiris) ─────────────────────────────────

    # A. VOLATILITY SCORE (0-50 pts)
    # Dari large dataset: BBW corr=+0.236, ATR_pct corr=+0.230
    # HIT avg BBW = 0.137 vs MISS avg = 0.057 (2.4x ratio)
    # HIT avg ATR% = 3.12 vs MISS avg = 1.40 (2.2x ratio)
    "score_vol_max":        50,    # Total A component
    "bbw_strong":          0.150,  # ≥ ini → full BBW score (above HIT avg)
    "bbw_medium":          0.078,  # ≥ ini → half BBW score (top quintile)
    "atr_strong":          3.12,   # ≥ ini → full ATR score (HIT avg)
    "atr_medium":          1.62,   # ≥ ini → half ATR score (top quintile)
    # Bobot dalam komponen A: BBW 55%, ATR 45% (BBW sedikit lebih kuat)
    "bbw_weight":          0.55,
    "atr_weight":          0.45,

    # B. VOLUME SCORE (0-30 pts)
    # Sub-komponen B1: VolCompression (recent avg / prior avg)
    # Dari large dataset: corr=+0.089 (confirmed), HIT avg=3.76 vs MISS=1.42
    # Sub-komponen B2: VolZ_4H (Z-score vs 4H window)
    # Dari large dataset: corr=+0.083 (confirmed), HIT avg=1.03 vs MISS=0.16
    # BERBEDA: B1=recent spike vs baseline, B2=historical anomaly 4H window
    "score_vol_micro_max":  30,
    "vol_comp_strong":     3.76,   # ≥ HIT avg → full B1 score
    "vol_comp_medium":     1.82,   # ≥ top quintile → half B1 score
    "vol_z4h_strong":      2.0,    # ≥ ini → full B2 score (7.8% hit rate)
    "vol_z4h_medium":      0.729,  # ≥ ini → half B2 score (threshold optimal)
    "vol_comp_weight":     0.55,   # B1 sedikit lebih kuat dari B2
    "vol_z4h_weight":      0.45,

    # C. MOMENTUM SCORE (0-20 pts)
    # Sub-komponen C1: Bear streak (consecutive bearish candles masuk zone)
    # Dari large dataset: corr=+0.030, streak 3-4 = hit 3.9-4.7% vs base 2.9%
    # Sub-komponen C2: VolRatio (current vol vs 20-bar avg)
    # Dari large dataset: corr=+0.068, ratio ≥1.44 = 4.3% hit rate
    "score_momentum_max":   20,
    "bear_streak_strong":    4,    # ≥ 4 consecutive bear candles
    "bear_streak_medium":    2,    # ≥ 2 consecutive bear candles
    "vol_ratio_strong":    2.0,    # ≥ 2× avg → full C2 score
    "vol_ratio_medium":    1.44,   # ≥ 1.44× avg → half C2 score (threshold optimal)
    "bear_weight":         0.40,   # C1 lebih lemah dari C2
    "vol_ratio_weight":    0.60,

    # ── THRESHOLD — dari backtest: score 60-64 adalah EV negatif ─────────
    # Backtest 262 signals: score 60-64 WR=20.7% < breakeven 31.5%
    # Naikkan dari 60 ke 70 menghapus 40% sinyal EV negatif
    "score_threshold_normal":  70,  # FIX: dari 60 → 70 (backtest: score<70 = -EV)
    "score_threshold_caution": 80,  # saat BTC+ETH < SMA50 daily
    "score_strong":            85,  # → kirim Telegram

    # ── REGIME CHECK ───────────────────────────────────────────────────────
    # FIX: Bitget 1D max ~90 candles (terbukti dari run nyata n=90)
    # SMA50 dari n=90 sudah valid dan cukup untuk deteksi trend
    # Dual method: EMA200 jika ada (>200 candles), SMA50 jika tidak
    "ema_regime_period":       50,  # SMA50 sebagai primary (90 candles cukup)

    # ── OUTPUT ────────────────────────────────────────────────────────────
    "top_n":                   10,
    "max_alerts":               5,
    "alert_cooldown_sec":    3600,
    "cooldown_file": "/tmp/nexus_sr_v32_state.json",
    "sleep_between_coins":    0.2,
}

MANUAL_EXCLUDE: set = set()

# ══════════════════════════════════════════════════════════════════════════════
#  🔗  COINALYZE CONFIG (dari probe lapangan Apr 2026)
# ══════════════════════════════════════════════════════════════════════════════
# Bitget TIDAK ada di Coinalyze. Pakai Binance (A) + Bybit (6) sebagai proxy.
# Coin yang sama (SOLUSDT, FETUSDT, dll.) bergerak hampir identik karena arbitrase.
# Data derivatives ini sebagai CONTEXT INFO (tidak masuk scoring).
CLZ = {
    "api_key":    os.getenv("COINALYZE_API_KEY",
                            "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "base":       "https://api.coinalyze.net/v1",

    # Symbol suffix dari probe: Binance=PERP.A, Bybit=.6
    "bn_suffix":  "_PERP.A",   # BTCUSDT → BTCUSDT_PERP.A
    "by_suffix":  ".6",        # BTCUSDT → BTCUSDT.6

    # Rate limit: 40 req/menit → sleep 1.6s antar call
    "min_interval": 1.6,
    "batch_size":    10,       # konservatif (doc max=20, tiap sym = 1 call)

    # Lookback
    "lookback_h":    48,       # 48 jam untuk OI, L/S, liquidations
    "funding_h":     72,       # 72 jam untuk funding rate trend

    # Timeout per TESTING kandidat — Coinalyze hanya dipanggil untuk candidates
    # yang sudah lolos gate, tidak untuk semua 470 coin
    "timeout":       12,
}

_clz_last: float = 0.0

def _clz_get(endpoint: str, params: dict) -> Optional[Any]:
    """Coinalyze GET dengan rate limit enforcement."""
    global _clz_last
    elapsed = time.time() - _clz_last
    if elapsed < CLZ["min_interval"]:
        time.sleep(CLZ["min_interval"] - elapsed)
    _clz_last = time.time()

    p = dict(params)
    p["api_key"] = CLZ["api_key"]
    try:
        r = requests.get(f"{CLZ['base']}/{endpoint}", params=p,
                         timeout=CLZ["timeout"])
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 10)) + 1
            log.warning(f"Coinalyze 429 — tunggu {retry}s")
            time.sleep(retry)
            return None
        if r.status_code == 401:
            log.error("Coinalyze: API key invalid")
            return None
        if r.status_code == 400:
            log.debug(f"Coinalyze 400 {endpoint} params={params}")
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug(f"Coinalyze error {endpoint}: {e}")
        return None

def _bn_sym(base_usdt: str) -> str:
    """SOLUSDT → SOLUSDT_PERP.A"""
    return base_usdt + CLZ["bn_suffix"]

def _by_sym(base_usdt: str) -> str:
    """SOLUSDT → SOLUSDT.6"""
    # Bybit tidak selalu punya semua coin — fallback ke Binance jika gagal
    return base_usdt.replace("USDT", "USDT") + CLZ["by_suffix"]


class CoinalyzeData:
    """
    Fetch semua derivatif data untuk satu coin.
    Dipanggil hanya untuk TESTING candidates yang lolos semua gate.

    Semua nilai dalam bentuk mentah dari API, diinterpretasikan di caller.
    """

    @staticmethod
    def fetch_ohlcv_btx(symbol_usdt: str) -> Optional[dict]:
        """
        OHLCV + btx/bv/tx dari Binance.
        Probe: field t,o,h,l,c,v,bv,tx,btx — 0% null untuk Binance.
        Return: dict dengan last_candle dan baseline (48H)
        """
        bn  = _bn_sym(symbol_usdt)
        now = int(time.time())
        data = _clz_get("ohlcv-history", {
            "symbols":  bn,
            "interval": "1hour",
            "from":     now - CLZ["lookback_h"] * 3600,
            "to":       now,
        })
        if not isinstance(data, list) or not data:
            return None
        hist = data[0].get("history", [])
        if len(hist) < 5:
            return None

        last    = hist[-2]  # confirmed bar, bukan live
        prev24  = hist[-26:-2] if len(hist) >= 26 else hist[:-2]

        btx     = last.get("btx", 0) or 0
        tx      = last.get("tx",  0) or 0
        bv      = last.get("bv",  0) or 0
        v       = last.get("v",   0) or 0

        btx_ratio = btx / tx if tx > 0 else 0.0
        bv_ratio  = bv  / v  if v  > 0 else 0.0

        # Baseline btx_ratio untuk Z-score
        baseline = []
        for c in prev24:
            c_tx  = c.get("tx",  0) or 0
            c_btx = c.get("btx", 0) or 0
            if c_tx > 0:
                baseline.append(c_btx / c_tx)

        btx_z = 0.0
        if len(baseline) >= 5:
            mu    = sum(baseline) / len(baseline)
            sigma = math.sqrt(sum((x-mu)**2 for x in baseline) / len(baseline))
            btx_z = (btx_ratio - mu) / sigma if sigma > 0 else 0.0

        return {
            "btx":       btx,
            "tx":        tx,
            "bv":        bv,
            "v":         v,
            "btx_ratio": round(btx_ratio, 4),
            "bv_ratio":  round(bv_ratio,  4),
            "btx_z":     round(btx_z,     3),
            "source":    "Binance",
        }

    @staticmethod
    def fetch_oi(symbol_usdt: str) -> Optional[dict]:
        """
        Open Interest history dari Binance.
        Probe: fields t,o,h,l,c — value dalam USD (convert_to_usd=true).
        OI change = (c[-1] - c[-2]) / c[-2] * 100
        OI trend  = arah 4H terakhir
        """
        bn  = _bn_sym(symbol_usdt)
        now = int(time.time())
        data = _clz_get("open-interest-history", {
            "symbols":       bn,
            "interval":      "1hour",
            "from":          now - CLZ["lookback_h"] * 3600,
            "to":            now,
            "convert_to_usd": "true",
        })
        if not isinstance(data, list) or not data:
            return None
        hist = data[0].get("history", [])
        if len(hist) < 3:
            return None

        curr   = hist[-1].get("c", 0) or 0
        prev1h = hist[-2].get("c", 0) or 0
        prev4h = hist[-5].get("c", 0) if len(hist) >= 5 else prev1h

        oi_chg_1h = (curr - prev1h) / prev1h * 100 if prev1h > 0 else 0.0
        oi_chg_4h = (curr - prev4h) / prev4h * 100 if prev4h > 0 else 0.0

        # OI divergence: OI naik saat harga turun = short buildup
        # Harga tidak tersedia di sini; caller akan hitung jika butuh

        return {
            "oi_usd":     round(curr,       0),
            "oi_chg_1h":  round(oi_chg_1h,  3),
            "oi_chg_4h":  round(oi_chg_4h,  3),
            "source":     "Binance",
        }

    @staticmethod
    def fetch_funding(symbol_usdt: str) -> Optional[dict]:
        """
        Funding rate history dari Binance.
        Probe: HTTP 400 saat interval=8hour. Fix: pakai interval=1hour.
        value = per-period rate. 0.007073 = 0.7073% per 8 jam.
        Negatif = shorts bayar longs (short squeeze fuel).
        """
        bn  = _bn_sym(symbol_usdt)
        now = int(time.time())
        data = _clz_get("funding-rate-history", {
            "symbols":  bn,
            "interval": "1hour",    # FIX: bukan 8hour (tidak ada di enum)
            "from":     now - CLZ["funding_h"] * 3600,
            "to":       now,
        })
        if not isinstance(data, list) or not data:
            return None
        hist = data[0].get("history", [])
        # Filter hanya entry dengan nilai non-zero (funding settle per 8H,
        # sehingga mayoritas candle 1H akan bernilai 0)
        nonzero = [c.get("c", 0) for c in hist if (c.get("c") or 0) != 0]
        if not nonzero:
            return None

        current = nonzero[-1]
        trend   = nonzero[-1] - nonzero[0] if len(nonzero) >= 2 else 0.0
        neg_pct = sum(1 for v in nonzero if v < 0) / len(nonzero) * 100

        return {
            "funding_current": round(current,  6),
            "funding_trend":   round(trend,    6),
            "funding_neg_pct": round(neg_pct,  1),
            "funding_periods": len(nonzero),
            "source":          "Binance",
        }

    @staticmethod
    def fetch_ls_ratio(symbol_usdt: str) -> Optional[dict]:
        """
        Long/Short ratio dari Bybit.
        Probe: field l=long%, s=short%, r=ratio. Bybit punya 542 USDT perps.
        l + s = 100. Crowded short = l < 40.
        """
        by  = _by_sym(symbol_usdt)
        now = int(time.time())
        data = _clz_get("long-short-ratio-history", {
            "symbols":  by,
            "interval": "1hour",
            "from":     now - CLZ["lookback_h"] * 3600,
            "to":       now,
        })
        if not isinstance(data, list) or not data:
            # Fallback: coba Binance jika Bybit tidak punya coin ini
            bn   = _bn_sym(symbol_usdt)
            data = _clz_get("long-short-ratio-history", {
                "symbols":  bn,
                "interval": "1hour",
                "from":     now - CLZ["lookback_h"] * 3600,
                "to":       now,
            })
            if not isinstance(data, list) or not data:
                return None

        hist = data[0].get("history", [])
        if not hist:
            return None

        last     = hist[-1]
        long_pct = last.get("l", 50.0) or 50.0
        short_pct= last.get("s", 50.0) or 50.0
        ratio    = last.get("r", 1.0)  or 1.0

        # Trend: apakah longs makin berkurang (bearish momentum)
        long_trend = 0.0
        if len(hist) >= 5:
            long_trend = last.get("l", 50) - hist[-5].get("l", 50)

        return {
            "long_pct":   round(long_pct,  2),
            "short_pct":  round(short_pct, 2),
            "ls_ratio":   round(ratio,     4),
            "long_trend": round(long_trend,2),  # + = longs bertambah, - = berkurang
            "source":     "Bybit",
        }

    @staticmethod
    def fetch_liquidations(symbol_usdt: str) -> Optional[dict]:
        """
        Liquidation history dari Binance.
        Probe: field l=long_liq, s=short_liq (USD per jam).
        Short liq besar = forced buying → short squeeze fuel.
        """
        bn  = _bn_sym(symbol_usdt)
        now = int(time.time())
        data = _clz_get("liquidation-history", {
            "symbols":       bn,
            "interval":      "1hour",
            "from":          now - CLZ["lookback_h"] * 3600,
            "to":            now,
            "convert_to_usd": "true",
        })
        if not isinstance(data, list) or not data:
            return None
        hist = data[0].get("history", [])
        if not hist:
            return None

        last        = hist[-1]
        long_liq_1h = last.get("l", 0) or 0   # long liquidations (bearish)
        short_liq_1h= last.get("s", 0) or 0   # short liquidations (bullish)
        long_liq_4h = sum(c.get("l", 0) or 0 for c in hist[-4:])
        short_liq_4h= sum(c.get("s", 0) or 0 for c in hist[-4:])
        long_liq_24h= sum(c.get("l", 0) or 0 for c in hist[-24:])
        short_liq_24h=sum(c.get("s", 0) or 0 for c in hist[-24:])

        # Dominance: short_liq / (long_liq + short_liq)
        total_24h = long_liq_24h + short_liq_24h
        short_dom = short_liq_24h / total_24h if total_24h > 0 else 0.5

        return {
            "short_liq_1h":  round(short_liq_1h,  0),
            "long_liq_1h":   round(long_liq_1h,   0),
            "short_liq_4h":  round(short_liq_4h,  0),
            "long_liq_4h":   round(long_liq_4h,   0),
            "short_liq_24h": round(short_liq_24h, 0),
            "long_liq_24h":  round(long_liq_24h,  0),
            "short_dom_24h": round(short_dom,      3),  # > 0.6 = short squeeze fuel
            "source":        "Binance",
        }

    @classmethod
    def fetch_all(cls, symbol_usdt: str) -> dict:
        """
        Fetch semua data derivatives untuk satu coin.
        Tiap call dilindungi try/except — partial data lebih baik dari crash.
        Return: dict kosong jika semua gagal.
        """
        result = {}

        try:
            d = cls.fetch_ohlcv_btx(symbol_usdt)
            if d: result["btx"] = d
        except Exception as e:
            log.debug(f"  CLZ btx {symbol_usdt}: {e}")

        try:
            d = cls.fetch_oi(symbol_usdt)
            if d: result["oi"] = d
        except Exception as e:
            log.debug(f"  CLZ oi {symbol_usdt}: {e}")

        try:
            d = cls.fetch_funding(symbol_usdt)
            if d: result["funding"] = d
        except Exception as e:
            log.debug(f"  CLZ funding {symbol_usdt}: {e}")

        try:
            d = cls.fetch_ls_ratio(symbol_usdt)
            if d: result["ls"] = d
        except Exception as e:
            log.debug(f"  CLZ ls {symbol_usdt}: {e}")

        try:
            d = cls.fetch_liquidations(symbol_usdt)
            if d: result["liq"] = d
        except Exception as e:
            log.debug(f"  CLZ liq {symbol_usdt}: {e}")

        return result


# ══════════════════════════════════════════════════════════════════════════════
#  💾  STATE FILE
# ══════════════════════════════════════════════════════════════════════════════
def _load_state() -> dict:
    try:
        path = CONFIG["cooldown_file"]
        if os.path.exists(path):
            with open(path) as f:
                state = json.load(f)
            now = time.time()
            state["cooldowns"] = {
                k: v for k, v in state.get("cooldowns", {}).items()
                if now - v < CONFIG["alert_cooldown_sec"]
            }
            return state
    except Exception:
        pass
    return {"cooldowns": {}}

def _save_state(state: dict) -> None:
    try:
        with open(CONFIG["cooldown_file"], "w") as f:
            json.dump(state, f)
    except Exception:
        pass

_state = _load_state()
log.info(f"State loaded: {len(_state['cooldowns'])} cooldowns")

def is_on_cooldown(sym: str) -> bool:
    return (time.time() - _state["cooldowns"].get(sym, 0)) < CONFIG["alert_cooldown_sec"]

def set_cooldown(sym: str) -> None:
    _state["cooldowns"][sym] = time.time()
    _save_state(_state)


# ══════════════════════════════════════════════════════════════════════════════
#  📐  MATH UTILITIES
# ══════════════════════════════════════════════════════════════════════════════
def _mean(arr: list) -> float:
    return sum(arr) / len(arr) if arr else 0.0

def _std(arr: list) -> float:
    if len(arr) < 2:
        return 0.0
    m = _mean(arr)
    return math.sqrt(sum((x - m) ** 2 for x in arr) / len(arr))

def _zscore(value: float, series: list, min_samples: int = 10) -> float:
    if len(series) < min_samples:
        return 0.0
    sigma = _std(series)
    return (value - _mean(series)) / sigma if sigma > 0 else 0.0

def _linear_score(value: float, strong: float, medium: float, weight: float) -> float:
    """
    Interpolasi linear [0, weight] dari value relatif terhadap strong/medium.
    value >= strong  → weight (full)
    value >= medium  → weight/2 .. weight (partial)
    value >= 0       → 0 .. weight/2 (minimal)
    value < 0        → 0
    """
    if value >= strong:
        return weight
    if value >= medium:
        ratio = (value - medium) / (strong - medium)
        return weight * 0.5 + ratio * weight * 0.5
    if value >= 0:
        ratio = value / medium if medium > 0 else 0.0
        return ratio * weight * 0.5
    return 0.0


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET CLIENT
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
                    log.warning("Bitget rate limit — tunggu 30s")
                    time.sleep(30); continue
                if e.response.status_code not in (400, 404):
                    log.warning(f"Bitget HTTP {e.response.status_code}")
                break
            except Exception:
                if attempt < 2:
                    time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        """GET /api/v2/mix/market/tickers — semua USDT-Futures."""
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers",
                        params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {d["symbol"]: d for d in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, granularity: str = "1H",
                    limit: int = 200) -> List[dict]:
        """
        GET /api/v2/mix/market/candles
        Mendukung limit > 200 via pagination otomatis.
        """
        cache_key = f"{symbol}:{granularity}:{limit}"
        if cache_key in cls._candle_cache:
            return cls._candle_cache[cache_key]

        def _parse(raw_rows: list) -> List[dict]:
            out = []
            for row in raw_rows:
                try:
                    vol = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                    out.append({
                        "ts":    int(row[0]),
                        "open":  float(row[1]),
                        "high":  float(row[2]),
                        "low":   float(row[3]),
                        "close": float(row[4]),
                        "vol":   vol,
                    })
                except (IndexError, ValueError):
                    continue
            return out

        if limit <= 200:
            data = cls._get(
                f"{cls.BASE}/api/v2/mix/market/candles",
                params={"symbol": symbol, "productType": "USDT-FUTURES",
                        "granularity": granularity, "limit": limit}
            )
            if not data or data.get("code") != "00000":
                return []
            result = sorted(_parse(data.get("data", [])), key=lambda x: x["ts"])
            cls._candle_cache[cache_key] = result
            return result

        # Pagination untuk limit > 200
        collected: Dict[int, dict] = {}
        end_time = None
        for _ in range(math.ceil(limit / 200)):
            params = {"symbol": symbol, "productType": "USDT-FUTURES",
                      "granularity": granularity, "limit": 200}
            if end_time is not None:
                params["endTime"] = str(end_time)
            data = cls._get(f"{cls.BASE}/api/v2/mix/market/candles", params=params)
            if not data or data.get("code") != "00000":
                break
            raw = data.get("data", [])
            if not raw:
                break
            for c in _parse(raw):
                collected[c["ts"]] = c
            if len(raw) < 200:
                break
            end_time = min(c["ts"] for c in _parse(raw)) - 1
            if len(collected) >= limit:
                break
            time.sleep(0.15)

        result = sorted(collected.values(), key=lambda x: x["ts"])[-limit:]
        cls._candle_cache[cache_key] = result
        return result

    @classmethod
    def clear_cache(cls) -> None:
        cls._candle_cache.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  📊  INDICATORS (Pine Script compatible)
# ══════════════════════════════════════════════════════════════════════════════

def _wilder_ema(values: list, period: int) -> list:
    """Wilder's RMA — Pine's ta.atr internal."""
    if not values:
        return []
    alpha  = 1.0 / period
    result = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1.0 - alpha) * result[-1])
    return result

def _std_ema(values: list, period: int) -> list:
    """Standard EMA — Pine's ta.ema."""
    if not values:
        return []
    alpha  = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1.0 - alpha) * result[-1])
    return result

def compute_delta_volume(candles: List[dict]) -> List[float]:
    """Pine: upAndDownVolume() — lines 23-38. Signed volume per candle."""
    result = []
    is_buy = True
    for c in candles:
        if   c["close"] > c["open"]: is_buy = True
        elif c["close"] < c["open"]: is_buy = False
        result.append(c["vol"] if is_buy else -c["vol"])
    return result

def compute_vol_thresholds(dv: List[float], vol_len: int) -> Tuple[List[float], List[float]]:
    """Pine: vol_hi = ta.highest(Vol/2.5, vol_len), vol_lo = ta.lowest(...)"""
    n      = len(dv)
    scaled = [v / 2.5 for v in dv]
    vol_hi = [max(scaled[max(0, i - vol_len + 1): i + 1]) for i in range(n)]
    vol_lo = [min(scaled[max(0, i - vol_len + 1): i + 1]) for i in range(n)]
    return vol_hi, vol_lo

def compute_atr(candles: List[dict], period: int = 200) -> List[float]:
    """Pine: ta.atr(200) — Wilder's smoothed ATR."""
    trs = []
    for i, c in enumerate(candles):
        pc  = candles[i-1]["close"] if i > 0 else c["close"]
        trs.append(max(c["high"] - c["low"],
                       abs(c["high"] - pc),
                       abs(c["low"]  - pc)))
    return _wilder_ema(trs, period)

def compute_ema(values: list, period: int) -> List[float]:
    return _std_ema(values, period)

def compute_bbw(candles: List[dict], period: int = 20, mult: float = 2.0) -> List[float]:
    """
    Bollinger Band Width = (upper - lower) / middle.
    Normalized: output adalah desimal relatif terhadap SMA.
    Dari data empiris: HIT avg BBW = 0.137 vs MISS avg = 0.057 (2.4× ratio).
    """
    closes = [c["close"] for c in candles]
    n      = len(closes)
    result = [0.0] * n
    for i in range(period - 1, n):
        window = closes[i - period + 1: i + 1]
        sma    = sum(window) / period
        sd     = math.sqrt(sum((x - sma) ** 2 for x in window) / period)
        result[i] = (mult * 2 * sd / sma) if sma > 0 else 0.0
    return result

def compute_vol_compression(candles: List[dict], fast: int = 5,
                             slow: int = 20) -> float:
    """
    Volume compression: avg vol fast period / avg vol slow period.
    Dari data empiris: HIT avg = 3.76 vs MISS avg = 1.42.
    Nilai > 1.0 = volume recent lebih tinggi dari baseline (spike).
    """
    if len(candles) < slow + fast + 1:
        return 1.0
    vols      = [c["vol"] for c in candles]
    recent    = vols[-fast:]
    prior_end = len(vols) - fast
    prior     = vols[max(0, prior_end - slow): prior_end]
    avg_r     = _mean(recent)
    avg_p     = _mean(prior)
    return avg_r / avg_p if avg_p > 0 else 1.0

def compute_vol_z4h(candles: List[dict], window: int = 96) -> float:
    """
    Volume Z-score vs 4H window (96 candles 1H = 4 hari).
    Dari data empiris: HIT avg Z = 1.03 vs MISS avg = 0.16.
    Gunakan candle[-2] (confirmed bar, bukan live).
    """
    vols = [c["vol"] for c in candles]
    if len(vols) < window + 5:
        return 0.0
    cur_vol  = vols[-2]
    baseline = vols[-(window + 5):-5]
    return _zscore(cur_vol, baseline, min_samples=20)

def compute_vol_ratio(candles: List[dict], period: int = 20) -> float:
    """
    Current vol / rolling avg vol (period candles sebelumnya).
    Dari data empiris: HIT avg = 2.63 vs MISS avg = 1.38.
    Gunakan candle[-2].
    """
    vols = [c["vol"] for c in candles]
    if len(vols) < period + 2:
        return 1.0
    cur_vol  = vols[-2]
    baseline = vols[-(period + 2):-2]
    avg      = _mean(baseline)
    return cur_vol / avg if avg > 0 else 1.0

def compute_atr_pct(candles: List[dict], atr_arr: List[float]) -> float:
    """ATR sebagai % dari harga saat ini. Dari data: HIT avg=3.12% vs MISS=1.40%."""
    price = candles[-1]["close"]
    if price <= 0 or not atr_arr:
        return 0.0
    return atr_arr[-1] / price * 100

def compute_bear_streak(candles: List[dict]) -> int:
    """
    Berapa candle bearish berturut-turut sebelum candle terakhir.
    Dari data: streak 3-4 = hit 3.9-4.7% vs base 2.9%.
    """
    streak = 0
    for c in reversed(candles[:-1]):   # exclude last (live)
        if c["close"] < c["open"]:
            streak += 1
        else:
            break
        if streak >= 6:
            break
    return streak


# ══════════════════════════════════════════════════════════════════════════════
#  📐  ENTRY / SL / TP  (per-coin, bukan fixed %)
# ══════════════════════════════════════════════════════════════════════════════

def find_resistance_levels(candles: List[dict], entry: float,
                           atr: float, lookback: int = 10) -> List[float]:
    """
    Cari pivot highs di atas entry price — kandidat TP resistance.
    Lookback lebih kecil (10) dari zone detection (20) karena
    resistance tidak harus sekuat support.

    Juga tambahkan swing high dari 100 candle terakhir jika relevan.
    """
    highs = [c["high"] for c in candles]
    n     = len(highs)
    found: set = set()

    for i in range(lookback, n - lookback):
        val = highs[i]
        if val < entry * 1.003:          # minimal 0.3% di atas entry
            continue
        if (val >= max(highs[max(0, i - lookback): i]) and
                val >= max(highs[i + 1: i + lookback + 1])):
            found.add(round(val, 10))

    # Recent swing high (last 100 candles) sebagai kandidat tambahan
    window = min(100, n)
    recent_max = max(highs[-window:])
    if recent_max > entry * 1.01:        # minimal 1% di atas entry
        found.add(round(recent_max, 10))

    # Hanya yang benar-benar di atas entry
    return sorted(r for r in found if r > entry * 1.002)


def compute_trade_setup(
    candles:  List[dict],
    zone:     dict,
    atr_arr:  List[float],
    bbw:      float,
    price:    float,
) -> dict:
    """
    Hitung Entry, SL, TP per coin berdasarkan kondisi masing-masing.

    ── ENTRY ─────────────────────────────────────────────────────────────────
    · LIMIT di zone_top  → jika harga masih di atas zone_top
    · MARKET di harga saat ini → jika sudah di dalam zone

    ── SL ────────────────────────────────────────────────────────────────────
    · zone_bottom − 1.0×ATR
    · Rasionale: zone_bottom = pivot_low − ATR_formasi.
      Turun 1 ATR lagi dari sana = struktur zone benar-benar rusak.
      Lebih akurat dari fixed % karena mempertimbangkan volatility coin.

    ── TP (per-coin, 3 metode) ───────────────────────────────────────────────
    Metode 1 — ATR Projection (basis volatility regime dari BBW):
      BBW ≥ 0.150  → 5.0×ATR   (sangat volatile, potensi move besar)
      BBW ≥ 0.078  → 3.5×ATR   (volatile)
      BBW ≥ 0.050  → 2.5×ATR   (medium-high, minimum gate)

    Metode 2 — Resistance Level (pivot high terdekat di atas entry):
      Dipakai jika R:R ≥ 2.0 dari SL yang sudah dihitung.
      Jika resistance terlalu dekat (R:R < 2.0), gunakan ATR projection.

    Metode 3 — Minimum R:R floor = 2.0:1
      TP tidak pernah kurang dari entry + 2.0 × risk.

    Final TP = nilai tertinggi yang masih logis:
      Jika resistance ada DAN lebih dekat dari ATR target DAN R:R ≥ 2.0:
        → gunakan resistance (lebih konservatif, lebih mungkin tercapai)
      Else:
        → ATR projection (lebih agresif)
      Kemudian enforce min R:R 2.0:1.
    """
    atr = atr_arr[-1] if atr_arr else price * 0.02

    # ── ENTRY ──────────────────────────────────────────────────────────────
    zone_top = zone["zone_top"]
    zone_bot = zone["zone_bottom"]

    if price <= zone_top:
        entry      = price       # sudah dalam zone → masuk market
        entry_type = "MARKET"
    else:
        entry      = zone_top    # di atas zone → limit di zone_top
        entry_type = "LIMIT"

    # ── SL ─────────────────────────────────────────────────────────────────
    sl = zone_bot - atr * 1.0
    sl = max(sl, price * 0.0001)   # tidak boleh negatif/nol

    risk = entry - sl
    if risk <= 0:
        risk = atr                 # fallback: risk = 1 ATR

    # ── RESISTANCE ─────────────────────────────────────────────────────────
    res_levels = find_resistance_levels(candles, entry, atr)

    # ── TP METODE 1: ATR Projection ─────────────────────────────────────────
    if   bbw >= 0.150: atr_mult, regime_label = 5.0, "volatile"
    elif bbw >= 0.078: atr_mult, regime_label = 3.5, "high"
    else:              atr_mult, regime_label = 2.5, "medium"

    tp_atr = entry + atr * atr_mult

    # ── TP METODE 2: Resistance level ──────────────────────────────────────
    tp_res    = None
    res_label = ""
    for res in res_levels:
        rr_res = (res - entry) / risk
        if rr_res >= 2.0:          # hanya pakai jika R:R ≥ 2.0
            tp_res    = res
            res_label = f"R:R{rr_res:.1f}"
            break

    # ── TP METODE 3: Floor R:R = 2.0 ───────────────────────────────────────
    tp_floor = entry + risk * 2.0

    # ── FINAL TP SELECTION ──────────────────────────────────────────────────
    if tp_res is not None and tp_res < tp_atr:
        # Resistance lebih dekat dari ATR target
        # Gunakan resistance (lebih konservatif tapi lebih realistis)
        tp        = tp_res
        tp_method = f"Resist {res_label}"
    else:
        # Pakai ATR projection
        tp        = tp_atr
        extra     = f"+Res@{tp_res:.5f}" if tp_res else ""
        tp_method = f"ATR×{atr_mult:.1f}({regime_label}){extra}"

    # Pastikan minimum R:R = 2.0
    if tp < tp_floor:
        tp        = tp_floor
        tp_method = "MinRR2.0"

    # ── METRICS ────────────────────────────────────────────────────────────
    risk_pct   = (entry - sl) / entry * 100    if entry > 0 else 0
    reward_pct = (tp    - entry) / entry * 100 if entry > 0 else 0
    rr         = reward_pct / risk_pct         if risk_pct > 0 else 0

    return {
        "entry":      round(entry, 8),
        "entry_type": entry_type,
        "sl":         round(sl, 8),
        "tp":         round(tp, 8),
        "tp_method":  tp_method,
        "risk_pct":   round(risk_pct, 2),
        "reward_pct": round(reward_pct, 2),
        "rr":         round(rr, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🟩  ZONE DETECTION  (Pine Script identical)
# ══════════════════════════════════════════════════════════════════════════════

def find_pivot_lows(lows: List[float], lookback: int) -> List[Optional[float]]:
    """Pine: ta.pivotlow(src, lookbackPeriod, lookbackPeriod) — line 67."""
    n      = len(lows)
    result = [None] * n
    for i in range(lookback, n - lookback):
        val = lows[i]
        if val <= min(lows[i - lookback: i]) and \
           val <= min(lows[i + 1: i + lookback + 1]):
            result[i] = val
    return result

def detect_support_zones(candles: List[dict]) -> List[dict]:
    """
    Pine: calcSupportResistance() — support branch.
    Pivot low + Vol > vol_hi → zone terbentuk.
    Hitung touch_count dan break_count dari sisa candle.
    """
    cfg  = CONFIG
    lb   = cfg["lookback_period"]
    vl   = cfg["vol_len"]
    bw   = cfg["box_width_multiplier"]
    ap   = cfg["atr_period"]

    if len(candles) < lb * 2 + 10:
        return []

    dv      = compute_delta_volume(candles)
    vh, _   = compute_vol_thresholds(dv, vl)
    atr     = compute_atr(candles, ap)
    lows    = [c["low"] for c in candles]
    pivots  = find_pivot_lows(lows, lb)

    zones = []
    n     = len(candles)

    for i, piv in enumerate(pivots):
        if piv is None:
            continue
        if dv[i] <= vh[i]:              # Pine line 74: Vol > vol_hi
            continue
        if math.isnan(atr[i]) or atr[i] <= 0:
            continue

        zone_top    = piv
        zone_bottom = piv - atr[i] * bw
        break_count = 0
        touch_count = 0
        in_zone     = False
        broke_at    = n

        for j in range(i + lb, n):
            if j >= broke_at:
                break
            c_low  = candles[j]["low"]
            c_high = candles[j]["high"]
            if not in_zone:
                if zone_bottom <= c_low <= zone_top:
                    in_zone      = True
                    touch_count += 1
            else:
                if c_low > zone_top:
                    in_zone = False
                elif c_high < zone_bottom:
                    break_count += 1
                    in_zone  = False
                    broke_at = j

        zones.append({
            "zone_top":    zone_top,
            "zone_bottom": zone_bottom,
            "touch_count": touch_count,
            "break_count": break_count,
        })

    return zones

def get_zone_state(zone: dict, c_low: float, c_high: float) -> str:
    """Pine: brekout_sup/sup_holds — lines 105-120."""
    top = zone["zone_top"]; bot = zone["zone_bottom"]
    if c_high < bot:             return "BROKEN"
    if bot <= c_low <= top:      return "TESTING"
    return "VALID"


# ══════════════════════════════════════════════════════════════════════════════
#  🌍  REGIME CHECK  (SMA50 daily — Bitget max ~90 candles 1D dari probe)
# ══════════════════════════════════════════════════════════════════════════════
def is_caution_mode(btc_1d: List[dict], eth_1d: List[dict]) -> Tuple[bool, dict]:
    """
    CAUTION MODE: BTC dan ETH KEDUANYA di bawah SMA50 daily.

    Dari probe lapangan: Bitget 1D candles max ~90 dalam praktik.
    SMA50 dari 90 candles = 3 bulan data = valid untuk trend detection.
    Dual method:
    - Primary  : SMA50 jika n >= 50
    - Fallback : EMA200 jika n >= 200 (belum pernah terjadi di Bitget 1D)
    """
    period = CONFIG["ema_regime_period"]   # 50

    def _check(candles: List[dict], name: str) -> Tuple[bool, str]:
        if not candles:
            return False, f"{name}: no data"
        closes = [c["close"] for c in candles]
        price  = closes[-1]
        n      = len(closes)
        if n >= 200:
            ema   = _std_ema(closes, 200)
            below = price < ema[-1]
            return below, (f"{name} ${price:,.0f} vs EMA200 ${ema[-1]:,.0f} "
                           f"n={n} ({'BELOW' if below else 'ABOVE'})")
        elif n >= period:
            sma   = sum(closes[-period:]) / period
            below = price < sma
            return below, (f"{name} ${price:,.0f} vs SMA{period} ${sma:,.0f} "
                           f"n={n} ({'BELOW' if below else 'ABOVE'})")
        return False, f"{name}: n={n} < {period}, insufficient"

    btc_below, btc_msg = _check(btc_1d, "BTC")
    eth_below, eth_msg = _check(eth_1d, "ETH")
    caution = btc_below and eth_below
    return caution, {"btc": btc_msg, "eth": eth_msg, "caution": caution}


# ══════════════════════════════════════════════════════════════════════════════
#  🎯  SCORING  (3 komponen, total 100 pts, semua dari data empiris)
# ══════════════════════════════════════════════════════════════════════════════

def score_A_volatility(bbw: float, atr_pct: float) -> Tuple[int, dict]:
    """
    Komponen A: Volatility Score (0-50 pts)
    BASIS DATA: BBW corr=+0.236, ATR_pct corr=+0.230 (terkuat di kedua dataset)
    HIT avg BBW = 0.137 (2.4× MISS avg 0.057)
    HIT avg ATR_pct = 3.12% (2.2× MISS avg 1.40%)

    Sub-A1: BBW score (0-27.5 pts = 55% dari 50)
    Sub-A2: ATR_pct score (0-22.5 pts = 45% dari 50)
    """
    cfg = CONFIG

    # A1: BBW
    a1_raw = _linear_score(bbw, cfg["bbw_strong"], cfg["bbw_medium"], 1.0)
    a1_pts = round(a1_raw * cfg["score_vol_max"] * cfg["bbw_weight"])

    # A2: ATR_pct
    a2_raw = _linear_score(atr_pct, cfg["atr_strong"], cfg["atr_medium"], 1.0)
    a2_pts = round(a2_raw * cfg["score_vol_max"] * cfg["atr_weight"])

    total = min(a1_pts + a2_pts, cfg["score_vol_max"])
    return total, {"a1_bbw": a1_pts, "a2_atr": a2_pts,
                   "bbw": round(bbw, 4), "atr_pct": round(atr_pct, 2)}


def score_B_volume(vol_compression: float, vol_z4h: float) -> Tuple[int, dict]:
    """
    Komponen B: Volume Score (0-30 pts)
    BASIS DATA: vol_compression corr=+0.089, vol_z4h corr=+0.083 (confirmed kedua dataset)

    Sub-B1: Vol Compression (0-16.5 pts = 55% dari 30)
    Mengukur: spike volume RECENT vs baseline (akumulasi short-term)
    HIT avg = 3.76 vs MISS avg = 1.42

    Sub-B2: Vol Z-score 4H (0-13.5 pts = 45% dari 30)
    Mengukur: anomali volume vs window 4H historis (akumulasi longer-term)
    HIT avg Z = 1.03 vs MISS avg = 0.16
    BERBEDA dari B1: B1=recent spike, B2=historical anomaly
    """
    cfg = CONFIG

    # B1: Vol Compression
    b1_raw = _linear_score(vol_compression, cfg["vol_comp_strong"],
                           cfg["vol_comp_medium"], 1.0)
    b1_pts = round(b1_raw * cfg["score_vol_micro_max"] * cfg["vol_comp_weight"])

    # B2: Vol Z-score 4H
    b2_raw = _linear_score(vol_z4h, cfg["vol_z4h_strong"], cfg["vol_z4h_medium"], 1.0)
    b2_pts = round(b2_raw * cfg["score_vol_micro_max"] * cfg["vol_z4h_weight"])

    total = min(b1_pts + b2_pts, cfg["score_vol_micro_max"])
    return total, {"b1_comp": b1_pts, "b2_z4h": b2_pts,
                   "vol_comp": round(vol_compression, 3),
                   "vol_z4h": round(vol_z4h, 3)}


def score_C_momentum(bear_streak: int, vol_ratio: float) -> Tuple[int, dict]:
    """
    Komponen C: Momentum Score (0-20 pts)
    BASIS DATA: bear_streak corr=+0.030, vol_ratio corr=+0.068 (confirmed kedua dataset)

    Sub-C1: Bear Streak (0-8 pts = 40% dari 20)
    Mengukur: berapa candle bearish berturut-turut sebelum entry
    Data: streak 3-4 = 3.9-4.7% hit rate vs base 2.9%
    Ini adalah oversold setup — harga sudah ditekan cukup untuk bisa bounce

    Sub-C2: Vol Ratio (0-12 pts = 60% dari 20)
    Mengukur: apakah ada volume lebih saat masuk zone
    Data: vol_ratio ≥1.44 = kombinasi BBW+ATR+VolRatio = 12.7% hit rate
    BERBEDA dari B1/B2: ini adalah CURRENT bar vol vs recent avg (bukan spike detection)
    """
    cfg = CONFIG

    # C1: Bear streak
    # streak ≥ 4 = full, streak ≥ 2 = half, streak < 2 = minimal
    if bear_streak >= cfg["bear_streak_strong"]:
        c1_raw = 1.0
    elif bear_streak >= cfg["bear_streak_medium"]:
        c1_raw = 0.5 + 0.5 * (bear_streak - cfg["bear_streak_medium"]) / \
                       (cfg["bear_streak_strong"] - cfg["bear_streak_medium"])
    else:
        c1_raw = bear_streak / cfg["bear_streak_medium"] * 0.5 \
                 if cfg["bear_streak_medium"] > 0 else 0.0

    c1_pts = round(c1_raw * cfg["score_momentum_max"] * cfg["bear_weight"])

    # C2: Vol ratio
    c2_raw = _linear_score(vol_ratio, cfg["vol_ratio_strong"],
                           cfg["vol_ratio_medium"], 1.0)
    c2_pts = round(c2_raw * cfg["score_momentum_max"] * cfg["vol_ratio_weight"])

    total = min(c1_pts + c2_pts, cfg["score_momentum_max"])
    return total, {"c1_bear": c1_pts, "c2_volr": c2_pts,
                   "bear_streak": bear_streak,
                   "vol_ratio": round(vol_ratio, 3)}


# ══════════════════════════════════════════════════════════════════════════════
#  📱  TELEGRAM & OUTPUT
# ══════════════════════════════════════════════════════════════════════════════
def send_telegram(msg: str) -> bool:
    bot  = CONFIG["bot_token"]
    chat = CONFIG["chat_id"]
    if not bot or not chat:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot}/sendMessage",
            json={"chat_id": chat, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        return r.ok
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


def build_table(results: list, caution: bool) -> str:
    mode = "⚠️ CAUTION (thr=80)" if caution else "✅ NORMAL (thr=70)"
    now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "",
        "═" * 160,
        f"  NEXUS-SR v3.2  |  Bitget Futures  |  {now}  |  {mode}",
        "═" * 160,
        f"  {'#':>2}  {'Coin':<13} {'Price':>12}  "
        f"{'Score':>5}  {'A':>3} {'B':>3} {'C':>3}  "
        f"{'BBW':>6} {'ATR%':>5}  "
        f"{'Entry':>12} {'Type':>5}  "
        f"{'SL':>12} {'Risk%':>5}  "
        f"{'TP':>12} {'Rwd%':>5}  {'R:R':>4}  "
        f"── Coinalyze Context ──",
        "─" * 160,
    ]
    for i, r in enumerate(results, 1):
        raw = r["symbol"].replace("USDT", "")

        # Build derivatives context string
        deriv_parts = []
        if r.get("btx_z")       is not None: deriv_parts.append(f"btxZ={r['btx_z']:+.2f}")
        if r.get("oi_chg_1h")   is not None: deriv_parts.append(f"OI1h={r['oi_chg_1h']:+.2f}%")
        if r.get("funding")      is not None: deriv_parts.append(f"fund={r['funding']:.5f}")
        if r.get("long_pct")    is not None: deriv_parts.append(f"L%={r['long_pct']:.1f}")
        if r.get("short_liq_4h") is not None and r["short_liq_4h"] > 0:
            deriv_parts.append(f"sLiq4h=${r['short_liq_4h']:,.0f}")
        deriv_str = "  ".join(deriv_parts) if deriv_parts else "(no CLZ data)"

        lines.append(
            f"  {i:>2}  {raw:<13} {r['price']:>12.6f}  "
            f"{r['score']:>5.1f}  {r['sa']:>3} {r['sb']:>3} {r['sc']:>3}  "
            f"{r['bbw']:>6.4f} {r['atr_pct']:>4.1f}%  "
            f"{r['entry']:>12.6f} {r['entry_type']:>5}  "
            f"{r['sl']:>12.6f} {r['risk_pct']:>4.1f}%  "
            f"{r['tp']:>12.6f} {r['reward_pct']:>4.1f}%  {r['rr']:>4.1f}  "
            f"{deriv_str}  {r['strength']}"
        )
    lines += [
        "─" * 160,
        "  Score: A=Volatility(0-50) B=Volume(0-30) C=Momentum(0-20)",
        "  CLZ context: btxZ=taker_buy_zscore  OI1h=OI_change_1H  fund=funding_rate",
        "               L%=long_position_%  sLiq4h=short_liquidations_4H",
        "  Note: CLZ data dari Binance/Bybit (proxy). Tidak masuk scoring.",
        "═" * 160,
    ]
    return "\n".join(lines)


def build_approaching_table(approaching: list) -> str:
    """Coin dalam 5% di atas zone_top — belum TESTING tapi mendekati."""
    if not approaching:
        return ""
    lines = [
        "",
        "  ↘  APPROACHING ZONES  (0-5% di atas zone_top, belum masuk)",
        "  " + "─" * 90,
        f"  {'#':>2}  {'Coin':<13} {'Price':>12} {'ZoneTop':>10} {'ZoneBot':>10} "
        f"{'Dist%':>6}  {'BBW':>6} {'ATR%':>6}  Vol24h(M)",
        "  " + "─" * 90,
    ]
    for i, a in enumerate(sorted(approaching, key=lambda x: x["dist_pct"]), 1):
        raw = a["symbol"].replace("USDT", "")
        gate_ok = "✓" if a["gate_ok"] else "✗"
        lines.append(
            f"  {i:>2}  {raw:<13} {a['price']:>12.6f} {a['zone_top']:>10.5f} "
            f"{a['zone_bot']:>10.5f} {a['dist_pct']:>5.2f}%  "
            f"{a['bbw']:>6.4f} {a['atr_pct']:>5.2f}%  "
            f"${a['vol_24h_m']:.1f}M  gate:{gate_ok}"
        )
    lines.append("  " + "─" * 90)
    lines.append("  ✓ = lolos gate BBW+ATR  ✗ = belum cukup volatil untuk 15% move")
    return "\n".join(lines)


def build_telegram_msg(results: list, caution: bool,
                       n_tested: int, n_candidates: int) -> str:
    mode = "⚠️ CAUTION" if caution else "🟢 NORMAL"
    now  = datetime.now(timezone.utc).strftime("%H:%M UTC")
    txt  = (
        f"🎯 <b>NEXUS-SR v3.2</b> [{now}]\n"
        f"Mode: {mode} | {len(results)}/{n_tested} signals | Universe: {n_candidates}\n"
        f"{'─'*28}\n"
    )
    for i, r in enumerate(results, 1):
        raw      = r["symbol"].replace("USDT", "")
        rr_emoji = "🔥" if r["rr"] >= 3 else "✅" if r["rr"] >= 2 else "⚠️"

        # Derivatives context lines (hanya yang ada datanya)
        deriv_lines = []
        if r.get("funding") is not None:
            sign = "🔴" if r["funding"] < -0.0001 else "🟡" if r["funding"] < 0 else "🟢"
            deriv_lines.append(f"  {sign} Funding: <code>{r['funding']:.5f}</code> "
                               f"({'neg=squeeze' if r['funding'] < 0 else 'pos=longs'})")
        if r.get("long_pct") is not None:
            crowded = "🐻 crowded short" if r["long_pct"] < 40 else \
                      "🐂 crowded long" if r["long_pct"] > 65 else "⚖️ balanced"
            deriv_lines.append(f"  L/S: Long <code>{r['long_pct']:.1f}%</code> "
                               f"/ Short <code>{r['short_pct']:.1f}%</code>  {crowded}")
        if r.get("oi_chg_1h") is not None:
            oi_sign = "↑" if r["oi_chg_1h"] > 0 else "↓"
            deriv_lines.append(f"  OI: <code>{r['oi_chg_1h']:+.2f}%</code>/1H  "
                               f"<code>{r['oi_chg_4h']:+.2f}%</code>/4H {oi_sign}")
        if r.get("short_liq_4h") is not None and r["short_liq_4h"] > 0:
            deriv_lines.append(f"  Short Liq 4H: <code>${r['short_liq_4h']:,.0f}</code> "
                               f"(squeeze fuel)")
        if r.get("btx_z") is not None:
            btx_label = "⚡ buy aggression" if r["btx_z"] > 1.5 else "normal"
            deriv_lines.append(f"  BuyTx Z: <code>{r['btx_z']:+.2f}</code>  {btx_label}")

        deriv_block = "\n".join(deriv_lines) if deriv_lines else \
                      "  <i>CLZ: no derivatives data</i>"

        txt += (
            f"{i}. <b>{raw}</b>  [{r['strength']}]  Score:<b>{r['score']:.0f}</b>\n"
            f"   Zone: <code>{r['zone_bot']:.5f} – {r['zone_top']:.5f}</code>\n"
            f"   📥 Entry ({r['entry_type']}): <code>{r['entry']:.6f}</code>\n"
            f"   🛑 SL: <code>{r['sl']:.6f}</code> (-{r['risk_pct']:.1f}%)\n"
            f"   🎯 TP: <code>{r['tp']:.6f}</code> (+{r['reward_pct']:.1f}%)"
            f"  {rr_emoji} R:R <b>{r['rr']:.2f}</b>\n"
            f"   📐 <i>{r['tp_method']}</i>\n"
            f"{deriv_block}\n\n"
        )
    txt += (
        "📊 <i>Score dari Bitget OHLCV | CLZ context dari Binance/Bybit proxy\n"
        "⚠️ Paper mode — verifikasi sebelum live trade.</i>"
    )
    return txt


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan() -> None:
    cfg      = CONFIG
    start_ts = time.time()

    log.info("=" * 70)
    log.info(f"  NEXUS-SR v3.2 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 70)

    # ── 1. Regime check (FIX: EMA200 daily) ───────────────────────────────
    log.info("Fetching BTC/ETH daily data untuk regime check …")
    btc_1d  = BitgetClient.get_candles("BTCUSDT", "1D", cfg["candle_limit_1d"])
    eth_1d  = BitgetClient.get_candles("ETHUSDT", "1D", cfg["candle_limit_1d"])
    caution, regime_debug = is_caution_mode(btc_1d, eth_1d)
    threshold = cfg["score_threshold_caution"] if caution else cfg["score_threshold_normal"]
    log.info(f"  {regime_debug['btc']}")
    log.info(f"  {regime_debug['eth']}")
    log.info(f"Market regime: {'⚠️ CAUTION (thr=80)' if caution else '✅ NORMAL (thr=70)'}")

    # ── 2. Fetch tickers ────────────────────────────────────────────────
    log.info("Fetching Bitget USDT-Futures tickers …")
    tickers = BitgetClient.get_tickers()
    if not tickers:
        send_telegram("⚠️ NEXUS-SR v3: Gagal fetch tickers")
        return
    log.info(f"Tickers received: {len(tickers)}")

    # ── 3. Build candidates ────────────────────────────────────────────
    skip_stats   = defaultdict(int)
    candidates   = []

    for sym, t in tickers.items():
        if sym in MANUAL_EXCLUDE:
            skip_stats["excluded"] += 1; continue
        if is_on_cooldown(sym):
            skip_stats["cooldown"] += 1; continue
        try:
            vol = float(t.get("quoteVolume", 0))
            chg = abs(float(t.get("change24h", 0)) * 100)
        except Exception:
            skip_stats["parse_err"] += 1; continue
        if vol < cfg["min_vol_24h"]:
            skip_stats["vol_low"] += 1; continue
        if vol > cfg["max_vol_24h"]:
            skip_stats["vol_high"] += 1; continue
        if chg > cfg["gate_chg_24h_max"]:
            skip_stats["pumped"] += 1; continue
        candidates.append((sym, t))

    log.info(f"Candidates: {len(candidates)} | Skip: {dict(skip_stats)}")

    # ── 4. Phase 1: candle fetch + zone detection + gate filtering ──────
    log.info("Phase 1: candle fetch + zone detection …")
    BitgetClient.clear_cache()

    testing_candidates = []
    all_approaching    = []

    for idx, (sym, ticker) in enumerate(candidates):
        if (idx + 1) % 100 == 0:
            log.info(f"  Phase 1: {idx+1}/{len(candidates)}")

        try:
            price   = float(ticker.get("lastPr", 0))
            vol_24h = float(ticker.get("quoteVolume", 0))
            if price <= 0:
                skip_stats["no_price"] += 1; continue

            candles = BitgetClient.get_candles(sym, "1H", cfg["candle_limit_1h"])
            if len(candles) < cfg["lookback_period"] * 2 + 30:
                skip_stats["candle_short"] += 1; continue

            # ── Zone Gate G3: volume outlier ──────────────────────────────
            # FIX: gunakan candles[-2] (confirmed bar), bukan candles[-1] (live)
            # Candle live volume bisa undercount jika jam baru dimulai
            lb  = cfg["vol_outlier_lookback"]
            cur = candles[-2]["vol"]                           # ← FIX: [-2] bukan [-1]
            avg = _mean([c["vol"] for c in candles[-lb-2:-2]]) # ← baseline sebelum bar itu
            if avg > 0 and cur > cfg["vol_outlier_mult"] * avg:
                skip_stats["vol_outlier"] += 1; continue

            # ── Pre-compute indicators (one pass per coin) ────────────────
            atr_arr  = compute_atr(candles, cfg["atr_period"])
            bbw_arr  = compute_bbw(candles, 20)
            atr_pct  = compute_atr_pct(candles, atr_arr)
            bbw      = bbw_arr[-1]

            # ── Zone detection ────────────────────────────────────────────
            zones = detect_support_zones(candles)
            if not zones:
                skip_stats["no_zones"] += 1; continue

            # ── Zone Gate G1+G2: TESTING state, break_count < 3 ──────────
            testing_zones = []
            for z in zones:
                if z["break_count"] >= cfg["max_break_count"]:
                    continue
                for lookback_i in (1, 2):
                    if lookback_i >= len(candles):
                        continue
                    c = candles[-lookback_i]
                    if get_zone_state(z, c["low"], c["high"]) == "TESTING":
                        testing_zones.append(z)
                        break

            # ── Approaching zones (pre-alert, tidak kirim telegram) ───────
            if not testing_zones:
                for z in zones:
                    if z["break_count"] >= cfg["max_break_count"]:
                        continue
                    state = get_zone_state(z, candles[-1]["low"], candles[-1]["high"])
                    if state == "VALID":
                        dist_pct = (price - z["zone_top"]) / price * 100
                        if 0 < dist_pct <= 5.0:
                            gate_ok = (bbw >= cfg["gate_bbw_min"] and
                                       atr_pct >= cfg["gate_atr_pct_min"])
                            all_approaching.append({
                                "symbol":    sym,
                                "price":     price,
                                "zone_top":  z["zone_top"],
                                "zone_bot":  z["zone_bottom"],
                                "dist_pct":  round(dist_pct, 2),
                                "bbw":       round(bbw, 4),
                                "atr_pct":   round(atr_pct, 2),
                                "vol_24h_m": round(vol_24h / 1_000_000, 1),
                                "gate_ok":   gate_ok,
                            })
                skip_stats["no_testing"] += 1; continue

            # ── Volatility Gate G4+G5 (dari data empiris) ─────────────────
            # Coin yang tidak lolos gate ini secara mekanis tidak bisa 15% move
            if bbw < cfg["gate_bbw_min"]:
                skip_stats["gate_bbw"] += 1; continue
            if atr_pct < cfg["gate_atr_pct_min"]:
                skip_stats["gate_atr"] += 1; continue

            testing_candidates.append({
                "sym":     sym,
                "ticker":  ticker,
                "candles": candles,
                "zones":   testing_zones,
                "atr_arr": atr_arr,
                "bbw_arr": bbw_arr,
                "atr_pct": atr_pct,
                "bbw":     bbw,
                "vol_24h": vol_24h,
            })

        except Exception as e:
            log.debug(f"  Error Phase1 {sym}: {e}")

        time.sleep(cfg["sleep_between_coins"])

    log.info(
        f"Phase 1 done: {len(testing_candidates)} pass all gates | "
        f"approaching: {len(all_approaching)} | "
        f"skip: {dict(skip_stats)}"
    )

    # ── 5. Phase 2: score each testing candidate + Coinalyze derivatives ──
    log.info(f"Phase 2: scoring {len(testing_candidates)} candidates + Coinalyze …")
    results = []

    for d in testing_candidates:
        sym     = d["sym"]
        candles = d["candles"]
        zones   = d["zones"]
        atr_pct = d["atr_pct"]
        bbw     = d["bbw"]

        try:
            price = float(d["ticker"].get("lastPr", 0))
            if price <= 0:
                continue

            # Pilih zona terbaik: paling dekat harga (sudah dalam TESTING state)
            best_zone = min(zones, key=lambda z: abs(z["zone_top"] - price))

            # ── Zone width guard: filter low-liquidity coins ──────────────
            zone_width_pct = (best_zone["zone_top"] - best_zone["zone_bottom"]) \
                             / price * 100 if price > 0 else 999
            if zone_width_pct > cfg["max_zone_width_pct"]:
                log.info(
                    f"  {sym}: zone width {zone_width_pct:.1f}% > "
                    f"{cfg['max_zone_width_pct']}% → low liquidity, skip"
                )
                continue

            # Compute score components
            vol_comp    = compute_vol_compression(candles)
            vol_z4h     = compute_vol_z4h(candles)
            vol_ratio   = compute_vol_ratio(candles)
            bear_streak = compute_bear_streak(candles)

            sa, det_a = score_A_volatility(bbw, atr_pct)
            sb, det_b = score_B_volume(vol_comp, vol_z4h)
            sc, det_c = score_C_momentum(bear_streak, vol_ratio)

            total = sa + sb + sc

            # ── Pump rejection gate ───────────────────────────────────────
            # Pattern: volume spike BESAR + tidak ada bear streak = pump event
            # Bounce legitimate: ada tekanan jual (bear streak) LALU volume masuk
            # Referensi audit: 4USDT vol_ratio=10.66x, bear=0 → pump, bukan bounce
            is_pump = (vol_ratio > cfg["pump_reject_vol_ratio"] and
                       bear_streak < cfg["pump_reject_bear_min"])
            if is_pump:
                log.info(
                    f"  {sym}: PUMP REJECTED — "
                    f"vol_ratio={vol_ratio:.2f}x bear={bear_streak} "
                    f"(pattern: spike without prior selling = pump event)"
                )
                continue

            # ── Minimum B score gate ──────────────────────────────────────
            # Volume harus mengkonfirmasi sinyal — volatility saja tidak cukup
            if sb < cfg["min_score_B"]:
                log.info(
                    f"  {sym}: B={sb} < min {cfg['min_score_B']} — "
                    f"no volume confirmation, skip "
                    f"(A={sa} B={sb} C={sc} total={sa+sb+sc})"
                )
                continue

            log.info(
                f"  {sym}: score={total} (thr={threshold}) | "
                f"A={sa}(BBW={bbw:.4f},ATR={atr_pct:.2f}%) "
                f"B={sb}(VC={vol_comp:.2f}x,VZ4h={vol_z4h:.2f}) "
                f"C={sc}(bear={bear_streak},vr={vol_ratio:.2f}x) | "
                f"Zone={best_zone['zone_top']:.5f} Price={price:.5f}"
            )

            if total < threshold:
                log.info(f"    ❌ {sym} gagal threshold")
                continue

            strength = ("STRONG"   if total >= cfg["score_strong"]           else
                        "MODERATE" if total >= cfg["score_threshold_normal"] else
                        "WEAK")

            # ── Trade setup (Entry / SL / TP per-coin) ───────────────────
            trade = compute_trade_setup(
                candles  = candles,
                zone     = best_zone,
                atr_arr  = d["atr_arr"],
                bbw      = bbw,
                price    = price,
            )

            log.info(
                f"    ✅ {sym} lolos! strength={strength} | "
                f"Entry={trade['entry_type']}@{trade['entry']:.5f} "
                f"SL={trade['sl']:.5f}(-{trade['risk_pct']:.1f}%) "
                f"TP={trade['tp']:.5f}(+{trade['reward_pct']:.1f}%) "
                f"R:R={trade['rr']:.2f} [{trade['tp_method']}]"
            )

            # ── Coinalyze derivatives (context info, tidak masuk score) ──
            # Hanya fetch untuk coin yang sudah lolos semua gate
            deriv = CoinalyzeData.fetch_all(sym)
            btx_d = deriv.get("btx", {})
            oi_d  = deriv.get("oi",  {})
            fund_d= deriv.get("funding", {})
            ls_d  = deriv.get("ls",  {})
            liq_d = deriv.get("liq", {})

            # Log ringkas derivatives
            deriv_summary = []
            if btx_d:  deriv_summary.append(f"btx_z={btx_d.get('btx_z',0):.2f}")
            if oi_d:   deriv_summary.append(f"oi_1h={oi_d.get('oi_chg_1h',0):+.2f}%")
            if fund_d: deriv_summary.append(f"fund={fund_d.get('funding_current',0):.5f}")
            if ls_d:   deriv_summary.append(f"long%={ls_d.get('long_pct',50):.1f}")
            if liq_d:  deriv_summary.append(f"s_liq4h=${liq_d.get('short_liq_4h',0):,.0f}")
            if deriv_summary:
                log.info(f"      CLZ: {' | '.join(deriv_summary)}")

            results.append({
                "symbol":       sym,
                "price":        round(price, 8),
                "zone_top":     round(best_zone["zone_top"], 8),
                "zone_bot":     round(best_zone["zone_bottom"], 8),
                # Trade setup
                "entry":        trade["entry"],
                "entry_type":   trade["entry_type"],
                "sl":           trade["sl"],
                "tp":           trade["tp"],
                "tp_method":    trade["tp_method"],
                "risk_pct":     trade["risk_pct"],
                "reward_pct":   trade["reward_pct"],
                "rr":           trade["rr"],
                # Score
                "score":        round(total, 1),
                "sa":           sa, "sb": sb, "sc": sc,
                # Indicators
                "bbw":          round(bbw, 4),
                "atr_pct":      round(atr_pct, 2),
                "vol_comp":     round(vol_comp, 3),
                "vol_z4h":      round(vol_z4h, 3),
                "vol_ratio":    round(vol_ratio, 3),
                "bear_streak":  bear_streak,
                "zone_width_pct": round(zone_width_pct, 1),
                "touches":      best_zone["touch_count"],
                "breaks":       best_zone["break_count"],
                "strength":     strength,
                "caution":      caution,
                "vol_24h_m":    round(d["vol_24h"] / 1_000_000, 1),
                # Coinalyze derivatives (context info)
                "btx_z":        btx_d.get("btx_z",       None),
                "btx_ratio":    btx_d.get("btx_ratio",   None),
                "bv_ratio":     btx_d.get("bv_ratio",    None),
                "oi_usd":       oi_d.get("oi_usd",       None),
                "oi_chg_1h":    oi_d.get("oi_chg_1h",   None),
                "oi_chg_4h":    oi_d.get("oi_chg_4h",   None),
                "funding":      fund_d.get("funding_current", None),
                "funding_trend":fund_d.get("funding_trend",   None),
                "long_pct":     ls_d.get("long_pct",     None),
                "short_pct":    ls_d.get("short_pct",    None),
                "ls_ratio":     ls_d.get("ls_ratio",     None),
                "long_trend":   ls_d.get("long_trend",   None),
                "short_liq_4h": liq_d.get("short_liq_4h", None),
                "long_liq_4h":  liq_d.get("long_liq_4h",  None),
                "short_dom_24h":liq_d.get("short_dom_24h", None),
                "deriv_source": "Binance/Bybit proxy",
            })

        except Exception as e:
            log.warning(f"  Error Phase2 {sym}: {e}")

    # ── 6. Sort + output ────────────────────────────────────────────────
    # Sort: Score DESC → A(volatility) DESC → B(volume) DESC
    results.sort(key=lambda x: (-x["score"], -x["sa"], -x["sb"]))
    top = results[:cfg["top_n"]]

    elapsed = round(time.time() - start_ts, 1)
    log.info(
        f"\nSignals: {len(results)} | Shown: {len(top)} | "
        f"Approaching: {len(all_approaching)} | Time: {elapsed}s"
    )

    # ── Terminal output ─────────────────────────────────────────────────
    print(build_table(top, caution))
    print(build_approaching_table(all_approaching[:10]))

    if not top:
        log.info("Tidak ada sinyal saat ini.")
        _save_state(_state)
        return

    # ── Telegram ────────────────────────────────────────────────────────
    strong  = [r for r in top if r["strength"] == "STRONG"]
    # Filter Telegram: hanya coin dengan vol ≥ min_vol_signal (kurangi noise low-cap)
    tg_pool = [r for r in (strong if strong else top)
               if r["vol_24h_m"] * 1_000_000 >= cfg["min_vol_signal"]]
    targets = tg_pool[:cfg["max_alerts"]] if tg_pool else []

    if targets:
        ok = send_telegram(
            build_telegram_msg(targets, caution,
                               len(testing_candidates), len(candidates))
        )
        if ok:
            log.info(f"📤 Telegram: {len(targets)} signals sent")
            for r in targets:
                set_cooldown(r["symbol"])

    _save_state(_state)
    log.info(f"=== SELESAI — {datetime.now(timezone.utc).strftime('%H:%M UTC')} ===")


# ══════════════════════════════════════════════════════════════════════════════
#  ▶️  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not CONFIG["bot_token"] or not CONFIG["chat_id"]:
        log.error("FATAL: BOT_TOKEN / CHAT_ID tidak ditemukan!")
        exit(1)
    run_scan()
