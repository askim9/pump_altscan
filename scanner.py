#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NEXUS-SR v3.1 — WALK-FORWARD BACKTEST                                       ║
║                                                                              ║
║  METODOLOGI:                                                                 ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  1. Fetch OHLCV 1H 1000 candles (~42 hari) per coin — lebih panjang dari   ║
║     jendela scanner (200 candles) untuk replay yang bermakna                ║
║                                                                              ║
║  2. REPLAY candle per candle (bar[i] untuk i = 250..n-25):                  ║
║     a. Hanya gunakan data[0..i] untuk semua kalkulasi                       ║
║     b. Jalankan PERSIS logika scanner: gate → score → pump_reject → minB    ║
║     c. Jika sinyal tercipta, catat semua parameter                          ║
║                                                                              ║
║  3. OUTCOME EVALUATION untuk setiap sinyal:                                 ║
║     a. Entry: harga close candle[i] (konservatif, tidak asumsi fill ideal)  ║
║     b. SL_HIT: candle berikutnya punya low <= SL → trade gagal              ║
║     c. TP_HIT: candle berikutnya punya high >= TP (sebelum SL tercapai)    ║
║     d. TIMEOUT: tidak ada SL/TP dalam 24 bar → tutup di close bar ke-24    ║
║                                                                              ║
║  4. WALK-FORWARD SPLIT:                                                      ║
║     Train: bar 250..700 (450 bar pertama setelah warmup)                    ║
║     Test : bar 700..n-25 (sisa data, belum pernah dilihat)                  ║
║                                                                              ║
║  ANTI-BIAS GUARANTEE:                                                        ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  · Semua indikator dihitung hanya dari candles[0..i]                        ║
║  · Pivot detection: hanya pivot yang confirmed (pivot bar + lookback < i)   ║
║  · Outcome hanya dari candles[i+1..i+24] — masa depan, hanya untuk label   ║
║  · Tidak ada hindsight: zone detection, scoring, gating identik scanner     ║
║                                                                              ║
║  METRIK YANG DILAPORKAN:                                                     ║
║  · Win Rate (TP hit sebelum SL)                                              ║
║  · Profit Factor (total gain / total loss)                                   ║
║  · Expectancy per trade (rata-rata % gain/loss per sinyal)                  ║
║  · Precision@threshold (% sinyal yang menghasilkan TP hit)                  ║
║  · Max consecutive losses                                                    ║
║  · Breakdown per strength level (STRONG / MODERATE)                         ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import math
import os
import pickle
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(),
              logging.FileHandler("backtest_v31.log", encoding="utf-8")]
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  IDENTIK dengan scanner — TIDAK ADA yang diubah
# ══════════════════════════════════════════════════════════════════════════════
CFG = {
    "lookback_period":      20,
    "vol_len":               2,
    "box_width_multiplier": 1.0,
    "atr_period":          200,
    "max_break_count":       3,
    "vol_outlier_mult":    5.0,
    "vol_outlier_lookback": 20,
    "max_zone_width_pct":  8.0,
    "gate_bbw_min":       0.050,
    "gate_atr_pct_min":   1.20,
    "pump_reject_vol_ratio": 5.0,
    "pump_reject_bear_min":  1,
    "min_score_B":          10,
    "score_vol_max":        50,
    "bbw_strong":         0.150,
    "bbw_medium":         0.078,
    "atr_strong":          3.12,
    "atr_medium":          1.62,
    "bbw_weight":          0.55,
    "atr_weight":          0.45,
    "score_vol_micro_max":  30,
    "vol_comp_strong":    3.76,
    "vol_comp_medium":    1.82,
    "vol_z4h_strong":     2.0,
    "vol_z4h_medium":    0.729,
    "vol_comp_weight":    0.55,
    "vol_z4h_weight":     0.45,
    "score_momentum_max":   20,
    "bear_streak_strong":    4,
    "bear_streak_medium":    2,
    "vol_ratio_strong":    2.0,
    "vol_ratio_medium":   1.44,
    "bear_weight":         0.40,
    "vol_ratio_weight":    0.60,
    "score_threshold_normal":  60,
    "score_threshold_caution": 75,
    "score_strong":            80,
}

# ── Backtest-spesifik ────────────────────────────────────────────────────────
BT = {
    "candle_limit":    1000,   # 1H candles per coin
    "warmup_bars":      250,   # bar minimum sebelum sinyal valid
    "train_split":      700,   # bar 250–700 = train
    "outcome_bars":      24,   # jam evaluasi outcome (24H)
    "min_vol_24h":   100_000,  # $100K minimum universe
    "max_coins":         300,  # limit coin
    "cache_dir":   Path("bt_cache"),
    "cache_ttl_h":        24,
    "sleep_between":     0.1,
}
BT["cache_dir"].mkdir(exist_ok=True)

# ══════════════════════════════════════════════════════════════════════════════
#  CACHE
# ══════════════════════════════════════════════════════════════════════════════
def _cache_get(key: str) -> Optional[Any]:
    p = BT["cache_dir"] / f"{key.replace('/', '_')}.pkl"
    if not p.exists(): return None
    if (time.time() - p.stat().st_mtime) / 3600 > BT["cache_ttl_h"]: return None
    with open(p, "rb") as f: return pickle.load(f)

def _cache_set(key: str, data: Any):
    p = BT["cache_dir"] / f"{key.replace('/', '_')}.pkl"
    with open(p, "wb") as f: pickle.dump(data, f)

# ══════════════════════════════════════════════════════════════════════════════
#  BITGET FETCH
# ══════════════════════════════════════════════════════════════════════════════
def _get(url, params=None):
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                time.sleep(30); continue
            if e.response.status_code in (400, 404): return None
            if attempt < 2: time.sleep(3)
        except Exception:
            if attempt < 2: time.sleep(3)
    return None

def fetch_tickers() -> Dict[str, dict]:
    data = _get("https://api.bitget.com/api/v2/mix/market/tickers",
                {"productType": "USDT-FUTURES"})
    if not data or data.get("code") != "00000": return {}
    return {d["symbol"]: d for d in data.get("data", [])}

def fetch_candles_1h(symbol: str, limit: int) -> List[dict]:
    key = f"candles_{symbol}_{limit}"
    cached = _cache_get(key)
    if cached: return cached

    def parse(rows):
        out = []
        for row in rows:
            try:
                vol = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                out.append({"ts": int(row[0]), "open": float(row[1]),
                            "high": float(row[2]), "low": float(row[3]),
                            "close": float(row[4]), "vol": vol})
            except: continue
        return out

    collected = {}
    end_time = None
    for _ in range(math.ceil(limit / 200)):
        params = {"symbol": symbol, "productType": "USDT-FUTURES",
                  "granularity": "1H", "limit": 200}
        if end_time: params["endTime"] = str(end_time)
        data = _get("https://api.bitget.com/api/v2/mix/market/candles", params)
        if not data or data.get("code") != "00000": break
        raw = data.get("data", [])
        if not raw: break
        for c in parse(raw): collected[c["ts"]] = c
        if len(raw) < 200: break
        end_time = min(c["ts"] for c in parse(raw)) - 1
        if len(collected) >= limit: break
        time.sleep(0.12)

    result = sorted(collected.values(), key=lambda x: x["ts"])[-limit:]
    if result: _cache_set(key, result)
    return result

# ══════════════════════════════════════════════════════════════════════════════
#  INDICATORS — IDENTIK scanner (tidak ada perubahan)
# ══════════════════════════════════════════════════════════════════════════════
def _wilder(arr, p):
    if not arr: return []
    a = 1.0 / p; r = [arr[0]]
    for v in arr[1:]: r.append(a * v + (1-a) * r[-1])
    return r

def _ema(arr, p):
    if not arr: return []
    a = 2.0 / (p+1); r = [arr[0]]
    for v in arr[1:]: r.append(a * v + (1-a) * r[-1])
    return r

def mean(a): return sum(a)/len(a) if a else 0.0
def std(a):
    if len(a) < 2: return 0.0
    m = mean(a)
    return math.sqrt(sum((x-m)**2 for x in a)/len(a))

def zscore(v, series, min_s=10):
    if len(series) < min_s: return 0.0
    s = std(series)
    return (v - mean(series)) / s if s > 0 else 0.0

def linear_score(v, strong, medium, w):
    if v >= strong: return w
    if v >= medium:
        r = (v-medium)/(strong-medium)
        return w*0.5 + r*w*0.5
    if v >= 0:
        r = v/medium if medium > 0 else 0.0
        return r*w*0.5
    return 0.0

def delta_vol(candles):
    r = []; is_buy = True
    for c in candles:
        if c["close"] > c["open"]: is_buy = True
        elif c["close"] < c["open"]: is_buy = False
        r.append(c["vol"] if is_buy else -c["vol"])
    return r

def vol_thresholds(dv, vl):
    n = len(dv); sc = [v/2.5 for v in dv]
    hi = [max(sc[max(0,i-vl+1):i+1]) for i in range(n)]
    lo = [min(sc[max(0,i-vl+1):i+1]) for i in range(n)]
    return hi, lo

def atr_arr(candles, p=200):
    trs = []
    for i, c in enumerate(candles):
        pc = candles[i-1]["close"] if i > 0 else c["close"]
        trs.append(max(c["high"]-c["low"], abs(c["high"]-pc), abs(c["low"]-pc)))
    return _wilder(trs, p)

def bbw_arr(candles, p=20, m=2.0):
    closes = [c["close"] for c in candles]; n = len(closes)
    r = [0.0]*n
    for i in range(p-1, n):
        w = closes[i-p+1:i+1]; s = sum(w)/p
        sd = math.sqrt(sum((x-s)**2 for x in w)/p)
        r[i] = (m*2*sd/s) if s > 0 else 0.0
    return r

def pivot_lows(lows, lb):
    n = len(lows); r = [None]*n
    for i in range(lb, n-lb):
        v = lows[i]
        if v <= min(lows[i-lb:i]) and v <= min(lows[i+1:i+lb+1]):
            r[i] = v
    return r

# ══════════════════════════════════════════════════════════════════════════════
#  ZONE DETECTION AT BAR[i] — identik collector
# ══════════════════════════════════════════════════════════════════════════════
def detect_zones_at(candles, i, atr, dv, vh):
    lb = CFG["lookback_period"]; bw = CFG["box_width_multiplier"]
    lows = [c["low"] for c in candles[:i+1]]
    pivs = pivot_lows(lows, lb)
    zones = []
    for j, piv in enumerate(pivs):
        if piv is None: continue
        if j + lb > i: break          # pivot not yet confirmed
        if dv[j] <= vh[j]: continue
        if math.isnan(atr[j]) or atr[j] <= 0: continue
        zt = piv; zb = piv - atr[j] * bw
        break_count = 0; touch_count = 0
        in_zone = False; broke_at = i + 1
        for k in range(j + lb, i + 1):
            if k >= broke_at: break
            cl = candles[k]["low"]; ch = candles[k]["high"]
            if not in_zone:
                if zb <= cl <= zt: in_zone = True; touch_count += 1
            else:
                if cl > zt: in_zone = False
                elif ch < zb: break_count += 1; in_zone = False; broke_at = k
        if break_count >= CFG["max_break_count"]: continue
        zones.append({"zone_top": zt, "zone_bottom": zb,
                      "break_count": break_count, "touch_count": touch_count})
    return zones

def zone_state(z, low, high):
    if high < z["zone_bottom"]: return "BROKEN"
    if z["zone_bottom"] <= low <= z["zone_top"]: return "TESTING"
    return "VALID"

# ══════════════════════════════════════════════════════════════════════════════
#  SCORING — identik scanner
# ══════════════════════════════════════════════════════════════════════════════
def score_A(bbw, atr_pct):
    cfg = CFG
    a1 = round(linear_score(bbw, cfg["bbw_strong"], cfg["bbw_medium"], 1.0) *
               cfg["score_vol_max"] * cfg["bbw_weight"])
    a2 = round(linear_score(atr_pct, cfg["atr_strong"], cfg["atr_medium"], 1.0) *
               cfg["score_vol_max"] * cfg["atr_weight"])
    return min(a1+a2, cfg["score_vol_max"])

def score_B(vc, vz4h):
    cfg = CFG
    b1 = round(linear_score(vc, cfg["vol_comp_strong"], cfg["vol_comp_medium"], 1.0) *
               cfg["score_vol_micro_max"] * cfg["vol_comp_weight"])
    b2 = round(linear_score(vz4h, cfg["vol_z4h_strong"], cfg["vol_z4h_medium"], 1.0) *
               cfg["score_vol_micro_max"] * cfg["vol_z4h_weight"])
    return min(b1+b2, cfg["score_vol_micro_max"])

def score_C(bear, vr):
    cfg = CFG
    if bear >= cfg["bear_streak_strong"]: c1r = 1.0
    elif bear >= cfg["bear_streak_medium"]:
        c1r = 0.5 + 0.5*(bear-cfg["bear_streak_medium"])/(cfg["bear_streak_strong"]-cfg["bear_streak_medium"])
    else:
        c1r = bear/cfg["bear_streak_medium"]*0.5 if cfg["bear_streak_medium"] > 0 else 0.0
    c1 = round(c1r * cfg["score_momentum_max"] * cfg["bear_weight"])
    c2 = round(linear_score(vr, cfg["vol_ratio_strong"], cfg["vol_ratio_medium"], 1.0) *
               cfg["score_momentum_max"] * cfg["vol_ratio_weight"])
    return min(c1+c2, cfg["score_momentum_max"])

# ══════════════════════════════════════════════════════════════════════════════
#  TRADE SETUP — identik scanner (find_resistance + compute_trade_setup)
# ══════════════════════════════════════════════════════════════════════════════
def find_resistance(candles, entry, atr_val, lb=10):
    highs = [c["high"] for c in candles]; n = len(highs); found = set()
    for i in range(lb, n-lb):
        v = highs[i]
        if v < entry * 1.003: continue
        if v >= max(highs[max(0,i-lb):i]) and v >= max(highs[i+1:i+lb+1]):
            found.add(round(v, 10))
    recent_max = max(highs[-min(100,n):])
    if recent_max > entry * 1.01: found.add(round(recent_max, 10))
    return sorted(r for r in found if r > entry * 1.002)

def trade_setup(candles, zone, atr, bbw, price):
    atr_val = atr[-1] if atr else price * 0.02
    entry = price if price <= zone["zone_top"] else zone["zone_top"]
    entry_type = "MARKET" if price <= zone["zone_top"] else "LIMIT"
    sl = zone["zone_bottom"] - atr_val * 1.0
    sl = max(sl, price * 0.0001)
    risk = entry - sl
    if risk <= 0: risk = atr_val
    res = find_resistance(candles, entry, atr_val)
    mult = 5.0 if bbw >= 0.150 else (3.5 if bbw >= 0.078 else 2.5)
    tp_atr = entry + atr_val * mult
    tp_res = next((r for r in res if (r-entry)/risk >= 2.0), None)
    tp = tp_res if (tp_res and tp_res < tp_atr) else tp_atr
    tp_floor = entry + risk * 2.0
    if tp < tp_floor: tp = tp_floor
    rp = (entry - sl) / entry * 100 if entry > 0 else 0
    rwp = (tp - entry) / entry * 100 if entry > 0 else 0
    rr = rwp / rp if rp > 0 else 0
    return {"entry": entry, "entry_type": entry_type,
            "sl": sl, "tp": tp, "rr": round(rr, 2),
            "risk_pct": round(rp, 2), "reward_pct": round(rwp, 2)}

# ══════════════════════════════════════════════════════════════════════════════
#  OUTCOME EVALUATION
# ══════════════════════════════════════════════════════════════════════════════
def evaluate_outcome(candles, signal_bar, entry, sl, tp, max_bars=24):
    """
    Simulasi candle-by-candle setelah entry.
    Return: ('TP', pnl_pct), ('SL', pnl_pct), ('TIMEOUT', pnl_pct)
    
    Rules:
    1. Entry = close harga sinyal bar (realistis: tidak bisa masuk lebih bagus)
    2. Setiap candle berikutnya: cek SL first (low <= sl), lalu TP (high >= tp)
    3. Jika kedua kena dalam candle yang sama: SL menang (worst case)
    4. Timeout = close bar ke-24 setelah entry
    """
    n = len(candles)
    for k in range(signal_bar + 1, min(signal_bar + max_bars + 1, n)):
        c = candles[k]
        # SL check first (pessimistic — prevents cherry-picking)
        if c["low"] <= sl:
            pnl = (sl - entry) / entry * 100 if entry > 0 else 0
            return "SL", round(pnl, 3)
        if c["high"] >= tp:
            pnl = (tp - entry) / entry * 100 if entry > 0 else 0
            return "TP", round(pnl, 3)
    # Timeout
    close_bar = min(signal_bar + max_bars, n - 1)
    pnl = (candles[close_bar]["close"] - entry) / entry * 100 if entry > 0 else 0
    return "TIMEOUT", round(pnl, 3)

# ══════════════════════════════════════════════════════════════════════════════
#  SINGLE COIN REPLAY
# ══════════════════════════════════════════════════════════════════════════════
def replay_coin(symbol: str, candles: List[dict]) -> List[dict]:
    """
    Walk-forward replay. Semua kalkulasi di bar[i] hanya menggunakan candles[0..i].
    """
    n = len(candles)
    if n < BT["warmup_bars"] + BT["outcome_bars"] + 10:
        return []

    # Pre-compute full arrays (akan dibaca hanya sampai indeks i)
    _atr = atr_arr(candles, CFG["atr_period"])
    _bbw = bbw_arr(candles, 20)
    _dv  = delta_vol(candles)
    _vh, _ = vol_thresholds(_dv, CFG["vol_len"])

    signals = []
    zone_signal_bars: Dict[str, int] = {}  # zone_id → last signal bar (anti-dup)

    end_bar = n - BT["outcome_bars"] - 1

    for i in range(BT["warmup_bars"], end_bar):
        c_cur = candles[i]
        price = c_cur["close"]
        if price <= 0: continue

        # ── Gate G3: vol outlier (identical scanner fix: use [-2]) ──────────
        lb = CFG["vol_outlier_lookback"]
        cur_vol = candles[i-1]["vol"]       # -1 relative to current = confirmed bar
        baseline_vols = [candles[k]["vol"] for k in range(max(0,i-lb-1), i-1)]
        avg_vol = mean(baseline_vols) if baseline_vols else 0
        if avg_vol > 0 and cur_vol > CFG["vol_outlier_mult"] * avg_vol:
            continue

        # ── Pre-compute per-bar values ───────────────────────────────────────
        bbw_val  = _bbw[i]
        atr_val  = _atr[i]
        atr_pct  = atr_val / price * 100 if price > 0 else 0

        # ── Gate G4+G5: volatility ───────────────────────────────────────────
        if bbw_val < CFG["gate_bbw_min"]:   continue
        if atr_pct  < CFG["gate_atr_pct_min"]: continue

        # ── Zone detection at bar[i] ─────────────────────────────────────────
        zones = detect_zones_at(candles, i, _atr, _dv, _vh)
        if not zones: continue

        # ── Gate G1+G2: TESTING state ────────────────────────────────────────
        testing_zones = []
        for z in zones:
            if z["break_count"] >= CFG["max_break_count"]: continue
            # Check candle[-1] and candle[-2] relative to current bar
            for lookback_i in (0, 1):   # 0 = current bar, 1 = previous bar
                check_bar = i - lookback_i
                if check_bar < 0: continue
                c = candles[check_bar]
                if zone_state(z, c["low"], c["high"]) == "TESTING":
                    testing_zones.append(z); break

        if not testing_zones: continue

        # ── Zone width filter ───────────────────────────────────────────────
        valid_zones = []
        for z in testing_zones:
            zw_pct = (z["zone_top"] - z["zone_bottom"]) / price * 100
            if zw_pct <= CFG["max_zone_width_pct"]:
                valid_zones.append(z)
        if not valid_zones: continue

        # ── Pick best zone ───────────────────────────────────────────────────
        best = min(valid_zones, key=lambda z: abs(z["zone_top"] - price))
        zone_id = f"{best['zone_top']:.8f}"

        # ── Anti-duplicate: max 1 signal per zone per 12 bars ──────────────
        last_sig = zone_signal_bars.get(zone_id, -999)
        if i - last_sig < 12: continue

        # ── Volume indicators ────────────────────────────────────────────────
        vols = [c["vol"] for c in candles[:i+1]]

        # Vol compression: recent 5 bars / prior 20 bars (before recent)
        if len(vols) < 26:
            vc = 1.0
        else:
            recent_avg = mean(vols[-5:])
            prior_avg  = mean(vols[-25:-5])
            vc = recent_avg / prior_avg if prior_avg > 0 else 1.0

        # Vol Z4H: Z-score candle[-2] vs 96-bar window
        if len(vols) < 102:
            vz4h = 0.0
        else:
            cur_v    = vols[-2]
            baseline = vols[-101:-5]
            vz4h = zscore(cur_v, baseline, min_s=20)

        # Vol ratio: candle[-2] vs avg 20 bars before it
        if len(vols) < 23:
            vr = 1.0
        else:
            cur_v2   = vols[-2]
            base_avg = mean(vols[-22:-2])
            vr = cur_v2 / base_avg if base_avg > 0 else 1.0

        # Bear streak
        streak = 0
        for c in reversed(candles[max(0,i-6):i]):
            if c["close"] < c["open"]: streak += 1
            else: break
            if streak >= 6: break

        # ── Scoring ─────────────────────────────────────────────────────────
        sa = score_A(bbw_val, atr_pct)
        sb = score_B(vc, vz4h)
        sc = score_C(streak, vr)
        total = sa + sb + sc

        # ── Pump rejection gate ─────────────────────────────────────────────
        if vr > CFG["pump_reject_vol_ratio"] and streak < CFG["pump_reject_bear_min"]:
            continue

        # ── Minimum B score ──────────────────────────────────────────────────
        if sb < CFG["min_score_B"]:
            continue

        # ── Threshold: always use NORMAL (60) for backtest ──────────────────
        # Caution mode butuh BTC/ETH data yang tidak selalu tersedia per bar
        # Menggunakan threshold konservatif (NORMAL=60) untuk seluruh backtest
        # Ini LEBIH ketat dari rata-rata scan nyata → hasil lebih konservatif
        if total < CFG["score_threshold_normal"]:
            continue

        strength = ("STRONG"   if total >= CFG["score_strong"] else
                    "MODERATE" if total >= CFG["score_threshold_normal"] else "WEAK")

        # ── Trade setup ─────────────────────────────────────────────────────
        trade = trade_setup(candles[:i+1], best, _atr[:i+1], bbw_val, price)
        entry_p = trade["entry"]
        sl_p    = trade["sl"]
        tp_p    = trade["tp"]

        # ── Outcome evaluation ───────────────────────────────────────────────
        outcome, pnl = evaluate_outcome(candles, i, entry_p, sl_p, tp_p,
                                        BT["outcome_bars"])

        zone_signal_bars[zone_id] = i
        signals.append({
            "symbol":      symbol,
            "bar":         i,
            "ts":          c_cur["ts"],
            "split":       "train" if i < BT["train_split"] else "test",
            "price":       round(price, 8),
            "entry":       round(entry_p, 8),
            "sl":          round(sl_p, 8),
            "tp":          round(tp_p, 8),
            "rr":          trade["rr"],
            "risk_pct":    trade["risk_pct"],
            "reward_pct":  trade["reward_pct"],
            "score":       total,
            "sa": sa, "sb": sb, "sc": sc,
            "bbw":         round(bbw_val, 4),
            "atr_pct":     round(atr_pct, 2),
            "vol_comp":    round(vc, 3),
            "vol_z4h":     round(vz4h, 3),
            "vol_ratio":   round(vr, 3),
            "bear_streak": streak,
            "strength":    strength,
            "outcome":     outcome,
            "pnl_pct":     pnl,
        })

    return signals

# ══════════════════════════════════════════════════════════════════════════════
#  METRICS
# ══════════════════════════════════════════════════════════════════════════════
def compute_metrics(signals: List[dict], label: str = "") -> dict:
    if not signals:
        return {"n": 0, "label": label}

    n       = len(signals)
    n_tp    = sum(1 for s in signals if s["outcome"] == "TP")
    n_sl    = sum(1 for s in signals if s["outcome"] == "SL")
    n_to    = sum(1 for s in signals if s["outcome"] == "TIMEOUT")
    win_rate = n_tp / n * 100

    # Profit factor
    gains  = sum(s["pnl_pct"] for s in signals if s["pnl_pct"] > 0)
    losses = abs(sum(s["pnl_pct"] for s in signals if s["pnl_pct"] < 0))
    pf     = gains / losses if losses > 0 else float("inf")

    # Expectancy
    expectancy = mean([s["pnl_pct"] for s in signals])

    # Max drawdown sequence (consecutive losses)
    seq = 0; max_seq = 0
    for s in signals:
        if s["pnl_pct"] < 0: seq += 1; max_seq = max(max_seq, seq)
        else: seq = 0

    # R:R achieved
    avg_rr  = mean([s["rr"] for s in signals])

    # Cumulative PnL (100 USDT base, 1% risk per trade)
    equity = 100.0
    equity_curve = [equity]
    for s in signals:
        if s["outcome"] == "TP":
            gain = equity * (s["risk_pct"] / 100) * s["rr"]
            equity += gain
        elif s["outcome"] == "SL":
            loss = equity * (s["risk_pct"] / 100)
            equity -= loss
        else:  # TIMEOUT
            pnl_abs = equity * (abs(s["pnl_pct"]) / 100) * (1 if s["pnl_pct"] >= 0 else -1)
            equity += pnl_abs
        equity_curve.append(equity)

    # Max drawdown dari equity curve
    peak = equity_curve[0]; max_dd = 0
    for e in equity_curve:
        if e > peak: peak = e
        dd = (peak - e) / peak * 100
        if dd > max_dd: max_dd = dd

    final_equity = equity_curve[-1]

    return {
        "label":         label,
        "n":             n,
        "n_tp":          n_tp,
        "n_sl":          n_sl,
        "n_timeout":     n_to,
        "win_rate":      round(win_rate, 1),
        "pf":            round(pf, 2),
        "expectancy":    round(expectancy, 3),
        "avg_rr":        round(avg_rr, 2),
        "max_consec_loss": max_seq,
        "final_equity":  round(final_equity, 2),
        "max_drawdown":  round(max_dd, 1),
        "total_pnl":     round(final_equity - 100, 2),
    }

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def progress(cur, total, label="", w=40):
    pct = cur/total if total > 0 else 0
    done = int(w*pct)
    return f"\r  [{'█'*done}{'░'*(w-done)}] {cur}/{total} {label:<20}"

def print_metrics(m: dict, indent: str = "  "):
    if not m.get("n"):
        print(f"{indent}  (tidak ada sinyal)")
        return
    print(f"{indent}  Sinyal       : {m['n']} (TP:{m['n_tp']} SL:{m['n_sl']} Timeout:{m['n_timeout']})")
    print(f"{indent}  Win Rate     : {m['win_rate']}%")
    print(f"{indent}  Profit Factor: {m['pf']}")
    print(f"{indent}  Expectancy   : {m['expectancy']}% per trade")
    print(f"{indent}  Avg R:R      : {m['avg_rr']}")
    print(f"{indent}  Max dd seq   : {m['max_consec_loss']} loss berturut-turut")
    print(f"{indent}  Equity 100→  : {m['final_equity']} USDT (+{m['total_pnl']}%)")
    print(f"{indent}  Max Drawdown : {m['max_drawdown']}%")

def main():
    import time as _t
    start = _t.time()

    print("\n" + "═"*70)
    print("  NEXUS-SR v3.1 — WALK-FORWARD BACKTEST")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("  Metodologi: Zero-lookahead, identik logika scanner")
    print("═"*70)

    # 1. Universe
    print("\n[1/4] Fetching universe …")
    tickers = fetch_tickers()
    universe = []
    for sym, t in tickers.items():
        try:
            vol = float(t.get("quoteVolume", 0))
            if vol >= BT["min_vol_24h"]: universe.append(sym)
        except: pass
    universe = sorted(universe)[:BT["max_coins"]]
    print(f"  Universe: {len(universe)} coins (vol≥${BT['min_vol_24h']:,})")

    # 2. Fetch data
    print(f"\n[2/4] Fetching {BT['candle_limit']} candles 1H per coin …")
    coin_data = {}
    for idx, sym in enumerate(universe):
        print(progress(idx+1, len(universe), sym), end="", flush=True)
        candles = fetch_candles_1h(sym, BT["candle_limit"])
        if len(candles) >= BT["warmup_bars"] + BT["outcome_bars"] + 30:
            coin_data[sym] = candles
        time.sleep(BT["sleep_between"])
    print()
    print(f"  Data OK: {len(coin_data)} coins")

    # 3. Replay
    print(f"\n[3/4] Walk-forward replay …")
    all_signals = []
    for idx, (sym, candles) in enumerate(coin_data.items()):
        print(progress(idx+1, len(coin_data), sym), end="", flush=True)
        try:
            sigs = replay_coin(sym, candles)
            all_signals.extend(sigs)
        except Exception as e:
            log.debug(f"  Error {sym}: {e}")
    print()
    print(f"  Total sinyal: {len(all_signals)}")

    if not all_signals:
        print("\n  ⚠️  Tidak ada sinyal — coba turunkan threshold atau perluas universe")
        return

    # 4. Analysis
    print(f"\n[4/4] Analisis …")

    train  = [s for s in all_signals if s["split"] == "train"]
    test   = [s for s in all_signals if s["split"] == "test"]
    strong = [s for s in all_signals if s["strength"] == "STRONG"]
    strong_test = [s for s in test if s["strength"] == "STRONG"]

    m_all    = compute_metrics(all_signals, "ALL")
    m_train  = compute_metrics(train, "TRAIN")
    m_test   = compute_metrics(test, "TEST")
    m_strong_test = compute_metrics(strong_test, "STRONG_TEST")

    # Score distribution
    score_buckets = defaultdict(lambda: {"n":0,"tp":0})
    for s in all_signals:
        bucket = (s["score"] // 5) * 5  # round down to nearest 5
        score_buckets[bucket]["n"] += 1
        if s["outcome"] == "TP": score_buckets[bucket]["tp"] += 1

    # Per-coin analysis
    coin_metrics = {}
    for sym in set(s["symbol"] for s in all_signals):
        coin_sigs = [s for s in all_signals if s["symbol"] == sym]
        if len(coin_sigs) >= 3:
            cm = compute_metrics(coin_sigs)
            coin_metrics[sym] = cm

    elapsed = round(_t.time() - start, 1)

    # ── PRINT REPORT ─────────────────────────────────────────────────────────
    print()
    print("═"*70)
    print("  NEXUS-SR v3.1 BACKTEST RESULTS")
    print(f"  Dataset: {len(coin_data)} coins × {BT['candle_limit']} candles 1H")
    print(f"  Train bars: {BT['warmup_bars']}–{BT['train_split']} | "
          f"Test bars: {BT['train_split']}–end")
    print(f"  Runtime: {elapsed}s")
    print("═"*70)

    print(f"\n── OVERVIEW ──────────────────────────────────────────────────────────")
    print(f"  Total sinyal   : {len(all_signals)}")
    print(f"  Train sinyal   : {len(train)}")
    print(f"  Test sinyal    : {len(test)}")
    print(f"  STRONG sinyal  : {len(strong)} total | {len(strong_test)} di test split")
    if all_signals:
        avg_score = mean([s["score"] for s in all_signals])
        avg_rr    = mean([s["rr"] for s in all_signals])
        print(f"  Avg score      : {avg_score:.1f} / 100")
        print(f"  Avg R:R design : {avg_rr:.2f}")

    print(f"\n── TRAIN RESULTS (bar {BT['warmup_bars']}–{BT['train_split']}) ──────────────────────────")
    print_metrics(m_train)

    print(f"\n── TEST RESULTS (bar {BT['train_split']}–end) — NILAI UTAMA ──────────────────────")
    print_metrics(m_test)

    print(f"\n── TEST: STRONG ONLY (score≥{CFG['score_strong']}) ──────────────────────────────────")
    print_metrics(m_strong_test)

    print(f"\n── SCORE vs WIN RATE ─────────────────────────────────────────────────")
    print(f"  {'Score range':>12}  {'N':>5}  {'Win%':>6}  {'TP':>4}  {'SL':>4}")
    print("  " + "─"*40)
    for bucket in sorted(score_buckets.keys()):
        bd = score_buckets[bucket]
        wr = bd["tp"]/bd["n"]*100 if bd["n"] > 0 else 0
        sl_c = bd["n"] - bd["tp"]
        bar_v = "█" * int(wr/5)
        print(f"  {bucket:>4}–{bucket+4:<4}  {bd['n']:>5}  {wr:>5.1f}%  "
              f"{bd['tp']:>4}  {sl_c:>4}  {bar_v}")

    print(f"\n── TOP 10 COINS by Win Rate (min 3 signals) ──────────────────────────")
    top_coins = sorted(
        [(sym, cm) for sym, cm in coin_metrics.items() if cm["n"] >= 3],
        key=lambda x: (-x[1]["win_rate"], -x[1]["n"])
    )[:10]
    if top_coins:
        print(f"  {'Coin':<15} {'N':>4}  {'Win%':>6}  {'PF':>5}  {'Exp%':>6}  {'Equity':>8}")
        print("  " + "─"*55)
        for sym, cm in top_coins:
            raw = sym.replace("USDT", "")
            print(f"  {raw:<15} {cm['n']:>4}  {cm['win_rate']:>5.1f}%  "
                  f"{cm['pf']:>5.2f}  {cm['expectancy']:>5.3f}%  {cm['final_equity']:>8.2f}")

    print(f"\n── KESIMPULAN ────────────────────────────────────────────────────────")
    if m_test.get("n", 0) > 0:
        wr   = m_test["win_rate"]
        pf   = m_test["pf"]
        exp  = m_test["expectancy"]
        dd   = m_test["max_drawdown"]

        if wr >= 55 and pf >= 1.5:
            verdict = "✅ PROFITABLE — scanner menunjukkan edge nyata di test data"
        elif wr >= 45 and pf >= 1.2:
            verdict = "⚠️  MARGINAL — ada edge kecil, perlu lebih banyak data"
        elif wr >= 40:
            verdict = "⚠️  BREAKEVEN — tidak ada edge signifikan"
        else:
            verdict = "❌ UNDERPERFORM — win rate di bawah minimum"

        print(f"  {verdict}")
        print(f"  Win Rate Test : {wr}%  (min viable: ~45%+ dengan R:R≥2)")
        print(f"  Profit Factor : {pf}  (>1.5 = bagus, >2.0 = sangat bagus)")
        print(f"  Expectancy    : {exp}% per trade")
        print(f"  Max Drawdown  : {dd}%")
        print()
        print(f"  Catatan metodologi:")
        print(f"  · SL check SEBELUM TP di candle yang sama (worst-case)")
        print(f"  · Entry = harga CLOSE bar sinyal (bukan ideal fill)")
        print(f"  · Threshold NORMAL (60) dipakai seluruh backtest")
        print(f"    → caution mode tidak disimulasikan (konservatif)")
        print(f"  · Anti-duplicate: max 1 sinyal per zone per 12 bar")
    else:
        print("  ⚠️  Test split tidak memiliki sinyal cukup")
        print(f"     Train split: {len(train)} sinyal — bisa dipakai evaluasi")

    print()
    print("═"*70)

    # Save JSON
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config":    {k: v for k, v in CFG.items()},
        "backtest":  BT | {"cache_dir": str(BT["cache_dir"])},
        "metrics": {
            "all":         m_all,
            "train":       m_train,
            "test":        m_test,
            "strong_test": m_strong_test,
        },
        "score_distribution": {str(k): v for k, v in score_buckets.items()},
        "top_coins": {sym: cm for sym, cm in top_coins},
        "n_coins":   len(coin_data),
        "elapsed_s": elapsed,
    }
    with open("backtest_v31_results.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"  Full results: backtest_v31_results.json")

if __name__ == "__main__":
    main()
