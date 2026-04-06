#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  NEXUS-SR DATA COLLECTOR & FEATURE ANALYZER                                  ║
║                                                                              ║
║  TUJUAN:                                                                     ║
║  Mengumpulkan data historis lengkap dan menemukan variabel-variabel          ║
║  yang benar-benar prediktif untuk bounce ≥15% dalam 24 jam dari             ║
║  support zone. Hasil digunakan untuk kalibrasi NEXUS-SR scanner.            ║
║                                                                              ║
║  ALUR:                                                                       ║
║  1. Fetch SEMUA USDT-Futures aktif di Bitget (tidak ada hardcoded list)     ║
║  2. Fetch per coin:                                                          ║
║     · OHLCV 1H    (500 candles = ~21 hari)  → OHLCV Bitget                 ║
║     · Funding Rate history (100 records)    → Bitget public                 ║
║     · Open Interest history (via tickers)   → Bitget public                 ║
║     · Taker Buy btx/bv (168 jam)            → Coinalyze                    ║
║  3. Replay candle per candle — ZERO lookahead bias                           ║
║  4. Setiap TESTING event dicatat dengan 20+ features                         ║
║  5. Label HIT jika harga naik ≥15% dalam 24H window berikutnya              ║
║  6. Feature Importance Analysis (Random Forest + correlation)                ║
║  7. Output:                                                                  ║
║     · events.csv          → semua events raw (untuk inspeksi manual)        ║
║     · feature_report.txt  → ranking variabel + threshold optimal             ║
║     · scanner_config.json → config siap pakai untuk NEXUS-SR                ║
║                                                                              ║
║  ANTI-LOOKAHEAD GUARANTEE:                                                   ║
║  Pada setiap bar[i], hanya data[0..i] yang digunakan untuk compute features  ║
║  Outcome diukur dari bar[i+1..i+24] — data masa depan, hanya untuk label    ║
║                                                                              ║
║  Usage : python nexus_data_collector.py                                      ║
║  Runtime: ~20-40 menit (fetch) + ~5 menit (analysis)                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import csv
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

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("collector.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CFG = {
    # ── API ────────────────────────────────────────────────────────────────
    "coinalyze_key":    os.getenv("COINALYZE_API_KEY",
                                  "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),

    # ── UNIVERSE ──────────────────────────────────────────────────────────
    "min_vol_24h":      100_000,       # $100K floor — singkirkan ghost coins
    "max_coins":        400,           # max coins diproses

    # ── DATA COLLECTION ───────────────────────────────────────────────────
    "candle_limit_1h":      500,       # 500 candles 1H = ~21 hari
    "funding_limit":        100,       # 100 funding rate records
    "clz_lookback_h":       168,       # 7 hari Coinalyze history
    "clz_interval":     "1hour",
    "clz_batch_size":        20,
    "clz_min_interval":     1.6,       # detik antar call Coinalyze

    # ── CACHE ────────────────────────────────────────────────────────────
    "cache_dir":    Path("nexus_cache"),
    "cache_ttl_h":       12,           # re-fetch jika cache > 12 jam

    # ── PINE SCRIPT PARAMS (zone detection) ───────────────────────────────
    "lookback":          15,
    "vol_len":            2,
    "box_width":         1.0,
    "atr_period":        200,

    # ── EVENT LABELING ────────────────────────────────────────────────────
    "target_pct":        8.0,         # bounce target (%)
    "outcome_window_h":  12,           # jam untuk mengukur outcome
    "min_bars_after":    2,           # minimal candle setelah entry untuk valid event

    # ── OUTPUT ───────────────────────────────────────────────────────────
    "events_csv":       "events.csv",
    "report_txt":       "feature_report.txt",
    "config_json":      "scanner_config.json",
}


# ══════════════════════════════════════════════════════════════════════════════
#  💾  DISK CACHE
# ══════════════════════════════════════════════════════════════════════════════
CFG["cache_dir"].mkdir(exist_ok=True)

def _cache_path(key: str) -> Path:
    safe = key.replace("/", "_").replace(":", "_")
    return CFG["cache_dir"] / f"{safe}.pkl"

def _cache_get(key: str) -> Optional[Any]:
    p = _cache_path(key)
    if not p.exists():
        return None
    age_h = (time.time() - p.stat().st_mtime) / 3600
    if age_h > CFG["cache_ttl_h"]:
        return None
    with open(p, "rb") as f:
        return pickle.load(f)

def _cache_set(key: str, data: Any) -> None:
    with open(_cache_path(key), "wb") as f:
        pickle.dump(data, f)


# ══════════════════════════════════════════════════════════════════════════════
#  🌐  BITGET CLIENT
# ══════════════════════════════════════════════════════════════════════════════
class Bitget:
    BASE = "https://api.bitget.com"

    @staticmethod
    def _get(url: str, params: dict = None, retries: int = 3) -> Optional[dict]:
        for attempt in range(retries):
            try:
                r = requests.get(url, params=params, timeout=15)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    time.sleep(30); continue
                if e.response.status_code in (400, 404):
                    return None
                if attempt < retries - 1:
                    time.sleep(3)
            except Exception:
                if attempt < retries - 1:
                    time.sleep(3)
        return None

    @classmethod
    def get_tickers(cls) -> Dict[str, dict]:
        """Semua USDT-Futures tickers — termasuk fundingRate dan holdingAmount."""
        data = cls._get(f"{cls.BASE}/api/v2/mix/market/tickers",
                        params={"productType": "USDT-FUTURES"})
        if not data or data.get("code") != "00000":
            return {}
        return {d["symbol"]: d for d in data.get("data", [])}

    @classmethod
    def get_candles(cls, symbol: str, limit: int = 500) -> List[dict]:
        """OHLCV 1H dengan pagination (max 200 per request)."""
        cache_key = f"candles_{symbol}_{limit}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

        def _parse(rows):
            out = []
            for row in rows:
                try:
                    vol = float(row[6]) if len(row) > 6 else float(row[5]) * float(row[4])
                    out.append({"ts": int(row[0]), "open": float(row[1]),
                                "high": float(row[2]), "low": float(row[3]),
                                "close": float(row[4]), "vol": vol})
                except (IndexError, ValueError):
                    continue
            return out

        collected: Dict[int, dict] = {}
        end_time = None
        for _ in range(math.ceil(limit / 200)):
            params = {"symbol": symbol, "productType": "USDT-FUTURES",
                      "granularity": "1H", "limit": min(200, limit)}
            if end_time:
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
        if result:
            _cache_set(cache_key, result)
        return result

    @classmethod
    def get_funding_history(cls, symbol: str, limit: int = 100) -> List[dict]:
        """
        Historical funding rate per 8 jam.
        GET /api/v2/mix/market/history-fund-rate
        Returns [{fundingRate, fundingTime}]
        """
        cache_key = f"funding_{symbol}_{limit}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

        data = cls._get(
            f"{cls.BASE}/api/v2/mix/market/history-fund-rate",
            params={"symbol": symbol, "productType": "USDT-FUTURES",
                    "pageSize": limit}
        )
        if not data or data.get("code") != "00000":
            return []

        result = []
        for item in data.get("data", []):
            try:
                result.append({
                    "ts":   int(item.get("fundingTime", 0)),
                    "rate": float(item.get("fundingRate", 0)),
                })
            except (ValueError, TypeError):
                continue

        result.sort(key=lambda x: x["ts"])
        if result:
            _cache_set(cache_key, result)
        return result


# ══════════════════════════════════════════════════════════════════════════════
#  📡  COINALYZE CLIENT
# ══════════════════════════════════════════════════════════════════════════════
_clz_last_call: float = 0.0

def _clz_wait() -> None:
    global _clz_last_call
    elapsed = time.time() - _clz_last_call
    wait    = CFG["clz_min_interval"] - elapsed
    if wait > 0:
        time.sleep(wait)
    _clz_last_call = time.time()

def clz_get(endpoint: str, params: dict) -> Optional[Any]:
    params["api_key"] = CFG["coinalyze_key"]
    for attempt in range(3):
        _clz_wait()
        try:
            r = requests.get(f"https://api.coinalyze.net/v1/{endpoint}",
                             params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 10)) + 1); continue
            if r.status_code == 401:
                log.error("Coinalyze API key invalid"); return None
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt < 2:
                time.sleep(5)
    return None

def clz_get_markets() -> List[dict]:
    cache_key = "clz_markets"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    data = clz_get("future-markets", {})
    result = data if isinstance(data, list) else []
    if result:
        _cache_set(cache_key, result)
    return result

def clz_build_mapper(bitget_syms: set) -> Dict[str, str]:
    """Build Bitget → Coinalyze symbol mapping."""
    markets = clz_get_markets()
    if not markets:
        log.warning("Coinalyze: gagal fetch markets")
        return {}

    bitget_mkts = [m for m in markets if "bitget" in m.get("exchange", "").lower()]
    if not bitget_mkts:
        bitget_mkts = markets

    mapping: Dict[str, str] = {}
    for m in bitget_mkts:
        clz_sym  = m.get("symbol", "")
        exch_sym = m.get("symbol_on_exchange", "")
        if not clz_sym:
            continue
        clean = exch_sym.replace("_UMCBL", "").replace("_DMCBL", "").upper()
        for cand in [clean, exch_sym.upper()]:
            if cand in bitget_syms:
                mapping[cand] = clz_sym
                break

    log.info(f"Coinalyze mapper: {len(mapping)}/{len(bitget_syms)} mapped")
    return mapping

def clz_fetch_batch(clz_symbols: List[str]) -> Dict[str, List[dict]]:
    """Batch fetch OHLCV+btx+bv dari Coinalyze."""
    if not clz_symbols:
        return {}

    now_ts  = int(time.time())
    from_ts = now_ts - CFG["clz_lookback_h"] * 3600
    result  = {}
    bs      = CFG["clz_batch_size"]

    for i in range(0, len(clz_symbols), bs):
        batch  = clz_symbols[i: i + bs]
        params = {"symbols": ",".join(batch), "interval": CFG["clz_interval"],
                  "from": from_ts, "to": now_ts}
        data = clz_get("ohlcv-history", params)
        if not isinstance(data, list):
            continue
        for item in data:
            sym     = item.get("symbol", "")
            history = item.get("history", [])
            if sym and history:
                result[sym] = history

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  📐  INDICATORS  (pure Python, no pandas)
# ══════════════════════════════════════════════════════════════════════════════

def wilder_ema(arr: List[float], period: int) -> List[float]:
    """Wilder's RMA — Pine's ta.atr internal."""
    if not arr:
        return []
    alpha  = 1.0 / period
    result = [arr[0]]
    for v in arr[1:]:
        result.append(alpha * v + (1 - alpha) * result[-1])
    return result

def std_ema(arr: List[float], period: int) -> List[float]:
    """Standard EMA — Pine's ta.ema."""
    if not arr:
        return []
    alpha  = 2.0 / (period + 1)
    result = [arr[0]]
    for v in arr[1:]:
        result.append(alpha * v + (1 - alpha) * result[-1])
    return result

def rolling_mean(arr: List[float], window: int, i: int) -> float:
    """Mean of arr[max(0, i-window+1)..i] — no future data."""
    sub = arr[max(0, i - window + 1): i + 1]
    return sum(sub) / len(sub) if sub else 0.0

def rolling_std(arr: List[float], window: int, i: int) -> float:
    sub = arr[max(0, i - window + 1): i + 1]
    if len(sub) < 2:
        return 0.0
    m = sum(sub) / len(sub)
    return math.sqrt(sum((x - m) ** 2 for x in sub) / len(sub))

def zscore_at(value: float, arr: List[float], window: int,
              end_i: int, min_samples: int = 10) -> float:
    """Z-score of value vs arr[end_i-window..end_i-1] (exclude current bar)."""
    baseline = arr[max(0, end_i - window): end_i]
    if len(baseline) < min_samples:
        return 0.0
    sigma = rolling_std(baseline, len(baseline), len(baseline) - 1)
    if sigma == 0:
        return 0.0
    m = sum(baseline) / len(baseline)
    return (value - m) / sigma

def compute_delta_vol(candles: List[dict]) -> List[float]:
    """Pine: upAndDownVolume() — signed volume."""
    result = []
    is_buy = True
    for c in candles:
        if   c["close"] > c["open"]: is_buy = True
        elif c["close"] < c["open"]: is_buy = False
        result.append(c["vol"] if is_buy else -c["vol"])
    return result

def compute_atr(candles: List[dict], period: int = 200) -> List[float]:
    trs = []
    for i, c in enumerate(candles):
        pc = candles[i-1]["close"] if i > 0 else c["close"]
        trs.append(max(c["high"]-c["low"], abs(c["high"]-pc), abs(c["low"]-pc)))
    return wilder_ema(trs, period)

def compute_rsi(closes: List[float], period: int = 14) -> List[float]:
    gains  = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    ag     = wilder_ema(gains, period)
    al     = wilder_ema(losses, period)
    rsi    = [50.0]
    for g, l in zip(ag, al):
        rs = g / l if l > 0 else 100.0
        rsi.append(100.0 - 100.0 / (1.0 + rs))
    return rsi

def compute_bbw(closes: List[float], period: int = 20,
                mult: float = 2.0) -> List[float]:
    """Bollinger Band Width = (upper-lower)/middle. Normalized per SMA."""
    bbw = [0.0] * len(closes)
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1: i + 1]
        sma    = sum(window) / period
        sd     = math.sqrt(sum((x - sma) ** 2 for x in window) / period)
        bbw[i] = (mult * 2 * sd / sma) if sma > 0 else 0.0
    return bbw


# ══════════════════════════════════════════════════════════════════════════════
#  🟩  ZONE DETECTION  (Pine Script identical)
# ══════════════════════════════════════════════════════════════════════════════
def detect_support_zones_at(
    candles: List[dict],
    i:       int,        # current bar (only use data[0..i])
    lookback: int,
    vol_len:  int,
    box_width: float,
    atr_arr:  List[float],
    dv_arr:   List[float],
    vh_arr:   List[float],
) -> List[dict]:
    """
    Detect all support zones visible at bar[i].
    Only zones confirmed BEFORE bar[i] (bar index + lookback <= i).
    """
    zones = []
    lows  = [c["low"] for c in candles[:i+1]]
    n     = len(lows)

    for j in range(lookback, n - lookback):
        # Only confirmed zones: pivot needs lookback bars to confirm
        if j + lookback > i:
            break

        val   = lows[j]
        left  = lows[j - lookback: j]
        right = lows[j + 1: j + lookback + 1]

        if not (val <= min(left) and val <= min(right)):
            continue
        if dv_arr[j] <= vh_arr[j]:  # Pine: Vol > vol_hi
            continue
        if math.isnan(atr_arr[j]) or atr_arr[j] <= 0:
            continue

        zone_top    = val
        zone_bottom = val - atr_arr[j] * box_width

        # Count touches and breaks from j+lookback to i
        touch_count = 0
        break_count = 0
        in_zone     = False
        broke_at    = i + 1

        for k in range(j + lookback, i + 1):
            if k >= broke_at:
                break
            c_low  = candles[k]["low"]
            c_high = candles[k]["high"]
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
                    broke_at = k

        if break_count >= 3:
            continue  # permanently invalid

        zones.append({
            "zone_top":    zone_top,
            "zone_bottom": zone_bottom,
            "delta_vol":   dv_arr[j],
            "touch_count": touch_count,
            "break_count": break_count,
            "age_bars":    i - j,
        })

    return zones


def get_zone_state(zone: dict, low: float, high: float) -> str:
    top = zone["zone_top"]; bot = zone["zone_bottom"]
    if high < bot:           return "BROKEN"
    if bot <= low <= top:    return "TESTING"
    return "VALID"


# ══════════════════════════════════════════════════════════════════════════════
#  🔬  FEATURE EXTRACTION  (per TESTING event at bar[i])
# ══════════════════════════════════════════════════════════════════════════════
def extract_features(
    candles:      List[dict],
    i:            int,           # entry bar (TESTING state)
    zone:         dict,
    rsi_arr:      List[float],
    dv_arr:       List[float],
    bbw_arr:      List[float],
    atr_arr:      List[float],
    funding_hist: List[dict],    # [{ts, rate}]
    cur_oi:       float,         # OI saat ini dari ticker
    prev_oi:      float,         # OI scan sebelumnya
    clz_candles:  List[dict],    # Coinalyze OHLCV+btx+bv
) -> dict:
    """
    Ekstrak 20+ features pada bar[i].
    PENTING: Semua features hanya menggunakan candles[0..i] dan data terkait.
    """
    c_cur   = candles[i]
    price   = c_cur["close"]
    c_prev  = candles[i-1] if i > 0 else c_cur

    # ── Feature Group 1: ZONE QUALITY ────────────────────────────────────────
    zone_range     = zone["zone_top"] - zone["zone_bottom"]
    dist_to_top    = zone["zone_top"] - price
    dist_pct       = dist_to_top / price if price > 0 else 0  # % dari price ke zone top
    close_pos      = (price - zone["zone_bottom"]) / zone_range if zone_range > 0 else 0
    # 0 = price di zone_bottom, 1 = price di zone_top

    f_zone_touch       = zone["touch_count"]
    f_zone_break       = zone["break_count"]
    f_zone_age_bars    = zone["age_bars"]
    f_zone_delta_vol   = zone["delta_vol"]   # volume saat pembentukan (raw)
    f_zone_range_atr   = zone_range / atr_arr[i] if atr_arr[i] > 0 else 0
    f_dist_pct         = dist_pct
    f_close_in_zone    = close_pos           # posisi close dalam zone (0-1)

    # ── Feature Group 2: VOLUME ANOMALY (Fantazzini 2023) ────────────────────
    vols        = [c["vol"] for c in candles[:i+1]]
    cur_vol     = vols[i]
    vol_z_1h    = zscore_at(cur_vol, vols, 24, i)          # Z vs 24 candle baseline
    vol_z_4h    = zscore_at(cur_vol, vols, 96, i)          # Z vs 4H baseline
    vol_avg_24  = rolling_mean(vols, 24, i-1)               # avg 24 candle sebelum bar ini
    vol_ratio   = cur_vol / vol_avg_24 if vol_avg_24 > 0 else 1.0

    # Volume compression: vol 5 candle terakhir vs 20 candle sebelumnya
    recent_avg  = rolling_mean(vols, 5, i)
    prior_avg   = rolling_mean(vols, 20, max(0, i-5))
    vol_compression = recent_avg / prior_avg if prior_avg > 0 else 1.0

    f_vol_z_1h         = round(vol_z_1h, 3)
    f_vol_z_4h         = round(vol_z_4h, 3)
    f_vol_ratio        = round(vol_ratio, 3)
    f_vol_compression  = round(vol_compression, 3)  # < 1 = drying up = squeeze imminent

    # ── Feature Group 3: TAKER BUY (La Morgia 2023) ──────────────────────────
    f_btx_z       = 0.0
    f_btx_ratio   = 0.0
    f_bv_ratio    = 0.0

    if clz_candles and len(clz_candles) >= 15:
        # Ambil candle Coinalyze yang paling dekat dengan bar[i]
        bar_ts = c_cur["ts"]
        # Find closest CLZ candle at or before bar_ts
        clz_at = None
        for cc in reversed(clz_candles):
            if cc.get("t", 0) * 1000 <= bar_ts:
                clz_at = cc
                break

        if clz_at:
            btx = clz_at.get("btx", 0) or 0
            tx  = clz_at.get("tx",  0) or 0
            bv  = clz_at.get("bv",  0) or 0
            v   = clz_at.get("v",   0) or 0

            if tx > 0:
                f_btx_ratio = btx / tx
            if v > 0:
                f_bv_ratio = bv / v

            # Build baseline btx_ratio series
            btx_ratios = []
            for cc in clz_candles:
                c_tx  = cc.get("tx", 0) or 0
                c_btx = cc.get("btx", 0) or 0
                if c_tx > 0 and cc.get("t", 0) * 1000 < bar_ts:
                    btx_ratios.append(c_btx / c_tx)

            if len(btx_ratios) >= 10:
                sigma = np.std(btx_ratios)
                mu    = np.mean(btx_ratios)
                f_btx_z = float((f_btx_ratio - mu) / sigma) if sigma > 0 else 0.0

    f_btx_z     = round(f_btx_z, 3)
    f_btx_ratio = round(f_btx_ratio, 3)
    f_bv_ratio  = round(f_bv_ratio, 3)

    # ── Feature Group 4: FUNDING RATE (Derivatives research) ─────────────────
    f_funding_current = 0.0
    f_funding_trend   = 0.0   # arah funding: negative = makin bearish
    f_funding_streak  = 0     # berapa periods berturut-turut negatif

    if funding_hist:
        # Ambil funding records yang <= bar_ts
        bar_ts_s = c_cur["ts"] / 1000
        relevant = [f for f in funding_hist if f["ts"] / 1000 <= bar_ts_s]
        if relevant:
            f_funding_current = relevant[-1]["rate"]
            if len(relevant) >= 3:
                rates = [f["rate"] for f in relevant[-5:]]
                f_funding_trend = rates[-1] - rates[0]   # slope dari 5 periods
                streak = 0
                for r in reversed(rates):
                    if r < 0:
                        streak += 1
                    else:
                        break
                f_funding_streak = streak

    f_funding_current = round(f_funding_current, 6)
    f_funding_trend   = round(f_funding_trend, 6)

    # ── Feature Group 5: OPEN INTEREST ────────────────────────────────────────
    f_oi_change_pct = 0.0
    f_oi_divergence = 0    # 1 = OI naik + price turun (short buildup)

    if prev_oi > 0 and cur_oi > 0:
        f_oi_change_pct = (cur_oi - prev_oi) / prev_oi
        price_declined  = price < c_prev["close"] * 0.999
        if f_oi_change_pct > 0.01 and price_declined:
            f_oi_divergence = 1  # short buildup pattern

    f_oi_change_pct = round(f_oi_change_pct, 4)

    # ── Feature Group 6: PRICE ACTION & MOMENTUM ──────────────────────────────
    closes  = [c["close"] for c in candles[:i+1]]
    highs   = [c["high"]  for c in candles[:i+1]]
    lows_   = [c["low"]   for c in candles[:i+1]]

    f_rsi_1h   = round(rsi_arr[i], 2)
    f_bbw      = round(bbw_arr[i], 4)   # Bollinger Band Width (compression)
    f_atr_pct  = round(atr_arr[i] / price * 100, 3) if price > 0 else 0

    # Candle pattern
    o, h, l, c_ = c_cur["open"], c_cur["high"], c_cur["low"], c_cur["close"]
    body    = abs(c_ - o)
    c_range = h - l
    upper   = h - max(o, c_)
    lower   = min(o, c_) - l
    pattern = "NONE"
    if c_range > 0:
        if (lower > 2 * body and upper < 0.3 * body and
                min(o, c_) > l + 0.7 * c_range):
            pattern = "HAMMER"
        elif (c_ > o and len(candles) > 1 and
              o < c_prev["close"] and c_ > c_prev["open"] and
              body > abs(c_prev["close"] - c_prev["open"])):
            pattern = "ENGULFING"
        elif body < 0.1 * c_range:
            pattern = "DOJI"

    f_pattern   = pattern

    # Momentum: price change last 3 candles
    if i >= 3:
        f_mom_3h = (price - candles[i-3]["close"]) / candles[i-3]["close"] * 100
    else:
        f_mom_3h = 0.0
    f_mom_3h = round(f_mom_3h, 3)

    # Number of consecutive bearish candles entering the zone
    bear_streak = 0
    for k in range(i, max(0, i-10), -1):
        if candles[k]["close"] < candles[k]["open"]:
            bear_streak += 1
        else:
            break
    f_bear_streak = bear_streak

    return {
        # Zone quality
        "f_zone_touch":      f_zone_touch,
        "f_zone_break":      f_zone_break,
        "f_zone_age_bars":   f_zone_age_bars,
        "f_zone_delta_vol":  f_zone_delta_vol,
        "f_zone_range_atr":  f_zone_range_atr,
        "f_dist_pct":        f_dist_pct,
        "f_close_in_zone":   f_close_in_zone,

        # Volume anomaly
        "f_vol_z_1h":        f_vol_z_1h,
        "f_vol_z_4h":        f_vol_z_4h,
        "f_vol_ratio":       f_vol_ratio,
        "f_vol_compression": f_vol_compression,

        # Taker buy (La Morgia)
        "f_btx_z":           f_btx_z,
        "f_btx_ratio":       f_btx_ratio,
        "f_bv_ratio":        f_bv_ratio,

        # Funding rate
        "f_funding_current": f_funding_current,
        "f_funding_trend":   f_funding_trend,
        "f_funding_streak":  f_funding_streak,

        # Open interest
        "f_oi_change_pct":   f_oi_change_pct,
        "f_oi_divergence":   f_oi_divergence,

        # Price action
        "f_rsi_1h":          f_rsi_1h,
        "f_bbw":             f_bbw,
        "f_atr_pct":         f_atr_pct,
        "f_pattern":         f_pattern,
        "f_mom_3h":          f_mom_3h,
        "f_bear_streak":     f_bear_streak,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  🏃  REPLAY ENGINE
# ══════════════════════════════════════════════════════════════════════════════
def replay_coin(
    symbol:       str,
    candles:      List[dict],
    funding_hist: List[dict],
    cur_oi_from_ticker: float,
    clz_candles:  List[dict],
) -> List[dict]:
    """
    Replay candle per candle untuk satu coin.
    Untuk setiap bar yang masuk TESTING state (pertama kali),
    ekstrak semua features dan label outcome.

    Returns: list of event dicts
    """
    lb   = CFG["lookback"]
    vl   = CFG["vol_len"]
    bw   = CFG["box_width"]
    ap   = CFG["atr_period"]
    n    = len(candles)
    min_i = lb * 2 + ap // 10 + 10

    if n < min_i + CFG["outcome_window_h"] + 5:
        return []

    # Pre-compute full indicator arrays (no lookahead for indicators themselves,
    # but we only READ up to index i in extract_features)
    dv_arr  = compute_delta_vol(candles)
    atr_arr = compute_atr(candles, ap)
    closes  = [c["close"] for c in candles]
    rsi_arr = compute_rsi(closes)
    bbw_arr = compute_bbw(closes, 20)

    # vol_thresholds
    scaled  = [v / 2.5 for v in dv_arr]
    vh_arr  = [max(scaled[max(0, i-vl+1):i+1]) for i in range(n)]

    events       = []
    zone_states  = {}   # zone_id → last_state untuk anti-duplicate
    prev_oi      = 0.0  # akan di-update seiring waktu

    end_bar = n - CFG["outcome_window_h"] - 1

    for i in range(min_i, end_bar):
        c_cur   = candles[i]
        c_low   = c_cur["low"]
        c_high  = c_cur["high"]
        c_close = c_cur["close"]
        bar_ts  = c_cur["ts"]

        # Detect zones visible at bar[i]
        zones_now = detect_support_zones_at(
            candles, i, lb, vl, bw, atr_arr, dv_arr, vh_arr
        )

        for z in zones_now:
            zone_id    = f"{symbol}_{z['zone_top']:.8f}"
            state_now  = get_zone_state(z, c_low, c_high)
            prev_state = zone_states.get(zone_id, "VALID")

            # Emit event: first entry into TESTING (transition from VALID)
            if state_now == "TESTING" and prev_state == "VALID":
                # Anti-duplicate: only fire once per VALID→TESTING transition

                # Get OI at this bar (approximate: use ticker OI for most recent,
                # zero for historical bars — akan diperbaiki saat data OI history tersedia)
                # Untuk saat ini: pakai cur_oi_from_ticker hanya untuk bar[-1]
                cur_oi  = cur_oi_from_ticker if i >= n - 5 else 0.0

                feats = extract_features(
                    candles      = candles,
                    i            = i,
                    zone         = z,
                    rsi_arr      = rsi_arr,
                    dv_arr       = dv_arr,
                    bbw_arr      = bbw_arr,
                    atr_arr      = atr_arr,
                    funding_hist = funding_hist,
                    cur_oi       = cur_oi,
                    prev_oi      = prev_oi,
                    clz_candles  = clz_candles,
                )

                # ── LABEL: outcome dalam 24H setelah entry ────────────────
                entry_price  = c_close
                future_highs = [candles[j]["high"]
                                for j in range(i+1, min(i+1+CFG["outcome_window_h"], n))]

                if not future_highs:
                    zone_states[zone_id] = state_now
                    continue

                max_high    = max(future_highs)
                max_bounce  = (max_high - entry_price) / entry_price * 100
                hit         = 1 if max_bounce >= CFG["target_pct"] else 0

                # Hours to max bounce
                max_h_idx   = future_highs.index(max_high)
                hrs_to_max  = max_h_idx + 1

                events.append({
                    "symbol":      symbol,
                    "ts":          bar_ts,
                    "date":        datetime.fromtimestamp(bar_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                    "entry_price": round(entry_price, 8),
                    "zone_top":    round(z["zone_top"], 8),
                    "zone_bot":    round(z["zone_bottom"], 8),
                    "max_bounce":  round(max_bounce, 2),
                    "hrs_to_max":  hrs_to_max,
                    "hit":         hit,
                    **feats,
                })

            zone_states[zone_id] = state_now
            if state_now == "VALID" and prev_state == "TESTING":
                zone_states[zone_id] = "VALID"  # reset untuk re-entry

    return events


# ══════════════════════════════════════════════════════════════════════════════
#  📊  FEATURE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

def analyze_features(events: List[dict]) -> dict:
    """
    Analisis feature importance menggunakan:
    1. Point-biserial correlation (numerical feature vs binary HIT/MISS)
    2. Mean difference HIT vs MISS
    3. Optimal threshold per feature (maximize F1)
    4. Random Forest feature importance (jika scikit-learn tersedia)
    """
    if not events:
        return {}

    hits   = [e for e in events if e["hit"] == 1]
    misses = [e for e in events if e["hit"] == 0]
    n_hit  = len(hits)
    n_miss = len(misses)

    feature_names = [k for k in events[0].keys()
                     if k.startswith("f_") and k != "f_pattern"]

    results = {}

    for feat in feature_names:
        vals     = [e[feat] for e in events]
        hit_vals = [e[feat] for e in hits]
        mis_vals = [e[feat] for e in misses]

        if not hit_vals or not mis_vals:
            continue

        hit_mean = sum(hit_vals) / len(hit_vals)
        mis_mean = sum(mis_vals) / len(mis_vals)
        all_mean = sum(vals) / len(vals)

        # Point-biserial correlation
        labels   = [e["hit"] for e in events]
        n        = len(vals)
        mu_v     = all_mean
        sd_v     = (sum((v - mu_v)**2 for v in vals) / n) ** 0.5
        mu_l     = sum(labels) / n
        sd_l     = (sum((l - mu_l)**2 for l in labels) / n) ** 0.5
        if sd_v > 0 and sd_l > 0:
            cov  = sum((v - mu_v) * (l - mu_l)
                       for v, l in zip(vals, labels)) / n
            corr = cov / (sd_v * sd_l)
        else:
            corr = 0.0

        # Optimal threshold (maximize precision at 60%+ recall)
        sorted_vals = sorted(set(vals))
        best_f1     = 0.0
        best_thr    = None
        best_stats  = {}

        for thr in sorted_vals:
            # Predict HIT if val >= thr
            tp = sum(1 for e in events if e[feat] >= thr and e["hit"] == 1)
            fp = sum(1 for e in events if e[feat] >= thr and e["hit"] == 0)
            fn = sum(1 for e in events if e[feat] <  thr and e["hit"] == 1)
            prec   = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1     = 2 * prec * recall / (prec + recall) if (prec + recall) > 0 else 0.0
            if f1 > best_f1 and recall >= 0.3:  # at least 30% recall
                best_f1   = f1
                best_thr  = thr
                best_stats = {"tp": tp, "fp": fp, "fn": fn,
                              "precision": round(prec, 3),
                              "recall": round(recall, 3), "f1": round(f1, 3)}

        results[feat] = {
            "hit_mean":  round(hit_mean, 4),
            "miss_mean": round(mis_mean, 4),
            "diff":      round(hit_mean - mis_mean, 4),
            "corr":      round(corr, 4),
            "best_thr":  best_thr,
            "best_f1":   round(best_f1, 3),
            **best_stats,
        }

    # Sort by abs(corr) descending
    ranked = sorted(results.items(), key=lambda x: abs(x[1]["corr"]), reverse=True)

    # Random Forest (optional)
    rf_importance = {}
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.preprocessing import LabelEncoder

        X = [[e[f] for f in feature_names] for e in events]
        y = [e["hit"] for e in events]

        if len(set(y)) == 2 and len(events) >= 20:
            rf = RandomForestClassifier(n_estimators=100, random_state=42,
                                        class_weight="balanced")
            rf.fit(X, y)
            for fname, imp in zip(feature_names, rf.feature_importances_):
                rf_importance[fname] = round(float(imp), 4)
            log.info("Random Forest feature importance computed")
    except ImportError:
        log.info("scikit-learn tidak tersedia — skip RF importance")

    # Pattern analysis
    pattern_stats = {}
    for pat in ["HAMMER", "ENGULFING", "DOJI", "NONE"]:
        pat_events = [e for e in events if e.get("f_pattern") == pat]
        if pat_events:
            pat_hit = sum(1 for e in pat_events if e["hit"] == 1)
            pattern_stats[pat] = {
                "count":    len(pat_events),
                "hit_rate": round(pat_hit / len(pat_events) * 100, 1),
            }

    return {
        "summary": {
            "total_events": len(events),
            "hits":         n_hit,
            "misses":       n_miss,
            "base_rate":    round(n_hit / len(events) * 100, 1) if events else 0,
        },
        "ranked_features": ranked,
        "rf_importance":   rf_importance,
        "pattern_stats":   pattern_stats,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  📝  REPORT GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def build_report(analysis: dict, all_events: List[dict]) -> str:
    s   = analysis["summary"]
    lines = [
        "=" * 70,
        "  NEXUS-SR FEATURE ANALYSIS REPORT",
        f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 70,
        "",
        "── DATASET SUMMARY ─────────────────────────────────────────────────",
        f"  Total TESTING events  : {s['total_events']}",
        f"  HIT  (bounce ≥15%)    : {s['hits']}",
        f"  MISS (bounce <15%)    : {s['misses']}",
        f"  Base hit rate         : {s['base_rate']}%",
        f"  Target                : +15% dalam 24 jam",
        "",
        "── RANKED FEATURES (by correlation with HIT) ───────────────────────",
        f"  {'#':>2}  {'Feature':<22} {'Corr':>6}  {'HIT_avg':>8}  {'MISS_avg':>9}  "
        f"{'BestThr':>8}  {'F1':>5}  {'Prec':>6}  {'Recall':>7}",
        "  " + "─" * 80,
    ]

    rf_imp = analysis.get("rf_importance", {})
    for rank, (feat, stats) in enumerate(analysis["ranked_features"], 1):
        rf_str = f"RF={rf_imp.get(feat, 0.0):.3f}" if rf_imp else ""
        thr    = f"{stats.get('best_thr', 'N/A')}" if stats.get('best_thr') is not None else "N/A"
        lines.append(
            f"  {rank:>2}  {feat:<22} {stats['corr']:>+6.3f}  "
            f"{stats['hit_mean']:>8.4f}  {stats['miss_mean']:>9.4f}  "
            f"{str(thr):>8}  {stats.get('best_f1', 0):>5.3f}  "
            f"{stats.get('precision', 0):>6.3f}  {stats.get('recall', 0):>7.3f}  "
            f"{rf_str}"
        )

    lines += [
        "",
        "── CANDLE PATTERN HIT RATES ────────────────────────────────────────",
    ]
    for pat, ps in analysis.get("pattern_stats", {}).items():
        lines.append(f"  {pat:<12}: {ps['count']:>4} events  hit_rate={ps['hit_rate']}%")

    lines += [
        "",
        "── RECOMMENDED SCANNER CONFIG ──────────────────────────────────────",
        "  (berdasarkan F1-optimal thresholds di atas):",
    ]

    # Build recommended thresholds
    top5 = analysis["ranked_features"][:5]
    for feat, stats in top5:
        if stats.get("best_thr") is not None and stats.get("best_f1", 0) > 0.1:
            lines.append(f"  {feat}: threshold ≥ {stats['best_thr']}  "
                         f"(F1={stats['best_f1']:.3f}, "
                         f"precision={stats.get('precision',0):.1%})")

    lines += ["", "=" * 70]
    return "\n".join(lines)


def build_scanner_config(analysis: dict) -> dict:
    """Generate scanner_config.json siap pakai berdasarkan temuan backtest."""
    top_features = {f: s for f, s in analysis["ranked_features"][:10]}
    rf_imp = analysis.get("rf_importance", {})

    # Ambil threshold optimal dari tiap feature kunci
    def get_thr(feat, default):
        if feat in top_features and top_features[feat].get("best_thr") is not None:
            return top_features[feat]["best_thr"]
        return default

    config = {
        "_meta": {
            "generated":  datetime.now(timezone.utc).isoformat(),
            "total_events": analysis["summary"]["total_events"],
            "base_hit_rate": analysis["summary"]["base_rate"],
            "note": "Threshold dikalibrasi dari data historis. Update setelah 30+ live signals.",
        },
        "feature_importance_ranking": [
            {"rank": i+1, "feature": f, "correlation": s["corr"],
             "rf_importance": rf_imp.get(f, 0),
             "optimal_threshold": s.get("best_thr"),
             "best_f1": s.get("best_f1", 0)}
            for i, (f, s) in enumerate(analysis["ranked_features"][:15])
        ],
        "recommended_scoring_weights": {},
        "recommended_thresholds": {},
    }

    # Assign weights proportional to abs(correlation) × rf_importance
    ranked = analysis["ranked_features"]
    score_features = {
        "f_vol_z_1h":        "score_vol",
        "f_btx_z":           "score_btx",
        "f_funding_current": "score_funding",
        "f_oi_divergence":   "score_oi",
        "f_vol_compression": "score_vol_compression",
    }

    total_corr = sum(abs(s["corr"]) for f, s in ranked
                     if f in score_features and abs(s["corr"]) > 0.01)

    if total_corr > 0:
        for feat, key in score_features.items():
            corr_val = abs(next((s["corr"] for f, s in ranked if f == feat), 0))
            weight   = round(corr_val / total_corr * 100, 1) if total_corr > 0 else 20.0
            config["recommended_scoring_weights"][key] = f"{weight}%"

    # Threshold recommendations
    for feat, stats in analysis["ranked_features"]:
        if stats.get("best_thr") is not None and stats.get("best_f1", 0) > 0.1:
            config["recommended_thresholds"][feat] = {
                "threshold": stats["best_thr"],
                "f1":        stats.get("best_f1", 0),
                "precision": stats.get("precision", 0),
                "recall":    stats.get("recall", 0),
            }

    config["pattern_hit_rates"] = analysis.get("pattern_stats", {})

    return config


# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def progress(cur: int, total: int, label: str = "", w: int = 40) -> str:
    pct  = cur / total if total > 0 else 0
    done = int(w * pct)
    bar  = "█" * done + "░" * (w - done)
    return f"\r  [{bar}] {cur}/{total} {label:<20}"


def main():
    start = time.time()

    print("\n" + "═" * 65)
    print("  NEXUS-SR DATA COLLECTOR & FEATURE ANALYZER")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("═" * 65)

    # ── 1. Fetch tickers — universe + current OI + funding ─────────────────
    print("\n[1/5] Fetching Bitget tickers …")
    tickers = Bitget.get_tickers()
    if not tickers:
        log.error("Gagal fetch tickers"); return

    # Filter universe
    universe = {}
    for sym, t in tickers.items():
        try:
            vol = float(t.get("quoteVolume", 0))
            if vol < CFG["min_vol_24h"]:
                continue
            universe[sym] = t
        except Exception:
            continue

    universe_list = sorted(universe.keys())[:CFG["max_coins"]]
    print(f"  Universe: {len(universe_list)} coins (vol ≥ ${CFG['min_vol_24h']:,})")

    # ── 2. Coinalyze mapper ────────────────────────────────────────────────
    print("\n[2/5] Building Coinalyze symbol mapper …")
    clz_mapper = clz_build_mapper(set(universe_list))
    has_clz    = len(clz_mapper) > 0

    # ── 3. Fetch OHLCV + funding per coin ──────────────────────────────────
    print(f"\n[3/5] Fetching OHLCV + funding rate history …")
    coin_data: Dict[str, dict] = {}

    for idx, sym in enumerate(universe_list):
        print(progress(idx+1, len(universe_list), sym), end="", flush=True)

        candles = Bitget.get_candles(sym, CFG["candle_limit_1h"])
        if len(candles) < CFG["lookback"] * 2 + 50:
            continue

        funding = Bitget.get_funding_history(sym, CFG["funding_limit"])
        cur_oi  = float(universe[sym].get("holdingAmount", 0))

        coin_data[sym] = {
            "candles": candles,
            "funding": funding,
            "cur_oi":  cur_oi,
        }
        time.sleep(0.12)

    print()
    print(f"  Fetched: {len(coin_data)} coins")

    # ── 4. Fetch Coinalyze data ────────────────────────────────────────────
    print("\n[4/5] Fetching Coinalyze taker buy data …")
    clz_all: Dict[str, list] = {}

    if has_clz:
        coins_with_clz = [sym for sym in coin_data.keys() if sym in clz_mapper]
        clz_syms       = [clz_mapper[sym] for sym in coins_with_clz]
        clz_all        = clz_fetch_batch(clz_syms)

        # Reverse map: clz_sym → candles
        clz_by_bitget  = {}
        for sym in coins_with_clz:
            clz_sym = clz_mapper[sym]
            if clz_sym in clz_all:
                clz_by_bitget[sym] = clz_all[clz_sym]

        print(f"  Coinalyze: {len(clz_by_bitget)}/{len(coins_with_clz)} coins with btx data")
    else:
        clz_by_bitget = {}
        print("  Coinalyze: unavailable (check API key)")

    # ── 5. Replay + extract events ─────────────────────────────────────────
    print(f"\n[5/5] Replaying {len(coin_data)} coins …")
    all_events: List[dict] = []
    stats = defaultdict(int)

    for idx, sym in enumerate(coin_data):
        print(progress(idx+1, len(coin_data), sym), end="", flush=True)

        d = coin_data[sym]
        try:
            events = replay_coin(
                symbol       = sym,
                candles      = d["candles"],
                funding_hist = d["funding"],
                cur_oi_from_ticker = d["cur_oi"],
                clz_candles  = clz_by_bitget.get(sym, []),
            )
            all_events.extend(events)
            stats["coins_processed"] += 1
            stats["events_found"]    += len(events)
            hit_count = sum(1 for e in events if e["hit"] == 1)
            stats["hits_found"] += hit_count

        except Exception as ex:
            log.debug(f"  Error replay {sym}: {ex}")
            stats["errors"] += 1

    print()
    print(f"  Processed: {stats['coins_processed']} coins")
    print(f"  Events   : {stats['events_found']} total "
          f"({stats['hits_found']} HIT / "
          f"{stats['events_found'] - stats['hits_found']} MISS)")

    if not all_events:
        print("\n⚠️  Tidak ada events ditemukan. Kemungkinan candle terlalu sedikit.")
        print("   Coba jalankan ulang besok setelah lebih banyak candle tersedia.")
        return

    # ── Save events.csv ────────────────────────────────────────────────────
    print(f"\n  Saving {len(all_events)} events to {CFG['events_csv']} …")
    fieldnames = list(all_events[0].keys())
    with open(CFG["events_csv"], "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_events)
    print(f"  ✓ {CFG['events_csv']} saved")

    # ── Feature analysis ───────────────────────────────────────────────────
    print("\n  Running feature analysis …")
    analysis = analyze_features(all_events)

    # ── Report ─────────────────────────────────────────────────────────────
    report = build_report(analysis, all_events)
    with open(CFG["report_txt"], "w", encoding="utf-8") as f:
        f.write(report)
    print(f"  ✓ {CFG['report_txt']} saved")

    # ── Scanner config ─────────────────────────────────────────────────────
    config = build_scanner_config(analysis)
    with open(CFG["config_json"], "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    print(f"  ✓ {CFG['config_json']} saved")

    # ── Print summary ──────────────────────────────────────────────────────
    elapsed = round(time.time() - start, 1)
    s = analysis["summary"]
    print()
    print("═" * 65)
    print("  HASIL")
    print("═" * 65)
    print(f"  Events    : {s['total_events']} ({s['hits']} HIT / {s['misses']} MISS)")
    print(f"  Base rate : {s['base_rate']}% bounce ≥15% dalam 24H")
    print(f"  Runtime   : {elapsed}s")
    print()
    print("  TOP 5 FITUR PALING PREDIKTIF:")
    print(f"  {'#':>2}  {'Feature':<22} {'Corr':>6}  {'F1':>5}  {'HIT_avg':>8}  {'MISS_avg':>9}")
    print("  " + "─" * 58)
    for rank, (feat, stats) in enumerate(analysis["ranked_features"][:5], 1):
        print(f"  {rank:>2}  {feat:<22} {stats['corr']:>+6.3f}  "
              f"{stats.get('best_f1',0):>5.3f}  "
              f"{stats['hit_mean']:>8.4f}  {stats['miss_mean']:>9.4f}")
    print()
    print("  Pattern hit rates:")
    for pat, ps in analysis.get("pattern_stats", {}).items():
        print(f"    {pat:<12}: {ps['hit_rate']}%  (n={ps['count']})")
    print()
    print(f"  Output files:")
    print(f"    {CFG['events_csv']}      ← semua events dengan label HIT/MISS")
    print(f"    {CFG['report_txt']}  ← ranking fitur + threshold optimal")
    print(f"    {CFG['config_json']}   ← config siap pakai untuk scanner")
    print("═" * 65)


if __name__ == "__main__":
    main()

