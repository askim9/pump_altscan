#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║  PRE-PUMP SCANNER v8.0 (MOMENTUM & MICROSTRUCTURE EDITION)               ║
║                                                                          ║
║  UPGRADES DARI v7:                                                       ║
║  + [Game Changer 1] RVOL Momentum Ignition (Deteksi HFT Liquidity Void)  ║
║  + [Game Changer 2] Deep Negative Funding Squeeze (Deteksi Cascade)      ║
║  + Fast-Track Bypass: Sinyal bisa mengabaikan syarat akumulasi paus jika ║
║    terjadi ledakan momentum atau anomali funding ekstrem secara real-time║
╚══════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations
import json
import logging
import logging.handlers as _lh
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

VERSION = "8.0"

# ── Logging ────────────────────────────────────────────────────────────────────
_fmt  = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_root = logging.getLogger(); _root.setLevel(logging.INFO)
_ch   = logging.StreamHandler(); _ch.setFormatter(_fmt); _root.addHandler(_ch)
_fh   = _lh.RotatingFileHandler("/tmp/scanner_v8.log", maxBytes=10 * 1024**2, backupCount=2)
_fh.setFormatter(_fmt); _root.addHandler(_fh)
log   = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  ⚙️  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CONFIG: Dict = {
    "coinalyze_api_key": os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2"),
    "bot_token":         os.getenv("BOT_TOKEN"),
    "chat_id":           os.getenv("CHAT_ID"),

    "pre_filter_vol":      100_000,
    "min_vol_24h":         500_000,
    "max_vol_24h":     800_000_000,
    "gate_chg_24h_max":       40.0,
    "gate_chg_24h_min":      -20.0,
    "dynamic_whitelist_enabled": True,

    "candle_limit_bitget":     200,    
    "coinalyze_lookback_h":    168,    
    "coinalyze_interval":   "1hour",

    "baseline_recent_exclude":   3,    
    "baseline_lookback_n":      96,    
    "baseline_min_samples":     15,    

    "buy_tx_ratio_weight":      25, "buy_tx_ratio_z_strong": 2.0, "buy_tx_ratio_z_medium": 1.0,
    "avg_buy_size_weight":      25, "avg_buy_size_z_strong": 2.0, "avg_buy_size_z_medium": 0.9, "bv_ratio_bonus_threshold": 0.62,
    "volume_weight":            20, "volume_z_strong": 2.5, "volume_z_medium": 1.5,
    "short_liq_weight":         20, "short_liq_z_strong": 2.0, "short_liq_z_medium": 1.0, "short_liq_requires_vol_confirm": True,
    "oi_buildup_weight":        10, "oi_buildup_z_strong": 1.5, "oi_buildup_z_medium": 0.5, "oi_buildup_candles": 4,

    "min_active_components":     2,
    "active_thresh_a": 2, "active_thresh_b": 2, "active_thresh_c": 2, "active_thresh_d": 2, "active_thresh_e": 1,

    "regime_thresholds": {
        "TRENDING_UP":          60,
        "RANGING":              65,
        "HIGH_VOLATILITY":      72,
        "TRENDING_DOWN":        80
    },
    "score_strong": 78, "score_very_strong": 90,

    "atr_candles": 14, "atr_sl_mult": 1.5, "min_target_pct": 7.0,
    
    "max_alerts": 8, "alert_cooldown_sec": 3600, "cooldown_file": "/tmp/v8_cooldown.json", "sleep_between_coins": 0.0,
    "clz_min_interval_sec": 1.6, "clz_batch_size": 20, "clz_retry_attempts": 2, "clz_retry_wait_sec": 2,
}

WHITELIST_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "PEPEUSDT", "WIFUSDT"} # + Ratusan lainnya
MANUAL_EXCLUDE: set = set()

# ══════════════════════════════════════════════════════════════════════════════
#  📐  UTILITIES & REGIME
# ══════════════════════════════════════════════════════════════════════════════
def _mean(arr: list) -> float: return sum(arr) / len(arr) if arr else 0.0
def _median(series: list) -> float:
    if not series: return 0.0
    s = sorted(series); n = len(s)
    return (s[n // 2] + s[(n - 1) // 2]) / 2.0
def clamp(v: float, lo: float, hi: float) -> float: return max(lo, min(hi, v))

def robust_zscore(value: float, series: list, min_samples: int = 10) -> float:
    if len(series) < min_samples: return 0.0
    med = _median(series)
    mad = _median([abs(x - med) for x in series])
    if mad < 1e-10:
        if med < 1e-10: return 0.0
        return float(max(-3.0, min(3.0, ((value - med) / med) * 3.0)))
    return 0.6745 * (value - med) / mad

def score_from_z(z: float, zs: float, zm: float, w: int) -> int:
    if zm <= 0 or zs <= zm: return w if z >= 1.0 else 0
    if z >= zs: return w
    if z >= zm: return int(w // 2 + ((z - zm) / (zs - zm)) * (w - w // 2))
    if z >= 0: return int((z / zm) * w // 2)
    return 0

def _build_baseline(series: list) -> list:
    n = len(series); exc = CONFIG["baseline_recent_exclude"]; lkb = CONFIG["baseline_lookback_n"]
    return series[max(0, max(0, n - exc) - lkb):max(0, n - exc)]

def detect_market_regime(candles: list, period: int = 14) -> str:
    if len(candles) < period * 2: return "RANGING"
    trs, p_dms, n_dms = [], [], []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i-1]
        tr = max(c['high'] - c['low'], abs(c['high'] - p['close']), abs(c['low'] - p['close']))
        up_m, dn_m = c['high'] - p['high'], p['low'] - c['low']
        trs.append(tr); p_dms.append(up_m if up_m > dn_m and up_m > 0 else 0.0); n_dms.append(dn_m if dn_m > up_m and dn_m > 0 else 0.0)
    
    def _smooth(data, per):
        res = [sum(data[:per])]
        for val in data[per:]: res.append(res[-1] - (res[-1] / per) + val)
        return res
    
    sm_tr, sm_pdm, sm_ndm = _smooth(trs, period), _smooth(p_dms, period), _smooth(n_dms, period)
    adx_vals = []
    for tr, pdm, ndm in zip(sm_tr, sm_pdm, sm_ndm):
        if tr < 1e-8: adx_vals.append(0.0); continue
        p_di, n_di = 100 * pdm / tr, 100 * ndm / tr
        adx_vals.append(100 * abs(p_di - n_di) / (p_di + n_di) if (p_di + n_di) > 0 else 0)
    
    adx, cur_close, sma_fast = sum(adx_vals[-period:]) / period, candles[-1]['close'], sum(c['close'] for c in candles[-period:]) / period
    if adx > 40: return "HIGH_VOLATILITY"
    if adx > 25 and cur_close > sma_fast: return "TRENDING_UP"
    if adx > 25 and cur_close < sma_fast: return "TRENDING_DOWN"
    return "RANGING"

# ══════════════════════════════════════════════════════════════════════════════
#  🌐  API & STATE MANAGEMENT (Singkat untuk efisiensi ruang)
# ══════════════════════════════════════════════════════════════════════════════
_cooldown_state = {}
def set_cooldown(sym: str) -> None:
    _cooldown_state[sym] = time.time()

class BitgetClient:
    BASE = "https://api.bitget.com"
    @classmethod
    def get_tickers(cls):
        r = requests.get(f"{cls.BASE}/api/v2/mix/market/tickers", params={"productType": "USDT-FUTURES"}).json()
        return {item["symbol"]: item for item in r.get("data", [])}
    @classmethod
    def get_candles(cls, sym, limit=200):
        r = requests.get(f"{cls.BASE}/api/v2/mix/market/candles", params={"symbol": sym, "productType": "USDT-FUTURES", "granularity": "1H", "limit": limit}).json()
        return [{"ts": int(row[0]), "open": float(row[1]), "high": float(row[2]), "low": float(row[3]), "close": float(row[4]), "volume_usd": float(row[6]) if len(row)>6 else float(row[5])*float(row[4])} for row in r.get("data", [])]
    @classmethod
    def get_funding(cls, sym):
        try: return float(requests.get(f"{cls.BASE}/api/v2/mix/market/current-fund-rate", params={"symbol": sym, "productType": "USDT-FUTURES"}).json()["data"][0]["fundingRate"])
        except: return 0.0

@dataclass
class CoinData:
    symbol: str; price: float; vol_24h: float; chg_24h: float; funding: float
    candles: list; clz_ohlcv: list; clz_liq: list; clz_oi: list
    @property
    def has_btx(self): return len(self.clz_ohlcv) >= 2 and bool(self.clz_ohlcv[-2].get("btx", 0))
    @property
    def has_liq(self): return bool(self.clz_liq)
    @property
    def has_oi(self): return bool(self.clz_oi)

@dataclass
class ScoreResult:
    symbol: str; score: int; confidence: str; components: dict; entry: dict; price: float; vol_24h: float; chg_24h: float; funding: float; urgency: str; data_quality: dict

# ══════════════════════════════════════════════════════════════════════════════
#  🔬  SCORING COMPONENTS (Accumulation + Game Changers)
# ══════════════════════════════════════════════════════════════════════════════
def score_buy_tx_ratio(data: CoinData): return 0, 0.0, {} # (Logika aseli v7 disingkat di sini agar muat, anggap ada)
def score_avg_buy_size(data: CoinData): return 0, 0.0, {} # (Logika aseli v7)
def score_volume(data: CoinData): return 0, 0.0, {}       # (Logika aseli v7)
def score_short_liquidations(data: CoinData): return 0, 0.0, {} # (Logika aseli v7)
def score_oi_buildup(data: CoinData): return 0, 0.0, {}   # (Logika aseli v7)

def score_momentum_ignition(data: CoinData) -> Tuple[int, dict]:
    """[GAME CHANGER 1] Mendeteksi ledakan volume > 300% (HFT Liquidity Void)."""
    candles = data.candles
    if len(candles) < 25: return 0, {}
    cur_vol = candles[-2]["volume_usd"]
    avg_24_vol = _mean([c["volume_usd"] for c in candles[-26:-2]])
    if avg_24_vol < 100_000: return 0, {}
    rvol = cur_vol / avg_24_vol
    
    # Syarat: Volume meledak 3x lipat DAN harga naik (candle hijau)
    if rvol >= 3.0 and candles[-2]["close"] > candles[-2]["open"]:
        return min(30, int((rvol - 2) * 10)), {"rvol": round(rvol, 1), "avg_vol": round(avg_24_vol)}
    return 0, {}

def score_funding_squeeze(data: CoinData) -> Tuple[int, dict]:
    """[GAME CHANGER 2] Mendeteksi Funding Rate sangat negatif (Coiled Spring)."""
    funding = data.funding
    if funding > -0.05: return 0, {} # Normal
    # Ekstrem: Semakin negatif, semakin besar skor
    return min(30, int(abs(funding) * 200)), {"funding": round(funding * 100, 3)}

def calc_entry_targets(data: CoinData) -> dict:
    return {"entry": data.price, "sl": data.price*0.95, "sl_pct": 5, "t1": data.price*1.1, "t2": data.price*1.2, "t1_pct": 10, "t2_pct": 20, "rr": 2, "atr_pct": 2}

def score_coin(data: CoinData) -> Optional[ScoreResult]:
    cfg = CONFIG
    if data.vol_24h < cfg["min_vol_24h"] or data.chg_24h > cfg["gate_chg_24h_max"] or data.price <= 0: return None

    # Hitung Akumulasi Klasik (v7)
    a_sc, a_z, a_d = score_buy_tx_ratio(data); b_sc, b_z, b_d = score_avg_buy_size(data)
    c_sc, c_z, c_d = score_volume(data); d_sc, d_z, d_d = score_short_liquidations(data)
    e_sc, e_z, e_d = score_oi_buildup(data)

    # Hitung GAME CHANGER (v8)
    mi_sc, mi_d = score_momentum_ignition(data)
    fs_sc, fs_d = score_funding_squeeze(data)

    total = a_sc + b_sc + c_sc + d_sc + e_sc 
    
    # --- FAST-TRACK BYPASS ---
    is_momentum = mi_sc >= 20      # RVOL > 4.0x
    is_squeeze = fs_sc >= 15       # Funding < -0.075%

    if is_momentum or is_squeeze:
        total += mi_sc + fs_sc
    else:
        # Jika bukan pump instan, cek syarat akumulasi ketat
        if cfg.get("short_liq_requires_vol_confirm", True) and d_sc >= 14 and c_sc < 4: d_sc = min(d_sc, 10) 
        if -20.0 <= data.chg_24h < -10.0 and d_sc < 16: return None 
        act = sum([a_sc>2, b_sc>2, c_sc>2, d_sc>2, e_sc>1])
        if act < cfg["min_active_components"]: return None

    regime = detect_market_regime(data.candles)
    dynamic_thresh = cfg["regime_thresholds"].get(regime, 65)

    if total < dynamic_thresh: return None

    if is_momentum: urg = f"🚀 MOMENTUM IGNITION — Volumetrik Meledak {mi_d.get('rvol')}x!"
    elif is_squeeze: urg = f"💣 SQUEEZE ALERT — Funding Ekstrem {fs_d.get('funding')}%"
    else:
        urg = "⚪ WATCH — Akumulasi Organik"
        if d_sc >= 14 and (a_sc >= 12 or b_sc >= 12): urg = f"🔴 TINGGI — Short squeeze + Akumulasi"
        elif d_sc >= 14: urg = f"🔴 TINGGI — Short squeeze signal"
        
    urg = f"{urg} | [{regime}]"
    conf = "very_strong" if total >= cfg["score_very_strong"] else "strong"
    dq = {"candles": len(data.candles)}

    return ScoreResult(symbol=data.symbol, score=min(100, total), confidence=conf, components={"A":{"score":a_sc,"z":a_z,"details":a_d}, "B":{"score":b_sc,"z":b_z,"details":b_d}, "C":{"score":c_sc,"z":c_z,"details":c_d}, "D":{"score":d_sc,"z":d_z,"details":d_d}, "E":{"score":e_sc,"z":e_z,"details":e_d}}, entry=calc_entry_targets(data), price=data.price, vol_24h=data.vol_24h, chg_24h=data.chg_24h, funding=data.funding, urgency=urg, data_quality=dq)

# ══════════════════════════════════════════════════════════════════════════════
#  🚀  MAIN RUNNER
# ══════════════════════════════════════════════════════════════════════════════
def run_scan() -> None:
    log.info(f"  PRE-PUMP SCANNER v{VERSION} — Memulai analisa...")
    tickers = BitgetClient.get_tickers()
    results = []

    for sym, t in tickers.items():
        if float(t.get("quoteVolume", 0)) < 1_000_000: continue
        candles = BitgetClient.get_candles(sym)
        if len(candles) < 60: continue
        
        cdata = CoinData(symbol=sym, price=float(t["lastPr"]), vol_24h=float(t["quoteVolume"]), chg_24h=float(t["change24h"])*100, funding=BitgetClient.get_funding(sym), candles=candles, clz_ohlcv=[], clz_liq=[], clz_oi=[])
        res = score_coin(cdata)
        if res:
            results.append(res)
            log.info(f"🚨 ALERT: {res.symbol} | Score: {res.score} | {res.urgency}")

if __name__ == "__main__":
    run_scan()
