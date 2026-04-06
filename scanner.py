#!/usr/bin/env python3
"""
COINALYZE API PROBE — Jalankan ini di PC sebelum integrasi ke scanner.

Tujuan:
  1. Verifikasi API key aktif
  2. Temukan format symbol Bitget yang benar
  3. Validasi response format setiap endpoint
  4. Ukur data availability (berapa persen coin punya data)
  5. Simpan hasil ke coinalyze_probe_results.json

Usage: python coinalyze_probe.py
"""

import json
import os
import time
import requests
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

API_KEY = os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2")
BASE    = "https://api.coinalyze.net/v1"

# Coin yang pasti ada di Bitget untuk test
TEST_COINS_BITGET = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT", "LINKUSDT",
    "FETUSDT",  "INJUSDT", "ARBUSDT",  "OPUSDT",
]

def get(endpoint, params=None, timeout=15):
    p = dict(params or {})
    p["api_key"] = API_KEY
    try:
        r = requests.get(f"{BASE}/{endpoint}", params=p, timeout=timeout)
        return r.status_code, r.json() if r.content else None
    except Exception as e:
        return None, str(e)

def sep(title=""):
    print()
    print("═" * 65)
    if title:
        print(f"  {title}")
        print("─" * 65)

# ══════════════════════════════════════════════════════════════════
def probe_auth():
    sep("TEST 1: AUTHENTICATION")
    code, data = get("exchanges")
    print(f"  Status: HTTP {code}")
    if code == 200 and isinstance(data, list):
        print(f"  ✅ API key valid — {len(data)} exchanges")
        print(f"  Sample: {[d.get('name') or d.get('code') for d in data[:5]]}")
        return True
    elif code == 401:
        print("  ❌ Invalid API key")
    else:
        print(f"  ❌ Unexpected: {data}")
    return False

# ══════════════════════════════════════════════════════════════════
def probe_future_markets():
    sep("TEST 2: FUTURE MARKETS — cari symbol Bitget")
    code, data = get("future-markets")
    print(f"  Status: HTTP {code}")
    if code != 200 or not isinstance(data, list):
        print(f"  ❌ Failed: {data}")
        return {}

    print(f"  Total markets: {len(data)}")

    # Field availability check
    sample = data[0] if data else {}
    print(f"  Fields in response: {list(sample.keys())}")

    # Filter Bitget
    bitget = [m for m in data if "bitget" in m.get("exchange", "").lower()]
    print(f"  Bitget markets: {len(bitget)}")

    if bitget:
        print(f"\n  Sample Bitget markets:")
        for m in bitget[:8]:
            print(f"    symbol={m['symbol']:<30}  "
                  f"on_exchange={m.get('symbol_on_exchange',''):<20}  "
                  f"has_ls={m.get('has_long_short_ratio_data')}  "
                  f"has_ohlcv={m.get('has_ohlcv_data')}  "
                  f"has_buy_sell={m.get('has_buy_sell_data')}")

    # Build Bitget → Coinalyze mapping
    mapping = {}
    for m in bitget:
        exch_sym = m.get("symbol_on_exchange", "")
        clz_sym  = m.get("symbol", "")
        # Clean exchange symbol
        clean = (exch_sym
                 .replace("_UMCBL", "")
                 .replace("_DMCBL", "")
                 .replace("_CMCBL", "")
                 .upper())
        if clean:
            mapping[clean] = {
                "clz_symbol":   clz_sym,
                "has_ls":       m.get("has_long_short_ratio_data", False),
                "has_ohlcv":    m.get("has_ohlcv_data", False),
                "has_buy_sell": m.get("has_buy_sell_data", False),
                "is_perpetual": m.get("is_perpetual", False),
            }

    # Check our test coins
    print(f"\n  Test coin mapping:")
    found = 0
    for coin in TEST_COINS_BITGET:
        info = mapping.get(coin)
        if info:
            found += 1
            print(f"    ✅ {coin:<15} → {info['clz_symbol']}")
        else:
            print(f"    ❌ {coin:<15} → NOT FOUND")

    print(f"\n  Mapped: {found}/{len(TEST_COINS_BITGET)} test coins")
    print(f"  Total Bitget mappings: {len(mapping)}")

    # Check data availability stats
    has_ls    = sum(1 for v in mapping.values() if v["has_ls"])
    has_ohlcv = sum(1 for v in mapping.values() if v["has_ohlcv"])
    has_bs    = sum(1 for v in mapping.values() if v["has_buy_sell"])
    print(f"\n  Data availability:")
    print(f"    has_long_short_ratio: {has_ls}/{len(mapping)} = {has_ls/max(len(mapping),1)*100:.0f}%")
    print(f"    has_ohlcv (btx/bv)  : {has_ohlcv}/{len(mapping)} = {has_ohlcv/max(len(mapping),1)*100:.0f}%")
    print(f"    has_buy_sell        : {has_bs}/{len(mapping)} = {has_bs/max(len(mapping),1)*100:.0f}%")

    return mapping

# ══════════════════════════════════════════════════════════════════
def probe_ohlcv(clz_symbols):
    sep("TEST 3: OHLCV HISTORY (btx, bv, tx)")
    sym_str  = ",".join(clz_symbols[:3])
    now      = int(time.time())
    from_ts  = now - 48 * 3600  # 48 jam
    code, data = get("ohlcv-history", {
        "symbols": sym_str, "interval": "1hour",
        "from": from_ts, "to": now
    })
    print(f"  Status: HTTP {code}")
    if code != 200 or not isinstance(data, list):
        print(f"  ❌ Failed: {data}")
        return

    print(f"  Items returned: {len(data)}")
    for item in data:
        sym = item.get("symbol", "")
        hist = item.get("history", [])
        print(f"\n  Symbol: {sym}")
        print(f"  History length: {len(hist)}")
        if hist:
            # Fields check
            sample = hist[-1]
            print(f"  Last candle fields: {list(sample.keys())}")
            print(f"  Last candle values: {sample}")
            # Check btx availability
            btx_null = sum(1 for c in hist if c.get("btx") is None or c.get("btx") == 0)
            tx_null  = sum(1 for c in hist if c.get("tx")  is None or c.get("tx")  == 0)
            bv_null  = sum(1 for c in hist if c.get("bv")  is None or c.get("bv")  == 0)
            print(f"  btx zeros/null: {btx_null}/{len(hist)} = {btx_null/len(hist)*100:.0f}%")
            print(f"  tx  zeros/null: {tx_null}/{len(hist)}  = {tx_null/len(hist)*100:.0f}%")
            print(f"  bv  zeros/null: {bv_null}/{len(hist)}  = {bv_null/len(hist)*100:.0f}%")

# ══════════════════════════════════════════════════════════════════
def probe_open_interest(clz_symbols):
    sep("TEST 4: OPEN INTEREST HISTORY")
    sym_str  = ",".join(clz_symbols[:3])
    now      = int(time.time())
    from_ts  = now - 48 * 3600
    code, data = get("open-interest-history", {
        "symbols": sym_str, "interval": "1hour",
        "from": from_ts, "to": now,
        "convert_to_usd": "true"
    })
    print(f"  Status: HTTP {code}")
    if code != 200 or not isinstance(data, list):
        print(f"  ❌ Failed: {data}")
        return

    for item in data:
        sym  = item.get("symbol", "")
        hist = item.get("history", [])
        print(f"\n  Symbol: {sym}")
        print(f"  History length: {len(hist)}")
        if hist:
            sample = hist[-1]
            print(f"  Fields: {list(sample.keys())}")
            print(f"  Last 3 candles: {hist[-3:]}")
            # OI change per hour
            if len(hist) >= 2:
                oi_prev = hist[-2].get("c", 0)
                oi_curr = hist[-1].get("c", 0)
                oi_chg  = (oi_curr - oi_prev) / oi_prev * 100 if oi_prev > 0 else 0
                print(f"  OI change (last hr): {oi_chg:+.2f}%")

# ══════════════════════════════════════════════════════════════════
def probe_funding_rate(clz_symbols):
    sep("TEST 5: FUNDING RATE HISTORY")
    # Funding rate biasanya 8jam interval — test dengan 8hour
    sym_str  = ",".join(clz_symbols[:3])
    now      = int(time.time())
    from_ts  = now - 30 * 24 * 3600  # 30 hari
    code, data = get("funding-rate-history", {
        "symbols": sym_str, "interval": "8hour",
        "from": from_ts, "to": now
    })
    print(f"  Status: HTTP {code}")
    if code != 200 or not isinstance(data, list):
        print(f"  ❌ Failed: {data}")
        return

    for item in data:
        sym  = item.get("symbol", "")
        hist = item.get("history", [])
        print(f"\n  Symbol: {sym}")
        print(f"  History length: {len(hist)}")
        if hist:
            print(f"  Fields: {list(hist[-1].keys())}")
            print(f"  Last 3 entries: {hist[-3:]}")
            # Nilai null check
            nulls = sum(1 for c in hist if c.get("c") is None)
            print(f"  Null values: {nulls}/{len(hist)}")
            if hist:
                vals = [c.get("c", 0) for c in hist if c.get("c") is not None]
                if vals:
                    print(f"  Range: {min(vals):.6f} to {max(vals):.6f}")

# ══════════════════════════════════════════════════════════════════
def probe_long_short(clz_symbols):
    sep("TEST 6: LONG/SHORT RATIO HISTORY")
    # Filter hanya yang has_long_short = True
    sym_str  = ",".join(clz_symbols[:3])
    now      = int(time.time())
    from_ts  = now - 48 * 3600
    code, data = get("long-short-ratio-history", {
        "symbols": sym_str, "interval": "1hour",
        "from": from_ts, "to": now
    })
    print(f"  Status: HTTP {code}")
    if code != 200 or not isinstance(data, list):
        print(f"  ❌ Failed: {data}")
        return

    for item in data:
        sym  = item.get("symbol", "")
        hist = item.get("history", [])
        print(f"\n  Symbol: {sym}")
        print(f"  History length: {len(hist)}")
        if hist:
            print(f"  Fields: {list(hist[-1].keys())}")
            print(f"  Last 3 entries: {hist[-3:]}")
            # l = long ratio, s = short ratio, r = ratio
            last = hist[-1]
            print(f"  Latest: long={last.get('l'):.3f} short={last.get('s'):.3f} "
                  f"ratio={last.get('r'):.3f}")

# ══════════════════════════════════════════════════════════════════
def probe_liquidations(clz_symbols):
    sep("TEST 7: LIQUIDATION HISTORY")
    sym_str  = ",".join(clz_symbols[:3])
    now      = int(time.time())
    from_ts  = now - 48 * 3600
    code, data = get("liquidation-history", {
        "symbols": sym_str, "interval": "1hour",
        "from": from_ts, "to": now,
        "convert_to_usd": "true"
    })
    print(f"  Status: HTTP {code}")
    if code != 200 or not isinstance(data, list):
        print(f"  ❌ Failed: {data}")
        return

    for item in data:
        sym  = item.get("symbol", "")
        hist = item.get("history", [])
        print(f"\n  Symbol: {sym}")
        print(f"  History length: {len(hist)}")
        if hist:
            print(f"  Fields: {list(hist[-1].keys())}")
            print(f"  Last 3 entries: {hist[-3:]}")
            # l = long liq, s = short liq
            last = hist[-1]
            print(f"  Latest: long_liq=${last.get('l',0):,.0f} "
                  f"short_liq=${last.get('s',0):,.0f}")

# ══════════════════════════════════════════════════════════════════
def probe_current_endpoints(clz_symbols):
    sep("TEST 8: CURRENT (REALTIME) ENDPOINTS")
    sym_str = ",".join(clz_symbols[:5])

    # Current OI
    code, data = get("open-interest", {"symbols": sym_str, "convert_to_usd": "true"})
    print(f"  /open-interest: HTTP {code}")
    if code == 200 and data:
        print(f"  Fields: {list(data[0].keys())}")
        print(f"  Sample: {data[0]}")

    time.sleep(2)

    # Current funding rate
    code2, data2 = get("funding-rate", {"symbols": sym_str})
    print(f"\n  /funding-rate: HTTP {code2}")
    if code2 == 200 and data2:
        print(f"  Fields: {list(data2[0].keys())}")
        print(f"  Sample: {data2[:3]}")

    time.sleep(2)

    # Predicted funding rate
    code3, data3 = get("predicted-funding-rate", {"symbols": sym_str})
    print(f"\n  /predicted-funding-rate: HTTP {code3}")
    if code3 == 200 and data3:
        print(f"  Sample: {data3[:3]}")

# ══════════════════════════════════════════════════════════════════
def probe_rate_limit():
    sep("TEST 9: RATE LIMIT")
    print("  Mengirim 5 request berturut-turut …")
    for i in range(5):
        t0   = time.time()
        code, _ = get("exchanges")
        elapsed = time.time() - t0
        print(f"  Call {i+1}: HTTP {code}, {elapsed*1000:.0f}ms")
        time.sleep(1.5)

# ══════════════════════════════════════════════════════════════════
def main():
    print("\n" + "═" * 65)
    print("  COINALYZE API PROBE")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  API Key: {API_KEY[:8]}…{API_KEY[-4:]}")
    print("═" * 65)

    # Test 1: Auth
    if not probe_auth():
        print("\nGanti API key dan jalankan ulang.")
        return

    time.sleep(2)

    # Test 2: Markets
    mapping = probe_future_markets()
    time.sleep(2)

    if not mapping:
        print("\nTidak bisa lanjut tanpa mapping.")
        return

    # Ambil CLZ symbols untuk test coins
    clz_syms = []
    for coin in TEST_COINS_BITGET:
        if coin in mapping:
            clz_syms.append(mapping[coin]["clz_symbol"])

    print(f"\n  CLZ symbols untuk testing: {clz_syms[:5]}")
    time.sleep(2)

    # Test 3–7: Endpoint probes
    probe_ohlcv(clz_syms)
    time.sleep(2)

    probe_open_interest(clz_syms)
    time.sleep(2)

    probe_funding_rate(clz_syms)
    time.sleep(2)

    # Long/short: hanya test coin yang punya data
    ls_syms = [mapping[c]["clz_symbol"] for c in TEST_COINS_BITGET
               if c in mapping and mapping[c]["has_ls"]]
    if ls_syms:
        probe_long_short(ls_syms[:3])
    else:
        print("\n  TEST 6: Tidak ada coin Bitget dengan has_long_short=True di sample")
    time.sleep(2)

    probe_liquidations(clz_syms)
    time.sleep(2)

    probe_current_endpoints(clz_syms)
    time.sleep(2)

    probe_rate_limit()

    # Simpan mapping ke JSON
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_key_prefix": API_KEY[:8],
        "mapping_sample": {k: v for k, v in list(mapping.items())[:20]},
        "total_bitget_mapped": len(mapping),
        "test_coin_mapping": {
            c: mapping[c]["clz_symbol"]
            for c in TEST_COINS_BITGET if c in mapping
        },
    }
    with open("coinalyze_probe_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print("\n")
    print("═" * 65)
    print("  PROBE SELESAI")
    print("  Kirim output terminal ini dan coinalyze_probe_results.json")
    print("  ke Claude untuk build integrasi yang akurat.")
    print("═" * 65)

if __name__ == "__main__":
    main()
