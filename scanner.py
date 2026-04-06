#!/usr/bin/env python3
"""
COINALYZE API PROBE 2 — Investigasi exchange yang tersedia dan alternatif Bitget.
"""

import json, os, time, requests
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

API_KEY = os.getenv("COINALYZE_API_KEY", "ab447e9a-3a26-4253-a68e-1cd0603d22d2")
BASE    = "https://api.coinalyze.net/v1"

def get(endpoint, params=None, timeout=15):
    p = dict(params or {}); p["api_key"] = API_KEY
    try:
        r = requests.get(f"{BASE}/{endpoint}", params=p, timeout=timeout)
        return r.status_code, r.json() if r.content else None
    except Exception as e:
        return None, str(e)

def sep(title=""):
    print("\n" + "═"*65)
    if title:
        print(f"  {title}")
        print("─"*65)

def main():
    print("\n" + "═"*65)
    print("  COINALYZE PROBE 2 — Exchange + Alternative Investigation")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("═"*65)

    # ── 1. List semua exchanges ───────────────────────────────────────
    sep("TEST 1: SEMUA EXCHANGES YANG TERSEDIA")
    code, data = get("exchanges")
    print(f"  HTTP {code}, total: {len(data) if isinstance(data, list) else 0}")
    if isinstance(data, list):
        for ex in data:
            print(f"  · name={ex.get('name',''):<20} code={ex.get('code','')}")
    time.sleep(2)

    # ── 2. Semua future markets — group by exchange ───────────────────
    sep("TEST 2: FUTURE MARKETS PER EXCHANGE")
    code, markets = get("future-markets")
    print(f"  HTTP {code}, total markets: {len(markets) if isinstance(markets, list) else 0}")
    if isinstance(markets, list):
        from collections import Counter
        ex_count = Counter(m.get("exchange","") for m in markets)
        for ex, cnt in ex_count.most_common(30):
            print(f"  · {ex:<30} {cnt:>5} markets")
    time.sleep(2)

    # ── 3. Cari semua exchange yang punya USDT perpetual + OI data ────
    sep("TEST 3: EXCHANGE DENGAN DATA TERLENGKAP (has_ohlcv + has_ls)")
    if isinstance(markets, list):
        ex_stats = {}
        for m in markets:
            ex = m.get("exchange","")
            if ex not in ex_stats:
                ex_stats[ex] = {"total":0, "has_ohlcv":0, "has_ls":0, "has_bs":0,
                                "perpetual":0, "usdt_perp":0}
            s = ex_stats[ex]
            s["total"] += 1
            if m.get("has_ohlcv_data"):     s["has_ohlcv"] += 1
            if m.get("has_long_short_ratio_data"): s["has_ls"] += 1
            if m.get("has_buy_sell_data"):  s["has_bs"] += 1
            if m.get("is_perpetual"):       s["perpetual"] += 1
            if m.get("is_perpetual") and m.get("quote_asset","").upper() in ("USDT","USD"):
                s["usdt_perp"] += 1

        print(f"  {'Exchange':<25} {'Total':>5} {'OHLCV':>6} {'L/S':>5} {'BuySell':>8} {'USDT_Perp':>10}")
        print("  " + "─"*65)
        for ex, s in sorted(ex_stats.items(), key=lambda x: -x[1]["usdt_perp"]):
            if s["usdt_perp"] > 0:
                print(f"  {ex:<25} {s['total']:>5} {s['has_ohlcv']:>6} {s['has_ls']:>5} "
                      f"{s['has_bs']:>8} {s['usdt_perp']:>10}")
    time.sleep(2)

    # ── 4. Cari Binance BTC/ETH untuk test baseline ───────────────────
    sep("TEST 4: SAMPLE SYMBOLS DARI EXCHANGE TERBESAR")
    if isinstance(markets, list):
        # Ambil semua exchange unik
        exchanges_list = list(set(m.get("exchange","") for m in markets))
        
        # Cari BTCUSDT perpetual di semua exchange
        btc_syms = [m for m in markets
                    if "BTC" in m.get("base_asset","").upper()
                    and m.get("is_perpetual")
                    and m.get("quote_asset","").upper() in ("USDT","USD")]
        
        print(f"  BTC perpetual markets: {len(btc_syms)}")
        for m in btc_syms[:15]:
            print(f"  · {m['exchange']:<20} symbol={m['symbol']:<30} "
                  f"on_ex={m.get('symbol_on_exchange','')}")
    time.sleep(2)

    # ── 5. Test OHLCV untuk Binance BTC (baseline apakah API works) ───
    sep("TEST 5: OHLCV HISTORY TEST (Binance BTCUSDT sebagai baseline)")
    # Cari symbol Binance BTCUSDT dari markets
    binance_btc = None
    if isinstance(markets, list):
        for m in markets:
            ex = m.get("exchange","").lower()
            sym = m.get("symbol","")
            base = m.get("base_asset","").upper()
            quote = m.get("quote_asset","").upper()
            perp = m.get("is_perpetual", False)
            if "binance" in ex and base == "BTC" and quote == "USDT" and perp:
                binance_btc = sym
                print(f"  Found Binance BTC: {sym}")
                break

    if binance_btc:
        now = int(time.time())
        from_ts = now - 24 * 3600
        code, data = get("ohlcv-history", {
            "symbols": binance_btc,
            "interval": "1hour",
            "from": from_ts, "to": now
        })
        print(f"  HTTP {code}")
        if code == 200 and isinstance(data, list) and data:
            hist = data[0].get("history", [])
            print(f"  Candles returned: {len(hist)}")
            if hist:
                print(f"  Fields: {list(hist[-1].keys())}")
                print(f"  Last candle: {hist[-1]}")
                btx_nulls = sum(1 for c in hist if not c.get("btx"))
                print(f"  btx zeros: {btx_nulls}/{len(hist)}")
    time.sleep(2)

    # ── 6. Test OI history untuk Binance BTC ─────────────────────────
    sep("TEST 6: OPEN INTEREST HISTORY (Binance BTC)")
    if binance_btc:
        now = int(time.time())
        from_ts = now - 24 * 3600
        code, data = get("open-interest-history", {
            "symbols": binance_btc,
            "interval": "1hour",
            "from": from_ts, "to": now,
            "convert_to_usd": "true"
        })
        print(f"  HTTP {code}")
        if code == 200 and isinstance(data, list) and data:
            hist = data[0].get("history", [])
            print(f"  Candles returned: {len(hist)}")
            if hist:
                print(f"  Fields: {list(hist[-1].keys())}")
                print(f"  Last 3: {hist[-3:]}")
    time.sleep(2)

    # ── 7. Funding rate history ───────────────────────────────────────
    sep("TEST 7: FUNDING RATE HISTORY (Binance BTC)")
    if binance_btc:
        now = int(time.time())
        from_ts = now - 7 * 24 * 3600  # 7 hari
        code, data = get("funding-rate-history", {
            "symbols": binance_btc,
            "interval": "8hour",
            "from": from_ts, "to": now
        })
        print(f"  HTTP {code}")
        if code == 200 and isinstance(data, list) and data:
            hist = data[0].get("history", [])
            print(f"  Entries returned: {len(hist)}")
            if hist:
                print(f"  Fields: {list(hist[-1].keys())}")
                print(f"  Last 3: {hist[-3:]}")
    time.sleep(2)

    # ── 8. Long/Short ratio ───────────────────────────────────────────
    sep("TEST 8: LONG/SHORT RATIO HISTORY (Binance BTC)")
    if binance_btc:
        now = int(time.time())
        from_ts = now - 24 * 3600
        code, data = get("long-short-ratio-history", {
            "symbols": binance_btc,
            "interval": "1hour",
            "from": from_ts, "to": now
        })
        print(f"  HTTP {code}")
        if code == 200 and isinstance(data, list) and data:
            hist = data[0].get("history", [])
            print(f"  Entries returned: {len(hist)}")
            if hist:
                print(f"  Fields: {list(hist[-1].keys())}")
                print(f"  Last 3: {hist[-3:]}")
    time.sleep(2)

    # ── 9. Liquidation history ────────────────────────────────────────
    sep("TEST 9: LIQUIDATION HISTORY (Binance BTC)")
    if binance_btc:
        now = int(time.time())
        from_ts = now - 24 * 3600
        code, data = get("liquidation-history", {
            "symbols": binance_btc,
            "interval": "1hour",
            "from": from_ts, "to": now,
            "convert_to_usd": "true"
        })
        print(f"  HTTP {code}")
        if code == 200 and isinstance(data, list) and data:
            hist = data[0].get("history", [])
            print(f"  Entries returned: {len(hist)}")
            if hist:
                print(f"  Fields: {list(hist[-1].keys())}")
                print(f"  Last 3: {hist[-3:]}")
    time.sleep(2)

    # ── 10. Current endpoints ─────────────────────────────────────────
    sep("TEST 10: CURRENT (REALTIME) ENDPOINTS")
    if binance_btc:
        code, data = get("open-interest", {
            "symbols": binance_btc, "convert_to_usd": "true"
        })
        print(f"  /open-interest HTTP {code}: {data}")
        time.sleep(2)

        code2, data2 = get("funding-rate", {"symbols": binance_btc})
        print(f"  /funding-rate HTTP {code2}: {data2}")
        time.sleep(2)

        code3, data3 = get("predicted-funding-rate", {"symbols": binance_btc})
        print(f"  /predicted-funding-rate HTTP {code3}: {data3}")

    # ── 11. Cari exchange yang mirip Bitget (USDT perp + coin sama) ───
    sep("TEST 11: COIN OVERLAP — exchange mana yang ada SOLUSDT/FETUSDT/INJUSDT?")
    target_bases = {"SOL", "FET", "INJ", "ARB", "OP", "WIF", "PEPE", "BONK"}
    if isinstance(markets, list):
        ex_overlap = {}
        for m in markets:
            base  = m.get("base_asset","").upper()
            quote = m.get("quote_asset","").upper()
            perp  = m.get("is_perpetual", False)
            ex    = m.get("exchange","")
            if perp and quote in ("USDT","USD") and base in target_bases:
                if ex not in ex_overlap:
                    ex_overlap[ex] = set()
                ex_overlap[ex].add(base)

        for ex, bases in sorted(ex_overlap.items(), key=lambda x: -len(x[1])):
            print(f"  {ex:<25}: {sorted(bases)}")

    # ── 12. Simpan hasil ──────────────────────────────────────────────
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "exchanges": data if code == 200 else [],
        "bitget_found": False,
        "binance_btc_symbol": binance_btc,
        "markets_per_exchange": {
            ex: s for ex, s in ex_stats.items()
        } if isinstance(markets, list) else {},
    }
    with open("coinalyze_probe2_results.json", "w") as f:
        json.dump(results, f, indent=2)

    sep("SELESAI")
    print("  Kirim seluruh output ini ke Claude.")
    print("  File: coinalyze_probe2_results.json")
    print("═"*65)

if __name__ == "__main__":
    main()
