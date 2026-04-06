#!/usr/bin/env python3
"""
COINALYZE PROBE 3 — Test semua endpoint dengan symbol yang benar.

Dari probe 2:
  Binance = code "A"  → BTCUSDT_PERP.A / BTCUSD_PERP.A
  Bybit   = code "6"  → BTCUSDT.6, has_long_short=564 markets
  Gate.io = code "Y"  → BTC_USDT.Y
  OKX     = code "3"  → BTCUSD_PERP.3

Exchange dengan has_long_short_ratio:
  Binance (A): 602 markets
  Bybit   (6): 564 markets ← paling banyak L/S data
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
    r = requests.get(f"{BASE}/{endpoint}", params=p, timeout=timeout)
    return r.status_code, r.json() if r.content else None

def sep(title=""):
    print("\n" + "═"*65)
    if title: print(f"  {title}\n" + "─"*65)

def main():
    print("\n" + "═"*65)
    print("  COINALYZE PROBE 3 — Endpoint Tests dengan Symbol Benar")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("═"*65)

    now     = int(time.time())
    from_48h = now - 48 * 3600
    from_7d  = now - 7 * 24 * 3600

    # ── Dari probe 2: kita tahu ini symbolnya ────────────────────────
    # Binance: exchange code = A
    # Bybit  : exchange code = 6 (has L/S data)
    BN_BTC  = "BTCUSDT_PERP.A"
    BN_ETH  = "ETHUSDT_PERP.A"
    BN_SOL  = "SOLUSDT_PERP.A"
    BY_BTC  = "BTCUSDT.6"
    BY_ETH  = "ETHUSDT.6"
    BY_SOL  = "SOLUSDT.6"

    # ── Step 1: Verifikasi symbol dari future-markets ─────────────────
    sep("STEP 1: Cari exact symbols dari markets list")
    code, markets = get("future-markets")
    print(f"  HTTP {code}, total: {len(markets)}")

    # Binance USDT perps
    bn_usdt = {m["symbol_on_exchange"]: m["symbol"]
               for m in markets
               if m.get("exchange") == "A"
               and m.get("is_perpetual")
               and m.get("quote_asset","").upper() == "USDT"}
    print(f"\n  Binance USDT perps: {len(bn_usdt)}")
    for k in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
               "FETUSDT", "INJUSDT", "ARBUSDT", "OPUSDT", "WIFUSDT"]:
        sym = bn_usdt.get(k, "NOT FOUND")
        print(f"    {k:<15} → {sym}")

    # Bybit USDT perps (has L/S data)
    by_usdt = {m["symbol_on_exchange"]: m
               for m in markets
               if m.get("exchange") == "6"
               and m.get("is_perpetual")
               and m.get("quote_asset","").upper() == "USDT"}
    by_ls = {k: v for k, v in by_usdt.items() if v.get("has_long_short_ratio_data")}
    print(f"\n  Bybit USDT perps: {len(by_usdt)}, has L/S: {len(by_ls)}")
    for k in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "FETUSDT", "INJUSDT"]:
        info = by_usdt.get(k)
        if info:
            sym = info["symbol"]
            has_ls = info.get("has_long_short_ratio_data", False)
            print(f"    {k:<15} → {sym}  has_ls={has_ls}")
        else:
            print(f"    {k:<15} → NOT FOUND")

    # Ambil actual symbol strings
    bn_btc = bn_usdt.get("BTCUSDT", BN_BTC)
    bn_eth = bn_usdt.get("ETHUSDT", BN_ETH)
    bn_sol = bn_usdt.get("SOLUSDT", BN_SOL)
    by_btc_info = by_usdt.get("BTCUSDT", {})
    by_btc = by_btc_info.get("symbol", BY_BTC) if isinstance(by_btc_info, dict) else BY_BTC

    print(f"\n  Using: bn_btc={bn_btc}  by_btc={by_btc}")
    time.sleep(2)

    # ── Step 2: OHLCV Binance (btx, bv, tx) ─────────────────────────
    sep("STEP 2: OHLCV HISTORY — btx, bv, tx availability")
    batch = ",".join([bn_btc, bn_eth, bn_sol])
    code, data = get("ohlcv-history", {
        "symbols": batch, "interval": "1hour",
        "from": from_48h, "to": now
    })
    print(f"  HTTP {code}")
    if code == 200 and isinstance(data, list):
        for item in data:
            sym  = item["symbol"]
            hist = item.get("history", [])
            print(f"\n  {sym}: {len(hist)} candles")
            if hist:
                last = hist[-1]
                print(f"    Fields  : {list(last.keys())}")
                print(f"    Last    : {last}")
                # Data quality
                for field in ["v", "bv", "tx", "btx"]:
                    nulls = sum(1 for c in hist if not c.get(field))
                    print(f"    {field} zeros: {nulls}/{len(hist)} = {nulls/len(hist)*100:.0f}%")
    time.sleep(2)

    # ── Step 3: Open Interest Binance ────────────────────────────────
    sep("STEP 3: OPEN INTEREST HISTORY")
    code, data = get("open-interest-history", {
        "symbols": batch, "interval": "1hour",
        "from": from_48h, "to": now,
        "convert_to_usd": "true"
    })
    print(f"  HTTP {code}")
    if code == 200 and isinstance(data, list):
        for item in data:
            sym  = item["symbol"]
            hist = item.get("history", [])
            print(f"\n  {sym}: {len(hist)} candles")
            if hist:
                print(f"    Fields  : {list(hist[-1].keys())}")
                print(f"    Last 3  : {hist[-3:]}")
                # OI change
                if len(hist) >= 2:
                    prev = hist[-2].get("c", 0)
                    curr = hist[-1].get("c", 0)
                    chg  = (curr-prev)/prev*100 if prev > 0 else 0
                    print(f"    OI chg  : {chg:+.3f}%  (prev={prev:,.0f}, curr={curr:,.0f})")
    time.sleep(2)

    # ── Step 4: Funding Rate ─────────────────────────────────────────
    sep("STEP 4: FUNDING RATE HISTORY")
    code, data = get("funding-rate-history", {
        "symbols": batch, "interval": "8hour",
        "from": from_7d, "to": now
    })
    print(f"  HTTP {code}")
    if code == 200 and isinstance(data, list):
        for item in data:
            sym  = item["symbol"]
            hist = item.get("history", [])
            print(f"\n  {sym}: {len(hist)} entries")
            if hist:
                print(f"    Fields  : {list(hist[-1].keys())}")
                print(f"    Last 3  : {hist[-3:]}")
                vals = [c.get("c", 0) for c in hist if c.get("c") is not None]
                if vals:
                    print(f"    Range   : {min(vals):.6f} to {max(vals):.6f}")
                    neg = sum(1 for v in vals if v < 0)
                    print(f"    Negative: {neg}/{len(vals)} = {neg/len(vals)*100:.0f}%")
    time.sleep(2)

    # ── Step 5: Long/Short Bybit (has L/S data) ──────────────────────
    sep("STEP 5: LONG/SHORT RATIO HISTORY (Bybit)")
    by_batch_syms = []
    for k in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "FETUSDT"]:
        info = by_usdt.get(k)
        if info and info.get("has_long_short_ratio_data"):
            by_batch_syms.append(info["symbol"])
    if not by_batch_syms:
        by_batch_syms = [by_btc]
    print(f"  Using Bybit symbols: {by_batch_syms[:4]}")
    code, data = get("long-short-ratio-history", {
        "symbols": ",".join(by_batch_syms[:4]),
        "interval": "1hour",
        "from": from_48h, "to": now
    })
    print(f"  HTTP {code}")
    if code == 200 and isinstance(data, list):
        for item in data:
            sym  = item["symbol"]
            hist = item.get("history", [])
            print(f"\n  {sym}: {len(hist)} entries")
            if hist:
                last = hist[-1]
                print(f"    Fields  : {list(last.keys())}")
                print(f"    Last 3  : {hist[-3:]}")
                # Check: l=long_ratio, s=short_ratio, r=ratio
                print(f"    Latest  : long={last.get('l'):.4f}  "
                      f"short={last.get('s'):.4f}  ratio={last.get('r'):.4f}")
    time.sleep(2)

    # ── Step 6: Liquidations ─────────────────────────────────────────
    sep("STEP 6: LIQUIDATION HISTORY")
    code, data = get("liquidation-history", {
        "symbols": batch, "interval": "1hour",
        "from": from_48h, "to": now,
        "convert_to_usd": "true"
    })
    print(f"  HTTP {code}")
    if code == 200 and isinstance(data, list):
        for item in data:
            sym  = item["symbol"]
            hist = item.get("history", [])
            print(f"\n  {sym}: {len(hist)} entries")
            if hist:
                print(f"    Fields  : {list(hist[-1].keys())}")
                print(f"    Last 3  : {hist[-3:]}")
                last = hist[-1]
                print(f"    Latest  : long_liq=${last.get('l',0):,.0f}  "
                      f"short_liq=${last.get('s',0):,.0f}")
                # Total 48h liquidations
                total_long  = sum(c.get("l", 0) for c in hist)
                total_short = sum(c.get("s", 0) for c in hist)
                print(f"    48h sum : long_liq=${total_long:,.0f}  short_liq=${total_short:,.0f}")
    time.sleep(2)

    # ── Step 7: Current realtime ──────────────────────────────────────
    sep("STEP 7: CURRENT REALTIME ENDPOINTS")
    code, data = get("open-interest", {"symbols": bn_btc, "convert_to_usd": "true"})
    print(f"  /open-interest  HTTP {code}: {data}")
    time.sleep(1)

    code2, data2 = get("funding-rate", {"symbols": bn_btc})
    print(f"  /funding-rate   HTTP {code2}: {data2}")
    time.sleep(1)

    code3, data3 = get("predicted-funding-rate", {"symbols": bn_btc})
    print(f"  /predicted-fr   HTTP {code3}: {data3}")
    time.sleep(2)

    # ── Step 8: Batch size test — 20 symbols ─────────────────────────
    sep("STEP 8: BATCH SIZE TEST (20 symbols)")
    bn_sample = list(bn_usdt.values())[:20]
    print(f"  Sending {len(bn_sample)} symbols batch …")
    code, data = get("ohlcv-history", {
        "symbols": ",".join(bn_sample),
        "interval": "1hour",
        "from": now - 3600, "to": now
    })
    returned = len(data) if isinstance(data, list) else 0
    print(f"  HTTP {code}, returned {returned} items")
    if isinstance(data, list):
        for item in data[:3]:
            h = item.get("history", [])
            print(f"    {item['symbol']}: {len(h)} candles")
    time.sleep(2)

    # ── Step 9: Altcoin L/S availability di Bybit ────────────────────
    sep("STEP 9: ALTCOIN L/S AVAILABILITY (Bybit)")
    # Cek berapa banyak altcoin di Bybit punya L/S data
    by_ls_list = [v["symbol"] for v in by_usdt.values()
                  if v.get("has_long_short_ratio_data")]
    print(f"  Bybit symbols with L/S data: {len(by_ls_list)}")
    # Test batch of 10 altcoins
    by_test = [v["symbol"] for k, v in by_usdt.items()
               if v.get("has_long_short_ratio_data")
               and k not in ("BTCUSDT", "ETHUSDT")][:10]
    print(f"  Testing altcoins: {by_test[:5]} …")
    if by_test:
        code, data = get("long-short-ratio-history", {
            "symbols": ",".join(by_test[:10]),
            "interval": "1hour",
            "from": now - 12*3600, "to": now
        })
        print(f"  HTTP {code}")
        if isinstance(data, list):
            for item in data[:5]:
                h = item.get("history", [])
                print(f"    {item['symbol']}: {len(h)} entries")
    time.sleep(2)

    # ── Step 10: Bybit-Bitget coin overlap ───────────────────────────
    sep("STEP 10: BYBIT vs BITGET COIN OVERLAP")
    # Bitget universe dari probe sebelumnya ada ~470 coins
    # Berapa yang ada di Bybit dengan L/S data?
    by_bases = {v.get("symbol_on_exchange","").replace("USDT","")
                for v in by_usdt.values()
                if v.get("has_long_short_ratio_data")}
    print(f"  Bybit coins dengan L/S: {len(by_bases)}")
    # Sample
    print(f"  Sample: {sorted(by_bases)[:30]}")

    # ── Simpan hasil ──────────────────────────────────────────────────
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "binance_btc_symbol": bn_btc,
        "bybit_btc_symbol": by_btc,
        "binance_usdt_perps": bn_usdt,
        "bybit_usdt_perps_with_ls": {
            k: v["symbol"] for k, v in by_usdt.items()
            if v.get("has_long_short_ratio_data")
        },
        "bybit_all_usdt_perps": {k: v["symbol"] for k, v in by_usdt.items()},
    }
    with open("coinalyze_probe3_results.json", "w") as f:
        json.dump(results, f, indent=2)

    sep("SELESAI")
    print("  Kirim seluruh output dan coinalyze_probe3_results.json")
    print("═"*65)

if __name__ == "__main__":
    main()
