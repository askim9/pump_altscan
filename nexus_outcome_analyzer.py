#!/usr/bin/env python3
"""
nexus_outcome_analyzer.py — NEXUS-PB Outcome Tracker
Versi: 1.0.0 | NEXUS-PB Sprint 1

Jalankan SEBELUM nexus_pb.py di setiap GitHub Actions run.

CLI: python nexus_outcome_analyzer.py [--hours N]
  --hours N : lookback window untuk update (default: 48)

Alur:
  1. Fetch semua tickers Bitget (harga sekarang)
  2. Query nexus_signals WHERE checked=0 AND alerted_at <= now-3600
  3. Untuk setiap sinyal:
     a. Hitung elapsed = now - alerted_at
     b. Hitung ret = (cur_price - entry_price) / entry_price * 100
     c. Update max_return = MAX(max_return, ret) SETIAP RUN
     d. Update min_return = MIN(min_return, ret) SETIAP RUN
     e. Set return_1h  jika elapsed >= 1h  dan masih NULL
     f. Set return_3h  jika elapsed >= 3h  dan masih NULL
     g. Set return_6h  jika elapsed >= 6h  dan masih NULL
     h. Set return_12h jika elapsed >= 12h dan masih NULL
     i. Jika elapsed >= 24h dan return_24h NULL → close sinyal
  4. Print precision report

FIX BUG-06 (nexus_pb.py line 376):
  nexus_pb.py menggunakan: new_min <= sl_price   ← SALAH (% vs harga)
  Analyzer ini menggunakan: min_return <= -sl_pct ← BENAR (% vs %)
  sl_pct dari DB sudah positif (e.g., 8.0), jadi SL kena saat return <= -8.0%
"""

import argparse
import logging
import sqlite3
import time
from typing import Dict, Optional

import requests

# ══════════════════════════════════════════════════════════════════════
#  KONFIGURASI
# ══════════════════════════════════════════════════════════════════════
DB_PATH            = "nexus_pb_history.db"
OUTCOME_WINDOW_H   = 24          # jam — 1 window NEXUS-PB
BITGET_TIMEOUT     = 15
VERSION            = "1.0.0"

# ══════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("nexus_outcome")


# ══════════════════════════════════════════════════════════════════════
#  BITGET: FETCH TICKERS
# ══════════════════════════════════════════════════════════════════════
def fetch_bitget_tickers() -> Dict[str, float]:
    """
    Ambil semua tickers Bitget USDT-Futures.
    Return: dict {symbol: last_price}
    """
    try:
        resp = requests.get(
            "https://api.bitget.com/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"},
            headers={"User-Agent": f"NexusPB-Analyzer/{VERSION}"},
            timeout=BITGET_TIMEOUT,
        )
        data = resp.json()
        result = {}
        for t in data.get("data", []):
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            try:
                result[sym] = float(t.get("lastPr", 0) or 0)
            except (ValueError, TypeError):
                continue
        log.info(f"  Bitget tickers: {len(result)} symbols")
        return result
    except Exception as e:
        log.warning(f"  Bitget ticker fetch error: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════
#  DB HELPER
# ══════════════════════════════════════════════════════════════════════
def get_pending_signals(lookback_hours: int) -> list:
    """
    Query sinyal yang belum checked dan sudah lewat 1 jam.
    Hanya ambil sinyal dalam lookback window.
    """
    try:
        conn  = sqlite3.connect(DB_PATH)
        c     = conn.cursor()
        now   = int(time.time())
        since = now - (lookback_hours * 3600)

        c.execute("""
            SELECT
                id, symbol, alerted_at, entry_price,
                sl_pct,
                return_1h, return_3h, return_6h, return_12h, return_24h,
                max_return, min_return
            FROM nexus_signals
            WHERE checked=0
              AND alerted_at <= ?
              AND alerted_at >= ?
            ORDER BY alerted_at ASC
        """, (now - 3600, since))
        rows = c.fetchall()
        conn.close()
        log.info(f"  Pending sinyal (last {lookback_hours}h): {len(rows)}")
        return rows
    except Exception as e:
        log.warning(f"  get_pending_signals error: {e}")
        return []


def update_signal_outcomes(
    conn: sqlite3.Connection,
    row_id: int,
    updates: dict,
) -> None:
    """Generic UPDATE untuk satu sinyal berdasarkan dict updates."""
    if not updates:
        return
    cols   = ", ".join(f"{k}=?" for k in updates)
    vals   = list(updates.values()) + [row_id]
    conn.execute(f"UPDATE nexus_signals SET {cols} WHERE id=?", vals)


# ══════════════════════════════════════════════════════════════════════
#  MAIN OUTCOME UPDATE LOGIC
# ══════════════════════════════════════════════════════════════════════
def run_outcome_update(lookback_hours: int) -> dict:
    """
    Update outcomes untuk semua sinyal pending.

    Return: stats dict untuk precision report.
    """
    tickers = fetch_bitget_tickers()
    if not tickers:
        log.warning("  Tidak ada ticker — outcome update dilewati")
        return {}

    rows    = get_pending_signals(lookback_hours)
    if not rows:
        log.info("  Tidak ada sinyal pending untuk diupdate")
        # Tetap cetak report dari data yang sudah ada
        return _collect_precision_stats()

    now        = int(time.time())
    n_updated  = 0
    n_closed   = 0

    try:
        conn = sqlite3.connect(DB_PATH)

        for row in rows:
            (row_id, symbol, alerted_at, entry_price,
             sl_pct,
             r1h, r3h, r6h, r12h, r24h,
             max_ret, min_ret) = row

            # Validasi data dasar
            if not entry_price or entry_price <= 0:
                log.debug(f"  Skip {symbol}: entry_price invalid")
                continue

            cur_price = tickers.get(symbol, 0.0)
            if cur_price <= 0:
                log.debug(f"  Skip {symbol}: tidak ada harga ticker")
                continue

            elapsed = now - alerted_at

            # Hitung return % saat ini dari entry
            ret = round((cur_price - entry_price) / entry_price * 100, 2)

            # Update max_return dan min_return setiap run
            new_max = max(v for v in [max_ret, ret] if v is not None)
            new_min = min(v for v in [min_ret, ret] if v is not None)
            new_max = round(new_max, 2)
            new_min = round(new_min, 2)

            upd: dict = {
                "max_return": new_max,
                "min_return": new_min,
            }

            # Update return_Xh jika waktunya sudah tiba dan masih NULL
            # (set sekali, tidak dioverwrite)
            if elapsed >= 3600     and r1h  is None: upd["return_1h"]  = ret
            if elapsed >= 3 * 3600 and r3h  is None: upd["return_3h"]  = ret
            if elapsed >= 6 * 3600 and r6h  is None: upd["return_6h"]  = ret
            if elapsed >= 12 * 3600 and r12h is None: upd["return_12h"] = ret

            # Close sinyal di 24 jam
            if elapsed >= OUTCOME_WINDOW_H * 3600 and r24h is None:
                # FIX BUG-06: hit_sl harus cek min_return <= -sl_pct
                # Bukan min_return <= sl_price (type mismatch di nexus_pb.py L376)
                hit_15  = 1 if new_max >= 15.0 else 0
                hit_10  = 1 if new_max >= 10.0 else 0

                if sl_pct and sl_pct > 0:
                    hit_sl = 1 if new_min <= -sl_pct else 0
                else:
                    hit_sl = 0  # sl_pct tidak valid → konservatif: tidak dihitung kena SL

                upd.update({
                    "return_24h": ret,
                    "hit_15pct":  hit_15,
                    "hit_10pct":  hit_10,
                    "hit_sl":     hit_sl,
                    "checked":    1,
                })
                n_closed += 1
                log.info(
                    f"  ✅ CLOSED {symbol}: max={new_max:+.1f}% min={new_min:+.1f}% "
                    f"hit15={hit_15} hit10={hit_10} hitSL={hit_sl} "
                    f"(sl_pct=-{sl_pct:.1f}%)"
                )
            else:
                elapsed_h = elapsed / 3600
                log.info(
                    f"  ~ {symbol}: elapsed={elapsed_h:.1f}h "
                    f"cur={ret:+.1f}% max={new_max:+.1f}% min={new_min:+.1f}%"
                )

            update_signal_outcomes(conn, row_id, upd)
            n_updated += 1

        conn.commit()
        conn.close()
        log.info(f"  Outcome update: {n_updated} updated, {n_closed} closed (24h)")

    except Exception as e:
        log.warning(f"  run_outcome_update DB error: {e}")

    return _collect_precision_stats()


# ══════════════════════════════════════════════════════════════════════
#  PRECISION STATS (UNTUK REPORT)
# ══════════════════════════════════════════════════════════════════════
def _safe_pct(numerator: float, denominator: float) -> float:
    return round(numerator / denominator * 100, 1) if denominator > 0 else 0.0


def _collect_precision_stats() -> dict:
    """
    Kumpulkan stats dari nexus_signals untuk precision report.
    Hanya rows dengan checked=1.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c    = conn.cursor()

        # Overall
        c.execute("""
            SELECT
                COUNT(*) AS n,
                SUM(hit_15pct) AS h15,
                SUM(hit_10pct) AS h10,
                SUM(hit_sl)    AS hsl,
                AVG(max_return) AS avg_max,
                COUNT(CASE WHEN checked=0 THEN 1 END) AS pending
            FROM nexus_signals
        """)
        row = c.fetchone()
        n, h15, h10, hsl, avg_max, pending = row if row else (0, 0, 0, 0, 0, 0)

        # Per squeeze_duration_h bracket
        c.execute("""
            SELECT
                CASE WHEN squeeze_duration_h < 8  THEN '< 8h'
                     WHEN squeeze_duration_h < 12 THEN '8-12h'
                     WHEN squeeze_duration_h < 24 THEN '12-24h'
                     ELSE '> 24h' END AS bracket,
                COUNT(*) AS n,
                ROUND(AVG(CASE WHEN hit_15pct=1 THEN 100.0 ELSE 0 END), 1) AS hit_pct,
                ROUND(AVG(CASE WHEN hit_sl=1    THEN 100.0 ELSE 0 END), 1) AS sl_pct
            FROM nexus_signals WHERE checked=1
            GROUP BY 1 ORDER BY hit_pct DESC
        """)
        squeeze_stats = c.fetchall()

        # Per oi_change_6h_pct bracket
        c.execute("""
            SELECT
                CASE WHEN oi_change_6h_pct < 10 THEN '5-10%'
                     WHEN oi_change_6h_pct < 20 THEN '10-20%'
                     ELSE '> 20%' END AS bracket,
                COUNT(*) AS n,
                ROUND(AVG(CASE WHEN hit_15pct=1 THEN 100.0 ELSE 0 END), 1) AS hit_pct
            FROM nexus_signals WHERE checked=1
            GROUP BY 1 ORDER BY hit_pct DESC
        """)
        oi_stats = c.fetchall()

        # Per cross_volume_ratio bracket
        c.execute("""
            SELECT
                CASE WHEN cross_volume_ratio < 2.0 THEN '1.5-2x'
                     WHEN cross_volume_ratio < 3.0 THEN '2-3x'
                     ELSE '> 3x' END AS bracket,
                COUNT(*) AS n,
                ROUND(AVG(CASE WHEN hit_15pct=1 THEN 100.0 ELSE 0 END), 1) AS hit_pct
            FROM nexus_signals WHERE checked=1
            GROUP BY 1 ORDER BY hit_pct DESC
        """)
        vol_stats = c.fetchall()

        # Per dist_to_ema200_pct bracket
        c.execute("""
            SELECT
                CASE WHEN dist_to_ema200_pct < 15 THEN '8-15%'
                     WHEN dist_to_ema200_pct < 25 THEN '15-25%'
                     ELSE '25-30%' END AS bracket,
                COUNT(*) AS n,
                ROUND(AVG(CASE WHEN hit_15pct=1 THEN 100.0 ELSE 0 END), 1) AS hit_pct
            FROM nexus_signals WHERE checked=1
            GROUP BY 1 ORDER BY hit_pct DESC
        """)
        ema_stats = c.fetchall()

        # Per jam UTC (alerted_at)
        c.execute("""
            SELECT
                CASE WHEN CAST(strftime('%H', datetime(alerted_at, 'unixepoch')) AS INT) < 6
                          THEN '00:00-06:00 UTC'
                     WHEN CAST(strftime('%H', datetime(alerted_at, 'unixepoch')) AS INT) < 12
                          THEN '06:00-12:00 UTC'
                     WHEN CAST(strftime('%H', datetime(alerted_at, 'unixepoch')) AS INT) < 18
                          THEN '12:00-18:00 UTC'
                     ELSE '18:00-24:00 UTC' END AS bracket,
                COUNT(*) AS n,
                ROUND(AVG(CASE WHEN hit_15pct=1 THEN 100.0 ELSE 0 END), 1) AS hit_pct
            FROM nexus_signals WHERE checked=1
            GROUP BY 1 ORDER BY hit_pct DESC
        """)
        hour_stats = c.fetchall()

        conn.close()

        return {
            "checked":      n or 0,
            "pending":      pending or 0,
            "hit_15pct":    _safe_pct(h15 or 0, n or 0),
            "hit_10pct":    _safe_pct(h10 or 0, n or 0),
            "hit_sl_pct":   _safe_pct(hsl or 0, n or 0),
            "avg_max":      round(avg_max or 0, 1),
            "squeeze_stats": squeeze_stats,
            "oi_stats":     oi_stats,
            "vol_stats":    vol_stats,
            "ema_stats":    ema_stats,
            "hour_stats":   hour_stats,
        }

    except Exception as e:
        log.warning(f"  _collect_precision_stats error: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════
#  PRECISION REPORT PRINTER
# ══════════════════════════════════════════════════════════════════════
def print_precision_report(stats: dict) -> None:
    """Print laporan precision ke stdout."""
    if not stats:
        print("\n  [NEXUS-PB] Tidak ada data untuk dilaporkan.")
        return

    checked  = stats.get("checked", 0)
    pending  = stats.get("pending", 0)
    h15      = stats.get("hit_15pct", 0)
    h10      = stats.get("hit_10pct", 0)
    hsl      = stats.get("hit_sl_pct", 0)
    avg_max  = stats.get("avg_max", 0)

    print()
    print("═" * 50)
    print("  === NEXUS-PB PRECISION REPORT ===")
    print("═" * 50)
    print(f"  Checked: {checked} sinyal | Pending: {pending} sinyal")
    if checked > 0:
        print(f"  OVERALL: HIT@15%={h15:.1f}%  HIT@10%={h10:.1f}%  "
              f"SL={hsl:.1f}%  avg_max=+{avg_max:.1f}%")
        if checked < 50:
            print(f"  ⚠️  [{checked}/50 sinyal] — belum cukup untuk kalibrasi threshold")
    else:
        print("  OVERALL: belum ada sinyal closed")
    print()

    def _print_bracket_table(title: str, rows: list, extra_col: Optional[str] = None) -> None:
        if not rows:
            return
        print(f"  Per {title}:")
        for row in rows:
            bracket, n, hit_pct, *rest = row
            extra = f"  SL={rest[0]:.1f}%" if rest and extra_col else ""
            print(f"    {bracket:<14}: n={n}  HIT@15%={hit_pct:.1f}%{extra}")
        print()

    _print_bracket_table("squeeze_duration_h", stats.get("squeeze_stats", []), "SL")
    _print_bracket_table("oi_change_6h_pct",   stats.get("oi_stats", []))
    _print_bracket_table("cross_volume_ratio",  stats.get("vol_stats", []))
    _print_bracket_table("dist_to_ema200_pct",  stats.get("ema_stats", []))

    hour_rows = stats.get("hour_stats", [])
    if hour_rows:
        print("  Per jam UTC (alerted_at):")
        for bracket, n, hit_pct in hour_rows:
            print(f"    {bracket}: n={n}  HIT={hit_pct:.1f}%")
        print()

    print("═" * 50)
    print()


# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="NEXUS-PB Outcome Analyzer")
    parser.add_argument(
        "--hours", type=int, default=48,
        help="Lookback window untuk update (default: 48 jam)"
    )
    args = parser.parse_args()

    log.info("══════════════════════════════════════════════════════════")
    log.info(f"  NEXUS-PB Outcome Analyzer v{VERSION}")
    log.info(f"  Lookback: {args.hours} jam | DB: {DB_PATH}")
    log.info("══════════════════════════════════════════════════════════")

    # Cek apakah DB ada (jika belum ada, tidak ada yang perlu diupdate)
    try:
        conn_test = sqlite3.connect(DB_PATH)
        conn_test.execute("SELECT 1 FROM nexus_signals LIMIT 1")
        conn_test.close()
    except Exception:
        log.info("  DB belum ada atau tabel belum dibuat — lewati update")
        log.info("  (DB akan dibuat saat nexus_pb.py pertama kali dijalankan)")
        return  # exit 0

    stats = run_outcome_update(lookback_hours=args.hours)
    print_precision_report(stats)

    log.info("  Outcome analyzer selesai.")


if __name__ == "__main__":
    main()
