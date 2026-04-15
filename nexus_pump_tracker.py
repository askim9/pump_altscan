#!/usr/bin/env python3
"""
nexus_pump_tracker.py — NEXUS-PB Pump Event Recorder & Recall Tracker
Versi: 1.0.0 | NEXUS-PB Sprint 1

Jalankan SETELAH nexus_pb.py di setiap GitHub Actions run.

Alur:
  1. Fetch semua tickers Bitget
  2. Catat ke nexus_pump_events WHERE chg_24h >= 8%
     Deduplicate: skip jika symbol sudah ada dalam 1 jam terakhir
  3. Cross-reference dengan nexus_signals:
     JOIN WHERE nexus_signals.alerted_at >= (detected_at - 30h)
     Window 30 jam — lebih panjang dari v16 karena lead time NEXUS-PB lebih panjang
  4. Update scanner_alerted=1 dan signal_id jika ada match
  5. Print recall report

Catatan "catchable":
  Pump dari coin dengan vol_24h >= $2M yang dihitung sebagai missed.
  Pump dari vol < $2M adalah by-design tidak ditangkap.
"""

import logging
import sqlite3
import time
from typing import Dict, List, Optional, Tuple

import requests

# ══════════════════════════════════════════════════════════════════════
#  KONFIGURASI
# ══════════════════════════════════════════════════════════════════════
DB_PATH                = "nexus_pb_history.db"
PUMP_THRESHOLD_PCT     = 8.0     # chg_24h >= 8% = pump event
BIG_PUMP_THRESHOLD_PCT = 15.0    # pump >= 15% = "significant"
MIN_VOL_USD            = 2_000_000  # $2M — threshold catchable (sama dengan scanner)
DEDUP_WINDOW_SECS      = 3600    # skip jika sudah ada dalam 1 jam terakhir
SIGNAL_LOOKBACK_SECS   = 30 * 3600  # 30 jam — window match sinyal ke pump
BITGET_TIMEOUT         = 15
VERSION                = "1.0.0"

# ══════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("nexus_tracker")


# ══════════════════════════════════════════════════════════════════════
#  BITGET: FETCH TICKERS
# ══════════════════════════════════════════════════════════════════════
def fetch_bitget_tickers() -> List[dict]:
    """
    Ambil semua tickers Bitget USDT-Futures.
    Return: list of raw ticker dicts
    """
    try:
        resp = requests.get(
            "https://api.bitget.com/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"},
            headers={"User-Agent": f"NexusPB-Tracker/{VERSION}"},
            timeout=BITGET_TIMEOUT,
        )
        data = resp.json()
        tickers = []
        for t in data.get("data", []):
            sym = t.get("symbol", "")
            if sym.endswith("USDT"):
                tickers.append(t)
        log.info(f"  Bitget tickers: {len(tickers)} symbols")
        return tickers
    except Exception as e:
        log.warning(f"  Bitget fetch error: {e}")
        return []


def parse_ticker(t: dict) -> Optional[Tuple[str, float, float, float]]:
    """
    Parse satu ticker menjadi (symbol, price, chg_24h_pct, vol_24h_usd).
    Return None jika data tidak valid.
    """
    try:
        sym     = t.get("symbol", "")
        price   = float(t.get("lastPr", 0) or 0)
        # Bitget change24h bisa raw (0.084 = 8.4%) atau sudah persen — cek ukurannya
        raw_chg = float(t.get("change24h", 0) or 0)
        # Bitget API v2: change24h adalah desimal (0.084 = 8.4%)
        chg_24h = raw_chg * 100 if abs(raw_chg) <= 10 else raw_chg
        vol_24h = float(t.get("usdtVolume", 0) or 0)
        if price <= 0 or not sym:
            return None
        return sym, price, chg_24h, vol_24h
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════════════════
#  DB HELPERS
# ══════════════════════════════════════════════════════════════════════
def is_already_recorded(conn: sqlite3.Connection, symbol: str) -> bool:
    """
    Return True jika symbol sudah ada di nexus_pump_events dalam DEDUP_WINDOW_SECS.
    """
    since = int(time.time()) - DEDUP_WINDOW_SECS
    row = conn.execute(
        "SELECT 1 FROM nexus_pump_events WHERE symbol=? AND detected_at >= ? LIMIT 1",
        (symbol, since)
    ).fetchone()
    return row is not None


def find_prior_signal(conn: sqlite3.Connection, symbol: str, detected_at: int) -> Optional[int]:
    """
    Cari sinyal NEXUS-PB sebelumnya untuk coin ini dalam window 30 jam.
    Return signal_id jika ditemukan, None jika tidak.
    """
    window_start = detected_at - SIGNAL_LOOKBACK_SECS
    row = conn.execute("""
        SELECT id FROM nexus_signals
        WHERE symbol = ?
          AND alerted_at >= ?
          AND alerted_at <= ?
        ORDER BY alerted_at DESC
        LIMIT 1
    """, (symbol, window_start, detected_at)).fetchone()
    return row[0] if row else None


def record_pump_event(
    conn: sqlite3.Connection,
    symbol: str,
    chg_24h: float,
    vol_24h: float,
    detected_at: int,
    signal_id: Optional[int],
) -> int:
    """Insert satu pump event ke nexus_pump_events. Return inserted id."""
    scanner_alerted = 1 if signal_id is not None else 0
    cursor = conn.execute("""
        INSERT INTO nexus_pump_events (symbol, detected_at, chg_24h, vol_24h, scanner_alerted, signal_id)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (symbol, detected_at, round(chg_24h, 2), round(vol_24h, 0), scanner_alerted, signal_id))
    return cursor.lastrowid


# ══════════════════════════════════════════════════════════════════════
#  MAIN TRACKING LOGIC
# ══════════════════════════════════════════════════════════════════════
def run_pump_tracker() -> dict:
    """
    Jalankan satu siklus pump tracking.
    Return: stats untuk recall report.
    """
    tickers = fetch_bitget_tickers()
    if not tickers:
        log.warning("  Tidak ada ticker — pump tracker dilewati")
        return {}

    # Cek DB ada
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("SELECT 1 FROM nexus_pump_events LIMIT 1")
    except sqlite3.OperationalError:
        log.warning("  DB belum siap (tabel belum ada) — jalankan nexus_pb.py dulu")
        return {}

    now = int(time.time())

    # Kategori untuk recall report
    pump_today_all: List[dict]       = []  # semua pump >= PUMP_THRESHOLD_PCT hari ini
    pump_big: List[dict]             = []  # pump >= BIG_PUMP_THRESHOLD_PCT
    caught: List[dict]               = []  # pump yang ada sinyal sebelumnya
    missed_catchable: List[dict]     = []  # pump vol >= $2M yang tidak ada sinyal
    missed_uncatchable: List[dict]   = []  # pump vol < $2M (by-design)

    n_recorded = 0
    one_day_ago = now - 24 * 3600

    for t in tickers:
        parsed = parse_ticker(t)
        if parsed is None:
            continue
        sym, price, chg_24h, vol_24h = parsed

        if chg_24h < PUMP_THRESHOLD_PCT:
            continue

        # Deduplicate — skip jika sudah dicatat < 1 jam lalu
        if is_already_recorded(conn, sym):
            continue

        # Cari sinyal sebelumnya (dalam 30 jam)
        signal_id = find_prior_signal(conn, sym, now)

        # Catat ke DB
        try:
            ev_id = record_pump_event(conn, sym, chg_24h, vol_24h, now, signal_id)
            n_recorded += 1
        except Exception as e:
            log.warning(f"  record_pump_event error ({sym}): {e}")
            continue

        ev = {
            "id":        ev_id,
            "symbol":    sym,
            "chg_24h":   chg_24h,
            "vol_24h":   vol_24h,
            "signal_id": signal_id,
        }

        pump_today_all.append(ev)
        if chg_24h >= BIG_PUMP_THRESHOLD_PCT:
            pump_big.append(ev)

        if signal_id is not None:
            # Ambil alerted_at untuk hitung lead time
            sig_row = conn.execute(
                "SELECT alerted_at FROM nexus_signals WHERE id=?", (signal_id,)
            ).fetchone()
            lead_h = (now - sig_row[0]) / 3600 if sig_row else 0
            caught.append({**ev, "lead_h": lead_h})
        elif vol_24h >= MIN_VOL_USD:
            missed_catchable.append(ev)
        else:
            missed_uncatchable.append(ev)

    conn.commit()
    conn.close()
    log.info(f"  Pump events baru dicatat: {n_recorded}")

    # Hitung recall — hanya pump catchable (vol >= $2M) yang dihitung
    catchable_big = [p for p in (caught + missed_catchable) if p["chg_24h"] >= BIG_PUMP_THRESHOLD_PCT]
    caught_big    = [p for p in caught if p["chg_24h"] >= BIG_PUMP_THRESHOLD_PCT]

    return {
        "pump_today_all":       pump_today_all,
        "pump_big":             pump_big,
        "caught":               caught,
        "missed_catchable":     missed_catchable,
        "missed_uncatchable":   missed_uncatchable,
        "catchable_big":        catchable_big,
        "caught_big":           caught_big,
        "n_recorded":           n_recorded,
    }


# ══════════════════════════════════════════════════════════════════════
#  RECALL REPORT PRINTER
# ══════════════════════════════════════════════════════════════════════
def print_recall_report(stats: dict) -> None:
    """Print laporan recall ke stdout."""
    if not stats:
        print("\n  [NEXUS-PB Tracker] Tidak ada data untuk dilaporkan.")
        return

    pump_all      = stats.get("pump_today_all", [])
    pump_big      = stats.get("pump_big", [])
    caught        = stats.get("caught", [])
    missed_c      = stats.get("missed_catchable", [])
    missed_unc    = stats.get("missed_uncatchable", [])
    catchable_big = stats.get("catchable_big", [])
    caught_big    = stats.get("caught_big", [])

    print()
    print("═" * 50)
    print("  === NEXUS-PB PUMP TRACKER ===")
    print("═" * 50)
    print(f"  Pump events baru (chg >= {PUMP_THRESHOLD_PCT:.0f}%): {len(pump_all)}")
    print(f"  Pump >= {BIG_PUMP_THRESHOLD_PCT:.0f}%: {len(pump_big)}")
    print()

    # CAUGHT
    if caught:
        print("  CAUGHT (ada sinyal NEXUS-PB sebelumnya):")
        for ev in sorted(caught, key=lambda x: -x["chg_24h"]):
            lead  = ev.get("lead_h", 0)
            print(
                f"    ✅ {ev['symbol']:<14} "
                f"alert={lead:.1f}h lalu  "
                f"pump={ev['chg_24h']:+.1f}%  "
                f"vol=${ev['vol_24h']/1e6:.1f}M  "
                f"lead={lead:.1f}h"
            )
        print()

    # MISSED — catchable (vol >= $2M)
    if missed_c:
        print(f"  MISSED — catchable (vol >= ${MIN_VOL_USD/1e6:.0f}M, belum ada sinyal):")
        for ev in sorted(missed_c, key=lambda x: -x["chg_24h"]):
            print(
                f"    ⚠️  {ev['symbol']:<14} "
                f"pump={ev['chg_24h']:+.1f}%  "
                f"vol=${ev['vol_24h']/1e6:.1f}M"
            )
        print()

    # MISSED — uncatchable (vol < $2M, by-design)
    if missed_unc:
        print(f"  LOW-VOL (vol < ${MIN_VOL_USD/1e6:.0f}M — by-design tidak ditangkap):")
        for ev in sorted(missed_unc, key=lambda x: -x["chg_24h"])[:5]:  # max 5 baris
            print(
                f"    —  {ev['symbol']:<14} "
                f"pump={ev['chg_24h']:+.1f}%  "
                f"vol=${ev['vol_24h']/1e6:.2f}M  "
                f"(wajar missed)"
            )
        if len(missed_unc) > 5:
            print(f"    ... dan {len(missed_unc)-5} lainnya")
        print()

    # RECALL SUMMARY
    n_catchable = len(catchable_big)
    n_caught    = len(caught_big)
    recall_str  = f"{n_caught}/{n_catchable}" if n_catchable > 0 else "0/0"
    recall_pct  = (n_caught / n_catchable * 100) if n_catchable > 0 else 0
    print(
        f"  RECALL: {recall_str} pump >= {BIG_PUMP_THRESHOLD_PCT:.0f}% "
        f"yang catchable (vol >= ${MIN_VOL_USD/1e6:.0f}M)  "
        f"[{recall_pct:.0f}%]"
    )
    if n_catchable < 10:
        print(f"  ⚠️  [{n_catchable}/10+ events] — recall rate belum stabil, butuh lebih banyak data")

    print("═" * 50)
    print()


# ══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════
def main():
    log.info("══════════════════════════════════════════════════════════")
    log.info(f"  NEXUS-PB Pump Tracker v{VERSION}")
    log.info(f"  Threshold: chg >= {PUMP_THRESHOLD_PCT:.0f}% | Catchable vol >= ${MIN_VOL_USD/1e6:.0f}M")
    log.info(f"  Signal lookback window: {SIGNAL_LOOKBACK_SECS//3600} jam")
    log.info("══════════════════════════════════════════════════════════")

    stats = run_pump_tracker()
    print_recall_report(stats)

    log.info("  Pump tracker selesai.")
    # Return code 0 selalu (GitHub Actions: nexus_pump_tracker.py || true)


if __name__ == "__main__":
    main()
