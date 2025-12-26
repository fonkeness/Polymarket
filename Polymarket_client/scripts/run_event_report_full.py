from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from app.ingestion.event_resolver import resolve_event
from app.ingestion.trades_loader import iter_event_trades, Trade
from app.reporting.excel_exporter import export_event_report_xlsx
from app.services.event_aggregator import aggregate_event

from app.storage.sqlite_event_store import (
    ensure_db,
    _connect,
    upsert_event,
    upsert_markets,
    insert_trades,
    count_trades,
    iter_trades_from_db,
)


def log(msg: str) -> None:
    print(msg, flush=True)


def utc_now_str() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch event trades into SQLite, aggregate from DB, export XLSX."
    )
    p.add_argument("event_url", help="https://polymarket.com/event/<slug>?tid=...")
    p.add_argument(
        "--out-dir",
        default=str(Path.cwd() / "out"),
        help="Output directory (default: ./out)",
    )

    # API page size (how many trades per API request)
    p.add_argument("--api-limit", type=int, default=500)

    # DB insert chunk size (how many trades we buffer before writing to DB)
    p.add_argument("--chunk-size", type=int, default=500)

    # Optional safety limits (useful for testing huge events)
    p.add_argument("--max-trades", type=int, default=0, help="Stop after N fetched trades (0 = no limit)")
    p.add_argument(
        "--max-shares",
        type=float,
        default=0.0,
        help="Stop after cumulative abs(size) >= X (0 = no limit)",
    )

    p.add_argument("--taker-only", action="store_true", help="Pass takerOnly=true to API")

    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    log("1) Resolve event...")
    ev = resolve_event(args.event_url)

    as_of = utc_now_str()
    log(f"Event: {ev.title} (id={ev.event_id}, slug={ev.slug})")
    log(f"As of (UTC): {as_of}")

    db_path = out_dir / f"event_{ev.slug}.sqlite"
    xlsx_path = out_dir / f"event_{ev.slug}.xlsx"
    xlsx_tmp_path = out_dir / f"event_{ev.slug}.xlsx.part"

    log("2) DB created/opened...")
    ensure_db(str(db_path))
    conn = _connect(str(db_path))

    try:
        upsert_event(conn, ev)
        upsert_markets(conn, ev)
        conn.commit()

        # =========================
        # Load trades -> DB
        # =========================
        log("3) Loading trades from API -> SQLite...")

        buf: List[Trade] = []
        fetched = 0
        inserted_total = 0
        ignored_total = 0
        cum_shares = 0.0

        trades_iter = iter_event_trades(
            ev.event_id,
            limit=int(args.api_limit),
            taker_only=bool(args.taker_only),
        )

        for tr in trades_iter:
            fetched += 1
            cum_shares += abs(float(tr.size or 0.0))
            buf.append(tr)

            if len(buf) >= int(args.chunk_size):
                stats = insert_trades(conn, event_id=ev.event_id, trades=buf)
                conn.commit()
                inserted_total += stats.inserted
                ignored_total += stats.ignored
                buf.clear()
                log(
                    f"   +{args.chunk_size} fetched. total_fetched={fetched} "
                    f"inserted={inserted_total} ignored={ignored_total} cum_shares≈{cum_shares:.4f}"
                )

            if args.max_trades and fetched >= int(args.max_trades):
                log(f"   Reached --max-trades={args.max_trades}, stopping fetch.")
                break

            if args.max_shares and cum_shares >= float(args.max_shares):
                log(f"   Reached --max-shares={args.max_shares}, stopping fetch.")
                break

        # flush leftovers
        if buf:
            stats = insert_trades(conn, event_id=ev.event_id, trades=buf)
            conn.commit()
            inserted_total += stats.inserted
            ignored_total += stats.ignored
            log(
                f"   +{len(buf)} fetched. total_fetched={fetched} "
                f"inserted={inserted_total} ignored={ignored_total} cum_shares≈{cum_shares:.4f}"
            )
            buf.clear()

        in_db = count_trades(conn, event_id=ev.event_id)
        log(f"DB filled. trades_in_db={in_db}")

        # =========================
        # Aggregate from DB
        # =========================
        log("4) Aggregating from DB...")
        report = aggregate_event(
            ev,
            iter_trades_from_db(conn, event_id=ev.event_id),
            as_of_utc=as_of,
        )

        # =========================
        # Export Excel
        # =========================
        log("5) Exporting XLSX...")
        if xlsx_tmp_path.exists():
            try:
                xlsx_tmp_path.unlink()
            except Exception:
                pass

        export_event_report_xlsx(event=ev, report=report, out_path=str(xlsx_tmp_path))

        # atomic-ish replace
        if xlsx_path.exists():
            try:
                xlsx_path.unlink()
            except Exception:
                pass
        xlsx_tmp_path.rename(xlsx_path)

        log(f"6) Done. XLSX: {xlsx_path}")
        log(f"   DB: {db_path}")
        return 0

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
