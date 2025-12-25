from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone

# чтобы работали импорты app/...
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../Polymarket_client -> .../Polymarket
POLY_DIR = PROJECT_ROOT / "Polymarket_client"
if str(POLY_DIR) not in sys.path:
    sys.path.insert(0, str(POLY_DIR))

from app.ingestion.event_resolver import resolve_event
from app.ingestion.trades_loader import iter_event_trades
from app.services.event_aggregator import aggregate_event
from app.reporting.excel_exporter import export_event_report_xlsx


def main():
    if len(sys.argv) < 2:
        print("Usage: python run_event_report_full.py <event_url_or_slug>")
        sys.exit(1)

    event_url = sys.argv[1].strip()
    ev = resolve_event(event_url)

    as_of = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    out_dir = PROJECT_ROOT / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"event_{ev.slug}_FULL.xlsx"

    # ВАЖНО: limit здесь = размер страницы, а не общий лимит.
    # iter_event_trades сам пагинирует до конца.
    trades_iter = iter_event_trades(ev.event_id, limit=1000, taker_only=False)

    report = aggregate_event(ev, trades_iter, as_of_utc=as_of)
    export_event_report_xlsx(event=ev, report=report, out_path=str(out_path))

    print(f"OK: {out_path}")


if __name__ == "__main__":
    main()
