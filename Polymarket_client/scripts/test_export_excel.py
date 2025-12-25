from datetime import datetime, timezone

from app.ingestion.event_resolver import resolve_event
from app.ingestion.trades_loader import iter_event_trades
from app.services.event_aggregator import aggregate_event
from app.reporting.excel_exporter import export_event_report_xlsx


def main() -> None:
    EVENT_URL = "https://polymarket.com/event/kodiak-fdv-above-one-day-after-launch/kodiak-fdv-above-50m-one-day-after-launch?tid=1766649532829"
    ev = resolve_event(EVENT_URL)

    # без миллисекунд и без +00:00
    as_of = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    report = aggregate_event(
        ev,
        iter_event_trades(ev.event_id, limit=1000, taker_only=False),
        as_of_utc=as_of,
    )

    path = export_event_report_xlsx(
        event=ev,
        report=report,
        out_path=f"out/event_{ev.slug}.xlsx",
    )
    print("saved:", path)


if __name__ == "__main__":
    main()
