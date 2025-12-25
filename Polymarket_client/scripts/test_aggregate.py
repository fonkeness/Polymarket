from datetime import datetime, timezone

from app.ingestion.event_resolver import resolve_event
from app.ingestion.trades_loader import iter_event_trades
from app.services.event_aggregator import aggregate_event


def main() -> None:
    EVENT_URL = "https://polymarket.com/event/fed-decision-in-january"
    ev = resolve_event(EVENT_URL)

    as_of = datetime.now(tz=timezone.utc).isoformat()
    report = aggregate_event(
        ev,
        iter_event_trades(ev.event_id, limit=1000, taker_only=False),
        as_of_utc=as_of,
    )

    print("EVENT:", report.event_slug, "| trades:", report.total_trades, "| traders:", report.unique_traders)
    # top markets by turnover
    top = sorted(report.markets.values(), key=lambda x: x.turnover_usd, reverse=True)[:10]
    for m in top:
        print("MARKET:", m.market_slug, "| trades:", m.trades_count, "| traders:", len(m.unique_traders), "| turnover:", round(m.turnover_usd, 2))


if __name__ == "__main__":
    main()
