from app.ingestion.event_resolver import resolve_event
from app.ingestion.trades_loader import iter_event_trades


def main() -> None:
    EVENT_URL = "https://polymarket.com/event/fed-decision-in-january"
    ev = resolve_event(EVENT_URL)

    print("event_id:", ev.event_id, "| markets:", len(ev.markets))

    n = 0
    for tr in iter_event_trades(ev.event_id, limit=500, taker_only=False):
        print(
            tr.timestamp,
            tr.condition_id,
            tr.side,
            tr.outcome,
            tr.size,
            tr.price,
            tr.proxy_wallet,
            tr.name or tr.pseudonym,
        )
        n += 1
        if n >= 20:
            break

    print("printed:", n)


if __name__ == "__main__":
    main()
