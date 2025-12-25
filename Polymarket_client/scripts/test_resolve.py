from app.ingestion.event_resolver import resolve_event


def main() -> None:
    EVENT_URL = "https://polymarket.com/event/fed-decision-in-january?tid=123"

    ev = resolve_event(EVENT_URL)
    print("event_id:", ev.event_id)
    print("slug:", ev.slug)
    print("title:", ev.title)
    print("markets:", len(ev.markets))
    for m in ev.markets[:10]:
        print("-", m.market_id, m.slug, m.condition_id, m.question)


if __name__ == "__main__":
    main()
