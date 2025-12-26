#!/usr/bin/env python3
import argparse
from typing import Any


def _get(obj: Any, *names: str, default=None):
    """Safe getter for dataclass/object/dict."""
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
        if isinstance(obj, dict) and n in obj:
            return obj[n]
    return default


def main():
    ap = argparse.ArgumentParser(description="List markets inside a Polymarket event URL")
    ap.add_argument("event_url", help="https://polymarket.com/event/...")
    args = ap.parse_args()

    from app.ingestion.event_resolver import resolve_event

    ev = resolve_event(args.event_url)

    ev_id = _get(ev, "id", "event_id")
    ev_slug = _get(ev, "slug")
    ev_title = _get(ev, "title", "name")

    print(f"EVENT: {ev_id}  slug={ev_slug}")
    print(f"TITLE: {ev_title}")
    print()

    markets = _get(ev, "markets", default=[]) or []
    if not markets:
        print("No markets found in this event.")
        return

    # Header
    print(f"{'#':>3}  {'market_id':>10}  {'conditionId':<66}  {'market_slug':<35}  question")
    print("-" * 140)

    for i, m in enumerate(markets, start=1):
        market_id = _get(m, "market_id", "id", default="")
        condition_id = _get(m, "condition_id", "conditionId", default="")
        slug = _get(m, "market_slug", "slug", default="")
        question = _get(m, "question", "title", default="")
        print(f"{i:>3}  {str(market_id):>10}  {str(condition_id):<66}  {str(slug):<35}  {question}")


if __name__ == "__main__":
    main()
