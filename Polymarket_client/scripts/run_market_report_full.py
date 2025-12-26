#!/usr/bin/env python3
import argparse
import os
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Callable, Iterator, Optional

DATA_API_TRADES = "https://data-api.polymarket.com/trades"


def _get(obj: Any, *names: str, default=None):
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
        if isinstance(obj, dict) and n in obj:
            return obj[n]
    return default


def _pick_market(ev: Any, market_selector: str) -> Any:
    markets = _get(ev, "markets", default=[]) or []
    if not markets:
        raise RuntimeError("No markets in event")

    s = market_selector.strip()

    # 1) index (1-based)
    if s.isdigit():
        idx = int(s)
        if not (1 <= idx <= len(markets)):
            raise ValueError(f"market index out of range: {idx} (1..{len(markets)})")
        return markets[idx - 1]

    # 2) direct match by conditionId / market_id / slug
    for m in markets:
        if s == str(_get(m, "condition_id", "conditionId", default="")):
            return m
        if s == str(_get(m, "market_id", "id", default="")):
            return m
        if s == str(_get(m, "market_slug", "slug", default="")):
            return m

    raise ValueError("market not found by index/conditionId/market_id/slug")


def _import_exporter() -> Callable:
    # Try several likely locations so you don't need to edit imports manually
    candidates = [
        ("app.export.excel_export", "export_event_report_xlsx"),
        ("app.exporters.excel_export", "export_event_report_xlsx"),
        ("app.export.export_excel", "export_event_report_xlsx"),
        ("app.export.xlsx_export", "export_event_report_xlsx"),
        ("app.exporters.xlsx_export", "export_event_report_xlsx"),
    ]
    last_err = None
    for mod, fn in candidates:
        try:
            m = __import__(mod, fromlist=[fn])
            return getattr(m, fn)
        except Exception as e:
            last_err = e
            continue
    raise ImportError(
        "Can't import export_event_report_xlsx. "
        "Search it via: grep -R \"def export_event_report_xlsx\" -n /root/Polymarket/Polymarket_client/app"
    ) from last_err


def iter_market_trades(
    condition_id: str,
    *,
    batch_size: int = 500,
    max_trades: Optional[int] = None,
    taker_only: bool = True,
    sleep_s: float = 0.06,
    on_batch: Optional[Callable[[int, int], None]] = None,  # (downloaded_total, batch_len)
) -> Iterator[dict]:
    """
    Loads trades using data-api /trades?market=<conditionId>.
    NOTE: API docs show limit/offset max 10000 each, so extremely large markets may be partially accessible.
    """
    import requests

    offset = 0
    downloaded = 0

    # Protect from accidental infinite loops if API starts repeating pages
    last_first_tx = None
    repeat_guard = 0

    while True:
        params = {
            "market": condition_id,  # conditionId
            "limit": batch_size,
            "offset": offset,
        }
        if taker_only:
            params["takerOnly"] = "true"

        r = requests.get(DATA_API_TRADES, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json() or []
        if not isinstance(batch, list):
            raise RuntimeError(f"Unexpected response type: {type(batch)}")

        if not batch:
            break

        # Repeat-guard: if API starts giving identical first tx for same offset progression
        first_tx = batch[0].get("transactionHash")
        if first_tx == last_first_tx:
            repeat_guard += 1
        else:
            repeat_guard = 0
        last_first_tx = first_tx
        if repeat_guard >= 3:
            raise RuntimeError("Pagination appears stuck (same first transactionHash repeating).")

        # Yield trades
        for tr in batch:
            yield tr
            downloaded += 1
            if max_trades is not None and downloaded >= max_trades:
                if on_batch:
                    on_batch(downloaded, len(batch))
                return

        if on_batch:
            on_batch(downloaded, len(batch))

        # Next page
        offset += batch_size

        # rate limit: /trades 200 req / 10 sec → stay safely under it
        time.sleep(sleep_s)

        # stop condition (when last page)
        if len(batch) < batch_size:
            break


def main():
    ap = argparse.ArgumentParser(description="Generate Excel report for ONE market (conditionId) inside an event")
    ap.add_argument("event_url", help="https://polymarket.com/event/...")
    ap.add_argument(
        "--market",
        required=True,
        help="Market selector: 1-based index OR conditionId OR market_id OR market_slug (from list_event_markets.py output)",
    )
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--max-trades", type=int, default=None, help="Hard cap (e.g. 20000). If omitted, fetch until API ends.")
    ap.add_argument("--no-taker-only", action="store_true", help="Fetch all trades (not only takerOnly).")
    ap.add_argument("--out-dir", default="/root/Polymarket/out")
    args = ap.parse_args()

    from app.ingestion.event_resolver import resolve_event
    from app.services.event_aggregator import aggregate_event

    export_event_report_xlsx = _import_exporter()

    ev = resolve_event(args.event_url)
    m = _pick_market(ev, args.market)

    ev_id = _get(ev, "id", "event_id")
    ev_slug = _get(ev, "slug", default="event")
    ev_title = _get(ev, "title", "name", default="")

    condition_id = _get(m, "condition_id", "conditionId")
    market_slug = _get(m, "market_slug", "slug", default="market")
    question = _get(m, "question", "title", default="")

    print(f"EVENT: {ev_id} {ev_slug}  | {ev_title}")
    print(f"MARKET: {market_slug}")
    print(f"QUESTION: {question}")
    print(f"conditionId: {condition_id}")
    print()

    os.makedirs(args.out_dir, exist_ok=True)
    as_of = datetime.now(timezone.utc).replace(microsecond=0)

    # Build a “single-market event” wrapper so the existing aggregator/excel exporter keeps working
    ev_one = SimpleNamespace(
        id=ev_id,
        slug=ev_slug,
        title=ev_title,
        markets=[m],
        url=args.event_url,
    )

    def on_batch(downloaded_total: int, batch_len: int):
        print(f"DOWNLOADED: {downloaded_total} (last batch={batch_len})")

    trades_iter = iter_market_trades(
        condition_id,
        batch_size=args.batch_size,
        max_trades=args.max_trades,
        taker_only=not args.no_taker_only,
        on_batch=on_batch,
    )

    report = aggregate_event(ev_one, trades_iter, as_of_utc=as_of)

    # Write to tmp then atomic rename (so WinSCP won't grab half-written file)
    ts = as_of.strftime("%Y%m%d_%H%M%S")
    safe_slug = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(market_slug))[:80]
    out_path = os.path.join(args.out_dir, f"market_{ev_id}_{safe_slug}_{ts}.xlsx")
    tmp_path = out_path + ".tmp"

    print(f"Writing Excel to: {out_path}")
    export_event_report_xlsx(report, tmp_path)
    os.replace(tmp_path, out_path)
    print("DONE:", out_path)


if __name__ == "__main__":
    main()
