from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from app.ingestion.market_resolver import resolve_market
from app.ingestion.data_api_trades import fetch_market_trades


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, help="Ссылка на рынок polymarket.com/market/<slug> или просто slug")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--max", type=int, default=0, help="Ограничить кол-во трейдов (0 = без лимита)")
    ap.add_argument("--taker-only", action="store_true", help="Оставить только taker trades (по умолчанию false)")
    ap.add_argument("--out", default="out", help="Папка для сохранения raw json")
    args = ap.parse_args()

    meta = resolve_market(args.market)
    trades = fetch_market_trades(
        meta.condition_id,
        limit=args.limit,
        max_trades=(args.max if args.max > 0 else None),
        taker_only=args.taker_only,
    )

    unique_wallets = len({t.proxy_wallet for t in trades if t.proxy_wallet})
    ts_min = min((t.timestamp for t in trades if t.timestamp), default=None)
    ts_max = max((t.timestamp for t in trades if t.timestamp), default=None)

    def fmt_ts(ts: int | None) -> str:
        if not ts:
            return "-"
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    print("MARKET")
    print(f"  slug: {meta.slug}")
    print(f"  conditionId: {meta.condition_id}")
    print(f"  question: {meta.question}")
    print(f"  outcomes: {meta.outcomes}")
    print("TRADES")
    print(f"  count: {len(trades)}")
    print(f"  unique wallets: {unique_wallets}")
    print(f"  range: {fmt_ts(ts_min)} .. {fmt_ts(ts_max)}")

    payload = {
        "market": {
            "slug": meta.slug,
            "conditionId": meta.condition_id,
            "question": meta.question,
            "outcomes": meta.outcomes,
            "as_of": datetime.now(tz=timezone.utc).isoformat(),
        },
        "trades": [t.__dict__ for t in trades],
    }

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = out_dir / f"market_{meta.slug}_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    fname.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"SAVED: {fname}")


if __name__ == "__main__":
    main()
