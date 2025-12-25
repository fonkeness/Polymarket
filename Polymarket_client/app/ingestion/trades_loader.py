from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator

import requests

DATA_API_BASE = "https://data-api.polymarket.com"


@dataclass(frozen=True)
class Trade:
    condition_id: str
    market_slug: str
    market_title: str

    proxy_wallet: str
    name: str
    pseudonym: str

    side: str
    outcome: str
    outcome_index: int | None

    size: float
    price: float
    timestamp: int
    tx_hash: str


def iter_event_trades(
    event_id: int,
    *,
    limit: int = 1000,
    taker_only: bool = False,
    timeout_s: int = 30,
) -> Iterator[Trade]:
    offset = 0
    while True:
        r = requests.get(
            f"{DATA_API_BASE}/trades",
            params={
                "eventId": str(event_id),
                "limit": limit,
                "offset": offset,
                "takerOnly": str(taker_only).lower(),
            },
            timeout=timeout_s,
        )
        r.raise_for_status()
        batch: list[dict[str, Any]] = r.json()
        if not batch:
            break

        for t in batch:
            yield Trade(
                condition_id=str(t.get("conditionId") or ""),
                market_slug=str(t.get("slug") or ""),
                market_title=str(t.get("title") or ""),
                proxy_wallet=str(t.get("proxyWallet") or ""),
                name=str(t.get("name") or ""),
                pseudonym=str(t.get("pseudonym") or ""),
                side=str(t.get("side") or ""),
                outcome=str(t.get("outcome") or ""),
                outcome_index=t.get("outcomeIndex"),
                size=float(t.get("size") or 0.0),
                price=float(t.get("price") or 0.0),
                timestamp=int(t.get("timestamp") or 0),
                tx_hash=str(t.get("transactionHash") or ""),
            )

        offset += len(batch)
        if len(batch) < limit:
            break
