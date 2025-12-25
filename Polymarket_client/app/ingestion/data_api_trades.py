from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import requests


DATA_API_BASE = "https://data-api.polymarket.com"


@dataclass(frozen=True)
class RawTrade:
    proxy_wallet: str
    side: str
    size: float
    price: float
    timestamp: int
    outcome: str | None
    outcome_index: int | None
    transaction_hash: str | None
    name: str | None
    pseudonym: str | None


def fetch_market_trades(
    condition_id: str,
    *,
    limit: int = 500,
    max_trades: int | None = None,
    taker_only: bool = False,
    timeout_s: int = 30,
) -> list[RawTrade]:
    """
    Data-API trades: GET /trades?market=<conditionId>&limit=&offset=&takerOnly=
    """
    out: list[RawTrade] = []
    offset = 0

    while True:
        resp = requests.get(
            f"{DATA_API_BASE}/trades",
            params={
                "market": condition_id,
                "limit": limit,
                "offset": offset,
                "takerOnly": str(taker_only).lower(),
            },
            timeout=timeout_s,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break

        for t in batch:
            out.append(
                RawTrade(
                    proxy_wallet=str(t.get("proxyWallet") or ""),
                    side=str(t.get("side") or ""),
                    size=float(t.get("size") or 0),
                    price=float(t.get("price") or 0),
                    timestamp=int(t.get("timestamp") or 0),
                    outcome=t.get("outcome"),
                    outcome_index=t.get("outcomeIndex"),
                    transaction_hash=t.get("transactionHash"),
                    name=t.get("name"),
                    pseudonym=t.get("pseudonym"),
                )
            )

        offset += len(batch)
        if max_trades is not None and len(out) >= max_trades:
            out = out[:max_trades]
            break

        # если пришло меньше limit — скорее всего конец
        if len(batch) < limit:
            break

    return out
