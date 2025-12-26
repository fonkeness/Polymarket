from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Optional, Callable
import random
import time

import requests

DATA_API_BASE = "https://data-api.polymarket.com"
MAX_PAGE_LIMIT = 500  # у data-api фактический потолок


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


def _sleep_s(seconds: float) -> None:
    time.sleep(seconds)


def _get_json_with_retries(
    *,
    url: str,
    params: dict[str, Any],
    timeout_s: int,
    max_attempts: int = 8,
) -> list[dict[str, Any]]:
    last_err: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout_s)
            # rate limit / временные ошибки
            if r.status_code == 429 or 500 <= r.status_code < 600:
                retry_after = r.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    delay = int(retry_after)
                else:
                    # экспонента + небольшая случайность
                    delay = min(60, int(2 ** (attempt - 1))) + random.random()
                _sleep_s(delay)
                continue

            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []

        except Exception as e:
            last_err = e
            delay = min(30, int(2 ** (attempt - 1))) + random.random()
            _sleep_s(delay)

    # если сюда дошли — значит всё реально плохо
    raise RuntimeError(f"Failed to fetch trades after {max_attempts} attempts: {last_err}")


def iter_event_trades(
    event_id: int,
    *,
    limit: int = 500,
    taker_only: bool = False,
    timeout_s: int = 30,
    max_trades: int | None = None,
    on_batch: Optional[Callable[[int], None]] = None,  # вызывается после каждой пачки (передаём total_yielded)
) -> Iterator[Trade]:
    # ключевой фикс: clamp limit до 500
    limit = max(1, min(int(limit), MAX_PAGE_LIMIT))

    offset = 0
    yielded = 0

    # важный фикс: страницы перекрываются, + рынок живой -> dedupe по tx
    seen_tx: set[str] = set()
    no_new_batches = 0

    while True:
        batch: list[dict[str, Any]] = _get_json_with_retries(
            url=f"{DATA_API_BASE}/trades",
            params={
                "eventId": str(event_id),
                "limit": str(limit),
                "offset": str(offset),
                "takerOnly": str(taker_only).lower(),
            },
            timeout_s=timeout_s,
        )

        if not batch:
            break

        new_in_batch = 0
        for t in batch:
            tx = str(t.get("transactionHash") or "")
            if tx:
                if tx in seen_tx:
                    continue
                seen_tx.add(tx)

            new_in_batch += 1
            yielded += 1

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
                tx_hash=tx,
            )

            if max_trades is not None and yielded >= max_trades:
                if on_batch:
                    on_batch(yielded)
                return

        if on_batch:
            on_batch(yielded)

        # если пачка пришла, но ничего нового не добавили — защита от бесконечного цикла
        if new_in_batch == 0:
            no_new_batches += 1
            if no_new_batches >= 3:
                break
        else:
            no_new_batches = 0

        offset += len(batch)

        # теперь это корректно, потому что limit зажат до 500
        if len(batch) < limit:
            break
