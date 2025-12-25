from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Iterable
import time
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


def _get_json_with_retries(
    *,
    url: str,
    params: dict[str, str],
    timeout_s: int,
    max_attempts: int = 6,
) -> Any:
    delay = 0.6
    for attempt in range(max_attempts):
        try:
            r = requests.get(url, params=params, timeout=timeout_s)

            # retryable statuses
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                if attempt == max_attempts - 1:
                    r.raise_for_status()
                time.sleep(delay)
                delay = min(delay * 2, 8.0)
                continue

            r.raise_for_status()
            return r.json()

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError):
            if attempt == max_attempts - 1:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 8.0)

    # unreachable
    return None


def _iter_trades_paged(
    *,
    base_params: dict[str, str],
    page_limit: int,
    timeout_s: int,
) -> Iterator[dict[str, Any]]:
    offset = 0
    while True:
        batch: list[dict[str, Any]] = _get_json_with_retries(
            url=f"{DATA_API_BASE}/trades",
            params={**base_params, "limit": str(page_limit), "offset": str(offset)},
            timeout_s=timeout_s,
        )
        if not batch:
            break

        for t in batch:
            yield t

        offset += len(batch)
        if len(batch) < page_limit:
            break


def iter_event_trades(
    event_id: int,
    *,
    limit: int = 1000,
    taker_only: bool = False,
    timeout_s: int = 30,
    market_condition_ids: Iterable[str] | None = None,
) -> Iterator[Trade]:
    # ВАЖНО: data-api на практике часто режет limit до 500
    page_limit = min(int(limit), 500)

    base = {"takerOnly": str(taker_only).lower()}

    # 1) Пытаемся грузить по eventId
    event_params = {**base, "eventId": str(event_id)}

    # быстрый "пинг" первой страницы
    first_batch: list[dict[str, Any]] = _get_json_with_retries(
        url=f"{DATA_API_BASE}/trades",
        params={**event_params, "limit": str(page_limit), "offset": "0"},
        timeout_s=timeout_s,
    )

    if first_batch:
        # есть трейды по eventId -> обычная пагинация по eventId
        # сначала отдаём первую страницу, затем остальные
        for t in first_batch:
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

        offset = len(first_batch)
        if len(first_batch) < page_limit:
            return

        # дальше страницы
        while True:
            batch: list[dict[str, Any]] = _get_json_with_retries(
                url=f"{DATA_API_BASE}/trades",
                params={**event_params, "limit": str(page_limit), "offset": str(offset)},
                timeout_s=timeout_s,
            )
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
            if len(batch) < page_limit:
                break

        return

    # 2) Если по eventId пусто -> fallback: по каждому market (conditionId)
    if not market_condition_ids:
        return

    for cid in market_condition_ids:
        cid = (cid or "").strip()
        if not cid:
            continue

        # лёгкий троттлинг, чтобы не убиваться об API на огромных ивентах
        time.sleep(0.05)

        market_params = {**base, "market": cid}
        for t in _iter_trades_paged(base_params=market_params, page_limit=page_limit, timeout_s=timeout_s):
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
