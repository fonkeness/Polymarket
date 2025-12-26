from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterator

import logging
import random
import time

import requests


DATA_API_BASE = "https://data-api.polymarket.com"

log = logging.getLogger(__name__)

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "polymarket-client/1.0"})


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


def _sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def _backoff_sleep(attempt: int, base: float = 0.8, cap: float = 20.0) -> None:
    # экспонента + небольшой jitter
    s = min(cap, base * (2**attempt))
    s *= random.uniform(0.85, 1.15)
    _sleep(s)


def _get_json_with_retries(
    url: str,
    *,
    params: dict[str, Any],
    timeout_s: float = 30.0,
    max_retries: int = 8,
) -> list[dict[str, Any]]:
    last_err: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            r = _SESSION.get(url, params=params, timeout=timeout_s)

            # 429 / лимиты
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait_s = float(ra) if ra and ra.isdigit() else None
                log.warning("429 rate limited. retry_after=%s attempt=%s", ra, attempt)
                if attempt >= max_retries:
                    r.raise_for_status()
                _sleep(wait_s if wait_s is not None else 3.0)
                _backoff_sleep(attempt)
                continue

            # 5xx — временные проблемы
            if 500 <= r.status_code < 600:
                log.warning("HTTP %s from data-api. attempt=%s", r.status_code, attempt)
                if attempt >= max_retries:
                    r.raise_for_status()
                _backoff_sleep(attempt)
                continue

            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected response type: {type(data)}")
            return data

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError) as e:
            last_err = e
            log.warning("Request failed: %r attempt=%s/%s params=%s", e, attempt, max_retries, params)
            if attempt >= max_retries:
                raise
            _backoff_sleep(attempt)

    # теоретически не дойдём
    raise RuntimeError(f"Failed to fetch json after retries: {last_err!r}")


def iter_event_trades(
    event_id: int,
    *,
    limit: int = 500,
    taker_only: bool = False,
    timeout_s: float = 30.0,
    max_retries: int = 8,
    min_request_interval_s: float = 0.15,  # ~6-7 req/sec (безопаснее чем “10 в сек”)
    progress_cb: Callable[[int, int], None] | None = None,  # (processed_trades, offset)
    progress_every: int = 2000,
    max_trades: int | None = None,  # если хочешь ограничить для теста
) -> Iterator[Trade]:
    offset = 0
    processed = 0

    prev_signature: tuple[Any, ...] | None = None
    last_request_ts = 0.0
    last_progress_at = 0

    while True:
        # rate limit
        now = time.time()
        elapsed = now - last_request_ts
        if elapsed < min_request_interval_s:
            _sleep(min_request_interval_s - elapsed)

        last_request_ts = time.time()

        batch = _get_json_with_retries(
            f"{DATA_API_BASE}/trades",
            params={
                "eventId": str(event_id),
                "limit": int(limit),
                "offset": int(offset),
                "takerOnly": str(taker_only).lower(),
            },
            timeout_s=timeout_s,
            max_retries=max_retries,
        )

        if not batch:
            break

        # защита от “вечной” страницы (API вернул тот же батч снова)
        sig0 = batch[0]
        signature = (
            sig0.get("timestamp"),
            sig0.get("transactionHash"),
            sig0.get("conditionId"),
            len(batch),
            offset,
        )
        if prev_signature == signature:
            raise RuntimeError(f"API returned same page twice, abort. signature={signature}")
        prev_signature = signature

        for t in batch:
            tr = Trade(
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
            yield tr

            processed += 1
            if max_trades is not None and processed >= max_trades:
                if progress_cb:
                    progress_cb(processed, offset)
                return

            if progress_cb and (processed - last_progress_at) >= progress_every:
                last_progress_at = processed
                progress_cb(processed, offset)

        offset += len(batch)

        if len(batch) < limit:
            break

    if progress_cb:
        progress_cb(processed, offset)
