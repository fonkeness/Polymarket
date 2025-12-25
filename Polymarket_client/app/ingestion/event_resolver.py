from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests

GAMMA_API_BASE = "https://gamma-api.polymarket.com"


@dataclass(frozen=True)
class MarketMeta:
    market_id: str
    condition_id: str
    slug: str
    question: str


@dataclass(frozen=True)
class EventMeta:
    event_id: int
    slug: str
    title: str
    markets: list[MarketMeta]


def _extract_event_slug(url_or_slug: str) -> str:
    s = (url_or_slug or "").strip()
    if s.startswith("http://") or s.startswith("https://"):
        path = urlparse(s).path.strip("/")
        parts = [p for p in path.split("/") if p]
        if "event" in parts:
            i = parts.index("event")
            if i + 1 < len(parts) and parts[i + 1]:
                return parts[i + 1]
        # fallback: последний сегмент
        if parts:
            return parts[-1]
        raise ValueError(f"Не смог извлечь event slug из ссылки: {url_or_slug}")
    return s


def resolve_event(event_url_or_slug: str, timeout_s: int = 30) -> EventMeta:
    slug = _extract_event_slug(event_url_or_slug)

    r = requests.get(f"{GAMMA_API_BASE}/events/slug/{slug}", timeout=timeout_s)
    r.raise_for_status()
    data: dict[str, Any] = r.json()

    event_id = int(data["id"])
    title = str(data.get("title") or "")

    markets: list[MarketMeta] = []
    for m in (data.get("markets") or []):
        condition_id = str(m.get("conditionId") or "")
        market_slug = str(m.get("slug") or "")
        if not condition_id or not market_slug:
            continue
        markets.append(
            MarketMeta(
                market_id=str(m.get("id") or ""),
                condition_id=condition_id,
                slug=market_slug,
                question=str(m.get("question") or ""),
            )
        )

    return EventMeta(event_id=event_id, slug=slug, title=title, markets=markets)
