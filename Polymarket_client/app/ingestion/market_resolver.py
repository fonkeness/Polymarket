from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse

import requests


GAMMA_API_BASE = "https://gamma-api.polymarket.com"


@dataclass(frozen=True)
class MarketMeta:
    slug: str
    condition_id: str
    question: str | None
    outcomes: list[str] | None


def _extract_slug(market_url_or_slug: str) -> str:
    s = market_url_or_slug.strip()
    if s.startswith("http://") or s.startswith("https://"):
        path = urlparse(s).path.strip("/")
        # ожидаем что-то вроде: polymarket.com/market/<slug>
        parts = path.split("/")
        if "market" in parts:
            i = parts.index("market")
            if i + 1 < len(parts) and parts[i + 1]:
                return parts[i + 1]
        # fallback: последний сегмент
        if parts and parts[-1]:
            return parts[-1]
        raise ValueError(f"Не смог извлечь slug из ссылки: {market_url_or_slug}")
    return s


def resolve_market(market_url_or_slug: str, timeout_s: int = 20) -> MarketMeta:
    slug = _extract_slug(market_url_or_slug)

    # Gamma API: GET /markets?slug=<slug>
    # (в документации slug указан как string[]; на практике обычно работает slug=<value>)
    resp = requests.get(
        f"{GAMMA_API_BASE}/markets",
        params={"slug": slug, "limit": 1, "offset": 0},
        timeout=timeout_s,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data:
        raise ValueError(f"Gamma API не вернул рынок по slug={slug}")

    m: dict[str, Any] = data[0]
    condition_id = m.get("conditionId") or m.get("condition_id")
    if not condition_id:
        raise ValueError(f"У рынка нет conditionId (slug={slug})")

    # outcomes часто приходит как JSON-строка; аккуратно парсим
    outcomes_raw = m.get("outcomes")
    outcomes: Optional[list[str]] = None
    if isinstance(outcomes_raw, list):
        outcomes = [str(x) for x in outcomes_raw]
    elif isinstance(outcomes_raw, str) and outcomes_raw.strip():
        try:
            parsed = json.loads(outcomes_raw)
            if isinstance(parsed, list):
                outcomes = [str(x) for x in parsed]
        except Exception:
            outcomes = None

    return MarketMeta(
        slug=slug,
        condition_id=str(condition_id),
        question=m.get("question"),
        outcomes=outcomes,
    )
