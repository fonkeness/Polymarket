from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Tuple

from app.ingestion.event_resolver import EventMeta, MarketMeta
from app.ingestion.trades_loader import Trade


@dataclass
class MarketTotals:
    condition_id: str
    market_slug: str
    question: str

    trades_count: int = 0
    buy_usd: float = 0.0
    sell_usd: float = 0.0
    turnover_usd: float = 0.0
    unique_traders: set[str] = None

    def __post_init__(self):
        if self.unique_traders is None:
            self.unique_traders = set()


@dataclass
class ParticipantTotals:
    condition_id: str
    trader_address: str
    outcome: str

    trader_name: str = ""
    trader_pseudonym: str = ""

    buy_shares: float = 0.0
    buy_usd: float = 0.0
    sell_shares: float = 0.0
    sell_usd: float = 0.0

    trades_count: int = 0
    first_ts: int | None = None
    last_ts: int | None = None

    @property
    def net_shares(self) -> float:
        return self.buy_shares - self.sell_shares

    @property
    def net_spent_usd(self) -> float:
        return self.buy_usd - self.sell_usd

    @property
    def avg_buy_price(self) -> float | None:
        return (self.buy_usd / self.buy_shares) if self.buy_shares else None

    @property
    def avg_sell_price(self) -> float | None:
        return (self.sell_usd / self.sell_shares) if self.sell_shares else None


@dataclass
class EventReportData:
    event_id: int
    event_slug: str
    event_title: str
    as_of_utc: str

    total_trades: int
    unique_traders: int
    total_turnover_usd: float

    markets: Dict[str, MarketTotals]  # conditionId -> totals
    participants: Dict[Tuple[str, str, str], ParticipantTotals]  # (conditionId, wallet, outcome) -> totals


def aggregate_event(event: EventMeta, trades: Iterable[Trade], as_of_utc: str) -> EventReportData:
    market_by_cid: dict[str, MarketMeta] = {m.condition_id: m for m in event.markets}

    markets: dict[str, MarketTotals] = {}
    participants: dict[tuple[str, str, str], ParticipantTotals] = {}

    all_traders: set[str] = set()
    total_trades = 0
    total_turnover = 0.0

    for tr in trades:
        cid = tr.condition_id
        wallet = tr.proxy_wallet
        if not cid or not wallet:
            continue

        mm = market_by_cid.get(cid)
        if cid not in markets:
            markets[cid] = MarketTotals(
                condition_id=cid,
                market_slug=(mm.slug if mm else tr.market_slug),
                question=(mm.question if mm else tr.market_title),
            )
        mt = markets[cid]

        usd = float(tr.size) * float(tr.price)
        side = (tr.side or "").upper()

        mt.trades_count += 1
        mt.turnover_usd += usd
        if side == "BUY":
            mt.buy_usd += usd
        elif side == "SELL":
            mt.sell_usd += usd

        mt.unique_traders.add(wallet)
        all_traders.add(wallet)

        # per-participant per-outcome
        outcome = tr.outcome or ""
        key = (cid, wallet, outcome)
        if key not in participants:
            participants[key] = ParticipantTotals(
                condition_id=cid,
                trader_address=wallet,
                outcome=outcome,
                trader_name=tr.name or "",
                trader_pseudonym=tr.pseudonym or "",
            )
        pt = participants[key]

        # заполняем имя/ник если раньше пусто
        if not pt.trader_name and tr.name:
            pt.trader_name = tr.name
        if not pt.trader_pseudonym and tr.pseudonym:
            pt.trader_pseudonym = tr.pseudonym

        if side == "BUY":
            pt.buy_shares += float(tr.size)
            pt.buy_usd += usd
        elif side == "SELL":
            pt.sell_shares += float(tr.size)
            pt.sell_usd += usd

        pt.trades_count += 1
        ts = int(tr.timestamp or 0)
        if ts:
            if pt.first_ts is None or ts < pt.first_ts:
                pt.first_ts = ts
            if pt.last_ts is None or ts > pt.last_ts:
                pt.last_ts = ts

        total_trades += 1
        total_turnover += usd

    return EventReportData(
        event_id=event.event_id,
        event_slug=event.slug,
        event_title=event.title,
        as_of_utc=as_of_utc,
        total_trades=total_trades,
        unique_traders=len(all_traders),
        total_turnover_usd=total_turnover,
        markets=markets,
        participants=participants,
    )
