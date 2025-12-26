from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional

from app.ingestion.event_resolver import EventMeta, MarketMeta
from app.ingestion.trades_loader import Trade


@dataclass(frozen=True)
class DbStats:
    inserted: int
    ignored: int


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY,
  slug TEXT NOT NULL,
  title TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS markets (
  event_id INTEGER NOT NULL,
  condition_id TEXT NOT NULL,
  market_id TEXT,
  slug TEXT,
  question TEXT,
  PRIMARY KEY (event_id, condition_id)
);

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,

  condition_id TEXT NOT NULL,
  market_slug TEXT,
  market_title TEXT,

  proxy_wallet TEXT NOT NULL,
  name TEXT,
  pseudonym TEXT,

  side TEXT,
  outcome TEXT,
  outcome_index INTEGER,

  size REAL,
  price REAL,
  timestamp INTEGER,
  tx_hash TEXT,

  inserted_at TEXT DEFAULT CURRENT_TIMESTAMP,

  UNIQUE (
    event_id, condition_id, proxy_wallet,
    side, outcome, size, price, timestamp, tx_hash
  )
);

CREATE INDEX IF NOT EXISTS idx_trades_event_id ON trades(event_id);
CREATE INDEX IF NOT EXISTS idx_trades_event_condition ON trades(event_id, condition_id);
CREATE INDEX IF NOT EXISTS idx_trades_event_wallet ON trades(event_id, proxy_wallet);
"""


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


def ensure_db(db_path: str) -> str:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(str(p))
    conn.close()
    return str(p)


def upsert_event(conn: sqlite3.Connection, event: EventMeta) -> None:
    conn.execute(
        """
        INSERT INTO events(event_id, slug, title)
        VALUES(?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
          slug=excluded.slug,
          title=excluded.title
        """,
        (int(event.event_id), event.slug, event.title),
    )


def upsert_markets(conn: sqlite3.Connection, event: EventMeta) -> None:
    rows = []
    for m in event.markets:
        rows.append(
            (
                int(event.event_id),
                m.condition_id,
                m.market_id,
                m.slug,
                m.question,
            )
        )
    conn.executemany(
        """
        INSERT INTO markets(event_id, condition_id, market_id, slug, question)
        VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(event_id, condition_id) DO UPDATE SET
          market_id=excluded.market_id,
          slug=excluded.slug,
          question=excluded.question
        """,
        rows,
    )


def insert_trades(
    conn: sqlite3.Connection,
    *,
    event_id: int,
    trades: Iterable[Trade],
) -> DbStats:
    sql = """
    INSERT OR IGNORE INTO trades(
      event_id,
      condition_id, market_slug, market_title,
      proxy_wallet, name, pseudonym,
      side, outcome, outcome_index,
      size, price, timestamp, tx_hash
    ) VALUES (
      ?, ?, ?, ?,
      ?, ?, ?,
      ?, ?, ?,
      ?, ?, ?, ?
    )
    """

    data = []
    for t in trades:
        data.append(
            (
                int(event_id),
                t.condition_id,
                t.market_slug,
                t.market_title,
                t.proxy_wallet,
                t.name,
                t.pseudonym,
                t.side,
                t.outcome,
                int(t.outcome_index) if t.outcome_index is not None else None,
                float(t.size),
                float(t.price),
                int(t.timestamp),
                t.tx_hash,
            )
        )

    before = conn.total_changes
    conn.executemany(sql, data)
    after = conn.total_changes

    inserted = after - before
    ignored = max(0, len(data) - inserted)
    return DbStats(inserted=inserted, ignored=ignored)


def count_trades(conn: sqlite3.Connection, *, event_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(1) AS c FROM trades WHERE event_id=?",
        (int(event_id),),
    ).fetchone()
    return int(row["c"])


def iter_trades_from_db(
    conn: sqlite3.Connection,
    *,
    event_id: int,
    condition_id: Optional[str] = None,
) -> Iterator[Trade]:
    if condition_id:
        cur = conn.execute(
            """
            SELECT
              condition_id, market_slug, market_title,
              proxy_wallet, name, pseudonym,
              side, outcome, outcome_index,
              size, price, timestamp, tx_hash
            FROM trades
            WHERE event_id=? AND condition_id=?
            ORDER BY id
            """,
            (int(event_id), condition_id),
        )
    else:
        cur = conn.execute(
            """
            SELECT
              condition_id, market_slug, market_title,
              proxy_wallet, name, pseudonym,
              side, outcome, outcome_index,
              size, price, timestamp, tx_hash
            FROM trades
            WHERE event_id=?
            ORDER BY id
            """,
            (int(event_id),),
        )

    for r in cur:
        yield Trade(
            condition_id=str(r["condition_id"] or ""),
            market_slug=str(r["market_slug"] or ""),
            market_title=str(r["market_title"] or ""),
            proxy_wallet=str(r["proxy_wallet"] or ""),
            name=str(r["name"] or ""),
            pseudonym=str(r["pseudonym"] or ""),
            side=str(r["side"] or ""),
            outcome=str(r["outcome"] or ""),
            outcome_index=r["outcome_index"],
            size=float(r["size"] or 0.0),
            price=float(r["price"] or 0.0),
            timestamp=int(r["timestamp"] or 0),
            tx_hash=str(r["tx_hash"] or ""),
        )
