import psycopg2
from typing import Dict, Any
from pathlib import Path
import yaml


def load_settings() -> Dict[str, Any]:
    base_dir = Path(__file__).resolve().parents[1]
    settings_path = base_dir / "config" / "settings.yaml"

    if not settings_path.exists():
        raise FileNotFoundError(f"settings.yaml not found at {settings_path}")

    with settings_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_connection():
    settings = load_settings()
    db = settings["database"]

    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"],
    )


def save_raw_trade(trade: Dict[str, Any]) -> bool:
    """
    Пытается вставить raw trade в БД.
    Возвращает:
      True  — если вставили (это новый трейд для нашей БД)
      False — если уже был (trade_id конфликтнул) и вставки не произошло
    """
    sql = """
        INSERT INTO raw_trades (
            trade_id,
            wallet_address,
            token_id,
            condition_id,
            side,
            price,
            size,
            notional,
            trade_ts,
            source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_id) DO NOTHING
        RETURNING trade_id;
    """

    values = (
        trade["trade_id"],
        trade["wallet_address"],
        trade["token_id"],
        trade.get("condition_id"),
        trade["side"],
        trade["price"],
        trade["size"],
        trade["notional"],
        trade["trade_ts"],
        trade.get("source", "unknown"),
    )

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)
                row = cur.fetchone()
                return row is not None
    finally:
        conn.close()


# ---- ручной тест ----
if __name__ == "__main__":
    from datetime import datetime, timezone

    test_trade = {
        "trade_id": "test_trade_1",
        "wallet_address": "TEST_WALLET",
        "token_id": "TEST_TOKEN_ID",
        "condition_id": "TEST_CONDITION_ID",
        "side": "BUY",
        "price": 0.62,
        "size": 10000.0,
        "notional": 6200.0,
        "trade_ts": datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
        "source": "test",
    }

    inserted = save_raw_trade(test_trade)
    print("Inserted:", inserted)
