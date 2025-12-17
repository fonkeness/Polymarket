import psycopg2
from psycopg2.extras import execute_values
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


def save_raw_trade(trade: Dict[str, Any]) -> None:
    sql = """
        INSERT INTO raw_trades (
            trade_id,
            wallet_address,
            token_id,
            side,
            price,
            size,
            notional,
            trade_ts,
            source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_id) DO NOTHING;
    """

    values = (
        trade["trade_id"],
        trade.get("wallet_address", "TEST_WALLET"),
        trade["token_id"],
        trade["side"],
        trade["price"],
        trade["size"],
        trade["notional"],
        trade["trade_ts"],
        "test",
    )

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)
    finally:
        conn.close()



if __name__ == "__main__":
    from datetime import datetime, timezone

    test_trade = {
        "trade_id": "test_trade_1",
        "wallet_address": "TEST_WALLET",
        "token_id": "TEST_TOKEN_ID",
        "side": "BUY",
        "price": 0.62,
        "size": 10000.0,
        "notional": 6200.0,
        "trade_ts": datetime(2024, 3, 9, 16, 0, tzinfo=timezone.utc),
    }

    save_raw_trade(test_trade)
    print("Saved raw trade")
