import psycopg2
from pathlib import Path
import yaml
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config" / "settings.yaml"


def _load_db_settings():
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["database"]


def get_connection():
    db = _load_db_settings()
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["name"],
        user=db["user"],
        password=db["password"],
    )


def get_user_state(wallet_address: str):
    sql = """
    SELECT wallet_address,
           first_trade_ts,
           last_trade_ts,
           total_trades,
           last_notional,
           median_notional,
           status
    FROM user_state
    WHERE wallet_address = %s
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (wallet_address,))
            row = cur.fetchone()
            return row
    finally:
        conn.close()


def upsert_user_state(
    wallet_address: str,
    first_trade_ts: datetime,
    last_trade_ts: datetime,
    total_trades: int,
    last_notional,
    median_notional,
    status: str,
):
    sql = """
    INSERT INTO user_state (
        wallet_address,
        first_trade_ts,
        last_trade_ts,
        total_trades,
        last_notional,
        median_notional,
        status,
        updated_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s, now())
    ON CONFLICT (wallet_address) DO UPDATE SET
        last_trade_ts = EXCLUDED.last_trade_ts,
        total_trades = EXCLUDED.total_trades,
        last_notional = EXCLUDED.last_notional,
        median_notional = EXCLUDED.median_notional,
        status = EXCLUDED.status,
        updated_at = now();
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    wallet_address,
                    first_trade_ts,
                    last_trade_ts,
                    total_trades,
                    last_notional,
                    median_notional,
                    status,
                ),
            )
            conn.commit()
    finally:
        conn.close()
