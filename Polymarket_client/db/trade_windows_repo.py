import psycopg2
from typing import Dict, Any, Optional, Tuple
from pathlib import Path
import yaml
from datetime import datetime


def load_settings() -> Dict[str, Any]:
    base_dir = Path(__file__).resolve().parents[1]
    settings_path = base_dir / "config" / "settings.yaml"
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


def upsert_window(
    wallet_address: str,
    condition_id: str,
    window_start_ts: datetime,
    window_minutes: int,
    add_notional: float,
    trade_ts: datetime,
) -> Tuple[float, int]:
    """
    Увеличивает окно на add_notional и +1 trade_count.
    Возвращает (total_notional_after, trade_count_after).
    """
    sql = """
    INSERT INTO trade_windows (
        wallet_address, condition_id, window_start_ts, window_minutes,
        total_notional, trade_count, first_trade_ts, last_trade_ts, updated_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now())
    ON CONFLICT (wallet_address, condition_id, window_start_ts, window_minutes)
    DO UPDATE SET
        total_notional = trade_windows.total_notional + EXCLUDED.total_notional,
        trade_count = trade_windows.trade_count + 1,
        first_trade_ts = COALESCE(trade_windows.first_trade_ts, EXCLUDED.first_trade_ts),
        last_trade_ts = GREATEST(trade_windows.last_trade_ts, EXCLUDED.last_trade_ts),
        updated_at = now()
    RETURNING total_notional, trade_count;
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        wallet_address,
                        condition_id,
                        window_start_ts,
                        window_minutes,
                        add_notional,
                        1,
                        trade_ts,
                        trade_ts,
                    ),
                )
                total_notional, trade_count = cur.fetchone()
                return float(total_notional), int(trade_count)
    finally:
        conn.close()
