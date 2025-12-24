from datetime import datetime, timezone

from app.rules_loader import load_rules
from db.trade_windows_repo import upsert_window


def floor_to_window_start(trade_ts: datetime, window_minutes: int) -> datetime:
    """
    Округляет trade_ts вниз до начала окна window_minutes.
    Пример: 12:07 при window=5 -> 12:05.
    trade_ts должен быть timezone-aware (UTC).
    """
    if trade_ts.tzinfo is None:
        raise ValueError("trade_ts must be timezone-aware (UTC)")

    # переводим в секунды, округляем по минутам
    window_seconds = window_minutes * 60
    epoch = int(trade_ts.timestamp())
    floored = epoch - (epoch % window_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


def update_window_and_check_alert(normalized_trade: dict) -> dict:
    """
    Обновляет окно для (wallet, condition_id) и возвращает dict:
    {
      "is_candidate": bool,
      "total_notional": float,
      "trade_count": int,
      "window_start_ts": datetime,
      "window_minutes": int,
      "min_total_notional": float
    }
    """
    rules = load_rules()
    agg = rules.get("aggregation", {}) or {}
    window_minutes = int(agg.get("window_minutes", 5))
    min_total = float(agg.get("min_total_notional", 10000))

    wallet = normalized_trade["wallet_address"]
    condition_id = normalized_trade.get("condition_id")
    trade_ts = normalized_trade["trade_ts"]
    notional = float(normalized_trade["notional"])

    if not condition_id:
        # без рынка окно не построить
        return {
            "is_candidate": False,
            "reason": "missing_condition_id",
        }

    window_start_ts = floor_to_window_start(trade_ts, window_minutes)

    total_after, count_after = upsert_window(
        wallet_address=wallet,
        condition_id=condition_id,
        window_start_ts=window_start_ts,
        window_minutes=window_minutes,
        add_notional=notional,
        trade_ts=trade_ts,
    )

    return {
        "is_candidate": total_after >= min_total,
        "total_notional": total_after,
        "trade_count": count_after,
        "window_start_ts": window_start_ts,
        "window_minutes": window_minutes,
        "min_total_notional": min_total,
    }
