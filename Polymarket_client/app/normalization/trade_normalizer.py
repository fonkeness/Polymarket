from datetime import datetime, timezone
from typing import Dict, Any


def normalize_trade(raw_trade: Dict[str, Any]) -> Dict[str, Any]:
    """
    Приводит сырой trade к единому формату системы.
    Ожидает минимально необходимые поля в raw_trade.
    """

    # Обязательные поля (если чего-то нет — пусть падает сразу)
    trade_id = raw_trade["id"]
    token_id = raw_trade["token_id"]
    side = raw_trade["side"].upper()  # BUY / SELL

    price = float(raw_trade["price"])
    size = float(raw_trade["size"])

    # timestamp может прийти как int (unix) или строка
    ts_raw = raw_trade["timestamp"]
    if isinstance(ts_raw, (int, float)):
        trade_ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
    else:
        trade_ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)

    notional = price * size

    return {
        "trade_id": trade_id,
        "token_id": token_id,
        "side": side,
        "price": price,
        "size": size,
        "notional": notional,
        "trade_ts": trade_ts,
    }

