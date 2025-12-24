from datetime import datetime, timezone, timedelta
from typing import Dict, Any


def should_alert(
    normalized_trade: dict,
    user_state: dict,
    window_info: dict,
    rules: dict,
) -> Dict[str, Any]:
    """
    Возвращает:
    {
      "should_alert": bool,
      "reason": str,
      "alert_type": "BUY_BIG_NEW" | "SELL_BIG_NEW" | ...
    }
    """
    logic = rules.get("alert_logic", {}) or {}

    new_user_max_trades = int(logic.get("new_user_max_trades", 20))
    dormant_days = int(logic.get("dormant_days", 30))
    min_window_notional = float(logic.get("min_window_notional", 10000))
    min_vs_median_mult = float(logic.get("min_vs_median_mult", 5))
    min_window_trades = int(logic.get("min_window_trades", 2))
    track_sells_separately = bool(logic.get("track_sells_separately", True))

    side = normalized_trade.get("side")
    win_total = float(window_info.get("total_notional", 0) or 0)
    win_trades = int(window_info.get("trade_count", 0) or 0)

    total_trades = int(user_state.get("total_trades", 0) or 0)
    median = user_state.get("median_notional")
    median = float(median) if median is not None else None

    last_trade_ts = user_state.get("last_trade_ts")
    if isinstance(last_trade_ts, str):
        # на всякий случай, если вдруг строкой
        return {"should_alert": False, "reason": "last_trade_ts_string_unhandled"}

    now_utc = datetime.now(timezone.utc)

    # 1) должно быть "крупно" по окну
    big_by_abs = win_total >= min_window_notional
    big_by_median = (median is not None and median > 0 and win_total >= median * min_vs_median_mult)

    if not (big_by_abs or big_by_median):
        return {"should_alert": False, "reason": "not_big_enough"}

    # 2) минимальное число трейдов в окне (чтобы не ловить 1 мелкую сделку)
    if win_trades < min_window_trades:
        return {"should_alert": False, "reason": "not_enough_trades_in_window"}

    # 3) новый или проснувшийся
    is_new = total_trades <= new_user_max_trades

    is_revived = False
    if last_trade_ts is not None:
        if last_trade_ts < (now_utc - timedelta(days=dormant_days)):
            is_revived = True

    if not (is_new or is_revived):
        return {"should_alert": False, "reason": "user_not_new_or_revived"}

    # 4) тип алерта
    if side == "SELL" and track_sells_separately:
        return {"should_alert": True, "reason": "sell_big_new_or_revived", "alert_type": "SELL_BIG_NEW"}
    else:
        return {"should_alert": True, "reason": "buy_big_new_or_revived", "alert_type": "BUY_BIG_NEW"}
