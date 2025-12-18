from datetime import timedelta

from Polymarket_client.app.rules_loader import load_rules
from Polymarket_client.db.user_state_repo import get_user_state, upsert_user_state


def update_user_state(normalized_trade: dict) -> str:
    rules = load_rules().get("user_state", {})
    dormant_days = int(rules.get("dormant_days", 30))
    active_trades_threshold = int(rules.get("active_trades_threshold", 50))
    statuses = rules.get("statuses", {}) or {}

    STATUS_NEW = statuses.get("new", "new")
    STATUS_REVIVED = statuses.get("revived", "revived")
    STATUS_ACTIVE = statuses.get("active", "active")
    STATUS_IGNORED = statuses.get("ignored", "ignored")

    wallet = normalized_trade["wallet_address"]
    trade_ts = normalized_trade["trade_ts"]
    notional = normalized_trade["notional"]

    existing = get_user_state(wallet)

    # 1) Первый раз видим кошелек - создаем user_state
    if existing is None:
        upsert_user_state(
            wallet_address=wallet,
            first_trade_ts=trade_ts,
            last_trade_ts=trade_ts,
            total_trades=1,
            last_notional=notional,
            median_notional=notional,   #временно: медиана = первая сделка, позже пересчитаем из recent_trades
            status=STATUS_NEW,
        )
        return STATUS_NEW

    (
        _wallet,
        first_trade_ts,
        last_trade_ts,
        total_trades,
        last_notional,
        median_notional,
        status,
    ) = existing

    new_total_trades = int(total_trades) + 1

    # 2) dormant - если последняя сделка была раньше, чем trade_ts - dormant_days
    dormant_cutoff = trade_ts - timedelta(days=dormant_days)
    is_revived = last_trade_ts < dormant_cutoff

    # 3) Вычисляем новый статус
    if status == STATUS_IGNORED:
        new_status = STATUS_IGNORED  # ignored не переопределяем (пока так)
    elif is_revived:
        new_status = STATUS_REVIVED
    elif new_total_trades >= active_trades_threshold:
        new_status = STATUS_ACTIVE
    else:
        new_status = status

    # 4) Пока медиану не пересчитываем "честно" (сделаем через recent_user_trades)
    new_median = notional if median_notional is None else median_notional

    upsert_user_state(
        wallet_address=wallet,
        first_trade_ts=first_trade_ts,
        last_trade_ts=trade_ts,
        total_trades=new_total_trades,
        last_notional=notional,
        median_notional=new_median,
        status=new_status,
    )

    return new_status
