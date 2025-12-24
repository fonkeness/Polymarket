import json
import subprocess
from datetime import datetime, timezone

from db.raw_trades_repo import save_raw_trade
from app.state.user_state_updater import update_user_state
from app.bet_aggregation.window_aggregator import update_window_and_check_alert
from db.trade_windows_repo import mark_window_alerted

from app.rules_loader import load_rules
from db.user_state_repo import get_user_state
from app.alert_engine.alert_decider import should_alert


TRADES_URL = "https://data-api.polymarket.com/trades?limit=25&offset=0&takerOnly=true"


def fetch_trades() -> list[dict]:
    out = subprocess.check_output(["curl", "-s", TRADES_URL], text=True)
    return json.loads(out)


def normalize(t: dict) -> dict:
    wallet = t.get("proxyWallet")
    condition_id = t.get("conditionId")
    token_id = t.get("asset")
    side = t.get("side")

    price = float(t.get("price")) if t.get("price") is not None else None
    size = float(t.get("size")) if t.get("size") is not None else None

    ts = int(t.get("timestamp"))
    trade_ts = datetime.fromtimestamp(ts, tz=timezone.utc)

    tx = t.get("transactionHash") or ""

    notional = None
    if price is not None and size is not None:
        notional = price * size

    trade_id = f"{tx}:{token_id}:{side}:{ts}:{size}"

    return {
        "trade_id": trade_id,
        "wallet_address": wallet,
        "token_id": token_id,
        "condition_id": condition_id,
        "side": side,
        "price": price,
        "size": size,
        "notional": notional,
        "trade_ts": trade_ts,
        "source": "data_api",
    }


def main():
    rules = load_rules()  # грузим один раз

    trades = fetch_trades()
    print("fetched:", len(trades))

    ok = 0
    skipped_existing = 0

    for t in trades:
        nt = normalize(t)

        if not nt["wallet_address"] or nt["notional"] is None or not nt["condition_id"]:
            continue

        # 1) сохраняем сырую сделку; если уже была — пропускаем всё остальное
        inserted = save_raw_trade(nt)
        if not inserted:
            skipped_existing += 1
            continue

        # 2) обновляем состояние пользователя (в БД)
        _ = update_user_state(nt)

        # 3) обновляем окно (в БД)
        win = update_window_and_check_alert(nt)

        ok += 1
        print(
            ok,
            nt["wallet_address"],
            nt["condition_id"],
            nt["side"],
            float(nt["notional"]),
            "win_total",
            win.get("total_notional"),
        )

        # 4) читаем user_state и принимаем решение по алерту
        us = get_user_state(nt["wallet_address"]) or {}
        decision = should_alert(nt, us, win, rules)

        if decision.get("should_alert"):
            updated = mark_window_alerted(
                nt["wallet_address"],
                nt["condition_id"],
                win["window_start_ts"],
                win["window_minutes"],
            )
            if updated == 1:
                print(
                    "ALERT",
                    decision.get("alert_type"),
                    nt["wallet_address"],
                    nt["condition_id"],
                    "window_start",
                    win["window_start_ts"].isoformat(),
                    "total",
                    win["total_notional"],
                    "trades",
                    win["trade_count"],
                    "user_trades",
                    us.get("total_trades"),
                    "status",
                    us.get("status"),
                    "reason",
                    decision.get("reason"),
                )

    print("processed:", ok, "skipped_existing:", skipped_existing)


if __name__ == "__main__":
    main()
