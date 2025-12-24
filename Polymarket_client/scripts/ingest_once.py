import json
import subprocess
from datetime import datetime, timezone

from Polymarket_client.db.raw_trades_repo import save_raw_trade
from Polymarket_client.app.state.user_state_updater import update_user_state
from Polymarket_client.app.bet_aggregation.window_aggregator import update_window_and_check_alert
from Polymarket_client.db.trade_windows_repo import mark_window_alerted



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

    # Дедуп-ключ (стабильный для одной и той же сделки)
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
    trades = fetch_trades()
    print("fetched:", len(trades))

    ok = 0
    for t in trades:
        nt = normalize(t)

        # базовая валидация
        if not nt["wallet_address"] or nt["notional"] is None or not nt["condition_id"]:
            continue

        # 1) сохраняем сырую сделку
        save_raw_trade(nt)

        # 2) обновляем состояние пользователя
        status = update_user_state(nt)

        # 3) обновляем окно и проверяем порог
        win = update_window_and_check_alert(nt)

        ok += 1
        print(
            ok,
            nt["wallet_address"],
            nt["condition_id"],
            nt["side"],
            float(nt["notional"]),
            status,
            "win_total",
            win.get("total_notional"),
        )

        if win.get("is_candidate"):
            updated = mark_window_alerted(
                nt["wallet_address"],
                nt["condition_id"],
                win["window_start_ts"],
                win["window_minutes"],
            )
            if updated == 1:
                print(
                    "ALERT_CANDIDATE",
                    nt["wallet_address"],
                    nt["condition_id"],
                    "window_start",
                    win["window_start_ts"].isoformat(),
                    "total",
                    win["total_notional"],
                    "trades",
                    win["trade_count"],
                    "min_total",
                    win["min_total_notional"],
                )

    print("processed:", ok)


if __name__ == "__main__":
    main()
