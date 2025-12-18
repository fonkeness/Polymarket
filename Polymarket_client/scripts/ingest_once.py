from Polymarket_client.app.ingestion.fetch_trades import fetch_latest_trades
from Polymarket_client.app.normalization.data_api_trade_normalizer import normalize_data_api_trade
from Polymarket_client.db.raw_trades_repo import save_raw_trade
from Polymarket_client.app.state.user_state_updater import update_user_state


def main():
    trades = fetch_latest_trades(limit=25, offset=0, taker_only=True)
    print("fetched:", len(trades))

    for t in trades:
        nt = normalize_data_api_trade(t)

        # фильтр на всякий: если нет кошелька или цены/сайза - пропускаем
        if not nt["wallet_address"] or nt["notional"] is None:
            continue

        save_raw_trade(nt)
        st = update_user_state(nt)
        print(nt["wallet_address"], nt["condition_id"], nt["side"], nt["notional"], st)


if __name__ == "__main__":
    main()
