import requests


DATA_API_TRADES = "https://data-api.polymarket.com/trades"


def fetch_latest_trades(limit: int = 50, offset: int = 0, taker_only: bool = True) -> list[dict]:
    params = {
        "limit": limit,
        "offset": offset,
        "takerOnly": "true" if taker_only else "false",
    }
    r = requests.get(DATA_API_TRADES, params=params, timeout=15)
    r.raise_for_status()
    return r.json()
