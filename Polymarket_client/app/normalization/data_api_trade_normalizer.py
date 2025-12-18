from datetime import datetime, timezone


def normalize_data_api_trade(t: dict) -> dict:
    # Поля из Data API
    wallet = t.get("proxyWallet")
    token_id = t.get("asset")              # это token/outcome token id (ERC1155-ish id)
    condition_id = t.get("conditionId")    # рынок
    side = t.get("side")
    price = float(t.get("price")) if t.get("price") is not None else None
    size = float(t.get("size")) if t.get("size") is not None else None
    ts = int(t.get("timestamp"))

    trade_ts = datetime.fromtimestamp(ts, tz=timezone.utc)

    # notional в долларах ~= size * price (ок для начала)
    notional = None
    if price is not None and size is not None:
        notional = float(price * size)

    # Дедуп ключ: txHash + outcome token id + side + ts (на случай если trade_id нет)
    tx = t.get("transactionHash") or ""
    trade_id = f"{tx}:{token_id}:{side}:{ts}:{size}"

    return {
        "trade_id": trade_id,
        "wallet_address": wallet,
        "token_id": token_id,         # outcome token id
        "condition_id": condition_id, # рынок
        "side": side,
        "price": price,
        "size": size,
        "notional": notional,
        "trade_ts": trade_ts,
        "source": "data_api",
        # полезно для алертов (пока просто тащим)
        "title": t.get("title"),
        "slug": t.get("slug"),
        "outcome": t.get("outcome"),
        "event_slug": t.get("eventSlug"),
        "tx": tx,
    }
