# trapi

Unofficial Python client for Trade Republic: portfolio, live quotes, orders, and an export of your timeline
and documents for Portfolio Performance.

> **Disclaimer.** Not affiliated with Trade Republic Bank GmbH. trapi uses the same unofficial interface as the
> Trade Republic web app, which can change at any time, and automated access may be against Trade Republic's
> terms. Use at your own risk. Orders are real orders with real money. Nothing here is investment advice.

## Login

trapi logs in the way the web app does, so **you stay logged in on your phone**. Nothing gets registered as
a new device and the app keeps working as before. You confirm the login once in the app (or with your
authenticator code). After that the session is kept in `~/.trapi/session.txt` and renewed automatically, so
later runs need no confirmation until Trade Republic asks again. Your PIN is never stored.

## Install

```bash
pip install git+https://github.com/LukasRahn/TradeRepublicApi
```

Python 3.11 or newer.

## Command line

```bash
trapi login                                # phone number and PIN, then confirm in the app
trapi portfolio                            # positions with current prices, and cash
trapi export [FOLDER]                      # events.json, transactions.csv and all PDFs
trapi sell DE000BD6BNQ7 555 --limit 0.05   # limit sell, shows the order and asks first
trapi request instrument id=US0378331005   # any request, answer as JSON
```

- `trapi sell` picks the exchange from the instrument. Without `--limit` it sells at market. `--expiry gtd
  --expiry-date 2026-12-31` keeps a limit order until that date (some exchanges, e.g. `BVT`, have no `gtc`).
- `transactions.csv` imports into Portfolio Performance (CSV import, separator `;`, decimal `.`). Splits,
  spin-offs and transfers are listed at the end instead of converted.

## Library

```python
import asyncio
from trapi import TradeRepublic

async def main():
    tr = TradeRepublic(session_file="~/.trapi/session.txt")
    if not tr.resume():
        tr.login("+491701234567", "1234")

    print(await tr.portfolio())
    print(await tr.request("instrument", id="US0378331005"))
    async for quote in tr.stream("ticker", id="US0378331005.LSX"):
        print(quote["last"]["price"])

asyncio.run(main())
```

`request(topic, **params)` returns the first answer, `stream()` every update after it. `Blocking(TradeRepublic())`
gives the same methods without asyncio. Orders: `limit_order()`, `market_order()`, `cancel_order()`; check the
`status` of the answer.

If Trade Republic starts refusing the client, update `APP_VERSION` and `CONNECT_VERSION` in `trapi/api.py`.

## License

MIT
