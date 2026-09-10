# trapi

Unofficial Python client for Trade Republic: login, portfolio, live quotes, orders, a timeline export for
Portfolio Performance, and a download of all your documents.

> **Disclaimer.** trapi is not affiliated with, endorsed by or connected to Trade Republic Bank GmbH.
> Trade Republic offers no public API; trapi talks to the same interface the Trade Republic web app uses,
> which can change or break at any time. Automated access may be against Trade Republic's terms of service.
> You use trapi at your own risk. Orders placed with trapi are real orders with real money.
> Nothing in this project is investment advice.

## Install

```bash
pip install git+https://github.com/LukasRahn/TradeRepublicApi
# or, inside a clone
pip install -e .
```

Python 3.11 or newer.

## Command line

```bash
trapi login                                # phone number and PIN, then confirm in the app
trapi portfolio                            # positions with current prices, and cash
trapi export [FOLDER]                      # timeline, CSV and PDFs, default ./trade-republic
trapi export --no-documents                # without the PDFs
trapi request instrument id=US0378331005   # any subscription, answer printed as JSON
trapi request ticker id=US0378331005.LSX
trapi sell DE000BD6BNQ7 555 --limit 0.05   # limit sell, shows the order and asks before sending
trapi sell DE000BD59BU9 1                  # market sell
```

`python -m trapi` works as well.

The login is kept in `~/.trapi/session.txt` (`--session FILE` for another place), so the app does not have
to confirm every run. The file is as good as your login, keep it private. Phone number and PIN can come from
`TR_PHONE` and `TR_PIN`; the PIN is never stored. Logging in works like the web app, your phone stays logged in.

### Selling

`trapi sell` takes the exchange from the instrument (turbos usually trade on `BVT`), `--exchange` overrides it.
Limit orders are valid until the end of the day by default. `--expiry gtc` keeps them until cancelled,
`--expiry gtd --expiry-date 2026-12-31` until a date. Not every exchange offers every expiry: `BVT` accepts
only `gfd` and `gtd`. `--yes` sends without asking. Trade Republic's answer is always printed; its `status`
tells whether the order was accepted.

### Export

`trapi export` writes:

| File | Content |
|---|---|
| `events.json` | every timeline entry with its details, raw |
| `transactions.csv` | buys, sells, savings plans, deposits, removals, dividends, interest, taxes, bonus shares, knock-outs |
| `documents/YEAR/*.pdf` | settlements, statements and so on; files already on disk are skipped |

`transactions.csv` is made for the CSV import of Portfolio Performance (File > Import > CSV files, type
"Account and portfolio transactions", separator `;`, decimal separator `.`). Its column and type names are the
German ones Portfolio Performance expects. Splits, spin-offs, swaps and securities transfers are not converted;
the export lists them at the end so nothing goes missing silently.

## As a library

```python
import asyncio
from trapi import TradeRepublic

async def main():
    tr = TradeRepublic(session_file="~/.trapi/session.txt")
    if not tr.resume():
        tr.login("+491701234567", "1234")        # waits for the confirmation in the app

    print(await tr.portfolio())
    print(await tr.request("cash"))
    print(await tr.ticker("US0378331005", "LSX"))

    async for quote in tr.stream("ticker", id="US0378331005.LSX"):
        print(quote["last"]["price"])            # runs until you stop it

    await tr.close()

asyncio.run(main())
```

Without asyncio:

```python
from trapi import Blocking, TradeRepublic

with Blocking(TradeRepublic(session_file="~/.trapi/session.txt")) as tr:
    tr.resume() or tr.login("+491701234567", "1234")
    print(tr.request("cash"))
    print(tr.timeline())
```

### Requests

Every request is a subscription on a websocket. `request(topic, **params)` sends it, returns the first answer
and ends the subscription. `stream(topic, **params)` yields every update after it as well.

| Topic | Parameters | Login |
|---|---|---|
| `instrument` | `id=ISIN` | no |
| `stockDetails`, `etfDetails`, `etfComposition` | `id=ISIN` | no |
| `ticker`, `performance` | `id="ISIN.LSX"` | no |
| `aggregateHistoryLight` | `id="ISIN.LSX", range="1y"` (`1d 5d 1m 3m 1y max`) | no |
| `neonSearch` | `data={"q": "apple", "page": 1, "pageSize": 20, "filter": [{"key": "type", "value": "stock"}]}` | no |
| `neonNews` | `isin=ISIN` | no |
| `priceForOrder` | `parameters={"instrumentId": ISIN, "exchangeId": "LSX", "type": "buy"}` | no |
| `cash`, `availableCash`, `availableCashForPayout` | | yes |
| `compactPortfolioByType` | through `tr.portfolio()` | yes |
| `timelineTransactions`, `timelineActivityLog` | through `tr.timeline(topic)` | yes |
| `timelineDetailV2` | through `tr.timeline_detail(id)` | yes |
| `orders` | `terminated=False` | yes |
| `savingsPlans`, `priceAlarms`, `watchlists` | | yes |

Orders:

```python
await tr.limit_order("US0378331005", "buy", size=1, limit=180.0, expiry="gfd")
await tr.limit_order("US0378331005", "sell", size=1, limit=250.0, expiry="gtd", expiry_date="2026-12-31")
await tr.market_order("US0378331005", "buy", size=0.5)
await tr.cancel_order(order_id)
```

The answer always comes back, a rejection included. Check its `status` (`succeeded` or `failed`), and
`orders` to see whether an accepted order still stands.

## When it stops working

Trade Republic refuses outdated client versions. Update `APP_VERSION`, `CONNECT_VERSION` or `CONNECT_INFO` in
`trapi/api.py` to what the current web app sends (browser developer tools, network tab on
app.traderepublic.com).

## Development

```bash
python -m unittest discover -s tests
```

## License

MIT, see [LICENSE](LICENSE).
