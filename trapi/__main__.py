"""Command line: ``python -m trapi --help``."""

import argparse
import asyncio
import getpass
import json
import os
import sys
from pathlib import Path

from trapi.api import TradeRepublic, TradeRepublicError
from trapi.export import export

SESSION = Path.home() / ".trapi" / "session.txt"


def main(argv=None):
    parser = argparse.ArgumentParser(prog="trapi", description="Unofficial Trade Republic client")
    parser.add_argument("--session", type=Path, default=SESSION, help="where the login is kept (%(default)s)")
    parser.add_argument("--locale", default="de", help="language of the answers (%(default)s)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("login", help="log in and keep the session")
    commands.add_parser("portfolio", help="positions and cash")
    command = commands.add_parser("export", help="timeline as JSON and CSV for Portfolio Performance, plus documents")
    command.add_argument("folder", type=Path, nargs="?", default=Path("trade-republic"))
    command.add_argument("--no-documents", action="store_true", help="skip the PDF downloads")
    command = commands.add_parser("request", help="send any subscription and print the answer")
    command.add_argument("topic", help="e.g. instrument, ticker, neonSearch")
    command.add_argument("params", nargs="*", help="key=value, values that are valid JSON are read as JSON")
    command = commands.add_parser("sell", help="sell a position with a limit, or at market without one")
    command.add_argument("isin")
    command.add_argument("size", type=float)
    command.add_argument("--limit", type=float, help="limit price, without it the order is a market order")
    command.add_argument("--expiry", default="gfd", choices=["gfd", "gtc", "gtd"], help="limit orders (%(default)s)")
    command.add_argument("--expiry-date", help="YYYY-MM-DD, for --expiry gtd")
    command.add_argument("--exchange", help="default: the instrument's first exchange, e.g. BVT for turbos")
    command.add_argument("--yes", action="store_true", help="send without asking first")
    args = parser.parse_args(argv)

    tr = TradeRepublic(locale=args.locale, session_file=args.session)
    try:
        if args.command == "login":
            login(tr, force=True)
            print(f"Logged in, session kept in {args.session}")
        elif args.command == "request":
            tr.resume()  # topics that need no login work without a session
            params = dict(parse_param(p) for p in args.params)
            print(json.dumps(asyncio.run(run(tr, tr.request(args.topic, **params))), indent=2, ensure_ascii=False))
        elif args.command == "sell":
            login(tr)
            answer = asyncio.run(run(tr, sell(tr, args)))
            if answer is None:
                sys.exit("not sent")
            print(json.dumps(answer, indent=2, ensure_ascii=False))
            if answer.get("status") != "succeeded":
                sys.exit(1)
        elif args.command == "portfolio":
            login(tr)
            asyncio.run(run(tr, show_portfolio(tr)))
        else:
            login(tr)
            asyncio.run(run(tr, export(tr, args.folder, documents=not args.no_documents)))
    except (TradeRepublicError, TimeoutError, ValueError) as e:
        sys.exit(f"error: {e}")
    except KeyboardInterrupt:
        sys.exit(130)


def login(tr, force=False):
    if not force and tr.resume():
        return
    phone = os.environ.get("TR_PHONE") or input("Phone number (+49...): ")
    pin = os.environ.get("TR_PIN") or getpass.getpass("PIN: ")
    print("Confirm the login in the Trade Republic app...")
    tr.login(phone.strip(), pin.strip())


async def run(tr, coroutine):
    try:
        return await coroutine
    finally:
        await tr.close()


async def sell(tr, args):
    """Shows the order, asks unless --yes, sends it. Returns the answer, or None when not sent."""
    exchange = args.exchange
    if not exchange:
        instrument = await tr.request("instrument", id=args.isin)
        exchange = (instrument.get("exchangeIds") or ["LSX"])[0]
    limited = args.limit is not None
    kind = f"limit {args.limit:g}, valid {args.expiry_date or args.expiry}" if limited else "market"
    print(f"SELL {args.size:g} x {args.isin} on {exchange}, {kind}")
    if not args.yes:
        reply = await asyncio.to_thread(input, "Send this order? [y/N] ")
        if reply.strip().lower() not in ("y", "yes", "j", "ja"):
            return None
    if limited:
        return await tr.limit_order(
            args.isin,
            "sell",
            args.size,
            args.limit,
            expiry=args.expiry,
            expiry_date=args.expiry_date,
            exchange=exchange,
        )
    return await tr.market_order(args.isin, "sell", args.size, exchange=exchange)


async def show_portfolio(tr):
    portfolio, cash = await asyncio.gather(tr.portfolio(), tr.request("cash"))
    positions = [p for category in portfolio.get("categories", []) for p in category.get("positions", [])]

    async def price(position):
        isin = position.get("isin") or position.get("instrumentId")
        position["isin"] = isin
        try:
            instrument = await tr.request("instrument", id=isin)
            position["name"] = instrument.get("shortName", isin)
            exchanges = instrument.get("exchangeIds") or []
            if exchanges:
                # ponytail: bonds are quoted in percent, their value comes out 100x too high.
                position["price"] = float((await tr.ticker(isin, exchanges[0]))["last"]["price"])
        except (TradeRepublicError, TimeoutError, KeyError):
            pass

    await asyncio.gather(*(price(p) for p in positions))

    total = 0.0
    print(f"{'Name':<28} {'ISIN':<12} {'Stück':>12} {'Einstand':>10} {'Kurs':>10} {'Wert':>11} {'+/-':>7}")
    for p in sorted(positions, key=lambda p: -float(p.get("netSize", 0)) * p.get("price", 0)):
        size, buy_in, price = float(p.get("netSize", 0)), float(p.get("averageBuyIn", 0)), p.get("price")
        value = size * price if price is not None else None
        total += value or 0
        change = f"{(price / buy_in - 1) * 100:>6.1f}%" if price and buy_in else ""
        print(
            f"{p.get('name', p['isin'])[:28]:<28} {p['isin']:<12} {size:>12.6g} {amount(buy_in):>10} "
            f"{amount(price):>10} {amount(value):>11} {change:>7}"
        )
    for account in cash if isinstance(cash, list) else [cash]:
        print(f"{'Cash ' + str(account.get('currencyId', '')):<66} {float(account.get('amount', 0)):>11.2f}")
        total += float(account.get("amount", 0))
    print(f"{'Total':<66} {total:>11.2f}")


def amount(number):
    # Turbos cost fractions of a cent, two decimals would show 0.00.
    if number is None:
        return ""
    return f"{number:.4f}" if abs(number) < 1 else f"{number:.2f}"


def parse_param(text):
    key, _, value = text.partition("=")
    try:
        return key, json.loads(value)
    except ValueError:
        return key, value


if __name__ == "__main__":
    main()
