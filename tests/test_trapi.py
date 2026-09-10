import asyncio
import json
import unittest
from decimal import Decimal
from unittest import mock

from trapi import Blocking, TradeRepublic, TradeRepublicError
from trapi.__main__ import main
from trapi.api import apply_delta
from trapi.export import parse_number, transactions


class FakeSocket:
    """Stands in for the websocket. Answers each subscription with the frames
    listed for its type."""

    def __init__(self, frames):
        self.frames = frames
        self.sent = []
        self.close_code = None
        self._incoming = asyncio.Queue()

    async def recv(self):
        return "connected"

    async def send(self, message):
        self.sent.append(message)
        if message.startswith("sub "):
            _, sid, payload = message.split(" ", 2)
            for frame in self.frames.get(json.loads(payload)["type"], []):
                self._incoming.put_nowait(f"{sid} {frame}")

    def push(self, frame):
        self._incoming.put_nowait(frame)

    def __aiter__(self):
        return self

    async def __anext__(self):
        frame = await self._incoming.get()
        if frame is None:
            raise StopAsyncIteration
        return frame

    async def close(self):
        self.close_code = 1000
        self._incoming.put_nowait(None)


def connected(frames):
    socket = FakeSocket(frames)

    async def connect(*args, **kwargs):
        return socket

    return socket, mock.patch("trapi.api.websockets.connect", connect)


class ProtocolTest(unittest.IsolatedAsyncioTestCase):
    def test_delta(self):
        previous = '{"a":1,"b":"x y"}'
        self.assertEqual(apply_delta(previous, "=5\t-1\t+2\t=5\t-5\t+%22z+w%22\t=1"), '{"a":2,"b":"z w"}')

    async def test_request_returns_first_answer_and_unsubscribes(self):
        socket, patch = connected({"instrument": ['A {"shortName":"Apple"}']})
        with patch:
            tr = TradeRepublic()
            self.assertEqual(await tr.request("instrument", id="US0378331005"), {"shortName": "Apple"})
            await tr.close()
        self.assertTrue(socket.sent[0].startswith("connect 31 "))
        self.assertEqual(json.loads(socket.sent[1].split(" ", 2)[2]), {"type": "instrument", "id": "US0378331005"})
        self.assertEqual(socket.sent[2], "unsub 1")

    async def test_stream_applies_updates(self):
        socket, patch = connected({"ticker": ['A {"last":{"price":10.5}}', "D =20\t-1\t+75\t=2"]})
        with patch:
            tr = TradeRepublic()
            prices = []
            async for quote in tr.stream("ticker", id="X.LSX"):
                prices.append(quote["last"]["price"])
                if len(prices) == 2:
                    break
            await tr.close()
        self.assertEqual(prices, [10.5, 10.75])

    async def test_error_frame_raises(self):
        socket, patch = connected({"portfolio": ['E {"errors":[{"errorCode":"BAD_SUBSCRIPTION_TYPE"}]}']})
        with patch:
            tr = TradeRepublic()
            with self.assertRaisesRegex(TradeRepublicError, "BAD_SUBSCRIPTION_TYPE"):
                await tr.request("portfolio")
            await tr.close()

    async def test_late_update_of_an_ended_subscription_is_ignored(self):
        socket, patch = connected({"cash": ['A [{"amount":1}]'], "instrument": ['A {"ok":true}']})
        with patch:
            tr = TradeRepublic()
            await tr.request("cash")
            socket.push("1 D =2\t-1\t+2\t=12")  # used to crash the read loop
            self.assertEqual(await tr.request("instrument", id="X"), {"ok": True})
            await tr.close()

    async def test_order_payload(self):
        tr = TradeRepublic()
        with mock.patch.object(tr, "request", mock.AsyncMock(return_value={"status": "succeeded"})) as request:
            await tr.limit_order("US0378331005", "buy", "1", "180.5", expiry="gtd", expiry_date="2026-12-31")
        payload = request.call_args.kwargs
        self.assertEqual(request.call_args.args, ("simpleCreateOrder",))
        self.assertEqual(payload["parameters"]["size"], 1.0)
        self.assertEqual(payload["parameters"]["limit"], 180.5)
        self.assertEqual(payload["parameters"]["expiry"], {"type": "gtd", "value": "2026-12-31"})
        self.assertEqual(len(payload["clientProcessId"]), 36)

    async def test_order_validation(self):
        tr = TradeRepublic()
        for args, kwargs in [
            (("US0378331005", "hold", 1, 10), {}),
            (("APPLE", "buy", 1, 10), {}),
            (("US0378331005", "buy", 0, 10), {}),
            (("US0378331005", "buy", 1, "abc"), {}),
            (("US0378331005", "buy", 1, 10), {"expiry": "gtd"}),
            (("US0378331005", "buy", 1, 10), {"expiry_date": "2026-12-31"}),
        ]:
            with self.subTest(args=args, kwargs=kwargs), self.assertRaises(ValueError):
                await tr.limit_order(*args, **kwargs)


class BlockingTest(unittest.TestCase):
    def test_blocking_request(self):
        socket, patch = connected({"cash": ['A [{"amount":12.5,"currencyId":"EUR"}]']})
        with patch, Blocking(TradeRepublic()) as tr:
            self.assertEqual(tr.request("cash"), [{"amount": 12.5, "currencyId": "EUR"}])


class ExportTest(unittest.TestCase):
    def test_parse_number(self):
        cases = {
            "1,00 €": "1.00",
            "€11.14": "11.14",
            "17,77 €": "17.77",
            "-0,78 €": "-0.78",
            "0,685102": "0.685102",
            "10.640298": "10.640298",
            "14.000000": "14.000000",
            "9.400": "9400",
            "1,875": "1.875",
            "0.347": "0.347",
            "50": "50",
            "3.002,80 €": "3002.80",
            "€1,234.56": "1234.56",
            "0,685102 × 160,56 €": "0.685102",
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(parse_number(text), Decimal(expected))
        self.assertIsNone(parse_number(None))
        self.assertIsNone(parse_number("Ausgeführt"))

    def event(self, event_type, value, sections=(), **extra):
        return {
            "id": "e1",
            "timestamp": "2025-10-10T19:29:43.928+0000",
            "title": "NVIDIA",
            "icon": "logos/US67066G1040/v2",
            "eventType": event_type,
            "status": "EXECUTED",
            "amount": {"currency": "EUR", "value": value},
            "details": {"sections": list(sections)},
            **extra,
        }

    def test_buy_reads_shares_from_the_nested_screen(self):
        nested = {"sections": [{"type": "table", "data": [{"title": "Aktien", "detail": {"text": "0,685102"}}]}]}
        overview = {
            "type": "table",
            "title": "Übersicht",
            "data": [
                {"title": "Transaktion", "detail": {"text": "0,685102 × 160,56 €", "action": {"payload": nested}}},
                {"title": "Gebühr", "detail": {"text": "1,00 €"}},
            ],
        }
        header = {"type": "header", "action": {"type": "instrumentDetail", "payload": "US67066G1040"}}
        [row] = transactions(self.event("trading_trade_executed", -111.0, [header, overview]))
        self.assertEqual(row["Typ"], "Kauf")
        self.assertEqual(row["Wert"], -111.0)
        self.assertEqual(row["ISIN"], "US67066G1040")
        self.assertEqual(row["Stück"], Decimal("0.685102"))
        self.assertEqual(row["Gebühren"], Decimal("1.00"))

    def test_dividend_and_taxes(self):
        shares = {"title": "Aktien", "detail": {"text": "10.640298"}}
        table = {"type": "table", "data": [shares, {"title": "Steuer", "detail": {"text": "-0,78 €"}}]}
        [row] = transactions(self.event("ssp_corporate_action_invoice_cash", 2.24, [table]))
        self.assertEqual(
            (row["Typ"], row["Stück"], row["Steuern"]), ("Dividende", Decimal("10.640298"), Decimal("0.78"))
        )
        [row] = transactions(self.event("SSP_CORPORATE_ACTION_CASH", -3.5))
        self.assertEqual(row["Typ"], "Steuern")

    def test_sell_deposit_saveback_cancelled_unknown(self):
        self.assertEqual(transactions(self.event("ORDER_EXECUTED", 94.76))[0]["Typ"], "Verkauf")
        self.assertEqual(transactions(self.event("BANK_TRANSACTION_INCOMING", 200.0))[0]["Typ"], "Einlage")
        buy, deposit = transactions(self.event("SAVEBACK_AGGREGATE", -15.0))
        self.assertEqual((buy["Typ"], deposit["Typ"], deposit["Wert"]), ("Kauf", "Einlage", 15.0))
        self.assertIsNone(deposit["ISIN"])
        self.assertEqual(transactions(self.event("TRADING_TRADE_EXECUTED", -5.0, status="CANCELED")), [])
        self.assertIsNone(transactions(self.event("SSP_CORPORATE_ACTION_INVOICE_SHARES", 0)))

    def test_events_without_event_type(self):
        overview = {
            "type": "table",
            "data": [
                {"title": "Transaktion", "detail": {"text": "", "displayValue": {"prefix": "0.105 ×"}}},
                {"title": "Gebühr", "detail": {"text": "Kostenlos"}},
                {"title": "Steuer", "detail": {"text": "0,00 €"}},
            ],
        }
        [row] = transactions(self.event(None, -75.6, [overview], subtitle="Limit-Buy-Order"))
        self.assertEqual(
            (row["Typ"], row["Stück"], row["Gebühren"], row["Steuern"]), ("Kauf", Decimal("0.105"), None, None)
        )
        spent = {"type": "header", "title": "Du hast 2,00 € ausgegeben", "data": {"status": "executed"}}
        self.assertEqual(transactions(self.event(None, -2.0, [spent], title="Baecker"))[0]["Typ"], "Entnahme")
        self.assertIsNone(transactions(self.event(None, None, [spent], title="Baecker")))

    def test_share_bonus_takes_its_value_from_the_total(self):
        total = {"type": "table", "data": [{"title": "Gesamt", "detail": {"text": "10,03 €"}}]}
        event = self.event("ACQUISITION_TRADE_PERK", None, [total])
        event["amount"] = None
        buy, deposit = transactions(event)
        self.assertEqual((buy["Wert"], deposit["Wert"]), (Decimal("-10.03"), Decimal("10.03")))

    def test_knock_out_ends_the_position_even_without_payout(self):
        removed = {"type": "table", "data": [{"title": "Aktien entfernt", "detail": {"text": "250.000000"}}]}
        event = self.event("SSP_CORPORATE_ACTION_CASH_NON_DIVIDEND", 0.0, [removed], subtitle="Tilgung")
        [row] = transactions(event)
        self.assertEqual((row["Typ"], row["Wert"], row["Stück"]), ("Verkauf", 0.0, Decimal("250")))

    def test_free_shares_are_a_deposit_plus_a_buy(self):
        shares = {"title": "Anteile", "detail": {"text": "0.0394"}}
        table = {"type": "table", "data": [shares, {"title": "Gesamt", "detail": {"text": "14,83 €"}}]}
        event = self.event("STOCK_PERK_REFUNDED", None, [table])
        event["amount"] = None
        buy, deposit = transactions(event)
        self.assertEqual((buy["Typ"], buy["Stück"], buy["Wert"]), ("Kauf", Decimal("0.0394"), Decimal("-14.83")))
        self.assertEqual((deposit["Typ"], deposit["Wert"]), ("Einlage", Decimal("14.83")))


class SellCommandTest(unittest.TestCase):
    def client(self):
        patch = mock.patch("trapi.__main__.TradeRepublic")
        tr = patch.start().return_value
        self.addCleanup(patch.stop)
        tr.resume.return_value = True
        tr.request = mock.AsyncMock(return_value={"exchangeIds": ["BVT"]})
        tr.limit_order = mock.AsyncMock(return_value={"status": "succeeded", "orderId": "o1"})
        tr.market_order = mock.AsyncMock(return_value={"status": "succeeded", "orderId": "o2"})
        tr.close = mock.AsyncMock()
        return tr

    def test_limit_sell_goes_to_the_instruments_exchange(self):
        tr = self.client()
        main(["sell", "DE000BD6BNQ7", "555", "--limit", "0.05", "--yes"])
        tr.limit_order.assert_awaited_once_with(
            "DE000BD6BNQ7", "sell", 555.0, 0.05, expiry="gfd", expiry_date=None, exchange="BVT"
        )
        tr.market_order.assert_not_awaited()

    def test_nothing_is_sent_without_a_yes(self):
        tr = self.client()
        with mock.patch("builtins.input", return_value="n"), self.assertRaises(SystemExit):
            main(["sell", "DE000BD59BU9", "1"])
        tr.limit_order.assert_not_awaited()
        tr.market_order.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
