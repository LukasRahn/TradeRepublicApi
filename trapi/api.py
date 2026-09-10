"""Client for the unofficial Trade Republic API.

Trade Republic publishes no API. This follows what the web app at
app.traderepublic.com does: a login over REST that leaves session cookies
behind, and a websocket on which every request is a subscription.
"""

import asyncio
import base64
import hashlib
import inspect
import json
import os
import platform
import re
import time
import urllib.parse
import uuid
from datetime import date, datetime
from http.cookiejar import MozillaCookieJar
from pathlib import Path

import requests
import websockets

HOST = "https://api.traderepublic.com"
WS_URL = "wss://api.traderepublic.com"

# What the web app identifies itself with. Trade Republic refuses outdated
# values (HTTP 426 CLIENT_VERSION_OUTDATED, websocket answer "failed <n>"),
# so bump them when logins or connections start failing.
APP_VERSION = "2.2631.13"
PLATFORM = "web-pro"
CHROME = "146.0.0.0"
USER_AGENT = f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{CHROME} Safari/537.36"
CONNECT_VERSION = 31
CONNECT_INFO = {
    "platformId": "webtrading",
    "platformVersion": "chrome - 94.0.4606",
    "clientId": "app.traderepublic.com",
    "clientVersion": "5582",
}

# The session ends after about five minutes unless it is refreshed.
SESSION_REFRESH = 240

ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}\d")

_CLOSED = object()


class TradeRepublicError(Exception):
    """Trade Republic refused a request, or the session is gone."""


def apply_delta(previous, delta):
    """Applies an update to the previous answer of a subscription.

    Updates are instructions against the previous JSON text, separated by tabs:
    ``=n`` keeps n characters, ``-n`` skips n characters, ``+text`` inserts
    url-encoded text.
    """
    out, pos = [], 0
    for step in delta.split("\t"):
        sign, arg = step[:1], step[1:]
        if sign == "=":
            out.append(previous[pos : pos + int(arg)])
            pos += int(arg)
        elif sign == "-":
            pos += int(arg)
        elif sign == "+":
            out.append(urllib.parse.unquote_plus(arg))
    return "".join(out)


class TradeRepublic:
    """Async client. ``Blocking`` wraps it for plain synchronous code.

    ``session_file`` keeps the login between runs, so the app does not have to
    confirm every start. It is as good as a login, keep it private.
    """

    def __init__(self, locale="de", session_file=None):
        self.locale = locale
        self.session_file = Path(session_file).expanduser() if session_file else None
        self.http = requests.Session()
        self.http.headers["User-Agent"] = USER_AGENT
        self.http.cookies = MozillaCookieJar(self.session_file)
        self._refreshed = 0.0
        self._account = None
        self._lock = asyncio.Lock()
        self._ws = None
        self._reader = None
        self._ids = 0
        self._queues = {}
        self._payloads = {}
        self._last = {}

    # ------------------------------------------------------------ session

    @property
    def logged_in(self):
        return any(c.name == "tr_session" for c in self.http.cookies)

    def resume(self):
        """Picks up the session kept in ``session_file``. Returns whether it is still valid."""
        if not (self.session_file and self.session_file.exists()):
            return False
        self.http.cookies.load(ignore_discard=True)
        try:
            self.refresh()
        except TradeRepublicError:
            self.http.cookies.clear()
            return False
        return True

    def login(self, phone, pin, ask_code=input, timeout=120):
        """Logs in with phone number and PIN.

        Trade Republic then wants a second factor: usually a confirmation in the
        app, which this waits for. Accounts set up with an authenticator app are
        asked for its code through ``ask_code`` instead.
        """
        headers = self._device_headers()
        r = self.http.post(
            f"{HOST}/api/v2/auth/web/login", json={"phoneNumber": phone, "pin": pin}, headers=headers, timeout=30
        )
        process_id = _answer(r).get("processId")
        if not process_id:
            raise TradeRepublicError("login refused, no login process started")
        process = f"{HOST}/api/v2/auth/web/login/processes/{process_id}"

        state = _answer(self.http.get(process, headers=headers, timeout=30))
        if state.get("requiredAction") == "AUTHENTICATOR_VERIFICATION":
            code = ask_code("Code from your authenticator app: ").strip()
            url = f"{process}/authenticator-verification"
            _answer(self.http.post(url, json={"code": code}, headers=headers, timeout=30))
        else:
            deadline = time.monotonic() + timeout
            while state.get("status") not in ("CONFIRMED", "COMPLETED"):
                if state.get("status") != "PENDING":
                    raise TradeRepublicError(f"login ended with status {state.get('status')!r}")
                if time.monotonic() > deadline:
                    raise TradeRepublicError("login was not confirmed in the app in time")
                time.sleep(2)
                state = _answer(self.http.get(process, headers=headers, timeout=30))

        if not self.logged_in:
            raise TradeRepublicError("login finished without a session")
        self._refreshed = time.monotonic()
        self._save()

    def refresh(self):
        """Extends the session. Raises TradeRepublicError when it is gone for good."""
        r = self.http.get(f"{HOST}/api/v1/auth/web/session", timeout=30)
        if r.status_code != 200:
            raise TradeRepublicError(f"session expired (HTTP {r.status_code}), log in again")
        self._refreshed = time.monotonic()
        self._save()

    def keep_alive(self):
        """Refreshes the session when it is about to run out."""
        if self.logged_in and time.monotonic() - self._refreshed > SESSION_REFRESH:
            self.refresh()

    def account(self):
        """Account details, among them the securities account number."""
        if self._account is None:
            self.keep_alive()
            self._account = _answer(self.http.get(f"{HOST}/api/v2/auth/account", timeout=30))
        return self._account

    def _save(self):
        if self.session_file:
            self.session_file.parent.mkdir(parents=True, exist_ok=True)
            self.http.cookies.save(ignore_discard=True)
            os.chmod(self.session_file, 0o600)

    def _device_headers(self):
        machine = "|".join([str(uuid.getnode()), platform.node(), platform.machine(), platform.system()])
        offset = datetime.now().astimezone().utcoffset()
        device = {
            # Has to stay the same across logins, or every login counts as a new device.
            "stableDeviceId": hashlib.sha512(machine.encode()).hexdigest(),
            "browser": "Chrome",
            "browserVersion": CHROME,
            "os": platform.system(),
            "osVersion": platform.release(),
            "timezone": "Europe/Berlin",
            "timezoneOffset": -int(offset.total_seconds() // 60) if offset else 0,
            "screen": "1920x1080x24",
            "preferredLanguages": [self.locale],
            "numberOfCores": os.cpu_count() or 1,
        }
        return {
            "X-TR-Device-Info": base64.b64encode(json.dumps(device).encode()).decode(),
            "X-TR-App-Version": APP_VERSION,
            "X-Tr-Platform": PLATFORM,
            "Accept-Language": self.locale,
        }

    # ---------------------------------------------------------- websocket

    async def request(self, topic, timeout=30, **params):
        """Subscribes, returns the first answer and ends the subscription.

        ``topic`` is the subscription type, ``params`` go into the payload as
        they are: ``await tr.request("instrument", id="US0378331005")``.
        """
        sid, queue = await self._subscribe({"type": topic, **params})
        try:
            answer = await asyncio.wait_for(queue.get(), timeout)
        finally:
            await self._unsubscribe(sid)
        if isinstance(answer, Exception):
            raise answer
        return None if answer is _CLOSED else answer

    async def stream(self, topic, **params):
        """Yields the first answer and every update after it, e.g. for ``ticker``."""
        sid, queue = await self._subscribe({"type": topic, **params})
        try:
            while (answer := await queue.get()) is not _CLOSED:
                if isinstance(answer, Exception):
                    raise answer
                yield answer
        finally:
            await self._unsubscribe(sid)

    async def close(self):
        if self._ws is not None:
            await self._ws.close()
            await self._reader
            self._ws = None

    async def _subscribe(self, payload):
        ws = await self._connect()
        self._ids += 1
        sid = str(self._ids)
        self._queues[sid] = queue = asyncio.Queue()
        self._payloads[sid] = payload
        await ws.send(f"sub {sid} {json.dumps(payload)}")
        return sid, queue

    async def _unsubscribe(self, sid):
        self._queues.pop(sid, None)
        self._payloads.pop(sid, None)
        self._last.pop(sid, None)
        # Without this the server keeps sending updates for it on this connection.
        if self._ws is not None and self._ws.close_code is None:
            await self._ws.send(f"unsub {sid}")

    async def _connect(self):
        async with self._lock:
            if self._ws is None or self._ws.close_code is not None:
                await asyncio.to_thread(self.keep_alive)
                self._ws = await self._open()
                self._reader = asyncio.create_task(self._read(self._ws))
        return self._ws

    async def _open(self):
        cookies = "; ".join(f"{c.name}={c.value}" for c in self.http.cookies if c.domain.endswith("traderepublic.com"))
        headers = {"Cookie": cookies} if cookies else None
        ws = await websockets.connect(WS_URL, additional_headers=headers, max_size=None)
        await ws.send(f"connect {CONNECT_VERSION} {json.dumps({'locale': self.locale, **CONNECT_INFO})}")
        answer = await ws.recv()
        if answer != "connected":
            await ws.close()
            raise TradeRepublicError(f"websocket refused the connection: {answer}")
        return ws

    async def _read(self, ws):
        """Hands every frame to the queue of its subscription.

        Frames look like ``<id> <code> <body>``: A is a full answer, D an update
        against the previous one, C the end of the subscription, E an error.
        """
        try:
            async for frame in ws:
                sid, _, rest = frame.partition(" ")
                code, body = rest[:1], rest[1:].lstrip()
                queue = self._queues.get(sid)
                if queue is None:
                    continue  # late frame of a subscription that already ended
                if code == "C":
                    queue.put_nowait(_CLOSED)
                    continue
                if code == "E":
                    topic = self._payloads.get(sid, {}).get("type")
                    queue.put_nowait(TradeRepublicError(f"{topic}: {body}"))
                    continue
                if code == "D":
                    body = apply_delta(self._last.get(sid, ""), body)
                self._last[sid] = body
                try:
                    queue.put_nowait(json.loads(body) if body else {})
                except ValueError:
                    queue.put_nowait(TradeRepublicError(f"unreadable answer: {body[:200]}"))
        except websockets.ConnectionClosed:
            pass
        finally:
            for queue in self._queues.values():
                queue.put_nowait(TradeRepublicError("connection to Trade Republic closed"))
            self._queues.clear()
            self._last.clear()

    # ------------------------------------------------------------- topics
    # Everything else goes through request(), see the README for common topics.

    async def portfolio(self):
        """Positions, grouped by instrument type: ``categories[].positions[]``."""
        account = await asyncio.to_thread(self.account)
        return await self.request("compactPortfolioByType", secAccNo=account["securitiesAccountNumber"])

    async def ticker(self, isin, exchange="LSX"):
        return await self.request("ticker", id=f"{isin}.{exchange}")

    async def timeline(self, topic="timelineTransactions"):
        """Every entry of a timeline, following its pages to the end.

        ``timelineTransactions`` holds what moved money, ``timelineActivityLog``
        everything else.
        """
        items, after = [], None
        while True:
            page = await self.request(topic, after=after)
            items += page.get("items", [])
            after = (page.get("cursors") or {}).get("after")
            if not after:
                return items

    async def timeline_detail(self, event_id):
        return await self.request("timelineDetailV2", id=event_id)

    async def limit_order(self, isin, side, size, limit, expiry="gfd", expiry_date=None, exchange="LSX"):
        """Places a limit order and returns the answer. Check its "status".

        ``side`` is "buy" or "sell". ``expiry`` is "gfd" (end of day), "gtc"
        (until cancelled) or "gtd" (until ``expiry_date``, "YYYY-MM-DD").
        """
        limit = _positive(limit, "limit")
        return await self._order(isin, side, size, exchange, expiry, expiry_date, mode="limit", limit=limit)

    async def market_order(self, isin, side, size, exchange="LSX", sell_fractions=False):
        """Places a market order and returns the answer. Check its "status"."""
        return await self._order(isin, side, size, exchange, "gfd", None, mode="market", sellFractions=sell_fractions)

    async def cancel_order(self, order_id):
        return await self.request("cancelOrder", orderId=order_id)

    async def _order(self, isin, side, size, exchange, expiry, expiry_date, **parameters):
        if not ISIN.fullmatch(isin):
            raise ValueError(f"not an ISIN: {isin!r}")
        if side not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")
        if expiry not in ("gfd", "gtc", "gtd"):
            raise ValueError("expiry must be 'gfd', 'gtc' or 'gtd'")
        if (expiry == "gtd") != bool(expiry_date):
            raise ValueError("expiry_date goes with expiry 'gtd', and only with it")
        expires = {"type": expiry}
        if expiry_date:
            expires["value"] = date.fromisoformat(str(expiry_date)).isoformat()
        return await self.request(
            "simpleCreateOrder",
            clientProcessId=str(uuid.uuid4()),
            # Trade Republic refuses orders without these two.
            warningsShown=["userExperience"],
            acceptedWarnings=["userExperience"],
            parameters={
                "instrumentId": isin,
                "exchangeId": exchange,
                "type": side,
                "size": _positive(size, "size"),
                "expiry": expires,
                **parameters,
            },
        )


class Blocking:
    """Synchronous access: every coroutine method of the client becomes a plain call.

        with Blocking(TradeRepublic(session_file="~/.trapi/session.txt")) as tr:
            tr.resume() or tr.login(phone, pin)
            print(tr.request("cash"))

    ``stream`` stays async.
    """

    def __init__(self, client=None):
        self.client = client or TradeRepublic()
        self._loop = asyncio.new_event_loop()

    def __getattr__(self, name):
        attr = getattr(self.client, name)
        if inspect.iscoroutinefunction(attr):
            return lambda *args, **kwargs: self._loop.run_until_complete(attr(*args, **kwargs))
        return attr

    def close(self):
        self._loop.run_until_complete(self.client.close())
        self._loop.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _answer(response):
    """The JSON of a REST answer, or an error naming Trade Republic's error codes."""
    try:
        data = response.json()
    except ValueError:
        data = {}
    if response.status_code >= 400:
        errors = data.get("errors", []) if isinstance(data, dict) else []
        codes = ", ".join(str(e.get("errorCode")) for e in errors if isinstance(e, dict))
        raise TradeRepublicError(f"HTTP {response.status_code} {codes}".strip())
    return data


def _positive(value, name):
    # Numbers have to go out as numbers, a string makes Trade Republic refuse the order.
    number = float(value)
    if not number > 0:
        raise ValueError(f"{name} must be a positive number, got {value!r}")
    return number
