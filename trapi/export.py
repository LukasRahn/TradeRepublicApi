"""Export of the timeline: raw events, a CSV for Portfolio Performance, the documents."""

import asyncio
import csv
import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import requests

from trapi.api import HOST, ISIN, TradeRepublicError

# eventType -> meaning for the CSV. "trade" becomes a buy or a sell by the sign
# of the amount, "corporate_cash" a dividend or taxes. The names come from
# real accounts.
EVENT_TYPES = {
    **dict.fromkeys(
        [
            "ORDER_EXECUTED",
            "TRADING_TRADE_EXECUTED",
            "SAVINGS_PLAN_EXECUTED",
            "SAVINGS_PLAN_INVOICE_CREATED",
            "TRADING_SAVINGSPLAN_EXECUTED",
            "TRADE_INVOICE",
            "TRADE_CORRECTED",
            "IPO_TRADE_EXECUTED",
            "SPARE_CHANGE_AGGREGATE",
            "BENEFITS_SPARE_CHANGE_EXECUTION",
        ],
        "trade",
    ),
    **dict.fromkeys(
        [
            "ACCOUNT_TRANSFER_INCOMING",
            "BANK_TRANSACTION_INCOMING",
            "CARD_REFUND",
            "CARD_SUCCESSFUL_OCT",
            "CARD_TR_REFUND",
            "INCOMING_TRANSFER",
            "INCOMING_TRANSFER_DELEGATION",
            "PAYMENT_INBOUND",
            "PAYMENT_INBOUND_APPLE_PAY",
            "PAYMENT_INBOUND_CREDIT_CARD",
            "PAYMENT_INBOUND_GOOGLE_PAY",
            "PAYMENT_INBOUND_SEPA_DIRECT_DEBIT",
        ],
        "deposit",
    ),
    **dict.fromkeys(
        [
            "BANK_TRANSACTION_OUTGOING",
            "CARD_FAILED_TRANSACTION",
            "CARD_ORDER_BILLED",
            "CARD_SUCCESSFUL_ATM_WITHDRAWAL",
            "CARD_SUCCESSFUL_TRANSACTION",
            "CARD_TRANSACTION",
            "JUNIOR_P2P_TRANSFER",
            "OUTGOING_TRANSFER",
            "OUTGOING_TRANSFER_DELEGATION",
            "PAYMENT_OUTBOUND",
        ],
        "removal",
    ),
    **dict.fromkeys(["INTEREST_PAYOUT", "INTEREST_PAYOUT_CREATED"], "interest"),
    "CREDIT": "dividend",
    # A payout, taxes, or a redemption ("Tilgung", e.g. a knocked-out turbo) which ends the position.
    **dict.fromkeys(
        ["SSP_CORPORATE_ACTION_CASH", "SSP_CORPORATE_ACTION_INVOICE_CASH", "SSP_CORPORATE_ACTION_CASH_NON_DIVIDEND"],
        "corporate_cash",
    ),
    **dict.fromkeys(["SSP_TAX_CORRECTION", "SSP_TAX_CORRECTION_INVOICE", "TAX_CORRECTION", "TAX_REFUND"], "tax_refund"),
    # A bonus or free shares: a deposit from Trade Republic plus a buy. STOCK_PERK_REFUNDED
    # ("Gratisaktien eingelöst") delivers the shares.
    **dict.fromkeys(
        ["ACQUISITION_TRADE_PERK", "BENEFITS_SAVEBACK_EXECUTION", "SAVEBACK_AGGREGATE", "STOCK_PERK_REFUNDED"],
        "saveback",
    ),
}
# Events without an eventType (old ones, and some new ones too) are recognised
# by their title, then their subtitle, then the headline of their detail.
TITLES = {"Einzahlung": "deposit", "Zinsen": "interest", "Steuerkorrektur": "tax_refund", "Aktien-Bonus": "saveback"}
SUBTITLES = {
    **dict.fromkeys(
        [
            "Kauforder",
            "Verkaufsorder",
            "Limit-Buy-Order",
            "Limit-Sell-Order",
            "Limit Verkauf-Order neu abgerechnet",
            "Stop-Sell-Order",
            "Sparplan ausgeführt",
            "Round up",
            "Tilgung",
        ],
        "trade",
    ),
    **dict.fromkeys(
        [
            "Bardividende",
            "Bardividende korrigiert",
            "Dividende",
            "Dividende Wahlweise",
            "Aktienprämiendividende",
        ],
        "dividend",
    ),
    "Saveback": "saveback",
    "Vorabpauschale": "taxes",
}
# ponytail: splits, spin-offs, swaps, securities transfers and private markets
# are not mapped, they are counted and reported instead. Map them here when
# you need them in Portfolio Performance.

# Transaction types as Portfolio Performance's German CSV import names them.
KINDS = {
    "buy": "Kauf",
    "sell": "Verkauf",
    "deposit": "Einlage",
    "removal": "Entnahme",
    "dividend": "Dividende",
    "interest": "Zinsen",
    "taxes": "Steuern",
    "tax_refund": "Steuerrückerstattung",
}
COLUMNS = ["Datum", "Typ", "Wert", "Notiz", "ISIN", "Stück", "Gebühren", "Steuern"]

SHARES = ("Aktien", "Anteile", "Shares", "Aktien entfernt")
FEES = ("Gebühr", "Gebühren", "Fremdkostenzuschlag", "Fee", "Fees")
TAXES = ("Steuer", "Steuern", "Tax", "Taxes")

_NUMBER = re.compile(r"-?\d[\d.,]*")
_TIMES = re.compile(r"\s[x×]\s")
_LOGO_ISIN = re.compile(rf"logos/({ISIN.pattern})/")
_UNSAFE = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')


async def export(tr, folder, documents=True, log=print):
    """Writes events.json, transactions.csv and the documents into ``folder``."""
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)

    events = {}
    for topic in ("timelineTransactions", "timelineActivityLog"):
        for event in await tr.timeline(topic):
            events[event["id"]] = event
    log(f"{len(events)} events, fetching their details...")

    limit = asyncio.Semaphore(16)

    async def fetch(event):
        async with limit:
            try:
                event["details"] = await tr.timeline_detail(event["id"])
            except (TradeRepublicError, TimeoutError) as e:
                log(f"no details for {event['id']} ({event.get('title')}): {e}")

    await asyncio.gather(*(fetch(e) for e in events.values() if _has_details(e)))
    ordered = sorted(events.values(), key=lambda e: e.get("timestamp", ""))

    with open(folder / "events.json", "w", encoding="utf-8") as f:
        json.dump(ordered, f, ensure_ascii=False, indent=2)

    rows, unknown = write_csv(ordered, folder / "transactions.csv")
    log(f"{rows} rows in {folder / 'transactions.csv'}")
    for event_type, count in unknown.most_common():
        log(f"  not in the CSV: {count} x {event_type}")

    if documents:
        fetched = await asyncio.to_thread(download_documents, tr, ordered, folder / "documents", log)
        log(f"{fetched} new documents in {folder / 'documents'}")


def write_csv(events, path):
    """Writes the Portfolio Performance CSV. Returns the row count and the
    kinds of events that moved money but are not mapped."""
    count, unknown = 0, Counter()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, COLUMNS, delimiter=";")
        writer.writeheader()
        for event in events:
            rows = transactions(event)
            if rows is None:
                if (event.get("amount") or {}).get("value") and "CANCEL" not in (event.get("status") or "").upper():
                    unknown[event.get("eventType") or f"{event.get('title')} / {event.get('subtitle')}"] += 1
                continue
            writer.writerows(rows)
            count += len(rows)
    return count, unknown


def transactions(event):
    """The CSV rows for one timeline event.

    Returns None for events that are not mapped, an empty list for cancelled ones.
    """
    details = event.get("details") or {}
    header = next((s for s in details.get("sections") or [] if s.get("type") == "header"), {})
    kind = _kind(event, header)
    if kind is None:
        return None
    header_data = header.get("data") if isinstance(header.get("data"), dict) else {}
    if "CANCEL" in (event.get("status") or "").upper() or str(header_data.get("status")).lower() == "canceled":
        return []

    rows = _rows(details)
    value = (event.get("amount") or {}).get("value")
    if value is None and kind == "saveback":
        # A share bonus carries no amount, only its total.
        total = parse_number(_first(rows, ("Gesamt",)))
        value = -total if total is not None else None
    if kind == "trade":
        kind = "buy" if (value or 0) < 0 else "sell"
    elif kind == "corporate_cash" and event.get("subtitle") == "Tilgung":
        kind = "sell"  # the shares are gone, even when nothing is paid out
    elif kind == "corporate_cash":
        kind = "taxes" if (value or 0) < 0 else "dividend"

    row = {
        "Datum": _local_time(event["timestamp"]),
        "Typ": KINDS.get(kind, "Kauf"),
        "Wert": value,
        "Notiz": event.get("title"),
        "ISIN": _isin(event, details),
        "Stück": _shares(rows),
        "Gebühren": _amount(_first(rows, FEES)),
        "Steuern": _amount(_first(rows, TAXES)),
    }
    if kind == "saveback":
        deposit = dict.fromkeys(COLUMNS) | {"Datum": row["Datum"], "Typ": KINDS["deposit"], "Notiz": row["Notiz"]}
        deposit["Wert"] = -value if value is not None else None
        return [row, deposit]
    return [row]


def parse_number(text):
    """Reads a number the way Trade Republic renders it.

    It mixes German and English formatting, even within one answer:
    "1,00 €", "€11.14", "0,685102", "10.640298", "9.400" (nine thousand four
    hundred shares).
    """
    match = _NUMBER.search(text or "")
    if not match:
        return None
    number = match.group().rstrip(".,")
    dots, commas = number.count("."), number.count(",")
    if dots and commas:
        decimal = "." if number.rfind(".") > number.rfind(",") else ","
    elif commas:
        decimal = "," if commas == 1 else None
    elif dots == 1:
        whole, fraction = number.split(".")
        # ponytail: a single dot before three digits is read as German grouping,
        # so English "1.500" shares would come out as 1500. Not seen in real data.
        decimal = None if len(fraction) == 3 and whole.lstrip("-") != "0" else "."
    else:
        decimal = None
    if decimal is None:
        return Decimal(number.replace(".", "").replace(",", ""))
    grouping = "," if decimal == "." else "."
    return Decimal(number.replace(grouping, "").replace(decimal, "."))


def download_documents(tr, events, folder, log=print):
    """Downloads the documents of the events that are not on disk yet. Returns how many."""
    fetched = 0
    for event in events:
        for section in (event.get("details") or {}).get("sections") or []:
            if section.get("type") != "documents":
                continue
            for doc in section.get("data") or []:
                payload = (doc.get("action") or {}).get("payload")
                if isinstance(payload, dict) and payload.get("path"):
                    # Served by the API itself and only with the session cookies.
                    tr.keep_alive()
                    url, session = f"{HOST}/{payload['path'].lstrip('/')}", tr.http
                elif isinstance(payload, str) and payload.startswith("https://"):
                    # A signed storage link, it gets no cookies.
                    url, session = payload, requests
                else:
                    continue
                path = folder / event["timestamp"][:4] / _filename(event, doc)
                if path.exists():
                    continue
                r = session.get(url, timeout=60)
                if r.status_code != 200 or not r.content.startswith(b"%PDF"):
                    log(f"could not download {path.name} (HTTP {r.status_code})")
                    continue
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(r.content)
                fetched += 1
    return fetched


def _kind(event, header):
    event_type = (event.get("eventType") or "").upper()
    if event_type not in ("", "TIMELINE_LEGACY_MIGRATED_EVENTS"):
        if event.get("subtitle") == "Aufruf von Zwischenpapieren":
            return None  # a swap of securities, not a payout
        return EVENT_TYPES.get(event_type)
    if event.get("title") == "Private Equity":
        return None  # private markets, not mapped
    kind = TITLES.get(event.get("title")) or SUBTITLES.get(event.get("subtitle"))
    headline = header.get("title") or ""
    if kind is None and (event.get("amount") or {}).get("value") and headline.startswith("Du hast"):
        if headline.endswith("erhalten") and "Angebot" not in headline:
            kind = "deposit"
        elif headline.endswith(("gesendet", "ausgegeben")):
            kind = "removal"
    return kind


def _has_details(event):
    action = event.get("action") or {}
    return action.get("type") == "timelineDetail" and action.get("payload") == event["id"]


def _rows(node):
    """Every labelled table row of a detail as (label, text, prefix), including
    the rows of screens nested in it (a trade keeps its share count in one)."""
    found = []
    for section in (node or {}).get("sections") or []:
        if section.get("type") != "table" or not isinstance(section.get("data"), list):
            continue
        for row in section["data"]:
            detail = row.get("detail") if isinstance(row.get("detail"), dict) else {}
            display = detail.get("displayValue")
            found.append(
                (row.get("title"), detail.get("text"), display.get("prefix") if isinstance(display, dict) else None)
            )
            payload = (detail.get("action") or {}).get("payload")
            if isinstance(payload, dict):
                found += _rows(payload)
    return found


def _first(rows, labels):
    return next((text for label, text, _ in rows if label in labels and text), None)


def _shares(rows):
    shares = parse_number(_first(rows, SHARES))
    if shares is not None:
        return shares
    for label, text, prefix in rows:
        if label == "Transaktion" and (prefix or _TIMES.search(text or "")):
            return parse_number(prefix or text)  # "0.546348 x " or "2 × 37,30 €"
    # A bond: the transaction is money, the quotation a percentage of the nominal value.
    amount, quotation = parse_number(_first(rows, ("Transaktion",))), parse_number(_first(rows, ("Quotation",)))
    if amount and quotation:
        return (amount / quotation * 100).quantize(Decimal("0.01"))
    return None


def _amount(text):
    number = parse_number(text)
    return abs(number) if number else None


def _isin(event, details):
    sections = details.get("sections") or []
    for section in sections:
        action = section.get("action") or {}
        if action.get("type") == "instrumentDetail" and ISIN.fullmatch(str(action.get("payload"))):
            return action["payload"]
    icons = [json.dumps(s.get("data")) for s in sections if s.get("type") == "header"] + [event.get("icon")]
    for icon in icons:
        match = _LOGO_ISIN.search(str(icon))
        if match:
            return match.group(1)
    return None


def _local_time(timestamp):
    return datetime.fromisoformat(timestamp).astimezone().replace(tzinfo=None).isoformat(timespec="seconds")


def _filename(event, doc):
    doc_id = str(doc.get("id", ""))[:8]
    name = f"{event['timestamp'][:10]} {event.get('title', '')} - {doc.get('title', '')} ({doc_id}).pdf"
    return _UNSAFE.sub("_", name)
