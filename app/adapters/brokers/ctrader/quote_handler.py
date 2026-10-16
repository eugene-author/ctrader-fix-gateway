"""Maps 35=W/X (MarketDataSnapshot/Incremental) -> quote entries.

Parses the repeating group by entry type (tag 269) paired with price (tag
270) rather than trusting list position.
"""

from dataclasses import dataclass
from decimal import Decimal

from ctrader_fix import ResponseMessage

from app.adapters.brokers.ctrader.fields import as_list, first

BID = "0"
OFFER = "1"


@dataclass(frozen=True, slots=True)
class QuoteEntry:
    """One (MDEntryType, MDEntryPx) pair from the repeating group."""

    entry_type: str
    price: Decimal


@dataclass(frozen=True, slots=True)
class MarketDataUpdate:
    """A mapped 35=W/X message: which symbol, and its entries."""

    broker_symbol_id: int
    entries: tuple[QuoteEntry, ...]


def map_market_data(message: ResponseMessage) -> MarketDataUpdate:
    """Map a 35=W (snapshot) or 35=X (incremental) ResponseMessage."""
    broker_symbol_id = int(first(message.getFieldValue(55)) or 0)
    entry_types = as_list(message.getFieldValue(269))
    prices = as_list(message.getFieldValue(270))
    entries = tuple(
        QuoteEntry(entry_type=entry_type, price=Decimal(price))
        for entry_type, price in zip(entry_types, prices, strict=True)
    )
    return MarketDataUpdate(broker_symbol_id=broker_symbol_id, entries=entries)
