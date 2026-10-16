"""Maps 35=y SecurityList -> SecurityListReceived.

Parses the repeating group (tag 55, one entry per symbol, paired with its name) rather
than reading scalars, since cTrader sends them as parallel lists once there's
more than one symbol.
"""

from ctrader_fix import ResponseMessage

from app.adapters.brokers.ctrader.fields import as_list, first
from app.domain.events import DiscoveredSymbol, SecurityListReceived


def map_security_list(message: ResponseMessage) -> SecurityListReceived:
    """Map a 35=y ResponseMessage to a SecurityListReceived event."""
    broker_symbol_ids = as_list(message.getFieldValue(55))
    names = as_list(message.getFieldValue(1007)) or as_list(message.getFieldValue(48))
    symbols = tuple(
        DiscoveredSymbol(broker_symbol_id=int(sid), name=name)
        for sid, name in zip(broker_symbol_ids, names, strict=True)
    )
    return SecurityListReceived(symbols=symbols, received_at=first(message.getFieldValue(52)) or "")
