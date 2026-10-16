"""Domain <-> FIX enum, quantity and price conversions."""

from decimal import Decimal

from app.domain.enums import OrderType, Side

_SIDE_TO_FIX = {Side.BUY: "1", Side.SELL: "2"}
_FIX_TO_SIDE = {value: side for side, value in _SIDE_TO_FIX.items()}

_ORDER_TYPE_TO_FIX = {
    OrderType.MARKET: "1",
    OrderType.LIMIT: "2",
    OrderType.STOP: "3",
    OrderType.STOP_LIMIT: "4",
}
_FIX_TO_ORDER_TYPE = {value: order_type for order_type, value in _ORDER_TYPE_TO_FIX.items()}


def side_to_fix(side: Side) -> str:
    """Map a domain Side to FIX tag 54."""
    return _SIDE_TO_FIX[side]


def side_from_fix(value: str) -> Side:
    """Map FIX tag 54 to a domain Side."""
    return _FIX_TO_SIDE[value]


def order_type_to_fix(order_type: OrderType) -> str:
    """Map a domain OrderType to FIX tag 40."""
    return _ORDER_TYPE_TO_FIX[order_type]


def order_type_from_fix(value: str) -> OrderType:
    """Map FIX tag 40 to a domain OrderType."""
    return _FIX_TO_ORDER_TYPE[value]


def format_quantity(quantity: Decimal) -> str:
    """Format a quantity for a FIX field: plain decimal digits."""
    return format(quantity, "f")


def format_price(price: Decimal) -> str:
    """Format a price for a FIX field: plain decimal digits."""
    return format(price, "f")
