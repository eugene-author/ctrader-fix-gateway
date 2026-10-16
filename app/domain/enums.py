"""Domain enumerations."""

import enum


class Side(enum.StrEnum):
    """Which side of the market an order or position is on."""

    BUY = "BUY"
    SELL = "SELL"


class OrderType(enum.StrEnum):
    """cTrader order types."""

    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class TimeInForce(enum.StrEnum):
    """Time-in-force qualifiers."""

    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"


class OrderStatus(enum.StrEnum):
    """Order lifecycle states. See app.domain.state_machine for legal transitions."""

    PENDING = "PENDING"
    DISPATCHING = "DISPATCHING"
    DISPATCH_UNCERTAIN = "DISPATCH_UNCERTAIN"
    SUBMITTED = "SUBMITTED"
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


class OrderPurpose(enum.StrEnum):
    """Whether an order opens a new position or closes an existing one."""

    OPEN = "OPEN"
    CLOSE = "CLOSE"


class PositionStatus(enum.StrEnum):
    """Position lifecycle states."""

    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class ProtectiveKind(enum.StrEnum):
    """Which half of a protective bracket this order is."""

    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"


class ProtectionState(enum.StrEnum):
    """A position's protective-order coverage."""

    NONE = "NONE"
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class SymbolSource(enum.StrEnum):
    """Where a symbol's trading constraints came from."""

    DISCOVERY = "discovery"
    OVERLAY = "overlay"
