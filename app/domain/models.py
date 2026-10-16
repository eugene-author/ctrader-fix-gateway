"""Domain models and value objects."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from app.domain.enums import (
    OrderPurpose,
    OrderStatus,
    OrderType,
    PositionStatus,
    ProtectionState,
    ProtectiveKind,
    Side,
    SymbolSource,
    TimeInForce,
)


@dataclass(frozen=True, slots=True)
class Quantity:
    """A non-negative amount. Value is Decimal."""

    value: Decimal

    def __post_init__(self) -> None:
        """Reject a float value and a negative amount."""
        if not isinstance(self.value, Decimal):
            raise TypeError("Quantity must be constructed from a Decimal")
        if self.value < 0:
            raise ValueError("Quantity must not be negative")


@dataclass(frozen=True, slots=True)
class Price:
    """A positive price level. Value is Decimal."""

    value: Decimal

    def __post_init__(self) -> None:
        """Reject a float value and a non-positive price."""
        if not isinstance(self.value, Decimal):
            raise TypeError("Price must be constructed from a Decimal")
        if self.value <= 0:
            raise ValueError("Price must be positive")


@dataclass(slots=True)
class Symbol:
    """A tradable symbol and its trading constraints."""

    id: UUID
    account_id: UUID
    name: str
    enabled: bool
    subscribe: bool
    needs_review: bool
    broker_symbol_id: int | None = None
    digits: int | None = None
    lot_size: Quantity | None = None
    qty_step: Quantity | None = None
    min_qty: Quantity | None = None
    max_qty: Quantity | None = None
    price_step: Price | None = None
    source: SymbolSource | None = None
    last_seen_at: datetime | None = None


@dataclass(slots=True)
class Order:
    """Fields specific to a protective order.

    `protective_kind`/`protects_position_id`/`parent_order_id`/`oco_group_id`
    are set only on a protective (SL/TP) order.
    """

    id: UUID
    account_id: UUID
    client_order_id: str
    symbol_id: UUID
    side: Side
    order_type: OrderType
    quantity: Quantity
    status: OrderStatus
    purpose: OrderPurpose
    created_at: datetime
    updated_at: datetime
    time_in_force: TimeInForce = TimeInForce.GTC
    fix_cl_ord_id: str | None = None
    orig_cl_ord_id: str | None = None
    ctrader_order_id: str | None = None
    position_id: str | None = None
    filled_quantity: Quantity = field(default_factory=lambda: Quantity(Decimal(0)))
    avg_fill_price: Price | None = None
    limit_price: Price | None = None
    stop_price: Price | None = None
    stop_loss: Price | None = None
    take_profit: Price | None = None
    reject_reason_code: str | None = None
    error_message: str | None = None
    closes_position_id: str | None = None
    protective_kind: ProtectiveKind | None = None
    protects_position_id: str | None = None
    parent_order_id: UUID | None = None
    oco_group_id: UUID | None = None
    idempotency_key: str | None = None
    request_fingerprint: str | None = None
    correlation_id: str | None = None
    version: int = 1
    submitted_at: datetime | None = None
    terminal_at: datetime | None = None
    last_broker_transact_at: datetime | None = None


@dataclass(slots=True)
class Position:
    """A projection."""

    id: UUID
    account_id: UUID
    position_id: str
    symbol_id: UUID
    side: Side
    quantity: Quantity
    entry_price: Price
    status: PositionStatus
    opened_at: datetime
    opened_by_order_id: UUID | None = None
    closed_by_order_id: UUID | None = None
    closed_at: datetime | None = None
    protection_state: ProtectionState = ProtectionState.NONE
    stop_loss_order_id: UUID | None = None
    take_profit_order_id: UUID | None = None
    version: int = 1


@dataclass(slots=True)
class Execution:
    """An immutable log entry, one row per ExecutionReport."""

    id: UUID
    account_id: UUID
    exec_id: str
    exec_type: str
    ord_status: str
    cum_qty: Quantity
    leaves_qty: Quantity
    transact_time: datetime
    raw_message: str
    received_at: datetime
    order_id: UUID | None = None
    last_qty: Quantity | None = None
    last_price: Price | None = None
    position_id: str | None = None
    text: str | None = None


@dataclass(frozen=True, slots=True)
class Quote:
    """A cached bid/ask snapshot for one symbol."""

    symbol: str
    bid: Price
    ask: Price
    timestamp: datetime
