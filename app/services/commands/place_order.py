"""PlaceOrderCommand and its handler: the write side of POST /orders."""

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from app.core.metrics import orders_received_total
from app.domain.commands import CreateOrderCommand
from app.domain.enums import OrderPurpose, OrderStatus, OrderType, Side, TimeInForce
from app.domain.errors import (
    DuplicateKeyError,
    IdempotencyKeyReusedError,
    PermanentError,
    SymbolsNotReadyError,
    ValidationError,
)
from app.domain.models import Order, Price, Quantity, Symbol
from app.domain.validation import is_multiple_of_step
from app.ports.unit_of_work import UnitOfWork

_CLIENT_ORDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_UNSUPPORTED_ORDER_TYPES = frozenset({OrderType.STOP, OrderType.STOP_LIMIT})


@dataclass(frozen=True, slots=True)
class PlaceOrderCommand:
    """The write side input for placing an order."""

    account_id: UUID
    client_order_id: str
    symbol: str
    side: Side
    order_type: OrderType
    request_fingerprint: str
    quantity: Decimal | None = None
    volume_lots: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    idempotency_key: str | None = None
    correlation_id: str = ""


@dataclass(frozen=True, slots=True)
class PlaceOrderResult:
    """What the API returns for a placed (or replayed) order."""

    order_id: UUID
    client_order_id: str
    status: OrderStatus
    created_at: datetime
    idempotency_replayed: bool


class PlaceOrderHandler:
    """Validates, then persists PENDING + the outbox event in one transaction."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        """Store the factory. A fresh UnitOfWork is opened per handle() call."""
        self._uow_factory = uow_factory

    async def handle(self, command: PlaceOrderCommand) -> PlaceOrderResult:
        """Place an order, or replay the original response for a repeated request."""
        _validate_client_order_id(command.client_order_id)

        async with self._uow_factory() as uow:
            existing = await uow.orders.get_by_client_order_id(command.account_id, command.client_order_id)
            if existing is not None:
                return _replay(existing, command)

            fetched_symbol = await uow.symbols.get_by_name(command.account_id, command.symbol)
            symbol = await _validate_symbol(uow, command.account_id, fetched_symbol, command.symbol)
            _validate_order_type(command.order_type)
            quantity = _resolve_quantity(command, symbol)
            _validate_quantity(quantity, symbol)
            _validate_prices(command, symbol)

            now = datetime.now(UTC)
            order_id = uuid4()
            correlation_id = command.correlation_id or str(order_id)
            order = _build_order(command, symbol, order_id, now, quantity, correlation_id)
            create_order_command = _build_create_order_command(command, symbol, order_id, order)

            try:
                await uow.orders.add(order)
                await uow.outbox.add(
                    account_id=command.account_id,
                    aggregate_type="order",
                    aggregate_id=order_id,
                    event_type="CreateOrderCommand",
                    payload=create_order_command.to_payload(),
                    routing_key="order.create",
                    correlation_id=correlation_id,
                    causation_id=correlation_id,
                )
                await uow.commit()
            except DuplicateKeyError:
                raise IdempotencyKeyReusedError(command.client_order_id) from None

        orders_received_total.labels(symbol=command.symbol).inc()
        return PlaceOrderResult(
            order_id=order_id,
            client_order_id=command.client_order_id,
            status=OrderStatus.PENDING,
            created_at=now,
            idempotency_replayed=False,
        )


def _validate_client_order_id(client_order_id: str) -> None:
    if not _CLIENT_ORDER_ID_PATTERN.fullmatch(client_order_id):
        raise ValidationError("client_order_id must match ^[A-Za-z0-9_.:-]{1,64}$")


async def _validate_symbol(uow: UnitOfWork, account_id: UUID, symbol: Symbol | None, name: str) -> Symbol:
    if symbol is None:
        if not await uow.symbols.has_any(account_id):
            raise SymbolsNotReadyError
        raise ValidationError(f"Unknown symbol: {name}", code="SYMBOL_UNKNOWN")
    if not symbol.enabled:
        raise ValidationError(f"Symbol is disabled: {name}", code="SYMBOL_DISABLED")
    if symbol.needs_review:
        raise ValidationError(f"Symbol is not fully configured: {name}", code="SYMBOL_UNCONFIGURED")
    if not symbol.subscribe:
        raise ValidationError(f"Symbol is not subscribed for this account: {name}", code="SYMBOL_DISABLED")
    if symbol.broker_symbol_id is None:
        raise SymbolsNotReadyError
    return symbol


def _resolve_quantity(command: PlaceOrderCommand, symbol: Symbol) -> Decimal:
    """Resolve `quantity`/`volume_lots` (exactly one must be supplied)."""
    if command.quantity is not None and command.volume_lots is not None:
        raise ValidationError("exactly one of quantity or volume_lots must be supplied")
    if command.quantity is not None:
        return command.quantity
    if command.volume_lots is None:
        raise ValidationError("exactly one of quantity or volume_lots must be supplied")
    if symbol.lot_size is None:
        raise ValidationError(f"lot_size is not configured for {symbol.name}", code="SYMBOL_UNCONFIGURED")
    return command.volume_lots * symbol.lot_size.value


def _validate_order_type(order_type: OrderType) -> None:
    if order_type in _UNSUPPORTED_ORDER_TYPES:
        raise PermanentError(
            f"{order_type.value} orders are not yet supported",
            code="ORDER_TYPE_UNSUPPORTED",
            status_code=501,
        )


def _validate_quantity(quantity: Decimal, symbol: Symbol) -> None:
    if quantity <= 0:
        raise ValidationError("quantity must be positive", code="QUANTITY_OUT_OF_RANGE")
    if symbol.min_qty is not None and quantity < symbol.min_qty.value:
        raise ValidationError(f"quantity below minimum {symbol.min_qty.value}", code="QUANTITY_OUT_OF_RANGE")
    if symbol.max_qty is not None and quantity > symbol.max_qty.value:
        raise ValidationError(f"quantity above maximum {symbol.max_qty.value}", code="QUANTITY_OUT_OF_RANGE")
    if symbol.qty_step is not None and not is_multiple_of_step(quantity, symbol.qty_step.value):
        raise ValidationError(f"quantity must be a multiple of {symbol.qty_step.value}", code="QUANTITY_STEP_INVALID")


def _validate_prices(command: PlaceOrderCommand, symbol: Symbol) -> None:
    if command.order_type == OrderType.MARKET:
        if command.limit_price is not None:
            raise ValidationError("limit_price is not allowed for a MARKET order", code="PRICE_NOT_ALLOWED")
        if command.stop_price is not None:
            raise ValidationError("stop_price is not allowed for a MARKET order", code="PRICE_NOT_ALLOWED")
    elif command.order_type == OrderType.LIMIT:
        if command.limit_price is None:
            raise ValidationError("limit_price is required for a LIMIT order", code="PRICE_REQUIRED")
        if command.stop_price is not None:
            raise ValidationError("stop_price is not allowed for a LIMIT order", code="PRICE_NOT_ALLOWED")

    if symbol.price_step is None:
        return
    step = symbol.price_step.value
    for label, value in (
        ("limit_price", command.limit_price),
        ("stop_price", command.stop_price),
        ("stop_loss", command.stop_loss),
        ("take_profit", command.take_profit),
    ):
        if value is not None and not is_multiple_of_step(value, step):
            raise ValidationError(f"{label} must be a multiple of {step}", code="PRICE_STEP_INVALID")


def _build_order(
    command: PlaceOrderCommand, symbol: Symbol, order_id: UUID, now: datetime, quantity: Decimal, correlation_id: str
) -> Order:
    return Order(
        id=order_id,
        account_id=command.account_id,
        client_order_id=command.client_order_id,
        symbol_id=symbol.id,
        side=command.side,
        order_type=command.order_type,
        quantity=Quantity(quantity),
        status=OrderStatus.PENDING,
        purpose=OrderPurpose.OPEN,
        created_at=now,
        updated_at=now,
        time_in_force=command.time_in_force,
        limit_price=Price(command.limit_price) if command.limit_price is not None else None,
        stop_price=Price(command.stop_price) if command.stop_price is not None else None,
        stop_loss=Price(command.stop_loss) if command.stop_loss is not None else None,
        take_profit=Price(command.take_profit) if command.take_profit is not None else None,
        idempotency_key=command.idempotency_key,
        request_fingerprint=command.request_fingerprint,
        correlation_id=correlation_id,
    )


def _require_broker_symbol_id(symbol: Symbol) -> int:
    if symbol.broker_symbol_id is None:
        raise SymbolsNotReadyError
    return symbol.broker_symbol_id


def _build_create_order_command(
    command: PlaceOrderCommand, symbol: Symbol, order_id: UUID, order: Order
) -> CreateOrderCommand:
    """Build the create-order command."""
    fix_cl_ord_id = order_id.hex[:30]
    return CreateOrderCommand(
        order_id=order_id,
        client_order_id=command.client_order_id,
        fix_cl_ord_id=fix_cl_ord_id,
        symbol=command.symbol,
        broker_symbol_id=_require_broker_symbol_id(symbol),
        side=command.side,
        order_type=command.order_type,
        quantity=order.quantity,
        time_in_force=command.time_in_force,
        limit_price=order.limit_price,
        stop_price=order.stop_price,
    )


def _replay(existing: Order, command: PlaceOrderCommand) -> PlaceOrderResult:
    """Replay the order."""
    if existing.request_fingerprint != command.request_fingerprint:
        raise IdempotencyKeyReusedError(command.client_order_id)
    if (
        existing.idempotency_key is not None
        and command.idempotency_key is not None
        and existing.idempotency_key != command.idempotency_key
    ):
        raise IdempotencyKeyReusedError(command.client_order_id)
    return PlaceOrderResult(
        order_id=existing.id,
        client_order_id=existing.client_order_id,
        status=existing.status,
        created_at=existing.created_at,
        idempotency_replayed=True,
    )
