"""GetPositionQuery and ListPositionsQuery: the read side of GET /positions[/{id}]."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from app.domain.enums import OrderStatus, PositionStatus, ProtectionState, Side
from app.domain.errors import PositionNotFoundError
from app.domain.models import Position
from app.ports.unit_of_work import UnitOfWork


@dataclass(frozen=True, slots=True)
class ProtectiveOrderView:
    """One half (SL or TP) of a bracket's current state, for the `protection` block."""

    price: Decimal
    order_id: UUID
    status: OrderStatus


@dataclass(frozen=True, slots=True)
class PositionProtectionView:
    """`GET /positions/{id}`'s `protection` block, so a client can verify its stop is actually live."""

    state: ProtectionState
    stop_loss: ProtectiveOrderView | None
    take_profit: ProtectiveOrderView | None


@dataclass(frozen=True, slots=True)
class PositionView:
    """One position resource."""

    position_id: str
    symbol: str
    side: Side
    quantity: Decimal
    entry_price: Decimal
    status: PositionStatus
    opened_at: datetime
    closed_at: datetime | None
    protection: PositionProtectionView


async def _resolve_symbol_name(uow: UnitOfWork, symbol_id: UUID) -> str:
    symbol = await uow.symbols.get_by_id(symbol_id)
    return symbol.name if symbol is not None else "UNKNOWN"


async def _resolve_protective_order(uow: UnitOfWork, order_id: UUID | None) -> ProtectiveOrderView | None:
    if order_id is None:
        return None
    order = await uow.orders.get(order_id)
    if order is None:
        return None
    price = order.stop_price if order.stop_price is not None else order.limit_price
    if price is None:
        return None
    return ProtectiveOrderView(price=price.value, order_id=order.id, status=order.status)


async def _view(uow: UnitOfWork, position: Position, symbol_name: str) -> PositionView:
    protection = PositionProtectionView(
        state=position.protection_state,
        stop_loss=await _resolve_protective_order(uow, position.stop_loss_order_id),
        take_profit=await _resolve_protective_order(uow, position.take_profit_order_id),
    )
    return PositionView(
        position_id=position.position_id,
        symbol=symbol_name,
        side=position.side,
        quantity=position.quantity.value,
        entry_price=position.entry_price.value,
        status=position.status,
        opened_at=position.opened_at,
        closed_at=position.closed_at,
        protection=protection,
    )


class GetPositionQuery:
    """Fetch one position resource."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        """Store the factory."""
        self._uow_factory = uow_factory

    async def handle(self, account_id: UUID, position_id: str) -> PositionView:
        """Return the position, or raise PositionNotFoundError if it doesn't belong to this account."""
        async with self._uow_factory() as uow:
            position = await uow.positions.get_by_broker_position_id(account_id, position_id)
            if position is None:
                raise PositionNotFoundError(position_id)
            symbol_name = await _resolve_symbol_name(uow, position.symbol_id)
            return await _view(uow, position, symbol_name)


class ListPositionsQuery:
    """List open positions for the requesting account."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        """Store the factory."""
        self._uow_factory = uow_factory

    async def handle(self, account_id: UUID) -> list[PositionView]:
        """Return every OPEN position for this account."""
        async with self._uow_factory() as uow:
            positions = await uow.positions.list_by_account(account_id, status=PositionStatus.OPEN.value)
            symbols = {symbol.id: symbol.name for symbol in await uow.symbols.list_by_account(account_id)}
            return [await _view(uow, position, symbols.get(position.symbol_id, "UNKNOWN")) for position in positions]
