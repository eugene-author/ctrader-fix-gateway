"""ClosePositionCommand and its handler: the write side of POST /positions/{id}/close."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from app.domain.commands import ClosePositionCommand as FixClosePositionCommand
from app.domain.enums import OrderPurpose, OrderStatus, OrderType, PositionStatus, Side
from app.domain.errors import PermanentError, PositionAlreadyClosingError, PositionNotFoundError
from app.domain.models import Order
from app.ports.unit_of_work import UnitOfWork


@dataclass(frozen=True, slots=True)
class ClosePositionCommand:
    """The write side input for closing a position."""

    account_id: UUID
    position_id: str
    correlation_id: str = ""


@dataclass(frozen=True, slots=True)
class ClosePositionResult:
    """What POST /positions/{id}/close returns: the closing order (per its idempotency note)."""

    order_id: UUID
    client_order_id: str
    status: OrderStatus
    created_at: datetime


def _opposite_side(side: Side) -> Side:
    return Side.SELL if side is Side.BUY else Side.BUY


class ClosePositionHandler:
    """Validates, then writes the closing order and a ClosePositionCommand in one transaction."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        """Store the factory. A fresh UnitOfWork is opened per handle() call."""
        self._uow_factory = uow_factory

    async def handle(self, command: ClosePositionCommand) -> ClosePositionResult:
        """Close an open position, or replay if one is already running.

        Exception is handled at UOW aexit level.
        """
        async with self._uow_factory() as uow:
            position = await uow.positions.get_by_broker_position_id(
                command.account_id, command.position_id, for_update=True
            )
            if position is None:
                raise PositionNotFoundError(command.position_id)

            if position.status is PositionStatus.CLOSING:
                # Idempotent on position id
                existing = (
                    await uow.orders.get(position.closed_by_order_id)
                    if position.closed_by_order_id is not None
                    else None
                )
                if existing is not None:
                    return ClosePositionResult(
                        order_id=existing.id,
                        client_order_id=existing.client_order_id,
                        status=existing.status,
                        created_at=existing.created_at,
                    )
                raise PermanentError(f"Position {position.position_id} is CLOSING with no closing order on record")

            if position.status is not PositionStatus.OPEN:
                raise PositionAlreadyClosingError(position.position_id)

            symbol = await uow.symbols.get_by_id(position.symbol_id)
            if symbol is None or symbol.broker_symbol_id is None:
                raise PermanentError(
                    f"Symbol {position.symbol_id} is not resolvable for position {position.position_id}"
                )

            now = datetime.now(UTC)
            order_id = uuid4()
            client_order_id = f"close-{position.position_id}-{uuid4().hex[:8]}"
            fix_cl_ord_id = order_id.hex[:30]
            correlation_id = command.correlation_id or str(order_id)
            order = Order(
                id=order_id,
                account_id=command.account_id,
                client_order_id=client_order_id,
                symbol_id=position.symbol_id,
                side=_opposite_side(position.side),
                order_type=OrderType.MARKET,
                quantity=position.quantity,
                status=OrderStatus.PENDING,
                purpose=OrderPurpose.CLOSE,
                created_at=now,
                updated_at=now,
                closes_position_id=position.position_id,
                correlation_id=correlation_id,
            )
            fix_command = FixClosePositionCommand(
                order_id=order_id,
                position_id=position.position_id,
                fix_cl_ord_id=fix_cl_ord_id,
                symbol=symbol.name,
                broker_symbol_id=symbol.broker_symbol_id,
                side=order.side,
                quantity=order.quantity,
            )

            await uow.orders.add(order)
            await uow.outbox.add(
                account_id=command.account_id,
                aggregate_type="order",
                aggregate_id=order_id,
                event_type="ClosePositionCommand",
                payload=fix_command.to_payload(),
                routing_key="position.close",
                correlation_id=correlation_id,
                causation_id=correlation_id,
            )

            position.status = PositionStatus.CLOSING
            position.closed_by_order_id = order_id
            await uow.positions.update(position)
            await uow.commit()

        return ClosePositionResult(
            order_id=order.id, client_order_id=order.client_order_id, status=order.status, created_at=order.created_at
        )
