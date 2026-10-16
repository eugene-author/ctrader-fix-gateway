"""CancelOrderCommand and its handler: the write side of POST /orders/{id}/cancel."""

from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID, uuid4

from app.domain.commands import CancelOrderCommand as FixCancelOrderCommand
from app.domain.enums import OrderStatus
from app.domain.errors import OrderNotCancellableError, OrderNotFoundError, PermanentError
from app.ports.unit_of_work import UnitOfWork

_CANCELLABLE_STATUSES = frozenset({OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED})


@dataclass(frozen=True, slots=True)
class CancelOrderCommand:
    """The write-side input for cancelling an order."""

    account_id: UUID
    order_id: UUID
    correlation_id: str = ""


@dataclass(frozen=True, slots=True)
class CancelOrderResult:
    """What POST /orders/{id}/cancel returns: the order, still in its pre-cancel status."""

    order_id: UUID
    status: OrderStatus


class CancelOrderHandler:
    """Validates, then writes a CancelOrderCommand to the outbox in the same transaction."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        """Store the factory. A fresh UnitOfWork is opened per handle() call."""
        self._uow_factory = uow_factory

    async def handle(self, command: CancelOrderCommand) -> CancelOrderResult:
        """Cancel a working order, or raise if it doesn't exist or isn't cancellable.

        Exception is handled at UOW aexit level.
        """
        async with self._uow_factory() as uow:
            order = await uow.orders.get(command.order_id)
            if order is None or order.account_id != command.account_id:
                raise OrderNotFoundError(command.order_id)
            if order.status not in _CANCELLABLE_STATUSES:
                raise OrderNotCancellableError(order.id, order.status)
            if order.fix_cl_ord_id is None:
                raise PermanentError(f"Order {order.id} has no fix_cl_ord_id to cancel against")

            symbol = await uow.symbols.get_by_id(order.symbol_id)
            if symbol is None or symbol.broker_symbol_id is None:
                raise PermanentError(f"Symbol {order.symbol_id} is not resolvable for order {order.id}")

            correlation_id = command.correlation_id or str(order.id)
            cancel_command = FixCancelOrderCommand(
                order_id=order.id,
                fix_cl_ord_id=uuid4().hex[:30],
                orig_cl_ord_id=order.fix_cl_ord_id,
                symbol=symbol.name,
                broker_symbol_id=symbol.broker_symbol_id,
            )
            await uow.outbox.add(
                account_id=order.account_id,
                aggregate_type="order",
                aggregate_id=order.id,
                event_type="CancelOrderCommand",
                payload=cancel_command.to_payload(),
                routing_key="order.cancel",
                correlation_id=correlation_id,
                causation_id=correlation_id,
            )
            await uow.commit()

        return CancelOrderResult(order_id=order.id, status=order.status)
