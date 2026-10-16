"""GetOrderQuery and GetOrderStatusQuery: the read side of GET /orders/{id}."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from app.domain.enums import OrderStatus
from app.domain.errors import OrderNotFoundError
from app.domain.models import Order
from app.ports.unit_of_work import UnitOfWork


@dataclass(frozen=True, slots=True)
class OrderView:
    """The full order resource."""

    order_id: UUID
    client_order_id: str
    status: OrderStatus
    quantity: Decimal
    filled_quantity: Decimal
    avg_fill_price: Decimal | None
    position_id: str | None
    reject_reason: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OrderStatusView:
    """Order status projection."""

    order_id: UUID
    client_order_id: str
    status: OrderStatus
    filled_quantity: Decimal
    avg_fill_price: Decimal | None
    position_id: str | None
    reject_reason: str | None
    updated_at: datetime


def _get_owned_order(order: Order | None, account_id: UUID, order_id: UUID) -> Order:
    if order is None or order.account_id != account_id:
        raise OrderNotFoundError(order_id)
    return order


def order_to_view(order: Order) -> OrderView:
    """Render an Order as the full order resource - shared with ListOrdersQuery."""
    return OrderView(
        order_id=order.id,
        client_order_id=order.client_order_id,
        status=order.status,
        quantity=order.quantity.value,
        filled_quantity=order.filled_quantity.value,
        avg_fill_price=order.avg_fill_price.value if order.avg_fill_price is not None else None,
        position_id=order.position_id,
        reject_reason=order.reject_reason_code,
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


class GetOrderQuery:
    """Fetch the full order resource, scoped to the requesting account."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        """Store the factory."""
        self._uow_factory = uow_factory

    async def handle(self, account_id: UUID, order_id: UUID) -> OrderView:
        """Return the order, or raise OrderNotFoundError if it doesn't belong to this account."""
        async with self._uow_factory() as uow:
            order = _get_owned_order(await uow.orders.get(order_id), account_id, order_id)

        return order_to_view(order)


class GetOrderStatusQuery:
    """Fetch the order status  projection."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        """Store the factory."""
        self._uow_factory = uow_factory

    async def handle(self, account_id: UUID, order_id: UUID) -> OrderStatusView:
        """Return the order's status, or raise OrderNotFoundError if it doesn't belong to this account."""
        async with self._uow_factory() as uow:
            order = _get_owned_order(await uow.orders.get(order_id), account_id, order_id)

        return OrderStatusView(
            order_id=order.id,
            client_order_id=order.client_order_id,
            status=order.status,
            filled_quantity=order.filled_quantity.value,
            avg_fill_price=order.avg_fill_price.value if order.avg_fill_price is not None else None,
            position_id=order.position_id,
            reject_reason=order.reject_reason_code,
            updated_at=order.updated_at,
        )
