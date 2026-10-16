from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from app.domain.enums import OrderPurpose, OrderStatus, OrderType, Side
from app.domain.errors import OrderNotFoundError
from app.domain.models import Order, Price, Quantity
from app.services.queries.get_order import GetOrderQuery, GetOrderStatusQuery

from ..conftest import MockUnitOfWork, build_uow_factory


def _order(*, account_id, order_id=None, **overrides) -> Order:
    now = datetime.now(UTC)
    defaults = {
        "id": order_id or uuid4(),
        "account_id": account_id,
        "client_order_id": "bot7-0001",
        "symbol_id": uuid4(),
        "side": Side.BUY,
        "order_type": OrderType.MARKET,
        "quantity": Quantity(Decimal(1000)),
        "status": OrderStatus.PENDING,
        "purpose": OrderPurpose.OPEN,
        "created_at": now,
        "updated_at": now,
    }
    defaults.update(overrides)
    return Order(**defaults)


@pytest.mark.asyncio
async def test_get_order_query_returns_the_full_view() -> None:
    account_id = uuid4()
    order = _order(
        account_id=account_id,
        status=OrderStatus.FILLED,
        filled_quantity=Quantity(Decimal(1000)),
        avg_fill_price=Price(Decimal("1.08243")),
        position_id="4471123",
    )
    uow = MockUnitOfWork(orders={order.id: order})
    query = GetOrderQuery(build_uow_factory(uow))

    view = await query.handle(account_id, order.id)

    assert view.order_id == order.id
    assert view.client_order_id == "bot7-0001"
    assert view.status == OrderStatus.FILLED
    assert view.filled_quantity == Decimal(1000)
    assert view.avg_fill_price == Decimal("1.08243")
    assert view.position_id == "4471123"


@pytest.mark.asyncio
async def test_get_order_query_raises_not_found_for_unknown_order() -> None:
    uow = MockUnitOfWork()
    query = GetOrderQuery(build_uow_factory(uow))

    with pytest.raises(OrderNotFoundError):
        await query.handle(uuid4(), uuid4())


@pytest.mark.asyncio
async def test_get_order_query_raises_not_found_for_another_accounts_order() -> None:
    order = _order(account_id=uuid4())
    uow = MockUnitOfWork(orders={order.id: order})
    query = GetOrderQuery(build_uow_factory(uow))

    with pytest.raises(OrderNotFoundError):
        await query.handle(uuid4(), order.id)  # a different account_id


@pytest.mark.asyncio
async def test_get_order_status_query_returns_the_slim_view() -> None:
    account_id = uuid4()
    order = _order(account_id=account_id, status=OrderStatus.PARTIALLY_FILLED)
    uow = MockUnitOfWork(orders={order.id: order})
    query = GetOrderStatusQuery(build_uow_factory(uow))

    view = await query.handle(account_id, order.id)

    assert view.order_id == order.id
    assert view.status == OrderStatus.PARTIALLY_FILLED


@pytest.mark.asyncio
async def test_get_order_status_query_raises_not_found_for_unknown_order() -> None:
    uow = MockUnitOfWork()
    query = GetOrderStatusQuery(build_uow_factory(uow))

    with pytest.raises(OrderNotFoundError):
        await query.handle(uuid4(), uuid4())
