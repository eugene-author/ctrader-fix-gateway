from uuid import uuid4

import pytest

from app.domain.enums import OrderStatus
from app.domain.errors import OrderNotCancellableError, OrderNotFoundError
from app.services.commands.cancel_order import CancelOrderCommand, CancelOrderHandler

from ..conftest import MockUnitOfWork, build_uow_factory


@pytest.mark.asyncio
async def test_cancel_accepted_order_writes_a_cancel_order_command_to_the_outbox(make_symbol, make_order) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    order = make_order(account_id=account_id, symbol_id=symbol.id, status=OrderStatus.ACCEPTED)
    uow = MockUnitOfWork(orders={order.id: order}, symbols={(account_id, symbol.name): symbol})
    handler = CancelOrderHandler(build_uow_factory(uow))

    result = await handler.handle(CancelOrderCommand(account_id=account_id, order_id=order.id))

    assert result.order_id == order.id
    assert result.status == OrderStatus.ACCEPTED
    assert len(uow.outbox_events) == 1
    event = uow.outbox_events[0]
    assert event["event_type"] == "CancelOrderCommand"
    assert event["routing_key"] == "order.cancel"
    assert event["payload"]["order_id"] == str(order.id)
    assert event["payload"]["orig_cl_ord_id"] == order.fix_cl_ord_id
    assert event["payload"]["symbol"] == symbol.name
    assert event["payload"]["broker_symbol_id"] == symbol.broker_symbol_id
    assert uow.committed is True


@pytest.mark.asyncio
async def test_cancel_partially_filled_order_is_allowed(make_symbol, make_order) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    order = make_order(account_id=account_id, symbol_id=symbol.id, status=OrderStatus.PARTIALLY_FILLED)
    uow = MockUnitOfWork(orders={order.id: order}, symbols={(account_id, symbol.name): symbol})
    handler = CancelOrderHandler(build_uow_factory(uow))

    result = await handler.handle(CancelOrderCommand(account_id=account_id, order_id=order.id))

    assert result.status == OrderStatus.PARTIALLY_FILLED


@pytest.mark.parametrize(
    "status", [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.FILLED, OrderStatus.CANCELLED]
)
@pytest.mark.asyncio
async def test_cancel_rejects_an_order_not_in_a_working_state(status, make_symbol, make_order) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    order = make_order(account_id=account_id, symbol_id=symbol.id, status=status)
    uow = MockUnitOfWork(orders={order.id: order}, symbols={(account_id, symbol.name): symbol})
    handler = CancelOrderHandler(build_uow_factory(uow))

    with pytest.raises(OrderNotCancellableError):
        await handler.handle(CancelOrderCommand(account_id=account_id, order_id=order.id))

    assert uow.outbox_events == []


@pytest.mark.asyncio
async def test_cancel_raises_not_found_for_an_unknown_order() -> None:
    uow = MockUnitOfWork()
    handler = CancelOrderHandler(build_uow_factory(uow))

    with pytest.raises(OrderNotFoundError):
        await handler.handle(CancelOrderCommand(account_id=uuid4(), order_id=uuid4()))


@pytest.mark.asyncio
async def test_cancel_raises_not_found_when_the_order_belongs_to_another_account(make_symbol, make_order) -> None:
    account_id = uuid4()
    other_account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    order = make_order(account_id=account_id, symbol_id=symbol.id, status=OrderStatus.ACCEPTED)
    uow = MockUnitOfWork(orders={order.id: order}, symbols={(account_id, symbol.name): symbol})
    handler = CancelOrderHandler(build_uow_factory(uow))

    with pytest.raises(OrderNotFoundError):
        await handler.handle(CancelOrderCommand(account_id=other_account_id, order_id=order.id))
