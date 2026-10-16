from uuid import uuid4

import pytest

from app.domain.enums import OrderPurpose, OrderStatus, PositionStatus, Side
from app.domain.errors import PermanentError, PositionAlreadyClosingError, PositionNotFoundError
from app.services.commands.close_position import ClosePositionCommand, ClosePositionHandler

from ..conftest import MockUnitOfWork, build_uow_factory


@pytest.mark.asyncio
async def test_close_open_position_creates_an_opposite_market_order(make_symbol, make_position) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    position = make_position(account_id=account_id, symbol_id=symbol.id, side=Side.BUY, status=PositionStatus.OPEN)
    uow = MockUnitOfWork(positions={position.id: position}, symbols={(account_id, symbol.name): symbol})
    handler = ClosePositionHandler(build_uow_factory(uow))

    result = await handler.handle(ClosePositionCommand(account_id=account_id, position_id=position.position_id))

    assert result.status == OrderStatus.PENDING
    created_order = uow.orders_store[result.order_id]
    assert created_order.side == Side.SELL  # opposite of the position's BUY
    assert created_order.purpose == OrderPurpose.CLOSE
    assert created_order.closes_position_id == position.position_id
    assert created_order.quantity == position.quantity

    assert len(uow.outbox_events) == 1
    event = uow.outbox_events[0]
    assert event["event_type"] == "ClosePositionCommand"
    assert event["routing_key"] == "position.close"
    assert event["payload"]["position_id"] == position.position_id
    assert event["payload"]["side"] == "SELL"

    updated_position = uow.positions_store[position.id]
    assert updated_position.status == PositionStatus.CLOSING
    assert updated_position.closed_by_order_id == result.order_id


@pytest.mark.asyncio
async def test_close_a_position_already_closing_returns_the_in_flight_order(
    make_symbol, make_position, make_order
) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    in_flight_order = make_order(
        account_id=account_id, symbol_id=symbol.id, status=OrderStatus.SUBMITTED, purpose=OrderPurpose.CLOSE
    )
    position = make_position(
        account_id=account_id,
        symbol_id=symbol.id,
        status=PositionStatus.CLOSING,
        closed_by_order_id=in_flight_order.id,
    )
    uow = MockUnitOfWork(
        orders={in_flight_order.id: in_flight_order},
        positions={position.id: position},
        symbols={(account_id, symbol.name): symbol},
    )
    handler = ClosePositionHandler(build_uow_factory(uow))

    result = await handler.handle(ClosePositionCommand(account_id=account_id, position_id=position.position_id))

    assert result.order_id == in_flight_order.id
    assert result.status == OrderStatus.SUBMITTED
    # No second order or outbox row - the in-flight close was replayed, not repeated.
    assert len(uow.orders_store) == 1
    assert uow.outbox_events == []


@pytest.mark.asyncio
async def test_close_a_closed_position_is_rejected(make_symbol, make_position) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    position = make_position(account_id=account_id, symbol_id=symbol.id, status=PositionStatus.CLOSED)
    uow = MockUnitOfWork(positions={position.id: position}, symbols={(account_id, symbol.name): symbol})
    handler = ClosePositionHandler(build_uow_factory(uow))

    with pytest.raises(PositionAlreadyClosingError):
        await handler.handle(ClosePositionCommand(account_id=account_id, position_id=position.position_id))


@pytest.mark.asyncio
async def test_close_raises_not_found_for_an_unknown_position() -> None:
    uow = MockUnitOfWork()
    handler = ClosePositionHandler(build_uow_factory(uow))

    with pytest.raises(PositionNotFoundError):
        await handler.handle(ClosePositionCommand(account_id=uuid4(), position_id="does-not-exist"))


@pytest.mark.asyncio
async def test_close_raises_permanent_error_when_the_symbol_is_unresolvable(make_position) -> None:
    account_id = uuid4()
    position = make_position(account_id=account_id, symbol_id=uuid4(), status=PositionStatus.OPEN)
    uow = MockUnitOfWork(positions={position.id: position})  # no matching symbol seeded
    handler = ClosePositionHandler(build_uow_factory(uow))

    with pytest.raises(PermanentError):
        await handler.handle(ClosePositionCommand(account_id=account_id, position_id=position.position_id))
