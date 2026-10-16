"""ClosePositionHandler against real Postgres: proves the row-lock closes the concurrent-close race."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AccountRow, OrderRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.enums import PositionStatus, Side
from app.domain.models import Position, Price, Quantity
from app.services.commands.close_position import ClosePositionCommand, ClosePositionHandler


@pytest.mark.asyncio
async def test_two_concurrent_close_requests_produce_exactly_one_closing_order(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, symbol = seeded_account_and_symbol
    position = Position(
        id=uuid4(),
        account_id=account.id,
        position_id="race-1",
        symbol_id=symbol.id,
        side=Side.BUY,
        quantity=Quantity(Decimal(1000)),
        entry_price=Price(Decimal("1.08243")),
        status=PositionStatus.OPEN,
        opened_at=datetime.now(UTC),
    )
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.positions.add(position)
        await uow.commit()

    handler = ClosePositionHandler(lambda: SqlAlchemyUnitOfWork(session_factory))
    command = ClosePositionCommand(account_id=account.id, position_id="race-1")

    # Without the row lock (for_update=True in ClosePositionHandler.handle),
    # both requests can read status=OPEN before either commits, and each
    # creates its own closing order.
    first, second = await asyncio.gather(handler.handle(command), handler.handle(command))

    assert first.order_id == second.order_id  # the second racer replays the first's in-flight close

    async with session_factory() as session:
        closing_order_count = (
            await session.execute(
                select(func.count()).select_from(OrderRow).where(OrderRow.closes_position_id == "race-1")
            )
        ).scalar_one()

    assert closing_order_count == 1

    async with SqlAlchemyUnitOfWork(session_factory) as verify_uow:
        refreshed = await verify_uow.positions.get_by_broker_position_id(account.id, "race-1")
    assert refreshed is not None
    assert refreshed.status == PositionStatus.CLOSING
    assert refreshed.closed_by_order_id == first.order_id
