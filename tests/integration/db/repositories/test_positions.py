from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AccountRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.enums import PositionStatus, Side
from app.domain.models import Position, Price, Quantity


@pytest.mark.asyncio
async def test_position_repository_round_trips_through_id_and_broker_position_id(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, symbol = seeded_account_and_symbol
    now = datetime.now(UTC)
    position = Position(
        id=uuid4(),
        account_id=account.id,
        position_id="4471123",
        symbol_id=symbol.id,
        side=Side.BUY,
        quantity=Quantity(Decimal(1000)),
        entry_price=Price(Decimal("1.08243")),
        status=PositionStatus.OPEN,
        opened_at=now,
    )

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.positions.add(position)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as read_uow:
        by_id = await read_uow.positions.get(position.id)
        by_broker_id = await read_uow.positions.get_by_broker_position_id(account.id, "4471123")

    assert by_id is not None
    assert by_id.position_id == "4471123"
    assert by_id.entry_price == Price(Decimal("1.08243"))
    assert by_id.status == PositionStatus.OPEN

    assert by_broker_id is not None
    assert by_broker_id.id == position.id


@pytest.mark.asyncio
async def test_get_returns_none_for_unknown_position(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        result = await uow.positions.get(uuid4())

    assert result is None
