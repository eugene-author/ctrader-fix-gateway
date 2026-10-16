from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AccountRow, ExecutionRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.models import Execution, Price, Quantity


@pytest.mark.asyncio
async def test_execution_repository_persists_a_report(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, _symbol = seeded_account_and_symbol
    now = datetime.now(UTC)
    execution = Execution(
        id=uuid4(),
        account_id=account.id,
        exec_id=str(uuid4()),
        exec_type="F",
        ord_status="2",
        cum_qty=Quantity(Decimal(1000)),
        leaves_qty=Quantity(Decimal(0)),
        transact_time=now,
        raw_message="8=FIX.4.4|...",
        received_at=now,
        last_qty=Quantity(Decimal(1000)),
        last_price=Price(Decimal("1.08243")),
        position_id="4471123",
    )

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.executions.add(execution)
        await uow.commit()

    # ExecutionRepository is write-only  (an immutable log,
    # no query use yet); read the raw row to verify persistence.
    async with session_factory() as session:
        row = await session.get(ExecutionRow, execution.id)

    assert row is not None
    assert row.exec_id == execution.exec_id
    assert row.cum_qty == Decimal(1000)
    assert row.position_id == "4471123"
