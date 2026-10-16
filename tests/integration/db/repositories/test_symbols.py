from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AccountRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.models import Price, Quantity


@pytest.mark.asyncio
async def test_symbol_repository_fetches_by_name_with_constraints(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        account = AccountRow(broker="ctrader", external_account_id="1", mode="hedging", currency="USD", enabled=True)
        session.add(account)
        await session.flush()
        session.add(
            SymbolRow(
                account_id=account.id,
                name="EURUSD",
                broker_symbol_id=1,
                digits=5,
                lot_size=Decimal(100000),
                qty_step=Decimal(1000),
                min_qty=Decimal(1000),
                max_qty=Decimal(10_000_000),
                price_step=Decimal("0.00001"),
                enabled=True,
                subscribe=True,
                needs_review=False,
            )
        )
        await session.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        symbol = await uow.symbols.get_by_name(account.id, "EURUSD")

    assert symbol is not None
    assert symbol.name == "EURUSD"
    assert symbol.broker_symbol_id == 1
    assert symbol.enabled is True
    assert symbol.needs_review is False
    assert symbol.qty_step == Quantity(Decimal(1000))
    assert symbol.price_step == Price(Decimal("0.00001"))


@pytest.mark.asyncio
async def test_symbol_repository_returns_none_for_unknown_symbol(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        symbol = await uow.symbols.get_by_name(uuid4(), "DOESNOTEXIST")

    assert symbol is None
