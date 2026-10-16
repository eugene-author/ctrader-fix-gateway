from collections.abc import Callable
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AccountRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.errors import DuplicateKeyError
from app.domain.models import Order


@pytest.mark.asyncio
async def test_unit_of_work_rolls_back_when_an_exception_escapes_the_block(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)

    with pytest.raises(RuntimeError, match="boom"):
        async with SqlAlchemyUnitOfWork(session_factory) as uow:
            await uow.orders.add(order)
            raise RuntimeError("boom")

    async with SqlAlchemyUnitOfWork(session_factory) as verify_uow:
        result = await verify_uow.orders.get(order.id)

    assert result is None


@pytest.mark.asyncio
async def test_commit_translates_a_unique_violation_into_duplicate_key_error(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    account, symbol = seeded_account_and_symbol
    first = make_order(account_id=account.id, symbol_id=symbol.id)

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(first)
        await uow.commit()

    # Same client_order_id, different order id -> violates uq_orders_account_client_order_id.
    second = make_order(account_id=account.id, symbol_id=symbol.id)
    second.client_order_id = first.client_order_id

    with pytest.raises(DuplicateKeyError):
        async with SqlAlchemyUnitOfWork(session_factory) as uow:
            await uow.orders.add(second)
            await uow.commit()


@pytest.mark.asyncio
async def test_commit_does_not_mislabel_a_foreign_key_violation_as_duplicate_key(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    _account, symbol = seeded_account_and_symbol
    order = make_order(account_id=uuid4(), symbol_id=symbol.id)  # no such account -> FK violation, not a duplicate

    with pytest.raises(IntegrityError) as exc_info:
        async with SqlAlchemyUnitOfWork(session_factory) as uow:
            await uow.orders.add(order)
            await uow.commit()

    assert not isinstance(exc_info.value, DuplicateKeyError)
