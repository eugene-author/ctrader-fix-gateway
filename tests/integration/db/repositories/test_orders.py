from collections.abc import Callable
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AccountRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.enums import OrderStatus, Side
from app.domain.errors import ConcurrentModificationError
from app.domain.models import Order


@pytest.mark.asyncio
async def test_order_repository_round_trips_through_id_and_client_order_id(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(order)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as read_uow:
        by_id = await read_uow.orders.get(order.id)
        by_client_order_id = await read_uow.orders.get_by_client_order_id(account.id, order.client_order_id)

    assert by_id is not None
    assert by_id.id == order.id
    assert by_id.client_order_id == order.client_order_id
    assert by_id.quantity == order.quantity
    assert by_id.status == OrderStatus.PENDING
    assert by_id.side == Side.BUY

    assert by_client_order_id is not None
    assert by_client_order_id.id == order.id


@pytest.mark.asyncio
async def test_get_returns_none_for_unknown_order(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        result = await uow.orders.get(uuid4())

    assert result is None


@pytest.mark.asyncio
async def test_update_persists_changes_to_an_existing_order(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(order)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        fetched = await uow.orders.get(order.id)
        assert fetched is not None
        fetched.status = OrderStatus.SUBMITTED
        fetched.fix_cl_ord_id = "abc123"
        fetched.submitted_at = datetime.now(UTC)
        fetched.version = 2
        await uow.orders.update(fetched)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        reread = await uow.orders.get(order.id)

    assert reread is not None
    assert reread.status == OrderStatus.SUBMITTED
    assert reread.fix_cl_ord_id == "abc123"
    assert reread.submitted_at is not None
    assert reread.version == 2


@pytest.mark.asyncio
async def test_concurrent_updates_to_the_same_order_raise_concurrent_modification_error(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    # orders.version is a real version_id_col: two transactions reading the
    # same row and both trying to write it back must not both succeed - the
    # second writer's stale in-memory version no longer matches the row
    # SQLAlchemy re-reads at flush time.
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(order)
        await uow.commit()

    async with (
        SqlAlchemyUnitOfWork(session_factory) as uow_a,
        SqlAlchemyUnitOfWork(session_factory) as uow_b,
    ):
        order_a = await uow_a.orders.get(order.id)
        order_b = await uow_b.orders.get(order.id)
        assert order_a is not None
        assert order_b is not None
        assert order_a.version == order_b.version == 1

        order_a.status = OrderStatus.SUBMITTED
        await uow_a.orders.update(order_a)
        await uow_a.commit()  # first writer: succeeds, version 1 -> 2

        order_b.status = OrderStatus.CANCELLED
        await uow_b.orders.update(order_b)
        with pytest.raises(ConcurrentModificationError):
            await uow_b.commit()  # second writer: its version (1) is stale

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        reread = await uow.orders.get(order.id)
    assert reread is not None
    assert reread.status == OrderStatus.SUBMITTED  # tx A's write survived; tx B's did not
    assert reread.version == 2


@pytest.mark.asyncio
async def test_update_raises_for_an_order_that_does_not_exist(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)  # never added

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        with pytest.raises(ValueError, match=str(order.id)):
            await uow.orders.update(order)


@pytest.mark.parametrize("status", [OrderStatus.DISPATCHING, OrderStatus.DISPATCH_UNCERTAIN])
@pytest.mark.asyncio
async def test_dispatch_states_round_trip_through_the_real_status_column(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    status: OrderStatus,
) -> None:
    # No migration widened the orders.status column for these two members -
    # it's a plain VARCHAR(20) with no DB-level CHECK constraint, so this is
    # the only thing that actually proves they persist correctly.
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(order)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        fetched = await uow.orders.get(order.id)
        assert fetched is not None
        fetched.status = status
        fetched.fix_cl_ord_id = order.id.hex[:16]  # unique per test/parametrization, unlike a hardcoded literal
        fetched.submitted_at = datetime.now(UTC)
        await uow.orders.update(fetched)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        reread = await uow.orders.get(order.id)

    assert reread is not None
    assert reread.status is status


@pytest.mark.asyncio
async def test_list_by_account_and_statuses_returns_only_matching_orders(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    account, symbol = seeded_account_and_symbol
    dispatching = make_order(account_id=account.id, symbol_id=symbol.id)
    uncertain = make_order(account_id=account.id, symbol_id=symbol.id)
    filled = make_order(account_id=account.id, symbol_id=symbol.id)

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        for order in (dispatching, uncertain, filled):
            await uow.orders.add(order)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        for order, status in (
            (dispatching, OrderStatus.DISPATCHING),
            (uncertain, OrderStatus.DISPATCH_UNCERTAIN),
            (filled, OrderStatus.FILLED),
        ):
            fetched = await uow.orders.get(order.id)
            assert fetched is not None
            fetched.status = status
            await uow.orders.update(fetched)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        found = await uow.orders.list_by_account_and_statuses(
            account.id, frozenset({OrderStatus.DISPATCHING, OrderStatus.DISPATCH_UNCERTAIN})
        )

    assert {order.id for order in found} == {dispatching.id, uncertain.id}
