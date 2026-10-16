"""ListOrdersQuery against real Postgres: keyset pagination, filters, and account scoping."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AccountRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.enums import OrderStatus
from app.domain.errors import ValidationError
from app.domain.models import Order
from app.services.queries.list_orders import ListOrdersQuery, decode_cursor


@pytest.fixture
def uow_factory(session_factory: async_sessionmaker[AsyncSession]):
    return lambda: SqlAlchemyUnitOfWork(session_factory)


async def _seed_orders(
    session_factory: async_sessionmaker[AsyncSession],
    make_order: Callable[..., Order],
    *,
    account_id,
    symbol_id,
    count: int,
    status: OrderStatus = OrderStatus.PENDING,
    client_order_id_prefix: str = "seed",
) -> list[Order]:
    base = datetime.now(UTC)
    orders = []
    for i in range(count):
        order = make_order(account_id=account_id, symbol_id=symbol_id)
        order.client_order_id = f"{client_order_id_prefix}-{i:04d}-{uuid4().hex[:6]}"
        order.status = status
        order.created_at = base + timedelta(milliseconds=i)
        order.updated_at = order.created_at
        orders.append(order)

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        for order in orders:
            await uow.orders.add(order)
        await uow.commit()
    return orders


@pytest.mark.asyncio
async def test_list_orders_paginates_across_150_orders_with_no_gaps_or_duplicates(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    seeded = await _seed_orders(
        session_factory,
        make_order,
        account_id=account.id,
        symbol_id=symbol.id,
        count=150,
        client_order_id_prefix="page",
    )
    expected_ids_newest_first = [o.id for o in sorted(seeded, key=lambda o: o.created_at, reverse=True)]

    query = ListOrdersQuery(uow_factory)
    seen_ids: list = []
    cursor = None
    pages = 0
    while True:
        page = await query.handle(account.id, cursor=cursor, limit=50)
        seen_ids.extend(view.order_id for view in page.orders)
        pages += 1
        if page.next_cursor is None:
            break
        cursor = page.next_cursor
        assert pages <= 10  # sanity cap - a bug looping forever must not hang the suite

    assert pages == 3  # 150 orders / 50 per page
    assert seen_ids == expected_ids_newest_first  # exact order, no gaps, no duplicates


@pytest.mark.asyncio
async def test_list_orders_filters_by_status_and_symbol(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    pending = await _seed_orders(
        session_factory,
        make_order,
        account_id=account.id,
        symbol_id=symbol.id,
        count=5,
        status=OrderStatus.PENDING,
        client_order_id_prefix="pending",
    )
    filled = await _seed_orders(
        session_factory,
        make_order,
        account_id=account.id,
        symbol_id=symbol.id,
        count=3,
        status=OrderStatus.FILLED,
        client_order_id_prefix="filled",
    )

    query = ListOrdersQuery(uow_factory)

    by_status = await query.handle(account.id, statuses=frozenset({OrderStatus.FILLED}))
    assert {view.order_id for view in by_status.orders} == {o.id for o in filled}

    by_symbol = await query.handle(account.id, symbol=symbol.name)
    assert {view.order_id for view in by_symbol.orders} == {o.id for o in pending + filled}

    by_unknown_symbol = await query.handle(account.id, symbol="NOTREAL")
    assert by_unknown_symbol.orders == []
    assert by_unknown_symbol.next_cursor is None


@pytest.mark.asyncio
async def test_list_orders_never_returns_another_accounts_orders(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    await _seed_orders(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, count=3, client_order_id_prefix="mine"
    )

    async with session_factory() as session:
        other_account = AccountRow(
            broker="ctrader", external_account_id="other", mode="hedging", currency="USD", enabled=True
        )
        session.add(other_account)
        await session.flush()
        other_symbol = SymbolRow(account_id=other_account.id, name="EURUSD", broker_symbol_id=1)
        session.add(other_symbol)
        await session.flush()
        await session.commit()
    await _seed_orders(
        session_factory,
        make_order,
        account_id=other_account.id,
        symbol_id=other_symbol.id,
        count=3,
        client_order_id_prefix="theirs",
    )

    query = ListOrdersQuery(uow_factory)
    page = await query.handle(account.id)

    assert len(page.orders) == 3
    assert all(view.order_id for view in page.orders)  # sanity: not silently empty
    for view in page.orders:
        assert view.client_order_id.startswith("mine-")


def test_decode_cursor_rejects_garbage() -> None:
    with pytest.raises(ValidationError):
        decode_cursor("not-a-valid-cursor!!")
