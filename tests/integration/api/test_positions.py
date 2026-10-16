from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.api_keys import hash_api_key, key_prefix
from app.core.config import Settings
from app.db.models import AccountRow, ApiClientRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.enums import OrderPurpose, OrderStatus, OrderType, PositionStatus, ProtectionState, ProtectiveKind, Side
from app.domain.models import Order, Position, Price, Quantity
from app.main import create_app


def _settings(database_url: str) -> Settings:
    return Settings(
        database_url=database_url,
        redis_url="redis://localhost:6379/0",
        rabbitmq_url="amqp://guest:guest@localhost/",
        api_key_pepper="test-pepper",
    )


async def _seed_account_symbol_and_api_key(
    session_factory: async_sessionmaker[AsyncSession], raw_key: str
) -> tuple[AccountRow, SymbolRow]:
    async with session_factory() as session:
        account = AccountRow(broker="ctrader", external_account_id="1", mode="hedging", currency="USD", enabled=True)
        session.add(account)
        await session.flush()

        symbol = SymbolRow(
            account_id=account.id,
            name="EURUSD",
            broker_symbol_id=1,
            digits=5,
            lot_size=Decimal(100_000),
            qty_step=Decimal(1000),
            min_qty=Decimal(1000),
            max_qty=Decimal(10_000_000),
            price_step=Decimal("0.00001"),
            enabled=True,
            subscribe=True,
            needs_review=False,
        )
        session.add(symbol)

        session.add(
            ApiClientRow(
                account_id=account.id,
                name="bot7",
                key_prefix=key_prefix(raw_key),
                api_key_hmac=hash_api_key(raw_key, pepper="test-pepper"),
                scopes=["positions:read", "positions:write"],
            )
        )
        await session.commit()

    return account, symbol


@pytest.fixture
async def client_setup(
    migrated_postgres_url: str, session_factory: async_sessionmaker[AsyncSession]
) -> tuple[AsyncClient, AccountRow, SymbolRow]:
    raw_key = f"sk_live_{uuid4().hex}"
    account, symbol = await _seed_account_symbol_and_api_key(session_factory, raw_key)
    app = create_app(settings=_settings(migrated_postgres_url))
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://test", headers={"X-API-Key": raw_key})
    return client, account, symbol


async def _seed_position(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    account_id,
    symbol_id,
    position_id: str = "4471123",
    status: PositionStatus = PositionStatus.OPEN,
) -> Position:
    position = Position(
        id=uuid4(),
        account_id=account_id,
        position_id=position_id,
        symbol_id=symbol_id,
        side=Side.BUY,
        quantity=Quantity(Decimal(1000)),
        entry_price=Price(Decimal("1.08243")),
        status=status,
        opened_at=datetime.now(UTC),
    )
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.positions.add(position)
        await uow.commit()
    return position


@pytest.mark.asyncio
async def test_list_positions_returns_only_open_ones(
    client_setup: tuple[AsyncClient, AccountRow, SymbolRow],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, account, symbol = client_setup
    await _seed_position(session_factory, account_id=account.id, symbol_id=symbol.id, position_id="open-1")
    await _seed_position(
        session_factory,
        account_id=account.id,
        symbol_id=symbol.id,
        position_id="closed-1",
        status=PositionStatus.CLOSED,
    )

    async with client:
        response = await client.get("/api/v1/positions")

    assert response.status_code == 200
    position_ids = {p["position_id"] for p in response.json()["positions"]}
    assert position_ids == {"open-1"}


@pytest.mark.asyncio
async def test_get_position_returns_the_resource(
    client_setup: tuple[AsyncClient, AccountRow, SymbolRow],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, account, symbol = client_setup
    await _seed_position(session_factory, account_id=account.id, symbol_id=symbol.id, position_id="4471123")

    async with client:
        response = await client.get("/api/v1/positions/4471123")

    assert response.status_code == 200
    body = response.json()
    assert body["position_id"] == "4471123"
    assert body["symbol"] == "EURUSD"
    assert body["status"] == "OPEN"
    assert Decimal(body["entry_price"]) == Decimal("1.08243")


@pytest.mark.asyncio
async def test_get_position_includes_the_protection_block(
    client_setup: tuple[AsyncClient, AccountRow, SymbolRow],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, account, symbol = client_setup
    position = await _seed_position(session_factory, account_id=account.id, symbol_id=symbol.id, position_id="pos-p1")
    now = datetime.now(UTC)
    sl_order = Order(
        id=uuid4(),
        account_id=account.id,
        client_order_id="sl:pos-p1",
        symbol_id=symbol.id,
        side=Side.SELL,
        order_type=OrderType.STOP,
        quantity=Quantity(Decimal(1000)),
        status=OrderStatus.ACCEPTED,
        purpose=OrderPurpose.CLOSE,
        created_at=now,
        updated_at=now,
        fix_cl_ord_id="sl:pos-p1",
        stop_price=Price(Decimal("1.07000")),
        protective_kind=ProtectiveKind.STOP_LOSS,
        protects_position_id="pos-p1",
    )
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(sl_order)
        position.stop_loss_order_id = sl_order.id
        position.protection_state = ProtectionState.PARTIAL
        await uow.positions.update(position)
        await uow.commit()

    async with client:
        response = await client.get("/api/v1/positions/pos-p1")

    assert response.status_code == 200
    protection = response.json()["protection"]
    assert protection["state"] == "PARTIAL"
    assert protection["stop_loss"]["order_id"] == str(sl_order.id)
    assert Decimal(protection["stop_loss"]["price"]) == Decimal("1.07000")
    assert protection["stop_loss"]["status"] == "ACCEPTED"
    assert protection["take_profit"] is None


@pytest.mark.asyncio
async def test_get_position_for_unknown_id_is_404(client_setup: tuple[AsyncClient, AccountRow, SymbolRow]) -> None:
    client, _account, _symbol = client_setup

    async with client:
        response = await client.get("/api/v1/positions/does-not-exist")

    assert response.status_code == 404
    assert response.json()["code"] == "POSITION_NOT_FOUND"


@pytest.mark.asyncio
async def test_close_position_creates_an_opposite_order_and_marks_it_closing(
    client_setup: tuple[AsyncClient, AccountRow, SymbolRow],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, account, symbol = client_setup
    await _seed_position(session_factory, account_id=account.id, symbol_id=symbol.id, position_id="4471123")

    async with client:
        response = await client.post("/api/v1/positions/4471123/close")
        position_response = await client.get("/api/v1/positions/4471123")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "PENDING"

    assert position_response.status_code == 200
    assert position_response.json()["status"] == "CLOSING"


@pytest.mark.asyncio
async def test_close_position_is_idempotent_while_in_flight(
    client_setup: tuple[AsyncClient, AccountRow, SymbolRow],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, account, symbol = client_setup
    await _seed_position(session_factory, account_id=account.id, symbol_id=symbol.id, position_id="4471123")

    async with client:
        first = await client.post("/api/v1/positions/4471123/close")
        second = await client.post("/api/v1/positions/4471123/close")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["order_id"] == second.json()["order_id"]


@pytest.mark.asyncio
async def test_close_an_unknown_position_is_404(client_setup: tuple[AsyncClient, AccountRow, SymbolRow]) -> None:
    client, _account, _symbol = client_setup

    async with client:
        response = await client.post("/api/v1/positions/does-not-exist/close")

    assert response.status_code == 404
    assert response.json()["code"] == "POSITION_NOT_FOUND"
