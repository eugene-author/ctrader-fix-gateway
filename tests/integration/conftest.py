import os
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import aio_pika
import pytest
import pytest_asyncio
import redis.asyncio as redis
from aio_pika.abc import AbstractRobustConnection
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer
from testcontainers.community.redis import RedisContainer

from app.db.models import AccountRow, SymbolRow
from app.db.session import create_engine, create_session_factory
from app.domain.enums import OrderPurpose, OrderStatus, OrderType, Side
from app.domain.models import Order, Quantity
from app.queue.topology import declare_topology

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


@pytest.fixture(scope="session")
def migrated_postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:16", username="gateway", password="gateway", dbname="gateway") as postgres:
        raw_url = (
            f"postgresql+asyncpg://{postgres.username}:{postgres.password}"
            f"@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
        )

        # alembic/env.py prefers DATABASE_URL from the environment (credentials
        # come from the environment only, never a config file), so point it at
        # the throwaway container for the duration of the migration run, then
        # restore it.
        original = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = raw_url
        try:
            config = Config(str(_ALEMBIC_INI))
            command.upgrade(config, "head")
        finally:
            if original is not None:
                os.environ["DATABASE_URL"] = original
            else:
                os.environ.pop("DATABASE_URL", None)

        yield raw_url


@pytest_asyncio.fixture
async def session_factory(migrated_postgres_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    # Function-scoped deliberately: an AsyncEngine's connections are bound to
    # the event loop that created them, and pytest-asyncio gives each test
    # function its own loop by default. A session-scoped engine breaks on the
    # second test with "Event loop is closed". The container above (the
    # expensive part) stays session-scoped; only this cheap engine is per-test.
    engine = create_engine(migrated_postgres_url)
    try:
        yield create_session_factory(engine)
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def db_session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def seeded_account_and_symbol(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[AccountRow, SymbolRow]:
    """A fresh account + one tradable symbol, for tests that need valid FKs."""
    async with session_factory() as session:
        account = AccountRow(
            broker="ctrader",
            external_account_id=str(uuid4()),
            mode="hedging",
            currency="USD",
            enabled=True,
        )
        session.add(account)
        await session.flush()

        symbol = SymbolRow(account_id=account.id, name="EURUSD", broker_symbol_id=1)
        session.add(symbol)
        await session.flush()
        await session.commit()

    return account, symbol


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    with RabbitMqContainer("rabbitmq:3.13-management", username="guest", password="guest") as rabbitmq:
        yield (f"amqp://guest:guest@{rabbitmq.get_container_host_ip()}:{rabbitmq.get_exposed_port(5672)}/")


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer("redis:7") as container:
        yield f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0"


@pytest_asyncio.fixture
async def redis_client(redis_url: str) -> AsyncIterator[redis.Redis]:
    # Function-scoped for the same reason session_factory is (see above):
    # connections are bound to the event loop that created them.
    client = redis.from_url(redis_url, decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture
async def rabbitmq_connection(rabbitmq_url: str) -> AsyncIterator[AbstractRobustConnection]:
    # Function-scoped for the same reason session_factory is (see above):
    # connections are bound to the event loop that created them.
    connection = await aio_pika.connect_robust(rabbitmq_url)
    try:
        await declare_topology(connection)
        yield connection
    finally:
        await connection.close()


@pytest.fixture
def make_order() -> Callable[..., Order]:
    """Factory for a minimal, valid PENDING market order."""

    def _make_order(*, account_id, symbol_id) -> Order:
        now = datetime.now(UTC)
        return Order(
            id=uuid4(),
            account_id=account_id,
            client_order_id=f"bot7-{uuid4()}",
            symbol_id=symbol_id,
            side=Side.BUY,
            order_type=OrderType.MARKET,
            quantity=Quantity(Decimal(1000)),
            status=OrderStatus.PENDING,
            purpose=OrderPurpose.OPEN,
            created_at=now,
            updated_at=now,
        )

    return _make_order
