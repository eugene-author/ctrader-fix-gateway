from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
import redis.asyncio as redis
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.cache.redis_price_cache import RedisPriceCacheRepository
from app.core.api_keys import hash_api_key, key_prefix
from app.core.config import Settings
from app.db.models import AccountRow, ApiClientRow
from app.main import create_app


def _settings(database_url: str, redis_url: str, rabbitmq_url: str) -> Settings:
    return Settings(
        database_url=database_url, redis_url=redis_url, rabbitmq_url=rabbitmq_url, api_key_pepper="test-pepper"
    )


async def _seed_account_and_api_key(session_factory: async_sessionmaker[AsyncSession], raw_key: str) -> AccountRow:
    async with session_factory() as session:
        account = AccountRow(broker="ctrader", external_account_id="1", mode="hedging", currency="USD", enabled=True)
        session.add(account)
        await session.flush()
        session.add(
            ApiClientRow(
                account_id=account.id,
                name="bot7",
                key_prefix=key_prefix(raw_key),
                api_key_hmac=hash_api_key(raw_key, pepper="test-pepper"),
                scopes=["prices:read"],
            )
        )
        await session.commit()
    return account


@pytest.fixture
async def client_setup(
    migrated_postgres_url: str,
    redis_url: str,
    rabbitmq_url: str,
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[AsyncClient, AccountRow]:
    raw_key = f"sk_live_{uuid4().hex}"
    account = await _seed_account_and_api_key(session_factory, raw_key)
    app = create_app(settings=_settings(migrated_postgres_url, redis_url, rabbitmq_url))
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://test", headers={"X-API-Key": raw_key})
    return client, account


@pytest.mark.asyncio
async def test_get_price_returns_the_cached_quote(
    client_setup: tuple[AsyncClient, AccountRow], redis_client: redis.Redis
) -> None:
    client, account = client_setup
    await RedisPriceCacheRepository(redis_client).set(
        account_id=str(account.id),
        symbol="EURUSD",
        bid=Decimal("1.08231"),
        ask=Decimal("1.08245"),
        timestamp=datetime.now(UTC).isoformat(),
        ttl_seconds=60,
    )

    async with client:
        response = await client.get("/api/v1/prices/EURUSD")

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "EURUSD"
    assert Decimal(str(body["bid"])) == Decimal("1.08231")
    assert Decimal(str(body["ask"])) == Decimal("1.08245")
    assert Decimal(str(body["spread"])) == Decimal("0.00014")
    assert body["stale"] is False


@pytest.mark.asyncio
async def test_get_price_flags_a_stale_quote(
    client_setup: tuple[AsyncClient, AccountRow], redis_client: redis.Redis
) -> None:
    client, account = client_setup
    stale_timestamp = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
    await RedisPriceCacheRepository(redis_client).set(
        account_id=str(account.id),
        symbol="EURUSD",
        bid=Decimal("1.08231"),
        ask=Decimal("1.08245"),
        timestamp=stale_timestamp,
        ttl_seconds=60,
    )

    async with client:
        response = await client.get("/api/v1/prices/EURUSD")

    assert response.status_code == 200
    assert response.json()["stale"] is True


@pytest.mark.asyncio
async def test_get_price_returns_503_on_a_cache_miss(client_setup: tuple[AsyncClient, AccountRow]) -> None:
    client, _account = client_setup

    async with client:
        response = await client.get("/api/v1/prices/GBPUSD")

    assert response.status_code == 503
    assert response.json()["code"] == "PRICE_UNAVAILABLE"


@pytest.mark.asyncio
async def test_get_prices_batch_omits_symbols_without_a_cached_quote(
    client_setup: tuple[AsyncClient, AccountRow], redis_client: redis.Redis
) -> None:
    client, account = client_setup
    await RedisPriceCacheRepository(redis_client).set(
        account_id=str(account.id),
        symbol="EURUSD",
        bid=Decimal("1.08231"),
        ask=Decimal("1.08245"),
        timestamp=datetime.now(UTC).isoformat(),
        ttl_seconds=60,
    )

    async with client:
        response = await client.get("/api/v1/prices", params={"symbols": "EURUSD,GBPUSD"})

    assert response.status_code == 200
    prices = response.json()["prices"]
    assert [p["symbol"] for p in prices] == ["EURUSD"]
