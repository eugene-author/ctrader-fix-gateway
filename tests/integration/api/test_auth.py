from datetime import UTC, datetime

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.auth import get_authenticated_account_id
from app.core.api_keys import hash_api_key, key_prefix
from app.core.config import Settings
from app.core.exceptions import register_exception_handlers
from app.db.models import AccountRow, ApiClientRow
from app.main import create_app


def _settings(database_url: str) -> Settings:
    return Settings(
        database_url=database_url,
        redis_url="redis://localhost:6379/0",
        rabbitmq_url="amqp://guest:guest@localhost/",
        api_key_pepper="test-pepper",
    )


def _app_with_whoami_route(database_url: str) -> FastAPI:
    app = create_app(settings=_settings(database_url))
    register_exception_handlers(app)

    @app.get("/whoami")
    async def _whoami(account_id: str = Depends(get_authenticated_account_id)) -> dict[str, str]:
        return {"account_id": str(account_id)}

    return app


async def _seed_api_client(session_factory: async_sessionmaker[AsyncSession], raw_key: str) -> AccountRow:
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
                scopes=["orders:write"],
            )
        )
        await session.commit()
    return account


@pytest.mark.asyncio
async def test_valid_api_key_resolves_to_its_account(
    migrated_postgres_url: str, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    raw_key = "sk_live_valid0000000000000000"
    account = await _seed_api_client(session_factory, raw_key)

    app = _app_with_whoami_route(migrated_postgres_url)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/whoami", headers={"X-API-Key": raw_key})

    assert response.status_code == 200
    assert response.json()["account_id"] == str(account.id)


@pytest.mark.asyncio
async def test_missing_api_key_is_rejected(migrated_postgres_url: str) -> None:
    app = _app_with_whoami_route(migrated_postgres_url)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/whoami")

    assert response.status_code == 401
    assert response.json()["code"] == "UNAUTHORIZED"


@pytest.mark.asyncio
async def test_wrong_api_key_is_rejected(
    migrated_postgres_url: str, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await _seed_api_client(session_factory, "sk_live_correct000000000000000")

    app = _app_with_whoami_route(migrated_postgres_url)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/whoami", headers={"X-API-Key": "sk_live_wrongwrongwrongwrongwr"})

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_disabled_api_client_is_rejected(
    migrated_postgres_url: str, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    raw_key = "sk_live_disabled0000000000000"
    async with session_factory() as session:
        account = AccountRow(broker="ctrader", external_account_id="2", mode="hedging", currency="USD", enabled=True)
        session.add(account)
        await session.flush()
        session.add(
            ApiClientRow(
                account_id=account.id,
                name="disabled-bot",
                key_prefix=key_prefix(raw_key),
                api_key_hmac=hash_api_key(raw_key, pepper="test-pepper"),
                scopes=[],
                disabled_at=datetime.now(UTC),
            )
        )
        await session.commit()

    app = _app_with_whoami_route(migrated_postgres_url)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/whoami", headers={"X-API-Key": raw_key})

    assert response.status_code == 401
