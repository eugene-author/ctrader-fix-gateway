from decimal import Decimal
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.api_keys import hash_api_key, key_prefix
from app.core.config import Settings
from app.db.models import AccountRow, ApiClientRow, SymbolRow
from app.main import create_app


def _settings(database_url: str, redis_url: str, rabbitmq_url: str) -> Settings:
    return Settings(
        database_url=database_url, redis_url=redis_url, rabbitmq_url=rabbitmq_url, api_key_pepper="test-pepper"
    )


async def _seed_account_symbol_and_api_key(
    session_factory: async_sessionmaker[AsyncSession], raw_key: str
) -> AccountRow:
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
                lot_size=Decimal(100_000),
                qty_step=Decimal(1000),
                min_qty=Decimal(1000),
                max_qty=Decimal(10_000_000),
                price_step=Decimal("0.00001"),
                enabled=True,
                subscribe=True,
                needs_review=False,
            )
        )
        session.add(
            SymbolRow(account_id=account.id, name="GBPUSD", broker_symbol_id=None, enabled=True, subscribe=True)
        )
        session.add(
            ApiClientRow(
                account_id=account.id,
                name="bot7",
                key_prefix=key_prefix(raw_key),
                api_key_hmac=hash_api_key(raw_key, pepper="test-pepper"),
                scopes=["symbols:read"],
            )
        )
        await session.commit()

    return account


@pytest.mark.asyncio
async def test_get_symbols_lists_every_known_symbol(
    migrated_postgres_url: str,
    redis_url: str,
    rabbitmq_url: str,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    raw_key = f"sk_live_{uuid4().hex}"
    await _seed_account_symbol_and_api_key(session_factory, raw_key)
    app = create_app(settings=_settings(migrated_postgres_url, redis_url, rabbitmq_url))
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test", headers={"X-API-Key": raw_key}) as client:
        response = await client.get("/api/v1/symbols")

    assert response.status_code == 200
    by_name = {s["symbol"]: s for s in response.json()["symbols"]}
    assert set(by_name) == {"EURUSD", "GBPUSD"}
    assert by_name["EURUSD"]["needs_review"] is False
    assert by_name["EURUSD"]["broker_symbol_id"] == 1
    assert Decimal(by_name["EURUSD"]["lot_size"]) == Decimal(100_000)
    assert by_name["GBPUSD"]["needs_review"] is True
    assert by_name["GBPUSD"]["broker_symbol_id"] is None
