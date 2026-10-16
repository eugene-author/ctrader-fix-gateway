import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
import redis.asyncio as redis
from starlette.testclient import TestClient

from app.cache.redis_price_cache import RedisQuotePublisher
from app.core.api_keys import hash_api_key, key_prefix
from app.core.config import Settings
from app.db.models import AccountRow, ApiClientRow
from app.main import create_app


def _settings(database_url: str, redis_url: str, rabbitmq_url: str) -> Settings:
    return Settings(
        database_url=database_url, redis_url=redis_url, rabbitmq_url=rabbitmq_url, api_key_pepper="test-pepper"
    )


async def _seed_account_and_api_key(session_factory, raw_key: str) -> AccountRow:
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


@pytest.mark.asyncio
async def test_ws_prices_streams_a_subscribed_quote(
    migrated_postgres_url: str,
    redis_url: str,
    rabbitmq_url: str,
    session_factory,
    redis_client: redis.Redis,
) -> None:
    raw_key = f"sk_live_{uuid4().hex}"
    await _seed_account_and_api_key(session_factory, raw_key)
    app = create_app(settings=_settings(migrated_postgres_url, redis_url, rabbitmq_url))

    # TestClient runs the ASGI app on its own thread/loop, separate from this
    # async test's - send_json() only guarantees the frame was written, not
    # that the server has finished awaiting pubsub.subscribe() yet.
    with TestClient(app).websocket_connect(f"/api/v1/ws/prices?api_key={raw_key}") as websocket:
        websocket.send_json({"action": "subscribe", "symbols": ["EURUSD"]})
        await asyncio.sleep(0.3)

        await RedisQuotePublisher(redis_client).publish(
            symbol="EURUSD",
            bid=Decimal("1.08231"),
            ask=Decimal("1.08245"),
            timestamp=datetime.now(UTC).isoformat(),
        )

        message = websocket.receive_json()

    assert message["type"] == "quote"
    assert message["symbol"] == "EURUSD"
    assert Decimal(message["bid"]) == Decimal("1.08231")
    assert Decimal(message["ask"]) == Decimal("1.08245")


@pytest.mark.asyncio
async def test_ws_prices_rejects_a_missing_api_key(
    migrated_postgres_url: str, redis_url: str, rabbitmq_url: str
) -> None:
    app = create_app(settings=_settings(migrated_postgres_url, redis_url, rabbitmq_url))

    with pytest.raises(Exception), TestClient(app).websocket_connect("/api/v1/ws/prices"):  # noqa: B017
        pass
