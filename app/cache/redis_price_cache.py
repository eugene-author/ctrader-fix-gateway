"""Price cache key/channel naming, and the Redis-backed cache/pub-sub adapters built on them.

fix-worker writes through RedisPriceCacheRepository.set()/RedisQuotePublisher.publish()
directly, the API reads through RedisPriceCacheRepository, behind the
PriceCacheRepository port, so the application layer doesn't import redis directly.
"""

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

import redis.asyncio as redis

from app.domain.models import Price, Quote


def price_key(account_id: UUID | str, symbol: str) -> str:
    """Build the Redis hash key one symbol's cached quote lives at, for one account."""
    return f"price:{account_id}:{symbol}"


def quotes_channel(symbol: str) -> str:
    """Build the Redis pub/sub channel /ws/prices subscribes to for one symbol."""
    return f"quotes:{symbol}"


class RedisPriceCacheRepository:
    """PriceCacheRepository backed by a Redis hash, one per (account, symbol)."""

    def __init__(self, client: redis.Redis) -> None:
        """Wrap an already-connected redis.asyncio client."""
        self._client = client

    async def get(self, account_id: UUID, symbol: str) -> Quote | None:
        """Fetch the cached quote, or None if no quote is cached."""
        data = cast(dict[str, str], await self._client.hgetall(price_key(account_id, symbol)))
        if not data:
            return None
        return Quote(
            symbol=symbol,
            bid=Price(Decimal(data["bid"])),
            ask=Price(Decimal(data["ask"])),
            timestamp=datetime.fromisoformat(data["timestamp"]).astimezone(UTC),
        )

    async def set(
        self, account_id: UUID | str, *, symbol: str, bid: Decimal, ask: Decimal, timestamp: str, ttl_seconds: int
    ) -> None:
        """HSET the cached quote with an expiry. Raises on failure - callers catch it."""
        key = price_key(account_id, symbol)
        await self._client.hset(key, mapping={"bid": str(bid), "ask": str(ask), "timestamp": timestamp})
        await self._client.expire(key, ttl_seconds)


class RedisQuotePublisher:
    """Publishes live quotes to their symbol's pub/sub channel, for /ws/prices."""

    def __init__(self, client: redis.Redis) -> None:
        """Wrap an already-connected redis.asyncio client."""
        self._client = client

    async def publish(self, *, symbol: str, bid: Decimal, ask: Decimal, timestamp: str) -> None:
        """PUBLISH the quote to its symbol's channel. Raises on failure."""
        await self._client.publish(
            quotes_channel(symbol),
            json.dumps({"type": "quote", "symbol": symbol, "bid": str(bid), "ask": str(ask), "timestamp": timestamp}),
        )
