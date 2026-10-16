"""Redis token-bucket rate limit on POST /orders.

A Lua script makes the read-refill-check-decrement sequence atomic - two
concurrent requests for the same client must do not both read the bucket
before either writes it back.
"""

import time
from uuid import UUID

import redis.asyncio as redis
from fastapi import Depends

from app.api.auth import ApiClient, get_authenticated_client
from app.api.dependencies import get_redis_client, get_settings
from app.core.config import Settings
from app.domain.errors import RateLimitedError

_RATE_LIMIT_LUA_SCRIPT = """
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tokens == nil then
    tokens = burst
    ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(burst, tokens + elapsed * rate)

local allowed = 0
local retry_after = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
else
    retry_after = (1 - tokens) / rate
end

redis.call("HMSET", key, "tokens", tokens, "ts", now)
redis.call("EXPIRE", key, math.ceil(burst / rate) + 1)

return {allowed, tostring(retry_after)}
"""


class RedisTokenBucketRateLimiter:
    """An atomic Redis-backed token bucket, one bucket per client_id."""

    def __init__(
        self,
        redis_client: redis.Redis,
        *,
        rate_per_second: float,
        burst: float,
        key_prefix: str = "ratelimit:orders",
    ) -> None:
        """Store the client and this limiter's rate/burst/key policy."""
        self._client = redis_client
        self._rate_per_second = rate_per_second
        self._burst = burst
        self._key_prefix = key_prefix

    async def check(self, client_id: UUID) -> None:
        """Raise RateLimitedError if `client_id` has exhausted its token bucket."""
        allowed, retry_after = await self._client.eval(
            _RATE_LIMIT_LUA_SCRIPT,
            1,
            f"{self._key_prefix}:{client_id}",
            self._rate_per_second,
            self._burst,
            time.time(),
        )
        if not int(allowed):
            raise RateLimitedError(float(retry_after))


def get_rate_limiter(
    redis_client: redis.Redis = Depends(get_redis_client),
    settings: Settings = Depends(get_settings),
) -> RedisTokenBucketRateLimiter:
    """Build the orders rate limiter from the app's shared redis client and settings."""
    return RedisTokenBucketRateLimiter(
        redis_client,
        rate_per_second=settings.rate_limit_orders_per_second,
        burst=settings.rate_limit_orders_burst,
    )


async def rate_limit_orders(
    client: ApiClient = Depends(get_authenticated_client),
    limiter: RedisTokenBucketRateLimiter = Depends(get_rate_limiter),
) -> None:
    """Enforces the per-client token bucket for POST /orders."""
    await limiter.check(client.id)
