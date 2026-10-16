import asyncio
from uuid import uuid4

import pytest
import redis.asyncio as redis

from app.api.rate_limit import RedisTokenBucketRateLimiter
from app.domain.errors import RateLimitedError


@pytest.mark.asyncio
async def test_requests_within_burst_are_all_allowed(redis_client: redis.Redis) -> None:
    client_id = uuid4()
    limiter = RedisTokenBucketRateLimiter(redis_client, rate_per_second=10.0, burst=5.0)
    for _ in range(5):
        await limiter.check(client_id)


@pytest.mark.asyncio
async def test_a_request_beyond_the_burst_is_rejected_with_a_retry_after(redis_client: redis.Redis) -> None:
    client_id = uuid4()
    limiter = RedisTokenBucketRateLimiter(redis_client, rate_per_second=1.0, burst=3.0)
    for _ in range(3):
        await limiter.check(client_id)

    with pytest.raises(RateLimitedError) as exc_info:
        await limiter.check(client_id)

    assert exc_info.value.retry_after_seconds > 0


@pytest.mark.asyncio
async def test_tokens_refill_over_time(redis_client: redis.Redis) -> None:
    client_id = uuid4()
    limiter = RedisTokenBucketRateLimiter(redis_client, rate_per_second=20.0, burst=2.0)
    for _ in range(2):
        await limiter.check(client_id)

    with pytest.raises(RateLimitedError):
        await limiter.check(client_id)

    await asyncio.sleep(0.1)  # 20/s means ~2 tokens back after 100ms

    await limiter.check(client_id)


@pytest.mark.asyncio
async def test_different_clients_have_independent_buckets(redis_client: redis.Redis) -> None:
    client_a, client_b = uuid4(), uuid4()
    limiter = RedisTokenBucketRateLimiter(redis_client, rate_per_second=1.0, burst=3.0)
    for _ in range(3):
        await limiter.check(client_a)

    with pytest.raises(RateLimitedError):
        await limiter.check(client_a)

    # client_b's bucket is untouched by client_a exhausting theirs.
    await limiter.check(client_b)
