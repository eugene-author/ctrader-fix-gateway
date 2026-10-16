"""Liveness and readiness endpoints."""

from dataclasses import dataclass

import aio_pika
import redis.asyncio as redis
import structlog
from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.dependencies import get_redis_client, get_session_factory, get_settings
from app.core.config import Settings

router = APIRouter()
logger = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class ReadinessResult:
    """The outcome of one /health/ready check."""

    database: bool
    redis: bool
    rabbitmq: bool

    @property
    def healthy(self) -> bool:
        """True only if every dependency is reachable."""
        return self.database and self.redis and self.rabbitmq


class ReadinessChecker:
    """Checks database, Redis and RabbitMQ reachability for /health/ready."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        redis_client: redis.Redis,
        rabbitmq_url: str,
    ) -> None:
        """Store the shared session factory/redis client and the RabbitMQ URL to probe."""
        self._session_factory = session_factory
        self._redis_client = redis_client
        self._rabbitmq_url = rabbitmq_url

    async def check(self) -> ReadinessResult:
        """Probe all three dependencies and return the combined result."""
        return ReadinessResult(
            database=await self._check_database(),
            redis=await self._check_redis(),
            rabbitmq=await self._check_rabbitmq(),
        )

    async def _check_database(self) -> bool:
        try:
            async with self._session_factory() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception:
            logger.warning("health_check_failed", dependency="database")
            return False

    async def _check_redis(self) -> bool:
        try:
            return bool(await self._redis_client.ping())
        except Exception:
            logger.warning("health_check_failed", dependency="redis")
            return False

    async def _check_rabbitmq(self) -> bool:
        try:
            connection = await aio_pika.connect(self._rabbitmq_url, timeout=5)
        except Exception:
            logger.warning("health_check_failed", dependency="rabbitmq")
            return False
        await connection.close()
        return True


def get_readiness_checker(
    session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
    redis_client: redis.Redis = Depends(get_redis_client),
    settings: Settings = Depends(get_settings),
) -> ReadinessChecker:
    """FastAPI dependency: build a ReadinessChecker sharing the app's session factory and redis client."""
    return ReadinessChecker(session_factory, redis_client, settings.rabbitmq_url)


@router.get("/live")
async def live() -> dict[str, str]:
    """Report that the process is alive, with no dependency checks."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(
    response: Response,
    checker: ReadinessChecker = Depends(get_readiness_checker),
) -> dict[str, object]:
    """Report whether the database, Redis and RabbitMQ are all reachable."""
    result = await checker.check()
    checks = {"database": result.database, "redis": result.redis, "rabbitmq": result.rabbitmq}
    if not result.healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        logger.warning("health_ready_failed", **checks)
    return {"status": "ok" if result.healthy else "unavailable", "checks": checks}
