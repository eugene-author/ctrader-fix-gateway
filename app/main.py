"""FastAPI application initialization."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from app.api.v1 import health, orders, positions, prices, symbols, ws_prices
from app.core import metrics_router
from app.core.config import Settings
from app.core.correlation import CorrelationIdMiddleware
from app.core.exceptions import register_exception_handlers
from app.core.logging import configure_logging
from app.core.metrics_middleware import MetricsMiddleware
from app.core.sentry import init_sentry
from app.core.tracing import init_tracing
from app.db.session import create_engine, create_session_factory


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the FastAPI app: configure logging, then mount every router."""
    settings = settings or Settings()
    configure_logging(env=settings.app_env, log_level=settings.app_log_level)
    init_sentry(dsn=settings.sentry_dsn, env=settings.app_env, traces_sample_rate=settings.sentry_traces_sample_rate)
    init_tracing(
        service_name=f"{settings.app_name}-api",
        env=settings.app_env,
        otlp_endpoint=settings.otel_exporter_otlp_endpoint,
    )

    engine = create_engine(settings.database_url)
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await engine.dispose()
            await redis_client.aclose()

    fastapi_app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        docs_url="/docs",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )
    fastapi_app.state.settings = settings
    fastapi_app.state.engine = engine
    fastapi_app.state.session_factory = create_session_factory(engine)
    fastapi_app.state.redis_client = redis_client
    FastAPIInstrumentor.instrument_app(fastapi_app)
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
    fastapi_app.add_middleware(MetricsMiddleware)
    fastapi_app.add_middleware(CorrelationIdMiddleware)
    register_exception_handlers(fastapi_app)
    fastapi_app.include_router(health.router, prefix="/health", tags=["health"])
    fastapi_app.include_router(metrics_router.router)
    fastapi_app.include_router(orders.router, prefix="/api/v1", tags=["orders"])
    fastapi_app.include_router(positions.router, prefix="/api/v1", tags=["positions"])
    fastapi_app.include_router(prices.router, prefix="/api/v1", tags=["prices"])
    fastapi_app.include_router(symbols.router, prefix="/api/v1", tags=["symbols"])
    fastapi_app.include_router(ws_prices.router, prefix="/api/v1", tags=["prices"])
    return fastapi_app


app = create_app()
