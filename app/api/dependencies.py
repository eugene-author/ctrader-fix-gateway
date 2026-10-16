"""Shared FastAPI dependencies: settings, uow, handlers, queries."""

import redis.asyncio as redis
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.cache.redis_price_cache import RedisPriceCacheRepository
from app.core.config import Settings
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.ports.unit_of_work import UnitOfWork
from app.services.commands.cancel_order import CancelOrderHandler
from app.services.commands.close_position import ClosePositionHandler
from app.services.commands.place_order import PlaceOrderHandler
from app.services.queries.get_order import GetOrderQuery, GetOrderStatusQuery
from app.services.queries.get_position import GetPositionQuery, ListPositionsQuery
from app.services.queries.get_price import GetPriceQuery
from app.services.queries.list_orders import ListOrdersQuery
from app.services.queries.list_symbols import ListSymbolsQuery


def get_settings(request: Request) -> Settings:
    """Return the Settings the app was built with."""
    return request.app.state.settings


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    """Return the app's shared session factory (built once, at startup)."""
    return request.app.state.session_factory


def get_unit_of_work(request: Request) -> UnitOfWork:
    """Build a fresh UnitOfWork for this request, sharing the app's session factory."""
    return SqlAlchemyUnitOfWork(get_session_factory(request))


def get_correlation_id(request: Request) -> str:
    """Return this request's correlation id, bound by CorrelationIdMiddleware."""
    return getattr(request.state, "correlation_id", "")


def get_place_order_handler(request: Request) -> PlaceOrderHandler:
    """Build a PlaceOrderHandler sharing the app's session factory."""
    session_factory = get_session_factory(request)
    return PlaceOrderHandler(lambda: SqlAlchemyUnitOfWork(session_factory))


def get_order_query(request: Request) -> GetOrderQuery:
    """Build a GetOrderQuery sharing the app's session factory."""
    session_factory = get_session_factory(request)
    return GetOrderQuery(lambda: SqlAlchemyUnitOfWork(session_factory))


def get_order_status_query(request: Request) -> GetOrderStatusQuery:
    """Build a GetOrderStatusQuery sharing the app's session factory."""
    session_factory = get_session_factory(request)
    return GetOrderStatusQuery(lambda: SqlAlchemyUnitOfWork(session_factory))


def get_orders_list_query(request: Request) -> ListOrdersQuery:
    """Build a ListOrdersQuery sharing the app's session factory."""
    session_factory = get_session_factory(request)
    return ListOrdersQuery(lambda: SqlAlchemyUnitOfWork(session_factory))


def get_redis_client(request: Request) -> redis.Redis:
    """Return the app's shared redis client (built once, at startup)."""
    return request.app.state.redis_client


def get_price_query(request: Request) -> GetPriceQuery:
    """Build a GetPriceQuery sharing the app's redis client."""
    settings = get_settings(request)
    cache = RedisPriceCacheRepository(get_redis_client(request))
    return GetPriceQuery(cache, stale_ms=settings.price_stale_ms)


def get_symbols_query(request: Request) -> ListSymbolsQuery:
    """Build a ListSymbolsQuery sharing the app's session factory."""
    session_factory = get_session_factory(request)
    return ListSymbolsQuery(lambda: SqlAlchemyUnitOfWork(session_factory))


def get_cancel_order_handler(request: Request) -> CancelOrderHandler:
    """Build a CancelOrderHandler sharing the app's session factory."""
    session_factory = get_session_factory(request)
    return CancelOrderHandler(lambda: SqlAlchemyUnitOfWork(session_factory))


def get_close_position_handler(request: Request) -> ClosePositionHandler:
    """Build a ClosePositionHandler sharing the app's session factory."""
    session_factory = get_session_factory(request)
    return ClosePositionHandler(lambda: SqlAlchemyUnitOfWork(session_factory))


def get_position_query(request: Request) -> GetPositionQuery:
    """Build a GetPositionQuery sharing the app's session factory."""
    session_factory = get_session_factory(request)
    return GetPositionQuery(lambda: SqlAlchemyUnitOfWork(session_factory))


def get_positions_query(request: Request) -> ListPositionsQuery:
    """Build a ListPositionsQuery sharing the app's session factory."""
    session_factory = get_session_factory(request)
    return ListPositionsQuery(lambda: SqlAlchemyUnitOfWork(session_factory))
