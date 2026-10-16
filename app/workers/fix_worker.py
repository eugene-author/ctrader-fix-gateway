"""The fix-worker process entrypoint.

Inbound FIX messages are mapped and published, not written to the database
directly here. The projector is what persists them
"""

import asyncio

from twisted.internet import asyncioreactor

_event_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_event_loop)
asyncioreactor.install(eventloop=_event_loop)

import contextlib
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from itertools import batched
from typing import Protocol, cast

import aio_pika
import redis.asyncio as redis
import structlog
from aio_pika.abc import AbstractRobustConnection
from ctrader_fix import ResponseMessage
from opentelemetry import trace
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy.ext.asyncio import AsyncEngine
from twisted.internet import defer, reactor

from app.adapters.brokers.ctrader.client import create_client
from app.adapters.brokers.ctrader.execution_handler import map_execution_report
from app.adapters.brokers.ctrader.message_factory import CTraderMessageFactory
from app.adapters.brokers.ctrader.quote_handler import BID, OFFER, map_market_data
from app.adapters.brokers.ctrader.reject_handler import (
    map_business_message_reject,
    map_order_cancel_reject,
)
from app.adapters.brokers.ctrader.session_manager import SessionManager, SessionState
from app.adapters.brokers.ctrader.symbol_handler import map_security_list
from app.cache.redis_price_cache import RedisPriceCacheRepository, RedisQuotePublisher
from app.core.config import FixWorkerSettings
from app.core.logging import configure_logging
from app.core.metrics import (
    fix_handler_errors_total,
    quote_staleness_seconds,
    quotes_received_total,
)
from app.core.metrics_server import start_metrics_server
from app.core.sentry import init_sentry
from app.core.tracing import init_tracing
from app.db.session import create_engine, create_session_factory
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.events import SessionStateChanged
from app.ports.publisher import EventPublisher
from app.queue.consumer import EnvelopeEventConsumer
from app.queue.publisher import EnvelopeEventPublisher
from app.queue.topology import COMMANDS_QUEUE, declare_topology
from app.workers.order_command_handler import OrderCommandHandler
from app.workers.reconciliation import OrderDispatchReconciler, ProtectiveOrderReconciler

logger = structlog.get_logger()
tracer = trace.get_tracer(__name__)

_QUOTE_STALENESS_POLL_SECONDS = 5.0


class FixWorker:
    """Owns the fix-worker process's runtime state and start/shutdown lifecycle.

    Symbol registry, latest bid/ask, quote-received timestamps, reconnect
    count, and the command-consumer background tasks all live on
    the instance
    """

    def __init__(
        self,
        *,
        settings: FixWorkerSettings,
        trade_session: SessionManager,
        quote_session: SessionManager,
        trade_message_factory: CTraderMessageFactory,
        quote_message_factory: CTraderMessageFactory,
        publisher: EventPublisher,
        price_cache: RedisPriceCacheRepository,
        quote_publisher: RedisQuotePublisher,
        command_consumer: EnvelopeEventConsumer,
        reconciler: ProtectiveOrderReconciler,
        dispatch_reconciler: OrderDispatchReconciler,
        connection: AbstractRobustConnection,
        redis_client: redis.Redis,
        engine: AsyncEngine,
    ) -> None:
        """Store every collaborator."""
        self._settings = settings
        self._trade_session = trade_session
        self._quote_session = quote_session
        self._trade_message_factory = trade_message_factory
        self._quote_message_factory = quote_message_factory
        self._publisher = publisher
        self._price_cache = price_cache
        self._quote_publisher = quote_publisher
        self._command_consumer = command_consumer
        self._reconciler = reconciler
        self._dispatch_reconciler = dispatch_reconciler
        self._connection = connection
        self._redis_client = redis_client
        self._engine = engine
        self._symbol_registry: dict[int, str] = {}
        self._last_quotes: dict[str, dict[str, Decimal]] = {}
        self._last_quote_at: dict[str, float] = {}
        self._reconnect_count = 0
        self._consume_task: asyncio.Task[None] | None = None
        self._staleness_task: asyncio.Task[None] | None = None
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._subscription_generation = 0

    def _track(self, task: asyncio.Task[None]) -> None:
        """Hold a strong reference to a fire-and-forget task until it completes."""
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def run(self) -> None:
        """Log on TRADE, reconcile protective orders, log on QUOTE, then start consuming commands."""
        self._trade_session.start()
        await self._trade_session.ready()
        logger.info("trade_session_logged_on")

        # Mandatory startup reconciliation: re-place any protective
        # order that isn't currently live at the broker.
        await self._reconciler.reconcile()

        # Mandatory startup reconciliation: query broker status for any
        # order left DISPATCHING/DISPATCH_UNCERTAIN as an extra safety check
        await self._dispatch_reconciler.reconcile()

        self._quote_session.start()
        await self._quote_session.ready()
        logger.info("quote_session_logged_on")

        self._consume_task = asyncio.ensure_future(self._command_consumer.run())
        self._staleness_task = asyncio.ensure_future(self._update_quote_staleness_gauges())

    async def shutdown(self) -> None:
        """Stop consuming, log out both sessions, and release the DB/AMQP/Redis connections."""
        logger.info("fix_worker_shutting_down")
        if self._consume_task is not None:
            await self._command_consumer.stop()
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await asyncio.wait_for(self._consume_task, timeout=self._settings.graceful_timeout)
        if self._staleness_task is not None:
            self._staleness_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._staleness_task
        self._trade_session.send(self._trade_message_factory.logout())
        self._trade_session.stop()
        self._quote_session.send(self._quote_message_factory.logout())
        self._quote_session.stop()
        await self._connection.close()
        await self._redis_client.aclose()
        await self._engine.dispose()
        logger.info("fix_worker_shutdown_complete")

    def on_trade_state_change(self, state: SessionState) -> None:
        """SessionManager callback: track reconnects and publish SessionStateChanged."""
        if state is SessionState.CONNECTING:
            self._reconnect_count += 1
        logger.info("trade_session_state_changed", state=state.value)
        self._track(asyncio.ensure_future(self._publish_session_state_changed(state)))

    def on_trade_message(self, message: ResponseMessage) -> None:
        """SessionManager callback: dispatch an inbound TRADE message under _guarded()."""
        msg_type = str(message.getFieldValue(35))
        self._track(asyncio.ensure_future(_guarded(msg_type, self._handle_trade_message(message))))

    def on_quote_state_change(self, state: SessionState) -> None:
        """SessionManager callback: kick off symbol discovery on every QUOTE logon."""
        logger.info("quote_session_state_changed", state=state.value)
        if state is SessionState.LOGGED_ON:
            # Symbol discovery on every QUOTE logon, including reconnects.
            self._quote_session.send(self._quote_message_factory.security_list(req_id=str(uuid.uuid4())))

    def on_quote_message(self, message: ResponseMessage) -> None:
        """SessionManager callback: dispatch an inbound QUOTE message under _guarded()."""
        msg_type = str(message.getFieldValue(35))
        self._track(asyncio.ensure_future(_guarded(msg_type, self._handle_quote_message(message))))

    async def _publish_session_state_changed(self, state: SessionState) -> None:
        event = SessionStateChanged(session_type="TRADE", state=state.value, reconnect_count=self._reconnect_count)
        await self._publisher.publish(
            routing_key="session.state_changed",
            message_type="SessionStateChanged",
            payload=event.to_payload(),
            correlation_id=str(uuid.uuid4()),
            causation_id=str(uuid.uuid4()),
            account_id=self._settings.account_id,
        )

    async def _handle_trade_message(self, message: ResponseMessage) -> None:
        """Dispatch on tag 35. Session-level types (A/0/1/5) are handled by SessionManager itself."""
        msg_type = message.getFieldValue(35)
        logger.debug("trade_message_received", msg_type=msg_type, raw=message.getMessage().replace("\x01", "|"))
        account_id = self._settings.account_id

        if msg_type == "8":
            event = map_execution_report(message)
            await self._publisher.publish(
                routing_key="order.execution_report",
                message_type="ExecutionReportReceived",
                payload=event.to_payload(),
                correlation_id=str(uuid.uuid4()),
                causation_id=str(uuid.uuid4()),
                account_id=account_id,
            )
        elif msg_type == "9":
            failed = map_order_cancel_reject(message)
            await self._publisher.publish(
                routing_key="order.failed",
                message_type="OrderFailed",
                payload=failed.to_payload(),
                correlation_id=str(uuid.uuid4()),
                causation_id=str(uuid.uuid4()),
                account_id=account_id,
            )
        elif msg_type == "j":
            failed = map_business_message_reject(message)
            await self._publisher.publish(
                routing_key="order.failed",
                message_type="OrderFailed",
                payload=failed.to_payload(),
                correlation_id=str(uuid.uuid4()),
                causation_id=str(uuid.uuid4()),
                account_id=account_id,
            )
        elif msg_type == "3":
            # Session-level reject: log only - the message it complains about may not
            # even be one of ours to fail.
            logger.warning("trade_session_level_reject", text=message.getFieldValue(58))

    async def _handle_quote_message(self, message: ResponseMessage) -> None:
        """Dispatch on tag 35 for the QUOTE session: 35=y discovers symbols, 35=W/X carry prices."""
        msg_type = message.getFieldValue(35)
        logger.debug("quote_message_received", msg_type=msg_type, raw=message.getMessage().replace("\x01", "|"))

        if msg_type == "y":
            event = map_security_list(message)
            logger.info("security_list_received", symbol_count=len(event.symbols), raw=message.getMessage())
            for discovered in event.symbols:
                self._symbol_registry[discovered.broker_symbol_id] = discovered.name
            await self._publisher.publish(
                routing_key="symbols.received",
                message_type="SecurityListReceived",
                payload=event.to_payload(),
                correlation_id=str(uuid.uuid4()),
                causation_id=str(uuid.uuid4()),
                account_id=self._settings.account_id,
            )
            # Subscribing to every symbol at once overflows both the broker (which
            # starts rejecting as rate-limited) and our own Redis pool, so the
            # requests are batched and spaced instead.
            self._subscription_generation += 1
            generation = self._subscription_generation
            for batch in batched(event.symbols, self._settings.quote_subscription_batch_size, strict=False):
                if generation != self._subscription_generation:
                    logger.info("symbol_subscription_superseded", generation=generation)
                    return
                for discovered in batch:
                    # One request per symbol, since cTrader rejects a second subscription to
                    # the same symbol
                    request = self._quote_message_factory.market_data_request(
                        md_req_id=str(uuid.uuid4()),
                        broker_symbol_id=discovered.broker_symbol_id,
                        entry_type=BID,
                    )
                    self._quote_session.send(request)
                await asyncio.sleep(self._settings.quote_subscription_batch_delay_seconds)
        elif msg_type == "Y":
            # MarketDataRequestReject: the broker refused a subscription
            # (rate-limited, unknown symbol) - log only, there's no
            # domain event for a subscription that was never accepted
            logger.warning(
                "quote_subscription_rejected",
                md_req_id=message.getFieldValue(262),
                reason=message.getFieldValue(281),
                text=message.getFieldValue(58),
            )
        elif msg_type == "j":
            # BusinessMessageReject on the QUOTE session: a MarketDataRequest was
            # refused (e.g. "ALREADY_SUBSCRIBED")
            logger.warning(
                "quote_subscription_business_rejected",
                ref_id=message.getFieldValue(379),
                text=message.getFieldValue(58),
            )
        elif msg_type in ("W", "X"):
            await self._handle_market_data_update(message)

    async def _handle_market_data_update(self, message: ResponseMessage) -> None:
        update = map_market_data(message)
        symbol = self._symbol_registry.get(update.broker_symbol_id)
        if symbol is None:
            logger.warning("quote_unknown_symbol", broker_symbol_id=update.broker_symbol_id)
            return
        quotes_received_total.labels(symbol=symbol).inc()

        quote = self._last_quotes.setdefault(symbol, {})
        for entry in update.entries:
            if entry.entry_type == BID:
                quote["bid"] = entry.price
            elif entry.entry_type == OFFER:
                quote["ask"] = entry.price
        if "bid" not in quote or "ask" not in quote:
            return  # wait for the other side - a quote needs both bid and ask
        self._last_quote_at[symbol] = time.monotonic()

        timestamp = datetime.now(UTC).isoformat()
        try:
            await self._price_cache.set(
                self._settings.account_id,
                symbol=symbol,
                bid=quote["bid"],
                ask=quote["ask"],
                timestamp=timestamp,
                ttl_seconds=self._settings.price_ttl_seconds,
            )
            await self._quote_publisher.publish(symbol=symbol, bid=quote["bid"], ask=quote["ask"], timestamp=timestamp)
        except Exception:
            logger.exception("price_cache_write_failed", symbol=symbol)

    async def _update_quote_staleness_gauges(self) -> None:
        """Keep quote_staleness_seconds live between updates.

        The staleness alert needs a continuously-current value.
        """
        while True:
            await asyncio.sleep(_QUOTE_STALENESS_POLL_SECONDS)
            now = time.monotonic()
            for symbol, received_at in self._last_quote_at.items():
                quote_staleness_seconds.labels(symbol=symbol).set(now - received_at)


class _ReactorCoreProtocol(Protocol):
    """The subset of IReactorCore."""

    def callWhenRunning(self, callable_: Callable[[], None]) -> object:
        """Schedule `callable_` to run once the reactor starts (or immediately, if already running)."""
        ...

    def run(self) -> None:
        """Start the event loop; blocks until `stop()` is called."""
        ...

    def stop(self) -> None:
        """Shut the event loop down."""
        ...

    def addSystemEventTrigger(
        self, phase: str, event_type: str, callable_: Callable[..., object], *args: object
    ) -> object:
        """Register `callable_` to run during the named system event phase (e.g. "before"/"shutdown")."""
        ...


_reactor = cast(_ReactorCoreProtocol, reactor)


def main() -> None:
    """Load settings, configure logging, and hand off to the reactor."""
    settings = FixWorkerSettings()
    configure_logging(env=settings.app_env, log_level=settings.app_log_level)
    init_sentry(dsn=settings.sentry_dsn, env=settings.app_env, traces_sample_rate=settings.sentry_traces_sample_rate)
    init_tracing(
        service_name=f"{settings.app_name}-fix-worker",
        env=settings.app_env,
        otlp_endpoint=settings.otel_exporter_otlp_endpoint,
    )
    start_metrics_server(settings.metrics_port)
    _startup_failed = False

    def _on_run_done(future: asyncio.Future[None]) -> None:
        nonlocal _startup_failed
        if future.cancelled():
            return
        exc = future.exception()
        if exc is not None:
            logger.error("fix_worker_startup_failed", error=str(exc))
            _startup_failed = True
            _reactor.stop()

    def _start() -> None:
        asyncio.ensure_future(_build_and_run(settings)).add_done_callback(_on_run_done)

    _reactor.callWhenRunning(_start)
    _reactor.run()
    if _startup_failed:
        sys.exit(1)


async def _build_and_run(settings: FixWorkerSettings) -> None:
    """Composition of every collaborator, construct FixWorker, and start it."""
    engine = create_engine(settings.database_url)
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
    session_factory = create_session_factory(engine)

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    await declare_topology(connection)
    publisher = EnvelopeEventPublisher(connection)
    redis_client = redis.from_url(
        settings.redis_url, decode_responses=True, max_connections=settings.redis_max_connections
    )
    price_cache = RedisPriceCacheRepository(redis_client)
    quote_publisher = RedisQuotePublisher(redis_client)

    worker: FixWorker

    trade_config = settings.trade_fix_config()
    trade_message_factory = CTraderMessageFactory(trade_config)
    trade_client = create_client(trade_config, reconnect_max_delay=settings.fix_reconnect_max_delay)
    trade_session = SessionManager(
        session_type="TRADE",
        client=trade_client,
        config=trade_config,
        reactor=reactor,
        on_state_change=lambda state: worker.on_trade_state_change(state),
        on_message=lambda message: worker.on_trade_message(message),
    )

    quote_config = settings.quote_fix_config()
    quote_message_factory = CTraderMessageFactory(quote_config)
    quote_client = create_client(quote_config, reconnect_max_delay=settings.fix_reconnect_max_delay)
    quote_session = SessionManager(
        session_type="QUOTE",
        client=quote_client,
        config=quote_config,
        reactor=reactor,
        on_state_change=lambda state: worker.on_quote_state_change(state),
        on_message=lambda message: worker.on_quote_message(message),
    )

    def uow_factory() -> SqlAlchemyUnitOfWork:
        return SqlAlchemyUnitOfWork(session_factory)

    order_handler = OrderCommandHandler(
        session=trade_session,
        uow_factory=uow_factory,
        publisher=publisher,
        message_factory=trade_message_factory,
    )
    command_consumer = EnvelopeEventConsumer(connection, COMMANDS_QUEUE, order_handler.handle)
    reconciler = ProtectiveOrderReconciler(
        uow_factory, trade_session, trade_message_factory, uuid.UUID(settings.account_id)
    )
    dispatch_reconciler = OrderDispatchReconciler(
        uow_factory, trade_session, trade_message_factory, uuid.UUID(settings.account_id)
    )

    worker = FixWorker(
        settings=settings,
        trade_session=trade_session,
        quote_session=quote_session,
        trade_message_factory=trade_message_factory,
        quote_message_factory=quote_message_factory,
        publisher=publisher,
        price_cache=price_cache,
        quote_publisher=quote_publisher,
        command_consumer=command_consumer,
        reconciler=reconciler,
        dispatch_reconciler=dispatch_reconciler,
        connection=connection,
        redis_client=redis_client,
        engine=engine,
    )

    _reactor.addSystemEventTrigger(
        "before",
        "shutdown",
        lambda: defer.Deferred.fromFuture(asyncio.ensure_future(worker.shutdown())),
    )

    await worker.run()


async def _guarded(msg_type: str, coro: Awaitable[None]) -> None:
    """Run a fire-and-forget inbound-message handler, counting and logging any failure."""
    try:
        with tracer.start_as_current_span(f"fix.message_received {msg_type}"):
            await coro
    except Exception:
        fix_handler_errors_total.labels(msg_type=msg_type).inc()
        logger.exception("fix_message_handler_failed", msg_type=msg_type)


if __name__ == "__main__":
    main()
