"""The outbox relay process: publishes outbox_events onto ctrader.commands.

A separate process from fix-worker and the projector
"""

import asyncio
import contextlib
import signal
from datetime import UTC, datetime, timedelta

import aio_pika
import structlog
from aio_pika.abc import AbstractExchange, AbstractRobustConnection
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import RelaySettings
from app.core.logging import configure_logging
from app.core.metrics import (
    cancel_dispatch_parked_total,
    outbox_parked_total,
    outbox_pending,
    protection_failures_total,
    queue_depth,
)
from app.core.metrics_server import start_metrics_server
from app.core.sentry import init_sentry
from app.core.tracing import init_tracing
from app.db.models import OrderRow, OutboxEventRow, PositionRow
from app.db.session import create_engine, create_session_factory
from app.domain.enums import OrderStatus, ProtectionState
from app.domain.state_machine import is_terminal
from app.queue.serialization import build_envelope, encode_envelope
from app.queue.topology import (
    COMMANDS_EXCHANGE,
    COMMANDS_QUEUE,
    DLQ_COMMANDS_QUEUE,
    DLQ_EVENTS_QUEUE,
    EVENTS_QUEUE,
    declare_topology,
)

_MONITORED_QUEUES = (COMMANDS_QUEUE, EVENTS_QUEUE, DLQ_COMMANDS_QUEUE, DLQ_EVENTS_QUEUE)

logger = structlog.get_logger()

BATCH_LIMIT = 100
BACKOFF_CAP_SECONDS = 60.0
QUEUE_DEPTH_POLL_SECONDS = 5.0

_PARKING_FAILS_THE_ORDER = frozenset({"CreateOrderCommand", "ClosePositionCommand"})


class OutboxRelay:
    """Polls outbox_events and publishes due rows onto ctrader.commands."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        exchange: AbstractExchange,
        connection: AbstractRobustConnection,
        settings: RelaySettings,
    ) -> None:
        """Store collaborators; run() starts the poll loop and the queue-depth poller."""
        self._session_factory = session_factory
        self._exchange = exchange
        self._connection = connection
        self._settings = settings
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        """Poll outbox_events every outbox_poll_interval_seconds until stop() is called."""
        depth_task = asyncio.ensure_future(self.poll_queue_depths())
        logger.info("relay_started")
        try:
            while not self._stop_event.is_set():
                try:
                    await self.publish_batch()
                except Exception:
                    logger.exception("relay_drain_failed")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._settings.outbox_poll_interval_seconds)
        finally:
            depth_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await depth_task

    async def stop(self) -> None:
        """Signal run()'s poll loop and queue-depth poller to exit after their current tick."""
        self._stop_event.set()

    async def poll_queue_depths(self) -> None:
        """Keep queue_depth current for every monitored queue."""
        while not self._stop_event.is_set():
            for name in _MONITORED_QUEUES:
                try:
                    channel = await self._connection.channel()
                    queue = await channel.declare_queue(name, passive=True)
                    queue_depth.labels(queue=name).set(queue.declaration_result.message_count or 0)
                    await channel.close()
                except Exception:
                    logger.exception("queue_depth_poll_failed", queue=name)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=QUEUE_DEPTH_POLL_SECONDS)

    async def publish_batch(self, max_attempts: int | None = None) -> None:
        """Publish up to BATCH_LIMIT."""
        effective_max_attempts = max_attempts if max_attempts is not None else self._settings.outbox_max_attempts
        async with self._session_factory() as session:
            stmt = (
                select(OutboxEventRow)
                .where(
                    OutboxEventRow.published_at.is_(None),
                    OutboxEventRow.attempts < effective_max_attempts,
                    OutboxEventRow.available_at <= datetime.now(UTC),
                )
                .order_by(OutboxEventRow.created_at)
                .limit(BATCH_LIMIT)
                .with_for_update(skip_locked=True)
            )
            rows = (await session.execute(stmt)).scalars().all()
            for row in rows:
                await self._publish_one(session, row, max_attempts=effective_max_attempts)
            await session.commit()

            pending_stmt = select(func.count()).select_from(OutboxEventRow).where(OutboxEventRow.published_at.is_(None))
            pending_count = (await session.execute(pending_stmt)).scalar_one()
            outbox_pending.set(pending_count)

    async def _publish_one(self, session: AsyncSession, row: OutboxEventRow, *, max_attempts: int) -> None:
        # message_id equals outbox_events.id
        envelope = build_envelope(
            message_id=str(row.id),
            message_type=row.event_type,
            payload=row.payload,
            correlation_id=row.correlation_id,
            causation_id=row.causation_id,
            account_id=str(row.account_id),
        )
        try:
            await self._exchange.publish(
                aio_pika.Message(
                    body=encode_envelope(envelope),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json",
                    message_id=envelope.message_id,
                    correlation_id=row.correlation_id,
                    headers=dict(row.trace_context or {}),
                ),
                routing_key=row.routing_key,
            )
        except Exception as exc:
            await self.handle_publish_failure(session, row, exc, max_attempts=max_attempts)
            return
        row.published_at = datetime.now(UTC)

    async def handle_publish_failure(
        self, session: AsyncSession, row: OutboxEventRow, exc: Exception, *, max_attempts: int | None = None
    ) -> None:
        """Back off the row, or park it and fail its order if `max_attempts` is exhausted."""
        effective_max_attempts = max_attempts if max_attempts is not None else self._settings.outbox_max_attempts
        row.attempts += 1
        row.last_error = str(exc)
        logger.warning("outbox_event_publish_failed", outbox_id=str(row.id), attempts=row.attempts, error=str(exc))
        if row.attempts >= effective_max_attempts:
            outbox_parked_total.inc()
            logger.error("outbox_event_parked", outbox_id=str(row.id), attempts=row.attempts)

            if row.event_type in _PARKING_FAILS_THE_ORDER:
                order = await session.get(OrderRow, row.aggregate_id)
                if order is not None and not is_terminal(order.status):
                    order.status = OrderStatus.FAILED
                    order.error_message = f"outbox relay exhausted after {row.attempts} attempts: {row.last_error}"

            elif row.event_type == "CancelOrderCommand":
                # aggregate_id is a LIVE order at the broker
                cancel_dispatch_parked_total.inc()
                logger.error(
                    "cancel_dispatch_parked",
                    outbox_id=str(row.id),
                    order_id=str(row.aggregate_id),
                    error=row.last_error,
                )

            elif row.event_type == "PlaceProtectiveOrdersCommand":
                position = await session.get(PositionRow, row.aggregate_id)
                if position is not None:
                    position.protection_state = ProtectionState.FAILED
                protection_failures_total.labels(reason="OUTBOX_PARKED").inc()

        else:
            row.available_at = datetime.now(UTC) + timedelta(seconds=min(2**row.attempts, BACKOFF_CAP_SECONDS))


def main() -> None:
    """Load settings, configure logging, and run the poll."""
    settings = RelaySettings()
    configure_logging(env=settings.app_env, log_level=settings.app_log_level)
    init_sentry(dsn=settings.sentry_dsn, env=settings.app_env, traces_sample_rate=settings.sentry_traces_sample_rate)
    init_tracing(
        service_name=f"{settings.app_name}-relay",
        env=settings.app_env,
        otlp_endpoint=settings.otel_exporter_otlp_endpoint,
    )
    start_metrics_server(settings.metrics_port)
    asyncio.run(_run(settings))


async def _run(settings: RelaySettings) -> None:
    """Link up the DB and AMQP connections, build the OutboxRelay, and run it."""
    engine = create_engine(settings.database_url)
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
    session_factory = create_session_factory(engine)

    connection: AbstractRobustConnection = await aio_pika.connect_robust(settings.rabbitmq_url)
    await declare_topology(connection)
    channel = await connection.channel(publisher_confirms=True)
    exchange = await channel.get_exchange(COMMANDS_EXCHANGE)

    relay = OutboxRelay(session_factory, exchange, connection, settings)

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.ensure_future(relay.stop()))

    try:
        await relay.run()
    finally:
        await connection.close()
        await engine.dispose()
        logger.info("relay_shutdown_complete")


if __name__ == "__main__":
    main()
