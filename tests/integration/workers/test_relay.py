import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import RelaySettings
from app.core.metrics import cancel_dispatch_parked_total, outbox_parked_total, outbox_pending
from app.db.models import AccountRow, OrderRow, OutboxEventRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.enums import OrderStatus
from app.domain.events import ExecutionReportReceived
from app.domain.models import Order
from app.queue.serialization import build_envelope, decode_envelope
from app.queue.topology import COMMANDS_EXCHANGE, COMMANDS_QUEUE
from app.workers.projector import ProjectorHandler
from app.workers.relay import OutboxRelay


async def _build_relay(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
    migrated_postgres_url: str,
    rabbitmq_url: str,
) -> OutboxRelay:
    channel = await rabbitmq_connection.channel(publisher_confirms=True)
    exchange = await channel.get_exchange(COMMANDS_EXCHANGE)
    settings = RelaySettings(database_url=migrated_postgres_url, rabbitmq_url=rabbitmq_url)
    return OutboxRelay(session_factory, exchange, rabbitmq_connection, settings)


async def _seed_outbox_row(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    account_id,
    aggregate_id,
    aggregate_type: str = "order",
    event_type: str = "CreateOrderCommand",
    attempts: int = 0,
    available_at: datetime | None = None,
    trace_context: dict[str, str] | None = None,
) -> OutboxEventRow:
    async with session_factory() as session:
        row = OutboxEventRow(
            account_id=account_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload={"order_id": str(aggregate_id)},
            routing_key="order.create",
            correlation_id=str(aggregate_id),
            causation_id=str(aggregate_id),
            attempts=attempts,
            available_at=available_at or datetime.now(UTC),
            trace_context=trace_context,
        )
        session.add(row)
        await session.commit()
        await session.refresh(row)
    return row


async def _wait_for_message_id(connection: AbstractRobustConnection, queue_name: str, message_id: str):
    # outbox_events and commands.orders are shared, session-scoped fixtures
    # (real Postgres/RabbitMQ, not reset between tests), so other tests'
    # still-unpublished rows can legitimately share the queue with this
    # test's row. Drain until the specific message this test cares about
    # shows up, acking everything else along the way rather than assuming
    # this test's message is the only - or the next - one on the queue.
    channel = await connection.channel()
    queue = await channel.get_queue(queue_name)
    async with asyncio.timeout(5), queue.iterator() as queue_iter:
        async for message in queue_iter:
            await message.ack()
            if decode_envelope(message.body).message_id == message_id:
                return message
    raise AssertionError(f"message {message_id} not seen on {queue_name} within the timeout")


@pytest.mark.asyncio
async def test_publish_batch_publishes_a_due_row_and_marks_it_published(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
    migrated_postgres_url: str,
    rabbitmq_url: str,
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, _symbol = seeded_account_and_symbol
    aggregate_id = uuid4()
    row = await _seed_outbox_row(session_factory, account_id=account.id, aggregate_id=aggregate_id)

    relay = await _build_relay(session_factory, rabbitmq_connection, migrated_postgres_url, rabbitmq_url)
    await relay.publish_batch(max_attempts=10)

    message = await _wait_for_message_id(rabbitmq_connection, COMMANDS_QUEUE, str(row.id))
    envelope = decode_envelope(message.body)
    assert envelope.message_type == "CreateOrderCommand"
    assert envelope.account_id == str(account.id)

    async with session_factory() as session:
        refreshed = await session.get(OutboxEventRow, row.id)
    assert refreshed is not None
    assert refreshed.published_at is not None


@pytest.mark.asyncio
async def test_publish_batch_carries_the_rows_trace_context_into_the_amqp_headers(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
    migrated_postgres_url: str,
    rabbitmq_url: str,
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, _symbol = seeded_account_and_symbol
    aggregate_id = uuid4()
    trace_context = {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}
    row = await _seed_outbox_row(
        session_factory, account_id=account.id, aggregate_id=aggregate_id, trace_context=trace_context
    )

    relay = await _build_relay(session_factory, rabbitmq_connection, migrated_postgres_url, rabbitmq_url)
    await relay.publish_batch(max_attempts=10)

    message = await _wait_for_message_id(rabbitmq_connection, COMMANDS_QUEUE, str(row.id))
    assert (message.headers or {}).get("traceparent") == trace_context["traceparent"]


@pytest.mark.asyncio
async def test_publish_batch_ignores_a_row_not_yet_available(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
    migrated_postgres_url: str,
    rabbitmq_url: str,
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, _symbol = seeded_account_and_symbol
    aggregate_id = uuid4()
    row = await _seed_outbox_row(
        session_factory,
        account_id=account.id,
        aggregate_id=aggregate_id,
        available_at=datetime.now(UTC) + timedelta(hours=1),
    )

    relay = await _build_relay(session_factory, rabbitmq_connection, migrated_postgres_url, rabbitmq_url)
    await relay.publish_batch(max_attempts=10)

    async with session_factory() as session:
        refreshed = await session.get(OutboxEventRow, row.id)
    assert refreshed is not None
    assert refreshed.published_at is None
    assert refreshed.attempts == 0


@pytest.mark.asyncio
async def test_publish_batch_ignores_a_parked_row(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
    migrated_postgres_url: str,
    rabbitmq_url: str,
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, _symbol = seeded_account_and_symbol
    aggregate_id = uuid4()
    row = await _seed_outbox_row(session_factory, account_id=account.id, aggregate_id=aggregate_id, attempts=10)

    relay = await _build_relay(session_factory, rabbitmq_connection, migrated_postgres_url, rabbitmq_url)
    await relay.publish_batch(max_attempts=10)

    async with session_factory() as session:
        refreshed = await session.get(OutboxEventRow, row.id)
    assert refreshed is not None
    assert refreshed.published_at is None
    assert refreshed.attempts == 10  # untouched by publish_batch() - parked rows are excluded from its query


@pytest.mark.asyncio
async def test_handle_publish_failure_backs_off_before_the_attempt_cap(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
    migrated_postgres_url: str,
    rabbitmq_url: str,
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)
    async with session_factory() as session:
        session.add(
            OrderRow(
                id=order.id,
                account_id=order.account_id,
                client_order_id=order.client_order_id,
                symbol_id=order.symbol_id,
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity.value,
                status=OrderStatus.PENDING,
                purpose=order.purpose,
            )
        )
        await session.commit()
    row = await _seed_outbox_row(session_factory, account_id=account.id, aggregate_id=order.id)

    relay = await _build_relay(session_factory, rabbitmq_connection, migrated_postgres_url, rabbitmq_url)
    async with session_factory() as session:
        tracked_row = await session.get(OutboxEventRow, row.id)
        assert tracked_row is not None
        await relay.handle_publish_failure(session, tracked_row, RuntimeError("connection refused"), max_attempts=10)
        await session.commit()

    async with session_factory() as session:
        refreshed_row = await session.get(OutboxEventRow, row.id)
        refreshed_order = await session.get(OrderRow, order.id)
    assert refreshed_row is not None
    assert refreshed_row.attempts == 1
    assert refreshed_row.available_at > datetime.now(UTC)
    assert refreshed_row.last_error == "connection refused"
    assert refreshed_order is not None
    assert refreshed_order.status == OrderStatus.PENDING  # not yet exhausted


@pytest.mark.asyncio
async def test_handle_publish_failure_parks_and_fails_the_order_at_the_attempt_cap(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
    migrated_postgres_url: str,
    rabbitmq_url: str,
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)
    async with session_factory() as session:
        session.add(
            OrderRow(
                id=order.id,
                account_id=order.account_id,
                client_order_id=order.client_order_id,
                symbol_id=order.symbol_id,
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity.value,
                status=OrderStatus.PENDING,
                purpose=order.purpose,
            )
        )
        await session.commit()
    row = await _seed_outbox_row(session_factory, account_id=account.id, aggregate_id=order.id, attempts=9)
    parked_before = outbox_parked_total._value.get()

    relay = await _build_relay(session_factory, rabbitmq_connection, migrated_postgres_url, rabbitmq_url)
    async with session_factory() as session:
        tracked_row = await session.get(OutboxEventRow, row.id)
        assert tracked_row is not None
        await relay.handle_publish_failure(session, tracked_row, RuntimeError("connection refused"), max_attempts=10)
        await session.commit()

    async with session_factory() as session:
        refreshed_row = await session.get(OutboxEventRow, row.id)
        refreshed_order = await session.get(OrderRow, order.id)
    assert refreshed_row is not None
    assert refreshed_row.attempts == 10
    assert refreshed_order is not None
    assert refreshed_order.status == OrderStatus.FAILED
    assert refreshed_order.error_message is not None
    assert outbox_parked_total._value.get() == parked_before + 1


@pytest.mark.asyncio
async def test_handle_publish_failure_parks_a_cancel_without_touching_the_live_order(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
    migrated_postgres_url: str,
    rabbitmq_url: str,
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
) -> None:
    # aggregate_id for a CancelOrderCommand is the order being cancelled -
    # already live at the broker by definition (cancel_order.py refuses
    # anything outside its cancellable statuses). Parking the *command* must
    # not fail the *order*: it's still working, and a failure here would make
    # is_terminal() discard every later report for it (see below).
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)
    async with session_factory() as session:
        session.add(
            OrderRow(
                id=order.id,
                account_id=order.account_id,
                client_order_id=order.client_order_id,
                fix_cl_ord_id="cancelparked0001",
                symbol_id=order.symbol_id,
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity.value,
                status=OrderStatus.ACCEPTED,
                purpose=order.purpose,
            )
        )
        await session.commit()
    row = await _seed_outbox_row(
        session_factory, account_id=account.id, aggregate_id=order.id, event_type="CancelOrderCommand", attempts=9
    )
    parked_before = cancel_dispatch_parked_total._value.get()

    relay = await _build_relay(session_factory, rabbitmq_connection, migrated_postgres_url, rabbitmq_url)
    async with session_factory() as session:
        tracked_row = await session.get(OutboxEventRow, row.id)
        assert tracked_row is not None
        await relay.handle_publish_failure(session, tracked_row, RuntimeError("connection refused"), max_attempts=10)
        await session.commit()

    async with session_factory() as session:
        refreshed_row = await session.get(OutboxEventRow, row.id)
        refreshed_order = await session.get(OrderRow, order.id)
    assert refreshed_row is not None
    assert refreshed_row.attempts == 10
    assert refreshed_order is not None
    assert refreshed_order.status == OrderStatus.ACCEPTED  # untouched - the order is still live
    assert cancel_dispatch_parked_total._value.get() == parked_before + 1

    # The real damage the old behaviour caused: a FAILED order is terminal,
    # so a genuine fill arriving afterwards would be silently discarded.
    # Prove that doesn't happen here.
    event = ExecutionReportReceived(
        fix_cl_ord_id="cancelparked0001",
        exec_id="exec-cancelparked-1",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=datetime.now(UTC).strftime("%Y%m%d-%H:%M:%S.%f")[:-3],
        raw="8=FIX.4.4|...",
        last_qty="1000",
        last_price="1.08243",
        position_id="pos-cancelparked-1",
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )
    handler = ProjectorHandler(lambda: SqlAlchemyUnitOfWork(session_factory))
    await handler.handle(envelope)

    async with session_factory() as session:
        filled_order = await session.get(OrderRow, order.id)
    assert filled_order is not None
    assert filled_order.status == OrderStatus.FILLED  # the report still applied - nothing was discarded


@pytest.mark.asyncio
async def test_publish_batch_updates_the_outbox_pending_gauge(
    session_factory: async_sessionmaker[AsyncSession],
    rabbitmq_connection: AbstractRobustConnection,
    migrated_postgres_url: str,
    rabbitmq_url: str,
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
) -> None:
    account, _symbol = seeded_account_and_symbol
    # Park one row (excluded from the query outbox_pending counts against, so
    # it should NOT be reflected) and leave one genuinely pending.
    await _seed_outbox_row(session_factory, account_id=account.id, aggregate_id=uuid4(), attempts=0)

    relay = await _build_relay(session_factory, rabbitmq_connection, migrated_postgres_url, rabbitmq_url)
    await relay.publish_batch(max_attempts=10)

    async with session_factory() as session:
        stmt = select(func.count()).select_from(OutboxEventRow).where(OutboxEventRow.published_at.is_(None))
        expected_pending = (await session.execute(stmt)).scalar_one()
    assert outbox_pending._value.get() == expected_pending
