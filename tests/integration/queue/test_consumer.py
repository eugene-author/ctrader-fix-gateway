import asyncio
import contextlib

import aio_pika
import pytest
from aio_pika.abc import AbstractRobustConnection
from opentelemetry import trace

from app.core.metrics import messages_dead_lettered_total
from app.queue.consumer import consume_commands
from app.queue.serialization import Envelope, build_envelope, encode_envelope
from app.queue.topology import COMMANDS_EXCHANGE, COMMANDS_QUEUE, DLQ_COMMANDS_QUEUE, DLQ_EVENTS_QUEUE


async def _publish_command(
    connection: AbstractRobustConnection,
    envelope: Envelope,
    *,
    routing_key: str,
    headers: dict[str, str] | None = None,
) -> None:
    channel = await connection.channel()
    exchange = await channel.get_exchange(COMMANDS_EXCHANGE)
    await exchange.publish(
        aio_pika.Message(body=encode_envelope(envelope), headers=headers or {}),  # type: ignore[arg-type]
        routing_key=routing_key,
    )
    await channel.close()


async def _wait_for_one_message(connection: AbstractRobustConnection, queue_name: str):
    # queue.get() polls once and returns immediately if empty - it does not
    # wait for a future message. An iterator is a real (blocking) consumer.
    channel = await connection.channel()
    queue = await channel.get_queue(queue_name)
    async with asyncio.timeout(5), queue.iterator() as queue_iter:
        return await queue_iter.__anext__()


async def _run_until(connection: AbstractRobustConnection, handler, done: asyncio.Event) -> None:
    task = asyncio.create_task(consume_commands(connection, handler))
    try:
        await asyncio.wait_for(done.wait(), timeout=5)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_consume_commands_calls_handler_and_acks(rabbitmq_connection: AbstractRobustConnection) -> None:
    envelope = build_envelope(
        message_type="CreateOrderCommand",
        payload={"order_id": "abc-123"},
        correlation_id="corr-1",
        causation_id="corr-1",
        account_id="acct-1",
    )
    await _publish_command(rabbitmq_connection, envelope, routing_key="order.create")

    received: list[Envelope] = []
    done = asyncio.Event()

    async def handler(env: Envelope) -> None:
        received.append(env)
        done.set()

    await _run_until(rabbitmq_connection, handler, done)

    assert len(received) == 1
    assert received[0].message_type == "CreateOrderCommand"

    channel = await rabbitmq_connection.channel()
    queue = await channel.get_queue(COMMANDS_QUEUE)
    leftover = await queue.get(fail=False)
    assert leftover is None  # the message was acked, not left on the queue


@pytest.mark.asyncio
async def test_consume_activates_the_trace_context_from_the_messages_headers(
    rabbitmq_connection: AbstractRobustConnection,
) -> None:
    # 00-<32 hex trace id>-<16 hex span id>-01, per W3C traceparent syntax.
    trace_id_hex = "4bf92f3577b34da6a3ce929d0e0e4736"
    traceparent = f"00-{trace_id_hex}-00f067aa0ba902b7-01"
    envelope = build_envelope(
        message_type="CreateOrderCommand",
        payload={},
        correlation_id="corr-trace",
        causation_id="corr-trace",
        account_id="acct-1",
    )
    await _publish_command(
        rabbitmq_connection, envelope, routing_key="order.create", headers={"traceparent": traceparent}
    )

    observed_trace_ids: list[int] = []
    done = asyncio.Event()

    async def handler(env: Envelope) -> None:
        observed_trace_ids.append(trace.get_current_span().get_span_context().trace_id)
        done.set()

    await _run_until(rabbitmq_connection, handler, done)

    assert observed_trace_ids == [int(trace_id_hex, 16)]


@pytest.mark.asyncio
async def test_malformed_body_is_dead_lettered_immediately(rabbitmq_connection: AbstractRobustConnection) -> None:
    dead_lettered_before = messages_dead_lettered_total.labels(queue=COMMANDS_QUEUE)._value.get()
    channel = await rabbitmq_connection.channel()
    exchange = await channel.get_exchange(COMMANDS_EXCHANGE)
    await exchange.publish(aio_pika.Message(body=b"not json at all"), routing_key="order.create")
    await channel.close()

    done = asyncio.Event()

    async def handler(env: Envelope) -> None:
        done.set()  # should never be called

    task = asyncio.create_task(consume_commands(rabbitmq_connection, handler))
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(done.wait(), timeout=1)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    dead_letter = await _wait_for_one_message(rabbitmq_connection, DLQ_COMMANDS_QUEUE)
    assert dead_letter.body == b"not json at all"
    await dead_letter.ack()
    assert messages_dead_lettered_total.labels(queue=COMMANDS_QUEUE)._value.get() == dead_lettered_before + 1

    # Each source queue's x-dead-letter-routing-key keeps its failures out of
    # the other pipeline's DLQ - both DLQs used to bind "#" on the shared DLX
    # and would each receive every dead-lettered message.
    with pytest.raises(TimeoutError):
        await _wait_for_one_message(rabbitmq_connection, DLQ_EVENTS_QUEUE)


@pytest.mark.asyncio
async def test_permanent_handler_failure_is_dead_lettered(rabbitmq_connection: AbstractRobustConnection) -> None:
    envelope = build_envelope(
        message_type="CreateOrderCommand",
        payload={},
        correlation_id="corr-2",
        causation_id="corr-2",
        account_id="acct-1",
    )
    await _publish_command(rabbitmq_connection, envelope, routing_key="order.create")

    async def handler(env: Envelope) -> None:
        raise RuntimeError("boom")

    task = asyncio.create_task(consume_commands(rabbitmq_connection, handler))
    try:
        dead_letter = await _wait_for_one_message(rabbitmq_connection, DLQ_COMMANDS_QUEUE)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    await dead_letter.ack()
