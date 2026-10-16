"""Generic command consumer with redelivery support."""

from collections.abc import Awaitable, Callable

import structlog
from aio_pika.abc import AbstractIncomingMessage, AbstractQueueIterator, AbstractRobustConnection
from opentelemetry import context as otel_context
from opentelemetry import trace

from app.core.metrics import messages_dead_lettered_total
from app.queue.serialization import Envelope, decode_envelope
from app.queue.topology import COMMANDS_QUEUE
from app.queue.tracing import extract_context

logger = structlog.get_logger()
tracer = trace.get_tracer(__name__)

MAX_REDELIVERIES = 5


class TransientHandlerError(Exception):
    """Raised by a handler to request requeue with backoff."""

    ...


def _redelivery_count(message: AbstractIncomingMessage) -> int:
    headers = message.headers or {}
    value = headers.get("x-delivery-count")
    if value is None:
        return 0
    if not isinstance(value, (int, str, bytes)):
        raise TypeError(f"x-delivery-count header has an unexpected type: {type(value)!r}")
    return int(value)


class EnvelopeEventConsumer:
    """Consumes one queue with prefetch, manual ack and redelivery."""

    def __init__(
        self,
        connection: AbstractRobustConnection,
        queue_name: str,
        handler: Callable[[Envelope], Awaitable[None]],
        *,
        prefetch_count: int = 1,
        max_redeliveries: int = MAX_REDELIVERIES,
    ) -> None:
        """Store the connection and this consumer's queue/handler/redelivery policy."""
        self._connection = connection
        self._queue_name = queue_name
        self._handler = handler
        self._prefetch_count = prefetch_count
        self._max_redeliveries = max_redeliveries
        self._queue_iterator: AbstractQueueIterator | None = None

    async def run(self) -> None:
        """Consume `queue_name` until stop() is called or the connection/channel closes."""
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=self._prefetch_count)
        queue = await channel.get_queue(self._queue_name)

        async with queue.iterator() as queue_iterator:
            self._queue_iterator = queue_iterator
            async for message in queue_iterator:
                await self.handle_message(message)

    async def stop(self) -> None:
        """Stop consuming. The pending run() call returns once its in-flight handler finishes."""
        if self._queue_iterator is not None:
            await self._queue_iterator.close()

    async def handle_message(self, message: AbstractIncomingMessage) -> None:
        """Decode, dispatch to the handler, and ack/requeue/dead-letter per the outcome."""
        try:
            envelope = decode_envelope(message.body)
        except Exception:
            logger.exception("command_undecodable", body_preview=message.body[:200])
            messages_dead_lettered_total.labels(queue=self._queue_name).inc()
            await message.reject(requeue=False)
            return

        token = otel_context.attach(extract_context(message.headers))
        try:
            with tracer.start_as_current_span(f"queue.consume {self._queue_name}"):
                await self._handler(envelope)
        except TransientHandlerError:
            if _redelivery_count(message) >= self._max_redeliveries:
                logger.error("command_redeliveries_exhausted", message_id=envelope.message_id)
                messages_dead_lettered_total.labels(queue=self._queue_name).inc()
                await message.reject(requeue=False)
            else:
                await message.reject(requeue=True)
            return
        except Exception:
            logger.exception("command_handler_failed", message_id=envelope.message_id)
            messages_dead_lettered_total.labels(queue=self._queue_name).inc()
            await message.reject(requeue=False)
            return
        finally:
            otel_context.detach(token)

        await message.ack()


async def consume(
    connection: AbstractRobustConnection,
    queue_name: str,
    handler: Callable[[Envelope], Awaitable[None]],
) -> None:
    """Consume `queue_name` with the standard prefetch/redelivery policy."""
    await EnvelopeEventConsumer(connection, queue_name, handler).run()


async def consume_commands(
    connection: AbstractRobustConnection,
    handler: Callable[[Envelope], Awaitable[None]],
) -> None:
    """Consume commands.orders with the standard prefetch/redelivery policy."""
    await consume(connection, COMMANDS_QUEUE, handler)
