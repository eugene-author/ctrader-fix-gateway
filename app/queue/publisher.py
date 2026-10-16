"""EnvelopeEventPublisher: implements app.ports.publisher.EventPublisher."""

import aio_pika
from aio_pika.abc import AbstractRobustConnection

from app.queue.serialization import build_envelope, encode_envelope
from app.queue.topology import EVENTS_EXCHANGE
from app.queue.tracing import inject_current_context


class EnvelopeEventPublisher:
    """Publishes events to the `ctrader.events` topic exchange, with publisher confirms."""

    def __init__(self, connection: AbstractRobustConnection) -> None:
        """Store the connection. A confirming channel is opened lazily on first publish."""
        self._connection = connection
        self._channel: aio_pika.abc.AbstractChannel | None = None

    async def _get_channel(self) -> aio_pika.abc.AbstractChannel:
        if self._channel is None or self._channel.is_closed:
            self._channel = await self._connection.channel(publisher_confirms=True)
        return self._channel

    async def publish(
        self,
        *,
        routing_key: str,
        message_type: str,
        payload: dict[str, object],
        correlation_id: str,
        causation_id: str,
        account_id: str,
    ) -> None:
        """Publish one event envelope, persistent, confirmed."""
        envelope = build_envelope(
            message_type=message_type,
            payload=payload,
            correlation_id=correlation_id,
            causation_id=causation_id,
            account_id=account_id,
        )
        channel = await self._get_channel()
        exchange = await channel.get_exchange(EVENTS_EXCHANGE)
        await exchange.publish(
            aio_pika.Message(
                body=encode_envelope(envelope),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
                message_id=envelope.message_id,
                correlation_id=correlation_id,
                headers=dict(inject_current_context()),
            ),
            routing_key=routing_key,
        )
