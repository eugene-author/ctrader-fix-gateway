import asyncio

import pytest
from aio_pika.abc import AbstractRobustConnection

from app.queue.publisher import EnvelopeEventPublisher
from app.queue.serialization import decode_envelope
from app.queue.topology import EVENTS_QUEUE


@pytest.mark.asyncio
async def test_publish_lands_on_the_events_queue(rabbitmq_connection: AbstractRobustConnection) -> None:
    publisher = EnvelopeEventPublisher(rabbitmq_connection)

    await publisher.publish(
        routing_key="order.submitted",
        message_type="OrderSubmitted",
        payload={"order_id": "abc-123", "fix_cl_ord_id": "abc123"},
        correlation_id="corr-1",
        causation_id="corr-1",
        account_id="acct-1",
    )

    channel = await rabbitmq_connection.channel()
    queue = await channel.get_queue(EVENTS_QUEUE)
    message = await asyncio.wait_for(queue.get(), timeout=5)
    await message.ack()

    envelope = decode_envelope(message.body)
    assert envelope.message_type == "OrderSubmitted"
    assert envelope.account_id == "acct-1"
    assert envelope.payload == {"order_id": "abc-123", "fix_cl_ord_id": "abc123"}
