"""RabbitMQ topology declaration. Idempotent: safe on every process startup."""

import aio_pika
from aio_pika.abc import AbstractRobustConnection

COMMANDS_EXCHANGE = "ctrader.commands"
EVENTS_EXCHANGE = "ctrader.events"
DLX_EXCHANGE = "ctrader.dlx"

COMMANDS_QUEUE = "commands.orders"
EVENTS_QUEUE = "events.projector"
DLQ_COMMANDS_QUEUE = "dlq.commands.orders"
DLQ_EVENTS_QUEUE = "dlq.events.projector"

_DLQ_COMMANDS_ROUTING_KEY = "dlq.commands"
_DLQ_EVENTS_ROUTING_KEY = "dlq.events"


async def declare_topology(connection: AbstractRobustConnection) -> None:
    """Declare every exchange and queue this project uses on a short-lived channel."""
    channel = await connection.channel()
    try:
        commands_exchange = await channel.declare_exchange(COMMANDS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
        events_exchange = await channel.declare_exchange(EVENTS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
        dlx_exchange = await channel.declare_exchange(DLX_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)

        commands_queue = await channel.declare_queue(
            COMMANDS_QUEUE,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": DLX_EXCHANGE,
                "x-dead-letter-routing-key": _DLQ_COMMANDS_ROUTING_KEY,
            },
        )
        await commands_queue.bind(commands_exchange, routing_key="order.*")
        await commands_queue.bind(commands_exchange, routing_key="position.*")

        events_queue = await channel.declare_queue(
            EVENTS_QUEUE,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": DLX_EXCHANGE,
                "x-dead-letter-routing-key": _DLQ_EVENTS_ROUTING_KEY,
            },
        )
        await events_queue.bind(events_exchange, routing_key="#")

        dlq_commands_queue = await channel.declare_queue(DLQ_COMMANDS_QUEUE, durable=True)
        await dlq_commands_queue.bind(dlx_exchange, routing_key=_DLQ_COMMANDS_ROUTING_KEY)

        dlq_events_queue = await channel.declare_queue(DLQ_EVENTS_QUEUE, durable=True)
        await dlq_events_queue.bind(dlx_exchange, routing_key=_DLQ_EVENTS_ROUTING_KEY)
    finally:
        await channel.close()
