"""Protocol for publishing domain events onto the message bus."""

from typing import Protocol


class EventPublisher(Protocol):
    """Publishes one event envelope to the events exchange."""

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
        """Publish one event. Delivery is at least once."""
        ...
