"""SqlAlchemyOutboxRepository: implements app.ports.repositories.OutboxRepository."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEventRow
from app.queue.tracing import inject_current_context


class SqlAlchemyOutboxRepository:
    """OutboxRepository backed by the `outbox_events` table."""

    def __init__(self, session: AsyncSession) -> None:
        """Wrap a session shared with the rest of the current unit of work."""
        self._session = session

    async def add(
        self,
        *,
        account_id: UUID,
        aggregate_type: str,
        aggregate_id: UUID,
        event_type: str,
        payload: dict[str, object],
        routing_key: str,
        correlation_id: str,
        causation_id: str,
    ) -> None:
        """Stage a new outbox row for insertion on the next flush/commit."""
        self._session.add(
            OutboxEventRow(
                account_id=account_id,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                event_type=event_type,
                payload=payload,
                routing_key=routing_key,
                correlation_id=correlation_id,
                causation_id=causation_id,
                trace_context=inject_current_context() or None,
            )
        )
