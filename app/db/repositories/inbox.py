"""SqlAlchemyInboxRepository: implements app.ports.repositories.InboxRepository."""

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import InboxEventRow


class SqlAlchemyInboxRepository:
    """InboxRepository backed by the `inbox_events` table."""

    def __init__(self, session: AsyncSession) -> None:
        """Wrap a session shared with the rest of the current unit of work."""
        self._session = session

    async def claim(self, *, consumer: str, message_id: str, payload_hash: str) -> bool:
        """Claim this message for `consumer`. True if we got it first."""
        stmt = (
            pg_insert(InboxEventRow)
            .values(consumer=consumer, message_id=message_id, payload_hash=payload_hash)
            .on_conflict_do_nothing(index_elements=["consumer", "message_id"])
            .returning(InboxEventRow.consumer)
        )
        result = await self._session.execute(stmt)
        return result.first() is not None
