"""Protocol for reading cached quotes."""

from typing import Protocol
from uuid import UUID

from app.domain.models import Quote


class PriceCacheRepository(Protocol):
    """Read-only access to the price cache fix-worker writes."""

    async def get(self, account_id: UUID, symbol: str) -> Quote | None:
        """Fetch the latest cached quote for `symbol`, or None if no quote is cached."""
        ...
