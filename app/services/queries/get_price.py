"""GetPriceQuery: the read side of GET /prices and GET /prices/{symbol}."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from app.domain.errors import PriceUnavailableError
from app.ports.price_cache import PriceCacheRepository


@dataclass(frozen=True, slots=True)
class PriceView:
    """One symbol's price resource."""

    symbol: str
    bid: Decimal
    ask: Decimal
    spread: Decimal
    timestamp: datetime
    age_ms: int
    stale: bool
    source: str = "cache"


class GetPriceQuery:
    """Fetch one symbol's cached quote, or raise PriceUnavailableError if no quote is cached."""

    def __init__(self, cache: PriceCacheRepository, *, stale_ms: int) -> None:
        """Store the cache port and the staleness threshold."""
        self._cache = cache
        self._stale_ms = stale_ms

    async def handle(self, account_id: UUID, symbol: str) -> PriceView:
        """Return `symbol`'s latest cached quote, flagged stale if past the threshold."""
        quote = await self._cache.get(account_id, symbol)
        if quote is None:
            raise PriceUnavailableError(symbol)

        age_ms = max(0, int((datetime.now(UTC) - quote.timestamp).total_seconds() * 1000))
        return PriceView(
            symbol=quote.symbol,
            bid=quote.bid.value,
            ask=quote.ask.value,
            spread=quote.ask.value - quote.bid.value,
            timestamp=quote.timestamp,
            age_ms=age_ms,
            stale=age_ms > self._stale_ms,
        )
