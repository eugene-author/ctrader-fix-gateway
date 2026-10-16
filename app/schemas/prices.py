"""GET /prices, GET /prices/{symbol} DTOs."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class PriceResource(BaseModel):
    """One symbol's cached quote."""

    symbol: str
    bid: Decimal
    ask: Decimal
    spread: Decimal
    timestamp: datetime
    age_ms: int
    stale: bool
    source: str


class PricesResponse(BaseModel):
    """GET /prices response body: a batch of price resources."""

    prices: list[PriceResource]
