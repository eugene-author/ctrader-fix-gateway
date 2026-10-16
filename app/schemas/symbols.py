"""GET /symbols DTOs."""

from decimal import Decimal

from pydantic import BaseModel


class SymbolResource(BaseModel):
    """One symbol and its trading constraints."""

    symbol: str
    enabled: bool
    subscribe: bool
    needs_review: bool
    broker_symbol_id: int | None
    lot_size: Decimal | None
    qty_step: Decimal | None
    min_qty: Decimal | None
    max_qty: Decimal | None
    price_step: Decimal | None


class SymbolsResponse(BaseModel):
    """GET /symbols response body."""

    symbols: list[SymbolResource]
