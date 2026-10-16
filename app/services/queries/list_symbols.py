"""ListSymbolsQuery: the read side of GET /symbols."""

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID

from app.ports.unit_of_work import UnitOfWork


@dataclass(frozen=True, slots=True)
class SymbolView:
    """One symbol resource: its trading constraints."""

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


class ListSymbolsQuery:
    """List every known symbol for the requesting account."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        """Store the factory."""
        self._uow_factory = uow_factory

    async def handle(self, account_id: UUID) -> list[SymbolView]:
        """Return every symbol row data for this account."""
        async with self._uow_factory() as uow:
            symbols = await uow.symbols.list_by_account(account_id)

        return [
            SymbolView(
                symbol=symbol.name,
                enabled=symbol.enabled,
                subscribe=symbol.subscribe,
                needs_review=symbol.needs_review,
                broker_symbol_id=symbol.broker_symbol_id,
                lot_size=symbol.lot_size.value if symbol.lot_size is not None else None,
                qty_step=symbol.qty_step.value if symbol.qty_step is not None else None,
                min_qty=symbol.min_qty.value if symbol.min_qty is not None else None,
                max_qty=symbol.max_qty.value if symbol.max_qty is not None else None,
                price_step=symbol.price_step.value if symbol.price_step is not None else None,
            )
            for symbol in symbols
        ]
