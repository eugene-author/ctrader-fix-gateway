"""SqlAlchemySymbolRepository: implements app.ports.repositories.SymbolRepository."""

from uuid import UUID

from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import SymbolRow
from app.domain.models import Price, Quantity, Symbol


def _to_domain(row: SymbolRow) -> Symbol:
    return Symbol(
        id=row.id,
        account_id=row.account_id,
        name=row.name,
        enabled=row.enabled,
        subscribe=row.subscribe,
        needs_review=row.needs_review,
        broker_symbol_id=row.broker_symbol_id,
        digits=row.digits,
        lot_size=Quantity(row.lot_size) if row.lot_size is not None else None,
        qty_step=Quantity(row.qty_step) if row.qty_step is not None else None,
        min_qty=Quantity(row.min_qty) if row.min_qty is not None else None,
        max_qty=Quantity(row.max_qty) if row.max_qty is not None else None,
        price_step=Price(row.price_step) if row.price_step is not None else None,
        source=row.source,
        last_seen_at=row.last_seen_at,
    )


class SqlAlchemySymbolRepository:
    """SymbolRepository backed by the `symbols` table."""

    def __init__(self, session: AsyncSession) -> None:
        """Wrap a session shared with the rest of the current unit of work."""
        self._session = session

    async def get_by_name(self, account_id: UUID, name: str) -> Symbol | None:
        """Fetch a symbol by its display name, or None if it doesn't exist."""
        stmt = select(SymbolRow).where(SymbolRow.account_id == account_id, SymbolRow.name == name)
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        return _to_domain(row) if row is not None else None

    async def get_by_id(self, symbol_id: UUID) -> Symbol | None:
        """Fetch a symbol by its primary key, or None if it doesn't exist."""
        row = await self._session.get(SymbolRow, symbol_id)
        return _to_domain(row) if row is not None else None

    async def has_any(self, account_id: UUID) -> bool:
        """Report whether any symbol row exists for this account (the cold-start gate)."""
        stmt = select(exists().where(SymbolRow.account_id == account_id))
        return bool((await self._session.execute(stmt)).scalar())

    async def list_by_account(self, account_id: UUID) -> list[Symbol]:
        """Fetch every known symbol for this account, discovered or not."""
        stmt = select(SymbolRow).where(SymbolRow.account_id == account_id)
        rows = (await self._session.execute(stmt)).scalars()
        return [_to_domain(row) for row in rows]

    async def upsert(self, symbol: Symbol) -> None:
        """Insert or update the row matching (account_id, name) with `symbol`'s full field values."""
        values = {
            "id": symbol.id,
            "account_id": symbol.account_id,
            "name": symbol.name,
            "enabled": symbol.enabled,
            "subscribe": symbol.subscribe,
            "needs_review": symbol.needs_review,
            "broker_symbol_id": symbol.broker_symbol_id,
            "digits": symbol.digits,
            "lot_size": symbol.lot_size.value if symbol.lot_size is not None else None,
            "qty_step": symbol.qty_step.value if symbol.qty_step is not None else None,
            "min_qty": symbol.min_qty.value if symbol.min_qty is not None else None,
            "max_qty": symbol.max_qty.value if symbol.max_qty is not None else None,
            "price_step": symbol.price_step.value if symbol.price_step is not None else None,
            "source": symbol.source.value if symbol.source is not None else None,
            "last_seen_at": symbol.last_seen_at,
        }
        update_values = {k: v for k, v in values.items() if k not in ("id", "account_id", "name")}
        stmt = (
            pg_insert(SymbolRow)
            .values(**values)
            .on_conflict_do_update(index_elements=["account_id", "name"], set_=update_values)
        )
        await self._session.execute(stmt)
