"""SqlAlchemyPositionRepository: implements app.ports.repositories.PositionRepository."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import PositionRow
from app.domain.models import Position, Price, Quantity


def _to_domain(row: PositionRow) -> Position:
    return Position(
        id=row.id,
        account_id=row.account_id,
        position_id=row.position_id,
        symbol_id=row.symbol_id,
        side=row.side,
        quantity=Quantity(row.quantity),
        entry_price=Price(row.entry_price),
        status=row.status,
        opened_at=row.opened_at,
        opened_by_order_id=row.opened_by_order_id,
        closed_by_order_id=row.closed_by_order_id,
        closed_at=row.closed_at,
        protection_state=row.protection_state,
        stop_loss_order_id=row.stop_loss_order_id,
        take_profit_order_id=row.take_profit_order_id,
        version=row.version,
    )


def _to_row(position: Position) -> PositionRow:
    return PositionRow(
        id=position.id,
        account_id=position.account_id,
        position_id=position.position_id,
        symbol_id=position.symbol_id,
        side=position.side,
        quantity=position.quantity.value,
        entry_price=position.entry_price.value,
        status=position.status,
        opened_at=position.opened_at,
        opened_by_order_id=position.opened_by_order_id,
        closed_by_order_id=position.closed_by_order_id,
        closed_at=position.closed_at,
        protection_state=position.protection_state,
        stop_loss_order_id=position.stop_loss_order_id,
        take_profit_order_id=position.take_profit_order_id,
        version=position.version,
    )


class SqlAlchemyPositionRepository:
    """PositionRepository backed by the `positions` table."""

    def __init__(self, session: AsyncSession) -> None:
        """Wrap a session shared with the rest of the current unit of work."""
        self._session = session
        self._loaded: dict[UUID, PositionRow] = {}

    async def add(self, position: Position) -> None:
        """Stage a new position row for insertion on the next commit."""
        self._session.add(_to_row(position))

    async def update(self, position: Position) -> None:
        """Persist changes to an already-existing position."""
        row = self._loaded.get(position.id) or await self._session.get(PositionRow, position.id)
        if row is None:
            raise ValueError(f"Position {position.id} does not exist")
        row.quantity = position.quantity.value
        row.entry_price = position.entry_price.value
        row.status = position.status
        row.closed_by_order_id = position.closed_by_order_id
        row.closed_at = position.closed_at
        row.protection_state = position.protection_state
        row.stop_loss_order_id = position.stop_loss_order_id
        row.take_profit_order_id = position.take_profit_order_id

    async def get(self, position_id: UUID) -> Position | None:
        """Fetch a position by its primary key, or None if it doesn't exist."""
        # populate_existing: see SqlAlchemyOrderRepository.get()'s identical
        # comment - re-read properly through the awaited path rather than
        # risk _to_domain() below hitting an expired attribute synchronously.
        row = await self._session.get(PositionRow, position_id, populate_existing=True)
        if row is None:
            return None
        self._loaded[row.id] = row
        return _to_domain(row)

    async def get_by_broker_position_id(
        self, account_id: UUID, position_id: str, *, for_update: bool = False
    ) -> Position | None:
        """Fetch a position by the broker's own position id, or None if it doesn't exist."""
        stmt = (
            select(PositionRow)
            .where(PositionRow.account_id == account_id, PositionRow.position_id == position_id)
            .execution_options(populate_existing=True)
        )
        if for_update:
            stmt = stmt.with_for_update()
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        if row is None:
            return None
        self._loaded[row.id] = row
        return _to_domain(row)

    async def list_by_account(self, account_id: UUID, *, status: str | None = None) -> list[Position]:
        """Fetch every position for this account, optionally filtered to one status."""
        stmt = select(PositionRow).where(PositionRow.account_id == account_id)
        if status is not None:
            stmt = stmt.where(PositionRow.status == status)
        rows = (await self._session.execute(stmt)).scalars()
        return [_to_domain(row) for row in rows]
