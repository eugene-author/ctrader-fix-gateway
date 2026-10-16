"""SqlAlchemyOrderRepository: implements app.ports.repositories.OrderRepository."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import literal, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OrderRow
from app.domain.enums import OrderStatus
from app.domain.models import Order, Price, Quantity


def _to_domain(row: OrderRow) -> Order:
    return Order(
        id=row.id,
        account_id=row.account_id,
        client_order_id=row.client_order_id,
        symbol_id=row.symbol_id,
        side=row.side,
        order_type=row.order_type,
        quantity=Quantity(row.quantity),
        status=row.status,
        purpose=row.purpose,
        created_at=row.created_at,
        updated_at=row.updated_at,
        time_in_force=row.time_in_force,
        fix_cl_ord_id=row.fix_cl_ord_id,
        orig_cl_ord_id=row.orig_cl_ord_id,
        ctrader_order_id=row.ctrader_order_id,
        position_id=row.position_id,
        filled_quantity=Quantity(row.filled_quantity),
        avg_fill_price=Price(row.avg_fill_price) if row.avg_fill_price is not None else None,
        limit_price=Price(row.limit_price) if row.limit_price is not None else None,
        stop_price=Price(row.stop_price) if row.stop_price is not None else None,
        stop_loss=Price(row.stop_loss) if row.stop_loss is not None else None,
        take_profit=Price(row.take_profit) if row.take_profit is not None else None,
        reject_reason_code=row.reject_reason_code,
        error_message=row.error_message,
        closes_position_id=row.closes_position_id,
        protective_kind=row.protective_kind,
        protects_position_id=row.protects_position_id,
        parent_order_id=row.parent_order_id,
        oco_group_id=row.oco_group_id,
        idempotency_key=row.idempotency_key,
        request_fingerprint=row.request_fingerprint,
        correlation_id=row.correlation_id,
        last_broker_transact_at=row.last_broker_transact_at,
        version=row.version,
        submitted_at=row.submitted_at,
        terminal_at=row.terminal_at,
    )


def _to_row(order: Order) -> OrderRow:
    return OrderRow(
        id=order.id,
        account_id=order.account_id,
        client_order_id=order.client_order_id,
        symbol_id=order.symbol_id,
        side=order.side,
        order_type=order.order_type,
        time_in_force=order.time_in_force,
        quantity=order.quantity.value,
        status=order.status,
        purpose=order.purpose,
        fix_cl_ord_id=order.fix_cl_ord_id,
        orig_cl_ord_id=order.orig_cl_ord_id,
        ctrader_order_id=order.ctrader_order_id,
        position_id=order.position_id,
        filled_quantity=order.filled_quantity.value,
        avg_fill_price=order.avg_fill_price.value if order.avg_fill_price is not None else None,
        limit_price=order.limit_price.value if order.limit_price is not None else None,
        stop_price=order.stop_price.value if order.stop_price is not None else None,
        stop_loss=order.stop_loss.value if order.stop_loss is not None else None,
        take_profit=order.take_profit.value if order.take_profit is not None else None,
        reject_reason_code=order.reject_reason_code,
        error_message=order.error_message,
        closes_position_id=order.closes_position_id,
        protective_kind=order.protective_kind,
        protects_position_id=order.protects_position_id,
        parent_order_id=order.parent_order_id,
        oco_group_id=order.oco_group_id,
        idempotency_key=order.idempotency_key,
        request_fingerprint=order.request_fingerprint,
        correlation_id=order.correlation_id,
        last_broker_transact_at=order.last_broker_transact_at,
        version=order.version,
        submitted_at=order.submitted_at,
        terminal_at=order.terminal_at,
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


class SqlAlchemyOrderRepository:
    """OrderRepository backed by the `orders` table."""

    def __init__(self, session: AsyncSession) -> None:
        """Wrap a session shared with the rest of the current unit of work."""
        self._session = session
        self._loaded: dict[UUID, OrderRow] = {}

    async def add(self, order: Order) -> None:
        """Stage a new order row for insertion on the next flush/commit."""
        self._session.add(_to_row(order))

    async def update(self, order: Order) -> None:
        """Persist changes to an already-existing order."""
        row = self._loaded.get(order.id) or await self._session.get(OrderRow, order.id)
        if row is None:
            raise ValueError(f"Order {order.id} does not exist")
        row.fix_cl_ord_id = order.fix_cl_ord_id
        row.orig_cl_ord_id = order.orig_cl_ord_id
        row.ctrader_order_id = order.ctrader_order_id
        row.position_id = order.position_id
        row.filled_quantity = order.filled_quantity.value
        row.avg_fill_price = order.avg_fill_price.value if order.avg_fill_price is not None else None
        row.status = order.status
        row.reject_reason_code = order.reject_reason_code
        row.error_message = order.error_message
        row.closes_position_id = order.closes_position_id
        row.submitted_at = order.submitted_at
        row.terminal_at = order.terminal_at
        row.last_broker_transact_at = order.last_broker_transact_at

    async def get(self, order_id: UUID) -> Order | None:
        """Fetch an order by its primary key, or None if it doesn't exist."""
        row = await self._session.get(OrderRow, order_id, populate_existing=True)
        if row is None:
            return None
        self._loaded[row.id] = row
        return _to_domain(row)

    async def get_by_client_order_id(self, account_id: UUID, client_order_id: str) -> Order | None:
        """Fetch an order by its client-supplied id, or None if it doesn't exist."""
        stmt = (
            select(OrderRow)
            .where(OrderRow.account_id == account_id, OrderRow.client_order_id == client_order_id)
            .execution_options(populate_existing=True)
        )
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        if row is None:
            return None
        self._loaded[row.id] = row
        return _to_domain(row)

    async def get_by_fix_cl_ord_id(self, fix_cl_ord_id: str) -> Order | None:
        """Fetch an order by the ClOrdID it was dispatched with, or None if it doesn't exist."""
        stmt = select(OrderRow).where(OrderRow.fix_cl_ord_id == fix_cl_ord_id).execution_options(populate_existing=True)
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        if row is None:
            return None
        self._loaded[row.id] = row
        return _to_domain(row)

    async def list_by_account_and_statuses(self, account_id: UUID, statuses: frozenset[OrderStatus]) -> list[Order]:
        """Fetch every order for this account currently in one of `statuses`."""
        stmt = select(OrderRow).where(OrderRow.account_id == account_id, OrderRow.status.in_(statuses))
        rows = (await self._session.execute(stmt)).scalars()
        return [_to_domain(row) for row in rows]

    async def list_page(
        self,
        account_id: UUID,
        *,
        statuses: frozenset[OrderStatus] | None = None,
        symbol_id: UUID | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        cursor: tuple[datetime, UUID] | None = None,
        limit: int,
    ) -> list[Order]:
        """Fetch up to `limit` orders for this account, newest first (created_at, id both DESC)."""
        stmt = select(OrderRow).where(OrderRow.account_id == account_id)
        if statuses is not None:
            stmt = stmt.where(OrderRow.status.in_(statuses))
        if symbol_id is not None:
            stmt = stmt.where(OrderRow.symbol_id == symbol_id)
        if created_from is not None:
            stmt = stmt.where(OrderRow.created_at >= created_from)
        if created_to is not None:
            stmt = stmt.where(OrderRow.created_at <= created_to)
        if cursor is not None:
            cursor_created_at, cursor_id = cursor
            stmt = stmt.where(
                tuple_(OrderRow.created_at, OrderRow.id) < tuple_(literal(cursor_created_at), literal(cursor_id))
            )
        stmt = stmt.order_by(OrderRow.created_at.desc(), OrderRow.id.desc()).limit(limit)
        rows = (await self._session.execute(stmt)).scalars()
        return [_to_domain(row) for row in rows]
