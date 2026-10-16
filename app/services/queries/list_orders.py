"""ListOrdersQuery: the read side of GET /orders, keyset-paginated."""

import base64
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from app.domain.enums import OrderStatus
from app.domain.errors import ValidationError
from app.ports.unit_of_work import UnitOfWork
from app.services.queries.get_order import OrderView, order_to_view

MAX_LIMIT = 200
DEFAULT_LIMIT = 50


@dataclass(frozen=True, slots=True)
class OrdersPage:
    """One page of orders, newest first."""

    orders: list[OrderView]
    next_cursor: str | None


def encode_cursor(created_at: datetime, order_id: UUID) -> str:
    """Render a (created_at, order_id) keyset position as an opaque, URL-safe string."""
    raw = f"{created_at.isoformat()}|{order_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    """Parse a cursor by encode_cursor back into (created_at, order_id)."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        created_at_raw, _, order_id_raw = raw.partition("|")
        return datetime.fromisoformat(created_at_raw), UUID(order_id_raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValidationError("Invalid cursor", code="INVALID_CURSOR") from exc


class ListOrdersQuery:
    """List orders for the requesting account, newest first, keyset-paginated."""

    def __init__(self, uow_factory: Callable[[], UnitOfWork]) -> None:
        """Store the factory."""
        self._uow_factory = uow_factory

    async def handle(
        self,
        account_id: UUID,
        *,
        statuses: frozenset[OrderStatus] | None = None,
        symbol: str | None = None,
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        cursor: str | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> OrdersPage:
        """Return up to `limit` orders (capped at MAX_LIMIT)."""
        effective_limit = min(limit, MAX_LIMIT)
        keyset = decode_cursor(cursor) if cursor is not None else None

        async with self._uow_factory() as uow:
            symbol_id: UUID | None = None
            if symbol is not None:
                resolved = await uow.symbols.get_by_name(account_id, symbol)
                if resolved is None:
                    return OrdersPage(orders=[], next_cursor=None)
                symbol_id = resolved.id

            rows = await uow.orders.list_page(
                account_id,
                statuses=statuses,
                symbol_id=symbol_id,
                created_from=created_from,
                created_to=created_to,
                cursor=keyset,
                limit=effective_limit + 1,
            )

        has_more = len(rows) > effective_limit
        page = rows[:effective_limit]
        next_cursor = encode_cursor(page[-1].created_at, page[-1].id) if has_more and page else None

        return OrdersPage(orders=[order_to_view(order) for order in page], next_cursor=next_cursor)
