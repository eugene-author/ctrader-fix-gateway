"""Repository Protocols."""

from datetime import datetime
from typing import Protocol
from uuid import UUID

from app.domain.enums import OrderStatus
from app.domain.models import Execution, Order, Position, Symbol


class SymbolRepository(Protocol):
    """Read and write access to tradable symbols and their trading constraints."""

    async def get_by_name(self, account_id: UUID, name: str) -> Symbol | None:
        """Fetch a symbol by its display name (e.g. "EURUSD"), or None if unknown."""
        ...

    async def get_by_id(self, symbol_id: UUID) -> Symbol | None:
        """Fetch a symbol by its primary key, or None if it doesn't exist."""
        ...

    async def has_any(self, account_id: UUID) -> bool:
        """Report whether any symbol row exists for this account."""
        ...

    async def list_by_account(self, account_id: UUID) -> list[Symbol]:
        """Fetch every known symbol for this account, discovered or not."""
        ...

    async def upsert(self, symbol: Symbol) -> None:
        """Insert or update the row matching (account_id, name) with `symbol`'s full field values."""
        ...


class OrderRepository(Protocol):
    """Persistence for the Order model."""

    async def add(self, order: Order) -> None:
        """Stage a new order for insertion on the next flush/commit."""
        ...

    async def update(self, order: Order) -> None:
        """Persist changes to an already-existing order."""
        ...

    async def get(self, order_id: UUID) -> Order | None:
        """Fetch an order by its primary key, or None if it doesn't exist."""
        ...

    async def get_by_client_order_id(self, account_id: UUID, client_order_id: str) -> Order | None:
        """Fetch an order by its client-supplied id, or None if it doesn't exist."""
        ...

    async def get_by_fix_cl_ord_id(self, fix_cl_ord_id: str) -> Order | None:
        """Fetch an order by the ClOrdID it was dispatched with, or None if it doesn't exist."""
        ...

    async def list_by_account_and_statuses(self, account_id: UUID, statuses: frozenset[OrderStatus]) -> list[Order]:
        """Fetch every order for this account currently in one of `statuses`."""
        ...

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
        ...


class ExecutionRepository(Protocol):
    """Persistence for the immutable Execution log."""

    async def add(self, execution: Execution) -> None:
        """Stage a new execution report for insertion on the next flush/commit."""
        ...


class PositionRepository(Protocol):
    """Persistence for the Position projection."""

    async def add(self, position: Position) -> None:
        """Stage a new position for insertion on the next flush/commit."""
        ...

    async def update(self, position: Position) -> None:
        """Persist changes to an already-existing position."""
        ...

    async def get(self, position_id: UUID) -> Position | None:
        """Fetch a position by its primary key, or None if it doesn't exist."""
        ...

    async def get_by_broker_position_id(
        self, account_id: UUID, position_id: str, *, for_update: bool = False
    ) -> Position | None:
        """Fetch a position by the broker's own position id, or None if it doesn't exist."""
        ...

    async def list_by_account(self, account_id: UUID, *, status: str | None = None) -> list[Position]:
        """Fetch every position for this account, optionally filtered to one status."""
        ...


class OutboxRepository(Protocol):
    """Writes to the transactional outbox."""

    async def add(
        self,
        *,
        account_id: UUID,
        aggregate_type: str,
        aggregate_id: UUID,
        event_type: str,
        payload: dict[str, object],
        routing_key: str,
        correlation_id: str,
        causation_id: str,
    ) -> None:
        """Stage a new outbox row for insertion on the next flush/commit."""
        ...


class InboxRepository(Protocol):
    """At least once delivery deduplication."""

    async def claim(self, *, consumer: str, message_id: str, payload_hash: str) -> bool:
        """Claim this message for `consumer`. True if we got it first."""
        ...
