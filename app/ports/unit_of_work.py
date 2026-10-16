"""The Unit of Work Protocol."""

from types import TracebackType
from typing import Protocol, Self

from app.ports.repositories import (
    ExecutionRepository,
    InboxRepository,
    OrderRepository,
    OutboxRepository,
    PositionRepository,
    SymbolRepository,
)


class UnitOfWork(Protocol):
    """One transaction's worth of repositories, committed or rolled back as a unit."""

    orders: OrderRepository
    executions: ExecutionRepository
    positions: PositionRepository
    outbox: OutboxRepository
    symbols: SymbolRepository
    inbox: InboxRepository

    async def __aenter__(self) -> Self:
        """Open the transaction and construct the repositories that share it."""
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Roll back on an escaping exception, then always release resources."""
        ...

    async def commit(self) -> None:
        """Commit the transaction's pending work."""
        ...

    async def rollback(self) -> None:
        """Roll back the transaction's pending work."""
        ...
