"""SQLAlchemy implementation of app.ports.unit_of_work.UnitOfWork."""

from types import TracebackType
from typing import Self

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm.exc import StaleDataError

from app.db.repositories.executions import SqlAlchemyExecutionRepository
from app.db.repositories.inbox import SqlAlchemyInboxRepository
from app.db.repositories.orders import SqlAlchemyOrderRepository
from app.db.repositories.outbox import SqlAlchemyOutboxRepository
from app.db.repositories.positions import SqlAlchemyPositionRepository
from app.db.repositories.symbols import SqlAlchemySymbolRepository
from app.domain.errors import ConcurrentModificationError, DuplicateKeyError
from app.ports.repositories import (
    ExecutionRepository,
    InboxRepository,
    OrderRepository,
    OutboxRepository,
    PositionRepository,
    SymbolRepository,
)

_UNIQUE_VIOLATION_SQLSTATE = "23505"


class SqlAlchemyUnitOfWork:
    """One SQLAlchemy session shared by all repositories opened in the same `async with` block."""

    orders: OrderRepository
    executions: ExecutionRepository
    positions: PositionRepository
    outbox: OutboxRepository
    symbols: SymbolRepository
    inbox: InboxRepository

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        """Store the session factory.  No session is opened until __aenter__."""
        self._session_factory = session_factory
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> Self:
        """Open a session and construct the repositories that share it."""
        self._session = self._session_factory()
        self.orders = SqlAlchemyOrderRepository(self._session)
        self.executions = SqlAlchemyExecutionRepository(self._session)
        self.positions = SqlAlchemyPositionRepository(self._session)
        self.outbox = SqlAlchemyOutboxRepository(self._session)
        self.symbols = SqlAlchemySymbolRepository(self._session)
        self.inbox = SqlAlchemyInboxRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Roll back on an escaping exception, then always close the session."""
        session = self._require_session()
        if exc_type is not None:
            await session.rollback()
        await session.close()
        self._session = None

    async def commit(self) -> None:
        """Commit the session's pending work."""
        session = self._require_session()
        try:
            await session.commit()
        except StaleDataError as exc:
            await session.rollback()
            raise ConcurrentModificationError(str(exc)) from exc
        except IntegrityError as exc:
            await session.rollback()
            if getattr(exc.orig, "sqlstate", None) == _UNIQUE_VIOLATION_SQLSTATE:
                raise DuplicateKeyError(str(exc.orig)) from exc
            raise

    async def rollback(self) -> None:
        """Roll back the session's pending work."""
        await self._require_session().rollback()

    def _require_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("SqlAlchemyUnitOfWork used outside an 'async with' block")
        return self._session
