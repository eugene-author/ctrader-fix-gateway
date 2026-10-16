"""SqlAlchemyExecutionRepository: implements app.ports.repositories.ExecutionRepository."""

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ExecutionRow
from app.domain.models import Execution


def _to_row(execution: Execution) -> ExecutionRow:
    return ExecutionRow(
        id=execution.id,
        order_id=execution.order_id,
        account_id=execution.account_id,
        exec_id=execution.exec_id,
        exec_type=execution.exec_type,
        ord_status=execution.ord_status,
        last_qty=execution.last_qty.value if execution.last_qty is not None else None,
        last_price=execution.last_price.value if execution.last_price is not None else None,
        cum_qty=execution.cum_qty.value,
        leaves_qty=execution.leaves_qty.value,
        position_id=execution.position_id,
        text=execution.text,
        transact_time=execution.transact_time,
        raw_message=execution.raw_message,
        received_at=execution.received_at,
    )


class SqlAlchemyExecutionRepository:
    """ExecutionRepository backed by the `executions` table."""

    def __init__(self, session: AsyncSession) -> None:
        """Wrap a session shared with the rest of the current unit of work."""
        self._session = session

    async def add(self, execution: Execution) -> None:
        """Stage a new execution row for insertion on the next flush/commit."""
        self._session.add(_to_row(execution))
