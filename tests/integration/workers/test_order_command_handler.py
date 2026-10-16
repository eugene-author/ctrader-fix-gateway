"""OrderCommandHandler against a real Postgres-backed UnitOfWork: the inbox/send ordering bug (B)."""

from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.brokers.ctrader.message_factory import CTraderMessageFactory
from app.db.models import AccountRow, OrderRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.commands import CancelOrderCommand
from app.domain.enums import OrderPurpose, OrderStatus, OrderType, Side
from app.queue.serialization import build_envelope
from app.workers.order_command_handler import OrderCommandHandler


class _FlakySession:
    """Raises on its first send(), succeeds on every call after - simulates a
    transient socket failure followed by a successful retry."""

    def __init__(self) -> None:
        self.sent: list[Any] = []
        self._calls = 0

    def is_logged_on(self) -> bool:
        return True

    def send(self, message: Any) -> None:
        self._calls += 1
        if self._calls == 1:
            raise ConnectionError("socket write failed")
        self.sent.append(message)


class _NullPublisher:
    async def publish(self, **kwargs: object) -> None:
        raise NotImplementedError("cancel never publishes")


def _rendered(message: Any) -> str:
    message.delimiter = "|"
    return message.getMessage(1)


@pytest.fixture
def message_factory() -> CTraderMessageFactory:
    return CTraderMessageFactory(
        {
            "BeginString": "FIX.4.4",
            "SenderCompID": "demo.test.1",
            "TargetCompID": "cServer",
            "TargetSubID": "TRADE",
            "SenderSubID": "TRADE",
        }
    )


@pytest.mark.asyncio
async def test_cancel_survives_a_failed_send_and_actually_retries_on_redelivery(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    message_factory: CTraderMessageFactory,
) -> None:
    account, symbol = seeded_account_and_symbol
    order_id = uuid4()
    async with session_factory() as session:
        session.add(
            OrderRow(
                id=order_id,
                account_id=account.id,
                client_order_id="bot7-cancel-0001",
                fix_cl_ord_id="bot7fixcancel01",
                symbol_id=symbol.id,
                side=Side.BUY,
                order_type=OrderType.MARKET,
                quantity=1000,
                status=OrderStatus.ACCEPTED,
                purpose=OrderPurpose.OPEN,
            )
        )
        await session.commit()

    session_double = _FlakySession()
    handler = OrderCommandHandler(
        session=session_double,
        uow_factory=lambda: SqlAlchemyUnitOfWork(session_factory),
        publisher=_NullPublisher(),
        message_factory=message_factory,
    )
    command = CancelOrderCommand(
        order_id=order_id,
        fix_cl_ord_id="bot7fixcancel02",
        orig_cl_ord_id="bot7fixcancel01",
        symbol=symbol.name,
        broker_symbol_id=1,
    )
    envelope = build_envelope(
        message_type="CancelOrderCommand",
        payload=command.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    # First delivery: send() raises. If the inbox row committed anyway (the
    # bug), the redelivery below would be swallowed as an already-seen
    # message and the cancel would never go out at all.
    with pytest.raises(ConnectionError):
        await handler.handle(envelope)
    assert session_double.sent == []

    # Redelivery of the *same* envelope (same message_id): must actually
    # retry the send, not be silently dropped as a duplicate.
    await handler.handle(envelope)

    assert len(session_double.sent) == 1
    rendered = _rendered(session_double.sent[0])
    assert "35=F" in rendered  # OrderCancelRequest actually reached the wire
    assert "11=bot7fixcancel02" in rendered
