from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest

from app.adapters.brokers.ctrader.message_factory import CTraderMessageFactory
from app.domain.enums import OrderPurpose, OrderStatus, OrderType, PositionStatus, ProtectionState, ProtectiveKind, Side
from app.domain.models import Order, Position, Price, Quantity, Symbol
from app.workers.reconciliation import OrderDispatchReconciler, ProtectiveOrderReconciler


class MockOrderRepository:
    def __init__(self, store: dict[UUID, Order]) -> None:
        self._store = store

    async def get(self, order_id: UUID) -> Order | None:
        return self._store.get(order_id)

    async def list_by_account_and_statuses(self, account_id: UUID, statuses: frozenset[OrderStatus]) -> list[Order]:
        return [order for order in self._store.values() if order.account_id == account_id and order.status in statuses]


class MockPositionRepository:
    def __init__(self, positions: list[Position]) -> None:
        self._positions = positions

    async def list_by_account(self, account_id: UUID, *, status: str | None = None) -> list[Position]:
        return [p for p in self._positions if p.account_id == account_id and (status is None or p.status == status)]


class MockSymbolRepository:
    def __init__(self, symbols: dict[UUID, Symbol]) -> None:
        self._symbols = symbols

    async def get_by_id(self, symbol_id: UUID) -> Symbol | None:
        return self._symbols.get(symbol_id)


class MockUnitOfWork:
    def __init__(self, *, orders: dict[UUID, Order], positions: list[Position], symbols: dict[UUID, Symbol]) -> None:
        self.orders = MockOrderRepository(orders)
        self.positions = MockPositionRepository(positions)
        self.symbols = MockSymbolRepository(symbols)

    async def __aenter__(self) -> MockUnitOfWork:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


class MockSession:
    def __init__(self) -> None:
        self.sent: list[Any] = []

    def is_logged_on(self) -> bool:
        return True

    def send(self, message: Any) -> None:
        self.sent.append(message)


def _rendered(message: Any) -> str:
    message.delimiter = "|"
    return message.getMessage(1)


MESSAGE_FACTORY = CTraderMessageFactory(
    {
        "BeginString": "FIX.4.4",
        "SenderCompID": "demo.broker.1234567",
        "TargetCompID": "cServer",
        "TargetSubID": "TRADE",
        "SenderSubID": "TRADE",
    }
)


def _make_symbol(symbol_id: UUID) -> Symbol:
    return Symbol(
        id=symbol_id,
        account_id=uuid4(),
        name="EURUSD",
        enabled=True,
        subscribe=True,
        needs_review=False,
        broker_symbol_id=1,
    )


def _make_protective_order(
    *, order_id: UUID, symbol_id: UUID, status: OrderStatus, kind: ProtectiveKind, fix_cl_ord_id: str | None
) -> Order:
    now = datetime.now(UTC)
    is_stop = kind == ProtectiveKind.STOP_LOSS
    return Order(
        id=order_id,
        account_id=uuid4(),
        client_order_id=fix_cl_ord_id or f"bot7-{order_id}",
        symbol_id=symbol_id,
        side=Side.SELL,
        order_type=OrderType.STOP if is_stop else OrderType.LIMIT,
        quantity=Quantity(Decimal(1000)),
        status=status,
        purpose=OrderPurpose.CLOSE,
        created_at=now,
        updated_at=now,
        fix_cl_ord_id=fix_cl_ord_id,
        stop_price=Price(Decimal("1.07000")) if is_stop else None,
        limit_price=None if is_stop else Price(Decimal("1.09000")),
        protective_kind=kind,
        protects_position_id="pos-1",
    )


def _make_position(
    *, account_id: UUID, protection_state: ProtectionState, sl_id: UUID | None, tp_id: UUID | None
) -> Position:
    return Position(
        id=uuid4(),
        account_id=account_id,
        position_id="pos-1",
        symbol_id=uuid4(),
        side=Side.BUY,
        quantity=Quantity(Decimal(1000)),
        entry_price=Price(Decimal("1.08000")),
        status=PositionStatus.OPEN,
        opened_at=datetime.now(UTC),
        protection_state=protection_state,
        stop_loss_order_id=sl_id,
        take_profit_order_id=tp_id,
    )


@pytest.mark.asyncio
async def test_a_pending_protection_states_non_live_protective_order_is_resent() -> None:
    account_id = uuid4()
    symbol_id = uuid4()
    sl_order = _make_protective_order(
        order_id=uuid4(),
        symbol_id=symbol_id,
        status=OrderStatus.REJECTED,
        kind=ProtectiveKind.STOP_LOSS,
        fix_cl_ord_id="sl:pos-1",
    )
    position = _make_position(
        account_id=account_id, protection_state=ProtectionState.PARTIAL, sl_id=sl_order.id, tp_id=None
    )
    uow = MockUnitOfWork(
        orders={sl_order.id: sl_order}, positions=[position], symbols={symbol_id: _make_symbol(symbol_id)}
    )
    session = MockSession()

    await ProtectiveOrderReconciler(lambda: uow, session, MESSAGE_FACTORY, account_id).reconcile()

    assert len(session.sent) == 1
    rendered = _rendered(session.sent[0])
    assert "35=D" in rendered
    assert "11=sl:pos-1" in rendered
    assert "40=3" in rendered  # OrdType=STOP
    assert "99=1.07000" in rendered  # StopPx


@pytest.mark.asyncio
async def test_a_live_protective_order_is_not_resent() -> None:
    account_id = uuid4()
    symbol_id = uuid4()
    sl_order = _make_protective_order(
        order_id=uuid4(),
        symbol_id=symbol_id,
        status=OrderStatus.ACCEPTED,
        kind=ProtectiveKind.STOP_LOSS,
        fix_cl_ord_id="sl:pos-1",
    )
    tp_order = _make_protective_order(
        order_id=uuid4(),
        symbol_id=symbol_id,
        status=OrderStatus.SUBMITTED,
        kind=ProtectiveKind.TAKE_PROFIT,
        fix_cl_ord_id="tp:pos-1",
    )
    position = _make_position(
        account_id=account_id, protection_state=ProtectionState.PARTIAL, sl_id=sl_order.id, tp_id=tp_order.id
    )
    uow = MockUnitOfWork(
        orders={sl_order.id: sl_order, tp_order.id: tp_order},
        positions=[position],
        symbols={symbol_id: _make_symbol(symbol_id)},
    )
    session = MockSession()

    await ProtectiveOrderReconciler(lambda: uow, session, MESSAGE_FACTORY, account_id).reconcile()

    assert len(session.sent) == 1  # only the SUBMITTED TP order is resent
    rendered = _rendered(session.sent[0])
    assert "11=tp:pos-1" in rendered


@pytest.mark.asyncio
async def test_a_position_with_active_protection_is_skipped() -> None:
    account_id = uuid4()
    symbol_id = uuid4()
    sl_order = _make_protective_order(
        order_id=uuid4(),
        symbol_id=symbol_id,
        status=OrderStatus.REJECTED,
        kind=ProtectiveKind.STOP_LOSS,
        fix_cl_ord_id="sl:pos-1",
    )
    position = _make_position(
        account_id=account_id, protection_state=ProtectionState.ACTIVE, sl_id=sl_order.id, tp_id=None
    )
    uow = MockUnitOfWork(
        orders={sl_order.id: sl_order}, positions=[position], symbols={symbol_id: _make_symbol(symbol_id)}
    )
    session = MockSession()

    await ProtectiveOrderReconciler(lambda: uow, session, MESSAGE_FACTORY, account_id).reconcile()

    assert session.sent == []


@pytest.mark.asyncio
async def test_a_protective_order_never_dispatched_is_left_to_the_command_consumer() -> None:
    account_id = uuid4()
    symbol_id = uuid4()
    sl_order = _make_protective_order(
        order_id=uuid4(),
        symbol_id=symbol_id,
        status=OrderStatus.PENDING,
        kind=ProtectiveKind.STOP_LOSS,
        fix_cl_ord_id=None,
    )
    position = _make_position(
        account_id=account_id, protection_state=ProtectionState.PENDING, sl_id=sl_order.id, tp_id=None
    )
    uow = MockUnitOfWork(
        orders={sl_order.id: sl_order}, positions=[position], symbols={symbol_id: _make_symbol(symbol_id)}
    )
    session = MockSession()

    await ProtectiveOrderReconciler(lambda: uow, session, MESSAGE_FACTORY, account_id).reconcile()

    assert session.sent == []


def _make_primary_order(*, order_id: UUID, account_id: UUID, status: OrderStatus, fix_cl_ord_id: str | None) -> Order:
    now = datetime.now(UTC)
    return Order(
        id=order_id,
        account_id=account_id,
        client_order_id=f"bot7-{order_id}",
        symbol_id=uuid4(),
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal(1000)),
        status=status,
        purpose=OrderPurpose.OPEN,
        created_at=now,
        updated_at=now,
        fix_cl_ord_id=fix_cl_ord_id,
        submitted_at=now if fix_cl_ord_id is not None else None,
    )


@pytest.mark.asyncio
async def test_an_order_stuck_dispatching_is_queried() -> None:
    account_id = uuid4()
    order = _make_primary_order(
        order_id=uuid4(), account_id=account_id, status=OrderStatus.DISPATCHING, fix_cl_ord_id="bot7fix0001"
    )
    uow = MockUnitOfWork(orders={order.id: order}, positions=[], symbols={})
    session = MockSession()

    await OrderDispatchReconciler(lambda: uow, session, MESSAGE_FACTORY, account_id).reconcile()

    assert len(session.sent) == 1
    rendered = _rendered(session.sent[0])
    assert "35=H" in rendered  # OrderStatusRequest
    assert "11=bot7fix0001" in rendered


@pytest.mark.asyncio
async def test_an_order_stuck_dispatch_uncertain_is_queried() -> None:
    account_id = uuid4()
    order = _make_primary_order(
        order_id=uuid4(), account_id=account_id, status=OrderStatus.DISPATCH_UNCERTAIN, fix_cl_ord_id="bot7fix0002"
    )
    uow = MockUnitOfWork(orders={order.id: order}, positions=[], symbols={})
    session = MockSession()

    await OrderDispatchReconciler(lambda: uow, session, MESSAGE_FACTORY, account_id).reconcile()

    assert len(session.sent) == 1
    rendered = _rendered(session.sent[0])
    assert "11=bot7fix0002" in rendered


@pytest.mark.parametrize(
    "status",
    [
        OrderStatus.PENDING,
        OrderStatus.SUBMITTED,
        OrderStatus.ACCEPTED,
        OrderStatus.FILLED,
        OrderStatus.FAILED,
    ],
)
@pytest.mark.asyncio
async def test_an_order_not_in_a_dispatch_uncertain_status_is_not_queried(status: OrderStatus) -> None:
    account_id = uuid4()
    order = _make_primary_order(order_id=uuid4(), account_id=account_id, status=status, fix_cl_ord_id="bot7fix0003")
    uow = MockUnitOfWork(orders={order.id: order}, positions=[], symbols={})
    session = MockSession()

    await OrderDispatchReconciler(lambda: uow, session, MESSAGE_FACTORY, account_id).reconcile()

    assert session.sent == []


@pytest.mark.asyncio
async def test_a_stuck_order_for_a_different_account_is_ignored() -> None:
    order = _make_primary_order(
        order_id=uuid4(), account_id=uuid4(), status=OrderStatus.DISPATCHING, fix_cl_ord_id="bot7fix0004"
    )
    uow = MockUnitOfWork(orders={order.id: order}, positions=[], symbols={})
    session = MockSession()

    await OrderDispatchReconciler(lambda: uow, session, MESSAGE_FACTORY, uuid4()).reconcile()

    assert session.sent == []
