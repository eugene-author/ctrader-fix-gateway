from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest

from app.adapters.brokers.ctrader.message_factory import CTraderMessageFactory
from app.domain.commands import (
    CancelOrderCommand,
    ClosePositionCommand,
    CreateOrderCommand,
    PlaceProtectiveOrdersCommand,
    ProtectiveOrder,
)
from app.domain.enums import OrderPurpose, OrderStatus, OrderType, Side, TimeInForce
from app.domain.models import Execution, Order, Position, Price, Quantity, Symbol
from app.ports.unit_of_work import UnitOfWork
from app.queue.consumer import TransientHandlerError
from app.queue.serialization import build_envelope
from app.workers.order_command_handler import OrderCommandHandler


class MockOrderRepository:
    def __init__(self, store: dict[UUID, Order]) -> None:
        self._store = store

    async def add(self, order: Order) -> None:
        self._store[order.id] = order

    async def update(self, order: Order) -> None:
        if order.id not in self._store:
            raise ValueError(f"order {order.id} does not exist")
        self._store[order.id] = order

    async def get(self, order_id: UUID) -> Order | None:
        return self._store.get(order_id)

    async def get_by_client_order_id(self, account_id: UUID, client_order_id: str) -> Order | None:
        raise NotImplementedError("not used by OrderCommandHandler")


class MockInboxRepository:
    def __init__(self, seen: dict[tuple[str, str], str]) -> None:
        self._seen = seen

    async def claim(self, *, consumer: str, message_id: str, payload_hash: str) -> bool:
        key = (consumer, message_id)
        if key in self._seen:
            return False
        self._seen[key] = payload_hash
        return True


class MockExecutionRepository:
    async def add(self, execution: Execution) -> None:
        raise NotImplementedError("not used by OrderCommandHandler")


class MockPositionRepository:
    async def add(self, position: Position) -> None:
        raise NotImplementedError("not used by OrderCommandHandler")

    async def get(self, position_id: UUID) -> Position | None:
        raise NotImplementedError("not used by OrderCommandHandler")

    async def get_by_broker_position_id(
        self, account_id: UUID, position_id: str, *, for_update: bool = False
    ) -> Position | None:
        raise NotImplementedError("not used by OrderCommandHandler")


class MockOutboxRepository:
    async def add(self, **kwargs: Any) -> None:
        raise NotImplementedError("not used by OrderCommandHandler")


class MockSymbolRepository:
    async def get_by_name(self, account_id: UUID, name: str) -> Symbol | None:
        raise NotImplementedError("not used by OrderCommandHandler")


class MockUnitOfWork:
    """An in-memory UnitOfWork double covering only what OrderCommandHandler touches."""

    def __init__(self, *, orders: dict[UUID, Order]) -> None:
        self.orders = MockOrderRepository(orders)
        self.executions = MockExecutionRepository()
        self.positions = MockPositionRepository()
        self.outbox = MockOutboxRepository()
        self.symbols = MockSymbolRepository()
        self.inbox = MockInboxRepository({})
        self.committed_count = 0

    async def __aenter__(self) -> MockUnitOfWork:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    async def commit(self) -> None:
        self.committed_count += 1

    async def rollback(self) -> None:
        pass


class MockSessionManager:
    def __init__(self, *, logged_on: bool = True, raise_on_send: Exception | None = None) -> None:
        self._logged_on = logged_on
        self._raise_on_send = raise_on_send
        self.sent: list[Any] = []

    def is_logged_on(self) -> bool:
        return self._logged_on

    def send(self, message: Any) -> None:
        if self._raise_on_send is not None:
            raise self._raise_on_send
        self.sent.append(message)


class MockEventPublisher:
    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    async def publish(
        self,
        *,
        routing_key: str,
        message_type: str,
        payload: dict[str, object],
        correlation_id: str,
        causation_id: str,
        account_id: str,
    ) -> None:
        self.published.append(
            {
                "routing_key": routing_key,
                "message_type": message_type,
                "payload": payload,
                "correlation_id": correlation_id,
                "causation_id": causation_id,
                "account_id": account_id,
            }
        )


def make_order(*, order_id: UUID, submitted_at: datetime | None = None) -> Order:
    now = datetime.now(UTC)
    return Order(
        id=order_id,
        account_id=uuid4(),
        client_order_id="bot7-0001",
        symbol_id=uuid4(),
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal(1000)),
        status=OrderStatus.PENDING if submitted_at is None else OrderStatus.SUBMITTED,
        purpose=OrderPurpose.OPEN,
        created_at=now,
        updated_at=now,
        submitted_at=submitted_at,
    )


def make_command(*, order_id: UUID) -> CreateOrderCommand:
    return CreateOrderCommand(
        order_id=order_id,
        client_order_id="bot7-0001",
        fix_cl_ord_id="bot7fix0001",
        symbol="EURUSD",
        broker_symbol_id=1,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal(1000)),
        time_in_force=TimeInForce.GTC,
    )


def build_command_envelope(order_id: UUID):
    return build_envelope(
        message_type="CreateOrderCommand",
        payload=make_command(order_id=order_id).to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id="acct-1",
    )


def make_cancel_command(*, order_id: UUID) -> CancelOrderCommand:
    return CancelOrderCommand(
        order_id=order_id,
        fix_cl_ord_id="bot7fix0002",
        orig_cl_ord_id="bot7fix0001",
        symbol="EURUSD",
        broker_symbol_id=1,
    )


def build_cancel_envelope(order_id: UUID):
    return build_envelope(
        message_type="CancelOrderCommand",
        payload=make_cancel_command(order_id=order_id).to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id="acct-1",
    )


def make_close_command(*, order_id: UUID) -> ClosePositionCommand:
    return ClosePositionCommand(
        order_id=order_id,
        position_id="4471123",
        fix_cl_ord_id="bot7fix0003",
        symbol="EURUSD",
        broker_symbol_id=1,
        side=Side.SELL,
        quantity=Quantity(Decimal(1000)),
    )


def build_close_envelope(order_id: UUID):
    return build_envelope(
        message_type="ClosePositionCommand",
        payload=make_close_command(order_id=order_id).to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id="acct-1",
    )


def make_protective_orders_command(
    *, stop_loss_order_id: UUID | None, take_profit_order_id: UUID | None
) -> PlaceProtectiveOrdersCommand:
    return PlaceProtectiveOrdersCommand(
        position_id="4471123",
        symbol="EURUSD",
        broker_symbol_id=1,
        close_side=Side.SELL,
        quantity=Quantity(Decimal(1000)),
        oco_group_id=uuid4(),
        stop_loss=(
            ProtectiveOrder(order_id=stop_loss_order_id, fix_cl_ord_id="sl:4471123", price=Price(Decimal("1.08000")))
            if stop_loss_order_id is not None
            else None
        ),
        take_profit=(
            ProtectiveOrder(order_id=take_profit_order_id, fix_cl_ord_id="tp:4471123", price=Price(Decimal("1.08600")))
            if take_profit_order_id is not None
            else None
        ),
    )


def build_protective_orders_envelope(
    *, stop_loss_order_id: UUID | None = None, take_profit_order_id: UUID | None = None
):
    return build_envelope(
        message_type="PlaceProtectiveOrdersCommand",
        payload=make_protective_orders_command(
            stop_loss_order_id=stop_loss_order_id, take_profit_order_id=take_profit_order_id
        ).to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id="acct-1",
    )


def build_uow_factory(uow: MockUnitOfWork) -> Callable[[], UnitOfWork]:
    return lambda: uow


def _rendered(message: Any) -> str:
    message.delimiter = "|"
    return message.getMessage(1)


@pytest.fixture
def message_factory() -> CTraderMessageFactory:
    return CTraderMessageFactory(
        {
            "BeginString": "FIX.4.4",
            "SenderCompID": "demo.broker.1234567",
            "TargetCompID": "cServer",
            "TargetSubID": "TRADE",
            "SenderSubID": "TRADE",
        }
    )


@pytest.mark.asyncio
async def test_handle_ignores_unrecognized_message_type(message_factory: CTraderMessageFactory) -> None:
    orders: dict[UUID, Order] = {}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )
    envelope = build_envelope(
        message_type="SomethingElse", payload={}, correlation_id="c", causation_id="c", account_id="a"
    )

    await handler.handle(envelope)

    assert session.sent == []
    assert publisher.published == []


@pytest.mark.asyncio
async def test_handle_raises_transient_error_when_session_not_logged_on(message_factory: CTraderMessageFactory) -> None:
    order_id = uuid4()
    orders = {order_id: make_order(order_id=order_id)}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager(logged_on=False)
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    with pytest.raises(TransientHandlerError):
        await handler.handle(build_command_envelope(order_id))

    assert session.sent == []
    assert publisher.published == []
    assert uow.committed_count == 0


@pytest.mark.asyncio
async def test_handle_sends_new_order_single_and_publishes_order_submitted(
    message_factory: CTraderMessageFactory,
) -> None:
    order_id = uuid4()
    orders = {order_id: make_order(order_id=order_id)}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_command_envelope(order_id))

    assert len(session.sent) == 1
    rendered = _rendered(session.sent[0])
    assert "35=D" in rendered
    assert "11=bot7fix0001" in rendered

    assert orders[order_id].status == OrderStatus.SUBMITTED
    assert orders[order_id].submitted_at is not None
    assert orders[order_id].fix_cl_ord_id == "bot7fix0001"

    assert len(publisher.published) == 1
    published = publisher.published[0]
    assert published["routing_key"] == "order.submitted"
    assert published["message_type"] == "OrderSubmitted"
    assert published["payload"]["order_id"] == str(order_id)
    assert published["correlation_id"] == "corr-1"
    assert published["causation_id"] is not None
    assert published["account_id"] == "acct-1"


@pytest.mark.asyncio
async def test_handle_marks_the_order_dispatch_uncertain_when_send_fails(
    message_factory: CTraderMessageFactory,
) -> None:
    order_id = uuid4()
    orders = {order_id: make_order(order_id=order_id)}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager(raise_on_send=ConnectionError("socket write failed"))
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_command_envelope(order_id))

    assert orders[order_id].status == OrderStatus.DISPATCH_UNCERTAIN
    assert orders[order_id].submitted_at is not None  # dispatch was still committed before the failed send
    assert orders[order_id].error_message == "socket write failed"
    assert publisher.published == []  # no OrderSubmitted - we don't know if the broker got it


@pytest.mark.asyncio
async def test_handle_is_idempotent_on_a_redelivered_envelope(message_factory: CTraderMessageFactory) -> None:
    order_id = uuid4()
    orders = {order_id: make_order(order_id=order_id)}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )
    envelope = build_command_envelope(order_id)

    await handler.handle(envelope)
    await handler.handle(envelope)

    assert len(session.sent) == 1
    assert len(publisher.published) == 1


@pytest.mark.asyncio
async def test_handle_applies_the_dispatch_guard_for_an_already_submitted_order(
    message_factory: CTraderMessageFactory,
) -> None:
    order_id = uuid4()
    orders = {order_id: make_order(order_id=order_id, submitted_at=datetime.now(UTC))}
    orders[order_id].fix_cl_ord_id = "bot7fix0001"
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_command_envelope(order_id))

    assert len(session.sent) == 1
    rendered = _rendered(session.sent[0])
    assert "35=H" in rendered  # OrderStatusRequest, not a resent NewOrderSingle
    assert "11=bot7fix0001" in rendered
    assert publisher.published == []


@pytest.mark.asyncio
async def test_handle_returns_without_sending_when_order_is_unknown(message_factory: CTraderMessageFactory) -> None:
    order_id = uuid4()
    uow = MockUnitOfWork(orders={})
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_command_envelope(order_id))

    assert session.sent == []
    assert publisher.published == []


@pytest.mark.asyncio
async def test_handle_sends_order_cancel_request(message_factory: CTraderMessageFactory) -> None:
    order_id = uuid4()
    orders = {order_id: make_order(order_id=order_id, submitted_at=datetime.now(UTC))}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_cancel_envelope(order_id))

    assert len(session.sent) == 1
    rendered = _rendered(session.sent[0])
    assert "35=F" in rendered
    assert "11=bot7fix0002" in rendered
    assert "41=bot7fix0001" in rendered
    assert publisher.published == []


@pytest.mark.asyncio
async def test_handle_cancel_order_is_idempotent_on_a_redelivered_envelope(
    message_factory: CTraderMessageFactory,
) -> None:
    order_id = uuid4()
    orders = {order_id: make_order(order_id=order_id, submitted_at=datetime.now(UTC))}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )
    envelope = build_cancel_envelope(order_id)

    await handler.handle(envelope)
    await handler.handle(envelope)

    assert len(session.sent) == 1


@pytest.mark.asyncio
async def test_handle_cancel_order_logs_and_returns_when_order_is_unknown(
    message_factory: CTraderMessageFactory,
) -> None:
    order_id = uuid4()
    uow = MockUnitOfWork(orders={})
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_cancel_envelope(order_id))

    assert session.sent == []


@pytest.mark.asyncio
async def test_handle_sends_close_new_order_single_and_publishes_order_submitted(
    message_factory: CTraderMessageFactory,
) -> None:
    order_id = uuid4()
    orders = {order_id: make_order(order_id=order_id)}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_close_envelope(order_id))

    assert len(session.sent) == 1
    rendered = _rendered(session.sent[0])
    assert "35=D" in rendered  # a close is a new market order
    assert "11=bot7fix0003" in rendered
    assert "721=4471123" in rendered

    assert orders[order_id].status == OrderStatus.SUBMITTED
    assert orders[order_id].fix_cl_ord_id == "bot7fix0003"

    assert len(publisher.published) == 1
    assert publisher.published[0]["message_type"] == "OrderSubmitted"


@pytest.mark.asyncio
async def test_handle_close_position_applies_the_dispatch_guard(message_factory: CTraderMessageFactory) -> None:
    order_id = uuid4()
    orders = {order_id: make_order(order_id=order_id, submitted_at=datetime.now(UTC))}
    orders[order_id].fix_cl_ord_id = "bot7fix0003"
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_close_envelope(order_id))

    assert len(session.sent) == 1
    rendered = _rendered(session.sent[0])
    assert "35=H" in rendered  # OrderStatusRequest
    assert publisher.published == []


@pytest.mark.asyncio
async def test_handle_place_protective_orders_dispatches_the_full_bracket(
    message_factory: CTraderMessageFactory,
) -> None:
    sl_order_id, tp_order_id = uuid4(), uuid4()
    orders = {
        sl_order_id: make_order(order_id=sl_order_id),
        tp_order_id: make_order(order_id=tp_order_id),
    }
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(
        build_protective_orders_envelope(stop_loss_order_id=sl_order_id, take_profit_order_id=tp_order_id)
    )

    assert len(session.sent) == 2
    rendered = [_rendered(m) for m in session.sent]
    sl_message = next(r for r in rendered if "11=sl:4471123" in r)
    tp_message = next(r for r in rendered if "11=tp:4471123" in r)
    assert "40=3" in sl_message  # OrdType Stop
    assert "99=1.08000" in sl_message  # StopPx
    assert "40=2" in tp_message  # OrdType Limit
    assert "44=1.08600" in tp_message  # Price
    assert "721=4471123" in sl_message
    assert "721=4471123" in tp_message

    assert orders[sl_order_id].status == OrderStatus.SUBMITTED
    assert orders[sl_order_id].fix_cl_ord_id == "sl:4471123"
    assert orders[tp_order_id].status == OrderStatus.SUBMITTED
    assert orders[tp_order_id].fix_cl_ord_id == "tp:4471123"

    assert len(publisher.published) == 2
    assert {p["payload"]["order_id"] for p in publisher.published} == {str(sl_order_id), str(tp_order_id)}


@pytest.mark.asyncio
async def test_handle_place_protective_orders_dispatches_only_the_requested_order(
    message_factory: CTraderMessageFactory,
) -> None:
    sl_order_id = uuid4()
    orders = {sl_order_id: make_order(order_id=sl_order_id)}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_protective_orders_envelope(stop_loss_order_id=sl_order_id))

    assert len(session.sent) == 1
    assert "40=3" in _rendered(session.sent[0])
    assert len(publisher.published) == 1


@pytest.mark.asyncio
async def test_handle_place_protective_orders_one_half_missing_still_dispatches_the_other(
    message_factory: CTraderMessageFactory,
) -> None:
    tp_order_id = uuid4()
    # sl_order_id deliberately not seeded: simulates a protective order whose
    # row somehow doesn't exist - must not stop the other half from dispatching.
    orders = {tp_order_id: make_order(order_id=tp_order_id)}
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )

    await handler.handle(build_protective_orders_envelope(stop_loss_order_id=uuid4(), take_profit_order_id=tp_order_id))

    assert len(session.sent) == 1
    assert "40=2" in _rendered(session.sent[0])
    assert len(publisher.published) == 1


@pytest.mark.asyncio
async def test_handle_place_protective_orders_is_idempotent_on_a_redelivered_envelope(
    message_factory: CTraderMessageFactory,
) -> None:
    sl_order_id, tp_order_id = uuid4(), uuid4()
    orders = {
        sl_order_id: make_order(order_id=sl_order_id),
        tp_order_id: make_order(order_id=tp_order_id),
    }
    uow = MockUnitOfWork(orders=orders)
    session = MockSessionManager()
    publisher = MockEventPublisher()
    handler = OrderCommandHandler(
        session=session, uow_factory=build_uow_factory(uow), publisher=publisher, message_factory=message_factory
    )
    envelope = build_protective_orders_envelope(stop_loss_order_id=sl_order_id, take_profit_order_id=tp_order_id)

    await handler.handle(envelope)
    await handler.handle(envelope)

    assert len(session.sent) == 2
    assert len(publisher.published) == 2
