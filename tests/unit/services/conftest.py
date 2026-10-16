from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest

from app.domain.enums import OrderPurpose, OrderStatus, OrderType, PositionStatus, Side
from app.domain.errors import DuplicateKeyError
from app.domain.models import Execution, Order, Position, Price, Quantity, Symbol
from app.ports.unit_of_work import UnitOfWork


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
        for order in self._store.values():
            if order.account_id == account_id and order.client_order_id == client_order_id:
                return order
        return None

    async def get_by_fix_cl_ord_id(self, fix_cl_ord_id: str) -> Order | None:
        for order in self._store.values():
            if order.fix_cl_ord_id == fix_cl_ord_id:
                return order
        return None


class MockExecutionRepository:
    async def add(self, execution: Execution) -> None:
        raise NotImplementedError("not used by PlaceOrderHandler")


class MockPositionRepository:
    def __init__(self, store: dict[UUID, Position]) -> None:
        self._store = store

    async def add(self, position: Position) -> None:
        self._store[position.id] = position

    async def update(self, position: Position) -> None:
        if position.id not in self._store:
            raise ValueError(f"position {position.id} does not exist")
        self._store[position.id] = position

    async def get(self, position_id: UUID) -> Position | None:
        return self._store.get(position_id)

    async def get_by_broker_position_id(
        self, account_id: UUID, position_id: str, *, for_update: bool = False
    ) -> Position | None:
        for position in self._store.values():
            if position.account_id == account_id and position.position_id == position_id:
                return position
        return None

    async def list_by_account(self, account_id: UUID, *, status: str | None = None) -> list[Position]:
        return [
            position
            for position in self._store.values()
            if position.account_id == account_id and (status is None or position.status.value == status)
        ]


class MockOutboxRepository:
    def __init__(self, events: list[dict[str, Any]]) -> None:
        self._events = events

    async def add(self, **kwargs: Any) -> None:
        self._events.append(kwargs)


class MockSymbolRepository:
    def __init__(self, symbols: dict[tuple[UUID, str], Symbol]) -> None:
        self._symbols = symbols

    async def get_by_name(self, account_id: UUID, name: str) -> Symbol | None:
        return self._symbols.get((account_id, name))

    async def get_by_id(self, symbol_id: UUID) -> Symbol | None:
        for symbol in self._symbols.values():
            if symbol.id == symbol_id:
                return symbol
        return None

    async def has_any(self, account_id: UUID) -> bool:
        return any(existing_account_id == account_id for existing_account_id, _name in self._symbols)

    async def list_by_account(self, account_id: UUID) -> list[Symbol]:
        raise NotImplementedError("not used by PlaceOrderHandler")

    async def upsert(self, symbol: Symbol) -> None:
        raise NotImplementedError("not used by PlaceOrderHandler")


class MockUnitOfWork:
    """An in-memory UnitOfWork double - no I/O, for the unit-test tier."""

    def __init__(
        self,
        *,
        orders: dict[UUID, Order] | None = None,
        positions: dict[UUID, Position] | None = None,
        symbols: dict[tuple[UUID, str], Symbol] | None = None,
        outbox_events: list[dict[str, Any]] | None = None,
        fail_on_commit_with: Exception | None = None,
    ) -> None:
        self.orders_store = orders if orders is not None else {}
        self.positions_store = positions if positions is not None else {}
        self.outbox_events = outbox_events if outbox_events is not None else []
        self.orders = MockOrderRepository(self.orders_store)
        self.executions = MockExecutionRepository()
        self.positions = MockPositionRepository(self.positions_store)
        self.outbox = MockOutboxRepository(self.outbox_events)
        self.symbols = MockSymbolRepository(symbols if symbols is not None else {})
        self._fail_on_commit_with = fail_on_commit_with
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self) -> MockUnitOfWork:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    async def commit(self) -> None:
        if self._fail_on_commit_with is not None:
            raise self._fail_on_commit_with
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


@pytest.fixture
def make_symbol() -> Callable[..., Symbol]:
    def _make_symbol(
        *,
        account_id: UUID,
        name: str = "EURUSD",
        enabled: bool = True,
        subscribe: bool = True,
        needs_review: bool = False,
        broker_symbol_id: int | None = 1,
        lot_size: Decimal | None = Decimal(100_000),
        qty_step: Decimal | None = Decimal(1000),
        min_qty: Decimal | None = Decimal(1000),
        max_qty: Decimal | None = Decimal(10_000_000),
        price_step: Decimal | None = Decimal("0.00001"),
    ) -> Symbol:
        return Symbol(
            id=uuid4(),
            account_id=account_id,
            name=name,
            enabled=enabled,
            subscribe=subscribe,
            needs_review=needs_review,
            broker_symbol_id=broker_symbol_id,
            lot_size=Quantity(lot_size) if lot_size is not None else None,
            qty_step=Quantity(qty_step) if qty_step is not None else None,
            min_qty=Quantity(min_qty) if min_qty is not None else None,
            max_qty=Quantity(max_qty) if max_qty is not None else None,
            price_step=Price(price_step) if price_step is not None else None,
        )

    return _make_symbol


@pytest.fixture
def make_order() -> Callable[..., Order]:
    def _make_order(
        *,
        account_id: UUID,
        symbol_id: UUID,
        status: OrderStatus = OrderStatus.ACCEPTED,
        side: Side = Side.BUY,
        purpose: OrderPurpose = OrderPurpose.OPEN,
        fix_cl_ord_id: str | None = "bot7fix0001",
        closes_position_id: str | None = None,
    ) -> Order:
        now = datetime.now(UTC)
        return Order(
            id=uuid4(),
            account_id=account_id,
            client_order_id=f"bot7-{uuid4()}",
            symbol_id=symbol_id,
            side=side,
            order_type=OrderType.MARKET,
            quantity=Quantity(Decimal(1000)),
            status=status,
            purpose=purpose,
            created_at=now,
            updated_at=now,
            fix_cl_ord_id=fix_cl_ord_id,
            submitted_at=now if status is not OrderStatus.PENDING else None,
            closes_position_id=closes_position_id,
        )

    return _make_order


@pytest.fixture
def make_position() -> Callable[..., Position]:
    def _make_position(
        *,
        account_id: UUID,
        symbol_id: UUID,
        position_id: str = "4471123",
        side: Side = Side.BUY,
        status: PositionStatus = PositionStatus.OPEN,
        closed_by_order_id: UUID | None = None,
    ) -> Position:
        now = datetime.now(UTC)
        return Position(
            id=uuid4(),
            account_id=account_id,
            position_id=position_id,
            symbol_id=symbol_id,
            side=side,
            quantity=Quantity(Decimal(1000)),
            entry_price=Price(Decimal("1.08243")),
            status=status,
            opened_at=now,
            closed_by_order_id=closed_by_order_id,
        )

    return _make_position


def build_uow_factory(uow: MockUnitOfWork) -> Callable[[], UnitOfWork]:
    return lambda: uow


def utcnow() -> datetime:
    return datetime.now(UTC)


def dedupe_key_error() -> DuplicateKeyError:
    return DuplicateKeyError("duplicate key value violates unique constraint")
