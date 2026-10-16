from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch
from uuid import uuid4

import pytest
import structlog.contextvars
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.workers.projector as projector_module
from app.core.metrics import order_illegal_transition_total, order_latency_seconds
from app.core.symbol_overlay import SymbolOverlayEntry
from app.db.models import AccountRow, OrderRow, OutboxEventRow, SymbolRow
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.enums import (
    OrderPurpose,
    OrderStatus,
    OrderType,
    PositionStatus,
    ProtectionState,
    ProtectiveKind,
    Side,
    SymbolSource,
)
from app.domain.errors import ConcurrentModificationError
from app.domain.events import DiscoveredSymbol, ExecutionReportReceived, OrderFailed, SecurityListReceived
from app.domain.models import Order, Position, Price, Quantity
from app.queue.consumer import TransientHandlerError
from app.queue.serialization import build_envelope
from app.workers.projector import ProjectorHandler


def _fix_time(offset_seconds: float = 0) -> str:
    # The state machine rejects a transact_time before the order's current
    # updated_at (a monotonic guard), and updated_at is set to real
    # wall-clock time when the order is seeded - a hardcoded past date would
    # be rejected as a regression, not for the status transition itself.
    moment = datetime.now(UTC) + timedelta(seconds=offset_seconds)
    return moment.strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


@pytest.fixture
def uow_factory(session_factory: async_sessionmaker[AsyncSession]):
    return lambda: SqlAlchemyUnitOfWork(session_factory)


async def _seed_submitted_order(
    session_factory: async_sessionmaker[AsyncSession],
    make_order: Callable[..., Order],
    *,
    account_id,
    symbol_id,
    fix_cl_ord_id: str,
) -> Order:
    order = make_order(account_id=account_id, symbol_id=symbol_id)
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(order)
        await uow.commit()
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        fetched = await uow.orders.get(order.id)
        assert fetched is not None
        fetched.status = OrderStatus.SUBMITTED
        fetched.fix_cl_ord_id = fix_cl_ord_id
        fetched.submitted_at = datetime.now(UTC)
        fetched.version = 2
        await uow.orders.update(fetched)
        await uow.commit()
    return fetched


@pytest.mark.asyncio
async def test_execution_report_fills_an_order_and_opens_a_position(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    order = await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="fillme0001"
    )
    event = ExecutionReportReceived(
        fix_cl_ord_id="fillme0001",
        exec_id="exec-1",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_qty="1000",
        last_price="1.08243",
        position_id="pos-1",
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    await ProjectorHandler(uow_factory).handle(envelope)

    async with uow_factory() as uow:
        refreshed = await uow.orders.get(order.id)
        position = await uow.positions.get_by_broker_position_id(account.id, "pos-1")

    assert refreshed is not None
    assert refreshed.status == OrderStatus.FILLED
    assert refreshed.filled_quantity.value == Decimal("1000")
    assert refreshed.avg_fill_price is not None
    assert refreshed.avg_fill_price.value == Decimal("1.08243")
    assert refreshed.position_id == "pos-1"
    assert refreshed.terminal_at is not None

    assert position is not None
    assert position.status == PositionStatus.OPEN
    assert position.quantity.value == Decimal("1000")
    assert position.entry_price.value == Decimal("1.08243")
    assert position.opened_by_order_id == order.id


@pytest.mark.asyncio
async def test_execution_report_processing_rebinds_the_orders_own_correlation_id_into_log_lines(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    # fix-worker's own publish of ExecutionReportReceived necessarily uses a
    # fresh, unrelated-looking correlation_id - it never touches the
    # database, so it has nothing to rebind from (see fix_worker.py's
    # _handle_trade_message). The projector, which already loads the order
    # by fix_cl_ord_id, is what actually has the original POST /orders'
    # X-Correlation-Id (persisted on the order at creation) to rebind - so
    # its own log lines should carry that, not the envelope's.
    account, symbol = seeded_account_and_symbol
    order = make_order(account_id=account.id, symbol_id=symbol.id)
    order.correlation_id = "corr-from-original-post"
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(order)
        await uow.commit()
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        fetched = await uow.orders.get(order.id)
        assert fetched is not None
        fetched.status = OrderStatus.SUBMITTED
        fetched.fix_cl_ord_id = "fillmecorr0001"
        fetched.submitted_at = datetime.now(UTC)
        fetched.version = 2
        await uow.orders.update(fetched)
        await uow.commit()

    event = ExecutionReportReceived(
        fix_cl_ord_id="fillmecorr0001",
        exec_id="exec-corr-1",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_qty="1000",
        last_price="1.08243",
        position_id="pos-corr-1",
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="fix-worker-generated-unrelated-id",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    # structlog.testing.capture_logs() mutates GLOBAL processor config (its
    # own docstring: "not thread-safe") - unsafe under a full-suite run
    # where other tests reconfigure logging concurrently. Spying on the
    # module's logger and reading contextvars synchronously at call time
    # avoids touching global state entirely.
    observed_contextvars: list[dict[str, object]] = []
    original_info = projector_module.logger.info

    def _spy_info(event: str, **kwargs: object) -> None:
        if event == "order_execution_report_applied":
            observed_contextvars.append(dict(structlog.contextvars.get_contextvars()))
        original_info(event, **kwargs)

    with patch.object(projector_module.logger, "info", side_effect=_spy_info):
        await ProjectorHandler(uow_factory).handle(envelope)

    assert len(observed_contextvars) == 1
    assert observed_contextvars[0]["correlation_id"] == "corr-from-original-post"
    # And the binding doesn't leak past the call it was scoped to.
    assert "correlation_id" not in structlog.contextvars.get_contextvars()


@pytest.mark.asyncio
async def test_a_concurrent_modification_at_commit_becomes_a_transient_handler_error(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    # orders.version (version_id_col) raises StaleDataError - translated by
    # SqlAlchemyUnitOfWork.commit() into ConcurrentModificationError - when
    # this order was modified elsewhere between being read and being written
    # back here. The projector must convert that into TransientHandlerError
    # so the consumer requeues it (a fresh read reapplies cleanly) rather
    # than dead-lettering a report that did nothing wrong.
    account, symbol = seeded_account_and_symbol
    order = await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="stalever0001"
    )
    event = ExecutionReportReceived(
        fix_cl_ord_id="stalever0001",
        exec_id="exec-stalever-1",
        exec_type="0",
        ord_status="0",
        symbol="1",
        cum_qty="0",
        leaves_qty="1000",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    stale_error = ConcurrentModificationError("simulated stale version")
    with (
        patch.object(SqlAlchemyUnitOfWork, "commit", side_effect=stale_error),
        pytest.raises(TransientHandlerError),
    ):
        await ProjectorHandler(uow_factory).handle(envelope)

    # Genuinely rolled back, not half-applied: the order is exactly as
    # _seed_submitted_order left it.
    async with uow_factory() as uow:
        refreshed = await uow.orders.get(order.id)
    assert refreshed is not None
    assert refreshed.status == OrderStatus.SUBMITTED


@pytest.mark.asyncio
async def test_execution_report_uses_avg_px_not_last_px_for_avg_fill_price_and_entry_price(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    # Tag 31 (LastPx, this fill's own price) and tag 6 (AvgPx, the broker's
    # cumulative average) must not be conflated - order.avg_fill_price and
    # position.entry_price should reflect AvgPx when the report carries both.
    account, symbol = seeded_account_and_symbol
    order = await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="fillme0002"
    )
    event = ExecutionReportReceived(
        fix_cl_ord_id="fillme0002",
        exec_id="exec-1",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_qty="1000",
        last_price="1.09000",
        avg_price="1.08500",
        position_id="pos-2",
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    await ProjectorHandler(uow_factory).handle(envelope)

    async with uow_factory() as uow:
        refreshed = await uow.orders.get(order.id)
        position = await uow.positions.get_by_broker_position_id(account.id, "pos-2")

    assert refreshed is not None
    assert refreshed.avg_fill_price is not None
    assert refreshed.avg_fill_price.value == Decimal("1.08500")

    assert position is not None
    assert position.entry_price.value == Decimal("1.08500")


@pytest.mark.asyncio
async def test_a_first_report_behind_the_local_clock_still_applies(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    # order.updated_at is set from the local clock at PENDING/SUBMITTED time
    # (see _seed_submitted_order), not from any broker event -
    # last_broker_transact_at is still None. A first report whose TransactTime
    # is *behind* the local clock (the broker's clock running a few seconds
    # slow is normal, not a fault) must not be rejected as out-of-order: there
    # is no prior broker event for it to be out of order relative to.
    account, symbol = seeded_account_and_symbol
    order = await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="clockskew01"
    )
    assert order.last_broker_transact_at is None

    event = ExecutionReportReceived(
        fix_cl_ord_id="clockskew01",
        exec_id="exec-clockskew-1",
        exec_type="0",
        ord_status="0",
        symbol="1",
        cum_qty="0",
        leaves_qty="1000",
        transact_time=_fix_time(offset_seconds=-5),  # 5s behind local wall-clock
        raw="8=FIX.4.4|...",
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    await ProjectorHandler(uow_factory).handle(envelope)

    async with uow_factory() as uow:
        refreshed = await uow.orders.get(order.id)
    assert refreshed is not None
    assert refreshed.status == OrderStatus.ACCEPTED
    assert refreshed.last_broker_transact_at is not None


@pytest.mark.asyncio
async def test_a_report_earlier_than_the_last_broker_transact_at_is_still_rejected(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    # Once an order has a real last_broker_transact_at, the guard must still
    # reject a genuinely out-of-order report - the fix removes a false
    # positive on the *first* report, not the guard itself.
    account, symbol = seeded_account_and_symbol
    order = await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="clockskew02"
    )
    first_report_time = _fix_time()
    first_event = ExecutionReportReceived(
        fix_cl_ord_id="clockskew02",
        exec_id="exec-clockskew-2a",
        exec_type="0",
        ord_status="0",
        symbol="1",
        cum_qty="0",
        leaves_qty="1000",
        transact_time=first_report_time,
        raw="8=FIX.4.4|...",
    )
    await ProjectorHandler(uow_factory).handle(
        build_envelope(
            message_type="ExecutionReportReceived",
            payload=first_event.to_payload(),
            correlation_id="corr-1",
            causation_id="cause-1",
            account_id=str(account.id),
        )
    )
    async with uow_factory() as uow:
        after_first = await uow.orders.get(order.id)
    assert after_first is not None
    assert after_first.status == OrderStatus.ACCEPTED
    assert after_first.last_broker_transact_at is not None

    illegal_before = order_illegal_transition_total.labels(
        from_status=OrderStatus.ACCEPTED.value, to_status=OrderStatus.PARTIALLY_FILLED.value
    )._value.get()

    second_event = ExecutionReportReceived(
        fix_cl_ord_id="clockskew02",
        exec_id="exec-clockskew-2b",
        exec_type="1",
        ord_status="1",
        symbol="1",
        cum_qty="500",
        leaves_qty="500",
        transact_time=_fix_time(offset_seconds=-1),  # behind the order's own last_broker_transact_at
        raw="8=FIX.4.4|...",
        last_qty="500",
        last_price="1.08300",
    )
    await ProjectorHandler(uow_factory).handle(
        build_envelope(
            message_type="ExecutionReportReceived",
            payload=second_event.to_payload(),
            correlation_id="corr-1",
            causation_id="cause-1",
            account_id=str(account.id),
        )
    )

    async with uow_factory() as uow:
        after_second = await uow.orders.get(order.id)
    assert after_second is not None
    assert after_second.status == OrderStatus.ACCEPTED  # unmoved - the report was rejected
    illegal_after = order_illegal_transition_total.labels(
        from_status=OrderStatus.ACCEPTED.value, to_status=OrderStatus.PARTIALLY_FILLED.value
    )._value.get()
    assert illegal_after == illegal_before + 1


@pytest.mark.asyncio
async def test_the_first_report_for_an_order_observes_end_to_end_order_latency(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="latency001"
    )
    sum_before = order_latency_seconds.labels(stage="end_to_end")._sum.get()
    event = ExecutionReportReceived(
        fix_cl_ord_id="latency001",
        exec_id="exec-latency-1",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_qty="1000",
        last_price="1.08000",
        position_id="pos-latency-1",
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    await ProjectorHandler(uow_factory).handle(envelope)

    assert order_latency_seconds.labels(stage="end_to_end")._sum.get() > sum_before


@pytest.mark.asyncio
async def test_a_duplicate_exec_id_changes_nothing(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    order = await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="dupe00001"
    )
    event = ExecutionReportReceived(
        fix_cl_ord_id="dupe00001",
        exec_id="exec-dupe",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_qty="1000",
        last_price="1.08243",
        position_id="pos-dupe",
    )
    handler = ProjectorHandler(uow_factory)
    # Two distinct envelopes (distinct message_id, as a broker retransmission
    # mapped by two separate fix-worker publish() calls would produce) but the
    # same underlying report - the executions table's exec_id uniqueness must
    # catch this even though inbox dedup (keyed on message_id) would not.
    envelope_1 = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )
    envelope_2 = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-2",
        causation_id="cause-2",
        account_id=str(account.id),
    )

    await handler.handle(envelope_1)
    async with uow_factory() as uow:
        after_first = await uow.orders.get(order.id)
    assert after_first is not None
    version_after_first = after_first.version

    await handler.handle(envelope_2)

    async with uow_factory() as uow:
        after_second = await uow.orders.get(order.id)
    assert after_second is not None
    assert after_second.version == version_after_first
    assert after_second.status == OrderStatus.FILLED


@pytest.mark.asyncio
async def test_a_report_on_a_terminal_order_is_logged_but_does_not_move_it(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    order = await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="term00001"
    )
    fill_event = ExecutionReportReceived(
        fix_cl_ord_id="term00001",
        exec_id="exec-fill",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_price="1.08000",
        position_id="pos-term",
    )
    handler = ProjectorHandler(uow_factory)
    await handler.handle(
        build_envelope(
            message_type="ExecutionReportReceived",
            payload=fill_event.to_payload(),
            correlation_id="c1",
            causation_id="c1",
            account_id=str(account.id),
        )
    )

    # A late, out-of-order partial-fill report arrives after the order is
    # already FILLED (terminal) - it should be logged and stored in
    # executions, but must not regress the order.
    late_event = ExecutionReportReceived(
        fix_cl_ord_id="term00001",
        exec_id="exec-late",
        exec_type="1",
        ord_status="1",
        symbol="1",
        cum_qty="500",
        leaves_qty="500",
        transact_time=_fix_time(1),
        raw="8=FIX.4.4|...",
    )
    await handler.handle(
        build_envelope(
            message_type="ExecutionReportReceived",
            payload=late_event.to_payload(),
            correlation_id="c2",
            causation_id="c2",
            account_id=str(account.id),
        )
    )

    async with uow_factory() as uow:
        refreshed = await uow.orders.get(order.id)
    assert refreshed is not None
    assert refreshed.status == OrderStatus.FILLED  # unmoved


@pytest.mark.asyncio
async def test_order_failed_marks_the_order_failed(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    order = await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="failme0001"
    )
    event = OrderFailed(
        reason_code="CANCEL_REJECT", message="unknown order", retryable=False, fix_cl_ord_id="failme0001"
    )
    envelope = build_envelope(
        message_type="OrderFailed",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    await ProjectorHandler(uow_factory).handle(envelope)

    async with uow_factory() as uow:
        refreshed = await uow.orders.get(order.id)
    assert refreshed is not None
    assert refreshed.status == OrderStatus.FAILED
    assert refreshed.reject_reason_code == "CANCEL_REJECT"
    assert refreshed.error_message == "unknown order"
    assert refreshed.terminal_at is not None


@pytest.mark.asyncio
async def test_security_list_received_discovers_and_disables_symbols(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    account, existing_symbol = seeded_account_and_symbol  # "EURUSD", enabled=True
    event = SecurityListReceived(
        symbols=(DiscoveredSymbol(broker_symbol_id=2, name="GBPUSD"),),
        received_at="20260101-00:00:00.000",
    )
    envelope = build_envelope(
        message_type="SecurityListReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    await ProjectorHandler(uow_factory).handle(envelope)

    async with uow_factory() as uow:
        symbols = await uow.symbols.list_by_account(account.id)
    by_name = {s.name: s for s in symbols}

    assert "GBPUSD" in by_name
    discovered = by_name["GBPUSD"]
    assert discovered.broker_symbol_id == 2
    assert discovered.enabled is True
    assert discovered.needs_review is True  # no trading constraints yet

    assert by_name[existing_symbol.name].enabled is False  # absent from this reply


@pytest.mark.asyncio
async def test_security_list_received_applies_a_matching_overlay_entry(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    account, _ = seeded_account_and_symbol
    event = SecurityListReceived(
        symbols=(DiscoveredSymbol(broker_symbol_id=99, name="XAUUSD"),),
        received_at="20260101-00:00:00.000",
    )
    envelope = build_envelope(
        message_type="SecurityListReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )
    overlay = {
        "XAUUSD": SymbolOverlayEntry(
            digits=2,
            lot_size=Quantity(Decimal(100)),
            qty_step=Quantity(Decimal(1)),
            min_qty=Quantity(Decimal(1)),
            max_qty=Quantity(Decimal(10_000)),
            price_step=Price(Decimal("0.01")),
        )
    }

    await ProjectorHandler(uow_factory, overlay).handle(envelope)

    async with uow_factory() as uow:
        symbols = await uow.symbols.list_by_account(account.id)
    by_name = {s.name: s for s in symbols}

    discovered = by_name["XAUUSD"]
    assert discovered.needs_review is False
    assert discovered.source == SymbolSource.OVERLAY
    assert discovered.digits == 2
    assert discovered.lot_size == Quantity(Decimal(100))
    assert discovered.price_step == Price(Decimal("0.01"))


@pytest.mark.asyncio
async def test_security_list_received_leaves_a_manually_configured_symbol_untouched_without_an_overlay_entry(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    account, existing_symbol = seeded_account_and_symbol  # "EURUSD", broker_symbol_id=1
    async with session_factory() as session:
        row = await session.get(SymbolRow, existing_symbol.id)
        assert row is not None
        row.needs_review = False
        row.digits = 4
        row.lot_size = Decimal(50)
        row.source = None
        await session.commit()

    event = SecurityListReceived(
        symbols=(DiscoveredSymbol(broker_symbol_id=1, name="EURUSD"),),
        received_at="20260101-00:00:00.000",
    )
    envelope = build_envelope(
        message_type="SecurityListReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )
    # No overlay entry for EURUSD here - a manually fixed symbol must be
    # left exactly as it was.
    overlay = {"XAUUSD": SymbolOverlayEntry(digits=2)}

    await ProjectorHandler(uow_factory, overlay).handle(envelope)

    async with uow_factory() as uow:
        refreshed = await uow.symbols.get_by_name(account.id, "EURUSD")
    assert refreshed is not None
    assert refreshed.needs_review is False
    assert refreshed.digits == 4
    assert refreshed.lot_size == Quantity(Decimal(50))
    assert refreshed.source is None


@pytest.mark.asyncio
async def test_security_list_received_persists_symbol_names_longer_than_twenty_chars(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    # A real cTrader demo account's SecurityList reply includes CFD names
    # like "TELEPHONE AND DATA SYSTEMS" (27 chars) - the symbols.name column
    # used to be varchar(20), so this insert raised
    # StringDataRightTruncationError and rolled back the *entire* discovery
    # batch (one uow per event), silently discarding all 830 symbols.
    account, _ = seeded_account_and_symbol
    long_name = "TELEPHONE AND DATA SYSTEMS"
    event = SecurityListReceived(
        symbols=(DiscoveredSymbol(broker_symbol_id=21809, name=long_name),),
        received_at="20260101-00:00:00.000",
    )
    envelope = build_envelope(
        message_type="SecurityListReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    await ProjectorHandler(uow_factory).handle(envelope)

    async with uow_factory() as uow:
        symbols = await uow.symbols.list_by_account(account.id)
    by_name = {s.name: s for s in symbols}

    assert long_name in by_name
    assert by_name[long_name].broker_symbol_id == 21809


@pytest.mark.asyncio
async def test_handle_is_a_no_op_on_a_redelivered_envelope(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    make_order: Callable[..., Order],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    order = await _seed_submitted_order(
        session_factory, make_order, account_id=account.id, symbol_id=symbol.id, fix_cl_ord_id="redel0001"
    )
    event = ExecutionReportReceived(
        fix_cl_ord_id="redel0001",
        exec_id="exec-redel",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_price="1.08000",
        position_id="pos-redel",
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
        message_id="fixed-message-id-1",
    )
    handler = ProjectorHandler(uow_factory)

    await handler.handle(envelope)
    async with uow_factory() as uow:
        after_first = await uow.orders.get(order.id)
    assert after_first is not None

    await handler.handle(envelope)  # same message_id: inbox dedup should short-circuit
    async with uow_factory() as uow:
        after_second = await uow.orders.get(order.id)
    assert after_second is not None
    assert after_second.version == after_first.version


@pytest.mark.asyncio
async def test_a_close_orders_fill_closes_the_position(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    now = datetime.now(UTC)
    position = Position(
        id=uuid4(),
        account_id=account.id,
        position_id="pos-close-1",
        symbol_id=symbol.id,
        side=Side.BUY,
        quantity=Quantity(Decimal(1000)),
        entry_price=Price(Decimal("1.08000")),
        status=PositionStatus.CLOSING,
        opened_at=now,
    )
    close_order = Order(
        id=uuid4(),
        account_id=account.id,
        client_order_id=f"close-{uuid4()}",
        symbol_id=symbol.id,
        side=Side.SELL,
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal(1000)),
        status=OrderStatus.SUBMITTED,
        purpose=OrderPurpose.CLOSE,
        created_at=now,
        updated_at=now,
        fix_cl_ord_id="closefix0001",
        submitted_at=now,
        closes_position_id=position.position_id,
    )
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.positions.add(position)
        await uow.orders.add(close_order)
        position.closed_by_order_id = close_order.id
        await uow.positions.update(position)
        await uow.commit()

    event = ExecutionReportReceived(
        fix_cl_ord_id="closefix0001",
        exec_id="exec-close-1",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_price="1.08500",
        position_id=position.position_id,
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    await ProjectorHandler(uow_factory).handle(envelope)

    async with uow_factory() as uow:
        refreshed_order = await uow.orders.get(close_order.id)
        refreshed_position = await uow.positions.get(position.id)

    assert refreshed_order is not None
    assert refreshed_order.status == OrderStatus.FILLED

    assert refreshed_position is not None
    assert refreshed_position.status == PositionStatus.CLOSED
    assert refreshed_position.closed_by_order_id == close_order.id
    assert refreshed_position.closed_at is not None


def _make_protective_order(
    *, account_id, symbol_id, position_id: str, kind: ProtectiveKind, parent_order_id, oco_group_id, fix_cl_ord_id
) -> Order:
    now = datetime.now(UTC)
    is_stop = kind == ProtectiveKind.STOP_LOSS
    return Order(
        id=uuid4(),
        account_id=account_id,
        client_order_id=fix_cl_ord_id,
        symbol_id=symbol_id,
        side=Side.SELL,
        order_type=OrderType.STOP if is_stop else OrderType.LIMIT,
        quantity=Quantity(Decimal(1000)),
        status=OrderStatus.SUBMITTED,
        purpose=OrderPurpose.CLOSE,
        created_at=now,
        updated_at=now,
        fix_cl_ord_id=fix_cl_ord_id,
        submitted_at=now,
        stop_price=Price(Decimal("1.07000")) if is_stop else None,
        limit_price=None if is_stop else Price(Decimal("1.09000")),
        protective_kind=kind,
        protects_position_id=position_id,
        parent_order_id=parent_order_id,
        oco_group_id=oco_group_id,
    )


async def _seed_parent_order(session_factory: async_sessionmaker[AsyncSession], *, account_id, symbol_id) -> Order:
    """A FILLED OPEN order to satisfy a protective order's parent_order_id FK."""
    now = datetime.now(UTC)
    order = Order(
        id=uuid4(),
        account_id=account_id,
        client_order_id=f"bot7-{uuid4()}",
        symbol_id=symbol_id,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal(1000)),
        status=OrderStatus.FILLED,
        purpose=OrderPurpose.OPEN,
        created_at=now,
        updated_at=now,
        fix_cl_ord_id=f"bot7fix{uuid4().hex[:8]}",
        submitted_at=now,
        terminal_at=now,
    )
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(order)
        await uow.commit()
    return order


@pytest.mark.asyncio
async def test_an_open_orders_fill_with_sl_and_tp_stages_protective_orders_command(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    now = datetime.now(UTC)
    order = Order(
        id=uuid4(),
        account_id=account.id,
        client_order_id=f"bot7-{uuid4()}",
        symbol_id=symbol.id,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal(1000)),
        status=OrderStatus.SUBMITTED,
        purpose=OrderPurpose.OPEN,
        created_at=now,
        updated_at=now,
        fix_cl_ord_id="protectopen1",
        submitted_at=now,
        stop_loss=Price(Decimal("1.07000")),
        take_profit=Price(Decimal("1.09000")),
    )
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(order)
        await uow.commit()

    event = ExecutionReportReceived(
        fix_cl_ord_id="protectopen1",
        exec_id="exec-protect-open",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_qty="1000",
        last_price="1.08000",
        position_id="pos-protect-1",
    )
    envelope = build_envelope(
        message_type="ExecutionReportReceived",
        payload=event.to_payload(),
        correlation_id="corr-1",
        causation_id="cause-1",
        account_id=str(account.id),
    )

    await ProjectorHandler(uow_factory).handle(envelope)

    async with uow_factory() as uow:
        position = await uow.positions.get_by_broker_position_id(account.id, "pos-protect-1")
    assert position is not None
    assert position.protection_state == ProtectionState.PENDING
    assert position.stop_loss_order_id is not None
    assert position.take_profit_order_id is not None

    async with session_factory() as session:
        sl_row = await session.get(OrderRow, position.stop_loss_order_id)
        tp_row = await session.get(OrderRow, position.take_profit_order_id)
        outbox_rows = (
            (await session.execute(select(OutboxEventRow).where(OutboxEventRow.aggregate_id == position.id)))
            .scalars()
            .all()
        )

    assert sl_row is not None
    assert sl_row.status == OrderStatus.PENDING
    assert sl_row.protective_kind == ProtectiveKind.STOP_LOSS
    assert sl_row.protects_position_id == "pos-protect-1"
    assert sl_row.parent_order_id == order.id

    assert tp_row is not None
    assert tp_row.status == OrderStatus.PENDING
    assert tp_row.protective_kind == ProtectiveKind.TAKE_PROFIT

    assert sl_row.oco_group_id == tp_row.oco_group_id

    assert len(outbox_rows) == 1
    payload = outbox_rows[0].payload
    assert payload["position_id"] == "pos-protect-1"
    assert payload["stop_loss"]["order_id"] == str(position.stop_loss_order_id)
    assert payload["take_profit"]["order_id"] == str(position.take_profit_order_id)


@pytest.mark.asyncio
async def test_a_protective_orders_accepted_report_moves_protection_state_toward_active(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    now = datetime.now(UTC)
    oco_group_id = uuid4()
    parent_order = await _seed_parent_order(session_factory, account_id=account.id, symbol_id=symbol.id)
    parent_order_id = parent_order.id
    position = Position(
        id=uuid4(),
        account_id=account.id,
        position_id="pos-active-1",
        symbol_id=symbol.id,
        side=Side.BUY,
        quantity=Quantity(Decimal(1000)),
        entry_price=Price(Decimal("1.08000")),
        status=PositionStatus.OPEN,
        opened_at=now,
        protection_state=ProtectionState.PENDING,
    )
    sl_order = _make_protective_order(
        account_id=account.id,
        symbol_id=symbol.id,
        position_id=position.position_id,
        kind=ProtectiveKind.STOP_LOSS,
        parent_order_id=parent_order_id,
        oco_group_id=oco_group_id,
        fix_cl_ord_id="sl:pos-active-1",
    )
    tp_order = _make_protective_order(
        account_id=account.id,
        symbol_id=symbol.id,
        position_id=position.position_id,
        kind=ProtectiveKind.TAKE_PROFIT,
        parent_order_id=parent_order_id,
        oco_group_id=oco_group_id,
        fix_cl_ord_id="tp:pos-active-1",
    )
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(sl_order)
        await uow.orders.add(tp_order)
        position.stop_loss_order_id = sl_order.id
        position.take_profit_order_id = tp_order.id
        await uow.positions.add(position)
        await uow.commit()

    handler = ProjectorHandler(uow_factory)

    async def _accept(fix_cl_ord_id: str, exec_id: str, offset: float) -> None:
        event = ExecutionReportReceived(
            fix_cl_ord_id=fix_cl_ord_id,
            exec_id=exec_id,
            exec_type="0",
            ord_status="0",
            symbol="1",
            cum_qty="0",
            leaves_qty="1000",
            transact_time=_fix_time(offset),
            raw="8=FIX.4.4|...",
        )
        await handler.handle(
            build_envelope(
                message_type="ExecutionReportReceived",
                payload=event.to_payload(),
                correlation_id=exec_id,
                causation_id=exec_id,
                account_id=str(account.id),
            )
        )

    await _accept("sl:pos-active-1", "exec-sl-accept", 1)
    async with uow_factory() as uow:
        after_sl = await uow.positions.get(position.id)
    assert after_sl is not None
    assert after_sl.protection_state == ProtectionState.PARTIAL

    await _accept("tp:pos-active-1", "exec-tp-accept", 2)
    async with uow_factory() as uow:
        after_both = await uow.positions.get(position.id)
    assert after_both is not None
    assert after_both.protection_state == ProtectionState.ACTIVE


@pytest.mark.asyncio
async def test_a_protective_orders_fill_closes_the_position_and_cancels_the_sibling(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    now = datetime.now(UTC)
    oco_group_id = uuid4()
    parent_order = await _seed_parent_order(session_factory, account_id=account.id, symbol_id=symbol.id)
    parent_order_id = parent_order.id
    position = Position(
        id=uuid4(),
        account_id=account.id,
        position_id="pos-oco-1",
        symbol_id=symbol.id,
        side=Side.BUY,
        quantity=Quantity(Decimal(1000)),
        entry_price=Price(Decimal("1.08000")),
        status=PositionStatus.OPEN,
        opened_at=now,
        protection_state=ProtectionState.ACTIVE,
    )
    sl_order = _make_protective_order(
        account_id=account.id,
        symbol_id=symbol.id,
        position_id=position.position_id,
        kind=ProtectiveKind.STOP_LOSS,
        parent_order_id=parent_order_id,
        oco_group_id=oco_group_id,
        fix_cl_ord_id="sl:pos-oco-1",
    )
    tp_order = _make_protective_order(
        account_id=account.id,
        symbol_id=symbol.id,
        position_id=position.position_id,
        kind=ProtectiveKind.TAKE_PROFIT,
        parent_order_id=parent_order_id,
        oco_group_id=oco_group_id,
        fix_cl_ord_id="tp:pos-oco-1",
    )
    sl_order.status = OrderStatus.ACCEPTED
    tp_order.status = OrderStatus.ACCEPTED
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(sl_order)
        await uow.orders.add(tp_order)
        position.stop_loss_order_id = sl_order.id
        position.take_profit_order_id = tp_order.id
        await uow.positions.add(position)
        await uow.commit()

    fill_event = ExecutionReportReceived(
        fix_cl_ord_id="sl:pos-oco-1",
        exec_id="exec-sl-fill",
        exec_type="2",
        ord_status="2",
        symbol="1",
        cum_qty="1000",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        last_qty="1000",
        last_price="1.07000",
        position_id="pos-oco-1",
    )
    await ProjectorHandler(uow_factory).handle(
        build_envelope(
            message_type="ExecutionReportReceived",
            payload=fill_event.to_payload(),
            correlation_id="corr-1",
            causation_id="cause-1",
            account_id=str(account.id),
        )
    )

    async with uow_factory() as uow:
        refreshed_position = await uow.positions.get(position.id)
        refreshed_sl = await uow.orders.get(sl_order.id)
    assert refreshed_position is not None
    assert refreshed_position.status == PositionStatus.CLOSED
    assert refreshed_position.closed_by_order_id == sl_order.id
    assert refreshed_sl is not None
    assert refreshed_sl.status == OrderStatus.FILLED

    async with session_factory() as session:
        outbox_rows = (
            (await session.execute(select(OutboxEventRow).where(OutboxEventRow.aggregate_id == tp_order.id)))
            .scalars()
            .all()
        )
    assert len(outbox_rows) == 1
    assert outbox_rows[0].event_type == "CancelOrderCommand"
    assert outbox_rows[0].payload["order_id"] == str(tp_order.id)
    assert outbox_rows[0].payload["orig_cl_ord_id"] == "tp:pos-oco-1"


@pytest.mark.asyncio
async def test_a_protective_orders_rejection_leaves_the_other_active_as_partial(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    now = datetime.now(UTC)
    oco_group_id = uuid4()
    parent_order = await _seed_parent_order(session_factory, account_id=account.id, symbol_id=symbol.id)
    parent_order_id = parent_order.id
    position = Position(
        id=uuid4(),
        account_id=account.id,
        position_id="pos-partial-1",
        symbol_id=symbol.id,
        side=Side.BUY,
        quantity=Quantity(Decimal(1000)),
        entry_price=Price(Decimal("1.08000")),
        status=PositionStatus.OPEN,
        opened_at=now,
        protection_state=ProtectionState.PENDING,
    )
    sl_order = _make_protective_order(
        account_id=account.id,
        symbol_id=symbol.id,
        position_id=position.position_id,
        kind=ProtectiveKind.STOP_LOSS,
        parent_order_id=parent_order_id,
        oco_group_id=oco_group_id,
        fix_cl_ord_id="sl:pos-partial-1",
    )
    tp_order = _make_protective_order(
        account_id=account.id,
        symbol_id=symbol.id,
        position_id=position.position_id,
        kind=ProtectiveKind.TAKE_PROFIT,
        parent_order_id=parent_order_id,
        oco_group_id=oco_group_id,
        fix_cl_ord_id="tp:pos-partial-1",
    )
    tp_order.status = OrderStatus.ACCEPTED
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(sl_order)
        await uow.orders.add(tp_order)
        position.stop_loss_order_id = sl_order.id
        position.take_profit_order_id = tp_order.id
        await uow.positions.add(position)
        await uow.commit()

    reject_event = ExecutionReportReceived(
        fix_cl_ord_id="sl:pos-partial-1",
        exec_id="exec-sl-reject",
        exec_type="8",
        ord_status="8",
        symbol="1",
        cum_qty="0",
        leaves_qty="0",
        transact_time=_fix_time(),
        raw="8=FIX.4.4|...",
        text="invalid stop price",
    )
    await ProjectorHandler(uow_factory).handle(
        build_envelope(
            message_type="ExecutionReportReceived",
            payload=reject_event.to_payload(),
            correlation_id="corr-1",
            causation_id="cause-1",
            account_id=str(account.id),
        )
    )

    async with uow_factory() as uow:
        refreshed_position = await uow.positions.get(position.id)
        refreshed_sl = await uow.orders.get(sl_order.id)
    assert refreshed_sl is not None
    assert refreshed_sl.status == OrderStatus.REJECTED
    assert refreshed_position is not None
    assert refreshed_position.protection_state == ProtectionState.PARTIAL


@pytest.mark.asyncio
async def test_order_failed_on_a_protective_order_recomputes_protection_state(
    session_factory: async_sessionmaker[AsyncSession],
    seeded_account_and_symbol: tuple[AccountRow, SymbolRow],
    uow_factory,
) -> None:
    account, symbol = seeded_account_and_symbol
    now = datetime.now(UTC)
    oco_group_id = uuid4()
    parent_order = await _seed_parent_order(session_factory, account_id=account.id, symbol_id=symbol.id)
    parent_order_id = parent_order.id
    position = Position(
        id=uuid4(),
        account_id=account.id,
        position_id="pos-failed-1",
        symbol_id=symbol.id,
        side=Side.BUY,
        quantity=Quantity(Decimal(1000)),
        entry_price=Price(Decimal("1.08000")),
        status=PositionStatus.OPEN,
        opened_at=now,
        protection_state=ProtectionState.PENDING,
    )
    sl_order = _make_protective_order(
        account_id=account.id,
        symbol_id=symbol.id,
        position_id=position.position_id,
        kind=ProtectiveKind.STOP_LOSS,
        parent_order_id=parent_order_id,
        oco_group_id=oco_group_id,
        fix_cl_ord_id="sl:pos-failed-1",
    )
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.orders.add(sl_order)
        position.stop_loss_order_id = sl_order.id
        await uow.positions.add(position)
        await uow.commit()

    event = OrderFailed(
        reason_code="BUSINESS_REJECT", message="symbol not tradable", retryable=False, fix_cl_ord_id="sl:pos-failed-1"
    )
    await ProjectorHandler(uow_factory).handle(
        build_envelope(
            message_type="OrderFailed",
            payload=event.to_payload(),
            correlation_id="corr-1",
            causation_id="cause-1",
            account_id=str(account.id),
        )
    )

    async with uow_factory() as uow:
        refreshed_position = await uow.positions.get(position.id)
        refreshed_sl = await uow.orders.get(sl_order.id)
    assert refreshed_sl is not None
    assert refreshed_sl.status == OrderStatus.FAILED
    assert refreshed_position is not None
    assert refreshed_position.protection_state == ProtectionState.FAILED
