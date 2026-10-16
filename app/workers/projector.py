"""Projector: the only process that writes execution reports, positions and symbols.

A separate process from fix-worker and the relay. Its asyncio,
since it only ever reads envelopes off events.projector and writes to Postgres.
"""

import asyncio
import contextlib
import enum
import hashlib
import json
import signal
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import aio_pika
import structlog
from aio_pika.abc import AbstractRobustConnection
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

from app.core.config import ProjectorSettings
from app.core.logging import configure_logging
from app.core.metrics import (
    order_illegal_transition_total,
    order_latency_seconds,
    orders_failed_total,
    protection_failures_total,
    protection_gap_seconds,
    symbols_changed_total,
    symbols_discovered_total,
)
from app.core.metrics_server import start_metrics_server
from app.core.sentry import init_sentry
from app.core.symbol_overlay import SymbolOverlayEntry, load_symbol_overlay
from app.core.tracing import init_tracing
from app.db.base import new_uuid7
from app.db.session import create_engine, create_session_factory
from app.db.unit_of_work import SqlAlchemyUnitOfWork
from app.domain.commands import CancelOrderCommand, PlaceProtectiveOrdersCommand, ProtectiveOrder
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
from app.domain.errors import ConcurrentModificationError, DuplicateKeyError, IllegalOrderTransitionError
from app.domain.events import DiscoveredSymbol, ExecutionReportReceived, OrderFailed, SecurityListReceived
from app.domain.models import Execution, Order, Position, Price, Quantity, Symbol
from app.domain.state_machine import OrderTransition, apply_transition, is_terminal
from app.ports.unit_of_work import UnitOfWork
from app.queue.consumer import TransientHandlerError, consume
from app.queue.serialization import Envelope
from app.queue.topology import EVENTS_QUEUE, declare_topology

logger = structlog.get_logger()

CONSUMER_NAME = "projector"

_ORD_STATUS_TO_ORDER_STATUS: dict[str, OrderStatus] = {
    "0": OrderStatus.ACCEPTED,
    "1": OrderStatus.PARTIALLY_FILLED,
    "2": OrderStatus.FILLED,
    "4": OrderStatus.CANCELLED,
    "8": OrderStatus.REJECTED,
    "C": OrderStatus.EXPIRED,
}

_PRE_REPORT_STATUSES = frozenset({OrderStatus.DISPATCHING, OrderStatus.DISPATCH_UNCERTAIN, OrderStatus.SUBMITTED})

# --- helpers -----------------------------------------------------------------


def _hash(envelope: Envelope) -> str:
    return hashlib.sha256(json.dumps(envelope.payload, sort_keys=True).encode()).hexdigest()


def _parse_fix_time(value: str) -> datetime:
    """Parse a FIX UTCTimestamp, which legally may or may not carry fractional seconds."""
    if not value:
        return datetime.now(UTC)
    for fmt in ("%Y%m%d-%H:%M:%S.%f", "%Y%m%d-%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    logger.error("fix_timestamp_unparseable", value=value)
    return datetime.now(UTC)


def _target_status(event: ExecutionReportReceived) -> OrderStatus | None:
    if event.exec_type == "0" or event.ord_status == "0":
        return OrderStatus.ACCEPTED
    return _ORD_STATUS_TO_ORDER_STATUS.get(event.ord_status)


def _opposite_side(side: Side) -> Side:
    return Side.SELL if side is Side.BUY else Side.BUY


def _resolve_avg_fill_price(order: Order, event: ExecutionReportReceived) -> Price | None:
    """Prefer the broker's own cumulative AvgPx (tag 6) over a locally computed average."""
    if event.avg_price is not None:
        return Price(Decimal(event.avg_price))
    if event.last_price is None or event.last_qty is None:
        return order.avg_fill_price

    last_price = Decimal(event.last_price)
    last_qty = Decimal(event.last_qty)
    if last_qty <= 0:
        return order.avg_fill_price

    prior_qty = order.filled_quantity.value
    if order.avg_fill_price is None or prior_qty <= 0:
        return Price(last_price)

    total_qty = prior_qty + last_qty
    weighted_avg = ((order.avg_fill_price.value * prior_qty) + (last_price * last_qty)) / total_qty
    return Price(weighted_avg)


def _update_order_for_report(
    order: Order, target_status: OrderStatus, event: ExecutionReportReceived, transact_time: datetime
) -> None:
    order.status = target_status
    order.updated_at = transact_time
    order.last_broker_transact_at = transact_time
    order.avg_fill_price = _resolve_avg_fill_price(order, event)
    order.filled_quantity = Quantity(Decimal(event.cum_qty))
    if event.position_id is not None:
        order.position_id = event.position_id
    if is_terminal(target_status):
        order.terminal_at = transact_time


def _build_protective_order(
    order: Order, position: Position, kind: ProtectiveKind, price: Price, transact_time: datetime, *, oco_group_id: UUID
) -> Order:
    """Build one not-yet-dispatched SL/TP order protecting `position`."""
    is_stop_loss = kind is ProtectiveKind.STOP_LOSS
    prefix = "sl" if is_stop_loss else "tp"
    return Order(
        id=new_uuid7(),
        account_id=order.account_id,
        client_order_id=f"{prefix}:{position.position_id}"[:30],
        symbol_id=order.symbol_id,
        side=_opposite_side(position.side),
        order_type=OrderType.STOP if is_stop_loss else OrderType.LIMIT,
        quantity=position.quantity,
        status=OrderStatus.PENDING,
        purpose=OrderPurpose.CLOSE,
        created_at=transact_time,
        updated_at=transact_time,
        last_broker_transact_at=transact_time,
        stop_price=price if is_stop_loss else None,
        limit_price=None if is_stop_loss else price,
        protective_kind=kind,
        protects_position_id=position.position_id,
        parent_order_id=order.id,
        oco_group_id=oco_group_id,
    )


class SymbolChange(enum.Enum):
    """What `_upsert_symbol` did to one row, so its caller can pick the right metric and log line."""

    DISCOVERED = enum.auto()
    ID_CHANGED = enum.auto()
    UPDATED = enum.auto()


async def _upsert_symbol(
    uow: UnitOfWork,
    account_id: UUID,
    current: Symbol | None,
    incoming: DiscoveredSymbol,
    overlay: Mapping[str, SymbolOverlayEntry],
) -> SymbolChange:
    """Insert a newly discovered symbol or refresh an existing one, applying any matching overlay entry."""
    entry = overlay.get(incoming.name)

    if current is None:
        constraints = entry or SymbolOverlayEntry()
        await uow.symbols.upsert(
            Symbol(
                id=new_uuid7(),
                account_id=account_id,
                name=incoming.name,
                enabled=True,
                subscribe=True,
                needs_review=entry is None,
                broker_symbol_id=incoming.broker_symbol_id,
                digits=constraints.digits,
                lot_size=constraints.lot_size,
                qty_step=constraints.qty_step,
                min_qty=constraints.min_qty,
                max_qty=constraints.max_qty,
                price_step=constraints.price_step,
                source=SymbolSource.OVERLAY if entry else None,
                last_seen_at=datetime.now(UTC),
            )
        )
        return SymbolChange.DISCOVERED

    id_changed = current.broker_symbol_id is not None and current.broker_symbol_id != incoming.broker_symbol_id
    broker_symbol_id = current.broker_symbol_id if id_changed else incoming.broker_symbol_id

    updated = replace(current, enabled=True, broker_symbol_id=broker_symbol_id, last_seen_at=datetime.now(UTC))
    if entry is not None:
        updated = replace(
            updated,
            needs_review=False,
            digits=entry.digits,
            lot_size=entry.lot_size,
            qty_step=entry.qty_step,
            min_qty=entry.min_qty,
            max_qty=entry.max_qty,
            price_step=entry.price_step,
            source=SymbolSource.OVERLAY,
        )
    await uow.symbols.upsert(updated)
    return SymbolChange.ID_CHANGED if id_changed else SymbolChange.UPDATED


# --- ProjectorHandler: routing + inbox deduplication + the single transaction -------


class ProjectorHandler:
    """Dispatches one event envelope per handle() call to its apply_* method."""

    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        symbol_overlay: Mapping[str, SymbolOverlayEntry] | None = None,
    ) -> None:
        """Store the factory and the symbol trading-constraints overlay."""
        self._uow_factory = uow_factory
        self._symbol_overlay = symbol_overlay or {}

    async def handle(self, envelope: Envelope) -> None:
        """Route one envelope by message_type. An unrecognized type is logged and dropped."""
        async with self._uow_factory() as uow:
            is_new = await uow.inbox.claim(
                consumer=CONSUMER_NAME, message_id=envelope.message_id, payload_hash=_hash(envelope)
            )
            if not is_new:
                await uow.commit()
                return

            if envelope.message_type == "ExecutionReportReceived":
                await self._apply_execution_report(
                    uow, envelope.account_id, ExecutionReportReceived.from_payload(envelope.payload)
                )
            elif envelope.message_type == "OrderFailed":
                await self._apply_order_failed(uow, OrderFailed.from_payload(envelope.payload))
            elif envelope.message_type == "SecurityListReceived":
                await self._apply_security_list_received(
                    uow, envelope.account_id, SecurityListReceived.from_payload(envelope.payload)
                )
            else:
                logger.info("projector_ignored_message_type", message_type=envelope.message_type)

            try:
                await uow.commit()
            except DuplicateKeyError:
                logger.info("projector_duplicate_report_ignored", message_id=envelope.message_id)
            except ConcurrentModificationError as exc:
                raise TransientHandlerError(str(exc)) from exc

    # --- execution reports ----------------------------------------------------

    async def _apply_execution_report(self, uow: UnitOfWork, account_id: str, event: ExecutionReportReceived) -> None:
        order = await uow.orders.get_by_fix_cl_ord_id(event.fix_cl_ord_id)
        if order is None:
            logger.error("execution_report_unknown_order", fix_cl_ord_id=event.fix_cl_ord_id)
            return

        if order.correlation_id is None:
            await self._apply_execution_report_for_order(uow, order, event)
            return

        structlog.contextvars.bind_contextvars(correlation_id=order.correlation_id)
        try:
            await self._apply_execution_report_for_order(uow, order, event)
        finally:
            structlog.contextvars.unbind_contextvars("correlation_id")

    async def _apply_execution_report_for_order(
        self, uow: UnitOfWork, order: Order, event: ExecutionReportReceived
    ) -> None:
        if event.exec_id is None:
            logger.error("execution_report_missing_exec_id", fix_cl_ord_id=event.fix_cl_ord_id)
            return

        transact_time = _parse_fix_time(event.transact_time)
        execution = Execution(
            id=new_uuid7(),
            account_id=order.account_id,
            order_id=order.id,
            exec_id=event.exec_id,
            exec_type=event.exec_type,
            ord_status=event.ord_status,
            last_qty=Quantity(Decimal(event.last_qty)) if event.last_qty is not None else None,
            last_price=Price(Decimal(event.last_price)) if event.last_price else None,
            cum_qty=Quantity(Decimal(event.cum_qty)),
            leaves_qty=Quantity(Decimal(event.leaves_qty)),
            position_id=event.position_id,
            text=event.text,
            transact_time=transact_time,
            raw_message=event.raw,
            received_at=datetime.now(UTC),
        )

        await uow.executions.add(execution)

        target_status = _target_status(event)
        if target_status is None:
            logger.warning("execution_report_unmapped_status", ord_status=event.ord_status, exec_type=event.exec_type)
            return

        try:
            apply_transition(
                OrderTransition(status=order.status, transact_time=order.last_broker_transact_at or transact_time),
                OrderTransition(status=target_status, transact_time=transact_time),
            )
        except IllegalOrderTransitionError:
            order_illegal_transition_total.labels(from_status=order.status.value, to_status=target_status.value).inc()
            logger.error(
                "order_illegal_transition", order_id=str(order.id), from_status=order.status, to_status=target_status
            )
            return

        if order.status in _PRE_REPORT_STATUSES:
            order_latency_seconds.labels(stage="end_to_end").observe((transact_time - order.created_at).total_seconds())

        _update_order_for_report(order, target_status, event, transact_time)
        await uow.orders.update(order)
        logger.info("order_execution_report_applied", order_id=str(order.id), status=target_status.value)

        if order.protective_kind is not None:
            await self._apply_protective_order_transition(uow, order, target_status, transact_time)
            return

        if target_status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED) and event.position_id is not None:
            position = await self._apply_position_effects(
                uow, order, event, event.position_id, target_status, transact_time
            )
            if (
                position is not None
                and order.purpose == OrderPurpose.OPEN
                and target_status == OrderStatus.FILLED
                and position.protection_state == ProtectionState.NONE
                and (order.stop_loss is not None or order.take_profit is not None)
            ):
                await self._place_protective_orders(uow, order, position, transact_time)

    async def _apply_position_effects(
        self,
        uow: UnitOfWork,
        order: Order,
        event: ExecutionReportReceived,
        position_id: str,
        target_status: OrderStatus,
        transact_time: datetime,
    ) -> Position | None:
        """Open/update a position for an OPEN order's fill, or close one for a CLOSE order's fill."""
        existing = await uow.positions.get_by_broker_position_id(order.account_id, position_id)

        if order.purpose == OrderPurpose.CLOSE:
            if existing is None:
                logger.error("close_order_fill_unknown_position", position_id=position_id, order_id=str(order.id))
                return None
            if target_status == OrderStatus.FILLED:
                existing.status = PositionStatus.CLOSED
                existing.closed_by_order_id = order.id
                existing.closed_at = transact_time
                await uow.positions.update(existing)
            return existing

        quantity = Quantity(Decimal(event.cum_qty))
        if existing is None:
            entry_price_raw = event.avg_price or event.last_price
            if entry_price_raw is None:
                logger.warning("position_open_missing_price", position_id=position_id)
                return None
            position = Position(
                id=new_uuid7(),
                account_id=order.account_id,
                position_id=position_id,
                symbol_id=order.symbol_id,
                side=order.side,
                quantity=quantity,
                entry_price=Price(Decimal(entry_price_raw)),
                status=PositionStatus.OPEN,
                opened_at=transact_time,
                opened_by_order_id=order.id,
            )
            await uow.positions.add(position)
            return position

        existing.quantity = quantity
        await uow.positions.update(existing)
        return existing

    # --- protective orders -----------------------------------------------------

    async def _place_protective_orders(
        self, uow: UnitOfWork, order: Order, position: Position, transact_time: datetime
    ) -> None:
        """Synthesize SL/TP as real orders once the opening order fills."""
        symbol = await uow.symbols.get_by_id(order.symbol_id)
        if symbol is None or symbol.broker_symbol_id is None:
            logger.error(
                "protective_orders_symbol_unresolvable",
                symbol_id=str(order.symbol_id),
                position_id=position.position_id,
            )
            return

        oco_group_id = uuid4()
        close_side = _opposite_side(position.side)
        stop_loss_order: ProtectiveOrder | None = None
        take_profit_order: ProtectiveOrder | None = None

        if order.stop_loss is not None:
            sl_order = _build_protective_order(
                order, position, ProtectiveKind.STOP_LOSS, order.stop_loss, transact_time, oco_group_id=oco_group_id
            )
            await uow.orders.add(sl_order)
            stop_loss_order = ProtectiveOrder(
                order_id=sl_order.id, fix_cl_ord_id=sl_order.client_order_id, price=order.stop_loss
            )
            position.stop_loss_order_id = sl_order.id

        if order.take_profit is not None:
            tp_order = _build_protective_order(
                order, position, ProtectiveKind.TAKE_PROFIT, order.take_profit, transact_time, oco_group_id=oco_group_id
            )
            await uow.orders.add(tp_order)
            take_profit_order = ProtectiveOrder(
                order_id=tp_order.id, fix_cl_ord_id=tp_order.client_order_id, price=order.take_profit
            )
            position.take_profit_order_id = tp_order.id

        command = PlaceProtectiveOrdersCommand(
            position_id=position.position_id,
            symbol=symbol.name,
            broker_symbol_id=symbol.broker_symbol_id,
            close_side=close_side,
            quantity=position.quantity,
            oco_group_id=oco_group_id,
            stop_loss=stop_loss_order,
            take_profit=take_profit_order,
        )
        await uow.outbox.add(
            account_id=order.account_id,
            aggregate_type="position",
            aggregate_id=position.id,
            event_type="PlaceProtectiveOrdersCommand",
            payload=command.to_payload(),
            routing_key="position.protect",
            correlation_id=str(position.id),
            causation_id=str(order.id),
        )
        position.protection_state = ProtectionState.PENDING
        await uow.positions.update(position)
        logger.info("protective_orders_staged", position_id=position.position_id, order_id=str(order.id))

    async def _apply_protective_order_transition(
        self, uow: UnitOfWork, order: Order, target_status: OrderStatus, transact_time: datetime
    ) -> None:
        """Apply a protective (SL/TP) order's own execution report to its position."""
        if order.protects_position_id is None:
            logger.error("protective_order_missing_protects_position_id", order_id=str(order.id))
            return

        position = await uow.positions.get_by_broker_position_id(order.account_id, order.protects_position_id)
        if position is None:
            logger.error(
                "protective_order_unknown_position", position_id=order.protects_position_id, order_id=str(order.id)
            )
            return

        if target_status == OrderStatus.FILLED:
            position.status = PositionStatus.CLOSED
            position.closed_by_order_id = order.id
            position.closed_at = transact_time
            await uow.positions.update(position)
            await self._cancel_oco_sibling(uow, position, order)
            return

        if target_status in (OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.EXPIRED, OrderStatus.FAILED):
            protection_failures_total.labels(reason=order.reject_reason_code or target_status.value).inc()
            logger.error(
                "protective_order_failed",
                position_id=position.position_id,
                order_id=str(order.id),
                protective_kind=order.protective_kind,
                status=target_status,
            )

        previous_state = position.protection_state
        position.protection_state = await self._compute_protection_state(uow, position)
        await uow.positions.update(position)

        if position.protection_state == ProtectionState.ACTIVE and previous_state != ProtectionState.ACTIVE:
            symbol = await uow.symbols.get_by_id(position.symbol_id)
            symbol_name = symbol.name if symbol is not None else "UNKNOWN"
            protection_gap_seconds.labels(symbol=symbol_name).observe(
                (transact_time - position.opened_at).total_seconds()
            )

    async def _compute_protection_state(self, uow: UnitOfWork, position: Position) -> ProtectionState:
        required_ids = [oid for oid in (position.stop_loss_order_id, position.take_profit_order_id) if oid is not None]
        if not required_ids:
            return ProtectionState.NONE

        live_count = 0
        for protective_order_id in required_ids:
            protective_order = await uow.orders.get(protective_order_id)
            if protective_order is not None and protective_order.status in (
                OrderStatus.ACCEPTED,
                OrderStatus.PARTIALLY_FILLED,
            ):
                live_count += 1

        if live_count == len(required_ids):
            return ProtectionState.ACTIVE
        if live_count > 0:
            return ProtectionState.PARTIAL
        return ProtectionState.FAILED

    async def _cancel_oco_sibling(self, uow: UnitOfWork, position: Position, filled_protective_order: Order) -> None:
        """Stage a CancelOrderCommand for the bracket half that didn't fill."""
        sibling_id = (
            position.take_profit_order_id
            if filled_protective_order.protective_kind == ProtectiveKind.STOP_LOSS
            else position.stop_loss_order_id
        )
        if sibling_id is None or sibling_id == filled_protective_order.id:
            return

        sibling = await uow.orders.get(sibling_id)
        if sibling is None or is_terminal(sibling.status):
            return

        if sibling.fix_cl_ord_id is None:
            now = datetime.now(UTC)
            sibling.status = OrderStatus.CANCELLED
            sibling.updated_at = now
            sibling.terminal_at = now
            await uow.orders.update(sibling)
            return

        symbol = await uow.symbols.get_by_id(sibling.symbol_id)
        if symbol is None or symbol.broker_symbol_id is None:
            logger.error("oco_cancel_symbol_unresolvable", order_id=str(sibling.id))
            return

        cancel_command = CancelOrderCommand(
            order_id=sibling.id,
            fix_cl_ord_id=new_uuid7().hex[:30],
            orig_cl_ord_id=sibling.fix_cl_ord_id,
            symbol=symbol.name,
            broker_symbol_id=symbol.broker_symbol_id,
        )
        await uow.outbox.add(
            account_id=sibling.account_id,
            aggregate_type="order",
            aggregate_id=sibling.id,
            event_type="CancelOrderCommand",
            payload=cancel_command.to_payload(),
            routing_key="order.cancel",
            correlation_id=str(sibling.id),
            causation_id=str(filled_protective_order.id),
        )
        logger.info("oco_sibling_cancel_staged", position_id=position.position_id, cancelled_order_id=str(sibling.id))

    # --- security list -----------------------------------------------------------

    async def _apply_security_list_received(
        self, uow: UnitOfWork, account_id: str, event: SecurityListReceived
    ) -> None:
        account_uuid = UUID(account_id)
        existing = await uow.symbols.list_by_account(account_uuid)
        by_name = {symbol.name: symbol for symbol in existing}
        discovered_names = {s.name for s in event.symbols}

        for discovered in event.symbols:
            current = by_name.get(discovered.name)
            change = await _upsert_symbol(uow, account_uuid, current, discovered, self._symbol_overlay)
            if change is SymbolChange.DISCOVERED:
                symbols_discovered_total.inc()
            elif change is SymbolChange.ID_CHANGED and current is not None:
                # Loud event: a symbol whose broker id changed for the same
                # name. The old, known-safe id was kept (see _upsert_symbol).
                symbols_changed_total.labels(kind="id_changed").inc()
                logger.error(
                    "symbol_broker_id_changed",
                    name=discovered.name,
                    old_broker_symbol_id=current.broker_symbol_id,
                    new_broker_symbol_id=discovered.broker_symbol_id,
                )

        for name, current in by_name.items():
            if name in discovered_names or not current.enabled:
                continue
            symbols_changed_total.labels(kind="disabled").inc()
            await uow.symbols.upsert(
                Symbol(
                    id=current.id,
                    account_id=account_uuid,
                    name=current.name,
                    enabled=False,
                    subscribe=current.subscribe,
                    needs_review=current.needs_review,
                    broker_symbol_id=current.broker_symbol_id,
                    digits=current.digits,
                    lot_size=current.lot_size,
                    qty_step=current.qty_step,
                    min_qty=current.min_qty,
                    max_qty=current.max_qty,
                    price_step=current.price_step,
                    source=current.source,
                    last_seen_at=current.last_seen_at,
                )
            )

        logger.info("security_list_applied", discovered_count=len(event.symbols))

    # --- order failures ----------------------------------------------------------

    async def _apply_order_failed(self, uow: UnitOfWork, event: OrderFailed) -> None:
        order = None
        if event.order_id is not None:
            order = await uow.orders.get(event.order_id)
        if order is None and event.fix_cl_ord_id is not None:
            order = await uow.orders.get_by_fix_cl_ord_id(event.fix_cl_ord_id)
        if order is None:
            logger.error("order_failed_unknown_order", order_id=event.order_id, fix_cl_ord_id=event.fix_cl_ord_id)
            return

        if is_terminal(order.status):
            logger.info("order_failed_already_terminal", order_id=str(order.id), status=order.status)
            return

        try:
            apply_transition(
                OrderTransition(status=order.status, transact_time=order.last_broker_transact_at or datetime.now(UTC)),
                OrderTransition(status=OrderStatus.FAILED, transact_time=datetime.now(UTC)),
            )
        except IllegalOrderTransitionError:
            order_illegal_transition_total.labels(
                from_status=order.status.value, to_status=OrderStatus.FAILED.value
            ).inc()
            logger.error(
                "order_illegal_transition",
                order_id=str(order.id),
                from_status=order.status,
                to_status=OrderStatus.FAILED,
            )
            return

        now = datetime.now(UTC)
        order.status = OrderStatus.FAILED
        order.reject_reason_code = event.reason_code
        order.error_message = event.message
        order.updated_at = now
        order.terminal_at = now
        await uow.orders.update(order)
        orders_failed_total.labels(reason_code=order.reject_reason_code or "UNKNOWN").inc()

        if order.protects_position_id is not None:
            position = await uow.positions.get_by_broker_position_id(order.account_id, order.protects_position_id)
            if position is not None:
                position.protection_state = await self._compute_protection_state(uow, position)
                await uow.positions.update(position)
                protection_failures_total.labels(reason=order.reject_reason_code or "UNKNOWN").inc()
                logger.error(
                    "protective_order_failed",
                    position_id=position.position_id,
                    order_id=str(order.id),
                    protective_kind=order.protective_kind,
                    status=OrderStatus.FAILED,
                )

        logger.info("order_failed_applied", order_id=str(order.id), reason_code=order.reject_reason_code)


# --- entrypoint ----------------------------------------------------------------


def main() -> None:
    """Load settings, configure logging, and consume events.projector."""
    settings = ProjectorSettings()
    configure_logging(env=settings.app_env, log_level=settings.app_log_level)
    init_sentry(dsn=settings.sentry_dsn, env=settings.app_env, traces_sample_rate=settings.sentry_traces_sample_rate)
    init_tracing(
        service_name=f"{settings.app_name}-projector",
        env=settings.app_env,
        otlp_endpoint=settings.otel_exporter_otlp_endpoint,
    )
    start_metrics_server(settings.metrics_port)
    asyncio.run(_run(settings))


async def _run(settings: ProjectorSettings) -> None:
    """Link the DB and AMQP connections and consume until stop_event is set."""
    engine = create_engine(settings.database_url)
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
    session_factory = create_session_factory(engine)

    connection: AbstractRobustConnection = await aio_pika.connect_robust(settings.rabbitmq_url)
    await declare_topology(connection)

    def uow_factory() -> UnitOfWork:
        return SqlAlchemyUnitOfWork(session_factory)

    symbol_overlay = load_symbol_overlay(settings.symbols_overlay_path)
    handler = ProjectorHandler(uow_factory, symbol_overlay)

    stop_event = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop_event.set)

    consume_task = asyncio.ensure_future(consume(connection, EVENTS_QUEUE, handler.handle))
    logger.info("projector_started")
    try:
        await stop_event.wait()
    finally:
        logger.info("projector_shutting_down")
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task
        await connection.close()
        await engine.dispose()
        logger.info("projector_shutdown_complete")


if __name__ == "__main__":
    main()
