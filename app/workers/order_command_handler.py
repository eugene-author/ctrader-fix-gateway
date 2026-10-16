"""Handles order/position/protective-order commands."""

import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

import structlog

from app.adapters.brokers.ctrader.message_factory import CTraderMessageFactory
from app.core.metrics import orders_sent_total
from app.domain.commands import (
    CancelOrderCommand,
    ClosePositionCommand,
    CreateOrderCommand,
    PlaceProtectiveOrdersCommand,
    ProtectiveOrder,
)
from app.domain.enums import OrderStatus, OrderType, Side
from app.domain.errors import ConcurrentModificationError, IllegalOrderTransitionError
from app.domain.events import OrderSubmitted
from app.domain.state_machine import OrderTransition, apply_transition
from app.ports.fix_session import FixSession
from app.ports.publisher import EventPublisher
from app.ports.unit_of_work import UnitOfWork
from app.queue.consumer import TransientHandlerError
from app.queue.serialization import Envelope

logger = structlog.get_logger()

CONSUMER_NAME = "fix-worker"

_CREATE_ORDER_COMMAND = "CreateOrderCommand"
_CANCEL_ORDER_COMMAND = "CancelOrderCommand"
_CLOSE_POSITION_COMMAND = "ClosePositionCommand"
_PLACE_PROTECTIVE_ORDERS_COMMAND = "PlaceProtectiveOrdersCommand"


def _hash_payload(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


class OrderCommandHandler:
    """Processes order/position commands against one TRADE SessionManager."""

    def __init__(
        self,
        *,
        session: FixSession,
        uow_factory: Callable[[], UnitOfWork],
        publisher: EventPublisher,
        message_factory: CTraderMessageFactory,
    ) -> None:
        """Store every collaborator."""
        self._session = session
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._message_factory = message_factory
        self._handlers: dict[str, Callable[[Envelope], Awaitable[None]]] = {
            _CREATE_ORDER_COMMAND: self._handle_create_order,
            _CANCEL_ORDER_COMMAND: self._handle_cancel_order,
            _CLOSE_POSITION_COMMAND: self._handle_close_position,
            _PLACE_PROTECTIVE_ORDERS_COMMAND: self._handle_place_protective_orders,
        }

    async def handle(self, envelope: Envelope) -> None:
        """Routes one envelope. An unrecognized message type is logged and dropped."""
        handler = self._handlers.get(envelope.message_type)
        if handler is None:
            logger.warning("order_command_handler_unknown_message_type", message_type=envelope.message_type)
            return

        if not self._session.is_logged_on():
            raise TransientHandlerError("TRADE session is not logged on")

        try:
            await handler(envelope)
        except ConcurrentModificationError as exc:
            raise TransientHandlerError(str(exc)) from exc

    async def _handle_create_order(self, envelope: Envelope) -> None:
        command = CreateOrderCommand.from_payload(envelope.payload)
        await self._dispatch_new_order_single(
            envelope,
            order_id=command.order_id,
            fix_cl_ord_id=command.fix_cl_ord_id,
            symbol=command.symbol,
            side=command.side,
            order_type=command.order_type,
            build_message=lambda: self._message_factory.new_order(
                cl_ord_id=command.fix_cl_ord_id,
                broker_symbol_id=command.broker_symbol_id,
                side=command.side,
                order_type=command.order_type,
                quantity=command.quantity.value,
                price=command.limit_price.value if command.limit_price is not None else None,
                stop_price=command.stop_price.value if command.stop_price is not None else None,
            ),
        )

    async def _handle_close_position(self, envelope: Envelope) -> None:
        command = ClosePositionCommand.from_payload(envelope.payload)
        await self._dispatch_new_order_single(
            envelope,
            order_id=command.order_id,
            fix_cl_ord_id=command.fix_cl_ord_id,
            symbol=command.symbol,
            side=command.side,
            order_type=OrderType.MARKET,
            build_message=lambda: self._message_factory.new_order(
                cl_ord_id=command.fix_cl_ord_id,
                broker_symbol_id=command.broker_symbol_id,
                side=command.side,
                order_type=OrderType.MARKET,
                quantity=command.quantity.value,
                position_id=command.position_id,
            ),
        )

    async def _dispatch_new_order_single(
        self,
        envelope: Envelope,
        *,
        order_id: UUID,
        fix_cl_ord_id: str,
        symbol: str,
        side: Side,
        order_type: OrderType,
        build_message: Callable[[], object],
    ) -> None:
        """Inbox deduplication once, apply the dispatch guard for one order, send, and publish OrderSubmitted."""
        payload_hash = _hash_payload(envelope.payload)
        dispatch_result: tuple[object, datetime | None] | None = None
        async with self._uow_factory() as uow:
            is_new = await uow.inbox.claim(
                consumer=CONSUMER_NAME, message_id=envelope.message_id, payload_hash=payload_hash
            )
            if is_new:
                dispatch_result = await self._dispatch_order(
                    uow, order_id=order_id, fix_cl_ord_id=fix_cl_ord_id, build_message=build_message
                )
            await uow.commit()

        if dispatch_result is None:
            return
        await self._send_and_publish(
            envelope,
            order_id=order_id,
            fix_cl_ord_id=fix_cl_ord_id,
            dispatch_result=dispatch_result,
            symbol=symbol,
            side=side,
            order_type=order_type,
        )

    async def _dispatch_order(
        self, uow: UnitOfWork, *, order_id: UUID, fix_cl_ord_id: str, build_message: Callable[[], object]
    ) -> tuple[object, datetime | None] | None:
        """Apply the dispatch guard for one order inside an already-open, already-deduped transaction."""
        order = await uow.orders.get(order_id)
        if order is None:
            logger.error("order_command_handler_order_not_found", order_id=str(order_id))
            return None

        if order.submitted_at is not None:
            # Dispatch guard: already dispatched once.
            return self._message_factory.order_status(cl_ord_id=fix_cl_ord_id), None

        # Commit DISPATCHING before sending
        dispatched_at = datetime.now(UTC)
        order.fix_cl_ord_id = fix_cl_ord_id
        order.status = OrderStatus.DISPATCHING
        order.submitted_at = dispatched_at
        await uow.orders.update(order)
        return build_message(), dispatched_at

    async def _send_and_publish(
        self,
        envelope: Envelope,
        *,
        order_id: UUID,
        fix_cl_ord_id: str,
        dispatch_result: tuple[object, datetime | None],
        symbol: str,
        side: Side,
        order_type: OrderType,
    ) -> None:
        message, dispatched_at = dispatch_result
        if dispatched_at is None:
            self._session.send(message)
            return

        try:
            self._session.send(message)
        except Exception as exc:
            logger.error("order_command_handler_send_failed", order_id=str(order_id), error=str(exc))
            await self._record_dispatch_outcome(
                order_id=order_id,
                dispatched_at=dispatched_at,
                target_status=OrderStatus.DISPATCH_UNCERTAIN,
                error=str(exc),
            )
            return

        await self._record_dispatch_outcome(
            order_id=order_id, dispatched_at=dispatched_at, target_status=OrderStatus.SUBMITTED
        )

        orders_sent_total.labels(symbol=symbol, side=side.value, order_type=order_type.value).inc()

        event = OrderSubmitted(order_id=order_id, fix_cl_ord_id=fix_cl_ord_id, submitted_at=dispatched_at)
        await self._publisher.publish(
            routing_key="order.submitted",
            message_type="OrderSubmitted",
            payload=event.to_payload(),
            correlation_id=envelope.correlation_id,
            causation_id=envelope.message_id,
            account_id=envelope.account_id,
        )

    async def _record_dispatch_outcome(
        self, *, order_id: UUID, dispatched_at: datetime, target_status: OrderStatus, error: str | None = None
    ) -> None:
        """Record whether send() actually reached the destination."""
        async with self._uow_factory() as uow:
            order = await uow.orders.get(order_id)
            if order is not None and order.status is OrderStatus.DISPATCHING:
                try:
                    apply_transition(
                        OrderTransition(status=order.status, transact_time=dispatched_at),
                        OrderTransition(status=target_status, transact_time=datetime.now(UTC)),
                    )
                except IllegalOrderTransitionError:
                    pass
                else:
                    order.status = target_status
                    if error is not None:
                        order.error_message = error
                    await uow.orders.update(order)
            await uow.commit()

    async def _handle_cancel_order(self, envelope: Envelope) -> None:
        command = CancelOrderCommand.from_payload(envelope.payload)
        payload_hash = _hash_payload(envelope.payload)

        async with self._uow_factory() as uow:
            is_new = await uow.inbox.claim(
                consumer=CONSUMER_NAME, message_id=envelope.message_id, payload_hash=payload_hash
            )
            if not is_new:
                await uow.commit()
                return

            order = await uow.orders.get(command.order_id)
            if order is None:
                logger.error("order_command_handler_order_not_found", order_id=str(command.order_id))
                await uow.commit()  # a command for an unknown order is genuinely done; keep it suppressed
                return

            # Send inside the transaction, before the commit
            self._session.send(
                self._message_factory.cancel_order(
                    cl_ord_id=command.fix_cl_ord_id,
                    orig_cl_ord_id=command.orig_cl_ord_id,
                    broker_symbol_id=command.broker_symbol_id,
                )
            )
            await uow.commit()

    async def _handle_place_protective_orders(self, envelope: Envelope) -> None:
        """Dispatch one or both halves of the envelope under a single inbox entry."""
        command = PlaceProtectiveOrdersCommand.from_payload(envelope.payload)
        payload_hash = _hash_payload(envelope.payload)
        pending: list[tuple[UUID, str, tuple[object, datetime | None], OrderType]] = []

        async with self._uow_factory() as uow:
            is_new = await uow.inbox.claim(
                consumer=CONSUMER_NAME, message_id=envelope.message_id, payload_hash=payload_hash
            )
            if is_new:
                if command.stop_loss is not None:
                    await self._prepare_protective_order(
                        uow, command, command.stop_loss, order_type=OrderType.STOP, pending=pending
                    )
                if command.take_profit is not None:
                    await self._prepare_protective_order(
                        uow, command, command.take_profit, order_type=OrderType.LIMIT, pending=pending
                    )
            await uow.commit()

        for order_id, fix_cl_ord_id, dispatch_result, order_type in pending:
            await self._send_and_publish(
                envelope,
                order_id=order_id,
                fix_cl_ord_id=fix_cl_ord_id,
                dispatch_result=dispatch_result,
                symbol=command.symbol,
                side=command.close_side,
                order_type=order_type,
            )

    async def _prepare_protective_order(
        self,
        uow: UnitOfWork,
        command: PlaceProtectiveOrdersCommand,
        protective_order: ProtectiveOrder,
        *,
        order_type: OrderType,
        pending: list[tuple[UUID, str, tuple[object, datetime | None], OrderType]],
    ) -> None:
        def build_message() -> object:
            price_kwargs = (
                {"stop_price": protective_order.price.value}
                if order_type is OrderType.STOP
                else {"price": protective_order.price.value}
            )
            return self._message_factory.new_order(
                cl_ord_id=protective_order.fix_cl_ord_id,
                broker_symbol_id=command.broker_symbol_id,
                side=command.close_side,
                order_type=order_type,
                quantity=command.quantity.value,
                position_id=command.position_id,
                **price_kwargs,
            )

        result = await self._dispatch_order(
            uow,
            order_id=protective_order.order_id,
            fix_cl_ord_id=protective_order.fix_cl_ord_id,
            build_message=build_message,
        )
        if result is not None:
            pending.append((protective_order.order_id, protective_order.fix_cl_ord_id, result, order_type))
