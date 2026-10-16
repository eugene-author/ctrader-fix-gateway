"""Startup reconciliation, mandatory on every boot: protective (SL/TP) brackets and uncertain dispatches."""

from collections.abc import Callable
from uuid import UUID

import structlog

from app.adapters.brokers.ctrader.message_factory import CTraderMessageFactory
from app.domain.enums import OrderStatus, OrderType, PositionStatus, ProtectionState
from app.domain.models import Order
from app.ports.fix_session import FixSession
from app.ports.unit_of_work import UnitOfWork

logger = structlog.get_logger()

_LIVE_STATUSES = frozenset({OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED})


class ProtectiveOrderReconciler:
    """Re-places any protective order that isn't currently live at the broker, for one account."""

    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        session: FixSession,
        message_factory: CTraderMessageFactory,
        account_id: UUID,
    ) -> None:
        """Store every collaborator."""
        self._uow_factory = uow_factory
        self._session = session
        self._message_factory = message_factory
        self._account_id = account_id

    async def reconcile(self) -> None:
        """Replace any protective order that isn't currently live at the broker."""
        messages = []
        async with self._uow_factory() as uow:
            positions = await uow.positions.list_by_account(self._account_id, status=PositionStatus.OPEN.value)
            for position in positions:
                if position.protection_state in (ProtectionState.NONE, ProtectionState.ACTIVE):
                    continue
                for protective_order_id, ord_type in (
                    (position.stop_loss_order_id, OrderType.STOP),
                    (position.take_profit_order_id, OrderType.LIMIT),
                ):
                    if protective_order_id is None:
                        continue
                    protective_order = await uow.orders.get(protective_order_id)
                    message = await self._build_resend_message(uow, protective_order, ord_type)
                    if message is not None:
                        messages.append(message)

        for message in messages:
            self._session.send(message)

        if messages:
            logger.info("protective_orders_reconciliation_resent", protective_order_count=len(messages))

    async def _build_resend_message(
        self, uow: UnitOfWork, protective_order: Order | None, ord_type: OrderType
    ) -> object | None:
        if protective_order is None or protective_order.status in _LIVE_STATUSES:
            return None
        if protective_order.fix_cl_ord_id is None:
            return None

        price = protective_order.stop_price if ord_type is OrderType.STOP else protective_order.limit_price
        if price is None:
            logger.error("protective_reconciliation_missing_price", order_id=str(protective_order.id))
            return None

        symbol = await uow.symbols.get_by_id(protective_order.symbol_id)
        if symbol is None or symbol.broker_symbol_id is None:
            logger.error("protective_reconciliation_symbol_unresolvable", order_id=str(protective_order.id))
            return None

        price_kwargs = {"stop_price": price.value} if ord_type is OrderType.STOP else {"price": price.value}
        logger.info(
            "protective_reconciliation_resending",
            order_id=str(protective_order.id),
            position_id=protective_order.protects_position_id,
            fix_cl_ord_id=protective_order.fix_cl_ord_id,
        )
        return self._message_factory.new_order(
            cl_ord_id=protective_order.fix_cl_ord_id,
            broker_symbol_id=symbol.broker_symbol_id,
            side=protective_order.side,
            order_type=ord_type,
            quantity=protective_order.quantity.value,
            position_id=protective_order.protects_position_id,
            **price_kwargs,
        )


class OrderDispatchReconciler:
    """Re-queries the broker for any order left DISPATCHING/DISPATCH_UNCERTAIN, for one account.

    Both statuses mean "we don't know whether the broker actually received
    this order's NewOrderSingle"
    """

    _UNCERTAIN_STATUSES = frozenset({OrderStatus.DISPATCHING, OrderStatus.DISPATCH_UNCERTAIN})

    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork],
        session: FixSession,
        message_factory: CTraderMessageFactory,
        account_id: UUID,
    ) -> None:
        """Store every collaborator."""
        self._uow_factory = uow_factory
        self._session = session
        self._message_factory = message_factory
        self._account_id = account_id

    async def reconcile(self) -> None:
        """Send an OrderStatusRequest for every order left DISPATCHING/DISPATCH_UNCERTAIN."""
        async with self._uow_factory() as uow:
            uncertain_orders = await uow.orders.list_by_account_and_statuses(self._account_id, self._UNCERTAIN_STATUSES)

        for order in uncertain_orders:
            if order.fix_cl_ord_id is None:
                # Not actually reachable
                logger.error("order_dispatch_reconciliation_missing_cl_ord_id", order_id=str(order.id))
                continue
            logger.info(
                "order_dispatch_reconciliation_querying_status",
                order_id=str(order.id),
                fix_cl_ord_id=order.fix_cl_ord_id,
                status=order.status,
            )
            self._session.send(self._message_factory.order_status(cl_ord_id=order.fix_cl_ord_id))
