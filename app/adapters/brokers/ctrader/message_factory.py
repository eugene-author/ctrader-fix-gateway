"""Builds ctrader_fix request messages from domain values.

One FIX session (TRADE or QUOTE) needs exactly one FIX config for its whole
lifetime, so CTraderMessageFactory stores it once rather than threading it
through every call site - construct one instance per session.
"""

from collections.abc import Mapping
from decimal import Decimal

from ctrader_fix import (
    LogoutRequest,
    MarketDataRequest,
    NewOrderSingle,
    OrderCancelRequest,
    OrderStatusRequest,
    SecurityListRequest,
)

from app.adapters.brokers.ctrader import order_mapper
from app.domain.enums import OrderType, Side

_MAX_CL_ORD_ID_LENGTH = 30


def _assert_cl_ord_id_length(cl_ord_id: str) -> None:
    if len(cl_ord_id) > _MAX_CL_ORD_ID_LENGTH:
        raise ValueError(f"cl_ord_id must be <= {_MAX_CL_ORD_ID_LENGTH} chars, got {len(cl_ord_id)}: {cl_ord_id!r}")


class CTraderMessageFactory:
    """Builds outbound FIX messages for one sessions config, stored once."""

    def __init__(self, config: Mapping[str, object]) -> None:
        """Store the FIX config - one TRADE or QUOTE sessions."""
        self._config = config

    def new_order(
        self,
        *,
        cl_ord_id: str,
        broker_symbol_id: int,
        side: Side,
        order_type: OrderType,
        quantity: Decimal,
        price: Decimal | None = None,
        stop_price: Decimal | None = None,
        position_id: str | None = None,
    ) -> NewOrderSingle:
        """Build a NewOrderSingle (35=D)."""
        _assert_cl_ord_id_length(cl_ord_id)

        order = NewOrderSingle(self._config)
        order.ClOrdID = cl_ord_id
        order.Symbol = broker_symbol_id
        order.Side = order_mapper.side_to_fix(side)
        order.OrdType = order_mapper.order_type_to_fix(order_type)
        order.OrderQty = order_mapper.format_quantity(quantity)
        if price is not None:
            order.Price = order_mapper.format_price(price)
        if stop_price is not None:
            order.StopPx = order_mapper.format_price(stop_price)
        if position_id is not None:
            order.PosMaintRptID = position_id
        return order

    def cancel_order(self, *, cl_ord_id: str, orig_cl_ord_id: str, broker_symbol_id: int) -> OrderCancelRequest:
        """Build an OrderCancelRequest (35=F) for a working order that hasn't filled."""
        _assert_cl_ord_id_length(cl_ord_id)

        cancel = OrderCancelRequest(self._config)
        cancel.ClOrdID = cl_ord_id
        cancel.OrigClOrdID = orig_cl_ord_id
        cancel.Symbol = broker_symbol_id
        return cancel

    def market_data_request(self, *, md_req_id: str, broker_symbol_id: int, entry_type: str) -> MarketDataRequest:
        """Build one MarketDataRequest (35=V) subscribing to `broker_symbol_id`.

        One request per symbol.
        """
        request = MarketDataRequest(self._config)
        request.MDReqID = md_req_id
        request.SubscriptionRequestType = "1"
        request.MarketDepth = 1
        request.MDUpdateType = 1
        request.NoMDEntryTypes = 1
        request.MDEntryType = entry_type
        request.NoRelatedSym = 1
        request.Symbol = broker_symbol_id
        return request

    def order_status(self, *, cl_ord_id: str) -> OrderStatusRequest:
        """Build an OrderStatusRequest (35=H)."""
        request = OrderStatusRequest(self._config)
        request.ClOrdID = cl_ord_id
        return request

    def security_list(self, *, req_id: str) -> SecurityListRequest:
        """Build a SecurityListRequest (35=x) for the full tradable symbol list."""
        request = SecurityListRequest(self._config)
        request.SecurityReqID = req_id
        request.SecurityListRequestType = 0
        return request

    def logout(self) -> LogoutRequest:
        """Build a LogoutRequest (35=5), sent on graceful shutdown."""
        return LogoutRequest(self._config)
