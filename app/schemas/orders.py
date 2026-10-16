"""POST /orders, GET /orders/{id}, GET /orders/{id}/status DTOs."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from app.domain.enums import OrderStatus, OrderType, Side, TimeInForce


class PlaceOrderRequest(BaseModel):
    """POST /orders request body. Exactly one of quantity/volume_lots."""

    client_order_id: str
    symbol: str
    side: Side
    order_type: OrderType
    quantity: Decimal | None = None
    volume_lots: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    comment: str | None = None


class OrderLinks(BaseModel):
    """Hyperlinks on a freshly placed order."""

    model_config = ConfigDict(populate_by_name=True)

    href_self: str = Field(alias="self")
    status: str


class PlaceOrderResponse(BaseModel):
    """202 (or 200 on an idempotent replay) response body."""

    order_id: str
    client_order_id: str
    status: OrderStatus
    created_at: datetime
    links: OrderLinks


class OrderResource(BaseModel):
    """GET /orders/{id} response body: the full order resource."""

    order_id: str
    client_order_id: str
    status: OrderStatus
    quantity: Decimal
    filled_quantity: Decimal
    avg_fill_price: Decimal | None
    position_id: str | None
    reject_reason: str | None
    created_at: datetime
    updated_at: datetime


class OrdersListResponse(BaseModel):
    """GET /orders response body: one page, newest first."""

    orders: list[OrderResource]
    next_cursor: str | None


class OrderStatusResource(BaseModel):
    """GET /orders/{id}/status response body: order status projection."""

    order_id: str
    client_order_id: str
    status: OrderStatus
    filled_quantity: Decimal
    avg_fill_price: Decimal | None
    position_id: str | None
    reject_reason: str | None
    updated_at: datetime


class CancelOrderResponse(BaseModel):
    """POST /orders/{id}/cancel response body."""

    order_id: str
    status: OrderStatus
