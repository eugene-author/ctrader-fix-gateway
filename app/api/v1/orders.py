"""POST /orders, GET /orders, GET /orders/{id}, GET /orders/{id}/status."""

import hashlib
import json
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response, status

from app.api.auth import get_authenticated_account_id
from app.api.dependencies import (
    get_cancel_order_handler,
    get_correlation_id,
    get_order_query,
    get_order_status_query,
    get_orders_list_query,
    get_place_order_handler,
)
from app.api.rate_limit import rate_limit_orders
from app.domain.enums import OrderStatus
from app.schemas.orders import (
    CancelOrderResponse,
    OrderLinks,
    OrderResource,
    OrdersListResponse,
    OrderStatusResource,
    PlaceOrderRequest,
    PlaceOrderResponse,
)
from app.services.commands.cancel_order import CancelOrderCommand, CancelOrderHandler
from app.services.commands.place_order import PlaceOrderCommand, PlaceOrderHandler
from app.services.queries.get_order import GetOrderQuery, GetOrderStatusQuery
from app.services.queries.list_orders import ListOrdersQuery

router = APIRouter()

_ORDERS_BASE_PATH = "/api/v1/orders"


def _fingerprint(body: PlaceOrderRequest) -> str:
    canonical = json.dumps(body.model_dump(mode="json"), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


@router.post(
    "/orders",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PlaceOrderResponse,
    dependencies=[Depends(rate_limit_orders)],
)
async def place_order(
    body: PlaceOrderRequest,
    request: Request,
    response: Response,
    account_id: UUID = Depends(get_authenticated_account_id),
    handler: PlaceOrderHandler = Depends(get_place_order_handler),
    correlation_id: str = Depends(get_correlation_id),
) -> PlaceOrderResponse:
    """Place an order. Returns accepted response."""
    command = PlaceOrderCommand(
        account_id=account_id,
        client_order_id=body.client_order_id,
        symbol=body.symbol,
        side=body.side,
        order_type=body.order_type,
        request_fingerprint=_fingerprint(body),
        quantity=body.quantity,
        volume_lots=body.volume_lots,
        time_in_force=body.time_in_force,
        limit_price=body.limit_price,
        stop_price=body.stop_price,
        stop_loss=body.stop_loss,
        take_profit=body.take_profit,
        idempotency_key=request.headers.get("Idempotency-Key"),
        correlation_id=correlation_id,
    )
    result = await handler.handle(command)

    if result.idempotency_replayed:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotency-Replayed"] = "true"

    return PlaceOrderResponse(
        order_id=str(result.order_id),
        client_order_id=result.client_order_id,
        status=result.status,
        created_at=result.created_at,
        links=OrderLinks(
            href_self=f"{_ORDERS_BASE_PATH}/{result.order_id}",
            status=f"{_ORDERS_BASE_PATH}/{result.order_id}/status",
        ),
    )


@router.get("/orders", response_model=OrdersListResponse)
async def list_orders(
    status_filter: list[OrderStatus] | None = Query(default=None, alias="status"),
    symbol: str | None = None,
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    account_id: UUID = Depends(get_authenticated_account_id),
    query: ListOrdersQuery = Depends(get_orders_list_query),
) -> OrdersListResponse:
    """List orders for the authenticated account, newest first, keyset-paginated."""
    page = await query.handle(
        account_id,
        statuses=frozenset(status_filter) if status_filter else None,
        symbol=symbol,
        created_from=from_,
        created_to=to,
        cursor=cursor,
        limit=limit,
    )
    return OrdersListResponse(
        orders=[
            OrderResource(
                order_id=str(view.order_id),
                client_order_id=view.client_order_id,
                status=view.status,
                quantity=view.quantity,
                filled_quantity=view.filled_quantity,
                avg_fill_price=view.avg_fill_price,
                position_id=view.position_id,
                reject_reason=view.reject_reason,
                created_at=view.created_at,
                updated_at=view.updated_at,
            )
            for view in page.orders
        ],
        next_cursor=page.next_cursor,
    )


@router.get("/orders/{order_id}", response_model=OrderResource)
async def get_order(
    order_id: UUID,
    account_id: UUID = Depends(get_authenticated_account_id),
    query: GetOrderQuery = Depends(get_order_query),
) -> OrderResource:
    """Fetch the full order resource."""
    view = await query.handle(account_id, order_id)
    return OrderResource(
        order_id=str(view.order_id),
        client_order_id=view.client_order_id,
        status=view.status,
        quantity=view.quantity,
        filled_quantity=view.filled_quantity,
        avg_fill_price=view.avg_fill_price,
        position_id=view.position_id,
        reject_reason=view.reject_reason,
        created_at=view.created_at,
        updated_at=view.updated_at,
    )


@router.get("/orders/{order_id}/status", response_model=OrderStatusResource)
async def get_order_status(
    order_id: UUID,
    account_id: UUID = Depends(get_authenticated_account_id),
    query: GetOrderStatusQuery = Depends(get_order_status_query),
) -> OrderStatusResource:
    """Fetch the order status."""
    view = await query.handle(account_id, order_id)
    return OrderStatusResource(
        order_id=str(view.order_id),
        client_order_id=view.client_order_id,
        status=view.status,
        filled_quantity=view.filled_quantity,
        avg_fill_price=view.avg_fill_price,
        position_id=view.position_id,
        reject_reason=view.reject_reason,
        updated_at=view.updated_at,
    )


@router.post("/orders/{order_id}/cancel", response_model=CancelOrderResponse)
async def cancel_order(
    order_id: UUID,
    account_id: UUID = Depends(get_authenticated_account_id),
    handler: CancelOrderHandler = Depends(get_cancel_order_handler),
    correlation_id: str = Depends(get_correlation_id),
) -> CancelOrderResponse:
    """Cancel a working order: sends OrderCancelRequest, does not fill or close anything itself."""
    result = await handler.handle(
        CancelOrderCommand(account_id=account_id, order_id=order_id, correlation_id=correlation_id)
    )
    return CancelOrderResponse(order_id=str(result.order_id), status=result.status)
