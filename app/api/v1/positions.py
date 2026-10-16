"""GET /positions, GET /positions/{id}, POST /positions/{id}/close."""

from uuid import UUID

from fastapi import APIRouter, Depends

from app.api.auth import get_authenticated_account_id
from app.api.dependencies import (
    get_close_position_handler,
    get_correlation_id,
    get_position_query,
    get_positions_query,
)
from app.schemas.positions import (
    ClosePositionResponse,
    PositionProtectionResource,
    PositionResource,
    PositionsResponse,
    ProtectiveOrderResource,
)
from app.services.commands.close_position import ClosePositionCommand, ClosePositionHandler
from app.services.queries.get_position import GetPositionQuery, ListPositionsQuery, PositionView, ProtectiveOrderView

router = APIRouter()


def _protective_order_resource(protective_order: ProtectiveOrderView | None) -> ProtectiveOrderResource | None:
    if protective_order is None:
        return None
    return ProtectiveOrderResource(
        price=protective_order.price, order_id=protective_order.order_id, status=protective_order.status
    )


def _resource(view: PositionView) -> PositionResource:
    return PositionResource(
        position_id=view.position_id,
        symbol=view.symbol,
        side=view.side,
        quantity=view.quantity,
        entry_price=view.entry_price,
        status=view.status,
        opened_at=view.opened_at,
        closed_at=view.closed_at,
        protection=PositionProtectionResource(
            state=view.protection.state,
            stop_loss=_protective_order_resource(view.protection.stop_loss),
            take_profit=_protective_order_resource(view.protection.take_profit),
        ),
    )


@router.get("/positions", response_model=PositionsResponse)
async def list_positions(
    account_id: UUID = Depends(get_authenticated_account_id),
    query: ListPositionsQuery = Depends(get_positions_query),
) -> PositionsResponse:
    """List open positions."""
    views = await query.handle(account_id)
    return PositionsResponse(positions=[_resource(view) for view in views])


@router.get("/positions/{position_id}", response_model=PositionResource)
async def get_position(
    position_id: str,
    account_id: UUID = Depends(get_authenticated_account_id),
    query: GetPositionQuery = Depends(get_position_query),
) -> PositionResource:
    """Fetch one position, keyed by the broker's own position id."""
    view = await query.handle(account_id, position_id)
    return _resource(view)


@router.post("/positions/{position_id}/close", response_model=ClosePositionResponse)
async def close_position(
    position_id: str,
    account_id: UUID = Depends(get_authenticated_account_id),
    handler: ClosePositionHandler = Depends(get_close_position_handler),
    correlation_id: str = Depends(get_correlation_id),
) -> ClosePositionResponse:
    """Close a position by an opposite market order carrying tag 721."""
    result = await handler.handle(
        ClosePositionCommand(account_id=account_id, position_id=position_id, correlation_id=correlation_id)
    )
    return ClosePositionResponse(
        order_id=str(result.order_id),
        client_order_id=result.client_order_id,
        status=result.status,
        created_at=result.created_at,
    )
