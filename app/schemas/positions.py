"""GET /positions[/{id}], POST /positions/{id}/close DTOs."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel

from app.domain.enums import OrderStatus, PositionStatus, ProtectionState, Side


class ProtectiveOrderResource(BaseModel):
    """One half (SL or TP) of a bracket's current state."""

    price: Decimal
    order_id: UUID
    status: OrderStatus


class PositionProtectionResource(BaseModel):
    """The `protection` block."""

    state: ProtectionState
    stop_loss: ProtectiveOrderResource | None
    take_profit: ProtectiveOrderResource | None


class PositionResource(BaseModel):
    """One position resource."""

    position_id: str
    symbol: str
    side: Side
    quantity: Decimal
    entry_price: Decimal
    status: PositionStatus
    opened_at: datetime
    closed_at: datetime | None
    protection: PositionProtectionResource


class PositionsResponse(BaseModel):
    """GET /positions response body."""

    positions: list[PositionResource]


class ClosePositionResponse(BaseModel):
    """POST /positions/{id}/close response body: the closing order."""

    order_id: str
    client_order_id: str
    status: OrderStatus
    created_at: datetime
