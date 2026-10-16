"""Versioned message-bus commands."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from app.domain.enums import OrderType, Side, TimeInForce
from app.domain.models import Price, Quantity


@dataclass(frozen=True, slots=True)
class CreateOrderCommand:
    """What the fix-worker needs to send as a NewOrderSingle."""

    order_id: UUID
    client_order_id: str
    fix_cl_ord_id: str
    symbol: str
    broker_symbol_id: int
    side: Side
    order_type: OrderType
    quantity: Quantity
    time_in_force: TimeInForce
    limit_price: Price | None = None
    stop_price: Price | None = None

    def to_payload(self) -> dict[str, object]:
        """Render as the outbox_events.payload JSON."""
        return {
            "order_id": str(self.order_id),
            "client_order_id": self.client_order_id,
            "fix_cl_ord_id": self.fix_cl_ord_id,
            "symbol": self.symbol,
            "broker_symbol_id": self.broker_symbol_id,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": str(self.quantity.value),
            "limit_price": str(self.limit_price.value) if self.limit_price is not None else None,
            "stop_price": str(self.stop_price.value) if self.stop_price is not None else None,
            "time_in_force": self.time_in_force.value,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> CreateOrderCommand:
        """Parse an outbox_events.payload / envelope payload back into a CreateOrderCommand."""
        return cls(
            order_id=UUID(payload["order_id"]),
            client_order_id=payload["client_order_id"],
            fix_cl_ord_id=payload["fix_cl_ord_id"],
            symbol=payload["symbol"],
            broker_symbol_id=int(payload["broker_symbol_id"]),
            side=Side(payload["side"]),
            order_type=OrderType(payload["order_type"]),
            quantity=Quantity(Decimal(payload["quantity"])),
            time_in_force=TimeInForce(payload["time_in_force"]),
            limit_price=Price(Decimal(payload["limit_price"])) if payload.get("limit_price") is not None else None,
            stop_price=Price(Decimal(payload["stop_price"])) if payload.get("stop_price") is not None else None,
        )


@dataclass(frozen=True, slots=True)
class CancelOrderCommand:
    """What the fix-worker needs to send as an OrderCancelRequest."""

    order_id: UUID
    fix_cl_ord_id: str
    orig_cl_ord_id: str
    symbol: str
    broker_symbol_id: int

    def to_payload(self) -> dict[str, object]:
        """Render as the outbox_events.payload JSON."""
        return {
            "order_id": str(self.order_id),
            "fix_cl_ord_id": self.fix_cl_ord_id,
            "orig_cl_ord_id": self.orig_cl_ord_id,
            "symbol": self.symbol,
            "broker_symbol_id": self.broker_symbol_id,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> CancelOrderCommand:
        """Parse an outbox_events.payload / envelope payload back into a CancelOrderCommand."""
        return cls(
            order_id=UUID(payload["order_id"]),
            fix_cl_ord_id=payload["fix_cl_ord_id"],
            orig_cl_ord_id=payload["orig_cl_ord_id"],
            symbol=payload["symbol"],
            broker_symbol_id=int(payload["broker_symbol_id"]),
        )


@dataclass(frozen=True, slots=True)
class ClosePositionCommand:
    """What the fix-worker needs to send as a closing NewOrderSingle.

    Closing a position is a new market order on the opposite side carrying
    the position id in tag 721 - not an OrderCancelRequest.
    """

    order_id: UUID
    position_id: str
    fix_cl_ord_id: str
    symbol: str
    broker_symbol_id: int
    side: Side
    quantity: Quantity

    def to_payload(self) -> dict[str, object]:
        """Render as the outbox_events.payload JSON."""
        return {
            "order_id": str(self.order_id),
            "position_id": self.position_id,
            "fix_cl_ord_id": self.fix_cl_ord_id,
            "symbol": self.symbol,
            "broker_symbol_id": self.broker_symbol_id,
            "side": self.side.value,
            "quantity": str(self.quantity.value),
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> ClosePositionCommand:
        """Parse an outbox_events.payload / envelope payload back into a ClosePositionCommand."""
        return cls(
            order_id=UUID(payload["order_id"]),
            position_id=payload["position_id"],
            fix_cl_ord_id=payload["fix_cl_ord_id"],
            symbol=payload["symbol"],
            broker_symbol_id=int(payload["broker_symbol_id"]),
            side=Side(payload["side"]),
            quantity=Quantity(Decimal(payload["quantity"])),
        )


@dataclass(frozen=True, slots=True)
class ProtectiveOrder:
    """One half (SL or TP) of a bracket, as a PlaceProtectiveOrdersCommand needs it."""

    order_id: UUID
    fix_cl_ord_id: str
    price: Price


@dataclass(frozen=True, slots=True)
class PlaceProtectiveOrdersCommand:
    """What the fix-worker needs to send one or both halves of a bracket.

    `stop_loss`/`take_profit` are independently optional - only the ones the
    original order actually requested are present.
    """

    position_id: str
    symbol: str
    broker_symbol_id: int
    close_side: Side
    quantity: Quantity
    oco_group_id: UUID
    stop_loss: ProtectiveOrder | None = None
    take_profit: ProtectiveOrder | None = None

    def to_payload(self) -> dict[str, object]:
        """Render as the outbox_events.payload JSON."""
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "broker_symbol_id": self.broker_symbol_id,
            "close_side": self.close_side.value,
            "quantity": str(self.quantity.value),
            "oco_group_id": str(self.oco_group_id),
            "stop_loss": (
                {
                    "order_id": str(self.stop_loss.order_id),
                    "fix_cl_ord_id": self.stop_loss.fix_cl_ord_id,
                    "stop_price": str(self.stop_loss.price.value),
                }
                if self.stop_loss is not None
                else None
            ),
            "take_profit": (
                {
                    "order_id": str(self.take_profit.order_id),
                    "fix_cl_ord_id": self.take_profit.fix_cl_ord_id,
                    "price": str(self.take_profit.price.value),
                }
                if self.take_profit is not None
                else None
            ),
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> PlaceProtectiveOrdersCommand:
        """Parse an outbox_events.payload / envelope payload back into a PlaceProtectiveOrdersCommand."""
        stop_loss = payload.get("stop_loss")
        take_profit = payload.get("take_profit")
        return cls(
            position_id=payload["position_id"],
            symbol=payload["symbol"],
            broker_symbol_id=int(payload["broker_symbol_id"]),
            close_side=Side(payload["close_side"]),
            quantity=Quantity(Decimal(payload["quantity"])),
            oco_group_id=UUID(payload["oco_group_id"]),
            stop_loss=(
                ProtectiveOrder(
                    order_id=UUID(stop_loss["order_id"]),
                    fix_cl_ord_id=stop_loss["fix_cl_ord_id"],
                    price=Price(Decimal(stop_loss["stop_price"])),
                )
                if stop_loss is not None
                else None
            ),
            take_profit=(
                ProtectiveOrder(
                    order_id=UUID(take_profit["order_id"]),
                    fix_cl_ord_id=take_profit["fix_cl_ord_id"],
                    price=Price(Decimal(take_profit["price"])),
                )
                if take_profit is not None
                else None
            ),
        )
