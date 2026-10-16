"""Versioned message-bus events.

The worker emits raw payload. the projector interprets them.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class OrderSubmitted:
    """A NewOrderSingle left the process."""

    order_id: UUID
    fix_cl_ord_id: str
    submitted_at: datetime

    def to_payload(self) -> dict[str, object]:
        """Render as the event envelope's JSON payload."""
        return {
            "order_id": str(self.order_id),
            "fix_cl_ord_id": self.fix_cl_ord_id,
            "submitted_at": self.submitted_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ExecutionReportReceived:
    """A 35=8 ExecutionReport, mapped but not yet interpreted."""

    fix_cl_ord_id: str
    exec_id: str | None
    exec_type: str
    ord_status: str
    symbol: str | None
    cum_qty: str
    leaves_qty: str
    transact_time: str
    raw: str
    order_id: UUID | None = None
    last_qty: str | None = None
    last_price: str | None = None
    avg_price: str | None = None
    position_id: str | None = None
    text: str | None = None

    def to_payload(self) -> dict[str, object]:
        """Render as the event envelope's JSON payload."""
        return {
            "order_id": str(self.order_id) if self.order_id is not None else None,
            "fix_cl_ord_id": self.fix_cl_ord_id,
            "exec_id": self.exec_id,
            "exec_type": self.exec_type,
            "ord_status": self.ord_status,
            "symbol": self.symbol,
            "last_qty": self.last_qty,
            "last_price": self.last_price,
            "avg_price": self.avg_price,
            "cum_qty": self.cum_qty,
            "leaves_qty": self.leaves_qty,
            "position_id": self.position_id,
            "text": self.text,
            "transact_time": self.transact_time,
            "raw": self.raw,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> ExecutionReportReceived:
        """Parse an event envelope's payload back into an ExecutionReportReceived."""
        order_id = payload.get("order_id")
        return cls(
            fix_cl_ord_id=payload["fix_cl_ord_id"],
            exec_id=payload.get("exec_id"),
            exec_type=payload["exec_type"],
            ord_status=payload["ord_status"],
            symbol=payload.get("symbol"),
            cum_qty=payload["cum_qty"],
            leaves_qty=payload["leaves_qty"],
            transact_time=payload["transact_time"],
            raw=payload["raw"],
            order_id=UUID(order_id) if order_id is not None else None,
            last_qty=payload.get("last_qty"),
            last_price=payload.get("last_price"),
            avg_price=payload.get("avg_price"),
            position_id=payload.get("position_id"),
            text=payload.get("text"),
        )


@dataclass(frozen=True, slots=True)
class OrderFailed:
    """An order never reached the broker, or was rejected by it."""

    reason_code: str
    message: str
    retryable: bool
    order_id: UUID | None = None
    fix_cl_ord_id: str | None = None

    def to_payload(self) -> dict[str, object]:
        """Render as the event envelope's JSON payload."""
        return {
            "order_id": str(self.order_id) if self.order_id is not None else None,
            "fix_cl_ord_id": self.fix_cl_ord_id,
            "reason_code": self.reason_code,
            "message": self.message,
            "retryable": self.retryable,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> OrderFailed:
        """Parse an event envelope's payload back into an OrderFailed."""
        order_id = payload.get("order_id")
        return cls(
            reason_code=payload["reason_code"],
            message=payload["message"],
            retryable=bool(payload["retryable"]),
            order_id=UUID(order_id) if order_id is not None else None,
            fix_cl_ord_id=payload.get("fix_cl_ord_id"),
        )


@dataclass(frozen=True, slots=True)
class SessionStateChanged:
    """A FIX session's lifecycle state changed."""

    session_type: str
    state: str
    reconnect_count: int

    def to_payload(self) -> dict[str, object]:
        """Render as the event envelope's JSON payload."""
        return {
            "session_type": self.session_type,
            "state": self.state,
            "reconnect_count": self.reconnect_count,
        }


@dataclass(frozen=True, slots=True)
class DiscoveredSymbol:
    """One entry from a 35=y SecurityList reply's repeating group."""

    broker_symbol_id: int
    name: str


@dataclass(frozen=True, slots=True)
class SecurityListReceived:
    """A 35=y SecurityList reply, mapped but not yet interpreted."""

    symbols: tuple[DiscoveredSymbol, ...]
    received_at: str

    def to_payload(self) -> dict[str, object]:
        """Render as the event envelope's JSON payload."""
        return {
            "symbols": [{"broker_symbol_id": s.broker_symbol_id, "name": s.name} for s in self.symbols],
            "received_at": self.received_at,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> SecurityListReceived:
        """Parse an event envelope's payload back into a SecurityListReceived."""
        symbols = payload.get("symbols") or []
        return cls(
            symbols=tuple(
                DiscoveredSymbol(broker_symbol_id=int(s["broker_symbol_id"]), name=str(s["name"])) for s in symbols
            ),
            received_at=str(payload.get("received_at") or ""),
        )
