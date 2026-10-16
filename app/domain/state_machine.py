"""The order status state machine."""

from dataclasses import dataclass
from datetime import datetime

from app.domain.enums import OrderStatus
from app.domain.errors import IllegalOrderTransitionError

_REPORT_TARGETS: frozenset[OrderStatus] = frozenset(
    {
        OrderStatus.ACCEPTED,
        OrderStatus.REJECTED,
        OrderStatus.FAILED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.EXPIRED,
    }
)

# Legal forward transitions.
_LEGAL_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING: frozenset({OrderStatus.DISPATCHING, OrderStatus.FAILED}),
    OrderStatus.DISPATCHING: frozenset({OrderStatus.SUBMITTED, OrderStatus.DISPATCH_UNCERTAIN} | _REPORT_TARGETS),
    OrderStatus.DISPATCH_UNCERTAIN: frozenset({OrderStatus.SUBMITTED} | _REPORT_TARGETS),
    OrderStatus.SUBMITTED: _REPORT_TARGETS,
    OrderStatus.ACCEPTED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.PARTIALLY_FILLED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
    OrderStatus.FAILED: frozenset(),
}

TERMINAL_STATUSES = frozenset(status for status, targets in _LEGAL_TRANSITIONS.items() if not targets)


@dataclass(frozen=True, slots=True)
class OrderTransition:
    """A status report at a point in time - what arrives as an ExecutionReport (or an internal failure)."""

    status: OrderStatus
    transact_time: datetime


def is_terminal(status: OrderStatus) -> bool:
    """Report whether `status` accepts no further transitions."""
    return status in TERMINAL_STATUSES


def can_transition(current: OrderTransition, target: OrderTransition) -> bool:
    """Report whether `target` is a legal, non-regressing successor to `current`."""
    if target.status not in _LEGAL_TRANSITIONS.get(current.status, frozenset()):
        return False
    return target.transact_time >= current.transact_time


def apply_transition(current: OrderTransition, target: OrderTransition) -> OrderTransition:
    """Return `target` if the transition is legal, else raise IllegalOrderTransitionError."""
    if not can_transition(current, target):
        raise IllegalOrderTransitionError(from_status=current.status, to_status=target.status)
    return target
