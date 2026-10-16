from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest
from hypothesis import given
from hypothesis import strategies as st

from app.domain.enums import OrderStatus
from app.domain.errors import IllegalOrderTransitionError
from app.domain.state_machine import (
    TERMINAL_STATUSES,
    OrderTransition,
    apply_transition,
    can_transition,
    is_terminal,
)

_T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _t(status: OrderStatus, offset_seconds: float = 0) -> OrderTransition:
    return OrderTransition(status=status, transact_time=_T0 + timedelta(seconds=offset_seconds))


@pytest.mark.parametrize(
    ("current_status", "target_status"),
    [
        (OrderStatus.PENDING, OrderStatus.DISPATCHING),
        (OrderStatus.PENDING, OrderStatus.FAILED),
        (OrderStatus.DISPATCHING, OrderStatus.SUBMITTED),
        (OrderStatus.DISPATCHING, OrderStatus.DISPATCH_UNCERTAIN),
        (OrderStatus.DISPATCHING, OrderStatus.ACCEPTED),  # a broker report can race ahead of the local send-outcome
        (OrderStatus.DISPATCHING, OrderStatus.FAILED),
        (OrderStatus.DISPATCH_UNCERTAIN, OrderStatus.SUBMITTED),  # reconciliation confirms it was received
        (OrderStatus.DISPATCH_UNCERTAIN, OrderStatus.FAILED),  # reconciliation confirms the broker never got it
        (OrderStatus.DISPATCH_UNCERTAIN, OrderStatus.ACCEPTED),
        (OrderStatus.SUBMITTED, OrderStatus.ACCEPTED),
        (OrderStatus.SUBMITTED, OrderStatus.REJECTED),
        (OrderStatus.SUBMITTED, OrderStatus.FAILED),
        (OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED),  # a fast market order can skip ACCEPTED
        (OrderStatus.SUBMITTED, OrderStatus.FILLED),
        (OrderStatus.SUBMITTED, OrderStatus.CANCELLED),
        (OrderStatus.SUBMITTED, OrderStatus.EXPIRED),
        (OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED),
        (OrderStatus.ACCEPTED, OrderStatus.FILLED),
        (OrderStatus.ACCEPTED, OrderStatus.CANCELLED),
        (OrderStatus.ACCEPTED, OrderStatus.REJECTED),
        (OrderStatus.ACCEPTED, OrderStatus.EXPIRED),
        (OrderStatus.PARTIALLY_FILLED, OrderStatus.PARTIALLY_FILLED),
        (OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED),
        (OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED),
        (OrderStatus.PARTIALLY_FILLED, OrderStatus.EXPIRED),
    ],
)
def test_legal_transitions_are_allowed(current_status: OrderStatus, target_status: OrderStatus) -> None:
    current = _t(current_status, 0)
    target = _t(target_status, 1)

    assert can_transition(current, target) is True
    assert apply_transition(current, target) == target


@pytest.mark.parametrize(
    ("current_status", "target_status"),
    [
        (OrderStatus.PENDING, OrderStatus.ACCEPTED),  # skips DISPATCHING
        (OrderStatus.PENDING, OrderStatus.FILLED),
        (OrderStatus.PENDING, OrderStatus.SUBMITTED),  # skips DISPATCHING
        (OrderStatus.DISPATCHING, OrderStatus.PENDING),  # regression
        (OrderStatus.DISPATCH_UNCERTAIN, OrderStatus.PENDING),  # regression
        (OrderStatus.DISPATCH_UNCERTAIN, OrderStatus.DISPATCHING),  # not a legal path back
        (OrderStatus.SUBMITTED, OrderStatus.PENDING),  # regression
        (OrderStatus.SUBMITTED, OrderStatus.DISPATCHING),  # regression
        (OrderStatus.SUBMITTED, OrderStatus.DISPATCH_UNCERTAIN),  # not reachable once actually sent
        (OrderStatus.ACCEPTED, OrderStatus.SUBMITTED),  # regression
        (OrderStatus.ACCEPTED, OrderStatus.PENDING),
        (OrderStatus.PARTIALLY_FILLED, OrderStatus.ACCEPTED),  # regression
        (OrderStatus.PARTIALLY_FILLED, OrderStatus.SUBMITTED),
        (OrderStatus.PARTIALLY_FILLED, OrderStatus.REJECTED),  # not a legal target from a partial fill
    ],
)
def test_illegal_transitions_are_rejected(current_status: OrderStatus, target_status: OrderStatus) -> None:
    current = _t(current_status, 0)
    target = _t(target_status, 1)

    assert can_transition(current, target) is False
    with pytest.raises(IllegalOrderTransitionError):
        apply_transition(current, target)


@pytest.mark.parametrize("terminal_status", sorted(TERMINAL_STATUSES, key=lambda s: s.value))
def test_terminal_statuses_accept_no_further_transitions(terminal_status: OrderStatus) -> None:
    current = _t(terminal_status, 0)

    for target_status in OrderStatus:
        target = _t(target_status, 1)
        assert can_transition(current, target) is False


def test_terminal_statuses_are_exactly_the_expected_set() -> None:
    expected = {
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
        OrderStatus.FAILED,
    }
    assert expected == TERMINAL_STATUSES
    for status in expected:
        assert is_terminal(status)
    for status in set(OrderStatus) - expected:
        assert not is_terminal(status)


def test_out_of_order_report_does_not_regress_status() -> None:
    # Order is already at a later partial fill (t=10s); a stale, redelivered
    # partial-fill report (t=5s) must not be treated as new progress.
    current = _t(OrderStatus.PARTIALLY_FILLED, 10)
    stale_target = _t(OrderStatus.PARTIALLY_FILLED, 5)

    assert can_transition(current, stale_target) is False
    with pytest.raises(IllegalOrderTransitionError):
        apply_transition(current, stale_target)


def test_same_timestamp_partial_fill_repeat_is_allowed() -> None:
    current = _t(OrderStatus.PARTIALLY_FILLED, 5)
    target = _t(OrderStatus.PARTIALLY_FILLED, 5)

    assert can_transition(current, target) is True


@given(st.data())
def test_applied_transitions_are_time_monotonic_and_terminal_is_absorbing(data: st.DataObject) -> None:
    """Property: feed a random walk of candidate reports (legal and illegal, in
    arbitrary order) through the state machine. Whatever gets applied must never
    move transact_time backwards.
    """
    current = _t(OrderStatus.PENDING, 0)
    applied = [current]

    for _ in range(15):
        status = data.draw(st.sampled_from(list(OrderStatus)))
        offset = data.draw(st.floats(min_value=-5, max_value=20, allow_nan=False, allow_infinity=False))
        candidate = _t(status, offset)

        if can_transition(current, candidate):
            assert not is_terminal(current.status), "a terminal status must never accept a further transition"
            current = apply_transition(current, candidate)
            applied.append(current)

    for earlier, later in pairwise(applied):
        assert later.transact_time >= earlier.transact_time
