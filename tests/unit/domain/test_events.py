from datetime import UTC, datetime
from uuid import uuid4

from app.domain.events import ExecutionReportReceived, OrderFailed, OrderSubmitted, SessionStateChanged


def test_order_submitted_payload() -> None:
    order_id = uuid4()
    submitted_at = datetime(2026, 1, 1, tzinfo=UTC)

    event = OrderSubmitted(order_id=order_id, fix_cl_ord_id="abc123", submitted_at=submitted_at)

    assert event.to_payload() == {
        "order_id": str(order_id),
        "fix_cl_ord_id": "abc123",
        "submitted_at": submitted_at.isoformat(),
    }


def test_execution_report_received_payload_with_all_fields() -> None:
    order_id = uuid4()
    event = ExecutionReportReceived(
        order_id=order_id,
        fix_cl_ord_id="abc123",
        exec_id="exec-1",
        exec_type="F",
        ord_status="2",
        symbol="EURUSD",
        last_qty="1000",
        last_price="1.08243",
        cum_qty="1000",
        leaves_qty="0",
        position_id="4471123",
        text=None,
        transact_time="20260101-00:00:00.000",
        raw="8=FIX.4.4|...",
    )

    payload = event.to_payload()

    assert payload["order_id"] == str(order_id)
    assert payload["exec_type"] == "F"
    assert payload["position_id"] == "4471123"


def test_execution_report_received_payload_without_order_id() -> None:
    event = ExecutionReportReceived(
        order_id=None,
        fix_cl_ord_id="unknown",
        exec_id=None,
        exec_type="8",
        ord_status="8",
        symbol=None,
        cum_qty="0",
        leaves_qty="0",
        transact_time="20260101-00:00:00.000",
        raw="8=FIX.4.4|...",
    )

    assert event.to_payload()["order_id"] is None


def test_order_failed_payload_with_order_id() -> None:
    order_id = uuid4()

    event = OrderFailed(
        order_id=order_id, reason_code="SESSION_DOWN", message="TRADE session not logged on", retryable=True
    )

    assert event.to_payload() == {
        "order_id": str(order_id),
        "fix_cl_ord_id": None,
        "reason_code": "SESSION_DOWN",
        "message": "TRADE session not logged on",
        "retryable": True,
    }


def test_order_failed_payload_with_only_fix_cl_ord_id() -> None:
    event = OrderFailed(
        fix_cl_ord_id="abc123",
        reason_code="BUSINESS_REJECT",
        message="Unknown symbol",
        retryable=False,
    )

    payload = event.to_payload()
    assert payload["order_id"] is None
    assert payload["fix_cl_ord_id"] == "abc123"


def test_session_state_changed_payload() -> None:
    event = SessionStateChanged(session_type="TRADE", state="LOGGED_ON", reconnect_count=3)

    assert event.to_payload() == {"session_type": "TRADE", "state": "LOGGED_ON", "reconnect_count": 3}
