from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from app.domain.enums import OrderPurpose, OrderStatus, OrderType, Side
from app.domain.events import ExecutionReportReceived
from app.domain.models import Order, Price, Quantity
from app.workers.projector import _parse_fix_time, _resolve_avg_fill_price


def test_parse_fix_time_accepts_fractional_seconds() -> None:
    assert _parse_fix_time("20260115-10:30:00.123") == datetime(2026, 1, 15, 10, 30, 0, 123000, tzinfo=UTC)


def test_parse_fix_time_accepts_whole_seconds_without_fractions() -> None:
    assert _parse_fix_time("20260115-10:30:00") == datetime(2026, 1, 15, 10, 30, 0, tzinfo=UTC)


def test_parse_fix_time_falls_back_to_now_for_an_empty_value() -> None:
    before = datetime.now(UTC)
    result = _parse_fix_time("")
    after = datetime.now(UTC)
    assert before <= result <= after


def test_parse_fix_time_falls_back_to_now_for_an_unparseable_value() -> None:
    before = datetime.now(UTC)
    result = _parse_fix_time("not-a-timestamp")
    after = datetime.now(UTC)
    assert before <= result <= after


def _order(**overrides: object) -> Order:
    now = datetime.now(UTC)
    defaults: dict[str, object] = {
        "id": uuid4(),
        "account_id": uuid4(),
        "client_order_id": "bot7-0001",
        "symbol_id": uuid4(),
        "side": Side.BUY,
        "order_type": OrderType.MARKET,
        "quantity": Quantity(Decimal(1000)),
        "status": OrderStatus.SUBMITTED,
        "purpose": OrderPurpose.OPEN,
        "created_at": now,
        "updated_at": now,
    }
    defaults.update(overrides)
    return Order(**defaults)  # type: ignore[arg-type]


def _event(**overrides: object) -> ExecutionReportReceived:
    defaults: dict[str, object] = {
        "fix_cl_ord_id": "abc123",
        "exec_id": "exec-1",
        "exec_type": "1",
        "ord_status": "1",
        "symbol": "EURUSD",
        "cum_qty": "500",
        "leaves_qty": "500",
        "transact_time": "20260101-00:00:00.000",
        "raw": "",
    }
    defaults.update(overrides)
    return ExecutionReportReceived(**defaults)  # type: ignore[arg-type]


def test_resolve_avg_fill_price_prefers_broker_avg_px_over_last_px() -> None:
    order = _order()
    event = _event(last_price="1.08300", last_qty="500", avg_price="1.08200")

    result = _resolve_avg_fill_price(order, event)

    assert result is not None
    assert result.value == Decimal("1.08200")


def test_resolve_avg_fill_price_falls_back_to_last_px_for_a_fresh_orders_first_fill() -> None:
    order = _order()  # filled_quantity defaults to 0, no prior avg_fill_price
    event = _event(last_price="1.08300", last_qty="500")

    result = _resolve_avg_fill_price(order, event)

    assert result is not None
    assert result.value == Decimal("1.08300")


def test_resolve_avg_fill_price_computes_a_weighted_average_across_partial_fills_when_avg_px_is_absent() -> None:
    order = _order(filled_quantity=Quantity(Decimal(500)), avg_fill_price=Price(Decimal("1.08000")))
    event = _event(last_price="1.09000", last_qty="500")

    result = _resolve_avg_fill_price(order, event)

    # (1.08000*500 + 1.09000*500) / 1000 = 1.08500
    assert result is not None
    assert result.value == Decimal("1.08500")


def test_resolve_avg_fill_price_keeps_the_existing_average_when_the_report_has_no_price_at_all() -> None:
    order = _order(filled_quantity=Quantity(Decimal(500)), avg_fill_price=Price(Decimal("1.08000")))
    event = _event()  # no last_price, no avg_price

    result = _resolve_avg_fill_price(order, event)

    assert result is not None
    assert result.value == Decimal("1.08000")
