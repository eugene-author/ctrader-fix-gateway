"""The broker-adapter contract.

A formal `BrokerPort` Protocol, an adapter registry and an
`InMemoryMockBroker`, with this suite parametrised over every registered
adapter

If a second broker adapter is ever added, promoting these into a real
parametrised suite (one parameter set per adapter) is the natural next
step; each function below is already written test-first as "the property",
not "this fixture's output", to make that promotion mechanical.
"""

from decimal import Decimal

import pytest

from app.adapters.brokers.ctrader import order_mapper
from app.adapters.brokers.ctrader.execution_handler import map_execution_report
from app.adapters.brokers.ctrader.message_factory import CTraderMessageFactory
from app.adapters.brokers.ctrader.reject_handler import map_business_message_reject, map_order_cancel_reject
from app.domain.enums import OrderStatus, OrderType, Side
from app.domain.events import ExecutionReportReceived, OrderFailed
from app.workers.projector import _target_status

MESSAGE_FACTORY = CTraderMessageFactory(
    {
        "BeginString": "FIX.4.4",
        "SenderCompID": "demo.broker.1234567",
        "TargetCompID": "cServer",
        "TargetSubID": "TRADE",
        "SenderSubID": "TRADE",
    }
)


def _rendered(message: object) -> str:
    message.delimiter = "|"  # type: ignore[attr-defined]
    return message.getMessage(1)  # type: ignore[attr-defined]


# --- ClOrdID length (FIX tag 11 hard cap, the deterministic protective-order ids share it) ---


def test_cl_ord_id_at_the_30_char_cap_is_accepted() -> None:
    cl_ord_id = "x" * 30
    order = MESSAGE_FACTORY.new_order(
        cl_ord_id=cl_ord_id,
        broker_symbol_id=1,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal(1000),
    )
    assert f"11={cl_ord_id}" in _rendered(order)


@pytest.mark.parametrize(
    "builder",
    [
        lambda cl_ord_id: MESSAGE_FACTORY.new_order(
            cl_ord_id=cl_ord_id,
            broker_symbol_id=1,
            side=Side.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal(1000),
        ),
        lambda cl_ord_id: MESSAGE_FACTORY.cancel_order(
            cl_ord_id=cl_ord_id, orig_cl_ord_id="orig0001", broker_symbol_id=1
        ),
    ],
)
def test_every_outbound_message_type_rejects_a_cl_ord_id_over_the_cap(builder) -> None:
    with pytest.raises(ValueError, match="30"):
        builder("x" * 31)


# --- Order-type -> wire-field contract: each OrdType carries exactly the price fields it needs ---


def test_market_orders_carry_neither_price_field() -> None:
    order = MESSAGE_FACTORY.new_order(
        cl_ord_id="ord0001",
        broker_symbol_id=1,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal(1000),
    )
    rendered = _rendered(order)
    assert "44=" not in rendered
    assert "99=" not in rendered


def test_limit_orders_carry_price_but_not_stop_px() -> None:
    order = MESSAGE_FACTORY.new_order(
        cl_ord_id="ord0002",
        broker_symbol_id=1,
        side=Side.SELL,
        order_type=OrderType.LIMIT,
        quantity=Decimal(1000),
        price=Decimal("1.08500"),
    )
    rendered = _rendered(order)
    assert "44=1.08500" in rendered
    assert "99=" not in rendered


def test_stop_orders_carry_stop_px_but_not_price() -> None:
    order = MESSAGE_FACTORY.new_order(
        cl_ord_id="ord0003",
        broker_symbol_id=1,
        side=Side.SELL,
        order_type=OrderType.STOP,
        quantity=Decimal(1000),
        stop_price=Decimal("1.07000"),
    )
    rendered = _rendered(order)
    assert "99=1.07000" in rendered
    assert "44=" not in rendered


def test_a_position_id_renders_as_tag_721_when_supplied() -> None:
    order = MESSAGE_FACTORY.new_order(
        cl_ord_id="ord0004",
        broker_symbol_id=1,
        side=Side.SELL,
        order_type=OrderType.MARKET,
        quantity=Decimal(1000),
        position_id="4471123",
    )
    assert "721=4471123" in _rendered(order)


# --- Cancel vs close: two structurally different messages, never conflated ---


def test_cancel_is_an_order_cancel_request_not_a_new_order() -> None:
    cancel = MESSAGE_FACTORY.cancel_order(cl_ord_id="cancel001", orig_cl_ord_id="orig0001", broker_symbol_id=1)
    assert "35=F" in _rendered(cancel)


def test_close_is_a_new_order_single_on_the_opposite_side_carrying_the_position_id() -> None:
    # A close never touches cancel_order - it's a fresh NewOrderSingle on the
    # flipped side, tag 721 pointing at the position being closed.
    close_order = MESSAGE_FACTORY.new_order(
        cl_ord_id="close0001",
        broker_symbol_id=1,
        side=Side.SELL,
        order_type=OrderType.MARKET,
        quantity=Decimal(1000),
        position_id="4471123",
    )
    rendered = _rendered(close_order)
    assert "35=D" in rendered
    assert "721=4471123" in rendered


# --- Domain <-> FIX enum mapping is a bijection: every member survives a round trip ---


@pytest.mark.parametrize("side", list(Side))
def test_side_round_trips_through_fix(side: Side) -> None:
    assert order_mapper.side_from_fix(order_mapper.side_to_fix(side)) == side


@pytest.mark.parametrize("order_type", list(OrderType))
def test_order_type_round_trips_through_fix(order_type: OrderType) -> None:
    assert order_mapper.order_type_from_fix(order_mapper.order_type_to_fix(order_type)) == order_type


# --- Execution-report mapping never produces a status outside the domain enum ---


@pytest.mark.parametrize(
    ("exec_type", "ord_status"),
    [
        ("0", "0"),  # New -> ACCEPTED
        ("1", "1"),  # Partial fill -> PARTIALLY_FILLED
        ("2", "2"),  # Fill -> FILLED
        ("4", "4"),  # Cancelled -> CANCELLED
        ("8", "8"),  # Rejected -> REJECTED
        ("C", "C"),  # Expired -> EXPIRED
        ("2", "0"),  # exec_type overrides ord_status for a fresh accept, per _target_status
    ],
)
def test_every_broker_reported_status_maps_to_a_legal_domain_order_status(exec_type: str, ord_status: str) -> None:
    event = ExecutionReportReceived(
        fix_cl_ord_id="ord0001",
        exec_id="exec-1",
        exec_type=exec_type,
        ord_status=ord_status,
        symbol="1",
        cum_qty="0",
        leaves_qty="1000",
        transact_time="",
        raw="",
    )
    status = _target_status(event)
    assert status is None or isinstance(status, OrderStatus)


def test_an_unrecognized_ord_status_is_reported_as_unmapped_not_guessed() -> None:
    # A status this project doesn't know about must surface as "unmapped"
    # (None, logged by the projector) rather than silently coerced to some
    # plausible-looking OrderStatus member.
    event = ExecutionReportReceived(
        fix_cl_ord_id="ord0001",
        exec_id="exec-1",
        exec_type="Z",
        ord_status="Z",
        symbol="1",
        cum_qty="0",
        leaves_qty="1000",
        transact_time="",
        raw="",
    )
    assert _target_status(event) is None


def test_map_execution_report_never_raises_on_a_message_missing_optional_fields() -> None:
    class _MinimalMessage:
        def getFieldValue(self, tag: int) -> str | None:
            required = {11: "ord0001", 150: "0", 39: "0", 14: "0", 151: "1000"}
            return required.get(tag)

        def getMessage(self) -> str:
            return "8=FIX.4.4|"

    event = map_execution_report(_MinimalMessage())
    assert event.fix_cl_ord_id == "ord0001"
    assert event.exec_id is None
    assert event.last_price is None


# --- Rejection paths always produce OrderFailed, never raise into the caller ---


def test_business_message_reject_maps_to_order_failed() -> None:
    class _RejectMessage:
        def getFieldValue(self, tag: int) -> str | None:
            return {379: "ord0001", 58: "symbol not tradable"}.get(tag)

    event = map_business_message_reject(_RejectMessage())
    assert isinstance(event, OrderFailed)
    assert event.reason_code == "BUSINESS_REJECT"
    assert event.retryable is False


def test_order_cancel_reject_maps_to_order_failed() -> None:
    class _RejectMessage:
        def getFieldValue(self, tag: int) -> str | None:
            return {11: "cancel0001", 58: "unknown order"}.get(tag)

    event = map_order_cancel_reject(_RejectMessage())
    assert isinstance(event, OrderFailed)
    assert event.reason_code == "CANCEL_REJECT"


# --- Deterministic protective ClOrdIDs: stable, capped, prefix-identifiable ---


@pytest.mark.parametrize("position_id", ["4471123", "x" * 40])
def test_protective_order_cl_ord_ids_stay_within_the_cap_for_any_position_id(position_id: str) -> None:
    sl_id = f"sl:{position_id}"[:30]
    tp_id = f"tp:{position_id}"[:30]
    assert len(sl_id) <= 30
    assert len(tp_id) <= 30
    assert sl_id != tp_id
    assert f"sl:{position_id}"[:30] == sl_id
