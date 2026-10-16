from decimal import Decimal

import pytest

from app.adapters.brokers.ctrader.message_factory import CTraderMessageFactory
from app.domain.enums import OrderType, Side

_CFG = {
    "BeginString": "FIX.4.4",
    "SenderCompID": "demo.ctrader.123",
    "TargetCompID": "cServer",
    "TargetSubID": "TRADE",
    "SenderSubID": "TRADE",
}


def _rendered(message: object) -> str:
    message.delimiter = "|"  # type: ignore[attr-defined]
    return message.getMessage(1)  # type: ignore[attr-defined]


@pytest.fixture
def factory() -> CTraderMessageFactory:
    return CTraderMessageFactory(_CFG)


def test_new_order_renders_expected_tags(factory: CTraderMessageFactory) -> None:
    order = factory.new_order(
        cl_ord_id="abc123",
        broker_symbol_id=1,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal(1000),
    )

    rendered = _rendered(order)

    assert "35=D" in rendered
    assert "11=abc123" in rendered
    assert "55=1" in rendered
    assert "54=1" in rendered  # BUY
    assert "40=1" in rendered  # MARKET
    assert "38=1000" in rendered


def test_new_order_includes_price_for_limit_orders(factory: CTraderMessageFactory) -> None:
    order = factory.new_order(
        cl_ord_id="abc124",
        broker_symbol_id=1,
        side=Side.SELL,
        order_type=OrderType.LIMIT,
        quantity=Decimal(500),
        price=Decimal("1.08500"),
    )

    rendered = _rendered(order)

    assert "54=2" in rendered  # SELL
    assert "40=2" in rendered  # LIMIT
    assert "44=1.08500" in rendered


def test_new_order_includes_stop_px_tag_99_for_stop_orders(factory: CTraderMessageFactory) -> None:
    order = factory.new_order(
        cl_ord_id="abc125",
        broker_symbol_id=1,
        side=Side.SELL,
        order_type=OrderType.STOP,
        quantity=Decimal(1000),
        stop_price=Decimal("1.07000"),
    )

    rendered = _rendered(order)

    assert "40=3" in rendered  # STOP
    assert "99=1.07000" in rendered


def test_new_order_includes_pos_maint_rpt_id_for_a_close(factory: CTraderMessageFactory) -> None:
    order = factory.new_order(
        cl_ord_id="abc126",
        broker_symbol_id=1,
        side=Side.SELL,
        order_type=OrderType.MARKET,
        quantity=Decimal(1000),
        position_id="4471123",
    )

    rendered = _rendered(order)

    assert "721=4471123" in rendered


def test_new_order_rejects_a_client_order_id_over_thirty_chars(factory: CTraderMessageFactory) -> None:
    with pytest.raises(ValueError, match="30"):
        factory.new_order(
            cl_ord_id="x" * 31,
            broker_symbol_id=1,
            side=Side.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal(1000),
        )


def test_cancel_order_renders_expected_tags(factory: CTraderMessageFactory) -> None:
    cancel = factory.cancel_order(cl_ord_id="cancel-1", orig_cl_ord_id="orig-1", broker_symbol_id=1)

    rendered = _rendered(cancel)

    assert "35=F" in rendered
    assert "11=cancel-1" in rendered
    assert "41=orig-1" in rendered


def test_market_data_request_renders_one_message_for_the_symbol(factory: CTraderMessageFactory) -> None:
    request = factory.market_data_request(md_req_id="md-1", broker_symbol_id=1, entry_type="0")

    rendered = _rendered(request)

    assert "269=0" in rendered
    assert "MDReqID" not in rendered
    assert "262=md-1" in rendered
    assert "55=1" in rendered


def test_order_status_renders_cl_ord_id(factory: CTraderMessageFactory) -> None:
    request = factory.order_status(cl_ord_id="status-1")

    rendered = _rendered(request)

    assert "35=H" in rendered
    assert "11=status-1" in rendered


def test_security_list_renders_req_id_and_full_list_type(factory: CTraderMessageFactory) -> None:
    request = factory.security_list(req_id="sec-1")

    rendered = _rendered(request)

    assert "35=x" in rendered
    assert "320=sec-1" in rendered
    assert "559=0" in rendered


def test_logout_renders_the_logout_message_type(factory: CTraderMessageFactory) -> None:
    request = factory.logout()

    rendered = _rendered(request)

    assert "35=5" in rendered
