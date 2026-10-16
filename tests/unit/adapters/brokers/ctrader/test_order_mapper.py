from decimal import Decimal

import pytest

from app.adapters.brokers.ctrader import order_mapper
from app.domain.enums import OrderType, Side


@pytest.mark.parametrize(("side", "fix_value"), [(Side.BUY, "1"), (Side.SELL, "2")])
def test_side_to_fix(side: Side, fix_value: str) -> None:
    assert order_mapper.side_to_fix(side) == fix_value


@pytest.mark.parametrize(("fix_value", "side"), [("1", Side.BUY), ("2", Side.SELL)])
def test_side_from_fix(fix_value: str, side: Side) -> None:
    assert order_mapper.side_from_fix(fix_value) == side


@pytest.mark.parametrize(
    ("order_type", "fix_value"),
    [
        (OrderType.MARKET, "1"),
        (OrderType.LIMIT, "2"),
        (OrderType.STOP, "3"),
        (OrderType.STOP_LIMIT, "4"),
    ],
)
def test_order_type_to_fix(order_type: OrderType, fix_value: str) -> None:
    assert order_mapper.order_type_to_fix(order_type) == fix_value


@pytest.mark.parametrize(
    ("fix_value", "order_type"),
    [
        ("1", OrderType.MARKET),
        ("2", OrderType.LIMIT),
        ("3", OrderType.STOP),
        ("4", OrderType.STOP_LIMIT),
    ],
)
def test_order_type_from_fix(fix_value: str, order_type: OrderType) -> None:
    assert order_mapper.order_type_from_fix(fix_value) == order_type


def test_format_quantity_renders_plain_decimal_digits() -> None:
    assert order_mapper.format_quantity(Decimal(1000)) == "1000"
    assert order_mapper.format_quantity(Decimal("1000.50")) == "1000.50"


def test_format_quantity_never_uses_scientific_notation() -> None:
    assert "E" not in order_mapper.format_quantity(Decimal("1E+7")).upper()


def test_format_price_renders_plain_decimal_digits() -> None:
    assert order_mapper.format_price(Decimal("1.08243")) == "1.08243"
