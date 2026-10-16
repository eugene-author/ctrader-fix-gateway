from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from app.domain.enums import OrderPurpose, OrderStatus, OrderType, Side
from app.domain.models import Order, Price, Quantity


def test_quantity_rejects_float() -> None:
    with pytest.raises(TypeError):
        Quantity(1.5)  # type: ignore[arg-type]


def test_quantity_rejects_negative() -> None:
    with pytest.raises(ValueError, match="negative"):
        Quantity(Decimal("-1"))


def test_quantity_accepts_zero() -> None:
    assert Quantity(Decimal(0)).value == Decimal(0)


def test_price_rejects_float() -> None:
    with pytest.raises(TypeError):
        Price(1.5)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_value", [Decimal(0), Decimal(-1)])
def test_price_rejects_non_positive(bad_value: Decimal) -> None:
    with pytest.raises(ValueError, match="positive"):
        Price(bad_value)


def test_order_defaults_filled_quantity_to_zero_and_gtc_and_version_one() -> None:
    now = datetime.now(UTC)

    order = Order(
        id=uuid4(),
        account_id=uuid4(),
        client_order_id="bot7-0001",
        symbol_id=uuid4(),
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal(1000)),
        status=OrderStatus.PENDING,
        purpose=OrderPurpose.OPEN,
        created_at=now,
        updated_at=now,
    )

    assert order.filled_quantity == Quantity(Decimal(0))
    assert order.version == 1
    assert order.time_in_force.value == "GTC"
    assert order.position_id is None
