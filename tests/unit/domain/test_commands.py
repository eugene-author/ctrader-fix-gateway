from decimal import Decimal
from uuid import uuid4

from app.domain.commands import (
    CancelOrderCommand,
    ClosePositionCommand,
    CreateOrderCommand,
    PlaceProtectiveOrdersCommand,
    ProtectiveOrder,
)
from app.domain.enums import OrderType, Side, TimeInForce
from app.domain.models import Price, Quantity


def test_create_order_command_payload_stringifies_numerics_and_omits_none_prices() -> None:
    order_id = uuid4()
    command = CreateOrderCommand(
        order_id=order_id,
        client_order_id="bot7-0001",
        fix_cl_ord_id="bot7-0001",
        symbol="EURUSD",
        broker_symbol_id=1,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal(1000)),
        time_in_force=TimeInForce.GTC,
    )

    payload = command.to_payload()

    assert payload["order_id"] == str(order_id)
    assert payload["quantity"] == "1000"
    assert payload["limit_price"] is None
    assert payload["stop_price"] is None
    assert payload["side"] == "BUY"
    assert payload["order_type"] == "MARKET"
    assert payload["time_in_force"] == "GTC"


def test_create_order_command_payload_includes_limit_price_as_a_string() -> None:
    command = CreateOrderCommand(
        order_id=uuid4(),
        client_order_id="bot7-0002",
        fix_cl_ord_id="bot7-0002",
        symbol="EURUSD",
        broker_symbol_id=1,
        side=Side.SELL,
        order_type=OrderType.LIMIT,
        quantity=Quantity(Decimal(500)),
        time_in_force=TimeInForce.GTC,
        limit_price=Price(Decimal("1.08500")),
    )

    payload = command.to_payload()

    assert payload["limit_price"] == "1.08500"


def test_create_order_command_round_trips_through_payload_with_prices() -> None:
    original = CreateOrderCommand(
        order_id=uuid4(),
        client_order_id="bot7-0003",
        fix_cl_ord_id="bot7fix0003",
        symbol="EURUSD",
        broker_symbol_id=1,
        side=Side.SELL,
        order_type=OrderType.LIMIT,
        quantity=Quantity(Decimal(500)),
        time_in_force=TimeInForce.GTC,
        limit_price=Price(Decimal("1.08500")),
        stop_price=Price(Decimal("1.07000")),
    )

    restored = CreateOrderCommand.from_payload(original.to_payload())

    assert restored == original


def test_create_order_command_round_trips_through_payload_without_prices() -> None:
    original = CreateOrderCommand(
        order_id=uuid4(),
        client_order_id="bot7-0004",
        fix_cl_ord_id="bot7fix0004",
        symbol="EURUSD",
        broker_symbol_id=1,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal(1000)),
        time_in_force=TimeInForce.GTC,
    )

    restored = CreateOrderCommand.from_payload(original.to_payload())

    assert restored == original


def test_cancel_order_command_round_trips_through_payload() -> None:
    original = CancelOrderCommand(
        order_id=uuid4(),
        fix_cl_ord_id="new-cancel-id",
        orig_cl_ord_id="bot7fix0001",
        symbol="EURUSD",
        broker_symbol_id=1,
    )

    restored = CancelOrderCommand.from_payload(original.to_payload())

    assert restored == original


def test_close_position_command_round_trips_through_payload() -> None:
    original = ClosePositionCommand(
        order_id=uuid4(),
        position_id="4471123",
        fix_cl_ord_id="new-close-id",
        symbol="EURUSD",
        broker_symbol_id=1,
        side=Side.SELL,
        quantity=Quantity(Decimal(1000)),
    )

    restored = ClosePositionCommand.from_payload(original.to_payload())

    assert restored == original


def test_place_protective_orders_command_round_trips_with_the_full_bracket() -> None:
    original = PlaceProtectiveOrdersCommand(
        position_id="4471123",
        symbol="EURUSD",
        broker_symbol_id=1,
        close_side=Side.SELL,
        quantity=Quantity(Decimal(1000)),
        oco_group_id=uuid4(),
        stop_loss=ProtectiveOrder(order_id=uuid4(), fix_cl_ord_id="sl:4471123", price=Price(Decimal("1.08000"))),
        take_profit=ProtectiveOrder(order_id=uuid4(), fix_cl_ord_id="tp:4471123", price=Price(Decimal("1.08600"))),
    )

    restored = PlaceProtectiveOrdersCommand.from_payload(original.to_payload())

    assert restored == original


def test_place_protective_orders_command_round_trips_with_only_stop_loss() -> None:
    original = PlaceProtectiveOrdersCommand(
        position_id="4471123",
        symbol="EURUSD",
        broker_symbol_id=1,
        close_side=Side.SELL,
        quantity=Quantity(Decimal(1000)),
        oco_group_id=uuid4(),
        stop_loss=ProtectiveOrder(order_id=uuid4(), fix_cl_ord_id="sl:4471123", price=Price(Decimal("1.08000"))),
    )

    restored = PlaceProtectiveOrdersCommand.from_payload(original.to_payload())

    assert restored == original
    assert restored.take_profit is None
