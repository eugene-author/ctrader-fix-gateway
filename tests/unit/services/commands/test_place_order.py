from decimal import Decimal
from uuid import uuid4

import pytest

from app.domain.enums import OrderStatus, OrderType, Side, TimeInForce
from app.domain.errors import IdempotencyKeyReusedError, PermanentError, SymbolsNotReadyError, ValidationError
from app.services.commands.place_order import PlaceOrderCommand, PlaceOrderHandler

from ..conftest import MockUnitOfWork, build_uow_factory, dedupe_key_error


def _command(**overrides: object) -> PlaceOrderCommand:
    defaults: dict[str, object] = {
        "account_id": uuid4(),
        "client_order_id": "bot7-0001",
        "symbol": "EURUSD",
        "side": Side.BUY,
        "order_type": OrderType.MARKET,
        "quantity": Decimal(1000),
        "request_fingerprint": "fp-1",
    }
    defaults.update(overrides)
    return PlaceOrderCommand(**defaults)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_place_order_persists_pending_order_and_outbox_event_in_one_transaction(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    result = await handler.handle(_command(account_id=account_id))

    assert result.status == OrderStatus.PENDING
    assert result.idempotency_replayed is False
    assert uow.committed is True
    stored_order = uow.orders_store[result.order_id]
    assert stored_order.client_order_id == "bot7-0001"
    assert stored_order.symbol_id == symbol.id
    assert len(uow.outbox_events) == 1
    event = uow.outbox_events[0]
    assert event["event_type"] == "CreateOrderCommand"
    assert event["routing_key"] == "order.create"
    assert event["payload"]["quantity"] == "1000"
    assert event["payload"]["broker_symbol_id"] == 1


@pytest.mark.asyncio
async def test_duplicate_client_order_id_with_same_fingerprint_replays(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))
    command = _command(account_id=account_id)

    first = await handler.handle(command)
    second = await handler.handle(command)

    assert first.idempotency_replayed is False
    assert second.idempotency_replayed is True
    assert second.order_id == first.order_id
    assert len(uow.outbox_events) == 1  # no second event for the replay


@pytest.mark.asyncio
async def test_duplicate_client_order_id_with_different_fingerprint_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    await handler.handle(_command(account_id=account_id, request_fingerprint="fp-1"))

    with pytest.raises(IdempotencyKeyReusedError) as exc_info:
        await handler.handle(_command(account_id=account_id, request_fingerprint="fp-2"))
    assert exc_info.value.code == "IDEMPOTENCY_KEY_REUSED"
    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_duplicate_client_order_id_with_a_different_idempotency_key_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    await handler.handle(_command(account_id=account_id, idempotency_key="key-1"))

    # Same client_order_id and body fingerprint, but the client sent a
    # different Idempotency-Key - that disagreement must not be silently
    # replayed, even though the body itself is identical.
    with pytest.raises(IdempotencyKeyReusedError) as exc_info:
        await handler.handle(_command(account_id=account_id, idempotency_key="key-2"))
    assert exc_info.value.code == "IDEMPOTENCY_KEY_REUSED"


@pytest.mark.asyncio
async def test_duplicate_client_order_id_with_the_same_idempotency_key_replays(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))
    command = _command(account_id=account_id, idempotency_key="key-1")

    first = await handler.handle(command)
    second = await handler.handle(command)

    assert second.idempotency_replayed is True
    assert second.order_id == first.order_id


@pytest.mark.asyncio
async def test_commit_race_translates_to_idempotency_key_reused(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol}, fail_on_commit_with=dedupe_key_error())
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(IdempotencyKeyReusedError) as exc_info:
        await handler.handle(_command(account_id=account_id))
    assert exc_info.value.code == "IDEMPOTENCY_KEY_REUSED"


@pytest.mark.asyncio
async def test_unknown_symbol_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    # At least one symbol already known for this account, so this is a
    # genuinely unknown name - not the cold-start case, which is
    # covered separately below.
    known = make_symbol(account_id=account_id, name="EURUSD")
    uow = MockUnitOfWork(symbols={(account_id, known.name): known})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, symbol="DOESNOTEXIST"))
    assert exc_info.value.code == "SYMBOL_UNKNOWN"


@pytest.mark.asyncio
async def test_cold_start_with_no_known_symbols_raises_symbols_not_ready() -> None:
    account_id = uuid4()
    uow = MockUnitOfWork(symbols={})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(SymbolsNotReadyError):
        await handler.handle(_command(account_id=account_id, symbol="EURUSD"))


@pytest.mark.asyncio
async def test_disabled_symbol_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, enabled=False)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id))
    assert exc_info.value.code == "SYMBOL_DISABLED"


@pytest.mark.asyncio
async def test_symbol_needing_review_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, needs_review=True)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id))
    assert exc_info.value.code == "SYMBOL_UNCONFIGURED"


@pytest.mark.asyncio
async def test_unsubscribed_symbol_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, subscribe=False)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id))
    assert exc_info.value.code == "SYMBOL_DISABLED"


@pytest.mark.asyncio
async def test_symbol_without_broker_id_reports_symbols_not_ready(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, broker_symbol_id=None)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(SymbolsNotReadyError) as exc_info:
        await handler.handle(_command(account_id=account_id))
    assert exc_info.value.code == "SYMBOLS_NOT_READY"
    assert exc_info.value.status_code == 503


@pytest.mark.parametrize("order_type", [OrderType.STOP, OrderType.STOP_LIMIT])
@pytest.mark.asyncio
async def test_stop_order_types_return_501_unsupported(make_symbol, order_type: OrderType) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(PermanentError) as exc_info:
        await handler.handle(_command(account_id=account_id, order_type=order_type, stop_price=Decimal("1.08000")))
    assert exc_info.value.code == "ORDER_TYPE_UNSUPPORTED"
    assert exc_info.value.status_code == 501


@pytest.mark.asyncio
async def test_negative_quantity_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, quantity=Decimal(-1000)))
    assert exc_info.value.code == "QUANTITY_OUT_OF_RANGE"


@pytest.mark.asyncio
async def test_quantity_below_minimum_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, min_qty=Decimal(1000))
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, quantity=Decimal(500)))
    assert exc_info.value.code == "QUANTITY_OUT_OF_RANGE"


@pytest.mark.asyncio
async def test_quantity_above_maximum_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, max_qty=Decimal(10000))
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, quantity=Decimal(20000)))
    assert exc_info.value.code == "QUANTITY_OUT_OF_RANGE"


@pytest.mark.asyncio
async def test_quantity_not_a_multiple_of_step_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, qty_step=Decimal(1000), min_qty=Decimal(100))
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, quantity=Decimal(1050)))
    assert exc_info.value.code == "QUANTITY_STEP_INVALID"


@pytest.mark.asyncio
async def test_market_order_with_limit_price_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, limit_price=Decimal("1.08000")))
    assert exc_info.value.code == "PRICE_NOT_ALLOWED"


@pytest.mark.asyncio
async def test_limit_order_without_limit_price_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, order_type=OrderType.LIMIT))
    assert exc_info.value.code == "PRICE_REQUIRED"


@pytest.mark.asyncio
async def test_limit_order_with_stop_price_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(
            _command(
                account_id=account_id,
                order_type=OrderType.LIMIT,
                limit_price=Decimal("1.08000"),
                stop_price=Decimal("1.07000"),
            )
        )
    assert exc_info.value.code == "PRICE_NOT_ALLOWED"


@pytest.mark.asyncio
async def test_volume_lots_is_converted_to_quantity_using_lot_size(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, lot_size=Decimal(100_000), qty_step=Decimal(1), min_qty=Decimal(1))
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    result = await handler.handle(_command(account_id=account_id, quantity=None, volume_lots=Decimal("0.1")))

    stored = uow.orders_store[result.order_id]
    assert stored.quantity.value == Decimal(10_000)


@pytest.mark.asyncio
async def test_supplying_both_quantity_and_volume_lots_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError):
        await handler.handle(_command(account_id=account_id, quantity=Decimal(1000), volume_lots=Decimal("0.1")))


@pytest.mark.asyncio
async def test_supplying_neither_quantity_nor_volume_lots_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError):
        await handler.handle(_command(account_id=account_id, quantity=None))


@pytest.mark.asyncio
async def test_volume_lots_without_configured_lot_size_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, lot_size=None)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, quantity=None, volume_lots=Decimal("0.1")))
    assert exc_info.value.code == "SYMBOL_UNCONFIGURED"


@pytest.mark.asyncio
async def test_limit_price_not_a_multiple_of_price_step_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, price_step=Decimal("0.00001"))
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(
            _command(account_id=account_id, order_type=OrderType.LIMIT, limit_price=Decimal("1.080003"))
        )
    assert exc_info.value.code == "PRICE_STEP_INVALID"


@pytest.mark.asyncio
async def test_stop_loss_not_a_multiple_of_price_step_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id, price_step=Decimal("0.00001"))
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, stop_loss=Decimal("1.080003")))
    assert exc_info.value.code == "PRICE_STEP_INVALID"


@pytest.mark.asyncio
async def test_invalid_client_order_id_format_is_rejected(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    with pytest.raises(ValidationError) as exc_info:
        await handler.handle(_command(account_id=account_id, client_order_id="has a space"))
    assert exc_info.value.code == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_valid_limit_order_succeeds(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    result = await handler.handle(
        _command(
            account_id=account_id,
            order_type=OrderType.LIMIT,
            limit_price=Decimal("1.08500"),
            time_in_force=TimeInForce.GTC,
        )
    )

    assert result.status == OrderStatus.PENDING
    stored = uow.orders_store[result.order_id]
    assert stored.limit_price is not None
    assert stored.limit_price.value == Decimal("1.08500")


@pytest.mark.asyncio
async def test_fix_cl_ord_id_is_derived_and_within_the_thirty_char_cap(make_symbol) -> None:
    account_id = uuid4()
    symbol = make_symbol(account_id=account_id)
    uow = MockUnitOfWork(symbols={(account_id, "EURUSD"): symbol})
    handler = PlaceOrderHandler(build_uow_factory(uow))

    result = await handler.handle(_command(account_id=account_id))

    event = uow.outbox_events[0]
    fix_cl_ord_id = event["payload"]["fix_cl_ord_id"]
    assert isinstance(fix_cl_ord_id, str)
    assert len(fix_cl_ord_id) <= 30
    assert event["payload"]["order_id"] == str(result.order_id)
