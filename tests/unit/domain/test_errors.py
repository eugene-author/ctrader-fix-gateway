import pytest

from app.domain.enums import OrderStatus
from app.domain.errors import (
    AppError,
    ConflictError,
    DuplicateKeyError,
    IdempotencyKeyReusedError,
    IllegalOrderTransitionError,
    NotFoundError,
    OrderNotFoundError,
    PermanentError,
    SymbolsNotReadyError,
    TransientError,
    UnauthorizedError,
    ValidationError,
)


@pytest.mark.parametrize(
    ("error_cls", "expected_code", "expected_status"),
    [
        (ValidationError, "VALIDATION_ERROR", 422),
        (TransientError, "TRANSIENT_ERROR", 503),
        (PermanentError, "PERMANENT_ERROR", 500),
        (NotFoundError, "NOT_FOUND", 404),
        (ConflictError, "CONFLICT", 409),
        (UnauthorizedError, "UNAUTHORIZED", 401),
    ],
)
def test_error_families_have_stable_codes_and_statuses(
    error_cls: type[AppError], expected_code: str, expected_status: int
) -> None:
    assert error_cls.code == expected_code
    assert error_cls.status_code == expected_status
    assert issubclass(error_cls, AppError)


def test_code_and_status_can_be_overridden_per_instance() -> None:
    error = ValidationError("quantity must be a multiple of 100", code="QUANTITY_STEP_INVALID")

    assert error.code == "QUANTITY_STEP_INVALID"
    assert error.status_code == 422  # family default, untouched
    assert ValidationError.code == "VALIDATION_ERROR"  # class attribute unaffected


def test_illegal_order_transition_error_is_permanent_and_carries_statuses() -> None:
    error = IllegalOrderTransitionError(from_status=OrderStatus.FILLED, to_status=OrderStatus.ACCEPTED)

    assert isinstance(error, PermanentError)
    assert error.code == "ILLEGAL_ORDER_TRANSITION"
    assert error.from_status == OrderStatus.FILLED
    assert error.to_status == OrderStatus.ACCEPTED
    assert "FILLED" in str(error)
    assert "ACCEPTED" in str(error)


def test_order_not_found_error_is_404() -> None:
    order_id = "0192f3c2-0000-7000-8000-000000000000"

    error = OrderNotFoundError(order_id)

    assert isinstance(error, NotFoundError)
    assert error.code == "ORDER_NOT_FOUND"
    assert error.status_code == 404
    assert order_id in str(error)


def test_idempotency_key_reused_error_is_409() -> None:
    error = IdempotencyKeyReusedError("bot7-0001")

    assert isinstance(error, ConflictError)
    assert error.code == "IDEMPOTENCY_KEY_REUSED"
    assert error.status_code == 409
    assert "bot7-0001" in str(error)


def test_symbols_not_ready_error_is_503() -> None:
    error = SymbolsNotReadyError()

    assert isinstance(error, TransientError)
    assert error.code == "SYMBOLS_NOT_READY"
    assert error.status_code == 503


def test_duplicate_key_error_is_409() -> None:
    error = DuplicateKeyError("duplicate key value violates unique constraint")

    assert isinstance(error, ConflictError)
    assert error.code == "DUPLICATE_KEY"
    assert error.status_code == 409
