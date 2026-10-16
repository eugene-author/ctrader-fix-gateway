from decimal import Decimal

import pytest

from app.domain.validation import is_multiple_of_step


@pytest.mark.parametrize(
    ("value", "step"),
    [
        (Decimal(1000), Decimal(100)),
        (Decimal(0), Decimal(100)),
        (Decimal("1.5"), Decimal("0.5")),
        (Decimal(100), Decimal(100)),
    ],
)
def test_is_multiple_of_step_true_cases(value: Decimal, step: Decimal) -> None:
    assert is_multiple_of_step(value, step) is True


@pytest.mark.parametrize(
    ("value", "step"),
    [
        (Decimal(1050), Decimal(100)),
        (Decimal("1.51"), Decimal("0.5")),
    ],
)
def test_is_multiple_of_step_false_cases(value: Decimal, step: Decimal) -> None:
    assert is_multiple_of_step(value, step) is False


def test_is_multiple_of_step_rejects_a_non_positive_step() -> None:
    assert is_multiple_of_step(Decimal(100), Decimal(0)) is False
    assert is_multiple_of_step(Decimal(100), Decimal(-1)) is False
