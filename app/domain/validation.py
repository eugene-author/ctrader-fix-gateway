"""Order-validation rules."""

from decimal import Decimal


def is_multiple_of_step(value: Decimal, step: Decimal) -> bool:
    """Report whether `value` is an exact multiple of `step`."""
    if step <= 0:
        return False
    return value % step == 0
