"""Reconnect backoff policy."""

import random
from collections.abc import Callable

DEFAULT_BASE_DELAY_SECONDS = 1.0
DEFAULT_MAX_DELAY_SECONDS = 60.0
DEFAULT_STABLE_AFTER_SECONDS = 120.0


def next_delay(
    attempt: int,
    *,
    base: float = DEFAULT_BASE_DELAY_SECONDS,
    cap: float = DEFAULT_MAX_DELAY_SECONDS,
    uniform: Callable[[float, float], float] = random.uniform,
) -> float:
    """Return the next reconnect delay in seconds."""
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    ceiling = min(cap, base * (2**attempt))
    return uniform(0, ceiling)
