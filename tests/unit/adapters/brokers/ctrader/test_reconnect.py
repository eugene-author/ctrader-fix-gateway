import pytest

from app.adapters.brokers.ctrader.reconnect import next_delay


def test_next_delay_rejects_a_negative_attempt() -> None:
    with pytest.raises(ValueError, match="attempt"):
        next_delay(-1)


def test_next_delay_is_within_full_jitter_bounds() -> None:
    for attempt in range(10):
        delay = next_delay(attempt, base=1.0, cap=60.0, uniform=lambda lo, hi: hi)
        expected_ceiling = min(60.0, 1.0 * (2**attempt))
        assert delay == expected_ceiling


def test_next_delay_never_exceeds_the_cap() -> None:
    delay = next_delay(20, base=1.0, cap=60.0, uniform=lambda lo, hi: hi)
    assert delay == 60.0


def test_next_delay_passes_zero_to_ceiling_range_to_uniform() -> None:
    seen: list[tuple[float, float]] = []

    def _record_uniform(lo: float, hi: float) -> float:
        seen.append((lo, hi))
        return lo

    next_delay(3, base=1.0, cap=60.0, uniform=_record_uniform)

    assert seen == [(0, 8.0)]


def test_next_delay_zero_attempt_uses_base_as_ceiling() -> None:
    delay = next_delay(0, base=2.0, cap=60.0, uniform=lambda lo, hi: hi)
    assert delay == 2.0
