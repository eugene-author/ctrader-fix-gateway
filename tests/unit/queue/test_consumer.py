from dataclasses import dataclass, field

from app.queue.consumer import _redelivery_count


@dataclass
class _MockMessage:
    headers: dict[str, object] = field(default_factory=dict)


def test_redelivery_count_is_zero_when_header_absent() -> None:
    assert _redelivery_count(_MockMessage()) == 0


def test_redelivery_count_is_zero_when_headers_is_none() -> None:
    message = _MockMessage()
    message.headers = None  # type: ignore[assignment]
    assert _redelivery_count(message) == 0


def test_redelivery_count_reads_the_quorum_queue_header() -> None:
    assert _redelivery_count(_MockMessage(headers={"x-delivery-count": 3})) == 3
