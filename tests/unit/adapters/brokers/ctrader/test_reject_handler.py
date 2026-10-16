from app.adapters.brokers.ctrader.reject_handler import map_business_message_reject, map_order_cancel_reject


class _MockMessage:
    def __init__(self, fields: dict[int, str]) -> None:
        self._fields = fields

    def getFieldValue(self, tag: int) -> str | None:
        return self._fields.get(tag)


def test_map_business_message_reject_extracts_ref_id_and_text() -> None:
    message = _MockMessage({379: "abc123", 58: "Unknown symbol"})

    event = map_business_message_reject(message)

    assert event.fix_cl_ord_id == "abc123"
    assert event.message == "Unknown symbol"
    assert event.reason_code == "BUSINESS_REJECT"
    assert event.retryable is False
    assert event.order_id is None


def test_map_business_message_reject_falls_back_to_a_default_message() -> None:
    message = _MockMessage({379: "abc123"})

    event = map_business_message_reject(message)

    assert event.message == "Business message rejected"


def test_map_order_cancel_reject_extracts_cl_ord_id_and_text() -> None:
    message = _MockMessage({11: "cancel-1", 58: "Unknown order"})

    event = map_order_cancel_reject(message)

    assert event.fix_cl_ord_id == "cancel-1"
    assert event.message == "Unknown order"
    assert event.reason_code == "CANCEL_REJECT"
    assert event.retryable is False
