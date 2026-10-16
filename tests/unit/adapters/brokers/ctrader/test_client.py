from ctrader_fix import ResponseMessage

from app.adapters.brokers.ctrader.client import _FramedFixProtocol

_DELIMITER = "\x01"


class _MockFactory:
    delimiter = _DELIMITER

    def __init__(self) -> None:
        self.received_messages: list[ResponseMessage] = []

    def received(self, message: ResponseMessage) -> None:
        self.received_messages.append(message)


def _build_message(fields: list[tuple[int, str]]) -> str:
    """Render `fields` as a delimiter-joined FIX message with a correct trailing checksum."""
    body = _DELIMITER.join(f"{tag}={value}" for tag, value in fields) + _DELIMITER
    checksum = sum(bytes(body, "ascii")) % 256
    return f"{body}10={str(checksum).zfill(3)}{_DELIMITER}"


def _make_protocol() -> tuple[_FramedFixProtocol, _MockFactory]:
    factory = _MockFactory()
    protocol = _FramedFixProtocol()
    protocol.factory = factory
    return protocol, factory


def test_data_received_splits_several_messages_landing_in_one_read() -> None:
    """Regression test: ctrader_fix's own FixProtocol would merge these into one ResponseMessage,
    whose getFieldValue(35) then returns a list instead of a single value.
    """
    msg1 = _build_message([(8, "FIX.4.4"), (35, "W"), (55, "1")])
    msg2 = _build_message([(8, "FIX.4.4"), (35, "j"), (55, "2")])
    msg3 = _build_message([(8, "FIX.4.4"), (35, "W"), (55, "3")])
    protocol, factory = _make_protocol()

    protocol.dataReceived((msg1 + msg2 + msg3).encode("ascii"))

    assert len(factory.received_messages) == 3
    assert [m.getFieldValue(35) for m in factory.received_messages] == ["W", "j", "W"]
    assert [m.getFieldValue(55) for m in factory.received_messages] == ["1", "2", "3"]


def test_data_received_buffers_a_message_split_across_two_reads() -> None:
    msg1 = _build_message([(8, "FIX.4.4"), (35, "W"), (55, "1")])
    msg2 = _build_message([(8, "FIX.4.4"), (35, "j"), (55, "2")])
    protocol, factory = _make_protocol()
    split_at = len(msg1) // 2

    protocol.dataReceived(msg1[:split_at].encode("ascii"))
    assert factory.received_messages == []

    protocol.dataReceived((msg1[split_at:] + msg2).encode("ascii"))

    assert [m.getFieldValue(35) for m in factory.received_messages] == ["W", "j"]
