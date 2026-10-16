from app.adapters.brokers.ctrader.symbol_handler import map_security_list


class _MockMessage:
    def __init__(self, fields: dict[int, str | list[str]]) -> None:
        self._fields = fields

    def getFieldValue(self, tag: int) -> str | list[str] | None:
        return self._fields.get(tag)


def test_map_security_list_parses_a_single_symbol() -> None:
    message = _MockMessage({55: "1", 48: "EURUSD", 52: "20260101-00:00:00.000"})

    event = map_security_list(message)

    assert len(event.symbols) == 1
    assert event.symbols[0].broker_symbol_id == 1
    assert event.symbols[0].name == "EURUSD"
    assert event.received_at == "20260101-00:00:00.000"


def test_map_security_list_zips_the_repeating_group_pairwise() -> None:
    message = _MockMessage({55: ["1", "2", "3"], 48: ["EURUSD", "GBPUSD", "USDJPY"], 52: "20260101-00:00:00.000"})

    event = map_security_list(message)

    assert [(s.broker_symbol_id, s.name) for s in event.symbols] == [
        (1, "EURUSD"),
        (2, "GBPUSD"),
        (3, "USDJPY"),
    ]


def test_map_security_list_reads_names_from_tag_1007_when_tag_48_is_absent() -> None:
    # A real cTrader demo account's SecurityList reply carries the name in
    # tag 1007 (SymbolName), not tag 48 - tag 48 is absent entirely.
    message = _MockMessage({55: ["1", "2", "3"], 1007: ["EURUSD", "GBPUSD", "EURJPY"], 52: "20260101-00:00:00.000"})

    event = map_security_list(message)

    assert [(s.broker_symbol_id, s.name) for s in event.symbols] == [
        (1, "EURUSD"),
        (2, "GBPUSD"),
        (3, "EURJPY"),
    ]


def test_map_security_list_prefers_tag_1007_over_tag_48_when_both_present() -> None:
    message = _MockMessage({55: "1", 48: "stale", 1007: "EURUSD", 52: "20260101-00:00:00.000"})

    event = map_security_list(message)

    assert event.symbols[0].name == "EURUSD"


def test_map_security_list_defaults_to_no_symbols_and_empty_received_at() -> None:
    message = _MockMessage({})

    event = map_security_list(message)

    assert event.symbols == ()
    assert event.received_at == ""
