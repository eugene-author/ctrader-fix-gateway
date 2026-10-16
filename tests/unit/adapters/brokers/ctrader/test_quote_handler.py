from decimal import Decimal

from app.adapters.brokers.ctrader.quote_handler import map_market_data


class _MockMessage:
    def __init__(self, fields: dict[int, str | list[str]]) -> None:
        self._fields = fields

    def getFieldValue(self, tag: int) -> str | list[str] | None:
        return self._fields.get(tag)


def test_map_market_data_parses_a_single_bid_entry() -> None:
    message = _MockMessage({55: "1", 269: "0", 270: "1.08231"})

    update = map_market_data(message)

    assert update.broker_symbol_id == 1
    assert len(update.entries) == 1
    assert update.entries[0].entry_type == "0"
    assert update.entries[0].price == Decimal("1.08231")


def test_map_market_data_zips_the_repeating_group_pairwise() -> None:
    message = _MockMessage({55: "1", 269: ["0", "1"], 270: ["1.08231", "1.08245"]})

    update = map_market_data(message)

    assert [(e.entry_type, e.price) for e in update.entries] == [
        ("0", Decimal("1.08231")),
        ("1", Decimal("1.08245")),
    ]


def test_map_market_data_defaults_to_no_entries() -> None:
    message = _MockMessage({55: "1"})

    update = map_market_data(message)

    assert update.entries == ()
