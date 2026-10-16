from decimal import Decimal
from pathlib import Path

import pytest

from app.core.symbol_overlay import load_symbol_overlay


def test_load_symbol_overlay_returns_empty_mapping_when_path_is_unset() -> None:
    assert load_symbol_overlay(None) == {}


def test_load_symbol_overlay_parses_decimals_not_floats(tmp_path: Path) -> None:
    overlay_file = tmp_path / "symbols.yaml"
    overlay_file.write_text(
        "EURUSD:\n"
        "  digits: 5\n"
        "  lot_size: 100000\n"
        "  qty_step: 1000\n"
        "  min_qty: 1000\n"
        "  max_qty: 10000000\n"
        "  price_step: 0.00001\n"
    )

    overlay = load_symbol_overlay(str(overlay_file))

    entry = overlay["EURUSD"]
    assert entry.digits == 5
    assert entry.lot_size is not None and entry.lot_size.value == Decimal("100000")
    assert isinstance(entry.lot_size.value, Decimal)
    assert entry.price_step is not None and entry.price_step.value == Decimal("0.00001")
    assert isinstance(entry.price_step.value, Decimal)


def test_load_symbol_overlay_rejects_an_entry_with_missing_fields(tmp_path: Path) -> None:
    overlay_file = tmp_path / "symbols.yaml"
    overlay_file.write_text("GBPUSD:\n  digits: 5\n")

    with pytest.raises(ValueError, match="GBPUSD"):
        load_symbol_overlay(str(overlay_file))


def test_load_symbol_overlay_rejects_an_unknown_field(tmp_path: Path) -> None:
    overlay_file = tmp_path / "symbols.yaml"
    overlay_file.write_text(
        "EURUSD:\n"
        "  digits: 5\n"
        "  lot_size: 100000\n"
        "  qty_step: 1000\n"
        "  min_qtty: 1000\n"
        "  max_qty: 10000000\n"
        "  price_step: 0.00001\n"
    )

    with pytest.raises(ValueError, match="min_qtty"):
        load_symbol_overlay(str(overlay_file))


def test_load_symbol_overlay_raises_for_a_configured_but_missing_path(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.yaml"

    with pytest.raises(FileNotFoundError):
        load_symbol_overlay(str(missing))
