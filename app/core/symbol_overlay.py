"""Loads the per-symbol trading-constraints overlay."""

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from app.domain.models import Price, Quantity


@dataclass(frozen=True, slots=True)
class SymbolOverlayEntry:
    """One symbol's trading constraints, as declared in the overlay file."""

    digits: int | None = None
    lot_size: Quantity | None = None
    qty_step: Quantity | None = None
    min_qty: Quantity | None = None
    max_qty: Quantity | None = None
    price_step: Price | None = None


_FIELDS = frozenset({"digits", "lot_size", "qty_step", "min_qty", "max_qty", "price_step"})


def load_symbol_overlay(path: str | None) -> dict[str, SymbolOverlayEntry]:
    """Load `path` (a YAML mapping of symbol name -> constraints), or {} if unset."""
    if not path:
        return {}
    raw = yaml.safe_load(Path(path).read_text()) or {}
    return {name: _parse_entry(name, fields) for name, fields in raw.items()}


def _quantity(fields: dict[str, Any], key: str) -> Quantity | None:
    value = fields.get(key)
    return Quantity(Decimal(str(value))) if value is not None else None


def _parse_entry(name: str, fields: dict[str, Any] | None) -> SymbolOverlayEntry:
    fields = fields or {}
    missing, unknown = sorted(_FIELDS - fields.keys()), sorted(fields.keys() - _FIELDS)
    if missing or unknown:
        raise ValueError(f"Symbol overlay entry {name!r}: missing {missing}, unknown {unknown}")
    price_step = fields.get("price_step")
    digits = fields.get("digits")
    return SymbolOverlayEntry(
        digits=int(digits) if digits is not None else None,
        lot_size=_quantity(fields, "lot_size"),
        qty_step=_quantity(fields, "qty_step"),
        min_qty=_quantity(fields, "min_qty"),
        max_qty=_quantity(fields, "max_qty"),
        price_step=Price(Decimal(str(price_step))) if price_step is not None else None,
    )
