"""Shared helper for reading ctrader_fix ResponseMessage fields."""


def first(value: str | list[str] | None) -> str | None:
    """Take [0] if `value` is a list - cTrader may send a repeating-group field either way."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def as_list(value: str | list[str] | None) -> list[str]:
    """Normalize a repeating-group field to a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]
