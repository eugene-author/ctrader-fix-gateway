"""Credential redaction shared by logging and Settings.__repr__."""

import re

SENSITIVE_KEY_PATTERN = re.compile(
    r"(password|secret|api[_-]?key|token|dsn|url)$",
    re.IGNORECASE,
)

REDACTED = "***REDACTED***"


def redact(key: str, value: object) -> object:
    """Mask a value if its key looks like it carries a credential."""
    if SENSITIVE_KEY_PATTERN.search(key):
        return REDACTED
    return value
