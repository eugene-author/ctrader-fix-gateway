from app.core.security import redact


def test_redact_masks_sensitive_keys() -> None:
    assert redact("database_url", "postgresql://u:p@host/db") == "***REDACTED***"
    assert redact("api_key", "abc123") == "***REDACTED***"
    assert redact("password", "hunter2") == "***REDACTED***"


def test_redact_passes_through_non_sensitive_keys() -> None:
    assert redact("app_env", "dev") == "dev"
    assert redact("symbol", "EURUSD") == "EURUSD"
