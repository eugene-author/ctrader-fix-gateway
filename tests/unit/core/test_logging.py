import json

import structlog

from app.core.logging import configure_logging


def test_configure_logging_json_includes_correlation_id(capsys) -> None:
    configure_logging(env="prod", log_level="INFO")
    logger = structlog.get_logger()

    structlog.contextvars.bind_contextvars(correlation_id="abc-123")
    try:
        logger.info("order_submitted", order_id="1")
    finally:
        structlog.contextvars.clear_contextvars()

    out = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(out)
    assert payload["correlation_id"] == "abc-123"
    assert payload["order_id"] == "1"
    assert payload["event"] == "order_submitted"


def test_configure_logging_redacts_sensitive_fields(capsys) -> None:
    configure_logging(env="prod", log_level="INFO")
    logger = structlog.get_logger()

    logger.info("settings_loaded", database_url="postgresql://u:secret@host/db", app_env="dev")

    out = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(out)
    assert payload["database_url"] == "***REDACTED***"
    assert payload["app_env"] == "dev"


def test_configure_logging_dev_uses_console_renderer(capsys) -> None:
    configure_logging(env="dev", log_level="INFO")
    logger = structlog.get_logger()

    logger.info("hello")

    out = capsys.readouterr().out
    # Console renderer output is not valid JSON; confirm it did NOT emit JSON.
    assert not out.strip().startswith("{")
