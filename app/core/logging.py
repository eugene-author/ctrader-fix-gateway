"""Structured logging setup with secret redaction."""

import logging
from collections.abc import MutableMapping

import structlog

from app.core.security import redact


def _redact_processor(
    logger: object, method_name: str, event_dict: MutableMapping[str, object]
) -> MutableMapping[str, object]:
    for key in list(event_dict):
        if key == "event":
            continue
        event_dict[key] = redact(key, event_dict[key])
    return event_dict


def configure_logging(*, env: str, log_level: str = "INFO") -> None:
    """Configure structlog for the process. Called exactly once at startup."""
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        _redact_processor,
    ]
    renderer: structlog.types.Processor = (
        structlog.dev.ConsoleRenderer() if env == "dev" else structlog.processors.JSONRenderer()
    )

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, log_level.upper())),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
