"""Standalone /metrics HTTP server for the non-API processes."""

import structlog
from prometheus_client import start_http_server

logger = structlog.get_logger()


def start_metrics_server(port: int) -> None:
    """Start the background metrics server once. Safe to call at most once per process."""
    start_http_server(port)
    logger.info("metrics_server_started", port=port)
