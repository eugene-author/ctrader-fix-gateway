"""Sentry initialization, shared by all four processes."""

import re

import sentry_sdk
from sentry_sdk.types import Event, Hint

from app.core.security import REDACTED, SENSITIVE_KEY_PATTERN

_URL_CREDENTIALS_PATTERN = re.compile(r"//[^/@]+@")


def init_sentry(*, dsn: str | None, env: str, traces_sample_rate: float, release: str | None = None) -> None:
    """Initialize the Sentry SDK for this process."""
    sentry_sdk.init(
        dsn=dsn,
        environment=env,
        release=release,
        traces_sample_rate=traces_sample_rate,
        before_send=_scrub_event,
    )


def _scrub_event(event: Event, hint: Hint) -> Event | None:
    """Redact credential-looking values before an event leaves the process."""
    _scrub_mapping(event.get("extra"))

    breadcrumbs = event.get("breadcrumbs")
    if isinstance(breadcrumbs, dict):
        for entry in breadcrumbs.get("values", []):
            if isinstance(entry, dict):
                _scrub_mapping(entry.get("data"))

    request = event.get("request")
    if isinstance(request, dict):
        _scrub_mapping(request.get("headers"))
        url = request.get("url")
        if isinstance(url, str):
            request["url"] = _URL_CREDENTIALS_PATTERN.sub("//***REDACTED***@", url)

    return event


def _scrub_mapping(mapping: object) -> None:
    if not isinstance(mapping, dict):
        return
    for key in list(mapping.keys()):
        if isinstance(key, str) and SENSITIVE_KEY_PATTERN.search(key):
            mapping[key] = REDACTED
