"""W3C trace-context propagation across the AMQP boundary."""

from collections.abc import Mapping

from opentelemetry import context as otel_context
from opentelemetry import propagate


def inject_current_context() -> dict[str, str]:
    """Capture the ambient trace context as a plain str->str dict."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


def extract_context(headers: Mapping[str, object] | None) -> otel_context.Context:
    """Rebuild a Context from AMQP message headers (an empty/absent carrier yields the current context)."""
    if not headers:
        return otel_context.get_current()
    # the W3C propagator expects strings.
    carrier = {str(k): str(v) for k, v in headers.items() if k in ("traceparent", "tracestate")}
    return propagate.extract(carrier)
