from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from app.queue.tracing import extract_context, inject_current_context


def _configure_recording_tracer() -> tuple[InMemorySpanExporter, trace.Tracer]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter, provider.get_tracer(__name__)


def test_inject_current_context_is_empty_with_no_active_span() -> None:
    # No start_as_current_span() is active here, so the W3C propagator has
    # no valid span context to serialize into a traceparent header.
    assert inject_current_context() == {}


def test_extract_context_with_no_headers_returns_the_current_context() -> None:
    assert extract_context(None) == otel_context.get_current()
    assert extract_context({}) == otel_context.get_current()


def test_a_span_started_under_an_extracted_context_is_a_child_of_the_original() -> None:
    exporter, tracer = _configure_recording_tracer()

    with tracer.start_as_current_span("producer"):
        headers = inject_current_context()
    assert "traceparent" in headers

    extracted = extract_context(headers)
    token = otel_context.attach(extracted)
    try:
        with tracer.start_as_current_span("consumer"):
            pass
    finally:
        otel_context.detach(token)

    spans = {span.name: span for span in exporter.get_finished_spans()}
    assert spans["producer"].context.trace_id == spans["consumer"].context.trace_id
    consumer_parent = spans["consumer"].parent
    assert consumer_parent is not None
    assert consumer_parent.span_id == spans["producer"].context.span_id


def test_extract_context_ignores_non_trace_headers() -> None:
    # aio_pika's x-delivery-count header (or anything else riding along) must
    # not confuse the W3C propagator, which only looks at its own keys.
    ctx = extract_context({"x-delivery-count": 3, "traceparent": "not-a-real-traceparent"})
    assert ctx is not None
