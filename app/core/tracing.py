"""OpenTelemetry tracing setup, shared by all four processes."""

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def init_tracing(*, service_name: str, env: str, otlp_endpoint: str | None) -> None:
    """Set up the global TracerProvider for this process, if an OTLP endpoint is configured."""
    if not otlp_endpoint:
        return
    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: service_name, "deployment.environment": env}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint)))
    trace.set_tracer_provider(provider)
