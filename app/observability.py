"""
app/observability.py
────────────────────
Centralised observability bootstrap: Prometheus metrics registry and
OpenTelemetry / OTLP tracing.

PROMETHEUS METRICS
──────────────────
All counters and histograms are module-level singletons so that any
importer (ingestion service, rollup worker, query service) can call them
directly without passing a registry around.  The /metrics endpoint in
app/api/routes/prometheus.py exposes these via the default
prometheus_client registry.

Metrics defined here:

  events_ingested_total       Counter   — raw events accepted by ingestion
                                          service (labels: event_type)
  duplicate_events_total      Counter   — idempotency-key collisions
                                          (labels: event_type)
  rollup_batches_total        Counter   — worker batch cycles completed
  rollup_lag_seconds          Histogram — seconds between last event
                                          timestamp and now at batch end
                                          (labels: worker_id)

OTLP TRACING
────────────
Tracing is optional — if the opentelemetry packages are not installed or
OTLP_ENDPOINT is empty, init_tracing() is a no-op and a null tracer is
returned.  This lets the app run locally without an OTEL collector.

Usage from any module:

    from app.observability import (
        EVENTS_INGESTED, DUPLICATE_EVENTS,
        ROLLUP_BATCHES, ROLLUP_LAG,
        get_tracer,
    )

    EVENTS_INGESTED.labels(event_type="PAYMENT_CONFIRMED").inc()

    with get_tracer().start_as_current_span("ingest_event") as span:
        span.set_attribute("merchant_id", str(merchant_id))
        ...
"""
from __future__ import annotations

import logging

from prometheus_client import Counter, Histogram

logger = logging.getLogger(__name__)

# ── Prometheus metrics ────────────────────────────────────────────────────────

EVENTS_INGESTED: Counter = Counter(
    "events_ingested_total",
    "Total raw events accepted by the ingestion service",
    ["event_type"],
)

DUPLICATE_EVENTS: Counter = Counter(
    "duplicate_events_total",
    "Total events rejected as duplicates (idempotency-key collision)",
    ["event_type"],
)

ROLLUP_BATCHES: Counter = Counter(
    "rollup_batches_total",
    "Total rollup worker batch cycles completed",
    ["worker_id"],
)

ROLLUP_LAG: Histogram = Histogram(
    "rollup_lag_seconds",
    "Seconds between the last processed event timestamp and wall clock at batch end",
    ["worker_id"],
    buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, float("inf")],
)

# ── OpenTelemetry tracing ─────────────────────────────────────────────────────

_tracer = None
_tracer_provider = None


def init_tracing(service_name: str, otlp_endpoint: str | None = None) -> None:
    """
    Initialise the global OpenTelemetry tracer provider.

    If opentelemetry is not installed or otlp_endpoint is empty/None,
    this is a no-op and get_tracer() returns a no-op tracer.
    """
    global _tracer, _tracer_provider

    if not otlp_endpoint:
        logger.info("otlp_endpoint_not_configured_tracing_disabled")
        return

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        resource = Resource.create({SERVICE_NAME: service_name})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        _tracer_provider = provider
        _tracer = trace.get_tracer(service_name)
        logger.info("otlp_tracing_initialised", endpoint=otlp_endpoint)

    except ImportError:
        logger.warning(
            "opentelemetry_not_installed_tracing_disabled — "
            "pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc"
        )
    except Exception as exc:
        logger.warning("otlp_tracing_init_failed", error=str(exc))


def shutdown_tracing() -> None:
    """Flush pending spans and shut down the tracer provider gracefully."""
    global _tracer_provider
    if _tracer_provider is not None:
        try:
            _tracer_provider.shutdown()
        except Exception as exc:
            logger.warning("otlp_tracing_shutdown_error", error=str(exc))
        _tracer_provider = None


def get_tracer():  # noqa: ANN201
    """
    Return the active tracer, or a no-op tracer if tracing is disabled.

    Always safe to call — callers need not guard against None.
    """
    global _tracer
    if _tracer is not None:
        return _tracer
    # Return a no-op tracer so callers are unconditional
    try:
        from opentelemetry import trace
        return trace.get_tracer("noop")
    except ImportError:
        return _NoopTracer()


# ── No-op tracer fallback (when opentelemetry is not installed) ───────────────

class _NoopSpan:
    def set_attribute(self, key: str, value: object) -> None:  # noqa: ANN002
        pass

    def record_exception(self, exc: Exception) -> None:
        pass

    def set_status(self, status: object) -> None:  # noqa: ANN002
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):  # noqa: ANN002
        pass


class _NoopTracer:
    def start_as_current_span(self, name: str, **kwargs):  # noqa: ANN002, ANN003
        return _NoopSpan()

    def start_span(self, name: str, **kwargs):  # noqa: ANN002, ANN003
        return _NoopSpan()
