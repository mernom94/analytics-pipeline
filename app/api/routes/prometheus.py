"""
app/api/routes/prometheus.py
─────────────────────────────
Exposes the Prometheus text-format /metrics endpoint.

Uses prometheus_client's generate_latest() to serialise all registered
metrics (including the default process/GC collectors) into the standard
Prometheus exposition format.

Why a dedicated route rather than a Starlette middleware?
─────────────────────────────────────────────────────────
Starlette's built-in PrometheusMiddleware (from starlette-prometheus) uses
BaseHTTPMiddleware, which we have explicitly banned.  A plain APIRouter GET
handler is pure-ASGI and has no background-task lifecycle issues.
"""
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter(tags=["observability"])


@router.get(
    "/metrics",
    include_in_schema=False,
    summary="Prometheus metrics scrape endpoint",
)
async def prometheus_metrics() -> Response:
    """
    Prometheus-format metrics scrape endpoint.

    Scraped by Prometheus at the interval configured in prometheus.yml.
    The response content-type header tells Prometheus which parser to use.
    """
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
