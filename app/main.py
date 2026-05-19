"""
FastAPI application entrypoint.

CHANGES FROM ORIGINAL
─────────────────────
1. Redis wired via configure_redis() — no more auto-initialising singleton.
2. Embedded worker task REMOVED entirely.  The worker runs only as
   worker_rollup.py (standalone process).  settings.worker_enabled is
   ignored and will be removed from Settings in a future cleanup.
3. Prometheus metrics endpoint exposed at /metrics (via prometheus_client).
4. OTLP tracing initialised at startup via opentelemetry-sdk +
   opentelemetry-exporter-otlp-proto-grpc.
5. Pure-ASGI RequestContextMiddleware retained (no BaseHTTPMiddleware).
"""
from __future__ import annotations

import time
import uuid
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from app.api.routes import events, health, leaderboard, metrics
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.redis import close_redis, configure_redis, reset_redis
from app.observability import init_tracing, shutdown_tracing

settings = get_settings()
logger = get_logger(__name__)


# ── Pure ASGI middleware — no BaseHTTPMiddleware, no background task leak ─────

class RequestContextMiddleware:
    """
    Injects request ID + timing into the structlog context.

    Uses the raw ASGI interface instead of BaseHTTPMiddleware.
    BaseHTTPMiddleware spawns a background streaming task per request that
    outlives the response in test environments, causing:
        RuntimeError: Task <...BaseHTTPMiddleware.__call__...coro> was destroyed
    A pure ASGI middleware has no such task — it awaits the inner app directly.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = str(uuid.uuid4())
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            method=scope.get("method", ""),
            path=scope.get("path", ""),
        )

        t0 = time.monotonic()
        status_code: int = 0

        async def send_with_headers(message: dict) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                headers = list(message.get("headers", []))
                headers.append((b"x-request-id", request_id.encode()))
                message = {**message, "headers": headers}
            elif message["type"] == "http.response.body" and not message.get("more_body"):
                duration_ms = round((time.monotonic() - t0) * 1000, 2)
                logger.info("http_request", status_code=status_code, duration_ms=duration_ms)
            await send(message)

        await self.app(scope, receive, send_with_headers)


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN001
    # ── Startup ──────────────────────────────────────────────────────────────
    configure_logging()
    logger.info("app_starting", version=settings.app_version)

    # Tracing — initialise before any instrumented code runs
    init_tracing(
        service_name=settings.app_name,
        otlp_endpoint=settings.otlp_endpoint,
    )

    from app.db.engine import build_engine, build_session_factory
    from app.db.session_factory import configure_session_factory, get_session_factory, reset_session_factory

    # Only build an engine if tests haven't already injected one.
    _engine = None
    try:
        get_session_factory()
        logger.info("session_factory_already_configured_skipping_engine_build")
    except RuntimeError:
        _engine = build_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            echo=settings.debug,
        )
        configure_session_factory(build_session_factory(_engine))

    factory = get_session_factory()

    # Verify DB connectivity
    from sqlalchemy import text
    async with factory() as session:
        await session.execute(text("SELECT 1"))
    logger.info("database_connected")

    # Build and register the Redis client
    from redis.asyncio import ConnectionPool, Redis as AioRedis
    pool = ConnectionPool.from_url(
        settings.redis_url,
        max_connections=settings.redis_pool_size,
        decode_responses=True,
    )
    redis_client = AioRedis(connection_pool=pool)
    configure_redis(redis_client)

    await get_redis().ping()
    logger.info("redis_connected")

    if not settings.skip_migrations:
        await _run_migrations()

    await _seed_demo_data(factory)

    # ── NO embedded worker — worker runs as worker_rollup.py only ─────────────
    # Embedding the worker inside the API process couples two very different
    # resource profiles (bursty HTTP vs. steady-state DB batch) and prevents
    # independent scaling and crash isolation.  Any settings.worker_enabled
    # flag is intentionally ignored here.

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────
    logger.info("app_shutting_down")

    # Only dispose the engine if we created it — not if tests injected theirs.
    if _engine is not None:
        await _engine.dispose()
        reset_session_factory()
        logger.info("database_engine_disposed")

    await close_redis()
    reset_redis()

    shutdown_tracing()
    logger.info("app_stopped")


async def _run_migrations() -> None:
    import asyncio
    from alembic import command
    from alembic.config import Config

    def _run_sync() -> None:
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option(
            "sqlalchemy.url",
            settings.database_url.replace("+asyncpg", "+psycopg2"),
        )
        command.upgrade(alembic_cfg, "head")

    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _run_sync)
        logger.info("migrations_applied")
    except Exception as exc:
        logger.exception("migrations_failed", error=str(exc))
        raise RuntimeError(f"Alembic migration failed: {exc}") from exc


async def _seed_demo_data(factory) -> None:  # noqa: ANN001
    from sqlalchemy import text, select
    from app.models.orm import Merchant

    async with factory() as session:
        result = await session.execute(select(Merchant).limit(1))
        if result.scalars().first():
            return

        logger.info("seeding_demo_data")
        await session.execute(
            text("""
                INSERT INTO merchants (id, name) VALUES
                    ('a0000000-0000-0000-0000-000000000001', 'Acme Payments'),
                    ('a0000000-0000-0000-0000-000000000002', 'Stripe Demo'),
                    ('a0000000-0000-0000-0000-000000000003', 'PayCo Inc'),
                    ('a0000000-0000-0000-0000-000000000004', 'FastPay Ltd'),
                    ('a0000000-0000-0000-0000-000000000005', 'NovaPay'),
                    ('a0000000-0000-0000-0000-000000000006', 'QuickMerchant'),
                    ('a0000000-0000-0000-0000-000000000007', 'EuroTrade GmbH'),
                    ('a0000000-0000-0000-0000-000000000008', 'GlobalPay SA'),
                    ('a0000000-0000-0000-0000-000000000009', 'SwiftSettle'),
                    ('a0000000-0000-0000-0000-00000000000a', 'MegaMerchant')
                ON CONFLICT DO NOTHING
            """)
        )
        await session.commit()
        logger.info("demo_merchants_seeded")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description=(
            "High-throughput event ingestion with pre-aggregated rollups. "
            "Sub-millisecond query latency via rollup tables and Redis sorted sets."
        ),
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> ORJSONResponse:
        logger.exception("unhandled_exception", path=request.url.path, error=str(exc))
        return ORJSONResponse(status_code=500, content={"detail": "Internal server error"})

    app.include_router(events.router)
    app.include_router(metrics.router)
    app.include_router(leaderboard.router)
    app.include_router(health.router)

    # Prometheus /metrics endpoint
    from app.api.routes.prometheus import router as prom_router
    app.include_router(prom_router)

    @app.get("/", include_in_schema=False)
    async def root():  # noqa: ANN201
        return {"service": settings.app_name, "version": settings.app_version, "docs": "/docs"}

    return app


app = create_app()

# Re-export get_redis so existing import sites work without change
from app.db.redis import get_redis  # noqa: E402 — must come after module-level setup
