#!/usr/bin/env python
"""
Standalone rollup worker entrypoint.

Production deployment
---------------------
Run as a separate Docker service alongside the API:

    docker-compose up worker

Or directly:

    WORKER_ID=rollup-worker-1 python worker_rollup.py

The worker maintains its own DB connection pool and event loop, isolated from
API request latency.  Scale to exactly one replica per consumer group to avoid
offset conflicts (or implement partition-based sharding for multi-replica).

Health monitoring
-----------------
The worker writes a heartbeat to Redis every batch cycle (key:
`worker:heartbeat:{WORKER_ID}`, TTL 60s).  The API /health endpoint reads
this key to report worker lag — a missing key signals the worker is down.

WORKER ISOLATION
────────────────
The worker MUST NOT run inside the API process.  Embedding it via
asyncio.create_task() couples two very different resource profiles (bursty
HTTP vs. steady-state DB batch) and breaks crash isolation — a worker OOM
kills the API.  The standalone entry point here is the only supported way
to run the worker.
"""
from __future__ import annotations

import asyncio
import sys

from app.core.logging import configure_logging, get_logger

logger = get_logger(__name__)


async def main() -> None:
    configure_logging()
    logger.info("worker_rollup_starting")

    from app.core.config import get_settings
    from app.db.engine import build_engine, build_session_factory
    from app.db.session_factory import configure_session_factory
    from app.db.redis import configure_redis
    from app.observability import init_tracing, shutdown_tracing
    from app.workers.rollup_worker import RollupWorker
    from redis.asyncio import ConnectionPool, Redis as AioRedis

    settings = get_settings()

    init_tracing(
        service_name=settings.app_name,
        otlp_endpoint=settings.otlp_endpoint,
    )

    engine = build_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        echo=settings.debug,
    )
    factory = build_session_factory(engine)
    configure_session_factory(factory)

    pool = ConnectionPool.from_url(
        settings.redis_url,
        max_connections=settings.redis_pool_size,
        decode_responses=True,
    )
    configure_redis(AioRedis(connection_pool=pool))

    worker = RollupWorker(session_factory=factory)
    try:
        await worker.start()
    finally:
        await engine.dispose()
        shutdown_tracing()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
