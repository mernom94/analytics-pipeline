"""
Health endpoint.

Reports:
- Database connectivity + latency
- Redis connectivity + latency
- Rollup worker lag (seconds behind live events)
- Consumer offset
- Overall system status (ok | degraded | down)
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from app.core.config import get_settings
from app.db.session_factory import get_session_factory
from app.db.redis import get_redis
from app.models.schemas import ComponentHealth, HealthResponse
from app.repositories.redis_repository import RedisRepository
from app.repositories.rollup_repository import RollupRepository

router = APIRouter(prefix="/health", tags=["health"])
settings = get_settings()


@router.get("", response_model=HealthResponse, summary="System health and rollup lag")
async def health_check() -> HealthResponse:
    components: dict[str, ComponentHealth] = {}
    overall = "ok"

    # ── Database ─────────────────────────────────────────────────────────────
    db_latency_ms = None
    consumer_offset_info = None
    rollup_lag_seconds = None

    try:
        factory = get_session_factory()
        t0 = time.monotonic()
        async with factory() as session:
            await session.execute(text("SELECT 1"))
            db_latency_ms = round((time.monotonic() - t0) * 1000, 2)

            repo = RollupRepository(session)
            offset = await repo.get_consumer_offset(settings.worker_id)

            if offset and offset.last_event_at:
                now = datetime.now(timezone.utc)
                last = offset.last_event_at
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                rollup_lag_seconds = round((now - last).total_seconds(), 1)
                consumer_offset_info = {
                    "consumer_id": offset.consumer_id,
                    "last_event_id": str(offset.last_event_id) if offset.last_event_id else None,
                    "last_event_at": offset.last_event_at.isoformat() if offset.last_event_at else None,
                    "lag_seconds": rollup_lag_seconds,
                }

        components["database"] = ComponentHealth(
            status="ok",
            latency_ms=db_latency_ms,
        )
    except Exception as exc:
        components["database"] = ComponentHealth(status="down", detail=str(exc))
        overall = "down"

    # ── Redis ─────────────────────────────────────────────────────────────────
    try:
        redis_repo = RedisRepository(get_redis())
        redis_latency_ms = await redis_repo.ping()
        components["redis"] = ComponentHealth(status="ok", latency_ms=redis_latency_ms)
    except Exception as exc:
        components["redis"] = ComponentHealth(status="degraded", detail=str(exc))
        if overall == "ok":
            overall = "degraded"

    # ── Rollup worker ─────────────────────────────────────────────────────────
    if rollup_lag_seconds is not None:
        if rollup_lag_seconds > settings.worker_lag_threshold_s:
            worker_status = "degraded"
            if overall == "ok":
                overall = "degraded"
        else:
            worker_status = "ok"

        components["rollup_worker"] = ComponentHealth(
            status=worker_status,
            detail=f"lag={rollup_lag_seconds}s",
        )
    else:
        components["rollup_worker"] = ComponentHealth(
            status="unknown",
            detail="No offset recorded — worker may not have started",
        )

    return HealthResponse(
        status=overall,
        version=settings.app_version,
        components=components,
        rollup_lag_seconds=rollup_lag_seconds,
        consumer_offset=consumer_offset_info,
        timestamp=datetime.now(timezone.utc),
    )


@router.get("/ready", summary="Readiness probe (k8s)")
async def readiness() -> dict:
    """Lightweight readiness check for container orchestrators."""
    try:
        factory = get_session_factory()
        async with factory() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ready"}
    except Exception as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/live", summary="Liveness probe (k8s)")
async def liveness() -> dict:
    return {"status": "alive"}