import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.models.schemas import MetricsResponse, SparklineResponse
from app.services.query_service import QueryService

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get(
    "/{merchant_id}",
    response_model=MetricsResponse,
    summary="Volume, count, and success rate over a time range",
    description=(
        "Read path always hits pre-aggregated rollup tables — never raw events. "
        "Granularity is auto-selected based on range: "
        "≤3h → minute, ≤30d → hour, >30d → day."
    ),
)
async def get_metrics(
    merchant_id: uuid.UUID,
    start: datetime = Query(
        default=None,
        description="Range start (ISO 8601). Defaults to 24h ago.",
    ),
    end: datetime = Query(
        default=None,
        description="Range end (ISO 8601). Defaults to now.",
    ),
    db: AsyncSession = Depends(get_db),
) -> MetricsResponse:
    now = datetime.now(timezone.utc)

    if end is None:
        end = now
    if start is None:
        start = now - timedelta(hours=24)

    if start >= end:
        raise HTTPException(status_code=400, detail="start must be before end")
    if (end - start).days > 365:
        raise HTTPException(status_code=400, detail="Range cannot exceed 365 days")

    # Ensure timezone-aware
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    service = QueryService(db)
    return await service.get_merchant_metrics(merchant_id, start, end)


@router.get(
    "/{merchant_id}/sparkline",
    response_model=SparklineResponse,
    summary="Per-minute transaction counts for the last N minutes (live sparkline)",
)
async def get_sparkline(
    merchant_id: uuid.UUID,
    window_minutes: int = Query(
        default=60, ge=5, le=1440, description="Window size in minutes"
    ),
    db: AsyncSession = Depends(get_db),
) -> SparklineResponse:
    service = QueryService(db)
    return await service.get_sparkline(merchant_id, window_minutes)
