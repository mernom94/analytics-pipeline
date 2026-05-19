from fastapi import APIRouter, Depends, Response, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import EventValidationError, MerchantNotFoundError
from app.db.engine import get_db
from app.db.redis import get_redis
from app.models.schemas import BatchEventIngest, BatchIngestResponse, EventIngest, EventIngestResponse
from app.services.ingestion_service import IngestionService

router = APIRouter(prefix="/events", tags=["ingestion"])


@router.post(
    "",
    response_model=EventIngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a single event",
    description=(
        "Write path: validate → insert raw_events → NOTIFY rollup worker. "
        "Latency is bounded by a single INSERT regardless of rollup complexity. "
        "Returns 202 Accepted for new events, 200 OK for duplicates (same idempotency_key)."
    ),
)
async def ingest_event(
    payload: EventIngest,
    response: Response,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
) -> EventIngestResponse:
    service = IngestionService(db, redis)
    try:
        result, is_duplicate = await service.ingest_event(payload)
    except MerchantNotFoundError as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except EventValidationError as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if is_duplicate:
        response.status_code = status.HTTP_200_OK

    return result


@router.post(
    "/batch",
    response_model=BatchIngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest up to 500 events in a single request",
)
async def ingest_batch(
    payload: BatchEventIngest,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
) -> BatchIngestResponse:
    service = IngestionService(db, redis)
    try:
        return await service.ingest_batch(payload.events)
    except MerchantNotFoundError as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=str(exc)) from exc
