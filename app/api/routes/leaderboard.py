from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.models.schemas import LeaderboardHistoryResponse, LeaderboardResponse
from app.services.query_service import QueryService

router = APIRouter(prefix="/leaderboard", tags=["leaderboard"])


@router.get(
    "",
    response_model=LeaderboardResponse,
    summary="Top-N merchants by payment volume for a period",
    description=(
        "Served from Redis sorted set in O(log N). "
        "Falls back to rollup_day if Redis is cold or evicted. "
        "Period format: YYYY-MM (default: current month)."
    ),
)
async def get_leaderboard(
    period: str | None = Query(
        default=None,
        description="Period key: YYYY-MM. Defaults to current month.",
        pattern=r"^\d{4}-\d{2}$",
    ),
    top_n: int = Query(default=10, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> LeaderboardResponse:
    service = QueryService(db)
    return await service.get_leaderboard(period=period, top_n=top_n)


@router.get(
    "/history",
    response_model=LeaderboardHistoryResponse,
    summary="Historical leaderboard snapshots",
    description=(
        "Returns point-in-time leaderboard snapshots from the leaderboard_snapshots table. "
        "Populated by the nightly compaction job. "
        "period: daily | weekly | monthly. "
        "limit: number of snapshot periods to return."
    ),
)
async def get_leaderboard_history(
    period: str = Query(default="monthly", description="daily | weekly | monthly"),
    limit: int = Query(default=12, ge=1, le=52),
    db: AsyncSession = Depends(get_db),
) -> LeaderboardHistoryResponse:
    service = QueryService(db)
    return await service.get_leaderboard_history(period=period, limit=limit)
