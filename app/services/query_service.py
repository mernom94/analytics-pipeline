"""
Query service.

Routing rules:
    range ≤ 3h   → rollup_minute (TTL: 5s)
    range ≤ 30d  → rollup_hour   (TTL: 60s)
    range > 30d  → rollup_day    (TTL: 300s)

Cache-aside pattern:
    1. Check Redis cache
    2. On hit  → return immediately, record hit
    3. On miss → query rollup table, write to cache, return
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.redis import get_redis
from app.models.schemas import (
    BucketDataPoint,
    LeaderboardEntry,
    LeaderboardResponse,
    MetricsResponse,
    RollupGranularity,
    SparklineResponse,
)
from app.repositories.redis_repository import RedisRepository
from app.repositories.rollup_repository import RollupRepository

logger = get_logger(__name__)
settings = get_settings()

_HOUR = 3600
_DAY = 86400


def _select_granularity(start: datetime, end: datetime) -> tuple[RollupGranularity, int]:
    """Return (granularity, cache_ttl_seconds) based on requested range."""
    range_hours = (end - start).total_seconds() / _HOUR
    if range_hours <= 3:
        return RollupGranularity.MINUTE, settings.cache_ttl_live
    elif range_hours <= 720:  # 30 days
        return RollupGranularity.HOUR, settings.cache_ttl_historical
    else:
        return RollupGranularity.DAY, settings.cache_ttl_archive


def _period_key_for_datetime(dt: datetime) -> str:
    """Monthly period key, e.g. '2026-05'."""
    return dt.strftime("%Y-%m")


class QueryService:
    def __init__(self, session: AsyncSession) -> None:
        self._rollup_repo = RollupRepository(session)
        self._redis_repo = RedisRepository(get_redis())

    async def get_merchant_metrics(
        self,
        merchant_id: uuid.UUID,
        start: datetime,
        end: datetime,
    ) -> MetricsResponse:
        t0 = time.monotonic()
        granularity, ttl = _select_granularity(start, end)

        # ── Cache check ──────────────────────────────────────────────────────
        cached = await self._redis_repo.metric_cache_get(
            merchant_id, start, end, granularity.value
        )
        if cached:
            await self._redis_repo.record_cache_hit()
            logger.debug(
                "metrics_cache_hit",
                merchant_id=str(merchant_id),
                granularity=granularity.value,
            )
            return MetricsResponse(
                **cached,
                cache_hit=True,
                query_latency_ms=round((time.monotonic() - t0) * 1000, 2),
            )

        await self._redis_repo.record_cache_miss()

        # ── Rollup query ─────────────────────────────────────────────────────
        if granularity == RollupGranularity.MINUTE:
            rows = await self._rollup_repo.query_minute(merchant_id, start, end)
        elif granularity == RollupGranularity.HOUR:
            rows = await self._rollup_repo.query_hour(merchant_id, start, end)
        else:
            rows = await self._rollup_repo.query_day(merchant_id, start, end)

        success_rate = await self._rollup_repo.query_success_rate(merchant_id, start, end)

        data = [
            BucketDataPoint(
                bucket=r["bucket"],
                volume_cents=r["volume_cents"] or 0,
                count=r["count"] or 0,
            )
            for r in rows
        ]

        total_volume = sum(d.volume_cents for d in data)
        total_count = sum(d.count for d in data)
        latency_ms = round((time.monotonic() - t0) * 1000, 2)

        response = MetricsResponse(
            merchant_id=merchant_id,
            start=start,
            end=end,
            granularity=granularity,
            total_volume_cents=total_volume,
            total_count=total_count,
            success_rate=success_rate,
            data=data,
            cache_hit=False,
            query_latency_ms=latency_ms,
        )

        # ── Populate cache ───────────────────────────────────────────────────
        await self._redis_repo.metric_cache_set(
            merchant_id,
            start,
            end,
            granularity.value,
            response.model_dump(mode="json"),
            ttl,
        )

        logger.info(
            "metrics_query",
            merchant_id=str(merchant_id),
            granularity=granularity.value,
            data_points=len(data),
            total_volume_cents=total_volume,
            latency_ms=latency_ms,
            cache_hit=False,
        )

        return response

    async def get_sparkline(
        self, merchant_id: uuid.UUID, window_minutes: int = 60
    ) -> SparklineResponse:
        cached = await self._redis_repo.sparkline_cache_get(merchant_id, window_minutes)
        if cached:
            await self._redis_repo.record_cache_hit()
            return SparklineResponse(
                merchant_id=merchant_id,
                window_minutes=window_minutes,
                data=[BucketDataPoint(**d) for d in cached],
                cache_hit=True,
            )

        await self._redis_repo.record_cache_miss()
        rows = await self._rollup_repo.query_sparkline(merchant_id, window_minutes)
        data = [
            BucketDataPoint(
                bucket=r["bucket"],
                volume_cents=r["volume_cents"] or 0,
                count=r["count"] or 0,
            )
            for r in rows
        ]

        await self._redis_repo.sparkline_cache_set(
            merchant_id,
            window_minutes,
            [d.model_dump(mode="json") for d in data],
            ttl=settings.cache_ttl_live,
        )

        return SparklineResponse(
            merchant_id=merchant_id,
            window_minutes=window_minutes,
            data=data,
            cache_hit=False,
        )

    async def get_leaderboard(
        self,
        period: str | None = None,
        top_n: int | None = None,
    ) -> LeaderboardResponse:
        now = datetime.now(timezone.utc)
        period_key = period or _period_key_for_datetime(now)
        n = top_n or settings.leaderboard_top_n

        # Period bounds for this month
        year, month = (int(p) for p in period_key.split("-"))
        period_start = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            period_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            period_end = datetime(year, month + 1, 1, tzinfo=timezone.utc)

        source = "redis"

        # ── Redis fast path ───────────────────────────────────────────────────
        if await self._redis_repo.leaderboard_exists(period_key):
            raw = await self._redis_repo.leaderboard_top_n(period_key, n)
            entries = [
                LeaderboardEntry(
                    rank=i + 1,
                    merchant_id=uuid.UUID(mid),
                    volume_cents=int(score),
                )
                for i, (mid, score) in enumerate(raw)
            ]
            logger.debug("leaderboard_redis_hit", period=period_key)
        else:
            # ── DB rebuild ────────────────────────────────────────────────────
            logger.info("leaderboard_rebuild_from_db", period=period_key)
            await self._redis_repo.incr_leaderboard_rebuild_count()
            rows = await self._rollup_repo.query_leaderboard_from_db(
                period_start, period_end, top_n=n
            )
            if rows:
                await self._redis_repo.leaderboard_rebuild(period_key, rows)

            entries = [
                LeaderboardEntry(
                    rank=i + 1,
                    merchant_id=row["merchant_id"],
                    volume_cents=row["volume_cents"],
                    transaction_count=row.get("transaction_count"),
                )
                for i, row in enumerate(rows)
            ]
            source = "rollup_day"

        return LeaderboardResponse(
            period=period_key,
            period_start=period_start,
            period_end=period_end,
            entries=entries,
            source=source,
            generated_at=now,
        )

    async def get_leaderboard_history(
        self,
        period: str = "monthly",
        limit: int = 12,
    ) -> "LeaderboardHistoryResponse":
        """
        Return historical point-in-time leaderboard snapshots from the
        leaderboard_snapshots table (populated by nightly compaction).

        Grouping and ordering logic lives here, not in the route handler,
        so it is independently testable and reusable.
        """
        from collections import defaultdict
        from sqlalchemy import text

        rows_result = await self._session.execute(
            text("""
                SELECT
                    ls.period,
                    ls.period_start,
                    ls.merchant_id,
                    ls.rank,
                    ls.amount_sum_cents,
                    ls.created_at,
                    m.name AS merchant_name
                FROM leaderboard_snapshots ls
                LEFT JOIN merchants m ON m.id = ls.merchant_id
                WHERE ls.period = :period
                ORDER BY ls.period_start DESC, ls.rank ASC
                LIMIT :row_limit
            """),
            {"period": period, "row_limit": limit * 10},  # limit × max_rank per snapshot
        )
        rows = [dict(r._mapping) for r in rows_result]

        # Group by period_start, preserving DESC order
        grouped: dict = defaultdict(list)
        for row in rows:
            grouped[row["period_start"]].append(row)

        snapshots: list[LeaderboardResponse] = []
        for period_start, entries in sorted(grouped.items(), reverse=True)[:limit]:
            # Compute period_end from period_start (always deterministic)
            ps = period_start if hasattr(period_start, "year") else datetime.fromisoformat(str(period_start))
            if period == "monthly":
                if ps.month == 12:
                    period_end = ps.replace(year=ps.year + 1, month=1, day=1)
                else:
                    period_end = ps.replace(month=ps.month + 1, day=1)
            elif period == "daily":
                from datetime import timedelta
                period_end = ps + timedelta(days=1)
            else:
                from datetime import timedelta
                period_end = ps + timedelta(weeks=1)

            snapshots.append(
                LeaderboardResponse(
                    period=period,
                    period_start=ps.replace(tzinfo=timezone.utc) if ps.tzinfo is None else ps,
                    period_end=period_end.replace(tzinfo=timezone.utc) if period_end.tzinfo is None else period_end,
                    entries=[
                        LeaderboardEntry(
                            rank=e["rank"],
                            merchant_id=e["merchant_id"],
                            merchant_name=e.get("merchant_name"),
                            volume_cents=e["amount_sum_cents"],
                        )
                        for e in entries
                    ],
                    source="snapshot",
                    generated_at=entries[0]["created_at"] if entries else datetime.now(timezone.utc),
                )
            )

        from app.models.schemas import LeaderboardHistoryResponse, LeaderboardPeriod
        return LeaderboardHistoryResponse(
            period=LeaderboardPeriod(period),
            snapshots=snapshots,
        )
