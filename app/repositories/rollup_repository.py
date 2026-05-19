"""
Rollup repository.

All upserts use ON CONFLICT DO UPDATE with additive deltas — making them
safe to replay: re-processing an event N times produces the same result as
processing it once.

Two upsert paths:
  - _upsert_rollup: single-row path (used by compaction / backfill callers)
  - batch_upsert_*: multi-row path used by the rollup worker.
    Accepts a pre-aggregated delta map (Python dict) and emits a single
    executemany statement per granularity, not one statement per event.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models.orm import ConsumerOffset, RollupDay, RollupHour, RollupMinute

if TYPE_CHECKING:
    from app.workers.rollup_worker import _AggMap

logger = get_logger(__name__)


class RollupRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ─── Single-row upserts (backfill / compaction) ───────────────────────────

    async def upsert_minute(
        self,
        merchant_id: uuid.UUID,
        bucket: datetime,
        event_type: str,
        count_delta: int,
        amount_cents: int,
    ) -> None:
        await self._upsert_rollup(
            RollupMinute, merchant_id, bucket, event_type, count_delta, amount_cents
        )

    async def upsert_hour(
        self,
        merchant_id: uuid.UUID,
        bucket: datetime,
        event_type: str,
        count_delta: int,
        amount_cents: int,
    ) -> None:
        await self._upsert_rollup(
            RollupHour, merchant_id, bucket, event_type, count_delta, amount_cents
        )

    async def upsert_day(
        self,
        merchant_id: uuid.UUID,
        bucket: datetime,
        event_type: str,
        count_delta: int,
        amount_cents: int,
    ) -> None:
        await self._upsert_rollup(
            RollupDay, merchant_id, bucket, event_type, count_delta, amount_cents
        )

    async def _upsert_rollup(
        self,
        model: type[RollupMinute | RollupHour | RollupDay],
        merchant_id: uuid.UUID,
        bucket: datetime,
        event_type: str,
        count_delta: int,
        amount_cents: int,
    ) -> None:
        stmt = (
            insert(model)
            .values(
                merchant_id=merchant_id,
                bucket=bucket,
                event_type=event_type,
                count=count_delta,
                amount_sum_cents=amount_cents,
                amount_min_cents=amount_cents,
                amount_max_cents=amount_cents,
                updated_at=text("NOW()"),
            )
            .on_conflict_do_update(
                index_elements=["merchant_id", "bucket", "event_type"],
                set_={
                    "count": model.count + count_delta,
                    "amount_sum_cents": model.amount_sum_cents + amount_cents,
                    "amount_min_cents": text(
                        f"LEAST({model.__tablename__}.amount_min_cents, EXCLUDED.amount_min_cents)"
                    ),
                    "amount_max_cents": text(
                        f"GREATEST({model.__tablename__}.amount_max_cents, EXCLUDED.amount_max_cents)"
                    ),
                    "updated_at": text("NOW()"),
                },
            )
        )
        await self._session.execute(stmt)

    async def upsert_all_granularities(
        self,
        merchant_id: uuid.UUID,
        event_type: str,
        bucket_minute: datetime,
        bucket_hour: datetime,
        bucket_day: datetime,
        amount_cents: int,
    ) -> None:
        """
        Single-event rollup path (used by compaction scripts, not the live worker).
        The live worker uses batch_upsert_* instead.
        """
        await self.upsert_minute(merchant_id, bucket_minute, event_type, 1, amount_cents)
        await self.upsert_hour(merchant_id, bucket_hour, event_type, 1, amount_cents)
        await self.upsert_day(merchant_id, bucket_day, event_type, 1, amount_cents)

    # ─── Batch upserts (live worker path) ─────────────────────────────────────

    async def batch_upsert_minute(self, agg: "_AggMap") -> None:
        """
        Upsert pre-aggregated minute-level deltas.

        One executemany call for the entire batch rather than one execute() per
        event.  For a batch of 1000 events with 50 unique (merchant, bucket,
        type) combinations this is 1 query instead of 1000.
        """
        await self._batch_upsert(RollupMinute, agg)

    async def batch_upsert_hour(self, agg: "_AggMap") -> None:
        """Upsert pre-aggregated hour-level deltas."""
        await self._batch_upsert(RollupHour, agg)

    async def batch_upsert_day(self, agg: "_AggMap") -> None:
        """Upsert pre-aggregated day-level deltas."""
        await self._batch_upsert(RollupDay, agg)

    async def _batch_upsert(
        self,
        model: type[RollupMinute | RollupHour | RollupDay],
        agg: "_AggMap",
    ) -> None:
        """
        Core batch upsert implementation.

        Builds a single INSERT ... VALUES (...), (...) ... ON CONFLICT DO UPDATE
        statement covering all accumulated deltas for one granularity.

        Idempotency is preserved: the ON CONFLICT clause adds the incoming delta
        to the existing stored value, so replaying the same aggregated deltas
        is safe (same correctness guarantee as the single-row path).
        """
        if not agg:
            return

        values = [
            {
                "merchant_id": merchant_id,
                "bucket": bucket,
                "event_type": event_type,
                "count": accum.count,
                "amount_sum_cents": accum.amount_sum,
                "amount_min_cents": accum.amount_min,
                "amount_max_cents": accum.amount_max,
                "updated_at": text("NOW()"),
            }
            for (merchant_id, bucket, event_type), accum in agg.items()
        ]

        stmt = (
            insert(model)
            .values(values)
            .on_conflict_do_update(
                index_elements=["merchant_id", "bucket", "event_type"],
                set_={
                    "count": model.count + text("EXCLUDED.count"),
                    "amount_sum_cents": model.amount_sum_cents + text("EXCLUDED.amount_sum_cents"),
                    "amount_min_cents": text(
                        f"LEAST({model.__tablename__}.amount_min_cents, EXCLUDED.amount_min_cents)"
                    ),
                    "amount_max_cents": text(
                        f"GREATEST({model.__tablename__}.amount_max_cents, EXCLUDED.amount_max_cents)"
                    ),
                    "updated_at": text("NOW()"),
                },
            )
        )
        await self._session.execute(stmt)

    # ─── Queries ──────────────────────────────────────────────────────────────

    async def query_minute(
        self, merchant_id: uuid.UUID, start: datetime, end: datetime
    ) -> list[dict]:
        result = await self._session.execute(
            text("""
                SELECT
                    bucket,
                    SUM(count)            AS count,
                    SUM(amount_sum_cents) AS volume_cents
                FROM rollup_minute
                WHERE merchant_id = :merchant_id
                  AND bucket BETWEEN :start AND :end
                GROUP BY bucket
                ORDER BY bucket
            """),
            {"merchant_id": str(merchant_id), "start": start, "end": end},
        )
        return [dict(r._mapping) for r in result]

    async def query_hour(
        self, merchant_id: uuid.UUID, start: datetime, end: datetime
    ) -> list[dict]:
        result = await self._session.execute(
            text("""
                SELECT
                    date_trunc('hour', bucket) AS bucket,
                    SUM(count)                 AS count,
                    SUM(amount_sum_cents)      AS volume_cents
                FROM rollup_hour
                WHERE merchant_id = :merchant_id
                  AND bucket BETWEEN :start AND :end
                GROUP BY 1
                ORDER BY 1
            """),
            {"merchant_id": str(merchant_id), "start": start, "end": end},
        )
        return [dict(r._mapping) for r in result]

    async def query_day(
        self, merchant_id: uuid.UUID, start: datetime, end: datetime
    ) -> list[dict]:
        result = await self._session.execute(
            text("""
                SELECT
                    date_trunc('day', bucket) AS bucket,
                    SUM(count)                AS count,
                    SUM(amount_sum_cents)     AS volume_cents
                FROM rollup_day
                WHERE merchant_id = :merchant_id
                  AND bucket BETWEEN :start AND :end
                GROUP BY 1
                ORDER BY 1
            """),
            {"merchant_id": str(merchant_id), "start": start, "end": end},
        )
        return [dict(r._mapping) for r in result]

    async def query_success_rate(
        self, merchant_id: uuid.UUID, start: datetime, end: datetime
    ) -> float | None:
        """
        Success rate = PAYMENT_CONFIRMED / (PAYMENT_CONFIRMED + PAYMENT_FAILED)
        Computed from rollup_hour for efficiency.
        """
        result = await self._session.execute(
            text("""
                SELECT
                    SUM(CASE WHEN event_type = 'PAYMENT_CONFIRMED' THEN count ELSE 0 END) AS confirmed,
                    SUM(CASE WHEN event_type = 'PAYMENT_FAILED'    THEN count ELSE 0 END) AS failed
                FROM rollup_hour
                WHERE merchant_id = :merchant_id
                  AND bucket BETWEEN :start AND :end
            """),
            {"merchant_id": str(merchant_id), "start": start, "end": end},
        )
        row = result.mappings().first()
        if not row:
            return None
        confirmed = row["confirmed"] or 0
        failed = row["failed"] or 0
        total = confirmed + failed
        return round(confirmed / total, 4) if total > 0 else None

    async def query_sparkline(
        self, merchant_id: uuid.UUID, window_minutes: int = 60
    ) -> list[dict]:
        """Per-minute counts for the last N minutes — live sparkline."""
        result = await self._session.execute(
            text("""
                SELECT
                    bucket,
                    SUM(count)            AS count,
                    SUM(amount_sum_cents) AS volume_cents
                FROM rollup_minute
                WHERE merchant_id = :merchant_id
                  AND bucket >= NOW() - (:window * INTERVAL '1 minute')
                GROUP BY bucket
                ORDER BY bucket
            """),
            {"merchant_id": str(merchant_id), "window": window_minutes},
        )
        return [dict(r._mapping) for r in result]

    async def query_leaderboard_from_db(
        self, period_start: datetime, period_end: datetime, top_n: int = 10
    ) -> list[dict]:
        """
        Rebuild leaderboard from rollup_day.
        Used as fallback when Redis is unavailable or cold.
        """
        result = await self._session.execute(
            text("""
                SELECT
                    merchant_id,
                    SUM(amount_sum_cents) AS volume_cents,
                    SUM(count)            AS transaction_count
                FROM rollup_day
                WHERE bucket BETWEEN :start AND :end
                  AND event_type = 'PAYMENT_CONFIRMED'
                GROUP BY merchant_id
                ORDER BY volume_cents DESC
                LIMIT :top_n
            """),
            {"start": period_start, "end": period_end, "top_n": top_n},
        )
        return [dict(r._mapping) for r in result]

    # ─── Consumer Offsets ─────────────────────────────────────────────────────

    async def get_consumer_offset(self, consumer_id: str) -> ConsumerOffset | None:
        result = await self._session.execute(
            select(ConsumerOffset).where(ConsumerOffset.consumer_id == consumer_id)
        )
        return result.scalars().first()

    async def upsert_consumer_offset(
        self,
        consumer_id: str,
        last_event_id: uuid.UUID,
        last_event_at: datetime,
    ) -> None:
        stmt = (
            insert(ConsumerOffset)
            .values(
                consumer_id=consumer_id,
                last_event_id=last_event_id,
                last_event_at=last_event_at,
                updated_at=text("NOW()"),
            )
            .on_conflict_do_update(
                index_elements=["consumer_id"],
                set_={
                    "last_event_id": last_event_id,
                    "last_event_at": last_event_at,
                    "updated_at": text("NOW()"),
                },
            )
        )
        await self._session.execute(stmt)
