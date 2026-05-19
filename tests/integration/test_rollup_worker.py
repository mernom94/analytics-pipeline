"""
tests/integration/test_rollup_worker.py
────────────────────────────────────────
Integration tests for the rollup worker.

ISOLATION MODEL
───────────────
The conftest autouse `clean_db` fixture truncates ALL mutable tables before
each test (including processed_rollup_events and consumer_offsets), so every
test starts with an empty slate.

REPLAY / IDEMPOTENCY TESTS
──────────────────────────
`_reset_offset()` clears ONLY consumer_offsets.  It leaves rollup tables and
processed_rollup_events intact.  This simulates a real replay scenario:

  - The worker replayed from offset 0.
  - processed_rollup_events still contains markers from the first pass.
  - The worker therefore skips aggregation for already-processed events.
  - Rollup values remain exactly the same as after the first pass.

This verifies the exactly-once guarantee: resetting the offset does NOT
cause double-counting.

WORKER INSTANTIATION
────────────────────
Tests construct `RollupWorker(session_factory=...)` explicitly rather than
relying on a global session.  The session_factory fixture provides the
test-scoped factory so the worker shares the same pool as db_session and
worker_session.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.orm import ConsumerOffset, RawEvent, RollupDay, RollupMinute
from app.workers.rollup_worker import (
    get_consumer_id,
    RollupWorker,
    _aggregate_batch,
    _day_bucket,
    _hour_bucket,
    _minute_bucket,
)

from app.db.redis import get_redis
from app.repositories.redis_repository import RedisRepository

# ─── Helpers ──────────────────────────────────────────────────────────────────

async def _reset_offset(session: AsyncSession) -> None:
    """
    Clear the worker's consumer offset so _process_batch replays from zero.

    This does NOT touch rollup tables or processed_rollup_events.
    Idempotency tests depend on this distinction: the markers remain in place
    so the worker skips re-aggregation on replay.
    """
    await session.execute(
        text("DELETE FROM consumer_offsets WHERE consumer_id = :cid"),
        {"cid": get_consumer_id()},
    )
    await session.commit()


async def _get_minute_rollup(
    session: AsyncSession,
    merchant_id: uuid.UUID,
    bucket: datetime,
    event_type: str = "PAYMENT_CONFIRMED",
) -> RollupMinute | None:
    # expire_all() is unnecessary when using separate sessions per query —
    # a fresh session always reads committed state from PostgreSQL.
    result = await session.execute(
        select(RollupMinute).where(
            RollupMinute.merchant_id == merchant_id,
            RollupMinute.bucket == bucket,
            RollupMinute.event_type == event_type,
        )
    )
    return result.scalars().first()


async def _get_day_rollup(
    session: AsyncSession,
    merchant_id: uuid.UUID,
    bucket: datetime,
    event_type: str = "PAYMENT_CONFIRMED",
) -> RollupDay | None:
    result = await session.execute(
        select(RollupDay).where(
            RollupDay.merchant_id == merchant_id,
            RollupDay.bucket == bucket,
            RollupDay.event_type == event_type,
        )
    )
    return result.scalars().first()


async def _get_offset(session: AsyncSession) -> ConsumerOffset | None:
    result = await session.execute(
        select(ConsumerOffset).where(ConsumerOffset.consumer_id == get_consumer_id())
    )
    return result.scalars().first()


def _raw_event(
    merchant_id: uuid.UUID,
    amount_cents: int = 1000,
    event_type: str = "PAYMENT_CONFIRMED",
    client_timestamp: datetime | None = None,
) -> RawEvent:
    ts = client_timestamp or datetime.now(timezone.utc)
    return RawEvent(
        id=uuid.uuid4(),
        event_type=event_type,
        merchant_id=merchant_id,
        amount_cents=amount_cents,
        currency="EUR",
        idempotency_key=str(uuid.uuid4()),
        occurred_at=ts,
        client_timestamp=client_timestamp,
    )


def _make_worker(
    session_factory: async_sessionmaker[AsyncSession],
    test_redis,
) -> RollupWorker:
    return RollupWorker(
        session_factory=session_factory,
        redis_repo=RedisRepository(test_redis),
        worker_id=str,
    )


# ─── Rollup accuracy ──────────────────────────────────────────────────────────

@pytest.mark.asyncio(loop_scope="session")
class TestRollupAccuracy:

    async def test_single_event_creates_all_granularity_rollups(
        self,
        db_session: AsyncSession,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
        merchant: object,
    ) -> None:
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        db_session.add(_raw_event(merchant.id, amount_cents=5000, client_timestamp=ts))
        await db_session.commit()

        await _make_worker(session_factory, test_redis)._process_batch()

        row = await _get_minute_rollup(worker_session, merchant.id, _minute_bucket(ts))
        assert row is not None
        assert row.count == 1
        assert row.amount_sum_cents == 5000

        day = await _get_day_rollup(worker_session, merchant.id, _day_bucket(ts))
        assert day is not None
        assert day.count == 1

    async def test_batch_aggregation_sums_correctly(
        self,
        db_session: AsyncSession,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
        merchant: object,
    ) -> None:
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        for _ in range(10):
            db_session.add(_raw_event(merchant.id, amount_cents=1000, client_timestamp=ts))
        await db_session.commit()

        await _make_worker(session_factory, test_redis)._process_batch()

        row = await _get_minute_rollup(worker_session, merchant.id, _minute_bucket(ts))
        assert row is not None
        assert row.count == 10
        assert row.amount_sum_cents == 10_000

    async def test_min_max_amounts_are_correct(
        self,
        db_session: AsyncSession,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
        merchant: object,
    ) -> None:
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        for amt in [100, 9900, 500]:
            db_session.add(_raw_event(merchant.id, amount_cents=amt, client_timestamp=ts))
        await db_session.commit()

        await _make_worker(session_factory, test_redis)._process_batch()

        row = await _get_minute_rollup(worker_session, merchant.id, _minute_bucket(ts))
        assert row is not None
        assert row.amount_min_cents == 100
        assert row.amount_max_cents == 9900


# ─── Idempotency ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio(loop_scope="session")
class TestIdempotency:
    """
    Verify exactly-once semantics via processed_rollup_events markers.

    The key property under test:
        process → reset_offset → process again == same rollup values

    The processed_rollup_events markers are NOT cleared by _reset_offset().
    They ARE cleared by clean_db (between tests), ensuring per-test isolation
    without contaminating the idempotency invariant within a test.
    """

    async def test_processing_same_batch_twice_is_idempotent(
        self,
        db_session: AsyncSession,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
        merchant: object,
    ) -> None:
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        db_session.add(_raw_event(merchant.id, amount_cents=2500, client_timestamp=ts))
        await db_session.commit()

        worker = _make_worker(session_factory, test_redis)

        # First pass
        await worker._process_batch()
        row1 = await _get_minute_rollup(worker_session, merchant.id, _minute_bucket(ts))
        assert row1 is not None
        count1, sum1 = row1.count, row1.amount_sum_cents

        # Reset offset only — processed_rollup_events markers remain.
        await _reset_offset(worker_session)

        # Second pass — worker sees the same events but markers block reaggregation.
        await worker._process_batch()
        row2 = await _get_minute_rollup(worker_session, merchant.id, _minute_bucket(ts))

        assert row2 is not None
        assert row2.count == count1, (
            f"Replay changed count: {count1} → {row2.count}. "
            "processed_rollup_events markers are not preventing re-aggregation."
        )
        assert row2.amount_sum_cents == sum1, (
            f"Replay changed sum: {sum1} → {row2.amount_sum_cents}."
        )

    async def test_processing_batch_three_times_is_idempotent(
        self,
        db_session: AsyncSession,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
        merchant: object,
    ) -> None:
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        for _ in range(3):
            db_session.add(_raw_event(merchant.id, amount_cents=1000, client_timestamp=ts))
        await db_session.commit()

        worker = _make_worker(session_factory, test_redis)
        counts: list[int] = []

        for _ in range(3):
            await _reset_offset(worker_session)
            await worker._process_batch()
            row = await _get_minute_rollup(worker_session, merchant.id, _minute_bucket(ts))
            counts.append(row.count if row else 0)

        assert counts[0] == counts[1] == counts[2], (
            f"Counts diverged across replays: {counts}. "
            "Upserts are not idempotent — processed_rollup_events markers may not be working."
        )

    async def test_new_events_after_replay_are_aggregated(
        self,
        db_session: AsyncSession,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
        merchant: object,
    ) -> None:
        """
        Events ingested AFTER the replay are still aggregated correctly.

        Scenario:
          1. Ingest event A → process → rollup has count=1
          2. Reset offset
          3. Ingest event B → process
          4. On replay pass: A is skipped (marked), B is new (unmarked)
          5. Rollup should have count=2
        """
        ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        db_session.add(_raw_event(merchant.id, amount_cents=1000, client_timestamp=ts))
        await db_session.commit()

        worker = _make_worker(session_factory, test_redis)
        await worker._process_batch()

        # Add a second event AFTER first pass
        db_session.add(_raw_event(merchant.id, amount_cents=2000, client_timestamp=ts))
        await db_session.commit()

        await _reset_offset(worker_session)
        await worker._process_batch()

        row = await _get_minute_rollup(worker_session, merchant.id, _minute_bucket(ts))
        assert row is not None
        # A was skipped (marked), B was new → total count=2, sum=3000
        assert row.count == 2
        assert row.amount_sum_cents == 3000


# ─── Consumer offset atomicity ─────────────────────────────────────────────────

@pytest.mark.asyncio(loop_scope="session")
class TestConsumerOffsetAtomicity:

    async def test_offset_advances_after_successful_batch(
        self,
        db_session: AsyncSession,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
        merchant: object,
    ) -> None:
        event = _raw_event(merchant.id)
        db_session.add(event)
        await db_session.commit()

        processed = await _make_worker(session_factory, test_redis)._process_batch()

        assert processed > 0
        offset = await _get_offset(worker_session)
        assert offset is not None
        assert offset.last_event_id == event.id

    async def test_offset_not_written_when_no_events(
        self,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
    ) -> None:
        processed = await _make_worker(session_factory, test_redis)._process_batch()

        assert processed == 0
        offset = await _get_offset(worker_session)
        assert offset is None


# ─── Late event handling ───────────────────────────────────────────────────────

@pytest.mark.asyncio(loop_scope="session")
class TestLateEventHandling:

    async def test_late_event_updates_historical_minute_bucket(
        self,
        db_session: AsyncSession,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
        merchant: object,
    ) -> None:
        historical_ts = (datetime.now(timezone.utc) - timedelta(hours=3)).replace(
            second=0, microsecond=0
        )
        db_session.add(
            _raw_event(merchant.id, amount_cents=7500, client_timestamp=historical_ts)
        )
        await db_session.commit()

        await _make_worker(session_factory, test_redis)._process_batch()

        row = await _get_minute_rollup(
            worker_session, merchant.id, _minute_bucket(historical_ts)
        )
        assert row is not None, (
            f"Late event should create rollup at {_minute_bucket(historical_ts)}"
        )
        assert row.amount_sum_cents == 7500

    async def test_event_without_client_timestamp_uses_occurred_at(
        self,
        db_session: AsyncSession,
        worker_session: AsyncSession,
        session_factory: async_sessionmaker[AsyncSession],
        test_redis,
        merchant: object,
    ) -> None:
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        e = _raw_event(merchant.id, amount_cents=3000, client_timestamp=None)
        e.occurred_at = now
        db_session.add(e)
        await db_session.commit()

        await _make_worker(session_factory, test_redis)._process_batch()

        row = await _get_minute_rollup(worker_session, merchant.id, _minute_bucket(now))
        assert row is not None
        assert row.amount_sum_cents == 3000


# ─── Batch aggregation (pure Python, no DB) ───────────────────────────────────

class TestBatchAggregation:

    def _event(self, merchant_id, amount, ts, event_type="PAYMENT_CONFIRMED"):  # noqa: ANN
        from types import SimpleNamespace
        return SimpleNamespace(
            merchant_id=merchant_id,
            amount_cents=amount,
            event_type=event_type,
            client_timestamp=ts,
            occurred_at=ts,
        )

    def test_events_in_same_minute_bucket_are_merged(self) -> None:
        mid = uuid.uuid4()
        ts = datetime(2026, 5, 1, 14, 30, 0, tzinfo=timezone.utc)
        events = [self._event(mid, 1000, ts), self._event(mid, 2000, ts), self._event(mid, 500, ts)]
        minute_agg, _, _ = _aggregate_batch(events)

        key = (mid, _minute_bucket(ts), "PAYMENT_CONFIRMED")
        assert key in minute_agg
        acc = minute_agg[key]
        assert acc.count == 3
        assert acc.amount_sum == 3500
        assert acc.amount_min == 500
        assert acc.amount_max == 2000

    def test_events_in_different_minute_buckets_produce_separate_keys(self) -> None:
        mid = uuid.uuid4()
        ts1 = datetime(2026, 5, 1, 14, 30, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 5, 1, 14, 31, 0, tzinfo=timezone.utc)
        minute_agg, _, _ = _aggregate_batch([self._event(mid, 1000, ts1), self._event(mid, 2000, ts2)])
        assert len(minute_agg) == 2

    def test_events_for_different_merchants_produce_separate_keys(self) -> None:
        mid1, mid2 = uuid.uuid4(), uuid.uuid4()
        ts = datetime(2026, 5, 1, 14, 30, 0, tzinfo=timezone.utc)
        minute_agg, _, _ = _aggregate_batch([self._event(mid1, 1000, ts), self._event(mid2, 2000, ts)])
        assert len(minute_agg) == 2

    def test_same_ts_different_event_types_produce_separate_keys(self) -> None:
        mid = uuid.uuid4()
        ts = datetime(2026, 5, 1, 14, 30, 0, tzinfo=timezone.utc)
        minute_agg, _, _ = _aggregate_batch([
            self._event(mid, 1000, ts, "PAYMENT_CONFIRMED"),
            self._event(mid, 500, ts, "PAYMENT_FAILED"),
        ])
        assert len(minute_agg) == 2

    def test_empty_batch_produces_empty_agg_maps(self) -> None:
        minute_agg, hour_agg, day_agg = _aggregate_batch([])
        assert len(minute_agg) == 0
        assert len(hour_agg) == 0
        assert len(day_agg) == 0

    def test_hour_bucket_rolls_up_across_minutes(self) -> None:
        mid = uuid.uuid4()
        ts1 = datetime(2026, 5, 1, 14, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 5, 1, 14, 45, 0, tzinfo=timezone.utc)
        _, hour_agg, _ = _aggregate_batch([self._event(mid, 1000, ts1), self._event(mid, 2000, ts2)])
        assert len(hour_agg) == 1
        acc = next(iter(hour_agg.values()))
        assert acc.count == 2
        assert acc.amount_sum == 3000

    def test_day_bucket_rolls_up_across_hours(self) -> None:
        mid = uuid.uuid4()
        ts1 = datetime(2026, 5, 1, 9, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 5, 1, 18, 30, 0, tzinfo=timezone.utc)
        _, _, day_agg = _aggregate_batch([self._event(mid, 1000, ts1), self._event(mid, 2000, ts2)])
        assert len(day_agg) == 1
        acc = next(iter(day_agg.values()))
        assert acc.count == 2
        assert acc.amount_sum == 3000


# ─── HTTP endpoint deduplication ──────────────────────────────────────────────

@pytest.mark.asyncio(loop_scope="session")
class TestDuplicateEventNonCountedInRollup:

    async def test_duplicate_event_not_double_counted(
        self, client: AsyncClient, db_session: AsyncSession, valid_event: dict
    ) -> None:
        r1 = await client.post("/events", json=valid_event)
        r2 = await client.post("/events", json=valid_event)
        assert r1.status_code == 202
        assert r2.status_code == 200
        assert r1.json()["event_id"] == r2.json()["event_id"]

        result = await db_session.execute(
            text("SELECT COUNT(*) FROM raw_events WHERE idempotency_key = :key"),
            {"key": valid_event["idempotency_key"]},
        )
        assert result.scalar() == 1

    async def test_batch_duplicate_responses_have_correct_event_id(
        self, client: AsyncClient, db_session: AsyncSession, valid_event: dict
    ) -> None:
        r1 = await client.post("/events", json=valid_event)
        assert r1.status_code == 202
        original_event_id = r1.json()["event_id"]

        batch = {
            "events": [
                {**valid_event, "idempotency_key": str(uuid.uuid4())},
                {**valid_event},  # duplicate
            ]
        }
        r2 = await client.post("/events/batch", json=batch)
        assert r2.status_code == 202

        body = r2.json()
        assert body["duplicates"] == 1
        dup = next(e for e in body["events"] if e["idempotency_key"] == valid_event["idempotency_key"])
        assert dup["event_id"] == original_event_id
        assert dup["duplicate"] is True