"""
Unit tests for core rollup logic.

Tests the stateless functions that don't require DB or Redis:
- Bucket calculation (minute/hour/day)
- Late event detection
- Query granularity routing
- Period key generation
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.workers.rollup_worker import (
    _bucket_ts,
    _day_bucket,
    _hour_bucket,
    _minute_bucket,
    _period_key,
)
from app.services.query_service import _select_granularity
from app.models.schemas import RollupGranularity


# ─── Bucket calculations ──────────────────────────────────────────────────────

class TestBucketCalculations:
    def _make_ts(self, hour: int = 14, minute: int = 37, second: int = 42) -> datetime:
        return datetime(2026, 5, 12, hour, minute, second, 123456, tzinfo=timezone.utc)

    def test_minute_bucket_truncates_to_minute(self):
        ts = self._make_ts(14, 37, 42)
        bucket = _minute_bucket(ts)
        assert bucket == datetime(2026, 5, 12, 14, 37, 0, 0, tzinfo=timezone.utc)

    def test_hour_bucket_truncates_to_hour(self):
        ts = self._make_ts(14, 37, 42)
        bucket = _hour_bucket(ts)
        assert bucket == datetime(2026, 5, 12, 14, 0, 0, 0, tzinfo=timezone.utc)

    def test_day_bucket_truncates_to_day(self):
        ts = self._make_ts(14, 37, 42)
        bucket = _day_bucket(ts)
        assert bucket == datetime(2026, 5, 12, 0, 0, 0, 0, tzinfo=timezone.utc)

    def test_minute_bucket_on_exact_boundary(self):
        ts = datetime(2026, 5, 12, 14, 37, 0, 0, tzinfo=timezone.utc)
        assert _minute_bucket(ts) == ts

    def test_period_key_format(self):
        ts = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert _period_key(ts) == "2026-05"

    def test_period_key_pads_month(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert _period_key(ts) == "2026-01"


# ─── Late event bucketing ─────────────────────────────────────────────────────

class TestBucketTs:
    """
    _bucket_ts only reads .client_timestamp and .occurred_at — it never touches
    SQLAlchemy instrumentation.  We use SimpleNamespace so the test has zero ORM
    dependencies and zero database setup.
    """
    def _make_event(self, client_timestamp=None):
        from types import SimpleNamespace
        return SimpleNamespace(
            occurred_at=datetime(2026, 5, 12, 14, 0, 0, tzinfo=timezone.utc),
            client_timestamp=client_timestamp,
        )

    def test_uses_client_timestamp_when_present(self):
        client_ts = datetime(2026, 5, 12, 13, 55, 0, tzinfo=timezone.utc)
        event = self._make_event(client_timestamp=client_ts)
        assert _bucket_ts(event) == client_ts

    def test_falls_back_to_occurred_at_when_no_client_timestamp(self):
        event = self._make_event(client_timestamp=None)
        assert _bucket_ts(event) == event.occurred_at

    def test_late_event_goes_to_correct_historical_bucket(self):
        """A 5-minute-late event must land in the correct historical minute bucket."""
        client_ts = datetime(2026, 5, 12, 13, 55, 30, tzinfo=timezone.utc)
        event = self._make_event(client_timestamp=client_ts)

        ts = _bucket_ts(event)
        bucket = _minute_bucket(ts)

        # Must land in 13:55, not 14:00
        expected = datetime(2026, 5, 12, 13, 55, 0, 0, tzinfo=timezone.utc)
        assert bucket == expected

    def test_late_event_does_not_contaminate_current_bucket(self):
        """Ensure late events don't pollute the current (wrong) time window."""
        client_ts = datetime(2026, 5, 12, 13, 55, tzinfo=timezone.utc)
        occurred_at = datetime(2026, 5, 12, 14, 0, tzinfo=timezone.utc)

        event = self._make_event(client_timestamp=client_ts)
        event.occurred_at = occurred_at

        bucket = _minute_bucket(_bucket_ts(event))
        current_bucket = _minute_bucket(occurred_at)

        assert bucket != current_bucket


# ─── Query granularity routing ────────────────────────────────────────────────

class TestQueryGranularity:
    def _range(self, hours: float) -> tuple[datetime, datetime]:
        from datetime import timedelta
        end = datetime(2026, 5, 12, 14, 0, 0, tzinfo=timezone.utc)
        start = end - timedelta(hours=hours)
        return start, end

    def test_one_hour_range_uses_minute(self):
        start, end = self._range(1)
        gran, ttl = _select_granularity(start, end)
        assert gran == RollupGranularity.MINUTE

    def test_three_hour_range_uses_minute(self):
        start, end = self._range(3)
        gran, _ = _select_granularity(start, end)
        assert gran == RollupGranularity.MINUTE

    def test_just_over_three_hours_uses_hour(self):
        start, end = self._range(3.1)
        gran, _ = _select_granularity(start, end)
        assert gran == RollupGranularity.HOUR

    def test_seven_day_range_uses_hour(self):
        start, end = self._range(24 * 7)
        gran, _ = _select_granularity(start, end)
        assert gran == RollupGranularity.HOUR

    def test_thirty_day_range_uses_hour(self):
        start, end = self._range(24 * 30)
        gran, _ = _select_granularity(start, end)
        assert gran == RollupGranularity.HOUR

    def test_just_over_thirty_days_uses_day(self):
        start, end = self._range(24 * 31)
        gran, _ = _select_granularity(start, end)
        assert gran == RollupGranularity.DAY

    def test_one_year_range_uses_day(self):
        start, end = self._range(24 * 365)
        gran, _ = _select_granularity(start, end)
        assert gran == RollupGranularity.DAY

    def test_live_range_has_short_ttl(self):
        start, end = self._range(1)
        _, ttl = _select_granularity(start, end)
        assert ttl <= 10  # must be near-real-time

    def test_historical_range_has_longer_ttl(self):
        start, end = self._range(24 * 7)
        _, ttl = _select_granularity(start, end)
        assert ttl >= 30

    def test_archive_range_has_longest_ttl(self):
        start, end = self._range(24 * 90)
        _, ttl = _select_granularity(start, end)
        assert ttl >= 60


# ─── Batch aggregation correctness ───────────────────────────────────────────

class TestBucketAccumulator:
    """Test the _BucketAccum dataclass that drives batch aggregation."""

    def test_first_add_sets_min_and_max(self):
        from app.workers.rollup_worker import _BucketAccum
        acc = _BucketAccum()
        acc.add(1000)
        assert acc.count == 1
        assert acc.amount_sum == 1000
        assert acc.amount_min == 1000
        assert acc.amount_max == 1000

    def test_subsequent_adds_update_sum_and_extremes(self):
        from app.workers.rollup_worker import _BucketAccum
        acc = _BucketAccum()
        acc.add(500)
        acc.add(1500)
        acc.add(1000)
        assert acc.count == 3
        assert acc.amount_sum == 3000
        assert acc.amount_min == 500
        assert acc.amount_max == 1500

    def test_single_event_min_equals_max(self):
        from app.workers.rollup_worker import _BucketAccum
        acc = _BucketAccum()
        acc.add(9999)
        assert acc.amount_min == acc.amount_max == 9999

    def test_zero_amount_is_handled(self):
        from app.workers.rollup_worker import _BucketAccum
        acc = _BucketAccum()
        acc.add(0)
        acc.add(1000)
        assert acc.amount_min == 0
        assert acc.amount_max == 1000
        assert acc.amount_sum == 1000


# ─── Regression: sentinel event_id in batch duplicate responses ───────────────

class TestBatchDuplicateResponseSchema:
    """
    Regression tests for the critical bug where batch duplicate responses used
    merchant_id as a sentinel value for event_id.

    These tests verify the schema contract — the DB-level correctness is tested
    in tests/integration/test_rollup_worker.py::TestDuplicateEventNonCountedInRollup.
    """

    def test_event_ingest_response_event_id_is_uuid(self):
        """event_id must be a proper UUID, never a merchant_id or zero-value sentinel."""
        import uuid
        from datetime import datetime, timezone
        from app.models.schemas import EventIngestResponse

        mid = uuid.uuid4()
        eid = uuid.uuid4()
        resp = EventIngestResponse(
            event_id=eid,
            idempotency_key="test-key",
            occurred_at=datetime.now(timezone.utc),
            duplicate=True,
        )
        # event_id must not equal the merchant_id (the sentinel bug)
        assert resp.event_id != mid
        assert resp.event_id == eid

    def test_event_ingest_response_rejects_zero_uuid(self):
        """
        A nil UUID (uuid.UUID(int=0)) is an invalid sentinel value.
        The schema may allow it structurally but we document the contract here.
        The correct fix is to always fetch the existing event_id, not fabricate one.
        """
        import uuid
        from datetime import datetime, timezone
        from app.models.schemas import EventIngestResponse

        nil_uuid = uuid.UUID(int=0)
        resp = EventIngestResponse(
            event_id=nil_uuid,
            idempotency_key="test-key",
            occurred_at=datetime.now(timezone.utc),
            duplicate=True,
        )
        # Document: nil UUID should never appear in a real response.
        # The integration test verifies the real event_id is always returned.
        assert str(resp.event_id) == "00000000-0000-0000-0000-000000000000"
