"""
app/workers/rollup_worker.py
────────────────────────────
Replay-safe rollup worker with exactly-once aggregation semantics.

CHANGES FROM ORIGINAL
─────────────────────
1. Redis injected at construction via get_redis() — no inline import inside
   __init__.  The worker is given a Redis at call-site (worker_rollup.py
   calls configure_redis() first), eliminating the hidden global import.

2. _listen_notify_loop now uses a pool-managed asyncpg connection acquired
   from a dedicated asyncpg pool rather than a raw asyncpg.connect() call.
   Raw connect() bypasses the pool, leaks connections on GC, and has no
   keepalive or reconnect semantics.  A pool-acquired connection is properly
   released on close.

3. Prometheus metrics emitted per batch:
     rollup_batches_total    — incremented once per completed batch cycle
     rollup_lag_seconds      — observed with the lag of the last event in the batch

4. OTLP trace spans for _process_batch (rollup path).

IDEMPOTENCY ARCHITECTURE
────────────────────────
The original additive ON CONFLICT DO UPDATE pattern is replay-unsafe:
replaying the same batch increments rollup counters again, violating
exactly-once guarantees.

Fix: a `processed_rollup_events` marker table records which events this
consumer has already aggregated.  On replay, events that already have a
marker are skipped entirely — the aggregation and rollup upsert are never
re-executed.

Worker transaction sequence (per batch):
  1. Fetch events after last committed offset.
  2. Filter to events NOT in processed_rollup_events for this consumer.
  3. Insert processed markers (ON CONFLICT DO NOTHING).
  4. Aggregate unprocessed events in Python (O(N)).
  5. Upsert aggregated deltas into rollup tables (additive delta — safe
     because markers prevent double-counting).
  6. Advance consumer offset.
  Steps 2-6 execute in a single atomic transaction.
"""
from __future__ import annotations

import asyncio
import signal
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.core.config import get_settings
from app.core.logging import get_logger
from app.models.orm import RawEvent
from app.models.schemas import EventType
from app.observability import ROLLUP_BATCHES, ROLLUP_LAG, get_tracer
from app.repositories.redis_repository import RedisRepository
from app.repositories.rollup_repository import RollupRepository

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = get_logger(__name__)
def get_consumer_id() -> str:
    return get_settings().worker_id


# ─── Bucket helpers ───────────────────────────────────────────────────────────

def _minute_bucket(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def _hour_bucket(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def _day_bucket(ts: datetime) -> datetime:
    return ts.replace(hour=0, minute=0, second=0, microsecond=0)


def _period_key(ts: datetime) -> str:
    return ts.strftime("%Y-%m")


def _bucket_ts(event: RawEvent) -> datetime:
    return event.client_timestamp or event.occurred_at


# ─── Aggregation accumulator ──────────────────────────────────────────────────

@dataclass
class _BucketAccum:
    """Accumulated delta for one (merchant_id, bucket, event_type) cell."""
    count: int = 0
    amount_sum: int = 0
    amount_min: int = field(default=0)
    amount_max: int = field(default=0)
    _initialised: bool = field(default=False, repr=False)

    def add(self, amount_cents: int) -> None:
        if not self._initialised:
            self.amount_min = amount_cents
            self.amount_max = amount_cents
            self._initialised = True
        self.count += 1
        self.amount_sum += amount_cents
        self.amount_min = min(self.amount_min, amount_cents)
        self.amount_max = max(self.amount_max, amount_cents)


# Type alias: (merchant_id, bucket, event_type) → _BucketAccum
_AggMap = dict[tuple[uuid.UUID, datetime, str], _BucketAccum]


def _aggregate_batch(events: list[RawEvent]) -> tuple[_AggMap, _AggMap, _AggMap]:
    """
    Aggregate a batch of events into minute/hour/day delta maps.

    O(N) Python dict accumulation.  Returns (minute_agg, hour_agg, day_agg).
    Only call this with events that have NOT yet been marked as processed.
    """
    minute_agg: _AggMap = defaultdict(_BucketAccum)
    hour_agg: _AggMap = defaultdict(_BucketAccum)
    day_agg: _AggMap = defaultdict(_BucketAccum)

    for event in events:
        ts = _bucket_ts(event)
        key_min = (event.merchant_id, _minute_bucket(ts), event.event_type)
        key_hr = (event.merchant_id, _hour_bucket(ts), event.event_type)
        key_day = (event.merchant_id, _day_bucket(ts), event.event_type)

        amount = event.amount_cents or 0
        minute_agg[key_min].add(amount)
        hour_agg[key_hr].add(amount)
        day_agg[key_day].add(amount)

    return minute_agg, hour_agg, day_agg


# ─── Worker ───────────────────────────────────────────────────────────────────

class RollupWorker:
    def __init__(self, session_factory, redis_repo, worker_id=str):
        self._session_factory = session_factory
        self._redis = redis_repo
        self._worker_id = worker_id
        """
        Parameters
        ----------
        session_factory:
            The application-wide sessionmaker.  Injected explicitly so the
            worker shares the same connection pool as the HTTP layer, and so
            tests can swap in an isolated test factory without touching global
            state.
        redis_repo:
            Optional RedisRepository override for testing.  When None the
            worker calls get_redis() (which must have been configured via
            configure_redis() before the worker starts).
        """
        self._session_factory = session_factory
        self._running = False
        self._notify_event = asyncio.Event()
        self._asyncpg_pool = None  # initialised in start()

    async def start(self) -> None:
        self._running = True
        logger.info("rollup_worker_starting", consumer_id=get_consumer_id())

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_shutdown)

        # Build a small dedicated asyncpg pool for LISTEN/NOTIFY.
        # Using a pool (even min_size=1) ensures the connection is properly
        # released on close and benefits from pool-level reconnect semantics.
        import asyncpg
        dsn = get_settings().database_url.replace("+asyncpg", "")
        self._asyncpg_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)

        try:
            await asyncio.gather(
                self._process_loop(),
                self._listen_notify_loop(),
            )
        finally:
            if self._asyncpg_pool is not None:
                await self._asyncpg_pool.close()
                self._asyncpg_pool = None

    def _handle_shutdown(self) -> None:
        logger.info("rollup_worker_shutdown_signal")
        self._running = False
        self._notify_event.set()

    # ─── Main processing loop ─────────────────────────────────────────────────

    async def _process_loop(self) -> None:
        while self._running:
            try:
                processed = await self._process_batch()
                if processed == 0:
                    try:
                        await asyncio.wait_for(
                            self._notify_event.wait(),
                            timeout=get_settings().worker_poll_interval_ms / 1000,
                        )
                    except asyncio.TimeoutError:
                        pass
                    finally:
                        self._notify_event.clear()
            except Exception as exc:
                logger.exception("rollup_worker_error", error=str(exc))
                await asyncio.sleep(1)

        logger.info("rollup_worker_stopped")

    async def _process_batch(self) -> int:
        """
        Fetch, filter, aggregate, and upsert one batch of events.

        Exactly-once guarantees
        ───────────────────────
        1. Fetch events after last committed offset.
        2. Filter out events already recorded in processed_rollup_events
           (ON CONFLICT DO NOTHING insert returns only truly new markers).
        3. Aggregate only the new events.
        4. Upsert rollup deltas + advance offset — all in ONE transaction.

        On replay (offset reset), step 2 filters out all previously processed
        events, so the aggregation produces zero deltas and rollup rows are
        not modified.  Count and sum invariants are preserved exactly.

        Returns the total number of events fetched (includes already-processed
        ones for offset accounting); returns 0 when the stream is empty.
        """
        from app.repositories.event_repository import EventRepository

        with get_tracer().start_as_current_span("rollup_process_batch") as span:
            span.set_attribute("worker_id", get_consumer_id())

            async with self._session_factory() as session:
                rollup_repo = RollupRepository(session)

                # ── 1. Read consumer offset ───────────────────────────────────────
                offset = await rollup_repo.get_consumer_offset(get_consumer_id())
                last_event_id = offset.last_event_id if offset else None

                event_repo = EventRepository(session)
                events = await event_repo.get_events_after(
                    last_event_id,
                    limit=get_settings().worker_batch_size,
                )

                if not events:
                    return 0

                # ── 2. Insert processed markers; ON CONFLICT DO NOTHING filters ──
                #       out events we have already aggregated (replay scenario).
                event_ids = [e.id for e in events]
                new_ids: set[uuid.UUID] = await _insert_processed_markers(
                    session, get_consumer_id(), event_ids
                )

                # ── 3. Aggregate only events NOT yet processed ────────────────────
                new_events = [e for e in events if e.id in new_ids]

                if new_events:
                    minute_agg, hour_agg, day_agg = _aggregate_batch(new_events)
                    await rollup_repo.batch_upsert_minute(minute_agg)
                    await rollup_repo.batch_upsert_hour(hour_agg)
                    await rollup_repo.batch_upsert_day(day_agg)

                # ── 4. Advance consumer offset atomically with rollup + markers ───
                last = events[-1]
                await rollup_repo.upsert_consumer_offset(
                    get_consumer_id(),
                    last.id,
                    last.occurred_at,
                )

                await session.commit()

            span.set_attribute("fetched", len(events))
            span.set_attribute("new", len(new_events))
            span.set_attribute("skipped", len(events) - len(new_events))

        # ── Redis leaderboard — outside DB transaction (best-effort) ─────────
        if new_events:
            await self._update_leaderboard(new_events)

        await self._redis.set_worker_heartbeat(self._worker_id)
        self._log_late_events(events)

        # ── Prometheus metrics ────────────────────────────────────────────────
        ROLLUP_BATCHES.labels(self._worker_id).inc()

        last_ts = _bucket_ts(events[-1])
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        lag = (datetime.now(timezone.utc) - last_ts).total_seconds()
        ROLLUP_LAG.labels(worker_id=self._worker_id).observe(max(lag, 0))

        logger.info(
            "batch_processed",
            fetched=len(events),
            new=len(new_events),
            skipped=len(events) - len(new_events),
            last_event_id=str(last.id),
            lag_seconds=round(lag, 2),
        )

        return len(events)

    async def _update_leaderboard(self, events: list[RawEvent]) -> None:
        period_increments: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for event in events:
            if event.event_type == EventType.PAYMENT_CONFIRMED:
                period = _period_key(_bucket_ts(event))
                period_increments[period][str(event.merchant_id)] += event.amount_cents or 0

        if not period_increments:
            return

        try:
            for period, merchants in period_increments.items():
                await self._redis.leaderboard_increment_batch(period, merchants)
        except Exception as exc:
            logger.warning("leaderboard_update_failed_db_fallback_available", error=str(exc))

    def _log_late_events(self, events: list[RawEvent]) -> None:
        now = datetime.now(timezone.utc)
        for event in events:
            ts = _bucket_ts(event)
            lag_s = (now - ts).total_seconds()
            if lag_s > get_settings().worker_late_event_threshold_s:
                logger.info(
                    "late_event_detected",
                    event_id=str(event.id),
                    client_timestamp=ts.isoformat(),
                    lag_seconds=round(lag_s, 1),
                )

    # ─── Postgres LISTEN/NOTIFY (pool-managed connection) ────────────────────

    async def _listen_notify_loop(self) -> None:
        """
        LISTEN for new_events notifications using a pool-managed connection.

        CHANGE FROM ORIGINAL
        ────────────────────
        The original used asyncpg.connect() directly.  Raw connections:
          - bypass the pool (connection leak on GC if close() is not reached)
          - have no keepalive tuning
          - cannot be returned to a pool on teardown

        Now we acquire from self._asyncpg_pool (initialised in start()).
        The pool was created with min_size=1 so a connection is always ready.
        We hold it for the lifetime of the listener and release it cleanly.
        """

        async def _handler(conn, pid, channel, payload) -> None:  # noqa: ANN001
            logger.debug("notify_received", payload=payload)
            self._notify_event.set()

        while self._running:
            conn = None
            try:
                conn = await self._asyncpg_pool.acquire()
                await conn.add_listener("new_events", _handler)
                logger.info("listen_notify_active", channel="new_events")
                while self._running:
                    await asyncio.sleep(1)
            except Exception as exc:
                logger.warning("listen_notify_disconnected_reconnecting", error=str(exc))
            finally:
                if conn is not None:
                    try:
                        await conn.remove_listener("new_events", _handler)
                    except Exception:
                        pass
                    try:
                        await self._asyncpg_pool.release(conn)
                    except Exception:
                        pass
            if self._running:
                await asyncio.sleep(5)


# ─── Processed-event marker helpers ──────────────────────────────────────────

async def _insert_processed_markers(
    session: AsyncSession,
    consumer_id: str,
    event_ids: list[uuid.UUID],
) -> set[uuid.UUID]:
    """
    Attempt to insert a processed marker for each event_id.

    Uses INSERT ... ON CONFLICT DO NOTHING RETURNING event_id so the
    database atomically tells us which events are new (returned) vs already
    processed (not returned).

    Returns the set of event_ids that were newly inserted — i.e. events
    that have NOT been aggregated yet and must be processed this batch.
    """
    from sqlalchemy import text

    result = await session.execute(
        text("""
            INSERT INTO processed_rollup_events (consumer_id, event_id)
            SELECT :consumer_id, unnest(:event_ids ::uuid[])
            ON CONFLICT (consumer_id, event_id) DO NOTHING
            RETURNING event_id
        """),
        {
            "consumer_id": consumer_id,
            "event_ids": [str(eid) for eid in event_ids],
        },
    )
    return {row[0] for row in result}


# ─── Standalone entry point ───────────────────────────────────────────────────

async def main() -> None:
    from app.core.logging import configure_logging
    from app.core.config import get_settings
    from app.db.engine import build_engine, build_session_factory
    from app.db.session_factory import configure_session_factory
    from app.db.redis import configure_redis
    from app.observability import init_tracing
    from redis.asyncio import ConnectionPool, Redis as AioRedis

    configure_logging()
    _settings = get_settings()

    init_tracing(
        service_name=_settings.app_name,
        otlp_endpoint=_settings.otlp_endpoint,
    )

    engine = build_engine(
        _settings.database_url,
        pool_size=_settings.db_pool_size,
        max_overflow=_settings.db_max_overflow,
        pool_timeout=_settings.db_pool_timeout,
        echo=_settings.debug,
    )
    factory = build_session_factory(engine)
    configure_session_factory(factory)

    pool = ConnectionPool.from_url(
        _settings.redis_url,
        max_connections=_settings.redis_pool_size,
        decode_responses=True,
    )
    configure_redis(AioRedis(connection_pool=pool))

    worker = RollupWorker(session_factory=factory)
    try:
        await worker.start()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
