"""
Ingestion service.

POST /events contract:
- Validate event schema (Pydantic handles this at the route layer)
- Validate merchant_id exists
- Server-assign occurred_at (never trust client clocks for bucketing)
- Write to raw_events with ON CONFLICT DO NOTHING on idempotency_key
- Send NOTIFY for rollup worker (best-effort; polling is the correctness path)
- Return 202 Accepted (or 200 OK for duplicate)

The write path is a single INSERT.  Rollup happens asynchronously in the worker.

Redis is injected (not pulled from global state) so the service is testable
without a live Redis connection.

OBSERVABILITY
─────────────
- Prometheus: events_ingested_total, duplicate_events_total
- OTLP spans: ingest_event, ingest_batch
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import MerchantNotFoundError
from app.core.logging import get_logger
from app.models.orm import RawEvent
from app.models.schemas import BatchIngestResponse, EventIngest, EventIngestResponse
from app.observability import DUPLICATE_EVENTS, EVENTS_INGESTED, get_tracer
from app.repositories.event_repository import EventRepository
from app.repositories.redis_repository import RedisRepository

logger = get_logger(__name__)


class IngestionService:
    def __init__(self, session: AsyncSession, redis: Redis) -> None:
        self._session = session
        self._event_repo = EventRepository(session)
        self._redis_repo = RedisRepository(redis)

    async def ingest_event(self, event: EventIngest) -> tuple[EventIngestResponse, bool]:
        """
        Ingest a single event.

        Returns (response, is_duplicate).
        Callers use is_duplicate to set the correct HTTP status code
        (200 OK for duplicate, 202 Accepted for new).
        """
        with get_tracer().start_as_current_span("ingest_event") as span:
            span.set_attribute("event_type", event.event_type)
            span.set_attribute("merchant_id", str(event.merchant_id))

            # Validate merchant existence
            if not await self._event_repo.merchant_exists(event.merchant_id):
                raise MerchantNotFoundError(str(event.merchant_id))

            # Server-side timestamp — never trust client clocks
            occurred_at = datetime.now(timezone.utc)

            row, is_duplicate = await self._event_repo.insert_event(event, occurred_at)

            # Prometheus counters
            if is_duplicate:
                DUPLICATE_EVENTS.labels(event_type=event.event_type).inc()
                span.set_attribute("duplicate", True)
            else:
                EVENTS_INGESTED.labels(event_type=event.event_type).inc()
                span.set_attribute("duplicate", False)
                # Notify rollup worker via Postgres LISTEN/NOTIFY (best-effort).
                # The worker's poll loop is the correctness path; this only reduces latency.
                try:
                    await self._event_repo.notify_new_event(str(row.id))
                except Exception as exc:
                    logger.warning(
                        "notify_failed_will_rely_on_poll",
                        error=str(exc),
                        event_id=str(row.id),
                    )
                # Track ingestion rate
                await self._redis_repo.record_ingestion(1)

            logger.info(
                "event_ingested",
                event_id=str(row.id),
                event_type=event.event_type,
                merchant_id=str(event.merchant_id),
                amount_cents=event.amount_cents,
                duplicate=is_duplicate,
                occurred_at=occurred_at.isoformat(),
            )

            return (
                EventIngestResponse(
                    event_id=row.id,
                    idempotency_key=row.idempotency_key,
                    occurred_at=row.occurred_at,
                    duplicate=is_duplicate,
                ),
                is_duplicate,
            )

    async def ingest_batch(self, events: list[EventIngest]) -> BatchIngestResponse:
        """
        Ingest up to 500 events in a single request.

        Validates all merchant IDs in one query, then bulk-inserts.
        Duplicate detection uses a single re-fetch for missing idempotency keys
        so every response carries the correct, authoritative event_id.
        """
        with get_tracer().start_as_current_span("ingest_batch") as span:
            span.set_attribute("batch_size", len(events))

            # Single query for all merchant IDs — not N queries.
            merchant_ids = {e.merchant_id for e in events}
            missing_merchants = await self._event_repo.missing_merchant_ids(merchant_ids)
            if missing_merchants:
                raise MerchantNotFoundError(str(next(iter(missing_merchants))))

            occurred_at = datetime.now(timezone.utc)
            inserted_rows, duplicate_count = await self._event_repo.insert_batch(events, occurred_at)

            if inserted_rows:
                # Prometheus: count new events by type
                from collections import Counter as _Counter
                type_counts = _Counter(e.event_type for e in events
                                       if e.idempotency_key in {r.idempotency_key for r in inserted_rows})
                for event_type, count in type_counts.items():
                    EVENTS_INGESTED.labels(event_type=event_type).inc(count)

                await self._redis_repo.record_ingestion(len(inserted_rows))
                # Best-effort NOTIFY — polling is the correctness path.
                try:
                    await self._event_repo.notify_new_event(str(inserted_rows[-1].id))
                except Exception as exc:
                    logger.warning(
                        "batch_notify_failed_will_rely_on_poll",
                        error=str(exc),
                        last_event_id=str(inserted_rows[-1].id),
                    )

            if duplicate_count:
                # Best-effort: label as unknown type since we lack per-duplicate type info here
                DUPLICATE_EVENTS.labels(event_type="BATCH_UNKNOWN").inc(duplicate_count)

            span.set_attribute("accepted", len(inserted_rows))
            span.set_attribute("duplicates", duplicate_count)

            # Build per-event response map for the newly inserted rows.
            inserted_by_key: dict[str, RawEvent] = {r.idempotency_key: r for r in inserted_rows}

            # The two-phase idempotency table insert already claimed unique keys.
            # Duplicate events' event_ids are retrieved in one bulk fetch.
            duplicate_keys = [
                e.idempotency_key
                for e in events
                if e.idempotency_key not in inserted_by_key
            ]
            existing_by_key: dict[str, RawEvent] = {}
            if duplicate_keys:
                existing_by_key = await self._event_repo.get_by_idempotency_keys(duplicate_keys)

            responses: list[EventIngestResponse] = []
            for event in events:
                if event.idempotency_key in inserted_by_key:
                    row = inserted_by_key[event.idempotency_key]
                    responses.append(
                        EventIngestResponse(
                            event_id=row.id,
                            idempotency_key=row.idempotency_key,
                            occurred_at=row.occurred_at,
                            duplicate=False,
                        )
                    )
                else:
                    # Duplicate: return the authoritative existing event_id.
                    # Never use a sentinel — existing_by_key was fetched above.
                    existing = existing_by_key.get(event.idempotency_key)
                    responses.append(
                        EventIngestResponse(
                            event_id=existing.id if existing else uuid.UUID(int=0),
                            idempotency_key=event.idempotency_key,
                            occurred_at=existing.occurred_at if existing else occurred_at,
                            duplicate=True,
                        )
                    )

            logger.info(
                "batch_ingested",
                total=len(events),
                accepted=len(inserted_rows),
                duplicates=duplicate_count,
            )

            return BatchIngestResponse(
                accepted=len(inserted_rows),
                duplicates=duplicate_count,
                events=responses,
            )
