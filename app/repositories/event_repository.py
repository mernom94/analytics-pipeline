"""
Repository for raw_events.  All writes go through here.
Reads are intentionally minimal — this is an append-only audit log.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.models.orm import EventIdempotency, Merchant, RawEvent
from app.models.schemas import EventIngest

logger = get_logger(__name__)


class EventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_event(self, event: EventIngest, occurred_at: datetime) -> tuple[RawEvent, bool]:
        """
        Insert event with cross-partition-safe idempotency enforcement.

        Two-phase write:
          Phase 1: INSERT into event_idempotency ON CONFLICT DO NOTHING.
                   If this returns a row, the key is new — proceed.
                   If it returns nothing, the key exists — fetch existing event.
          Phase 2: INSERT into raw_events (only reached for new events).

        This pattern is safe across partition boundaries because
        event_idempotency is a non-partitioned table with a true primary key.

        Returns (event_record, was_duplicate).
        """
        event_id = uuid.uuid4()

        # Phase 1: claim the idempotency key
        idem_stmt = (
            insert(EventIdempotency)
            .values(idempotency_key=event.idempotency_key, event_id=event_id)
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(EventIdempotency.event_id)
        )
        idem_result = await self._session.execute(idem_stmt)
        claimed_id = idem_result.scalar()

        if claimed_id is None:
            # Duplicate — fetch existing raw_events row via the idempotency table
            existing_idem = await self._session.execute(
                select(EventIdempotency).where(
                    EventIdempotency.idempotency_key == event.idempotency_key
                )
            )
            idem_row = existing_idem.scalars().one()
            existing_event = await self._session.execute(
                select(RawEvent).where(RawEvent.id == idem_row.event_id)
            )
            existing_row = existing_event.scalars().one()
            logger.info(
                "event_duplicate",
                idempotency_key=event.idempotency_key,
                existing_id=str(existing_row.id),
            )
            return existing_row, True

        # Phase 2: insert the raw event (key is now claimed)
        raw_stmt = (
            insert(RawEvent)
            .values(
                id=event_id,
                event_type=event.event_type,
                merchant_id=event.merchant_id,
                amount_cents=event.amount_cents,
                currency=event.currency,
                idempotency_key=event.idempotency_key,
                occurred_at=occurred_at,
                client_timestamp=event.client_timestamp,
                metadata_=event.metadata,
            )
            .returning(RawEvent)
        )
        raw_result = await self._session.execute(raw_stmt)
        return raw_result.scalars().one(), False

    async def insert_batch(
        self, events: list[EventIngest], occurred_at: datetime
    ) -> tuple[list[RawEvent], int]:
        """
        Bulk insert events with cross-partition-safe idempotency enforcement.

        Phase 1: bulk INSERT into event_idempotency ON CONFLICT DO NOTHING.
                 RETURNING tells us which keys were newly claimed.
        Phase 2: bulk INSERT into raw_events for claimed keys only.

        Returns (inserted_rows, duplicate_count).
        """
        # Assign event_ids upfront so Phase 1 and Phase 2 can use the same UUIDs.
        event_id_map: dict[str, uuid.UUID] = {
            e.idempotency_key: uuid.uuid4() for e in events
        }

        # Phase 1: claim idempotency keys in bulk
        idem_values = [
            {"idempotency_key": key, "event_id": eid}
            for key, eid in event_id_map.items()
        ]
        idem_stmt = (
            insert(EventIdempotency)
            .values(idem_values)
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(EventIdempotency.idempotency_key)
        )
        idem_result = await self._session.execute(idem_stmt)
        claimed_keys: set[str] = {row[0] for row in idem_result}
        duplicate_count = len(events) - len(claimed_keys)

        if not claimed_keys:
            logger.info(
                "batch_insert_all_duplicates",
                total=len(events),
                duplicates=duplicate_count,
            )
            return [], duplicate_count

        # Phase 2: bulk insert raw_events for newly claimed keys only
        new_events = [e for e in events if e.idempotency_key in claimed_keys]
        raw_values = [
            {
                "id": event_id_map[e.idempotency_key],
                "event_type": e.event_type,
                "merchant_id": e.merchant_id,
                "amount_cents": e.amount_cents,
                "currency": e.currency,
                "idempotency_key": e.idempotency_key,
                "occurred_at": occurred_at,
                "client_timestamp": e.client_timestamp,
                "metadata_": e.metadata,
            }
            for e in new_events
        ]
        raw_stmt = insert(RawEvent).values(raw_values).returning(RawEvent)
        raw_result = await self._session.execute(raw_stmt)
        inserted = list(raw_result.scalars().all())

        logger.info(
            "batch_insert_complete",
            total=len(events),
            inserted=len(inserted),
            duplicates=duplicate_count,
        )
        return inserted, duplicate_count

    async def get_events_after(
        self,
        last_event_id: uuid.UUID | None,
        limit: int = 1000,
    ) -> list[RawEvent]:
        """
        Fetch events for rollup worker replay.
        If last_event_id is None, returns the oldest unprocessed events.
        """
        if last_event_id is None:
            stmt = select(RawEvent).order_by(RawEvent.created_at.asc()).limit(limit)
        else:
            # Use created_at + id for stable, index-friendly ordering.
            subq = select(RawEvent.created_at).where(RawEvent.id == last_event_id).scalar_subquery()
            stmt = (
                select(RawEvent)
                .where(RawEvent.created_at > subq)
                .order_by(RawEvent.created_at.asc(), RawEvent.id.asc())
                .limit(limit)
            )

        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def notify_new_event(self, event_id: str) -> None:
        """Send NOTIFY for Postgres LISTEN-based consumers (best-effort, at-most-once)."""
        await self._session.execute(
            text("SELECT pg_notify('new_events', :payload)"),
            {"payload": event_id},
        )

    async def missing_merchant_ids(self, merchant_ids: set[uuid.UUID]) -> set[uuid.UUID]:
        """
        Return the subset of merchant_ids that do NOT exist in the merchants table.

        Uses a single WHERE id = ANY(:ids) query regardless of set size — O(1) round-trips.
        """
        if not merchant_ids:
            return set()
        result = await self._session.execute(
            select(Merchant.id).where(Merchant.id.in_(merchant_ids))
        )
        found = {row[0] for row in result}
        return merchant_ids - found

    async def merchant_exists(self, merchant_id: uuid.UUID) -> bool:
        """Single-merchant existence check (used by the single-event ingest path)."""
        result = await self._session.execute(
            select(Merchant.id).where(Merchant.id == merchant_id)
        )
        return result.first() is not None

    async def get_by_idempotency_keys(self, keys: list[str]) -> dict[str, RawEvent]:
        """
        Bulk-fetch existing events by idempotency key.

        Used by the batch ingest path to populate correct event_ids in the response
        for duplicate events — a single query replaces N individual lookups.
        Returns a mapping of idempotency_key → RawEvent.
        """
        if not keys:
            return {}
        result = await self._session.execute(
            select(RawEvent).where(RawEvent.idempotency_key.in_(keys))
        )
        return {row.idempotency_key: row for row in result.scalars().all()}

    async def get_recent_events(self, limit: int = 20) -> list[RawEvent]:
        """For the live feed dashboard panel."""
        stmt = (
            select(RawEvent)
            .order_by(RawEvent.occurred_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

