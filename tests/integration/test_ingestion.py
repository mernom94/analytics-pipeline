"""
Integration tests for event ingestion.

Tests the full HTTP → DB path for:
- Single event ingestion (202 Accepted)
- Duplicate idempotency key (200 OK)
- Batch ingestion
- Validation errors (missing merchant, bad payload)
"""
from __future__ import annotations

import uuid

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio(loop_scope="session")
class TestSingleEventIngestion:
    async def test_valid_event_returns_202(self, client: AsyncClient, valid_event: dict):
        resp = await client.post("/events", json=valid_event)
        assert resp.status_code == 202
        body = resp.json()
        assert "event_id" in body
        assert body["duplicate"] is False
        assert body["idempotency_key"] == valid_event["idempotency_key"]

    async def test_duplicate_event_returns_200(self, client: AsyncClient, valid_event: dict):
        r1 = await client.post("/events", json=valid_event)
        assert r1.status_code == 202

        r2 = await client.post("/events", json=valid_event)
        assert r2.status_code == 200
        assert r2.json()["duplicate"] is True

    async def test_duplicate_does_not_change_event_id(self, client: AsyncClient, valid_event: dict):
        r1 = await client.post("/events", json=valid_event)
        r2 = await client.post("/events", json=valid_event)
        assert r1.json()["event_id"] == r2.json()["event_id"]

    async def test_unknown_merchant_returns_404(self, client: AsyncClient, valid_event: dict):
        valid_event["merchant_id"] = str(uuid.uuid4())
        resp = await client.post("/events", json=valid_event)
        assert resp.status_code == 404

    async def test_negative_amount_returns_422(self, client: AsyncClient, valid_event: dict):
        valid_event["amount_cents"] = -1
        resp = await client.post("/events", json=valid_event)
        assert resp.status_code == 422

    async def test_missing_idempotency_key_returns_422(self, client: AsyncClient, valid_event: dict):
        del valid_event["idempotency_key"]
        resp = await client.post("/events", json=valid_event)
        assert resp.status_code == 422

    async def test_response_contains_occurred_at(self, client: AsyncClient, valid_event: dict):
        resp = await client.post("/events", json=valid_event)
        assert "occurred_at" in resp.json()

    async def test_multiple_different_keys_all_accepted(self, client: AsyncClient, valid_event: dict):
        for _ in range(5):
            event = {**valid_event, "idempotency_key": str(uuid.uuid4())}
            resp = await client.post("/events", json=event)
            assert resp.status_code == 202


@pytest.mark.asyncio(loop_scope="session")
class TestBatchIngestion:
    async def test_batch_of_five_returns_202(self, client: AsyncClient, valid_event: dict):
        events = [
            {**valid_event, "idempotency_key": str(uuid.uuid4())}
            for _ in range(5)
        ]
        resp = await client.post("/events/batch", json={"events": events})
        assert resp.status_code == 202
        body = resp.json()
        assert body["accepted"] == 5
        assert body["duplicates"] == 0

    async def test_batch_with_duplicates_reports_count(self, client: AsyncClient, valid_event: dict):
        key = str(uuid.uuid4())
        # Send first time
        await client.post("/events", json={**valid_event, "idempotency_key": key})

        # Batch including the duplicate
        events = [
            {**valid_event, "idempotency_key": str(uuid.uuid4())},
            {**valid_event, "idempotency_key": key},  # duplicate
        ]
        resp = await client.post("/events/batch", json={"events": events})
        assert resp.status_code == 202
        body = resp.json()
        assert body["accepted"] == 1
        assert body["duplicates"] == 1

    async def test_empty_batch_returns_422(self, client: AsyncClient):
        resp = await client.post("/events/batch", json={"events": []})
        assert resp.status_code == 422

    async def test_batch_over_500_returns_422(self, client: AsyncClient, valid_event: dict):
        events = [
            {**valid_event, "idempotency_key": str(uuid.uuid4())}
            for _ in range(501)
        ]
        resp = await client.post("/events/batch", json={"events": events})
        assert resp.status_code == 422
