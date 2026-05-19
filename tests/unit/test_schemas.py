"""
Unit tests for event schema validation and idempotency logic.
"""
from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from app.models.schemas import EventIngest, EventType


class TestEventIngestSchema:
    def _valid_payload(self, **overrides) -> dict:
        base = {
            "event_type": "PAYMENT_CONFIRMED",
            "merchant_id": str(uuid.uuid4()),
            "amount_cents": 5000,
            "currency": "EUR",
            "idempotency_key": str(uuid.uuid4()),
        }
        return {**base, **overrides}

    def test_valid_event_parses(self):
        payload = self._valid_payload()
        event = EventIngest(**payload)
        assert event.event_type == EventType.PAYMENT_CONFIRMED
        assert event.amount_cents == 5000

    def test_currency_normalised_to_uppercase(self):
        event = EventIngest(**self._valid_payload(currency="eur"))
        assert event.currency == "EUR"

    def test_amount_zero_is_valid(self):
        event = EventIngest(**self._valid_payload(amount_cents=0))
        assert event.amount_cents == 0

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError):
            EventIngest(**self._valid_payload(amount_cents=-1))

    def test_unknown_event_type_rejected(self):
        with pytest.raises(ValidationError):
            EventIngest(**self._valid_payload(event_type="PAYMENT_REVERSED"))

    def test_empty_idempotency_key_rejected(self):
        with pytest.raises(ValidationError):
            EventIngest(**self._valid_payload(idempotency_key=""))

    def test_invalid_merchant_uuid_rejected(self):
        with pytest.raises(ValidationError):
            EventIngest(**self._valid_payload(merchant_id="not-a-uuid"))

    def test_metadata_is_optional(self):
        event = EventIngest(**self._valid_payload())
        assert event.metadata is None

    def test_metadata_accepts_nested_dict(self):
        event = EventIngest(**self._valid_payload(metadata={"source": "web", "version": 2}))
        assert event.metadata == {"source": "web", "version": 2}

    def test_client_timestamp_optional(self):
        event = EventIngest(**self._valid_payload())
        assert event.client_timestamp is None

    def test_all_event_types_accepted(self):
        for et in EventType:
            event = EventIngest(**self._valid_payload(event_type=et))
            assert event.event_type == et


class TestBatchIngestSchema:
    from app.models.schemas import BatchEventIngest

    def test_empty_batch_rejected(self):
        with pytest.raises(ValidationError):
            from app.models.schemas import BatchEventIngest
            BatchEventIngest(events=[])

    def test_batch_over_500_rejected(self):
        with pytest.raises(ValidationError):
            from app.models.schemas import BatchEventIngest
            events = [
                {
                    "event_type": "PAYMENT_CONFIRMED",
                    "merchant_id": str(uuid.uuid4()),
                    "amount_cents": 100,
                    "idempotency_key": str(uuid.uuid4()),
                }
                for _ in range(501)
            ]
            BatchEventIngest(events=events)
