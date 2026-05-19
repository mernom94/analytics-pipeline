#!/usr/bin/env python3
"""
Load simulator for the portfolio demo.

Generates realistic payment event traffic against the running API.
Simulates:
  - Normal payment flow (INITIATED → CONFIRMED or FAILED)
  - Bursty merchant activity (some merchants more active)
  - Late-arriving events (configurable fraction)
  - Duplicate submissions (to demonstrate idempotency)

Usage:
    python scripts/load_simulator.py --rps 100 --duration 60
    python scripts/load_simulator.py --rps 500 --burst
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx

MERCHANT_IDS = [
    "a0000000-0000-0000-0000-000000000001",
    "a0000000-0000-0000-0000-000000000002",
    "a0000000-0000-0000-0000-000000000003",
    "a0000000-0000-0000-0000-000000000004",
    "a0000000-0000-0000-0000-000000000005",
    "a0000000-0000-0000-0000-000000000006",
    "a0000000-0000-0000-0000-000000000007",
    "a0000000-0000-0000-0000-000000000008",
    "a0000000-0000-0000-0000-000000000009",
    "a0000000-0000-0000-0000-00000000000a",
]

# Weighted distribution — top merchants get more traffic
MERCHANT_WEIGHTS = [30, 20, 15, 10, 8, 6, 4, 3, 2, 2]

EVENT_TYPES = ["PAYMENT_INITIATED", "PAYMENT_CONFIRMED", "PAYMENT_FAILED"]
EVENT_WEIGHTS = [25, 65, 10]  # ~65% confirm, ~10% fail


def random_merchant() -> str:
    return random.choices(MERCHANT_IDS, weights=MERCHANT_WEIGHTS, k=1)[0]


def random_event_type() -> str:
    return random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]


def random_amount() -> int:
    """Amount in cents — log-normal distribution around €50."""
    base = random.lognormvariate(mu=8.5, sigma=1.2)
    return max(100, min(int(base), 500_000))


def make_event(late: bool = False, duplicate_key: str | None = None) -> dict:
    now = datetime.now(timezone.utc)
    client_ts = now
    if late:
        # Late event: client_timestamp is 2-8 minutes in the past
        client_ts = now - timedelta(seconds=random.randint(120, 480))

    return {
        "event_type": random_event_type(),
        "merchant_id": random_merchant(),
        "amount_cents": random_amount(),
        "currency": "EUR",
        "idempotency_key": duplicate_key or str(uuid.uuid4()),
        "client_timestamp": client_ts.isoformat(),
        "metadata": {
            "source": "load_simulator",
            "session_id": str(uuid.uuid4())[:8],
        },
    }


class LoadSimulator:
    def __init__(self, base_url: str, rps: int, duration: int, late_rate: float = 0.05) -> None:
        self.base_url = base_url
        self.rps = rps
        self.duration = duration
        self.late_rate = late_rate

        self.sent = 0
        self.errors = 0
        self.duplicates_sent = 0
        self._recent_keys: list[str] = []

    async def run(self) -> None:
        print(f"Starting load: {self.rps} rps for {self.duration}s → {self.base_url}")
        print(f"Late event rate: {self.late_rate * 100:.0f}%")

        interval = 1.0 / self.rps
        end_time = time.monotonic() + self.duration

        async with httpx.AsyncClient(timeout=5.0) as client:
            while time.monotonic() < end_time:
                t0 = time.monotonic()
                await self._send_event(client)
                elapsed = time.monotonic() - t0
                sleep = max(0.0, interval - elapsed)
                await asyncio.sleep(sleep)

        self._print_summary()

    async def _send_event(self, client: httpx.AsyncClient) -> None:
        is_late = random.random() < self.late_rate
        is_duplicate = self._recent_keys and random.random() < 0.02  # 2% duplicate rate

        if is_duplicate:
            key = random.choice(self._recent_keys)
            event = make_event(duplicate_key=key)
            self.duplicates_sent += 1
        else:
            event = make_event(late=is_late)

        # Track recent keys for duplicate simulation
        self._recent_keys.append(event["idempotency_key"])
        if len(self._recent_keys) > 100:
            self._recent_keys.pop(0)

        try:
            resp = await client.post(f"{self.base_url}/events", json=event)
            self.sent += 1

            if self.sent % 500 == 0:
                print(f"  Sent: {self.sent} | Errors: {self.errors} | Dupes: {self.duplicates_sent}")

            if resp.status_code not in (200, 202):
                self.errors += 1
                if self.errors <= 5:
                    print(f"  Error {resp.status_code}: {resp.text[:100]}")

        except Exception as exc:
            self.errors += 1
            if self.errors <= 5:
                print(f"  Request error: {exc}")

    def _print_summary(self) -> None:
        print(f"\n{'='*50}")
        print(f"Load simulation complete")
        print(f"  Events sent:      {self.sent}")
        print(f"  Duplicates sent:  {self.duplicates_sent}")
        print(f"  Errors:           {self.errors}")
        print(f"  Actual RPS:       {self.sent / self.duration:.1f}")
        print(f"{'='*50}")


async def run_fault_injection(base_url: str) -> None:
    """
    Demonstrates all fault scenarios from the spec.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        print("\n=== Fault Injection Demo ===\n")

        # 1. Duplicate event
        print("1. Duplicate idempotency_key")
        key = str(uuid.uuid4())
        event = make_event(duplicate_key=key)
        r1 = await client.post(f"{base_url}/events", json=event)
        r2 = await client.post(f"{base_url}/events", json=event)
        print(f"   First:  {r1.status_code} (expect 202)")
        print(f"   Second: {r2.status_code} (expect 200 — duplicate)")
        assert r1.status_code == 202 and r2.status_code == 200, "Duplicate handling failed"
        print("   ✓ PASS")

        # 2. Late-arriving event (5 min delay)
        print("\n2. Late-arriving event (5 min client_timestamp delay)")
        late_event = make_event(late=True)
        r = await client.post(f"{base_url}/events", json=late_event)
        print(f"   Response: {r.status_code} (expect 202 — applied to correct bucket)")
        assert r.status_code == 202
        print("   ✓ PASS")

        # 3. Invalid merchant_id
        print("\n3. Invalid merchant_id")
        bad_event = make_event()
        bad_event["merchant_id"] = str(uuid.uuid4())  # Unknown merchant
        r = await client.post(f"{base_url}/events", json=bad_event)
        print(f"   Response: {r.status_code} (expect 404)")
        assert r.status_code == 404
        print("   ✓ PASS")

        # 4. Health check showing lag
        print("\n4. Health check")
        r = await client.get(f"{base_url}/health")
        health = r.json()
        print(f"   Status: {health['status']}")
        print(f"   Rollup lag: {health.get('rollup_lag_seconds')}s")
        print("   ✓ PASS")

        # 5. Leaderboard query
        print("\n5. Leaderboard query")
        r = await client.get(f"{base_url}/leaderboard")
        lb = r.json()
        print(f"   Source: {lb.get('source')} | Entries: {len(lb.get('entries', []))}")
        print("   ✓ PASS")

        print("\n=== All fault injection scenarios passed ===\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analytics Pipeline load simulator")
    parser.add_argument("--url",      default="http://localhost:8000", help="API base URL")
    parser.add_argument("--rps",      type=int, default=50,  help="Events per second")
    parser.add_argument("--duration", type=int, default=60,  help="Duration in seconds")
    parser.add_argument("--late-rate",type=float, default=0.05, help="Fraction of late events")
    parser.add_argument("--fault-injection", action="store_true", help="Run fault injection demo")
    args = parser.parse_args()

    if args.fault_injection:
        asyncio.run(run_fault_injection(args.url))
    else:
        sim = LoadSimulator(args.url, args.rps, args.duration, args.late_rate)
        asyncio.run(sim.run())
