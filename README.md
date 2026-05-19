# Real-Time Analytics Pipeline

> **Performance at scale** — Pre-aggregated rollups · Redis sorted sets · Partitioned event log · Backpressure handling

Part of a three-project portfolio demonstrating distinct backend engineering problem classes:

| Project | Problem class | Key primitive |
|---|---|---|
| **Analytics Pipeline** | Performance at scale | Pre-aggregated rollups |
| Payment Orchestrator | Correctness under failure | Transactional outbox |
| Payment Scheduler | Distributed coordination | Leader election |

---

## The Core Problem

A fintech dashboard needs to show a merchant:
- Total payment volume in the last 24 hours
- Success rate over the last 7 days
- Per-minute transaction count (live sparkline)
- Top 10 merchants by volume this month (leaderboard)

At 10,000 events/minute, `COUNT(*) WHERE merchant_id = X AND created_at > NOW() - INTERVAL '24 hours'` takes **4 seconds**. The same query against a pre-aggregated rollup takes **2ms**.

## The Solution

**Pre-aggregate on the write path so reads are always O(1) against a bounded dataset.**

```
Write path:  POST /events → INSERT raw_events → NOTIFY → rollup_worker
                                                           ↓
                                            rollup_minute (per merchant, per minute)
                                            rollup_hour   (per merchant, per hour)
                                            rollup_day    (per merchant, per day)
                                            Redis ZINCRBY (leaderboard sorted set)

Read path:   GET /metrics → rollup_minute/hour/day (never raw_events)
             GET /leaderboard → Redis ZREVRANGE O(log N)
```

## Architecture

```
┌─────────────────────────────────────────────┐
│              FastAPI (HTTP)                  │
│  POST /events    GET /metrics    GET /lb     │
└──────────────────────┬──────────────────────┘
                       │
         ┌─────────────▼────────────┐
         │      raw_events          │  append-only, partitioned by day
         │      (PostgreSQL)        │
         └─────────────┬────────────┘
                       │  LISTEN/NOTIFY
         ┌─────────────▼────────────┐
         │      Rollup Worker       │  consumes from consumer_offsets
         │  minute · hour · day     │
         └──────┬───────────────────┘
                │              │
    ┌───────────▼────┐   ┌─────▼──────────┐
    │  rollup tables  │   │  Redis          │
    │  (PostgreSQL)   │   │  sorted sets    │
    └────────────────┘   └────────────────┘
```

## Quick Start

```bash
# Start all services
docker-compose up -d

# Verify health
curl http://localhost:8000/health

# Run the load simulator (50 rps for 60s)
python scripts/load_simulator.py --rps 50 --duration 60

# Open dashboard
open http://localhost:3000

# Run fault injection demo
python scripts/load_simulator.py --fault-injection
```

## Key Design Decisions

### 1. Idempotent ingestion (`UNIQUE idempotency_key`)

Every `POST /events` requires a client-supplied `idempotency_key`. Duplicates are silently deduplicated at the DB level:

```sql
INSERT INTO raw_events (..., idempotency_key)
VALUES (...)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id
```

Returns `202` for new events, `200` for duplicates — client always knows which case occurred.

### 2. Pre-aggregation on write path

The rollup worker maintains three granularities simultaneously:

```sql
INSERT INTO rollup_minute (merchant_id, bucket, event_type, count, amount_sum_cents, ...)
VALUES ($1, date_trunc('minute', $2), $3, 1, $4, ...)
ON CONFLICT (merchant_id, bucket, event_type) DO UPDATE
SET count            = rollup_minute.count + EXCLUDED.count,
    amount_sum_cents = rollup_minute.amount_sum_cents + EXCLUDED.amount_sum_cents,
    ...
```

The upsert is idempotent under replay — safe to re-process any event.

### 3. Redis sorted sets for O(log N) leaderboard

```python
await redis.zincrby(f"leaderboard:volume:{period}", amount_cents, merchant_id)
# ZREVRANGE is O(log N + K) — stays fast regardless of merchant count
```

Redis is the hot-path approximation. If flushed, leaderboard rebuilds from `rollup_day` on next request.

### 4. Late-arriving events bucketed by `client_timestamp`

Events bucket against `client_timestamp` (not server `occurred_at`), so a 5-minute-late event lands in the correct historical window:

```python
def bucket_ts(event):
    return event.client_timestamp or event.occurred_at
```

### 5. Crash-safe consumer with `consumer_offsets`

```sql
-- On startup/crash recovery:
SELECT * FROM raw_events
WHERE created_at > (SELECT last_event_at FROM consumer_offsets WHERE consumer_id = 'rollup-worker-1')
ORDER BY created_at ASC LIMIT 1000
```

`raw_events` is the durable buffer. A crashed worker replays from its last committed offset. No events lost.

### 6. Query routing by range

```
Range ≤ 3h   → rollup_minute   TTL: 5s
Range ≤ 30d  → rollup_hour     TTL: 60s
Range > 30d  → rollup_day      TTL: 300s
```

The read path never touches `raw_events`.

## Failure Modes

| Failure | Detection | Recovery |
|---|---|---|
| Worker crash | `consumer_offsets.last_event_at` stale | Replay from last offset; idempotent upserts |
| Redis flush | Cache miss on leaderboard key | Rebuild from `rollup_day` |
| Duplicate event | `UNIQUE(idempotency_key)` | `ON CONFLICT DO NOTHING` |
| Late event | `client_timestamp` delta | Applied to correct historical bucket |
| Consumer lag | `/health` rollup_lag_seconds | Batch mode (1000 events/tx) |
| Partition missing | Ingestion fails | Nightly partition pre-creation |

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/events` | Ingest a single event |
| `POST` | `/events/batch` | Ingest up to 500 events |
| `GET` | `/metrics/{merchant_id}` | Volume, count, success rate |
| `GET` | `/metrics/{merchant_id}/sparkline` | Per-minute live sparkline |
| `GET` | `/leaderboard` | Top-N merchants by volume |
| `GET` | `/health` | Consumer lag, rollup staleness, Redis status |
| `GET` | `/health/ready` | k8s readiness probe |
| `GET` | `/health/live` | k8s liveness probe |

## Project Structure

```
analytics-pipeline/
├── app/
│   ├── api/routes/         # FastAPI route handlers
│   │   ├── events.py       # POST /events, POST /events/batch
│   │   ├── metrics.py      # GET /metrics/{id}
│   │   ├── leaderboard.py  # GET /leaderboard
│   │   └── health.py       # GET /health
│   ├── core/
│   │   ├── config.py       # Settings (pydantic-settings)
│   │   ├── exceptions.py   # Domain exceptions
│   │   └── logging.py      # Structured logging (structlog)
│   ├── db/
│   │   ├── engine.py       # SQLAlchemy async engine
│   │   └── redis.py        # Redis connection pool
│   ├── models/
│   │   ├── orm.py          # SQLAlchemy table definitions
│   │   └── schemas.py      # Pydantic request/response schemas
│   ├── repositories/
│   │   ├── event_repository.py   # raw_events CRUD
│   │   ├── rollup_repository.py  # rollup upserts + queries
│   │   └── redis_repository.py   # leaderboard + cache
│   ├── services/
│   │   ├── ingestion_service.py  # Event ingestion logic
│   │   └── query_service.py      # Metric routing + cache-aside
│   ├── workers/
│   │   └── rollup_worker.py      # Consumer: raw_events → rollups
│   └── main.py                   # FastAPI app + lifespan
├── migrations/                   # Alembic migrations
├── scripts/
│   ├── create_partitions.py      # Daily partition pre-creation
│   ├── nightly_compaction.py     # Day rollup reconciliation + snapshot
│   └── load_simulator.py         # Traffic generator + fault injection
├── tests/
│   ├── unit/                     # Rollup logic, schema validation
│   └── integration/              # Full HTTP → DB tests
├── dashboard/
│   └── index.html                # Portfolio demo dashboard
└── docker-compose.yml
```

## Running Tests

```bash
# Unit tests (no infrastructure required)
pytest tests/unit/ -v

# Integration tests (requires docker-compose up postgres redis)
pytest tests/integration/ -v

# Full suite
pytest -v
```

## Stack

- **FastAPI** — async HTTP framework
- **PostgreSQL 16** — append-only event log (partitioned), rollup tables, consumer offsets
- **Redis 7** — leaderboard sorted sets, metric query cache
- **SQLAlchemy 2** — async ORM with `asyncpg`
- **Alembic** — database migrations
- **structlog** — structured JSON logging
- **asyncpg** — Postgres LISTEN/NOTIFY for worker wake-up
