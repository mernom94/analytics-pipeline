#!/bin/bash
alembic upgrade head
python -m app.workers.rollup_worker &
uvicorn app.main:app --host 0.0.0.0 --port $PORT