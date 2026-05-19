"""
app/db/engine.py
────────────────
Engine construction helpers.

OWNERSHIP MODEL
───────────────
The engine is created and disposed exclusively by the FastAPI lifespan
context (app/main.py).  No other code may call create_engine() or
engine.dispose().

Concretely:
  • lifespan startup  → build_engine() + configure_session_factory()
  • lifespan shutdown → engine.dispose()

Helper functions here are pure constructors — they do not maintain module-
level state.  The session factory registry lives in app.db.session_factory.

FASTAPI DEPENDENCY
──────────────────
get_db() is the canonical FastAPI dependency for route handlers.  It pulls
a session from the factory registered at startup, so tests can override it
by calling configure_session_factory() with a test factory before the app
starts.
"""
from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.logging import get_logger

logger = get_logger(__name__)


def build_engine(database_url: str, *, pool_size: int, max_overflow: int,
                 pool_timeout: float, echo: bool) -> AsyncEngine:
    """
    Construct a new AsyncEngine.

    Called once by the lifespan startup hook.  The caller owns the engine
    and is responsible for calling engine.dispose() at shutdown.

    pool_pre_ping=True   — validates connections before checkout; prevents
                           "SSL connection has been closed unexpectedly"
                           errors that show up after idle periods.
    expire_on_commit     — False on the sessionmaker, not the engine, but
                           documented here for traceability.
    """
    engine = create_async_engine(
        database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_pre_ping=True,
        echo=echo,
    )
    logger.info("database_engine_created", pool_size=pool_size)
    return engine


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """
    Construct the application-wide async_sessionmaker bound to *engine*.

    expire_on_commit=False prevents lazy-load errors after commit when
    ORM objects are accessed outside the session context.
    autoflush=False gives explicit control over when SQL is emitted.
    """
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency — yields one short-lived AsyncSession per request.

    Pulls the factory from the central registry (app.db.session_factory).
    Tests override the factory via configure_session_factory() so this
    dependency automatically uses the test pool without any per-route
    override boilerplate.
    """
    from app.db.session_factory import get_session_factory
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise