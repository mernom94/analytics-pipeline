"""
app/db/session_factory.py
─────────────────────────
Centralised session-factory registry.

All DB access paths — HTTP routes, background workers, CLI scripts — must
call get_session_factory() to obtain a session.  Nothing may construct its
own engine or sessionmaker.

Dependency inversion contract
──────────────────────────────
Production:  configure_session_factory() called once during app lifespan
             startup with the real engine's sessionmaker.
Tests:       conftest injects a test-specific factory BEFORE the app starts,
             so every consumer (routes + workers) sees the same pool.
"""
from __future__ import annotations

from typing import Final

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# ── Module-level slot — never accessed directly by callers ───────────────────
_factory: async_sessionmaker[AsyncSession] | None = None

_NOT_CONFIGURED: Final = (
    "Session factory has not been configured. "
    "Call configure_session_factory() during application startup "
    "before any DB access is attempted."
)


def configure_session_factory(factory: async_sessionmaker[AsyncSession]) -> None:
    """
    Register the application-wide session factory.

    Must be called exactly once, during the FastAPI lifespan startup hook
    (or at the top of conftest.py for tests).  Calling it a second time
    replaces the factory — tests may do this intentionally; production code
    must not.
    """
    global _factory
    _factory = factory


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """
    Return the configured session factory.

    Raises RuntimeError when called before configure_session_factory().
    This is a fast-fail guard: misconfigured startup is caught immediately
    on the first DB access rather than as a cryptic pool error later.
    """
    if _factory is None:
        raise RuntimeError(_NOT_CONFIGURED)
    return _factory


def reset_session_factory() -> None:
    """
    Clear the factory reference.

    Intended for test teardown only.  Production code must never call this.
    """
    global _factory
    _factory = None