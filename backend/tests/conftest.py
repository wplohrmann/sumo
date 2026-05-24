"""Test harness.

The test database is created once per pytest invocation (via a thin sync
script using psycopg, so it doesn't clash with the per-test asyncpg
loops). Each test then gets its own engine + session and the tables are
truncated between tests.
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest_asyncio
import psycopg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.db.base import Base
from app.db import models  # noqa: F401 - register models


def _base_url() -> str:
    return os.environ.get(
        "SUMO_TEST_DATABASE_URL",
        "postgresql+asyncpg://sumo:sumo@127.0.0.1:5432/sumo_test",
    )


def _sync_url() -> str:
    return _base_url().replace("+asyncpg", "+psycopg")


def _db_name() -> str:
    return _base_url().rsplit("/", 1)[-1]


def _admin_dsn() -> str:
    """libpq DSN for the default `postgres` database."""
    # Strip the SQLAlchemy dialect prefix and replace the dbname.
    raw = _base_url().split("://", 1)[1]  # user:pw@host:port/dbname
    user_host, _ = raw.rsplit("/", 1)
    return f"postgresql://{user_host}/postgres"


def pytest_configure(config):
    """Create the test database synchronously before any tests run."""
    name = _db_name()
    with psycopg.connect(_admin_dsn(), autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}"')
        conn.execute(f'CREATE DATABASE "{name}"')

    # Create all tables synchronously too.
    sync_engine_url = _sync_url()
    from sqlalchemy import create_engine

    engine = create_engine(sync_engine_url)
    with engine.begin() as conn:
        Base.metadata.create_all(conn)
    engine.dispose()


def pytest_unconfigure(config):
    name = _db_name()
    try:
        with psycopg.connect(_admin_dsn(), autocommit=True) as conn:
            conn.execute(f'DROP DATABASE IF EXISTS "{name}"')
    except Exception:
        pass


@pytest_asyncio.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(_base_url(), poolclass=NullPool)
    try:
        yield eng
    finally:
        await eng.dispose()


@pytest_asyncio.fixture
async def db(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        yield session

    # Truncate everything between tests.
    async with engine.begin() as conn:
        tables = ",".join(f'"{t.name}"' for t in reversed(Base.metadata.sorted_tables))
        await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
