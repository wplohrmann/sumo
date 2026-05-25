"""Tests for the basho-listing endpoint and its date helper."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.api.bashos import recent_basho_ids
from app.db.models import Basho
from app.db.session import get_session
from app.main import app


def test_recent_basho_ids_from_odd_month():
    ids = recent_basho_ids(date(2026, 5, 25))
    assert ids == [
        "202605",
        "202603",
        "202601",
        "202511",
        "202509",
        "202507",
        "202505",
        "202503",
        "202501",
        "202411",
    ]


def test_recent_basho_ids_from_even_month_rounds_down():
    # April 2026 → most recent basho is March 2026.
    ids = recent_basho_ids(date(2026, 4, 10))
    assert ids[0] == "202603"
    assert ids[1] == "202601"
    assert ids[2] == "202511"


def test_recent_basho_ids_year_wrap():
    # February 2024 → previous odd month is Jan 2024, then wrap into 2023.
    ids = recent_basho_ids(date(2024, 2, 1), count=4)
    assert ids == ["202401", "202311", "202309", "202307"]


@pytest_asyncio.fixture
async def client(
    engine: AsyncEngine, db: AsyncSession
) -> AsyncIterator[httpx.AsyncClient]:
    Session = async_sessionmaker(engine, expire_on_commit=False)

    async def _override():
        async with Session() as s:
            yield s

    app.dependency_overrides[get_session] = _override
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_recent_bashos_endpoint_marks_synced(client: httpx.AsyncClient):
    # Seed one basho that we expect to appear in the recent window.
    ids = recent_basho_ids(date.today())
    target = ids[2]  # any id from the window
    from tests.conftest import _base_url  # type: ignore

    eng = create_async_engine(_base_url())
    async with eng.begin() as conn:
        await conn.execute(
            insert(Basho).values(id=target, name="Tokyo")
        )
    await eng.dispose()

    await client.post("/api/auth/login", json={"token": "change-me"})
    r = await client.get("/api/bashos/recent")
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 10
    assert [row["id"] for row in rows] == ids
    by_id = {row["id"]: row for row in rows}
    assert by_id[target]["synced"] is True
    assert by_id[target]["name"] == "Tokyo"
    # All other entries should be unsynced.
    for bid, row in by_id.items():
        if bid == target:
            continue
        assert row["synced"] is False
        assert row["name"] is None


@pytest.mark.asyncio
async def test_recent_bashos_requires_auth(client: httpx.AsyncClient):
    r = await client.get("/api/bashos/recent")
    assert r.status_code in (401, 403)
