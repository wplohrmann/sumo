"""Tests for tournament status transitions, including unarchiving."""
from __future__ import annotations

from collections.abc import AsyncIterator

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

from app.db.models import Basho
from app.db.session import get_session
from app.main import app


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


async def _seed_bashos(ids: list[str]) -> None:
    from tests.conftest import _base_url  # type: ignore

    test_engine = create_async_engine(_base_url())
    async with test_engine.begin() as conn:
        for bid in ids:
            await conn.execute(insert(Basho).values(id=bid, name=f"Basho {bid}"))
    await test_engine.dispose()


@pytest.mark.asyncio
async def test_archive_then_unarchive_round_trip(client):
    await _seed_bashos(["202405"])
    await client.post("/api/auth/login", json={"token": "change-me"})

    r = await client.post("/api/tournaments", json={"basho_id": "202405"})
    assert r.status_code == 201
    tid = r.json()["id"]

    r = await client.patch(
        f"/api/tournaments/{tid}/status", json={"status": "archived"}
    )
    assert r.status_code == 200

    assert (await client.get("/api/tournaments/current")).json() is None

    r = await client.patch(
        f"/api/tournaments/{tid}/status", json={"status": "active"}
    )
    assert r.status_code == 200
    assert r.json()["status"] == "active"

    current = (await client.get("/api/tournaments/current")).json()
    assert current is not None and current["id"] == tid


@pytest.mark.asyncio
async def test_cannot_create_second_tournament_for_same_basho(client):
    """A given basho hosts at most one tournament — even if the existing one
    is archived, you unarchive it rather than spawning a duplicate."""
    await _seed_bashos(["202405"])
    await client.post("/api/auth/login", json={"token": "change-me"})

    first = (
        await client.post("/api/tournaments", json={"basho_id": "202405"})
    ).json()
    await client.patch(
        f"/api/tournaments/{first['id']}/status", json={"status": "archived"}
    )

    r = await client.post("/api/tournaments", json={"basho_id": "202405"})
    assert r.status_code == 409
    assert "already exists" in r.json()["detail"]


@pytest.mark.asyncio
async def test_can_re_add_participant_from_archived_tournament(client):
    """A viewer with a display_name that already exists on an archived
    tournament can be added to a new tournament. They get a fresh token; the
    same name twice within one tournament still 409s."""
    await _seed_bashos(["202405", "202407"])
    await client.post("/api/auth/login", json={"token": "change-me"})

    first = (
        await client.post("/api/tournaments", json={"basho_id": "202405"})
    ).json()
    r = await client.post(
        f"/api/tournaments/{first['id']}/participants",
        json={"display_name": "Dan"},
    )
    assert r.status_code == 201
    first_token = r.json()["token"]
    first_user_id = r.json()["user_id"]

    # Adding "Dan" again to the same tournament must fail loudly.
    r = await client.post(
        f"/api/tournaments/{first['id']}/participants",
        json={"display_name": "Dan"},
    )
    assert r.status_code == 409

    await client.patch(
        f"/api/tournaments/{first['id']}/status", json={"status": "archived"}
    )

    second = (
        await client.post("/api/tournaments", json={"basho_id": "202407"})
    ).json()
    r = await client.post(
        f"/api/tournaments/{second['id']}/participants",
        json={"display_name": "Dan"},
    )
    assert r.status_code == 201, r.text
    body = r.json()
    # Same person, same AppUser id — but a freshly rotated token.
    assert body["user_id"] == first_user_id
    assert body["token"] != first_token


@pytest.mark.asyncio
async def test_cannot_unarchive_when_another_is_current(client):
    await _seed_bashos(["202405", "202407"])
    await client.post("/api/auth/login", json={"token": "change-me"})

    first = (
        await client.post("/api/tournaments", json={"basho_id": "202405"})
    ).json()
    await client.patch(
        f"/api/tournaments/{first['id']}/status", json={"status": "archived"}
    )

    second = (
        await client.post("/api/tournaments", json={"basho_id": "202407"})
    ).json()
    assert second["status"] == "setup"

    r = await client.patch(
        f"/api/tournaments/{first['id']}/status", json={"status": "active"}
    )
    assert r.status_code == 409
