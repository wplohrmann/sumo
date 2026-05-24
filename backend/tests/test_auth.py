from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from app.db.session import get_session
from app.main import app


@pytest_asyncio.fixture
async def client(
    engine: AsyncEngine, db: AsyncSession
) -> AsyncIterator[httpx.AsyncClient]:
    """Mount the FastAPI app against the test DB."""
    from sqlalchemy.ext.asyncio import async_sessionmaker

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
async def test_admin_login_and_me(client: httpx.AsyncClient):
    # default settings.admin_password = "change-me"
    r = await client.post("/api/auth/login", json={"token": "change-me"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["role"] == "admin"
    assert body["display_name"] == "admin"
    assert "sumo_session" in client.cookies

    me = await client.get("/api/auth/me")
    assert me.status_code == 200
    assert me.json()["role"] == "admin"


@pytest.mark.asyncio
async def test_viewer_token_lifecycle(client: httpx.AsyncClient):
    # Admin logs in, creates a basho row, a tournament, then a viewer.
    await client.post("/api/auth/login", json={"token": "change-me"})

    # Bypass the basho FK by inserting a basho directly via the test DB:
    from sqlalchemy import insert
    from app.db.models import Basho
    from sqlalchemy.ext.asyncio import async_sessionmaker

    # The /api/admin/sync endpoint normally creates basho rows, but here
    # we cheat by inserting one via the dep-overridden session.
    deps = app.dependency_overrides
    # grab the session factory from the override closure
    Session = None
    for v in deps.values():
        # crude — but our override yields from a session factory
        if hasattr(v, "__wrapped__"):
            continue
    # Just use the engine that's running the override; insert via raw client
    # is awkward, so use a direct engine connection.
    from app.db.session import engine  # production engine; tests already override

    # Simpler: insert via the *override* session by calling the endpoint we want.
    # Use the create-tournament endpoint after manually adding a basho through
    # the override.
    # We patch in a basho through a fresh session of the test engine.
    from tests.conftest import _base_url  # type: ignore[attr-defined]
    from sqlalchemy.ext.asyncio import create_async_engine

    test_engine = create_async_engine(_base_url())
    async with test_engine.begin() as conn:
        await conn.execute(insert(Basho).values(id="202405", name="Tokyo"))
    await test_engine.dispose()

    r = await client.post(
        "/api/tournaments",
        json={"basho_id": "202405", "name": "May 2024 league"},
    )
    assert r.status_code == 201, r.text
    tid = r.json()["id"]

    # Add a viewer.
    r = await client.post(
        f"/api/tournaments/{tid}/participants",
        json={"display_name": "Alice"},
    )
    assert r.status_code == 201, r.text
    viewer_token = r.json()["token"]
    assert viewer_token

    # Viewer logs in with their token.
    client.cookies.clear()
    r = await client.post("/api/auth/login", json={"token": viewer_token})
    assert r.status_code == 200, r.text
    assert r.json()["role"] == "viewer"

    # Viewer can list participants but cannot create more.
    r = await client.get(f"/api/tournaments/{tid}/participants")
    assert r.status_code == 200
    assert any(p["display_name"] == "Alice" for p in r.json())

    r = await client.post(
        f"/api/tournaments/{tid}/participants",
        json={"display_name": "Bob"},
    )
    assert r.status_code == 403
