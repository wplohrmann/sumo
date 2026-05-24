"""Trade endpoint + budget-math invariants."""
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

from app.db.models import Basho, BashoRikishi, Rikishi
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


async def _seed() -> None:
    from tests.conftest import _base_url  # type: ignore

    test_engine = create_async_engine(_base_url())
    riders = [(i, f"R{i}") for i in range(1, 9)]
    async with test_engine.begin() as conn:
        await conn.execute(insert(Basho).values(id="202405", name="Tokyo"))
        for rid, name in riders:
            await conn.execute(insert(Rikishi).values(id=rid, name=name))
        for rid, _name in riders:
            await conn.execute(
                insert(BashoRikishi).values(
                    basho_id="202405",
                    rikishi_id=rid,
                    rank=f"M{rid}",
                    rank_value=rid,
                    division="Makuuchi",
                )
            )
    await test_engine.dispose()


async def _setup(client: httpx.AsyncClient):
    await _seed()
    await client.post("/api/auth/login", json={"token": "change-me"})
    r = await client.post(
        "/api/tournaments",
        json={"basho_id": "202405", "name": "May", "budget_pence": 5500},
    )
    tid = r.json()["id"]
    a = (
        await client.post(
            f"/api/tournaments/{tid}/participants",
            json={"display_name": "Alice"},
        )
    ).json()["user_id"]
    return tid, a


@pytest.mark.asyncio
async def test_trade_half_value_and_budget_math(client):
    tid, alice = await _setup(client)
    # Prices.
    for rid, p in [(1, 2000), (2, 1500), (3, 1000), (4, 1000), (5, 2500), (6, 800)]:
        await client.put(
            f"/api/tournaments/{tid}/rikishi/{rid}/price", json={"price_pence": p}
        )
    # Alice drafts 1,2,3,4 = £55 exact.
    for rid in (1, 2, 3, 4):
        r = await client.post(
            f"/api/tournaments/{tid}/picks",
            json={"participant_user_id": alice, "rikishi_id": rid},
        )
        assert r.status_code == 201, r.text

    # Activate the tournament; trade in window.
    await client.patch(f"/api/tournaments/{tid}/status", json={"status": "active"})

    # Find rikishi 1's roster entry.
    picks = (await client.get(f"/api/tournaments/{tid}/picks")).json()
    alice_roster = picks["rosters"][0]
    entry1 = next(e for e in alice_roster["entries"] if e["rikishi_id"] == 1)

    # Trade rikishi 1 (£20, sale £10) for rikishi 5 (£25). Remaining was £0;
    # after trade effective spent = £55 - £10 + £25 = £70. Over budget — reject.
    r = await client.post(
        f"/api/tournaments/{tid}/trades",
        json={
            "participant_user_id": alice,
            "sell_entry_id": entry1["id"],
            "buy_rikishi_id": 5,
            "effective_before_day": 3,
        },
    )
    assert r.status_code == 409
    assert "over budget" in r.json()["detail"]

    # Trade rikishi 1 (£20, refund £10) for rikishi 6 (£8). New spent = £55 - £10 + £8 = £53.
    r = await client.post(
        f"/api/tournaments/{tid}/trades",
        json={
            "participant_user_id": alice,
            "sell_entry_id": entry1["id"],
            "buy_rikishi_id": 6,
            "effective_before_day": 3,
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["sale_price_pence"] == 1000  # half of £20
    assert body["purchase_price_pence"] == 800

    # Roster should now show R6 in slot, R1 gone.
    board = (await client.get(f"/api/tournaments/{tid}/picks")).json()
    alice = board["rosters"][0]
    rikishi_ids = {e["rikishi_id"] for e in alice["entries"]}
    assert rikishi_ids == {2, 3, 4, 6}
    assert alice["spent_pence"] == 5500 - 1000 + 800
    assert alice["remaining_pence"] == 5500 - (5500 - 1000 + 800)

    # Same trade twice should fail (entry is already released).
    r = await client.post(
        f"/api/tournaments/{tid}/trades",
        json={
            "participant_user_id": alice["user_id"],
            "sell_entry_id": entry1["id"],
            "buy_rikishi_id": 5,
            "effective_before_day": 5,
        },
    )
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_trade_logs_and_lists(client):
    tid, alice = await _setup(client)
    for rid, p in [(1, 1000), (2, 1000), (3, 1000), (4, 1000), (5, 500)]:
        await client.put(
            f"/api/tournaments/{tid}/rikishi/{rid}/price", json={"price_pence": p}
        )
    for rid in (1, 2, 3, 4):
        await client.post(
            f"/api/tournaments/{tid}/picks",
            json={"participant_user_id": alice, "rikishi_id": rid},
        )
    await client.patch(f"/api/tournaments/{tid}/status", json={"status": "active"})

    picks = (await client.get(f"/api/tournaments/{tid}/picks")).json()
    entry1 = next(
        e for e in picks["rosters"][0]["entries"] if e["rikishi_id"] == 1
    )
    r = await client.post(
        f"/api/tournaments/{tid}/trades",
        json={
            "participant_user_id": alice,
            "sell_entry_id": entry1["id"],
            "buy_rikishi_id": 5,
            "effective_before_day": 5,
            "note": "regret",
        },
    )
    assert r.status_code == 201

    log = (await client.get(f"/api/tournaments/{tid}/trades")).json()
    assert len(log) == 1
    assert log[0]["sold_rikishi_id"] == 1
    assert log[0]["bought_rikishi_id"] == 5
    assert log[0]["effective_before_day"] == 5
    assert log[0]["note"] == "regret"
