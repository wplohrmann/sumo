"""End-to-end tests for pricing + draft picks."""
from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

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


async def _seed_basho(rikishi: list[tuple[int, str, int]]) -> None:
    from tests.conftest import _base_url  # type: ignore

    test_engine = create_async_engine(_base_url())
    async with test_engine.begin() as conn:
        await conn.execute(insert(Basho).values(id="202405", name="Tokyo"))
        for rid, name, rank_value in rikishi:
            await conn.execute(insert(Rikishi).values(id=rid, name=name))
            await conn.execute(
                insert(BashoRikishi).values(
                    basho_id="202405",
                    rikishi_id=rid,
                    rank=f"Maegashira {rank_value}",
                    rank_value=rank_value,
                    division="Makuuchi",
                )
            )
    await test_engine.dispose()


async def _bootstrap(
    client: httpx.AsyncClient, participants: list[str] | None = None
) -> tuple[str, list[str]]:
    """Log in admin, create tournament, add N participants (default 2)."""
    await client.post("/api/auth/login", json={"token": "change-me"})
    r = await client.post("/api/tournaments", json={"basho_id": "202405"})
    assert r.status_code == 201
    tid = r.json()["id"]

    names = participants or ["Alice", "Bob"]
    user_ids: list[str] = []
    for name in names:
        p = await client.post(
            f"/api/tournaments/{tid}/participants",
            json={"display_name": name},
        )
        user_ids.append(p.json()["user_id"])
    return tid, user_ids


@pytest.mark.asyncio
async def test_list_rikishi_and_pricing(client):
    await _seed_basho([(1, "Terunofuji", 1), (2, "Hoshoryu", 2)])
    tid, _ = await _bootstrap(client)

    # Default prices come from rank: "Maegashira 1" → £17, "Maegashira 2" → £16.
    r = await client.get(f"/api/tournaments/{tid}/rikishi")
    assert r.status_code == 200
    by_id = {row["rikishi_id"]: row for row in r.json()}
    assert by_id[1]["price_pence"] == 1700
    assert by_id[2]["price_pence"] == 1600

    # Admin override still wins.
    await client.put(
        f"/api/tournaments/{tid}/rikishi/1/price", json={"price_pence": 2500}
    )
    await client.put(
        f"/api/tournaments/{tid}/rikishi/2/price", json={"price_pence": 2000}
    )

    r = await client.get(f"/api/tournaments/{tid}/rikishi")
    by_id = {row["rikishi_id"]: row for row in r.json()}
    assert by_id[1]["price_pence"] == 2500
    assert by_id[2]["price_pence"] == 2000


@pytest.mark.asyncio
async def test_pick_creation_and_constraints(client):
    await _seed_basho(
        [
            (1, "Terunofuji", 1),
            (2, "Hoshoryu", 2),
            (3, "Onosato", 3),
            (4, "Kotonowaka", 4),
            (5, "Wakatakakage", 5),
        ]
    )
    tid, [alice, bob] = await _bootstrap(client)

    # Price everyone.
    prices = {1: 2000, 2: 1500, 3: 1500, 4: 1000, 5: 1200}
    for rid, p in prices.items():
        await client.put(
            f"/api/tournaments/{tid}/rikishi/{rid}/price", json={"price_pence": p}
        )

    # Alice drafts 4 rikishi: 2000 + 1500 + 1000 + 1200 = 5700 — over budget on
    # last one. Verify the 4th rejects.
    for rid in (1, 2, 4):
        r = await client.post(
            f"/api/tournaments/{tid}/picks",
            json={"participant_user_id": alice, "rikishi_id": rid},
        )
        assert r.status_code == 201, r.text

    # Try to pick rikishi 5 (£12) — Alice has spent £45, would be £57 > £55.
    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={"participant_user_id": alice, "rikishi_id": 5},
    )
    assert r.status_code == 409
    assert "over budget" in r.json()["detail"]

    # Pick rikishi 3 (£15) — Alice has spent £45, would be £60. Even worse.
    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={"participant_user_id": alice, "rikishi_id": 3},
    )
    assert r.status_code == 409

    # Bob CAN share a rikishi with Alice — sharing rules allow up to 2 owners
    # before trading opens, so picking rikishi 1 lands without a warning.
    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={"participant_user_id": bob, "rikishi_id": 1},
    )
    assert r.status_code == 201, r.text

    # Bob picks his other 4.
    for rid in (3, 5):
        r = await client.post(
            f"/api/tournaments/{tid}/picks",
            json={"participant_user_id": bob, "rikishi_id": rid},
        )
        assert r.status_code == 201

    # Roster board snapshot.
    r = await client.get(f"/api/tournaments/{tid}/picks")
    board = r.json()
    by_name = {row["display_name"]: row for row in board["rosters"]}
    assert by_name["Alice"]["spent_pence"] == 2000 + 1500 + 1000
    assert by_name["Alice"]["remaining_pence"] == 5500 - (2000 + 1500 + 1000)
    assert len(by_name["Alice"]["entries"]) == 3
    # Bob shares rikishi 1 with Alice, plus owns 3 and 5.
    assert by_name["Bob"]["spent_pence"] == 2000 + 1500 + 1200
    # Sharing 1 rikishi between Alice/Bob with cap 2 → no warnings.
    assert board["warnings"] == []


@pytest.mark.asyncio
async def test_sharing_rules_warn_and_can_be_forced(client):
    await _seed_basho(
        [
            (1, "R1", 10),
            (2, "R2", 11),
            (3, "R3", 12),
            (4, "R4", 13),
        ]
    )
    tid, [alice, bob, carol] = await _bootstrap(client, ["Alice", "Bob", "Carol"])
    # Cap everyone's prices low so the cheap budget covers shared picks.
    for rid in (1, 2, 3, 4):
        await client.put(
            f"/api/tournaments/{tid}/rikishi/{rid}/price",
            json={"price_pence": 500},
        )

    # Alice and Bob both pick rikishi 1 — fine (2 owners ≤ cap of 2).
    for who in (alice, bob):
        r = await client.post(
            f"/api/tournaments/{tid}/picks",
            json={"participant_user_id": who, "rikishi_id": 1},
        )
        assert r.status_code == 201, r.text

    # Carol tries to pick rikishi 1 — 3rd owner during drafting → warning.
    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={"participant_user_id": carol, "rikishi_id": 1},
    )
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["code"] == "sharing_warning"
    assert any(w["code"] == "rikishi_over_capped" for w in detail["warnings"])

    # Forcing it succeeds, and the warning surfaces on the roster board.
    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={
            "participant_user_id": carol,
            "rikishi_id": 1,
            "force": True,
        },
    )
    assert r.status_code == 201, r.text
    board = (await client.get(f"/api/tournaments/{tid}/picks")).json()
    assert any(w["code"] == "rikishi_over_capped" for w in board["warnings"])

    # Pair-overlap rule: Alice and Bob already share R1. Pushing the pair
    # overlap above 2 requires sharing 3 rikishi between them.
    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={"participant_user_id": alice, "rikishi_id": 2},
    )
    assert r.status_code == 201
    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={"participant_user_id": bob, "rikishi_id": 2},
    )
    assert r.status_code == 201  # pair overlap goes to 2 = cap, no warning yet

    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={"participant_user_id": alice, "rikishi_id": 3},
    )
    assert r.status_code == 201
    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={"participant_user_id": bob, "rikishi_id": 3},
    )
    assert r.status_code == 409
    codes = {w["code"] for w in r.json()["detail"]["warnings"]}
    assert "pair_overlap_over_capped" in codes


@pytest.mark.asyncio
async def test_pick_locked_after_active(client):
    await _seed_basho([(1, "Terunofuji", 1)])
    tid, [alice, _] = await _bootstrap(client)
    await client.put(
        f"/api/tournaments/{tid}/rikishi/1/price", json={"price_pence": 1000}
    )
    await client.patch(
        f"/api/tournaments/{tid}/status", json={"status": "active"}
    )
    r = await client.post(
        f"/api/tournaments/{tid}/picks",
        json={"participant_user_id": alice, "rikishi_id": 1},
    )
    assert r.status_code == 409
    assert "draft is closed" in r.json()["detail"]
