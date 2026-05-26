"""Smoke test for the async sumo-api sync against canned JSON responses."""
from __future__ import annotations

import httpx
import pytest

from sqlalchemy import select

from app.db.models import Basho, BashoRikishi, Match, Measurement, Rikishi
from app.sync.sumo_api import (
    SumoApiClient,
    sync_banzuke,
    sync_basho,
    sync_matches_for_day,
    sync_measurements,
)


def _mock_transport(routes: dict[str, dict]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        # routes keys are paths under /api
        key = path.removeprefix("/api")
        if key in routes:
            return httpx.Response(200, json=routes[key])
        return httpx.Response(404, json={"error": "not found"})

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_sync_basho_and_banzuke_and_matches(db):
    routes = {
        "/basho/202405": {
            "location": "Tokyo",
            "startDate": "2024-05-12T00:00:00Z",
            "endDate": "2024-05-26T00:00:00Z",
        },
        "/basho/202405/banzuke/Makuuchi": {
            "east": [
                {
                    "rikishiID": 1,
                    "shikonaEn": "Hokutofuji",
                    "rank": "Maegashira 1 East",
                    "rankValue": 100,
                }
            ],
            "west": [
                {
                    "rikishiID": 2,
                    "shikonaEn": "Mitakeumi",
                    "rank": "Maegashira 1 West",
                    "rankValue": 101,
                }
            ],
        },
        "/basho/202405/torikumi/Makuuchi/1": {
            "torikumi": [
                {
                    "id": "match-1",
                    "eastId": 1,
                    "westId": 2,
                    "winnerId": 1,
                    "kimarite": "yorikiri",
                }
            ]
        },
    }
    client = httpx.AsyncClient(transport=_mock_transport(routes), base_url="https://x/api")
    api = SumoApiClient(base_url="https://x/api", client=client)

    await sync_basho(db, api, "202405")
    await sync_banzuke(db, api, "202405", "Makuuchi")
    added = await sync_matches_for_day(db, api, "202405", "Makuuchi", 1)
    await client.aclose()

    assert added == 1
    basho = await db.get(Basho, "202405")
    assert basho is not None
    assert basho.name == "Tokyo"
    assert (await db.get(Rikishi, 1)).name == "Hokutofuji"
    assert (await db.get(BashoRikishi, ("202405", 1))).division == "Makuuchi"
    match = await db.get(Match, "match-1")
    assert match is not None
    assert match.winner_id == 1
    assert match.division == "Makuuchi"

    # Re-running is a no-op.
    client2 = httpx.AsyncClient(transport=_mock_transport(routes), base_url="https://x/api")
    api2 = SumoApiClient(base_url="https://x/api", client=client2)
    re_added = await sync_matches_for_day(db, api2, "202405", "Makuuchi", 1)
    await client2.aclose()
    assert re_added == 0


@pytest.mark.asyncio
async def test_sync_measurements_skips_unknown_rikishi(db):
    # Pre-load one known rikishi; the /measurements payload also references an
    # unknown rikishi from another division — that one must be silently skipped
    # so the FK constraint never fires.
    db.add(Basho(id="202405", name="Tokyo"))
    db.add(Rikishi(id=1, name="Hokutofuji"))
    await db.commit()

    routes = {
        "/measurements?bashoId=202405": [
            {"bashoId": "202405", "rikishiId": 1, "height": 185, "weight": 160},
            {"bashoId": "202405", "rikishiId": 9117, "height": 170, "weight": 152},
            {"bashoId": "202403", "rikishiId": 1, "height": 184, "weight": 159},
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        # MockTransport strips the query string from path, so route by full URL.
        key = str(request.url).removeprefix("https://x/api")
        if key in routes:
            return httpx.Response(200, json=routes[key])
        return httpx.Response(404, json={"error": "not found"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://x/api")
    api = SumoApiClient(base_url="https://x/api", client=client)
    await sync_measurements(db, api, "202405")
    await client.aclose()

    rows = (await db.execute(select(Measurement))).scalars().all()
    assert len(rows) == 1
    assert rows[0].rikishi_id == 1
    assert rows[0].basho_id == "202405"


@pytest.mark.asyncio
async def test_sync_matches_lazy_loads_unknown_rikishi(db):
    # Makuuchi torikumi can reference a Juryo wrestler (juryo-up bout). The
    # banzuke load only contains Makuuchi, so the missing rikishi must be
    # fetched on demand rather than blowing the FK.
    from datetime import date as _date

    db.add(Basho(id="202605", name="Tokyo", start_date=_date(2026, 5, 10)))
    db.add(Rikishi(id=9051, name="MakuuchiGuy"))
    await db.commit()

    routes = {
        "/basho/202605/torikumi/Makuuchi/3": {
            "torikumi": [
                {
                    "id": "202605-3-1-9051-8853",
                    "eastId": 9051,
                    "westId": 8853,
                    "winnerId": 8853,
                    "kimarite": "hatakikomi",
                }
            ]
        },
        "/rikishi/8853": {"id": 8853, "shikonaEn": "JuryoUp"},
    }
    client = httpx.AsyncClient(transport=_mock_transport(routes), base_url="https://x/api")
    api = SumoApiClient(base_url="https://x/api", client=client)
    added = await sync_matches_for_day(db, api, "202605", "Makuuchi", 3)
    await client.aclose()

    assert added == 1
    juryo = await db.get(Rikishi, 8853)
    assert juryo is not None
    assert juryo.name == "JuryoUp"
    match = await db.get(Match, "202605-3-1-9051-8853")
    assert match is not None
    assert match.winner_id == 8853
