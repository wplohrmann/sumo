"""Smoke test for the async sumo-api sync against canned JSON responses."""
from __future__ import annotations

import httpx
import pytest

from app.db.models import Basho, BashoRikishi, Match, Rikishi
from app.sync.sumo_api import (
    SumoApiClient,
    sync_banzuke,
    sync_basho,
    sync_matches_for_day,
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
