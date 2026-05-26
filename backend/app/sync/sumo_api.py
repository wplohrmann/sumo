"""Async client + idempotent loaders for sumo-api.com data.

Ports `sumo/download_data.py` to async SQLAlchemy + httpx. Inserts use
dialect-aware "do nothing on conflict" so re-running mid-tournament is
safe and cheap.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Basho, BashoRikishi, Match, Measurement, Rikishi
from app.settings import get_settings

logger = logging.getLogger(__name__)

DIVISIONS = ["Makuuchi", "Juryo", "Makushita", "Sandanme", "Jonidan", "Jonokuchi"]
MAKUUCHI = "Makuuchi"
DAYS_PER_BASHO = 15


@dataclass
class SyncReport:
    basho_id: str
    divisions: list[str]
    days_synced: list[int]
    new_matches: int


class SumoApiClient:
    def __init__(self, base_url: str | None = None, client: httpx.AsyncClient | None = None):
        self._base_url = base_url or get_settings().sumo_api_base_url
        self._client = client or httpx.AsyncClient(timeout=30.0)
        self._owns_client = client is None

    async def __aenter__(self) -> "SumoApiClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def fetch(self, path: str) -> Any:
        url = f"{self._base_url}{path}"
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()


def _on_conflict_do_nothing(session: AsyncSession, table, values: dict):
    """Return a dialect-appropriate INSERT ... ON CONFLICT DO NOTHING statement."""
    dialect = session.bind.dialect.name if session.bind else "postgresql"
    if dialect == "postgresql":
        return pg_insert(table).values(**values).on_conflict_do_nothing()
    if dialect == "sqlite":
        return sqlite_insert(table).values(**values).on_conflict_do_nothing()
    raise RuntimeError(f"Unsupported dialect for upsert: {dialect}")


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    # sumo-api returns dates as ISO strings, sometimes with time appended.
    return datetime.fromisoformat(value.replace("Z", "+00:00")).date()


def _parse_basho_month(value: str | None) -> date | None:
    """sumo-api returns a rikishi's `debut` as a basho id (YYYYMM), not a real
    date. Map it to the first of that month so we can store it as a Date."""
    if not value:
        return None
    s = str(value)
    if len(s) == 6 and s.isdigit():
        return date(int(s[:4]), int(s[4:]), 1)
    return None


async def sync_basho(session: AsyncSession, api: SumoApiClient, basho_id: str) -> None:
    exists = await session.scalar(select(Basho).where(Basho.id == basho_id))
    if exists:
        return
    data = await api.fetch(f"/basho/{basho_id}")
    await session.execute(
        _on_conflict_do_nothing(
            session,
            Basho.__table__,
            {
                "id": basho_id,
                "name": data.get("location"),
                "start_date": _parse_date(data.get("startDate")),
                "end_date": _parse_date(data.get("endDate")),
            },
        )
    )
    await session.commit()


async def sync_banzuke(
    session: AsyncSession, api: SumoApiClient, basho_id: str, division: str
) -> None:
    data = await api.fetch(f"/basho/{basho_id}/banzuke/{division}")
    for side in ("east", "west"):
        riders = data.get(side) or []
        for r in riders:
            rikishi_id = r["rikishiID"]
            await session.execute(
                _on_conflict_do_nothing(
                    session,
                    Rikishi.__table__,
                    {
                        "id": rikishi_id,
                        "name": r.get("shikonaEn"),
                    },
                )
            )
            await session.execute(
                _on_conflict_do_nothing(
                    session,
                    BashoRikishi.__table__,
                    {
                        "basho_id": basho_id,
                        "rikishi_id": rikishi_id,
                        "rank": r.get("rank"),
                        "rank_value": r.get("rankValue"),
                        "division": division,
                    },
                )
            )
    await session.commit()


async def _ensure_rikishi(
    session: AsyncSession, api: SumoApiClient, rikishi_id: int | None
) -> None:
    """Ensure `rikishi_id` exists in the rikishi table, lazily fetching from
    the API if needed. Used when a torikumi references a rikishi outside the
    division we loaded (e.g. juryo-up bouts in a Makuuchi torikumi)."""
    if rikishi_id is None:
        return
    if await session.scalar(select(Rikishi.id).where(Rikishi.id == rikishi_id)):
        return
    try:
        data = await api.fetch(f"/rikishi/{rikishi_id}")
    except httpx.HTTPStatusError as e:
        logger.warning("rikishi %s lookup failed: %s", rikishi_id, e)
        data = {"id": rikishi_id, "shikonaEn": f"#{rikishi_id}"}
    await session.execute(
        _on_conflict_do_nothing(
            session,
            Rikishi.__table__,
            {
                "id": data.get("id") or rikishi_id,
                "name": data.get("shikonaEn") or f"#{rikishi_id}",
                "debut_date": _parse_basho_month(data.get("debut")),
                "birth_date": _parse_date(data.get("birthDate")),
            },
        )
    )


async def sync_rikishi_details(
    session: AsyncSession, api: SumoApiClient, basho_id: str
) -> None:
    rows = (
        await session.execute(
            select(BashoRikishi.rikishi_id).where(BashoRikishi.basho_id == basho_id)
        )
    ).all()
    for (rikishi_id,) in rows:
        existing = await session.scalar(select(Rikishi).where(Rikishi.id == rikishi_id))
        if existing and existing.birth_date is not None:
            continue
        try:
            data = await api.fetch(f"/rikishi/{rikishi_id}")
        except httpx.HTTPStatusError as e:
            logger.warning("rikishi %s: %s", rikishi_id, e)
            continue
        await session.execute(
            _on_conflict_do_nothing(
                session,
                Rikishi.__table__,
                {
                    "id": data.get("id"),
                    "name": data.get("shikonaEn"),
                    "debut_date": _parse_basho_month(data.get("debut")),
                    "birth_date": _parse_date(data.get("birthDate")),
                },
            )
        )
    await session.commit()


async def sync_measurements(
    session: AsyncSession, api: SumoApiClient, basho_id: str
) -> None:
    existing = await session.scalar(
        select(Measurement).where(Measurement.basho_id == basho_id)
    )
    if existing:
        return
    # The /measurements endpoint returns rows for every division; we only keep
    # measurements for rikishi we've already loaded, to avoid FK violations
    # when an active-tournament sync only pulled the Makuuchi banzuke.
    known_ids = set(
        (await session.execute(select(Rikishi.id))).scalars().all()
    )
    measurements = await api.fetch(f"/measurements?bashoId={basho_id}")
    for m in measurements:
        if m.get("bashoId") != basho_id:
            continue
        rikishi_id = m.get("rikishiId")
        if rikishi_id not in known_ids:
            continue
        await session.execute(
            _on_conflict_do_nothing(
                session,
                Measurement.__table__,
                {
                    "rikishi_id": rikishi_id,
                    "basho_id": basho_id,
                    "height_cm": m.get("height"),
                    "weight_kg": m.get("weight"),
                },
            )
        )
    await session.commit()


async def sync_matches_for_day(
    session: AsyncSession,
    api: SumoApiClient,
    basho_id: str,
    division: str,
    day: int,
) -> int:
    """Pull a single day's torikumi for one division. Returns # of new matches."""
    basho = await session.scalar(select(Basho).where(Basho.id == basho_id))
    if basho is None or basho.start_date is None:
        return 0
    match_date = basho.start_date + timedelta(days=day - 1)
    try:
        data = await api.fetch(f"/basho/{basho_id}/torikumi/{division}/{day}")
    except httpx.HTTPStatusError as e:
        logger.info("no torikumi for %s %s day %s: %s", basho_id, division, day, e)
        return 0
    matches = data.get("torikumi") or []
    new_count = 0
    for m in matches:
        match_id = m["id"]
        existing = await session.scalar(select(Match).where(Match.id == match_id))
        if existing:
            continue
        east_id = m.get("eastId")
        west_id = m.get("westId")
        await _ensure_rikishi(session, api, east_id)
        await _ensure_rikishi(session, api, west_id)
        await session.execute(
            _on_conflict_do_nothing(
                session,
                Match.__table__,
                {
                    "id": match_id,
                    "basho_id": basho_id,
                    "rikishi1_id": east_id,
                    "rikishi2_id": west_id,
                    "winner_id": m.get("winnerId"),
                    "kimarite": m.get("kimarite"),
                    "day": day,
                    "match_date": match_date,
                    "division": division,
                },
            )
        )
        new_count += 1
    await session.commit()
    return new_count


async def sync_active_tournament(
    session: AsyncSession,
    api: SumoApiClient,
    basho_id: str,
    division: str = MAKUUCHI,
    days: int = DAYS_PER_BASHO,
) -> SyncReport:
    """Full sync for an active tournament: basho metadata + banzuke + Makuuchi
    days 1..N. Idempotent."""
    await sync_basho(session, api, basho_id)
    await sync_banzuke(session, api, basho_id, division)
    await sync_rikishi_details(session, api, basho_id)
    await sync_measurements(session, api, basho_id)
    new_matches = 0
    days_synced: list[int] = []
    for day in range(1, days + 1):
        added = await sync_matches_for_day(session, api, basho_id, division, day)
        new_matches += added
        days_synced.append(day)
    return SyncReport(
        basho_id=basho_id,
        divisions=[division],
        days_synced=days_synced,
        new_matches=new_matches,
    )


async def sync_full_basho(
    session: AsyncSession, api: SumoApiClient, basho_id: str
) -> SyncReport:
    """Bootstrap: every division, every day. Used by the historical CLI."""
    await sync_basho(session, api, basho_id)
    for division in DIVISIONS:
        await sync_banzuke(session, api, basho_id, division)
    await sync_rikishi_details(session, api, basho_id)
    await sync_measurements(session, api, basho_id)
    new_matches = 0
    days_synced: list[int] = []
    for division in DIVISIONS:
        for day in range(1, DAYS_PER_BASHO + 1):
            new_matches += await sync_matches_for_day(
                session, api, basho_id, division, day
            )
            days_synced.append(day)
    return SyncReport(
        basho_id=basho_id,
        divisions=DIVISIONS,
        days_synced=days_synced,
        new_matches=new_matches,
    )
