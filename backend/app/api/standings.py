from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_user
from app.db.models import (
    AppUser,
    Match,
    Rikishi,
    RosterEntry,
    Tournament,
)
from app.db.session import get_session
from app.scoring.engine import REGULAR_DAYS, compute_standings

router = APIRouter(prefix="/tournaments", tags=["standings"])


class DayBreakdownOut(BaseModel):
    day: int
    wins: int
    scalps: int
    wins_points: int
    scalp_points: int
    adjustment_points: int


class StandingOut(BaseModel):
    user_id: str
    display_name: str
    total_points: int
    wins_points: int
    scalp_points: int
    award_points: int
    adjustment_points: int
    by_day: list[DayBreakdownOut]


def _effective_through_day(user: AppUser, requested: int | None) -> int:
    candidates = [REGULAR_DAYS]
    if requested is not None:
        candidates.append(requested)
    if user.spoiler_day is not None:
        candidates.append(user.spoiler_day)
    return max(0, min(candidates))


@router.get("/{tournament_id}/standings", response_model=list[StandingOut])
async def standings(
    tournament_id: uuid.UUID,
    through_day: int | None = Query(default=None, ge=0, le=20),
    session: AsyncSession = Depends(get_session),
    user: AppUser = Depends(current_user),
) -> list[StandingOut]:
    if not await session.get(Tournament, tournament_id):
        raise HTTPException(status_code=404, detail="tournament not found")
    effective = _effective_through_day(user, through_day)
    rows = await compute_standings(session, tournament_id, effective)
    return [
        StandingOut(
            user_id=str(r.user_id),
            display_name=r.display_name,
            total_points=r.total_points,
            wins_points=r.wins_points,
            scalp_points=r.scalp_points,
            award_points=r.award_points,
            adjustment_points=r.adjustment_points,
            by_day=[
                DayBreakdownOut(
                    day=d.day,
                    wins=d.wins,
                    scalps=d.scalps,
                    wins_points=d.wins_points,
                    scalp_points=d.scalp_points,
                    adjustment_points=d.adjustment_points,
                )
                for d in r.by_day
            ],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Per-day match list with ownership annotations.
# ---------------------------------------------------------------------------


class OwnerOut(BaseModel):
    user_id: str
    display_name: str


class DayMatchOut(BaseModel):
    match_id: str
    rikishi1_id: int
    rikishi1_name: str | None
    rikishi1_owner: OwnerOut | None
    rikishi2_id: int
    rikishi2_name: str | None
    rikishi2_owner: OwnerOut | None
    winner_id: int | None
    kimarite: str | None


@router.get("/{tournament_id}/days/{day}", response_model=list[DayMatchOut])
async def get_day(
    tournament_id: uuid.UUID,
    day: int,
    session: AsyncSession = Depends(get_session),
    user: AppUser = Depends(current_user),
) -> list[DayMatchOut]:
    tournament = await session.get(Tournament, tournament_id)
    if tournament is None:
        raise HTTPException(status_code=404, detail="tournament not found")
    if user.spoiler_day is not None and day > user.spoiler_day:
        raise HTTPException(status_code=403, detail="day hidden by spoiler cap")

    matches = (
        await session.scalars(
            select(Match)
            .where(
                Match.basho_id == tournament.basho_id,
                Match.division == "Makuuchi",
                Match.day == day,
            )
            .order_by(Match.id)
        )
    ).all()

    entries = (
        await session.scalars(
            select(RosterEntry).where(RosterEntry.tournament_id == tournament_id)
        )
    ).all()
    users = {
        u.id: u
        for u in (
            await session.execute(select(AppUser))
        ).scalars().all()
    }

    def owner_for(rikishi_id: int) -> OwnerOut | None:
        for e in entries:
            if e.rikishi_id != rikishi_id:
                continue
            if e.acquired_before_day > day:
                continue
            if e.released_before_day is not None and e.released_before_day <= day:
                continue
            u = users.get(e.participant_user_id)
            if u:
                return OwnerOut(user_id=str(u.id), display_name=u.display_name)
        return None

    # Hydrate rikishi names in one go.
    name_rows = (
        await session.execute(
            select(Rikishi.id, Rikishi.name).where(
                Rikishi.id.in_(
                    {m.rikishi1_id for m in matches}
                    | {m.rikishi2_id for m in matches}
                )
            )
        )
    ).all()
    name_by_id = {rid: name for rid, name in name_rows}

    return [
        DayMatchOut(
            match_id=m.id,
            rikishi1_id=m.rikishi1_id,
            rikishi1_name=name_by_id.get(m.rikishi1_id),
            rikishi1_owner=owner_for(m.rikishi1_id),
            rikishi2_id=m.rikishi2_id,
            rikishi2_name=name_by_id.get(m.rikishi2_id),
            rikishi2_owner=owner_for(m.rikishi2_id),
            winner_id=m.winner_id,
            kimarite=m.kimarite,
        )
        for m in matches
    ]
