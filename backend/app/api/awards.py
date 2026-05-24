from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_admin, current_user
from app.db.models import (
    AppUser,
    BashoRikishi,
    Rikishi,
    Tournament,
    TournamentAward,
)
from app.db.session import get_session

router = APIRouter(prefix="/tournaments", tags=["awards"])


ALLOWED_KINDS = {"yusho", "playoff", "shukun", "kanto", "gino"}


class AwardIn(BaseModel):
    rikishi_id: int
    kind: str


class AwardSetIn(BaseModel):
    awards: list[AwardIn]


class AwardOut(BaseModel):
    rikishi_id: int
    rikishi_name: str | None
    kind: str


async def _load_awards(
    tournament_id: uuid.UUID, session: AsyncSession
) -> list[AwardOut]:
    rows = (
        await session.execute(
            select(TournamentAward, Rikishi.name)
            .join(Rikishi, Rikishi.id == TournamentAward.rikishi_id)
            .where(TournamentAward.tournament_id == tournament_id)
        )
    ).all()
    return [
        AwardOut(
            rikishi_id=a.rikishi_id,
            rikishi_name=name,
            kind=a.kind,
        )
        for a, name in rows
    ]


@router.get("/{tournament_id}/awards", response_model=list[AwardOut])
async def list_awards(
    tournament_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> list[AwardOut]:
    return await _load_awards(tournament_id, session)


@router.put("/{tournament_id}/awards", response_model=list[AwardOut])
async def replace_awards(
    tournament_id: uuid.UUID,
    body: AwardSetIn,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> list[AwardOut]:
    tournament = await session.get(Tournament, tournament_id)
    if tournament is None:
        raise HTTPException(status_code=404, detail="tournament not found")

    seen: set[tuple[int, str]] = set()
    for a in body.awards:
        if a.kind not in ALLOWED_KINDS:
            raise HTTPException(
                status_code=400,
                detail=f"kind must be one of {sorted(ALLOWED_KINDS)}",
            )
        key = (a.rikishi_id, a.kind)
        if key in seen:
            raise HTTPException(status_code=400, detail=f"duplicate award {key}")
        seen.add(key)
        # Rikishi must have been in Makuuchi for this basho.
        valid = await session.scalar(
            select(BashoRikishi).where(
                BashoRikishi.basho_id == tournament.basho_id,
                BashoRikishi.rikishi_id == a.rikishi_id,
                BashoRikishi.division == "Makuuchi",
            )
        )
        if valid is None:
            raise HTTPException(
                status_code=400,
                detail=f"rikishi {a.rikishi_id} is not in Makuuchi for this basho",
            )

    # Replace the set wholesale.
    existing = (
        await session.scalars(
            select(TournamentAward).where(
                TournamentAward.tournament_id == tournament_id
            )
        )
    ).all()
    for row in existing:
        await session.delete(row)
    for a in body.awards:
        session.add(
            TournamentAward(
                tournament_id=tournament_id,
                rikishi_id=a.rikishi_id,
                kind=a.kind,
            )
        )
    await session.commit()

    return await _load_awards(tournament_id, session)
