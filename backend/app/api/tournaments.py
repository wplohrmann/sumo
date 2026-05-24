from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_admin, current_user
from app.db.models import AppUser, Basho, Tournament, TournamentParticipant
from app.db.session import get_session

router = APIRouter(prefix="/tournaments", tags=["tournaments"])


ALLOWED_STATUSES = {"setup", "drafting", "active", "archived"}


class TournamentCreate(BaseModel):
    basho_id: str
    name: str
    budget_pence: int = 5500
    roster_size: int = 4


class TournamentStatusUpdate(BaseModel):
    status: str = Field(description="one of setup/drafting/active/archived")


class TournamentOut(BaseModel):
    id: str
    basho_id: str
    name: str
    status: str
    budget_pence: int
    roster_size: int


def _serialize(t: Tournament) -> TournamentOut:
    return TournamentOut(
        id=str(t.id),
        basho_id=t.basho_id,
        name=t.name,
        status=t.status,
        budget_pence=t.budget_pence,
        roster_size=t.roster_size,
    )


@router.get("/current", response_model=TournamentOut | None)
async def get_current(
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> TournamentOut | None:
    t = await session.scalar(
        select(Tournament).where(Tournament.status != "archived")
    )
    return _serialize(t) if t else None


@router.get("", response_model=list[TournamentOut])
async def list_tournaments(
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> list[TournamentOut]:
    rows = (
        await session.scalars(
            select(Tournament).order_by(Tournament.created_at.desc())
        )
    ).all()
    return [_serialize(t) for t in rows]


@router.post("", response_model=TournamentOut, status_code=status.HTTP_201_CREATED)
async def create_tournament(
    body: TournamentCreate,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> TournamentOut:
    if not await session.get(Basho, body.basho_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="unknown basho_id — run a sync first",
        )
    existing = await session.scalar(
        select(Tournament).where(Tournament.status != "archived")
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="archive the current tournament first",
        )
    t = Tournament(
        basho_id=body.basho_id,
        name=body.name,
        status="setup",
        budget_pence=body.budget_pence,
        roster_size=body.roster_size,
    )
    session.add(t)
    await session.commit()
    await session.refresh(t)
    return _serialize(t)


@router.patch("/{tournament_id}/status", response_model=TournamentOut)
async def update_status(
    tournament_id: uuid.UUID,
    body: TournamentStatusUpdate,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> TournamentOut:
    if body.status not in ALLOWED_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"status must be one of {sorted(ALLOWED_STATUSES)}",
        )
    t = await session.get(Tournament, tournament_id)
    if t is None:
        raise HTTPException(status_code=404, detail="tournament not found")
    t.status = body.status
    await session.commit()
    await session.refresh(t)
    return _serialize(t)


@router.get("/{tournament_id}", response_model=TournamentOut)
async def get_tournament(
    tournament_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> TournamentOut:
    t = await session.get(Tournament, tournament_id)
    if t is None:
        raise HTTPException(status_code=404, detail="tournament not found")
    return _serialize(t)


# ---------------------------------------------------------------------------
# Participants
# ---------------------------------------------------------------------------


class ParticipantCreate(BaseModel):
    display_name: str


class ParticipantOut(BaseModel):
    user_id: str
    display_name: str


class ParticipantCreated(ParticipantOut):
    token: str  # plain token, shown once


@router.get("/{tournament_id}/participants", response_model=list[ParticipantOut])
async def list_participants(
    tournament_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> list[ParticipantOut]:
    rows = (
        await session.execute(
            select(AppUser)
            .join(
                TournamentParticipant,
                TournamentParticipant.user_id == AppUser.id,
            )
            .where(TournamentParticipant.tournament_id == tournament_id)
            .order_by(AppUser.display_name)
        )
    ).scalars().all()
    return [ParticipantOut(user_id=str(u.id), display_name=u.display_name) for u in rows]


@router.post(
    "/{tournament_id}/participants",
    response_model=ParticipantCreated,
    status_code=status.HTTP_201_CREATED,
)
async def add_participant(
    tournament_id: uuid.UUID,
    body: ParticipantCreate,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> ParticipantCreated:
    from app.auth.tokens import hash_token, new_viewer_token

    if not await session.get(Tournament, tournament_id):
        raise HTTPException(status_code=404, detail="tournament not found")

    display_name = body.display_name.strip()
    if not display_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="display_name required"
        )
    existing = await session.scalar(
        select(AppUser).where(AppUser.display_name == display_name)
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="display_name already in use",
        )
    token = new_viewer_token()
    user = AppUser(
        display_name=display_name,
        role="viewer",
        token_hash=hash_token(token),
    )
    session.add(user)
    await session.flush()
    session.add(
        TournamentParticipant(tournament_id=tournament_id, user_id=user.id)
    )
    await session.commit()
    await session.refresh(user)
    return ParticipantCreated(
        user_id=str(user.id),
        display_name=user.display_name,
        token=token,
    )
