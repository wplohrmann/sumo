from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_admin, current_user
from app.db.models import (
    AppUser,
    ScoreAdjustment,
    Tournament,
    TournamentParticipant,
)
from app.db.session import get_session

router = APIRouter(prefix="/tournaments", tags=["adjustments"])


class AdjustmentIn(BaseModel):
    participant_user_id: uuid.UUID
    points: int
    reason: str
    day: int | None = None


class AdjustmentOut(BaseModel):
    id: str
    participant_user_id: str
    points: int
    reason: str
    day: int | None


@router.get(
    "/{tournament_id}/adjustments",
    response_model=list[AdjustmentOut],
)
async def list_adjustments(
    tournament_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> list[AdjustmentOut]:
    rows = (
        await session.scalars(
            select(ScoreAdjustment)
            .where(ScoreAdjustment.tournament_id == tournament_id)
            .order_by(ScoreAdjustment.created_at)
        )
    ).all()
    return [
        AdjustmentOut(
            id=str(r.id),
            participant_user_id=str(r.participant_user_id),
            points=r.points,
            reason=r.reason,
            day=r.day,
        )
        for r in rows
    ]


@router.post(
    "/{tournament_id}/adjustments",
    response_model=AdjustmentOut,
    status_code=201,
)
async def create_adjustment(
    tournament_id: uuid.UUID,
    body: AdjustmentIn,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> AdjustmentOut:
    if not await session.get(Tournament, tournament_id):
        raise HTTPException(status_code=404, detail="tournament not found")
    if not await session.get(
        TournamentParticipant, (tournament_id, body.participant_user_id)
    ):
        raise HTTPException(
            status_code=400, detail="user is not a participant"
        )
    if body.day is not None and (body.day < 1 or body.day > 20):
        raise HTTPException(status_code=400, detail="day must be 1..20 or null")
    row = ScoreAdjustment(
        tournament_id=tournament_id,
        participant_user_id=body.participant_user_id,
        points=body.points,
        reason=body.reason,
        day=body.day,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return AdjustmentOut(
        id=str(row.id),
        participant_user_id=str(row.participant_user_id),
        points=row.points,
        reason=row.reason,
        day=row.day,
    )


@router.delete("/{tournament_id}/adjustments/{adjustment_id}", status_code=204)
async def delete_adjustment(
    tournament_id: uuid.UUID,
    adjustment_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> None:
    row = await session.get(ScoreAdjustment, adjustment_id)
    if row is None or row.tournament_id != tournament_id:
        raise HTTPException(status_code=404, detail="adjustment not found")
    await session.delete(row)
    await session.commit()
