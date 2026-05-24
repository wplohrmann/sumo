from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_user
from app.db.models import AppUser
from app.db.session import get_session

router = APIRouter(prefix="/me", tags=["me"])


class SpoilerDayUpdate(BaseModel):
    spoiler_day: int | None = Field(default=None, ge=1, le=20)


@router.patch("/spoiler-day")
async def update_spoiler_day(
    body: SpoilerDayUpdate,
    user: AppUser = Depends(current_user),
    session: AsyncSession = Depends(get_session),
) -> dict:
    if body.spoiler_day is not None and (body.spoiler_day < 1 or body.spoiler_day > 20):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="bad day")
    user.spoiler_day = body.spoiler_day
    await session.commit()
    return {"spoiler_day": user.spoiler_day}
