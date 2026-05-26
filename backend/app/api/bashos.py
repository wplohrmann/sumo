from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_user
from app.db.models import AppUser, Basho
from app.db.session import get_session

router = APIRouter(prefix="/bashos", tags=["bashos"])


# Honbasho run in odd-numbered months: Jan, Mar, May, Jul, Sep, Nov.
BASHO_MONTHS = {1, 3, 5, 7, 9, 11}


def recent_basho_ids(today: date, count: int = 10) -> list[str]:
    """Return the `count` most recent basho IDs (YYYYMM) up to and including
    the current calendar month if it's a basho month, newest first."""
    year, month = today.year, today.month
    if month not in BASHO_MONTHS:
        month -= 1  # round down to the previous odd month
    ids: list[str] = []
    for _ in range(count):
        if month < 1:
            year -= 1
            month += 12
        ids.append(f"{year:04d}{month:02d}")
        month -= 2
    return ids


class RecentBashoOut(BaseModel):
    id: str
    synced: bool
    name: str | None = None
    start_date: date | None = None
    end_date: date | None = None


@router.get("/recent", response_model=list[RecentBashoOut])
async def recent_bashos(
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> list[RecentBashoOut]:
    ids = recent_basho_ids(date.today())
    rows = (await session.scalars(select(Basho).where(Basho.id.in_(ids)))).all()
    by_id = {b.id: b for b in rows}
    return [
        RecentBashoOut(
            id=bid,
            synced=bid in by_id,
            name=by_id[bid].name if bid in by_id else None,
            start_date=by_id[bid].start_date if bid in by_id else None,
            end_date=by_id[bid].end_date if bid in by_id else None,
        )
        for bid in ids
    ]
