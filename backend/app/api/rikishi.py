"""Per-tournament Makuuchi rikishi + pricing."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_admin, current_user
from app.db.models import (
    AppUser,
    BashoRikishi,
    Rikishi,
    RikishiPrice,
    Tournament,
)
from app.db.session import get_session
from app.pricing import default_price_pence

router = APIRouter(prefix="/tournaments", tags=["rikishi"])


class RikishiOut(BaseModel):
    rikishi_id: int
    name: str | None
    rank: str | None
    rank_value: int | None
    price_pence: int | None


class PriceUpdate(BaseModel):
    price_pence: int = Field(ge=0)


@router.get("/{tournament_id}/rikishi", response_model=list[RikishiOut])
async def list_rikishi(
    tournament_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> list[RikishiOut]:
    t = await session.get(Tournament, tournament_id)
    if t is None:
        raise HTTPException(status_code=404, detail="tournament not found")

    rows = (
        await session.execute(
            select(
                Rikishi.id,
                Rikishi.name,
                BashoRikishi.rank,
                BashoRikishi.rank_value,
                RikishiPrice.price_pence,
            )
            .join(BashoRikishi, BashoRikishi.rikishi_id == Rikishi.id)
            .outerjoin(
                RikishiPrice,
                (RikishiPrice.rikishi_id == Rikishi.id)
                & (RikishiPrice.tournament_id == tournament_id),
            )
            .where(
                BashoRikishi.basho_id == t.basho_id,
                BashoRikishi.division == "Makuuchi",
            )
            .order_by(BashoRikishi.rank_value)
        )
    ).all()

    # Seed default prices for rows that don't have one yet. We only do this
    # while the draft is still mutable so we don't accidentally re-introduce
    # prices for an archived tournament.
    seeded = False
    if t.status in ("setup", "drafting"):
        for r in rows:
            if r.price_pence is not None:
                continue
            default = default_price_pence(r.rank)
            if default is None:
                continue
            session.add(
                RikishiPrice(
                    tournament_id=tournament_id,
                    rikishi_id=r.id,
                    price_pence=default,
                )
            )
            seeded = True
        if seeded:
            await session.commit()

    return [
        RikishiOut(
            rikishi_id=r.id,
            name=r.name,
            rank=r.rank,
            rank_value=r.rank_value,
            price_pence=(
                r.price_pence
                if r.price_pence is not None
                else default_price_pence(r.rank)
            ),
        )
        for r in rows
    ]


@router.put("/{tournament_id}/rikishi/{rikishi_id}/price", response_model=RikishiOut)
async def set_price(
    tournament_id: uuid.UUID,
    rikishi_id: int,
    body: PriceUpdate,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> RikishiOut:
    t = await session.get(Tournament, tournament_id)
    if t is None:
        raise HTTPException(status_code=404, detail="tournament not found")
    if t.status not in ("setup", "drafting"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="prices are locked once the tournament is active",
        )
    rikishi = await session.get(Rikishi, rikishi_id)
    if rikishi is None:
        raise HTTPException(status_code=404, detail="rikishi not found")
    # Ensure they're in Makuuchi for this basho.
    in_makuuchi = await session.scalar(
        select(BashoRikishi).where(
            BashoRikishi.basho_id == t.basho_id,
            BashoRikishi.rikishi_id == rikishi_id,
            BashoRikishi.division == "Makuuchi",
        )
    )
    if in_makuuchi is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="rikishi is not in Makuuchi for this basho",
        )

    existing = await session.get(RikishiPrice, (tournament_id, rikishi_id))
    if existing:
        existing.price_pence = body.price_pence
    else:
        session.add(
            RikishiPrice(
                tournament_id=tournament_id,
                rikishi_id=rikishi_id,
                price_pence=body.price_pence,
            )
        )
    await session.commit()
    return RikishiOut(
        rikishi_id=rikishi_id,
        name=rikishi.name,
        rank=in_makuuchi.rank,
        rank_value=in_makuuchi.rank_value,
        price_pence=body.price_pence,
    )
