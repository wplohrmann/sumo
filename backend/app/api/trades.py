"""Trade endpoint.

A trade atomically closes one roster_entry (sets sale_price_pence and
released_before_day) and opens a new one (acquired_via='trade'). The
sale price is floor(purchase_price / 2). Budget math accounts for the
half-loss: a participant's effective spend is
`sum(purchase for active) + sum(purchase - sale for sold)`.
"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_admin, current_user
from app.db.models import (
    AppUser,
    Rikishi,
    RikishiPrice,
    RosterEntry,
    Tournament,
    Trade,
    TournamentParticipant,
)
from app.db.session import get_session
from app.sharing import (
    detail_for_warnings,
    load_rikishi_names,
    load_user_index,
    warnings_from_added,
)

router = APIRouter(prefix="/tournaments", tags=["trades"])


class TradeIn(BaseModel):
    participant_user_id: uuid.UUID
    sell_entry_id: uuid.UUID
    buy_rikishi_id: int
    effective_before_day: int = Field(ge=1, le=15)
    note: str | None = None
    force: bool = False


class TradeOut(BaseModel):
    id: str
    participant_user_id: str
    sold_entry_id: str
    bought_entry_id: str
    effective_before_day: int
    note: str | None
    sold_rikishi_id: int
    sold_rikishi_name: str | None
    sale_price_pence: int
    bought_rikishi_id: int
    bought_rikishi_name: str | None
    purchase_price_pence: int


async def _effective_spent(
    session: AsyncSession, tournament_id: uuid.UUID, user_id: uuid.UUID
) -> int:
    entries = (
        await session.scalars(
            select(RosterEntry).where(
                RosterEntry.tournament_id == tournament_id,
                RosterEntry.participant_user_id == user_id,
            )
        )
    ).all()
    spent = 0
    for e in entries:
        if e.released_before_day is None:
            spent += e.purchase_price_pence
        else:
            spent += e.purchase_price_pence - (e.sale_price_pence or 0)
    return spent


async def _resolve_trade(
    session: AsyncSession, t: Trade
) -> TradeOut:
    sold = await session.get(RosterEntry, t.sold_entry_id)
    bought = await session.get(RosterEntry, t.bought_entry_id)
    assert sold is not None and bought is not None
    sold_name = (
        await session.scalar(select(Rikishi.name).where(Rikishi.id == sold.rikishi_id))
    )
    bought_name = (
        await session.scalar(select(Rikishi.name).where(Rikishi.id == bought.rikishi_id))
    )
    return TradeOut(
        id=str(t.id),
        participant_user_id=str(t.participant_user_id),
        sold_entry_id=str(t.sold_entry_id),
        bought_entry_id=str(t.bought_entry_id),
        effective_before_day=t.effective_before_day,
        note=t.note,
        sold_rikishi_id=sold.rikishi_id,
        sold_rikishi_name=sold_name,
        sale_price_pence=sold.sale_price_pence or 0,
        bought_rikishi_id=bought.rikishi_id,
        bought_rikishi_name=bought_name,
        purchase_price_pence=bought.purchase_price_pence,
    )


@router.get("/{tournament_id}/trades", response_model=list[TradeOut])
async def list_trades(
    tournament_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> list[TradeOut]:
    rows = (
        await session.scalars(
            select(Trade)
            .where(Trade.tournament_id == tournament_id)
            .order_by(Trade.effective_before_day, Trade.created_at)
        )
    ).all()
    return [await _resolve_trade(session, t) for t in rows]


@router.post(
    "/{tournament_id}/trades",
    response_model=TradeOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_trade(
    tournament_id: uuid.UUID,
    body: TradeIn,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> TradeOut:
    tournament = await session.get(Tournament, tournament_id)
    if tournament is None:
        raise HTTPException(status_code=404, detail="tournament not found")
    if tournament.status not in ("drafting", "active"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="trades only allowed during drafting/active",
        )

    if not await session.get(
        TournamentParticipant, (tournament_id, body.participant_user_id)
    ):
        raise HTTPException(status_code=400, detail="user is not a participant")

    sold = await session.get(RosterEntry, body.sell_entry_id)
    if (
        sold is None
        or sold.tournament_id != tournament_id
        or sold.participant_user_id != body.participant_user_id
    ):
        raise HTTPException(
            status_code=400, detail="sell entry not found for this participant"
        )
    if sold.released_before_day is not None:
        raise HTTPException(status_code=400, detail="that entry was already released")
    if body.effective_before_day <= sold.acquired_before_day:
        raise HTTPException(
            status_code=400,
            detail="trade day must be after acquisition day",
        )

    if body.buy_rikishi_id == sold.rikishi_id:
        raise HTTPException(status_code=400, detail="must trade for a different rikishi")

    price = await session.get(RikishiPrice, (tournament_id, body.buy_rikishi_id))
    if price is None:
        raise HTTPException(
            status_code=400, detail="rikishi has no price for this tournament"
        )

    # The participant can't already hold this rikishi themselves.
    self_owner = await session.scalar(
        select(RosterEntry).where(
            RosterEntry.tournament_id == tournament_id,
            RosterEntry.rikishi_id == body.buy_rikishi_id,
            RosterEntry.released_before_day.is_(None),
            RosterEntry.participant_user_id == body.participant_user_id,
        )
    )
    if self_owner is not None:
        raise HTTPException(
            status_code=409, detail="participant already owns this rikishi"
        )

    sale_price = sold.purchase_price_pence // 2

    # Budget check: new effective spend.
    current_spent = await _effective_spent(
        session, tournament_id, body.participant_user_id
    )
    new_spent = current_spent - sale_price + price.price_pence
    if new_spent > tournament.budget_pence:
        raise HTTPException(
            status_code=409,
            detail=(
                f"over budget: would be £{new_spent / 100:.2f} > "
                f"£{tournament.budget_pence / 100:.2f}"
            ),
        )

    if not body.force:
        # Compute warnings as if the trade had already been applied:
        # release the sold entry, then check the resulting set.
        all_active = (
            await session.scalars(
                select(RosterEntry).where(
                    RosterEntry.tournament_id == tournament_id,
                    RosterEntry.released_before_day.is_(None),
                )
            )
        ).all()
        after_release = [e for e in all_active if e.id != sold.id]
        user_index = await load_user_index(session, tournament_id)
        rikishi_ids = sorted(
            {e.rikishi_id for e in after_release} | {body.buy_rikishi_id}
        )
        rikishi_names = await load_rikishi_names(session, rikishi_ids)
        new_warnings = warnings_from_added(
            after_release,
            tournament,
            user_index,
            rikishi_names,
            body.participant_user_id,
            body.buy_rikishi_id,
        )
        if new_warnings:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=detail_for_warnings(new_warnings),
            )

    # Apply.
    sold.sale_price_pence = sale_price
    sold.released_before_day = body.effective_before_day

    bought = RosterEntry(
        tournament_id=tournament_id,
        participant_user_id=body.participant_user_id,
        rikishi_id=body.buy_rikishi_id,
        purchase_price_pence=price.price_pence,
        acquired_via="trade",
        acquired_before_day=body.effective_before_day,
    )
    session.add(bought)
    await session.flush()

    trade = Trade(
        tournament_id=tournament_id,
        participant_user_id=body.participant_user_id,
        sold_entry_id=sold.id,
        bought_entry_id=bought.id,
        effective_before_day=body.effective_before_day,
        note=body.note,
    )
    session.add(trade)
    await session.commit()
    await session.refresh(trade)
    return await _resolve_trade(session, trade)
