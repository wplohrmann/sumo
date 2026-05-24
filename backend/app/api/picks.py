"""Draft picks and roster board.

A "pick" is a roster_entry with acquired_via='draft'. The roster board
endpoint returns each participant's currently-held entries (whether they
were drafted or acquired via trade) along with budget math.
"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.deps import current_admin, current_user
from app.db.models import (
    AppUser,
    Rikishi,
    RikishiPrice,
    RosterEntry,
    Tournament,
    TournamentParticipant,
)
from app.db.session import get_session

router = APIRouter(prefix="/tournaments", tags=["picks"])


class PickIn(BaseModel):
    participant_user_id: uuid.UUID
    rikishi_id: int


class RosterEntryOut(BaseModel):
    id: str
    rikishi_id: int
    rikishi_name: str | None
    purchase_price_pence: int
    acquired_via: str
    acquired_before_day: int


class ParticipantRoster(BaseModel):
    user_id: str
    display_name: str
    spent_pence: int
    remaining_pence: int
    entries: list[RosterEntryOut]


class RosterBoard(BaseModel):
    budget_pence: int
    roster_size: int
    rosters: list[ParticipantRoster]


@router.get("/{tournament_id}/picks", response_model=RosterBoard)
async def list_picks(
    tournament_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _user: AppUser = Depends(current_user),
) -> RosterBoard:
    t = await session.get(Tournament, tournament_id)
    if t is None:
        raise HTTPException(status_code=404, detail="tournament not found")

    participants = (
        await session.execute(
            select(AppUser, TournamentParticipant.draft_seed)
            .join(
                TournamentParticipant,
                TournamentParticipant.user_id == AppUser.id,
            )
            .where(TournamentParticipant.tournament_id == tournament_id)
            .order_by(AppUser.display_name)
        )
    ).all()

    # Pull every entry (active and released) so we can compute the sunk
    # cost from past trades (half-loss).
    entries = (
        await session.execute(
            select(RosterEntry, Rikishi.name)
            .join(Rikishi, Rikishi.id == RosterEntry.rikishi_id)
            .where(RosterEntry.tournament_id == tournament_id)
            .order_by(RosterEntry.created_at)
        )
    ).all()

    by_user: dict[uuid.UUID, list[tuple[RosterEntry, str | None]]] = {}
    for entry, name in entries:
        by_user.setdefault(entry.participant_user_id, []).append((entry, name))

    rosters = []
    for user, _seed in participants:
        all_entries = by_user.get(user.id, [])
        active = [(e, name) for e, name in all_entries if e.released_before_day is None]
        spent = 0
        for e, _ in all_entries:
            if e.released_before_day is None:
                spent += e.purchase_price_pence
            else:
                spent += e.purchase_price_pence - (e.sale_price_pence or 0)
        rosters.append(
            ParticipantRoster(
                user_id=str(user.id),
                display_name=user.display_name,
                spent_pence=spent,
                remaining_pence=t.budget_pence - spent,
                entries=[
                    RosterEntryOut(
                        id=str(e.id),
                        rikishi_id=e.rikishi_id,
                        rikishi_name=name,
                        purchase_price_pence=e.purchase_price_pence,
                        acquired_via=e.acquired_via,
                        acquired_before_day=e.acquired_before_day,
                    )
                    for e, name in active
                ],
            )
        )

    return RosterBoard(
        budget_pence=t.budget_pence,
        roster_size=t.roster_size,
        rosters=rosters,
    )


@router.post(
    "/{tournament_id}/picks",
    response_model=RosterEntryOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_pick(
    tournament_id: uuid.UUID,
    body: PickIn,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> RosterEntryOut:
    t = await session.get(Tournament, tournament_id)
    if t is None:
        raise HTTPException(status_code=404, detail="tournament not found")
    if t.status not in ("setup", "drafting"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="draft is closed",
        )

    participant = await session.get(
        TournamentParticipant, (tournament_id, body.participant_user_id)
    )
    if participant is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user is not a participant in this tournament",
        )

    price_row = await session.get(RikishiPrice, (tournament_id, body.rikishi_id))
    if price_row is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="rikishi has no price set for this tournament",
        )

    # All entries (active + released) for budget math; active subset for
    # roster limits.
    all_entries = (
        await session.execute(
            select(RosterEntry).where(
                RosterEntry.tournament_id == tournament_id,
                RosterEntry.participant_user_id == body.participant_user_id,
            )
        )
    ).scalars().all()
    active = [e for e in all_entries if e.released_before_day is None]

    if len(active) >= t.roster_size:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"roster full ({t.roster_size})",
        )
    if any(e.rikishi_id == body.rikishi_id for e in active):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="participant already owns this rikishi",
        )
    # No two participants can hold the same rikishi at once.
    other_owner = await session.scalar(
        select(RosterEntry).where(
            RosterEntry.tournament_id == tournament_id,
            RosterEntry.rikishi_id == body.rikishi_id,
            RosterEntry.released_before_day.is_(None),
        )
    )
    if other_owner is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="another participant already owns this rikishi",
        )

    spent = 0
    for e in all_entries:
        if e.released_before_day is None:
            spent += e.purchase_price_pence
        else:
            spent += e.purchase_price_pence - (e.sale_price_pence or 0)
    if spent + price_row.price_pence > t.budget_pence:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"over budget: spent £{spent / 100:.2f} + "
                f"£{price_row.price_pence / 100:.2f} > "
                f"£{t.budget_pence / 100:.2f}"
            ),
        )

    entry = RosterEntry(
        tournament_id=tournament_id,
        participant_user_id=body.participant_user_id,
        rikishi_id=body.rikishi_id,
        purchase_price_pence=price_row.price_pence,
        acquired_via="draft",
        acquired_before_day=1,
    )
    session.add(entry)
    await session.commit()
    await session.refresh(entry)

    rikishi = await session.get(Rikishi, entry.rikishi_id)
    return RosterEntryOut(
        id=str(entry.id),
        rikishi_id=entry.rikishi_id,
        rikishi_name=rikishi.name if rikishi else None,
        purchase_price_pence=entry.purchase_price_pence,
        acquired_via=entry.acquired_via,
        acquired_before_day=entry.acquired_before_day,
    )


@router.delete(
    "/{tournament_id}/picks/{entry_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_pick(
    tournament_id: uuid.UUID,
    entry_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _admin: AppUser = Depends(current_admin),
) -> None:
    """Undo a draft pick. Only allowed during setup/drafting."""
    t = await session.get(Tournament, tournament_id)
    if t is None:
        raise HTTPException(status_code=404, detail="tournament not found")
    if t.status not in ("setup", "drafting"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="draft is closed; record a trade instead",
        )
    entry = await session.get(RosterEntry, entry_id)
    if entry is None or entry.tournament_id != tournament_id:
        raise HTTPException(status_code=404, detail="entry not found")
    if entry.acquired_via != "draft":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="only draft picks can be deleted; use trade for in-tournament moves",
        )
    await session.delete(entry)
    await session.commit()
