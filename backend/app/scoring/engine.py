"""Pure scoring engine.

Given a tournament and a `through_day` cutoff, return per-participant
standings with a breakdown by source and a per-day timeline. Playoff
bouts do NOT contribute to win/scalp counts (we filter day <= 15).
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    AppUser,
    Match,
    RosterEntry,
    ScoreAdjustment,
    Tournament,
    TournamentAward,
    TournamentParticipant,
)


REGULAR_DAYS = 15  # bouts on days 1..15 score; anything beyond is playoff
WIN_POINTS = 2
SCALP_POINTS = 1
YUSHO_POINTS = 2
PLAYOFF_POINTS = 1
SANSHO_POINTS = 1
SANSHO_KINDS = {"shukun", "kanto", "gino"}


@dataclass
class DayBreakdown:
    day: int
    wins: int = 0
    scalps: int = 0
    wins_points: int = 0
    scalp_points: int = 0
    adjustment_points: int = 0


@dataclass
class ParticipantStanding:
    user_id: uuid.UUID
    display_name: str
    total_points: int = 0
    wins_points: int = 0
    scalp_points: int = 0
    award_points: int = 0
    adjustment_points: int = 0
    by_day: list[DayBreakdown] = field(default_factory=list)


def _entry_owner_on_day(entries: list[RosterEntry], rikishi_id: int, day: int) -> uuid.UUID | None:
    """Return participant_user_id owning `rikishi_id` on `day`, or None."""
    for e in entries:
        if e.rikishi_id != rikishi_id:
            continue
        if e.acquired_before_day > day:
            continue
        if e.released_before_day is not None and e.released_before_day <= day:
            continue
        return e.participant_user_id
    return None


async def compute_standings(
    session: AsyncSession,
    tournament_id: uuid.UUID,
    through_day: int,
) -> list[ParticipantStanding]:
    tournament = await session.get(Tournament, tournament_id)
    if tournament is None:
        raise ValueError("tournament not found")

    through_day = max(0, min(through_day, REGULAR_DAYS))

    participants = (
        await session.execute(
            select(AppUser)
            .join(
                TournamentParticipant,
                TournamentParticipant.user_id == AppUser.id,
            )
            .where(TournamentParticipant.tournament_id == tournament_id)
        )
    ).scalars().all()

    entries = (
        await session.scalars(
            select(RosterEntry).where(RosterEntry.tournament_id == tournament_id)
        )
    ).all()

    matches = (
        await session.scalars(
            select(Match).where(
                Match.basho_id == tournament.basho_id,
                Match.division == "Makuuchi",
                Match.day <= through_day,
                Match.day <= REGULAR_DAYS,
            )
        )
    ).all()

    # Per-user, per-day { 'wins': int, 'scalps': int }
    by_user_day: dict[uuid.UUID, dict[int, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {"wins": 0, "scalps": 0})
    )

    for m in matches:
        if m.winner_id is None:
            continue
        winner_owner = _entry_owner_on_day(entries, m.winner_id, m.day)
        loser_id = (
            m.rikishi2_id if m.winner_id == m.rikishi1_id else m.rikishi1_id
        )
        loser_owner = _entry_owner_on_day(entries, loser_id, m.day)
        if winner_owner is not None:
            by_user_day[winner_owner][m.day]["wins"] += 1
            if loser_owner is not None and loser_owner != winner_owner:
                by_user_day[winner_owner][m.day]["scalps"] += 1

    # Adjustments
    adjustments = (
        await session.scalars(
            select(ScoreAdjustment).where(
                ScoreAdjustment.tournament_id == tournament_id
            )
        )
    ).all()
    adj_by_user_day: dict[uuid.UUID, dict[int, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for a in adjustments:
        if a.day is None:
            # Whole-tournament adjustment; only count when we've seen the full
            # tournament. Bucket onto day 15 in the breakdown.
            if through_day >= REGULAR_DAYS:
                adj_by_user_day[a.participant_user_id][REGULAR_DAYS] += a.points
        elif a.day <= through_day:
            adj_by_user_day[a.participant_user_id][a.day] += a.points

    # Awards (yusho/playoff/sansho)
    award_totals: dict[uuid.UUID, int] = defaultdict(int)
    if through_day >= REGULAR_DAYS:
        awards = (
            await session.scalars(
                select(TournamentAward).where(
                    TournamentAward.tournament_id == tournament_id
                )
            )
        ).all()
        for a in awards:
            owner = _entry_owner_on_day(entries, a.rikishi_id, REGULAR_DAYS)
            if owner is None:
                continue
            if a.kind == "yusho":
                award_totals[owner] += YUSHO_POINTS
            elif a.kind == "playoff":
                award_totals[owner] += PLAYOFF_POINTS
            elif a.kind in SANSHO_KINDS:
                award_totals[owner] += SANSHO_POINTS

    standings: list[ParticipantStanding] = []
    for user in participants:
        s = ParticipantStanding(
            user_id=user.id,
            display_name=user.display_name,
        )
        for d in range(1, through_day + 1):
            ud = by_user_day.get(user.id, {}).get(d, {"wins": 0, "scalps": 0})
            wp = ud["wins"] * WIN_POINTS
            sp = ud["scalps"] * SCALP_POINTS
            ap = adj_by_user_day.get(user.id, {}).get(d, 0)
            s.wins_points += wp
            s.scalp_points += sp
            s.adjustment_points += ap
            s.by_day.append(
                DayBreakdown(
                    day=d,
                    wins=ud["wins"],
                    scalps=ud["scalps"],
                    wins_points=wp,
                    scalp_points=sp,
                    adjustment_points=ap,
                )
            )
        s.award_points = award_totals.get(user.id, 0)
        s.total_points = (
            s.wins_points + s.scalp_points + s.award_points + s.adjustment_points
        )
        standings.append(s)

    # Sort: total desc, wins desc, scalps desc, then alpha as a stable tiebreaker.
    standings.sort(
        key=lambda s: (
            -s.total_points,
            -s.wins_points,
            -s.scalp_points,
            s.display_name.lower(),
        )
    )
    return standings
