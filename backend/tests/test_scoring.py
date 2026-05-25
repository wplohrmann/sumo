"""Scoring engine correctness tests.

Each test builds a small in-memory tournament and asserts the standings
output. The fixtures are intentionally explicit (no helpers that hide
the data) so a failing test is easy to debug.
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    AppUser,
    Basho,
    BashoRikishi,
    Match,
    Rikishi,
    RosterEntry,
    ScoreAdjustment,
    Tournament,
    TournamentAward,
    TournamentParticipant,
)
from app.scoring.engine import compute_standings


async def _make_world(db: AsyncSession) -> dict:
    """Set up a basho, 5 rikishi, 2 players, and a tournament.

    Returns a dict with named IDs for use in tests.
    """
    basho_id = "202405"
    db.add(Basho(id=basho_id, name="Tokyo"))
    riders = [
        (1, "Terunofuji"),
        (2, "Hoshoryu"),
        (3, "Kotonowaka"),
        (4, "Onosato"),
        (5, "Wakatakakage"),
        (6, "Tobizaru"),
        (7, "Endo"),
        (8, "Takarafuji"),
    ]
    for rid, name in riders:
        db.add(Rikishi(id=rid, name=name))
    await db.flush()
    for rid, _name in riders:
        db.add(
            BashoRikishi(
                basho_id=basho_id,
                rikishi_id=rid,
                rank=f"M{rid}",
                rank_value=rid,
                division="Makuuchi",
            )
        )

    alice = AppUser(display_name="Alice", role="viewer", token_hash="a" * 64)
    bob = AppUser(display_name="Bob", role="viewer", token_hash="b" * 64)
    db.add(alice)
    db.add(bob)
    await db.flush()

    tournament = Tournament(
        basho_id=basho_id,
        name="May 2024",
        status="active",
        budget_pence=5500,
        roster_size=4,
    )
    db.add(tournament)
    await db.flush()

    db.add(
        TournamentParticipant(tournament_id=tournament.id, user_id=alice.id)
    )
    db.add(TournamentParticipant(tournament_id=tournament.id, user_id=bob.id))
    await db.flush()

    return {
        "tournament_id": tournament.id,
        "alice_id": alice.id,
        "bob_id": bob.id,
        "basho_id": basho_id,
    }


def _draft(
    db: AsyncSession,
    tournament_id,
    participant_user_id,
    rikishi_id: int,
    price_pence: int = 1000,
) -> None:
    db.add(
        RosterEntry(
            tournament_id=tournament_id,
            participant_user_id=participant_user_id,
            rikishi_id=rikishi_id,
            purchase_price_pence=price_pence,
            acquired_via="draft",
            acquired_before_day=1,
        )
    )


def _match(
    db,
    basho_id: str,
    day: int,
    east_id: int,
    west_id: int,
    winner_id: int,
    division: str = "Makuuchi",
) -> None:
    db.add(
        Match(
            id=f"d{day}-{east_id}v{west_id}",
            basho_id=basho_id,
            rikishi1_id=east_id,
            rikishi2_id=west_id,
            winner_id=winner_id,
            kimarite="yorikiri",
            day=day,
            division=division,
        )
    )


@pytest.mark.asyncio
async def test_wins_and_scalps_basic(db):
    w = await _make_world(db)
    # Alice owns 1, 2. Bob owns 3, 4. Through day 2.
    _draft(db, w["tournament_id"], w["alice_id"], 1)
    _draft(db, w["tournament_id"], w["alice_id"], 2)
    _draft(db, w["tournament_id"], w["bob_id"], 3)
    _draft(db, w["tournament_id"], w["bob_id"], 4)

    # Day 1: Alice's 1 beats Bob's 3 (scalp+win for Alice). Alice's 2 beats
    # unowned rikishi 5 (just a win). Bob's 4 beats unowned 6.
    _match(db, w["basho_id"], 1, 1, 3, 1)
    _match(db, w["basho_id"], 1, 2, 5, 2)
    _match(db, w["basho_id"], 1, 4, 6, 4)

    # Day 2: Alice's 1 beats Alice's own 2 (would be impossible in real sumo,
    # but exercises the self-scalp-not-counted rule).
    _match(db, w["basho_id"], 2, 1, 2, 1)

    await db.commit()
    standings = await compute_standings(db, w["tournament_id"], through_day=2)

    by_name = {s.display_name: s for s in standings}
    # Alice: day 1 wins=2 (rikishi 1+2), scalps=1 (vs Bob's 3) ⇒ 4+1 = 5
    #        day 2 wins=1 (rikishi 1, self-match doesn't scalp) ⇒ 2
    #        total wins_points = 6, scalp_points = 1, total = 7
    assert by_name["Alice"].wins_points == 6
    assert by_name["Alice"].scalp_points == 1
    assert by_name["Alice"].total_points == 7

    # Bob: day 1 wins=1 (rikishi 4), no scalp (rikishi 6 unowned) ⇒ 2
    assert by_name["Bob"].wins_points == 2
    assert by_name["Bob"].scalp_points == 0
    assert by_name["Bob"].total_points == 2


@pytest.mark.asyncio
async def test_through_day_truncates(db):
    w = await _make_world(db)
    _draft(db, w["tournament_id"], w["alice_id"], 1)
    _match(db, w["basho_id"], 1, 1, 5, 1)
    _match(db, w["basho_id"], 2, 1, 6, 1)
    _match(db, w["basho_id"], 3, 1, 7, 1)
    await db.commit()

    s1 = (await compute_standings(db, w["tournament_id"], 1))[0]
    s2 = (await compute_standings(db, w["tournament_id"], 2))[0]
    s3 = (await compute_standings(db, w["tournament_id"], 3))[0]
    assert s1.wins_points == 2
    assert s2.wins_points == 4
    assert s3.wins_points == 6
    assert len(s1.by_day) == 1
    assert len(s2.by_day) == 2
    assert len(s3.by_day) == 3


@pytest.mark.asyncio
async def test_playoff_bouts_excluded(db):
    """Day 16 bouts must not score wins."""
    w = await _make_world(db)
    _draft(db, w["tournament_id"], w["alice_id"], 1)
    _match(db, w["basho_id"], 15, 1, 5, 1)  # regular win
    _match(db, w["basho_id"], 16, 1, 6, 1)  # playoff bout — must not score
    await db.commit()

    s = (await compute_standings(db, w["tournament_id"], through_day=15))[0]
    assert s.wins_points == 2  # only the day-15 win


@pytest.mark.asyncio
async def test_non_makuuchi_excluded(db):
    w = await _make_world(db)
    _draft(db, w["tournament_id"], w["alice_id"], 1)
    db.add(Rikishi(id=99, name="Some Juryo"))
    await db.flush()
    db.add(
        BashoRikishi(
            basho_id=w["basho_id"],
            rikishi_id=99,
            rank="J1",
            rank_value=200,
            division="Juryo",
        )
    )
    await db.flush()
    _match(db, w["basho_id"], 1, 1, 99, 1, division="Juryo")
    await db.commit()

    s = (await compute_standings(db, w["tournament_id"], 5))[0]
    assert s.wins_points == 0


@pytest.mark.asyncio
async def test_ownership_changes_via_trade(db):
    """A trade mid-tournament moves both wins and scalps to the new owner."""
    w = await _make_world(db)
    # Alice drafts rikishi 1; before day 3 she trades it to Bob.
    db.add(
        RosterEntry(
            tournament_id=w["tournament_id"],
            participant_user_id=w["alice_id"],
            rikishi_id=1,
            purchase_price_pence=2000,
            acquired_via="draft",
            acquired_before_day=1,
            sale_price_pence=1000,
            released_before_day=3,
        )
    )
    db.add(
        RosterEntry(
            tournament_id=w["tournament_id"],
            participant_user_id=w["bob_id"],
            rikishi_id=1,
            purchase_price_pence=2000,
            acquired_via="trade",
            acquired_before_day=3,
        )
    )
    # Wins on day 1 and day 4 — first goes to Alice, second to Bob.
    _match(db, w["basho_id"], 1, 1, 5, 1)
    _match(db, w["basho_id"], 4, 1, 6, 1)
    await db.commit()

    standings = await compute_standings(db, w["tournament_id"], 5)
    by_name = {s.display_name: s for s in standings}
    assert by_name["Alice"].wins_points == 2
    assert by_name["Bob"].wins_points == 2


@pytest.mark.asyncio
async def test_self_scalp_not_counted(db):
    """If both rikishi in a match belong to the same player, no scalp."""
    w = await _make_world(db)
    _draft(db, w["tournament_id"], w["alice_id"], 1)
    _draft(db, w["tournament_id"], w["alice_id"], 2)
    _match(db, w["basho_id"], 1, 1, 2, 1)
    await db.commit()
    s = (await compute_standings(db, w["tournament_id"], 1))[0]
    assert s.wins_points == 2
    assert s.scalp_points == 0


@pytest.mark.asyncio
async def test_awards_and_yusho(db):
    w = await _make_world(db)
    _draft(db, w["tournament_id"], w["alice_id"], 1)  # will win yusho (+2)
    _draft(db, w["tournament_id"], w["alice_id"], 2)  # will get shukun (+1)
    _draft(db, w["tournament_id"], w["bob_id"], 3)  # playoff loser (+1)

    db.add(
        TournamentAward(
            tournament_id=w["tournament_id"], rikishi_id=1, kind="yusho"
        )
    )
    db.add(
        TournamentAward(
            tournament_id=w["tournament_id"], rikishi_id=2, kind="shukun"
        )
    )
    db.add(
        TournamentAward(
            tournament_id=w["tournament_id"], rikishi_id=3, kind="playoff"
        )
    )
    await db.commit()

    # Through day 14: awards don't count yet.
    s14 = {s.display_name: s for s in await compute_standings(db, w["tournament_id"], 14)}
    assert s14["Alice"].award_points == 0
    assert s14["Bob"].award_points == 0

    # Through day 15: awards count.
    s15 = {s.display_name: s for s in await compute_standings(db, w["tournament_id"], 15)}
    assert s15["Alice"].award_points == 2 + 1  # yusho + shukun
    assert s15["Bob"].award_points == 1  # playoff


@pytest.mark.asyncio
async def test_score_adjustments(db):
    w = await _make_world(db)
    _draft(db, w["tournament_id"], w["alice_id"], 1)
    db.add(
        ScoreAdjustment(
            tournament_id=w["tournament_id"],
            participant_user_id=w["alice_id"],
            day=3,
            points=5,
            reason="bonus",
        )
    )
    db.add(
        ScoreAdjustment(
            tournament_id=w["tournament_id"],
            participant_user_id=w["alice_id"],
            day=None,
            points=-2,
            reason="penalty",
        )
    )
    await db.commit()

    s2 = (await compute_standings(db, w["tournament_id"], 2))[0]
    assert s2.adjustment_points == 0
    s5 = (await compute_standings(db, w["tournament_id"], 5))[0]
    assert s5.adjustment_points == 5  # day-3 adjustment visible; whole-tournament not yet
    s15 = (await compute_standings(db, w["tournament_id"], 15))[0]
    assert s15.adjustment_points == 5 - 2


@pytest.mark.asyncio
async def test_shared_picks_split_wins_and_scalps(db):
    """When two players share a rikishi, both get the win + scalp credit."""
    w = await _make_world(db)
    # Alice and Bob both own rikishi 1 (the winner).
    _draft(db, w["tournament_id"], w["alice_id"], 1)
    _draft(db, w["tournament_id"], w["bob_id"], 1)
    # Bob also owns rikishi 3 (the loser).
    _draft(db, w["tournament_id"], w["bob_id"], 3)

    _match(db, w["basho_id"], 1, 1, 3, 1)  # rikishi 1 beats rikishi 3
    await db.commit()

    standings = {
        s.display_name: s
        for s in await compute_standings(db, w["tournament_id"], 1)
    }
    # Alice owns the winner only → +2 win, +1 scalp (Bob owns the loser).
    assert standings["Alice"].wins_points == 2
    assert standings["Alice"].scalp_points == 1
    # Bob owns winner AND loser → +2 win for the winner ownership, but the
    # only loser-owner (himself) is filtered as a self-scalp → 0 scalps.
    assert standings["Bob"].wins_points == 2
    assert standings["Bob"].scalp_points == 0


@pytest.mark.asyncio
async def test_sort_order(db):
    """Standings sorted by total desc, then wins, scalps, name."""
    w = await _make_world(db)
    # Alice: 1 win
    _draft(db, w["tournament_id"], w["alice_id"], 1)
    _match(db, w["basho_id"], 1, 1, 5, 1)
    # Bob: 2 wins, no scalps
    _draft(db, w["tournament_id"], w["bob_id"], 3)
    _draft(db, w["tournament_id"], w["bob_id"], 4)
    _match(db, w["basho_id"], 1, 3, 6, 3)
    _match(db, w["basho_id"], 1, 4, 7, 4)
    await db.commit()

    standings = await compute_standings(db, w["tournament_id"], 1)
    assert [s.display_name for s in standings] == ["Bob", "Alice"]
