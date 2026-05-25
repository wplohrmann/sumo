"""Shared-pick rule helpers.

Two-tier league rules around how many participants may own the same
rikishi simultaneously, and how much overlap is allowed between any
pair of participants:

  * Two participants may share at most 2 rikishi (pair overlap).
  * A rikishi can be owned by at most 2 participants before trading
    opens; 3 once the tournament is active.

These limits are surfaced as warnings rather than hard rejections — the
admin can pass `force=true` to push a pick or trade through anyway.
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass

from app.db.models import AppUser, RosterEntry, Tournament


MAX_PAIR_OVERLAP = 2


def max_owners_per_rikishi(status: str) -> int:
    """Cap on simultaneous owners of a single rikishi.

    Drafting phase: 2. Once trading is open (`active`/`archived`): 3.
    """
    if status in ("setup", "drafting"):
        return 2
    return 3


@dataclass
class SharingWarning:
    code: str  # 'rikishi_over_capped' | 'pair_overlap_over_capped'
    message: str
    rikishi_id: int | None = None
    user_ids: list[str] | None = None


def _active_entries(entries: list[RosterEntry]) -> list[RosterEntry]:
    return [e for e in entries if e.released_before_day is None]


def _user_label(user_index: dict[uuid.UUID, str], uid: uuid.UUID) -> str:
    return user_index.get(uid, str(uid))


def _names_by_rikishi(
    entries: list[RosterEntry],
) -> dict[int, list[uuid.UUID]]:
    out: dict[int, list[uuid.UUID]] = defaultdict(list)
    for e in entries:
        out[e.rikishi_id].append(e.participant_user_id)
    return out


def _pair_overlap(
    entries: list[RosterEntry],
) -> dict[tuple[uuid.UUID, uuid.UUID], int]:
    rikishi_owners = _names_by_rikishi(entries)
    pair_counts: dict[tuple[uuid.UUID, uuid.UUID], int] = defaultdict(int)
    for owners in rikishi_owners.values():
        unique_owners = sorted(set(owners), key=str)
        for i in range(len(unique_owners)):
            for j in range(i + 1, len(unique_owners)):
                pair_counts[(unique_owners[i], unique_owners[j])] += 1
    return pair_counts


def compute_warnings(
    entries: list[RosterEntry],
    tournament: Tournament,
    user_index: dict[uuid.UUID, str],
    rikishi_names: dict[int, str | None],
) -> list[SharingWarning]:
    """Inspect a tournament's *active* roster entries and return any
    sharing-rule violations.
    """
    active = _active_entries(entries)
    cap = max_owners_per_rikishi(tournament.status)
    warnings: list[SharingWarning] = []

    rikishi_owners = _names_by_rikishi(active)
    for rid, owners in rikishi_owners.items():
        distinct = sorted(set(owners), key=str)
        if len(distinct) > cap:
            name = rikishi_names.get(rid) or f"#{rid}"
            who = ", ".join(_user_label(user_index, u) for u in distinct)
            warnings.append(
                SharingWarning(
                    code="rikishi_over_capped",
                    rikishi_id=rid,
                    user_ids=[str(u) for u in distinct],
                    message=(
                        f"{name} is owned by {len(distinct)} participants "
                        f"({who}); the limit is {cap}."
                    ),
                )
            )

    for (a, b), count in _pair_overlap(active).items():
        if count > MAX_PAIR_OVERLAP:
            warnings.append(
                SharingWarning(
                    code="pair_overlap_over_capped",
                    user_ids=[str(a), str(b)],
                    message=(
                        f"{_user_label(user_index, a)} and "
                        f"{_user_label(user_index, b)} share {count} rikishi; "
                        f"the limit is {MAX_PAIR_OVERLAP}."
                    ),
                )
            )

    return warnings


def warnings_from_added(
    existing_entries: list[RosterEntry],
    tournament: Tournament,
    user_index: dict[uuid.UUID, str],
    rikishi_names: dict[int, str | None],
    new_user_id: uuid.UUID,
    new_rikishi_id: int,
) -> list[SharingWarning]:
    """Return only the *new* warnings introduced by adding (user, rikishi)
    to the active set. Existing warnings are filtered out so the admin
    isn't re-prompted for violations they've already accepted.
    """
    before = {
        (w.code, w.rikishi_id, tuple(w.user_ids or ()))
        for w in compute_warnings(
            existing_entries, tournament, user_index, rikishi_names
        )
    }
    synthetic = RosterEntry(
        tournament_id=tournament.id,
        participant_user_id=new_user_id,
        rikishi_id=new_rikishi_id,
        purchase_price_pence=0,
        acquired_via="draft",
        acquired_before_day=1,
    )
    after = compute_warnings(
        existing_entries + [synthetic], tournament, user_index, rikishi_names
    )
    return [
        w
        for w in after
        if (w.code, w.rikishi_id, tuple(w.user_ids or ())) not in before
    ]


def detail_for_warnings(warnings: list[SharingWarning]) -> dict:
    return {
        "code": "sharing_warning",
        "message": "sharing rule violation; pass force=true to override",
        "warnings": [
            {
                "code": w.code,
                "message": w.message,
                "rikishi_id": w.rikishi_id,
                "user_ids": w.user_ids,
            }
            for w in warnings
        ],
    }


async def load_user_index(session, tournament_id) -> dict[uuid.UUID, str]:
    from sqlalchemy import select
    from app.db.models import TournamentParticipant

    rows = (
        await session.execute(
            select(AppUser.id, AppUser.display_name)
            .join(
                TournamentParticipant,
                TournamentParticipant.user_id == AppUser.id,
            )
            .where(TournamentParticipant.tournament_id == tournament_id)
        )
    ).all()
    return {uid: name for uid, name in rows}


async def load_rikishi_names(
    session, rikishi_ids: list[int]
) -> dict[int, str | None]:
    if not rikishi_ids:
        return {}
    from sqlalchemy import select
    from app.db.models import Rikishi

    rows = (
        await session.execute(
            select(Rikishi.id, Rikishi.name).where(Rikishi.id.in_(rikishi_ids))
        )
    ).all()
    return {rid: name for rid, name in rows}
