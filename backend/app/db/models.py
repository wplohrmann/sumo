"""All SQLAlchemy models live here so Alembic sees them in one place."""
from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import (
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


# ---------------------------------------------------------------------------
# Sumo data (mirrors the existing schema.sql)
# ---------------------------------------------------------------------------


class Basho(Base):
    __tablename__ = "basho"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)


class Rikishi(Base):
    __tablename__ = "rikishi"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    debut_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    birth_date: Mapped[date | None] = mapped_column(Date, nullable=True)


class Measurement(Base):
    __tablename__ = "measurement"

    rikishi_id: Mapped[int] = mapped_column(
        ForeignKey("rikishi.id"), primary_key=True
    )
    basho_id: Mapped[str] = mapped_column(ForeignKey("basho.id"), primary_key=True)
    height_cm: Mapped[int | None] = mapped_column(Integer, nullable=True)
    weight_kg: Mapped[int | None] = mapped_column(Integer, nullable=True)


class BashoRikishi(Base):
    __tablename__ = "basho_rikishi"

    basho_id: Mapped[str] = mapped_column(ForeignKey("basho.id"), primary_key=True)
    rikishi_id: Mapped[int] = mapped_column(
        ForeignKey("rikishi.id"), primary_key=True
    )
    rank: Mapped[str | None] = mapped_column(Text, nullable=True)
    rank_value: Mapped[int | None] = mapped_column(Integer, nullable=True)
    division: Mapped[str | None] = mapped_column(Text, nullable=True)


class Match(Base):
    __tablename__ = "match"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    basho_id: Mapped[str] = mapped_column(ForeignKey("basho.id"))
    rikishi1_id: Mapped[int] = mapped_column(ForeignKey("rikishi.id"))
    rikishi2_id: Mapped[int] = mapped_column(ForeignKey("rikishi.id"))
    winner_id: Mapped[int | None] = mapped_column(
        ForeignKey("rikishi.id"), nullable=True
    )
    kimarite: Mapped[str | None] = mapped_column(Text, nullable=True)
    day: Mapped[int] = mapped_column(Integer)
    match_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    division: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_match_basho_day", "basho_id", "day"),
        Index("idx_match_basho_division_day", "basho_id", "division", "day"),
    )


# ---------------------------------------------------------------------------
# Fantasy app
# ---------------------------------------------------------------------------


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )


class AppUser(Base):
    __tablename__ = "app_user"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    display_name: Mapped[str] = mapped_column(Text, unique=True)
    role: Mapped[str] = mapped_column(String(16))  # 'admin' | 'viewer'
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    spoiler_day: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Tournament(Base):
    __tablename__ = "tournament"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    basho_id: Mapped[str] = mapped_column(ForeignKey("basho.id"), unique=True)
    name: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16))  # 'setup' | 'drafting' | 'active' | 'archived'
    budget_pence: Mapped[int] = mapped_column(Integer, default=5500)
    roster_size: Mapped[int] = mapped_column(Integer, default=4)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        # Only one non-archived tournament at a time. Partial unique index
        # works in both Postgres and SQLite.
        Index(
            "idx_one_live_tournament",
            "status",
            unique=True,
            postgresql_where=text("status IN ('setup', 'drafting', 'active')"),
            sqlite_where=text("status IN ('setup', 'drafting', 'active')"),
        ),
    )


class TournamentParticipant(Base):
    __tablename__ = "tournament_participant"

    tournament_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tournament.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("app_user.id", ondelete="CASCADE"), primary_key=True
    )
    draft_seed: Mapped[int | None] = mapped_column(Integer, nullable=True)

    user: Mapped[AppUser] = relationship()


class RikishiPrice(Base):
    __tablename__ = "rikishi_price"

    tournament_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tournament.id", ondelete="CASCADE"), primary_key=True
    )
    rikishi_id: Mapped[int] = mapped_column(
        ForeignKey("rikishi.id"), primary_key=True
    )
    price_pence: Mapped[int] = mapped_column(Integer)


class RosterEntry(Base):
    __tablename__ = "roster_entry"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tournament_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tournament.id", ondelete="CASCADE")
    )
    participant_user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("app_user.id", ondelete="CASCADE")
    )
    rikishi_id: Mapped[int] = mapped_column(ForeignKey("rikishi.id"))
    purchase_price_pence: Mapped[int] = mapped_column(Integer)
    acquired_via: Mapped[str] = mapped_column(String(16))  # 'draft' | 'trade'
    acquired_before_day: Mapped[int] = mapped_column(Integer)
    sale_price_pence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    released_before_day: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index(
            "idx_roster_active",
            "tournament_id",
            "participant_user_id",
            postgresql_where=text("released_before_day IS NULL"),
            sqlite_where=text("released_before_day IS NULL"),
        ),
    )


class Trade(Base):
    __tablename__ = "trade"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tournament_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tournament.id", ondelete="CASCADE")
    )
    participant_user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("app_user.id", ondelete="CASCADE")
    )
    sold_entry_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("roster_entry.id"))
    bought_entry_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("roster_entry.id"))
    effective_before_day: Mapped[int] = mapped_column(Integer)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ScoreAdjustment(Base):
    __tablename__ = "score_adjustment"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tournament_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tournament.id", ondelete="CASCADE")
    )
    participant_user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("app_user.id", ondelete="CASCADE")
    )
    day: Mapped[int | None] = mapped_column(Integer, nullable=True)
    points: Mapped[int] = mapped_column(Integer)
    reason: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class TournamentAward(Base):
    __tablename__ = "tournament_award"

    tournament_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tournament.id", ondelete="CASCADE"), primary_key=True
    )
    rikishi_id: Mapped[int] = mapped_column(
        ForeignKey("rikishi.id"), primary_key=True
    )
    kind: Mapped[str] = mapped_column(String(16), primary_key=True)
    # 'yusho' | 'playoff' | 'shukun' | 'kanto' | 'gino'


# Keep an explicit list for tests/utilities that want to iterate tables.
ALL_MODELS = [
    Basho,
    Rikishi,
    Measurement,
    BashoRikishi,
    Match,
    AppUser,
    Tournament,
    TournamentParticipant,
    RikishiPrice,
    RosterEntry,
    Trade,
    ScoreAdjustment,
    TournamentAward,
]

__all__ = [m.__name__ for m in ALL_MODELS]


# Avoid unused-symbol warnings in env.py.
UniqueConstraint  # noqa: B018
_uuid_pk  # noqa: B018
