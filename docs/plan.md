# Fantasy Sumo — Implementation Plan

Phased plan. Each phase ends in something runnable. Goal: be ready for
the next basho with at least Phase 4 shipped.

## Phase 0 — Repo scaffolding

- [ ] Add `pyproject.toml` (or keep `requirements.txt`) with FastAPI,
      SQLAlchemy, asyncpg, Alembic, APScheduler, Pydantic v2.
- [ ] Create `backend/` package (move existing `sumo/` underneath as
      `backend/sumo/` or rename). Layout:
      ```
      backend/
        app/
          main.py        # FastAPI factory
          api/           # routers grouped by resource
          db/            # SQLAlchemy models, session
          scoring/       # scoring engine
          sync/          # sumo-api pull (port of download_data.py)
          auth/          # token middleware
          settings.py
        alembic/
        tests/
      ```
- [ ] Create `frontend/` with Vite + React + TS template.
- [ ] `docker-compose.yml` for local Postgres + adminer.
- [ ] `Makefile` (or `justfile`) with `dev`, `lint`, `test`, `migrate`.

## Phase 1 — Database & sumo-api sync (port existing)

- [ ] Define SQLAlchemy models mirroring existing tables (`basho`,
      `rikishi`, `measurement`, `basho_rikishi`, `match`).
- [ ] Alembic initial migration matching `schema.sql` but in Postgres
      (types: `INT` for ids, `DATE` for dates).
- [ ] Port `download_data.py` to async SQLAlchemy. Keep the
      `maybe_insert_*` idempotent pattern.
- [ ] One-shot CLI: `python -m app.sync.bootstrap` to populate history.
- [ ] APScheduler job that runs every hour during an active basho.
- [ ] Test: run the script against a local Postgres, confirm the same
      counts as the existing SQLite DB.

## Phase 2 — Auth + users + tournaments (skeleton API)

- [ ] Migrations for `app_user`, `tournament`, `tournament_participant`.
- [ ] Token middleware: read cookie, look up user, attach to request.
      Admin password from env var; admin login sets the same cookie with
      an admin token in the DB.
- [ ] Endpoints:
      - `POST /api/auth/login`, `POST /api/auth/logout`, `GET /api/me`
      - `POST /api/tournaments` (admin), `GET /api/tournaments/current`
      - `POST /api/tournaments/{id}/participants` (admin) — creates a
        viewer `app_user` and returns the bearer token once (rotatable
        later).
- [ ] Frontend: `/login` page, auth-aware shell, "you're logged in as
      X" header. No real content yet.

## Phase 3 — Draft entry & roster display

- [ ] Migrations for `rikishi_price`, `roster_entry`.
- [ ] Endpoints:
      - `GET /api/tournaments/{id}/rikishi` (joins `basho_rikishi`
        filtered to Makuuchi + `rikishi_price`)
      - `PUT /api/tournaments/{id}/rikishi/{rid}/price` (admin)
      - `GET /api/tournaments/{id}/picks`
      - `POST /api/tournaments/{id}/picks` (admin) — validates budget
        and roster size, creates a `roster_entry` with
        `acquired_via='draft'`, `acquired_before_day=1`.
- [ ] Frontend:
      - `/admin` page: pricing table with editable cells, draft pick
        form (player → rikishi dropdown autofilled with current price).
      - `/draft` page: read-only grid showing every participant's 4
        slots and £ remaining.
- [ ] Test: invariant — can't pick 5 rikishi, can't overspend.

## Phase 4 — Scoring engine & standings (MVP-complete)

- [ ] Migration for `score_adjustment`, `tournament_award`.
- [ ] Scoring engine (`app/scoring/engine.py`):
      - Wins: count `match` rows in Makuuchi where the rikishi is owned.
      - Scalps: same, but `winner_id` is owned by player A and loser's
        owner ≠ A (NULL is fine; just not self).
      - Awards: yusho=2, playoff=1, sansho kinds=1 each, attributed via
        roster ownership at end of day 15.
      - Adjustments: signed sum.
      - All filtered by `through_day`. Awards only count if
        `through_day >= 15` (configurable, defaults to 15).
- [ ] Endpoints:
      - `GET /api/tournaments/{id}/standings?through_day=N`
      - `GET /api/tournaments/{id}/days/{n}`
      - `POST /api/tournaments/{id}/adjustments` (admin)
      - `PUT /api/tournaments/{id}/awards` (admin)
      - `PATCH /api/me/spoiler-day`
- [ ] Frontend:
      - `/` dashboard: leaderboard + my roster.
      - `/standings` with `through_day` slider.
      - `/day/:n` table of bouts.
      - Spoiler-day selector in header that pins `through_day` for the
        whole session.
- [ ] **Tests** (this is the riskiest piece):
      - Fixture tournament with hand-picked matches.
      - Verify win count, scalp count, self-scalp exclusion, ownership
        across a mid-tournament trade, award totals at day 15.

## Phase 5 — Trading

- [ ] Migration for `trade`.
- [ ] Endpoint `POST /api/tournaments/{id}/trades` (admin): atomically
      closes `sold_entry` (sets `sale_price_pence`,
      `released_before_day`) and opens a new `roster_entry` with
      `acquired_via='trade'`. Adds the half-price refund to budget by
      not consuming the full new price.
- [ ] Half-value rule: `sale_price = floor(purchase_price / 2)` in
      pence.
- [ ] Frontend admin trade form: pick participant, day, sell rikishi,
      buy rikishi, preview budget impact, submit.
- [ ] `/trades` page chronological list.
- [ ] Tests: trade math, ownership-on-day correctness in scoring.

## Phase 6 — Polish & deploy

- [ ] Archive flow (`PATCH /api/tournaments/{id}/status` → `archived`).
- [ ] Admin "rotate token" for a participant.
- [ ] Mobile-friendly CSS pass.
- [ ] Deploy backend container to Fly.io / Railway pointing at
      Supabase. Static frontend served from the same container.
- [ ] Set up Supabase backups & basic monitoring (uptime check).

## Phase 7+ — v2 ideas (not committed)

- Live scoring via WebSockets, faster polling on match days.
- Auto-suggested rikishi prices from rank.
- Read-only share link for spectators.
- WhatsApp/email notifications when a day closes.
- "What-if" tool: see standings as if a different pick had been made.

## Stretch: reuse the ML predictions

The repo already has Elo + XGBoost models predicting match outcomes
(`sumo/match_prediction.py`). A fun v2 feature: pre-tournament, show
each rikishi's projected wins from the model so players can draft
informedly. Decoupled from the core app; would live behind a feature
flag.
