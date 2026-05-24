# Fantasy Sumo — Design Document

## 1. Overview

A small web app for a friend group's fantasy sumo league. One admin drives
all state-changing operations (draft picks, trades, prize awards); everyone
else gets a read-only view with a per-user spoiler cutoff.

Scope: **one active tournament at a time**. Older tournaments are archived
as read-only snapshots. The next basho is the primary target.

### Scoring rules (recap)

For each tournament, every participant picks 4 rikishi from Makuuchi with a
£55 total budget. Points are awarded as follows:

| Event                                            | Points |
| ------------------------------------------------ | ------ |
| Win in a Makuuchi bout (per win, per rikishi)    | 2      |
| Win where opponent is picked by another player[^1] | 1      |
| Rikishi reaches the yusho playoff                | 1      |
| Rikishi wins the yusho (replaces playoff point)  | 2      |
| Each special prize (sansho) won by a rikishi     | 1      |

[^1]: "Scalp" bonus — self-scalps (beating a rikishi you also picked) do not
count. Two players can both claim the scalp if they each picked the loser
(but the win itself still scores its own 2 points, unaffected).

The "yusho playoff" rule: a rikishi who finishes day 15 tied for first and
loses the playoff scores +1; the winner scores +2 (not +1 +2). The base 2/win
applies to all bouts including playoff bouts.

### Trading

Before a given day's bouts, the admin can record a trade for a participant:
sell one rikishi for half its purchase price (rounded down to integer £),
buy any other rikishi at its list price. Trade timing is honour-system —
admin enters the day the trade applies before and the app trusts it.

## 2. Roles & auth

Two roles, **admin** and **viewer**.

- **Admin** is a single principal. Authenticates with a separate admin
  password (env-configured). Has full write access.
- **Viewers** are pre-created by the admin. Each viewer gets a unique
  bearer token (a short URL-safe random string) which they bookmark. No
  signup flow, no email, no passwords.
- Sessions are HTTP-only cookies storing the bearer token. Tokens are
  rotatable from the admin UI in case one leaks.

The app does not attempt to be hostile-resistant — invite tokens are
unguessable but the trust model assumes the friend group.

## 3. Architecture

```
┌────────────────────────┐      ┌─────────────────────────┐
│  React + Vite + TS     │◀────▶│  FastAPI (Python)       │
│  (SPA, served static)  │ HTTP │  - REST API             │
└────────────────────────┘      │  - Scoring engine       │
                                │  - Cron: sumo-api sync  │
                                └───────────┬─────────────┘
                                            │ SQLAlchemy
                                            ▼
                                  ┌─────────────────────┐
                                  │  Postgres           │
                                  │  (Supabase in prod) │
                                  └─────────────────────┘
```

### Components

- **Frontend** — React 18 + Vite + TypeScript. React Query for server
  state. Plain CSS or Tailwind (decide in v0). Built static assets are
  served by FastAPI in prod (or by Supabase/Vercel if we split deploys).
- **Backend** — FastAPI, SQLAlchemy 2.x async, Pydantic v2. Single Python
  package `sumo/` (extending the existing one). Migrations via Alembic.
- **Data sync** — the existing `download_data.py` is refactored to write
  to Postgres instead of SQLite. A daily cron (APScheduler in-process, or
  a Supabase scheduled function) calls it during the active tournament.
- **Scoring engine** — pure function in Python that takes a tournament
  ID and a `through_day` integer and returns the standings breakdown.
  No caching in v1; recompute on every request.

### Deployment

- **Dev** — local Postgres in Docker, FastAPI via `uvicorn --reload`,
  Vite dev server with proxy to backend.
- **Prod** — Supabase Postgres. FastAPI on Fly.io / Railway / a small VM
  (any container host). Frontend bundle served from the same container at
  `/`. Daily cron via APScheduler started inside the FastAPI process
  (fine for one replica; if we ever scale, move to a separate worker).

## 4. Data model

The existing schema (`sumo/schema.sql`) covers external sumo data:
`basho`, `rikishi`, `measurement`, `basho_rikishi`, `match`. These are
ported to Postgres unchanged in spirit.

New tables for the fantasy app:

### `app_user`
| column         | type        | notes                                 |
| -------------- | ----------- | ------------------------------------- |
| id             | uuid PK     |                                       |
| display_name   | text        | unique within active tournament       |
| role           | text        | `admin` \| `viewer`                   |
| token_hash     | text        | sha256 of bearer token (admin password for admin) |
| spoiler_day    | int null    | per-user cap; null = show everything  |
| created_at     | timestamptz |                                       |

### `tournament`
| column        | type        | notes                                      |
| ------------- | ----------- | ------------------------------------------ |
| id            | uuid PK     |                                            |
| basho_id      | text FK     | references `basho.id`                      |
| status        | text        | `setup` \| `drafting` \| `active` \| `archived` |
| budget_pence  | int         | default 5500 (£55.00) — pence to avoid floats |
| roster_size   | int         | default 4                                  |
| created_at    | timestamptz |                                            |

Exactly one tournament has status in (`drafting`, `active`) at a time
(enforced by a partial unique index).

### `tournament_participant`
| column         | type    | notes                                  |
| -------------- | ------- | -------------------------------------- |
| tournament_id  | uuid FK |                                        |
| user_id        | uuid FK |                                        |
| draft_seed     | int     | tie-break / ordering hint, optional    |
| PRIMARY KEY (tournament_id, user_id)                              |

### `rikishi_price`
Set by admin before the draft. One row per (tournament, rikishi).
| column         | type    | notes                                  |
| -------------- | ------- | -------------------------------------- |
| tournament_id  | uuid FK |                                        |
| rikishi_id     | int FK  |                                        |
| price_pence    | int     |                                        |
| PRIMARY KEY (tournament_id, rikishi_id)                           |

### `roster_entry`
A single ownership event for a (participant, rikishi) pairing. A trade
closes one entry and opens another. The active roster is
`released_before_day IS NULL`.
| column                  | type    | notes                                 |
| ----------------------- | ------- | ------------------------------------- |
| id                      | uuid PK |                                       |
| tournament_id           | uuid FK |                                       |
| participant_user_id     | uuid FK |                                       |
| rikishi_id              | int FK  |                                       |
| purchase_price_pence    | int     |                                       |
| acquired_via            | text    | `draft` \| `trade`                    |
| acquired_before_day     | int     | 1 = before day 1 (i.e. drafted)       |
| sale_price_pence        | int null| set on trade-out                      |
| released_before_day     | int null| set on trade-out                      |
| created_at              | timestamptz |                                   |

Invariant: per (tournament, participant) at most `roster_size` entries
with `released_before_day IS NULL`.

### `trade`
Audit record. One row per swap; references two `roster_entry` rows.
| column            | type    |
| ----------------- | ------- |
| id                | uuid PK |
| tournament_id     | uuid FK |
| participant_user_id | uuid FK |
| sold_entry_id     | uuid FK |
| bought_entry_id   | uuid FK |
| effective_before_day | int  |
| note              | text    |
| created_at        | timestamptz |

### `score_adjustment`
Admin discretion: bonuses, deductions, manual fixes.
| column            | type    |
| ----------------- | ------- |
| id                | uuid PK |
| tournament_id     | uuid FK |
| participant_user_id | uuid FK |
| day               | int null| null = whole-tournament adjustment    |
| points            | int     | signed                                |
| reason            | text    |                                       |
| created_at        | timestamptz |                                   |

### `tournament_award`
Yusho/playoff/sansho records (admin-entered on day 15 / after playoff).
| column         | type   |
| -------------- | ------ |
| tournament_id  | uuid FK|
| rikishi_id     | int FK |
| kind           | text   | `yusho` \| `playoff` \| `shukun` \| `kanto` \| `gino` |
| PRIMARY KEY (tournament_id, rikishi_id, kind)         |

Playoff entrants who lost the playoff have kind `playoff`. Winner has
kind `yusho` (only). Scoring engine maps `yusho` → 2pt, `playoff` → 1pt,
sansho kinds → 1pt each, all attributed to whichever player owned the
rikishi at end of day 15.

## 5. Scoring engine

Pure function:

```python
def compute_standings(
    session: AsyncSession,
    tournament_id: UUID,
    through_day: int,  # 1..15
) -> StandingsResponse: ...
```

Returns per-participant: total points, breakdown by source (wins,
scalps, awards, adjustments), and a per-day timeline. Filters everything
by `match.day <= through_day` and `score_adjustment.day <= through_day`
and ignores awards if `through_day < 15` (or whatever day the playoff
occurred — typically 15).

Ownership at a given day is derived from `roster_entry`: the entry is
owned on day `d` iff `acquired_before_day <= d AND (released_before_day
IS NULL OR released_before_day > d)`. Scalp bonuses use ownership on the
day of the match for both winner and loser.

## 6. API surface

All routes under `/api`. Auth: cookie holding bearer token, validated
on every request. Admin-only routes return 403 for viewers.

```
POST   /api/auth/login              {token}     -> sets cookie
POST   /api/auth/logout
GET    /api/me

GET    /api/tournaments/current
GET    /api/tournaments/{id}
POST   /api/tournaments                          (admin) create
POST   /api/tournaments/{id}/status              (admin) drafting/active/archived

GET    /api/tournaments/{id}/participants
POST   /api/tournaments/{id}/participants        (admin) create viewer user + participant

GET    /api/tournaments/{id}/rikishi             list of Makuuchi rikishi w/ prices
PUT    /api/tournaments/{id}/rikishi/{rid}/price (admin)

GET    /api/tournaments/{id}/picks
POST   /api/tournaments/{id}/picks               (admin) record a draft pick

GET    /api/tournaments/{id}/trades
POST   /api/tournaments/{id}/trades              (admin) record a trade

GET    /api/tournaments/{id}/awards
PUT    /api/tournaments/{id}/awards              (admin) replace award set

GET    /api/tournaments/{id}/adjustments
POST   /api/tournaments/{id}/adjustments         (admin)

GET    /api/tournaments/{id}/standings?through_day=N
GET    /api/tournaments/{id}/days/{n}            day's matches w/ ownership annotations

POST   /api/admin/sync                           (admin) trigger sumo-api pull
PATCH  /api/me/spoiler-day                       set per-user cutoff
```

## 7. Frontend pages

- `/login` — paste token, sets cookie.
- `/` — current tournament dashboard: leaderboard, my roster, last day
  highlights. Day-cutoff selector in header.
- `/draft` — participant grid with each player's 4 slots and remaining
  budget. Read-only for viewers; admin sees the "record pick" form.
- `/standings` — full leaderboard with breakdown columns and a slider
  for `through_day`.
- `/day/:n` — every Makuuchi bout that day, with both rikishi annotated
  by their owner (if any) and the resulting points.
- `/trades` — chronological list of all trades.
- `/admin` — admin console: participants, rikishi pricing, draft entry,
  trade entry, award entry, manual adjustments, sync trigger.

State management: React Query for server state; local component state
for forms. No Redux.

## 8. Data sync

The existing `sumo/download_data.py` is the source of truth for sumo
data. Two changes:

1. Swap the SQLite connection for SQLAlchemy / asyncpg against the
   shared Postgres DB.
2. Wrap `main()` in a function callable from FastAPI's lifespan or
   from APScheduler.

The cron runs once an hour during an active tournament (the basho's
`start_date <= today <= end_date`) and is a no-op otherwise. It only
pulls Makuuchi data for the active basho during the tournament; a
fuller historical pull stays as a one-shot script.

`POST /api/admin/sync` triggers the same routine on demand for the
active tournament.

## 9. Open questions / v2

- **Live scoring during the day** — for now the cron polls hourly. If we
  want bout-by-bout live updates, we'd add WebSockets and a faster poll.
- **Rikishi pricing UX** — admin sets prices manually. Possible v2: auto-
  suggest based on rank (yokozuna/ozeki priced higher).
- **Public leaderboard share link** — read-only URL for showing off.
- **Notifications** — could send a WhatsApp/email when day's results
  land. Out of scope for v1.
- **Self-scalps in mixed-pick days** — if A and B both pick rikishi X
  and X loses to Y (also picked by A), how many scalps does A get? Rule
  reads "1 point if you beat a rikishi that someone else has picked,
  not counting self-scalps" — so A gets 0 scalps from X's loss (it's a
  self-scalp), B gets 0 scalps because B didn't beat anyone. Confirmed
  reading; will encode this exact semantic.
- **Tie-breakers in standings** — undefined in the rules. Suggestion:
  total wins, then total scalps, then alphabetical.
