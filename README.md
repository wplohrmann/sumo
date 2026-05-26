# Fantasy Sumo

A small web app for running a fantasy sumo league among a friend group.
One admin records draft picks, trades, and prize awards; everyone else
gets a read-only view of standings with a per-user spoiler cutoff so
late-watchers don't get spoiled.

Scoring follows the league's house rules: 2 pts per Makuuchi win, 1 pt
scalp bonus when you beat a rikishi another player picked, plus yusho /
playoff / sansho bonuses. See [`docs/spec.md`](./docs/spec.md) for the
full design and [`docs/plan.md`](./docs/plan.md) for what's left to
build.

## Stack

- **Backend** — FastAPI + SQLAlchemy 2 (async) + Alembic, served by
  uvicorn. Talks to Postgres (local Docker in dev, Supabase in prod).
  Pulls match data from [sumo-api.com](https://www.sumo-api.com) on
  admin-triggered sync.
- **Frontend** — React 18 + Vite + TypeScript + Tailwind. React Query
  for server state.
- **Tooling** — [uv](https://docs.astral.sh/uv/) for Python deps, npm
  for frontend, Docker Compose for the local DB.

## Running locally

One-time setup:

```sh
make install        # uv sync --all-extras + npm install
```

Each session needs two terminals:

```sh
# terminal 1: database + backend
make db-up          # Postgres on :5433, Adminer on :8081
make migrate        # apply Alembic migrations
make backend        # FastAPI on :8000

# terminal 2: frontend
make frontend       # Vite on :5173, proxies /api -> :8000
```

Open <http://localhost:5173>.

### First-run bootstrap

There are no users in a fresh DB, so:

1. Log in at `/login` as admin. The admin password defaults to
   `change-me` — override with `SUMO_ADMIN_PASSWORD=...` in
   `backend/.env` for anything real (see `backend/app/settings.py` for
   all env vars; the `SUMO_` prefix is required).
2. From `/admin`, hit **Sync** to pull current Makuuchi data from
   sumo-api.
3. Create a tournament, add participants (each one returns a bearer
   token *once* — save it), set rikishi prices, then record draft
   picks. Flip the tournament to `active` to start scoring.

Adminer is at <http://localhost:8081> for poking at the DB directly
(server `postgres`, user/pass/db all `sumo`).

## Make targets

| Target | What it does |
| --- | --- |
| `make install` | Install backend + frontend deps |
| `make db-up` / `make db-down` | Start / stop Postgres + Adminer |
| `make migrate` | Apply Alembic migrations |
| `make makemigration m="msg"` | Autogenerate a new migration |
| `make backend` | Run the FastAPI dev server |
| `make frontend` | Run the Vite dev server |
| `make test` | Run backend pytest suite |
| `make lint` | ruff + black check |

## Repo layout

```
backend/        FastAPI app, Alembic migrations, pytest suite
frontend/       Vite + React SPA
docs/           spec.md (design), plan.md (remaining work)
sumo/           legacy Python package — Elo + XGBoost match
                predictions, original sumo-api downloader. Kept for the
                stretch "predicted wins" feature in docs/plan.md.
```

For contribution rules — branch naming, doc upkeep, the
`docs/decisions.md` staging area — see [`CLAUDE.md`](./CLAUDE.md).
