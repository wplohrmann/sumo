.PHONY: help install dev backend frontend db-up db-down migrate makemigration test lint

help:
	@echo "Targets:"
	@echo "  install         install backend + frontend deps"
	@echo "  db-up           start local Postgres + Adminer"
	@echo "  db-down         stop local services"
	@echo "  migrate         apply Alembic migrations"
	@echo "  makemigration m=msg  autogenerate a new migration"
	@echo "  backend         run FastAPI dev server"
	@echo "  frontend        run Vite dev server"
	@echo "  test            run backend tests"
	@echo "  lint            ruff + black check"

install:
	cd backend && uv sync --all-extras
	cd frontend && npm install

db-up:
	docker compose up -d postgres adminer

db-down:
	docker compose down

migrate:
	cd backend && uv run alembic upgrade head

makemigration:
	cd backend && uv run alembic revision --autogenerate -m "$(m)"

backend:
	cd backend && uv run uvicorn app.main:app --reload --port 8000

frontend:
	cd frontend && npm run dev

test:
	cd backend && uv run pytest -q

lint:
	cd backend && uv run ruff check . && uv run black --check .
