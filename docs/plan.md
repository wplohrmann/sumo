# Fantasy Sumo — Implementation Plan

Phased plan. Each phase ends in something runnable. Goal: be ready for
the next basho with at least Phase 4 shipped.

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
