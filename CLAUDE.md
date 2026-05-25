# CLAUDE.md

Implementation guidelines for this project. Read before making changes.

## Source of truth

- [`docs/spec.md`](./docs/spec.md) describes the system as it is *meant to be*.
- [`docs/plan.md`](./docs/plan.md) describes the work remaining to get there.
- [`docs/decisions.md`](./docs/decisions.md) is a staging area for in-flight design decisions (see below).
- The code is the system as it actually is. When the three disagree: code wins for *what is*; spec wins for *what should be*; plan is updated to close the gap.

## Keeping docs in sync

- When a slice of work is finished, **delete the now-completed items from `docs/plan.md`**. Don't leave them as crossed-out lines, comments, or "done" markers — just remove them. The plan should always read as a forward-looking to-do list.
- When the spec needs to change because reality bit back, **edit `docs/spec.md` to reflect the new design**. Do not annotate it with "previously this was X, now it's Y". The spec describes the *current* design only.
- The same rule applies in code, comments, commit messages, and any other docs: **never reference removed or superseded approaches**. No "instead of method X, we now use method Y" comments. No `// removed because…` stubs. If method X is gone, it is gone everywhere except git history.
- The single exception is `docs/decisions.md`, which is allowed to discuss alternatives.

## Splitting docs

`docs/spec.md` should stay readable end-to-end. When a topic needs more depth than fits there comfortably — a ranking-algorithm derivation, a prompt template with examples, a schema-migration playbook, etc. — pull it into its own file under `docs/` and link to it from `spec.md`.

- Name files by topic: `docs/ranking.md`, `docs/llm-prompts.md`, `docs/sync.md`, etc.
- Always add a one-line pointer from `spec.md` to the new file (e.g. "See [`docs/ranking.md`](./ranking.md) for the derivation and tuning notes."). Workers reading the spec must be able to discover the detail file without grepping.
- The detail file follows the same rules as the spec: describes the current state only, no historical "we used to do X" notes.
- Don't pre-emptively split. Wait until something is actually hard to read inline; then extract.

## docs/decisions.md

A staging area for design decisions made during implementation that aren't already captured in spec/plan.

- **When to write to it**: any time a non-trivial choice gets made that isn't unambiguously dictated by the spec — a library pick, a schema tweak, a tradeoff between two reasonable options, a deviation from the plan.
- **Format**: one short section per decision. Title, context, the choice taken, the alternative(s) considered, why.
- **Lifecycle**: the user reviews entries. Once an entry is approved, **delete it from `decisions.md`** and (if it changed how the system works) fold the new behavior into `spec.md` or `plan.md`. The file should slowly empty itself as things get reviewed.

## Tests

- Write tests whenever they meaningfully pin behavior. This is partially how the spec is enforced — when ranker behavior, parser robustness, or API contracts drift, tests should catch it.
- Follow the test strategy in `docs/plan.md` (unit / integration / live-gated / manual).
- Live-network tests (real ollama, real Supabase) must be marked `@pytest.mark.live` and skipped by default.
- A failing test is a signal, not a nuisance. Fix the cause, not the test, unless the test itself was wrong.

## Secrets

- Never commit `.env`, Supabase keys, or any credential. `.env.example` is the only file in that family that gets tracked.
