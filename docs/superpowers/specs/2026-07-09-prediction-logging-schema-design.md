# Prediction Logging Schema — Design

Date: 2026-07-09
Status: Approved
Scope: Schema only (`db.py`). No wiring into `brain.py`'s bet flow, no CLV
grader, no public record view. Those are separate future design/plan cycles.

## Background

This is a sub-project carved out of a much larger multi-part brief
("PARLAY-OS SESSION 1: FOUNDATION BUILD") that also asked for an OddsPapi
odds-feed swap, a live Kalshi trading integration, and a SQLite→Postgres
migration in the same session. That brief was decomposed on scope grounds
— it described six-plus largely independent subsystems touching a live,
real-money production system, several of which require credentials that
don't currently exist in `.env` (`ODDSPAPI_KEY`, Kalshi API keys,
`SUPABASE_DB_URL`). This spec covers only the piece that needs none of
that: the logging schema (original brief's Task 4).

A prerequisite audit (Task 0, `PROFILE_AUDIT.md`) also surfaced a directly
relevant precedent: `profile_engine.py` is a well-built, time-aware
profiling system that has never persisted a single row in production, due
to wiring bugs, and even its read functions are never called. The lesson
carried into this design: build the schema now, but do not wire it into
the live bet flow in the same change — verify each layer independently.

## Problem

The original brief asked for new `predictions` / `feature_snapshots` /
`model_versions` tables to capture every pick's inputs before the game
starts, for later CLV grading and a learning loop. Investigating the
existing schema (`db.py`) found this would have been substantially
duplicative:

- `bets` (`db.py:123`) already has `model_prob`, `market_prob`, `edge_pct`,
  `conviction`, `stake`, `closing_odds`, `clv_pct`, `result`, `profit` —
  most of what a `predictions` table would hold.
- `clv_log` and `calibration_buckets` already exist for CLV/learning-loop
  purposes (CLAUDE.md already lists both as "needs implementation" against
  *existing* tables, not new ones).
- `log_bet()` (`db.py:360`) already accepts several individual signal
  values as flat columns (`pitch_trap`, `framing_edge`, `closer_avail`,
  `lineup_slot_score`, `sharp_signal`, `umpire_edge`, `home_dog_angle`,
  `first_pitch_strike_rate`, `sp_gb_rate`, `situations_triggered`,
  `abs_score`, `sharp_checklist_results`, `confidence_engine_score`) — an
  early, flat precursor to what `feature_snapshots` should formalize.

Building a parallel `predictions` table alongside `bets` would create two
sources of truth for the same pick — exactly the kind of drift risk this
project's hard rules (CLAUDE.md) warn against elsewhere (e.g. slip dedup
living only in `last_scout.json`, not SQLite, specifically to avoid a
second source of truth).

Also found, out of scope for this spec but worth recording: only ML
(`bet_type="ML"`) picks are logged to `bets` at all today. `prop_results`
exists with a `log_prop_result()` writer, but it's never called from
`brain.py`. PARLAY/NRFI/TOTAL picks get no DB row of any kind — they exist
only in the Telegram message and `last_scout.json`. Fixing that is bet-flow
work (a future "Task 5" cycle), not schema work.

## Design

### 1. Extend `bets`, don't duplicate it

New nullable columns, added via the same guarded-`ALTER TABLE` pattern
already used in `init_db()` (`db.py:317-342`: each DDL statement wrapped
in `try/except sqlite3.OperationalError: pass`, so it's safe to run
against both fresh and existing databases):

```sql
ALTER TABLE bets ADD COLUMN sport TEXT DEFAULT 'MLB'
ALTER TABLE bets ADD COLUMN model_version TEXT
ALTER TABLE bets ADD COLUMN reasoning_text TEXT
ALTER TABLE bets ADD COLUMN kalshi_price REAL
ALTER TABLE bets ADD COLUMN kalshi_liquidity_ok INTEGER
ALTER TABLE bets ADD COLUMN roi REAL
ALTER TABLE bets ADD COLUMN graded_at TEXT
```

Mapping from the original brief's `predictions` spec to what already
exists, so nothing is duplicated:

| Original spec field | Where it actually lives |
|---|---|
| `id` | `bets.id` |
| `timestamp_created` | `bets.timestamp` |
| `sport` | new `bets.sport` |
| `market` / `event` / `selection` | `bets.type` / `bets.game` / `bets.bet` |
| `model_version` | new `bets.model_version` |
| `confidence` | `bets.conviction` |
| `edge_percentage` | `bets.edge_pct` |
| `implied_probability` | `bets.market_prob` |
| `kalshi_price` / `kalshi_liquidity_ok` | new columns |
| `reasoning_text` | new column |
| `result` | `bets.result` |
| `closing_line` | `bets.closing_odds` |
| `clv` | `bets.clv_pct` |
| `roi` | new column |
| `graded_at` | new column |

`log_bet()`'s existing signature, its `INSERT OR IGNORE`, and every
existing call site are untouched — the new columns default to `NULL`
(or `'MLB'` for `sport`) and are simply not passed by current callers.

### 2. New `feature_snapshots` table (EAV)

```sql
CREATE TABLE IF NOT EXISTS feature_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    bet_id              INTEGER NOT NULL REFERENCES bets(id),
    feature_name        TEXT NOT NULL,
    feature_value       REAL,
    feature_value_text  TEXT,
    feature_weight      REAL,
    created_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_feature_snapshots_bet_id
    ON feature_snapshots(bet_id);
```

One row per named feature per pick, per the original spec and the user's
choice of EAV over a JSON blob — queryable per-feature (e.g. "every pick
where `sharp_signal` was true and it lost") without parsing JSON in
Python, and new features don't require a schema change.

`feature_value_text` is an addition beyond the original brief: several of
the existing per-pick signal columns on `bets` are text, not numeric
(`situations_triggered`, `sharp_checklist_results` look like JSON/list
text). A REAL-only value column would silently drop those. Exactly one of
`feature_value` / `feature_value_text` is populated per row; the other
stays `NULL`.

Not populated as part of this spec — writing rows into this table from the
bet flow is Task 5, a separate cycle.

### 3. New `model_versions` table

```sql
CREATE TABLE IF NOT EXISTS model_versions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    model_name    TEXT NOT NULL,
    version       TEXT NOT NULL,
    sport         TEXT DEFAULT 'MLB',
    created_date  TEXT NOT NULL,
    feature_list  TEXT,
    notes         TEXT,
    rolling_roi   REAL,
    rolling_clv   REAL,
    UNIQUE(model_name, version)
);
```

As spec'd in the original brief. `UNIQUE(model_name, version)` prevents
accidental duplicate version rows.

### 4. `sport` column placement

- `bets.sport` — new, default `'MLB'`.
- `model_versions.sport` — new, default `'MLB'` (a given model version is
  normally trained for one sport).
- `feature_snapshots` — deliberately **no** `sport` column. It inherits
  sport transitively via `bet_id → bets.sport`. Denormalizing it onto every
  feature row would risk drift if a bet's sport were ever corrected after
  the fact, for a column that a single join always recovers.

### 5. Postgres-readiness

All new DDL uses `INTEGER PRIMARY KEY AUTOINCREMENT` and ISO-8601 text for
dates/timestamps — the same conventions the rest of `db.py` already uses.
Nothing here relies on SQLite-only behavior, so it maps directly to
`SERIAL` / `TIMESTAMPTZ` whenever the Postgres migration (original brief's
Task 3) actually happens — deferred for now since `SUPABASE_DB_URL` isn't
configured.

### 6. Testing

A test script that:
1. Copies `parlay_os.db` to a scratch path.
2. Records row counts for every existing table.
3. Runs `init_db()` against the scratch copy.
4. Asserts row counts are unchanged for every pre-existing table (no data
   loss from the `ALTER TABLE` migrations).
5. Asserts the new columns exist on `bets` (via `PRAGMA table_info`) and
   the new tables exist with the expected columns.
6. Calls `log_bet()` with its current (unmodified) signature against the
   scratch copy and asserts it still succeeds — proving the extension
   didn't break the existing write path.
7. Runs `init_db()` a second time against the same scratch copy and
   asserts no exception — proving the migration is idempotent, matching
   the existing guarded-`ALTER TABLE` convention.

## Out of scope (explicitly deferred, not forgotten)

- Writing to `feature_snapshots` / setting `model_version` from the bet
  flow (Task 5 — touches live pick output, needs its own design + a
  byte-identical-output test per CLAUDE.md's hard rules).
- CLV auto-grading, write-once enforcement on graded predictions (Task 6).
- `public_record` view (Task 7).
- OddsPapi client, no-vig helper (Task 1) — blocked on `ODDSPAPI_KEY`.
- Kalshi engine, liquidity check (Task 2) — blocked on Kalshi credentials.
- Postgres migration (Task 3) — blocked on `SUPABASE_DB_URL`.
- Backfilling `feature_snapshots` from `bets`' existing flat signal
  columns (`pitch_trap`, `sharp_signal`, etc.) for historical rows — a
  reasonable follow-up once the live write path exists, not before.
- Logging PROP/PARLAY/NRFI/TOTAL picks to any table (currently only ML
  picks are logged at all) — a pre-existing gap, bet-flow work, not schema
  work.
