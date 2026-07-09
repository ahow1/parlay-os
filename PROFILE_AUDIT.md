# Profile Engine Audit — 2026-07-09

Scope: `profile_engine.py` and the player/team profile system it feeds
(`pitcher_profiles`, `hitter_profiles`, `bullpen_memory`, `team_profiles`,
`sequence_memory` tables in `memory_engine.py`). Report only — nothing in
`profile_engine.py` was modified.

## 1. Live data or stored snapshot?

**Live.** Every `update_*_profile()` call hits the MLB Stats API (and
Statcast via `statcast_engine`) fresh — game logs, season stats, situational
splits. It does not read from a cache to compute the profile. It then writes
the computed result into SQLite as a snapshot for that `stat_date`.

## 2. Does it save state, or compute-and-discard?

**Designed to save** — `update_sp_profile`, `update_hitter_profile`,
`update_bullpen_profile`, `update_team_profile` all call an `upsert_*`
into `memory_engine.py`, which has matching `get_pitcher_profile()` /
`get_hitter_profile()` readers ready to serve that data back.

**In practice, it isn't saving anything.** Checked the live DB directly:

```
pitcher_profiles   0 rows
hitter_profiles    0 rows
bullpen_memory      0 rows
team_profiles       table does not exist (CREATE TABLE IF NOT EXISTS never fired)
sequence_memory     table does not exist (same)
```

Two separate reasons this system has never actually persisted anything:

- **brain.py path** (`brain.py:3612-3849`): profile updates run on a
  `daemon=True` background thread, started *after* the Telegram slip is
  sent, and the main process is never joined to it. On a one-shot GitHub
  Actions run the process exits right after starting the thread, killing it
  before any of the (dozens of, blocking, sequential) HTTP calls finish.
- **scheduler.py path** (`scheduler.py:71-72`, `196-197`, `279-280`): calls
  `run_nightly_profile_updates()` and `run_weekly_team_updates()` with
  **zero arguments**, but both functions require several positional args
  (`game_pk, game_date, away_team, home_team, away_score, home_score, ...`
  / a `team_ids` dict). Every scheduled call raises `TypeError` immediately,
  silently swallowed by the broad `except Exception` wrapper and logged as
  an error — this path has never once succeeded.

**Bonus finding**: even the write path worked, nothing reads it back.
`get_pitcher_profile()` and `get_hitter_profile()` (`memory_engine.py:581,
627`) have zero call sites anywhere else in the codebase — not in
`sp_engine.py`, `offense_engine.py`, `bullpen_engine.py`, or `brain.py`.
This system is currently a dead end in both directions.

## 3. Is there any time-awareness?

**Yes — genuinely, and it's well built.** This isn't flat season numbers:

- `k9_trend` / `bb9_trend`: slope between early half and late half of a
  pitcher's last 10 starts
- `recent_era_3`: recency-weighted ERA over the last 3 starts (weights
  `[0.2, 0.3, 0.5]`, most recent weighted highest)
- `velocity_trend` / `velocity_decline` / `velocity_injury_risk`: flags a
  3+ K/9 drop as possible injury risk
- `hot_cold_score`: 14-day rolling OPS vs. season OPS, scaled -5 to +5
- `streak_type` / `current_streak`: hot/cold streak detection with
  historical average streak length
- `pitch_count_cliff`: bucket-based fatigue point where ERA jumps
- `ttop_era_1/2/3`: times-through-order ERA proxy from pitch-count ranges
- `sequence_memory`: cross-series momentum between two teams

This is real, non-trivial trend modeling — the kind of signal that would
matter for pick quality. It's just entirely disconnected from anything
downstream right now (see #2).

## 4. When was profile data last actually refreshed?

**Never, successfully.** Zero rows in every profile table in the live DB.
Both automated entry points (`brain.py` background thread, `scheduler.py`
nightly/weekly hooks) have been broken since they were wired up — the
scheduler calls have been raising `TypeError` on every invocation, and the
brain.py thread has been getting killed before completing on every run.

## Bottom line

`profile_engine.py` is a well-designed, genuinely time-aware profiling
system that has **never produced a single persisted row** in the running
system, due to two independent wiring bugs (daemon thread never joined;
scheduler calling functions with the wrong arity). Its output is also not
read by any picking logic even where it exists. Right now it costs API
calls and does nothing else — no upside, no downside to current picks.

Not fixed as part of this audit per Task 0 instructions (report only). Flag
for a follow-up task: fixing the wiring + hooking `get_pitcher_profile()` /
`get_hitter_profile()` into `sp_engine.py` / `offense_engine.py` would
plug in trend signal that's fully built but currently inert.
