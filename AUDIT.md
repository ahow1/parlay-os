# Parlay OS — Full Repository Audit

**Date:** 2026-07-12
**Scope:** Full read-only audit of `/workspaces/parlay-os` (~26,000 lines, 45+ Python files, SQLite DB, GitHub Actions automation). No code was changed as part of this audit.
**Method:** 14 parallel subagents, one per major section, each independently reading source, grepping call graphs, querying the live DB, and running the test suite. Findings below are compiled and cross-referenced from their reports.

---

## 1. EXECUTIVE SUMMARY

The core pick-generation and Telegram-delivery path works, but sits on top of a system that has grown much faster than it's been wired together. `BANKROLL_OVERRIDE` is currently **unset** in `.env`, meaning stake sizing is running in a partially-collapsed state right now. A confirmed bug makes every "richer-model" K-prop use a hardcoded 55% win probability regardless of actual edge, while a strikeout-count gap gets mislabeled as a percentage edge and used to sort bets into LOCK/FLIP tiers — this directly mis-sizes and mis-labels real-money bets. `_send_telegram()` returns success even when Telegram credentials are missing, which can falsely mark an empty slip as sent. About a third of the engine files audited (profile_engine, defense_engine, hitter_prop_engine, clv_tracker, discord_bot, math_engine's BankrollManager, most of memory_engine's learning-loop functions) are either never called, or write data that's never read back — the "learning loop" and "CLV capture" gaps CLAUDE.md already flags are real and now precisely located. Settlement has no doubleheader disambiguation and unmatched bets stay pending forever, silently deflating the bankroll calculation. The live production Telegram bot token and Odds API key are sitting exposed in a local git stash (not pushed to GitHub, but recoverable) and should be rotated. Test coverage exists but almost entirely misses the money-moving code (Kelly sizing, settlement, dedup) — one test even writes fake rows into the live production DB on every run. GitHub Actions cron times have silently drifted an hour off from their own documented ET times because the workflow was tuned for one DST state and never adjusted. None of this is unfixable — most fixes are small and localized — but several of these bugs are actively shaping which bets get sent and how big they are, today.

---

## 2. FINDINGS BY SEVERITY

### BLOCKING — breaks picks or loses money right now

**B1. K-prop stake sizing and LOCK/FLIP classification use fabricated data.**
`brain.py:3251,3257,3259`. `analyze_k_prop()` in `strikeout_engine.py` never returns a `"model_p"` key (confirmed via grep — the key doesn't exist anywhere in that file). The caller's guard `_akp["model_p"] if "model_p" in _akp else 0.55` therefore *always* falls through to the hardcoded `0.55`, so every K-prop routed through this path is Kelly-sized off a constant 55% probability regardless of actual projected edge. Separately, `"edge_pct": _akp.get("gap", 0) * 10` treats `gap` — a **strikeout-count differential** (e.g. 1.2 more/fewer projected Ks than the market line) — as if it were a probability-point edge, multiplying it by 10 and feeding it into the `prop_locks`/`prop_flips` tier thresholds (`brain.py:1897-1898`). Example: a projected gap of 1.0 K produces `edge_pct=10.0`, crossing the LOCK threshold and getting sent as a confident `✅ BET`, while the actual Kelly stake was computed from the flat 0.55 default, never from the real projection. Two props with very different real edges (gap=1.0 vs gap=2.5) get different Telegram framing but identical stakes. The parallel fallback path (when `analyze_k_prop` returns `None`) is correct and uses a real Poisson-derived probability — only the "richer model" path is broken.
*Fix (describe only):* Have `analyze_k_prop()` return a real `model_p` derived from the Ks-vs-line projection (e.g. via a Poisson/normal CDF the same way the fallback path does), and stop deriving `edge_pct` from `gap*10`.

**B2. `_send_telegram()` reports success when credentials are missing.**
`brain.py:2619-2630`. If `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` are unset, the function prints a warning and **returns `True`**. Every caller (`_daily_bet_slip`, `_send_slip_update`, sanity-check alerts, cap-block alerts) treats `True` as "delivered," so `slip_sent=True` is written to `last_scout.json` and the dedup logic never retries — even though nothing was sent. This directly violates the hard rule "never mark slip as sent if it was empty," specifically in the missing-credentials case.
*Fix:* Return `False` (or raise) when credentials are missing, so callers don't mark the slip as sent.

**B3. `analyze_game()` is wrapped in one blanket try/except — any single-factor failure silently drops the entire game.**
`brain.py:3150-3155`. The ~750-line `analyze_game()` (weather, SP, bullpen, offense, situations, memory calibration, Kelly sizing, etc.) is called inside one outer `try/except Exception`, most of which is *not* individually guarded. Any exception anywhere inside it drops the whole game for the day with only `print(f"ERROR in analyze_game: {e}")` — no Telegram alert, no `scout_out` entry, indistinguishable in the log from a routine skip unless grepping for the literal string `ERROR`.
*Fix:* Narrow the try/except to genuinely optional sub-steps (as is already done for most sub-calls individually) and let truly fatal errors surface distinctly from routine skips.

**B4. A logged bet can be sent to Telegram but never persisted to the `bets` table.**
`brain.py:3372-3403`. `_db.log_bet()` is wrapped in `try/except Exception as e: print(f"DB log error: {e}")`. By the time it's called, the pick is already in `all_locks`/`all_flips`/`scout_out["bets"]` and **will still be sent to Telegram** — but if the DB write fails, it can never be auto-settled, never feeds `memory_engine`/`calibration_buckets`, and never appears on the dashboard. A pick the user sees becomes permanently invisible to every downstream learning/settlement system.
*Fix:* If `log_bet()` fails, either retry, queue for later persistence, or suppress the Telegram send for that pick rather than silently decoupling display from storage.

**B5. `scheduler.py`'s nightly/weekly profile-update jobs crash on every single invocation.**
`scheduler.py:72` calls `profile_engine.run_nightly_profile_updates()` with zero arguments against a signature requiring 6 positional args (`game_pk, game_date, away_team, home_team, away_score, home_score`) — confirmed empirically to raise `TypeError` every time, swallowed by `scheduler.py:74-75`'s broad except and logged only as "Nightly profiles failed." Same pattern at `scheduler.py:280` for `run_weekly_team_updates()` (requires `team_ids: dict`, called with none). As a result, `sequence_memory`, `team_profiles`, `physical_fatigue`, `sp_season_ip`, `hitter_season_ab` tables have **never received a single row** in the live DB (confirmed: these tables don't even exist in `parlay_os.db`, proving the code paths have literally never executed).
*Fix:* Pass the required arguments at the call site (e.g. the day's completed games for the nightly job, `constants.MLB_TEAM_IDS` for the weekly job), or remove the dead scheduler hooks if this data isn't actually needed (brain.py's own background thread already covers SP/hitter/bullpen profile writes separately — see W-list below).

**B6. `ml_model.py` fails to import in the GitHub Actions scout runs that actually produce daily picks.**
`.github/workflows/mega_scout.yml`'s scout jobs only `pip install requests pytz flask flask-cors`, but `ml_model.py` unconditionally imports `numpy`/`pandas`/`joblib`/`requests_cache` at module load. `brain.py:72-80`'s `try/except ImportError` silently swallows this, so the ML ensemble and `detect_regression_flags()` are permanently disabled in the pipeline that generates the actual daily slip (Railway's `--bot` process may differ — not confirmed). Compounding this: `models/` is empty (no `.pkl` files committed), so even where the import succeeds, `models_available()` is always `False` and `predict_game()` always falls back to a Pythagorean estimate.
*Fix:* Either add the ML dependencies to the GH Actions pip install step and commit trained model artifacts, or explicitly document that ML predictions only run on Railway and remove the false expectation elsewhere.

**B7. `situations_engine.get_active_situations()` doesn't exist — the import fails silently on every call, every game.**
`brain.py:589` imports `get_active_situations` from `situations_engine`, a function that is not defined anywhere in that module (confirmed by reading the file). The call sits inside a bare `except Exception: pass` (`brain.py` ~591), so it fails silently every single time. `_away_sits`/`_home_sits`/`_brain_situations` are therefore always empty — this code path has likely never worked since it was written. Separately, the 9 situational angles' weighted probability adjustments (`total_away_adj`/`total_home_adj`, e.g. `+0.04` for `HOME_DOG_ELITE_SP`) are computed by `check_situations()` (which *is* correctly called) but are **never applied** to `away_model_p`/`home_model_p` — only the trigger count feeds a flat `+8` confidence bump (`brain.py:2332-2338`), contradicting the module's own docstring that describes them as additive to the base win probability.
*Fix:* Either implement `get_active_situations()` in `situations_engine.py` or remove the dead import; separately, wire `total_away_adj`/`total_home_adj` into `_weighted_win_prob()` as the docstring already claims happens.

**B8. Team offensive strength (wRC+) barely reaches run projections or the moneyline blend.**
`offense_engine.py:445-454`. `wrc_plus_adj` — consumed as `adj_wrc_plus` for both `run_factor` (`:483`) and the win-probability blend inputs `away_wrc_v`/`home_wrc_v` (`brain.py:396-397`) — is computed from a hardcoded placeholder (`wrc_plus_14d = 100`) *before* the real recency-weighted value (40% 7d / 35% 30d / 25% season) is computed 8 lines later. The dependent `wrc_plus_adj` is never recalculated with the real number. Net effect: actual team offensive form contributes almost nothing to run/win projections — only the platoon-handedness delta does.
*Fix:* Move the `wrc_plus_adj` computation to after the real `wrc_plus_14d` assignment.

**B9. Doubleheader settlement has no game disambiguation — a bet can be graded against the wrong game.**
`telegram_handler.py:1365-1370` (`run_settlement_check`). The `bets` schema (`db.py:123-146`) has no `game_pk`/`game_number` column, and bet parsing (`parse_bet()`) has no game-1/game-2 syntax. On a doubleheader date, `_fetch_final_games()` returns both games for the two teams; the matching loop takes the **first** final game it encounters and stops. A bet intended for game 2 can be silently graded against game 1's score if game 1 finishes first.
*Fix:* Capture `game_pk`/game-number at bet-logging time (available from the odds/schedule API) and match on it during settlement.

**B10. Bets that can't be matched to a result stay pending forever with zero operator-visible signal, and permanently deflate the bankroll.**
`telegram_handler.py:1372-1373`. An unmatched bet (postponed-and-rescheduled game, API gap, etc.) just `continue`s — no timeout, no manual-review flag. It won't show in the daily summary or debrief (both filter strictly to `date == today`). Confirmed empirically: **44 of 45 bets in the live DB have `result IS NULL`**, including rows up to 46 days old. `bankroll_engine.current_bankroll()` (`bankroll_engine.py:57-74`) sums *all* unresolved bets' stakes with no date bound and subtracts them from bankroll — so this stuck-pending population permanently and silently shrinks the computed bankroll, compounding the already-known `BANKROLL_OVERRIDE`-missing collapse bug.
*Fix:* Add a "stuck pending" alert (e.g. bets >48h past game time still unresolved) and/or exclude clearly-orphaned pending bets from the `current_bankroll()` deduction until manually resolved.

**B11. `BANKROLL_OVERRIDE` is not set in `.env` right now.**
Confirmed by the config-audit agent: absent from both `.env` and `.env.example`. Without it, `sizing_bankroll()`/`current_bankroll()` fall back to computing from the DB, ultimately bottoming out at `math_engine.STARTING_BANKROLL = 150.0` — a stale hardcoded fallback that doesn't match any of the numbers currently documented for this system. This is exactly the collapse scenario CLAUDE.md's own "Known Bugs" table warns about, and it also means CLAUDE.md's own env-var table (`BANKROLL_OVERRIDE=741`) and "Current State" section (`Bankroll: $300`) can't both be describing the live `.env`, because neither value is set there at all.
*Fix:* Set `BANKROLL_OVERRIDE` in `.env` to the current real bankroll figure from Aidan, per the existing `sed` procedure in CLAUDE.md, and add it to `.env.example` so this can't silently regress again.

**B12. Live Telegram bot token and Odds API key are exposed in local git history (stash), and are the currently-active keys.**
See Security section (S1) below for full detail — elevated to BLOCKING here because these are live production credentials with a confirmed, reproducible leak path on this machine.

### MAJOR — silently loses data or edge

**M1. SP-data-failure false-positive path is only partially closed.**
`sp_engine.py`'s `_default_sp()` fabricates a league-average pitcher (`era=4.35, k9=8.5,` etc.) on any stats-fetch failure and sets `sp_missing=True` correctly — but `get_game_sps()` (`sp_engine.py:737-753`) unconditionally overwrites `name` with the real probable-pitcher name (fetched via a separate, independently-succeeding API call). This means a fully-fabricated pitcher record can carry a real ace's name. The final ML-pick suppression gate (`brain.py:3328-3336`) correctly checks the real `sp_missing` flag and blocks straight moneyline/K-prop/ER-prop/NRFI-total recommendations for that game. But three real gaps remain, all user-facing: (1) `build_sgp_suggestions()` (`props_engine.py:256-269`) has **no `sp_missing` check at all**, and its `k9<8.0` skip-check is passed by the fabricated `k9=8.5` default, so a fake-ace SGP leg can reach Telegram under the real pitcher's name; (2) the `/props` K-prop feed (`brain.py:3927`) is gated on `name=="TBD"`, not `sp_missing`, so it has the same masking issue; (3) the confidence-dampening shrink (`brain.py:611-616`) also checks `name` instead of `sp_missing`, so it never fires in this exact scenario even though it was clearly built to cover it.
*Fix:* Add `sp_missing` checks to the SGP builder and the props K-prop gate; switch the confidence-dampening check from `name=="TBD"` to `sp_missing`.

**M2. Same neutral-default pattern exists in bullpen and offense engines, without the SP engine's partial safety net.**
`bullpen_engine.py:284-285` (`bullpen_run_factor`) resolves a deliberately-flagged `tier=="UNKNOWN"` (set when `data_ok=False`) to a neutral `1.0` run factor for the run-expectancy calculation — even though `analyze_bullpen()` went out of its way to flag the failure, this specific consumption path doesn't check `data_ok` (unlike the win-prob weighting path, which does at `brain.py:500-503`). `offense_engine.py` has **no aggregate `offense_missing` flag at all** (unlike SP/bullpen) — every sub-fetch independently falls back to league-average numbers with no suppression path anywhere in brain.py for a fully-down offense feed.
*Fix:* Extend `data_ok`-style flagging and suppression to the run-expectancy consumption path in bullpen, and add an aggregate missing-data flag to `offense_engine.py`.

**M3. Five Savant-derived factor lookups fail completely silently — no print, no log.**
`brain.py:417-426, 431-442, 446-454, 472-479, 484-493`. Five separate `savant_leaderboards` factor lookups (bullpen stuff+, bat-tracking, park/OF defense, sprint/baserunning, arm-angle platoon) are each wrapped in a bare `except Exception: pass` with zero logging. A broken import or API failure silently zeroes 5 of the 12 win-probability blend factors for every game, with no trace anywhere in the log.
*Fix:* At minimum log the exception; consider surfacing an aggregate "savant factors degraded" flag in the daily health check.

**M4. `h2h_engine` and `situations_engine.check_situations` failures are also fully silent.**
`brain.py:574-576` (H2H) and `brain.py:594-595` (situations) both use bare `except Exception: pass`, silently muting a 10%-weight blend input with no log line on failure.

**M5. Confidence-engine retrain pipeline never fires in production.**
`confidence_engine.py`'s LogReg retrain path (`confidence_weights` table, blend at `confidence_engine.py:381-386`) is only ever triggered by `scheduler.py`'s `run_confidence_retrain_task()` — but `scheduler.py` is not part of any deployed process (Procfile only runs `api.py`, `brain.py --bot`, `health_check.py --loop`). `confidence_weights` never gets a row; `get_confidence_score()` always runs the pure-heuristic fallback.
*Fix:* Either add `scheduler.py` as a deployed process, or move its retrain trigger into `brain.py --bot`'s own loop.

**M6. `capture_pre_game_clv()` is fully implemented and never called.**
`bankroll_engine.py:432-490`. Confirms CLAUDE.md's "CLV capture: needs implementation" is more precisely "implemented, never invoked" — its only caller anywhere is `test_fixes.py`.

**M7. Learning loop is a write-only sink: `calibration_buckets` and `brain_weights.json` are computed but never fed back into scoring.**
`db.get_calibration()` (db.py:555-558) — the reader for `calibration_buckets` — has zero production callers (only `test_fixes.py`). `db.set_weight_adj()` (would apply a learned adjustment) also has zero callers anywhere, including tests. Separately, `brain_weights.json` is regenerated by `recalibrate_weights()` every ~20 settled bets but is read by **nothing** outside `memory_engine.py` itself — no engine file consumes it. There is also a second, entirely separate and fully dead calibration system (`model_calibration` table in `memory_engine.py`, fed only by dead functions `post_game_update`/`record_player_result`/`record_team_result` — 0 real rows in the live DB) running in parallel with the first.
*Fix:* Wire `get_calibration()`'s output into pick-scoring or stake sizing; wire `brain_weights.json` into whichever engine it's meant to adjust; delete or complete the second calibration system.

**M8. Props parlay Kelly sizing assumes independence with no same-game correlation filter.**
`brain.py:1888-1934`. `prop_locks[:3]` is pooled across *all* games with no game-uniqueness filter before computing `joint_p = product of individual model_p`. Two legs from the same game (e.g. a SP's K-prop and an opposing batter's hits prop) can be strongly correlated in either direction; the naive product either overbets or underbets the true joint probability with no code path to catch it.
*Fix:* Filter parlay legs to one per game, or explicitly model/flag same-game correlation before applying the independence-assuming Kelly formula.

**M9. Hitter-prop and props-parlay Kelly sizing use synthetic no-vig "fair" odds instead of real market odds.**
`brain.py:2927-2933`, `1840-1844`. `market_p` (a hardcoded per-stat baseline probability) is converted directly into a fair-odds American-odds string and fed into `kelly_stake()` as if it were the real payout. Since real books price with vig (larger `q`, smaller effective `b`), this systematically overstates the Kelly-optimal fraction — worked example in the source report shows a ~2.6x difference in computed Kelly fraction between fair odds and a realistic -110 market price for the same edge.
*Fix:* Use the actual fetched market odds for stake sizing on these paths, reserving the no-vig probability for edge calculation only.

**M10. Kelly's band-floor frequently overrides the actual Kelly-computed fraction.**
`bankroll_engine.py:330-333`. Worked example: HIGH conviction, `model_prob=0.53`, odds -105 → true Kelly-with-multiplier output is ~2.4% of bankroll (~$7), but the HIGH band floor (10%) forces the stake up to $30 — over 4x the formula's output. This is a documented, Aidan-approved design (per CLAUDE.md's 2026-07-07 sign-off), not a bug in the formula itself, but it means stake size for tier-qualifying bets is largely flat-per-tier rather than Kelly-driven, and any bet that just clears the qualifying threshold gets the same stake as one with a much larger edge. Flagged for re-confirmation, not as a code defect.

**M11. `profile_engine.py`'s SP/hitter/bullpen writes fire correctly but are never read back.**
The background-thread path (`brain.py:3617-3688`) does correctly call `update_sp_profile`/`update_hitter_profile`/`update_bullpen_profile`, which persist to `pitcher_profiles`/`hitter_profiles`/`bullpen_memory` every scout run. But `memory_engine.get_pitcher_profile()`/`get_hitter_profile()` — the only functions that can read this data back — have **zero callers anywhere in the repo**, and `bullpen_memory` has no reader at all. So this is "persists constantly, reads never," not "persists nothing" as originally suspected — a more precise and arguably worse version of the known issue (real API calls and compute are spent every run producing data nothing ever uses).
*Fix:* Wire `get_pitcher_profile()`/`get_hitter_profile()` into `sp_engine.py`/`offense_engine.py` scoring, or stop collecting this data.

**M12. `defense_engine.py`'s OAA adjustment is never applied to any pick.**
`defense_engine.py:88,99`. Imported into `brain.py:140` with a fallback stub, but the real `get_team_oaa()`/`check_defense_edge()` functions are never invoked anywhere in the repo outside the file's own `__main__` block.

**M13. `hitter_prop_engine.py` (360 lines, fully built xBA/xSLG hits & total-bases prop model) is entirely orphaned.**
Zero importers anywhere in the repo. `brain.py` instead reimplements a separate, parallel hitter-prop pipeline inline (`_scan_hitter_props`/`_fetch_game_hitter_props`, using 14-day rolling stats rather than this file's xwOBA-leaderboard projections) — two independent, disconnected hitter-prop systems exist side by side, only one of which is used.

**M14. `clv_tracker.py` (469 lines, Claude-powered post-game debrief/CLV pipeline) is never invoked by anything.**
Not imported by any module, not in any GitHub Actions job, not in Procfile. `brain.py`'s `_run_debrief()` independently reimplements the same CLV/debrief math via `math_engine` directly, and the two pipelines read/write entirely different state (`clv_log.json`/`bankroll.json`/`bet_history.json`/`model_lessons.json` vs. the live system's SQLite + `last_scout.json`). Its module-level `os.environ["ANTHROPIC_API_KEY"]` would hard-crash (`KeyError`) if anyone ran it in the current environment, since that key isn't set anywhere.

**M15. `discord_bot.py` is fully orphaned.**
Zero importers repo-wide, not referenced in Procfile/railway.json/the GH Actions workflow. The entire Discord integration (`post_picks_embed`, `post_result_embed`, `post_daily_summary`) has no caller.

**M16. `log_prop_result()`/`settle_prop()`/`get_prop_accuracy()` — the entire props-accuracy tracking pipeline — is dead.**
`db.py:730,745,753`. Zero callers anywhere in the repo. `prop_results` table is confirmed at 0 rows in the live DB. Scheduler's nightly "expire old props" UPDATE (`scheduler.py:180-183`) runs against this table every night and always affects 0 rows.

**M17. Two disconnected CLV pipelines exist.**
Pre-game capture (`bankroll_engine.capture_pre_game_clv()` → `db.log_clv()` → `clv_log` SQL table) is fully built but never called in production (only from `test_fixes.py`). Post-game capture (`telegram_handler.py:1308` `_update_clv_log()`, called from the live auto-settler) writes to `clv_log.json` instead. `api.py`'s `/api/clv` endpoints read the JSON file, never the DB table — confirmed empirically: `clv_log` SQL table has exactly 1 row since inception.

**M18. `arbitrage_log.json` and `live_alerts.json` serve stale/dead data to real dashboard/Telegram endpoints with no indication to the user.**
No writer exists anywhere in the current codebase for `arbitrage_log.json` (mtime matches repo checkout, git history shows the original scanner script was removed) — `/api/edges` and the Telegram edges view will forever return a frozen May 2026 snapshot. `live_alerts.json`'s only writer (`live_engine.py:846`) runs exclusively under `brain.py --live`, a flag the production `--bot` process never passes — `/api/live` will always return `[]` in the current deployment.

**M19. Dashboard "today" uses UTC while the rest of the system uses ET — live, currently-active instance of the UTC-mismatch bug class.**
`api.py:48-50` (`_utc_today()`) is UTC-based and used for `/api/scout`, `/api/bankroll`, `/api/summary`, and `_today_pnl()`'s date filter. Every other "today" computation in the system (bet logging at `brain.py:3025`, all of `bankroll_engine.py`'s pool/exposure/P&L functions) is ET-based. During the ~4-5 hour evening window each day (roughly 8pm–midnight ET) the UTC calendar date has already rolled to tomorrow, so the dashboard's "today" filters silently miss bets logged/settled that evening — exactly the bug class CLAUDE.md's hard rule (UTC internally, ET for display) exists to prevent, just introduced in the opposite direction, isolated to `api.py`.
*Fix:* Change `api.py`'s "today" computation to match the ET-based convention used everywhere else, or explicitly document why the dashboard intentionally differs.

**M20. The "2-hour pre-game filter" only blocks games that have already started, not games starting within 2 hours as documented.**
`brain.py:3139-3147`. Comment says "never bet on games starting within 2 hours"; actual check is `if _hours_until < 0.0: continue` — i.e. it only excludes games already in progress. A pick could be logged 2 minutes before first pitch with unconfirmed lineups/SPs. The parse is also wrapped in a bare `except Exception: pass` with no logging — malformed `commence_time` silently skips the check entirely.
*Fix:* Change the threshold to `_hours_until < 2.0`, and log parse failures instead of silently passing.

**M21. K-prop juice/odds-parsing failure silently includes rather than excludes a leg in 4 near-identical code blocks.**
`brain.py:1631-1635 / 1654-1658 / 2172-2175 / 2150-2154`. Odds-string parsing (`int(str(odds_val).replace("+",""))`) is wrapped in `except (ValueError, TypeError): pass`. If parsing fails, the `continue` that would exclude a bad-juice leg is skipped entirely, so a malformed odds string is silently **included** in the parlay — the opposite of the intended fail-safe direction.

**M22. GitHub Actions cron schedule has drifted an hour off its own documented ET times for 6 of 9 jobs, due to unadjusted DST.**
`.github/workflows/mega_scout.yml:7,8,9,10,11,12,13`. GH Actions cron is fixed-UTC and does not shift for DST. `daily_brain_day`/`evening` were tuned for EDT (summer) and currently match their comments; `daily_brain_west`, `line_movement`, `live_engine` (both windows), `daily_debrief`, `weekly_roi`, and `morning_planner` were tuned for EST (winter) and are currently firing **one hour later** than their own in-file comments claim. Worst case: `daily_debrief` (intended 11:30pm ET same-night) actually fires at 12:30am ET the *next calendar day* during DST — a genuine date-boundary risk for anything the debrief keys off "today." This silently flips back to matching comments each November and drifts again each March.
*Fix:* Either maintain two cron sets (DST/non-DST) or move all scheduling logic inside Python using `pytz`-aware "next N:MM ET" computation instead of hardcoded UTC cron strings.

**M23. `sp_monitor.py`'s 15-minute SP-change detection has no fallback if the Railway `--bot` process is down.**
It only runs as a daemon thread inside `brain.py --bot`; there is no GitHub Actions job or `scheduler.py` invocation for it. If Railway restarts or crashes, SP monitoring silently stops with no alternate path and no alerting specific to that failure mode.

**M24. Scheduled runs that die mid-window are not retried and lose that window's picks for the day.**
No checkpoint/resume system exists; `last_scout.json`/`props_output.json` are only written at the very end of a fully successful run (`brain.py:3889` onward). A crash or 25-minute GH Actions timeout mid-window means that window's games are never scouted that day (the next scheduled trigger is a different, non-overlapping time window) unless a human manually triggers `workflow_dispatch`. Re-running is safe (DB unique index + Telegram dedup both prevent duplicates), but there's no automatic recovery for a run that never completed.

**M25. Bankroll figures are internally contradictory across CLAUDE.md and the code, and none currently match `.env`.**
CLAUDE.md's env-var table says `BANKROLL_OVERRIDE=741`; its "Current State" section says `Bankroll: $300`; `math_engine.STARTING_BANKROLL=150.0` is the code's own fallback constant. `.env` currently has none of these set (see B11). Whichever number is correct needs to be confirmed with Aidan and set consistently.

**M26. `clv_log.json`/`brain_weights.json`/`props_output.json`/`last_scout.json`/`parlay_os.db` are all committed to git and rewritten on every scout run.**
`.gitignore` contains only `.worktrees/`. `parlay_os.db` alone has 189 commits in history at ~1-1.2MB per blob — steady, unbounded repo growth from a binary DB being versioned on every automated run, and every commit touching these files mixes real code changes with scout-run noise (confirmed: `git status` at session start showed all three modified simultaneously with no code changes pending).

**M27. Data health: `bets` table is 97.8% unsettled, some rows 46+ days old; CLV/closing-odds columns are essentially never populated; two calibration systems both barely/never fire.**
Concrete counts: `bets` — 45 rows, only 1 has `result` set (`'W'`), 44 are `NULL`; `closing_odds` NULL on 44/45, `clv_pct` NULL on 45/45; `sharp_signal`/`situations_triggered`/`abs_score`/`confidence_engine_score` NULL on all 45 rows despite existing as schema columns. `calibration_buckets` has exactly 1 row (`total_bets=2, wins=2`) which doesn't reconcile with `bets` (only 1 `'W'`, 0 `'L'` present) — a stale/orphaned learning signal. `model_calibration` (the second, parallel calibration table) has 20 seed rows, all `total=0`. `umpire_stats.updated_date` is frozen at `2026-05-27` for all 90 rows — 46+ days without a refresh during active season. Only 10 of 47 calendar days between 5/27 and 7/12 have any logged bets, with gaps up to 13 days — worth checking against `runlog.txt`/GH Actions run history for whether these represent missed scout runs.

### MINOR — cleanup, hygiene, nice-to-fix

- **`_sp_platoon_splits()`** (`sp_engine.py:493-545,591`) makes 2 extra Stats API calls per pitcher per game to compute `platoon_vulnerability`/`platoon_vuln_detail`/`platoon_splits`, none of which are read anywhere else — pure wasted I/O and dead output every run.
- **`market_engine.py`'s F5 (first-5-innings) odds path is dead**: `get_odds_for_event()` hardcodes `f5: None` (`market_engine.py:225-226`), so `f5_books` (`:29,519,547`) is always `{}`; `brain.py` never reads it. Docstring suggests this was intentionally disabled to avoid tripping a circuit breaker.
- **`confidence_engine.py:137-174`** selects `sp_gb_rate`/`first_pitch_strike_rate` from `bets` but `_extract_training_row()` never uses them — fetched and discarded, same shape as the known `est_woba`/`xwoba` mismatch bug class, just silent-drop instead of wrong-key.
- **`math_engine.py`'s `BankrollManager` class, `kelly_criterion()`, `hedge_calc()`, `expected_value()`** are all dead code — no instantiation/call sites outside the orphaned `clv_tracker.py`. Note `math_engine.kelly_criterion()` expects `true_prob_pct` as 0-100 while the live `bankroll_engine.kelly_stake()` expects 0-1 — a latent unit-mixing trap if anyone wires this back in by analogy.
- **`umpire_engine.get_umpire_edge()["total_adj"]`** (over/under lean signal) is computed but `brain.py` only ever reads `home_win_adj` from the same dict — the totals signal never reaches NRFI/over-under pricing. `umpire_telegram_flag()` is also imported but never called.
- **`bet_type_validator.live_stake()`** (flat 3%, "never use Kelly on in-game bets" per its own docstring) is dead — `live_engine.py` instead sizes LIVE bets with `bankroll_engine.kelly_stake()`, directly contradicting the documented rule.
- **`weather_engine.py`'s `WEATHER_ESTIMATED` flag** (set correctly when falling back to historical averages) is never read anywhere in brain.py — legitimate fallback, wasted transparency signal.
- **`intelligence_engine.py`'s regression/injury flags** (`direction: fade/back`) are computed and displayed in Telegram text but never numerically applied to win probability. `bullpen_regression_flags`, `format_sharp_pick`, `format_discord_pick` are imported but never called.
- **`statcast_engine.get_lineup_statcast`**, **`line_movement_engine.get_confidence_adj`**, **`home_dog_engine.home_dog_telegram_tag`** — each imported, none ever called.
- **Nine `savant_leaderboards.py` adjustment functions** (`arm_angle_platoon_adj`, `sprint_lambda_adj`, `chase_k_adj`, `framing_k_adj`, `team_of_lambda_adj`, `baserunning_lambda_adj`, `ps_conf_adj`, `bullpen_stuff_lambda_adj`, `batter_savant_signals`) — defined, none called anywhere.
- **`props_engine.py`'s `correlated_parlay()`/`scan_k_prop()`** — dead; `build_sgp_suggestions`/`brain.py` reimplement the logic inline instead.
- **`db.py`'s ROI-breakdown queries** (`get_roi_by_type`, `get_roi_by_sp`, `get_roi_by_park`, `get_roi_by_umpire`) — fully built, never called; `/api/stats` reimplements a cruder win/loss count instead.
- **`db.py`'s `save_bankroll_snapshot()`/`get_bankroll_history()`** (`bankroll_log` table) — writer and reader both dead; `bankroll_log` is 0 rows.
- **`db.py`'s `get_odds_history()`** — reader is dead even though the writer (`save_odds_snapshot`) fires every game; odds-history/CLV data is captured but never surfaced anywhere.
- **`ml_model.py`'s `compute_shap_explanation()`/`explain_prediction_text()`** — dead; no pick-explanation text is ever generated despite the machinery existing.
- **`scheduler.py`'s `_WEEKLY_TASKS` list** (`:194-198`) — defined, never iterated anywhere.
- **`api_client.py`'s `circuit_status()`** — never exposed on any endpoint or log; the circuit-breaker's live state is invisible to the operator.
- **`test_xwoba_pipeline.py::test_known_pitchers`** errors under `pytest -v` (not a skip) — the test takes a positional `data` arg that pytest interprets as an undeclared fixture. It passes when run as a standalone script (`python3 test_xwoba_pipeline.py`) but not the way the debugging docs actually invoke it.
- **`test_fixes.py::TestCLVCapture::test_clv_log_written_to_db`** has no DB-isolation fixture (unlike its sibling classes and unlike the same test on the `prediction-logging-schema` worktree branch, which does isolate it) — it writes real rows into the live production `parlay_os.db` on every run. Confirmed: two duplicate fake CLV rows for a fabricated "SF" bet exist in the live DB right now.
- **`discord_bot.py`** implements its own ad-hoc `.env` parser instead of using the app's normal env-loading convention, and silently swallows all exceptions around it.
- **`..env.swp`** (stale vim swap file) sitting in repo root — may contain fragments of `.env` contents; safe to delete.
- **`runlog.txt`/`errors.log`/`data_cache/`/`__pycache__/`/`.pytest_cache/`/`*.db-shm`/`*.db-wal`** are all currently untracked but **not** in `.gitignore` — currently safe only because nobody has run `git add -A`.
- **`transaction_monitor.py`** referenced in CLAUDE.md's architecture table does not exist and never has (confirmed via `git log --all --diff-filter=A`) — the functionality it describes actually lives in `sp_monitor.py:_check_il_transactions()` (with a second, apparently redundant IL-transaction fetcher in `intelligence_engine.py:197-245`). CLAUDE.md's table entry is inaccurate and should be corrected.

---

## 3. WIRED-BUT-DEAD INVENTORY

Everything below is built (imported and/or importable) but never actually connected into the live scoring/persistence path. Grouped by what kind of gap it is.

**Whole files with zero importers/callers anywhere in the repo:**
| File | What it does | Notes |
|---|---|---|
| `hitter_prop_engine.py` | xBA/xSLG hits & total-bases prop model | Superseded by a parallel inline system in brain.py (M13) |
| `clv_tracker.py` | Claude-powered post-game CLV/debrief pipeline | Reimplemented independently and incompatibly in `brain.py._run_debrief()` (M14) |
| `discord_bot.py` | Discord embed posting | No caller anywhere, not in any deploy config (M15) |
| `defense_engine.py`'s real functions | OAA defensive-runs adjustment | Only the import-fallback stub "runs" (M12) |

**Functions that persist data nothing reads back:**
| Writer | Table | Reader (exists, never called) |
|---|---|---|
| `profile_engine.update_sp_profile`/`update_hitter_profile` (called live from brain.py background thread) | `pitcher_profiles`, `hitter_profiles` | `memory_engine.get_pitcher_profile()`/`get_hitter_profile()` — zero callers (M11) |
| `profile_engine.update_bullpen_profile` (called live) | `bullpen_memory` | No reader exists at all, not even a dead one |
| `db.save_odds_snapshot` (called live, brain.py:238) | `odds_history` | `db.get_odds_history()` — zero callers |
| `db.log_clv` (called from bankroll_engine, never actually invoked) | `clv_log` (SQL) | `db.get_clv_log()` — only called from `test_fixes.py` |

**Functions defined, never called by anything (production or test), grouped by file:**
- `memory_engine.py`: `post_game_update`, `record_player_result`, `record_team_result`, `resolve_live_bet`, `adjust_model_prob`, `should_retrain_ml`, `update_clv_analytics`, `log_worst_bet`, `get_accuracy_trend`, `track_bet_pattern`, `update_sp_memory`
- `db.py`: `log_prop_result`, `settle_prop`, `get_prop_accuracy`, `set_weight_adj`, `save_bankroll_snapshot`, `get_bankroll_history`, `get_roi_by_type`/`by_sp`/`by_park`/`by_umpire`, `log_line_snapshot`, `check_integrity`, `get_umpire_stat`
- `profile_engine.py`: `run_nightly_profile_updates`/`run_weekly_team_updates` (called, but crash every time — B5), `update_team_profile`, `update_sequence_memory`, `get_series_context`
- `brain.py`: `_momentum_score`, `_format_sgp`, `_format_bet_message`, `_format_pass_message`, `_format_props_message`
- `math_engine.py`: `BankrollManager` (entire class), `kelly_criterion`, `hedge_calc`, `expected_value`
- `savant_leaderboards.py`: `arm_angle_platoon_adj`, `sprint_lambda_adj`, `chase_k_adj`, `framing_k_adj`, `team_of_lambda_adj`, `baserunning_lambda_adj`, `ps_conf_adj`, `bullpen_stuff_lambda_adj`, `batter_savant_signals`
- Misc: `props_engine.correlated_parlay`/`scan_k_prop`, `sp_engine._sp_platoon_splits` output fields, `statcast_engine.get_lineup_statcast`, `line_movement_engine.get_confidence_adj`, `umpire_engine.umpire_telegram_flag`, `home_dog_engine.home_dog_telegram_tag`, `intelligence_engine.bullpen_regression_flags`/`format_sharp_pick`/`format_discord_pick`, `ml_model.compute_shap_explanation`/`explain_prediction_text`, `bullpen_engine._is_closer`, `offense_engine._wrc_plus_rolling`/`_recent_form`, `market_engine.normalize_team_name`, `live_engine.run_live_cycle_compat`, `bankroll_engine.capture_pre_game_clv`, `clv_tracker.get_bankroll`, `api_client.circuit_status`, `bet_type_validator.live_stake`, `scheduler._WEEKLY_TASKS` (list, never iterated)

**DB tables that exist but have zero rows in the live database** (confirmed by direct SELECT COUNT):
`ballpark_memory`, `bankroll_log`, `betting_patterns`, `blind_spots`, `brain_weight_history`, `bullpen_memory`, `clv_analytics`, `confidence_weights`, `factor_reliability`, `game_updates_log`, `hitter_profiles`, `live_bet_memory`, `manager_memory`, `model_accuracy_log`, `pitcher_profiles`, `player_memory`, `situation_memory`, `sp_performance`, `team_memory`, `umpire_memory`, `worst_bets_log`, `prop_results`

**Tables referenced in code via `CREATE TABLE IF NOT EXISTS` that don't exist in the live DB at all** (proving those code paths have never executed once):
`sequence_memory`, `sp_season_ip`, `hitter_season_ab`, `team_profiles`, `physical_fatigue` (all `profile_engine.py`), `live_alert_log`, `live_component_weights` (`live_engine.py`)

**Duplicate/parallel systems doing the same job, only one of which is wired:**
- Two calibration systems: `db.py`'s `calibration_buckets` (written, essentially unread) vs. `memory_engine.py`'s `model_calibration` (both writer and reader dead)
- Two CLV pipelines: pre-game (`bankroll_engine`→SQL `clv_log` table, never called) vs. post-game (`telegram_handler`→`clv_log.json`, actively used)
- Two hitter-prop systems: `hitter_prop_engine.py` (orphaned) vs. `brain.py`'s inline `_scan_hitter_props` (actually used)
- Two debrief/CLV analysis systems: `clv_tracker.py` (orphaned, Claude-powered) vs. `brain.py._run_debrief()` (actually used, math_engine-based)
- Two IL-transaction fetchers: `sp_monitor.py._check_il_transactions()` (used) and `intelligence_engine.py:197-245` (unclear if used — flagged for follow-up)
- Two "8pm summary / 11pm debrief" schedulers: `scheduler.py`'s in-process triggers vs. GH Actions' `daily_debrief` cron job — both may fire independently; not confirmed whether they share the same dedup mechanism as the bet slip

---

## 4. SECURITY

**S1 — CONFIRMED: live production `TELEGRAM_BOT_TOKEN` and `ODDS_API_KEY` are recoverable from local git history.**
`git log --all --full-history -- .env` finds `.env` committed inside `refs/stash` (`stash@{0}`, commit `61202ca76ac8e08f2cecad6b79e0c3e083a3e075`, path `.env`), containing both keys. Both values match the current working `.env` exactly — these are the **active production keys**, not old/retired ones. The commit is reachable only via the stash ref, not any branch (local or remote), and stashes are never transmitted by `git push` — confirmed not pushed to GitHub. It is still fully recoverable via `git show`/`git stash show` on this machine and will persist until the stash is dropped and the repo is gc'd.
**Action: rotate `TELEGRAM_BOT_TOKEN` (BotFather) and `ODDS_API_KEY` (the-odds-api.com) now.** After rotating, `git stash drop` on `stash@{0}` (destructive — confirm with Aidan first) followed by `git reflog expire --expire=now --all && git gc --prune=now` to purge the blobs.

**S2 — `.gitignore` has essentially no safety net.**
The entire file is one line: `.worktrees/`. `.env`, `*.db-shm`/`*.db-wal`, `__pycache__/`, `data_cache/`, `*.log`, `*.swp` are all currently un-ignored — the only reason they aren't tracked is that nobody has run `git add -A` since the repo was last cleaned. This is exactly the mechanism that produced S1.
**Action:** add `.env`, `*.db-shm`, `*.db-wal`, `__pycache__/`, `.pytest_cache/`, `data_cache/`, `*.log`, `*.swp` to `.gitignore` immediately.

**S3 — No hardcoded secrets found in the current working tree or in `.github/workflows/mega_scout.yml`.**
The workflow correctly uses `${{ secrets.X }}` interpolation throughout; grepped all tracked `.py`/`.json`/`.yml` for key/token-shaped literals with no hits beyond the known S1 stash exposure. `ANTHROPIC_API_KEY` was checked separately across all of git history and every occurrence is a placeholder literal or `os.environ[...]` code, never a real value — no action needed on that key.

**S4 — Backup files (`brain.py.bak_fullslip`, `market_engine.py.bak_*`, etc.) the user suspected exist are confirmed to have existed, but only inside the same stash commit as S1 — not present on disk or tracked now.** Cleaning up the stash (S1's remediation) removes these too.

---

## 5. SCHEDULED JOBS SNAPSHOT

- Single workflow file: `.github/workflows/mega_scout.yml`, 9 jobs, no other workflow files exist.
- `gh run list --workflow=mega_scout.yml --limit 20` shows **all 20 most recent runs completed successfully** — no evidence of currently-failing runs in that sample (does not cover full history).
- Cron→ET drift (M22) is real and currently active — 6 of 9 jobs are firing 1 hour later than their own comments claim, because the workflow mixes crons tuned for EDT and EST without adjusting for DST.
- No job-level failure alerting exists in the workflow (no `if: failure()` step) — a failed run is only visible via the Actions UI or `gh run list`, not pushed to Telegram.
- `scheduler.py` (in-process, runs only inside `brain.py --bot` on Railway) handles a separate set of responsibilities (nightly/weekly maintenance, auto-settlement polling, prop expiry, umpire refresh) and does not define the day/evening/west game windows — those live in `brain.py:1000-1002` and correctly match CLAUDE.md's documented boundaries.
- `sp_monitor.py`'s 15-minute SP-change polling is real code, but only runs as a daemon thread inside `brain.py --bot` — entirely dependent on the Railway process staying up, with no GH Actions equivalent (M23).

---

## 6. QUESTIONS FOR THE OWNER

1. **Bankroll figure** — CLAUDE.md says `$741` in one place and `$300` in another, `.env` currently has `BANKROLL_OVERRIDE` unset entirely, and `math_engine.py` hardcodes a `$150` fallback. What is the actual current bankroll, and can `BANKROLL_OVERRIDE` be set in `.env`/Railway/GH Actions today? (B11, M25)
2. **Kelly band-floor behavior (M10)** — is it still intentional that the tier floor (e.g. $30 for HIGH) frequently overrides the Kelly-computed fraction, meaning stakes are largely flat-per-tier rather than truly Kelly-proportional within a tier? This was signed off 2026-07-07 but is worth reconfirming given how far the actual computed Kelly fraction can be from the floor in the worked example.
3. **Which orphaned subsystems should be wired in vs. deleted?** Specifically: `hitter_prop_engine.py` (a full second hitter-prop model), `clv_tracker.py` (a full second, Claude-powered debrief pipeline), `discord_bot.py` (Discord integration), `defense_engine.py`'s OAA adjustment, `arbitrage_log.json`'s dashboard tab (writer no longer exists in the codebase), `live_alerts.json`'s dashboard tab (writer only runs under a flag production never uses). Each is either dead weight or a genuine missing feature depending on intent — worth a quick keep/wire/delete decision per item rather than fixing all of them blind.
4. **Two calibration systems (M7)** — `calibration_buckets` (barely used, 1 row) and `model_calibration` (fully dead, 0 real rows) both exist. Should one be deleted and the other completed, or is there a reason for both to exist?
5. **Git stash cleanup (S1)** — after rotating the exposed keys, is it OK to `git stash drop` on `stash@{0}` and run `git gc --prune=now` to purge them from the local repo? This is destructive to that stash's contents (which also include the old `.bak_*` files) — confirming before doing it.
6. **Settlement for stuck-pending bets (B10)** — 44 of 45 bets in the live DB are currently unsettled, some 46+ days old. Is there a known reason recent settlement hasn't been running (e.g. this is a stale dev DB copy rather than the Railway production DB), or is this a live gap that needs the doubleheader-matching and stuck-pending-alert fixes prioritized?
7. **GH Actions cron/DST (M22)** — should the fix be two hardcoded cron sets swapped twice a year, or a rewrite to compute "next N:MM ET" in Python so this stops silently drifting?
8. **`transaction_monitor.py`** — CLAUDE.md documents this file as if it exists; it never has. Should CLAUDE.md be corrected to point at `sp_monitor.py:_check_il_transactions()` instead, and is the possibly-redundant second IL-transaction fetcher in `intelligence_engine.py` intentional?

---

*Compiled from 14 independent read-only subagent audits (flow trace, 3× engine inventory, wiring gaps, betting math, silent failures, data health, duplicate sources of truth, settlement/timezone, scheduled jobs, config/dependencies, security/hygiene, tests). No files other than this one were created or modified.*
