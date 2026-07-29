# Parlay OS — Claude Code Reference

## Commands

```bash
# Run scout (full, all windows)
rm -f /tmp/parlay_os_tg.lock && export $(cat .env | grep -v '#' | grep '=' | xargs) && python brain.py

# Run scout for a specific window
python brain.py --window day       # day games (<3pm ET)
python brain.py --window evening   # 3–8pm ET
python brain.py --window west      # 8pm+ ET

# Run bot (Railway mode, persistent)
python brain.py --bot

# Update bankroll (replace 741 with exact amount from Aidan)
sed -i 's/BANKROLL_OVERRIDE=.*/BANKROLL_OVERRIDE=741/' .env

# Clear pending bets for today
python -c "import sqlite3; c=sqlite3.connect('parlay_os.db'); c.execute('DELETE FROM bets WHERE date=date(\"now\") AND result IS NULL'); c.commit()"

# Force resend slip (clears slip_sent flag — slip NOT in SQLite, it's in last_scout.json)
python3 -c "import json; f=open('last_scout.json','r+'); d=json.load(f); d['slip_sent']=False; f.seek(0); json.dump(d,f,indent=2); f.truncate()"

# Debug picks — first thing to run when something is wrong
grep -E 'BET |SLIP|day=|locks=|flips=|has_bets|Scout done|POOL|ERROR|BLOCK' runlog.txt | head -40
```

---

## Architecture — Every File and What It Does

| File | Purpose |
|------|---------|
| `brain.py` | Main orchestrator — runs daily scout, builds Telegram slip, routes all flags |
| `bankroll_engine.py` | Kelly sizing, pool budgets, drawdown protection |
| `sp_engine.py` | Starting pitcher analysis: xFIP, ERA flags, velocity trends |
| `offense_engine.py` | Lineup analysis: wRC+, platoon splits |
| `bullpen_engine.py` | Bullpen fatigue scoring |
| `statcast_engine.py` | Baseball Savant CSV parsing |
| `savant_leaderboards.py` | xwOBA leaderboard — uses `est_woba` column, rolling form |
| `props_engine.py` | K props, hitter props, ER props |
| `memory_engine.py` | Persistent learning from settled bets |
| `sp_monitor.py` | SP change detection every 15 minutes |
| `transaction_monitor.py` | IL transaction alerts |
| `scheduler.py` | Cron logic, ET time windows |
| `db.py` | SQLite schema, all queries |
| `api.py` | Flask endpoints for dashboard |
| `parlay_dashboard.html` | Web dashboard |
| `constants.py` | Team maps, park factors, weights |
| `brain_weights.json` | Current learned model weights |
| `last_scout.json` | Scout output + slip dedup state (`slip_sent` flag) |
| `monitor_agent.py` | Agent 1 (THE MONITOR) — rule-based 24/7 health/data-quality watcher, zero LLM cost |
| `analyst_agent.py` | Agent 2 (THE ANALYST) — daily Claude-powered reflection agent; observes and builds evidence only, never changes model weights/thresholds |
| `agent_memory/` | Agent 2's local, gitignored institutional memory (`knowledge_base.json`, `open_questions.json`, `daily_debriefs/`) — persists on Railway's filesystem across deploys, never committed to git |

---

## Environment Variables

| Variable | Value | Notes |
|----------|-------|-------|
| `BANKROLL_OVERRIDE` | `741` | Current bankroll — must be set or pool/stakes will be wrong |
| `TELEGRAM_BOT_TOKEN` | secret | Bot auth |
| `TELEGRAM_CHAT_ID` | `7852968108` | Aidan's chat |
| `ODDS_API_KEY` | secret | The Odds API |
| `ANTHROPIC_API_KEY` | secret | Used by clv_tracker.py's Claude pick reviewer AND Agent 2 (THE ANALYST)'s daily debrief call |

**Critical**: `BANKROLL_OVERRIDE` must be set in GitHub Actions secrets AND Railway environment vars.
Without it, `current_bankroll()` computes from the DB (deducting pending bets) and can collapse to
$27 while Kelly stakes stay at $9 — the daily cap ($3.32) blocks every bet on the first game.

---

## Current State

- Bankroll: $300 (update with `sed` command above when it changes)
- Stake tiers (banded Kelly — see `CONVICTION_BANDS` in `bankroll_engine.py`):
  HIGH (sharp/locks) $30-40, MEDIUM (value/flips) $20-25, PROP (K-props/hitter
  props/NRFI/totals) $15-20 — bands are % of bankroll, so they rescale automatically
  if `BANKROLL_OVERRIDE` changes. Set 2026-07-07 with Aidan's explicit sign-off.
- xwOBA: working — uses `est_woba` column from Savant leaderboard
- Rolling form: fixed — uses `rolling_xwoba_tier` key
- Auto-settlement: working for ML/TOTAL/RUNLINE **and PROP** bets (hitter/K/ER props settle via MLB Stats API box scores as of 2026-07-28) — runs on Railway's `_settler_loop` (see Deployment). 1,136 PROP bets were stranded pending as of 2026-07-28 before this shipped; a single manual pass cleared 84 (52 W / 31 L / 1 P), with the remainder mostly blocked by a separate pre-existing data gap — ~1,035 older PROP rows have an empty `game` column and can't be matched to a game at all (not a settlement-logic bug; there's no reliable way to reconstruct which game those historical rows belonged to). New PROP picks logged going forward always populate `game`, so this shouldn't recur.
- Learning loop: working as of 2026-07-28 — `calibration_buckets` is fed directly by `telegram_handler.run_settlement_check()` (`db.feed_calibration_from_bet()`) at the moment each bet settles, in both passes. It used to be fed once/day by `brain._run_debrief()` on GitHub Actions, but that job never sees Railway's settlement results (Railway never touches git — see Deployment), so it had been silently dead since settlement moved to Railway-only. `calibration_buckets` was completely empty (0 rows) until a one-time backfill over the 123 already-resolved bets sitting in the DB; it now reflects real historical accuracy (55.3% overall win rate across 11 probability buckets). `_run_debrief()`'s daily Telegram win/loss/P&L summary is a separate, still-open gap (same root cause) — not fixed by this change.
- CLV capture: working — runs on Railway's `run_pre_game_clv_loop` (see Deployment)

---

## Hard Rules — Never Break These

- Never change Kelly multipliers without explicit permission from Aidan
- Never mark slip as sent if it was empty (has_bets must be True)
- Always use UTC internally — convert to ET only for display
- RED day only when zero BET signals exist (`len(all_locks)+len(all_flips)==0`)
- Pool calculations only count today's UTC date — never cumulative
- Stake minimum $1.00, maximum 15% of bankroll (absolute safety backstop — `MAX_STAKE_PCT`
  in `bankroll_engine.py`; tier ceilings above sit at 13.3%/8.3%/6.7% and bind first)
- Never update bankroll via estimate — always use exact number from Aidan
- Never clear bets table without backing up first
- Never deploy to Railway without testing locally

---

## Known Bugs and Fixes

| Bug | Fix |
|-----|-----|
| xwOBA column | Savant uses `est_woba` not `xwoba` |
| UTC mismatch | Always use `datetime.utcnow().date().isoformat()` |
| Telegram 400 | Sanitize angle brackets `<>` before sending |
| RED day bug | `day=RED` only when `len(all_locks)+len(all_flips)==0` |
| Pool negative | Filter pool_exposure by UTC today only — never cumulative |
| Slip dedup blocking | Only mark sent if `has_bets=True` AND `avg_stake > $1` |
| GH Actions bankroll | Add `BANKROLL_OVERRIDE` secret to GitHub — without it, daily cap collapses to ~$3 |
| Stake mismatch | brain.py daily cap uses `sizing_bankroll()` — Kelly and cap must use same basis |
| Telegram spam (empty/status-only sends) | `_daily_bet_slip` sent all 3 parts unconditionally, checked `has_bets` only after sending; near-miss "no qualifying bets" message also fired every empty run. Fixed 2026-07-07: gated all sends behind a real `_has_any_pick` check (locks/flips/parlay/nrfi/totals/props), near-miss message now log-only, never sent to Telegram |
| Daily cap / props pool too small for new stake bands | Raising stake bands without raising `daily_budget_pct` and `POOL_PROPS` reintroduces the "cap blocks everything" bug — the two must move together |
| Railway config-as-code is per-service, not shared | A single `railway.json` with a top-level `deploy.startCommand` applies to **every** Railway service that doesn't have its own config path set — there is no `services` array in Railway's schema. This meant `worker` and `health` had no service-specific config path set in their dashboard Settings, so both silently fell back to reading root `railway.json` and ran `python api.py` (the `web` command) instead of their real commands. **Practical effect: none of Railway's continuous loops — CLV capture, settlement, the Monitor, the Analyst, and the sync bridge — had ever actually run in production**, discovered 2026-07-29. Fixed by splitting into `railway.json` (web, default path — no dashboard change needed), `railway.worker.json`, and `railway.health.json`, with `worker`'s and `health`'s dashboard Config-as-code Path pointed at their respective files. If someone edits `railway.json` in the future assuming it's global, this regresses silently — always check whether the change needs to land in all three files. |
| FanGraphs xFIP silently truncated to top-30-by-WAR | `sp_engine.py`'s `FG_PITCHING_URL` had no `pageitems` param, so FanGraphs' leaderboard API returned only its default 30-row page (sorted by WAR desc) while `totalCount` in that same response was 761 — `data_health` reported the feed as "live" (it was — just paginated) while `get_real_xfip()` silently missed 81% of a real slate's actual starters (confirmed 2026-07-29: 21/26, including Tarik Skubal, Logan Webb), all falling through to `_xfip_estimate()`'s ERA/BB9/K9/HR9 formula fallback with no signal this had happened. Root-cause fixed by adding `&pageitems=2000000` (`test_fangraphs_pagination.py` guards the row count). As defense-in-depth for the remaining tail (rookies not yet listed, name-normalization misses), `earned_runs_engine.py` now reads the `xfip_source` tag sp_engine already computed but never consumed: an "estimated"/"unavailable" xFIP shrinks model_p_over toward the market baseline (`XFIP_ESTIMATE_SHRINK`) and caps confidence (`XFIP_ESTIMATE_CONF_CAP`) instead of computing a pick as if the fallback were real (`test_er_prop_xfip_downgrade.py`). |
| K-prop market_prob hardcoded to 0.5, discarding real SGO odds | `brain.py`'s `_norm_k()` set `"market_p": 0.5` unconditionally when building the slip's K-prop entries, even on picks where the K-prop collection loop upstream had already fetched a real SGO odds-implied `market_p` for that exact SP/line and used it to compute `edge_pct` correctly — the real value was computed, used for the (correct) displayed edge%, then thrown away before being stored. This corrupted the bets table's `market_prob` column for anything reading it directly later (backtesting, calibration audits) instead of recomputing from raw odds. Fixed 2026-07-29 by changing it to `b.get("market_p", 0.5)`, so the real SGO value (already present on the upstream dict) survives into storage, with 0.5 still the correct fallback when SGO genuinely has no market for that prop (`test_prop_market_prob.py`). Hitter props (`_norm_h`) were already correct — this only affected K props. |
| Same real-world game double-analyzed → duplicate prop picks | The Odds API occasionally lists one real-world game twice under two different event IDs a few minutes apart (confirmed 2026-07-29: ATL@NYM, same probable starters both times) — `get_mlb_events()` had no dedup, so the whole game (and every hitter/K/ER prop in it) got analyzed and appended twice in a single scout run, producing exact-duplicate picks in the slip. Root-cause fixed with `market_engine._dedup_events()`, which collapses same-team-pair events within 90 minutes of each other (a real doubleheader's games are hours apart, so those are never merged — `test_event_dedup.py`). As defense-in-depth, `brain.py`'s `_daily_bet_slip` now also dedupes `all_player_props`/`_over_cap_props` on (player, stat, game) before logging or sending (`test_prop_dedup.py`) — this required threading a `game` label through `_scan_hitter_props`/`_fetch_game_hitter_props`/`_norm_h`, which had never carried one before (hitter-prop bets rows had an always-blank `game` column until now). A full audit of `parlay_os.db` found zero already-graded duplicate PROP rows needing cleanup — the duplicates that reached Telegram apparently didn't both survive `MAX_PROPS_PER_DAY`'s top-N cut into logged bets. |

---

## Deduplication — How It Works

Slip dedup is **not** in SQLite. It lives in `last_scout.json`:
- `slip_sent: true` → skip resend (same date)
- `sent_pick_ids: [...]` → ML IDs already sent; only new picks trigger an update

To force a resend today, clear the flag:
```bash
python3 -c "import json; f=open('last_scout.json','r+'); d=json.load(f); d['slip_sent']=False; f.seek(0); json.dump(d,f,indent=2); f.truncate()"
```

---

## Debugging Decision Tree — Picks Not Reaching Telegram

```
Step 1: grep 'POOL' runlog.txt          — Is ML pool ≤ $0?
Step 2: grep 'day=' runlog.txt          — Is it RED when BET signals exist?
Step 3: grep 'locks= flips=' runlog.txt — Are BET signals being found?
Step 4: grep 'BLOCK' runlog.txt         — Is daily cap blocking all bets?
Step 5: grep 'has_bets' runlog.txt      — Is the slip being built?
Step 6: grep 'ERROR' runlog.txt         — Is there a crash?
Step 7: Fix first error found, rerun scout from step 1
```

Common quick checks:
```bash
# Full diagnostic in one command
grep -E 'BET |SLIP|day=|locks=|flips=|has_bets|Scout done|POOL|ERROR|BLOCK' runlog.txt | head -40

# Is BANKROLL_OVERRIDE set?
echo $BANKROLL_OVERRIDE

# What bankroll is the scout using?
python3 -c "from bankroll_engine import sizing_bankroll; print(f'sizing_bankroll: \${sizing_bankroll():.2f}')"
```

---

## Deployment

Railway is a permanent, paid, always-on host as of 2026-07-28. The system is
split so each function runs in **exactly one place** — no job runs on both
GitHub Actions and Railway.

- **GitHub Actions** (`mega_scout.yml`): **pick generation only** —
  `daily_brain_day` (11am ET), `daily_brain_evening` (4pm ET),
  `daily_brain_west` (6:30pm ET), plus `line_movement`, `live_engine`,
  `daily_debrief`, `weekly_roi`, `morning_planner`. One-shot, scheduled,
  proven. Each job checks out fresh from git, runs, and commits its
  non-database outputs (`last_scout.json` / `props_output.json` /
  `clv_log.json` / `live_alerts.json`) back.
- **`parlay_os.db` is NOT committed to git as of 2026-07-28.** It was
  tracked from the repo's very first commit and, since this repo is
  **public**, every scheduled run had been publishing real stakes,
  results, and bankroll figures in plain sight in git history. `*.db` is
  now gitignored and the file was `git rm --cached`. Each GH Actions job's
  local `parlay_os.db` is now purely a scratch file for that single run —
  it is never persisted anywhere and is destroyed when the runner exits.
  **This means GitHub Actions no longer has any durable pick history at
  all** — that claim from the old two-database design is gone, not just
  stale. Telegram delivery is unaffected (direct send, independent of
  git); only git-based persistence of the picks is gone.
- **GitHub → Railway pick sync is push-based, not pull-based.** The old
  `db.sync_pending_bets_from_github()` / `run_github_bet_sync_loop()`
  fetched `parlay_os.db` from a public `raw.githubusercontent.com` URL —
  dead once the db stopped being committed to git. Replaced (2026-07-29)
  with a push: right after `brain.py`'s `_log_bet_with_retry` /
  `_log_pick_with_retry` successfully log a pick locally on a GH Actions
  scout run, `brain._push_synced_pick()` fetches that just-inserted row
  and calls `db.push_bet_to_railway()`, which `POST`s it to Railway's
  `POST /api/sync_bet` endpoint (`api.py`), auth'd with a shared
  `SYNC_SECRET` bearer token. The receiving side, `db.insert_synced_bet()`,
  dedupes by `verify_hash` (reusing the sender's original hash/timestamp,
  never recomputing them) and only ever inserts — it never overwrites an
  existing local row, same guarantee the old bridge made. No loop runs on
  Railway's side anymore; the push happens once per pick, at log time.
  `push_bet_to_railway()` no-ops (no HTTP call) unless both
  `RAILWAY_SYNC_URL` and `SYNC_SECRET` are set — true on Railway itself,
  which never needs to push to itself.
- **Railway** (`brain.py --bot`, persistent worker): **all continuous loops**
  — CLV capture (`run_pre_game_clv_loop`, every 15 min), settlement
  (`_settler_loop`, every 30 min 4pm–1am ET, no `days_back` bound), SP
  monitor, hedge monitor, and the Telegram command handler (`/win` `/loss`
  `/push` `/bet` `/scout` etc). Railway's worker never touches git — its
  local `parlay_os.db` lives on Railway's persistent filesystem
  (unaffected by the git change above) and remains **the only durable
  record of settlement results and CLV grading** (whether a pick actually
  won, and what the closing line was) — this was always true, since
  Railway never depended on git for its own db's persistence.
- If you need a full settled history, pull it from Railway's copy (via its
  dashboard/DB export) — it's now the only copy with any durable history
  at all, not one of two intentional copies.
- `brain.py --capture-clv` and `brain.py --settle` (the one-shot CLI modes)
  still exist and are still wired into `mega_scout.yml` as
  `workflow_dispatch`-only manual fallbacks — trigger them from the Actions
  tab if Railway is ever down. They no longer run on a schedule.
- **Dashboard**: web-production-4366d.up.railway.app
- **Seed bets to Railway**: `POST /api/reset_bets`
- **Config-as-code is split per service** (see Known Bugs table for why): `railway.json`
  (repo root, default path → `web` service, `python api.py`), `railway.worker.json`
  (→ `worker` service, `python brain.py --bot`), `railway.health.json` (→ `health`
  service, `python health_check.py --loop`). Each of `worker` and `health`'s Railway
  dashboard **Settings → Config-as-code Path** must point at its matching file — this
  is a per-service dashboard setting, not something a repo file alone can control. If
  you add a 4th service or rename one, it needs both a new file here AND the dashboard
  path set, or it silently inherits `web`'s command.

### GitHub Actions Required Secrets
- `ODDS_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `BANKROLL_OVERRIDE` ← critical, without this stakes collapse
- `RAILWAY_SYNC_URL` ← Railway dashboard base URL (e.g. `https://web-production-4366d.up.railway.app`), used by the 3 scout jobs (day/evening/west) to push newly-logged picks to Railway
- `SYNC_SECRET` ← shared bearer-token secret for `POST /api/sync_bet`; must match Railway's `SYNC_SECRET` exactly

### Railway Required Env Vars
- `TELEGRAM_BOT_TOKEN` ← must match the GitHub secret exactly (same bot, one chat)
- `TELEGRAM_CHAT_ID`
- `ODDS_API_KEY`
- `SPORTSGAMEODDS_API_KEY` ← needed now that CLV capture runs here (SGO no-vig consensus)
- `BANKROLL_OVERRIDE`
- `SYNC_SECRET` ← must match the GitHub secret exactly; auths incoming `POST /api/sync_bet` requests. Do NOT also set `RAILWAY_SYNC_URL` here — that would make Railway try to push picks to itself.
- `ODDS_SOURCE=sgo`
- `ANTHROPIC_API_KEY` ← used by clv_tracker.py's Claude pick reviewer AND Agent 2 (THE ANALYST)
- `MONITOR_ENABLED` ← Agent 1 (THE MONITOR), default `true`. Set `false` to disable without a code change.
- `TELEGRAM_ALERT_CHAT_ID` ← optional, defaults to `TELEGRAM_CHAT_ID`. Where Monitor alerts go.
- `ANALYST_ENABLED` ← Agent 2 (THE ANALYST), default `true`. Set `false` to disable without a code change.

### Agent 1 (THE MONITOR) — rule-based, zero LLM cost
24/7 daemon thread in `--bot` mode, checks every 15 min: scout freshness, odds
feed connectivity, abnormal neutral-fallback rates, stuck pending bets (>48h),
CLV capture activity, error spikes. Sends `⚠️ MONITOR: ...` Telegram alerts,
deduped with a 2-hour cooldown per check. `GET /api/monitor` exposes its last
known state as JSON.

### Agent 2 (THE ANALYST) — daily, Claude-powered, adaptive memory
Fires once/day at 1:30am ET in `--bot` mode (after Railway's last settle
pass), or manually via `python brain.py --debrief-agent`. Reads its own prior
findings (`agent_memory/knowledge_base.json`) and open questions
(`agent_memory/open_questions.json`), reads that day's staked + over_cap picks
with results/CLV/diagnostics, makes **exactly one** Claude API call
(`claude-sonnet-4-6`, `max_tokens=3000`) to analyze what happened, then writes
a debrief file (`agent_memory/daily_debriefs/YYYY-MM-DD.md`), appends new
findings to the knowledge base and the `analyst_findings` DB table, updates
open questions, and sends a condensed Telegram summary.
- **Cost**: `claude-sonnet-4-6` is $3/$15 per MTok in/out. With `max_tokens=3000`
  and input growing from ~3K tokens (early) toward ~10-15K tokens/day as the
  knowledge base fills (context is capped at the most recent 50 entries +
  all high-confidence ones once the KB passes 100 total — see
  `KB_CONTEXT_CAP_THRESHOLD` in `analyst_agent.py`), each call costs roughly
  $0.03-$0.08 — well under the $0.50-1.00/day upper bound this feature was
  originally scoped against. Realistic run rate: **~$1-3/month**.
- **Safety**: the Analyst OBSERVES and BUILDS EVIDENCE ONLY — it can never
  recommend or make a weight/threshold change. This isn't just a system-prompt
  instruction: `contains_weight_change_language()` scans every free-text field
  in Claude's response and redacts anything that reads like a tuning
  recommendation before it can reach Telegram or memory, regardless of what
  the model actually said. Covered by
  `test_analyst_agent.py::TestWeightChangeSafety`.
- `agent_memory/` is gitignored — local institutional memory, not code.
  Railway's persistent filesystem keeps it safe across deploys.

---

## Priority Order — Always Work in This Order

1. **Picks reaching Telegram** — nothing else matters if broken
2. **Correct stake sizing** — Kelly must use real bankroll via `BANKROLL_OVERRIDE`
3. **Data pipeline** — xwOBA, rolling form, bullpen flowing correctly
4. **Learning loop** — settled bets feeding memory and calibration_buckets
5. **Dashboard accuracy**
6. **New features** — only after 1–5 confirmed working
