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

---

## Environment Variables

| Variable | Value | Notes |
|----------|-------|-------|
| `BANKROLL_OVERRIDE` | `741` | Current bankroll — must be set or pool/stakes will be wrong |
| `TELEGRAM_BOT_TOKEN` | secret | Bot auth |
| `TELEGRAM_CHAT_ID` | `7852968108` | Aidan's chat |
| `ODDS_API_KEY` | secret | The Odds API |
| `ANTHROPIC_API_KEY` | secret | Used by clv_tracker.py for Claude pick reviewer |

**Critical**: `BANKROLL_OVERRIDE` must be set in GitHub Actions secrets AND Railway environment vars.
Without it, `current_bankroll()` computes from the DB (deducting pending bets) and can collapse to
$27 while Kelly stakes stay at $9 — the daily cap ($3.32) blocks every bet on the first game.

---

## Current State

- Bankroll: $741 (update with `sed` command above when it changes)
- xwOBA: working — uses `est_woba` column from Savant leaderboard
- Rolling form: fixed — uses `rolling_xwoba_tier` key
- Auto-settlement: working
- Learning loop: needs wiring to `calibration_buckets`
- CLV capture: needs implementation

---

## Hard Rules — Never Break These

- Never change Kelly multipliers without explicit permission from Aidan
- Never mark slip as sent if it was empty (has_bets must be True)
- Always use UTC internally — convert to ET only for display
- RED day only when zero BET signals exist (`len(all_locks)+len(all_flips)==0`)
- Pool calculations only count today's UTC date — never cumulative
- Stake minimum $1.00, maximum 6% of bankroll
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

- **Railway**: runs `brain.py --bot` 24/7
- **GitHub Actions**: `mega_scout.yml` fires at 11am / 4pm / 7:30pm ET (day/evening/west windows)
- **Dashboard**: web-production-4366d.up.railway.app
- **Seed bets to Railway**: `POST /api/reset_bets`

### GitHub Actions Required Secrets
All three must be set in GitHub repo secrets:
- `ODDS_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `BANKROLL_OVERRIDE` ← critical, without this stakes collapse

### Railway Required Env Vars
Same four as above, plus `ANTHROPIC_API_KEY`.

---

## Priority Order — Always Work in This Order

1. **Picks reaching Telegram** — nothing else matters if broken
2. **Correct stake sizing** — Kelly must use real bankroll via `BANKROLL_OVERRIDE`
3. **Data pipeline** — xwOBA, rolling form, bullpen flowing correctly
4. **Learning loop** — settled bets feeding memory and calibration_buckets
5. **Dashboard accuracy**
6. **New features** — only after 1–5 confirmed working
