# PARLAY OS

MLB betting intelligence system. Runs 24/7 on Railway with three processes:
- **web** — Flask API + dashboard (port 5000)
- **worker** — Telegram bot + daily scout
- **health** — 5-minute health checks with auto-restart

---

## Railway Deployment

### 1. Fork the repo on GitHub

Fork this repository to your GitHub account.

### 2. Connect to Railway

1. Go to [railway.app](https://railway.app) and sign in
2. Click **New Project → Deploy from GitHub repo**
3. Select your fork
4. Railway auto-detects `Procfile` and starts the build

### 3. Add environment variables

In Railway: **Project → Variables → Raw Editor**, paste and fill in:

```
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
TELEGRAM_ALERT_CHAT_ID=your_alert_channel_id
ODDS_API_KEY=your_primary_key
ODDS_API_KEY_BACKUP=your_backup_key
PORT=5000
PARLAY_DB=parlay_os.db
```

See `.env.example` for descriptions of each variable.

### 4. Deploy

Railway automatically runs all three Procfile processes after each push to `main`.

- `web` serves the dashboard at your Railway domain
- `worker` runs the Telegram bot + daily scout
- `health` pings all systems every 5 minutes and alerts via Telegram on failures

The `/health` endpoint is checked every 30 seconds by Railway. If it returns non-200 the service restarts automatically (up to 10 times).

---

## Local development

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and fill in your env vars
cp .env.example .env

# Run the scout once
python brain.py

# Run the API
python api.py

# Run the Telegram bot
python brain.py --bot

# Run health check
python health_check.py
```

---

## Architecture

| File | Purpose |
|---|---|
| `brain.py` | Main orchestrator — daily scout, live mode, debrief |
| `api.py` | Flask REST API + dashboard server |
| `health_check.py` | 10-point system health monitor |
| `api_client.py` | Unified HTTP client: rate limiting, cache, backoff, circuit breaker |
| `error_logger.py` | Centralized error logging + recurring-error Telegram alerts |
| `scheduler.py` | Background cron: Sunday 2am maintenance, daily accuracy log |
| `memory_engine.py` | Calibration, CLV analytics, worst-bet log, blind-spot detection |
| `profile_engine.py` | Player/team/sequence profile updaters |
| `live_engine.py` | 60-second live betting conviction scoring loop |
| `market_engine.py` | Odds API + Polymarket fetcher with backup key auto-switch |
| `sp_engine.py` | SP stats, rolling ERA, TTOP, ABS adjustments |
| `offense_engine.py` | Team offense, lineup confirmation, platoon splits |
| `bullpen_engine.py` | Bullpen fatigue (0–10 scale) and closer availability |
| `weather_engine.py` | wttr.in weather with historical-average fallback |
| `statcast_engine.py` | Baseball Savant exit velocity, barrel%, fastball velo |
| `ml_model.py` | XGBoost + LightGBM + LogReg ensemble |
| `math_engine.py` | Kelly criterion, CLV, Pythagorean, implied probability |
| `db.py` | SQLite layer with WAL mode, retry-on-lock, daily backup |
| `clv_tracker.py` | Post-game closing-line value tracker |

---

## Reliability features

- **Rate limiting** — 1 req/sec per domain via `api_client.py`
- **Response cache** — 5-minute GET cache; real-time endpoints use `skip_cache=True`
- **Exponential backoff** — 1s → 2s → 4s → 8s, 4 attempts
- **Circuit breaker** — 3 consecutive failures → 10-minute domain pause
- **Odds API failover** — auto-switches to `ODDS_API_KEY_BACKUP` on 429/401
- **Weather fallback** — historical park averages when wttr.in is down
- **DB WAL mode** — corruption-resistant write-ahead logging
- **DB backup** — daily copy to `backups/parlay_os_YYYY-MM-DD.db`, 7-day retention
- **DB locked retry** — 3 retries with 1s delay before raising
- **Error dedup alerting** — same error 3× in 1 hour → Telegram alert
- **Health auto-restart** — health_check.py relaunches dead worker process
