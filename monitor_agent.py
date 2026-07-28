"""PARLAY OS — monitor_agent.py
Agent 1 (THE MONITOR): a lightweight, always-on, rule-based health and
data-quality watcher. Runs as a daemon thread inside brain.py --bot on
Railway, alongside the CLV/settlement/sync loops. Zero external API calls
for analysis beyond two cheap connectivity checks — everything else reads
local files/DB the rest of the system already produces.

Toggle with MONITOR_ENABLED=false (default true).

Usage:
    import monitor_agent
    monitor_agent.run_monitor_loop()          # blocking, 15-min cadence
    monitor_agent.run_all_checks()            # one pass, for manual/testing use
    monitor_agent.get_monitor_status()        # last known state of every check
"""

import os
import re
import json
import time
import threading
from datetime import datetime, timedelta, timezone

import requests

import db as _db

CHECK_INTERVAL_SEC  = 900   # 15 minutes
ALERT_COOLDOWN_SEC  = 7200  # 2 hours — don't re-alert the same check more often than this
LAST_SCOUT_FILE     = "last_scout.json"
ERRORS_LOG_FILE     = "errors.log"
ERROR_SPIKE_THRESHOLD = 10   # ERROR-level lines in the trailing hour
NEUTRAL_FALLBACK_RATE_THRESHOLD = 0.40  # fraction of today's picks with >=1 fallback flag

BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALERT_CHAT_ID = os.getenv("TELEGRAM_ALERT_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", ""))
ODDS_API_KEY  = os.getenv("ODDS_API_KEY", "")

# ── Module state (read by get_monitor_status() / the /api/monitor endpoint) ──
_check_state: dict = {}
_last_alerted: dict[str, float] = {}
_state_lock = threading.Lock()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _send_alert(text: str) -> bool:
    """Self-contained Telegram send (mirrors error_logger.py's pattern) --
    monitor_agent must not import brain.py (brain.py imports things that
    would start a cycle) or telegram_handler's heavier machinery just to
    send one line."""
    if not BOT_TOKEN or not ALERT_CHAT_ID:
        print(f"[MONITOR] (no Telegram configured) {text}")
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": ALERT_CHAT_ID, "text": text},
            timeout=8,
        )
        return resp.status_code == 200
    except Exception as e:
        print(f"[MONITOR] alert send failed: {e}")
        return False


# ── Individual checks — each returns {"ok": bool, "detail": str, ...} ────────

def check_scout_freshness() -> dict:
    """Did the last scheduled scout run fire? Same 25h staleness threshold
    health_check.py already uses (GH Actions fires 3x/day, so 25h always
    covers a full cycle with margin)."""
    try:
        with open(LAST_SCOUT_FILE) as f:
            data = json.load(f)
    except Exception as e:
        return {"ok": False, "detail": f"{LAST_SCOUT_FILE} unreadable: {e}"}

    ts_str = data.get("timestamp") or data.get("last_updated") or ""
    if not ts_str:
        return {"ok": False, "detail": f"no timestamp in {LAST_SCOUT_FILE}"}
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_hrs = (_utc_now() - ts).total_seconds() / 3600
    except Exception as e:
        return {"ok": False, "detail": f"bad timestamp '{ts_str}': {e}"}

    ok = age_hrs < 25
    return {"ok": ok, "detail": f"last scout {age_hrs:.1f}h ago", "age_hrs": round(age_hrs, 1)}


def check_odds_feeds() -> dict:
    """Connectivity ping for both odds sources. Odds API gets a real
    lightweight call (same endpoint health_check.py already pings every
    5 min — this doesn't introduce new quota risk). SGO's free tier is
    quota-constrained (10 req/min, 2,500 entities/month per
    sportsgameodds_client.py) — pinging it independently every 15 min
    would burn quota the real CLV-capture pipeline needs, so SGO health is
    read passively from its own cache file's freshness instead of a live
    call."""
    problems = []

    if not ODDS_API_KEY:
        problems.append("ODDS_API_KEY not set")
    else:
        try:
            r = requests.get(
                "https://api.the-odds-api.com/v4/sports/baseball_mlb/events",
                params={"apiKey": ODDS_API_KEY},
                timeout=10,
            )
            if r.status_code == 401:
                problems.append("Odds API: invalid key")
            elif r.status_code == 429:
                problems.append("Odds API: quota exhausted")
            elif not r.ok:
                problems.append(f"Odds API: HTTP {r.status_code}")
        except Exception as e:
            problems.append(f"Odds API unreachable: {e}")

    try:
        from sportsgameodds_client import CACHE_FILE, CACHE_TTL_SEC
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        age_sec = time.time() - cache.get("fetched_at", 0)
        # Generous multiple of the real TTL -- a stale cache alone isn't a
        # failure (nothing may have needed a fetch recently); only flag if
        # it's stale for far longer than normal usage would ever leave it.
        if age_sec > CACHE_TTL_SEC * 12:
            problems.append(f"SGO cache stale ({age_sec/3600:.1f}h)")
    except FileNotFoundError:
        pass  # never fetched yet this deploy -- not itself a failure
    except Exception as e:
        problems.append(f"SGO cache unreadable: {e}")

    if problems:
        return {"ok": False, "detail": "; ".join(problems)}
    return {"ok": True, "detail": "odds feeds reachable"}


def check_neutral_fallback_rate() -> dict:
    """Are engines returning neutral fallbacks at an abnormal rate? Reads
    diagnostic_json on today's picks (built by brain.py's
    _build_ml_diagnostics/_build_game_diagnostics, both of which include a
    `flags` dict of boolean fallback indicators)."""
    today = _utc_now().date().isoformat()
    bets = [b for b in _db.get_bets(date=today) if b.get("diagnostic_json")]
    if not bets:
        return {"ok": True, "detail": "no picks with diagnostics today yet"}

    with_fallback = 0
    for b in bets:
        try:
            diag = json.loads(b["diagnostic_json"])
        except Exception:
            continue
        flags = diag.get("flags") or {}
        if any(bool(v) for v in flags.values()):
            with_fallback += 1

    rate = with_fallback / len(bets)
    ok = rate <= NEUTRAL_FALLBACK_RATE_THRESHOLD
    return {
        "ok": ok,
        "detail": f"{with_fallback}/{len(bets)} picks ({rate:.0%}) have a neutral fallback flag today",
        "rate": round(rate, 3),
    }


def check_stuck_pending_bets() -> dict:
    """Bets pending >48h -- reuses bankroll_engine's existing formalized
    stuck-pending detection rather than re-deriving it."""
    try:
        from bankroll_engine import get_stuck_pending_bets
        stuck = get_stuck_pending_bets()
    except Exception as e:
        return {"ok": False, "detail": f"check failed: {e}"}

    ok = len(stuck) == 0
    return {"ok": ok, "detail": f"{len(stuck)} bet(s) stuck pending >48h", "count": len(stuck)}


def check_clv_loop_activity() -> dict:
    """Is CLV capture actually writing rows? clv_log has no capture
    timestamp column (only the bet's game date), so the closest available
    signal is: if there are pending bets logged today, has anything been
    captured for today at all."""
    today = _utc_now().date().isoformat()
    pending_today = [b for b in _db.get_bets(date=today)
                     if not b.get("result") and (b.get("stake") or 0) > 0]
    if not pending_today:
        return {"ok": True, "detail": "no staked pending bets today to capture CLV for"}

    todays_clv = [c for c in _db.get_clv_log(days=1) if c.get("date") == today]
    ok = len(todays_clv) > 0
    return {
        "ok": ok,
        "detail": f"{len(todays_clv)} CLV row(s) captured today ({len(pending_today)} staked pending)",
    }


def check_error_spike() -> dict:
    """Any error spikes in the last hour? Coarser than error_logger.py's
    per-error-type recurring alert (3x/hour for the SAME error) -- this
    counts total ERROR-level lines across all sources in the trailing
    hour, catching a broad system-wide problem even if no single error
    type repeats often enough to trip that finer-grained alert."""
    if not os.path.exists(ERRORS_LOG_FILE):
        return {"ok": True, "detail": "no errors.log yet"}

    cutoff = _utc_now() - timedelta(hours=1)
    count = 0
    line_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| ERROR")
    try:
        with open(ERRORS_LOG_FILE, "r", errors="replace") as f:
            for line in f:
                m = line_re.match(line)
                if not m:
                    continue
                try:
                    ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except Exception:
                    continue
                if ts >= cutoff:
                    count += 1
    except Exception as e:
        return {"ok": False, "detail": f"couldn't read errors.log: {e}"}

    ok = count < ERROR_SPIKE_THRESHOLD
    return {"ok": ok, "detail": f"{count} error(s) in the last hour", "count": count}


_CHECKS = {
    "scout_freshness":       check_scout_freshness,
    "odds_feeds":             check_odds_feeds,
    "neutral_fallback_rate":  check_neutral_fallback_rate,
    "stuck_pending_bets":     check_stuck_pending_bets,
    "clv_loop_activity":      check_clv_loop_activity,
    "error_spike":            check_error_spike,
}


def _maybe_alert(name: str, result: dict) -> None:
    """Send a Telegram alert for a failing check, deduped with a cooldown
    per check name so a continuously-failing check doesn't spam every
    15-min tick."""
    if result.get("ok", True):
        return
    now = time.monotonic()
    with _state_lock:
        last = _last_alerted.get(name, 0.0)
        if now - last < ALERT_COOLDOWN_SEC:
            return
        _last_alerted[name] = now
    _send_alert(f"⚠️ MONITOR: {name} — {result.get('detail', 'check failed')}")


def run_all_checks() -> dict:
    """Run every check once, update module state, alert on new/ongoing
    failures (respecting cooldown), and return the full result dict.
    Never raises -- a single check's exception is caught so one broken
    check can't take down the whole monitor loop."""
    results = {"timestamp": _utc_now().isoformat()}
    for name, fn in _CHECKS.items():
        try:
            result = fn()
        except Exception as e:
            result = {"ok": False, "detail": f"check crashed: {e}"}
        results[name] = result
        _maybe_alert(name, result)

    failures = [k for k in _CHECKS if not results[k].get("ok", True)]
    results["all_ok"] = len(failures) == 0
    results["failures"] = failures

    with _state_lock:
        _check_state.clear()
        _check_state.update(results)
    return results


def get_monitor_status() -> dict:
    """Last known state of every check -- for the /api/monitor endpoint."""
    with _state_lock:
        if not _check_state:
            return {"status": "not yet run"}
        return dict(_check_state)


def is_enabled() -> bool:
    return os.getenv("MONITOR_ENABLED", "true").strip().lower() not in ("false", "0", "no")


def run_monitor_loop(stop_event=None) -> None:
    """Background loop: run_all_checks() every 15 minutes. Meant to run as
    a daemon thread started by brain.py in --bot mode."""
    _stop = stop_event or threading.Event()
    print("[MONITOR] loop started")
    while not _stop.is_set():
        try:
            results = run_all_checks()
            if not results.get("all_ok"):
                print(f"[MONITOR] failures: {results.get('failures')}")
        except Exception as e:
            print(f"[MONITOR] loop error: {e}")
        _stop.wait(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    import pprint
    pprint.pprint(run_all_checks())
