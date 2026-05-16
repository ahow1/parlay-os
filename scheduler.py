"""PARLAY OS — scheduler.py
Background maintenance scheduler.

Responsibilities:
  • Sunday 2am ET: full weekly maintenance (accuracy audit, CLV rebuild, blind-spot scan)
  • Daily midnight ET: monthly accuracy log update
  • On-demand: post-game profile updates, improvement triggers

Run standalone:   python scheduler.py
Or import and call schedule_loop() in a background thread from brain.py / main.
"""

import logging
import signal
import sys
import time
from datetime import datetime, timezone

import pytz

log = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")

# ── HELPERS ───────────────────────────────────────────────────────────────────

def _now_et() -> datetime:
    return datetime.now(ET)


def _next_occurrence(weekday: int, hour: int, minute: int = 0) -> float:
    """Return Unix timestamp of next occurrence of weekday/hour/minute ET."""
    now  = _now_et()
    days = (weekday - now.weekday()) % 7
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    from datetime import timedelta
    candidate += timedelta(days=days)
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate.timestamp()


def _seconds_until(ts: float) -> float:
    return max(0.0, ts - time.time())


# ── TASKS ─────────────────────────────────────────────────────────────────────

def run_weekly_maintenance_task():
    try:
        from memory_engine import run_weekly_maintenance
        report = run_weekly_maintenance()
        log.info(f"[scheduler] Weekly maintenance done: {report.get('accuracy', {})}")
    except Exception as e:
        log.error(f"[scheduler] Weekly maintenance failed: {e}", exc_info=True)


def run_daily_accuracy_task():
    try:
        from memory_engine import log_monthly_accuracy
        log_monthly_accuracy("ML")
        log.info("[scheduler] Daily accuracy log updated")
    except Exception as e:
        log.error(f"[scheduler] Daily accuracy log failed: {e}", exc_info=True)


def run_nightly_profiles_task():
    """Update player/team profiles for all games played today."""
    try:
        from profile_engine import run_nightly_profile_updates
        run_nightly_profile_updates()
        log.info("[scheduler] Nightly profile updates done")
    except Exception as e:
        log.error(f"[scheduler] Nightly profiles failed: {e}", exc_info=True)


def run_improvement_check_task():
    """Check n-bet milestones and fire audits if needed."""
    try:
        from memory_engine import _check_improvement_triggers
        _check_improvement_triggers()
    except Exception as e:
        log.error(f"[scheduler] Improvement check failed: {e}", exc_info=True)


# ── SCHEDULER LOOP ────────────────────────────────────────────────────────────

# (weekday, hour, minute) → task function
# weekday: 0=Mon ... 6=Sun
_WEEKLY_TASKS = [
    (6, 2, 0, run_weekly_maintenance_task),  # Sunday 2:00am ET
    (6, 2, 30, lambda: __import__("profile_engine",
                                   fromlist=["run_weekly_team_updates"]).run_weekly_team_updates()),
]

_POLL_INTERVAL = 60  # seconds between heartbeat checks


def schedule_loop(stop_event=None):
    """
    Main scheduler loop. Runs until stop_event is set (or KeyboardInterrupt).
    Tracks last-run timestamps per task to avoid double-firing.
    """
    log.info("[scheduler] Starting scheduler loop")

    last_daily  = ""   # YYYY-MM-DD
    last_weekly = ""   # YYYY-WW

    while True:
        if stop_event and stop_event.is_set():
            log.info("[scheduler] Stop event received — exiting")
            break

        now_et = _now_et()
        today  = now_et.strftime("%Y-%m-%d")
        week   = now_et.strftime("%Y-%W")

        # Daily midnight task (run once per calendar day)
        if today != last_daily and now_et.hour == 0 and now_et.minute < 5:
            # Reset daily exposure cap so yesterday's pending bets don't count today
            try:
                import db as _db
                n = _db.reset_daily_exposure(last_daily)
                log.info(f"[scheduler] Midnight ET cap reset: cleared {n} stale pending bets from {last_daily}")
            except Exception as e:
                log.error(f"[scheduler] Daily cap reset failed: {e}", exc_info=True)
            run_daily_accuracy_task()
            run_nightly_profiles_task()
            run_improvement_check_task()
            last_daily = today
            log.info(f"[scheduler] Daily tasks fired for {today}")

        # Weekly Sunday 2am task
        if (now_et.weekday() == 6
                and now_et.hour == 2
                and now_et.minute < 5
                and week != last_weekly):
            run_weekly_maintenance_task()
            try:
                from profile_engine import run_weekly_team_updates
                run_weekly_team_updates()
            except Exception as e:
                log.error(f"[scheduler] Weekly team updates failed: {e}", exc_info=True)
            last_weekly = week
            log.info(f"[scheduler] Weekly tasks fired for week {week}")

        time.sleep(_POLL_INTERVAL)


# ── STATUS / NEXT-RUN REPORT ──────────────────────────────────────────────────

def scheduler_status() -> dict:
    """Human-readable status for dashboard."""
    now     = _now_et()
    sun_2am = _next_occurrence(weekday=6, hour=2)
    midnight = _next_occurrence(
        weekday=now.weekday(),
        hour=0
    )
    return {
        "current_et":       now.strftime("%Y-%m-%d %H:%M %Z"),
        "next_daily_et":    datetime.fromtimestamp(midnight, ET).strftime("%Y-%m-%d %H:%M %Z"),
        "next_weekly_et":   datetime.fromtimestamp(sun_2am, ET).strftime("%Y-%m-%d %H:%M %Z"),
        "next_weekly_secs": int(_seconds_until(sun_2am)),
    }


# ── ENTRY POINT ───────────────────────────────────────────────────────────────

def _handle_signal(signum, frame):
    log.info(f"[scheduler] Signal {signum} received — shutting down")
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    signal.signal(signal.SIGINT,  _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    status = scheduler_status()
    log.info(f"[scheduler] Booting. Next weekly: {status['next_weekly_et']}, "
             f"Next daily: {status['next_daily_et']}")

    # Run an immediate improvement check on startup
    run_improvement_check_task()

    schedule_loop()
