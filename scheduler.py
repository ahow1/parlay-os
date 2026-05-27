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
    """Always returns tz-aware ET datetime regardless of server TZ setting."""
    return datetime.now(timezone.utc).astimezone(ET)


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


def run_umpire_refresh_task():
    """Monday morning: refresh umpire stats DB from last 14 days of MLB games."""
    try:
        from umpire_engine import refresh_umpire_stats
        result = refresh_umpire_stats(days=14)
        log.info(f"[scheduler] Umpire refresh done: {len(result)} umpires updated")
    except Exception as e:
        log.error(f"[scheduler] Umpire refresh failed: {e}", exc_info=True)


def run_confidence_retrain_task():
    """Sunday 2am ET: retrain confidence logistic regression model on settled bets."""
    try:
        from confidence_engine import retrain_confidence_model
        report = retrain_confidence_model()
        status = report.get("status", "unknown")
        n      = report.get("n_bets", 0)
        acc    = report.get("accuracy", 0)
        log.info(f"[scheduler] Confidence retrain: status={status} n={n} accuracy={acc:.1%}")
        if status == "trained":
            log.info(f"[scheduler] Confidence model updated ({n} bets, acc={acc:.1%})")
        elif status == "insufficient_data":
            log.info(f"[scheduler] Confidence retrain skipped — {n}/{report.get('need',20)} bets available")
    except Exception as e:
        log.error(f"[scheduler] Confidence retrain failed: {e}", exc_info=True)


def run_pattern_report_task(send_telegram_fn=None):
    """Monday morning: compute betting pattern win rates and send Telegram summary."""
    try:
        from memory_engine import weekly_pattern_report
        msg = weekly_pattern_report(send_fn=send_telegram_fn)
        log.info("[scheduler] Weekly pattern report sent")
    except Exception as e:
        log.error(f"[scheduler] Pattern report failed: {e}", exc_info=True)


def run_auto_settlement_task():
    """Run MLB score-based auto-settlement for all pending bets."""
    try:
        from telegram_handler import run_settlement_check
        settled = run_settlement_check()
        if settled:
            log.info(f"[scheduler] Auto-settlement: settled {len(settled)} bet(s)")
            for s in settled:
                log.info(f"[scheduler]   {s['bet']} {s['outcome']} {s.get('score','')}")
        else:
            log.info("[scheduler] Auto-settlement: no new settlements")
    except Exception as e:
        log.error(f"[scheduler] Auto-settlement failed: {e}", exc_info=True)


def _get_brain():
    """Lazy import of brain module, cached at module level so tests can patch scheduler.brain."""
    global brain
    if brain is None:
        import brain as _brain_mod
        brain = _brain_mod
    return brain


brain = None  # module-level ref; populated lazily by _get_brain()


def run_debrief_task(send_fn=None):
    """11pm ET: send nightly debrief via brain._run_debrief."""
    try:
        _get_brain()._run_debrief(send_fn=send_fn)
        log.info("[scheduler] Nightly debrief sent")
    except Exception as e:
        log.error(f"[scheduler] Nightly debrief failed: {e}", exc_info=True)


def run_daily_summary_task(send_fn=None):
    """8pm ET: send full-day pick summary via brain._send_daily_summary."""
    try:
        _get_brain()._send_daily_summary(send_fn=send_fn)
        log.info("[scheduler] Daily summary sent")
    except Exception as e:
        log.error(f"[scheduler] Daily summary failed: {e}", exc_info=True)


def run_prop_settlement_task():
    """
    Nightly: auto-settle unsettled prop_results rows that are >= 1 day old.
    Marks expired props as 'expired' so accuracy stats aren't polluted.
    """
    try:
        from datetime import date as _date, timedelta as _td
        import db as _db
        cutoff = (_date.today() - _td(days=1)).isoformat()
        with _db._conn() as conn:
            # Mark any prop result older than yesterday with no result as expired
            updated = conn.execute(
                "UPDATE prop_results SET result='expired' WHERE result IS NULL AND date < ?",
                (cutoff,),
            ).rowcount
        if updated:
            log.info(f"[scheduler] Prop settlement: marked {updated} old props as expired")
    except Exception as e:
        log.error(f"[scheduler] Prop settlement failed: {e}", exc_info=True)


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

    last_daily        = ""   # YYYY-MM-DD
    last_weekly       = ""   # YYYY-WW
    last_monday       = ""   # YYYY-MM-DD — for Monday umpire refresh + pattern report
    last_prop_settle  = ""   # YYYY-MM-DD
    last_conf_retrain = ""   # YYYY-WW — Sunday confidence retrain
    last_auto_settle  = 0.0  # unix timestamp of last settlement run
    last_summary      = ""   # YYYY-MM-DD — 8pm daily summary
    last_debrief      = ""   # YYYY-MM-DD — 11pm nightly debrief

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

        # Auto-settlement: every 30 min from 9pm–1am ET
        _in_settle_window = (now_et.hour >= 21 or now_et.hour < 1)
        if _in_settle_window and (time.time() - last_auto_settle) >= 1800:
            run_auto_settlement_task()
            last_auto_settle = time.time()

        # Nightly 1am prop settlement (run once per day after midnight tasks)
        if today != last_prop_settle and now_et.hour == 1 and now_et.minute < 5:
            run_prop_settlement_task()
            last_prop_settle = today

        # Monday 7am ET: umpire stats refresh + weekly pattern report
        if (now_et.weekday() == 0                   # Monday
                and now_et.hour == 7
                and now_et.minute < 5
                and today != last_monday):
            run_umpire_refresh_task()
            # Pattern report: try to get send_fn from brain module
            _send_fn = None
            try:
                import brain as _brain_mod
                _send_fn = _brain_mod._send_telegram
            except Exception:
                pass
            run_pattern_report_task(send_telegram_fn=_send_fn)
            last_monday = today
            log.info(f"[scheduler] Monday tasks (umpire + pattern report) fired for {today}")

        # Weekly Sunday 2am task
        if (now_et.weekday() == 6
                and now_et.hour == 2
                and now_et.minute < 5
                and week != last_weekly):
            run_weekly_maintenance_task()
            run_confidence_retrain_task()
            try:
                from profile_engine import run_weekly_team_updates
                run_weekly_team_updates()
            except Exception as e:
                log.error(f"[scheduler] Weekly team updates failed: {e}", exc_info=True)
            last_weekly = week
            log.info(f"[scheduler] Weekly tasks (maintenance + conf_retrain) fired for week {week}")

        # 8pm ET: daily pick summary
        if (today != last_summary
                and now_et.hour == 20
                and now_et.minute < 5):
            _send_fn = None
            try:
                import brain as _brain_mod
                _send_fn = _brain_mod._send_telegram
            except Exception:
                pass
            run_daily_summary_task(send_fn=_send_fn)
            last_summary = today
            log.info(f"[scheduler] 8pm daily summary fired for {today}")

        # 11pm ET: nightly debrief
        if (today != last_debrief
                and now_et.hour == 23
                and now_et.minute < 5):
            _send_fn = None
            try:
                import brain as _brain_mod
                _send_fn = _brain_mod._send_telegram
            except Exception:
                pass
            run_debrief_task(send_fn=_send_fn)
            last_debrief = today
            log.info(f"[scheduler] 11pm debrief fired for {today}")

        time.sleep(_POLL_INTERVAL)


# ── STATUS / NEXT-RUN REPORT ──────────────────────────────────────────────────

def scheduler_status() -> dict:
    """Human-readable status for dashboard."""
    now      = _now_et()
    sun_2am  = _next_occurrence(weekday=6, hour=2)
    mon_7am  = _next_occurrence(weekday=0, hour=7)
    midnight = _next_occurrence(weekday=now.weekday(), hour=0)
    return {
        "current_et":           now.strftime("%Y-%m-%d %H:%M %Z"),
        "next_daily_et":        datetime.fromtimestamp(midnight, ET).strftime("%Y-%m-%d %H:%M %Z"),
        "next_weekly_et":       datetime.fromtimestamp(sun_2am, ET).strftime("%Y-%m-%d %H:%M %Z"),
        "next_weekly_secs":     int(_seconds_until(sun_2am)),
        "next_umpire_refresh":  datetime.fromtimestamp(mon_7am, ET).strftime("%Y-%m-%d %H:%M %Z"),
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

    _boot_et  = _now_et()
    _boot_utc = datetime.now(timezone.utc)
    log.info(
        f"[scheduler] Booting. Server UTC={_boot_utc.strftime('%H:%M')}, "
        f"ET={_boot_et.strftime('%H:%M %Z')} (offset {_boot_et.utcoffset()}). "
        f"All schedule times are ET."
    )
    status = scheduler_status()
    log.info(f"[scheduler] Next weekly: {status['next_weekly_et']}, "
             f"Next daily: {status['next_daily_et']}")

    # Run an immediate improvement check on startup
    run_improvement_check_task()

    schedule_loop()
