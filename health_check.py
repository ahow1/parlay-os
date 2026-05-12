"""PARLAY OS — health_check.py
Runs every 5 minutes. Checks every critical system. Alerts + auto-restarts on failure.

Usage:
  python health_check.py          # single check, print report
  python health_check.py --loop   # continuous 5-minute loop (Railway worker)
"""

import os
import sys
import time
import json
import sqlite3
import subprocess
import requests
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [HEALTH] %(message)s")
log = logging.getLogger(__name__)

BOT_TOKEN         = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID           = os.getenv("TELEGRAM_CHAT_ID", "")
ALERT_CHAT_ID     = os.getenv("TELEGRAM_ALERT_CHAT_ID", CHAT_ID)
ODDS_API_KEY      = os.getenv("ODDS_API_KEY", "")
DB_PATH           = os.getenv("PARLAY_DB", "parlay_os.db")
LAST_SCOUT_FILE   = Path("last_scout.json")
CHECK_INTERVAL    = 300   # 5 minutes

STATSAPI   = "https://statsapi.mlb.com/api/v1"
ODDS_BASE  = "https://api.the-odds-api.com/v4"
POLY_API   = "https://gamma-api.polymarket.com"
MIN_DISK_MB = 100


# ── Individual checks ─────────────────────────────────────────────────────────

def check_database() -> dict:
    """Verify SQLite is accessible and not corrupted."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA integrity_check").fetchone()
        n = conn.execute("SELECT COUNT(*) FROM bets").fetchone()[0]
        conn.close()
        return {"ok": True, "bets": n}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_odds_api() -> dict:
    """Verify Odds API responds and quota is not exhausted."""
    if not ODDS_API_KEY:
        return {"ok": False, "error": "ODDS_API_KEY not set"}
    try:
        r = requests.get(
            f"{ODDS_BASE}/sports/baseball_mlb/events",
            params={"apiKey": ODDS_API_KEY},
            timeout=10,
        )
        remaining = r.headers.get("x-requests-remaining", "?")
        if r.status_code == 401:
            return {"ok": False, "error": "Invalid API key"}
        if r.status_code == 429:
            return {"ok": False, "error": "Quota exhausted", "remaining": "0"}
        r.raise_for_status()
        return {"ok": True, "events": len(r.json()), "remaining": remaining}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_mlb_stats_api() -> dict:
    """Verify MLB Stats API is up."""
    try:
        r = requests.get(f"{STATSAPI}/sports/1", timeout=8)
        r.raise_for_status()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_telegram_bot() -> dict:
    """Verify bot token is valid."""
    if not BOT_TOKEN:
        return {"ok": False, "error": "BOT_TOKEN not set"}
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getMe",
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        return {"ok": data.get("ok", False), "bot": data.get("result", {}).get("username")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_last_scout() -> dict:
    """Verify a scout ran within the last 25 hours."""
    if not LAST_SCOUT_FILE.exists():
        return {"ok": False, "error": "last_scout.json missing"}
    try:
        with open(LAST_SCOUT_FILE) as f:
            data = json.load(f)
        ts_str = data.get("timestamp") or data.get("last_updated", "")
        if not ts_str:
            return {"ok": False, "error": "no timestamp in last_scout.json"}

        ts  = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        age = now - ts
        ok  = age < timedelta(hours=25)
        return {
            "ok":        ok,
            "age_hrs":   round(age.total_seconds() / 3600, 1),
            "timestamp": ts_str,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_polymarket() -> dict:
    """Verify Polymarket API is reachable."""
    try:
        r = requests.get(f"{POLY_API}/markets", params={"limit": 1}, timeout=8)
        r.raise_for_status()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_disk_space() -> dict:
    """Verify at least MIN_DISK_MB free on the working-directory partition."""
    try:
        stat = os.statvfs(".")
        free_mb = stat.f_bavail * stat.f_frsize / (1024 * 1024)
        ok = free_mb >= MIN_DISK_MB
        return {"ok": ok, "free_mb": round(free_mb, 1)}
    except AttributeError:
        # Windows fallback
        try:
            import shutil
            total, used, free = shutil.disk_usage(".")
            free_mb = free / (1024 * 1024)
            return {"ok": free_mb >= MIN_DISK_MB, "free_mb": round(free_mb, 1)}
        except Exception as e:
            return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_last_settlement() -> dict:
    """Verify auto-settlement ran within the last 25 hours (only meaningful at night)."""
    try:
        conn = sqlite3.connect(DB_PATH)
        row  = conn.execute("""
            SELECT MAX(timestamp) as last_settle
            FROM bets
            WHERE result IS NOT NULL AND result != ''
        """).fetchone()
        conn.close()
        if not row or not row[0]:
            return {"ok": True, "note": "no settled bets yet"}
        ts    = datetime.fromisoformat(row[0].replace("Z", "+00:00"))
        age   = datetime.now(timezone.utc) - ts
        # Only flag as stale after 25 hours
        ok    = age < timedelta(hours=25)
        return {"ok": ok, "age_hrs": round(age.total_seconds() / 3600, 1)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_process_running() -> dict:
    """Check if brain.py --bot is running (Railway worker process)."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "brain.py"],
            capture_output=True, text=True, timeout=5,
        )
        pids = result.stdout.strip().splitlines()
        running = len(pids) > 0
        return {"ok": running, "pids": pids}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Alert and restart ─────────────────────────────────────────────────────────

def _send_alert(msg: str):
    """Send alert to TELEGRAM_ALERT_CHAT_ID (separate from main chat)."""
    log.warning(f"ALERT: {msg}")
    if not BOT_TOKEN or not ALERT_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": ALERT_CHAT_ID, "text": f"🚨 HEALTH ALERT\n{msg}"},
            timeout=8,
        )
    except Exception as e:
        log.error(f"Alert send failed: {e}")


def _try_restart_brain():
    """Attempt to restart brain.py --bot in background."""
    log.info("Attempting to restart brain.py --bot...")
    try:
        subprocess.Popen(
            [sys.executable, "brain.py", "--bot"],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log.info("Restart issued")
        return True
    except Exception as e:
        log.error(f"Restart failed: {e}")
        return False


# ── Full health check ─────────────────────────────────────────────────────────

def run_health_check(auto_restart: bool = True) -> dict:
    """Run all checks. Alert on failures. Return full status dict."""
    now = datetime.utcnow().isoformat()
    log.info("Running health check...")

    results = {
        "timestamp":      now,
        "database":       check_database(),
        "odds_api":       check_odds_api(),
        "mlb_stats_api":  check_mlb_stats_api(),
        "polymarket":     check_polymarket(),
        "telegram_bot":   check_telegram_bot(),
        "last_scout":     check_last_scout(),
        "last_settlement":check_last_settlement(),
        "disk_space":     check_disk_space(),
        "process":        check_process_running(),
    }

    failures = [k for k, v in results.items() if k != "timestamp" and not v.get("ok")]

    # Build status summary
    all_ok = len(failures) == 0
    results["all_ok"]   = all_ok
    results["failures"] = failures

    if failures:
        alert_lines = [f"• {f}: {results[f].get('error', 'check failed')}" for f in failures]
        _send_alert("\n".join(alert_lines))

    # Auto-restart if process is dead
    if "process" in failures and auto_restart:
        restarted = _try_restart_brain()
        if restarted:
            _send_alert("brain.py was dead — restart issued")
        else:
            _send_alert("brain.py restart FAILED — manual intervention required")

    # Log summary
    status_str = "OK" if all_ok else f"FAILURES: {', '.join(failures)}"
    log.info(f"Health check: {status_str}")

    return results


def print_report(results: dict):
    print(f"\nHealth Check — {results['timestamp']}")
    print("=" * 50)
    for key, val in results.items():
        if key in ("timestamp", "all_ok", "failures"):
            continue
        icon = "✓" if val.get("ok") else "✗"
        detail = ""
        if not val.get("ok"):
            detail = f" — {val.get('error', '?')}"
        elif key == "database":
            detail = f" ({val.get('bets', 0)} bets)"
        elif key == "odds_api":
            detail = f" ({val.get('events', '?')} events, {val.get('remaining', '?')} calls remaining)"
        elif key == "last_scout":
            detail = f" ({val.get('age_hrs', '?')}h ago)"
        elif key == "disk_space":
            detail = f" ({val.get('free_mb', '?')} MB free)"
        elif key == "process":
            detail = f" (pids: {val.get('pids', [])})"
        print(f"  {icon} {key:<20} {detail}")
    print()
    if results.get("all_ok"):
        print("All systems operational")
    else:
        print(f"FAILURES: {', '.join(results.get('failures', []))}")


# ── Continuous loop ───────────────────────────────────────────────────────────

def run_loop():
    """Continuous 5-minute health check loop."""
    log.info("Health check loop started (5-minute interval)")
    while True:
        try:
            results = run_health_check()
            if not results.get("all_ok"):
                log.warning(f"Check failed: {results['failures']}")
        except Exception as e:
            log.error(f"Health check crashed: {e}")
            _send_alert(f"health_check.py itself crashed: {e}")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    if "--loop" in sys.argv:
        run_loop()
    else:
        results = run_health_check(auto_restart=False)
        print_report(results)
