"""PARLAY OS — odds_quota.py
Tracks The Odds API's monthly credit allowance and enforces a hard guard on
live_engine, the single largest consumer of odds quota (markets x regions
credits per call, polled repeatedly through the evening — see
CLAUDE.md's live-engine quota-guard note).

Design: The Odds API reports cumulative usage itself via the
x-requests-remaining / x-requests-used response headers on every call —
that count is server-side and shared across every process hitting this
account/key, so we read it back rather than tallying credits locally
(local tallying would drift the moment two independent GitHub Actions job
runs, which don't share a database — see CLAUDE.md's Deployment section —
both make calls in the same month).

The one thing that DOES need to survive across separate GH Actions job
invocations is "the guard already tripped this month, stay disabled" —
kept in a small git-committed JSON file, the same pattern last_scout.json
already uses for cross-run state.
"""

import os
import json
from datetime import datetime, timezone

STATE_FILE = "odds_quota_state.json"

# Free-tier default (500 credits/month). Raise via env var if the account is
# ever upgraded to a paid plan — kept as a config value, not a hardcoded
# constant, so that doesn't require a code change.
MONTHLY_ALLOWANCE = int(os.getenv("ODDS_MONTHLY_ALLOWANCE", "500"))

GUARD_THRESHOLD_PCT = 0.80

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")


def _current_month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[QUOTA] failed to persist {STATE_FILE}: {e}")


def _to_int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def record_usage(headers) -> dict:
    """Parse The Odds API's quota headers off a response and log them so
    consumption is visible instead of inferred. Called from
    market_engine._odds_request() after every call that returns headers
    (success or a handled HTTPError), so this covers every consumer —
    scouts, line_movement, and live_engine alike — not just live_engine.

    Returns a snapshot: {"remaining": int|None, "used": int|None,
    "pct_used": float|None}. Persists the same snapshot to STATE_FILE so a
    later, separate process (e.g. live_engine's next invocation) can read
    the most recently observed usage without making its own call."""
    remaining = _to_int(headers.get("x-requests-remaining"))
    used      = _to_int(headers.get("x-requests-used"))

    if used is None and remaining is not None:
        used = MONTHLY_ALLOWANCE - remaining

    pct_used = (used / MONTHLY_ALLOWANCE) if (used is not None and MONTHLY_ALLOWANCE) else None

    if pct_used is not None:
        print(
            f"[QUOTA] x-requests-remaining={remaining} x-requests-used={used} "
            f"({pct_used * 100:.1f}% of {MONTHLY_ALLOWANCE}/mo allowance)"
        )
    else:
        print(f"[QUOTA] x-requests-remaining={remaining} x-requests-used={used} — no allowance data")

    state = _load_state()
    state["last_checked"] = datetime.now(timezone.utc).isoformat()
    state["remaining"]    = remaining
    state["used"]         = used
    state["pct_used"]     = round(pct_used, 4) if pct_used is not None else None
    _save_state(state)

    return {"remaining": remaining, "used": used, "pct_used": pct_used}


def get_last_snapshot() -> dict:
    """Most recently observed usage, from any process/consumer this month."""
    state = _load_state()
    return {
        "remaining": state.get("remaining"),
        "used":      state.get("used"),
        "pct_used":  state.get("pct_used"),
        "last_checked": state.get("last_checked"),
    }


def is_live_engine_disabled() -> tuple[bool, str]:
    """True + reason if the guard already tripped this calendar month.
    Scoped to live_engine only — scout runs never consult this and are
    never blocked by it (they're the actual product; see module docstring
    and CLAUDE.md)."""
    state = _load_state()
    if state.get("disabled_for_month") == _current_month():
        return True, state.get("disabled_reason", "quota guard tripped")
    return False, ""


def maybe_trip_guard(pct_used: float | None) -> bool:
    """Check the latest usage snapshot against the 80% threshold. If
    crossed and not already tripped this month, persist the disable and
    send exactly one Telegram alert. Returns True if live_engine should
    stop polling right now — either because this call just tripped the
    guard, or because it was already tripped earlier this month.

    Idempotent by design: a second call in the same month with pct_used
    still >= 80% (or missing entirely) does not re-alert — disabled_for_month
    already matches the current month, so is_live_engine_disabled() short-
    circuits before any new Telegram send."""
    already_disabled, _ = is_live_engine_disabled()
    if already_disabled:
        return True

    if pct_used is None or pct_used < GUARD_THRESHOLD_PCT:
        return False

    month  = _current_month()
    reason = f"{pct_used * 100:.1f}% of {MONTHLY_ALLOWANCE}/mo allowance used"

    state = _load_state()
    state["disabled_for_month"] = month
    state["disabled_reason"]    = reason
    state["disabled_at"]        = datetime.now(timezone.utc).isoformat()
    _save_state(state)

    print(f"[QUOTA] GUARD TRIPPED — {reason} — live_engine disabled for the rest of {month}")
    _send_alert(
        f"🚨 ODDS QUOTA GUARD TRIPPED — live_engine disabled for the rest of {month}. "
        f"{reason}. Scout runs are unaffected and always take priority."
    )
    return True


def _send_alert(text: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        print(f"[QUOTA] (no Telegram configured) {text}")
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": text},
            timeout=8,
        )
    except Exception as e:
        print(f"[QUOTA] alert send failed: {e}")
