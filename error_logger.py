"""PARLAY OS — error_logger.py
Centralized error logging with deduplication and Telegram alerting.

Features:
  • All unhandled exceptions logged to errors.log with full traceback
  • Same error type occurring 3+ times in 1 hour → Telegram alert
  • Weekly rotation — keeps last 4 weeks of logs
  • setup() installs as Python root-logger exception handler

Usage:
    import error_logger
    error_logger.setup()                        # call once at startup
    error_logger.log("market_engine", exc)      # call in except blocks
"""

import os
import sys
import time
import traceback
import logging
import logging.handlers
import threading
from collections import defaultdict, deque
from datetime import datetime

BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALERT_CHAT_ID = os.getenv("TELEGRAM_ALERT_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID", ""))
LOG_FILE      = "errors.log"
_ALERT_WINDOW = 3600    # 1-hour window for dedup
_ALERT_THRESH = 3       # occurrences before alerting

# ── File handler (weekly rotation, keep 4 weeks) ─────────────────────────────

_file_handler = logging.handlers.TimedRotatingFileHandler(
    LOG_FILE,
    when="W0",           # rotate every Monday
    backupCount=4,
    encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
_file_handler.setLevel(logging.ERROR)

_err_log = logging.getLogger("parlay.errors")
_err_log.setLevel(logging.ERROR)
_err_log.addHandler(_file_handler)
_err_log.propagate = False

# ── Dedup tracker ─────────────────────────────────────────────────────────────

_dedup_lock   = threading.Lock()
_recent_errs: dict[str, deque] = defaultdict(deque)  # error_key → deque of timestamps
_alerted: dict[str, float]     = {}                   # error_key → last alert time


def _error_key(engine: str, error_type: str) -> str:
    return f"{engine}::{error_type}"


def _should_alert(key: str) -> bool:
    """Return True if this error has occurred ≥ _ALERT_THRESH times in the last hour."""
    with _dedup_lock:
        now  = time.monotonic()
        dq   = _recent_errs[key]
        dq.append(now)
        # Prune events older than 1 hour
        while dq and now - dq[0] > _ALERT_WINDOW:
            dq.popleft()
        count = len(dq)

        if count >= _ALERT_THRESH:
            last = _alerted.get(key, 0.0)
            if now - last > _ALERT_WINDOW:  # don't spam within same hour
                _alerted[key] = now
                return True
    return False


# ── Telegram alert ────────────────────────────────────────────────────────────

def _send_alert(text: str):
    if not BOT_TOKEN or not ALERT_CHAT_ID:
        return
    try:
        import requests as _req
        _req.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": ALERT_CHAT_ID, "text": text},
            timeout=8,
        )
    except Exception:
        pass  # Never crash the error logger itself


# ── Main logging function ─────────────────────────────────────────────────────

def log_error(engine: str, exc: Exception, extra: str = ""):
    """
    Log an exception to errors.log. Alert Telegram if it's recurring.

    Args:
        engine: module/function name (e.g. 'market_engine', 'brain.analyze_game')
        exc:    the caught exception
        extra:  optional context string
    """
    error_type = type(exc).__name__
    tb_str     = traceback.format_exc()
    now_str    = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    msg = f"[{engine}] {error_type}: {exc}"
    if extra:
        msg += f" | ctx={extra}"

    _err_log.error(f"{msg}\n{tb_str}")

    key = _error_key(engine, error_type)
    if _should_alert(key):
        alert = (
            f"🔴 RECURRING ERROR — {_ALERT_THRESH}× in 1h\n"
            f"Engine:  {engine}\n"
            f"Error:   {error_type}: {str(exc)[:200]}\n"
            f"Time:    {now_str}"
        )
        _send_alert(alert)


# ── Global exception hook ─────────────────────────────────────────────────────

def _excepthook(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    _err_log.error(f"[UNHANDLED] {exc_type.__name__}: {exc_value}\n{tb_str}")
    _send_alert(
        f"💥 UNHANDLED EXCEPTION\n{exc_type.__name__}: {str(exc_value)[:300]}\n"
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    sys.__excepthook__(exc_type, exc_value, exc_tb)


def setup():
    """
    Install error_logger as the global exception handler.
    Call once at the start of brain.py / api.py.
    """
    sys.excepthook = _excepthook
    # Also attach to root logger so all logging.error() calls hit our file
    root = logging.getLogger()
    if _file_handler not in root.handlers:
        root.addHandler(_file_handler)
