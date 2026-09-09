"""Per-scout-run feed health tracking.

Every external data source (FanGraphs, pybaseball, Savant, odds API, MLB
lineups, ...) records its outcome here exactly once per feed per run via
`record()`. `run_daily_scout()` calls `reset()` at the start of a run and
reads `as_dict()`/`live_count()`/`total_count()` when building the slip
and persisting `scout_runs`/`last_scout.json`.

Not thread-safe by design — one scout run owns the singleton at a time.
"""

import logging

log = logging.getLogger(__name__)

VALID_STATUSES = ("live", "degraded", "failed")


class DataHealth:
    def __init__(self):
        self._feeds: dict[str, str] = {}
        self._last_fail_detail: dict[str, str] = {}
        self._no_odds_skips = 0

    def reset(self) -> None:
        self._feeds.clear()
        self._last_fail_detail.clear()
        self._no_odds_skips = 0

    def record(self, feed: str, status: str) -> None:
        if status not in VALID_STATUSES:
            log.warning(f"[DATA HEALTH] unknown status '{status}' for feed '{feed}', treating as 'failed'")
            status = "failed"
        self._feeds[feed] = status
        if status != "live":
            log.warning(f"[DATA HEALTH] {feed} = {status}")

    def record_ok(self, feed: str, ok: bool, detail: str | None = None) -> None:
        """Merge a single call's pass/fail into the feed's run-level status —
        a feed that's called many times per run (e.g. one leaderboard fetch
        per game) shouldn't flip back to 'live' after a real failure just
        because a later call happened to succeed.

        `detail`, when given on a failing call (e.g. "HTTP 401"), is kept as
        the most recent failure reason for that feed — read back via
        `last_fail_detail()` so a caller reporting a feed-down alert can name
        the actual cause instead of just "it failed"."""
        current = self._feeds.get(feed)
        if current is None:
            self.record(feed, "live" if ok else "failed")
        elif ok and current != "live":
            self.record(feed, "degraded")
        elif not ok and current == "live":
            self.record(feed, "degraded")
        if not ok and detail:
            self._last_fail_detail[feed] = detail

    def last_fail_detail(self, feed: str) -> str | None:
        return self._last_fail_detail.get(feed)

    def mark_skip_no_odds(self) -> None:
        """Called once per game a scout run drops because no usable market
        (no-vig consensus) could be built for it — the precise symptom of a
        dead/quota-exhausted odds feed, as opposed to an unrelated skip like
        an unrecognised team code."""
        self._no_odds_skips += 1

    def skip_no_odds_count(self) -> int:
        return self._no_odds_skips

    def as_dict(self) -> dict:
        return dict(self._feeds)

    def live_count(self) -> int:
        return sum(1 for s in self._feeds.values() if s == "live")

    def total_count(self) -> int:
        return len(self._feeds)

    def summary(self) -> str:
        return f"{self.live_count()}/{self.total_count()} live"


_health = DataHealth()


def reset() -> None:
    _health.reset()


def record(feed: str, status: str) -> None:
    _health.record(feed, status)


def record_ok(feed: str, ok: bool, detail: str | None = None) -> None:
    _health.record_ok(feed, ok, detail)


def last_fail_detail(feed: str) -> str | None:
    return _health.last_fail_detail(feed)


def mark_skip_no_odds() -> None:
    _health.mark_skip_no_odds()


def skip_no_odds_count() -> int:
    return _health.skip_no_odds_count()


def as_dict() -> dict:
    return _health.as_dict()


def live_count() -> int:
    return _health.live_count()


def total_count() -> int:
    return _health.total_count()


def summary() -> str:
    return _health.summary()
