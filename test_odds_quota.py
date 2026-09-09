"""Tests for odds_quota.py -- The Odds API monthly-allowance tracking and
the hard guard that disables live_engine (never scouts) once cumulative
usage crosses 80% of the configured monthly allowance.

Run: python -m pytest test_odds_quota.py -v
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

import odds_quota


@pytest.fixture(autouse=True)
def _isolate_state(tmp_path, monkeypatch):
    monkeypatch.setattr(odds_quota, "STATE_FILE", str(tmp_path / "odds_quota_state.json"))
    monkeypatch.setattr(odds_quota, "MONTHLY_ALLOWANCE", 500)


class TestRecordUsage:
    def test_parses_both_headers(self):
        snap = odds_quota.record_usage({"x-requests-remaining": "350", "x-requests-used": "150"})
        assert snap == {"remaining": 350, "used": 150, "pct_used": 0.30}

    def test_derives_used_from_remaining_when_used_header_absent(self):
        snap = odds_quota.record_usage({"x-requests-remaining": "400"})
        assert snap["used"] == 100
        assert snap["pct_used"] == 0.20

    def test_missing_headers_yields_all_none(self):
        snap = odds_quota.record_usage({})
        assert snap == {"remaining": None, "used": None, "pct_used": None}

    def test_non_numeric_header_values_do_not_crash(self):
        snap = odds_quota.record_usage({"x-requests-remaining": "unlimited", "x-requests-used": "n/a"})
        assert snap == {"remaining": None, "used": None, "pct_used": None}

    def test_persists_snapshot_for_later_read(self):
        odds_quota.record_usage({"x-requests-remaining": "350", "x-requests-used": "150"})
        snap = odds_quota.get_last_snapshot()
        assert snap["remaining"] == 350
        assert snap["used"] == 150
        assert snap["pct_used"] == 0.30
        assert snap["last_checked"]

    def test_get_last_snapshot_before_any_call(self):
        snap = odds_quota.get_last_snapshot()
        assert snap == {"remaining": None, "used": None, "pct_used": None, "last_checked": None}

    def test_respects_custom_monthly_allowance(self, monkeypatch):
        monkeypatch.setattr(odds_quota, "MONTHLY_ALLOWANCE", 1000)
        snap = odds_quota.record_usage({"x-requests-used": "500"})
        assert snap["pct_used"] == 0.5


class TestGuardThreshold:
    def test_below_threshold_does_not_trip(self):
        with patch.object(odds_quota, "_send_alert") as mock_alert:
            tripped = odds_quota.maybe_trip_guard(0.79)
        assert tripped is False
        mock_alert.assert_not_called()
        assert odds_quota.is_live_engine_disabled() == (False, "")

    def test_exactly_at_threshold_trips(self):
        with patch.object(odds_quota, "_send_alert") as mock_alert:
            tripped = odds_quota.maybe_trip_guard(0.80)
        assert tripped is True
        mock_alert.assert_called_once()

    def test_above_threshold_trips_and_disables_for_the_month(self):
        with patch.object(odds_quota, "_send_alert"):
            odds_quota.maybe_trip_guard(0.85)
        disabled, reason = odds_quota.is_live_engine_disabled()
        assert disabled is True
        assert "85.0%" in reason

    def test_none_pct_used_does_not_trip(self):
        with patch.object(odds_quota, "_send_alert") as mock_alert:
            tripped = odds_quota.maybe_trip_guard(None)
        assert tripped is False
        mock_alert.assert_not_called()

    def test_alert_message_names_scout_priority(self):
        with patch.object(odds_quota, "_send_alert") as mock_alert:
            odds_quota.maybe_trip_guard(0.9)
        msg = mock_alert.call_args[0][0]
        assert "Scout runs are unaffected" in msg
        assert "live_engine" in msg.lower() or "live" in msg.lower()


class TestGuardIdempotency:
    """Once tripped, the guard must stay tripped for the rest of the
    calendar month without re-alerting on every subsequent check."""

    def test_second_trip_in_same_month_does_not_realert(self):
        with patch.object(odds_quota, "_send_alert") as mock_alert:
            odds_quota.maybe_trip_guard(0.9)
            odds_quota.maybe_trip_guard(0.95)
        assert mock_alert.call_count == 1

    def test_already_disabled_short_circuits_before_checking_pct(self):
        """Even a pct_used that would otherwise not trip (or is missing)
        must still report 'disabled' once the month's flag is set --
        the guard doesn't un-trip mid-month just because a later call
        happens to see a lower/unknown number."""
        with patch.object(odds_quota, "_send_alert"):
            odds_quota.maybe_trip_guard(0.9)
        with patch.object(odds_quota, "_send_alert") as mock_alert:
            tripped = odds_quota.maybe_trip_guard(None)
        assert tripped is True
        mock_alert.assert_not_called()

    def test_disabled_flag_is_scoped_to_the_tripped_month(self):
        with patch.object(odds_quota, "_send_alert"):
            odds_quota.maybe_trip_guard(0.9)
        # Simulate the calendar rolling over to next month.
        with patch.object(odds_quota, "_current_month", return_value="2099-01"):
            disabled, _ = odds_quota.is_live_engine_disabled()
        assert disabled is False


class TestScoutsNeverGated:
    """Nothing in this module exposes a scout-facing gate -- the guard is
    consulted exclusively by live_engine (enforced at the call site, not
    here), but assert the public surface itself carries no such hook so a
    future caller can't accidentally wire a scout path into it."""

    def test_no_scout_facing_function_exists(self):
        assert not hasattr(odds_quota, "is_scout_disabled")
        assert not hasattr(odds_quota, "disable_scouts")
