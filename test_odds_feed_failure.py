"""Tests for A7: a dead/quota-exhausted odds feed must not look like an
empty slate. Covers data_health's new failure-detail/no-odds-skip tracking,
market_engine._odds_request()'s wiring into it, brain.analyze_game()'s
no-market-data skip marking, and the slate-level alert brain.run_daily_scout()
sends when most of the slate got skipped for no odds.

Run: python -m pytest test_odds_feed_failure.py -v
"""

import inspect
from unittest.mock import patch, MagicMock

import pytest
import requests

import data_health
import odds_quota


@pytest.fixture(autouse=True)
def _reset_data_health():
    data_health.reset()
    yield
    data_health.reset()


@pytest.fixture(autouse=True)
def _isolate_quota_state(tmp_path, monkeypatch):
    """odds_quota.record_usage() persists to a git-committed state file so
    it survives across separate GH Actions job invocations -- redirect it
    to a scratch path so these tests never write into the real repo."""
    monkeypatch.setattr(odds_quota, "STATE_FILE", str(tmp_path / "odds_quota_state.json"))


class TestDataHealthFailureDetail:
    def test_last_fail_detail_starts_none(self):
        assert data_health.last_fail_detail("odds") is None

    def test_detail_recorded_on_failure(self):
        data_health.record_ok("odds", False, detail="HTTP 401")
        assert data_health.last_fail_detail("odds") == "HTTP 401"

    def test_detail_ignored_on_success(self):
        data_health.record_ok("odds", False, detail="HTTP 401")
        data_health.record_ok("odds", True, detail="should not be stored")
        assert data_health.last_fail_detail("odds") == "HTTP 401"

    def test_latest_failure_detail_wins(self):
        data_health.record_ok("odds", False, detail="HTTP 401")
        data_health.record_ok("odds", False, detail="HTTP 429")
        assert data_health.last_fail_detail("odds") == "HTTP 429"

    def test_reset_clears_detail_and_skip_count(self):
        data_health.record_ok("odds", False, detail="HTTP 401")
        data_health.mark_skip_no_odds()
        data_health.reset()
        assert data_health.last_fail_detail("odds") is None
        assert data_health.skip_no_odds_count() == 0


class TestNoOddsSkipCount:
    def test_starts_at_zero(self):
        assert data_health.skip_no_odds_count() == 0

    def test_increments_per_call(self):
        data_health.mark_skip_no_odds()
        data_health.mark_skip_no_odds()
        data_health.mark_skip_no_odds()
        assert data_health.skip_no_odds_count() == 3


class TestOddsRequestDetailCapture:
    """market_engine._odds_request() must feed data_health.record_ok('odds',
    False, detail=...) with the actual HTTP status so a downstream alert can
    name the real cause (401=bad key, 429=quota) instead of just 'it failed'."""

    def test_missing_api_key_records_detail(self, monkeypatch):
        import market_engine
        monkeypatch.setattr(market_engine, "ODDS_API_KEY", "")
        monkeypatch.setattr(market_engine, "_active_key", [None])
        result = market_engine._odds_request("sports/baseball_mlb/events", {})
        assert result is None
        assert data_health.last_fail_detail("odds") == "no API key configured"

    def test_http_401_records_status_in_detail(self, monkeypatch):
        import market_engine
        monkeypatch.setattr(market_engine, "_active_key", ["dummy-key"])
        monkeypatch.setattr(market_engine, "ODDS_API_KEY_BACKUP", "")
        resp = MagicMock(status_code=401)
        err = requests.exceptions.HTTPError(response=resp)
        with patch.object(market_engine, "_http_get", side_effect=err):
            result = market_engine._odds_request("sports/baseball_mlb/events", {})
        assert result is None
        assert data_health.last_fail_detail("odds") == "HTTP 401"

    def test_http_429_records_status_in_detail(self, monkeypatch):
        import market_engine
        monkeypatch.setattr(market_engine, "_active_key", ["dummy-key"])
        monkeypatch.setattr(market_engine, "ODDS_API_KEY_BACKUP", "")
        resp = MagicMock(status_code=429)
        err = requests.exceptions.HTTPError(response=resp)
        with patch.object(market_engine, "_http_get", side_effect=err):
            result = market_engine._odds_request("sports/baseball_mlb/events", {})
        assert result is None
        assert data_health.last_fail_detail("odds") == "HTTP 429"

    def test_backup_key_failure_records_its_own_status(self, monkeypatch):
        import market_engine
        monkeypatch.setattr(market_engine, "ODDS_API_KEY_BACKUP", "backup-key")
        monkeypatch.setattr(market_engine, "_active_key", ["primary-key"])
        primary_resp = MagicMock(status_code=401)
        primary_err = requests.exceptions.HTTPError(response=primary_resp)
        backup_resp = MagicMock(status_code=401)
        backup_err = requests.exceptions.HTTPError(response=backup_resp)
        with patch.object(market_engine, "_http_get", side_effect=[primary_err, backup_err]):
            result = market_engine._odds_request("sports/baseball_mlb/events", {})
        assert result is None
        assert data_health.last_fail_detail("odds") == "HTTP 401"

    def test_success_after_prior_failure_leaves_detail_intact_but_status_live(self, monkeypatch):
        import market_engine
        monkeypatch.setattr(market_engine, "_active_key", ["dummy-key"])
        monkeypatch.setattr(market_engine, "ODDS_API_KEY_BACKUP", "")
        resp = MagicMock(status_code=401)
        err = requests.exceptions.HTTPError(response=resp)
        ok_resp = MagicMock(status_code=200)
        ok_resp.json.return_value = [{"id": "evt1"}]
        ok_resp.raise_for_status = lambda: None
        with patch.object(market_engine, "_http_get", side_effect=[err, ok_resp]):
            market_engine._odds_request("sports/baseball_mlb/events", {})
            result = market_engine._odds_request("sports/baseball_mlb/events", {})
        assert result == [{"id": "evt1"}]
        # Status recovers, but the stale detail from the earlier failure is
        # harmless — a later real failure always overwrites it (see above).
        assert data_health.as_dict()["odds"] in ("degraded", "live")


class TestAnalyzeGameMarksSkipNoOdds:
    def test_no_market_data_marks_the_skip(self, monkeypatch):
        import brain
        monkeypatch.setattr(
            brain, "full_market_snapshot",
            lambda *a, **k: {"no_vig": {}, "ml_books": {}},
        )
        result = brain.analyze_game(
            {"id": "evt1", "away": "New York Yankees", "home": "Boston Red Sox",
             "commence_utc": "2026-07-28T23:00:00Z"},
            "2026-07-28",
        )
        assert result is None
        assert data_health.skip_no_odds_count() == 1

    def test_unknown_team_code_does_not_mark_no_odds_skip(self):
        import brain
        result = brain.analyze_game(
            {"id": "evt1", "away": "Not A Real Team", "home": "Also Not Real",
             "commence_utc": "2026-07-28T23:00:00Z"},
            "2026-07-28",
        )
        assert result is None
        assert data_health.skip_no_odds_count() == 0


class TestOddsFeedFailureAlertInSource:
    """The alert block lives inline in run_daily_scout() rather than a
    standalone function (matching this file's existing pattern for
    per-game-loop logic — see test_money_bugs.py's structural checks on the
    same function). Assert its presence, threshold, message shape, and
    position: after the per-game loop (so the whole slate's skips are
    counted) and before the ML admission pass (so a dead feed is reported
    before any budget/cap logic runs)."""

    def test_alert_wired_off_data_health_not_a_parallel_path(self):
        import brain
        src = inspect.getsource(brain.run_daily_scout)
        assert "data_health.skip_no_odds_count()" in src
        assert "data_health.last_fail_detail(\"odds\")" in src

    def test_threshold_is_over_half_the_attempted_slate(self):
        import brain
        src = inspect.getsource(brain.run_daily_scout)
        assert "_no_odds_skips / _slate_attempted > 0.5" in src
        assert "_slate_attempted > 0 and" in src

    def test_alert_message_shape(self):
        import brain
        src = inspect.getsource(brain.run_daily_scout)
        assert "ODDS FEED FAILURE" in src
        assert "Check API quota" in src

    def test_alert_runs_after_the_game_loop_and_before_ml_admission(self):
        import brain
        src = inspect.getsource(brain.run_daily_scout)
        loop_idx        = src.index("for event in events:")
        odds_alert_idx  = src.index("ODDS FEED FAILURE")
        ml_admit_idx    = src.index("# ── ML admission:")
        assert loop_idx < odds_alert_idx < ml_admit_idx

    def test_slate_attempted_counts_games_handed_to_analyze_game(self):
        import brain
        src = inspect.getsource(brain.run_daily_scout)
        incr_idx  = src.index("_slate_attempted += 1")
        call_idx  = src.index("analysis = analyze_game(event, today)")
        assert incr_idx < call_idx, "counter must increment before the analyze_game() call it's counting"
