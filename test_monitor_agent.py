"""Tests for monitor_agent.py (Agent 1 -- THE MONITOR).

Run: python -m pytest test_monitor_agent.py -v
"""

import json
import time
from unittest.mock import patch, MagicMock

import pytest

import db
import monitor_agent as mon


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path):
    tmp_db = str(tmp_path / "monitor_test.db")
    with patch.object(db, "DB_PATH", tmp_db):
        db.init_db()
        yield db


@pytest.fixture(autouse=True)
def _reset_monitor_state():
    """Each test gets a clean cooldown/status state."""
    mon._last_alerted.clear()
    mon._check_state.clear()
    yield
    mon._last_alerted.clear()
    mon._check_state.clear()


def _log(d, bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox",
         bet_type="ML", date="2026-07-28", stake=25.0, diagnostic_json=None, **overrides):
    kwargs = dict(
        date=date, bet=bet, bet_type=bet_type, game=game,
        sp="", park="BOS", umpire="", bet_odds="-120",
        model_prob=0.55, market_prob=0.50, edge_pct=5.0,
        conviction="MEDIUM", stake=stake, diagnostic_json=diagnostic_json,
    )
    kwargs.update(overrides)
    d.log_bet(**kwargs)


class TestScoutFreshness:
    def test_missing_file_is_a_failure(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = mon.check_scout_freshness()
        assert result["ok"] is False

    def test_recent_timestamp_is_ok(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from datetime import datetime, timezone
        with open("last_scout.json", "w") as f:
            json.dump({"timestamp": datetime.now(timezone.utc).isoformat()}, f)
        result = mon.check_scout_freshness()
        assert result["ok"] is True

    def test_stale_timestamp_is_a_failure(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from datetime import datetime, timezone, timedelta
        old = datetime.now(timezone.utc) - timedelta(hours=30)
        with open("last_scout.json", "w") as f:
            json.dump({"timestamp": old.isoformat()}, f)
        result = mon.check_scout_freshness()
        assert result["ok"] is False
        assert "30" in result["detail"] or result["age_hrs"] > 25


class TestOddsFeeds:
    def test_missing_odds_api_key_is_a_failure(self, monkeypatch):
        monkeypatch.setattr(mon, "ODDS_API_KEY", "")
        with patch("monitor_agent.requests.get") as mock_get:
            result = mon.check_odds_feeds()
        assert result["ok"] is False
        assert "ODDS_API_KEY" in result["detail"]

    def test_odds_api_quota_exhausted_is_a_failure(self, monkeypatch):
        monkeypatch.setattr(mon, "ODDS_API_KEY", "fake-key")
        mock_resp = MagicMock(status_code=429, ok=False)
        with patch("monitor_agent.requests.get", return_value=mock_resp):
            result = mon.check_odds_feeds()
        assert result["ok"] is False
        assert "quota" in result["detail"].lower()

    def test_odds_api_healthy_and_no_sgo_cache_is_ok(self, monkeypatch, tmp_path):
        monkeypatch.setattr(mon, "ODDS_API_KEY", "fake-key")
        mock_resp = MagicMock(status_code=200, ok=True)
        with patch("monitor_agent.requests.get", return_value=mock_resp), \
             patch("sportsgameodds_client.CACHE_FILE", str(tmp_path / "no_such_cache.json")):
            result = mon.check_odds_feeds()
        assert result["ok"] is True

    def test_stale_sgo_cache_is_a_failure(self, monkeypatch, tmp_path):
        monkeypatch.setattr(mon, "ODDS_API_KEY", "fake-key")
        mock_resp = MagicMock(status_code=200, ok=True)
        cache_file = tmp_path / "sgo_cache.json"
        with open(cache_file, "w") as f:
            json.dump({"fetched_at": time.time() - 3600 * 24}, f)  # 24h old
        with patch("monitor_agent.requests.get", return_value=mock_resp), \
             patch("sportsgameodds_client.CACHE_FILE", str(cache_file)):
            result = mon.check_odds_feeds()
        assert result["ok"] is False
        assert "SGO" in result["detail"]


class TestNeutralFallbackRate:
    def test_no_diagnostics_today_is_ok(self, _isolated_db):
        result = mon.check_neutral_fallback_rate()
        assert result["ok"] is True

    def test_high_fallback_rate_is_a_failure(self, _isolated_db):
        fallback_diag = json.dumps({"flags": {"sp_missing": True}})
        clean_diag = json.dumps({"flags": {"sp_missing": False}})
        _log(_isolated_db, bet="A", diagnostic_json=fallback_diag)
        _log(_isolated_db, bet="B", diagnostic_json=fallback_diag, game="X @ Y")
        _log(_isolated_db, bet="C", diagnostic_json=clean_diag, game="Z @ W")
        result = mon.check_neutral_fallback_rate()
        assert result["ok"] is False
        assert result["rate"] > mon.NEUTRAL_FALLBACK_RATE_THRESHOLD

    def test_low_fallback_rate_is_ok(self, _isolated_db):
        clean_diag = json.dumps({"flags": {"sp_missing": False}})
        for i in range(5):
            _log(_isolated_db, bet=f"Team {i}", diagnostic_json=clean_diag, game=f"Team {i} @ Opp {i}")
        result = mon.check_neutral_fallback_rate()
        assert result["ok"] is True


class TestStuckPendingBets:
    def test_no_stuck_bets_is_ok(self, _isolated_db):
        result = mon.check_stuck_pending_bets()
        assert result["ok"] is True
        assert result["count"] == 0

    def test_stuck_bets_is_a_failure(self, _isolated_db):
        _log(_isolated_db, date="2026-06-01")  # far in the past -- definitely stuck
        result = mon.check_stuck_pending_bets()
        assert result["ok"] is False
        assert result["count"] >= 1


class TestClvLoopActivity:
    def test_no_pending_bets_today_is_ok(self, _isolated_db):
        result = mon.check_clv_loop_activity()
        assert result["ok"] is True

    def test_pending_bets_today_with_no_clv_is_a_failure(self, _isolated_db):
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).date().isoformat()
        _log(_isolated_db, date=today, stake=25.0)
        result = mon.check_clv_loop_activity()
        assert result["ok"] is False

    def test_pending_bets_today_with_clv_captured_is_ok(self, _isolated_db):
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).date().isoformat()
        _log(_isolated_db, date=today, stake=25.0)
        _isolated_db.log_clv(
            date=today, bet="Boston Red Sox", bet_type="ML",
            game="Tampa Bay Rays @ Boston Red Sox", sp="", park="", umpire="",
            bet_odds="-120", closing_odds="-115", clv_pct=1.2, result=None, model="12-factor",
            edge_pct=5.0,
        )
        result = mon.check_clv_loop_activity()
        assert result["ok"] is True


class TestErrorSpike:
    def test_no_log_file_is_ok(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = mon.check_error_spike()
        assert result["ok"] is True

    def test_many_recent_errors_is_a_failure(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from datetime import datetime, timezone
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with open("errors.log", "w") as f:
            for _ in range(15):
                f.write(f"{now_str} | ERROR    | something broke\n")
        result = mon.check_error_spike()
        assert result["ok"] is False
        assert result["count"] >= mon.ERROR_SPIKE_THRESHOLD

    def test_old_errors_do_not_count(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        old_str = "2020-01-01 00:00:00"
        with open("errors.log", "w") as f:
            for _ in range(15):
                f.write(f"{old_str} | ERROR    | ancient error\n")
        result = mon.check_error_spike()
        assert result["ok"] is True
        assert result["count"] == 0

    def test_few_recent_errors_is_ok(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from datetime import datetime, timezone
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with open("errors.log", "w") as f:
            f.write(f"{now_str} | ERROR    | one-off\n")
        result = mon.check_error_spike()
        assert result["ok"] is True


class TestAlertDedupCooldown:
    def test_first_alert_ever_fires_even_when_monotonic_clock_is_near_zero(self):
        """Regression: time.monotonic() is not epoch time -- it commonly
        starts near zero at process boot. A 0.0 default for "never alerted"
        made `now - 0.0 < ALERT_COOLDOWN_SEC` true right after a fresh
        process start, silently suppressing the very first alert for a
        given check for up to ALERT_COOLDOWN_SEC after every Railway
        deploy. The sentinel must be None, not 0.0."""
        with patch.object(mon, "_send_alert", return_value=True) as mock_send, \
             patch.object(mon.time, "monotonic", return_value=5.0):
            mon._maybe_alert("brand_new_check", {"ok": False, "detail": "boom"})
        mock_send.assert_called_once()

    def test_failing_check_alerts_once_then_cools_down(self):
        with patch.object(mon, "_send_alert", return_value=True) as mock_send:
            mon._maybe_alert("fake_check", {"ok": False, "detail": "boom"})
            mon._maybe_alert("fake_check", {"ok": False, "detail": "boom again"})
            mon._maybe_alert("fake_check", {"ok": False, "detail": "still broken"})
        assert mock_send.call_count == 1

    def test_passing_check_never_alerts(self):
        with patch.object(mon, "_send_alert", return_value=True) as mock_send:
            mon._maybe_alert("fake_check", {"ok": True, "detail": "fine"})
        mock_send.assert_not_called()

    def test_cooldown_expiring_allows_a_new_alert(self):
        with patch.object(mon, "_send_alert", return_value=True) as mock_send:
            mon._maybe_alert("fake_check", {"ok": False, "detail": "boom"})
            # Simulate cooldown having expired
            mon._last_alerted["fake_check"] -= (mon.ALERT_COOLDOWN_SEC + 1)
            mon._maybe_alert("fake_check", {"ok": False, "detail": "boom again"})
        assert mock_send.call_count == 2

    def test_different_checks_have_independent_cooldowns(self):
        with patch.object(mon, "_send_alert", return_value=True) as mock_send:
            mon._maybe_alert("check_a", {"ok": False, "detail": "a broke"})
            mon._maybe_alert("check_b", {"ok": False, "detail": "b broke"})
        assert mock_send.call_count == 2


class TestRunAllChecksAndStatus:
    def test_run_all_checks_updates_status(self, _isolated_db, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with patch.object(mon, "ODDS_API_KEY", ""):
            results = mon.run_all_checks()
        status = mon.get_monitor_status()
        assert status["timestamp"] == results["timestamp"]
        assert "failures" in status

    def test_status_before_any_run(self):
        assert mon.get_monitor_status() == {"status": "not yet run"}

    def test_one_broken_check_does_not_crash_the_others(self, _isolated_db, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with patch.object(mon, "_CHECKS", {
            **mon._CHECKS,
            "scout_freshness": lambda: (_ for _ in ()).throw(RuntimeError("kaboom")),
        }):
            results = mon.run_all_checks()
        assert results["scout_freshness"]["ok"] is False
        assert "kaboom" in results["scout_freshness"]["detail"]
        # sibling checks still ran and produced real results
        assert "stuck_pending_bets" in results


class TestMonitorApiEndpoint:
    def test_returns_not_yet_run_before_any_check(self):
        import api
        client = api.app.test_client()
        resp = client.get("/api/monitor")
        assert resp.status_code == 200
        assert resp.get_json() == {"status": "not yet run"}

    def test_returns_503_when_a_check_is_failing(self, _isolated_db, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with patch.object(mon, "ODDS_API_KEY", ""):
            mon.run_all_checks()
        import api
        client = api.app.test_client()
        resp = client.get("/api/monitor")
        assert resp.status_code == 503
        assert resp.get_json()["all_ok"] is False

    def test_returns_200_when_all_checks_pass(self, _isolated_db, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from datetime import datetime, timezone
        with open("last_scout.json", "w") as f:
            json.dump({"timestamp": datetime.now(timezone.utc).isoformat()}, f)
        mock_resp = MagicMock(status_code=200, ok=True)
        with patch.object(mon, "ODDS_API_KEY", "fake-key"), \
             patch("monitor_agent.requests.get", return_value=mock_resp), \
             patch("sportsgameodds_client.CACHE_FILE", str(tmp_path / "no_cache.json")):
            mon.run_all_checks()
        import api
        client = api.app.test_client()
        resp = client.get("/api/monitor")
        assert resp.status_code == 200
        assert resp.get_json()["all_ok"] is True


class TestEnabledToggle:
    def test_default_enabled(self, monkeypatch):
        monkeypatch.delenv("MONITOR_ENABLED", raising=False)
        assert mon.is_enabled() is True

    def test_explicitly_disabled(self, monkeypatch):
        monkeypatch.setenv("MONITOR_ENABLED", "false")
        assert mon.is_enabled() is False

    def test_explicitly_enabled(self, monkeypatch):
        monkeypatch.setenv("MONITOR_ENABLED", "true")
        assert mon.is_enabled() is True

    def test_zero_disables(self, monkeypatch):
        monkeypatch.setenv("MONITOR_ENABLED", "0")
        assert mon.is_enabled() is False
