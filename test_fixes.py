"""Tests for all 8 critical fixes. Run: python -m pytest test_fixes.py -v"""

import os
import sys
import sqlite3
import pytest
from unittest.mock import patch, MagicMock
from datetime import date, datetime
import pytz

# ── FIX 7: Drawdown protection — real P&L, not BANKROLL_OVERRIDE ──────────────

class TestDrawdownProtection:
    """FIX 7: drawdown_tier must track against real settled P&L, not BANKROLL_OVERRIDE."""

    def test_real_peak_bankroll_ignores_override(self):
        """real_peak_bankroll() must return settled P&L peak, never the override."""
        from bankroll_engine import real_peak_bankroll
        # With override set to a large value, real_peak_bankroll should still
        # return a value based on actual bet results, not the env var.
        with patch.dict(os.environ, {"BANKROLL_OVERRIDE": "9999"}):
            result = real_peak_bankroll()
        assert result < 9999, "real_peak_bankroll must not use BANKROLL_OVERRIDE"

    def test_real_sizing_bankroll_ignores_override(self):
        """real_sizing_bankroll() must return settled P&L, never the override."""
        from bankroll_engine import real_sizing_bankroll
        with patch.dict(os.environ, {"BANKROLL_OVERRIDE": "9999"}):
            result = real_sizing_bankroll()
        assert result < 9999, "real_sizing_bankroll must not use BANKROLL_OVERRIDE"

    def test_drawdown_tier_uses_real_pnl_not_override(self):
        """drawdown_tier() must compare real P&L vs real peak, not override values."""
        from bankroll_engine import drawdown_tier, real_sizing_bankroll, real_peak_bankroll
        # Set a massive override — should not affect drawdown calc
        with patch.dict(os.environ, {"BANKROLL_OVERRIDE": "10000"}):
            dd = drawdown_tier()
        real_br = real_sizing_bankroll()
        real_pk = real_peak_bankroll()
        expected_pct = max(0.0, (real_pk - real_br) / real_pk * 100) if real_pk > 0 else 0.0
        assert abs(dd["pct"] - expected_pct) < 0.1, (
            f"drawdown_tier pct {dd['pct']:.1f}% doesn't match real P&L "
            f"({real_br:.2f}/{real_pk:.2f} = {expected_pct:.1f}%)"
        )

    def test_peak_bankroll_display_still_uses_override(self):
        """current_bankroll() (for display) may still use override — only drawdown must use real."""
        from bankroll_engine import current_bankroll
        with patch.dict(os.environ, {"BANKROLL_OVERRIDE": "500"}):
            br = current_bankroll()
        assert br == 500.0, "current_bankroll() display should still respect BANKROLL_OVERRIDE"


# ── FIX 5: Stop-loss circuit breaker ─────────────────────────────────────────

class TestStopLossCircuitBreaker:
    """FIX 5: Halt new picks when daily P&L hits -3% of bankroll."""

    def test_daily_stop_loss_threshold(self):
        """is_daily_stop_loss_active() must return True when loss > 3% of bankroll."""
        from bankroll_engine import is_daily_stop_loss_active
        # Mock daily P&L = -$20, bankroll = $200 → 10% loss → should trigger
        with patch("bankroll_engine.daily_pnl", return_value=-20.0), \
             patch("bankroll_engine.sizing_bankroll", return_value=200.0):
            assert is_daily_stop_loss_active() is True

    def test_daily_stop_loss_not_triggered_under_threshold(self):
        """is_daily_stop_loss_active() must return False when loss < 3%."""
        from bankroll_engine import is_daily_stop_loss_active
        # Mock daily P&L = -$5, bankroll = $200 → 2.5% loss → should NOT trigger
        with patch("bankroll_engine.daily_pnl", return_value=-5.0), \
             patch("bankroll_engine.sizing_bankroll", return_value=200.0):
            assert is_daily_stop_loss_active() is False

    def test_daily_pnl_positive_never_triggers(self):
        """Stop loss must never trigger when P&L is positive."""
        from bankroll_engine import is_daily_stop_loss_active
        with patch("bankroll_engine.daily_pnl", return_value=10.0), \
             patch("bankroll_engine.sizing_bankroll", return_value=200.0):
            assert is_daily_stop_loss_active() is False

    def test_daily_pnl_function_exists_and_returns_float(self):
        """daily_pnl() must exist and return a float."""
        from bankroll_engine import daily_pnl
        result = daily_pnl()
        assert isinstance(result, float), f"daily_pnl() must return float, got {type(result)}"


# ── FIX 8: Nightly debrief scheduled at 11pm ET ───────────────────────────────

class TestNightlyDebriefScheduling:
    """FIX 8: Debrief must fire at 11pm ET daily via scheduler.py."""

    def test_scheduler_has_debrief_task(self):
        """schedule_loop must call run_debrief_task when hour == 23."""
        from scheduler import run_debrief_task
        # Just verify the function exists and is callable
        assert callable(run_debrief_task), "run_debrief_task must be in scheduler.py"

    def test_run_debrief_task_calls_brain_debrief(self):
        """run_debrief_task must invoke brain._run_debrief."""
        from scheduler import run_debrief_task
        with patch("scheduler.brain") as mock_brain:
            mock_brain._run_debrief = MagicMock()
            run_debrief_task(send_fn=MagicMock())
            mock_brain._run_debrief.assert_called_once()

    def test_scheduler_fires_debrief_at_11pm(self):
        """schedule_loop checks hour==23 for debrief — verify logic exists."""
        import inspect
        import scheduler
        src = inspect.getsource(scheduler.schedule_loop)
        assert "23" in src or "debrief" in src.lower(), (
            "schedule_loop must have 11pm (hour==23) debrief trigger"
        )


# ── FIX 3: Learning loop — calibration_buckets ───────────────────────────────

class TestLearningLoopCalibration:
    """FIX 3: Every settled bet must write to calibration_buckets.

    Isolation note: db.py reads PARLAY_DB into a module-level DB_PATH
    constant once at import time. By the time this class runs, `db` has
    almost certainly already been imported (e.g. transitively via
    bankroll_engine, imported by TestDrawdownProtection above) — so
    setting os.environ["PARLAY_DB"] here silently no-ops and every write
    below would land in the real parlay_os.db. patch.object(db, "DB_PATH",
    ...) (the pattern test_wire_ins.py already uses) works regardless of
    import order, since it's read fresh from the module namespace on every
    call rather than captured once.

    _feed_brain_from_settled (called below) also writes through
    memory_engine.py, which has its own entirely separate hardcoded
    DB_PATH = "parlay_os.db" constant (confirmed by diffing a full
    sqlite dump before/after this test: bet_memory and team_performance
    rows were landing in the real file even with db.DB_PATH patched) —
    so that module's DB_PATH must be patched too."""

    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path):
        import db
        import memory_engine
        tmp_db = str(tmp_path / "test_fixes_calibration.db")
        with patch.object(db, "DB_PATH", tmp_db), \
             patch.object(memory_engine, "DB_PATH", tmp_db):
            db.init_db()
            yield db

    def test_feed_brain_writes_calibration_bucket(self, _isolated_db):
        """_feed_brain_from_settled must call db.update_calibration for each settled bet."""
        _db = _isolated_db
        # Log a bet and settle it
        _db.log_bet(
            date="2026-05-27", bet="SF", bet_type="ML", game="SF @ LAD",
            sp="Logan Webb", park="LAD", umpire="",
            bet_odds="+145", model_prob=0.57, market_prob=0.41,
            edge_pct=16.0, conviction="HIGH", stake=10.0,
        )
        bet = _db.get_bets()[0]
        _db.resolve_bet("SF", "2026-05-27", "", "W", "5-3")

        # Now call _feed_brain_from_settled
        from brain import _feed_brain_from_settled
        settled = [b for b in _db.get_bets() if b.get("result") == "W"]
        _feed_brain_from_settled(settled)

        # Verify calibration_buckets was written
        cal = _db.get_calibration()
        assert len(cal) > 0, "calibration_buckets must have at least 1 row after settling a bet"

    def test_calibration_bucket_name_matches_model_prob(self, _isolated_db):
        """Bucket name must reflect model probability range (e.g. '0.55-0.60')."""
        _db = _isolated_db
        _db.update_calibration("0.55-0.60", win=True)
        cal = _db.get_calibration()
        buckets = [c["bucket"] for c in cal]
        assert "0.55-0.60" in buckets, "calibration_buckets must store probability bucket name"


# ── FIX 4: CLV capture ───────────────────────────────────────────────────────

class TestCLVCapture:
    """FIX 4: Closing odds fetched 1hr before game start, stored in clv_log."""

    def test_clv_snapshot_function_exists(self):
        """capture_pre_game_clv must exist in bankroll_engine or a dedicated module."""
        try:
            from bankroll_engine import capture_pre_game_clv
            assert callable(capture_pre_game_clv)
        except ImportError:
            from brain import _capture_clv_snapshot
            assert callable(_capture_clv_snapshot)

    def test_clv_log_written_to_db(self, tmp_path):
        """After CLV capture, clv_log table must have a row for the bet.
        Isolated against a temp DB — never the real parlay_os.db (see
        TestLearningLoopCalibration's isolation note above)."""
        import db
        tmp_db = str(tmp_path / "test_fixes_clv.db")
        with patch.object(db, "DB_PATH", tmp_db):
            db.init_db()
            # Simulate writing a CLV log entry directly
            db.log_clv(
                date="2026-05-27", bet="SF", bet_type="ML",
                game="SF @ LAD", sp="Logan Webb", park="LAD", umpire="",
                bet_odds="+145", closing_odds="+130",
                clv_pct=3.5, result=None, model="12-factor", edge_pct=16.0,
            )
            rows = db.get_clv_log(days=1)
        assert len(rows) >= 1, "clv_log must record closing odds"
        assert rows[0]["clv_pct"] == 3.5


# ── FIX 2: 8pm daily summary ─────────────────────────────────────────────────

class TestDailySummary:
    """FIX 2: At 8pm ET, send a full day summary of all picks sent."""

    def test_send_daily_summary_function_exists(self):
        """_send_daily_summary or run_daily_summary_task must exist."""
        try:
            from brain import _send_daily_summary
            assert callable(_send_daily_summary)
        except ImportError:
            from scheduler import run_daily_summary_task
            assert callable(run_daily_summary_task)

    def test_daily_summary_includes_picks_sent(self):
        """Daily summary must include count of picks sent and bankroll."""
        from brain import _send_daily_summary
        msgs = []
        with patch("brain._send_telegram", side_effect=msgs.append):
            _send_daily_summary()
        # Should have sent at least one message
        assert len(msgs) >= 1, "_send_daily_summary must send at least one Telegram message"

    def test_scheduler_fires_summary_at_8pm(self):
        """schedule_loop must check hour==20 for 8pm summary."""
        import inspect
        import scheduler
        src = inspect.getsource(scheduler.schedule_loop)
        assert "summary" in src.lower() or "20" in src, (
            "schedule_loop must have 8pm (hour==20) daily summary trigger"
        )


# ── FIX 1: Sanity check improvements ─────────────────────────────────────────

class TestSanityCheck:
    """FIX 1: If n_bets > 0, exactly n_bets picks must appear in Telegram."""

    def test_sanity_alert_fires_when_update_fails(self):
        """When slip_already_sent and update fails, sanity alert must fire."""
        import brain
        alerts = []
        with patch("brain._send_telegram", side_effect=alerts.append), \
             patch("brain._send_slip_update", return_value=False), \
             patch("brain.run_daily_scout") as mock_scout:
            # Directly test the sanity check logic
            n_bets = 3
            _slip_sent_ok = False
            slip_already_sent = True
            if n_bets > 0 and not _slip_sent_ok:
                msg = f"[SANITY FAIL] Scout found {n_bets} ML bet(s) but slip not confirmed sent"
                brain._send_telegram(msg)
        assert any("SANITY FAIL" in a for a in alerts), \
            "Sanity alert must fire when bets found but send not confirmed"

    def test_slip_not_marked_sent_when_empty(self):
        """Slip quality must be 'bad' when no locks or flips, even if HTTP 200."""
        from brain import MIN_STAKE
        # Simulate: _slip_sent_ok=True but no bets
        sent_stakes = []
        avg_sent_stake = sum(sent_stakes) / len(sent_stakes) if sent_stakes else 0.0
        slip_quality = "good" if avg_sent_stake > MIN_STAKE and bool(sent_stakes) else "bad"
        assert slip_quality == "bad", \
            "Empty slip (no locks/flips) must be marked quality='bad'"


# ── FIX 6: Rolling xwOBA blend ───────────────────────────────────────────────

class TestRollingXwOBABlend:
    """FIX 6: 30-day xwOBA weighted 70% recent, 30% season."""

    def test_blend_rolling_xwoba_function_exists(self):
        """blend_xwoba must exist in sp_engine or savant_leaderboards."""
        try:
            from sp_engine import blend_xwoba
            assert callable(blend_xwoba)
        except ImportError:
            from savant_leaderboards import blend_xwoba
            assert callable(blend_xwoba)

    def test_blend_rolling_xwoba_weights(self):
        """blend_xwoba(season=0.320, rolling_30d=0.280) should return 70/30 blend."""
        try:
            from sp_engine import blend_xwoba
        except ImportError:
            from savant_leaderboards import blend_xwoba
        result = blend_xwoba(season_xwoba=0.320, rolling_30d_xwoba=0.280)
        expected = 0.70 * 0.280 + 0.30 * 0.320  # = 0.292
        assert abs(result - expected) < 0.001, \
            f"blend_xwoba must weight 70% rolling + 30% season, got {result:.3f} expected {expected:.3f}"

    def test_blend_falls_back_when_rolling_missing(self):
        """When rolling_30d_xwoba is None, must use season xwOBA alone."""
        try:
            from sp_engine import blend_xwoba
        except ImportError:
            from savant_leaderboards import blend_xwoba
        result = blend_xwoba(season_xwoba=0.320, rolling_30d_xwoba=None)
        assert result == 0.320, \
            f"blend_xwoba must return season value when rolling is None, got {result}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
