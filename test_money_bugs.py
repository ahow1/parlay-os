"""Tests for TIER 1 money-bug audit fixes (AUDIT.md B1, B2, B10, B3, B4).
Run: python -m pytest test_money_bugs.py -v
"""

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest


# ── B1: K-prop fabricated math ────────────────────────────────────────────────

class TestKPropRealModelP:
    """B1: analyze_k_prop() must return a real model_p (Poisson-derived), and
    edge_pct must come from that probability vs. a market baseline — not
    gap (a strikeout-count differential) multiplied by 10."""

    def _sp_stats(self, k9=10.0):
        # pitcher_id=None avoids any live Savant HTTP call in get_pitcher_whiff_rate
        return {
            "name": "Test SP", "pitcher_id": None,
            "k9": k9, "ip": 100, "gs": 15, "ttop": True,
            "hand": "R", "rolling_k9_3": None,
        }

    def test_returns_real_model_p_not_hardcoded_fallback(self):
        """model_p must be a real probability in (0,1), not the 0.55 brain.py
        used to silently fall back to."""
        from strikeout_engine import analyze_k_prop
        result = analyze_k_prop(self._sp_stats(), None, "XXX", market_line=6.0)
        assert result is not None
        assert "model_p" in result
        assert 0.0 < result["model_p"] < 1.0

    def test_model_p_favors_over_when_projection_beats_line(self):
        """Large projected-K surplus over the line -> model_p for OVER > 0.5."""
        from strikeout_engine import analyze_k_prop
        result = analyze_k_prop(self._sp_stats(), None, "XXX", market_line=6.0)
        assert result["direction"] == "OVER"
        assert result["model_p"] > 0.5

    def test_model_p_favors_under_when_projection_below_line(self):
        """Projection well below the line -> model_p for UNDER > 0.5."""
        from strikeout_engine import analyze_k_prop
        result = analyze_k_prop(self._sp_stats(), None, "XXX", market_line=9.0)
        assert result["direction"] == "UNDER"
        assert result["model_p"] > 0.5

    def test_edge_pct_derived_from_probability_not_gap_times_ten(self):
        """edge_pct must equal a real probability edge vs. the 0.5 market
        baseline (matching the existing -110 K-prop convention elsewhere in
        brain.py), not gap*10."""
        from strikeout_engine import analyze_k_prop, project_strikeouts
        from props_engine import prob_over

        sp = self._sp_stats()
        result = analyze_k_prop(sp, None, "XXX", market_line=6.0)
        assert "edge_pct" in result

        # Recompute independently via the same Poisson model to verify the formula
        proj = project_strikeouts(sp, None, "XXX", 1.0, None)
        lam = proj["projected_k"]
        model_p_over = prob_over(lam, 6.0)
        expected_edge_pct = round((model_p_over - 0.5) * 100, 2)

        assert result["edge_pct"] == expected_edge_pct
        gap_times_ten = round(result["gap"] * 10, 2)
        assert result["edge_pct"] != gap_times_ten, (
            "edge_pct must not be derived from gap*10 (a K-count differential, "
            "not a probability-point edge)"
        )


# ── B2: Telegram false success on missing credentials ────────────────────────

class TestTelegramFalseSuccess:
    """B2: _send_telegram() must return False (not True) when credentials are
    missing, so callers don't mark an unsent slip as sent and dedup can retry."""

    def test_returns_false_when_bot_token_missing(self):
        import brain
        with patch.object(brain, "DRY_RUN", False), \
             patch.object(brain, "BOT_TOKEN", ""), \
             patch.object(brain, "CHAT_ID", "7852968108"):
            assert brain._send_telegram("test message") is False

    def test_returns_false_when_chat_id_missing(self):
        import brain
        with patch.object(brain, "DRY_RUN", False), \
             patch.object(brain, "BOT_TOKEN", "fake:token"), \
             patch.object(brain, "CHAT_ID", ""):
            assert brain._send_telegram("test message") is False

    def test_returns_false_when_both_missing(self):
        import brain
        with patch.object(brain, "DRY_RUN", False), \
             patch.object(brain, "BOT_TOKEN", ""), \
             patch.object(brain, "CHAT_ID", ""):
            assert brain._send_telegram("test message") is False

    def test_does_not_call_requests_when_credentials_missing(self):
        """Missing-credentials path must short-circuit before any HTTP call."""
        import brain
        with patch.object(brain, "DRY_RUN", False), \
             patch.object(brain, "BOT_TOKEN", ""), \
             patch.object(brain, "CHAT_ID", ""), \
             patch.object(brain.requests, "post") as mock_post:
            brain._send_telegram("test message")
        mock_post.assert_not_called()


# ── B10: stuck-pending bankroll deflation ─────────────────────────────────────

def _mock_bets(today_str, old_str):
    """Realistic bets table rows: one win, one loss, one fresh pending
    (today), one stuck pending (old_str, >48h in the past, no result)."""
    return [
        {"date": today_str, "stake": 20.0, "result": "W", "bet_odds": "-110"},
        {"date": today_str, "stake": 10.0, "result": "L", "bet_odds": "-110"},
        {"date": today_str, "stake": 15.0, "result": None, "bet_odds": "-120"},
        {"date": old_str,   "stake": 50.0, "result": None, "bet_odds": "+150"},
    ]


class TestStuckPendingBankroll:
    """B10: current_bankroll() must exclude orphaned (>48h past game date,
    still unresolved) pending bets from its deduction, and those bets must be
    surfaced via a dedicated getter so they're operator-visible."""

    def _dates(self):
        today = datetime.now(timezone.utc)
        old = today - timedelta(days=5)
        return today.strftime("%Y-%m-%d"), old.strftime("%Y-%m-%d")

    def test_get_stuck_pending_bets_returns_only_old_unresolved(self):
        from bankroll_engine import get_stuck_pending_bets
        today_str, old_str = self._dates()
        with patch("bankroll_engine._db.get_bets", return_value=_mock_bets(today_str, old_str)):
            stuck = get_stuck_pending_bets()
        assert len(stuck) == 1
        assert stuck[0]["date"] == old_str
        assert stuck[0]["stake"] == 50.0

    def test_fresh_pending_bet_not_flagged_stuck(self):
        from bankroll_engine import get_stuck_pending_bets
        today_str, old_str = self._dates()
        with patch("bankroll_engine._db.get_bets", return_value=_mock_bets(today_str, old_str)):
            stuck = get_stuck_pending_bets()
        assert all(b["date"] != today_str for b in stuck)

    def test_current_bankroll_excludes_stuck_pending_stake(self):
        """The $50 stuck pending stake must NOT be subtracted — only the real
        $15 fresh-pending exposure should reduce bankroll below settled P&L."""
        from bankroll_engine import current_bankroll
        from math_engine import STARTING_BANKROLL, american_to_decimal
        today_str, old_str = self._dates()
        env = dict(os.environ)
        env.pop("BANKROLL_OVERRIDE", None)
        with patch.dict(os.environ, env, clear=True), \
             patch("bankroll_engine._db.get_bets", return_value=_mock_bets(today_str, old_str)):
            result = current_bankroll()

        win_gain = (american_to_decimal("-110") - 1) * 20.0
        expected_with_stuck_excluded = round(STARTING_BANKROLL + win_gain - 10.0 - 15.0, 2)
        expected_if_stuck_wrongly_deducted = round(expected_with_stuck_excluded - 50.0, 2)

        assert result == expected_with_stuck_excluded
        assert result != expected_if_stuck_wrongly_deducted

    def test_stuck_pending_alert_message_empty_when_no_stuck_bets(self):
        from brain import _stuck_pending_alert_message
        assert _stuck_pending_alert_message([]) == ""

    def test_stuck_pending_alert_message_mentions_count_and_total(self):
        from brain import _stuck_pending_alert_message
        msg = _stuck_pending_alert_message([
            {"date": "2026-06-01", "stake": 50.0, "result": None},
            {"date": "2026-06-02", "stake": 25.0, "result": None},
        ])
        assert "2" in msg
        assert "75.00" in msg


# ── B3: blanket try/except silently drops whole games ────────────────────────

class TestGameAnalysisFailuresSurfaced:
    """B3: a truly fatal analyze_game() exception must be distinguishable
    from a routine skip — not just `print(...); continue` with no trace
    outside runlog.txt."""

    def test_failure_message_empty_when_no_failures(self):
        from brain import _game_analysis_failure_message
        assert _game_analysis_failure_message([]) == ""

    def test_failure_message_names_the_dropped_games(self):
        from brain import _game_analysis_failure_message
        msg = _game_analysis_failure_message([
            {"game": "SF @ LAD", "error": "KeyError: 'xfip'"},
            {"game": "NYY @ BOS", "error": "TypeError: bad odds"},
        ])
        assert "SF @ LAD" in msg
        assert "NYY @ BOS" in msg
        assert "2" in msg

    def test_analyze_game_call_site_tags_failures_distinctly_from_skip(self):
        """The call site around analyze_game() must no longer be a bare
        print+continue — it must append to a distinct failures list and not
        reuse the generic 'SKIP' wording routine skips use."""
        import inspect
        import brain
        src = inspect.getsource(brain)
        call_site_start = src.index("analysis = analyze_game(event, today)")
        call_site = src[call_site_start:call_site_start + 500]
        assert "_game_analysis_failures.append" in call_site, (
            "analyze_game() exceptions must be collected into a distinct "
            "failures list, not just printed and silently continued past"
        )

    def test_scout_out_records_analysis_failures(self):
        """scout_out must carry the analysis_failures list so it's visible
        in last_scout.json / the dashboard, not just runlog.txt."""
        import inspect
        import brain
        src = inspect.getsource(brain)
        assert 'scout_out["analysis_failures"]' in src


# ── B4: pick sent to Telegram but never persisted ─────────────────────────────

class TestLogBetRetryAndSuppress:
    """B4: a log_bet() failure must retry once; if it still fails, the pick
    must never be queued for Telegram/dashboard display, since a pick shown
    but not stored is invisible to settlement/learning/dashboard forever."""

    def _analysis(self):
        return {
            "away_name": "SF", "home_name": "LAD", "home": "LAD", "umpire": "",
            "away_sp": {}, "home_sp": {}, "ump_edge": {}, "home_dog": {},
            "best_away_odds": "+120", "away_model_p": 0.55, "away_nv": 0.50,
            "away_edge": 5.0, "away_stake": 20.0,
        }

    def test_succeeds_on_first_try_calls_once(self):
        from brain import _log_bet_with_retry
        with patch("brain._db.log_bet") as mock_log:
            ok = _log_bet_with_retry("2026-07-13", self._analysis(), "away", "MEDIUM")
        assert ok is True
        assert mock_log.call_count == 1

    def test_retries_once_then_succeeds(self):
        from brain import _log_bet_with_retry
        with patch("brain._db.log_bet", side_effect=[sqlite3.OperationalError("locked"), None]) as mock_log:
            ok = _log_bet_with_retry("2026-07-13", self._analysis(), "away", "MEDIUM")
        assert ok is True
        assert mock_log.call_count == 2

    def test_fails_twice_returns_false_and_stops_retrying(self):
        from brain import _log_bet_with_retry
        with patch("brain._db.log_bet", side_effect=sqlite3.OperationalError("locked")) as mock_log:
            ok = _log_bet_with_retry("2026-07-13", self._analysis(), "away", "MEDIUM")
        assert ok is False
        assert mock_log.call_count == 2, "must not retry more than once (2 attempts total)"

    def test_persist_check_precedes_telegram_queueing_in_source(self):
        """Structural guard: the retry/suppress check must run before the
        pick is queued into all_locks/all_flips or scout_out['bets'] —
        otherwise a DB failure can no longer suppress anything because the
        pick was already queued for Telegram."""
        import inspect
        import brain
        src = inspect.getsource(brain.run_daily_scout)
        persist_idx = src.index("_log_bet_with_retry(")
        locks_idx   = src.index("all_locks.append((analysis, side))")
        bets_idx    = src.index('scout_out["bets"].append({')
        assert persist_idx < locks_idx, "persist check must run before all_locks queueing"
        assert persist_idx < bets_idx, "persist check must run before scout_out['bets'] recording"
