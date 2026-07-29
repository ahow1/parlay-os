"""Tests for the --settle one-shot CLI mode and Telegram notification dedup.

Background: run_settlement_check() previously sent a Telegram grading
message and settled a bet's result in the same step, with nothing
tracking whether the message actually went out. If it ran more than once
over the same bet (e.g. concurrent GitHub Actions runs, a manual rerun),
or if a bet was graded through some other path (manual /settle) without
ever getting a Telegram ping, there was no way to tell "already notified"
from "not yet notified". notified_at (nullable TIMESTAMP on bets) is the
fix: NULL means not yet notified. run_settlement_check() now has two
passes -- settle-and-notify for pending bets, and a notify-only backlog
pass for bets that already have a result but never got the ping -- and
both stamp notified_at so a bet is never notified twice.

Run: python -m pytest test_settle_notify.py -v
"""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

import db


@pytest.fixture(autouse=True)
def _isolated_db(tmp_path):
    tmp_db = str(tmp_path / "settle_test.db")
    with patch.object(db, "DB_PATH", tmp_db):
        db.init_db()
        yield db


def _log(bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox",
          bet_type="ML", date="2026-07-19", **overrides):
    kwargs = dict(
        date=date, bet=bet, bet_type=bet_type, game=game,
        sp="", park="BOS", umpire="", bet_odds="-120",
        model_prob=0.55, market_prob=0.50, edge_pct=5.0,
        conviction="MEDIUM", stake=25.0,
    )
    kwargs.update(overrides)
    db.log_bet(**kwargs)


def _final_game(away_name="Tampa Bay Rays", home_name="Boston Red Sox",
                 away_score=2, home_score=5, game_pk=123456):
    return {
        "teams": {
            "away": {"team": {"name": away_name}, "score": away_score},
            "home": {"team": {"name": home_name}, "score": home_score},
        },
        "status": {"detailedState": "Final"},
        "gamePk": game_pk,
    }


class TestSchema:
    def test_notified_at_column_exists(self):
        with db._conn() as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(bets)")}
        assert "notified_at" in cols

    def test_new_bet_has_null_notified_at(self):
        _log()
        row = db.get_bets()[0]
        assert row["notified_at"] is None


def _boxscore(away_stats=None, home_stats=None):
    """away_stats/home_stats: list of (player_id, full_name, stat_group, stats_dict)."""
    def _players(entries):
        out = {}
        for pid, name, stat_group, stats in (entries or []):
            out[f"ID{pid}"] = {"person": {"id": pid, "fullName": name},
                                "stats": {stat_group: stats}}
        return out
    return {"teams": {"away": {"players": _players(away_stats)},
                       "home": {"players": _players(home_stats)}}}


class TestPropSettlement:
    """PROP bets (hitter/K/ER props) settle via MLB Stats API box score data,
    through the same resolve_bet_by_id()/CLV/notification path every other
    bet type uses -- see _parse_prop_bet/_match_prop_game/_settle_prop_bet in
    telegram_handler.py."""

    def _run(self, boxscore, game=None):
        import telegram_handler as th
        game = game or _final_game()
        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[game]), \
             patch.object(th, "_fetch_boxscore", return_value=boxscore), \
             patch.object(th, "_fetch_closing_odds", return_value=None), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check()
        return settled, mock_send

    # ── Hitter prop (total bases) ────────────────────────────────────────
    def test_hitter_prop_win(self):
        _log(bet="Aaron Judge TB O1.5", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(1, "Aaron Judge", "batting", {"totalBases": 3})])
        settled, mock_send = self._run(box)
        assert len(settled) == 1 and settled[0]["outcome"] == "W"
        assert db.get_bets()[0]["result"] == "W"
        mock_send.assert_called_once()

    def test_hitter_prop_loss(self):
        _log(bet="Aaron Judge TB O1.5", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(1, "Aaron Judge", "batting", {"totalBases": 1})])
        settled, _ = self._run(box)
        assert settled[0]["outcome"] == "L"

    def test_hitter_prop_push(self):
        """totalBases can land exactly on a whole-number line (unlike the
        fixed .5 lines _HITTER_PROP_LINES normally uses) -- still handled."""
        _log(bet="Aaron Judge TB O2", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(1, "Aaron Judge", "batting", {"totalBases": 2})])
        settled, _ = self._run(box)
        assert settled[0]["outcome"] == "P"

    # ── K prop (SP strikeouts, always Over) ──────────────────────────────
    def test_k_prop_win(self):
        _log(bet="Gerrit Cole Ks O6.5", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(2, "Gerrit Cole", "pitching", {"strikeOuts": 9})])
        settled, _ = self._run(box)
        assert settled[0]["outcome"] == "W"

    def test_k_prop_loss(self):
        _log(bet="Gerrit Cole Ks O6.5", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(2, "Gerrit Cole", "pitching", {"strikeOuts": 4})])
        settled, _ = self._run(box)
        assert settled[0]["outcome"] == "L"

    def test_k_prop_with_2025_data_flag_still_parses(self):
        """brain.py appends ' [2025 data]' after the line for some K props --
        must not break parsing."""
        _log(bet="Gerrit Cole Ks O6.5 [2025 data]", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(2, "Gerrit Cole", "pitching", {"strikeOuts": 9})])
        settled, _ = self._run(box)
        assert settled[0]["outcome"] == "W"

    # ── ER prop (SP earned runs, Over or Under) ──────────────────────────
    def test_er_prop_over_win(self):
        _log(bet="Shane Bieber ER O2.5", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(3, "Shane Bieber", "pitching", {"earnedRuns": 4})])
        settled, _ = self._run(box)
        assert settled[0]["outcome"] == "W"

    def test_er_prop_under_win(self):
        _log(bet="Shane Bieber ER U2.5", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(3, "Shane Bieber", "pitching", {"earnedRuns": 1})])
        settled, _ = self._run(box)
        assert settled[0]["outcome"] == "W"

    def test_er_prop_push(self):
        _log(bet="Shane Bieber ER O2", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(3, "Shane Bieber", "pitching", {"earnedRuns": 2})])
        settled, _ = self._run(box)
        assert settled[0]["outcome"] == "P"

    # ── Robustness ────────────────────────────────────────────────────────
    def test_unparseable_prop_format_leaves_bet_pending_no_crash(self, capsys):
        _log(bet="Aaron Judge Over 1.5 TB", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        settled, mock_send = self._run(_boxscore())
        assert settled == []
        mock_send.assert_not_called()
        assert db.get_bets()[0]["result"] is None
        assert "couldn't parse PROP bet string" in capsys.readouterr().out

    def test_player_not_found_in_boxscore_leaves_bet_pending_no_crash(self, capsys):
        _log(bet="Aaron Judge TB O1.5", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(1, "Someone Else", "batting", {"totalBases": 3})])
        settled, mock_send = self._run(box)
        assert settled == []
        mock_send.assert_not_called()
        assert db.get_bets()[0]["result"] is None
        assert "couldn't find box score stat" in capsys.readouterr().out

    def test_last_name_fallback_matches_when_full_name_differs_slightly(self):
        """Boxscore fullName formatting can differ slightly from the stored
        bet string (e.g. suffixes) -- last-name fallback still resolves it."""
        _log(bet="Judge TB O1.5", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")
        box = _boxscore(home_stats=[(1, "Aaron Judge", "batting", {"totalBases": 3})])
        settled, _ = self._run(box)
        assert settled[0]["outcome"] == "W"

    def test_prop_bet_does_not_block_other_bets_in_same_batch(self):
        import telegram_handler as th
        _log(bet="Aaron Judge Over 1.5 TB", bet_type="PROP", game="Tampa Bay Rays @ Boston Red Sox")  # unparseable
        _log(bet="Boston Red Sox", bet_type="ML")

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", side_effect=lambda g, code: "home" if code == "Boston Red Sox" else None), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check()

        assert len(settled) == 1
        assert settled[0]["type"] == "ML"
        mock_send.assert_called_once()


class TestPropBetParsing:
    """Unit coverage for _parse_prop_bet/_match_prop_game/_find_player_stat
    in isolation, independent of the full settlement loop."""

    def test_parses_hitter_props_across_all_categories(self):
        import telegram_handler as th
        cases = [
            ("Aaron Judge Hits O1.5", "batting", "hits", "O", 1.5),
            ("Aaron Judge HR O0.5", "batting", "homeRuns", "O", 0.5),
            ("Aaron Judge TB O1.5", "batting", "totalBases", "O", 1.5),
            ("Aaron Judge RBI O0.5", "batting", "rbi", "O", 0.5),
            ("Aaron Judge SO O0.5", "batting", "strikeOuts", "O", 0.5),
        ]
        for bet_str, group, key, direction, line in cases:
            parsed = th._parse_prop_bet(bet_str)
            assert parsed == {"player": "Aaron Judge", "stat_group": group,
                               "stat_key": key, "direction": direction, "line": line}, bet_str

    def test_parses_k_prop(self):
        import telegram_handler as th
        parsed = th._parse_prop_bet("Gerrit Cole Ks O7.5")
        assert parsed == {"player": "Gerrit Cole", "stat_group": "pitching",
                           "stat_key": "strikeOuts", "direction": "O", "line": 7.5}

    def test_parses_er_prop_both_directions(self):
        import telegram_handler as th
        over = th._parse_prop_bet("Shane Bieber ER O2.5")
        under = th._parse_prop_bet("Shane Bieber ER U2.5")
        assert over["direction"] == "O" and over["line"] == 2.5
        assert under["direction"] == "U" and under["line"] == 2.5
        assert over["stat_group"] == "pitching" and over["stat_key"] == "earnedRuns"

    def test_unparseable_string_returns_none(self):
        import telegram_handler as th
        assert th._parse_prop_bet("total nonsense") is None
        assert th._parse_prop_bet("") is None
        assert th._parse_prop_bet(None) is None

    def test_match_prop_game_exact_name_match(self):
        import telegram_handler as th
        games = [_final_game(away_name="Tampa Bay Rays", home_name="Boston Red Sox")]
        matched = th._match_prop_game("Tampa Bay Rays @ Boston Red Sox", games)
        assert matched is games[0]

    def test_match_prop_game_no_match_returns_none(self):
        import telegram_handler as th
        games = [_final_game(away_name="Tampa Bay Rays", home_name="Boston Red Sox")]
        assert th._match_prop_game("New York Yankees @ Houston Astros", games) is None

    def test_find_player_stat_prefers_exact_over_last_name(self):
        """Two players sharing a surname on opposite teams -- exact full-name
        match must win over a same-surname player on the other side."""
        import telegram_handler as th
        box = _boxscore(
            away_stats=[(1, "J.D. Martinez", "batting", {"totalBases": 1})],
            home_stats=[(2, "Victor Martinez", "batting", {"totalBases": 4})],
        )
        actual = th._find_player_stat(box, "Victor Martinez", "batting", "totalBases")
        assert actual == 4.0


class TestNotifiedAtGuard:
    """A newly-settled bet must get notified_at stamped atomically with
    its result, and exactly one Telegram send."""

    def test_settling_a_pending_bet_stamps_notified_at(self):
        import telegram_handler as th
        _log()

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        row = db.get_bets()[0]
        assert row["result"] == "W"
        assert row["notified_at"] is not None
        mock_send.assert_called_once()

    def test_over_cap_bet_stamps_notified_at_without_sending(self):
        import telegram_handler as th
        _log(over_cap=True, stake=0.0)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="L"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        row = db.get_bets()[0]
        assert row["result"] == "L"
        assert row["notified_at"] is not None
        mock_send.assert_not_called()


class TestBacklogPass:
    """Bets already graded through some other path (e.g. manual /settle)
    with result set but notified_at still NULL must get exactly one
    Telegram notification, using the existing result -- no game re-fetch
    or outcome re-determination needed."""

    def test_resolved_bet_missing_notification_gets_one_send(self):
        import telegram_handler as th
        _log()
        row = db.get_bets()[0]
        db.resolve_bet_by_id(bet_id=row["id"], closing_odds="-110",
                              result="W", game_score="BOS 5-2 TB")
        assert db.get_bets()[0]["notified_at"] is None  # sanity: old-style write, no stamp

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[]), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check()

        assert len(settled) == 1
        assert settled[0]["backlog_notify"] is True
        mock_send.assert_called_once()
        assert db.get_bets()[0]["notified_at"] is not None

    def test_backlog_over_cap_bet_marked_notified_without_sending(self):
        import telegram_handler as th
        _log(over_cap=True, stake=0.0)
        row = db.get_bets()[0]
        db.resolve_bet_by_id(bet_id=row["id"], closing_odds="-110",
                              result="L", game_score="BOS 1-4 TB")

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[]), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        mock_send.assert_not_called()
        assert db.get_bets()[0]["notified_at"] is not None


class TestIdempotency:
    """Running settlement twice must send exactly one Telegram message per
    bet, no matter which pass (pending or backlog) settled/notified it."""

    def test_running_twice_sends_exactly_one_message(self):
        import telegram_handler as th
        _log()

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()
            th.run_settlement_check()

        mock_send.assert_called_once()

    def test_second_run_returns_empty_settled_list(self):
        import telegram_handler as th
        _log()

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send"), \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()
            second = th.run_settlement_check()

        assert second == []


class TestDaysBackFilter:
    """days_back bounds only the pending-settlement pass (real MLB API
    calls) -- the default (None) is unbounded, matching this function's
    original behavior so existing callers/tests are unaffected. The
    backlog notify pass is never bounded since it makes no API call."""

    def test_none_default_settles_bet_regardless_of_age(self):
        import telegram_handler as th
        old_date = (datetime.now(th.ET).date() - timedelta(days=30)).isoformat()
        _log(date=old_date)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check()

        assert len(settled) == 1
        mock_send.assert_called_once()

    def test_days_back_excludes_older_pending_bet(self):
        import telegram_handler as th
        old_date = (datetime.now(th.ET).date() - timedelta(days=30)).isoformat()
        _log(date=old_date)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games") as mock_fetch, \
             patch.object(th, "_send") as mock_send:
            settled = th.run_settlement_check(days_back=3)

        assert settled == []
        mock_fetch.assert_not_called()
        mock_send.assert_not_called()

    def test_days_back_includes_recent_pending_bet(self):
        import telegram_handler as th
        recent_date = (datetime.now(th.ET).date() - timedelta(days=1)).isoformat()
        _log(date=recent_date)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check(days_back=3)

        assert len(settled) == 1
        mock_send.assert_called_once()


class TestManualSettlePaths:
    """handle_settle() (/win /loss /push Telegram commands) and the
    dashboard /api/settle + /api/resolve endpoints all resolve a bet
    outside the automated settle_bets flow. Each must stamp notified_at
    itself -- otherwise the next settle_bets run finds a resolved bet
    with notified_at still NULL and fires a spurious AUTO-SETTLE ping
    through the backlog-notify pass."""

    def test_handle_settle_stamps_notified_at(self):
        import telegram_handler as th
        _log()
        row = db.get_bets()[0]

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "sync_scout_json"):
            th.handle_settle("W", str(row["id"]))

        settled = db.get_bets()[0]
        assert settled["result"] == "W"
        assert settled["notified_at"] is not None

        # A subsequent settle_bets run must not re-notify via the backlog pass.
        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[]), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "_update_clv_log"):
            backlog_settled = th.run_settlement_check()

        assert backlog_settled == []
        mock_send.assert_not_called()

    def test_api_settle_stamps_notified_at(self):
        import api

        _log()
        row = db.get_bets()[0]

        with patch.object(api, "_db", db):
            client = api.app.test_client()
            resp = client.post("/api/settle", json={
                "bet_id": row["id"], "result": "W",
            })

        assert resp.status_code == 200
        settled = db.get_bets()[0]
        assert settled["result"] == "W"
        assert settled["notified_at"] is not None

    def test_api_resolve_stamps_notified_at(self):
        import api

        _log()
        row = db.get_bets()[0]

        with patch.object(api, "_db", db):
            client = api.app.test_client()
            resp = client.post("/api/resolve", json={
                "bet": row["bet"], "date": row["date"], "result": "win",
            })

        assert resp.status_code == 200
        settled = db.get_bets()[0]
        assert settled["result"] == "W"
        assert settled["notified_at"] is not None

    def test_backlog_pass_does_not_refire_after_any_manual_path(self):
        """settle_bets is the AUTO-SETTLE source -- once notified_at is
        stamped by any manual path, the backlog pass must skip the bet."""
        import telegram_handler as th
        import api

        _log()
        row = db.get_bets()[0]
        with patch.object(api, "_db", db):
            api.app.test_client().post("/api/settle", json={
                "bet_id": row["id"], "result": "L",
            })

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[]), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "_update_clv_log"):
            settled = th.run_settlement_check()

        assert settled == []
        mock_send.assert_not_called()


class TestRunSettleOneShot:
    """brain._run_settle() -- the --settle CLI entrypoint."""

    def test_calls_run_settlement_check_with_days_back_3(self):
        import brain
        with patch("telegram_handler.run_settlement_check", return_value=[{"id": 1}]) as mock_check:
            brain._run_settle()
        mock_check.assert_called_once_with(days_back=3)

    def test_swallows_exceptions_without_raising(self):
        import brain
        with patch("telegram_handler.run_settlement_check", side_effect=RuntimeError("boom")):
            brain._run_settle()  # must not raise


class TestCalibrationFeedAtSettlement:
    """calibration_buckets used to only get fed once/day via brain._run_debrief,
    which runs on GitHub Actions and never sees Railway's settlement results
    (Railway never touches git -- see CLAUDE.md's Deployment section). Since
    Railway is where every bet actually settles now, run_settlement_check()
    feeds calibration directly at the moment each bet gets a result, in both
    passes."""

    def test_pass1_auto_settled_bet_feeds_calibration(self):
        import telegram_handler as th
        _log(model_prob=0.57)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send"), \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        cal = db.get_calibration()
        assert any(c["bucket"] == "0.55-0.60" and c["wins"] == 1 for c in cal)

    def test_pass2_backlog_bet_feeds_calibration(self):
        """A bet resolved through some other path (manual /settle) that never
        got a Telegram ping -- Pass 2 must still feed calibration for it."""
        import telegram_handler as th
        _log(model_prob=0.62)
        db.resolve_bet("Boston Red Sox", "2026-07-19", "-110", "L", "2-5")

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[]), \
             patch.object(th, "_send"), \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        cal = db.get_calibration()
        assert any(c["bucket"] == "0.60-0.65" and c["total_bets"] == 1 and c["wins"] == 0 for c in cal)

    def test_push_does_not_feed_calibration(self):
        import telegram_handler as th
        _log(model_prob=0.55)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="P"), \
             patch.object(th, "_fetch_closing_odds", return_value=None), \
             patch.object(th, "_send"), \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        assert db.get_calibration() == []

    def test_over_cap_bet_still_feeds_calibration(self):
        """over_cap picks were never staked, but their model_prob accuracy
        is still real calibration signal -- must not be skipped."""
        import telegram_handler as th
        _log(model_prob=0.66, over_cap=True, stake=0.0)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value=None), \
             patch.object(th, "_send") as mock_send, \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        mock_send.assert_not_called()  # over_cap: no Telegram ping owed
        cal = db.get_calibration()
        assert any(c["bucket"] == "0.65-0.70" and c["wins"] == 1 for c in cal)

    def test_settled_prop_bet_feeds_calibration_via_settlement_not_just_debrief(self):
        """FIX 2 integration: a PROP bet settled through the new prop
        settlement path must also feed calibration immediately, not just
        when/if a debrief job eventually runs."""
        import telegram_handler as th
        _log(bet="Aaron Judge TB O1.5", bet_type="PROP",
             game="Tampa Bay Rays @ Boston Red Sox", model_prob=0.58)
        box = _boxscore(home_stats=[(1, "Aaron Judge", "batting", {"totalBases": 3})])

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_fetch_boxscore", return_value=box), \
             patch.object(th, "_fetch_closing_odds", return_value=None), \
             patch.object(th, "_send"), \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()

        cal = db.get_calibration()
        assert any(c["bucket"] == "0.55-0.60" and c["wins"] == 1 for c in cal)

    def test_running_settlement_twice_does_not_double_feed_calibration(self):
        """The same notified_at idempotency guard that prevents a duplicate
        Telegram send must also prevent a bet from feeding calibration twice."""
        import telegram_handler as th
        _log(model_prob=0.57)

        with patch.object(th, "_db", db), \
             patch.object(th, "_fetch_final_games", return_value=[_final_game()]), \
             patch.object(th, "_game_side", return_value="home"), \
             patch.object(th, "_determine_outcome", return_value="W"), \
             patch.object(th, "_fetch_closing_odds", return_value="-115"), \
             patch.object(th, "_send"), \
             patch.object(th, "sync_scout_json"), \
             patch.object(th, "_update_clv_log"):
            th.run_settlement_check()
            th.run_settlement_check()

        cal = db.get_calibration()
        bucket = next(c for c in cal if c["bucket"] == "0.55-0.60")
        assert bucket["total_bets"] == 1


class TestFeedCalibrationFromBetUnit:
    """db.feed_calibration_from_bet() in isolation."""

    def test_buckets_model_prob_correctly(self):
        db.feed_calibration_from_bet(0.57, "W")
        cal = db.get_calibration()
        assert cal[0]["bucket"] == "0.55-0.60"
        assert cal[0]["wins"] == 1
        assert cal[0]["total_bets"] == 1

    def test_loss_increments_total_but_not_wins(self):
        db.feed_calibration_from_bet(0.72, "L")
        cal = db.get_calibration()
        assert cal[0]["bucket"] == "0.70-0.75"
        assert cal[0]["wins"] == 0
        assert cal[0]["total_bets"] == 1

    def test_none_model_prob_is_a_noop(self):
        db.feed_calibration_from_bet(None, "W")
        assert db.get_calibration() == []

    def test_push_is_a_noop(self):
        db.feed_calibration_from_bet(0.55, "P")
        assert db.get_calibration() == []

    def test_multiple_bets_in_same_bucket_accumulate(self):
        db.feed_calibration_from_bet(0.56, "W")
        db.feed_calibration_from_bet(0.58, "L")
        db.feed_calibration_from_bet(0.59, "W")
        cal = db.get_calibration()
        bucket = next(c for c in cal if c["bucket"] == "0.55-0.60")
        assert bucket["total_bets"] == 3
        assert bucket["wins"] == 2
