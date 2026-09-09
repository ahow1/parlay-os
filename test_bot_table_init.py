"""
Regression test for the 2026-09-01 "no such table: team_memory" crash.

memory_engine.init_memory_tables() / init_brain_tables() were only ever
called from run_daily_scout() and live_engine.py (both scout-side paths).
brain.py's --bot startup (Railway's worker) never called them, so any
worker-side read/write into that table group -- team_memory, player_memory,
bet_memory, situation_memory, etc. -- threw OperationalError on a database
that never went through a scout run. Same bug independently affected
_run_morning_planner() (--planner), whose GH Actions job starts from a
fresh scratch db every run and unconditionally calls team_prior().

See CLAUDE.md "Known Bugs" for the full writeup.
"""
import sqlite3

import pytest

import memory_engine as me


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "fresh_worker.db")
    monkeypatch.setattr(me, "DB_PATH", db_path)
    return db_path


def test_team_prior_raises_on_uninitialized_db(fresh_db):
    """Documents the original crash: a brand-new db has no team_memory table."""
    with pytest.raises(sqlite3.OperationalError, match="team_memory"):
        me.team_prior("NYY", "home", 7)


def test_init_memory_and_brain_tables_creates_full_table_set(fresh_db):
    """This is the exact call sequence brain.py's --bot startup now runs
    before starting any thread (and _run_morning_planner runs before its
    first team_prior() call)."""
    me.init_memory_tables()
    me.init_brain_tables()

    conn = sqlite3.connect(fresh_db)
    tables = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    conn.close()

    expected = {
        # init_memory_tables
        "player_memory", "team_memory", "model_calibration", "live_bet_memory",
        "pitcher_profiles", "hitter_profiles", "bullpen_memory", "umpire_memory",
        "ballpark_memory", "manager_memory", "factor_reliability",
        "game_updates_log", "improvement_schedule", "clv_analytics",
        "worst_bets_log", "blind_spots", "model_accuracy_log",
        # init_brain_tables
        "bet_memory", "sp_performance", "team_performance",
        "situation_memory", "brain_weight_history",
    }
    missing = expected - tables
    assert not missing, f"tables never created: {missing}"


def test_team_prior_succeeds_after_init(fresh_db):
    me.init_memory_tables()
    me.init_brain_tables()
    # Must not raise -- this is the read that crashed the worker.
    assert me.team_prior("NYY", "home", 7) is None  # no data yet, but no exception
