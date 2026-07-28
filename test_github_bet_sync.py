"""Tests for db.sync_pending_bets_from_github() -- the one-way bridge that
lets Railway's --bot process (no git integration) pick up bets GitHub
Actions generates and commits, matched by verify_hash so it never
duplicates or overwrites a bet Railway already has.

Run: python -m pytest test_github_bet_sync.py -v
"""

from unittest.mock import patch, MagicMock

import pytest

import db


@pytest.fixture
def local_db(tmp_path):
    tmp_db = str(tmp_path / "local.db")
    with patch.object(db, "DB_PATH", tmp_db):
        db.init_db()
        yield db


def _log(d, bet="Boston Red Sox", game="Tampa Bay Rays @ Boston Red Sox",
         bet_type="ML", date="2026-07-28", **overrides):
    kwargs = dict(
        date=date, bet=bet, bet_type=bet_type, game=game,
        sp="", park="BOS", umpire="", bet_odds="-120",
        model_prob=0.55, market_prob=0.50, edge_pct=5.0,
        conviction="MEDIUM", stake=25.0,
    )
    kwargs.update(overrides)
    d.log_bet(**kwargs)


def _read_bytes_checkpointed(path):
    """Force a WAL checkpoint before reading raw bytes -- db.py runs in
    WAL mode, so a `with _conn() as conn:` write (which commits but never
    closes the connection) can leave committed rows sitting in the -wal
    file rather than the main .db file. Not a production concern (GitHub
    Actions' brain.py process fully exits before `git add`, which does
    checkpoint), but reading the file mid-process in these tests needs an
    explicit checkpoint first."""
    import sqlite3
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA wal_checkpoint(FULL)")
    conn.close()
    with open(path, "rb") as f:
        return f.read()


def _make_remote_db(tmp_path, name="remote.db"):
    """Build a standalone sqlite file with a real bets table + rows, fully
    independent of the local_db fixture's DB_PATH, mimicking what a
    fetched GitHub-committed parlay_os.db would look like."""
    path = str(tmp_path / name)
    with patch.object(db, "DB_PATH", path):
        db.init_db()
        yield_db = db
    return path


class TestSyncPendingBetsFromGithub:
    def test_pulls_new_pending_bet_not_present_locally(self, local_db, tmp_path):
        remote_path = _make_remote_db(tmp_path)
        with patch.object(db, "DB_PATH", remote_path):
            _log(db, bet="Chicago Cubs", game="Chicago Cubs @ Milwaukee Brewers")

        remote_bytes = _read_bytes_checkpointed(remote_path)
        mock_resp = MagicMock(content=remote_bytes)
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_resp):
            n = db.sync_pending_bets_from_github()

        assert n == 1
        rows = local_db.get_bets()
        assert len(rows) == 1
        assert rows[0]["bet"] == "Chicago Cubs"
        assert rows[0]["result"] is None

    def test_running_twice_against_the_same_remote_state_does_not_duplicate(self, local_db, tmp_path):
        """The real usage pattern: a 15-min polling tick hits the same
        remote commit multiple times before GitHub's next scout run --
        the second (and further) tick(s) must insert nothing new."""
        remote_path = _make_remote_db(tmp_path)
        with patch.object(db, "DB_PATH", remote_path):
            _log(db, bet="Boston Red Sox")

        remote_bytes = _read_bytes_checkpointed(remote_path)
        mock_resp = MagicMock(content=remote_bytes)
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_resp):
            first  = db.sync_pending_bets_from_github()
            second = db.sync_pending_bets_from_github()

        assert first == 1
        assert second == 0
        assert len(local_db.get_bets()) == 1

    def test_does_not_pull_settled_bets_from_remote(self, local_db, tmp_path):
        remote_path = _make_remote_db(tmp_path)
        with patch.object(db, "DB_PATH", remote_path):
            _log(db, bet="Colorado Rockies")
            row = db.get_bets()[0]
            db.resolve_bet_by_id(bet_id=row["id"], closing_odds="-110", result="W", game_score="COL 5-2")

        remote_bytes = _read_bytes_checkpointed(remote_path)
        mock_resp = MagicMock(content=remote_bytes)
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_resp):
            n = db.sync_pending_bets_from_github()

        assert n == 0
        assert local_db.get_bets() == []

    def test_never_touches_existing_local_settled_bet(self, local_db, tmp_path):
        """Railway's copy is authoritative for settlement -- a sync must
        never overwrite a local bet's result/notified_at, even if a
        same-hash row somehow appears on the remote side."""
        _log(local_db, bet="New York Yankees")
        row = local_db.get_bets()[0]
        local_db.resolve_bet_by_id(bet_id=row["id"], closing_odds="-115", result="L", game_score="NYY 2-4")
        settled_before = local_db.get_bets()[0]

        remote_path = _make_remote_db(tmp_path)
        remote_bytes = _read_bytes_checkpointed(remote_path)
        mock_resp = MagicMock(content=remote_bytes)
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_resp):
            db.sync_pending_bets_from_github()

        settled_after = local_db.get_bets()[0]
        assert settled_after["result"] == "L"
        assert settled_after["notified_at"] == settled_before["notified_at"]

    def test_network_failure_returns_zero_not_raise(self, local_db):
        with patch("requests.get", side_effect=ConnectionError("boom")):
            n = db.sync_pending_bets_from_github()
        assert n == 0

    def test_malformed_remote_bytes_returns_zero_not_raise(self, local_db):
        mock_resp = MagicMock(content=b"not a sqlite file")
        mock_resp.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mock_resp):
            n = db.sync_pending_bets_from_github()
        assert n == 0

    def test_multiple_new_pending_bets_all_pulled(self, local_db, tmp_path):
        remote_path = _make_remote_db(tmp_path)
        with patch.object(db, "DB_PATH", remote_path):
            _log(db, bet="Team A", game="Team A @ Team B")
            _log(db, bet="Team C", game="Team C @ Team D", date="2026-07-27")

        remote_bytes = _read_bytes_checkpointed(remote_path)
        mock_resp = MagicMock(content=remote_bytes)
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_resp):
            n = db.sync_pending_bets_from_github()

        assert n == 2
        assert {b["bet"] for b in local_db.get_bets()} == {"Team A", "Team C"}
