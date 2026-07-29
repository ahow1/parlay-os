"""Tests for the GitHub Actions -> Railway pick sync bridge.

This replaces the old pull-based db.sync_pending_bets_from_github() (dead
since parlay_os.db stopped being committed to git -- see the *.db gitignore
commit) with a push-based path: GH Actions' scout workflow POSTs each
newly-logged pending bet to Railway's POST /api/sync_bet endpoint right
after logging it locally.

Covers, in order: db.log_bet()'s verify_hash return, db.insert_synced_bet()
(the receiving-side insert/dedup logic), db.push_bet_to_railway() (the
sending-side HTTP call), api.py's /api/sync_bet endpoint (auth + wiring),
and brain.py's _push_synced_pick() integration point.

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
    return d.log_bet(**kwargs)


class TestLogBetReturnsVerifyHash:
    def test_log_bet_returns_the_hash_it_computed(self, local_db):
        vh = _log(local_db)
        assert vh
        row = local_db.get_pick_by_hash(vh)
        assert row is not None
        assert row["bet"] == "Boston Red Sox"

    def test_ignored_duplicate_insert_hash_does_not_resolve_to_a_row(self, local_db):
        """Same (date, game, bet, type) twice -- the unique index makes the
        second INSERT OR IGNORE a no-op, but log_bet() still returns the
        freshly-computed hash for that (unpersisted) attempt. Callers must
        handle a hash that doesn't resolve to any stored row (see
        _push_synced_pick's noop-on-missing-row test)."""
        _log(local_db)
        vh2 = _log(local_db)  # same date/game/bet/type -> silently ignored
        row = local_db.get_pick_by_hash(vh2)
        assert row is None
        assert len(local_db.get_bets()) == 1


class TestInsertSyncedBet:
    def _row(self, **overrides):
        row = {
            "date": "2026-07-28", "timestamp": "2026-07-28T10:00:00-04:00",
            "bet": "New York Yankees", "type": "ML", "game": "NYY @ BOS",
            "bet_odds": "-150", "model_prob": 0.6, "market_prob": 0.55,
            "edge_pct": 5.0, "conviction": "HIGH", "stake": 30.0,
            "verify_hash": "abc123",
        }
        row.update(overrides)
        return row

    def test_inserts_a_new_row(self, local_db):
        inserted = local_db.insert_synced_bet(self._row())
        assert inserted is True
        got = local_db.get_pick_by_hash("abc123")
        assert got is not None
        assert got["bet"] == "New York Yankees"
        assert got["stake"] == 30.0
        assert got["result"] is None  # arrives pending

    def test_duplicate_verify_hash_is_not_inserted_again(self, local_db):
        row = self._row()
        first = local_db.insert_synced_bet(row)
        second = local_db.insert_synced_bet(row)
        assert first is True
        assert second is False
        assert len(local_db.get_bets()) == 1

    def test_never_overwrites_an_existing_row_even_with_different_data(self, local_db):
        """Same verify_hash, different stake/result in the incoming payload
        -- the existing local row (which may already be settled) must win."""
        vh = _log(local_db, stake=25.0)
        local_db.resolve_bet_by_id(
            bet_id=local_db.get_pick_by_hash(vh)["id"],
            closing_odds="-115", result="W", game_score="5-3", mark_notified=True,
        )
        inserted = local_db.insert_synced_bet(self._row(
            verify_hash=vh, bet="Boston Red Sox", stake=9999.0))
        assert inserted is False
        row = local_db.get_pick_by_hash(vh)
        assert row["result"] == "W"       # untouched
        assert row["stake"] == 25.0       # untouched, not 9999.0

    def test_missing_verify_hash_is_rejected(self, local_db):
        assert local_db.insert_synced_bet({"date": "2026-07-28", "bet": "X"}) is False
        assert local_db.get_bets() == []

    def test_missing_date_is_rejected(self, local_db):
        assert local_db.insert_synced_bet({"verify_hash": "abc", "bet": "X"}) is False

    def test_existing_pending_bets_from_before_the_push_bridge_are_untouched(self, local_db):
        """Bets already synced via the old pull-based bridge (before this
        change) must be unaffected by anything the new push path does --
        insert_synced_bet only ever inserts brand-new verify_hashes."""
        pre_existing_vh = _log(local_db, bet="Pre-existing Pick", date="2026-07-20")
        pre_existing_row_before = dict(local_db.get_pick_by_hash(pre_existing_vh))

        local_db.insert_synced_bet(self._row(verify_hash="brand-new-hash", bet="Brand New Pick"))

        pre_existing_row_after = dict(local_db.get_pick_by_hash(pre_existing_vh))
        assert pre_existing_row_after == pre_existing_row_before
        assert len(local_db.get_bets()) == 2


class TestPushBetToRailway:
    def test_noop_without_railway_sync_url(self, monkeypatch):
        monkeypatch.delenv("RAILWAY_SYNC_URL", raising=False)
        monkeypatch.setenv("SYNC_SECRET", "shh")
        with patch("requests.post") as mock_post:
            result = db.push_bet_to_railway({"verify_hash": "x"})
        assert result is False
        mock_post.assert_not_called()

    def test_noop_without_sync_secret(self, monkeypatch):
        monkeypatch.setenv("RAILWAY_SYNC_URL", "https://example.railway.app")
        monkeypatch.delenv("SYNC_SECRET", raising=False)
        with patch("requests.post") as mock_post:
            result = db.push_bet_to_railway({"verify_hash": "x"})
        assert result is False
        mock_post.assert_not_called()

    def test_posts_with_bearer_auth_when_configured(self, monkeypatch):
        monkeypatch.setenv("RAILWAY_SYNC_URL", "https://example.railway.app")
        monkeypatch.setenv("SYNC_SECRET", "shh")
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"ok": True, "inserted": True}
        with patch("requests.post", return_value=mock_resp) as mock_post:
            result = db.push_bet_to_railway({"verify_hash": "x", "bet": "NYY"})
        assert result is True
        args, kwargs = mock_post.call_args
        assert args[0] == "https://example.railway.app/api/sync_bet"
        assert kwargs["json"] == {"verify_hash": "x", "bet": "NYY"}
        assert kwargs["headers"]["Authorization"] == "Bearer shh"

    def test_strips_trailing_slash_from_base_url(self, monkeypatch):
        monkeypatch.setenv("RAILWAY_SYNC_URL", "https://example.railway.app/")
        monkeypatch.setenv("SYNC_SECRET", "shh")
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"inserted": True}
        with patch("requests.post", return_value=mock_resp) as mock_post:
            db.push_bet_to_railway({"verify_hash": "x"})
        assert mock_post.call_args[0][0] == "https://example.railway.app/api/sync_bet"

    def test_non_200_response_returns_false_without_raising(self, monkeypatch):
        monkeypatch.setenv("RAILWAY_SYNC_URL", "https://example.railway.app")
        monkeypatch.setenv("SYNC_SECRET", "shh")
        mock_resp = MagicMock(status_code=401, text="unauthorized")
        with patch("requests.post", return_value=mock_resp):
            result = db.push_bet_to_railway({"verify_hash": "x"})
        assert result is False

    def test_network_exception_returns_false_without_raising(self, monkeypatch):
        monkeypatch.setenv("RAILWAY_SYNC_URL", "https://example.railway.app")
        monkeypatch.setenv("SYNC_SECRET", "shh")
        with patch("requests.post", side_effect=ConnectionError("boom")):
            result = db.push_bet_to_railway({"verify_hash": "x"})
        assert result is False


class TestSyncBetEndpoint:
    """POST /api/sync_bet in api.py -- the receiving side, as Railway's
    Flask process actually exposes it."""

    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path, monkeypatch):
        import api
        tmp_db = str(tmp_path / "api_sync_test.db")
        monkeypatch.setenv("SYNC_SECRET", "test-secret")
        with patch.object(db, "DB_PATH", tmp_db), patch.object(api, "_db", db):
            db.init_db()
            self.client = api.app.test_client()
            yield db

    def _payload(self, **overrides):
        row = {
            "date": "2026-07-28", "timestamp": "2026-07-28T10:00:00-04:00",
            "bet": "New York Yankees", "type": "ML", "game": "NYY @ BOS",
            "bet_odds": "-150", "model_prob": 0.6, "market_prob": 0.55,
            "edge_pct": 5.0, "conviction": "HIGH", "stake": 30.0,
            "verify_hash": "endpoint-hash-1",
        }
        row.update(overrides)
        return row

    def test_rejects_missing_auth_header(self):
        resp = self.client.post("/api/sync_bet", json=self._payload())
        assert resp.status_code == 401

    def test_rejects_wrong_secret(self):
        resp = self.client.post("/api/sync_bet", json=self._payload(),
                                 headers={"Authorization": "Bearer wrong-secret"})
        assert resp.status_code == 401

    def test_accepts_correct_secret_and_inserts(self):
        resp = self.client.post("/api/sync_bet", json=self._payload(),
                                 headers={"Authorization": "Bearer test-secret"})
        assert resp.status_code == 200
        assert resp.get_json() == {"ok": True, "inserted": True}
        assert db.get_pick_by_hash("endpoint-hash-1") is not None

    def test_duplicate_post_does_not_double_insert(self):
        headers = {"Authorization": "Bearer test-secret"}
        first = self.client.post("/api/sync_bet", json=self._payload(), headers=headers)
        second = self.client.post("/api/sync_bet", json=self._payload(), headers=headers)
        assert first.get_json()["inserted"] is True
        assert second.get_json()["inserted"] is False
        assert len(db.get_bets()) == 1

    def test_missing_verify_hash_returns_400(self):
        payload = self._payload()
        del payload["verify_hash"]
        resp = self.client.post("/api/sync_bet", json=payload,
                                 headers={"Authorization": "Bearer test-secret"})
        assert resp.status_code == 400


class TestPushSyncedPickIntegration:
    """brain._push_synced_pick() -- the glue called from _log_bet_with_retry
    / _log_pick_with_retry right after a successful local log_bet()."""

    def test_noop_when_hash_is_none(self):
        import brain
        with patch("db.push_bet_to_railway") as mock_push:
            brain._push_synced_pick(None)
        mock_push.assert_not_called()

    def test_fetches_row_and_pushes_it(self, local_db):
        import brain
        vh = _log(local_db)
        with patch.object(brain, "_db", local_db), \
             patch.object(local_db, "push_bet_to_railway", return_value=True) as mock_push:
            brain._push_synced_pick(vh)
        mock_push.assert_called_once()
        pushed_row = mock_push.call_args[0][0]
        assert pushed_row["verify_hash"] == vh
        assert pushed_row["bet"] == "Boston Red Sox"

    def test_hash_matching_no_row_is_a_noop(self):
        """The 'ignored duplicate' case (see TestLogBetReturnsVerifyHash) --
        a hash that doesn't resolve to any row must not crash or push None."""
        import brain
        with patch.object(brain._db, "get_pick_by_hash", return_value=None), \
             patch.object(brain._db, "push_bet_to_railway") as mock_push:
            brain._push_synced_pick("nonexistent-hash")
        mock_push.assert_not_called()

    def test_exception_in_push_never_raises(self, local_db):
        import brain
        vh = _log(local_db)
        with patch.object(brain, "_db", local_db), \
             patch.object(local_db, "push_bet_to_railway", side_effect=RuntimeError("boom")):
            brain._push_synced_pick(vh)  # must not raise


class TestEndToEndGhActionsToRailway:
    """Full path: a bet logged via the GH Actions pick-generation flow
    (log_bet -> verify_hash) reaches Railway's local db through the new
    endpoint, exercised as two separate processes/databases would see it."""

    def test_bet_logged_on_gh_actions_side_reaches_railway_via_endpoint(self, tmp_path, monkeypatch):
        import api

        gh_db_path = str(tmp_path / "gh_actions.db")
        railway_db_path = str(tmp_path / "railway.db")

        # "GitHub Actions" logs a bet locally.
        with patch.object(db, "DB_PATH", gh_db_path):
            db.init_db()
            vh = _log(db, bet="Boston Red Sox")
            gh_row = db.get_pick_by_hash(vh)

        # It gets pushed over HTTP -- we simulate the network hop by handing
        # the row straight to "Railway"'s Flask endpoint via its test client.
        with patch.object(db, "DB_PATH", railway_db_path), patch.object(api, "_db", db):
            db.init_db()
            monkeypatch.setenv("SYNC_SECRET", "shared-secret")
            client = api.app.test_client()
            resp = client.post("/api/sync_bet", json=dict(gh_row),
                                headers={"Authorization": "Bearer shared-secret"})
            assert resp.status_code == 200
            assert resp.get_json()["inserted"] is True

            railway_row = db.get_pick_by_hash(vh)
            assert railway_row is not None
            assert railway_row["bet"] == "Boston Red Sox"
            assert railway_row["result"] is None  # arrives pending

            # Duplicate POST (e.g. a retried request) must not double-insert.
            resp2 = client.post("/api/sync_bet", json=dict(gh_row),
                                 headers={"Authorization": "Bearer shared-secret"})
            assert resp2.get_json()["inserted"] is False
            assert len(db.get_bets()) == 1
