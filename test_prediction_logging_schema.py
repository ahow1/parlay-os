"""Tests for the prediction logging schema (bets extension + feature_snapshots
+ model_versions). Schema-only per docs/superpowers/specs/2026-07-09-
prediction-logging-schema-design.md — no bet-flow wiring here.

Run: python -m pytest test_prediction_logging_schema.py -v

Isolation note: tests patch db.DB_PATH directly (not just the PARLAY_DB env
var) because db.py reads the env var once at import time into a module-level
constant — if `db` has already been imported earlier in the test session by
any other module, an env-var-only swap silently no-ops and tests would write
to the real parlay_os.db. patch.object(db, "DB_PATH", ...) is safe regardless
of import order.
"""

import shutil
import sqlite3
from unittest.mock import patch

import pytest


def _all_table_counts(db_path: str) -> dict:
    conn = sqlite3.connect(db_path)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )]
    counts = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}
    conn.close()
    return counts


@pytest.fixture
def scratch_db(tmp_path):
    """A scratch copy of the real parlay_os.db so migration tests run
    against realistic pre-existing data, never the live file."""
    src = "parlay_os.db"
    dest = str(tmp_path / "scratch_parlay_os.db")
    shutil.copy2(src, dest)
    return dest


class TestBetsExtensionColumns:
    """New nullable columns on bets: sport, model_version, reasoning_text,
    kalshi_price, kalshi_liquidity_ok, roi, graded_at."""

    def test_bets_has_new_columns_after_init(self, scratch_db):
        import db
        with patch.object(db, "DB_PATH", scratch_db):
            db.init_db()
            with db._conn() as conn:
                cols = {row[1] for row in conn.execute("PRAGMA table_info(bets)")}
        expected = {"sport", "model_version", "reasoning_text", "kalshi_price",
                    "kalshi_liquidity_ok", "roi", "graded_at"}
        assert expected.issubset(cols), f"missing columns: {expected - cols}"

    def test_sport_column_defaults_to_mlb(self, scratch_db):
        import db
        with patch.object(db, "DB_PATH", scratch_db):
            db.init_db()
            db.log_bet(
                date="2026-07-14", bet="SF", bet_type="ML", game="SF @ LAD",
                sp="Logan Webb", park="LAD", umpire="",
                bet_odds="+145", model_prob=0.57, market_prob=0.41,
                edge_pct=16.0, conviction="HIGH", stake=10.0,
            )
            with db._conn() as conn:
                row = conn.execute(
                    "SELECT sport FROM bets WHERE bet='SF' AND date='2026-07-14'"
                ).fetchone()
        assert row["sport"] == "MLB"


class TestFeatureSnapshotsAndModelVersionsTables:
    """New feature_snapshots (EAV) and model_versions tables."""

    def test_feature_snapshots_table_has_expected_columns(self, scratch_db):
        import db
        with patch.object(db, "DB_PATH", scratch_db):
            db.init_db()
            with db._conn() as conn:
                cols = {row[1] for row in conn.execute("PRAGMA table_info(feature_snapshots)")}
        expected = {"id", "bet_id", "feature_name", "feature_value",
                    "feature_value_text", "feature_weight", "created_at"}
        assert expected.issubset(cols), f"missing columns: {expected - cols}"

    def test_model_versions_table_has_expected_columns(self, scratch_db):
        import db
        with patch.object(db, "DB_PATH", scratch_db):
            db.init_db()
            with db._conn() as conn:
                cols = {row[1] for row in conn.execute("PRAGMA table_info(model_versions)")}
        expected = {"id", "model_name", "version", "sport", "created_date",
                    "feature_list", "notes", "rolling_roi", "rolling_clv"}
        assert expected.issubset(cols), f"missing columns: {expected - cols}"

    def test_model_versions_enforces_unique_name_and_version(self, scratch_db):
        import db
        with patch.object(db, "DB_PATH", scratch_db):
            db.init_db()
            with db._conn() as conn:
                conn.execute(
                    "INSERT INTO model_versions (model_name, version, created_date) "
                    "VALUES ('mlb_ml', 'v1', '2026-07-14')"
                )
                with pytest.raises(sqlite3.IntegrityError):
                    conn.execute(
                        "INSERT INTO model_versions (model_name, version, created_date) "
                        "VALUES ('mlb_ml', 'v1', '2026-07-14')"
                    )


class TestMigrationSafety:
    """The migration must never lose existing data, must keep log_bet()'s
    existing (unmodified) call sites working, and must be idempotent."""

    def test_existing_row_counts_unchanged_after_init(self, scratch_db):
        import db
        before = _all_table_counts(scratch_db)
        with patch.object(db, "DB_PATH", scratch_db):
            db.init_db()
        after = _all_table_counts(scratch_db)
        for table, count in before.items():
            assert after.get(table) == count, (
                f"row count changed for {table}: {count} -> {after.get(table)}"
            )

    def test_log_bet_unmodified_signature_still_works(self, scratch_db):
        import db
        with patch.object(db, "DB_PATH", scratch_db):
            db.init_db()
            before = _all_table_counts(scratch_db)["bets"]
            db.log_bet(
                date="2026-07-14", bet="LAD", bet_type="ML", game="SF @ LAD",
                sp="Yoshinobu Yamamoto", park="LAD", umpire="",
                bet_odds="-160", model_prob=0.63, market_prob=0.59,
                edge_pct=6.5, conviction="MEDIUM", stake=20.0,
            )
        after = _all_table_counts(scratch_db)["bets"]
        assert after == before + 1

    def test_init_db_is_idempotent(self, scratch_db):
        import db
        with patch.object(db, "DB_PATH", scratch_db):
            db.init_db()
            db.init_db()  # second run must not raise
            with db._conn() as conn:
                cols = {row[1] for row in conn.execute("PRAGMA table_info(bets)")}
        expected = {"sport", "model_version", "reasoning_text", "kalshi_price",
                    "kalshi_liquidity_ok", "roi", "graded_at"}
        assert expected.issubset(cols)
