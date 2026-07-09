# Prediction Logging Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing `bets` table and add `feature_snapshots` (EAV) and `model_versions` tables to `db.py`, so a future bet-flow change can log per-pick feature data and model metadata without inventing a parallel schema.

**Architecture:** Pure schema addition inside `db.py`'s existing `init_db()` migration mechanism (guarded `ALTER TABLE` + `CREATE TABLE IF NOT EXISTS`), plus a small set of CRUD helper functions following the same style as the existing `log_bet()` / `get_pick_by_hash()` functions. No other file is touched. No caller anywhere is wired up yet — that's future work.

**Tech Stack:** Python 3, `sqlite3` (stdlib), `pytest`.

## Global Constraints

- Every new/altered column is nullable or has a constant default — existing rows in `bets` must never break or lose data.
- Follow the existing guarded-ALTER pattern (`db.py:317-342`): each new `ALTER TABLE` statement goes in the same `try/except sqlite3.OperationalError: pass` loop.
- `log_bet()`'s existing signature and every current call site (`brain.py:3378`) stay unmodified — this plan only adds columns with defaults, never touches the function.
- Use plain `INTEGER PRIMARY KEY AUTOINCREMENT` and ISO-8601 text for dates/timestamps — matches the rest of `db.py`, stays Postgres-portable for the (currently blocked, credential-less) future migration.
- `feature_snapshots` has **no** `sport` column — it inherits sport via `bet_id → bets.sport` (see design doc rationale: avoids a denormalized column that can drift from its parent row).
- Tests must use an isolated temp SQLite file via the `PARLAY_DB` env var swap — the exact pattern already used in `test_fixes.py:124-135` (`setup_method`/`teardown_method`, `tempfile.NamedTemporaryFile`, restore the env var on teardown). Never write to the real `parlay_os.db`.
- This plan does not modify `brain.py`, `last_scout.json`, `props_output.json`, or any Telegram/dashboard output.

Design reference: `docs/superpowers/specs/2026-07-09-prediction-logging-schema-design.md`

---

### Task 1: Extend `bets` and add `feature_snapshots` / `model_versions` tables

**Files:**
- Modify: `db.py:317-337` (guarded `ALTER TABLE` migrations list)
- Modify: `db.py:305-316` (end of the `init_db()` `executescript` block — add two new `CREATE TABLE` statements + one index)
- Test: `test_prediction_logging_schema.py` (new file, root of repo — matches `test_fixes.py` / `test_xwoba_pipeline.py` convention)

**Interfaces:**
- Consumes: `db.init_db()`, `db.log_bet()`, `db.get_bets()`, `db._conn()` — all pre-existing, signatures unchanged.
- Produces: `bets` table gains columns `sport TEXT DEFAULT 'MLB'`, `model_version TEXT`, `reasoning_text TEXT`, `kalshi_price REAL`, `kalshi_liquidity_ok INTEGER`, `roi REAL`, `graded_at TEXT`. New tables `feature_snapshots(id, bet_id, feature_name, feature_value, feature_value_text, feature_weight, created_at)` and `model_versions(id, model_name, version, sport, created_date, feature_list, notes, rolling_roi, rolling_clv)`. Task 2 depends on these existing.

- [ ] **Step 1: Write the failing test for the `bets` extension columns**

Create `test_prediction_logging_schema.py`:

```python
"""Tests for the prediction logging schema (bets extension +
feature_snapshots + model_versions). Run:
python -m pytest test_prediction_logging_schema.py -v
"""

import os
import sqlite3
import tempfile
import pytest


class TestBetsExtensionColumns:
    """New nullable columns on bets: sport, model_version, reasoning_text,
    kalshi_price, kalshi_liquidity_ok, roi, graded_at."""

    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._orig_path = os.environ.get("PARLAY_DB", "parlay_os.db")
        os.environ["PARLAY_DB"] = self._tmp.name

    def teardown_method(self):
        os.environ["PARLAY_DB"] = self._orig_path
        self._tmp.close()
        os.unlink(self._tmp.name)

    def test_bets_has_new_columns_after_init(self):
        import db as _db
        _db.init_db()
        with _db._conn() as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(bets)")}
        expected = {"sport", "model_version", "reasoning_text", "kalshi_price",
                    "kalshi_liquidity_ok", "roi", "graded_at"}
        assert expected.issubset(cols), f"missing columns: {expected - cols}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test_prediction_logging_schema.py::TestBetsExtensionColumns -v`
Expected: FAIL — `AssertionError: missing columns: {...}` (columns don't exist yet)

- [ ] **Step 3: Implement — add the new columns to the guarded migrations list**

In `db.py`, find this block (currently ends around line 337):

```python
            "ALTER TABLE bets ADD COLUMN confidence_engine_score INTEGER",
            # line_history: signal_type column (also added lazily by LME)
            "ALTER TABLE line_history ADD COLUMN signal_type TEXT",
```

Replace with:

```python
            "ALTER TABLE bets ADD COLUMN confidence_engine_score INTEGER",
            # Prediction logging schema (2026-07-09): bets extension
            "ALTER TABLE bets ADD COLUMN sport TEXT DEFAULT 'MLB'",
            "ALTER TABLE bets ADD COLUMN model_version TEXT",
            "ALTER TABLE bets ADD COLUMN reasoning_text TEXT",
            "ALTER TABLE bets ADD COLUMN kalshi_price REAL",
            "ALTER TABLE bets ADD COLUMN kalshi_liquidity_ok INTEGER",
            "ALTER TABLE bets ADD COLUMN roi REAL",
            "ALTER TABLE bets ADD COLUMN graded_at TEXT",
            # line_history: signal_type column (also added lazily by LME)
            "ALTER TABLE line_history ADD COLUMN signal_type TEXT",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test_prediction_logging_schema.py::TestBetsExtensionColumns -v`
Expected: PASS

- [ ] **Step 5: Write the failing test for the two new tables**

Append to `test_prediction_logging_schema.py`:

```python
class TestNewTablesExist:
    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._orig_path = os.environ.get("PARLAY_DB", "parlay_os.db")
        os.environ["PARLAY_DB"] = self._tmp.name

    def teardown_method(self):
        os.environ["PARLAY_DB"] = self._orig_path
        self._tmp.close()
        os.unlink(self._tmp.name)

    def test_feature_snapshots_table_exists_with_expected_columns(self):
        import db as _db
        _db.init_db()
        with _db._conn() as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(feature_snapshots)")}
        assert cols == {"id", "bet_id", "feature_name", "feature_value",
                         "feature_value_text", "feature_weight", "created_at"}

    def test_model_versions_table_exists_with_expected_columns(self):
        import db as _db
        _db.init_db()
        with _db._conn() as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(model_versions)")}
        assert cols == {"id", "model_name", "version", "sport", "created_date",
                         "feature_list", "notes", "rolling_roi", "rolling_clv"}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `python -m pytest test_prediction_logging_schema.py::TestNewTablesExist -v`
Expected: FAIL — `sqlite3.OperationalError: no such table: feature_snapshots`

- [ ] **Step 7: Implement — add the two `CREATE TABLE` statements**

In `db.py`, find the end of the `init_db()` `executescript` block:

```python
        CREATE TABLE IF NOT EXISTS lineup_tracker (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            date              TEXT NOT NULL,
            game_pk           TEXT NOT NULL,
            team              TEXT,
            projected_lineup  TEXT,
            confirmed_lineup  TEXT,
            changes_detected  TEXT,
            alert_sent        INTEGER DEFAULT 0,
            UNIQUE(date, team)
        );
        """)
```

Replace with:

```python
        CREATE TABLE IF NOT EXISTS lineup_tracker (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            date              TEXT NOT NULL,
            game_pk           TEXT NOT NULL,
            team              TEXT,
            projected_lineup  TEXT,
            confirmed_lineup  TEXT,
            changes_detected  TEXT,
            alert_sent        INTEGER DEFAULT 0,
            UNIQUE(date, team)
        );

        CREATE TABLE IF NOT EXISTS feature_snapshots (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            bet_id              INTEGER NOT NULL REFERENCES bets(id),
            feature_name        TEXT NOT NULL,
            feature_value       REAL,
            feature_value_text  TEXT,
            feature_weight      REAL,
            created_at          TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_feature_snapshots_bet_id
            ON feature_snapshots(bet_id);

        CREATE TABLE IF NOT EXISTS model_versions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            model_name    TEXT NOT NULL,
            version       TEXT NOT NULL,
            sport         TEXT DEFAULT 'MLB',
            created_date  TEXT NOT NULL,
            feature_list  TEXT,
            notes         TEXT,
            rolling_roi   REAL,
            rolling_clv   REAL,
            UNIQUE(model_name, version)
        );
        """)
```

- [ ] **Step 8: Run test to verify it passes**

Run: `python -m pytest test_prediction_logging_schema.py::TestNewTablesExist -v`
Expected: PASS

- [ ] **Step 9: Write the migration-safety tests (idempotency, no data loss, `log_bet()` unmodified)**

Append to `test_prediction_logging_schema.py`:

```python
class TestMigrationSafety:
    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._orig_path = os.environ.get("PARLAY_DB", "parlay_os.db")
        os.environ["PARLAY_DB"] = self._tmp.name

    def teardown_method(self):
        os.environ["PARLAY_DB"] = self._orig_path
        self._tmp.close()
        os.unlink(self._tmp.name)

    def test_init_db_is_idempotent(self):
        import db as _db
        _db.init_db()
        _db.init_db()  # must not raise

    def test_existing_log_bet_signature_still_works_and_defaults_sport(self):
        import db as _db
        _db.init_db()
        _db.log_bet(
            date="2026-07-09", bet="SF", bet_type="ML", game="SF @ LAD",
            sp="Logan Webb", park="LAD", umpire="",
            bet_odds="+145", model_prob=0.57, market_prob=0.41,
            edge_pct=16.0, conviction="HIGH", stake=10.0,
        )
        bets = _db.get_bets()
        assert len(bets) == 1
        assert bets[0]["sport"] == "MLB"
        assert bets[0]["model_version"] is None

    def test_migration_preserves_existing_bets_row_count(self):
        import db as _db
        _db.init_db()
        _db.log_bet(
            date="2026-07-09", bet="SF", bet_type="ML", game="SF @ LAD",
            sp="Logan Webb", park="LAD", umpire="",
            bet_odds="+145", model_prob=0.57, market_prob=0.41,
            edge_pct=16.0, conviction="HIGH", stake=10.0,
        )
        before = len(_db.get_bets())
        _db.init_db()  # re-run migrations against a DB that already has data
        after = len(_db.get_bets())
        assert before == after == 1
```

- [ ] **Step 10: Run the full test file to verify everything passes**

Run: `python -m pytest test_prediction_logging_schema.py -v`
Expected: all tests PASS (6 so far)

- [ ] **Step 11: Commit**

```bash
git add db.py test_prediction_logging_schema.py
git commit -m "feat: extend bets table + add feature_snapshots/model_versions tables"
```

---

### Task 2: CRUD helpers for `feature_snapshots` and `model_versions`

**Files:**
- Modify: `db.py:397-400` (insert new functions between `get_pick_by_hash()` and `reset_daily_exposure()`)
- Modify: `db.py:4` (`from datetime import datetime` → add `date`)
- Test: `test_prediction_logging_schema.py` (same file, new test classes)

**Interfaces:**
- Consumes: `db._conn()`, `db.log_bet()`, `db.get_bets()`, the `feature_snapshots`/`model_versions` tables from Task 1.
- Produces: `db.insert_feature_snapshot(bet_id: int, feature_name: str, feature_value: float | None = None, feature_value_text: str | None = None, feature_weight: float | None = None) -> None`, `db.get_feature_snapshots(bet_id: int) -> list[dict]`, `db.upsert_model_version(model_name: str, version: str, sport: str = "MLB", feature_list: str | None = None, notes: str | None = None, rolling_roi: float | None = None, rolling_clv: float | None = None) -> None`, `db.get_model_versions(model_name: str | None = None) -> list[dict]`. Not called from `brain.py` yet — that's future bet-flow work, out of scope here.

- [ ] **Step 1: Write the failing tests for `feature_snapshots` helpers**

Append to `test_prediction_logging_schema.py`:

```python
class TestFeatureSnapshotHelpers:
    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._orig_path = os.environ.get("PARLAY_DB", "parlay_os.db")
        os.environ["PARLAY_DB"] = self._tmp.name
        import db as _db
        _db.init_db()
        _db.log_bet(
            date="2026-07-09", bet="SF", bet_type="ML", game="SF @ LAD",
            sp="Logan Webb", park="LAD", umpire="",
            bet_odds="+145", model_prob=0.57, market_prob=0.41,
            edge_pct=16.0, conviction="HIGH", stake=10.0,
        )
        self.bet_id = _db.get_bets()[0]["id"]

    def teardown_method(self):
        os.environ["PARLAY_DB"] = self._orig_path
        self._tmp.close()
        os.unlink(self._tmp.name)

    def test_insert_and_get_numeric_feature_snapshot(self):
        import db as _db
        _db.insert_feature_snapshot(self.bet_id, "edge_pct", feature_value=16.0, feature_weight=1.0)
        rows = _db.get_feature_snapshots(self.bet_id)
        assert len(rows) == 1
        assert rows[0]["feature_name"] == "edge_pct"
        assert rows[0]["feature_value"] == 16.0
        assert rows[0]["feature_value_text"] is None

    def test_insert_and_get_text_feature_snapshot(self):
        import db as _db
        _db.insert_feature_snapshot(self.bet_id, "situations_triggered",
                                     feature_value_text='["home_dog","sharp_signal"]')
        rows = _db.get_feature_snapshots(self.bet_id)
        assert len(rows) == 1
        assert rows[0]["feature_value_text"] == '["home_dog","sharp_signal"]'
        assert rows[0]["feature_value"] is None

    def test_feature_snapshot_requires_existing_bet_id(self):
        import db as _db
        with pytest.raises(sqlite3.IntegrityError):
            _db.insert_feature_snapshot(999999, "edge_pct", feature_value=1.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest test_prediction_logging_schema.py::TestFeatureSnapshotHelpers -v`
Expected: FAIL — `AttributeError: module 'db' has no attribute 'insert_feature_snapshot'`

- [ ] **Step 3: Implement `insert_feature_snapshot` and `get_feature_snapshots`**

In `db.py`, find:

```python
def get_pick_by_hash(verify_hash: str) -> dict | None:
    """Look up a single pick by its SHA256 verification hash."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM bets WHERE verify_hash=?", (verify_hash,)
        ).fetchone()
    return dict(row) if row else None


def reset_daily_exposure(date: str | None = None) -> int:
```

Replace with:

```python
def get_pick_by_hash(verify_hash: str) -> dict | None:
    """Look up a single pick by its SHA256 verification hash."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM bets WHERE verify_hash=?", (verify_hash,)
        ).fetchone()
    return dict(row) if row else None


# ─── FEATURE SNAPSHOTS ──────────────────────────────────────────────────────

def insert_feature_snapshot(bet_id: int, feature_name: str,
                             feature_value: float | None = None,
                             feature_value_text: str | None = None,
                             feature_weight: float | None = None) -> None:
    """One row per named feature behind a pick. Exactly one of
    feature_value / feature_value_text is normally populated — numeric
    features use feature_value, text/JSON features use feature_value_text."""
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO feature_snapshots
              (bet_id, feature_name, feature_value, feature_value_text, feature_weight, created_at)
            VALUES (?,?,?,?,?,?)
        """, (bet_id, feature_name, feature_value, feature_value_text, feature_weight, now))


def get_feature_snapshots(bet_id: int) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM feature_snapshots WHERE bet_id=? ORDER BY id", (bet_id,)
        ).fetchall()
    return [dict(r) for r in rows]


# ─── MODEL VERSIONS ─────────────────────────────────────────────────────────

def upsert_model_version(model_name: str, version: str, sport: str = "MLB",
                          feature_list: str | None = None, notes: str | None = None,
                          rolling_roi: float | None = None,
                          rolling_clv: float | None = None) -> None:
    """Insert a model version row, or update its rolling stats/notes if
    (model_name, version) already exists."""
    today = date.today().isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO model_versions
              (model_name, version, sport, created_date, feature_list, notes, rolling_roi, rolling_clv)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(model_name, version) DO UPDATE SET
              rolling_roi=excluded.rolling_roi,
              rolling_clv=excluded.rolling_clv,
              notes=excluded.notes
        """, (model_name, version, sport, today, feature_list, notes, rolling_roi, rolling_clv))


def get_model_versions(model_name: str | None = None) -> list[dict]:
    with _conn() as conn:
        if model_name:
            rows = conn.execute(
                "SELECT * FROM model_versions WHERE model_name=? ORDER BY created_date DESC",
                (model_name,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM model_versions ORDER BY created_date DESC"
            ).fetchall()
    return [dict(r) for r in rows]


def reset_daily_exposure(date: str | None = None) -> int:
```

Also update the import line near the top of `db.py`:

```python
from datetime import datetime
```

to:

```python
from datetime import datetime, date
```

(Needed for `date.today()` in `upsert_model_version`. Note `reset_daily_exposure`'s own `date` parameter shadows this import inside that function only — harmless, existing code, not touched otherwise.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest test_prediction_logging_schema.py::TestFeatureSnapshotHelpers -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Write the failing tests for `model_versions` helpers**

Append to `test_prediction_logging_schema.py`:

```python
class TestModelVersionHelpers:
    def setup_method(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._orig_path = os.environ.get("PARLAY_DB", "parlay_os.db")
        os.environ["PARLAY_DB"] = self._tmp.name
        import db as _db
        _db.init_db()

    def teardown_method(self):
        os.environ["PARLAY_DB"] = self._orig_path
        self._tmp.close()
        os.unlink(self._tmp.name)

    def test_upsert_model_version_inserts_new_row(self):
        import db as _db
        _db.upsert_model_version("ML_v3", "1.0", feature_list='["k9_trend","xwoba"]')
        rows = _db.get_model_versions("ML_v3")
        assert len(rows) == 1
        assert rows[0]["sport"] == "MLB"
        assert rows[0]["rolling_roi"] is None

    def test_upsert_model_version_updates_existing_row_on_conflict(self):
        import db as _db
        _db.upsert_model_version("ML_v3", "1.0")
        _db.upsert_model_version("ML_v3", "1.0", rolling_roi=4.2, rolling_clv=1.8)
        rows = _db.get_model_versions("ML_v3")
        assert len(rows) == 1, "same (model_name, version) must update, not duplicate"
        assert rows[0]["rolling_roi"] == 4.2
        assert rows[0]["rolling_clv"] == 1.8
```

- [ ] **Step 6: Run test to verify it fails**

Run: `python -m pytest test_prediction_logging_schema.py::TestModelVersionHelpers -v`
Expected: FAIL — `AttributeError: module 'db' has no attribute 'upsert_model_version'`

(This should already pass once Step 3's implementation lands, since both helper pairs were written together — run it to confirm no typos before moving on.)

- [ ] **Step 7: Run test to verify it passes**

Run: `python -m pytest test_prediction_logging_schema.py::TestModelVersionHelpers -v`
Expected: PASS (2 tests)

- [ ] **Step 8: Run the entire test file**

Run: `python -m pytest test_prediction_logging_schema.py -v`
Expected: all 11 tests PASS

- [ ] **Step 9: Run the full existing test suite to confirm no regressions**

Run: `python -m pytest test_fixes.py test_xwoba_pipeline.py test_prediction_logging_schema.py -v`
Expected: all tests PASS — confirms the `bets` extension and new tables didn't break any existing behavior

- [ ] **Step 10: Commit**

```bash
git add db.py test_prediction_logging_schema.py
git commit -m "feat: add feature_snapshots/model_versions CRUD helpers"
```

---

## Note on deviation from the spec's literal test description

The design spec (`docs/superpowers/specs/2026-07-09-prediction-logging-schema-design.md`, section 6) described testing via "copy `parlay_os.db` to a scratch path, diff row counts." This plan instead uses the isolated-temp-DB pattern already established in `test_fixes.py:124-135` (swap the `PARLAY_DB` env var to a fresh `tempfile`, never touch the real database file at all). This satisfies the same intent — prove no data loss, prove idempotency, prove `log_bet()` is unmodified — while being strictly safer: the real production `parlay_os.db` is never opened by any test in this plan, and it matches the codebase's existing testing convention instead of introducing a new one.

## Out of scope (unchanged from the spec)

Writing to `feature_snapshots` / setting `model_version` from the live bet flow, CLV auto-grading, the `public_record` view, OddsPapi/Kalshi/Postgres work, and logging PROP/PARLAY/NRFI/TOTAL picks at all — all deferred, see the design doc's "Out of scope" section.
