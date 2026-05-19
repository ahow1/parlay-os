"""PARLAY OS — SQLite database layer (items 11, 10, 7)."""

import sqlite3, json, os, shutil, time, glob, hashlib
from datetime import datetime
import pytz

BACKUP_DIR  = "backups"
MAX_BACKUPS = 7

DB_PATH = os.environ.get("PARLAY_DB", "parlay_os.db")
ET = pytz.timezone("America/New_York")


def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _conn_with_retry(retries: int = 3, delay: float = 1.0):
    """Open connection; retry on OperationalError (database locked)."""
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return _conn()
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay)
                last_exc = e
            else:
                raise
    raise last_exc  # type: ignore


def check_integrity() -> bool:
    """Return True if DB passes integrity check. Called on startup."""
    try:
        conn = sqlite3.connect(DB_PATH)
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
        conn.close()
        if result != "ok":
            print(f"DB integrity check FAILED: {result}")
            _restore_from_backup()
            return False
        return True
    except Exception as e:
        print(f"DB integrity check failed: {e}")
        return False


def _restore_from_backup():
    """Attempt to restore DB from most recent backup."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    backups = sorted(glob.glob(f"{BACKUP_DIR}/parlay_os_*.db"), reverse=True)
    if not backups:
        print("[DB] No backup found — cannot restore")
        return
    latest = backups[0]
    print(f"[DB] Restoring from backup: {latest}")
    shutil.copy2(latest, DB_PATH)
    print("[DB] Restore complete")


def backup_database() -> str | None:
    """
    Copy DB to backups/parlay_os_YYYY-MM-DD.db.
    Keeps last MAX_BACKUPS daily backups. Returns backup path on success.
    """
    os.makedirs(BACKUP_DIR, exist_ok=True)
    today   = datetime.now(ET).strftime("%Y-%m-%d")
    dest    = f"{BACKUP_DIR}/parlay_os_{today}.db"
    try:
        shutil.copy2(DB_PATH, dest)
        # Prune old backups — keep newest MAX_BACKUPS only
        all_backups = sorted(glob.glob(f"{BACKUP_DIR}/parlay_os_*.db"), reverse=True)
        for old in all_backups[MAX_BACKUPS:]:
            try:
                os.remove(old)
            except OSError:
                pass
        return dest
    except Exception as e:
        print(f"[DB] Backup failed: {e}")
        return None


def _ensure_bets_unique_index():
    """Remove duplicate bets (keep earliest id per date+game+bet+type) then create unique index."""
    with _conn() as conn:
        conn.execute("""
            DELETE FROM bets WHERE id NOT IN (
                SELECT MIN(id) FROM bets GROUP BY date, game, bet, type
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_bets_no_dup
            ON bets(date, game, bet, type)
        """)


def init_db():
    with _conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS bets (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            date         TEXT NOT NULL,
            timestamp    TEXT NOT NULL,
            bet          TEXT NOT NULL,
            type         TEXT,
            game         TEXT,
            sp           TEXT,
            park         TEXT,
            umpire       TEXT,
            bet_odds     TEXT,
            model_prob   REAL,
            market_prob  REAL,
            edge_pct     REAL,
            conviction   TEXT,
            stake        REAL,
            closing_odds TEXT,
            clv_pct      REAL,
            result       TEXT,
            game_score   TEXT,
            notes        TEXT,
            verify_hash  TEXT,
            profit       REAL
        );

        CREATE TABLE IF NOT EXISTS bankroll_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            date         TEXT NOT NULL,
            timestamp    TEXT NOT NULL,
            opening      REAL,
            closing      REAL,
            peak         REAL,
            day_pnl      REAL,
            week_pnl     REAL,
            sessions     INTEGER,
            total_wagered REAL,
            status       TEXT
        );

        CREATE TABLE IF NOT EXISTS scout_runs (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT NOT NULL,
            timestamp      TEXT NOT NULL,
            games_analyzed INTEGER,
            high_count     INTEGER,
            medium_count   INTEGER,
            pass_count     INTEGER,
            avg_edge       REAL,
            data_json      TEXT
        );

        CREATE TABLE IF NOT EXISTS calibration_buckets (
            bucket        TEXT PRIMARY KEY,
            predicted_min REAL,
            predicted_max REAL,
            total_bets    INTEGER DEFAULT 0,
            wins          INTEGER DEFAULT 0,
            weight_adj    REAL DEFAULT 1.0,
            last_updated  TEXT
        );

        CREATE TABLE IF NOT EXISTS clv_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            date         TEXT NOT NULL,
            bet          TEXT NOT NULL,
            type         TEXT,
            game         TEXT,
            sp           TEXT,
            park         TEXT,
            umpire       TEXT,
            bet_odds     TEXT,
            closing_odds TEXT,
            clv_pct      REAL,
            result       TEXT,
            model        TEXT,
            edge_pct     REAL
        );

        CREATE TABLE IF NOT EXISTS scout_output (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL UNIQUE,
            timestamp   TEXT NOT NULL,
            scout_json  TEXT,
            props_json  TEXT
        );

        CREATE TABLE IF NOT EXISTS line_history (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp  TEXT NOT NULL,
            game_id    TEXT NOT NULL,
            away_team  TEXT,
            home_team  TEXT,
            away_ml    INTEGER,
            home_ml    INTEGER,
            game_date  TEXT
        );

        CREATE TABLE IF NOT EXISTS prop_results (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            date         TEXT NOT NULL,
            timestamp    TEXT NOT NULL,
            player       TEXT,
            team         TEXT,
            prop_type    TEXT,
            line         REAL,
            direction    TEXT,
            projected    REAL,
            confidence   INTEGER,
            stake        REAL,
            result       TEXT,
            actual_value REAL,
            edge_pct     REAL,
            notes        TEXT
        );
        """)
    # Migrations for existing DBs that predate schema additions
    with _conn() as conn:
        for ddl in [
            "ALTER TABLE bets ADD COLUMN verify_hash TEXT",
            "ALTER TABLE bets ADD COLUMN profit REAL",
            "ALTER TABLE bets ADD COLUMN pitch_trap TEXT",
            "ALTER TABLE bets ADD COLUMN framing_edge TEXT",
            "ALTER TABLE bets ADD COLUMN closer_avail TEXT",
            "ALTER TABLE bets ADD COLUMN lineup_slot_score REAL",
        ]:
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError:
                pass  # column already exists
        conn.execute("""
            UPDATE bets
            SET profit = CASE
                WHEN result='win'  THEN stake
                WHEN result='loss' THEN -stake
                ELSE 0
            END
            WHERE profit IS NULL OR profit = 0
        """)
    _ensure_bets_unique_index()


# ─── BETS ─────────────────────────────────────────────────────────────────────

def log_bet(date, bet, bet_type, game, sp, park, umpire,
            bet_odds, model_prob, market_prob, edge_pct, conviction, stake,
            pitch_trap=None, framing_edge=None, closer_avail=None,
            lineup_slot_score=None):
    now = datetime.now(ET).isoformat()
    verify_hash = hashlib.sha256(
        f"{game}|{bet}|{bet_odds}|{now}".encode()
    ).hexdigest()
    with _conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO bets
              (date, timestamp, bet, type, game, sp, park, umpire,
               bet_odds, model_prob, market_prob, edge_pct, conviction, stake, verify_hash,
               pitch_trap, framing_edge, closer_avail, lineup_slot_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (date, now, bet, bet_type, game, sp, park, umpire,
              bet_odds, model_prob, market_prob, edge_pct, conviction, stake, verify_hash,
              pitch_trap, framing_edge, closer_avail, lineup_slot_score))


def get_pick_by_hash(verify_hash: str) -> dict | None:
    """Look up a single pick by its SHA256 verification hash."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM bets WHERE verify_hash=?", (verify_hash,)
        ).fetchone()
    return dict(row) if row else None


def reset_daily_exposure(date: str | None = None) -> int:
    """
    Delete all pending (unsettled) bets for the given date.
    Defaults to today. Returns the number of rows deleted.
    Used by /resetcap when the daily cap needs to be cleared mid-day.
    """
    if date is None:
        from datetime import date as _date
        date = _date.today().isoformat()
    with _conn_with_retry() as conn:
        cur = conn.execute(
            "DELETE FROM bets WHERE date=? AND result IS NULL",
            (date,),
        )
        return cur.rowcount


def update_bet_stake(bet_id: int, new_stake: float):
    """Update stake on a pending bet (used by /update command)."""
    with _conn() as conn:
        conn.execute(
            "UPDATE bets SET stake=? WHERE id=? AND result IS NULL",
            (round(new_stake, 2), bet_id),
        )


def resolve_bet_by_id(bet_id: int, closing_odds: str, result: str,
                       game_score: str, notes: str = ""):
    """Settle a specific bet by primary key — used by auto-settler."""
    clv = None
    if closing_odds:
        try:
            from math_engine import calc_clv
            with _conn() as c:
                row = c.execute("SELECT bet_odds FROM bets WHERE id=?", (bet_id,)).fetchone()
            if row:
                clv = calc_clv(row["bet_odds"], closing_odds).get("clv_pct")
        except Exception:
            pass
    with _conn() as conn:
        conn.execute("""
            UPDATE bets SET closing_odds=?, clv_pct=?, result=?, game_score=?, notes=?
            WHERE id=? AND result IS NULL
        """, (closing_odds, clv, result, game_score, notes, bet_id))


def resolve_bet(bet, date, closing_odds, result, game_score, notes=""):
    clv = None
    if closing_odds:
        try:
            from math_engine import calc_clv
            with _conn() as c:
                row = c.execute(
                    "SELECT bet_odds FROM bets WHERE bet=? AND date=? LIMIT 1",
                    (bet, date)).fetchone()
            if row:
                clv = calc_clv(row["bet_odds"], closing_odds).get("clv_pct")
        except Exception:
            pass
    with _conn() as conn:
        conn.execute("""
            UPDATE bets SET closing_odds=?, clv_pct=?, result=?, game_score=?, notes=?
            WHERE bet=? AND date=? AND result IS NULL
        """, (closing_odds, clv, result, game_score, notes, bet, date))


def get_bets(date=None, unresolved_only=False):
    q = "SELECT * FROM bets"
    params, where = [], []
    if date:
        where.append("date=?"); params.append(date)
    if unresolved_only:
        where.append("result IS NULL")
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY timestamp DESC"
    with _conn() as conn:
        return [dict(r) for r in conn.execute(q, params)]


# ─── BANKROLL ─────────────────────────────────────────────────────────────────

def save_bankroll_snapshot(date, opening, closing, peak, day_pnl, week_pnl,
                           sessions, total_wagered, status):
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO bankroll_log
              (date, timestamp, opening, closing, peak, day_pnl, week_pnl,
               sessions, total_wagered, status)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (date, now, opening, closing, peak, day_pnl, week_pnl,
              sessions, total_wagered, status))


def get_bankroll_history(days=30):
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM bankroll_log ORDER BY date DESC LIMIT ?", (days,))
        return [dict(r) for r in rows]


# ─── CLV LOG ──────────────────────────────────────────────────────────────────

def log_clv(date, bet, bet_type, game, sp, park, umpire,
            bet_odds, closing_odds, clv_pct, result, model, edge_pct):
    with _conn() as conn:
        conn.execute("""
            INSERT INTO clv_log
              (date, bet, type, game, sp, park, umpire, bet_odds,
               closing_odds, clv_pct, result, model, edge_pct)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (date, bet, bet_type, game, sp, park, umpire, bet_odds,
              closing_odds, clv_pct, result, model, edge_pct))


def get_clv_log(days=30):
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM clv_log ORDER BY date DESC LIMIT ?", (days * 10,))
        return [dict(r) for r in rows]


# ─── CALIBRATION ──────────────────────────────────────────────────────────────

def update_calibration(bucket, win):
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO calibration_buckets
              (bucket, predicted_min, predicted_max, total_bets, wins, last_updated)
            VALUES (?,0,0,1,?,?)
            ON CONFLICT(bucket) DO UPDATE SET
              total_bets=total_bets+1,
              wins=wins+?,
              last_updated=?
        """, (bucket, 1 if win else 0, now, 1 if win else 0, now))


def get_calibration():
    with _conn() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM calibration_buckets ORDER BY bucket")]


def set_weight_adj(bucket, adj):
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            UPDATE calibration_buckets SET weight_adj=?, last_updated=?
            WHERE bucket=?
        """, (adj, now, bucket))


# ─── SCOUT RUNS ───────────────────────────────────────────────────────────────

def save_scout_run(date, games_analyzed, high_count, medium_count, pass_count,
                   avg_edge, data_json_str):
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO scout_runs
              (date, timestamp, games_analyzed, high_count, medium_count,
               pass_count, avg_edge, data_json)
            VALUES (?,?,?,?,?,?,?,?)
        """, (date, now, games_analyzed, high_count, medium_count,
              pass_count, avg_edge, data_json_str))


def log_scout_run(date, bets_found, data_json_str):
    """Simplified scout run logger called by brain.py."""
    save_scout_run(date, 0, 0, 0, 0, 0.0, data_json_str)


# ─── SCOUT OUTPUT ──────────────────────────────────────────────────────────────

def save_scout_output(date: str, scout_json_str: str, props_json_str: str) -> None:
    """Upsert today's full scout + props JSON into the DB for cross-env access."""
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO scout_output (date, timestamp, scout_json, props_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                timestamp  = excluded.timestamp,
                scout_json = excluded.scout_json,
                props_json = excluded.props_json
        """, (date, now, scout_json_str, props_json_str))


def get_latest_scout_output(date: str | None = None) -> dict | None:
    """Return the most recent scout_output row, optionally filtered to a date."""
    with _conn() as conn:
        if date:
            row = conn.execute(
                "SELECT * FROM scout_output WHERE date=? ORDER BY timestamp DESC LIMIT 1",
                (date,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM scout_output ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
    return dict(row) if row else None


# ─── ROI REPORTS ──────────────────────────────────────────────────────────────

def get_roi_by_type():
    with _conn() as conn:
        rows = conn.execute("""
            SELECT type,
                   COUNT(*) as total,
                   SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) as wins,
                   SUM(CASE WHEN result='L' THEN 1 ELSE 0 END) as losses,
                   AVG(clv_pct) as avg_clv,
                   SUM(stake) as total_staked
            FROM bets WHERE result IN ('W','L','P')
            GROUP BY type
        """)
        return [dict(r) for r in rows]


def get_roi_by_sp():
    with _conn() as conn:
        rows = conn.execute("""
            SELECT sp, COUNT(*) as total,
                   SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) as wins,
                   AVG(clv_pct) as avg_clv
            FROM bets WHERE result IN ('W','L','P') AND sp IS NOT NULL AND sp!=''
            GROUP BY sp ORDER BY total DESC LIMIT 20
        """)
        return [dict(r) for r in rows]


def get_roi_by_park():
    with _conn() as conn:
        rows = conn.execute("""
            SELECT park, COUNT(*) as total,
                   SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) as wins,
                   AVG(clv_pct) as avg_clv
            FROM bets WHERE result IN ('W','L','P') AND park IS NOT NULL AND park!=''
            GROUP BY park ORDER BY total DESC
        """)
        return [dict(r) for r in rows]


def get_roi_by_umpire():
    with _conn() as conn:
        rows = conn.execute("""
            SELECT umpire, COUNT(*) as total,
                   SUM(CASE WHEN result='W' THEN 1 ELSE 0 END) as wins,
                   AVG(clv_pct) as avg_clv
            FROM bets WHERE result IN ('W','L','P') AND umpire IS NOT NULL AND umpire!=''
            GROUP BY umpire ORDER BY total DESC LIMIT 20
        """)
        return [dict(r) for r in rows]


# ─── LINE HISTORY ─────────────────────────────────────────────────────────────

def log_line_snapshot(game_id: str, away_team: str, home_team: str,
                      away_ml, home_ml, game_date: str) -> None:
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO line_history
              (timestamp, game_id, away_team, home_team, away_ml, home_ml, game_date)
            VALUES (?,?,?,?,?,?,?)
        """, (now, game_id, away_team, home_team, away_ml, home_ml, game_date))


def get_line_history(game_id: str, hours_back: int = 4) -> list:
    """Return line snapshots for a game ordered newest-first."""
    cutoff = (datetime.now(ET) - __import__("datetime").timedelta(hours=hours_back)).isoformat()
    with _conn() as conn:
        rows = conn.execute("""
            SELECT * FROM line_history
            WHERE game_id=? AND timestamp >= ?
            ORDER BY timestamp DESC
        """, (game_id, cutoff)).fetchall()
    return [dict(r) for r in rows]


# ─── PROP RESULTS ─────────────────────────────────────────────────────────────

def log_prop_result(date: str, player: str, team: str, prop_type: str,
                    line: float, direction: str, projected: float,
                    confidence: int, stake: float, edge_pct: float,
                    notes: str = "") -> None:
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO prop_results
              (date, timestamp, player, team, prop_type, line, direction,
               projected, confidence, stake, edge_pct, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (date, now, player, team, prop_type, line, direction,
              projected, confidence, stake, edge_pct, notes))


def settle_prop(prop_id: int, result: str, actual_value: float) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE prop_results SET result=?, actual_value=? WHERE id=?",
            (result, actual_value, prop_id),
        )


def get_prop_accuracy(prop_type: str = None, days: int = 30) -> list:
    """Return settled prop results for accuracy analysis."""
    cutoff = (datetime.now(ET) - __import__("datetime").timedelta(days=days)).strftime("%Y-%m-%d")
    q = "SELECT * FROM prop_results WHERE result IS NOT NULL AND date >= ?"
    params: list = [cutoff]
    if prop_type:
        q += " AND prop_type=?"
        params.append(prop_type)
    q += " ORDER BY date DESC"
    with _conn() as conn:
        return [dict(r) for r in conn.execute(q, params)]


# Initialize on import
try:
    init_db()
except Exception as e:
    print(f"DB init warning: {e}")
