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


def _repair_win_profit():
    """Recalculate profit for all W bets using correct formula: (dec-1)*stake."""
    from math_engine import american_to_decimal
    with _conn() as conn:
        rows = conn.execute(
            "SELECT id, stake, bet_odds FROM bets WHERE result='W'"
        ).fetchall()
        updates = []
        for r in rows:
            dec = american_to_decimal(str(r["bet_odds"] or ""))
            if dec and dec > 1:
                updates.append((round((dec - 1) * float(r["stake"] or 0), 2), r["id"]))
        if updates:
            conn.executemany("UPDATE bets SET profit=? WHERE id=?", updates)


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

        CREATE TABLE IF NOT EXISTS odds_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT NOT NULL,
            timestamp   TEXT NOT NULL,
            game_id     TEXT NOT NULL,
            game        TEXT,
            sportsbook  TEXT,
            market      TEXT,
            side        TEXT,
            price       REAL
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

        CREATE TABLE IF NOT EXISTS umpire_stats (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            name          TEXT NOT NULL UNIQUE,
            games         INTEGER DEFAULT 0,
            home_win_rate REAL,
            avg_runs      REAL,
            k_rate        REAL,
            over_rate     REAL,
            updated_date  TEXT
        );

        CREATE TABLE IF NOT EXISTS betting_patterns (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_key     TEXT NOT NULL UNIQUE,
            category        TEXT NOT NULL,
            description     TEXT,
            bets_evaluated  INTEGER DEFAULT 0,
            wins            INTEGER DEFAULT 0,
            win_rate        REAL DEFAULT 0.0,
            confidence_adj  REAL DEFAULT 0.0,
            last_updated    TEXT
        );

        CREATE TABLE IF NOT EXISTS confidence_weights (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            trained_at    TEXT NOT NULL,
            n_bets        INTEGER,
            feature_names TEXT,
            coefficients  TEXT,
            intercept     REAL,
            accuracy      REAL,
            training_log  TEXT
        );

        CREATE TABLE IF NOT EXISTS sp_tracker (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date                TEXT NOT NULL,
            game_pk             TEXT NOT NULL,
            away_team           TEXT,
            home_team           TEXT,
            away_sp_id          INTEGER,
            away_sp_name        TEXT,
            away_sp_xfip        REAL,
            home_sp_id          INTEGER,
            home_sp_name        TEXT,
            home_sp_xfip        REAL,
            game_time           TEXT,
            sp_changed          INTEGER DEFAULT 0,
            change_detected_at  TEXT,
            new_away_sp         TEXT,
            new_home_sp         TEXT,
            alert_sent          INTEGER DEFAULT 0,
            UNIQUE(date, game_pk)
        );

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
    # Migrations for existing DBs that predate schema additions
    with _conn() as conn:
        for ddl in [
            "ALTER TABLE bets ADD COLUMN verify_hash TEXT",
            "ALTER TABLE bets ADD COLUMN profit REAL",
            "ALTER TABLE bets ADD COLUMN pitch_trap TEXT",
            "ALTER TABLE bets ADD COLUMN framing_edge TEXT",
            "ALTER TABLE bets ADD COLUMN closer_avail TEXT",
            "ALTER TABLE bets ADD COLUMN lineup_slot_score REAL",
            "ALTER TABLE bets ADD COLUMN sharp_signal TEXT",
            "ALTER TABLE bets ADD COLUMN umpire_edge TEXT",
            "ALTER TABLE bets ADD COLUMN home_dog_angle INTEGER",
            "ALTER TABLE bets ADD COLUMN first_pitch_strike_rate REAL",
            "ALTER TABLE bets ADD COLUMN sp_gb_rate REAL",
            # Priority 10: new columns for situations + confidence engine
            "ALTER TABLE bets ADD COLUMN situations_triggered TEXT",
            "ALTER TABLE bets ADD COLUMN abs_score REAL",
            "ALTER TABLE bets ADD COLUMN sharp_checklist_results TEXT",
            "ALTER TABLE bets ADD COLUMN confidence_engine_score INTEGER",
            # line_history: signal_type column (also added lazily by LME)
            "ALTER TABLE line_history ADD COLUMN signal_type TEXT",
            # Prediction logging schema (2026-07-09 design doc) — schema only,
            # not wired into the bet flow yet.
            "ALTER TABLE bets ADD COLUMN sport TEXT DEFAULT 'MLB'",
            "ALTER TABLE bets ADD COLUMN model_version TEXT",
            "ALTER TABLE bets ADD COLUMN reasoning_text TEXT",
            "ALTER TABLE bets ADD COLUMN kalshi_price REAL",
            "ALTER TABLE bets ADD COLUMN kalshi_liquidity_ok INTEGER",
            "ALTER TABLE bets ADD COLUMN roi REAL",
            "ALTER TABLE bets ADD COLUMN graded_at TEXT",
        ]:
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError:
                pass  # column already exists
        # Zero out pending bets' profit placeholder only — wins are repaired below
        conn.execute("""
            UPDATE bets SET profit = 0
            WHERE (profit IS NULL OR profit = 0) AND result IS NULL
        """)
        # L bets: profit = -stake (correct formula, idempotent)
        conn.execute("""
            UPDATE bets SET profit = -stake
            WHERE result = 'L' AND (profit IS NULL OR profit = 0)
        """)
    # W bets require decimal conversion — must be done in Python
    _repair_win_profit()
    _ensure_bets_unique_index()


# ─── BETS ─────────────────────────────────────────────────────────────────────

def log_bet(date, bet, bet_type, game, sp, park, umpire,
            bet_odds, model_prob, market_prob, edge_pct, conviction, stake,
            pitch_trap=None, framing_edge=None, closer_avail=None,
            lineup_slot_score=None, sharp_signal=None, umpire_edge=None,
            home_dog_angle=None, first_pitch_strike_rate=None, sp_gb_rate=None,
            situations_triggered=None, abs_score=None,
            sharp_checklist_results=None, confidence_engine_score=None):
    now = datetime.now(ET).isoformat()
    verify_hash = hashlib.sha256(
        f"{game}|{bet}|{bet_odds}|{now}".encode()
    ).hexdigest()
    with _conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO bets
              (date, timestamp, bet, type, game, sp, park, umpire,
               bet_odds, model_prob, market_prob, edge_pct, conviction, stake, verify_hash,
               pitch_trap, framing_edge, closer_avail, lineup_slot_score,
               sharp_signal, umpire_edge, home_dog_angle,
               first_pitch_strike_rate, sp_gb_rate,
               situations_triggered, abs_score,
               sharp_checklist_results, confidence_engine_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (date, now, bet, bet_type, game, sp, park, umpire,
              bet_odds, model_prob, market_prob, edge_pct, conviction, stake, verify_hash,
              pitch_trap, framing_edge, closer_avail, lineup_slot_score,
              sharp_signal, umpire_edge, home_dog_angle,
              first_pitch_strike_rate, sp_gb_rate,
              situations_triggered, abs_score,
              sharp_checklist_results, confidence_engine_score))


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


def _calc_profit(result: str, stake: float, bet_odds: str) -> float:
    """Compute settled profit: (dec-1)*stake for W, -stake for L, 0 for P."""
    from math_engine import american_to_decimal
    s = float(stake or 0)
    if result == "W":
        dec = american_to_decimal(str(bet_odds or ""))
        return round((dec - 1) * s, 2) if (dec and dec > 1) else 0.0
    if result == "L":
        return round(-s, 2)
    return 0.0


def resolve_bet_by_id(bet_id: int, closing_odds: str, result: str,
                       game_score: str, notes: str = ""):
    """Settle a specific bet by primary key — used by auto-settler."""
    clv = None
    profit = None
    with _conn() as c:
        row = c.execute("SELECT bet_odds, stake FROM bets WHERE id=?", (bet_id,)).fetchone()
    if row:
        if closing_odds:
            try:
                from math_engine import calc_clv
                clv = calc_clv(row["bet_odds"], closing_odds).get("clv_pct")
            except Exception:
                pass
        profit = _calc_profit(result, row["stake"], row["bet_odds"])
    with _conn() as conn:
        conn.execute("""
            UPDATE bets SET closing_odds=?, clv_pct=?, result=?, game_score=?, notes=?, profit=?
            WHERE id=? AND result IS NULL
        """, (closing_odds, clv, result, game_score, notes, profit, bet_id))


def resolve_bet(bet, date, closing_odds, result, game_score, notes=""):
    clv = None
    profit = None
    with _conn() as c:
        row = c.execute(
            "SELECT bet_odds, stake FROM bets WHERE bet=? AND date=? AND result IS NULL LIMIT 1",
            (bet, date)).fetchone()
    if row:
        if closing_odds:
            try:
                from math_engine import calc_clv
                clv = calc_clv(row["bet_odds"], closing_odds).get("clv_pct")
            except Exception:
                pass
        profit = _calc_profit(result, row["stake"], row["bet_odds"])
    with _conn() as conn:
        conn.execute("""
            UPDATE bets SET closing_odds=?, clv_pct=?, result=?, game_score=?, notes=?, profit=?
            WHERE bet=? AND date=? AND result IS NULL
        """, (closing_odds, clv, result, game_score, notes, profit, bet, date))


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


def clv_log_exists(date: str, bet: str, bet_type: str) -> bool:
    """True if a pre-game line has already been captured for this bet today
    — lets a recurring capture timer skip bets it's already snapshotted
    (TIER 3 WIRE-IN 4) instead of writing a duplicate row every tick."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM clv_log WHERE date=? AND bet=? AND type=? LIMIT 1",
            (date, bet, bet_type),
        ).fetchone()
    return row is not None


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


# ─── ODDS HISTORY ─────────────────────────────────────────────────────────────
# Full per-run odds snapshots (game/sportsbook/market/side/price/timestamp).
# Append-only — never overwrite a prior snapshot. Distinct from line_history
# (ML-only, consumed by line_movement_engine) so that existing consumer is
# untouched; this table is for CLV/market-analysis work that needs full
# market+sportsbook granularity.

def save_odds_snapshot(date: str, game_id: str, game: str, sportsbook: str,
                       market: str, side: str, price: float) -> None:
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO odds_history
              (date, timestamp, game_id, game, sportsbook, market, side, price)
            VALUES (?,?,?,?,?,?,?,?)
        """, (date, now, game_id, game, sportsbook, market, side, price))


def get_odds_history(game_id: str) -> list:
    """Return all odds snapshots for a game, oldest first."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT * FROM odds_history
            WHERE game_id=?
            ORDER BY timestamp ASC
        """, (game_id,)).fetchall()
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


# ─── UMPIRE STATS ─────────────────────────────────────────────────────────────

def upsert_umpire_stats(stats: dict) -> None:
    """Insert or update one umpire's stats row."""
    with _conn() as conn:
        conn.execute("""
            INSERT INTO umpire_stats
              (name, games, home_win_rate, avg_runs, k_rate, over_rate, updated_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
              games         = excluded.games,
              home_win_rate = excluded.home_win_rate,
              avg_runs      = excluded.avg_runs,
              k_rate        = excluded.k_rate,
              over_rate     = excluded.over_rate,
              updated_date  = excluded.updated_date
        """, (
            stats.get("name"), stats.get("games", 0),
            stats.get("home_win_rate"), stats.get("avg_runs"),
            stats.get("k_rate"), stats.get("over_rate"),
            stats.get("updated_date"),
        ))


def get_all_umpire_stats() -> dict:
    """Return all umpire stats as {name: stats_dict}."""
    with _conn() as conn:
        rows = conn.execute("SELECT * FROM umpire_stats").fetchall()
    return {row["name"]: dict(row) for row in rows}


def get_umpire_stat(name: str) -> dict | None:
    """Return stats for one umpire by name."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT * FROM umpire_stats WHERE name=?", (name,)
        ).fetchone()
    return dict(row) if row else None


# ─── BETTING PATTERNS ──────────────────────────────────────────────────────────

def upsert_betting_pattern(
    pattern_key: str,
    category: str,
    description: str,
    bets_evaluated: int,
    wins: int,
    win_rate: float,
    confidence_adj: float,
) -> None:
    """Upsert one betting pattern row."""
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO betting_patterns
              (pattern_key, category, description, bets_evaluated, wins,
               win_rate, confidence_adj, last_updated)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(pattern_key) DO UPDATE SET
              bets_evaluated = excluded.bets_evaluated,
              wins           = excluded.wins,
              win_rate       = excluded.win_rate,
              confidence_adj = excluded.confidence_adj,
              last_updated   = excluded.last_updated
        """, (pattern_key, category, description, bets_evaluated, wins,
              round(win_rate, 4), round(confidence_adj, 2), now))


def get_all_betting_patterns() -> list[dict]:
    """Return all betting patterns."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM betting_patterns ORDER BY bets_evaluated DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_pattern_confidence_adj(pattern_key: str) -> float:
    """Return confidence adjustment for a specific pattern. 0.0 if not found."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT confidence_adj FROM betting_patterns WHERE pattern_key=?",
            (pattern_key,),
        ).fetchone()
    return float(row["confidence_adj"]) if row else 0.0


# ─── SP TRACKER ───────────────────────────────────────────────────────────────

def upsert_sp_tracker(date: str, game_pk: str, away_team: str, home_team: str,
                      away_sp_id: int, away_sp_name: str, away_sp_xfip: float,
                      home_sp_id: int, home_sp_name: str, home_sp_xfip: float,
                      game_time: str) -> None:
    with _conn() as conn:
        conn.execute("""
            INSERT INTO sp_tracker
              (date, game_pk, away_team, home_team,
               away_sp_id, away_sp_name, away_sp_xfip,
               home_sp_id, home_sp_name, home_sp_xfip, game_time)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date, game_pk) DO UPDATE SET
              away_team    = excluded.away_team,
              home_team    = excluded.home_team,
              away_sp_id   = excluded.away_sp_id,
              away_sp_name = excluded.away_sp_name,
              away_sp_xfip = excluded.away_sp_xfip,
              home_sp_id   = excluded.home_sp_id,
              home_sp_name = excluded.home_sp_name,
              home_sp_xfip = excluded.home_sp_xfip,
              game_time    = excluded.game_time
        """, (date, str(game_pk), away_team, home_team,
              away_sp_id, away_sp_name, away_sp_xfip,
              home_sp_id, home_sp_name, home_sp_xfip, game_time))


def get_sp_tracker(date: str) -> list:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM sp_tracker WHERE date=? ORDER BY game_time", (date,)
        ).fetchall()
    return [dict(r) for r in rows]


def mark_sp_changed(date: str, game_pk: str, new_away_sp: str, new_home_sp: str) -> None:
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            UPDATE sp_tracker
            SET sp_changed=1, change_detected_at=?, new_away_sp=?, new_home_sp=?
            WHERE date=? AND game_pk=?
        """, (now, new_away_sp, new_home_sp, date, str(game_pk)))


def mark_sp_alert_sent(date: str, game_pk: str) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE sp_tracker SET alert_sent=1 WHERE date=? AND game_pk=?",
            (date, str(game_pk)),
        )


# ─── LINEUP TRACKER ───────────────────────────────────────────────────────────

def upsert_lineup_tracker(date: str, game_pk: str, team: str,
                          projected_lineup: list) -> None:
    lineup_json = json.dumps(projected_lineup)
    with _conn() as conn:
        conn.execute("""
            INSERT INTO lineup_tracker (date, game_pk, team, projected_lineup)
            VALUES (?,?,?,?)
            ON CONFLICT(date, team) DO UPDATE SET
              game_pk          = excluded.game_pk,
              projected_lineup = excluded.projected_lineup
        """, (date, str(game_pk), team, lineup_json))


def get_lineup_tracker(date: str) -> list:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM lineup_tracker WHERE date=?", (date,)
        ).fetchall()
    return [dict(r) for r in rows]


def mark_lineup_alert_sent(date: str, team: str, changes_json: str) -> None:
    with _conn() as conn:
        conn.execute("""
            UPDATE lineup_tracker
            SET alert_sent=1, changes_detected=?
            WHERE date=? AND team=?
        """, (changes_json, date, team))


def update_confirmed_lineup(date: str, team: str, confirmed_lineup: list) -> None:
    with _conn() as conn:
        conn.execute("""
            UPDATE lineup_tracker
            SET confirmed_lineup=?
            WHERE date=? AND team=?
        """, (json.dumps(confirmed_lineup), date, team))


# Initialize on import
try:
    init_db()
except Exception as e:
    print(f"DB init warning: {e}")
