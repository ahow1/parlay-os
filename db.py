"""PARLAY OS — SQLite database layer (items 11, 10, 7)."""

import sqlite3, json, os
from datetime import datetime
import pytz

DB_PATH = os.environ.get("PARLAY_DB", "parlay_os.db")
ET = pytz.timezone("America/New_York")


def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
            notes        TEXT
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
        """)


# ─── BETS ─────────────────────────────────────────────────────────────────────

def log_bet(date, bet, bet_type, game, sp, park, umpire,
            bet_odds, model_prob, market_prob, edge_pct, conviction, stake):
    now = datetime.now(ET).isoformat()
    with _conn() as conn:
        conn.execute("""
            INSERT INTO bets
              (date, timestamp, bet, type, game, sp, park, umpire,
               bet_odds, model_prob, market_prob, edge_pct, conviction, stake)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (date, now, bet, bet_type, game, sp, park, umpire,
              bet_odds, model_prob, market_prob, edge_pct, conviction, stake))


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


# Initialize on import
try:
    init_db()
except Exception as e:
    print(f"DB init warning: {e}")
