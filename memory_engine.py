"""PARLAY OS — memory_engine.py
Player/team/model performance memory. Stores patterns in DB and adjusts priors.
Auto-calibrates model after 50 resolved bets.
"""

import json
import sqlite3
from datetime import date, timedelta
from math_engine import american_to_decimal

DB_PATH = "parlay_os.db"


def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── SCHEMA INIT ───────────────────────────────────────────────────────────────

def init_memory_tables():
    with _conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS player_memory (
            player_name TEXT NOT NULL,
            stat_type   TEXT NOT NULL,  -- 'k_prop','nrfi','ml'
            date        TEXT NOT NULL,
            model_prob  REAL,
            actual      INTEGER,        -- 1=yes/over, 0=no/under
            notes       TEXT,
            PRIMARY KEY (player_name, stat_type, date)
        );
        CREATE TABLE IF NOT EXISTS team_memory (
            team        TEXT NOT NULL,
            context     TEXT NOT NULL,  -- 'home','away','vs_lhp','vs_rhp','bullpen_tired'
            date        TEXT NOT NULL,
            model_prob  REAL,
            actual      INTEGER,
            PRIMARY KEY (team, context, date)
        );
        CREATE TABLE IF NOT EXISTS model_calibration (
            bucket_lo   REAL NOT NULL,
            bucket_hi   REAL NOT NULL,
            hits        INTEGER DEFAULT 0,
            total       INTEGER DEFAULT 0,
            PRIMARY KEY (bucket_lo, bucket_hi)
        );
        CREATE TABLE IF NOT EXISTS live_bet_memory (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id     TEXT,
            team        TEXT,
            bet_time    TEXT,
            entry_odds  TEXT,
            gate_score  INTEGER,
            outcome     INTEGER  -- 1=W, 0=L, NULL=pending
        );
        """)
        # Seed calibration buckets
        buckets = [(i/20, (i+1)/20) for i in range(20)]
        for lo, hi in buckets:
            conn.execute(
                "INSERT OR IGNORE INTO model_calibration (bucket_lo, bucket_hi) VALUES (?,?)",
                (round(lo, 2), round(hi, 2))
            )


# ── RECORD OUTCOMES ──────────────────────────────────────────────────────────

def record_player_result(player_name: str, stat_type: str, game_date: str,
                          model_prob: float, actual: int, notes: str = ""):
    with _conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO player_memory
            (player_name, stat_type, date, model_prob, actual, notes)
            VALUES (?,?,?,?,?,?)
        """, (player_name, stat_type, game_date, model_prob, actual, notes))
        _update_calibration(conn, model_prob, actual)


def record_team_result(team: str, context: str, game_date: str,
                        model_prob: float, actual: int):
    with _conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO team_memory
            (team, context, date, model_prob, actual)
            VALUES (?,?,?,?,?)
        """, (team, context, game_date, model_prob, actual))
        _update_calibration(conn, model_prob, actual)


def record_live_bet(game_id: str, team: str, bet_time: str,
                     entry_odds: str, gate_score: int):
    with _conn() as conn:
        conn.execute("""
            INSERT INTO live_bet_memory
            (game_id, team, bet_time, entry_odds, gate_score)
            VALUES (?,?,?,?,?)
        """, (game_id, team, bet_time, entry_odds, gate_score))


def resolve_live_bet(game_id: str, team: str, outcome: int):
    with _conn() as conn:
        conn.execute("""
            UPDATE live_bet_memory SET outcome=?
            WHERE game_id=? AND team=? AND outcome IS NULL
        """, (outcome, game_id, team))


# ── CALIBRATION ───────────────────────────────────────────────────────────────

def _update_calibration(conn, model_prob: float, actual: int):
    if model_prob is None:
        return
    lo = round(int(model_prob * 20) / 20, 2)
    hi = round(lo + 0.05, 2)
    conn.execute("""
        UPDATE model_calibration
        SET total = total + 1,
            hits  = hits + ?
        WHERE bucket_lo=? AND bucket_hi=?
    """, (actual, lo, hi))


def calibration_summary() -> list[dict]:
    """Return list of calibration buckets with hit rate."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT bucket_lo, bucket_hi, hits, total FROM model_calibration ORDER BY bucket_lo"
        ).fetchall()
    result = []
    for r in rows:
        total = r["total"]
        if total == 0:
            continue
        result.append({
            "range":    f"{r['bucket_lo']:.0%}–{r['bucket_hi']:.0%}",
            "hits":     r["hits"],
            "total":    total,
            "hit_rate": round(r["hits"] / total, 3),
            "expected": round((r["bucket_lo"] + r["bucket_hi"]) / 2, 3),
        })
    return result


# ── PLAYER / TEAM PRIORS ─────────────────────────────────────────────────────

def player_prior(player_name: str, stat_type: str, lookback_days: int = 30) -> float | None:
    """
    Returns empirical hit rate for player/stat over last N days, or None if < 5 samples.
    """
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    with _conn() as conn:
        row = conn.execute("""
            SELECT COUNT(*) as n, SUM(actual) as hits
            FROM player_memory
            WHERE player_name=? AND stat_type=? AND date >= ?
        """, (player_name, stat_type, cutoff)).fetchone()
    n    = row["n"] if row else 0
    hits = row["hits"] or 0
    if n < 5:
        return None
    return round(hits / n, 3)


def team_prior(team: str, context: str, lookback_days: int = 30) -> float | None:
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    with _conn() as conn:
        row = conn.execute("""
            SELECT COUNT(*) as n, SUM(actual) as hits
            FROM team_memory
            WHERE team=? AND context=? AND date >= ?
        """, (team, context, cutoff)).fetchone()
    n    = row["n"] if row else 0
    hits = row["hits"] or 0
    if n < 5:
        return None
    return round(hits / n, 3)


def adjust_model_prob(raw_prob: float, player_name: str = None, team: str = None,
                       stat_type: str = None, context: str = None,
                       prior_weight: float = 0.20) -> float:
    """
    Bayesian blend: raw model + empirical prior.
    prior_weight = how much to weight the historical hit rate vs model.
    Returns adjusted probability.
    """
    prior = None
    if player_name and stat_type:
        prior = player_prior(player_name, stat_type)
    elif team and context:
        prior = team_prior(team, context)

    if prior is None:
        return raw_prob

    # Weighted blend
    adj = (1 - prior_weight) * raw_prob + prior_weight * prior
    return round(adj, 4)


# ── AUTO-CALIBRATION (runs after 50 resolved bets) ───────────────────────────

def should_recalibrate() -> bool:
    """True if we have at least 50 resolved bets and calibration is stale."""
    with _conn() as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM model_calibration WHERE total > 0"
        ).fetchone()[0]
        total_samples = conn.execute(
            "SELECT SUM(total) FROM model_calibration"
        ).fetchone()[0] or 0
    return total_samples >= 50


def calibration_multiplier(model_prob: float) -> float:
    """
    Lookup the calibration bucket for a probability and return
    a multiplier: actual_hit_rate / expected_midpoint.
    Returns 1.0 if uncalibrated.
    """
    lo = round(int(model_prob * 20) / 20, 2)
    hi = round(lo + 0.05, 2)
    with _conn() as conn:
        row = conn.execute("""
            SELECT hits, total FROM model_calibration
            WHERE bucket_lo=? AND bucket_hi=?
        """, (lo, hi)).fetchone()
    if not row or row["total"] < 10:
        return 1.0
    expected_mid = (lo + hi) / 2
    actual_rate  = row["hits"] / row["total"]
    if expected_mid <= 0:
        return 1.0
    return round(actual_rate / expected_mid, 4)


def recalibrate_model_prob(raw_prob: float) -> float:
    """Apply calibration multiplier if enough data, else return raw."""
    if not should_recalibrate():
        return raw_prob
    mult = calibration_multiplier(raw_prob)
    # Don't allow extreme adjustments (cap at 30% shift)
    mult = max(0.70, min(mult, 1.30))
    return round(min(raw_prob * mult, 0.95), 4)


# ── MEMORY REPORT ─────────────────────────────────────────────────────────────

def memory_report() -> dict:
    """Summary of memory state for dashboard / brain.py."""
    cal = calibration_summary()
    overcal = [b for b in cal if b["hit_rate"] < b["expected"] - 0.08]
    undercal = [b for b in cal if b["hit_rate"] > b["expected"] + 0.08]

    with _conn() as conn:
        n_player = conn.execute("SELECT COUNT(*) FROM player_memory").fetchone()[0]
        n_team   = conn.execute("SELECT COUNT(*) FROM team_memory").fetchone()[0]
        n_live   = conn.execute(
            "SELECT COUNT(*) FROM live_bet_memory WHERE outcome IS NOT NULL"
        ).fetchone()[0]
        live_wins = conn.execute(
            "SELECT SUM(outcome) FROM live_bet_memory WHERE outcome IS NOT NULL"
        ).fetchone()[0] or 0

    return {
        "calibration_buckets": len(cal),
        "overconfident_buckets": len(overcal),
        "underconfident_buckets": len(undercal),
        "player_records": n_player,
        "team_records":   n_team,
        "live_bets_resolved": n_live,
        "live_win_rate":  round(live_wins / n_live, 3) if n_live else None,
        "ready_to_recalibrate": should_recalibrate(),
    }


if __name__ == "__main__":
    init_memory_tables()
    print("Memory tables initialized")
    print(memory_report())
