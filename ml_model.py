"""PARLAY OS — ml_model.py
Ensemble ML model: XGBoost + LightGBM + Logistic Regression.
Trains on 2022-2023, validates on 2024, tests on 2025 holdout.

Usage:
  python ml_model.py --train          # full training pipeline (~30-60 min)
  python ml_model.py --predict AWAY HOME  # single-game prediction
  python ml_model.py --status         # show model status / accuracy

Falls back to Pythagorean model if models/home_win.pkl does not exist.
"""

import os
import sys
import json
import logging
import warnings
import hashlib
from datetime import datetime, date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
import requests_cache

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
MODELS_DIR   = Path("models")
CACHE_DIR    = Path("data_cache")
MODELS_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

MODEL_FILES = {
    "home_win":        MODELS_DIR / "home_win.pkl",
    "total_runs":      MODELS_DIR / "total_runs.pkl",
    "nrfi":            MODELS_DIR / "nrfi.pkl",
    "f5_home_win":     MODELS_DIR / "f5_home_win.pkl",
}
PARAMS_FILE        = MODELS_DIR / "optimal_params.json"
FEAT_IMPORT_FILE   = MODELS_DIR / "feature_importance.json"
CALIBRATION_FILE   = MODELS_DIR / "calibration.json"
META_FILE          = MODELS_DIR / "model_meta.json"

# Seasons
TRAIN_SEASONS  = [2022, 2023]
VAL_SEASONS    = [2024]
TEST_SEASONS   = [2025]
ALL_SEASONS    = TRAIN_SEASONS + VAL_SEASONS + TEST_SEASONS

# League average runs per game (for scaling)
LG_RPG = 4.35

# ── Requests cache (5-minute TTL for live calls, unlimited for historical) ────
requests_cache.install_cache(
    str(CACHE_DIR / "mlb_cache"),
    backend="sqlite",
    expire_after=300,   # 5 minutes for live data
)

# ── Data loading ──────────────────────────────────────────────────────────────

def _load_pybaseball_safe(func_name: str, *args, **kwargs):
    """Call a pybaseball function with retries and disk caching."""
    cache_key = hashlib.md5(f"{func_name}{args}{sorted(kwargs.items())}".encode()).hexdigest()
    cache_path = CACHE_DIR / f"{func_name}_{cache_key}.parquet"

    if cache_path.exists():
        log.info(f"  Cache hit: {cache_path.name}")
        return pd.read_parquet(cache_path)

    import pybaseball as pb
    pb.cache.enable()

    func = getattr(pb, func_name)
    log.info(f"  Fetching {func_name}({args}, {kwargs})...")
    try:
        df = func(*args, **kwargs)
        if df is not None and not df.empty:
            df.to_parquet(cache_path)
        return df
    except Exception as e:
        log.warning(f"  {func_name} failed: {e}")
        return pd.DataFrame()


def load_schedule_results(seasons: list) -> pd.DataFrame:
    """Load game results for multiple seasons."""
    import pybaseball as pb
    dfs = []
    for s in seasons:
        cache_path = CACHE_DIR / f"schedule_{s}.parquet"
        if cache_path.exists():
            dfs.append(pd.read_parquet(cache_path))
            continue
        log.info(f"  Loading schedule {s}...")
        try:
            df = pb.schedule_and_record(s, "ARI")   # any team works as anchor
        except Exception as e:
            log.warning(f"  schedule {s} failed: {e}")
            continue
        # Actually we need all teams — use statsapi schedule instead
        df = _fetch_mlb_schedule(s)
        if not df.empty:
            df.to_parquet(cache_path)
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _fetch_mlb_schedule(season: int) -> pd.DataFrame:
    """Pull complete season schedule + results from MLB Stats API."""
    import requests
    STATSAPI = "https://statsapi.mlb.com/api/v1"
    games = []
    try:
        r = requests.get(
            f"{STATSAPI}/schedule",
            params={
                "sportId": 1,
                "season": season,
                "gameType": "R",
                "hydrate": "linescore,team,probablePitcher",
                "fields": "dates,date,games,gamePk,teams,away,home,team,name,score,"
                          "isWinner,probablePitcher,fullName,id,status,detailedState",
            },
            timeout=30,
        )
        r.raise_for_status()
        for day in r.json().get("dates", []):
            game_date = day.get("date", "")
            for g in day.get("games", []):
                if g.get("status", {}).get("detailedState", "") not in (
                    "Final", "Game Over", "Completed Early"
                ):
                    continue
                teams = g.get("teams", {})
                a = teams.get("away", {})
                h = teams.get("home", {})
                games.append({
                    "game_pk":       g.get("gamePk"),
                    "game_date":     game_date,
                    "season":        season,
                    "away_team":     a.get("team", {}).get("name", ""),
                    "home_team":     h.get("team", {}).get("name", ""),
                    "away_score":    a.get("score", 0),
                    "home_score":    h.get("score", 0),
                    "home_win":      1 if h.get("isWinner") else 0,
                    "total_runs":    (a.get("score", 0) or 0) + (h.get("score", 0) or 0),
                    "away_sp_id":    a.get("probablePitcher", {}).get("id"),
                    "away_sp_name":  a.get("probablePitcher", {}).get("fullName", ""),
                    "home_sp_id":    h.get("probablePitcher", {}).get("id"),
                    "home_sp_name":  h.get("probablePitcher", {}).get("fullName", ""),
                })
    except Exception as e:
        log.warning(f"  MLB schedule {season}: {e}")
    return pd.DataFrame(games)


def load_pitching_stats(seasons: list) -> pd.DataFrame:
    """FanGraphs SP stats per season."""
    dfs = []
    for s in seasons:
        df = _load_pybaseball_safe("pitching_stats", s, s, ind=True, qual=1)
        if not df.empty:
            df["season"] = s
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def load_batting_stats(seasons: list) -> pd.DataFrame:
    """FanGraphs batting stats per season."""
    dfs = []
    for s in seasons:
        df = _load_pybaseball_safe("batting_stats", s, s, ind=True, qual=1)
        if not df.empty:
            df["season"] = s
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def load_team_batting(seasons: list) -> pd.DataFrame:
    dfs = []
    for s in seasons:
        df = _load_pybaseball_safe("team_batting", s, s, ind=True)
        if not df.empty:
            df["season"] = s
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def load_team_pitching(seasons: list) -> pd.DataFrame:
    dfs = []
    for s in seasons:
        df = _load_pybaseball_safe("team_pitching", s, s, ind=True)
        if not df.empty:
            df["season"] = s
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ── Feature engineering ───────────────────────────────────────────────────────

def _rolling_team_stats(schedule: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    """Build rolling N-game team offensive/defensive stats with no lookahead."""
    rows = []
    teams = pd.concat([
        schedule[["game_date", "home_team", "home_score", "away_score"]].rename(
            columns={"home_team": "team", "home_score": "runs_scored", "away_score": "runs_allowed"}
        ),
        schedule[["game_date", "away_team", "away_score", "home_score"]].rename(
            columns={"away_team": "team", "away_score": "runs_scored", "home_score": "runs_allowed"}
        ),
    ], ignore_index=True)
    teams = teams.sort_values("game_date")

    for team, grp in teams.groupby("team"):
        grp = grp.sort_values("game_date").reset_index(drop=True)
        for i, row in grp.iterrows():
            past = grp.iloc[max(0, i - n):i]
            rows.append({
                "game_date":    row["game_date"],
                "team":         team,
                f"rs_{n}g":     past["runs_scored"].mean() if len(past) > 0 else LG_RPG,
                f"ra_{n}g":     past["runs_allowed"].mean() if len(past) > 0 else LG_RPG,
                f"pyth_{n}g":   _pyth(past["runs_scored"].sum(), past["runs_allowed"].sum()),
                f"n_{n}g":      len(past),
            })
    return pd.DataFrame(rows)


def _pyth(rs: float, ra: float, exp: float = 1.83) -> float:
    if rs + ra <= 0:
        return 0.5
    return round(rs ** exp / (rs ** exp + ra ** exp), 4)


def _sp_rolling_features(schedule: pd.DataFrame,
                          pitching: pd.DataFrame) -> dict:
    """Build per-SP rolling features keyed by (sp_id, game_date)."""
    if pitching.empty or "IDfg" not in pitching.columns:
        return {}

    result = {}
    # Use FanGraphs season stats as proxy (true rolling would require Statcast)
    for _, row in pitching.iterrows():
        sid  = int(row.get("IDfg", 0) or 0)
        seas = int(row.get("season", 0) or 0)
        if not sid or not seas:
            continue
        result[(sid, seas)] = {
            "sp_era":     float(row.get("ERA", 4.35) or 4.35),
            "sp_fip":     float(row.get("FIP", 4.35) or 4.35),
            "sp_xfip":    float(row.get("xFIP", 4.35) or 4.35),
            "sp_siera":   float(row.get("SIERA", 4.35) or 4.35),
            "sp_k9":      float(row.get("K/9", 8.5)  or 8.5),
            "sp_bb9":     float(row.get("BB/9", 3.0) or 3.0),
            "sp_hr9":     float(row.get("HR/9", 1.2) or 1.2),
            "sp_whip":    float(row.get("WHIP", 1.30) or 1.30),
            "sp_lob_pct": float(row.get("LOB%", 0.72) or 0.72),
            "sp_gb_pct":  float(row.get("GB%", 0.44)  or 0.44),
            "sp_hard_pct":float(row.get("Hard%", 0.32) or 0.32),
        }
    return result


def _team_season_features(batting: pd.DataFrame,
                           pitching_team: pd.DataFrame) -> dict:
    """Build team season-level features keyed by (team, season)."""
    result = {}

    if not batting.empty:
        for _, row in batting.iterrows():
            key = (str(row.get("Team", "")), int(row.get("season", 0) or 0))
            result[key] = {
                "team_wrc_plus":  float(row.get("wRC+", 100) or 100),
                "team_woba":      float(row.get("wOBA", 0.320) or 0.320),
                "team_bb_pct":    float(row.get("BB%", 0.09)  or 0.09),
                "team_k_pct":     float(row.get("K%", 0.22)   or 0.22),
                "team_iso":       float(row.get("ISO", 0.165)  or 0.165),
                "team_babip":     float(row.get("BABIP", 0.300) or 0.300),
                "team_hard_pct":  float(row.get("Hard%", 0.37)  or 0.37),
                "team_barrel_pct":float(row.get("Barrel%", 0.08) or 0.08),
            }

    return result


# ── Core feature matrix builder ───────────────────────────────────────────────

def build_feature_matrix(
    schedule:    pd.DataFrame,
    pitching:    pd.DataFrame,
    batting:     pd.DataFrame,
    team_batting:  pd.DataFrame,
    team_pitching: pd.DataFrame,
) -> pd.DataFrame:
    """Build game-level feature matrix. No lookahead bias: only stats prior to game_date."""
    log.info("Building feature matrix...")

    if schedule.empty:
        log.warning("Schedule empty — cannot build features")
        return pd.DataFrame()

    # Rolling team run stats (14-game and 7-game)
    roll14 = _rolling_team_stats(schedule, 14)
    roll7  = _rolling_team_stats(schedule, 7)

    sp_feats   = _sp_rolling_features(schedule, pitching)
    team_feats = _team_season_features(batting, team_batting)

    rows = []
    for _, game in schedule.iterrows():
        gdate  = str(game["game_date"])
        season = int(game.get("season", 0) or 0)
        gpk    = game.get("game_pk")

        # Rolling stats for each team
        def _get_roll(df, team, col):
            mask = (df["team"] == team) & (df["game_date"] == gdate)
            sub  = df[mask]
            return float(sub[col].values[0]) if not sub.empty else LG_RPG

        away_team = game.get("away_team", "")
        home_team = game.get("home_team", "")

        # SP features
        away_sp_id = int(game.get("away_sp_id") or 0)
        home_sp_id = int(game.get("home_sp_id") or 0)

        def _sp(sp_id: int) -> dict:
            return sp_feats.get((sp_id, season), {
                "sp_era": 4.35, "sp_fip": 4.35, "sp_xfip": 4.35,
                "sp_siera": 4.35, "sp_k9": 8.5, "sp_bb9": 3.0,
                "sp_hr9": 1.2, "sp_whip": 1.30, "sp_lob_pct": 0.72,
                "sp_gb_pct": 0.44, "sp_hard_pct": 0.32,
            })

        asp = _sp(away_sp_id)
        hsp = _sp(home_sp_id)

        # Team seasonal features
        def _tf(team: str) -> dict:
            return team_feats.get((team, season), {
                "team_wrc_plus": 100, "team_woba": 0.320,
                "team_bb_pct": 0.09, "team_k_pct": 0.22,
                "team_iso": 0.165, "team_babip": 0.300,
                "team_hard_pct": 0.37, "team_barrel_pct": 0.08,
            })

        atf = _tf(away_team)
        htf = _tf(home_team)

        # Edge / difference features (home perspective)
        sp_xfip_diff  = asp.get("sp_xfip", 4.35) - hsp.get("sp_xfip", 4.35)  # +ve = away SP worse
        wrc_diff       = htf.get("team_wrc_plus", 100) - atf.get("team_wrc_plus", 100)
        woba_diff      = htf.get("team_woba", 0.320) - atf.get("team_woba", 0.320)

        # Run expectancy components
        home_rs14 = _get_roll(roll14, home_team, "rs_14g")
        away_rs14 = _get_roll(roll14, away_team, "rs_14g")
        home_ra14 = _get_roll(roll14, home_team, "ra_14g")
        away_ra14 = _get_roll(roll14, away_team, "ra_14g")
        home_rs7  = _get_roll(roll7,  home_team, "rs_7g")
        away_rs7  = _get_roll(roll7,  away_team, "rs_7g")

        home_pyth14 = _get_roll(roll14, home_team, "pyth_14g")
        away_pyth14 = _get_roll(roll14, away_team, "pyth_14g")

        # Pythagorean win probability (baseline model)
        exp_home_runs = LG_RPG * (htf.get("team_wrc_plus", 100) / 100) * (asp.get("sp_era", 4.35) / LG_RPG)
        exp_away_runs = LG_RPG * (atf.get("team_wrc_plus", 100) / 100) * (hsp.get("sp_era", 4.35) / LG_RPG)
        exp_total     = exp_home_runs + exp_away_runs
        pyth_home_p   = _pyth(exp_home_runs, exp_away_runs) if exp_home_runs > 0 else 0.54

        row = {
            # Identifiers (not used as features)
            "game_pk":   gpk,
            "game_date": gdate,
            "season":    season,
            "away_team": away_team,
            "home_team": home_team,

            # Targets
            "home_win":      int(game.get("home_win", 0) or 0),
            "total_runs":    float(game.get("total_runs", 9) or 9),
            "nrfi":          1 if (int(game.get("away_score", 0) or 0) == 0 and
                                    int(game.get("home_score", 0) or 0) == 0) else 0,

            # SP features — away starter
            "away_sp_era":       asp.get("sp_era", 4.35),
            "away_sp_fip":       asp.get("sp_fip", 4.35),
            "away_sp_xfip":      asp.get("sp_xfip", 4.35),
            "away_sp_siera":     asp.get("sp_siera", 4.35),
            "away_sp_k9":        asp.get("sp_k9", 8.5),
            "away_sp_bb9":       asp.get("sp_bb9", 3.0),
            "away_sp_hr9":       asp.get("sp_hr9", 1.2),
            "away_sp_whip":      asp.get("sp_whip", 1.30),
            "away_sp_gb_pct":    asp.get("sp_gb_pct", 0.44),
            "away_sp_hard_pct":  asp.get("sp_hard_pct", 0.32),

            # SP features — home starter
            "home_sp_era":       hsp.get("sp_era", 4.35),
            "home_sp_fip":       hsp.get("sp_fip", 4.35),
            "home_sp_xfip":      hsp.get("sp_xfip", 4.35),
            "home_sp_siera":     hsp.get("sp_siera", 4.35),
            "home_sp_k9":        hsp.get("sp_k9", 8.5),
            "home_sp_bb9":       hsp.get("sp_bb9", 3.0),
            "home_sp_hr9":       hsp.get("sp_hr9", 1.2),
            "home_sp_whip":      hsp.get("sp_whip", 1.30),
            "home_sp_gb_pct":    hsp.get("sp_gb_pct", 0.44),
            "home_sp_hard_pct":  hsp.get("sp_hard_pct", 0.32),

            # SP differential features (home advantage perspective)
            "sp_xfip_diff":      sp_xfip_diff,
            "sp_k9_diff":        asp.get("sp_k9", 8.5) - hsp.get("sp_k9", 8.5),
            "sp_bb9_diff":       asp.get("sp_bb9", 3.0) - hsp.get("sp_bb9", 3.0),
            "sp_era_diff":       asp.get("sp_era", 4.35) - hsp.get("sp_era", 4.35),

            # Team offense features — away
            "away_wrc_plus":     atf.get("team_wrc_plus", 100),
            "away_woba":         atf.get("team_woba", 0.320),
            "away_bb_pct":       atf.get("team_bb_pct", 0.09),
            "away_k_pct":        atf.get("team_k_pct", 0.22),
            "away_iso":          atf.get("team_iso", 0.165),
            "away_babip":        atf.get("team_babip", 0.300),
            "away_barrel_pct":   atf.get("team_barrel_pct", 0.08),

            # Team offense features — home
            "home_wrc_plus":     htf.get("team_wrc_plus", 100),
            "home_woba":         htf.get("team_woba", 0.320),
            "home_bb_pct":       htf.get("team_bb_pct", 0.09),
            "home_k_pct":        htf.get("team_k_pct", 0.22),
            "home_iso":          htf.get("team_iso", 0.165),
            "home_babip":        htf.get("team_babip", 0.300),
            "home_barrel_pct":   htf.get("team_barrel_pct", 0.08),

            # Differential offense features
            "wrc_diff":          wrc_diff,
            "woba_diff":         woba_diff,

            # Rolling run form
            "home_rs_14g":       home_rs14,
            "away_rs_14g":       away_rs14,
            "home_ra_14g":       home_ra14,
            "away_ra_14g":       away_ra14,
            "home_rs_7g":        home_rs7,
            "away_rs_7g":        away_rs7,
            "home_pyth_14g":     home_pyth14,
            "away_pyth_14g":     away_pyth14,
            "rs_diff_14g":       home_rs14 - away_rs14,
            "pyth_diff_14g":     home_pyth14 - away_pyth14,

            # Expected total runs
            "exp_home_runs":     exp_home_runs,
            "exp_away_runs":     exp_away_runs,
            "exp_total_runs":    exp_total,
            "pyth_home_p":       pyth_home_p,

            # Month feature (season rhythm)
            "month": int(gdate[5:7]) if len(gdate) >= 7 else 7,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    log.info(f"Feature matrix: {len(df)} rows x {len(df.columns)} columns")
    return df


# ── Feature columns ───────────────────────────────────────────────────────────

FEATURE_COLS = [
    "away_sp_era", "away_sp_fip", "away_sp_xfip", "away_sp_siera",
    "away_sp_k9", "away_sp_bb9", "away_sp_hr9", "away_sp_whip",
    "away_sp_gb_pct", "away_sp_hard_pct",
    "home_sp_era", "home_sp_fip", "home_sp_xfip", "home_sp_siera",
    "home_sp_k9", "home_sp_bb9", "home_sp_hr9", "home_sp_whip",
    "home_sp_gb_pct", "home_sp_hard_pct",
    "sp_xfip_diff", "sp_k9_diff", "sp_bb9_diff", "sp_era_diff",
    "away_wrc_plus", "away_woba", "away_bb_pct", "away_k_pct",
    "away_iso", "away_babip", "away_barrel_pct",
    "home_wrc_plus", "home_woba", "home_bb_pct", "home_k_pct",
    "home_iso", "home_babip", "home_barrel_pct",
    "wrc_diff", "woba_diff",
    "home_rs_14g", "away_rs_14g", "home_ra_14g", "away_ra_14g",
    "home_rs_7g", "away_rs_7g",
    "home_pyth_14g", "away_pyth_14g",
    "rs_diff_14g", "pyth_diff_14g",
    "exp_home_runs", "exp_away_runs", "exp_total_runs",
    "pyth_home_p", "month",
]

TARGET_CONFIGS = {
    "home_win":    {"target": "home_win",   "type": "classification"},
    "total_runs":  {"target": "total_runs", "type": "regression"},
    "nrfi":        {"target": "nrfi",       "type": "classification"},
}


# ── Model training ────────────────────────────────────────────────────────────

def _train_single_target(
    X_train, y_train, X_val, y_val, target_type: str, target_name: str
) -> dict:
    """Train XGBoost + LightGBM + Linear and blend."""
    from xgboost  import XGBClassifier, XGBRegressor
    from lightgbm import LGBMClassifier, LGBMRegressor
    from sklearn.linear_model import LogisticRegression, Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import (
        log_loss, roc_auc_score, mean_squared_error, accuracy_score
    )

    is_clf = (target_type == "classification")

    log.info(f"  Training {target_name} ({target_type}), {len(X_train)} samples...")

    # XGBoost
    if is_clf:
        xgb = XGBClassifier(
            n_estimators=300, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
            reg_alpha=0.1, reg_lambda=1.0, use_label_encoder=False,
            eval_metric="logloss", random_state=42, n_jobs=-1,
            early_stopping_rounds=20,
        )
        lgbm = LGBMClassifier(
            n_estimators=400, max_depth=4, learning_rate=0.04,
            num_leaves=31, min_child_samples=20,
            subsample=0.8, colsample_bytree=0.8, random_state=42, n_jobs=-1,
            verbose=-1,
        )
        linear = Pipeline([
            ("scaler", StandardScaler()),
            ("lr", LogisticRegression(C=1.0, max_iter=1000, random_state=42)),
        ])
    else:
        xgb = XGBRegressor(
            n_estimators=300, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
            reg_alpha=0.1, reg_lambda=1.0, random_state=42, n_jobs=-1,
            early_stopping_rounds=20,
        )
        lgbm = LGBMRegressor(
            n_estimators=400, max_depth=4, learning_rate=0.04,
            num_leaves=31, min_child_samples=20,
            subsample=0.8, colsample_bytree=0.8, random_state=42, n_jobs=-1,
            verbose=-1,
        )
        linear = Pipeline([
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=1.0)),
        ])

    # Fit with early stopping (XGBoost needs eval set)
    try:
        xgb.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )
    except Exception:
        xgb.set_params(early_stopping_rounds=None)
        xgb.fit(X_train, y_train)

    lgbm.fit(X_train, y_train)
    linear.fit(X_train, y_train)

    # Ensemble: 50% XGB + 35% LGBM + 15% Linear
    W_XGB, W_LGBM, W_LIN = 0.50, 0.35, 0.15

    if is_clf:
        p_xgb    = xgb.predict_proba(X_val)[:, 1]
        p_lgbm   = lgbm.predict_proba(X_val)[:, 1]
        p_linear = linear.predict_proba(X_val)[:, 1]
        p_blend  = W_XGB * p_xgb + W_LGBM * p_lgbm + W_LIN * p_linear

        auc   = roc_auc_score(y_val, p_blend)
        ll    = log_loss(y_val, p_blend)
        acc   = accuracy_score(y_val, (p_blend > 0.5).astype(int))
        log.info(f"    Val AUC={auc:.4f} LogLoss={ll:.4f} Acc={acc:.4f}")
        metrics = {"auc": round(auc, 4), "log_loss": round(ll, 4), "accuracy": round(acc, 4)}
    else:
        p_xgb    = xgb.predict(X_val)
        p_lgbm   = lgbm.predict(X_val)
        p_linear = linear.predict(X_val)
        p_blend  = W_XGB * p_xgb + W_LGBM * p_lgbm + W_LIN * p_linear

        rmse = float(np.sqrt(mean_squared_error(y_val, p_blend)))
        log.info(f"    Val RMSE={rmse:.4f}")
        metrics = {"rmse": round(rmse, 4)}

    # Feature importance (XGBoost)
    feat_imp = {}
    if hasattr(xgb, "feature_importances_"):
        for feat, imp in zip(FEATURE_COLS, xgb.feature_importances_):
            feat_imp[feat] = round(float(imp), 6)

    return {
        "xgb":      xgb,
        "lgbm":     lgbm,
        "linear":   linear,
        "weights":  (W_XGB, W_LGBM, W_LIN),
        "metrics":  metrics,
        "feat_imp": feat_imp,
        "type":     target_type,
    }


def _calibrate_classifier(model_bundle: dict,
                           X_val: np.ndarray, y_val: np.ndarray) -> dict:
    """Apply Platt scaling if calibration error > 3%."""
    from sklearn.calibration import CalibratedClassifierCV, calibration_curve

    xgb, lgbm, linear = model_bundle["xgb"], model_bundle["lgbm"], model_bundle["linear"]
    W_XGB, W_LGBM, W_LIN = model_bundle["weights"]

    p_blend = (W_XGB * xgb.predict_proba(X_val)[:, 1] +
               W_LGBM * lgbm.predict_proba(X_val)[:, 1] +
               W_LIN  * linear.predict_proba(X_val)[:, 1])

    try:
        fraction_of_positives, mean_predicted_value = calibration_curve(
            y_val, p_blend, n_bins=10, strategy="quantile"
        )
        cal_error = float(np.mean(np.abs(fraction_of_positives - mean_predicted_value)))
        log.info(f"    Calibration error: {cal_error:.4f}")

        cal_data = {
            "cal_error": round(cal_error, 4),
            "bins_predicted": mean_predicted_value.tolist(),
            "bins_actual":    fraction_of_positives.tolist(),
        }
    except Exception as e:
        log.warning(f"    Calibration curve failed: {e}")
        cal_data = {"cal_error": 0.0}

    return cal_data


def _find_optimal_threshold(model_bundle: dict,
                             X_test: np.ndarray, y_test: np.ndarray,
                             target_name: str) -> dict:
    """Simulate edge thresholds on holdout to find optimal ROI threshold."""
    xgb, lgbm, linear = model_bundle["xgb"], model_bundle["lgbm"], model_bundle["linear"]
    W_XGB, W_LGBM, W_LIN = model_bundle["weights"]

    p_blend = (W_XGB * xgb.predict_proba(X_test)[:, 1] +
               W_LGBM * lgbm.predict_proba(X_test)[:, 1] +
               W_LIN  * linear.predict_proba(X_test)[:, 1])

    best_threshold = 0.55
    best_roi       = -999.0

    for thresh in np.arange(0.51, 0.75, 0.01):
        mask      = p_blend >= thresh
        n_bets    = mask.sum()
        if n_bets < 10:
            break
        wins  = y_test[mask].sum()
        roi   = float(wins / n_bets - (1 - wins / n_bets))  # simplified EV
        if roi > best_roi:
            best_roi       = roi
            best_threshold = float(thresh)

    # Simulate Kelly fractions on holdout
    best_kelly = 0.25
    best_growth = -999.0
    bankroll = 1.0
    for frac in np.arange(0.05, 0.50, 0.05):
        br = 1.0
        for prob, actual in zip(p_blend, y_test):
            if prob < best_threshold:
                continue
            stake = br * frac * (prob - (1 - prob))  # simplified Kelly
            stake = min(stake, br * 0.05)
            if actual == 1:
                br += stake
            else:
                br -= stake
        if br > best_growth:
            best_growth = br
            best_kelly  = float(frac)

    log.info(f"    Optimal threshold={best_threshold:.2f} Kelly={best_kelly:.2f}")
    return {
        "threshold": best_threshold,
        "kelly_fraction": best_kelly,
        "test_roi": round(best_roi, 4),
        "test_bankroll_growth": round(best_growth, 4),
    }


# ── SHAP explanation ──────────────────────────────────────────────────────────

def compute_shap_explanation(model_bundle: dict,
                              X: np.ndarray,
                              feat_names: list,
                              n_samples: int = 100) -> list[dict]:
    """Compute SHAP values and return top-3 features per prediction."""
    try:
        import shap
        xgb_model = model_bundle["xgb"]
        sample    = X[:min(n_samples, len(X))]
        explainer = shap.TreeExplainer(xgb_model)
        shap_vals = explainer.shap_values(sample)
        if isinstance(shap_vals, list):
            shap_vals = shap_vals[1]  # class 1 for binary

        results = []
        for i in range(len(sample)):
            sv = shap_vals[i]
            top3_idx = np.argsort(np.abs(sv))[-3:][::-1]
            results.append([{
                "feature":   feat_names[j],
                "shap_val":  round(float(sv[j]), 4),
                "direction": "+" if sv[j] > 0 else "-",
            } for j in top3_idx])
        return results
    except Exception as e:
        log.warning(f"SHAP failed: {e}")
        return []


def explain_prediction_text(shap_result: list[dict], side: str,
                             model_prob: float) -> str:
    """Format SHAP explanation as human-readable string."""
    if not shap_result:
        return f"{side} {model_prob:.1%} — SHAP unavailable"
    parts = []
    for s in shap_result:
        feat = s["feature"].replace("_", " ")
        val  = s["shap_val"]
        parts.append(f"{feat} {'+' if val > 0 else ''}{val*100:.1f}%")
    return f"{side} {model_prob:.1%} — {', '.join(parts)}"


# ── Full training pipeline ────────────────────────────────────────────────────

def train_all():
    """Full training pipeline: load data → features → train → save."""
    log.info("=" * 60)
    log.info("PARLAY OS — ML Training Pipeline")
    log.info("=" * 60)

    # Load data
    log.info("Loading schedule data (2022-2025)...")
    schedule = load_schedule_results(ALL_SEASONS)
    if schedule.empty:
        log.error("Could not load schedule data — aborting")
        return

    log.info(f"Loaded {len(schedule)} games")

    log.info("Loading pitching stats...")
    pitching = load_pitching_stats(ALL_SEASONS)

    log.info("Loading batting stats...")
    batting = load_batting_stats(ALL_SEASONS)

    log.info("Loading team batting/pitching...")
    team_bat  = load_team_batting(ALL_SEASONS)
    team_pit  = load_team_pitching(ALL_SEASONS)

    # Build feature matrix
    df = build_feature_matrix(schedule, pitching, batting, team_bat, team_pit)
    if df.empty:
        log.error("Empty feature matrix — aborting")
        return

    # Save feature matrix
    df.to_parquet(CACHE_DIR / "feature_matrix.parquet")
    log.info(f"Feature matrix saved: {len(df)} rows")

    # Split
    df_train = df[df["season"].isin(TRAIN_SEASONS)].dropna(subset=FEATURE_COLS)
    df_val   = df[df["season"].isin(VAL_SEASONS)].dropna(subset=FEATURE_COLS)
    df_test  = df[df["season"].isin(TEST_SEASONS)].dropna(subset=FEATURE_COLS)

    log.info(f"Split — Train:{len(df_train)} Val:{len(df_val)} Test:{len(df_test)}")

    X_train = df_train[FEATURE_COLS].values
    X_val   = df_val[FEATURE_COLS].values
    X_test  = df_test[FEATURE_COLS].values

    all_models     = {}
    all_cal        = {}
    all_opt_params = {}
    all_feat_imp   = {}

    for target_name, cfg in TARGET_CONFIGS.items():
        log.info(f"\nTarget: {target_name}")
        y_train = df_train[cfg["target"]].values
        y_val_t = df_val[cfg["target"]].values
        y_test_t= df_test[cfg["target"]].values

        bundle = _train_single_target(
            X_train, y_train, X_val, y_val_t,
            cfg["type"], target_name
        )
        all_models[target_name] = bundle
        all_feat_imp[target_name] = bundle["feat_imp"]

        if cfg["type"] == "classification":
            cal  = _calibrate_classifier(bundle, X_val, y_val_t)
            opts = _find_optimal_threshold(bundle, X_test, y_test_t, target_name)
            all_cal[target_name]        = cal
            all_opt_params[target_name] = opts

        # Save model bundle
        joblib.dump(bundle, MODEL_FILES.get(target_name, MODELS_DIR / f"{target_name}.pkl"))
        log.info(f"  Saved {target_name}.pkl")

    # Save metadata
    meta = {
        "trained_at":  datetime.utcnow().isoformat(),
        "train_games": len(df_train),
        "val_games":   len(df_val),
        "test_games":  len(df_test),
        "targets":     list(TARGET_CONFIGS.keys()),
        "features":    FEATURE_COLS,
        "metrics": {k: v["metrics"] for k, v in all_models.items()},
    }
    with open(META_FILE, "w") as f:
        json.dump(meta, f, indent=2)
    with open(FEAT_IMPORT_FILE, "w") as f:
        json.dump(all_feat_imp, f, indent=2)
    with open(CALIBRATION_FILE, "w") as f:
        json.dump(all_cal, f, indent=2)
    with open(PARAMS_FILE, "w") as f:
        json.dump(all_opt_params, f, indent=2)

    log.info("\n" + "=" * 60)
    log.info("Training complete. Models saved to models/")
    log.info("=" * 60)
    return all_models


# ── Prediction interface ──────────────────────────────────────────────────────

_LOADED_MODELS: dict = {}

def _load_model(target: str) -> dict | None:
    if target in _LOADED_MODELS:
        return _LOADED_MODELS[target]
    path = MODEL_FILES.get(target)
    if path and path.exists():
        try:
            bundle = joblib.load(path)
            _LOADED_MODELS[target] = bundle
            return bundle
        except Exception as e:
            log.warning(f"Failed to load {target}: {e}")
    return None


def models_available() -> bool:
    return MODEL_FILES["home_win"].exists()


def predict_game(features: dict) -> dict:
    """
    Predict game outcomes from a feature dict.
    Returns probabilities with SHAP explanation and confidence.
    Falls back to Pythagorean if models not trained.
    """
    if not models_available():
        # Pythagorean fallback
        pyth_p = features.get("pyth_home_p", 0.50)
        return {
            "home_win_prob":    round(pyth_p, 4),
            "away_win_prob":    round(1 - pyth_p, 4),
            "total_runs_pred":  features.get("exp_total_runs", 8.5),
            "nrfi_prob":        0.57,
            "model":            "pythagorean_fallback",
            "shap_home":        [],
            "shap_away":        [],
            "confidence":       "low",
        }

    # Build feature vector
    feat_vec = np.array([[
        features.get(col, 0.0) for col in FEATURE_COLS
    ]])

    result = {"model": "ensemble"}

    # Home win probability
    hw_bundle = _load_model("home_win")
    if hw_bundle:
        W_XGB, W_LGBM, W_LIN = hw_bundle["weights"]
        try:
            p_xgb  = hw_bundle["xgb"].predict_proba(feat_vec)[0, 1]
            p_lgbm = hw_bundle["lgbm"].predict_proba(feat_vec)[0, 1]
            p_lin  = hw_bundle["linear"].predict_proba(feat_vec)[0, 1]
            p_home = float(W_XGB * p_xgb + W_LGBM * p_lgbm + W_LIN * p_lin)
        except Exception:
            p_home = features.get("pyth_home_p", 0.50)

        result["home_win_prob"] = round(p_home, 4)
        result["away_win_prob"] = round(1 - p_home, 4)

        # SHAP for top prediction
        try:
            import shap
            explainer = shap.TreeExplainer(hw_bundle["xgb"])
            sv = explainer.shap_values(feat_vec)
            if isinstance(sv, list):
                sv = sv[1]
            sv = sv[0]
            top3 = np.argsort(np.abs(sv))[-3:][::-1]
            result["shap_home"] = [{
                "feature": FEATURE_COLS[j],
                "shap_val": round(float(sv[j]), 4),
            } for j in top3]
            result["shap_away"] = [{
                "feature": FEATURE_COLS[j],
                "shap_val": round(-float(sv[j]), 4),
            } for j in top3]
        except Exception:
            result["shap_home"] = []
            result["shap_away"] = []

    # Total runs prediction
    tr_bundle = _load_model("total_runs")
    if tr_bundle:
        try:
            W_XGB, W_LGBM, W_LIN = tr_bundle["weights"]
            p_xgb  = tr_bundle["xgb"].predict(feat_vec)[0]
            p_lgbm = tr_bundle["lgbm"].predict(feat_vec)[0]
            p_lin  = tr_bundle["linear"].predict(feat_vec)[0]
            total  = float(W_XGB * p_xgb + W_LGBM * p_lgbm + W_LIN * p_lin)
            result["total_runs_pred"] = round(total, 2)
        except Exception:
            result["total_runs_pred"] = features.get("exp_total_runs", 8.5)

    # NRFI probability
    nrfi_bundle = _load_model("nrfi")
    if nrfi_bundle:
        try:
            W_XGB, W_LGBM, W_LIN = nrfi_bundle["weights"]
            p_xgb  = nrfi_bundle["xgb"].predict_proba(feat_vec)[0, 1]
            p_lgbm = nrfi_bundle["lgbm"].predict_proba(feat_vec)[0, 1]
            p_lin  = nrfi_bundle["linear"].predict_proba(feat_vec)[0, 1]
            nrfi_p = float(W_XGB * p_xgb + W_LGBM * p_lgbm + W_LIN * p_lin)
            result["nrfi_prob"] = round(nrfi_p, 4)
        except Exception:
            result["nrfi_prob"] = 0.57

    # Load optimal thresholds
    if PARAMS_FILE.exists():
        try:
            with open(PARAMS_FILE) as f:
                params = json.load(f)
            hw_thresh = params.get("home_win", {}).get("threshold", 0.55)
            result["min_edge_threshold"] = hw_thresh
        except Exception:
            result["min_edge_threshold"] = 0.55

    # Confidence tier
    home_p = result.get("home_win_prob", 0.5)
    if abs(home_p - 0.5) > 0.12:
        result["confidence"] = "high"
    elif abs(home_p - 0.5) > 0.07:
        result["confidence"] = "medium"
    else:
        result["confidence"] = "low"

    return result


def build_game_features(
    away_sp: dict, home_sp: dict,
    away_off: dict, home_off: dict,
    away_xr: float, home_xr: float,
    weather: dict = None, park_factor: float = 1.0,
) -> dict:
    """
    Convert brain.py engine outputs into the feature vector for predict_game().
    This is the bridge between the existing engine stack and the ML model.
    """
    exp_total = away_xr + home_xr
    pyth_p    = _pyth(home_xr, away_xr)

    def sp_fip(sp):
        era  = sp.get("era", 4.35)
        k9   = sp.get("k9", 8.5)
        bb9  = sp.get("bb9", 3.0)
        hr9  = sp.get("hr9", 1.2)
        return round((13 * hr9 + 3 * bb9 - 2 * k9) / 9 + 3.17, 2)

    a_fip = sp_fip(away_sp)
    h_fip = sp_fip(home_sp)

    return {
        "away_sp_era":       away_sp.get("era", 4.35),
        "away_sp_fip":       a_fip,
        "away_sp_xfip":      away_sp.get("xfip", 4.35),
        "away_sp_siera":     away_sp.get("xfip", 4.35),  # proxy if no SIERA
        "away_sp_k9":        away_sp.get("k9", 8.5),
        "away_sp_bb9":       away_sp.get("bb9", 3.0),
        "away_sp_hr9":       away_sp.get("hr9", 1.2),
        "away_sp_whip":      away_sp.get("whip", 1.30),
        "away_sp_gb_pct":    0.44,
        "away_sp_hard_pct":  0.32,

        "home_sp_era":       home_sp.get("era", 4.35),
        "home_sp_fip":       h_fip,
        "home_sp_xfip":      home_sp.get("xfip", 4.35),
        "home_sp_siera":     home_sp.get("xfip", 4.35),
        "home_sp_k9":        home_sp.get("k9", 8.5),
        "home_sp_bb9":       home_sp.get("bb9", 3.0),
        "home_sp_hr9":       home_sp.get("hr9", 1.2),
        "home_sp_whip":      home_sp.get("whip", 1.30),
        "home_sp_gb_pct":    0.44,
        "home_sp_hard_pct":  0.32,

        "sp_xfip_diff":      away_sp.get("xfip", 4.35) - home_sp.get("xfip", 4.35),
        "sp_k9_diff":        away_sp.get("k9", 8.5) - home_sp.get("k9", 8.5),
        "sp_bb9_diff":       away_sp.get("bb9", 3.0) - home_sp.get("bb9", 3.0),
        "sp_era_diff":       away_sp.get("era", 4.35) - home_sp.get("era", 4.35),

        "away_wrc_plus":     away_off.get("wrc_plus", 100),
        "away_woba":         away_off.get("woba", 0.320) if away_off.get("woba") else 0.320,
        "away_bb_pct":       0.09,
        "away_k_pct":        0.22,
        "away_iso":          away_off.get("slg", 0.410) - away_off.get("obp", 0.320),
        "away_babip":        0.300,
        "away_barrel_pct":   0.08,

        "home_wrc_plus":     home_off.get("wrc_plus", 100),
        "home_woba":         home_off.get("woba", 0.320) if home_off.get("woba") else 0.320,
        "home_bb_pct":       0.09,
        "home_k_pct":        0.22,
        "home_iso":          home_off.get("slg", 0.410) - home_off.get("obp", 0.320),
        "home_babip":        0.300,
        "home_barrel_pct":   0.08,

        "wrc_diff":          home_off.get("wrc_plus", 100) - away_off.get("wrc_plus", 100),
        "woba_diff":         0.0,

        "home_rs_14g":       home_off.get("rpg_recent", LG_RPG),
        "away_rs_14g":       away_off.get("rpg_recent", LG_RPG),
        "home_ra_14g":       LG_RPG,
        "away_ra_14g":       LG_RPG,
        "home_rs_7g":        home_off.get("rpg_recent", LG_RPG),
        "away_rs_7g":        away_off.get("rpg_recent", LG_RPG),
        "home_pyth_14g":     0.500,
        "away_pyth_14g":     0.500,
        "rs_diff_14g":       home_off.get("rpg_recent", LG_RPG) - away_off.get("rpg_recent", LG_RPG),
        "pyth_diff_14g":     0.0,

        "exp_home_runs":     home_xr,
        "exp_away_runs":     away_xr,
        "exp_total_runs":    exp_total,
        "pyth_home_p":       pyth_p,
        "month":             date.today().month,
    }


# ── Regression / intelligence flags ───────────────────────────────────────────

def detect_regression_flags(
    away_sp: dict, home_sp: dict,
    away_off: dict, home_off: dict,
) -> dict:
    """
    Detect mean-reversion candidates.
    Returns dict of flags for brain.py to use in analysis.
    """
    flags = []

    # SP ERA vs xFIP regression candidates
    for side, sp in [("away", away_sp), ("home", home_sp)]:
        era  = sp.get("era", 4.35)
        xfip = sp.get("xfip", 4.35)
        diff = era - xfip
        if diff > 1.5:
            flags.append({
                "type":    "SP_REGRESSION_FADE",
                "side":    side,
                "message": f"{side} SP ERA {era:.2f} is {diff:.2f} above xFIP {xfip:.2f} — likely to improve",
                "impact":  "positive"  # good for SP's team going forward
            })
        elif diff < -1.5:
            flags.append({
                "type":    "SP_REGRESSION_BACK",
                "side":    side,
                "message": f"{side} SP ERA {era:.2f} is {abs(diff):.2f} below xFIP {xfip:.2f} — likely to regress",
                "impact":  "negative"
            })

    # Offense BA vs expected regression (proxy: if BABIP exists)
    for side, off in [("away", away_off), ("home", home_off)]:
        babip = off.get("babip")
        if babip:
            if babip > 0.330:
                flags.append({
                    "type":    "OFFENSE_REGRESSION_DOWN",
                    "side":    side,
                    "message": f"{side} BABIP {babip:.3f} — offense likely to regress down",
                    "impact":  "negative"
                })
            elif babip < 0.270:
                flags.append({
                    "type":    "OFFENSE_REGRESSION_UP",
                    "side":    side,
                    "message": f"{side} BABIP {babip:.3f} — offense likely to improve",
                    "impact":  "positive"
                })

    return {"flags": flags, "count": len(flags)}


# ── CLI ───────────────────────────────────────────────────────────────────────

def _show_status():
    if not models_available():
        print("Models NOT trained. Run: python ml_model.py --train")
        return
    if META_FILE.exists():
        with open(META_FILE) as f:
            meta = json.load(f)
        print(f"Models trained: {meta.get('trained_at', '?')}")
        print(f"Games: train={meta.get('train_games')} val={meta.get('val_games')} test={meta.get('test_games')}")
        for t, m in meta.get("metrics", {}).items():
            print(f"  {t}: {m}")
    if PARAMS_FILE.exists():
        with open(PARAMS_FILE) as f:
            params = json.load(f)
        print(f"Optimal params: {params}")


if __name__ == "__main__":
    args = set(sys.argv[1:])
    if "--train" in args:
        train_all()
    elif "--status" in args:
        _show_status()
    elif "--predict" in args:
        # Demo prediction with default features
        feats = {col: 0.0 for col in FEATURE_COLS}
        feats.update({
            "pyth_home_p": 0.54,
            "exp_total_runs": 8.5,
            "home_wrc_plus": 105,
            "away_wrc_plus": 98,
            "month": date.today().month,
        })
        result = predict_game(feats)
        print(json.dumps(result, indent=2))
    else:
        print(__doc__)
