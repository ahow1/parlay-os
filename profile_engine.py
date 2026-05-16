"""PARLAY OS — profile_engine.py
Auto-updates SP, hitter, bullpen, and team profiles after every game.
Called from scheduler.py and brain.py settler.

Data sources:
  MLB Stats API: game logs, season stats, splits (sitCodes), schedule
  statcast_engine: Baseball Savant for exit velocity, barrel%, fastball velo
  weather_engine: temperature at game time for cold-weather splits
"""

import math
import requests
from api_client import get as _http_get
import json
from datetime import date, timedelta, datetime
from functools import lru_cache

from memory_engine import (
    upsert_pitcher_profile, upsert_hitter_profile,
    _conn as _mem_conn,
)

STATSAPI = "https://statsapi.mlb.com/api/v1"
SEASON   = 2026
_TIMEOUT = 10

# ── City coordinates for travel distance ─────────────────────────────────────
# (lat, lon) for MLB home cities
_CITY_COORDS = {
    "NYY": (40.83, -73.93), "NYM": (40.76, -73.85), "BOS": (42.35, -71.10),
    "TOR": (43.64, -79.39), "BAL": (39.28, -76.62), "TB":  (27.77, -82.65),
    "CLE": (41.50, -81.70), "CWS": (41.83, -87.63), "DET": (42.34, -83.05),
    "KC":  (39.05, -94.48), "MIN": (44.98, -93.28), "HOU": (29.76, -95.36),
    "LAA": (33.80, -117.88),"OAK": (37.75, -122.20),"SEA": (47.59, -122.33),
    "TEX": (32.75, -97.08), "MIL": (43.03, -87.97), "CHC": (41.95, -87.66),
    "CIN": (39.10, -84.51), "PIT": (40.44, -80.00), "STL": (38.62, -90.19),
    "ATL": (33.73, -84.39), "MIA": (25.78, -80.22), "NYM": (40.76, -73.85),
    "PHI": (39.91, -75.17), "WSH": (38.87, -77.01), "ARI": (33.45, -112.07),
    "COL": (39.76, -104.99),"LAD": (34.07, -118.24),"SD":  (32.71, -117.16),
    "SF":  (37.78, -122.39),
}


def _haversine(c1, c2) -> float:
    """Distance in miles between two (lat, lon) pairs."""
    R  = 3958.8
    la1, lo1 = math.radians(c1[0]), math.radians(c1[1])
    la2, lo2 = math.radians(c2[0]), math.radians(c2[1])
    dlat = la2 - la1
    dlon = lo2 - lo1
    a = math.sin(dlat/2)**2 + math.cos(la1)*math.cos(la2)*math.sin(dlon/2)**2
    return 2 * R * math.asin(min(a**0.5, 1.0))


def _safe_float(v, default=0.0) -> float:
    try:
        f = float(v)
        return default if math.isnan(f) else f
    except Exception:
        return default


def _ip_to_float(s) -> float:
    try:
        parts = str(s).split(".")
        return int(parts[0]) + int(parts[1] if len(parts) > 1 else 0) / 3
    except Exception:
        return 0.0


# ── SP PROFILE ────────────────────────────────────────────────────────────────

def _sp_game_log(pitcher_id: int, n: int = 12) -> list:
    """Pull last N starts from game log."""
    try:
        r = _http_get(
            f"{STATSAPI}/people/{pitcher_id}/stats",
            params={"stats": "gameLog", "group": "pitching",
                    "season": SEASON, "gameType": "R"},
            timeout=_TIMEOUT,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        starts = []
        for s in splits:
            st = s.get("stat", {})
            if int(st.get("gamesStarted", 0) or 0) == 0:
                continue
            ip  = _ip_to_float(st.get("inningsPitched", "0"))
            if ip <= 0:
                continue
            k   = int(st.get("strikeOuts", 0) or 0)
            bb  = int(st.get("baseOnBalls", 0) or 0)
            er  = int(st.get("earnedRuns", 0) or 0)
            np_ = int(st.get("pitchesThrown", 0) or 0)
            starts.append({
                "date": s.get("date", ""),
                "ip": ip, "k": k, "bb": bb, "er": er, "np": np_,
                "era": round(er / ip * 9, 2) if ip > 0 else 9.0,
                "k9":  round(k / ip * 9, 2)  if ip > 0 else 0.0,
                "bb9": round(bb / ip * 9, 2) if ip > 0 else 0.0,
            })
        return starts[-n:]
    except Exception:
        return []


def _sp_season_stats(pitcher_id: int) -> dict:
    try:
        r = _http_get(
            f"{STATSAPI}/people/{pitcher_id}/stats",
            params={"stats": "season", "group": "pitching", "season": SEASON},
            timeout=_TIMEOUT,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s  = splits[0].get("stat", {})
        ip = _ip_to_float(s.get("inningsPitched", "0"))
        return {
            "era":  _safe_float(s.get("era"), 4.35),
            "whip": _safe_float(s.get("whip"), 1.30),
            "k9":   _safe_float(s.get("strikeoutsPer9Inn"), 8.5),
            "bb9":  _safe_float(s.get("walksPer9Inn"), 3.0),
            "hr9":  _safe_float(s.get("homeRunsPer9"), 1.2),
            "ip":   round(ip, 1),
            "gs":   int(s.get("gamesStarted", 0) or 0),
        }
    except Exception:
        return {}


def _sp_situation_splits(pitcher_id: int) -> dict:
    """ERA home/away and vs L/R batters."""
    result = {}
    for sit, key in (("vl", "vs_lhb"), ("vr", "vs_rhb")):
        try:
            r = _http_get(
                f"{STATSAPI}/people/{pitcher_id}/stats",
                params={"stats": "statSplits", "group": "pitching",
                        "season": SEASON, "sitCodes": sit},
                timeout=_TIMEOUT,
            )
            splits = r.json().get("stats", [{}])[0].get("splits", [])
            if splits:
                st = splits[0].get("stat", {})
                result[key] = _safe_float(st.get("era"), 4.35)
        except Exception:
            pass
    for sit, key in (("h", "era_home"), ("a", "era_away")):
        try:
            r = _http_get(
                f"{STATSAPI}/people/{pitcher_id}/stats",
                params={"stats": "statSplits", "group": "pitching",
                        "season": SEASON, "sitCodes": sit},
                timeout=_TIMEOUT,
            )
            splits = r.json().get("stats", [{}])[0].get("splits", [])
            if splits:
                st = splits[0].get("stat", {})
                result[key] = _safe_float(st.get("era"), 4.35)
        except Exception:
            pass
    return result


def _compute_fatigue_cliff(starts: list) -> int | None:
    """
    Find pitch count where ERA rises sharply.
    Returns pitch count threshold or None if insufficient data.
    """
    buckets = {
        "0-79":   {"era_sum": 0, "ip": 0, "er": 0},
        "80-99":  {"era_sum": 0, "ip": 0, "er": 0},
        "100-119":{"era_sum": 0, "ip": 0, "er": 0},
        "120+":   {"era_sum": 0, "ip": 0, "er": 0},
    }
    for s in starts:
        np_ = s.get("np", 0)
        ip  = s.get("ip", 0)
        er  = s.get("er", 0)
        if ip <= 0:
            continue
        if np_ < 80:
            b = "0-79"
        elif np_ < 100:
            b = "80-99"
        elif np_ < 120:
            b = "100-119"
        else:
            b = "120+"
        buckets[b]["ip"] += ip
        buckets[b]["er"] += er

    eras = {}
    for key, data in buckets.items():
        if data["ip"] > 0:
            eras[key] = round(data["er"] / data["ip"] * 9, 2)

    order = ["0-79", "80-99", "100-119", "120+"]
    limits = {"0-79": 75, "80-99": 95, "100-119": 115, "120+": 125}
    prev_era = None
    for key in order:
        if key not in eras:
            continue
        if prev_era is not None and eras[key] - prev_era > 0.75:
            return limits[key]
        prev_era = eras[key]
    return None


def _compute_ttop_era(starts: list) -> dict:
    """
    Proxy TTOP ERA from pitch count ranges.
    1st TTO ≈ NP 1-50, 2nd TTO ≈ NP 51-85, 3rd TTO ≈ NP 86+
    """
    groups = {1: {"er": 0, "est_ip": 0}, 2: {"er": 0, "est_ip": 0}, 3: {"er": 0, "est_ip": 0}}
    for s in starts:
        np_ = s.get("np", 0)
        ip  = s.get("ip", 0)
        er  = s.get("er", 0)
        if np_ <= 0 or ip <= 0:
            continue
        # Estimate fraction of innings pitched in each TTO range
        tto1_frac = min(50, np_) / np_
        tto2_frac = max(0, min(35, np_ - 50)) / np_ if np_ > 50 else 0
        tto3_frac = max(0, np_ - 85) / np_ if np_ > 85 else 0
        total = tto1_frac + tto2_frac + tto3_frac or 1
        groups[1]["er"] += er * tto1_frac / total
        groups[1]["est_ip"] += ip * tto1_frac / total
        groups[2]["er"] += er * tto2_frac / total
        groups[2]["est_ip"] += ip * tto2_frac / total
        groups[3]["er"] += er * tto3_frac / total
        groups[3]["est_ip"] += ip * tto3_frac / total

    result = {}
    for tto, data in groups.items():
        if data["est_ip"] > 0:
            result[f"ttop_era_{tto}"] = round(data["er"] / data["est_ip"] * 9, 2)
        else:
            result[f"ttop_era_{tto}"] = None
    return result


def update_sp_profile(pitcher_name: str, pitcher_id: int) -> dict:
    """
    Build full SP profile and persist to pitcher_profiles table.
    Returns the profile dict.
    """
    today = date.today().isoformat()
    starts  = _sp_game_log(pitcher_id, n=12)
    season  = _sp_season_stats(pitcher_id)
    splits  = _sp_situation_splits(pitcher_id)

    if not season and not starts:
        return {}

    last3  = starts[-3:] if starts else []
    last10 = starts[-10:] if starts else []

    # ── Trend metrics ─────────────────────────────────────────────────────────
    # K9 trend: slope over last 10 starts
    k9_trend = 0.0
    if len(last10) >= 5:
        mid  = len(last10) // 2
        k9_early = sum(g["k9"] for g in last10[:mid]) / mid
        k9_late  = sum(g["k9"] for g in last10[mid:]) / (len(last10) - mid)
        k9_trend = round(k9_late - k9_early, 2)

    # BB9 trend
    bb9_trend = 0.0
    if len(last10) >= 5:
        bb9_early = sum(g["bb9"] for g in last10[:mid]) / mid
        bb9_late  = sum(g["bb9"] for g in last10[mid:]) / (len(last10) - mid)
        bb9_trend = round(bb9_late - bb9_early, 2)

    # Recent ERA (last 3 starts, weighted most-recent)
    recent_era = season.get("era", 4.35)
    if last3:
        weights = [0.2, 0.3, 0.5] if len(last3) == 3 else ([0.4, 0.6] if len(last3) == 2 else [1.0])
        recent_era = round(sum(g["era"] * w for g, w in zip(last3, weights[-len(last3):])), 2)

    # Velocity trend (K-rate proxy; use Statcast if available)
    velocity_avg = None
    velocity_trend = None
    try:
        from statcast_engine import get_pitcher_statcast
        sc = get_pitcher_statcast(pitcher_id)
        velocity_avg = sc.get("avg_fastball_velo")
    except Exception:
        pass

    if velocity_avg is None and last10:
        # K9 as velocity proxy: -0.5 K9 ≈ -0.5 mph
        velocity_trend = round(k9_trend * 0.5, 2)

    # Velocity decline flags — thresholds match sp_engine.py
    # velocity_decline: command/velocity change (not necessarily injury)
    # velocity_injury_risk: severe drop (3.0+ K/9) — possible arm injury
    velocity_decline     = k9_trend < -1.0
    velocity_injury_risk = k9_trend < -3.0

    # Walk worsening
    worsening_walk = False
    if len(last3) >= 2:
        bb9s = [g["bb9"] for g in last3]
        worsening_walk = (bb9s[-1] - bb9s[0]) > 1.0

    # Fatigue cliff
    cliff = _compute_fatigue_cliff(last10)

    # TTOP ERA proxy
    ttop = _compute_ttop_era(last10)

    # Cumulative IP flags
    cumulative_ip = season.get("ip", 0)
    ip_flag = None
    for threshold in (120, 150, 180):
        if cumulative_ip >= threshold:
            ip_flag = str(threshold)

    # Season-to-date month breakdown (for season fatigue curve)
    month_eras = {}
    for g in last10:
        gdate = g.get("date", "")
        if gdate:
            try:
                mo = gdate[5:7]
                month_eras.setdefault(mo, []).append(g["era"])
            except Exception:
                pass
    season_era_by_month = {k: round(sum(v)/len(v), 2) for k, v in month_eras.items()}

    profile = {
        "pitcher_id":          pitcher_id,
        "era":                 season.get("era", 4.35),
        "whip":                season.get("whip", 1.30),
        "k9":                  season.get("k9", 8.5),
        "bb9":                 season.get("bb9", 3.0),
        "hr9":                 season.get("hr9", 1.2),
        "ip_season":           cumulative_ip,
        "gs_season":           season.get("gs", 0),
        "ip_flag":             ip_flag,
        # Situation splits
        "era_vs_lhh":          splits.get("vs_lhb"),
        "era_vs_rhh":          splits.get("vs_rhb"),
        "era_home":            splits.get("era_home"),
        "era_away":            splits.get("era_away"),
        # TTOP proxy
        "ttop_era_1":          ttop.get("ttop_era_1"),
        "ttop_era_2":          ttop.get("ttop_era_2"),
        "ttop_era_3plus":      ttop.get("ttop_era_3"),
        # Fatigue
        "pitch_count_cliff":   cliff,
        "velocity_avg":        velocity_avg,
        "velocity_trend":      velocity_trend,
        "velocity_decline":    velocity_decline,
        "velocity_injury_risk": velocity_injury_risk,
        # Recent trends
        "recent_era_3":        recent_era,
        "k9_trend":            k9_trend,
        "bb9_trend":           bb9_trend,
        "worsening_walk":      worsening_walk,
        # Season IP tracking
        "cumulative_ip":       cumulative_ip,
        "ip_flag":             ip_flag,
        "season_era_by_month": season_era_by_month,
        "game_log_10":         last10,
    }

    # Persist
    upsert_pitcher_profile(pitcher_name, today, profile)

    # Update sp_season_ip tracker
    _upsert_sp_season_ip(pitcher_name, pitcher_id, cumulative_ip, season.get("gs", 0), ip_flag)

    return profile


# ── HITTER PROFILE ────────────────────────────────────────────────────────────

def _hitter_game_log(player_id: int, n: int = 30) -> list:
    try:
        r = _http_get(
            f"{STATSAPI}/people/{player_id}/stats",
            params={"stats": "gameLog", "group": "hitting",
                    "season": SEASON, "gameType": "R"},
            timeout=_TIMEOUT,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        games  = []
        for s in splits:
            st = s.get("stat", {})
            ab  = int(st.get("atBats", 0) or 0)
            h   = int(st.get("hits", 0) or 0)
            hr  = int(st.get("homeRuns", 0) or 0)
            bb  = int(st.get("baseOnBalls", 0) or 0)
            so  = int(st.get("strikeOuts", 0) or 0)
            obp = _safe_float(st.get("obp"), 0.0)
            slg = _safe_float(st.get("slg"), 0.0)
            games.append({
                "date": s.get("date", ""),
                "ab": ab, "h": h, "hr": hr, "bb": bb, "so": so,
                "obp": obp, "slg": slg,
                "ops": round(obp + slg, 3),
            })
        return games[-n:]
    except Exception:
        return []


def _hitter_season_stats(player_id: int) -> dict:
    try:
        r = _http_get(
            f"{STATSAPI}/people/{player_id}/stats",
            params={"stats": "season", "group": "hitting", "season": SEASON},
            timeout=_TIMEOUT,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        st  = splits[0].get("stat", {})
        pa  = int(st.get("plateAppearances", 0) or 0)
        ab  = int(st.get("atBats", 0) or 0)
        so  = int(st.get("strikeOuts", 0) or 0)
        bb  = int(st.get("baseOnBalls", 0) or 0)
        return {
            "avg":  _safe_float(st.get("avg"), 0.250),
            "obp":  _safe_float(st.get("obp"), 0.320),
            "slg":  _safe_float(st.get("slg"), 0.410),
            "ops":  _safe_float(st.get("ops"), 0.730),
            "ab":   ab,
            "pa":   pa,
            "k_pct": round(so / pa, 3) if pa else 0.22,
            "bb_pct": round(bb / pa, 3) if pa else 0.09,
        }
    except Exception:
        return {}


def _hitter_splits(player_id: int) -> dict:
    result = {}
    sit_map = {
        "vl":    "vs_lhp",
        "vr":    "vs_rhp",
        "risp":  "risp",
        "d":     "day",
        "n":     "night",
    }
    for sit, key in sit_map.items():
        try:
            r = _http_get(
                f"{STATSAPI}/people/{player_id}/stats",
                params={"stats": "statSplits", "group": "hitting",
                        "season": SEASON, "sitCodes": sit},
                timeout=_TIMEOUT,
            )
            splits = r.json().get("stats", [{}])[0].get("splits", [])
            if splits:
                st = splits[0].get("stat", {})
                result[f"{key}_ops"]  = _safe_float(st.get("ops"), 0.730)
                result[f"{key}_obp"]  = _safe_float(st.get("obp"), 0.320)
                result[f"{key}_slg"]  = _safe_float(st.get("slg"), 0.410)
        except Exception:
            pass
    return result


def _analyze_streaks(games: list) -> dict:
    """Find current streak, hot/cold streak lengths, and recovery pattern."""
    if not games:
        return {"current_streak": 0, "streak_type": "neutral",
                "hot_streak_avg_games": 5, "cold_streak_avg_games": 5}

    LG_OPS = 0.730
    hot_streaks  = []
    cold_streaks = []

    cur_streak = 0
    streak_type = "neutral"
    in_hot  = False
    in_cold = False
    streak_start = 0

    for i, g in enumerate(games):
        ops = g.get("ops", 0.0)
        if ops > LG_OPS * 1.1:  # hot game
            if in_cold:
                cold_streaks.append(i - streak_start)
                in_cold = False
            if not in_hot:
                in_hot = True
                streak_start = i
        elif ops < LG_OPS * 0.85:  # cold game
            if in_hot:
                hot_streaks.append(i - streak_start)
                in_hot = False
            if not in_cold:
                in_cold = True
                streak_start = i
        else:
            if in_hot:
                hot_streaks.append(i - streak_start)
                in_hot = False
            if in_cold:
                cold_streaks.append(i - streak_start)
                in_cold = False

    # Current streak
    last_ops = games[-1].get("ops", 0.0)
    if last_ops > LG_OPS * 1.1:
        streak_type = "hot"
        cur_streak = sum(1 for g in reversed(games) if g.get("ops", 0) > LG_OPS * 1.1)
        cur_streak_g = 0
        for g in reversed(games):
            if g.get("ops", 0) > LG_OPS * 1.1:
                cur_streak_g += 1
            else:
                break
        cur_streak = cur_streak_g
    elif last_ops < LG_OPS * 0.85:
        streak_type = "cold"
        cur_streak_g = 0
        for g in reversed(games):
            if g.get("ops", 0) < LG_OPS * 0.85:
                cur_streak_g += 1
            else:
                break
        cur_streak = cur_streak_g

    # Recovery pattern: games after cold streak bottom before hot
    recovery_games = 3  # default

    return {
        "current_streak":      cur_streak,
        "streak_type":         streak_type,
        "hot_streak_avg_games": round(sum(hot_streaks) / len(hot_streaks), 1) if hot_streaks else 5,
        "cold_streak_avg_games": round(sum(cold_streaks) / len(cold_streaks), 1) if cold_streaks else 5,
        "recovery_pattern":    recovery_games,
    }


def update_hitter_profile(player_name: str, player_id: int, team: str) -> dict:
    """Build full hitter profile and persist to hitter_profiles table."""
    today   = date.today().isoformat()
    games   = _hitter_game_log(player_id, 30)
    season  = _hitter_season_stats(player_id)
    splits  = _hitter_splits(player_id)

    if not season:
        return {}

    LG_OPS = 0.730

    # ── Hot/cold score: -5 to +5 ─────────────────────────────────────────────
    last14 = games[-14:] if len(games) >= 14 else games
    if last14:
        ab14  = sum(g["ab"] for g in last14)
        ops14 = sum(g["ops"] * g["ab"] for g in last14 if g["ab"] > 0) / max(ab14, 1)
    else:
        ops14 = season.get("ops", LG_OPS)

    season_ops = season.get("ops", LG_OPS)
    ops_diff   = ops14 - season_ops
    # Scale: +/-0.100 ops = +/-5 score
    hot_cold_score = round(max(-5, min(5, ops_diff / 0.100 * 5)), 1)

    # ── Exit velocity trend (from Statcast if available) ─────────────────────
    ev_season = None
    ev_14d    = None
    barrel_14d = None
    try:
        from statcast_engine import get_batter_statcast
        sc = get_batter_statcast(player_id)
        ev_season = sc.get("exit_velocity_avg")
        barrel_14d = sc.get("barrel_pct")
    except Exception:
        pass

    # ── Streak analysis ───────────────────────────────────────────────────────
    streaks = _analyze_streaks(games)

    # ── Cumulative AB flag ────────────────────────────────────────────────────
    cumulative_ab = season.get("ab", 0)
    ab_flag = None
    for threshold in (450, 500):
        if cumulative_ab >= threshold:
            ab_flag = str(threshold)

    profile = {
        "player_id":         player_id,
        "team":              team,
        # Season averages
        "avg":               season.get("avg", 0.250),
        "obp":               season.get("obp", 0.320),
        "slg":               season.get("slg", 0.410),
        "ops":               season_ops,
        "k_pct":             season.get("k_pct", 0.22),
        "bb_pct":            season.get("bb_pct", 0.09),
        # Rolling form
        "hot_cold_score":    hot_cold_score,
        "ops_14d":           round(ops14, 3),
        "streak_type":       streaks["streak_type"],
        "current_streak":    streaks["current_streak"],
        "hot_streak_avg_games":  streaks["hot_streak_avg_games"],
        "cold_streak_avg_games": streaks["cold_streak_avg_games"],
        "recovery_pattern":  streaks["recovery_pattern"],
        # Statcast
        "exit_velocity":     ev_season,
        "barrel_pct":        barrel_14d,
        # Splits
        "vs_lhp_ops":        splits.get("vs_lhp_ops"),
        "vs_rhp_ops":        splits.get("vs_rhp_ops"),
        "risp_ops":          splits.get("risp_ops"),
        "day_ops":           splits.get("day_ops"),
        "night_ops":         splits.get("night_ops"),
        # Fatigue
        "cumulative_ab":     cumulative_ab,
        "ab_flag":           ab_flag,
    }

    # Persist
    upsert_hitter_profile(player_name, today, profile)
    _upsert_hitter_season_ab(player_name, player_id, cumulative_ab, ab_flag)

    return profile


# ── BULLPEN PROFILE ───────────────────────────────────────────────────────────

def update_bullpen_profile(team_id: int, team_code: str) -> dict:
    """Update bullpen_memory table with current fatigue + recent ERA."""
    from bullpen_engine import analyze_bullpen, bullpen_run_factor
    today = date.today().isoformat()

    bp = analyze_bullpen(team_id, today, label=team_code)

    # Recent ERA (last 7 days) — from individual arm game logs
    total_er = 0.0
    total_ip = 0.0
    for arm in bp.get("arms", []):
        arm_id = arm.get("id")
        if not arm_id:
            continue
        try:
            r = _http_get(
                f"{STATSAPI}/people/{arm_id}/stats",
                params={"stats": "gameLog", "group": "pitching", "season": SEASON},
                timeout=_TIMEOUT,
            )
            splits = r.json().get("stats", [{}])[0].get("splits", [])
            cutoff = (date.today() - timedelta(days=7)).isoformat()
            for s in splits:
                if s.get("date", "") < cutoff:
                    continue
                st  = s.get("stat", {})
                ip  = _ip_to_float(st.get("inningsPitched", "0"))
                er  = int(st.get("earnedRuns", 0) or 0)
                total_ip += ip
                total_er += er
        except Exception:
            pass

    era_7d = round(total_er / total_ip * 9, 2) if total_ip > 0 else 4.35

    data = {
        "avg_fatigue":   bp.get("avg_fatigue", 0),
        "fatigue_tier":  bp.get("fatigue_tier", "MODERATE"),
        "era_7d":        era_7d,
        "closer_name":   bp.get("closer_name", ""),
        "closer_available": bp.get("closer_available", True),
        "high_fatigue_arms": bp.get("high_fatigue_arms", []),
        "total_rp":      bp.get("total_rp", 0),
    }

    with _mem_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO bullpen_memory
            (team, stat_date, era_7d, fatigue_score, fatigue_tier,
             closer_name, closer_pitched_yesterday, raw_data)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            team_code, today, era_7d,
            bp.get("avg_fatigue", 0), bp.get("fatigue_tier", "MODERATE"),
            bp.get("closer_name", ""),
            0 if bp.get("closer_available", True) else 1,
            json.dumps(data),
        ))

    return data


# ── TEAM PROFILE ──────────────────────────────────────────────────────────────

def _team_game_log_30(team_id: int) -> list:
    """Team game results last 30 days."""
    try:
        end_dt   = date.today()
        start_dt = end_dt - timedelta(days=30)
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={
                "sportId": 1, "teamId": team_id, "gameType": "R",
                "startDate": start_dt.isoformat(), "endDate": end_dt.isoformat(),
                "hydrate": "linescore,team",
            },
            timeout=_TIMEOUT,
        )
        games = []
        for day in r.json().get("dates", []):
            for g in day.get("games", []):
                status = g.get("status", {}).get("abstractGameState", "")
                if status != "Final":
                    continue
                teams = g.get("teams", {})
                a_t   = teams.get("away", {})
                h_t   = teams.get("home", {})
                is_home = h_t.get("team", {}).get("id") == team_id

                us  = h_t if is_home else a_t
                opp = a_t if is_home else h_t
                our_score = us.get("score", 0) or 0
                opp_score = opp.get("score", 0) or 0

                ls       = g.get("linescore", {})
                innings  = ls.get("innings", [])
                inning_runs = {}
                for inn in innings:
                    n   = inn.get("num", 0)
                    rns = (inn.get("home", {}) if is_home else inn.get("away", {})).get("runs")
                    if rns is not None:
                        inning_runs[n] = int(rns)

                games.append({
                    "date":         g.get("gameInfo", {}).get("firstPitch", day.get("date", "")),
                    "is_home":      is_home,
                    "our_score":    our_score,
                    "opp_score":    opp_score,
                    "win":          1 if our_score > opp_score else 0,
                    "margin":       our_score - opp_score,
                    "inning_runs":  inning_runs,
                    "total":        our_score + opp_score,
                    "opp_team":     (a_t if is_home else h_t).get("team", {}).get("abbreviation", ""),
                    "game_pk":      g.get("gamePk"),
                })
        return sorted(games, key=lambda x: x.get("date", ""))
    except Exception:
        return []


def _compute_run_scoring_patterns(games: list) -> dict:
    """
    Analyze how a team scores: inning distribution, cluster score,
    comeback rate, blowout rate, RISP conversion proxy.
    """
    if not games:
        return {}

    total_runs  = sum(g["our_score"] for g in games)
    total_games = len(games)
    if not total_games:
        return {}

    # Innings scored distribution (% of runs by inning)
    inning_totals = {}
    for g in games:
        for inn, runs in g.get("inning_runs", {}).items():
            inning_totals[inn] = inning_totals.get(inn, 0) + runs
    inning_pct = {}
    if total_runs > 0:
        for inn, runs in inning_totals.items():
            inning_pct[str(inn)] = round(runs / total_runs, 3)

    # Cluster score: avg runs per inning WHEN they score (ignore 0-run innings)
    scoring_innings = [r for g in games for r in g.get("inning_runs", {}).values() if r > 0]
    cluster_score   = round(sum(scoring_innings) / max(len(scoring_innings), 1), 2)

    # Comeback rate: win% when trailing after 5 innings
    trailing_5 = [g for g in games
                  if sum(list(g.get("inning_runs", {}).values())[:5]) < g["opp_score"]]
    comeback_rate = (round(sum(g["win"] for g in trailing_5) / len(trailing_5), 3)
                     if trailing_5 else None)

    # Blowout rate: % of games decided by 5+
    blowout_rate = round(sum(1 for g in games if abs(g["margin"]) >= 5) / total_games, 3)

    return {
        "inning_run_distribution": inning_pct,
        "cluster_score":           cluster_score,
        "comeback_rate":           comeback_rate,
        "blowout_rate":            blowout_rate,
    }


def _compute_travel_record(team_id: int, team_code: str) -> dict:
    """
    Calculate W-L after long road trips and west→east travel.
    Uses schedule from current season.
    """
    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={
                "sportId": 1, "teamId": team_id, "gameType": "R",
                "startDate": f"{SEASON}-03-01", "endDate": date.today().isoformat(),
                "hydrate": "team",
            },
            timeout=_TIMEOUT,
        )
        games = []
        for day in r.json().get("dates", []):
            for g in day.get("games", []):
                if g.get("status", {}).get("abstractGameState") != "Final":
                    continue
                teams  = g.get("teams", {})
                home_t = teams.get("home", {})
                away_t = teams.get("away", {})
                is_home = home_t.get("team", {}).get("id") == team_id

                opp_code = (away_t if is_home else home_t).get("team", {}).get("abbreviation", "")
                park_code = home_t.get("team", {}).get("abbreviation", opp_code)

                us_score  = (home_t if is_home else away_t).get("score", 0) or 0
                opp_score = (away_t if is_home else home_t).get("score", 0) or 0

                games.append({
                    "date":     day.get("date", ""),
                    "is_home":  is_home,
                    "park":     park_code,
                    "win":      1 if us_score > opp_score else 0,
                })

        # West to east: team's home park is east (lon > -100) and previous game was west
        home_coord  = _CITY_COORDS.get(team_code, (40.0, -75.0))
        is_east     = home_coord[1] > -100

        post_xe_wins = 0
        post_xe_total = 0
        post_long_trip_wins  = [0, 0, 0]  # game 6, 7, 8 of road trip
        post_long_trip_total = [0, 0, 0]

        road_streak = 0
        prev_park   = None

        for g in sorted(games, key=lambda x: x.get("date", "")):
            if g["is_home"]:
                if prev_park and prev_park != team_code:
                    prev_coord = _CITY_COORDS.get(prev_park, (40.0, -75.0))
                    # Was previous park west, home is east?
                    if prev_coord[1] < -100 and is_east:
                        post_xe_wins += g["win"]
                        post_xe_total += 1
                road_streak = 0
            else:
                road_streak += 1
                if road_streak in (6, 7, 8):
                    idx = road_streak - 6
                    post_long_trip_wins[idx]  += g["win"]
                    post_long_trip_total[idx] += 1
            prev_park = g.get("park", team_code)

        return {
            "west_to_east_wins":  post_xe_wins,
            "west_to_east_games": post_xe_total,
            "west_to_east_wpct":  round(post_xe_wins / post_xe_total, 3) if post_xe_total else None,
            "long_road_trip_g6_wpct": round(post_long_trip_wins[0] / post_long_trip_total[0], 3) if post_long_trip_total[0] else None,
            "long_road_trip_g7_wpct": round(post_long_trip_wins[1] / post_long_trip_total[1], 3) if post_long_trip_total[1] else None,
            "long_road_trip_g8_wpct": round(post_long_trip_wins[2] / post_long_trip_total[2], 3) if post_long_trip_total[2] else None,
        }
    except Exception:
        return {}


def _compute_manager_tendencies(team_id: int) -> dict:
    """
    Estimate manager pull timing, steal rate, IBB rate from boxscore data.
    """
    try:
        end_dt   = date.today()
        start_dt = end_dt - timedelta(days=30)
        r = _http_get(
            f"{STATSAPI}/teams/{team_id}/stats",
            params={"stats": "season", "group": "hitting", "season": SEASON},
            timeout=_TIMEOUT,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        st = splits[0].get("stat", {})
        games = int(st.get("gamesPlayed", 1) or 1)
        sb    = int(st.get("stolenBases", 0) or 0)
        ibb   = int(st.get("intentionalWalks", 0) or 0)
        return {
            "steal_rate_pg":    round(sb / games, 2) if games else 0,
            "ibb_rate_pg":      round(ibb / games, 2) if games else 0,
        }
    except Exception:
        return {}


def update_team_profile(team_id: int, team_code: str) -> dict:
    """Update team profile: run patterns, manager tendencies, travel."""
    today = date.today().isoformat()
    games   = _team_game_log_30(team_id)
    patterns = _compute_run_scoring_patterns(games)
    manager  = _compute_manager_tendencies(team_id)
    travel   = _compute_travel_record(team_id, team_code)

    # Physical fatigue metrics
    consecutive_days = 0
    for g in reversed(sorted(games, key=lambda x: x.get("date", ""))):
        consecutive_days += 1
        # Break if there's a gap (simplified — would need actual schedule)
        break  # placeholder: real consecutive days needs full schedule parsing

    day_games_7d = 0  # requires time-of-day data not in this endpoint

    extra_innings_14d = 0  # would need inning count per game

    data = {
        "patterns":           patterns,
        "manager_tendencies": manager,
        "travel":             travel,
    }

    _upsert_team_profile(team_code, today, data)
    _update_physical_fatigue(team_code, today, consecutive_days, day_games_7d, extra_innings_14d)

    return data


# ── SEQUENCE MEMORY ───────────────────────────────────────────────────────────

def update_sequence_memory(
    away_team: str, home_team: str,
    away_score: int, home_score: int,
    game_date: str,
):
    """
    Update series memory between two teams.
    Groups games into series by calendar proximity.
    """
    margin = abs(away_score - home_score)
    is_blowout = margin >= 5
    away_win   = away_score > home_score

    team_a, team_b = sorted([away_team, home_team])
    a_wins = 1 if (away_win and team_a == away_team) or (not away_win and team_a == home_team) else 0
    b_wins = 1 - a_wins

    with _mem_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sequence_memory (
                team_a        TEXT NOT NULL,
                team_b        TEXT NOT NULL,
                series_start  TEXT NOT NULL,
                team_a_wins   INTEGER DEFAULT 0,
                team_b_wins   INTEGER DEFAULT 0,
                total_games   INTEGER DEFAULT 0,
                blowout_flag  INTEGER DEFAULT 0,
                last_game_date TEXT,
                PRIMARY KEY (team_a, team_b, series_start)
            )
        """)
        # Find an open series (within 5 days)
        row = conn.execute("""
            SELECT series_start, total_games FROM sequence_memory
            WHERE team_a=? AND team_b=?
              AND last_game_date >= date(?, '-5 days')
            ORDER BY last_game_date DESC LIMIT 1
        """, (team_a, team_b, game_date)).fetchone()

        if row:
            conn.execute("""
                UPDATE sequence_memory
                SET team_a_wins = team_a_wins + ?,
                    team_b_wins = team_b_wins + ?,
                    total_games = total_games + 1,
                    blowout_flag = blowout_flag | ?,
                    last_game_date = ?
                WHERE team_a=? AND team_b=? AND series_start=?
            """, (a_wins, b_wins, int(is_blowout), game_date, team_a, team_b, row["series_start"]))
        else:
            conn.execute("""
                INSERT INTO sequence_memory
                (team_a, team_b, series_start, team_a_wins, team_b_wins,
                 total_games, blowout_flag, last_game_date)
                VALUES (?,?,?,?,?,1,?,?)
            """, (team_a, team_b, game_date, a_wins, b_wins, int(is_blowout), game_date))


def get_series_context(team_a: str, team_b: str) -> dict:
    """Return last 5 series context between two teams."""
    ta, tb = sorted([team_a, team_b])
    with _mem_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sequence_memory (
                team_a TEXT NOT NULL, team_b TEXT NOT NULL, series_start TEXT NOT NULL,
                team_a_wins INTEGER DEFAULT 0, team_b_wins INTEGER DEFAULT 0,
                total_games INTEGER DEFAULT 0, blowout_flag INTEGER DEFAULT 0,
                last_game_date TEXT,
                PRIMARY KEY (team_a, team_b, series_start)
            )
        """)
        rows = conn.execute("""
            SELECT * FROM sequence_memory
            WHERE team_a=? AND team_b=?
            ORDER BY series_start DESC LIMIT 5
        """, (ta, tb)).fetchall()

    series = [dict(r) for r in rows]
    if not series:
        return {"momentum": "neutral", "recent_series": []}

    # Series momentum: did ta or tb win more of last 3 series?
    ta_series_wins = sum(1 for s in series[:3] if s["team_a_wins"] > s["team_b_wins"])
    tb_series_wins = sum(1 for s in series[:3] if s["team_b_wins"] > s["team_a_wins"])
    if ta_series_wins > tb_series_wins:
        momentum = f"{ta}_dominant"
    elif tb_series_wins > ta_series_wins:
        momentum = f"{tb}_dominant"
    else:
        momentum = "even"

    return {"momentum": momentum, "recent_series": series}


# ── DB HELPERS ────────────────────────────────────────────────────────────────

def _upsert_sp_season_ip(name, pid, ip, gs, flag):
    with _mem_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sp_season_ip (
                pitcher_name TEXT NOT NULL, pitcher_id INTEGER,
                season INTEGER NOT NULL, cumulative_ip REAL DEFAULT 0,
                cumulative_gs INTEGER DEFAULT 0, ip_flag TEXT, updated_at TEXT,
                PRIMARY KEY (pitcher_name, season)
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO sp_season_ip
            (pitcher_name, pitcher_id, season, cumulative_ip, cumulative_gs, ip_flag, updated_at)
            VALUES (?,?,?,?,?,?,?)
        """, (name, pid, SEASON, ip, gs, flag, datetime.utcnow().isoformat()))


def _upsert_hitter_season_ab(name, pid, ab, flag):
    with _mem_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hitter_season_ab (
                player_name TEXT NOT NULL, player_id INTEGER,
                season INTEGER NOT NULL, cumulative_ab INTEGER DEFAULT 0,
                ab_flag TEXT, updated_at TEXT,
                PRIMARY KEY (player_name, season)
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO hitter_season_ab
            (player_name, player_id, season, cumulative_ab, ab_flag, updated_at)
            VALUES (?,?,?,?,?,?)
        """, (name, pid, SEASON, ab, flag, datetime.utcnow().isoformat()))


def _upsert_team_profile(team_code, stat_date, data):
    with _mem_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS team_profiles (
                team        TEXT NOT NULL,
                stat_date   TEXT NOT NULL,
                patterns_json TEXT,
                manager_json  TEXT,
                travel_json   TEXT,
                updated_at    TEXT,
                PRIMARY KEY (team, stat_date)
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO team_profiles
            (team, stat_date, patterns_json, manager_json, travel_json, updated_at)
            VALUES (?,?,?,?,?,?)
        """, (
            team_code, stat_date,
            json.dumps(data.get("patterns", {})),
            json.dumps(data.get("manager_tendencies", {})),
            json.dumps(data.get("travel", {})),
            datetime.utcnow().isoformat(),
        ))


def _update_physical_fatigue(team, stat_date, consec, day_games_7d, extra_inn_14d):
    with _mem_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS physical_fatigue (
                team            TEXT NOT NULL,
                stat_date       TEXT NOT NULL,
                consecutive_game_days INTEGER DEFAULT 0,
                day_games_last_7      INTEGER DEFAULT 0,
                extra_innings_last_14 INTEGER DEFAULT 0,
                updated_at      TEXT,
                PRIMARY KEY (team, stat_date)
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO physical_fatigue
            (team, stat_date, consecutive_game_days, day_games_last_7,
             extra_innings_last_14, updated_at)
            VALUES (?,?,?,?,?,?)
        """, (team, stat_date, consec, day_games_7d, extra_inn_14d,
              datetime.utcnow().isoformat()))


# ── NIGHTLY GAME UPDATE ───────────────────────────────────────────────────────

def run_nightly_profile_updates(game_pk: int, game_date: str,
                                 away_team: str, home_team: str,
                                 away_score: int, home_score: int,
                                 away_sp_id: int | None = None,
                                 home_sp_id: int | None = None,
                                 away_sp_name: str = "",
                                 home_sp_name: str = ""):
    """
    Called after every game settles.
    Updates SP profiles for starters, series memory, physical fatigue.
    Hitter profiles updated weekly (too slow for nightly individual fetches).
    """
    print(f"[PROFILE] Updating profiles for {away_team}@{home_team} (pk={game_pk})")

    # SP profiles
    if away_sp_id and away_sp_name:
        try:
            update_sp_profile(away_sp_name, away_sp_id)
            print(f"[PROFILE] SP updated: {away_sp_name}")
        except Exception as e:
            print(f"[PROFILE] SP error ({away_sp_name}): {e}")

    if home_sp_id and home_sp_name:
        try:
            update_sp_profile(home_sp_name, home_sp_id)
            print(f"[PROFILE] SP updated: {home_sp_name}")
        except Exception as e:
            print(f"[PROFILE] SP error ({home_sp_name}): {e}")

    # Sequence memory
    try:
        update_sequence_memory(away_team, home_team, away_score, home_score, game_date)
    except Exception as e:
        print(f"[PROFILE] Sequence error: {e}")


def run_weekly_team_updates(team_ids: dict):
    """
    Called every Sunday. Updates all team + bullpen profiles.
    team_ids: {team_code: team_id}
    """
    for code, tid in team_ids.items():
        try:
            update_team_profile(tid, code)
            update_bullpen_profile(tid, code)
            print(f"[PROFILE] Team updated: {code}")
        except Exception as e:
            print(f"[PROFILE] Team error ({code}): {e}")


if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "--sp"
    if cmd == "--sp":
        pid = int(sys.argv[2]) if len(sys.argv) > 2 else 554430
        name = sys.argv[3] if len(sys.argv) > 3 else "Test SP"
        p = update_sp_profile(name, pid)
        print(json.dumps({k: v for k, v in p.items() if k != "game_log_10"}, indent=2))
    elif cmd == "--hitter":
        pid  = int(sys.argv[2]) if len(sys.argv) > 2 else 545361
        name = sys.argv[3] if len(sys.argv) > 3 else "Mike Trout"
        p = update_hitter_profile(name, pid, "LAA")
        print(json.dumps({k: v for k, v in p.items()}, indent=2))
    elif cmd == "--series":
        ctx = get_series_context(sys.argv[2], sys.argv[3])
        print(json.dumps(ctx, indent=2))
