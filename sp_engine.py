"""PARLAY OS — sp_engine.py
Fetches SP stats from MLB Stats API and applies ABS / TTOP / platoon adjustments.
Includes last-3-start rolling ERA/K/BB and K-rate velocity-trend proxy.
"""

import requests
from constants import (
    MLB_TEAM_IDS, UMPIRE_TENDENCIES,
    ABS_COMMAND_BONUS, ABS_FB_HEAVY_MALUS,
    TEAM_LHB_PCT, PLATOON_WRCPLUS_DELTA,
)

STATSAPI = "https://statsapi.mlb.com/api/v1"


def _get_probable_pitchers(game_pk: int) -> dict:
    """Returns {away_id, home_id, away_name, home_name} from boxscore/schedule."""
    try:
        r2 = requests.get(f"{STATSAPI}/game/{game_pk}/boxscore", timeout=8)
        box = r2.json()
        teams = box.get("teams", {})
        result = {}
        for side in ("away", "home"):
            t  = teams.get(side, {})
            pp = t.get("probablePitcher") or {}
            result[f"{side}_id"]   = pp.get("id")
            result[f"{side}_name"] = pp.get("fullName", "")
        return result
    except Exception:
        return {}


def _pitcher_season_stats(pitcher_id: int) -> dict:
    """Pull current-season pitching stats from Stats API."""
    try:
        r = requests.get(
            f"{STATSAPI}/people/{pitcher_id}/stats",
            params={"stats": "season", "group": "pitching", "season": "2026"},
            timeout=8,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s = splits[0].get("stat", {})
        ip_str   = s.get("inningsPitched", "0.0")
        ip_parts = str(ip_str).split(".")
        ip  = int(ip_parts[0]) + int(ip_parts[1] if len(ip_parts) > 1 else 0) / 3
        era = float(s.get("era", 4.35) or 4.35)
        whip= float(s.get("whip", 1.30) or 1.30)
        k9  = float(s.get("strikeoutsPer9Inn", 8.5) or 8.5)
        bb9 = float(s.get("walksPer9Inn", 3.0) or 3.0)
        hr9 = float(s.get("homeRunsPer9", 1.2) or 1.2)
        return {
            "ip":   round(ip, 1),
            "era":  era,
            "whip": whip,
            "k9":   k9,
            "bb9":  bb9,
            "hr9":  hr9,
            "gs":   int(s.get("gamesStarted", 0) or 0),
        }
    except Exception:
        return {}


def _pitcher_game_log_starts(pitcher_id: int, n: int = 10) -> list:
    """
    Pull last N game starts for a pitcher from MLB Stats API game log.
    Returns list of per-game stat dicts, sorted oldest→newest.
    """
    try:
        r = requests.get(
            f"{STATSAPI}/people/{pitcher_id}/stats",
            params={
                "stats":    "gameLog",
                "group":    "pitching",
                "season":   "2026",
                "gameType": "R",
            },
            timeout=10,
        )
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        starts = []
        for s in splits:
            stat = s.get("stat", {})
            if int(stat.get("gamesStarted", 0) or 0) == 0:
                continue
            ip_str   = stat.get("inningsPitched", "0.0")
            ip_parts = str(ip_str).split(".")
            ip = int(ip_parts[0]) + int(ip_parts[1] if len(ip_parts) > 1 else 0) / 3
            if ip <= 0:
                continue
            er = int(stat.get("earnedRuns", 0) or 0)
            k  = int(stat.get("strikeOuts", 0) or 0)
            bb = int(stat.get("baseOnBalls", 0) or 0)
            np = int(stat.get("pitchesThrown", 0) or 0)
            starts.append({
                "date": s.get("date", ""),
                "ip":   round(ip, 2),
                "era":  round(er / ip * 9, 2),
                "k9":   round(k / ip * 9, 2),
                "bb9":  round(bb / ip * 9, 2),
                "np":   np,
                "er":   er,
            })
        return starts[-n:]   # last n starts, oldest first
    except Exception:
        return []


def _last_3_starts_summary(pitcher_id: int) -> dict:
    """
    Rolling stats over last 3 starts, plus trend flags.
    Velocity trend uses K-rate as proxy (velocity data requires Statcast).
    """
    starts = _pitcher_game_log_starts(pitcher_id, n=10)
    if not starts:
        return {}

    last3 = starts[-3:]
    if not last3:
        return {}

    eras = [g["era"] for g in last3]
    k9s  = [g["k9"]  for g in last3]
    bb9s = [g["bb9"] for g in last3]

    rolling_era = round(sum(eras) / len(eras), 2)
    rolling_k9  = round(sum(k9s)  / len(k9s), 2)
    rolling_bb9 = round(sum(bb9s) / len(bb9s), 2)

    # Worsening walk rate flag: BB/9 increased by >1.0 from earliest to latest start
    worsening_walk = (len(bb9s) >= 2) and (bb9s[-1] - bb9s[0] > 1.0)

    # Velocity trend proxy from K rate (Statcast velocity added in statcast_engine)
    # K9 drop >1.5 over last 3 suggests potential velocity decline
    k9_declining = (len(k9s) >= 2) and (k9s[0] - k9s[-1]) > 1.5

    result = {
        "rolling_era_3": rolling_era,
        "rolling_k9_3":  rolling_k9,
        "rolling_bb9_3": rolling_bb9,
        "n_starts":      len(last3),
        "worsening_walk": worsening_walk,
        "k9_declining":  k9_declining,
        "game_log_3":    last3,
    }

    # Velocity trend from last 10 starts K rate trend
    if len(starts) >= 5:
        first_half  = starts[:len(starts)//2]
        second_half = starts[len(starts)//2:]
        k9_early = sum(g["k9"] for g in first_half) / len(first_half)
        k9_late  = sum(g["k9"] for g in second_half) / len(second_half)
        k9_trend = round(k9_late - k9_early, 2)      # negative = declining K rate
        result["k9_trend_10s"]        = k9_trend
        result["velocity_decline"]    = k9_trend < -0.5   # flag: proxy for ~0.5 mph drop
        result["velocity_injury_risk"]= k9_trend < -1.0   # flag: proxy for ~1.0 mph drop
    else:
        result["k9_trend_10s"]         = 0.0
        result["velocity_decline"]     = False
        result["velocity_injury_risk"] = False

    return result


def _pitcher_meta(pitcher_id: int) -> dict:
    """Pull handedness from Stats API."""
    try:
        r = requests.get(
            f"{STATSAPI}/people/{pitcher_id}?hydrate=pitchingStats",
            timeout=8,
        )
        person = r.json().get("people", [{}])[0]
        hand   = person.get("pitchHand", {}).get("code", "R")
        return {"hand": hand}
    except Exception:
        return {"hand": "R"}


def _xfip_estimate(era: float, bb9: float, k9: float, hr9: float) -> float:
    """Rough xFIP from available stats."""
    xfip = (13 * (hr9 * 0.85) + 3 * bb9 - 2 * k9) / 9 + 3.17
    return round(xfip, 2)


def _ttop_flag(stats: dict) -> bool:
    """True if SP is likely to turn lineup over a 3rd time (TTOP risk)."""
    gs = stats.get("gs", 0)
    ip = stats.get("ip", 0)
    if gs == 0:
        return False
    return (ip / gs) >= 5.5


def _abs_adjustment(bb9: float, k9: float, hand: str) -> float:
    """ABS robot umpire: command SPs get edge, fastball-heavy SPs lose edge."""
    if bb9 < 2.5:
        return ABS_COMMAND_BONUS
    if bb9 > 3.5 and k9 < 7.5:
        return ABS_FB_HEAVY_MALUS
    return 0.0


def _platoon_run_factor(sp_hand: str, opp_team: str) -> float:
    """Adjust run expectancy based on SP handedness vs opponent LHB%."""
    lhb_pct   = TEAM_LHB_PCT.get(opp_team, 0.43)
    rhb_pct   = 1.0 - lhb_pct
    lhb_delta = PLATOON_WRCPLUS_DELTA.get((sp_hand, "L"), 0)
    rhb_delta = PLATOON_WRCPLUS_DELTA.get((sp_hand, "R"), 0)
    avg_delta = lhb_pct * lhb_delta + rhb_pct * rhb_delta
    return round(1.0 + avg_delta / 100, 4)


def analyze_sp(pitcher_id: int, opp_team: str, umpire: str = "") -> dict:
    """Full SP analysis: season stats + last-3-start rolling + adjustments."""
    stats  = _pitcher_season_stats(pitcher_id)
    meta   = _pitcher_meta(pitcher_id)
    last3  = _last_3_starts_summary(pitcher_id) if pitcher_id else {}

    if not stats:
        return _default_sp(pitcher_id, opp_team, umpire)

    hand = meta.get("hand", "R")
    era  = stats.get("era", 4.35)
    k9   = stats.get("k9", 8.5)
    bb9  = stats.get("bb9", 3.0)
    hr9  = stats.get("hr9", 1.2)
    xfip = _xfip_estimate(era, bb9, k9, hr9)

    # Use last-3-start ERA if available and significantly different from season ERA
    effective_era = era
    if last3.get("rolling_era_3") is not None:
        r3_era = last3["rolling_era_3"]
        # Weight: 60% season, 40% last 3 starts
        effective_era = round(0.60 * era + 0.40 * r3_era, 2)

    ump_k, ump_run, ump_note = UMPIRE_TENDENCIES.get(umpire, (1.0, 1.0, ""))
    abs_adj  = _abs_adjustment(bb9, k9, hand)
    plat_rf  = _platoon_run_factor(hand, opp_team)
    ttop     = _ttop_flag(stats)

    # Run factor: higher = more runs scored against this SP
    run_factor = round(ump_run * plat_rf * (1.0 + abs_adj), 4)

    return {
        "pitcher_id":       pitcher_id,
        "hand":             hand,
        "era":              era,
        "effective_era":    effective_era,
        "xfip":             xfip,
        "k9":               k9,
        "bb9":              bb9,
        "hr9":              hr9,
        "ip":               stats.get("ip", 0),
        "gs":               stats.get("gs", 0),
        "ttop":             ttop,
        "ump_k_factor":     ump_k,
        "ump_run_factor":   ump_run,
        "ump_note":         ump_note,
        "abs_adj":          abs_adj,
        "plat_run_factor":  plat_rf,
        "run_factor":       run_factor,
        # Last-3-start rolling stats
        "rolling_era_3":    last3.get("rolling_era_3"),
        "rolling_k9_3":     last3.get("rolling_k9_3"),
        "rolling_bb9_3":    last3.get("rolling_bb9_3"),
        "worsening_walk":   last3.get("worsening_walk", False),
        "k9_declining":     last3.get("k9_declining", False),
        # Velocity trend (K-rate proxy; true velocity requires Statcast)
        "velocity_decline":     last3.get("velocity_decline", False),
        "velocity_injury_risk": last3.get("velocity_injury_risk", False),
        "k9_trend_10s":         last3.get("k9_trend_10s", 0.0),
    }


def _default_sp(pitcher_id, opp_team, umpire) -> dict:
    ump_k, ump_run, ump_note = UMPIRE_TENDENCIES.get(umpire, (1.0, 1.0, ""))
    return {
        "pitcher_id":       pitcher_id,
        "hand":             "R",
        "era":              4.35,
        "effective_era":    4.35,
        "xfip":             4.35,
        "k9":               8.5,
        "bb9":              3.0,
        "hr9":              1.2,
        "ip":               0,
        "gs":               0,
        "ttop":             False,
        "ump_k_factor":     ump_k,
        "ump_run_factor":   ump_run,
        "ump_note":         ump_note,
        "abs_adj":          0.0,
        "plat_run_factor":  1.0,
        "run_factor":       ump_run,
        "rolling_era_3":    None,
        "rolling_k9_3":     None,
        "rolling_bb9_3":    None,
        "worsening_walk":   False,
        "k9_declining":     False,
        "velocity_decline": False,
        "velocity_injury_risk": False,
        "k9_trend_10s":     0.0,
    }


def get_game_sps(game_pk: int, away_team: str, home_team: str, umpire: str = "") -> dict:
    """Return SP analysis for both starters in a game."""
    pp        = _get_probable_pitchers(game_pk)
    away_id   = pp.get("away_id")
    home_id   = pp.get("home_id")
    away_name = pp.get("away_name", "TBD")
    home_name = pp.get("home_name", "TBD")

    away_sp = analyze_sp(away_id, home_team, umpire) if away_id else _default_sp(None, home_team, umpire)
    home_sp = analyze_sp(home_id, away_team, umpire) if home_id else _default_sp(None, away_team, umpire)

    away_sp["name"] = away_name
    home_sp["name"] = home_name

    return {"away": away_sp, "home": home_sp}


if __name__ == "__main__":
    # Quick test: Zack Wheeler ID = 554430
    r = analyze_sp(554430, "NYM", "Vic Carapazza")
    print(f"ERA={r['era']}  xFIP={r['xfip']}  effective_era={r['effective_era']}")
    print(f"rolling_era_3={r['rolling_era_3']}  rolling_k9_3={r['rolling_k9_3']}")
    print(f"worsening_walk={r['worsening_walk']}  velocity_decline={r['velocity_decline']}")
    print(f"run_factor={r['run_factor']}")
