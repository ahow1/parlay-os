"""PARLAY OS — sp_engine.py
Fetches SP stats from MLB Stats API and applies ABS / TTOP / platoon adjustments.
"""

import requests
from constants import (
    MLB_TEAM_IDS, UMPIRE_TENDENCIES,
    ABS_COMMAND_BONUS, ABS_FB_HEAVY_MALUS,
    TEAM_LHB_PCT, PLATOON_WRCPLUS_DELTA,
)

STATSAPI = "https://statsapi.mlb.com/api/v1"


def _get_probable_pitchers(game_pk: int) -> dict:
    """Returns {away_pitcher_id, home_pitcher_id, away_pitcher_name, home_pitcher_name}."""
    try:
        r = requests.get(f"{STATSAPI}/game/{game_pk}/linescore", timeout=8)
        data = r.json()
        # Try boxscore for probable pitchers
        r2 = requests.get(f"{STATSAPI}/game/{game_pk}/boxscore", timeout=8)
        box = r2.json()
        teams = box.get("teams", {})
        result = {}
        for side in ("away", "home"):
            t = teams.get(side, {})
            pitchers = t.get("pitchers", [])
            info = t.get("players", {})
            # Try probablePitcher field from schedule
            pp = t.get("probablePitcher") or {}
            result[f"{side}_id"]   = pp.get("id")
            result[f"{side}_name"] = pp.get("fullName", "")
        return result
    except Exception:
        return {}


def _pitcher_season_stats(pitcher_id: int) -> dict:
    """Pull current-season pitching stats from Stats API."""
    try:
        url = f"{STATSAPI}/people/{pitcher_id}/stats?stats=season&group=pitching&season=2026"
        r   = requests.get(url, timeout=8)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s = splits[0].get("stat", {})
        ip_str  = s.get("inningsPitched", "0.0")
        ip_parts= str(ip_str).split(".")
        ip      = int(ip_parts[0]) + int(ip_parts[1] if len(ip_parts)>1 else 0) / 3
        era     = float(s.get("era", 4.35) or 4.35)
        whip    = float(s.get("whip", 1.30) or 1.30)
        k9      = float(s.get("strikeoutsPer9Inn", 8.5) or 8.5)
        bb9     = float(s.get("walksPer9Inn", 3.0) or 3.0)
        hr9     = float(s.get("homeRunsPer9", 1.2) or 1.2)
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


def _pitcher_meta(pitcher_id: int) -> dict:
    """Pull handedness and pitch mix from Stats API."""
    try:
        r = requests.get(f"{STATSAPI}/people/{pitcher_id}?hydrate=pitchingStats", timeout=8)
        person = r.json().get("people", [{}])[0]
        hand   = person.get("pitchHand", {}).get("code", "R")
        return {"hand": hand}
    except Exception:
        return {"hand": "R"}


def _xfip_estimate(era: float, bb9: float, k9: float, hr9: float) -> float:
    """Rough xFIP from available stats. xFIP ≈ ((13*lgHR_FB) + (3*BB) - (2*K)) / IP + cFIP."""
    # Simplified: use FIP components with league-average HR/FB (~12%)
    # FIP = (13*HR9 + 3*BB9 - 2*K9) / 1 + cFIP (cFIP ≈ 3.17 to put on ERA scale)
    fip = (13 * hr9 + 3 * bb9 - 2 * k9) / 9 + 3.17
    # xFIP replaces HR9 with league-average HR/FB; estimate ≈ 0.8 * HR9 for mid-tier
    xfip = (13 * (hr9 * 0.85) + 3 * bb9 - 2 * k9) / 9 + 3.17
    return round(xfip, 2)


def _ttop_flag(stats: dict) -> bool:
    """True if SP is likely to turn lineup over a 3rd time (TTOP risk)."""
    # Approximate: SPs with IP/GS < 5 or ERA > 5 likely won't face TTO
    gs = stats.get("gs", 0)
    ip = stats.get("ip", 0)
    if gs == 0:
        return False
    ip_per_start = ip / gs
    return ip_per_start >= 5.5


def _abs_adjustment(bb9: float, k9: float, hand: str) -> float:
    """ABS robot umpire: command SPs get edge, fastball-heavy SPs lose edge."""
    # Command proxy: low BB9 (<2.5) = command pitcher
    # Fastball-heavy proxy: high K9 but not swing-and-miss (we use bb9 < 2 and k9 > 10 as power combo)
    if bb9 < 2.5:
        return ABS_COMMAND_BONUS
    if bb9 > 3.5 and k9 < 7.5:
        return ABS_FB_HEAVY_MALUS
    return 0.0


def _platoon_run_factor(sp_hand: str, opp_team: str) -> float:
    """Adjust run expectancy based on SP handedness vs opponent LHB%."""
    lhb_pct = TEAM_LHB_PCT.get(opp_team, 0.43)
    rhb_pct = 1.0 - lhb_pct
    # vs LHB platoon delta (pitcher faces LHB)
    lhb_delta = PLATOON_WRCPLUS_DELTA.get((sp_hand, "L"), 0)
    rhb_delta  = PLATOON_WRCPLUS_DELTA.get((sp_hand, "R"), 0)
    # Weighted average wRC+ delta; convert from wRC+ points to run factor
    # wRC+ 100 = average; each point ≈ 1% of average offensive output
    avg_delta = lhb_pct * lhb_delta + rhb_pct * rhb_delta
    return round(1.0 + avg_delta / 100, 4)


def analyze_sp(pitcher_id: int, opp_team: str, umpire: str = "") -> dict:
    """Full SP analysis: stats + adjustments + run_factor."""
    stats = _pitcher_season_stats(pitcher_id)
    meta  = _pitcher_meta(pitcher_id)

    if not stats:
        return _default_sp(pitcher_id, opp_team, umpire)

    hand = meta.get("hand", "R")
    era  = stats.get("era", 4.35)
    k9   = stats.get("k9", 8.5)
    bb9  = stats.get("bb9", 3.0)
    hr9  = stats.get("hr9", 1.2)
    xfip = _xfip_estimate(era, bb9, k9, hr9)

    ump_k, ump_run, ump_note = UMPIRE_TENDENCIES.get(umpire, (1.0, 1.0, ""))
    abs_adj   = _abs_adjustment(bb9, k9, hand)
    plat_rf   = _platoon_run_factor(hand, opp_team)
    ttop      = _ttop_flag(stats)

    # Combined run factor for this SP vs this opponent in this context
    # Higher = more runs against this SP
    run_factor = round(ump_run * plat_rf * (1.0 + abs_adj), 4)

    return {
        "pitcher_id":  pitcher_id,
        "hand":        hand,
        "era":         era,
        "xfip":        xfip,
        "k9":          k9,
        "bb9":         bb9,
        "hr9":         hr9,
        "ip":          stats.get("ip", 0),
        "gs":          stats.get("gs", 0),
        "ttop":        ttop,
        "ump_k_factor":  ump_k,
        "ump_run_factor":ump_run,
        "ump_note":    ump_note,
        "abs_adj":     abs_adj,
        "plat_run_factor": plat_rf,
        "run_factor":  run_factor,
    }


def _default_sp(pitcher_id, opp_team, umpire) -> dict:
    ump_k, ump_run, ump_note = UMPIRE_TENDENCIES.get(umpire, (1.0, 1.0, ""))
    return {
        "pitcher_id": pitcher_id,
        "hand":       "R",
        "era":        4.35,
        "xfip":       4.35,
        "k9":         8.5,
        "bb9":        3.0,
        "hr9":        1.2,
        "ip":         0,
        "gs":         0,
        "ttop":       False,
        "ump_k_factor":  ump_k,
        "ump_run_factor":ump_run,
        "ump_note":    ump_note,
        "abs_adj":    0.0,
        "plat_run_factor": 1.0,
        "run_factor":  ump_run,
    }


def get_game_sps(game_pk: int, away_team: str, home_team: str, umpire: str = "") -> dict:
    """Return SP analysis for both starters in a game."""
    pp = _get_probable_pitchers(game_pk)
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
    print(r)
