"""PARLAY OS — bullpen_engine.py
Fetches bullpen usage from MLB Stats API, computes fatigue score and closer availability.
"""

import requests
from datetime import date, timedelta

STATSAPI = "https://statsapi.mlb.com/api/v1"


def _team_roster(team_id: int, game_date: str) -> list:
    """Return list of active pitchers (player_id, full_name, position)."""
    try:
        r = requests.get(
            f"{STATSAPI}/teams/{team_id}/roster/Active?date={game_date}",
            timeout=8
        )
        data   = r.json()
        roster = data.get("roster", [])
        pitchers = [
            {
                "id":       p["person"]["id"],
                "name":     p["person"]["fullName"],
                "position": p.get("position", {}).get("abbreviation", ""),
            }
            for p in roster
            if p.get("position", {}).get("abbreviation", "") in ("SP", "RP", "CL")
        ]
        return pitchers
    except Exception:
        return []


def _pitcher_game_log(pitcher_id: int, days: int = 5) -> list:
    """Return pitching game log for past N days."""
    try:
        end   = date.today()
        start = end - timedelta(days=days)
        url   = (
            f"{STATSAPI}/people/{pitcher_id}/stats"
            f"?stats=gameLog&group=pitching&season=2026"
            f"&startDate={start.isoformat()}&endDate={end.isoformat()}"
        )
        r = requests.get(url, timeout=8)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        games  = []
        for s in splits:
            st  = s.get("stat", {})
            ip_str = st.get("inningsPitched", "0.0")
            ip_parts = str(ip_str).split(".")
            ip = int(ip_parts[0]) + int(ip_parts[1] if len(ip_parts) > 1 else 0) / 3
            games.append({
                "date":  s.get("date", ""),
                "ip":    round(ip, 1),
                "np":    int(st.get("pitchesThrown", 0) or 0),
                "er":    int(st.get("earnedRuns", 0) or 0),
            })
        return games
    except Exception:
        return []


def _fatigue_score(games: list) -> float:
    """
    Fatigue score 0-1: higher = more fatigued.
    Weight: today-1 = 0.6, today-2 = 0.3, today-3 = 0.1.
    Threshold: 1 IP = 0.25 fatigue contribution, 2+ IP → near-full day.
    """
    today = date.today()
    weights = {1: 0.6, 2: 0.3, 3: 0.1}
    score   = 0.0
    for g in games:
        try:
            gd = date.fromisoformat(g["date"])
            days_ago = (today - gd).days
            if days_ago in weights:
                ip = g.get("ip", 0)
                # Full day = 1 IP; 2+ IP = extra fatigue
                contribution = min(ip / 1.0, 2.0) * 0.5
                score += weights[days_ago] * contribution
        except Exception:
            pass
    return round(min(score, 1.0), 3)


def _is_closer(p: dict, stats: list) -> bool:
    """Heuristic: position CL or high SV rate."""
    if p.get("position") == "CL":
        return True
    svs = sum(int(g.get("sv", 0)) for g in stats if "sv" in g)
    return svs >= 5


def analyze_bullpen(team_id: int, game_date: str, label: str = "") -> dict:
    """
    Full bullpen analysis for team_id on game_date.
    Returns fatigue tier, closer availability, top relievers.
    """
    roster  = _team_roster(team_id, game_date)
    rp_list = [p for p in roster if p["position"] in ("RP", "CL")]

    total_fatigue = 0.0
    fatigued_arms = 0
    closer_available = True
    closer_name  = ""
    heavy_usage  = []
    details      = []

    for p in rp_list:
        games  = _pitcher_game_log(p["id"], days=3)
        fscore = _fatigue_score(games)
        np3    = sum(g.get("np", 0) for g in games)

        details.append({
            "id":      p["id"],
            "name":    p["name"],
            "fatigue": fscore,
            "np_3d":   np3,
        })
        total_fatigue += fscore
        if fscore > 0.55:
            fatigued_arms += 1
        if fscore > 0.7:
            heavy_usage.append(p["name"])
        if p["position"] == "CL":
            closer_name = p["name"]
            if fscore > 0.6:
                closer_available = False

    n = len(rp_list) or 1
    avg_fatigue = round(total_fatigue / n, 3)

    if avg_fatigue < 0.25:
        fatigue_tier = "FRESH"
    elif avg_fatigue < 0.50:
        fatigue_tier = "MODERATE"
    else:
        fatigue_tier = "TIRED"

    return {
        "team":             label,
        "team_id":          team_id,
        "avg_fatigue":      avg_fatigue,
        "fatigue_tier":     fatigue_tier,
        "fatigued_arms":    fatigued_arms,
        "total_rp":         len(rp_list),
        "closer_available": closer_available,
        "closer_name":      closer_name,
        "heavy_usage":      heavy_usage,
        "arms":             sorted(details, key=lambda x: -x["fatigue"]),
    }


def bullpen_run_factor(bp: dict) -> float:
    """Convert bullpen fatigue to a run expectancy multiplier."""
    tier = bp.get("fatigue_tier", "MODERATE")
    base = {"FRESH": 0.97, "MODERATE": 1.0, "TIRED": 1.04}.get(tier, 1.0)
    # Add 0.005 per heavy-usage arm
    heavy = len(bp.get("heavy_usage", []))
    return round(base + heavy * 0.005, 4)


if __name__ == "__main__":
    import sys
    from constants import MLB_TEAM_IDS
    team = sys.argv[1].upper() if len(sys.argv) > 1 else "SF"
    tid  = MLB_TEAM_IDS.get(team, 137)
    from datetime import date
    bp = analyze_bullpen(tid, date.today().isoformat(), label=team)
    print(f"{team}: fatigue={bp['avg_fatigue']} tier={bp['fatigue_tier']} "
          f"closer={bp['closer_name']} avail={bp['closer_available']}")
    print(f"  run_factor={bullpen_run_factor(bp)}")
    if bp['heavy_usage']:
        print(f"  heavy usage: {', '.join(bp['heavy_usage'])}")
