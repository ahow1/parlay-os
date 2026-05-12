"""PARLAY OS — bullpen_engine.py
Fetches bullpen usage from MLB Stats API, computes fatigue score and closer availability.
Fatigue scale: 0–10 (10 = exhausted). Arms scoring ≥7 are flagged.
"""

import requests
from api_client import get as _http_get
from datetime import date, timedelta

STATSAPI = "https://statsapi.mlb.com/api/v1"


def _team_roster(team_id: int, game_date: str) -> list:
    """Return list of active pitchers (player_id, full_name, position)."""
    try:
        r = _http_get(
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
    """Return pitching game log entries for past N days, with pitch counts."""
    try:
        end   = date.today()
        start = end - timedelta(days=days)
        url   = (
            f"{STATSAPI}/people/{pitcher_id}/stats"
            f"?stats=gameLog&group=pitching&season=2026"
            f"&startDate={start.isoformat()}&endDate={end.isoformat()}"
        )
        r = _http_get(url, timeout=8)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        games  = []
        for s in splits:
            st     = s.get("stat", {})
            ip_str = st.get("inningsPitched", "0.0")
            ip_parts = str(ip_str).split(".")
            ip = int(ip_parts[0]) + int(ip_parts[1] if len(ip_parts) > 1 else 0) / 3
            games.append({
                "date": s.get("date", ""),
                "ip":   round(ip, 1),
                "np":   int(st.get("pitchesThrown", 0) or 0),
                "er":   int(st.get("earnedRuns", 0) or 0),
            })
        return games
    except Exception:
        return []


def _fatigue_score(games: list) -> float:
    """
    Weighted pitch-count fatigue score, 0–10.
    Formula: pitches_yesterday × 1.0 + pitches_2_days_ago × 0.6 + pitches_3_days_ago × 0.3
    Normalised: 50 weighted pitch-points ≈ 10 (exhausted).
    Arms ≥ 7 are flagged as high-fatigue.
    """
    today   = date.today()
    weights = {1: 1.0, 2: 0.6, 3: 0.3}
    raw     = 0.0
    for g in games:
        try:
            gd       = date.fromisoformat(g["date"])
            days_ago = (today - gd).days
            if days_ago in weights:
                raw += weights[days_ago] * g.get("np", 0)
        except Exception:
            pass
    return round(min(raw / 5.0, 10.0), 1)


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
    Fatigue scores are on a 0–10 scale; arms ≥7 are flagged.
    """
    roster  = _team_roster(team_id, game_date)
    rp_list = [p for p in roster if p["position"] in ("RP", "CL")]

    total_fatigue    = 0.0
    fatigued_arms    = 0     # arms ≥5 (moderate+)
    high_fatigue_arms = []   # arms ≥7 (flagged)
    closer_available = True
    closer_name      = ""
    details          = []

    for p in rp_list:
        games  = _pitcher_game_log(p["id"], days=3)
        fscore = _fatigue_score(games)
        np3    = sum(g.get("np", 0) for g in games)

        details.append({
            "id":      p["id"],
            "name":    p["name"],
            "fatigue": fscore,       # 0–10 scale
            "np_3d":   np3,
            "flagged": fscore >= 7.0,
        })
        total_fatigue += fscore
        if fscore >= 5.0:
            fatigued_arms += 1
        if fscore >= 7.0:
            high_fatigue_arms.append(p["name"])
        if p["position"] == "CL":
            closer_name = p["name"]
            if fscore >= 6.0:
                closer_available = False

    n = len(rp_list) or 1
    avg_fatigue = round(total_fatigue / n, 1)

    if avg_fatigue < 2.5:
        fatigue_tier = "FRESH"
    elif avg_fatigue < 5.0:
        fatigue_tier = "MODERATE"
    else:
        fatigue_tier = "TIRED"

    return {
        "team":               label,
        "team_id":            team_id,
        "avg_fatigue":        avg_fatigue,       # 0–10
        "fatigue_tier":       fatigue_tier,
        "fatigued_arms":      fatigued_arms,
        "high_fatigue_arms":  high_fatigue_arms, # arms flagged ≥7
        "total_rp":           len(rp_list),
        "closer_available":   closer_available,
        "closer_name":        closer_name,
        # legacy alias kept for backward compat
        "heavy_usage":        high_fatigue_arms,
        "arms":               sorted(details, key=lambda x: -x["fatigue"]),
    }


def bullpen_run_factor(bp: dict) -> float:
    """Convert bullpen fatigue to a run expectancy multiplier."""
    tier  = bp.get("fatigue_tier", "MODERATE")
    base  = {"FRESH": 0.97, "MODERATE": 1.0, "TIRED": 1.04}.get(tier, 1.0)
    high  = len(bp.get("high_fatigue_arms", []))
    return round(base + high * 0.005, 4)


if __name__ == "__main__":
    import sys
    from constants import MLB_TEAM_IDS
    team = sys.argv[1].upper() if len(sys.argv) > 1 else "SF"
    tid  = MLB_TEAM_IDS.get(team, 137)
    from datetime import date
    bp = analyze_bullpen(tid, date.today().isoformat(), label=team)
    print(f"{team}: avg_fatigue={bp['avg_fatigue']}/10 tier={bp['fatigue_tier']} "
          f"closer={bp['closer_name']} avail={bp['closer_available']}")
    print(f"  run_factor={bullpen_run_factor(bp)}")
    if bp["high_fatigue_arms"]:
        print(f"  HIGH FATIGUE (≥7): {', '.join(bp['high_fatigue_arms'])}")
    for arm in bp["arms"][:5]:
        flag = " ⚠ FLAGGED" if arm["flagged"] else ""
        print(f"  {arm['name']}: {arm['fatigue']}/10 ({arm['np_3d']} pitches/3d){flag}")
