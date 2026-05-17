"""PARLAY OS — h2h_engine.py
Head-to-head historical matchup data from MLB Stats API.
Weight 10% in final model probability. Uses league average when <5 games found.
"""

from api_client import get as _http_get
from functools import lru_cache

STATSAPI  = "https://statsapi.mlb.com/api/v1"
_LG_TOTAL = 8.7  # league-average runs per game (used for over/under rate)


@lru_cache(maxsize=128)
def get_h2h_stats(away_tid: int, home_tid: int) -> dict:
    """
    Pull last 10 matchups between two teams across 2024-2026 seasons.
    Returns win rates, avg total, over/under rate from perspective of today's away team.
    Falls back to league average when fewer than 5 games found.
    """
    games = []
    for season in ("2024", "2025", "2026"):
        try:
            r = _http_get(
                f"{STATSAPI}/schedule",
                params={
                    "teamId":     away_tid,
                    "opponentId": home_tid,
                    "sportId":    1,
                    "gameType":   "R",
                    "season":     season,
                },
                timeout=10,
            )
            for d in r.json().get("dates", []):
                for g in d.get("games", []):
                    status = (g.get("status") or {}).get("detailedState", "")
                    if "Final" not in status:
                        continue
                    t = g.get("teams", {})
                    away_t = t.get("away", {})
                    home_t = t.get("home", {})
                    a_score = away_t.get("score")
                    h_score = home_t.get("score")
                    if a_score is None or h_score is None:
                        continue
                    a_tid_hist = away_t.get("team", {}).get("id")
                    games.append({
                        "date":       d["date"],
                        "away_tid":   a_tid_hist,
                        "away_score": int(a_score),
                        "home_score": int(h_score),
                        "total":      int(a_score) + int(h_score),
                    })
        except Exception:
            pass

    # Deduplicate by date, sort newest first, take last 10
    seen: set = set()
    unique = []
    for g in sorted(games, key=lambda x: x["date"], reverse=True):
        if g["date"] not in seen:
            seen.add(g["date"])
            unique.append(g)
    games = unique[:10]

    if len(games) < 5:
        return {
            "h2h_available": False,
            "n_games":       len(games),
            "away_win_rate": 0.5,
            "home_win_rate": 0.5,
            "avg_total":     _LG_TOTAL,
            "over_rate":     0.5,
        }

    away_wins = home_wins = total_sum = over_count = 0
    for g in games:
        a_score = g["away_score"]
        h_score = g["home_score"]
        # Map "away team today" to their historical side
        if g["away_tid"] == away_tid:
            if a_score > h_score:
                away_wins += 1
            else:
                home_wins += 1
        else:
            # Today's away team was the home team in this historical game
            if h_score > a_score:
                away_wins += 1
            else:
                home_wins += 1
        total_sum  += g["total"]
        if g["total"] > _LG_TOTAL:
            over_count += 1

    n = len(games)
    return {
        "h2h_available": True,
        "n_games":       n,
        "away_win_rate": round(away_wins / n, 3),
        "home_win_rate": round(home_wins / n, 3),
        "avg_total":     round(total_sum / n, 2),
        "over_rate":     round(over_count / n, 3),
    }
