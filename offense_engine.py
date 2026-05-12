"""PARLAY OS — offense_engine.py
Fetches team offensive stats, lineup confirmation, platoon splits, RISP, recent form.
"""

import requests
from constants import MLB_TEAM_IDS, TEAM_LHB_PCT, LG_RPG

STATSAPI = "https://statsapi.mlb.com/api/v1"


def _team_hitting_stats(team_id: int) -> dict:
    """Season team hitting stats from Stats API."""
    try:
        url = (
            f"{STATSAPI}/teams/{team_id}/stats"
            f"?stats=season&group=hitting&season=2026"
        )
        r      = requests.get(url, timeout=8)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s = splits[0].get("stat", {})
        return {
            "avg":      float(s.get("avg", 0.250) or 0.250),
            "obp":      float(s.get("obp", 0.320) or 0.320),
            "slg":      float(s.get("slg", 0.410) or 0.410),
            "ops":      float(s.get("ops", 0.730) or 0.730),
            "wrc_plus": None,  # Not in Stats API — computed from OPS proxy
            "runs":     int(s.get("runs", 0) or 0),
            "games":    int(s.get("gamesPlayed", 1) or 1),
        }
    except Exception:
        return {}


def _wrc_plus_proxy(ops: float) -> float:
    """Approximate wRC+ from OPS (league average OPS ≈ 0.730 → wRC+ 100)."""
    if ops <= 0:
        return 100.0
    return round((ops / 0.730) * 100, 1)


def _recent_form(team_id: int, last_n: int = 7) -> dict:
    """Runs/game and win/loss over last N games."""
    try:
        url = (
            f"{STATSAPI}/teams/{team_id}/stats"
            f"?stats=lastXGames&group=hitting&season=2026&gameType=R&limit={last_n}"
        )
        r      = requests.get(url, timeout=8)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s = splits[0].get("stat", {})
        games = int(s.get("gamesPlayed", last_n) or last_n)
        runs  = int(s.get("runs", 0) or 0)
        return {
            "games":         games,
            "runs":          runs,
            "runs_per_game": round(runs / games, 2) if games else LG_RPG,
        }
    except Exception:
        return {}


def _risp_stats(team_id: int) -> dict:
    """Runners in scoring position OPS — proxy from splits."""
    try:
        url = (
            f"{STATSAPI}/teams/{team_id}/stats"
            f"?stats=statSplits&group=hitting&season=2026&sitCodes=RISP"
        )
        r      = requests.get(url, timeout=8)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            return {}
        s   = splits[0].get("stat", {})
        ops = float(s.get("ops", 0.730) or 0.730)
        avg = float(s.get("avg", 0.250) or 0.250)
        return {"risp_avg": avg, "risp_ops": ops}
    except Exception:
        return {}


def _lineup_from_boxscore(game_pk: int, side: str) -> list:
    """Extract confirmed batting order from today's boxscore (if posted)."""
    try:
        r   = requests.get(f"{STATSAPI}/game/{game_pk}/boxscore", timeout=8)
        box = r.json()
        t   = box.get("teams", {}).get(side, {})
        # batters in batting order
        order = t.get("battingOrder", [])
        info  = t.get("players", {})
        lineup = []
        for pid in order:
            key    = f"ID{pid}"
            player = info.get(key, {}).get("person", {})
            lineup.append({
                "id":   pid,
                "name": player.get("fullName", ""),
            })
        return lineup
    except Exception:
        return []


def _lineup_confirmation(game_pk: int, side: str) -> bool:
    """True if lineup has been posted (batting order exists)."""
    return len(_lineup_from_boxscore(game_pk, side)) > 0


def analyze_offense(team_code: str, game_pk: int = None, side: str = "away",
                    opp_sp_hand: str = "R") -> dict:
    """
    Full offensive analysis for a team.
    opp_sp_hand: handedness of opposing SP for platoon adjustment.
    """
    team_id = MLB_TEAM_IDS.get(team_code)
    if not team_id:
        return _default_offense(team_code)

    hitting = _team_hitting_stats(team_id)
    if not hitting:
        return _default_offense(team_code)

    ops      = hitting.get("ops", 0.730)
    wrc_plus = _wrc_plus_proxy(ops)
    form     = _recent_form(team_id)
    risp     = _risp_stats(team_id)

    lineup_confirmed = False
    lineup = []
    if game_pk:
        lineup_confirmed = _lineup_confirmation(game_pk, side)
        if lineup_confirmed:
            lineup = _lineup_from_boxscore(game_pk, side)

    # Platoon adjustment: wRC+ proxy shift based on LHB% vs SP hand
    lhb_pct = TEAM_LHB_PCT.get(team_code, 0.43)
    # For each batter type, get typical platoon boost/penalty
    # Simplified: use team LHB% to weight the handedness advantage
    from constants import PLATOON_WRCPLUS_DELTA
    lhb_delta = PLATOON_WRCPLUS_DELTA.get(("L", opp_sp_hand), 0)
    rhb_delta = PLATOON_WRCPLUS_DELTA.get(("R", opp_sp_hand), 0)
    platoon_delta = lhb_pct * lhb_delta + (1 - lhb_pct) * rhb_delta
    adj_wrc_plus  = round(wrc_plus + platoon_delta, 1)

    # Run expectancy factor: (wRC+/100) represents offensive output vs average
    run_factor = round(adj_wrc_plus / 100, 4)

    # Recent form adjustment: if last 7 rpg >> season average, bump slightly
    rpg_recent = form.get("runs_per_game", LG_RPG)
    rpg_season = hitting.get("runs", 0) / max(hitting.get("games", 1), 1)
    if rpg_recent > 0 and rpg_season > 0:
        form_adj = min((rpg_recent / rpg_season) ** 0.25, 1.10)
        run_factor = round(run_factor * form_adj, 4)

    return {
        "team":              team_code,
        "avg":               hitting.get("avg"),
        "obp":               hitting.get("obp"),
        "slg":               hitting.get("slg"),
        "ops":               ops,
        "wrc_plus":          wrc_plus,
        "adj_wrc_plus":      adj_wrc_plus,
        "platoon_delta":     round(platoon_delta, 1),
        "risp_avg":          risp.get("risp_avg"),
        "risp_ops":          risp.get("risp_ops"),
        "rpg_recent":        rpg_recent,
        "rpg_season":        round(rpg_season, 2),
        "run_factor":        run_factor,
        "lineup_confirmed":  lineup_confirmed,
        "lineup":            lineup,
        "lhb_pct":           lhb_pct,
    }


def _default_offense(team_code: str) -> dict:
    return {
        "team":             team_code,
        "avg":              0.250,
        "obp":              0.320,
        "slg":              0.410,
        "ops":              0.730,
        "wrc_plus":         100.0,
        "adj_wrc_plus":     100.0,
        "platoon_delta":    0.0,
        "risp_avg":         None,
        "risp_ops":         None,
        "rpg_recent":       LG_RPG,
        "rpg_season":       LG_RPG,
        "run_factor":       1.0,
        "lineup_confirmed": False,
        "lineup":           [],
        "lhb_pct":          0.43,
    }


if __name__ == "__main__":
    import sys
    team = sys.argv[1].upper() if len(sys.argv) > 1 else "SF"
    sp_h = sys.argv[2].upper() if len(sys.argv) > 2 else "R"
    off  = analyze_offense(team, opp_sp_hand=sp_h)
    print(f"{team} vs {sp_h}HP: wRC+={off['wrc_plus']} adj={off['adj_wrc_plus']} "
          f"rf={off['run_factor']} rpg7={off['rpg_recent']}")
    print(f"  risp_avg={off['risp_avg']} lineup_confirmed={off['lineup_confirmed']}")
