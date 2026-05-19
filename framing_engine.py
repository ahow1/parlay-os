"""PARLAY OS — framing_engine.py
Catcher framing edge detection via Baseball Savant framing leaderboard.

Elite framer (>+8 framing_runs): +0.02 win prob for pitching team.
Poor framer (<-8 framing_runs):  -0.02 win prob for pitching team.

Framing runs: extra strikes gained (positive) or lost (negative) per catcher
relative to average, converted to run value.
"""

import csv
import io
import threading

from api_client import get as _http_get

SAVANT_BASE = "https://baseballsavant.mlb.com"
TIMEOUT     = 20

ELITE_FRAMING_RUNS = 8.0    # above this = elite framer
POOR_FRAMING_RUNS  = -8.0   # below this = poor framer
FRAMING_PROB_ADJ   = 0.02   # probability adjustment magnitude

# Savant catcher framing leaderboard URL
_FRAMING_URL = (
    f"{SAVANT_BASE}/leaderboard/catcher-framing"
    f"?year=2026&team=&min=q&csv=true"
)

_framing_lock  = threading.Lock()
_framing_cache: dict | None = None   # {team_abbrev: framing_runs}


def _fetch_framing_leaderboard(season: int = 2026) -> dict:
    """
    Fetch Savant catcher framing leaderboard.
    Returns {team_abbrev: total_framing_runs} (summed across all catchers on team).
    """
    global _framing_cache
    with _framing_lock:
        if _framing_cache is not None:
            return _framing_cache

        result: dict = {}
        for try_year in ([season, season - 1] if season == 2026 else [season]):
            url = (
                f"{SAVANT_BASE}/leaderboard/catcher-framing"
                f"?year={try_year}&team=&min=q&csv=true"
            )
            try:
                r = _http_get(
                    url, timeout=TIMEOUT,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
                    skip_cache=False,
                )
                if r.status_code != 200 or not r.text.strip():
                    continue

                text   = r.text.lstrip("﻿")
                reader = csv.DictReader(io.StringIO(text))

                def _flt(v, d=0.0):
                    try:
                        return float(v) if v not in ("", "null", "NA", ".", None) else d
                    except (ValueError, TypeError):
                        return d

                team_totals: dict = {}
                for row in reader:
                    team = (
                        row.get("team_name") or row.get("team_abbrev") or
                        row.get("team") or ""
                    ).strip().upper()
                    if not team:
                        continue

                    framing = _flt(
                        row.get("framing_runs") or row.get("strike_rate_extra_strikes") or
                        row.get("extra_strikes") or row.get("runs_extra_strikes")
                    )
                    pa = _flt(row.get("pa") or row.get("innings_called") or row.get("n", "0"))
                    if pa == 0:
                        pa = 1  # avoid division by zero

                    if team not in team_totals:
                        team_totals[team] = {"framing": 0.0, "pa": 0.0}
                    team_totals[team]["framing"] += framing
                    team_totals[team]["pa"]      += pa

                if team_totals:
                    # Use the primary catcher (highest PA) per team, or team total
                    for team, data in team_totals.items():
                        result[team] = round(data["framing"], 2)
                    break  # success — don't fall back to prior year

            except Exception as e:
                print(f"  [FRAMING] Fetch error (year={try_year}): {e}")
                continue

        _framing_cache = result
        return result


def get_team_framing(team_code: str) -> float | None:
    """
    Return the team's aggregate catcher framing runs this season.
    Returns None if data unavailable.
    """
    leaderboard = _fetch_framing_leaderboard()
    code = team_code.upper()
    val = leaderboard.get(code) or leaderboard.get(code[:3])
    return val


def check_framing_edge(framing_runs: float | None, team_code: str = "") -> dict:
    """
    Evaluate catcher framing edge.

    Returns:
        prob_adj:      float — signed probability adjustment (+0.02, -0.02, or 0)
        tag:           str   — Telegram flag string (empty if no edge)
        framing_runs:  float or None
        is_elite:      bool
        is_poor:       bool
    """
    result = {
        "prob_adj":     0.0,
        "tag":          "",
        "framing_runs": framing_runs,
        "is_elite":     False,
        "is_poor":      False,
    }
    if framing_runs is None:
        return result

    if framing_runs >= ELITE_FRAMING_RUNS:
        result["prob_adj"] = FRAMING_PROB_ADJ
        result["is_elite"] = True
        result["tag"]      = (
            f"🧤 FRAMING EDGE: {team_code} +{framing_runs:.1f} framing runs — "
            f"elite catcher boosting pitcher strike zone"
        )
    elif framing_runs <= POOR_FRAMING_RUNS:
        result["prob_adj"] = -FRAMING_PROB_ADJ
        result["is_poor"]  = True
        result["tag"]      = (
            f"⚠ FRAMING RISK: {team_code} {framing_runs:.1f} framing runs — "
            f"poor framer costing pitchers strikes"
        )

    return result


if __name__ == "__main__":
    lb = _fetch_framing_leaderboard()
    print(f"Framing leaderboard: {len(lb)} teams")
    for team, runs in sorted(lb.items(), key=lambda x: -x[1])[:5]:
        edge = check_framing_edge(runs, team)
        print(f"  {team}: {runs:+.1f} runs → adj={edge['prob_adj']:+.3f} {edge['tag'] or ''}")
