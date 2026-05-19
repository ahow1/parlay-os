"""PARLAY OS — defense_engine.py
Team defensive metrics via Baseball Savant Outs Above Average (OAA).

OAA >+15: -0.2 expected runs allowed per game (elite range suppression)
OAA <-15: +0.2 expected runs allowed per game (poor defense leaks runs)

Source: Baseball Savant OAA team leaderboard, aggregated by team.
"""

import csv
import io
import threading

from api_client import get as _http_get

SAVANT_BASE = "https://baseballsavant.mlb.com"
TIMEOUT     = 20

ELITE_OAA = 15   # OAA > +15 = elite defense
POOR_OAA  = -15  # OAA < -15 = poor defense
OAA_RUN_ADJ = 0.20   # expected runs per game adjustment

_oaa_lock  = threading.Lock()
_oaa_cache: dict | None = None   # {team_abbrev: total_oaa}


def _fetch_oaa_leaderboard(season: int = 2026) -> dict:
    """
    Fetch Savant OAA leaderboard and aggregate by team.
    Returns {team_abbrev: total_oaa}.
    """
    global _oaa_cache
    with _oaa_lock:
        if _oaa_cache is not None:
            return _oaa_cache

        result: dict = {}
        for try_year in ([season, season - 1] if season == 2026 else [season]):
            url = (
                f"{SAVANT_BASE}/leaderboard/outs-above-average"
                f"?type=Fielder&year={try_year}&team=&csv=true"
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
                    oaa = _flt(
                        row.get("outs_above_average") or row.get("oaa") or
                        row.get("raw_outs_above_average")
                    )
                    team_totals[team] = team_totals.get(team, 0.0) + oaa

                if team_totals:
                    for team, oaa in team_totals.items():
                        result[team] = round(oaa, 1)
                    break   # success

            except Exception as e:
                print(f"  [DEFENSE] OAA fetch error (year={try_year}): {e}")
                continue

        _oaa_cache = result
        return result


def get_team_oaa(team_code: str) -> float | None:
    """
    Return the team's total Outs Above Average (season-to-date).
    Returns None if data unavailable.
    """
    leaderboard = _fetch_oaa_leaderboard()
    code = team_code.upper()
    val = leaderboard.get(code) or leaderboard.get(code[:3])
    return val


def check_defense_edge(oaa: float | None, team_code: str = "") -> dict:
    """
    Evaluate team defensive edge from OAA.

    Returns:
        run_adj:      float — signed expected-runs adjustment (negative = fewer runs allowed)
        tag:          str   — Telegram flag string (empty if no edge)
        oaa:          float or None
        is_elite:     bool
        is_poor:      bool
    """
    result = {
        "run_adj":  0.0,
        "tag":      "",
        "oaa":      oaa,
        "is_elite": False,
        "is_poor":  False,
    }
    if oaa is None:
        return result

    if oaa >= ELITE_OAA:
        result["run_adj"]  = -OAA_RUN_ADJ   # elite D = fewer runs allowed
        result["is_elite"] = True
        result["tag"]      = (
            f"🛡 DEFENSE EDGE: {team_code} +{oaa:.0f} OAA — "
            f"elite range suppressing extra base hits"
        )
    elif oaa <= POOR_OAA:
        result["run_adj"]  = OAA_RUN_ADJ    # poor D = more runs allowed
        result["is_poor"]  = True
        result["tag"]      = (
            f"⚠ DEFENSE RISK: {team_code} {oaa:.0f} OAA — "
            f"poor defense leaking runs"
        )

    return result


if __name__ == "__main__":
    lb = _fetch_oaa_leaderboard()
    print(f"OAA leaderboard: {len(lb)} teams")
    for team, oaa in sorted(lb.items(), key=lambda x: -x[1])[:5]:
        edge = check_defense_edge(oaa, team)
        print(f"  {team}: {oaa:+.1f} OAA → run_adj={edge['run_adj']:+.2f} {edge['tag'] or ''}")
