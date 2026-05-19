"""PARLAY OS — framing_engine.py
Catcher framing edge detection via Baseball Savant framing leaderboard.

Elite framer (>+8 projected framing runs): +0.02 win prob for pitching team.
Poor framer (<-8 projected framing runs):  -0.02 win prob for pitching team.

Framing runs: rv_tot from Savant catcher-framing leaderboard, scaled to a
10,000-pitch full-season equivalent so partial-season values compare correctly.
"""

import csv
import io
import threading

from api_client import get as _http_get

SAVANT_BASE  = "https://baseballsavant.mlb.com"
STATSAPI     = "https://statsapi.mlb.com/api/v1"
TIMEOUT      = 20

ELITE_FRAMING_RUNS = 8.0    # projected runs above this = elite framer
POOR_FRAMING_RUNS  = -8.0   # projected runs below this = poor framer
FRAMING_PROB_ADJ   = 0.02   # probability adjustment magnitude
SCALE_PITCHES      = 10_000 # denominator for full-season projection

_framing_lock  = threading.Lock()
_framing_cache: dict | None = None   # {team_abbrev: projected_framing_runs}

_team_map_lock  = threading.Lock()
_team_map_cache: dict | None = None  # {str(player_id): team_code}


def _build_team_id_reverse() -> dict:
    """Return {mlb_team_id: team_code} from constants.MLB_TEAM_IDS."""
    from constants import MLB_TEAM_IDS
    rev: dict = {}
    for code, tid in MLB_TEAM_IDS.items():
        if tid not in rev:
            rev[tid] = code
    return rev


def _fetch_catcher_team_map(season: int = 2026) -> dict:
    """
    Fetch MLB Stats API player roster, filter catchers, return {str(player_id): team_code}.
    Falls back to {} on error.
    """
    global _team_map_cache
    with _team_map_lock:
        if _team_map_cache is not None:
            return _team_map_cache

        result: dict = {}
        tid_to_code = _build_team_id_reverse()
        try:
            r = _http_get(
                f"{STATSAPI}/sports/1/players",
                params={"season": season, "gameType": "R"},
                timeout=TIMEOUT,
            )
            for p in r.json().get("people", []):
                if p.get("primaryPosition", {}).get("abbreviation") != "C":
                    continue
                pid  = p.get("id")
                tid  = p.get("currentTeam", {}).get("id")
                code = tid_to_code.get(tid)
                if pid and code:
                    result[str(pid)] = code
        except Exception as e:
            print(f"  [FRAMING] Team map fetch error: {e}")

        _team_map_cache = result
        print(f"  [FRAMING] Catcher team map: {len(result)} catchers loaded")
        return result


def _fetch_framing_leaderboard(season: int = 2026) -> dict:
    """
    Fetch Savant catcher framing leaderboard (player-level).
    Scales rv_tot to a 10,000-pitch equivalent, then aggregates to team level
    using the primary catcher (highest pitch count).

    Returns {team_abbrev: projected_framing_runs}.
    """
    global _framing_cache
    with _framing_lock:
        if _framing_cache is not None:
            return _framing_cache

        result: dict = {}
        team_map = _fetch_catcher_team_map(season)

        def _flt(v, d=0.0):
            try:
                return float(v) if v not in ("", "null", "NA", ".", None) else d
            except (ValueError, TypeError):
                return d

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

                # player_id → (rv_tot, pitches) for primary catcher selection
                team_best: dict = {}  # team_code → {"rv": float, "pitches": float}

                for row in reader:
                    pid     = str(row.get("id", "")).strip()
                    rv_tot  = _flt(row.get("rv_tot"))
                    pitches = max(_flt(row.get("pitches"), 1.0), 1.0)

                    team_code = team_map.get(pid)
                    if not team_code:
                        continue

                    # Scale to full-season equivalent
                    projected = rv_tot * (SCALE_PITCHES / pitches)

                    # Keep primary catcher (highest pitch count) per team
                    if team_code not in team_best or pitches > team_best[team_code]["pitches"]:
                        team_best[team_code] = {"rv": projected, "pitches": pitches}

                if team_best:
                    for team, data in team_best.items():
                        result[team] = round(data["rv"], 2)

                    top = sorted(result.items(), key=lambda x: -x[1])
                    print(
                        f"  [FRAMING] Leaderboard loaded (year={try_year}): "
                        f"{len(result)} teams — "
                        f"best: {top[0][0]}={top[0][1]:+.1f} "
                        f"worst: {top[-1][0]}={top[-1][1]:+.1f}"
                    )
                    break  # success

            except Exception as e:
                print(f"  [FRAMING] Fetch error (year={try_year}): {e}")
                continue

        if not result:
            print("  [FRAMING] Leaderboard returned no data — framing edge disabled")

        _framing_cache = result
        return result


def get_team_framing(team_code: str) -> float | None:
    """
    Return the team's primary catcher projected framing runs (full-season equivalent).
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
            f"🧤 FRAMING EDGE: {team_code} +{framing_runs:.1f} proj framing runs — "
            f"elite catcher boosting pitcher strike zone"
        )
    elif framing_runs <= POOR_FRAMING_RUNS:
        result["prob_adj"] = -FRAMING_PROB_ADJ
        result["is_poor"]  = True
        result["tag"]      = (
            f"⚠ FRAMING RISK: {team_code} {framing_runs:.1f} proj framing runs — "
            f"poor framer costing pitchers strikes"
        )

    return result


if __name__ == "__main__":
    lb = _fetch_framing_leaderboard()
    print(f"Framing leaderboard: {len(lb)} teams")
    for team, runs in sorted(lb.items(), key=lambda x: -x[1])[:5]:
        edge = check_framing_edge(runs, team)
        print(f"  {team}: {runs:+.1f} runs → adj={edge['prob_adj']:+.3f} {edge['tag'] or ''}")
