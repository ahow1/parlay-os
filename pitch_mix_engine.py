"""PARLAY OS — pitch_mix_engine.py
SP pitch arsenal vs lineup chase rate (O-Swing%) detection.

PITCH TRAP: SP throws a pitch >25% usage AND opposing lineup O-Swing% >32%
→ Structural edge: lineup will chase off-speed/breaking balls out of zone.
→ Add 0.025 win-probability to the pitching team per exploitable pitch type.

Data sources:
  SP arsenal:    Baseball Savant individual pitch CSV (statcast_engine.get_pitcher_pitch_mix)
  Lineup chase:  Baseball Savant team plate discipline leaderboard → falls back to LG avg
"""

import csv
import io
import threading

from api_client import get as _http_get

SAVANT_BASE = "https://baseballsavant.mlb.com"
TIMEOUT     = 20

USAGE_THRESH       = 0.25    # SP must throw pitch >25% to be "primary"
CHASE_THRESH       = 0.320   # lineup O-Swing% > 32% = exploitable
PROB_ADD_PER_MATCH = 0.025   # +2.5% win prob per exploitable pitch type
MAX_PROB_ADD       = 0.075   # cap: 3 exploitable types = +7.5%
LG_AVG_CHASE       = 0.305   # league-average O-Swing%

# Discipline leaderboard — team-level, fetched once per session
_disc_lock  = threading.Lock()
_disc_cache: dict | None = None   # {team_abbrev: o_swing_pct}


def _fetch_team_discipline(season: int = 2026) -> dict:
    """
    Fetch Baseball Savant team plate discipline leaderboard.
    Returns {team_abbrev: o_swing_pct} for all MLB teams.
    Falls back to {} on any error (caller uses LG average).
    """
    global _disc_cache
    with _disc_lock:
        if _disc_cache is not None:
            return _disc_cache
        url = (
            f"{SAVANT_BASE}/leaderboard/plate-discipline"
            f"?type=team&year={season}&team=&min=q&csv=true"
        )
        result: dict = {}
        try:
            r = _http_get(
                url, timeout=TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
                skip_cache=False,
            )
            if r.status_code != 200 or not r.text.strip():
                _disc_cache = result
                return result

            text   = r.text.lstrip("﻿")
            reader = csv.DictReader(io.StringIO(text))

            def _flt(v, d=0.0):
                try:
                    return float(v) if v not in ("", "null", "NA", ".", None) else d
                except (ValueError, TypeError):
                    return d

            for row in reader:
                team = (row.get("team_abbrev") or row.get("team") or row.get("team_name") or "").strip().upper()
                if not team:
                    continue
                # O-Swing% may be a fraction (0-1) or percent (0-100)
                o_swing = (
                    _flt(row.get("o_swing_percent") or row.get("o_swing_pct") or
                         row.get("o_swing") or row.get("oz_swing_percent"))
                )
                if o_swing > 1:
                    o_swing = o_swing / 100
                if 0 < o_swing < 1:
                    result[team] = round(o_swing, 4)

        except Exception as e:
            print(f"  [PITCH_MIX] Discipline fetch error: {e}")

        if not result and season == 2026:
            # Try 2025 fallback
            url25 = url.replace(f"year={season}", "year=2025")
            try:
                r2 = _http_get(url25, timeout=TIMEOUT,
                               headers={"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/1.0)"},
                               skip_cache=False)
                if r2.status_code == 200 and r2.text.strip():
                    text2 = r2.text.lstrip("﻿")
                    for row2 in csv.DictReader(io.StringIO(text2)):
                        team2 = (row2.get("team_abbrev") or row2.get("team") or "").strip().upper()
                        if not team2:
                            continue
                        def _flt2(v, d=0.0):
                            try:
                                return float(v) if v not in ("", "null", "NA", ".", None) else d
                            except (ValueError, TypeError):
                                return d
                        ov = _flt2(row2.get("o_swing_percent") or row2.get("o_swing") or row2.get("oz_swing_percent"))
                        if ov > 1:
                            ov = ov / 100
                        if 0 < ov < 1:
                            result[team2] = round(ov, 4)
            except Exception:
                pass

        _disc_cache = result
        return result


def get_lineup_chase_rate(team_code: str) -> float:
    """
    Return the team's O-Swing% (fraction 0-1).
    Falls back to league average (0.305) if data unavailable.
    """
    disc = _fetch_team_discipline()
    code = team_code.upper()
    # Try direct match, then try without trailing 'S' (e.g. "REDS"→"RED")
    chase = disc.get(code) or disc.get(code[:3])
    return chase if chase else LG_AVG_CHASE


def check_pitch_trap(
    pitcher_id: int,
    pitcher_name: str,
    opp_team_code: str,
) -> dict:
    """
    Detect PITCH_TRAP: SP uses pitch >25% AND lineup O-Swing% > 32%.

    Returns:
        is_pitch_trap:       bool
        exploitable_pitches: list of pitch type codes that match
        prob_add:            float — total win-prob add for the pitching team
        tag:                 str   — Telegram flag string (empty if no trap)
        chase_rate:          float — opposing lineup O-Swing%
        arsenal:             dict  — {type: {usage_pct, whiff_rate}} summary
    """
    result = {
        "is_pitch_trap":      False,
        "exploitable_pitches": [],
        "prob_add":            0.0,
        "tag":                 "",
        "chase_rate":          LG_AVG_CHASE,
        "arsenal":             {},
    }

    try:
        from statcast_engine import get_pitcher_pitch_mix
        pm = get_pitcher_pitch_mix(pitcher_id)
    except Exception:
        return result

    if not pm or not pm.get("pitch_mix"):
        return result

    arsenal    = pm["pitch_mix"]
    chase_rate = get_lineup_chase_rate(opp_team_code)
    result["chase_rate"] = chase_rate
    result["arsenal"]    = {
        pt: {"usage_pct": d["usage_pct"], "whiff_rate": d["whiff_rate"]}
        for pt, d in arsenal.items()
    }

    if chase_rate <= CHASE_THRESH:
        return result

    # Off-speed / breaking pitches that are exploitable vs chase lineups
    _breaking_off = {"SL", "CU", "CH", "FS", "ST", "SC", "KC", "CS", "EP"}
    exploitable = []
    for pt, data in arsenal.items():
        if data.get("usage_pct", 0) > USAGE_THRESH and pt in _breaking_off:
            exploitable.append(pt)

    if not exploitable:
        return result

    prob_add = min(len(exploitable) * PROB_ADD_PER_MATCH, MAX_PROB_ADD)
    sp_last  = (pitcher_name or "SP").split()[-1]
    pitch_list = "/".join(exploitable[:3])

    result.update({
        "is_pitch_trap":       True,
        "exploitable_pitches": exploitable,
        "prob_add":            round(prob_add, 4),
        "tag": (
            f"🎣 PITCH TRAP: {sp_last} {pitch_list} "
            f"{arsenal.get(exploitable[0],{}).get('usage_pct',0):.0%} usage "
            f"vs lineup chasing {chase_rate:.1%} out of zone"
        ),
    })
    print(
        f"  [PITCH_MIX] PITCH_TRAP: {pitcher_name} {pitch_list} "
        f"vs {opp_team_code} chase={chase_rate:.1%} add={prob_add:+.3f}"
    )
    return result


if __name__ == "__main__":
    # Quick test: Corbin Burnes ID = 669203
    r = check_pitch_trap(669203, "Corbin Burnes", "NYY")
    print(f"PITCH_TRAP: {r['is_pitch_trap']}")
    print(f"  Exploitable: {r['exploitable_pitches']}")
    print(f"  Chase rate: {r['chase_rate']:.1%}")
    print(f"  Prob add: {r['prob_add']:+.3f}")
    print(f"  Tag: {r['tag']}")
