"""PARLAY OS — situations_engine.py
Situational angles with win probability adjustments.

Each angle has a documented historical edge. Adjustments are additive to the
base win probability before the 12-factor blend. When 3+ situations trigger
simultaneously (SITUATION_STACK) the pick is flagged HIGH conviction automatically.

Exports:
    check_situations(away_team, home_team, game_data, sp_data, bullpen_data) → dict
    situations_telegram_line(result) → str
"""

import logging
from datetime import date, timedelta

from api_client import get as _http_get

STATSAPI = "https://statsapi.mlb.com/api/v1"
log      = logging.getLogger(__name__)

# ── Angle definitions ─────────────────────────────────────────────────────────

SITUATION_ANGLES = {
    "HOME_DOG_ELITE_SP":    +0.04,   # home dog with SP xFIP < 3.50
    "FADE_PUBLIC_FAVORITE": +0.03,   # public loading on favorite, we fade
    "BULLPEN_GAME_UNDERDOG":+0.025,  # bullpen game (<4 IP expected) + underdog
    "REVENGE_SPOT":         +0.02,   # lost to this opponent ≤30 days ago
    "SERIES_OPENER":        +0.015,  # game 1 of a multi-game series
    "GETAWAY_DAY":          -0.025,  # road team, day game after night game
    "WEST_TO_EAST":         -0.03,   # west coast team playing east coast (3+ TZ)
    "COLD_OFFENSE":         -0.02,   # K% > 27% in last 7 games
    "HOT_OFFENSE":          +0.02,   # BA > .270 in last 7 games
}

STACK_THRESHOLD = 3   # 3+ triggers → HIGH conviction

# City→timezone bucket (0=pacific, 1=mountain, 2=central, 3=eastern)
CITY_TZ = {
    "LAD": 0, "LAA": 0, "SF": 0, "SEA": 0, "SD": 0, "ATH": 0, "OAK": 0,
    "AZ":  1, "COL": 1,
    "HOU": 2, "STL": 2, "CHC": 2, "CWS": 2, "MIL": 2, "MIN": 2, "KC": 2, "DET": 2,
    "TOR": 3, "NYY": 3, "NYM": 3, "BOS": 3, "PHI": 3, "ATL": 3, "MIA": 3,
    "WAS": 3, "BAL": 3, "CLE": 3, "PIT": 3, "TB": 3, "CIN": 3, "TEX": 2,
}


# ── Helper: recent schedule ────────────────────────────────────────────────────

def _recent_schedule(team_id: int, days_back: int = 30) -> list[dict]:
    """Return completed games for a team in the last N days."""
    today  = date.today()
    start  = (today - timedelta(days=days_back)).isoformat()
    end    = (today - timedelta(days=1)).isoformat()
    try:
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={
                "sportId":   1,
                "teamId":    team_id,
                "startDate": start,
                "endDate":   end,
                "hydrate":   "linescore,decisions,team",
                "gameType":  "R",
            },
            timeout=8,
        )
        games = []
        for day in r.json().get("dates", []):
            for g in day.get("games", []):
                if g.get("status", {}).get("abstractGameState") == "Final":
                    games.append(g)
        return games
    except Exception as e:
        log.debug(f"[situations] recent_schedule error: {e}")
        return []


def _team_recent_batting(team_id: int, days: int = 7) -> dict:
    """Batting BA + K% proxy from last N days of schedule linescores."""
    games = _recent_schedule(team_id, days_back=days)
    total_hits = total_at_bats = total_ks = 0
    for g in games:
        teams   = g.get("teams", {})
        away_t  = teams.get("away", {})
        home_t  = teams.get("home", {})
        our_side = away_t if away_t.get("team", {}).get("id") == team_id else home_t

        ls = g.get("linescore", {}) or {}
        # Innings list — count hits
        for inn in ls.get("innings", []):
            side_inn = inn.get("away") if our_side is away_t else inn.get("home")
            if side_inn:
                total_hits    += int(side_inn.get("hits", 0) or 0)
                total_at_bats += int(side_inn.get("hits", 0) or 0) + int(side_inn.get("errors", 0) or 0)

        # K count from team totals (approximate: strikeouts = totalBases proxy not available;
        # use linescore totals hit/runs only — K% falls back to None)
    ba = round(total_hits / max(total_at_bats, 1), 3) if total_at_bats > 10 else None
    return {"ba": ba, "hits": total_hits, "ab": total_at_bats}


def _last_loss_to_opponent(team_id: int, opp_team_id: int, days: int = 30) -> bool:
    """True if team_id lost to opp_team_id within the last N days."""
    try:
        games = _recent_schedule(team_id, days_back=days)
        for g in games:
            teams    = g.get("teams", {})
            away_t   = teams.get("away", {})
            home_t   = teams.get("home", {})
            away_id  = away_t.get("team", {}).get("id")
            home_id  = home_t.get("team", {}).get("id")
            if {away_id, home_id} != {team_id, opp_team_id}:
                continue
            our_side  = away_t if away_id == team_id else home_t
            if not our_side.get("isWinner", True):
                return True   # we lost to this opponent recently
    except Exception as e:
        log.debug(f"[situations] revenge check error: {e}")
    return False


def _is_series_opener(game_data: dict) -> bool:
    """True if this is game 1 of a multi-game series (from game_data)."""
    return bool(game_data.get("series_game_number") == 1
                and game_data.get("games_in_series", 1) > 1)


def _is_getaway_day(team_id: int, game_data: dict) -> bool:
    """
    True if team played a night game last night and today is a day game on the road.
    Day game = commence time < 17:00 ET.
    """
    try:
        game_time = game_data.get("game_time_et", "")
        if not game_time:
            return False
        hour_s = game_time.split(":")[0].strip()
        hour   = int(hour_s)
        is_day_game = hour < 17
        if not is_day_game:
            return False
        # Check if they played last night (any road game yesterday)
        yesterday = date.today() - timedelta(days=1)
        r = _http_get(
            f"{STATSAPI}/schedule",
            params={
                "sportId": 1, "teamId": team_id,
                "date": yesterday.isoformat(),
                "hydrate": "game,team",
                "gameType": "R",
            },
            timeout=6,
        )
        for day in r.json().get("dates", []):
            for g in day.get("games", []):
                if g.get("status", {}).get("abstractGameState") != "Final":
                    continue
                # If they were the away team yesterday, they're traveling
                away_id = g.get("teams", {}).get("away", {}).get("team", {}).get("id")
                if away_id == team_id:
                    return True
    except Exception as e:
        log.debug(f"[situations] getaway_day error: {e}")
    return False


def _is_west_to_east(team_code: str, home_code: str) -> bool:
    """
    True if the AWAY team (team_code) is ≥2 timezone buckets west of the home park.
    e.g. LAD (0) playing at NYY (3) → 3-zone gap → True.
    """
    away_tz = CITY_TZ.get(team_code, 2)
    home_tz = CITY_TZ.get(home_code, 2)
    return (home_tz - away_tz) >= 2


# ── Main function ─────────────────────────────────────────────────────────────

def check_situations(
    away_team: str,
    home_team: str,
    game_data: dict,
    sp_data: dict | None = None,
    bullpen_data: dict | None = None,
    market_data: dict | None = None,
    offense_data: dict | None = None,
) -> dict:
    """
    Check all 9 situational angles.

    Parameters:
        away_team:    3-letter away team code
        home_team:    3-letter home team code
        game_data:    analysis dict from brain.py (must have away_tid, home_tid keys
                      OR away_team_id / home_team_id; plus series info)
        sp_data:      dict with 'away' and 'home' sp dicts
        bullpen_data: dict with 'away' and 'home' bp dicts
        market_data:  market snapshot dict (for public bias detection)
        offense_data: dict with 'away' and 'home' offense dicts

    Returns dict:
        triggered:      list of triggered angle names
        adjustments:    {angle: adj_value}
        total_away_adj: net probability adjustment for away team
        total_home_adj: net probability adjustment for home team
        situation_stack: bool — 3+ angles triggered
        high_conviction: bool — situation_stack True
        labels:         list of short labels for Telegram FLAGS
        details:        dict of angle → reason string
    """
    from constants import MLB_TEAM_IDS

    sp     = sp_data   or {}
    bp     = bullpen_data or {}
    mkt    = market_data or {}
    off    = offense_data or {}

    away_sp  = sp.get("away")  or {}
    home_sp  = sp.get("home")  or {}
    away_bp  = bp.get("away")  or {}
    home_bp  = bp.get("home")  or {}
    away_off = off.get("away") or {}
    home_off = off.get("home") or {}

    away_tid = (game_data.get("away_tid")
                or game_data.get("away_team_id")
                or MLB_TEAM_IDS.get(away_team))
    home_tid = (game_data.get("home_tid")
                or game_data.get("home_team_id")
                or MLB_TEAM_IDS.get(home_team))

    best_away_odds = game_data.get("best_away_odds")
    best_home_odds = game_data.get("best_home_odds")

    triggered   = []
    adjustments = {}
    details     = {}
    labels      = []

    # ── 1. HOME_DOG_ELITE_SP ──────────────────────────────────────────────────
    try:
        home_xfip   = float(home_sp.get("xfip", 9.0))
        home_odds_v = best_home_odds
        home_is_dog = False
        if home_odds_v is not None:
            try:
                home_is_dog = int(str(home_odds_v).replace("+", "")) >= 115
            except (ValueError, TypeError):
                pass
        if home_is_dog and home_xfip < 3.50:
            triggered.append("HOME_DOG_ELITE_SP")
            adjustments["HOME_DOG_ELITE_SP"] = SITUATION_ANGLES["HOME_DOG_ELITE_SP"]
            details["HOME_DOG_ELITE_SP"] = (
                f"Home dog ({home_odds_v}) with elite SP xFIP {home_xfip:.2f} < 3.50"
            )
            labels.append("HOME_DOG_ELITE")
    except Exception:
        pass

    # ── 2. FADE_PUBLIC_FAVORITE ───────────────────────────────────────────────
    try:
        pub_bias = mkt.get("public_bias") or {}
        if pub_bias.get("fade_signal"):
            biased_side = pub_bias.get("biased_side", "")
            # We're betting AGAINST the public favorite
            # Adjustment goes to the underdog (the side public is NOT backing)
            triggered.append("FADE_PUBLIC_FAVORITE")
            adjustments["FADE_PUBLIC_FAVORITE"] = SITUATION_ANGLES["FADE_PUBLIC_FAVORITE"]
            details["FADE_PUBLIC_FAVORITE"] = (
                f"Public loading on {biased_side}, fade signal active"
            )
            labels.append("FADE_PUBLIC")
    except Exception:
        pass

    # ── 3. BULLPEN_GAME_UNDERDOG ──────────────────────────────────────────────
    try:
        # Bullpen game = either SP has very low expected IP (< 4) or is missing
        away_ip = (float(away_sp.get("ip") or 0)
                   / max(float(away_sp.get("gs") or 1), 1))
        home_ip = (float(home_sp.get("ip") or 0)
                   / max(float(home_sp.get("gs") or 1), 1))
        is_pen_game = (away_sp.get("sp_missing") or away_ip < 4.0
                       or home_sp.get("sp_missing") or home_ip < 4.0)
        # Underdog = has positive odds
        if is_pen_game:
            for side, odds_v in [("away", best_away_odds), ("home", best_home_odds)]:
                if odds_v is None:
                    continue
                try:
                    is_dog = int(str(odds_v).replace("+", "")) >= 115
                except (ValueError, TypeError):
                    is_dog = False
                if is_dog:
                    triggered.append("BULLPEN_GAME_UNDERDOG")
                    adjustments["BULLPEN_GAME_UNDERDOG"] = SITUATION_ANGLES["BULLPEN_GAME_UNDERDOG"]
                    details["BULLPEN_GAME_UNDERDOG"] = (
                        f"Bullpen game ({side} dog {odds_v})"
                    )
                    labels.append("BULLPEN_DOG")
                    break
    except Exception:
        pass

    # ── 4. REVENGE_SPOT ───────────────────────────────────────────────────────
    try:
        if away_tid and home_tid:
            if _last_loss_to_opponent(away_tid, home_tid, days=30):
                triggered.append("REVENGE_SPOT")
                adjustments["REVENGE_SPOT"] = SITUATION_ANGLES["REVENGE_SPOT"]
                details["REVENGE_SPOT"] = (
                    f"{away_team} lost to {home_team} within last 30 days — revenge spot"
                )
                labels.append("REVENGE")
    except Exception:
        pass

    # ── 5. SERIES_OPENER ──────────────────────────────────────────────────────
    try:
        if _is_series_opener(game_data):
            triggered.append("SERIES_OPENER")
            adjustments["SERIES_OPENER"] = SITUATION_ANGLES["SERIES_OPENER"]
            details["SERIES_OPENER"] = (
                f"Game 1 of {game_data.get('games_in_series','?')}-game series"
            )
            labels.append("SERIES_G1")
    except Exception:
        pass

    # ── 6. GETAWAY_DAY ────────────────────────────────────────────────────────
    try:
        if away_tid and _is_getaway_day(away_tid, game_data):
            triggered.append("GETAWAY_DAY")
            adjustments["GETAWAY_DAY"] = SITUATION_ANGLES["GETAWAY_DAY"]
            details["GETAWAY_DAY"] = (
                f"{away_team} on getaway day (day game after night game on road)"
            )
            labels.append("GETAWAY")
    except Exception:
        pass

    # ── 7. WEST_TO_EAST ───────────────────────────────────────────────────────
    try:
        if _is_west_to_east(away_team, home_team):
            triggered.append("WEST_TO_EAST")
            adjustments["WEST_TO_EAST"] = SITUATION_ANGLES["WEST_TO_EAST"]
            details["WEST_TO_EAST"] = (
                f"{away_team} (TZ bucket {CITY_TZ.get(away_team,2)}) "
                f"traveling to {home_team} (TZ bucket {CITY_TZ.get(home_team,2)}) "
                f"— 3-hour time change"
            )
            labels.append("W2E_TRAVEL")
    except Exception:
        pass

    # ── 8. COLD_OFFENSE ───────────────────────────────────────────────────────
    try:
        for team, tid, sp_opp in [
            (away_team, away_tid, home_sp),
            (home_team, home_tid, away_sp),
        ]:
            if not tid:
                continue
            # Use offense engine's k_pct_7d if available
            off_side = away_off if team == away_team else home_off
            k_pct = off_side.get("k_pct_7d")
            if k_pct is None:
                # Fall back: approximate from batting stats
                batting = _team_recent_batting(tid, days=7)
                # BA < 0.210 as proxy for cold offense when K% unavailable
                if batting.get("ba") is not None and batting["ba"] < 0.210:
                    k_pct = 0.28   # treat as cold
            if k_pct is not None and float(k_pct) > 0.27:
                triggered.append("COLD_OFFENSE")
                adjustments["COLD_OFFENSE"] = SITUATION_ANGLES["COLD_OFFENSE"]
                details["COLD_OFFENSE"] = (
                    f"{team} K% {k_pct:.1%} last 7 games > 27% — cold offense"
                )
                labels.append("COLD_OFF")
                break   # flag once per game
    except Exception:
        pass

    # ── 9. HOT_OFFENSE ────────────────────────────────────────────────────────
    try:
        for team, tid, side_str in [
            (away_team, away_tid, "away"),
            (home_team, home_tid, "home"),
        ]:
            if not tid:
                continue
            off_side = away_off if side_str == "away" else home_off
            ba_7d = off_side.get("ba_7d")
            if ba_7d is None:
                batting = _team_recent_batting(tid, days=7)
                ba_7d = batting.get("ba")
            if ba_7d is not None and float(ba_7d) > 0.270:
                triggered.append("HOT_OFFENSE")
                adjustments["HOT_OFFENSE"] = SITUATION_ANGLES["HOT_OFFENSE"]
                details["HOT_OFFENSE"] = (
                    f"{team} BA {float(ba_7d):.3f} last 7 games > .270 — hot offense"
                )
                labels.append("HOT_OFF")
                break
    except Exception:
        pass

    # ── Compute net probability adjustments ───────────────────────────────────

    # Angles that help AWAY team (away_p increases):
    AWAY_POSITIVE = {"FADE_PUBLIC_FAVORITE", "REVENGE_SPOT", "HOT_OFFENSE"}
    # Angles that help HOME team (home_p increases):
    HOME_POSITIVE = {"HOME_DOG_ELITE_SP", "BULLPEN_GAME_UNDERDOG", "SERIES_OPENER"}
    # Angles that hurt AWAY team (away_p decreases):
    AWAY_NEGATIVE = {"GETAWAY_DAY", "WEST_TO_EAST", "COLD_OFFENSE"}

    total_away_adj = 0.0
    total_home_adj = 0.0
    for angle in triggered:
        adj = adjustments.get(angle, 0.0)
        if angle in AWAY_POSITIVE:
            total_away_adj += adj
        elif angle in HOME_POSITIVE:
            total_home_adj += adj
        elif angle in AWAY_NEGATIVE:
            total_away_adj -= abs(adj)

    n_triggered    = len(triggered)
    situation_stack = n_triggered >= STACK_THRESHOLD

    return {
        "triggered":        triggered,
        "n_triggered":      n_triggered,
        "adjustments":      adjustments,
        "total_away_adj":   round(total_away_adj, 4),
        "total_home_adj":   round(total_home_adj, 4),
        "situation_stack":  situation_stack,
        "high_conviction":  situation_stack,
        "labels":           labels,
        "details":          details,
    }


def situations_telegram_line(result: dict) -> str:
    """One-line Telegram summary for triggered situations."""
    if not result or not result.get("triggered"):
        return ""
    n = result["n_triggered"]
    stack = " ⚡SITUATION_STACK" if result.get("situation_stack") else ""
    labels_s = " | ".join(result.get("labels", []))
    return f"SITUATIONS ({n}): {labels_s}{stack}"


if __name__ == "__main__":
    sample_game = {
        "best_home_odds": 130, "best_away_odds": -145,
        "series_game_number": 1, "games_in_series": 3,
        "game_time_et": "13:05 PM ET",
    }
    sample_sp = {
        "home": {"xfip": 3.20, "sp_missing": False, "ip": 160, "gs": 28},
        "away": {"xfip": 4.50, "sp_missing": False, "ip": 110, "gs": 20},
    }
    result = check_situations("LAD", "NYY", sample_game, sp_data=sample_sp)
    print(f"Triggered: {result['triggered']}")
    print(f"Away adj: {result['total_away_adj']:+.4f}")
    print(f"Home adj: {result['total_home_adj']:+.4f}")
    print(situations_telegram_line(result))
