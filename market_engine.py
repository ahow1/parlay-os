"""PARLAY OS — market_engine.py
Fetches odds from The Odds API (Pinnacle, DK, FD, etc.) and Polymarket.
Detects line movement, sharp money, and computes no-vig implied probabilities.
"""

import os
import logging
import requests
from datetime import datetime, timedelta
import pytz
from api_client import get as _http_get
from math_engine import american_to_decimal, implied_prob, no_vig_prob
from constants import BOOK_PRIORITY, TEAM_SLUGS

ODDS_API_KEY        = os.getenv("ODDS_API_KEY", "")
ODDS_API_KEY_BACKUP = os.getenv("ODDS_API_KEY_BACKUP", "")
ODDS_BASE           = "https://api.the-odds-api.com/v4"
_log = logging.getLogger(__name__)
_DEBUG = os.getenv("DEBUG", "").lower() in ("1", "true", "yes")

# Active key — switches to backup automatically on 429/401
_active_key: list[str] = [ODDS_API_KEY]   # mutable container for in-place swap
POLY_API     = "https://gamma-api.polymarket.com"

SPORT_KEY    = "baseball_mlb"
REGIONS      = "us"
MARKETS_ML   = "h2h"
MARKETS_TOT  = "totals"
MARKETS_F5   = "h2h_1st_5_innings"

# Full team name → 3-letter abbreviation. Public so other modules can import.
TEAM_NAME_TO_ABR: dict[str, str] = {
    "San Francisco Giants": "SF",   "Los Angeles Dodgers": "LAD",
    "New York Yankees": "NYY",      "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",        "Tampa Bay Rays": "TB",
    "Toronto Blue Jays": "TOR",     "Cleveland Guardians": "CLE",
    "Los Angeles Angels": "LAA",    "Houston Astros": "HOU",
    "Seattle Mariners": "SEA",      "Texas Rangers": "TEX",
    "Arizona Diamondbacks": "AZ",   "Atlanta Braves": "ATL",
    "Chicago Cubs": "CHC",          "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",       "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",        "Kansas City Royals": "KC",
    "Miami Marlins": "MIA",         "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",       "New York Mets": "NYM",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",       "St. Louis Cardinals": "STL",
    "Washington Nationals": "WAS",  "Athletics": "ATH",
    "Oakland Athletics": "ATH",
}
# Backward-compat alias (private name used internally)
_FULL_NAME_TO_CODE = TEAM_NAME_TO_ABR

# Reverse: 3-letter code → canonical full name
ABR_TO_TEAM_NAME: dict[str, str] = {v: k for k, v in TEAM_NAME_TO_ABR.items()}
_CODE_TO_FULL = ABR_TO_TEAM_NAME


def normalize_team_name(name: str) -> str:
    """Return 3-letter abbreviation for a full team name, or name unchanged if not found."""
    return TEAM_NAME_TO_ABR.get(name, name)


def _names_match(outcome_name: str, team_name: str) -> bool:
    """True when an Odds-API outcome name refers to the same team as team_name.
    Handles exact match, code-based lookup, abbr→canonical, and substring containment."""
    if outcome_name == team_name:
        return True
    # team_name is a full name → look up its code → compare to canonical form of that code
    code = TEAM_NAME_TO_ABR.get(team_name)
    if code:
        canonical = ABR_TO_TEAM_NAME.get(code, "")
        if outcome_name == canonical:
            return True
    # team_name might itself be a 3-letter abbr → check if outcome_name is its canonical full name
    canonical2 = ABR_TO_TEAM_NAME.get(team_name, "")
    if canonical2 and outcome_name == canonical2:
        return True
    # outcome_name might be the abbr of team_name
    if TEAM_NAME_TO_ABR.get(outcome_name) == TEAM_NAME_TO_ABR.get(team_name) and TEAM_NAME_TO_ABR.get(team_name):
        return True
    # Substring guard — one name must wholly contain the other (e.g. "Cubs" ⊂ "Chicago Cubs")
    return outcome_name in team_name or team_name in outcome_name


def _odds_request(endpoint: str, params: dict) -> dict | list | None:
    key = _active_key[0] or ODDS_API_KEY
    if not key:
        print("[MKT] ODDS_API_KEY not set — no market data")
        return None
    try:
        params["apiKey"] = key
        r = _http_get(f"{ODDS_BASE}/{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        if status in (401, 429) and ODDS_API_KEY_BACKUP and key != ODDS_API_KEY_BACKUP:
            _log.warning(f"[MKT] Primary key {status} — switching to backup key")
            _active_key[0] = ODDS_API_KEY_BACKUP
            params["apiKey"] = ODDS_API_KEY_BACKUP
            try:
                r = _http_get(f"{ODDS_BASE}/{endpoint}", params=params, timeout=10, skip_cache=True)
                r.raise_for_status()
                return r.json()
            except Exception as e2:
                print(f"[MKT] backup key also failed: {e2}")
                return None
        print(f"[MKT] odds request failed ({endpoint}): {e}")
        return None
    except Exception as e:
        print(f"[MKT] odds request failed ({endpoint}): {e}")
        return None


_ET = pytz.timezone("America/New_York")


def get_mlb_events() -> list:
    """Return today's MLB events (ET) with game_id, teams, commence_time.
    Skips games from yesterday/tomorrow and games starting within 10 minutes."""
    data = _odds_request(f"sports/{SPORT_KEY}/events", {})
    if not data:
        return []

    now_et       = datetime.now(_ET)
    today_et     = now_et.date()
    cutoff_et    = now_et + timedelta(minutes=10)

    events = []
    for e in data:
        commence_utc = e.get("commence_time", "")
        if not commence_utc:
            continue
        try:
            commence_dt = datetime.fromisoformat(commence_utc.replace("Z", "+00:00"))
            commence_et = commence_dt.astimezone(_ET)
        except Exception:
            _log.warning(f"[MKT] Could not parse commence_time={commence_utc!r}")
            continue

        if commence_et.date() != today_et:
            _log.debug(f"[MKT] SKIP {e.get('away_team','?')} @ {e.get('home_team','?')}: not today ET ({commence_et.date()})")
            continue

        if commence_et <= cutoff_et:
            print(f"[MKT] SKIP {e.get('away_team','?')} @ {e.get('home_team','?')}: starts at {commence_et.strftime('%H:%M ET')} (<10 min — live or imminent)")
            continue

        event = {
            "id":           e["id"],
            "away":         e.get("away_team", ""),
            "home":         e.get("home_team", ""),
            "commence_utc": commence_utc,
        }
        _EVENT_TEAM_MAP[event["id"]] = {
            "away": event["away"],
            "home": event["home"],
        }
        events.append(event)

    return events


_SLATE_ODDS_CACHE = {}
_EVENT_TEAM_MAP = {}

def _get_slate_odds(market: str) -> list:
    """
    Use slate-wide Odds API endpoint instead of event-level endpoint.
    Event-level /events/{id}/odds returns 401 on current key/plan.
    """
    if market in _SLATE_ODDS_CACHE:
        return _SLATE_ODDS_CACHE[market]

    data = _odds_request(
        f"sports/{SPORT_KEY}/odds",
        {"regions": REGIONS, "markets": market, "oddsFormat": "american"}
    )

    if not isinstance(data, list):
        data = []

    _SLATE_ODDS_CACHE[market] = data
    return data


def _find_event_from_slate(event_id: str, market: str) -> dict | None:
    slate = _get_slate_odds(market)

    # First try direct event ID match
    for event in slate:
        if event.get("id") == event_id:
            return event

    # Fallback: match by away/home team names from get_mlb_events()
    teams = _EVENT_TEAM_MAP.get(event_id, {})
    away = teams.get("away", "")
    home = teams.get("home", "")

    if away and home:
        for event in slate:
            ev_away = event.get("away_team", "")
            ev_home = event.get("home_team", "")

            if _names_match(ev_away, away) and _names_match(ev_home, home):
                return event

    return None


def get_odds_for_event(event_id: str) -> dict:
    """
    Pull odds from slate-wide endpoint and match by event_id.
    For today, only ML is required for picks. Totals/F5 are disabled to avoid
    optional market failures tripping the circuit breaker.
    """
    ml = _find_event_from_slate(event_id, MARKETS_ML)
    totals = _find_event_from_slate(event_id, MARKETS_TOT)
    return {"ml": ml, "totals": totals, "f5": None}

def _parse_ml_bookmakers(odds_data: dict | None, away_team: str, home_team: str) -> dict:
    """Extract ML odds by book for away and home teams."""
    result = {}
    if not odds_data:
        return result
    bookmakers = odds_data.get("bookmakers", [])
    for bk in bookmakers:
        key  = bk.get("key", "")
        name = bk.get("title", key)
        for market in bk.get("markets", []):
            if market.get("key") != "h2h":
                continue
            book_odds = {}
            for outcome in market.get("outcomes", []):
                t   = outcome.get("name", "")
                prc = outcome.get("price")
                ma  = _names_match(t, away_team)
                mh  = _names_match(t, home_team)
                if _DEBUG:
                    print(f"[MKT]   {key}: outcome='{t}' away='{away_team}'({ma}) home='{home_team}'({mh}) price={prc}")
                if ma:
                    book_odds["away"] = prc
                elif mh:
                    book_odds["home"] = prc
            if book_odds:
                result[key] = {"name": name, **book_odds}
    return result


def best_odds(books: dict, side: str) -> tuple[str, int | None]:
    """Return (book_key, odds) for best available price on side='away'|'home'."""
    best_book = None
    best_price = None
    for bk, info in books.items():
        price = info.get(side)
        if price is None:
            continue
        if best_price is None:
            best_price = price
            best_book  = bk
        else:
            # Higher american odds = better
            dec_new  = american_to_decimal(str(price))
            dec_best = american_to_decimal(str(best_price))
            if dec_new and dec_best and dec_new > dec_best:
                best_price = price
                best_book  = bk
    return best_book, best_price


def pinnacle_no_vig(books: dict, away_team: str, home_team: str) -> dict | None:
    """Return no-vig probs using the sharpest available book.
    Priority: Pinnacle → betonlineag → DraftKings → any book with both sides."""
    priority = ["pinnacle", "betonlineag", "draftkings"]
    # Build ordered list: preferred books first, then all others
    ordered = priority + [k for k in books if k not in priority]
    for bk in ordered:
        info = books.get(bk)
        if not info:
            continue
        away_odds = info.get("away")
        home_odds = info.get("home")
        if not away_odds or not home_odds:
            continue
        p_away = implied_prob(str(away_odds))
        p_home = implied_prob(str(home_odds))
        if not p_away or not p_home:
            continue
        nv   = no_vig_prob(str(away_odds), str(home_odds))
        nv_a = (nv.get("side1_true") or 0) / 100
        nv_h = (nv.get("side2_true") or 0) / 100
        if not nv_a or not nv_h:
            continue
        return {
            "book": bk,
            "away": round(nv_a, 4),
            "home": round(nv_h, 4),
            "vig":  round((p_away + p_home - 100) / 100, 4),
        }
    print(f"[MKT] no_vig: no book with both sides found in {list(books.keys())}")
    return None


def _parse_totals(odds_data: dict | None) -> dict | None:
    """Extract best over/under lines and odds."""
    if not odds_data:
        return None
    bookmakers = odds_data.get("bookmakers", [])
    lines: dict = {}
    for bk in bookmakers:
        key = bk.get("key", "")
        for market in bk.get("markets", []):
            if market.get("key") != "totals":
                continue
            for outcome in market.get("outcomes", []):
                name  = outcome.get("name", "")   # "Over" or "Under"
                point = outcome.get("point")
                price = outcome.get("price")
                if point not in lines:
                    lines[point] = {"over": {}, "under": {}}
                side = name.lower()
                if side in ("over", "under"):
                    lines[point][side][key] = price
    # Pick most common line
    if not lines:
        return None
    consensus_line = max(lines, key=lambda x: len(lines[x].get("over", {})))
    info   = lines[consensus_line]
    # Best over/under across books
    best_over  = max(info["over"].values(),  default=None) if info["over"]  else None
    best_under = max(info["under"].values(), default=None) if info["under"] else None
    return {
        "line":       consensus_line,
        "best_over":  best_over,
        "best_under": best_under,
        "books":      {k: {"over": info["over"].get(k), "under": info["under"].get(k)}
                       for k in set(list(info["over"].keys()) + list(info["under"].keys()))},
    }


def polymarket_prob(away_slug: str, home_slug: str, game_date: str) -> dict | None:
    """
    Fetch Polymarket win probability for a game.
    Slug format: mlb-{away}-{home}-{YYYY-MM-DD}
    """
    slug = f"mlb-{away_slug}-{home_slug}-{game_date}"
    try:
        r = _http_get(f"{POLY_API}/markets", params={"slug": slug}, timeout=8, skip_cache=True)
        r.raise_for_status()
        events = r.json()
        if not events:
            return None
        event  = events[0]
        markets = event.get("markets", [])
        result  = {}
        for m in markets:
            title = m.get("groupItemTitle", m.get("question", "")).lower()
            price = float(m.get("lastTradePrice") or m.get("midPrice") or 0)
            if "away" in title or away_slug in title:
                result["away"] = round(price, 4)
            elif "home" in title or home_slug in title:
                result["home"] = round(price, 4)
        return result if result else None
    except Exception:
        # NO_POLY_DATA: Polymarket unavailable — model analysis runs without market comparison
        return {"NO_POLY_DATA": True}


def detect_line_movement(event_id: str, current_books: dict, side: str = "away") -> dict:
    """
    Historical odds endpoint returns 404 on current plan — no live tracking.
    Opening line cache: store current lines on first sight; compare on subsequent calls.
    """
    global _opening_line_cache
    best_away = None
    best_home = None
    for bk in ("pinnacle", "betonlineag", "draftkings"):
        info = current_books.get(bk) or {}
        if info.get("away") and info.get("home"):
            best_away = info["away"]
            best_home = info["home"]
            break

    if best_away is None:
        return {"direction": "unknown", "magnitude": 0}

    cached = _opening_line_cache.get(event_id)
    if cached is None:
        _opening_line_cache[event_id] = {"away": best_away, "home": best_home}
        return {"direction": "stable", "magnitude": 0}

    open_away = cached["away"]
    open_home  = cached["home"]
    # Away line movement: negative movement = line moving toward home (away got more expensive)
    try:
        dec_curr_away = american_to_decimal(str(best_away)) or 1.0
        dec_open_away = american_to_decimal(str(open_away)) or 1.0
        magnitude = round(abs(dec_curr_away - dec_open_away), 4)
        if magnitude < 0.01:
            direction = "stable"
        elif dec_curr_away > dec_open_away:
            direction = "toward_away"   # away got longer (worse for away backers)
        else:
            direction = "toward_home"
        return {"direction": direction, "magnitude": magnitude,
                "open_away": open_away, "curr_away": best_away}
    except Exception:
        return {"direction": "unknown", "magnitude": 0}


_opening_line_cache: dict = {}


def detect_primetime(commence_utc: str) -> dict:
    """
    Flag nationally televised primetime games (7 PM+ ET weeknights).
    Public bets disproportionately on primetime games — inflates favorite price.
    """
    if not commence_utc:
        return {}
    try:
        from datetime import datetime
        import pytz
        ET = pytz.timezone("America/New_York")
        dt = datetime.fromisoformat(commence_utc.replace("Z", "+00:00")).astimezone(ET)
        if dt.hour >= 19 and dt.weekday() < 5:  # 7 PM+ Mon-Fri
            return {
                "primetime": True,
                "message":   (
                    f"Potential nationally televised game ({dt.strftime('%I:%M %p ET')}) — "
                    f"expect public inflation on the favorite"
                ),
            }
    except Exception:
        pass
    return {}


def detect_public_bias(event_id: str, ml_books: dict) -> dict:
    """
    Estimate public bias from line gap between sharp (Pinnacle) and square (DK/FD) books.
    Wide spread = square books taking heavy public action on one side.
    """
    pin  = ml_books.get("pinnacle") or {}
    dk   = ml_books.get("draftkings") or ml_books.get("fanduel") or {}
    if not pin.get("away") or not dk.get("away"):
        return {}
    try:
        pin_away = american_to_decimal(str(pin["away"])) or 1.0
        dk_away  = american_to_decimal(str(dk["away"])) or 1.0
        gap = round(abs(pin_away - dk_away), 4)
        if gap > 0.08:
            # Square books paying better on away = public backing away
            if dk_away > pin_away:
                biased_side = "away"
                msg = f"Square books ({gap:.3f} gap) better on away — public backing away team"
            else:
                biased_side = "home"
                msg = f"Square books ({gap:.3f} gap) better on home — public backing home team"
            return {
                "biased_side": biased_side,
                "gap":         gap,
                "message":     msg,
                "fade_signal": True,
            }
    except Exception:
        pass
    return {}


def detect_reverse_line_movement(lm: dict, public_bias: dict) -> dict:
    """
    Reverse line movement: public money on one side but line moves the other way.
    Strong signal for sharp money on the opposite side.
    """
    direction = lm.get("direction", "stable")
    biased    = public_bias.get("biased_side", "")
    if not biased or direction in ("unknown", "stable"):
        return {}

    # Public likes away but line moved toward home = sharp money on home
    if biased == "away" and direction == "toward_home":
        return {
            "sharp_side": "home",
            "message":    (
                f"Public backing away but line moved toward home — "
                f"SHARP REVERSE: sharp money on home"
            ),
        }
    if biased == "home" and direction == "toward_away":
        return {
            "sharp_side": "away",
            "message":    (
                f"Public backing home but line moved toward away — "
                f"SHARP REVERSE: sharp money on away"
            ),
        }
    return {}

def full_market_snapshot(event_id: str, away_team: str, home_team: str,
                         away_code: str, home_code: str, game_date: str,
                         commence_utc: str = "") -> dict:
    """Single call to get all market data for a game."""
    odds    = get_odds_for_event(event_id)
    ml_raw  = odds.get("ml")
    n_books = len(ml_raw.get("bookmakers", [])) if ml_raw else 0
    print(f"[MKT] {away_team} @ {home_team}: ml_raw={'None' if ml_raw is None else f'{n_books} books'}")
    ml_books = _parse_ml_bookmakers(ml_raw, away_team, home_team)
    print(f"[MKT]   matched books: {list(ml_books.keys())}")
    nv       = pinnacle_no_vig(ml_books, away_team, home_team)
    totals   = _parse_totals(odds.get("totals"))
    f5_books = _parse_ml_bookmakers(odds.get("f5"), away_team, home_team)
    lm       = detect_line_movement(event_id, ml_books)

    away_slug = TEAM_SLUGS.get(away_code, away_code.lower())
    home_slug = TEAM_SLUGS.get(home_code, home_code.lower())
    poly = polymarket_prob(away_slug, home_slug, game_date)

    best_away_book, best_away_odds = best_odds(ml_books, "away")
    best_home_book, best_home_odds = best_odds(ml_books, "home")

    _away_disp = (f"+{best_away_odds}" if isinstance(best_away_odds, int) and best_away_odds > 0
                  else str(best_away_odds or "N/A"))
    _home_disp = (f"+{best_home_odds}" if isinstance(best_home_odds, int) and best_home_odds > 0
                  else str(best_home_odds or "N/A"))
    print(f"[MKT]   best: away={_away_disp}@{best_away_book or '?'} home={_home_disp}@{best_home_book or '?'} "
          f"books matched={len(ml_books)}")

    primetime   = detect_primetime(commence_utc)
    public_bias = detect_public_bias(event_id, ml_books)
    rlm         = detect_reverse_line_movement(lm, public_bias)

    return {
        "event_id":        event_id,
        "away":            away_team,
        "home":            home_team,
        "ml_books":        ml_books,
        "no_vig":          nv,
        "totals":          totals,
        "f5_books":        f5_books,
        "polymarket":      poly,
        "line_movement":   lm,
        "best_away_book":  best_away_book,
        "best_away_odds":  best_away_odds,
        "best_home_book":  best_home_book,
        "best_home_odds":  best_home_odds,
        # Intelligence signals
        "primetime":       primetime,
        "public_bias":     public_bias,
        "reverse_line":    rlm,
    }


if __name__ == "__main__":
    events = get_mlb_events()
    print(f"Found {len(events)} MLB events today")
    if events:
        e = events[0]
        print(f"  {e['away']} @ {e['home']}")
        snap = full_market_snapshot(
            e["id"], e["away"], e["home"], "away", "home",
            e["commence_utc"][:10]
        )
        print(f"  no_vig: {snap['no_vig']}")
        print(f"  poly:   {snap['polymarket']}")
        print(f"  totals: {snap['totals']}")
