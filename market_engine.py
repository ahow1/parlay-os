"""PARLAY OS — market_engine.py
Fetches odds from The Odds API (Pinnacle, DK, FD, etc.) and Polymarket.
Detects line movement, sharp money, and computes no-vig implied probabilities.
"""

import os
import requests
from math_engine import american_to_decimal, implied_prob, no_vig_prob
from constants import BOOK_PRIORITY, TEAM_SLUGS

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
ODDS_BASE    = "https://api.the-odds-api.com/v4"
POLY_API     = "https://gamma-api.polymarket.com"

SPORT_KEY    = "baseball_mlb"
REGIONS      = "us,us2"
MARKETS_ML   = "h2h"
MARKETS_TOT  = "totals"
MARKETS_F5   = "h2h_1st_5_innings"

# Full team names exactly as returned by the Odds API → internal short code.
# Used as a fallback when outcome names don't match event names verbatim.
_FULL_NAME_TO_CODE: dict[str, str] = {
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
# Reverse: code → canonical full name (for matching outcome names)
_CODE_TO_FULL: dict[str, str] = {v: k for k, v in _FULL_NAME_TO_CODE.items()}


def _names_match(outcome_name: str, team_name: str) -> bool:
    """True when an Odds-API outcome name refers to the same team as team_name.
    Handles exact match, substring containment, and code-based lookup."""
    if outcome_name == team_name:
        return True
    code = _FULL_NAME_TO_CODE.get(team_name)
    if code:
        canonical = _CODE_TO_FULL.get(code, "")
        if outcome_name == canonical:
            return True
    # Substring guard — one name must wholly contain the other (e.g. "Cubs" ⊂ "Chicago Cubs")
    return outcome_name in team_name or team_name in outcome_name


def _odds_request(endpoint: str, params: dict) -> dict | list | None:
    if not ODDS_API_KEY:
        print("[MKT] ODDS_API_KEY not set — no market data")
        return None
    try:
        params["apiKey"] = ODDS_API_KEY
        r = requests.get(f"{ODDS_BASE}/{endpoint}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[MKT] odds request failed ({endpoint}): {e}")
        return None


def get_mlb_events() -> list:
    """Return today's MLB events with game_id, teams, commence_time."""
    data = _odds_request(f"sports/{SPORT_KEY}/events", {})
    if not data:
        return []
    return [
        {
            "id":           e["id"],
            "away":         e.get("away_team", ""),
            "home":         e.get("home_team", ""),
            "commence_utc": e.get("commence_time", ""),
        }
        for e in data
    ]


def get_odds_for_event(event_id: str) -> dict:
    """Pull ML, totals, F5 odds for a single event across all books."""
    ml   = _odds_request(
        f"sports/{SPORT_KEY}/events/{event_id}/odds",
        {"regions": REGIONS, "markets": MARKETS_ML, "oddsFormat": "american"}
    )
    tot  = _odds_request(
        f"sports/{SPORT_KEY}/events/{event_id}/odds",
        {"regions": REGIONS, "markets": MARKETS_TOT, "oddsFormat": "american"}
    )
    f5   = _odds_request(
        f"sports/{SPORT_KEY}/events/{event_id}/odds",
        {"regions": REGIONS, "markets": MARKETS_F5, "oddsFormat": "american"}
    )
    return {"ml": ml, "totals": tot, "f5": f5}


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
                if _names_match(t, away_team):
                    book_odds["away"] = prc
                elif _names_match(t, home_team):
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
        r = requests.get(f"{POLY_API}/markets", params={"slug": slug}, timeout=8)
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
        return None


def detect_line_movement(event_id: str, current_books: dict, side: str = "away") -> dict:
    """
    Detect sharp line movement by comparing opening vs current odds.
    Simplified: uses historical odds endpoint if available.
    Returns direction and magnitude.
    """
    hist = _odds_request(
        f"sports/{SPORT_KEY}/events/{event_id}/odds/history",
        {"regions": "us", "markets": MARKETS_ML, "oddsFormat": "american"}
    )
    if not hist:
        return {"direction": "unknown", "magnitude": 0}

    links = hist.get("links", {})
    prev  = hist.get("data", [])
    if len(prev) < 2:
        return {"direction": "unknown", "magnitude": 0}

    # Compare first available to current
    def get_price(snapshot):
        for bk in snapshot.get("bookmakers", []):
            if bk.get("key") == "pinnacle":
                for mkt in bk.get("markets", []):
                    for oc in mkt.get("outcomes", []):
                        if side in oc.get("name", "").lower():
                            return oc.get("price")
        return None

    opening = get_price(prev[0])
    current = get_price(prev[-1])

    if opening is None or current is None:
        return {"direction": "unknown", "magnitude": 0}

    dec_open = american_to_decimal(str(opening))
    dec_cur  = american_to_decimal(str(current))
    if not dec_open or not dec_cur:
        return {"direction": "unknown", "magnitude": 0}

    shift = round(dec_cur - dec_open, 3)
    if shift > 0.03:
        direction = "steam_away" if side == "away" else "steam_home"
    elif shift < -0.03:
        direction = "reverse" if side == "away" else "reverse_home"
    else:
        direction = "stable"

    return {"direction": direction, "magnitude": round(abs(shift), 3), "opening": opening, "current": current}


def full_market_snapshot(event_id: str, away_team: str, home_team: str,
                         away_code: str, home_code: str, game_date: str) -> dict:
    """Single call to get all market data for a game."""
    odds    = get_odds_for_event(event_id)
    ml_raw  = odds.get("ml")
    ml_books = _parse_ml_bookmakers(ml_raw, away_team, home_team)
    nv      = pinnacle_no_vig(ml_books, away_team, home_team)
    totals  = _parse_totals(odds.get("totals"))
    f5_books = _parse_ml_bookmakers(odds.get("f5"), away_team, home_team)
    lm      = detect_line_movement(event_id, ml_books)

    away_slug = TEAM_SLUGS.get(away_code, away_code.lower())
    home_slug = TEAM_SLUGS.get(home_code, home_code.lower())
    poly = polymarket_prob(away_slug, home_slug, game_date)

    best_away_book, best_away_odds = best_odds(ml_books, "away")
    best_home_book, best_home_odds = best_odds(ml_books, "home")

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
