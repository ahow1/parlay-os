"""PARLAY OS — sportsgameodds_client.py
Pulls MLB odds from SportsGameOdds (SGO) and normalizes them to decimal odds +
implied probability, independent of which sportsbook or market they came from.

Covers, per game: moneyline, totals, run-line (spread), open-vs-current line
for each, and player props (batter hits, home runs, total bases, RBI, pitcher Ks).

SGO Amateur (free) tier hard limits: 10 requests/min, 2,500 entities/month.
One request per cache refresh covers the ENTIRE day's MLB slate — never call
per-event. Do not lower SGO_CACHE_TTL_SEC without checking /account/usage;
the persistent Railway bot ticks far more often than GitHub Actions and will
burn the monthly cap fastest.
"""

import os
import json
import time
import logging
import threading

from api_client import get as _http_get
from math_engine import american_to_decimal, implied_prob, no_vig_prob, decimal_to_american

_log = logging.getLogger(__name__)

SGO_API_KEY = os.getenv("SPORTSGAMEODDS_API_KEY", "")
SGO_BASE    = "https://api.sportsgameodds.com/v2"
LEAGUE_ID   = "MLB"

CACHE_FILE    = os.getenv("SGO_CACHE_FILE", "sgo_cache.json")
CACHE_TTL_SEC = int(os.getenv("SGO_CACHE_TTL_SEC", "1800"))  # 30 min

# Player-prop stat IDs we normalize. SGO uses these exact statID strings
# (confirmed against a live response — they are not documented literally).
PROP_STAT_IDS = {
    "batting_hits":        "batter_hits",
    "batting_homeRuns":    "batter_home_runs",
    "batting_totalBases":  "batter_total_bases",
    "batting_RBI":         "batter_rbis",
    "pitching_strikeouts": "pitcher_strikeouts",
}

_MIN_CALL_GAP_SEC = 6.5   # keeps us under 10 req/min with margin
_rate_lock  = threading.Lock()
_last_call  = 0.0


def _rate_limit():
    """Enforce SGO's 10 req/min cap within this process. Cross-process bursts
    (GH Actions + Railway bot running at the same time) are covered by the
    on-disk cache instead — see fetch_mlb_slate()."""
    global _last_call
    with _rate_lock:
        now  = time.monotonic()
        wait = _MIN_CALL_GAP_SEC - (now - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.monotonic()


def _load_cache() -> dict | None:
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except Exception:
        return None


def _save_cache(cache: dict) -> None:
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        _log.warning(f"[SGO] failed to write cache: {e}")


def _cache_fresh(cache: dict | None) -> bool:
    if not cache:
        return False
    age = time.time() - cache.get("fetched_at", 0)
    return age < CACHE_TTL_SEC


def _normalize_odd(o: dict) -> dict:
    """Per-bookmaker decimal odds + implied probability, current and opening."""
    by_book = {}
    for book, info in (o.get("byBookmaker") or {}).items():
        am = info.get("odds")
        if am is None or not info.get("available", True):
            continue
        open_am = info.get("openOdds")
        by_book[book] = {
            "american":              am,
            "decimal":                american_to_decimal(str(am)),
            "implied_prob_pct":       implied_prob(str(am)),
            "open_american":          open_am,
            "open_decimal":           american_to_decimal(str(open_am)) if open_am is not None else None,
            "open_implied_prob_pct":  implied_prob(str(open_am)) if open_am is not None else None,
        }
    return by_book


def _normalize_event(ev: dict) -> dict:
    teams     = ev.get("teams") or {}
    home_name = ((teams.get("home") or {}).get("names") or {}).get("long", "")
    away_name = ((teams.get("away") or {}).get("names") or {}).get("long", "")
    odds      = ev.get("odds") or {}

    moneyline: dict = {}
    totals:    dict = {"line": None, "over": {}, "under": {}}
    spreads:   dict = {"home_line": None, "away_line": None, "home": {}, "away": {}}
    props:     list = []

    for o in odds.values():
        if o.get("periodID") != "game":
            continue  # full-game lines only for Step 1 — no 1st-5, 1st-inning, etc.
        stat   = o.get("statID")
        bettype = o.get("betTypeID")
        entity = o.get("statEntityID")
        side   = o.get("sideID")

        if stat == "points" and bettype == "ml" and entity in ("home", "away"):
            moneyline[entity] = _normalize_odd(o)

        elif stat == "points" and bettype == "ou" and entity == "all":
            totals["line"] = o.get("bookOverUnder")
            if side in ("over", "under"):
                totals[side] = _normalize_odd(o)

        elif stat == "points" and bettype == "sp" and entity in ("home", "away"):
            spreads[f"{entity}_line"] = o.get("bookSpread")
            spreads[entity] = _normalize_odd(o)

        elif stat in PROP_STAT_IDS and bettype == "ou" and side in ("over", "under"):
            props.append({
                "player_id": entity,
                "stat":      PROP_STAT_IDS[stat],
                "side":      side,
                "line":      o.get("bookOverUnder"),
                "by_book":   _normalize_odd(o),
            })

    return {
        "event_id":      ev.get("eventID"),
        "home":          home_name,
        "away":          away_name,
        "commence_utc":  (ev.get("status") or {}).get("startsAt", ""),
        "moneyline":     moneyline,
        "totals":        totals,
        "spreads":       spreads,
        "props":         props,
    }


def fetch_mlb_slate(force_refresh: bool = False) -> dict:
    """Return today's MLB slate, normalized, keyed by SGO eventID.

    Cached on disk for CACHE_TTL_SEC so repeated calls (brain.py's day/evening/
    west runs, the Railway bot's 15-min CLV loop) don't each burn a request —
    a normal slate fits in a single call regardless of game count.
    """
    cache = _load_cache()
    if not force_refresh and _cache_fresh(cache):
        return cache["data"]

    if not SGO_API_KEY:
        _log.warning("[SGO] SPORTSGAMEODDS_API_KEY not set — no odds data")
        return cache["data"] if cache else {}

    _rate_limit()
    try:
        r = _http_get(
            f"{SGO_BASE}/events/",
            params={
                "leagueID":             LEAGUE_ID,
                "oddsAvailable":        "true",
                "includeOpenCloseOdds": "true",
                "limit":                "50",
            },
            headers={"X-Api-Key": SGO_API_KEY},
            timeout=20,
            skip_cache=True,
        )
    except Exception as e:
        _log.warning(f"[SGO] slate fetch failed: {e} — using stale cache if available")
        return cache["data"] if cache else {}

    if r.status_code != 200:
        _log.warning(f"[SGO] slate fetch HTTP {r.status_code}: {r.text[:200]}")
        return cache["data"] if cache else {}

    raw        = r.json()
    normalized = {ev["eventID"]: _normalize_event(ev) for ev in raw.get("data", [])}
    _save_cache({"fetched_at": time.time(), "data": normalized})
    return normalized


_CONSENSUS_MARKETS = {
    "moneyline": ("away", "home"),
    "totals":    ("over", "under"),
    "spreads":   ("away", "home"),
}


def no_vig_consensus(event: dict, market: str = "moneyline") -> dict | None:
    """No-vig consensus line for one SGO event's two-sided market.

    SGO's free tier carries soft (retail) books only — no Pinnacle. This
    strips each soft book's vig with math_engine.no_vig_prob(), then averages
    the de-vigged probability across every book quoting both sides. That
    average stands in for Pinnacle as the CLV closing-line benchmark.

    market: "moneyline" (away/home), "totals" (over/under), or "spreads"
    (away/home run line, fixed at +/-1.5 in MLB). Returns None if no book
    quotes both sides.
    """
    if market not in _CONSENSUS_MARKETS:
        raise ValueError(f"no_vig_consensus: unsupported market {market!r}")
    key1, key2 = _CONSENSUS_MARKETS[market]
    book_lines = event.get(market) or {}
    side1 = book_lines.get(key1, {})
    side2 = book_lines.get(key2, {})

    common = sorted(set(side1) & set(side2))
    p1s, p2s = [], []
    for book in common:
        o1 = side1[book].get("american")
        o2 = side2[book].get("american")
        if o1 is None or o2 is None:
            continue
        nv = no_vig_prob(str(o1), str(o2))
        if nv.get("side1_true") is None:
            continue
        p1s.append(nv["side1_true"])
        p2s.append(nv["side2_true"])

    if not p1s:
        return None

    p1 = round(sum(p1s) / len(p1s), 2)
    p2 = round(sum(p2s) / len(p2s), 2)
    return {
        "market":           market,
        f"{key1}_prob_pct": p1,
        f"{key2}_prob_pct": p2,
        f"{key1}_american": decimal_to_american(100 / p1),
        f"{key2}_american": decimal_to_american(100 / p2),
        "books_used":       common,
        "n_books":          len(p1s),
    }


def get_event_by_teams(away_name: str, home_name: str, slate: dict | None = None) -> dict | None:
    """Look up a normalized event by away/home full team names (e.g. 'New York Mets')."""
    slate = slate if slate is not None else fetch_mlb_slate()
    for ev in slate.values():
        if ev["away"] == away_name and ev["home"] == home_name:
            return ev
    for ev in slate.values():
        if (away_name in ev["away"] or ev["away"] in away_name) and \
           (home_name in ev["home"] or ev["home"] in home_name):
            return ev
    return None


if __name__ == "__main__":
    slate = fetch_mlb_slate(force_refresh=True)
    print(f"SGO slate: {len(slate)} MLB games")
    if not slate:
        raise SystemExit("No games returned — check SPORTSGAMEODDS_API_KEY / oddsAvailable")

    ev = next(iter(slate.values()))
    print(f"\n{ev['away']} @ {ev['home']}  ({ev['commence_utc']})")

    print("\nMoneyline:")
    for side, books in ev["moneyline"].items():
        for book, o in books.items():
            print(f"  {side:5s} {book:12s} {o['american']:>5} -> dec {o['decimal']} "
                  f"implied {o['implied_prob_pct']}%  (open {o['open_american']})")

    print(f"\nTotals (line {ev['totals']['line']}):")
    for side in ("over", "under"):
        for book, o in ev["totals"][side].items():
            print(f"  {side:5s} {book:12s} {o['american']:>5} -> dec {o['decimal']} "
                  f"implied {o['implied_prob_pct']}%  (open {o['open_american']})")

    print(f"\nSpread (away {ev['spreads']['away_line']} / home {ev['spreads']['home_line']}):")
    for side in ("away", "home"):
        for book, o in ev["spreads"][side].items():
            print(f"  {side:5s} {book:12s} {o['american']:>5} -> dec {o['decimal']} "
                  f"implied {o['implied_prob_pct']}%")

    print(f"\nPlayer props ({len(ev['props'])} total, showing first 5):")
    for p in ev["props"][:5]:
        n_books = len(p["by_book"])
        print(f"  {p['player_id']:22s} {p['stat']:20s} {p['side']:5s} line={p['line']}  books={n_books}")
