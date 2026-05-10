"""
PARLAY OS — ODDS ARBITRAGE ENGINE v1
Compares US sportsbooks (the-odds-api.com) vs Kalshi vs Polymarket.
Flags lines where cross-market probability diverges ≥ 5%.
Saves to arbitrage_log.json. Sends Telegram alerts for top edges.
"""

import os, json, time, re, requests
from datetime import datetime
import pytz

ODDS_API_KEY       = os.environ.get("ODDS_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]

ET  = pytz.timezone("America/New_York")
NOW = datetime.now(ET)
DATE = NOW.strftime("%Y-%m-%d")

EDGE_THRESHOLD = 0.05   # 5% minimum edge to alert
LOG_FILE = "arbitrage_log.json"

MLB_TEAM_NAMES = {
    "AZ":  ["Arizona Diamondbacks", "Diamondbacks", "D-backs"],
    "ATL": ["Atlanta Braves", "Braves"],
    "BAL": ["Baltimore Orioles", "Orioles"],
    "BOS": ["Boston Red Sox", "Red Sox"],
    "CHC": ["Chicago Cubs", "Cubs"],
    "CWS": ["Chicago White Sox", "White Sox"],
    "CIN": ["Cincinnati Reds", "Reds"],
    "CLE": ["Cleveland Guardians", "Guardians"],
    "COL": ["Colorado Rockies", "Rockies"],
    "DET": ["Detroit Tigers", "Tigers"],
    "HOU": ["Houston Astros", "Astros"],
    "KC":  ["Kansas City Royals", "Royals"],
    "LAA": ["Los Angeles Angels", "Angels"],
    "LAD": ["Los Angeles Dodgers", "Dodgers"],
    "MIA": ["Miami Marlins", "Marlins"],
    "MIL": ["Milwaukee Brewers", "Brewers"],
    "MIN": ["Minnesota Twins", "Twins"],
    "NYM": ["New York Mets", "Mets"],
    "NYY": ["New York Yankees", "Yankees"],
    "ATH": ["Oakland Athletics", "Athletics", "Oakland"],
    "PHI": ["Philadelphia Phillies", "Phillies"],
    "PIT": ["Pittsburgh Pirates", "Pirates"],
    "SD":  ["San Diego Padres", "Padres"],
    "SF":  ["San Francisco Giants", "Giants"],
    "SEA": ["Seattle Mariners", "Mariners"],
    "STL": ["St. Louis Cardinals", "Cardinals"],
    "TB":  ["Tampa Bay Rays", "Rays"],
    "TEX": ["Texas Rangers", "Rangers"],
    "TOR": ["Toronto Blue Jays", "Blue Jays"],
    "WAS": ["Washington Nationals", "Nationals"],
}

MLB_TEAM_MAP = {alias.lower(): abr
                for abr, aliases in MLB_TEAM_NAMES.items()
                for alias in aliases}


def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for i, chunk in enumerate([text[i:i+4000] for i in range(0, len(text), 4000)]):
        try:
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk,
                                     "disable_web_page_preview": True}, timeout=10)
            if i > 0:
                time.sleep(0.5)
        except Exception as e:
            print(f"Telegram err: {e}")


def team_abr(name_str):
    """Normalize any team name string to 2-3 char abbreviation."""
    cleaned = name_str.strip().lower()
    if cleaned in MLB_TEAM_MAP:
        return MLB_TEAM_MAP[cleaned]
    # partial match
    for alias, abr in MLB_TEAM_MAP.items():
        if alias in cleaned or cleaned in alias:
            return abr
    return name_str[:3].upper()


def ip(ml):
    """American odds → implied probability."""
    try:
        n = float(str(ml).replace("+", ""))
        return 100 / (n + 100) if n > 0 else abs(n) / (abs(n) + 100)
    except Exception:
        return None


def devig_two(p1_raw, p2_raw):
    """Remove vig from two raw implied probabilities."""
    t = p1_raw + p2_raw
    if t <= 0:
        return p1_raw, p2_raw
    return p1_raw / t, p2_raw / t


def american_str(p):
    """Probability → American odds string."""
    p = max(min(p, 0.99), 0.01)
    if p >= 0.5:
        return f"{round(-100 * p / (1 - p))}"
    return f"+{round(100 * (1 - p) / p)}"


# ─── FETCH: THE-ODDS-API (US SPORTSBOOKS) ─────────────────────────────────────

def fetch_us_odds():
    if not ODDS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "h2h", "oddsFormat": "american"},
            timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Odds API err: {e}")
        return []


def parse_us_odds(raw):
    """Returns {away_abr@home_abr: {home_p, away_p, home_ml, away_ml, books}} dict."""
    out = {}
    for game in raw:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        ha = team_abr(home)
        aa = team_abr(away)

        home_prices, away_prices = [], []
        for bk in game.get("bookmakers", []):
            for mkt in bk.get("markets", []):
                if mkt.get("key") != "h2h":
                    continue
                for o in mkt.get("outcomes", []):
                    p = o.get("price")
                    if p is None:
                        continue
                    if o["name"] == home:
                        home_prices.append(p)
                    elif o["name"] == away:
                        away_prices.append(p)

        if not home_prices or not away_prices:
            continue

        # Use best available (highest) price for each side
        best_h = max(home_prices, key=lambda x: (x if x > 0 else -(100**2 / x)))
        best_a = max(away_prices, key=lambda x: (x if x > 0 else -(100**2 / x)))
        hp_raw = ip(best_h)
        ap_raw = ip(best_a)
        if hp_raw and ap_raw:
            hp, ap = devig_two(hp_raw, ap_raw)
        else:
            continue

        # Consensus (average)
        avg_hp = sum(ip(p) or 0 for p in home_prices) / len(home_prices)
        avg_ap = sum(ip(p) or 0 for p in away_prices) / len(away_prices)
        con_hp, con_ap = devig_two(avg_hp, avg_ap)

        key = f"{aa}@{ha}"
        out[key] = {
            "home_abr": ha, "away_abr": aa,
            "home_team": home, "away_team": away,
            "home_p": round(hp, 4), "away_p": round(ap, 4),
            "home_consensus_p": round(con_hp, 4), "away_consensus_p": round(con_ap, 4),
            "home_ml": best_h, "away_ml": best_a,
            "book_count": len(home_prices),
            "event_id": game.get("id", ""),
        }
    return out


# ─── FETCH: KALSHI ─────────────────────────────────────────────────────────────

def fetch_kalshi():
    """Pull open MLB markets from Kalshi public API."""
    headers = {"Accept": "application/json"}
    results = {}
    try:
        # Try the events endpoint filtered to sports
        r = requests.get(
            "https://trading-api.kalshi.com/trade-api/v2/events",
            params={"status": "open", "limit": 200, "series_ticker": "MLB"},
            headers=headers, timeout=15)
        if r.status_code == 200:
            events = r.json().get("events", [])
        else:
            # Fall back to general search
            r2 = requests.get(
                "https://trading-api.kalshi.com/trade-api/v2/markets",
                params={"status": "open", "limit": 500},
                headers=headers, timeout=15)
            r2.raise_for_status()
            markets = r2.json().get("markets", [])
            events = [{"markets": markets}]

        for evt in events:
            for mkt in evt.get("markets", []):
                title = mkt.get("title", "") + " " + mkt.get("subtitle", "")
                # Only today's games
                if DATE.replace("-", "") not in (mkt.get("open_date", "") + mkt.get("close_date", "") + title):
                    if DATE not in title and NOW.strftime("%B %d") not in title:
                        continue

                yes_ask = mkt.get("yes_ask") or mkt.get("last_price")
                no_ask  = mkt.get("no_ask")
                if yes_ask is None:
                    continue

                # Kalshi prices are in cents (1-99 range)
                yes_p = yes_ask / 100 if yes_ask > 1 else yes_ask
                no_p  = (100 - yes_ask) / 100 if yes_ask > 1 else (1 - yes_ask)

                # Try to identify the team from the title
                for abr, aliases in MLB_TEAM_NAMES.items():
                    for alias in aliases:
                        if alias.lower() in title.lower():
                            results[abr] = {
                                "source": "kalshi",
                                "market_id": mkt.get("ticker", ""),
                                "title": title[:80],
                                "yes_p": round(yes_p, 4),
                                "no_p": round(no_p, 4),
                                "raw_yes": yes_ask,
                            }
                            break

        print(f"Kalshi: {len(results)} MLB team contracts found")
    except Exception as e:
        print(f"Kalshi err: {e}")
    return results


# ─── FETCH: POLYMARKET ─────────────────────────────────────────────────────────

# Full team name → Polymarket slug segment, matching the URL pattern:
# polymarket.com/sports/mlb/mlb-{away_slug}-{home_slug}-YYYY-MM-DD
TEAM_SLUGS = {
    "AZ":  "arizona-diamondbacks",
    "ATL": "atlanta-braves",
    "BAL": "baltimore-orioles",
    "BOS": "boston-red-sox",
    "CHC": "chicago-cubs",
    "CWS": "chicago-white-sox",
    "CIN": "cincinnati-reds",
    "CLE": "cleveland-guardians",
    "COL": "colorado-rockies",
    "DET": "detroit-tigers",
    "HOU": "houston-astros",
    "KC":  "kansas-city-royals",
    "LAA": "los-angeles-angels",
    "LAD": "los-angeles-dodgers",
    "MIA": "miami-marlins",
    "MIL": "milwaukee-brewers",
    "MIN": "minnesota-twins",
    "NYM": "new-york-mets",
    "NYY": "new-york-yankees",
    "ATH": "oakland-athletics",
    "PHI": "philadelphia-phillies",
    "PIT": "pittsburgh-pirates",
    "SD":  "san-diego-padres",
    "SF":  "san-francisco-giants",
    "SEA": "seattle-mariners",
    "STL": "st-louis-cardinals",
    "TB":  "tampa-bay-rays",
    "TEX": "texas-rangers",
    "TOR": "toronto-blue-jays",
    "WAS": "washington-nationals",
}


def _parse_outcomes(mkt):
    """Return (outcomes_list, prices_list) from a Gamma API market dict."""
    try:
        outcomes = json.loads(mkt.get("outcomes", "[]"))
    except (ValueError, TypeError):
        outcomes = mkt.get("outcomes", []) or []
    try:
        prices = json.loads(mkt.get("outcomePrices", "[]"))
    except (ValueError, TypeError):
        prices = mkt.get("outcomePrices", []) or []
    return outcomes, prices


def fetch_polymarket(us_games):
    """Fetch Polymarket moneyline prices via direct game-to-slug mapping.

    For each game already found in us_games (keyed "AWAY@HOME"), we build
    the canonical Polymarket event slug:
        mlb-{away_slug}-{home_slug}-YYYY-MM-DD
    and query the Gamma API for that specific event only.

    No broad event scanning, no cross-game team-name matching.
    """
    results = {}

    for game_key, game in us_games.items():
        aa = game["away_abr"]
        ha = game["home_abr"]

        away_slug = TEAM_SLUGS.get(aa)
        home_slug = TEAM_SLUGS.get(ha)
        if not away_slug or not home_slug:
            print(f"  Polymarket: no slug mapping for {aa} or {ha} — skipped")
            continue

        event_slug = f"mlb-{away_slug}-{home_slug}-{DATE}"

        try:
            r = requests.get(
                "https://gamma-api.polymarket.com/events",
                params={"slug": event_slug},
                timeout=10)
            r.raise_for_status()
            data = r.json()
            events = data if isinstance(data, list) else data.get("events", [])

            # The slug query should return exactly one event; verify it matches.
            event = next(
                (e for e in events if e.get("slug") == event_slug), None
            )
            if not event:
                print(f"  Polymarket: no event found for {event_slug}")
                continue

            # Scan this event's markets for the moneyline (2 named-team outcomes).
            for mkt in event.get("markets", []):
                outcomes, prices = _parse_outcomes(mkt)

                # Moneyline has exactly 2 outcomes.
                if len(outcomes) != 2 or len(prices) != 2:
                    continue

                # Both must be team names, not "Yes"/"No".
                if any(str(o).strip().lower() in ("yes", "no") for o in outcomes):
                    continue

                # Match each outcome to the two teams we already know.
                matched = {}
                for i, outcome in enumerate(outcomes):
                    try:
                        price = float(prices[i])
                    except (TypeError, ValueError):
                        continue
                    abr = team_abr(str(outcome))
                    if abr in (aa, ha):
                        matched[abr] = price

                if len(matched) < 2:
                    # Outcomes didn't resolve to the expected pair — skip market.
                    continue

                for abr, price in matched.items():
                    results[abr] = {
                        "source":    "polymarket",
                        "market_id": mkt.get("id", ""),
                        "question":  mkt.get("question", "")[:80],
                        "slug":      event_slug,
                        "game_key":  game_key,
                        "win_p":     round(price, 4),
                    }

                # Found the moneyline — no need to check other markets.
                break

        except Exception as e:
            print(f"  Polymarket err ({event_slug}): {e}")

        time.sleep(0.15)  # be polite to the Gamma API

    print(f"Polymarket: {len(results)} game contracts matched for {DATE}")
    return results


# ─── CROSS-MARKET ANALYSIS ────────────────────────────────────────────────────

def analyze_edges(us_odds, kalshi_data, poly_data):
    """Compare US book prices vs Kalshi/Polymarket, return list of edge opportunities."""
    edges = []

    for key, us in us_odds.items():
        ha = us["home_abr"]
        aa = us["away_abr"]

        for side in ("home", "away"):
            abr = ha if side == "home" else aa
            us_p = us["home_p"] if side == "home" else us["away_p"]

            comparisons = []
            if abr in kalshi_data:
                k = kalshi_data[abr]
                comparisons.append(("kalshi", k["yes_p"], k["title"]))
            if abr in poly_data:
                p = poly_data[abr]
                comparisons.append(("polymarket", p["win_p"], p["question"]))

            for source, alt_p, label in comparisons:
                diff = us_p - alt_p
                abs_diff = abs(diff)
                if abs_diff < EDGE_THRESHOLD:
                    continue

                # Determine which market is offering the edge
                if diff > 0:
                    # US books price team HIGHER than Kalshi/Poly → buy on alt market (YES)
                    action = f"BUY {abr} YES on {source.upper()} ({alt_p*100:.1f}%) vs US books ({us_p*100:.1f}%)"
                    direction = "buy_alt"
                else:
                    # US books price team LOWER → bet them on US books
                    action = f"BET {abr} ML on US books ({us_p*100:.1f}%) vs {source.upper()} ({alt_p*100:.1f}%)"
                    direction = "buy_us"

                edges.append({
                    "game": key,
                    "team": abr,
                    "side": side,
                    "us_p": round(us_p, 4),
                    "alt_p": round(alt_p, 4),
                    "alt_source": source,
                    "edge_pct": round(abs_diff * 100, 2),
                    "direction": direction,
                    "action": action,
                    "us_ml": us["home_ml"] if side == "home" else us["away_ml"],
                    "alt_label": label[:60],
                    "us_books": us["book_count"],
                    "timestamp": NOW.isoformat(),
                })

    # Also find within-US book spread (consensus vs best price)
    for key, us in us_odds.items():
        for side in ("home", "away"):
            best_p = us["home_p"] if side == "home" else us["away_p"]
            con_p  = us["home_consensus_p"] if side == "home" else us["away_consensus_p"]
            diff   = best_p - con_p
            if diff >= EDGE_THRESHOLD:
                abr = us["home_abr"] if side == "home" else us["away_abr"]
                edges.append({
                    "game": key,
                    "team": abr,
                    "side": side,
                    "us_p": round(best_p, 4),
                    "alt_p": round(con_p, 4),
                    "alt_source": "consensus",
                    "edge_pct": round(diff * 100, 2),
                    "direction": "line_shop",
                    "action": f"SHOP: {abr} best line {best_p*100:.1f}% vs avg {con_p*100:.1f}% ({diff*100:.1f}% off-market)",
                    "us_ml": us["home_ml"] if side == "home" else us["away_ml"],
                    "alt_label": f"{us['book_count']} books consensus",
                    "us_books": us["book_count"],
                    "timestamp": NOW.isoformat(),
                })

    edges.sort(key=lambda x: x["edge_pct"], reverse=True)
    return edges


def format_alert(edges, us_odds):
    if not edges:
        return f"PARLAY OS — ARBITRAGE — {DATE}\nNo 5%+ cross-market edges found.\n{NOW.strftime('%I:%M %p ET')}"

    lines = [
        f"PARLAY OS — ARBITRAGE ENGINE — {DATE}",
        f"Found {len(edges)} edge(s) ≥{EDGE_THRESHOLD*100:.0f}%",
        "",
    ]
    for e in edges[:5]:
        lines += [
            f"  [{e['alt_source'].upper()}] {e['game']} | {e['team']}",
            f"  Edge: {e['edge_pct']:+.1f}%  ({e['us_p']*100:.1f}% vs {e['alt_p']*100:.1f}%)",
            f"  → {e['action']}",
            "",
        ]
    lines.append(f"Generated {NOW.strftime('%I:%M %p ET')} — Parlay OS")
    return "\n".join(lines)


def main():
    print(f"[{NOW.strftime('%H:%M ET')}] Arbitrage engine running — {DATE}")

    us_raw  = fetch_us_odds()
    us_odds = parse_us_odds(us_raw)
    kalshi  = fetch_kalshi()
    poly    = fetch_polymarket(us_odds)

    print(f"US books: {len(us_odds)} games | Kalshi: {len(kalshi)} | Poly: {len(poly)}")

    edges = analyze_edges(us_odds, kalshi, poly)

    # Save to log
    try:
        with open(LOG_FILE) as f:
            log = json.load(f)
    except Exception:
        log = []

    log_entry = {
        "date": DATE,
        "timestamp": NOW.isoformat(),
        "games_found": len(us_odds),
        "kalshi_markets": len(kalshi),
        "poly_markets": len(poly),
        "edges": edges,
    }
    # Keep last 48 hours of entries
    log = [e for e in log if e.get("date", "") >= (NOW.strftime("%Y-%m-%d"))]
    log.append(log_entry)
    with open(LOG_FILE, "w") as f:
        json.dump(log, f, indent=2)

    print(f"  Edges ≥{EDGE_THRESHOLD*100:.0f}%: {len(edges)}")

    if edges:
        msg = format_alert(edges, us_odds)
        send_telegram(msg)
    else:
        print("  No significant edges — no Telegram alert sent")

    print(f"Done. Log entry saved.")


if __name__ == "__main__":
    main()
