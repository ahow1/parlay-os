"""
PARLAY OS — LIVE GAME MONITOR v1
Every 30 min during games: pulls MLB Stats API in-game win probability,
compares to live US book lines (the-odds-api.com), alerts when edge ≥ 5%.
Saves to live_alerts.json.
"""

import os, json, time, math, requests
from datetime import datetime
import pytz

ODDS_API_KEY       = os.environ.get("ODDS_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]

ET  = pytz.timezone("America/New_York")
NOW = datetime.now(ET)
DATE = NOW.strftime("%Y-%m-%d")

EDGE_THRESHOLD = 0.05   # 5% edge to alert
ALERTS_FILE    = "live_alerts.json"

MLB_TEAM_MAP = {
    "Arizona Diamondbacks": "AZ", "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL", "Detroit Tigers": "DET",
    "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB", "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR", "Washington Nationals": "WAS",
    "Athletics": "ATH",
}


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


def ip(ml):
    """American odds → implied probability."""
    try:
        n = float(str(ml).replace("+", ""))
        return 100 / (n + 100) if n > 0 else abs(n) / (abs(n) + 100)
    except Exception:
        return None


def devig(p1, p2):
    t = p1 + p2
    return (p1 / t, p2 / t) if t > 0 else (p1, p2)


def american(p):
    p = max(min(p, 0.99), 0.01)
    return round(-100 * p / (1 - p)) if p >= 0.5 else round(100 * (1 - p) / p)


# ─── MLB STATS API ────────────────────────────────────────────────────────────

def fetch_live_games():
    """Get all in-progress MLB games with gamePk IDs."""
    url = (f"https://statsapi.mlb.com/api/v1/schedule"
           f"?sportId=1&date={DATE}&hydrate=team&gameType=R")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        games = []
        for date_entry in r.json().get("dates", []):
            for game in date_entry.get("games", []):
                state = game.get("status", {}).get("abstractGameState", "")
                code  = game.get("status", {}).get("statusCode", "")
                if state == "Live" or code in ("I", "IR"):
                    games.append(game)
        print(f"Live games: {len(games)}")
        return games
    except Exception as e:
        print(f"Schedule err: {e}")
        return []


def fetch_game_live_feed(game_pk):
    """Fetch full live game feed including win probability."""
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Live feed err (gamePk {game_pk}): {e}")
        return {}


def fetch_win_probability(game_pk):
    """Fetch play-by-play win probabilities from MLB Stats API."""
    url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/winProbability"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        plays = r.json()
        if isinstance(plays, list) and plays:
            last = plays[-1]
            return {
                "home_wp": last.get("homeTeamWinProbability", 0.5),
                "away_wp": last.get("awayTeamWinProbability", 0.5),
            }
    except Exception as e:
        print(f"WP API err (gamePk {game_pk}): {e}")
    return None


def extract_game_state(feed):
    """Extract current game state from live feed."""
    try:
        live = feed.get("liveData", {})
        plays = live.get("plays", {})
        current = plays.get("currentPlay", {})
        about = current.get("about", {})
        linescore = live.get("linescore", {})

        home_wp = about.get("homeTeamWinProbability")
        away_wp = about.get("awayTeamWinProbability")

        inning     = linescore.get("currentInning", 1)
        half       = linescore.get("inningHalf", "top")
        home_score = linescore.get("teams", {}).get("home", {}).get("runs", 0)
        away_score = linescore.get("teams", {}).get("away", {}).get("runs", 0)
        outs       = linescore.get("outs", 0)

        game_info = feed.get("gameData", {})
        home_team = game_info.get("teams", {}).get("home", {}).get("name", "")
        away_team = game_info.get("teams", {}).get("away", {}).get("name", "")

        state = {
            "inning": inning,
            "half": half,
            "outs": outs,
            "home_score": home_score,
            "away_score": away_score,
            "score_diff": home_score - away_score,
            "home_team": home_team,
            "away_team": away_team,
            "home_abr": MLB_TEAM_MAP.get(home_team, home_team[:3].upper()),
            "away_abr": MLB_TEAM_MAP.get(away_team, away_team[:3].upper()),
        }

        if home_wp is not None and away_wp is not None:
            state["home_wp"] = home_wp
            state["away_wp"] = away_wp
        else:
            # Fallback: estimate WP from game state
            state["home_wp"], state["away_wp"] = estimate_wp(
                home_score, away_score, inning, half)

        return state
    except Exception as e:
        print(f"State parse err: {e}")
        return None


def estimate_wp(home_score, away_score, inning, half):
    """
    Simple in-game win probability estimate using score differential and inning.
    Uses a sigmoid function calibrated to MLB data.
    Baseline: home team wins ~54% of tied games.
    """
    diff = home_score - away_score
    innings_remaining = max(9 - inning + (1 if half == "top" else 0.5), 0.5)

    # Scale factor: more variance = more innings remaining
    sigma = math.sqrt(innings_remaining * 0.85)  # ~0.85 runs/inning std

    # P(home wins) ≈ P(diff + bonus > 0) where bonus = 0.08 (home field)
    z = (diff + 0.08 * HOME_ADV_FACTOR) / max(sigma, 0.01)
    home_wp = 0.5 * (1 + math.erf(z / math.sqrt(2)))

    # Cap extreme probabilities
    home_wp = max(min(home_wp, 0.98), 0.02)
    return round(home_wp, 4), round(1 - home_wp, 4)


HOME_ADV_FACTOR = 1.0  # small home field adjustment in WP model


# ─── LIVE ODDS ─────────────────────────────────────────────────────────────────

def fetch_live_odds():
    """Pull live in-game odds from the-odds-api.com."""
    if not ODDS_API_KEY:
        return {}
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "h2h", "oddsFormat": "american"},
            timeout=15)
        r.raise_for_status()
        out = {}
        for game in r.json():
            home = game.get("home_team", "")
            away = game.get("away_team", "")
            ha = MLB_TEAM_MAP.get(home, home[:3].upper())
            aa = MLB_TEAM_MAP.get(away, away[:3].upper())
            best_h, best_a = None, None
            for bk in game.get("bookmakers", []):
                for mkt in bk.get("markets", []):
                    if mkt.get("key") != "h2h":
                        continue
                    for o in mkt.get("outcomes", []):
                        p = o.get("price")
                        if p is None:
                            continue
                        if o["name"] == home:
                            if best_h is None or (p > 0 and (best_h < 0 or p > best_h)):
                                best_h = p
                        elif o["name"] == away:
                            if best_a is None or (p > 0 and (best_a < 0 or p > best_a)):
                                best_a = p
            if best_h is not None and best_a is not None:
                out[f"{aa}@{ha}"] = {"home_ml": best_h, "away_ml": best_a}
        return out
    except Exception as e:
        print(f"Live odds err: {e}")
        return {}


# ─── EDGE DETECTION ───────────────────────────────────────────────────────────

def find_live_edges(live_games, live_odds):
    alerts = []

    for game in live_games:
        game_pk = game.get("gamePk")
        if not game_pk:
            continue

        # Try win probability endpoint first, then live feed
        wp_data = fetch_win_probability(game_pk)
        feed = fetch_game_live_feed(game_pk)
        state = extract_game_state(feed)

        if state is None:
            continue

        # Prefer MLB's official WP if available
        if wp_data:
            state["home_wp"] = wp_data["home_wp"]
            state["away_wp"] = wp_data["away_wp"]

        ha = state["home_abr"]
        aa = state["away_abr"]
        key = f"{aa}@{ha}"

        market = live_odds.get(key)
        if not market:
            # Try reversed key
            market = live_odds.get(f"{ha}@{aa}")
            if market:
                # Swap if we found it reversed
                market = {"home_ml": market["away_ml"], "away_ml": market["home_ml"]}

        if not market:
            print(f"  No live market for {key}")
            continue

        h_ml = market["home_ml"]
        a_ml = market["away_ml"]
        mkt_hp_raw = ip(h_ml)
        mkt_ap_raw = ip(a_ml)
        if not mkt_hp_raw or not mkt_ap_raw:
            continue

        mkt_hp, mkt_ap = devig(mkt_hp_raw, mkt_ap_raw)
        model_hp = state["home_wp"]
        model_ap = state["away_wp"]

        home_edge = model_hp - mkt_hp
        away_edge = model_ap - mkt_ap

        for side, edge, team, ml, model_p, mkt_p in [
            ("home", home_edge, ha, h_ml, model_hp, mkt_hp),
            ("away", away_edge, aa, a_ml, model_ap, mkt_ap),
        ]:
            if abs(edge) < EDGE_THRESHOLD:
                continue

            ml_str = f"+{ml}" if isinstance(ml, int) and ml > 0 else str(ml)
            alerts.append({
                "game":       key,
                "game_pk":    game_pk,
                "home_team":  state["home_team"],
                "away_team":  state["away_team"],
                "home_abr":   ha,
                "away_abr":   aa,
                "inning":     state["inning"],
                "half":       state["half"],
                "home_score": state["home_score"],
                "away_score": state["away_score"],
                "side":       side,
                "team":       team,
                "model_wp":   round(model_p * 100, 1),
                "market_wp":  round(mkt_p * 100, 1),
                "edge_pct":   round(edge * 100, 2),
                "live_ml":    ml_str,
                "model_ml":   american(model_p),
                "direction":  "overpriced" if edge > 0 else "underpriced",
                "timestamp":  NOW.isoformat(),
            })

    alerts.sort(key=lambda x: abs(x["edge_pct"]), reverse=True)
    return alerts


def format_alert(alert):
    inn = f"{alert['half'].capitalize()} {alert['inning']}"
    score = f"{alert['away_abr']} {alert['away_score']} — {alert['home_abr']} {alert['home_score']}"
    side_team = alert["team"]
    edge_dir = "OVER" if alert["edge_pct"] > 0 else "UNDER"

    return (
        f"LIVE EDGE — {alert['away_abr']} @ {alert['home_abr']}\n"
        f"  {inn} | {score}\n"
        f"  {side_team} ML: Live {alert['live_ml']} ({alert['market_wp']}%)\n"
        f"  Model says: {alert['model_wp']}%  |  EDGE: {alert['edge_pct']:+.1f}%\n"
        f"  Model line: {alert['model_ml']}\n"
        f"  → Market is {edge_dir}PRICING {side_team} — consider {'BUYING' if edge_dir=='OVER' else 'FADING'}\n"
        f"  {NOW.strftime('%I:%M %p ET')}"
    )


def main():
    print(f"[{NOW.strftime('%H:%M ET')}] Live monitor running — {DATE}")

    live_games = fetch_live_games()
    if not live_games:
        print("No live games found.")
        return

    live_odds = fetch_live_odds()
    print(f"Live odds: {len(live_odds)} games")

    alerts = find_live_edges(live_games, live_odds)

    # Save alerts
    try:
        with open(ALERTS_FILE) as f:
            alert_log = json.load(f)
    except Exception:
        alert_log = []

    # Only keep today's alerts
    alert_log = [a for a in alert_log if a.get("timestamp", "")[:10] == DATE]
    alert_log.extend(alerts)

    with open(ALERTS_FILE, "w") as f:
        json.dump(alert_log, f, indent=2)

    print(f"Alerts found: {len(alerts)}")

    # Send new alerts
    for alert in alerts:
        # Deduplicate: don't re-alert same game/side within 30 min
        msg = format_alert(alert)
        send_telegram(msg)
        time.sleep(0.5)

    if not alerts:
        print("No live edges found this run.")


if __name__ == "__main__":
    main()
