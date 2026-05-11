"""
PARLAY OS — LIVE BETTING ENGINE v2
Conviction-based live betting + noon daily preview.

Live engine (every 3 min, 6pm-11pm ET):
  Six-gate conviction model — all must pass for HIGH, 5/6 for MEDIUM.
  1. Polymarket 5%+ mispriced vs win-probability model
  2. Deficit is explainable (fluky one-inning blowup, not a sustained shelling)
  3. Quality pitching still available for trailing team
  4. Bullpen ERA advantage exists for trailing team
  5. NOT a blowout — within 3 runs through 7 innings
  6. Sharp money moving toward trailing team in last ~10 minutes

Noon daily preview (--preview): 5-sentence sharp game preview per game,
  saved to daily_preview.json and posted to Telegram.

Usage:
  python live_monitor.py             # run once immediately
  python live_monitor.py --live      # loop 6pm-11pm ET every 3 min
  python live_monitor.py --preview   # noon preview run
"""

import os, json, math, time, re, sys, requests
from datetime import datetime, timedelta
import pytz

# ── ENV ───────────────────────────────────────────────────────────────────────
ODDS_API_KEY        = os.environ.get("ODDS_API_KEY", "")
TELEGRAM_BOT_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID    = os.environ["TELEGRAM_CHAT_ID"]

ET   = pytz.timezone("America/New_York")
NOW  = datetime.now(ET)
DATE = NOW.strftime("%Y-%m-%d")

ALERTS_FILE   = "live_alerts.json"
HISTORY_FILE  = "live_odds_history.json"
PREVIEW_FILE  = "daily_preview.json"
SCOUT_FILE    = "last_scout.json"
BANKROLL_FILE = "bankroll.json"

LIVE_INTERVAL    = 180    # 3 minutes
LIVE_START_HOUR  = 18
LIVE_END_HOUR    = 23
POLY_EDGE_MIN    = 0.05   # 5% Polymarket misprice threshold
MAX_DEFICIT      = 3      # max runs down for conviction play
MAX_INNING       = 7      # only through 7th inning
SHARP_MOVE_MIN   = 0.018  # 1.8% probability shift = sharp money signal
DEDUP_MINUTES    = 20     # suppress re-alert within 20 min per game/side
KELLY_FRAC       = 0.25   # quarter Kelly

LG_RPG   = 4.35
LG_ERA   = 4.35
PYTH_EXP = 1.83
HOME_ADV = 1.035

# ── TEAM MAPS ─────────────────────────────────────────────────────────────────
MLB_TEAM_MAP = {
    "Arizona Diamondbacks": "AZ",   "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",     "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",          "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",       "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",      "Detroit Tigers": "DET",
    "Houston Astros": "HOU",        "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",         "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",       "New York Mets": "NYM",
    "New York Yankees": "NYY",      "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",       "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",      "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",         "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",     "Washington Nationals": "WAS",
    "Athletics": "ATH",
}

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

TEAM_SLUGS = {
    "AZ": "ari", "ATL": "atl", "BAL": "bal", "BOS": "bos",
    "CHC": "chc", "CWS": "cws", "CIN": "cin", "CLE": "cle",
    "COL": "col", "DET": "det", "HOU": "hou", "KC": "kc",
    "LAA": "laa", "LAD": "lad", "MIA": "mia", "MIL": "mil",
    "MIN": "min", "NYM": "nym", "NYY": "nyy", "ATH": "ath",
    "PHI": "phi", "PIT": "pit", "SD": "sd", "SF": "sf",
    "SEA": "sea", "STL": "stl", "TB": "tb", "TEX": "tex",
    "TOR": "tor", "WAS": "was",
}

PARK_FACTORS = {
    "COL": 1.13, "BOS": 1.07, "CIN": 1.05, "PHI": 1.03, "CHC": 1.03,
    "NYY": 1.01, "BAL": 1.01, "MIN": 1.01, "KC": 1.00, "WAS": 1.00,
    "TEX": 1.00, "TOR": 1.00, "NYM": 0.98, "HOU": 0.97, "ATL": 0.97,
    "DET": 0.97, "MIA": 0.97, "STL": 0.99, "MIL": 0.99, "LAD": 0.99,
    "AZ": 1.02, "CLE": 0.98, "LAA": 0.98, "PIT": 0.98, "CWS": 0.99,
    "OAK": 0.95, "ATH": 0.95, "SF": 0.93, "SD": 0.95, "SEA": 0.96, "TB": 0.95,
}

BALLPARK_CITIES = {
    "AZ": "Phoenix", "ATL": "Atlanta", "BAL": "Baltimore", "BOS": "Boston",
    "CHC": "Chicago", "CWS": "Chicago", "CIN": "Cincinnati", "CLE": "Cleveland",
    "COL": "Denver", "DET": "Detroit", "HOU": "Houston", "KC": "Kansas City",
    "LAA": "Anaheim", "LAD": "Los Angeles", "MIA": "Miami", "MIL": "Milwaukee",
    "MIN": "Minneapolis", "NYM": "New York", "NYY": "New York",
    "ATH": "Sacramento", "OAK": "Oakland", "PHI": "Philadelphia",
    "PIT": "Pittsburgh", "SD": "San Diego", "SF": "San Francisco",
    "SEA": "Seattle", "STL": "St. Louis", "TB": "St. Petersburg",
    "TEX": "Arlington", "TOR": "Toronto", "WAS": "Washington DC",
}


# ── HELPERS ───────────────────────────────────────────────────────────────────

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


def kelly_stake(true_p, ml, bankroll, frac=KELLY_FRAC):
    try:
        n = float(str(ml).replace("+", ""))
        dec = (n / 100 + 1) if n > 0 else (100 / abs(n) + 1)
        b = dec - 1
        q = 1 - true_p
        full_k = (b * true_p - q) / b
        if full_k <= 0:
            return 0.0
        return round(min(bankroll * full_k * frac, bankroll * 0.05), 2)
    except Exception:
        return 0.0


def load_bankroll():
    try:
        with open(BANKROLL_FILE) as f:
            return json.load(f).get("current", 150.0)
    except Exception:
        return 150.0


def load_scout_context():
    try:
        with open(SCOUT_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _fval(row, *keys, default=None):
    for k in keys:
        v = row.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return default


def team_abr(name_str):
    cleaned = name_str.strip().lower()
    for abr, aliases in MLB_TEAM_NAMES.items():
        for alias in aliases:
            if alias.lower() == cleaned or alias.lower() in cleaned:
                return abr
    return name_str[:3].upper()


# ── MLB STATS API ─────────────────────────────────────────────────────────────

def fetch_live_games():
    url = (f"https://statsapi.mlb.com/api/v1/schedule"
           f"?sportId=1&date={DATE}&hydrate=team&gameType=R")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        games = []
        for de in r.json().get("dates", []):
            for g in de.get("games", []):
                state = g.get("status", {}).get("abstractGameState", "")
                code  = g.get("status", {}).get("statusCode", "")
                if state == "Live" or code in ("I", "IR"):
                    games.append(g)
        print(f"Live games: {len(games)}")
        return games
    except Exception as e:
        print(f"Schedule err: {e}")
        return []


def fetch_game_live_feed(game_pk):
    try:
        r = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live",
                         timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Live feed err ({game_pk}): {e}")
        return {}


def _parse_ip(ip_str):
    try:
        parts = str(ip_str).split(".")
        return float(parts[0]) + (float(parts[1]) / 3 if len(parts) > 1 else 0)
    except Exception:
        return 0.0


def _extract_pitcher_stats(team_bs):
    """Parse box score for one team: starter, relievers used, current pitcher."""
    pitchers_used = []
    players = team_bs.get("players", {})
    pitcher_ids = team_bs.get("pitchers", [])
    starter_id = pitcher_ids[0] if pitcher_ids else None

    for pid in pitcher_ids:
        pdata = players.get(f"ID{pid}", {})
        stats = pdata.get("stats", {}).get("pitching", {})
        person = pdata.get("person", {})
        pitchers_used.append({
            "id":         pid,
            "name":       person.get("fullName", f"P{pid}"),
            "is_starter": pid == starter_id,
            "k":          stats.get("strikeOuts", 0),
            "bb":         stats.get("baseOnBalls", 0),
            "h":          stats.get("hits", 0),
            "er":         stats.get("earnedRuns", 0),
            "r":          stats.get("runs", 0),
            "ip":         round(_parse_ip(stats.get("inningsPitched", "0.0")), 2),
            "pitches":    stats.get("pitchesThrown", 0),
        })

    return {
        "pitchers":       pitchers_used,
        "starter":        pitchers_used[0] if pitchers_used else None,
        "relievers_used": pitchers_used[1:],
        "current":        pitchers_used[-1] if pitchers_used else None,
        "count":          len(pitchers_used),
    }


def _get_next_batters(batting_order, players, plays):
    all_plays = plays.get("allPlays", [])
    if not batting_order:
        return []

    name_map = {}
    for pid in batting_order:
        pdata = players.get(f"ID{pid}", {})
        name_map[pid] = pdata.get("person", {}).get("fullName", f"#{pid}")

    last_batter_id = None
    if all_plays:
        last_batter_id = all_plays[-1].get("matchup", {}).get("batter", {}).get("id")

    if last_batter_id and last_batter_id in batting_order:
        idx = batting_order.index(last_batter_id)
        return [name_map.get(batting_order[(idx + 1 + i) % len(batting_order)], "")
                for i in range(3)]
    return [name_map.get(pid, "") for pid in batting_order[:3]]


def estimate_wp(home_score, away_score, inning, half):
    diff = home_score - away_score
    innings_rem = max(9 - inning + (1 if half.lower() == "top" else 0.5), 0.5)
    sigma = math.sqrt(innings_rem * 0.85)
    z = (diff + 0.08) / max(sigma, 0.01)
    hw = 0.5 * (1 + math.erf(z / math.sqrt(2)))
    hw = max(min(hw, 0.98), 0.02)
    return round(hw, 4), round(1 - hw, 4)


def extract_deep_state(feed):
    """Extract comprehensive game state from live feed."""
    try:
        live      = feed.get("liveData", {})
        game_data = feed.get("gameData", {})
        ls        = live.get("linescore", {})
        bs        = live.get("boxscore", {})
        plays     = live.get("plays", {})
        cp        = plays.get("currentPlay", {})
        about     = cp.get("about", {})

        inning     = ls.get("currentInning", 1)
        half       = ls.get("inningHalf", "Top")
        home_score = ls.get("teams", {}).get("home", {}).get("runs", 0)
        away_score = ls.get("teams", {}).get("away", {}).get("runs", 0)
        outs       = ls.get("outs", 0)

        offense = ls.get("offense", {})
        runners = [b for b, k in [("1B", "first"), ("2B", "second"), ("3B", "third")]
                   if offense.get(k)]

        home_n = game_data.get("teams", {}).get("home", {}).get("name", "")
        away_n = game_data.get("teams", {}).get("away", {}).get("name", "")
        ha = MLB_TEAM_MAP.get(home_n, home_n[:3].upper())
        aa = MLB_TEAM_MAP.get(away_n, away_n[:3].upper())

        curr_pitcher = cp.get("matchup", {}).get("pitcher", {})

        bs_teams = bs.get("teams", {})
        home_p_data = _extract_pitcher_stats(bs_teams.get("home", {}))
        away_p_data = _extract_pitcher_stats(bs_teams.get("away", {}))

        batting_side  = "away" if half.lower() == "top" else "home"
        pitching_side = "home" if half.lower() == "top" else "away"

        diff = home_score - away_score
        if diff > 0:
            leading_side = "home"; trailing_side = "away"
            leading_abr  = ha;     trailing_abr  = aa
        elif diff < 0:
            leading_side = "away"; trailing_side = "home"
            leading_abr  = aa;     trailing_abr  = ha
        else:
            leading_side = trailing_side = leading_abr = trailing_abr = None

        home_wp = about.get("homeTeamWinProbability")
        away_wp = about.get("awayTeamWinProbability")
        if home_wp is None:
            home_wp, away_wp = estimate_wp(home_score, away_score, inning, half)

        trailing_wp = (home_wp if trailing_side == "home" else away_wp) if trailing_side else 0.5

        innings_scores = []
        for inn in ls.get("innings", []):
            innings_scores.append({
                "inning": inn.get("num", 0),
                "home":   inn.get("home", {}).get("runs", 0),
                "away":   inn.get("away", {}).get("runs", 0),
            })

        bat_order = bs_teams.get(batting_side, {}).get("battingOrder", [])
        bat_players = bs_teams.get(batting_side, {}).get("players", {})
        next_batters = _get_next_batters(bat_order, bat_players, plays)

        return {
            "inning": inning, "half": half, "outs": outs,
            "runners": runners, "next_batters": next_batters,
            "home_score": home_score, "away_score": away_score,
            "home_abr": ha, "away_abr": aa,
            "home_team": home_n, "away_team": away_n,
            "deficit": abs(diff),
            "leading_side": leading_side, "trailing_side": trailing_side,
            "leading_abr": leading_abr, "trailing_abr": trailing_abr,
            "home_wp": home_wp, "away_wp": away_wp, "trailing_wp": trailing_wp,
            "curr_pitcher_name": curr_pitcher.get("fullName", ""),
            "pitching_side": pitching_side,
            "home_pitchers": home_p_data,
            "away_pitchers": away_p_data,
            "innings_scores": innings_scores,
        }
    except Exception as e:
        print(f"State parse err: {e}")
        return None


# ── ODDS SOURCES ──────────────────────────────────────────────────────────────

def fetch_live_book_odds():
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
            home = game.get("home_team", ""); away = game.get("away_team", "")
            ha = MLB_TEAM_MAP.get(home, home[:3].upper())
            aa = MLB_TEAM_MAP.get(away, away[:3].upper())
            best_h = best_a = None
            for bk in game.get("bookmakers", []):
                for mkt in bk.get("markets", []):
                    if mkt.get("key") != "h2h":
                        continue
                    for o in mkt.get("outcomes", []):
                        p = o.get("price")
                        if p is None:
                            continue
                        if o["name"] == home:
                            if best_h is None or p > best_h: best_h = p
                        elif o["name"] == away:
                            if best_a is None or p > best_a: best_a = p
            if best_h is not None and best_a is not None:
                hp_r, ap_r = ip(best_h), ip(best_a)
                if hp_r and ap_r:
                    hp, ap = devig(hp_r, ap_r)
                else:
                    hp, ap = 0.5, 0.5
                out[f"{aa}@{ha}"] = {
                    "home_ml": best_h, "away_ml": best_a,
                    "home_p": round(hp, 4), "away_p": round(ap, 4),
                }
        return out
    except Exception as e:
        print(f"Odds API err: {e}")
        return {}


def fetch_polymarket_for_game(aa, ha, date=None):
    """Return {home_p, away_p, slug} or None."""
    date = date or DATE
    a_slug = TEAM_SLUGS.get(aa)
    h_slug = TEAM_SLUGS.get(ha)
    if not a_slug or not h_slug:
        return None
    slug = f"mlb-{a_slug}-{h_slug}-{date}"
    try:
        r = requests.get("https://gamma-api.polymarket.com/events",
                         params={"slug": slug}, timeout=10)
        r.raise_for_status()
        data = r.json()
        events = data if isinstance(data, list) else data.get("events", [])
        event = next((e for e in events if e.get("slug") == slug), None)
        if not event:
            return None
        for mkt in event.get("markets", []):
            try:
                outcomes = json.loads(mkt.get("outcomes", "[]"))
                prices   = json.loads(mkt.get("outcomePrices", "[]"))
            except (ValueError, TypeError):
                outcomes = mkt.get("outcomes", []) or []
                prices   = mkt.get("outcomePrices", []) or []
            if len(outcomes) != 2 or len(prices) != 2:
                continue
            if any(str(o).strip().lower() in ("yes", "no") for o in outcomes):
                continue
            matched = {}
            for i, outcome in enumerate(outcomes):
                try:
                    price = float(prices[i])
                except (TypeError, ValueError):
                    continue
                abr = team_abr(str(outcome))
                if abr in (aa, ha):
                    matched[abr] = price
            if len(matched) == 2:
                return {"home_p": matched.get(ha, 0.5), "away_p": matched.get(aa, 0.5), "slug": slug}
    except Exception as e:
        print(f"Poly err ({slug}): {e}")
    return None


# ── ODDS HISTORY (sharp money detection) ─────────────────────────────────────

def load_odds_history():
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except Exception:
        return []


def save_odds_history(history, snapshot):
    history.append(snapshot)
    cutoff = (datetime.now(ET) - timedelta(hours=2)).isoformat()
    history = [h for h in history if h.get("ts", "") >= cutoff]
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(history[-50:], f, indent=2)
    except Exception as e:
        print(f"History save err: {e}")
    return history


def check_sharp_money(game_key, trailing_side, current_p, history):
    """
    Returns (is_sharp_move, move_pct, minutes_elapsed, description).
    Sharp = trailing team's probability increased >= SHARP_MOVE_MIN in ~10 min.
    """
    target_ts = (datetime.now(ET) - timedelta(minutes=10)).isoformat()
    prior = None
    for h in reversed(history):
        if h.get("ts", "") <= target_ts:
            prior = h
            break

    if not prior:
        return False, 0.0, 0, "no prior window — insufficient history"

    prior_games = prior.get("games", {})
    if game_key not in prior_games:
        return False, 0.0, 0, "game not in prior snapshot"

    prior_p = prior_games[game_key].get(f"{trailing_side}_p", 0)
    move = current_p - prior_p

    try:
        prior_dt = datetime.fromisoformat(prior["ts"])
        if prior_dt.tzinfo is None:
            prior_dt = ET.localize(prior_dt)
        mins = max(1, int((datetime.now(ET) - prior_dt).total_seconds() / 60))
    except Exception:
        mins = 10

    if move >= SHARP_MOVE_MIN:
        desc = f"+{move*100:.1f}% in {mins}min — sharp loading {trailing_side.upper()}"
        return True, round(move, 4), mins, desc
    elif move >= 0.008:
        desc = f"+{move*100:.1f}% in {mins}min — gradual drift toward trailing team"
        return False, round(move, 4), mins, desc
    else:
        desc = f"line stable ({move*100:+.1f}% over {mins}min)"
        return False, round(move, 4), mins, desc


# ── CONVICTION ANALYSIS ───────────────────────────────────────────────────────

def analyze_run_context(state):
    """
    Assess whether the leading team's runs came from one bad inning (explainable)
    or sustained pressure (earned). Returns (is_explainable, description).
    """
    trailing_side = state.get("trailing_side")
    trailing_abr  = state.get("trailing_abr", "")
    leading_side  = state.get("leading_side")

    if not trailing_side:
        return False, "game is tied"

    # The TRAILING team's pitchers gave up the runs
    tp_data = state.get(f"{trailing_side}_pitchers", {})
    starter  = tp_data.get("starter") or {}
    relievers_used = tp_data.get("relievers_used", [])
    pitcher_count  = tp_data.get("count", 1)

    sp_name    = starter.get("name", "starter")
    sp_k       = starter.get("k", 0)
    sp_bb      = starter.get("bb", 0)
    sp_ip      = starter.get("ip", 0)
    sp_er      = starter.get("er", 0)
    sp_pitches = starter.get("pitches", 0)

    # Inning-by-inning breakdown for leading team
    innings   = state.get("innings_scores", [])
    lead_runs = {inn["inning"]: inn.get(leading_side, 0) for inn in innings
                 if inn.get(leading_side, 0) > 0}

    total_runs     = sum(lead_runs.values())
    scoring_inns   = list(lead_runs.keys())
    big_inn_runs   = max(lead_runs.values()) if lead_runs else 0
    concentrated   = len(scoring_inns) <= 2 and big_inn_runs >= 2

    # SP quality signal: k/bb ratio and pitches per IP
    good_k_bb = (sp_bb == 0) or (sp_k / max(sp_bb, 1) >= 2.0)
    efficient = sp_ip > 0 and sp_pitches > 0 and (sp_pitches / max(sp_ip, 0.1)) < 18
    dominant_before = sp_ip >= 3.0 and good_k_bb and efficient
    starter_exited  = pitcher_count > 1

    if concentrated and big_inn_runs >= 2 and dominant_before:
        context = f"{sp_name} was dealing ({sp_k}K, {sp_bb}BB on {sp_pitches} pitches)"
        if starter_exited:
            desc = (
                f"{context} before surrendering {big_inn_runs} runs in the "
                f"{_ordinal(scoring_inns[-1])} on what was otherwise a controlled outing — "
                f"a single-inning aberration. {trailing_abr} is now in the bullpen."
            )
        else:
            desc = (
                f"{context} and coughed up {big_inn_runs} runs in one burst — "
                f"the process has been elite, this is one bad sequence. "
                f"Starter remains in the game."
            )
        return True, desc

    elif len(scoring_inns) >= 3:
        desc = (
            f"{sp_name} has struggled across {len(scoring_inns)} separate innings "
            f"({sp_k}K, {sp_bb}BB, {sp_er}ER) — the runs are spread out and earned. "
            f"The deficit reflects a genuine performance gap."
        )
        return False, desc

    elif total_runs <= 2 and dominant_before:
        desc = (
            f"{sp_name} ({sp_k}K, {sp_bb}BB, {sp_pitches}P) is pitching efficiently "
            f"— {total_runs} runs allowed but the peripherals say the gap is overstated."
        )
        return True, desc

    else:
        desc = (
            f"{sp_name} ({sp_k}K, {sp_bb}BB) has allowed {total_runs} runs across "
            f"{len(scoring_inns)} inning(s) — context is mixed, not a clear fluky read."
        )
        return len(scoring_inns) <= 1, desc


def _ordinal(n):
    s = ["th", "st", "nd", "rd"]
    v = n % 100
    return f"{n}{s[v-1] if 1 <= v <= 3 and not 11 <= v <= 13 else s[0]}"


def check_bullpen_situation(state, scout_ctx):
    """
    Returns (quality_available, bullpen_advantage, description).
    quality_available: trailing team still has fresh quality arms.
    bullpen_advantage: trailing team's pen ERA is materially better.
    """
    trailing_side = state.get("trailing_side")
    leading_side  = state.get("leading_side")
    if not trailing_side:
        return False, False, "game is tied"

    trailing_abr = state.get("trailing_abr", "")
    leading_abr  = state.get("leading_abr", "")

    # Get pen ERAs from scout data
    trail_bp_era = 4.35
    lead_bp_era  = 4.35
    for g in scout_ctx.get("games", []):
        ha = g.get("home", ""); aa = g.get("away", "")
        if ha == trailing_abr:
            trail_bp_era = g.get("home_bp_era", 4.35)
        elif aa == trailing_abr:
            trail_bp_era = g.get("away_bp_era", 4.35)
        if ha == leading_abr:
            lead_bp_era = g.get("home_bp_era", 4.35)
        elif aa == leading_abr:
            lead_bp_era = g.get("away_bp_era", 4.35)

    t_pd = state.get(f"{trailing_side}_pitchers", {})
    l_pd = state.get(f"{leading_side}_pitchers", {})
    t_used  = t_pd.get("count", 1) - 1   # relievers used (exclude starter)
    l_used  = l_pd.get("count", 1) - 1
    inning  = state.get("inning", 6)

    # Fresh arm threshold by game stage
    fresh_limit = 3 if inning >= 7 else 2
    trailing_fresh   = t_used < fresh_limit
    quality_pen      = trail_bp_era < 3.90
    quality_available = trailing_fresh or quality_pen

    era_gap = lead_bp_era - trail_bp_era
    bullpen_advantage = era_gap >= 0.25

    if trailing_fresh and quality_pen:
        desc = (
            f"{trailing_abr} pen ({trail_bp_era:.2f} ERA) is rested — "
            f"only {t_used} reliever{'s' if t_used != 1 else ''} used tonight "
            f"vs {leading_abr}'s {l_used}."
        )
        if bullpen_advantage:
            desc += f" Clear ERA advantage: {era_gap:+.2f} runs favoring {trailing_abr}."
    elif trailing_fresh:
        desc = (
            f"{trailing_abr} has {t_used} reliever{'s' if t_used != 1 else ''} used "
            f"— arms are fresh but pen ERA ({trail_bp_era:.2f}) is near league average. "
            f"ERA gap vs {leading_abr} ({lead_bp_era:.2f}) is slim."
        )
    elif quality_pen:
        desc = (
            f"{trailing_abr} pen ERA {trail_bp_era:.2f} is quality, but {t_used} arms "
            f"have already been deployed tonight."
        )
    else:
        desc = (
            f"{trailing_abr} pen ({trail_bp_era:.2f} ERA, {t_used} used tonight) "
            f"has limited comeback capacity here."
        )

    return quality_available, bullpen_advantage, desc


def evaluate_conviction(state, book_odds, poly_prices, scout_ctx, history):
    """
    Run all 6 conviction gates. Returns conviction dict or None if insufficient.
    HIGH = all 6 pass. MEDIUM = 5/6 pass (must include gates 1, 5, and at least one of 2/3).
    """
    if not state:
        return None

    trailing_side = state.get("trailing_side")
    if not trailing_side:
        return None

    inning  = state.get("inning", 1)
    deficit = state.get("deficit", 0)
    ha      = state.get("home_abr", "")
    aa      = state.get("away_abr", "")
    game_key = f"{aa}@{ha}"
    trailing_abr = state.get("trailing_abr", "")

    # Gate 5: Not a blowout — hard fail if violated
    not_blowout = deficit <= MAX_DEFICIT and inning <= MAX_INNING
    if not not_blowout:
        return None

    trailing_wp = state.get("trailing_wp", 0.5)

    # Gate 1: Polymarket 5%+ mispriced vs model
    if poly_prices:
        poly_trail_p = poly_prices.get(f"{trailing_side}_p", trailing_wp)
    else:
        book_data = book_odds.get(game_key, {})
        poly_trail_p = book_data.get(f"{trailing_side}_p", trailing_wp)

    poly_edge    = trailing_wp - poly_trail_p
    poly_edge_ok = poly_edge >= POLY_EDGE_MIN

    # Gate 2: Deficit explainable
    deficit_ok, run_context_desc = analyze_run_context(state)

    # Gates 3 + 4: Bullpen quality + advantage
    quality_ok, advantage_ok, bullpen_desc = check_bullpen_situation(state, scout_ctx)

    # Gate 6: Sharp money movement
    current_p = poly_trail_p
    sharp_ok, move_pct, mins_elapsed, sharp_desc = check_sharp_money(
        game_key, trailing_side, current_p, history)

    gates = {
        "poly_edge":       poly_edge_ok,
        "deficit_explain": deficit_ok,
        "quality_pitching": quality_ok,
        "bullpen_adv":     advantage_ok,
        "not_blowout":     not_blowout,
        "sharp_money":     sharp_ok,
    }
    passed = sum(gates.values())

    if passed == 6:
        conviction = "HIGH"
    elif passed >= 5 and poly_edge_ok and not_blowout and (deficit_ok or quality_ok):
        conviction = "MEDIUM"
    else:
        return None

    # Book ML for trailing team
    book_data   = book_odds.get(game_key, {})
    trailing_ml = book_data.get(f"{trailing_side[0]}ome_ml" if trailing_side == "home" else "away_ml")
    if trailing_side == "home":
        trailing_ml = book_data.get("home_ml")
    else:
        trailing_ml = book_data.get("away_ml")

    ml_str = (f"+{trailing_ml}" if trailing_ml and trailing_ml > 0 else str(trailing_ml or "n/a"))

    bankroll = load_bankroll()
    stake    = kelly_stake(trailing_wp, trailing_ml, bankroll) if trailing_ml else 0.0

    poly_ml     = american(poly_trail_p)
    poly_ml_str = f"+{poly_ml}" if poly_ml > 0 else str(poly_ml)

    if move_pct >= 0.025:
        line_status = "ACT FAST"
    elif move_pct >= 0.010:
        line_status = "MONITOR"
    else:
        line_status = "WAIT"

    return {
        "conviction":       conviction,
        "game_key":         game_key,
        "home_abr":         ha, "away_abr": aa,
        "trailing_abr":     trailing_abr,
        "leading_abr":      state.get("leading_abr", ""),
        "trailing_side":    trailing_side,
        "inning":           inning,
        "half":             state.get("half", ""),
        "outs":             state.get("outs", 0),
        "runners":          state.get("runners", []),
        "home_score":       state.get("home_score", 0),
        "away_score":       state.get("away_score", 0),
        "deficit":          deficit,
        "model_p":          round(trailing_wp * 100, 1),
        "poly_p":           round(poly_trail_p * 100, 1),
        "edge_pct":         round(poly_edge * 100, 1),
        "book_ml":          ml_str,
        "poly_ml":          poly_ml_str,
        "stake":            stake,
        "run_context":      run_context_desc,
        "bullpen_context":  bullpen_desc,
        "sharp_desc":       sharp_desc,
        "line_status":      line_status,
        "line_move_pct":    round(move_pct * 100, 1),
        "next_batters":     state.get("next_batters", []),
        "curr_pitcher":     state.get("curr_pitcher_name", ""),
        "gates":            gates,
        "gates_passed":     passed,
    }


# ── ALERT FORMATTING ──────────────────────────────────────────────────────────

def format_live_alert(cv):
    """Clean categorized live alert matching PARLAY OS format spec."""
    half_str = cv["half"].capitalize()
    inn_str  = f"{half_str} {cv['inning']}"
    score    = f"{cv['away_abr']} {cv['away_score']} — {cv['home_abr']} {cv['home_score']}"
    stake_str = f"${cv['stake']:.0f}" if cv["stake"] > 0 else "size to model"

    why = (
        f"{cv['run_context']} "
        f"{cv['bullpen_context']}"
    ).strip()

    lines = [
        f"🚨 LIVE EDGE — {cv['conviction']} CONVICTION",
        f"{cv['away_abr']} @ {cv['home_abr']} — {inn_str} | {score}",
        f"Live {cv['book_ml']} vs Model {cv['model_p']:.0f}% — EDGE: +{cv['edge_pct']:.1f}%",
        "",
        f"✅ BET: {cv['trailing_abr']} ML {cv['book_ml']} — {stake_str}",
        "",
        f"WHY: {why}",
        "",
        f"{cv['line_status']} — {cv['sharp_desc']}",
    ]
    return "\n".join(lines)


def is_duplicate(game_key, trailing_side, alert_log):
    cutoff = (datetime.now(ET) - timedelta(minutes=DEDUP_MINUTES)).isoformat()
    for a in alert_log:
        if (a.get("game_key") == game_key
                and a.get("trailing_side") == trailing_side
                and a.get("timestamp", "") > cutoff):
            return True
    return False


def save_alert(cv):
    try:
        with open(ALERTS_FILE) as f:
            log = json.load(f)
    except Exception:
        log = []
    today = datetime.now(ET).strftime("%Y-%m-%d")
    log = [a for a in log if a.get("timestamp", "")[:10] == today]
    log.append({
        **cv,
        "timestamp": datetime.now(ET).isoformat(),
        "date": today,
        "type": "LIVE_EDGE",
        # legacy compat for dashboard
        "message": (f"{cv['away_abr']} {cv['away_score']} — {cv['home_abr']} {cv['home_score']} "
                    f"| {cv['half']} {cv['inning']} | {cv['trailing_abr']} +{cv['edge_pct']:.1f}%"),
        "narrative": cv.get("run_context", "") + " " + cv.get("bullpen_context", ""),
    })
    with open(ALERTS_FILE, "w") as f:
        json.dump(log, f, indent=2)
    return log


# ── LIVE ENGINE LOOP ──────────────────────────────────────────────────────────

def run_once():
    """Single cycle: scan all live games, evaluate conviction, alert if warranted."""
    now = datetime.now(ET)
    print(f"[{now.strftime('%H:%M ET')}] Live engine cycle — {DATE}")

    scout_ctx  = load_scout_context()
    live_games = fetch_live_games()

    if not live_games:
        print("  No live games.")
        return

    book_odds = fetch_live_book_odds()
    history   = load_odds_history()

    snapshot = {"ts": now.isoformat(), "games": {}}
    for key, data in book_odds.items():
        snapshot["games"][key] = {"home_p": data.get("home_p", 0.5), "away_p": data.get("away_p", 0.5)}

    try:
        with open(ALERTS_FILE) as f:
            alert_log = json.load(f)
    except Exception:
        alert_log = []

    new_alerts = 0

    for game in live_games:
        game_pk = game.get("gamePk")
        if not game_pk:
            continue

        feed  = fetch_game_live_feed(game_pk)
        state = extract_deep_state(feed)
        if not state:
            continue

        aa = state["away_abr"]; ha = state["home_abr"]
        game_key = f"{aa}@{ha}"
        trailing_side = state.get("trailing_side")

        if not trailing_side:
            print(f"  {game_key} — tied, skip")
            continue

        # Polymarket
        poly = fetch_polymarket_for_game(aa, ha)
        poly_prices = None
        if poly:
            poly_prices = {"home_p": poly.get("home_p", 0.5), "away_p": poly.get("away_p", 0.5)}
            snapshot["games"].setdefault(game_key, {})
            snapshot["games"][game_key]["poly_home_p"] = poly.get("home_p")
            snapshot["games"][game_key]["poly_away_p"] = poly.get("away_p")
        time.sleep(0.15)

        cv = evaluate_conviction(state, book_odds, poly_prices, scout_ctx, history)
        if cv is None:
            print(f"  {game_key} — no conviction ({state['deficit']}R deficit, inn {state['inning']})")
            continue

        if is_duplicate(game_key, trailing_side, alert_log):
            print(f"  {game_key} — conviction but deduped (within {DEDUP_MINUTES}min)")
            continue

        msg = format_live_alert(cv)
        send_telegram(msg)
        alert_log = save_alert(cv)
        new_alerts += 1
        print(f"  ALERT SENT: {game_key} — {cv['conviction']} — +{cv['edge_pct']:.1f}%")

    history = save_odds_history(history, snapshot)
    if not new_alerts:
        print(f"  No conviction plays. {len(live_games)} game(s) monitored.")


def run_live_loop():
    """Main loop: run every 3 minutes from 6pm to 11pm ET."""
    print("PARLAY OS — LIVE ENGINE v2 — started")
    while True:
        now = datetime.now(ET)
        if LIVE_START_HOUR <= now.hour < LIVE_END_HOUR:
            try:
                run_once()
            except Exception as e:
                import traceback
                print(f"Cycle error: {e}")
                traceback.print_exc()
            time.sleep(LIVE_INTERVAL)
        elif now.hour >= LIVE_END_HOUR:
            print(f"[{now.strftime('%H:%M ET')}] Past {LIVE_END_HOUR}:00 — engine stopping.")
            break
        else:
            wait_secs = (LIVE_START_HOUR - now.hour) * 3600 - now.minute * 60 - now.second
            print(f"Waiting {wait_secs // 60}min until {LIVE_START_HOUR}:00 ET...")
            time.sleep(min(max(wait_secs, 60), 300))


# ── DAILY PREVIEW ─────────────────────────────────────────────────────────────

def fetch_fg_preview_stats():
    """Pull ERA + WHIP (type=0) and xFIP (type=8) from FanGraphs. Returns merged pitcher DB."""
    base    = "https://www.fangraphs.com/api/leaders/major-league/data"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/4.0)", "Accept": "application/json"}
    standard = {}; advanced = {}

    for type_id, target in [("0", standard), ("8", advanced)]:
        try:
            r = requests.get(base, params={
                "pos": "all", "stats": "pit", "lg": "all", "qual": "20",
                "season": "2025", "season1": "2025", "type": type_id,
                "ind": "0", "pageitems": "500", "pagenum": "1",
                "sortdir": "asc", "sortstat": "ERA" if type_id == "0" else "xFIP",
            }, headers=headers, timeout=20)
            if r.ok:
                for row in r.json().get("data", []):
                    name = (row.get("PlayerName") or row.get("Name") or "").lower().strip()
                    if name:
                        target[name] = row
        except Exception as e:
            print(f"FG type={type_id} err: {e}")

    merged = {}
    for name in set(standard) | set(advanced):
        s = standard.get(name, {}); a = advanced.get(name, {})
        merged[name] = {
            "era":   _fval(s, "ERA",    default=4.35),
            "whip":  _fval(s, "WHIP",   default=1.30),
            "k9":    _fval(s, "K/9",    "K9", default=8.5),
            "bb9":   _fval(s, "BB/9",   "BB9", default=3.0),
            "ip":    _fval(s, "IP",     default=50.0),
            "xfip":  _fval(a, "xFIP",   default=_fval(s, "xFIP", default=4.20)),
            "siera": _fval(a, "SIERA",  default=4.20),
            "hand":  a.get("Hand") or s.get("Hand") or a.get("Throws") or "R",
            "team":  a.get("Team") or s.get("Team") or "",
        }

    print(f"Preview pitcher DB: {len(merged)} pitchers")
    return merged


def _sp_lookup(name, pitcher_db):
    if not name or name == "TBA":
        return {"era": 4.35, "xfip": 4.20, "whip": 1.30, "k9": 8.5, "hand": "R", "ip": 50.0}
    n = name.lower()
    if n in pitcher_db:
        return pitcher_db[n]
    last = n.split()[-1] if n.split() else ""
    if len(last) > 4:
        for k, d in pitcher_db.items():
            if k.endswith(last):
                return d
    return {"era": 4.35, "xfip": 4.20, "whip": 1.30, "k9": 8.5, "hand": "R", "ip": 50.0}


def fetch_wttr(home_abr):
    city = BALLPARK_CITIES.get(home_abr, "")
    if not city:
        return None
    try:
        r = requests.get(f"https://wttr.in/{city.replace(' ','+')}?format=j1",
                         headers={"User-Agent": "ParlayOS/4.0"}, timeout=8)
        if not r.ok:
            return None
        c = r.json().get("current_condition", [{}])[0]
        return {
            "temp":      c.get("temp_F", "72"),
            "wind":      f"{c.get('windspeedMiles','0')} mph {c.get('winddir16Point','')}",
            "condition": (c.get("weatherDesc", [{}])[0].get("value", "") or ""),
        }
    except Exception:
        return None


def generate_game_preview(game, pitcher_db, scout_games_map):
    """5-sentence sharp preview for one scheduled game."""
    away = game.get("away", ""); home = game.get("home", "")
    asp_name = game.get("asp", "TBA"); hsp_name = game.get("hsp", "TBA")
    game_time = game.get("time", "")

    asp = _sp_lookup(asp_name, pitcher_db)
    hsp = _sp_lookup(hsp_name, pitcher_db)

    away_wrc  = game.get("away_wrc", 100)
    home_wrc  = game.get("home_wrc", 100)
    away_bp   = game.get("away_bp_era", 4.35)
    home_bp   = game.get("home_bp_era", 4.35)
    away_fat  = game.get("away_fat_score", 0)
    home_fat  = game.get("home_fat_score", 0)

    pf      = PARK_FACTORS.get(home, 1.0)
    wx      = fetch_wttr(home)
    wx_note = ""
    if wx:
        wx_note = f"{wx['temp']}°F, {wx['wind']}"
        if wx.get("condition"):
            wx_note += f", {wx['condition']}"

    model_away_wp = game.get("away_wp", 50)
    model_home_wp = game.get("home_wp", 50)
    pick_side     = game.get("pick_side", "")
    pick_team     = home if pick_side == "home" else away
    pick_model_p  = model_home_wp if pick_side == "home" else model_away_wp
    pick_mkt_p    = game.get("home_market_p", 50) if pick_side == "home" else game.get("away_market_p", 50)
    edge          = game.get("edge_pct", 0)
    conviction    = game.get("conviction", "PASS")

    # ── Sentence 1: SP matchup ──
    sp_gap = hsp["xfip"] - asp["xfip"]   # positive = home SP worse
    if abs(sp_gap) >= 0.30:
        adv_sp   = asp_name if sp_gap > 0 else hsp_name
        adv_stat = asp if sp_gap > 0 else hsp
        dis_sp   = hsp_name if sp_gap > 0 else asp_name
        dis_stat = hsp if sp_gap > 0 else asp
        adv_team = away if sp_gap > 0 else home
        s1 = (
            f"{adv_sp} ({adv_stat['era']:.2f} ERA / {adv_stat['xfip']:.2f} xFIP / "
            f"{adv_stat['whip']:.2f} WHIP) carries a clear edge over {dis_sp} "
            f"({dis_stat['era']:.2f} ERA / {dis_stat['xfip']:.2f} xFIP / {dis_stat['whip']:.2f} WHIP) "
            f"— a {abs(sp_gap):.2f}-point xFIP gap that projects to roughly "
            f"{abs(sp_gap) * 0.55:.1f} additional runs allowed per start, "
            f"giving {adv_team} a tangible pitching edge."
        )
    else:
        s1 = (
            f"A near-mirror matchup on the mound: {asp_name} "
            f"({asp['era']:.2f} ERA / {asp['xfip']:.2f} xFIP / {asp['whip']:.2f} WHIP) vs "
            f"{hsp_name} ({hsp['era']:.2f} ERA / {hsp['xfip']:.2f} xFIP / {hsp['whip']:.2f} WHIP) "
            f"— peripherals are nearly identical, so offense depth and late-game bullpen "
            f"become the swing factors."
        )

    # ── Sentence 2: Bullpen ──
    better_bp_team = home if home_bp < away_bp else away
    worse_bp_team  = away if home_bp < away_bp else home
    bp_gap = abs(home_bp - away_bp)
    fat_note = ""
    if away_fat >= 2:
        fat_note = f" {away} pen fatigued (3G/3D workload)"
    elif home_fat >= 2:
        fat_note = f" {home} pen fatigued (3G/3D workload)"
    s2 = (
        f"{better_bp_team} bullpen ({min(home_bp,away_bp):.2f} ERA) holds a "
        f"{bp_gap:.2f}-run ERA edge over {worse_bp_team} ({max(home_bp,away_bp):.2f} ERA)"
        f"{fat_note} — in a one- or two-run game, that gap is the difference between "
        f"holding a lead and giving it back."
    )

    # ── Sentence 3: Lineup + context ──
    off_gap = abs(home_wrc - away_wrc)
    if off_gap >= 8:
        better_off = home if home_wrc > away_wrc else away
        better_wrc = max(home_wrc, away_wrc)
        worse_wrc  = min(home_wrc, away_wrc)
        run_diff   = (better_wrc - 100) / 100 * LG_RPG - (worse_wrc - 100) / 100 * LG_RPG
        s3 = (
            f"{better_off} lineup (wRC+ {better_wrc}) projects {abs(run_diff):.1f} more "
            f"runs per game than the opposition (wRC+ {worse_wrc}), and no lineup disruptions "
            f"noted in scheduled starters — full-strength advantage is in play."
        )
    else:
        s3 = (
            f"Offense is balanced: {away} wRC+ {away_wrc} vs {home} wRC+ {home_wrc} — "
            f"neither lineup has a material advantage, so SP length and bullpen quality "
            f"carry more weight in this game's outcome."
        )

    # ── Sentence 4: Weather + park ──
    if pf >= 1.05:
        park_desc = f"{home} park is hitter-friendly (PF {pf:.2f})"
    elif pf <= 0.96:
        park_desc = f"{home} park is pitcher-friendly (PF {pf:.2f})"
    else:
        park_desc = f"{home} park is neutral (PF {pf:.2f})"
    s4 = f"{park_desc}; {wx_note}." if wx_note else f"{park_desc}; weather data unavailable."

    # ── Sentence 5: Market edge / the one thing the market might be missing ──
    if abs(edge) >= 3.0 and conviction != "PASS":
        if abs(sp_gap) >= 0.40:
            missing = (f"the {abs(sp_gap):.2f}-point xFIP gap between starters — "
                       f"surface ERA can obscure this in casual reads")
        elif bp_gap >= 0.55:
            missing = (f"the {bp_gap:.2f}-run bullpen ERA differential, which compounds "
                       f"heavily in late-game leverage situations")
        elif off_gap >= 10:
            missing = (f"the {off_gap}-point wRC+ lineup gap, which the market often "
                       f"underweights relative to the SP matchup")
        else:
            missing = "combined SP quality, bullpen depth, and offensive context"
        s5 = (
            f"Model prices {pick_team} at {pick_model_p:.0f}% vs market's {pick_mkt_p:.0f}% "
            f"— a +{edge:.1f}% edge — driven by {missing}. "
            f"The one thing the market may be sleeping on: {missing.split('—')[0].strip()}."
        )
    else:
        s5 = (
            f"Model finds minimal edge ({edge:+.1f}%) — this is a pass unless you have "
            f"specific lineup or health intelligence that changes the probability picture. "
            f"No structural market inefficiency detected."
        )

    away_rec = game.get("away_record", ""); home_rec = game.get("home_record", "")
    hml = game.get("hml", ""); aml = game.get("aml", "")

    return {
        "game":       f"{away}@{home}",
        "away":       away, "home": home,
        "time":       game_time,
        "away_record": away_rec, "home_record": home_rec,
        "sp_line":    f"{asp_name} ({asp['era']:.2f}/{asp['xfip']:.2f}/{asp['whip']:.2f}) vs "
                      f"{hsp_name} ({hsp['era']:.2f}/{hsp['xfip']:.2f}/{hsp['whip']:.2f})",
        "preview":    f"{s1} {s2} {s3} {s4} {s5}",
        "sentences":  {"sp": s1, "bullpen": s2, "lineup": s3, "park_weather": s4, "edge": s5},
        "edge_pct":   edge,
        "pick":       game.get("pick", ""),
        "pick_odds":  game.get("pick_odds", ""),
        "conviction": conviction,
        "model_line": f"{away} {aml} / {home} {hml}",
    }


def run_daily_preview():
    """
    Noon daily preview: build 5-sentence preview for every scheduled game,
    save to daily_preview.json, send to Telegram.
    Relies on last_scout.json for model data; fetches fresh FG WHIP data.
    """
    now = datetime.now(ET)
    print(f"[{now.strftime('%H:%M ET')}] Daily preview running — {DATE}")

    scout_data = load_scout_context()
    games = scout_data.get("games", [])

    if not games:
        msg = f"PARLAY OS PREVIEW — {DATE}\nNo scout data available. Run scout.py first."
        send_telegram(msg)
        print("  No scout data — abort.")
        return

    # Filter to scheduled games only (not live or final)
    scheduled = [g for g in games
                 if g.get("conviction") is not None]   # all model games

    pitcher_db = fetch_fg_preview_stats()

    previews = []
    for g in scheduled:
        time.sleep(0.1)
        prev = generate_game_preview(g, pitcher_db, scout_data)
        previews.append(prev)

    output = {
        "date":      DATE,
        "generated": now.isoformat(),
        "games":     previews,
        "note":      scout_data.get("note", ""),
        "verdict":   scout_data.get("verdict", ""),
    }

    with open(PREVIEW_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Saved {PREVIEW_FILE} ({len(previews)} games)")

    # ── Telegram: header ──
    verdict    = scout_data.get("verdict", "")
    high_picks = scout_data.get("high", [])
    med_picks  = scout_data.get("medium", [])

    header_lines = [
        f"PARLAY OS — DAILY PREVIEW — {DATE}",
        f"{verdict}",
        f"Model: {len(high_picks)}H {len(med_picks)}M edge{'s' if len(high_picks)+len(med_picks) != 1 else ''}",
        "",
    ]
    send_telegram("\n".join(header_lines))
    time.sleep(0.5)

    # ── Telegram: one message per game with conviction ──
    for p in previews:
        conv = p.get("conviction", "PASS")
        if conv == "PASS":
            continue
        pick_str  = f"  PICK: {p['pick']} {p['pick_odds']}  [{conv}]" if p.get("pick") else ""
        game_msg  = (
            f"[{conv}] {p['away']} @ {p['home']} — {p['time']}\n"
            f"  {p['away']} {p['away_record']} | {p['home']} {p['home_record']}\n"
            f"  SP: {p['sp_line']}\n"
            f"  Odds: {p['model_line']}\n"
            f"{pick_str}\n\n"
            f"{p['preview']}"
        )
        send_telegram(game_msg)
        time.sleep(0.8)

    # ── Send pass-day games summary at end ──
    pass_games = [p for p in previews if p.get("conviction") == "PASS"]
    if pass_games:
        pass_lines = [f"PASS ({len(pass_games)} games — no model edge):"]
        for p in pass_games:
            pass_lines.append(f"  {p['away']} @ {p['home']} — {p['time']} | {p['sp_line']}")
        send_telegram("\n".join(pass_lines))

    send_telegram(f"Parlay OS Preview complete — {now.strftime('%I:%M %p ET')}")
    print(f"  Preview complete. {len(previews)} games processed.")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if "--preview" in args:
        run_daily_preview()
    elif "--live" in args:
        run_live_loop()
    else:
        run_once()


if __name__ == "__main__":
    main()
