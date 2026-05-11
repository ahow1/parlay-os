"""
PARLAY OS — SCOUT v4
Enhanced probability model: SP xFIP/SIERA + bullpen fatigue + offense wRC+
+ platoon splits + umpire zone + weather + run expectancy.
Outputs HIGH/MEDIUM/PASS conviction with fractional Kelly sizing.
"""

import os, json, re, time, requests
from datetime import datetime, timedelta
from math import exp
import pytz

from math_engine import (implied_prob, no_vig_prob, expected_value,
                         kelly_criterion, BankrollManager)

ODDS_API_KEY       = os.environ.get("ODDS_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]

ET    = pytz.timezone("America/New_York")
NOW   = datetime.now(ET)
DATE  = NOW.strftime("%Y-%m-%d")

LG_RPG   = 4.35
LG_ERA   = 4.35
PYTH_EXP = 1.83
HOME_ADV = 1.035

MIN_EDGE_PCT = 4.0   # flag only at 4%+

PARK_FACTORS = {
    "COL": 1.13, "BOS": 1.07, "CIN": 1.05, "PHI": 1.03, "CHC": 1.03,
    "NYY": 1.01, "BAL": 1.01, "MIN": 1.01, "KC": 1.00, "WAS": 1.00,
    "TEX": 1.00, "TOR": 1.00, "NYM": 0.98, "HOU": 0.97, "ATL": 0.97,
    "DET": 0.97, "MIA": 0.97, "STL": 0.99, "MIL": 0.99, "LAD": 0.99,
    "AZ": 1.02, "CLE": 0.98, "LAA": 0.98, "PIT": 0.98, "CWS": 0.99,
    "OAK": 0.95, "ATH": 0.95, "SF": 0.93, "SD": 0.95, "SEA": 0.96, "TB": 0.95,
}

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

# Ballpark cities for wttr.in weather queries
BALLPARK_CITIES = {
    "AZ":  "Phoenix",        "ATL": "Atlanta",       "BAL": "Baltimore",
    "BOS": "Boston",         "CHC": "Chicago",        "CWS": "Chicago",
    "CIN": "Cincinnati",     "CLE": "Cleveland",      "COL": "Denver",
    "DET": "Detroit",        "HOU": "Houston",        "KC":  "Kansas City",
    "LAA": "Anaheim",        "LAD": "Los Angeles",    "MIA": "Miami",
    "MIL": "Milwaukee",      "MIN": "Minneapolis",    "NYM": "New York",
    "NYY": "New York",       "ATH": "Sacramento",     "OAK": "Oakland",
    "PHI": "Philadelphia",   "PIT": "Pittsburgh",     "SD":  "San Diego",
    "SF":  "San Francisco",  "SEA": "Seattle",        "STL": "St. Louis",
    "TB":  "St. Petersburg", "TEX": "Arlington",      "TOR": "Toronto",
    "WAS": "Washington DC",
}

# MLB Stats API team IDs
MLB_TEAM_IDS = {
    "AZ": 109, "ATL": 144, "BAL": 110, "BOS": 111, "CHC": 112,
    "CWS": 145, "CIN": 113, "CLE": 114, "COL": 115, "DET": 116,
    "HOU": 117, "KC": 118, "LAA": 108, "LAD": 119, "MIA": 146,
    "MIL": 158, "MIN": 142, "NYM": 121, "NYY": 147, "ATH": 133,
    "OAK": 133, "PHI": 143, "PIT": 134, "SD": 135, "SF": 137,
    "SEA": 136, "STL": 138, "TB": 139, "TEX": 140, "TOR": 141, "WAS": 120,
}

# Umpire zone tendencies (k_factor, run_factor, note)
# k_factor > 1 = larger zone = more strikeouts
# run_factor < 1 = larger zone = fewer runs
UMPIRE_TENDENCIES = {
    "CB Bucknor":        (0.90, 0.93, "small/erratic zone — YRFI lean"),
    "Angel Hernandez":   (0.92, 0.95, "below-avg zone — hitter-friendly"),
    "Laz Diaz":          (0.93, 0.94, "tight zone — YRFI tendency"),
    "Doug Eddings":      (0.96, 0.97, "slightly tight zone"),
    "Ron Kulpa":         (0.98, 0.97, "slightly tight zone"),
    "Chris Guccione":    (0.98, 0.98, "near neutral, slightly tight"),
    "Vic Carapazza":     (1.06, 1.04, "large zone — K-friendly, under lean"),
    "Lance Barrett":     (1.04, 1.02, "above-avg zone — K-friendly"),
    "Mark Carlson":      (1.04, 1.02, "above-avg zone"),
    "Dan Bellino":       (1.05, 1.03, "above-avg zone — K-friendly"),
    "Fieldin Culbreth":  (1.03, 1.01, "above-avg zone"),
    "Alfonso Marquez":   (1.02, 1.01, "slight above avg"),
    "Jim Reynolds":      (1.02, 1.01, "slight above avg"),
    "Paul Emmel":        (1.02, 1.01, "slight above avg"),
    "Tripp Gibson":      (1.02, 1.01, "slight above avg"),
    "Hunter Wendelstedt":(1.01, 1.01, "near neutral"),
    "Bruce Dreckman":    (1.03, 1.02, "solid zone"),
    "Bill Welke":        (1.02, 1.01, "standard zone"),
    "Tom Hallion":       (1.01, 1.00, "neutral"),
    "Brian Gorman":      (1.00, 1.00, "neutral"),
    "Ted Barrett":       (1.00, 1.00, "neutral"),
    "Dana DeMuth":       (1.01, 1.00, "neutral"),
    "John Tumpane":      (0.99, 0.99, "near neutral"),
    "Marvin Hudson":     (0.97, 0.98, "slightly tight"),
    "Joe West":          (0.95, 0.97, "tight zone"),
    "Bill Miller":       (0.98, 0.98, "slightly tight"),
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


# ─── DATA FETCHING ────────────────────────────────────────────────────────────

def fetch_mlb_schedule():
    url = (f"https://statsapi.mlb.com/api/v1/schedule"
           f"?sportId=1&date={DATE}"
           f"&hydrate=probablePitcher,team,officials,weather"
           f"&gameType=R")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"MLB schedule err: {e}")
        return {}


def fetch_odds():
    if not ODDS_API_KEY:
        print("No ODDS_API_KEY — skipping live odds")
        return []
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/",
            params={"apiKey": ODDS_API_KEY, "regions": "us", "markets": "h2h",
                    "oddsFormat": "american", "dateFormat": "iso"},
            timeout=15)
        r.raise_for_status()
        print(f"Odds API: {len(r.json())} games, {r.headers.get('x-requests-remaining','?')} remaining")
        return r.json()
    except Exception as e:
        print(f"Odds API err: {e}")
        return []


def fetch_f5_odds():
    """Fetch first-5-innings moneyline odds (h2h_h1 market) from the-odds-api."""
    if not ODDS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/",
            params={"apiKey": ODDS_API_KEY, "regions": "us", "markets": "h2h_h1",
                    "oddsFormat": "american", "dateFormat": "iso"},
            timeout=15)
        r.raise_for_status()
        data = r.json()
        print(f"F5 Odds: {len(data)} games")
        return data
    except Exception as e:
        print(f"F5 Odds API err: {e}")
        return []


def fetch_wttr_weather(home_abr):
    """Pull current weather for ballpark city from wttr.in (free, no key)."""
    city = BALLPARK_CITIES.get(home_abr, "")
    if not city:
        return None
    try:
        r = requests.get(
            f"https://wttr.in/{city.replace(' ', '+')}?format=j1",
            headers={"User-Agent": "ParlayOS/4.0"},
            timeout=10)
        r.raise_for_status()
        data = r.json()
        cond = data.get("current_condition", [{}])[0]
        wind_mph   = int(cond.get("windspeedMiles", 0))
        wind_dir   = cond.get("winddir16Point", "")
        temp_f     = int(cond.get("temp_F", 72))
        desc       = (cond.get("weatherDesc", [{}])[0].get("value", "") or "")
        return {
            "temp":      str(temp_f),
            "wind":      f"{wind_mph} mph {wind_dir}",
            "condition": desc,
            "source":    "wttr.in",
        }
    except Exception as e:
        print(f"wttr.in err ({home_abr}/{city}): {e}")
        return None


def fetch_series_schedule(teams):
    """Fetch next 4 days' schedule to identify series SP matchups."""
    end_date = (NOW + timedelta(days=4)).strftime("%Y-%m-%d")
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "startDate": DATE, "endDate": end_date,
                    "gameType": "R", "hydrate": "probablePitcher,team"},
            timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Series schedule err: {e}")
        return {}


def _fg_get(params):
    base = "https://www.fangraphs.com/api/leaders/major-league/data"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ParlayOS/4.0)", "Accept": "application/json"}
    try:
        r = requests.get(base, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"FanGraphs err ({params.get('stats','?')}): {e}")
        return {}


def fetch_fg_pitchers():
    return _fg_get({"pos": "all", "stats": "pit", "lg": "all", "qual": "20",
                    "season": "2025", "season1": "2025", "type": "8",
                    "ind": "0", "pageitems": "500", "pagenum": "1",
                    "sortdir": "asc", "sortstat": "xFIP"})


def fetch_fg_pitcher_splits():
    """Pitcher splits: vs LHB and RHB."""
    return _fg_get({"pos": "all", "stats": "pit", "lg": "all", "qual": "10",
                    "season": "2025", "season1": "2025", "type": "0",
                    "ind": "0", "pageitems": "500", "pagenum": "1",
                    "split": "batter_hand", "sortdir": "asc", "sortstat": "ERA"})


def fetch_fg_team_batting():
    return _fg_get({"pos": "all", "stats": "bat", "lg": "all", "qual": "0",
                    "season": "2025", "season1": "2025", "type": "8",
                    "ind": "1", "pageitems": "50", "pagenum": "1",
                    "sortdir": "desc", "sortstat": "wRC+"})


def fetch_fg_team_pitching():
    return _fg_get({"pos": "all", "stats": "pit", "lg": "all", "qual": "0",
                    "season": "2025", "season1": "2025", "type": "1",
                    "ind": "1", "pageitems": "50", "pagenum": "1",
                    "sortdir": "asc", "sortstat": "ERA"})


def fetch_bullpen_fatigue(today_team_abrs):
    """
    For each team, check last 3 days for schedule density and extra innings.
    Returns {team_abr: {fatigue_score, games, innings, note}}.
    """
    three_days_ago = (NOW - timedelta(days=3)).strftime("%Y-%m-%d")
    yesterday      = (NOW - timedelta(days=1)).strftime("%Y-%m-%d")
    results = {}

    for abr in today_team_abrs:
        team_id = MLB_TEAM_IDS.get(abr)
        if not team_id:
            results[abr] = {"fatigue_score": 0, "games": 0, "innings": 0, "note": ""}
            continue
        try:
            r = requests.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": 1, "startDate": three_days_ago,
                        "endDate": yesterday, "teamId": team_id,
                        "gameType": "R", "hydrate": "linescore"},
                timeout=10)
            r.raise_for_status()
            data = r.json()

            games, total_inn, extra_inn = [], 0, 0
            for de in data.get("dates", []):
                for g in de.get("games", []):
                    if g.get("status", {}).get("abstractGameState") == "Final":
                        inn = g.get("linescore", {}).get("currentInning", 9)
                        games.append(inn)
                        total_inn += inn
                        extra_inn += max(inn - 9, 0)

            g_count = len(games)
            score = 0
            note  = ""
            if g_count >= 3:
                score += 1; note = "3G/3D"
            if extra_inn >= 2:
                score += 2; note = f"pen stressed ({total_inn}inn/{g_count}G)"
            elif extra_inn == 1:
                score += 1
            if g_count >= 3 and total_inn > 28:
                score = max(score, 2); note = f"heavy load ({total_inn}inn/{g_count}G)"

            results[abr] = {"fatigue_score": score, "games": g_count,
                            "innings": total_inn, "note": note}
        except Exception:
            results[abr] = {"fatigue_score": 0, "games": 0, "innings": 0, "note": ""}
        time.sleep(0.08)

    print(f"  Bullpen fatigue: {len(results)} teams checked")
    return results


# ─── DATA PARSING ─────────────────────────────────────────────────────────────

def _fval(row, *keys, default=None):
    for k in keys:
        v = row.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return default


def parse_pitchers(raw):
    db = {}
    for row in raw.get("data", []):
        name = row.get("PlayerName") or row.get("Name") or ""
        if not name:
            continue
        xfip  = _fval(row, "xFIP", "xfip", "xFIP-", default=4.20)
        siera = _fval(row, "SIERA", "siera", default=xfip)
        k9    = _fval(row, "K/9", "K9", "k9", default=8.5)
        bb9   = _fval(row, "BB/9", "BB9", "bb9", default=3.0)
        ip    = _fval(row, "IP", "ip", default=50.0)
        hand  = row.get("Hand") or row.get("Throws") or "R"
        team  = row.get("Team") or ""
        db[name.lower()] = {
            "name": name, "xfip": xfip, "siera": siera,
            "k9": k9, "bb9": bb9, "ip": ip, "hand": hand, "team": team,
        }
    print(f"  Pitchers loaded: {len(db)}")
    return db


def parse_team_stat(raw, *keys):
    out = {}
    for row in raw.get("data", []):
        team = row.get("Team") or row.get("team") or ""
        val  = _fval(row, *keys)
        if team and val is not None:
            out[team] = val
    return out


def get_sp(name, pitcher_db):
    if not name or name == "TBA":
        return {"xfip": 4.20, "siera": 4.20, "k9": 8.5, "bb9": 3.0, "ip": 50.0, "hand": "R"}
    n = name.lower()
    if n in pitcher_db:
        return pitcher_db[n]
    last = n.split()[-1] if n.split() else ""
    if len(last) > 4:
        for key, data in pitcher_db.items():
            if key.endswith(last):
                return data
    return {"xfip": 4.20, "siera": 4.20, "k9": 8.5, "bb9": 3.0, "ip": 50.0, "hand": "R"}


def extract_umpires(schedule_raw):
    """Return {away@home: hp_umpire_name} for today's games."""
    umpires = {}
    for de in schedule_raw.get("dates", []):
        for game in de.get("games", []):
            ha = MLB_TEAM_MAP.get(game["teams"]["home"]["team"]["name"], "")
            aa = MLB_TEAM_MAP.get(game["teams"]["away"]["team"]["name"], "")
            for off in game.get("officials", []):
                if off.get("officialType") == "Home Plate":
                    name = off.get("official", {}).get("fullName", "")
                    if name:
                        umpires[f"{aa}@{ha}"] = name
    return umpires


def extract_weather(schedule_raw):
    """Return {away@home: weather_dict} from MLB Stats API schedule hydration (fallback)."""
    weather_map = {}
    for de in schedule_raw.get("dates", []):
        for game in de.get("games", []):
            ha = MLB_TEAM_MAP.get(game["teams"]["home"]["team"]["name"], "")
            aa = MLB_TEAM_MAP.get(game["teams"]["away"]["team"]["name"], "")
            wx = game.get("weather")
            if wx:
                weather_map[f"{aa}@{ha}"] = wx
    return weather_map


def fetch_all_weather(schedule_raw, today_home_teams):
    """
    Build weather map for all today's games.
    Primary: wttr.in (free, no key, always available).
    Fallback: MLB Stats API hydrated weather.
    Returns {away@home: weather_dict}.
    """
    mlb_weather  = extract_weather(schedule_raw)
    weather_map  = {}
    fetched_cities = {}  # cache to avoid double-fetching CHC, NYY/NYM, etc.

    for game_key, home_abr in today_home_teams.items():
        # Try wttr.in first (city-level cache so CHC/CWS share one call)
        city = BALLPARK_CITIES.get(home_abr, "")
        if city:
            if city not in fetched_cities:
                wx = fetch_wttr_weather(home_abr)
                fetched_cities[city] = wx
            else:
                wx = fetched_cities[city]
            if wx:
                weather_map[game_key] = wx
                continue
        # Fallback to MLB Stats API weather
        if game_key in mlb_weather:
            weather_map[game_key] = mlb_weather[game_key]
        time.sleep(0.05)

    return weather_map


def parse_weather(wx, home_abr):
    """
    Convert weather dict → (run_factor, note_str).
    Handles both MLB Stats API format and wttr.in format.
    """
    if not wx:
        return 1.0, ""

    condition = wx.get("condition", "")
    try:
        temp_f = int(wx.get("temp", 72))
    except (TypeError, ValueError):
        temp_f = 72

    wind_str = wx.get("wind", "")
    # wttr.in gives "12 mph NNW" — MLB API gives "7 mph, In from RF"

    factor, notes = 1.0, []

    if temp_f < 50:
        factor *= 0.94; notes.append(f"{temp_f}°F cold")
    elif temp_f < 60:
        factor *= 0.97; notes.append(f"{temp_f}°F cool")
    elif temp_f > 90:
        factor *= 1.02; notes.append(f"{temp_f}°F hot")

    if any(w in condition.lower() for w in ("rain", "drizzle", "shower", "thunder")):
        factor *= 0.95; notes.append("rain")

    wind_match = re.search(r'(\d+)\s*mph', wind_str, re.IGNORECASE)
    if wind_match:
        spd = int(wind_match.group(1))
        wl  = wind_str.lower()
        if spd >= 10:
            if "out to" in wl:
                # MLB API directional: explicit out to CF/RF/LF
                boost = min((spd - 8) * 0.006, 0.08)
                factor *= (1 + boost)
                notes.append(f"{spd}mph out")
            elif "in from" in wl:
                supp = min((spd - 8) * 0.006, 0.07)
                factor *= (1 - supp)
                notes.append(f"{spd}mph in")
            else:
                # wttr.in compass direction — apply neutral wind factor
                # Strong wind adds run variance without clear direction bonus
                if spd >= 20:
                    factor *= 1.02
                    notes.append(f"{spd}mph wind ({wl.split()[-1] if wl.split() else ''})")
                elif spd >= 10:
                    notes.append(f"{spd}mph {wl.split()[-1] if wl.split() else ''}")

    return round(factor, 3), ", ".join(notes) if notes else ""


def get_umpire_factors(umpire_name):
    """Return (k_factor, run_factor, note) or neutral defaults."""
    if not umpire_name:
        return 1.0, 1.0, ""
    t = UMPIRE_TENDENCIES.get(umpire_name)
    if t:
        return t[0], t[1], t[2]
    return 1.0, 1.0, ""


# ─── PLATOON SPLIT ADJUSTMENT ─────────────────────────────────────────────────

# wRC+ boost/penalty based on platoon matchup
# Source: 2020-2024 MLB platoon splits averages
PLATOON_WRCPLUS_DELTA = {
    ("R", "R"): -4,   # RHB vs RHP (disadvantage)
    ("R", "L"): +6,   # RHB vs LHP (advantage)
    ("L", "R"): +5,   # LHB vs RHP (advantage)
    ("L", "L"): -5,   # LHB vs LHS (disadvantage)
    ("S", "R"): +2,   # Switch-hitter vs RHP (slight advantage)
    ("S", "L"): +2,   # Switch-hitter vs LHP (slight advantage)
}

# Approximate team batting handedness distribution (% of PAs from LHB)
# Higher = more left-handed lineup
TEAM_LHB_PCT = {
    "AZ": 0.42, "ATL": 0.44, "BAL": 0.40, "BOS": 0.48, "CHC": 0.46,
    "CWS": 0.38, "CIN": 0.43, "CLE": 0.40, "COL": 0.45, "DET": 0.42,
    "HOU": 0.46, "KC": 0.40, "LAA": 0.45, "LAD": 0.48, "MIA": 0.39,
    "MIL": 0.44, "MIN": 0.44, "NYM": 0.43, "NYY": 0.46, "ATH": 0.41,
    "PHI": 0.44, "PIT": 0.42, "SD": 0.42, "SF": 0.46, "SEA": 0.47,
    "STL": 0.43, "TB": 0.45, "TEX": 0.41, "TOR": 0.46, "WAS": 0.41,
}


def platoon_wrc_adjustment(batting_team_abr, opp_sp_hand):
    """
    Returns wRC+ adjustment points based on team handedness mix vs SP hand.
    A team with 48% LHB facing a LHP gets a disadvantage.
    """
    lhb_pct = TEAM_LHB_PCT.get(batting_team_abr, 0.43)
    rhb_pct = 1.0 - lhb_pct

    if opp_sp_hand == "R":
        # RHB disadvantaged vs RHP, LHB advantaged
        avg_delta = (rhb_pct * PLATOON_WRCPLUS_DELTA[("R", "R")] +
                     lhb_pct * PLATOON_WRCPLUS_DELTA[("L", "R")])
    elif opp_sp_hand == "L":
        # RHB advantaged vs LHP, LHB disadvantaged
        avg_delta = (rhb_pct * PLATOON_WRCPLUS_DELTA[("R", "L")] +
                     lhb_pct * PLATOON_WRCPLUS_DELTA[("L", "L")])
    else:
        avg_delta = 0.0

    return round(avg_delta, 1)


# ─── PROBABILITY MODEL ────────────────────────────────────────────────────────

def sp_ip_estimate(xfip):
    if xfip < 3.00: return 6.5
    if xfip < 3.50: return 6.0
    if xfip < 4.00: return 5.75
    if xfip < 4.50: return 5.5
    return 5.0


def expected_runs(wrc_plus, opp_sp_xfip, opp_bp_era, park_factor=1.0,
                  sp_ip=5.5, weather_factor=1.0, ump_run_factor=1.0,
                  bp_fatigue_score=0):
    """
    Expected runs in one game. Adds weather, umpire, and bullpen fatigue factors.
    Bullpen fatigue: fatigued pen (score 2+) has ERA boosted by 5-10%.
    """
    fatigue_bp_mult = 1.0 + (min(bp_fatigue_score, 3) * 0.04)
    effective_bp_era = opp_bp_era * fatigue_bp_mult

    bp_ip = 9 - sp_ip
    combined_era = (opp_sp_xfip * sp_ip + effective_bp_era * bp_ip) / 9
    runs = ((wrc_plus / 100) * LG_RPG *
            (combined_era / LG_ERA) * park_factor * weather_factor * ump_run_factor)
    return round(max(runs, 0.5), 3)


def win_prob(home_rs, away_rs):
    h = home_rs ** PYTH_EXP
    a = away_rs ** PYTH_EXP
    hwp = h / (h + a)
    return round(hwp, 4), round(1 - hwp, 4)


def prob_to_american(p):
    p = max(min(p, 0.99), 0.01)
    if p >= 0.5:
        return round(-100 * p / (1 - p))
    return round(100 * (1 - p) / p)


def devig(ml1, ml2):
    def ip(ml):
        n = float(str(ml).replace("+", ""))
        return 100 / (n + 100) if n > 0 else abs(n) / (abs(n) + 100)
    p1, p2 = ip(ml1), ip(ml2)
    t = p1 + p2
    return p1 / t, p2 / t


def score_edge(model_p, market_p):
    """
    Returns (edge_pct, conviction) where conviction is HIGH / MEDIUM / PASS.
    HIGH   = edge >= 7%
    MEDIUM = edge 4–7%
    PASS   = edge < 4%
    """
    edge = (model_p - market_p) * 100
    if edge >= 7.0:
        return round(edge, 2), "HIGH"
    if edge >= MIN_EDGE_PCT:
        return round(edge, 2), "MEDIUM"
    return round(edge, 2), "PASS"


# ─── CONVICTION NARRATIVE ─────────────────────────────────────────────────────

def conviction_narrative(g):
    """
    Generate 3-4 sentence plain-English narrative explaining the edge.
    """
    away, home = g["away"], g["home"]
    asp, hsp   = g.get("asp", "TBA"), g.get("hsp", "TBA")
    asp_xfip   = g.get("asp_xfip", 4.20)
    hsp_xfip   = g.get("hsp_xfip", 4.20)
    away_wrc   = g.get("away_wrc", 100)
    home_wrc   = g.get("home_wrc", 100)
    pick_side  = g.get("pick_side", "")
    pick_team  = away if pick_side == "away" else home
    model_p    = g.get("away_wp", 50) if pick_side == "away" else g.get("home_wp", 50)
    mkt_p      = g.get("away_market_p", 50) if pick_side == "away" else g.get("home_market_p", 50)
    edge       = g.get("edge_pct", 0)

    parts = []

    # 1. Pitching matchup
    sp_gap = asp_xfip - hsp_xfip
    if abs(sp_gap) > 0.25:
        adv_team  = home if asp_xfip > hsp_xfip else away
        adv_sp    = hsp  if asp_xfip > hsp_xfip else asp
        adv_xfip  = min(asp_xfip, hsp_xfip)
        disadv_xfip = max(asp_xfip, hsp_xfip)
        parts.append(
            f"{adv_team} benefits from a SP edge — {adv_sp} ({adv_xfip:.2f} xFIP) vs "
            f"{'their opponent'} ({disadv_xfip:.2f} xFIP), translating to fewer expected runs allowed."
        )
    else:
        parts.append(
            f"Starting pitching is roughly even ({asp} {asp_xfip:.2f} xFIP vs {hsp} {hsp_xfip:.2f} xFIP), "
            f"so offense and bullpen depth will determine the outcome."
        )

    # 2. Offense
    off_gap = home_wrc - away_wrc
    if abs(off_gap) >= 10:
        better_team = home if off_gap > 0 else away
        better_wrc  = max(home_wrc, away_wrc)
        worse_wrc   = min(home_wrc, away_wrc)
        parts.append(
            f"{better_team}'s lineup (wRC+ {better_wrc}) carries a material advantage "
            f"over the opponent (wRC+ {worse_wrc}), boosting projected run differential."
        )

    # 3. Market edge
    if abs(edge) >= MIN_EDGE_PCT and pick_side:
        parts.append(
            f"Our model assigns {pick_team} a {model_p:.0f}% win probability vs "
            f"the market's {mkt_p:.0f}% implied — a {edge:+.1f}% edge that clears "
            f"our 4% minimum threshold."
        )

    # 4. Contextual factors
    ctx = []
    fat = g.get("bp_fatigue_note", "")
    ump = g.get("umpire_note", "")
    wx  = g.get("weather_note", "")
    pf  = g.get("park_factor", 1.0)
    plat = g.get("platoon_note", "")

    if fat:  ctx.append(fat)
    if plat: ctx.append(plat)
    if ump:  ctx.append(ump)
    if wx:   ctx.append(wx)
    if pf >= 1.07:  ctx.append(f"hitter-friendly park (PF {pf:.2f})")
    elif pf <= 0.94: ctx.append(f"pitcher-friendly park (PF {pf:.2f})")

    if ctx:
        parts.append("Context: " + " | ".join(ctx[:3]) + ".")

    return " ".join(parts[:4])


# ─── POISSON K-PROP MODEL ─────────────────────────────────────────────────────

def poisson_over_under(lam, line):
    from math import factorial
    max_k = int(line)
    p_under = sum(exp(-lam) * (lam**k) / factorial(k) for k in range(max_k + 1))
    p_over  = 1.0 - p_under
    return round(p_over, 4), round(p_under, 4)


def k_prop_estimate(sp_stats, ump_k_factor=1.0, pitch_limit=None):
    """
    Expected strikeouts using K/9, IP estimate, and umpire zone factor.
    pitch_limit: if a SP has a known pitch count limit, adjust IP down.
    """
    sp_ip = sp_ip_estimate(sp_stats["xfip"])
    if pitch_limit and pitch_limit < 85:
        sp_ip = min(sp_ip, pitch_limit / 15.0)

    raw_k9  = sp_stats.get("k9", 8.5)
    adj_k9  = raw_k9 * ump_k_factor
    exp_k   = adj_k9 * sp_ip / 9.0

    # Model strikeout line as nearest 0.5
    k_line  = round(exp_k * 2) / 2
    p_over, p_under = poisson_over_under(exp_k, k_line)

    return {
        "expected_k":      round(exp_k, 2),
        "k_line":          k_line,
        "p_over":          p_over,
        "p_under":         p_under,
        "model_over_ml":   prob_to_american(p_over),
        "model_under_ml":  prob_to_american(p_under),
    }


# ─── ODDS LOOKUP ──────────────────────────────────────────────────────────────

def build_odds_lookup(odds_raw):
    lookup = {}
    for game in odds_raw:
        home     = game.get("home_team", "")
        away     = game.get("away_team", "")
        home_abr = MLB_TEAM_MAP.get(home, home[:3].upper())
        away_abr = MLB_TEAM_MAP.get(away, away[:3].upper())
        best_h, best_a = None, None

        for bk in game.get("bookmakers", []):
            for mkt in bk.get("markets", []):
                if mkt.get("key") != "h2h":
                    continue
                for outcome in mkt.get("outcomes", []):
                    price = outcome.get("price")
                    if price is None:
                        continue
                    if outcome.get("name") == home:
                        if best_h is None or (price > 0 and (best_h is None or price > best_h)) \
                           or (price < 0 and best_h is not None and price > best_h):
                            best_h = price
                    elif outcome.get("name") == away:
                        if best_a is None or (price > 0 and (best_a is None or price > best_a)) \
                           or (price < 0 and best_a is not None and price > best_a):
                            best_a = price

        key = f"{away_abr}@{home_abr}"
        lookup[key] = {
            "home_ml": best_h, "away_ml": best_a,
            "event_id": game.get("id", ""),
            "home_team": home, "away_team": away,
        }
    return lookup


# ─── F5 MODEL ─────────────────────────────────────────────────────────────────

def build_f5_odds_lookup(f5_raw):
    """Build {away@home: {home_f5_ml, away_f5_ml}} from h2h_h1 market."""
    lookup = {}
    for game in f5_raw:
        home     = game.get("home_team", "")
        away     = game.get("away_team", "")
        home_abr = MLB_TEAM_MAP.get(home, home[:3].upper())
        away_abr = MLB_TEAM_MAP.get(away, away[:3].upper())
        best_h, best_a = None, None
        for bk in game.get("bookmakers", []):
            for mkt in bk.get("markets", []):
                if mkt.get("key") != "h2h_h1":
                    continue
                for outcome in mkt.get("outcomes", []):
                    price = outcome.get("price")
                    if price is None:
                        continue
                    if outcome.get("name") == home:
                        if best_h is None or price > best_h:
                            best_h = price
                    elif outcome.get("name") == away:
                        if best_a is None or price > best_a:
                            best_a = price
        if best_h is not None and best_a is not None:
            lookup[f"{away_abr}@{home_abr}"] = {"home_f5_ml": best_h, "away_f5_ml": best_a}
    return lookup


def expected_runs_f5(wrc_plus, sp_xfip, park_factor=1.0,
                     weather_factor=1.0, ump_run_factor=1.0):
    """Expected runs scored in exactly 5 innings against one SP (no bullpen)."""
    runs = ((wrc_plus / 100) * LG_RPG * (sp_xfip / LG_ERA) *
            park_factor * weather_factor * ump_run_factor * (5.0 / 9.0))
    return round(max(runs, 0.1), 3)


def compute_f5_edge(game_key, home_wrc_adj, away_wrc_adj,
                    hsp, asp, pf, wx_factor, ump_rf, f5_lkp):
    """
    Compute F5 model win probability and compare to market.
    Returns dict with f5 data, or None if no F5 market found.
    """
    mkt_f5 = f5_lkp.get(game_key, {})
    if not mkt_f5:
        return None

    home_rs_f5 = expected_runs_f5(home_wrc_adj, asp["xfip"], pf, wx_factor, ump_rf) * HOME_ADV
    away_rs_f5 = expected_runs_f5(away_wrc_adj, hsp["xfip"], pf, wx_factor, ump_rf)
    home_f5_wp, away_f5_wp = win_prob(home_rs_f5, away_rs_f5)

    home_f5_ml = mkt_f5.get("home_f5_ml")
    away_f5_ml = mkt_f5.get("away_f5_ml")

    mkt_h_p, mkt_a_p = (devig(home_f5_ml, away_f5_ml)
                        if home_f5_ml and away_f5_ml
                        else (home_f5_wp, away_f5_wp))

    home_f5_edge, home_f5_conv = score_edge(home_f5_wp, mkt_h_p)
    away_f5_edge, away_f5_conv = score_edge(away_f5_wp, mkt_a_p)

    if home_f5_edge >= away_f5_edge:
        best_edge = home_f5_edge; best_conv = home_f5_conv
        f5_pick_ml = f"+{home_f5_ml}" if home_f5_ml and home_f5_ml > 0 else str(home_f5_ml)
        f5_side = "home"
    else:
        best_edge = away_f5_edge; best_conv = away_f5_conv
        f5_pick_ml = f"+{away_f5_ml}" if away_f5_ml and away_f5_ml > 0 else str(away_f5_ml)
        f5_side = "away"

    return {
        "home_rs_f5": round(home_rs_f5, 2),
        "away_rs_f5": round(away_rs_f5, 2),
        "home_f5_wp": round(home_f5_wp * 100, 1),
        "away_f5_wp": round(away_f5_wp * 100, 1),
        "home_f5_ml": home_f5_ml, "away_f5_ml": away_f5_ml,
        "f5_edge": round(best_edge, 2),
        "f5_conv": best_conv,
        "f5_pick_side": f5_side,
        "f5_pick_ml": f5_pick_ml,
    }


# ─── SERIES BETTING MODEL ─────────────────────────────────────────────────────

def detect_series_edges(series_schedule_raw, pitcher_db):
    """
    Scan next 4 days for same-team matchups. When team A's SPs average
    significantly lower xFIP across all games vs team B, flag as series pick
    for Polymarket series market.
    Returns list of series edge dicts.
    """
    series = {}   # (team_a, team_b): [(date, away, home, asp_xfip, hsp_xfip)]

    for de in series_schedule_raw.get("dates", []):
        game_date = de.get("date", "")
        for game in de.get("games", []):
            ht = game["teams"]["home"]
            at = game["teams"]["away"]
            ha = MLB_TEAM_MAP.get(ht["team"]["name"], "")
            aa = MLB_TEAM_MAP.get(at["team"]["name"], "")
            if not ha or not aa:
                continue

            asp_name = at.get("probablePitcher", {}).get("fullName", "TBA")
            hsp_name = ht.get("probablePitcher", {}).get("fullName", "TBA")
            asp_sp   = get_sp(asp_name, pitcher_db)
            hsp_sp   = get_sp(hsp_name, pitcher_db)

            key = tuple(sorted([ha, aa]))
            series.setdefault(key, []).append({
                "date": game_date, "away": aa, "home": ha,
                "asp_xfip": asp_sp["xfip"], "hsp_xfip": hsp_sp["xfip"],
            })

    edges = []
    for (t1, t2), games in series.items():
        if len(games) < 2:
            continue

        t1_sp_advantages = 0
        t1_total_xfip, t2_total_xfip = 0.0, 0.0
        for g in games:
            if g["home"] == t1:
                t1_opp_xfip = g["asp_xfip"]  # team1 faces away SP
                t2_opp_xfip = g["hsp_xfip"]
            else:
                t1_opp_xfip = g["hsp_xfip"]
                t2_opp_xfip = g["asp_xfip"]
            t1_total_xfip += t1_opp_xfip
            t2_total_xfip += t2_opp_xfip
            if t1_opp_xfip > t2_opp_xfip + 0.40:
                t1_sp_advantages += 1

        avg_gap = (t1_total_xfip - t2_total_xfip) / len(games)
        if abs(avg_gap) < 0.35:
            continue

        # Team with lower opponent xFIP (better SPs facing them) is the edge team
        if t1_total_xfip > t2_total_xfip:
            edge_team = t2
            opp_team  = t1
            gap       = avg_gap
        else:
            edge_team = t1
            opp_team  = t2
            gap       = -avg_gap

        edges.append({
            "teams": f"{t1} vs {t2}",
            "edge_team": edge_team,
            "opp_team": opp_team,
            "games_in_series": len(games),
            "avg_sp_xfip_gap": round(gap, 3),
            "dates": [g["date"] for g in games],
            "note": (f"{edge_team} faces opponents with avg xFIP {gap:.2f} higher "
                     f"over {len(games)}-game series — series ML edge on Polymarket"),
        })

    edges.sort(key=lambda x: x["avg_sp_xfip_gap"], reverse=True)
    return edges


# ─── LINE MOVEMENT ALERTS ─────────────────────────────────────────────────────

def check_line_movements(current_games, prev_scout_path="last_scout.json"):
    """
    Compare current market odds to the previous scout run.
    Alert when any team's implied probability moved >3%.
    Returns list of movement dicts.
    """
    try:
        with open(prev_scout_path) as f:
            prev = json.load(f)
    except Exception:
        return []

    prev_by_key = {f"{g['away']}@{g['home']}": g
                   for g in prev.get("games", [])}

    movements = []
    for g in current_games:
        key = f"{g['away']}@{g['home']}"
        old = prev_by_key.get(key)
        if not old:
            continue

        for side in ("home", "away"):
            curr_p = float(g.get(f"{side}_market_p", 0))
            prev_p = float(old.get(f"{side}_market_p", 0))
            if curr_p <= 0 or prev_p <= 0:
                continue
            move = curr_p - prev_p
            if abs(move) >= 3.0:
                team = g[side]
                direction = "SHARP TO" if move > 0 else "SHARP AGAINST"
                movements.append({
                    "game": key,
                    "team": team,
                    "side": side,
                    "prev_p": round(prev_p, 1),
                    "curr_p": round(curr_p, 1),
                    "move":   round(move, 1),
                    "direction": direction,
                    "note": (f"{team} {direction} ({prev_p:.1f}% → {curr_p:.1f}%  "
                             f"{'+' if move>0 else ''}{move:.1f}%)"),
                })

    return movements


def send_line_movement_alerts(movements):
    """Send Telegram alert for significant line movements."""
    if not movements:
        return
    lines = [f"PARLAY OS — LINE MOVEMENT ALERT — {DATE}", ""]
    for m in movements:
        arrow = "▲" if m["move"] > 0 else "▼"
        lines.append(f"{arrow} {m['game']} | {m['team']}")
        lines.append(f"  {m['note']}")
        lines.append(f"  Signal: {m['direction']}")
        lines.append("")
    send_telegram("\n".join(lines))


# ─── MAIN SCOUT LOOP ──────────────────────────────────────────────────────────

def run_scout():
    print(f"[{NOW.strftime('%H:%M ET')}] Scout v4 — {DATE}")

    schedule_raw    = fetch_mlb_schedule()
    odds_raw        = fetch_odds()
    f5_odds_raw     = fetch_f5_odds()
    fg_pitchers     = fetch_fg_pitchers()
    fg_batting      = fetch_fg_team_batting()
    fg_pitching     = fetch_fg_team_pitching()
    series_schedule = fetch_series_schedule(set())

    pitcher_db = parse_pitchers(fg_pitchers)
    wrc_db     = parse_team_stat(fg_batting,  "wRC+", "wrc_plus", "wRC")
    bp_era_db  = parse_team_stat(fg_pitching, "ERA", "era")
    odds_lkp   = build_odds_lookup(odds_raw)
    f5_lkp     = build_f5_odds_lookup(f5_odds_raw)

    umpires = extract_umpires(schedule_raw)

    print(f"  wRC+ teams: {len(wrc_db)}  BP ERA teams: {len(bp_era_db)}  Umpires: {len(umpires)}")

    # Collect all teams + home teams for fatigue check and weather
    today_teams      = set()
    today_home_teams = {}  # game_key → home_abr
    for de in schedule_raw.get("dates", []):
        for game in de.get("games", []):
            st = game.get("status", {}).get("abstractGameState", "")
            if st in ("Preview", "Scheduled", "Pre-Game"):
                ha = MLB_TEAM_MAP.get(game["teams"]["home"]["team"]["name"], "")
                aa = MLB_TEAM_MAP.get(game["teams"]["away"]["team"]["name"], "")
                if ha: today_teams.add(ha)
                if aa: today_teams.add(aa)
                if ha and aa:
                    today_home_teams[f"{aa}@{ha}"] = ha

    fatigue     = fetch_bullpen_fatigue(today_teams)
    weather_map = fetch_all_weather(schedule_raw, today_home_teams)

    bm = BankrollManager()

    games_out = []

    for de in schedule_raw.get("dates", []):
        for game in de.get("games", []):
            state = game.get("status", {}).get("abstractGameState", "")
            if state not in ("Preview", "Scheduled", "Pre-Game"):
                continue

            home_t = game["teams"]["home"]
            away_t = game["teams"]["away"]
            home_n = home_t["team"]["name"]
            away_n = away_t["team"]["name"]
            home_a = MLB_TEAM_MAP.get(home_n, home_n[:3].upper())
            away_a = MLB_TEAM_MAP.get(away_n, away_n[:3].upper())

            home_sp_name = home_t.get("probablePitcher", {}).get("fullName", "TBA")
            away_sp_name = away_t.get("probablePitcher", {}).get("fullName", "TBA")

            # SP handedness from schedule
            home_sp_hand = home_t.get("probablePitcher", {}).get("pitchHand", {}).get("code", "R")
            away_sp_hand = away_t.get("probablePitcher", {}).get("pitchHand", {}).get("code", "R")
            # Fallback to FG data
            if home_sp_hand not in ("R", "L"):
                home_sp_hand = pitcher_db.get(home_sp_name.lower(), {}).get("hand", "R")
            if away_sp_hand not in ("R", "L"):
                away_sp_hand = pitcher_db.get(away_sp_name.lower(), {}).get("hand", "R")

            try:
                from datetime import datetime as _dt
                utc_dt = _dt.strptime(game.get("gameDate", "")[:19], "%Y-%m-%dT%H:%M:%S")
                utc_dt = utc_dt.replace(tzinfo=pytz.utc)
                game_time = utc_dt.astimezone(ET).strftime("%-I:%M %p ET")
            except Exception:
                game_time = ""

            home_rec = "{}-{}".format(
                home_t.get("leagueRecord", {}).get("wins", 0),
                home_t.get("leagueRecord", {}).get("losses", 0))
            away_rec = "{}-{}".format(
                away_t.get("leagueRecord", {}).get("wins", 0),
                away_t.get("leagueRecord", {}).get("losses", 0))

            game_key = f"{away_a}@{home_a}"

            # SP stats
            hsp = get_sp(home_sp_name, pitcher_db)
            asp = get_sp(away_sp_name, pitcher_db)

            # Team stats with fallbacks
            home_wrc = wrc_db.get(home_a, wrc_db.get(home_n, 100))
            away_wrc = wrc_db.get(away_a, wrc_db.get(away_n, 100))
            home_bp  = bp_era_db.get(home_a, bp_era_db.get(home_n, 4.20))
            away_bp  = bp_era_db.get(away_a, bp_era_db.get(away_n, 4.20))

            # Platoon adjustments
            home_plat_adj = platoon_wrc_adjustment(home_a, away_sp_hand)
            away_plat_adj = platoon_wrc_adjustment(away_a, home_sp_hand)
            home_wrc_adj  = home_wrc + home_plat_adj
            away_wrc_adj  = away_wrc + away_plat_adj

            plat_notes = []
            if abs(home_plat_adj) >= 3:
                plat_notes.append(f"{home_a} platoon {home_plat_adj:+.0f} vs {away_sp_hand}HP")
            if abs(away_plat_adj) >= 3:
                plat_notes.append(f"{away_a} platoon {away_plat_adj:+.0f} vs {home_sp_hand}HP")
            platoon_note = ", ".join(plat_notes)

            # Umpire
            umpire_name = umpires.get(game_key, "")
            ump_k, ump_rf, ump_note = get_umpire_factors(umpire_name)

            # Weather
            wx_raw = weather_map.get(game_key)
            wx_factor, wx_note = parse_weather(wx_raw, home_a)

            # Bullpen fatigue
            home_fat = fatigue.get(home_a, {})
            away_fat = fatigue.get(away_a, {})
            home_fat_score = home_fat.get("fatigue_score", 0)
            away_fat_score = away_fat.get("fatigue_score", 0)
            fat_notes = []
            if home_fat_score >= 2:
                fat_notes.append(f"{home_a} pen {home_fat.get('note','')}")
            if away_fat_score >= 2:
                fat_notes.append(f"{away_a} pen {away_fat.get('note','')}")
            bp_fatigue_note = ", ".join(fat_notes)

            # Park factor
            pf     = PARK_FACTORS.get(home_a, 1.0)
            hsp_ip = sp_ip_estimate(hsp["xfip"]) if home_sp_name != "TBA" else 5.5
            asp_ip = sp_ip_estimate(asp["xfip"]) if away_sp_name != "TBA" else 5.5

            # Run expectancy (all factors combined)
            home_rs = expected_runs(
                home_wrc_adj, asp["xfip"], away_bp, pf, asp_ip,
                wx_factor, ump_rf, away_fat_score) * HOME_ADV
            away_rs = expected_runs(
                away_wrc_adj, hsp["xfip"], home_bp, pf, hsp_ip,
                wx_factor, ump_rf, home_fat_score)

            home_wp, away_wp = win_prob(home_rs, away_rs)

            # Market odds
            mkt    = odds_lkp.get(game_key, {})
            hml_r  = mkt.get("home_ml")
            aml_r  = mkt.get("away_ml")

            if hml_r is not None and aml_r is not None:
                hml_s = f"+{hml_r}" if hml_r > 0 else str(hml_r)
                aml_s = f"+{aml_r}" if aml_r > 0 else str(aml_r)
                mkt_home_p, mkt_away_p = devig(hml_r, aml_r)
            else:
                hml_s = str(prob_to_american(home_wp))
                aml_s = str(prob_to_american(away_wp))
                mkt_home_p, mkt_away_p = home_wp, away_wp

            home_edge, home_conv = score_edge(home_wp, mkt_home_p)
            away_edge, away_conv = score_edge(away_wp, mkt_away_p)

            if home_edge >= away_edge:
                best_edge = home_edge; best_conv = home_conv; pick_side = "home"
            else:
                best_edge = away_edge; best_conv = away_conv; pick_side = "away"

            pick_team  = home_a if pick_side == "home" else away_a
            pick_ml    = hml_s  if pick_side == "home" else aml_s
            pick_wp    = home_wp if pick_side == "home" else away_wp
            pick_mkt_p = mkt_home_p if pick_side == "home" else mkt_away_p

            # Kelly stake sizing
            stake = 0.0
            if best_conv != "PASS":
                stake = bm.stake_for_conviction(
                    best_conv, best_edge, pick_ml, pick_wp * 100)

            # K prop
            opp_sp_for_k = hsp if pick_side == "away" else asp
            ump_k_factor = ump_k
            k_data = k_prop_estimate(opp_sp_for_k, ump_k_factor)
            k_prop_label = (f"{(home_sp_name if pick_side=='away' else away_sp_name)} "
                            f"O{k_data['k_line']}K ({k_data['p_over']*100:.0f}%)")

            # NRFI
            lam_away = (asp["xfip"] * 0.92) / 9
            lam_home = (hsp["xfip"] * 0.92) / 9
            nrfi_p   = exp(-lam_away) * exp(-lam_home)
            nrfi_flag = nrfi_p > 0.52 and min(asp["xfip"], hsp["xfip"]) < 3.50

            # F5 model
            f5_data = compute_f5_edge(
                game_key, home_wrc_adj, away_wrc_adj,
                hsp, asp, pf, wx_factor, ump_rf, f5_lkp)

            # Bet type recommendation
            if best_conv == "PASS":
                rec_bet = ""
            elif abs(best_edge) >= 7:
                rec_bet = f"{pick_team} ML"
            elif abs(best_edge) >= 5:
                # Offer F5 as lower-variance alternative
                rec_bet = f"{pick_team} F5"
            elif (k_data["p_over"] > 0.58 and opp_sp_for_k.get("xfip", 4.2) < 3.5):
                rec_bet = k_prop_label
            else:
                rec_bet = f"{pick_team} ML"

            game_entry = {
                "away": away_a, "home": home_a,
                "away_name": away_n, "home_name": home_n,
                "time": game_time,
                "away_record": away_rec, "home_record": home_rec,
                "asp": away_sp_name, "hsp": home_sp_name,
                "asp_xfip": round(asp["xfip"], 2), "hsp_xfip": round(hsp["xfip"], 2),
                "asp_siera": round(asp["siera"], 2), "hsp_siera": round(hsp["siera"], 2),
                "asp_k9": round(asp["k9"], 1), "hsp_k9": round(hsp["k9"], 1),
                "asp_hand": away_sp_hand, "hsp_hand": home_sp_hand,
                "away_wrc": round(away_wrc), "home_wrc": round(home_wrc),
                "away_wrc_adj": round(away_wrc_adj, 1),
                "home_wrc_adj": round(home_wrc_adj, 1),
                "platoon_note": platoon_note,
                "away_bp_era": round(away_bp, 2), "home_bp_era": round(home_bp, 2),
                "away_fat_score": away_fat_score, "home_fat_score": home_fat_score,
                "bp_fatigue_note": bp_fatigue_note,
                "umpire": umpire_name, "umpire_note": ump_note,
                "weather_note": wx_note,
                "park_factor": pf,
                "away_exp_rs": round(away_rs, 2), "home_exp_rs": round(home_rs, 2),
                "home_wp": round(home_wp * 100, 1), "away_wp": round(away_wp * 100, 1),
                "model_home_ml": prob_to_american(home_wp),
                "model_away_ml": prob_to_american(away_wp),
                "aml": aml_s, "hml": hml_s,
                "home_market_p": round(mkt_home_p * 100, 1),
                "away_market_p": round(mkt_away_p * 100, 1),
                "home_edge": round(home_edge, 2), "away_edge": round(away_edge, 2),
                "edge_pct": round(best_edge, 2),
                "conviction": best_conv,
                "pick": rec_bet,
                "pick_odds": pick_ml,
                "pick_wp": round(pick_wp * 100, 1),
                "pick_side": pick_side,
                "stake": stake,
                "nrfi": "yes" if nrfi_flag else "no",
                "k_prop": k_prop_label,
                "k_data": k_data,
                "total": str(round(home_rs + away_rs, 1)),
                "event_id": mkt.get("event_id", ""),
                "f5": f5_data,
            }

            # Add conviction narrative AFTER building game_entry (needs all fields)
            game_entry["narrative"] = conviction_narrative(game_entry)

            games_out.append(game_entry)

    games_out.sort(key=lambda x: x["edge_pct"], reverse=True)

    high_picks   = [g for g in games_out if g["conviction"] == "HIGH"   and g["pick"]]
    medium_picks = [g for g in games_out if g["conviction"] == "MEDIUM" and g["pick"]]
    passes       = [f"{g['away']} @ {g['home']} — edge {max(g['home_edge'],g['away_edge']):+.1f}%"
                    for g in games_out if g["conviction"] == "PASS"]

    avg_edge = (sum(g["edge_pct"] for g in games_out) / len(games_out)) if games_out else 0

    # Series edges
    series_edges = detect_series_edges(series_schedule, pitcher_db)
    if series_edges:
        print(f"  Series edges: {len(series_edges)} found")
        lines_to_send = [f"PARLAY OS — SERIES EDGE — {DATE}", ""]
        for se in series_edges[:3]:
            lines_to_send.append(f"{se['edge_team']} vs {se['opp_team']} ({se['games_in_series']}G)")
            lines_to_send.append(f"  {se['note']}")
            lines_to_send.append(f"  Dates: {', '.join(se['dates'])}")
            lines_to_send.append("")
        send_telegram("\n".join(lines_to_send))

    # Line movement alerts vs previous run
    movements = check_line_movements(games_out)
    if movements:
        print(f"  Line movements: {len(movements)} significant moves detected")
        send_line_movement_alerts(movements)

    output = {
        "date":     DATE,
        "model":    "scout_v4_full_factors",
        "verdict":  ("GREEN - FULL SESSION" if high_picks
                     else "YELLOW - SELECTIVE SESSION" if medium_picks
                     else "RED - PASS DAY"),
        "note":     (f"{len(high_picks)}H {len(medium_picks)}M edges. "
                     f"Avg {avg_edge:+.1f}%. {len(pitcher_db)} SPs loaded."),
        "games":    games_out,
        "high":     [g["pick"] for g in high_picks],
        "medium":   [g["pick"] for g in medium_picks],
        "passes":   passes,
        "parlay_legs": [g["pick"] for g in high_picks[:2]],
        "bankroll": bm.status_str(),
        "session_note": (f"Factors: {len(pitcher_db)} pitchers, "
                         f"{len(wrc_db)} wRC+, {len(bp_era_db)} BP ERA, "
                         f"{len(umpires)} umpires, {len(weather_map)} weather, "
                         f"{len(odds_raw)} live odds, {len(f5_lkp)} F5 markets."),
        # legacy compat
        "locks":         [g["pick"] for g in high_picks],
        "coinflips":     [g["pick"] for g in medium_picks],
        "series_edges":  series_edges,
        "line_movements": movements,
        "f5_picks":      [g["f5"]["f5_pick_ml"] for g in games_out
                          if g.get("f5") and g["f5"].get("f5_conv") != "PASS"],
    }
    return output


def format_and_send(data):
    verdict = data.get("verdict", "")
    tag = "GREEN" if "GREEN" in verdict else "YELLOW" if "YELLOW" in verdict else "RED"

    msg = [
        f"PARLAY OS — SCOUT v4 — {data['date']}",
        f"{tag}  {verdict}",
        data.get("note", ""),
        f"Bankroll: {data.get('bankroll','')}",
        "",
    ]
    high   = data.get("high", [])
    medium = data.get("medium", [])
    if high:   msg.append(f"HIGH:   {', '.join(high)}")
    if medium: msg.append(f"MEDIUM: {', '.join(medium)}")

    legs = data.get("parlay_legs", [])
    if len(legs) >= 2:
        msg.append(f"\nPARLAY LEGS: {' + '.join(legs)}")

    send_telegram("\n".join(msg))

    for g in data.get("games", []):
        if g["conviction"] == "PASS":
            continue

        conv = g["conviction"]
        lbl  = f"[{conv}]"
        ml_s = f"+{g['pick_odds']}" if str(g['pick_odds']).lstrip('-').isdigit() and int(str(g['pick_odds']).lstrip('+-')) > 0 and not str(g['pick_odds']).startswith('-') else str(g['pick_odds'])

        lines = [
            f"\n{lbl} {g['away']} @ {g['home']} — {g.get('time','')}",
            f"  {g['away']} {g['away_record']} | {g['home']} {g['home_record']}",
            f"  SP: {g['asp']} ({g['asp_hand']}HP, xFIP {g['asp_xfip']}) vs {g['hsp']} ({g['hsp_hand']}HP, xFIP {g['hsp_xfip']})",
            f"  Offense: {g['away']} wRC+{int(g['away_wrc'])} ({g['away_wrc_adj']:+.0f} plat) / {g['home']} wRC+{int(g['home_wrc'])} ({g['home_wrc_adj']:+.0f} plat)",
            f"  Bullpen: {g['away']} {g['away_bp_era']} ERA (fat:{g['away_fat_score']}) / {g['home']} {g['home_bp_era']} ERA (fat:{g['home_fat_score']})",
        ]
        if g.get("umpire"):
            lines.append(f"  Umpire: {g['umpire']}{' — ' + g['umpire_note'] if g['umpire_note'] else ''}")
        if g.get("weather_note"):
            lines.append(f"  Weather: {g['weather_note']}")
        lines += [
            f"  Model WP: {g['away']} {g['away_wp']}% / {g['home']} {g['home_wp']}%",
            f"  Market:   {g['away']} {g['aml']} ({g['away_market_p']}%) / {g['home']} {g['hml']} ({g['home_market_p']}%)",
            f"  PICK: {g.get('pick','')} {ml_s}   EDGE: {g['edge_pct']:+.1f}%   CONVICTION: {conv}",
            f"  STAKE: ${g['stake']:.2f}",
            f"  NARRATIVE: {g.get('narrative','')}",
        ]
        if g.get("nrfi") == "yes":
            lines.append("  NRFI: both SPs elite — lean toward scoreless 1st")
        if g.get("k_prop"):
            lines.append(f"  K PROP: {g['k_prop']}")
        if g.get("total"):
            lines.append(f"  MODEL TOTAL: O/U {g['total']}")
        lines.append("  ─────")
        send_telegram("\n".join(lines))
        time.sleep(0.5)

    send_telegram(f"\nParlay OS Scout v4 — {NOW.strftime('%I:%M %p ET')}")


def main():
    try:
        data = run_scout()

        with open("last_scout.json", "w") as f:
            json.dump(data, f, indent=2)
        print("Saved last_scout.json")

        # Save to SQLite
        try:
            import db as _db
            _db.save_scout_run(
                date=DATE,
                games_analyzed=len(data.get("games", [])),
                high_count=len(data.get("high", [])),
                medium_count=len(data.get("medium", [])),
                pass_count=len(data.get("passes", [])),
                avg_edge=sum(g["edge_pct"] for g in data.get("games", [])) / max(len(data.get("games", [])), 1),
                data_json_str=json.dumps(data),
            )
            # Log each high/medium pick to bets table
            for g in data.get("games", []):
                if g["conviction"] in ("HIGH", "MEDIUM") and g.get("pick"):
                    _db.log_bet(
                        date=DATE, bet=g["pick"], bet_type="ML",
                        game=f"{g['away']}@{g['home']}", sp=g.get("asp",""),
                        park=g.get("home",""), umpire=g.get("umpire",""),
                        bet_odds=g.get("pick_odds",""), model_prob=g.get("pick_wp"),
                        market_prob=g.get("home_market_p") if g.get("pick_side")=="home" else g.get("away_market_p"),
                        edge_pct=g.get("edge_pct"), conviction=g["conviction"],
                        stake=g.get("stake", 0),
                    )
        except Exception as e:
            print(f"DB save warning: {e}")

        # Append to JSON CLV log for backward compat
        try:
            with open("clv_log.json") as f:
                clv_log = json.load(f)
        except Exception:
            clv_log = []

        for g in data.get("games", []):
            if g["conviction"] in ("HIGH", "MEDIUM") and g.get("pick"):
                pick = g["pick"]
                clv_log.append({
                    "date":         DATE,
                    "bet":          pick,
                    "type":         "F5" if "F5" in pick else "NRFI" if "NRFI" in pick else "ML",
                    "bet_odds":     g.get("pick_odds", ""),
                    "closing_odds": None,
                    "result":       None,
                    "clv_pct":      None,
                    "model":        "scout_v4",
                    "edge_pct":     g.get("edge_pct"),
                    "conviction":   g["conviction"],
                    "game":         f"{g['away']}@{g['home']}",
                    "sp":           g.get("asp",""),
                    "umpire":       g.get("umpire",""),
                    "park":         g.get("home",""),
                })

        with open("clv_log.json", "w") as f:
            json.dump(clv_log, f, indent=2)

        format_and_send(data)
        print(f"Done. {len(data.get('games', []))} games, "
              f"{len(data['high'])}H {len(data['medium'])}M.")

    except Exception as e:
        import traceback
        traceback.print_exc()
        try:
            send_telegram(f"PARLAY OS Scout FAILED\n{e}\n{NOW.strftime('%I:%M %p ET')}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
