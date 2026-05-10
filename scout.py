"""
PARLAY OS — SCOUT v3
Pure probability model: SP xFIP/SIERA + bullpen ERA + offense wRC+ + run expectancy.
No rigid team rules. Math drives every pick.
"""

import os, json, time, requests
from datetime import datetime
import pytz

from math_engine import implied_prob, no_vig_prob, expected_value, kelly_criterion

ODDS_API_KEY       = os.environ.get("ODDS_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]

ET    = pytz.timezone("America/New_York")
NOW   = datetime.now(ET)
DATE  = NOW.strftime("%Y-%m-%d")

LG_RPG   = 4.35   # 2025 MLB avg runs per game
LG_ERA   = 4.35   # 2025 MLB avg ERA
PYTH_EXP = 1.83   # Pythagorean exponent
HOME_ADV = 1.035  # Home field scoring bonus (~3.5%)

# Park factors (2025 estimates, 100 = neutral)
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
           f"?sportId=1&date={DATE}&hydrate=probablePitcher,team&gameType=R")
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


def _fg_get(params):
    """Generic FanGraphs leaderboard fetch with headers to avoid 403."""
    base = "https://www.fangraphs.com/api/leaders/major-league/data"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ParlayOS/3.0)",
        "Accept": "application/json",
    }
    try:
        r = requests.get(base, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"FanGraphs err ({params.get('stats','?')}/{params.get('type','?')}): {e}")
        return {}


def fetch_fg_pitchers():
    return _fg_get({
        "pos": "all", "stats": "pit", "lg": "all", "qual": "20",
        "season": "2025", "season1": "2025", "type": "8",
        "ind": "0", "pageitems": "500", "pagenum": "1",
        "sortdir": "asc", "sortstat": "xFIP",
    })


def fetch_fg_team_batting():
    return _fg_get({
        "pos": "all", "stats": "bat", "lg": "all", "qual": "0",
        "season": "2025", "season1": "2025", "type": "8",
        "ind": "1", "pageitems": "50", "pagenum": "1",
        "sortdir": "desc", "sortstat": "wRC+",
    })


def fetch_fg_team_pitching():
    return _fg_get({
        "pos": "all", "stats": "pit", "lg": "all", "qual": "0",
        "season": "2025", "season1": "2025", "type": "1",
        "ind": "1", "pageitems": "50", "pagenum": "1",
        "sortdir": "asc", "sortstat": "ERA",
    })


# ─── DATA PARSING ─────────────────────────────────────────────────────────────

def _fval(row, *keys, default=None):
    """Try multiple field name variants."""
    for k in keys:
        v = row.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return default


def parse_pitchers(raw):
    """Returns {name_lower: {xfip, siera, k9, team}} dict."""
    db = {}
    for row in raw.get("data", []):
        name = row.get("PlayerName") or row.get("Name") or ""
        if not name:
            continue
        xfip  = _fval(row, "xFIP", "xfip", "xFIP-", default=4.20)
        siera = _fval(row, "SIERA", "siera", default=xfip)
        k9    = _fval(row, "K/9", "K9", "k9", default=8.5)
        ip    = _fval(row, "IP", "ip", default=50.0)
        team  = row.get("Team") or row.get("team") or ""
        db[name.lower()] = {
            "name": name, "xfip": xfip, "siera": siera,
            "k9": k9, "ip": ip, "team": team,
        }
    print(f"  Pitchers loaded: {len(db)}")
    return db


def parse_team_stat(raw, *keys):
    """Returns {team_abbr: float} for the first matching key."""
    out = {}
    for row in raw.get("data", []):
        team = row.get("Team") or row.get("team") or ""
        val  = _fval(row, *keys)
        if team and val is not None:
            out[team] = val
    return out


def get_sp(name, pitcher_db):
    """Look up SP by name with last-name fallback."""
    if not name or name == "TBA":
        return {"xfip": 4.20, "siera": 4.20, "k9": 8.5, "ip": 50.0}
    n = name.lower()
    if n in pitcher_db:
        return pitcher_db[n]
    # Last name match (>4 chars to avoid noise)
    last = n.split()[-1] if n.split() else ""
    if len(last) > 4:
        for key, data in pitcher_db.items():
            if key.endswith(last):
                return data
    return {"xfip": 4.20, "siera": 4.20, "k9": 8.5, "ip": 50.0}


# ─── PROBABILITY MODEL ────────────────────────────────────────────────────────

def sp_ip_estimate(xfip):
    """Expected innings from SP based on quality."""
    if xfip < 3.00: return 6.5
    if xfip < 3.50: return 6.0
    if xfip < 4.00: return 5.75
    if xfip < 4.50: return 5.5
    return 5.0


def expected_runs(wrc_plus, opp_sp_xfip, opp_bp_era, park_factor=1.0, sp_ip=5.5):
    """
    Expected runs scored in one game.
    Formula: (wRC+/100) × LG_RPG × (combined_ERA / LG_ERA) × park_factor

    combined_ERA is the opposing staff's weighted ERA by innings.
    Higher opposing ERA → more runs scored (intuitive: bad pitching = more runs).
    """
    bp_ip = 9 - sp_ip
    combined_era = (opp_sp_xfip * sp_ip + opp_bp_era * bp_ip) / 9
    runs = (wrc_plus / 100) * LG_RPG * (combined_era / LG_ERA) * park_factor
    return round(max(runs, 0.5), 3)


def win_prob(home_rs, away_rs):
    """Pythagorean win probability."""
    h = home_rs ** PYTH_EXP
    a = away_rs ** PYTH_EXP
    hwp = h / (h + a)
    return round(hwp, 4), round(1 - hwp, 4)


def prob_to_american(p):
    """Convert probability [0,1] to American odds integer."""
    p = max(min(p, 0.99), 0.01)
    if p >= 0.5:
        return round(-100 * p / (1 - p))
    return round(100 * (1 - p) / p)


def devig(ml1, ml2):
    """Remove vig from two American odds strings, return (true_p1, true_p2)."""
    def ip(ml):
        n = float(str(ml).replace("+", ""))
        return 100 / (n + 100) if n > 0 else abs(n) / (abs(n) + 100)
    p1, p2 = ip(ml1), ip(ml2)
    t = p1 + p2
    return p1 / t, p2 / t


def score_edge(model_p, market_p, score_cap=100):
    """Return edge percentage and game score 0-100."""
    edge = model_p - market_p
    if edge >= 0.07:
        score = min(85 + (edge - 0.07) * 300, score_cap)
        tag = "LOCK"
    elif edge >= 0.04:
        score = 70 + (edge - 0.04) * 500
        tag = "LOCK"
    elif edge >= 0.02:
        score = 55 + (edge - 0.02) * 750
        tag = "CF"
    elif edge >= 0:
        score = 40 + edge * 750
        tag = "PASS"
    else:
        score = max(0, 40 + edge * 400)
        tag = "PASS"
    return round(edge * 100, 2), round(score), tag


# ─── ODDS LOOKUP ──────────────────────────────────────────────────────────────

def build_odds_lookup(odds_raw):
    """Parse the-odds-api response into {away@home: {home_ml, away_ml, event_id}} dict."""
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
                        # Pick best (most favorable for bettor) home line
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


# ─── MAIN SCOUT LOOP ──────────────────────────────────────────────────────────

def run_scout():
    print(f"[{NOW.strftime('%H:%M ET')}] Scout v3 — pure probability model — {DATE}")

    schedule_raw  = fetch_mlb_schedule()
    odds_raw      = fetch_odds()
    fg_pitchers   = fetch_fg_pitchers()
    fg_batting    = fetch_fg_team_batting()
    fg_pitching   = fetch_fg_team_pitching()

    pitcher_db = parse_pitchers(fg_pitchers)
    wrc_db     = parse_team_stat(fg_batting, "wRC+", "wrc_plus", "wRC")
    bp_era_db  = parse_team_stat(fg_pitching, "ERA", "era")
    odds_lkp   = build_odds_lookup(odds_raw)

    print(f"  wRC+ teams: {len(wrc_db)}  BP ERA teams: {len(bp_era_db)}")

    games_out = []

    for date_entry in schedule_raw.get("dates", []):
        for game in date_entry.get("games", []):
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

            try:
                from datetime import datetime as _dt
                utc_dt = _dt.strptime(game.get("gameDate","")[:19], "%Y-%m-%dT%H:%M:%S")
                utc_dt = utc_dt.replace(tzinfo=pytz.utc)
                game_time = utc_dt.astimezone(ET).strftime("%-I:%M %p ET")
            except Exception:
                game_time = ""

            home_rec = "{}-{}".format(
                home_t.get("leagueRecord",{}).get("wins",0),
                home_t.get("leagueRecord",{}).get("losses",0))
            away_rec = "{}-{}".format(
                away_t.get("leagueRecord",{}).get("wins",0),
                away_t.get("leagueRecord",{}).get("losses",0))

            # SP stats
            hsp = get_sp(home_sp_name, pitcher_db)
            asp = get_sp(away_sp_name, pitcher_db)

            # Team stats with fallbacks
            home_wrc = wrc_db.get(home_a, wrc_db.get(home_n, 100))
            away_wrc = wrc_db.get(away_a, wrc_db.get(away_n, 100))
            home_bp  = bp_era_db.get(home_a, bp_era_db.get(home_n, 4.20))
            away_bp  = bp_era_db.get(away_a, bp_era_db.get(away_n, 4.20))

            pf      = PARK_FACTORS.get(home_a, 1.0)
            hsp_ip  = sp_ip_estimate(hsp["xfip"]) if home_sp_name != "TBA" else 5.5
            asp_ip  = sp_ip_estimate(asp["xfip"]) if away_sp_name != "TBA" else 5.5

            # Run expectancy
            home_rs = expected_runs(home_wrc, asp["xfip"], away_bp, pf, asp_ip) * HOME_ADV
            away_rs = expected_runs(away_wrc, hsp["xfip"], home_bp, pf, hsp_ip)

            home_wp, away_wp = win_prob(home_rs, away_rs)

            # Market odds
            key   = f"{away_a}@{home_a}"
            mkt   = odds_lkp.get(key, {})
            hml_r = mkt.get("home_ml")
            aml_r = mkt.get("away_ml")

            if hml_r is not None and aml_r is not None:
                hml_s = f"+{hml_r}" if hml_r > 0 else str(hml_r)
                aml_s = f"+{aml_r}" if aml_r > 0 else str(aml_r)
                mkt_home_p, mkt_away_p = devig(hml_r, aml_r)
            else:
                hml_s = str(prob_to_american(home_wp))
                aml_s = str(prob_to_american(away_wp))
                mkt_home_p, mkt_away_p = home_wp, away_wp

            home_edge_pct, home_score, _ = score_edge(home_wp, mkt_home_p)
            away_edge_pct, away_score, _ = score_edge(away_wp, mkt_away_p)

            if home_edge_pct >= away_edge_pct:
                best_edge = home_edge_pct
                best_score = home_score
                pick_side = "home"
            else:
                best_edge = away_edge_pct
                best_score = away_score
                pick_side = "away"

            _, __, tag = score_edge(
                home_wp if pick_side == "home" else away_wp,
                mkt_home_p if pick_side == "home" else mkt_away_p
            )

            pick_team = home_a if pick_side == "home" else away_a
            pick_odds_str = hml_s if pick_side == "home" else aml_s
            pick_wp = home_wp if pick_side == "home" else away_wp

            # K prop estimate
            sp_for_k    = away_sp_name if tag != "PASS" and pick_side == "away" else home_sp_name
            sp_stats_k  = asp if pick_side == "away" else hsp
            sp_ip_k     = asp_ip if pick_side == "away" else hsp_ip
            k_line      = round(sp_stats_k["k9"] * sp_ip_k / 9, 1)

            # NRFI: both SPs elite → first inning likely scoreless
            nrfi_prob = (
                (LG_ERA / max(asp["xfip"], 0.1)) ** (1 / 9) *
                (LG_ERA / max(hsp["xfip"], 0.1)) ** (1 / 9)
            )
            nrfi_flag = nrfi_prob > 1.10 or (asp["xfip"] < 3.20 and hsp["xfip"] < 3.50)

            game_entry = {
                "away": away_a, "home": home_a,
                "away_name": away_n, "home_name": home_n,
                "time": game_time,
                "away_record": away_rec, "home_record": home_rec,
                "asp": away_sp_name, "hsp": home_sp_name,
                "asp_xfip": round(asp["xfip"], 2), "hsp_xfip": round(hsp["xfip"], 2),
                "asp_siera": round(asp["siera"], 2), "hsp_siera": round(hsp["siera"], 2),
                "asp_era": str(round(asp["xfip"], 2)), "hsp_era": str(round(hsp["xfip"], 2)),
                "asp_k9": round(asp["k9"], 1), "hsp_k9": round(hsp["k9"], 1),
                "away_wrc": round(away_wrc), "home_wrc": round(home_wrc),
                "away_bp_era": round(away_bp, 2), "home_bp_era": round(home_bp, 2),
                "park_factor": pf,
                "away_exp_rs": round(away_rs, 2), "home_exp_rs": round(home_rs, 2),
                "home_wp": round(home_wp * 100, 1), "away_wp": round(away_wp * 100, 1),
                "model_home_ml": prob_to_american(home_wp),
                "model_away_ml": prob_to_american(away_wp),
                "aml": aml_s, "hml": hml_s,
                "home_market_p": round(mkt_home_p * 100, 1),
                "away_market_p": round(mkt_away_p * 100, 1),
                "home_edge": round(home_edge_pct, 2),
                "away_edge": round(away_edge_pct, 2),
                "score": best_score, "tag": tag,
                "pick": f"{pick_team} ML" if tag != "PASS" else "",
                "pick_odds": pick_odds_str,
                "pick_wp": round(pick_wp * 100, 1),
                "edge_pct": round(best_edge, 2),
                "edge1": (f"Model: {pick_team} {pick_wp*100:.1f}% vs market "
                          f"{mkt_home_p*100:.1f if pick_side=='home' else mkt_away_p*100:.1f}% "
                          f"→ {best_edge:+.1f}% edge"),
                "risk1": (f"Exp runs: {away_a} {away_rs:.2f} vs {home_a} {home_rs:.2f}. "
                          f"PF {pf:.2f}"),
                "analysis": (
                    f"Pure model edge: {pick_team} {best_edge:+.1f}%. "
                    f"SP gap: {away_a} xFIP {asp['xfip']:.2f} ({asp['siera']:.2f} SIERA) "
                    f"vs {home_a} xFIP {hsp['xfip']:.2f} ({hsp['siera']:.2f} SIERA). "
                    f"Offense: {away_a} wRC+ {int(away_wrc)} / {home_a} wRC+ {int(home_wrc)}. "
                    f"Bullpens: {away_a} {away_bp:.2f} / {home_a} {home_bp:.2f} ERA."
                ),
                "nrfi": "yes" if nrfi_flag else "no",
                "nrfi_odds": "",
                "k_prop": f"{sp_for_k} O{k_line}K" if sp_for_k and sp_for_k != "TBA" else "",
                "total": str(round(home_rs + away_rs, 1)),
                "f5_pick": "",
                "clv_timing": "Market open" if hml_r is not None else "No live market",
                "event_id": mkt.get("event_id", ""),
            }
            games_out.append(game_entry)

    games_out.sort(key=lambda x: x["score"], reverse=True)

    locks     = [g["pick"] for g in games_out if g["tag"] == "LOCK" and g["pick"]]
    coinflips = [g["pick"] for g in games_out if g["tag"] == "CF" and g["pick"]]
    passes    = [f"{g['away']} @ {g['home']} — best edge {max(g['home_edge'],g['away_edge']):+.1f}%"
                 for g in games_out if g["tag"] == "PASS"]

    avg_edge = (sum(max(g["home_edge"], g["away_edge"]) for g in games_out) /
                len(games_out)) if games_out else 0

    output = {
        "date":     DATE,
        "model":    "pure_probability_v3",
        "verdict":  ("GREEN - FULL SESSION" if locks
                     else "YELLOW - REDUCED SESSION" if coinflips
                     else "RED - PASS DAY"),
        "note":     (f"Pure math model. {len(locks)}L {len(coinflips)}CF. "
                     f"Avg edge {avg_edge:+.1f}%. "
                     f"{len(pitcher_db)} SPs loaded."),
        "games":    games_out,
        "locks":    locks,
        "coinflips": coinflips,
        "passes":   passes,
        "parlay_legs": locks[:2],
        "parlay_odds": "",
        "session_note": (
            f"Inputs: {len(pitcher_db)} pitchers, "
            f"{len(wrc_db)} team wRC+, {len(bp_era_db)} team BP ERA, "
            f"{len(odds_raw)} live odds games."
        ),
    }
    return output


def format_and_send(data):
    verdict = data.get("verdict", "")
    tag     = "GREEN" if "GREEN" in verdict else "YELLOW" if "YELLOW" in verdict else "RED"

    msg = [
        f"PARLAY OS — PURE MATH MODEL — {data['date']}",
        f"{tag}  {verdict}",
        data.get("note", ""),
        "",
    ]
    locks = data.get("locks", [])
    cfs   = data.get("coinflips", [])
    if locks:  msg.append(f"LOCKS: {', '.join(locks)}")
    if cfs:    msg.append(f"COIN FLIPS: {', '.join(cfs)}")

    legs = data.get("parlay_legs", [])
    if len(legs) >= 2:
        msg.append(f"\nPARLAY LEGS: {' + '.join(legs)}")

    send_telegram("\n".join(msg))

    for g in data.get("games", []):
        if g["tag"] == "PASS":
            continue
        lbl = "[LOCK]" if g["tag"] == "LOCK" else "[CF]"
        lines = [
            f"\n{lbl} {g['away']} @ {g['home']} — {g.get('time','')}",
            f"  {g['away']} {g['away_record']} | {g['home']} {g['home_record']}",
            f"  SP: {g['asp']} (xFIP {g['asp_xfip']}) vs {g['hsp']} (xFIP {g['hsp_xfip']})",
            f"  Offense: {g['away']} wRC+{int(g['away_wrc'])} / {g['home']} wRC+{int(g['home_wrc'])}",
            f"  Bullpen: {g['away']} {g['away_bp_era']} ERA / {g['home']} {g['home_bp_era']} ERA",
            f"  Model WP: {g['away']} {g['away_wp']}% / {g['home']} {g['home_wp']}%",
            f"  Market:   {g['away']} {g['aml']} ({g['away_market_p']}%) / {g['home']} {g['hml']} ({g['home_market_p']}%)",
            f"  PICK: {g.get('pick','')} {g.get('pick_odds','')}   EDGE: {g['edge_pct']:+.1f}%   Score: {g['score']}/100",
        ]
        if g.get("nrfi") == "yes":  lines.append("  NRFI: elite SPs today")
        if g.get("k_prop"):         lines.append(f"  K PROP: {g['k_prop']}")
        if g.get("total"):          lines.append(f"  MODEL TOTAL: O/U {g['total']}")
        lines.append("  ─────")
        send_telegram("\n".join(lines))
        time.sleep(0.5)

    send_telegram(f"\nParlay OS Pure Model — {NOW.strftime('%I:%M %p ET')}")


def main():
    try:
        data = run_scout()

        with open("last_scout.json", "w") as f:
            json.dump(data, f, indent=2)
        print("Saved last_scout.json")

        try:
            with open("clv_log.json") as f:
                clv_log = json.load(f)
        except Exception:
            clv_log = []

        for pick in data.get("locks", []) + data.get("coinflips", []):
            if isinstance(pick, str) and pick:
                parts = pick.split()
                clv_log.append({
                    "date":         DATE,
                    "bet":          pick,
                    "type":         "F5" if "F5" in pick else "NRFI" if "NRFI" in pick else "ML",
                    "bet_odds":     parts[-1] if parts else "",
                    "closing_odds": None,
                    "result":       None,
                    "clv_pct":      None,
                    "model":        "pure_v3",
                    "edge_pct":     next(
                        (g["edge_pct"] for g in data["games"] if g.get("pick") == pick), None),
                })

        with open("clv_log.json", "w") as f:
            json.dump(clv_log, f, indent=2)

        format_and_send(data)
        print(f"Done. {len(data.get('games', []))} games, "
              f"{len(data['locks'])} locks, {len(data['coinflips'])} CFs.")

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
