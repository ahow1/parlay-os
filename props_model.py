"""
PARLAY OS — PROPS MODEL v1
SP strikeout props (K/9 Poisson model), NRFI (Poisson first-inning), totals (wRC+ run exp).
Data: FanGraphs (pitchers) + MLB Stats API (schedule) + the-odds-api.com (props lines).
Saves to props_output.json. Sends Telegram if edge ≥ 5%.
"""

import os, json, math, time, requests
from datetime import datetime
import pytz

ODDS_API_KEY       = os.environ.get("ODDS_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]

ET   = pytz.timezone("America/New_York")
NOW  = datetime.now(ET)
DATE = NOW.strftime("%Y-%m-%d")

EDGE_THRESHOLD = 0.05
PROPS_FILE     = "props_output.json"

LG_RPG = 4.35
LG_ERA = 4.35
HOME_ADV = 1.035

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


def ip_prob(ml):
    """American odds → implied probability."""
    try:
        n = float(str(ml).replace("+", ""))
        return 100 / (n + 100) if n > 0 else abs(n) / (abs(n) + 100)
    except Exception:
        return None


def devig(p_over, p_under):
    t = p_over + p_under
    return (p_over / t, p_under / t) if t > 0 else (p_over, p_under)


def american(p):
    p = max(min(p, 0.99), 0.01)
    return round(-100 * p / (1 - p)) if p >= 0.5 else round(100 * (1 - p) / p)


def edge_pct(model_p, market_p):
    return round((model_p - market_p) * 100, 2)


# ─── DATA FETCHING ────────────────────────────────────────────────────────────

def fetch_mlb_schedule():
    url = (f"https://statsapi.mlb.com/api/v1/schedule"
           f"?sportId=1&date={DATE}&hydrate=probablePitcher,team&gameType=R")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Schedule err: {e}")
        return {}


def fetch_fg_pitchers():
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    try:
        r = requests.get(
            "https://www.fangraphs.com/api/leaders/major-league/data",
            params={"pos": "all", "stats": "pit", "lg": "all", "qual": "20",
                    "season": "2025", "season1": "2025", "type": "8",
                    "ind": "0", "pageitems": "500", "pagenum": "1",
                    "sortdir": "asc", "sortstat": "xFIP"},
            headers=headers, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"FanGraphs err: {e}")
        return {}


def fetch_fg_team_batting():
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    try:
        r = requests.get(
            "https://www.fangraphs.com/api/leaders/major-league/data",
            params={"pos": "all", "stats": "bat", "lg": "all", "qual": "0",
                    "season": "2025", "season1": "2025", "type": "8",
                    "ind": "1", "pageitems": "50", "pagenum": "1",
                    "sortdir": "desc", "sortstat": "wRC+"},
            headers=headers, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"FanGraphs batting err: {e}")
        return {}


def fetch_k_props(event_id):
    """Pull strikeout props for a specific game from the-odds-api.com."""
    if not ODDS_API_KEY or not event_id:
        return []
    try:
        r = requests.get(
            f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{event_id}/odds/",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "pitcher_strikeouts", "oddsFormat": "american"},
            timeout=15)
        r.raise_for_status()
        return r.json().get("bookmakers", [])
    except Exception as e:
        print(f"Props API err ({event_id}): {e}")
        return []


def fetch_nrfi_props(event_id):
    """Pull NRFI/YRFI props."""
    if not ODDS_API_KEY or not event_id:
        return []
    try:
        r = requests.get(
            f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{event_id}/odds/",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "team_totals,innings", "oddsFormat": "american"},
            timeout=15)
        r.raise_for_status()
        return r.json().get("bookmakers", [])
    except Exception as e:
        return []


def fetch_totals_odds():
    """Pull game totals from the-odds-api.com."""
    if not ODDS_API_KEY:
        return []
    try:
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/",
            params={"apiKey": ODDS_API_KEY, "regions": "us",
                    "markets": "totals", "oddsFormat": "american"},
            timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Totals err: {e}")
        return []


# ─── PARSING ──────────────────────────────────────────────────────────────────

def parse_pitchers(raw):
    db = {}
    for row in raw.get("data", []):
        name = row.get("PlayerName") or row.get("Name") or ""
        if not name:
            continue
        def fv(*keys, default=4.20):
            for k in keys:
                v = row.get(k)
                if v is not None:
                    try: return float(v)
                    except: pass
            return default
        db[name.lower()] = {
            "name": name,
            "xfip": fv("xFIP", "xfip", "xFIP-", default=4.20),
            "siera": fv("SIERA", "siera", default=4.20),
            "k9":   fv("K/9", "K9", "k9", default=8.5),
            "kpct": fv("K%", "k_pct", default=0.22),
            "team": row.get("Team") or "",
        }
    return db


def get_sp(name, db):
    if not name or name == "TBA":
        return {"xfip": 4.20, "siera": 4.20, "k9": 8.5, "kpct": 0.22}
    n = name.lower()
    if n in db:
        return db[n]
    last = n.split()[-1] if n.split() else ""
    if len(last) > 4:
        for k, v in db.items():
            if k.endswith(last):
                return v
    return {"xfip": 4.20, "siera": 4.20, "k9": 8.5, "kpct": 0.22}


def parse_team_wrc(raw):
    out = {}
    for row in raw.get("data", []):
        team = row.get("Team") or ""
        for k in ("wRC+", "wrc_plus", "wRC"):
            v = row.get(k)
            if v is not None:
                try:
                    out[team] = float(v)
                    break
                except Exception:
                    pass
    return out


def get_best_k_line(bookmakers, sp_name):
    """Find best K prop line for a named pitcher."""
    best = {}
    for bk in bookmakers:
        for mkt in bk.get("markets", []):
            if "strikeout" not in mkt.get("key", "").lower():
                continue
            for o in mkt.get("outcomes", []):
                desc = o.get("description", "") or o.get("name", "")
                if sp_name and sp_name.split()[-1].lower() not in desc.lower():
                    continue
                side = "over" if "over" in desc.lower() else "under"
                pt   = o.get("point", 0)
                pr   = o.get("price", 0)
                key  = f"{side}_{pt}"
                if key not in best or pr > best[key]["price"]:
                    best[key] = {"side": side, "line": pt, "price": pr, "book": bk.get("key")}
    return best


# ─── PROBABILITY MODELS ───────────────────────────────────────────────────────

def sp_ip_estimate(xfip):
    """Expected SP innings based on quality."""
    if xfip < 3.00: return 6.5
    if xfip < 3.50: return 6.0
    if xfip < 4.00: return 5.75
    if xfip < 4.50: return 5.5
    return 5.0


def k_model(sp_stats, sp_ip=None):
    """
    Expected K count using Poisson distribution.
    lambda = K/9 × expected_IP / 9 × 9 = K/9 × IP... but K/9 is per 9 innings.
    expected_K = K/9 × expected_IP
    """
    if sp_ip is None:
        sp_ip = sp_ip_estimate(sp_stats["xfip"])
    lam = sp_stats["k9"] * sp_ip / 9  # expected Ks
    return lam


def poisson_over_under(lam, line):
    """P(X > line) and P(X <= line) for Poisson(lam), X is integer."""
    # P(X <= line) = sum P(X=k) for k=0..floor(line)
    from math import exp, factorial
    max_k = int(line)
    p_under = sum(exp(-lam) * (lam**k) / factorial(k) for k in range(max_k + 1))
    # Half-integer lines: line = 5.5 means over if X >= 6
    if line != int(line):
        # exact half-integer line
        p_over = 1 - p_under
    else:
        # Full integer line: push doesn't exist in most books, line like "O5.5"
        p_over = 1 - p_under
    return round(p_over, 4), round(p_under, 4)


def nrfi_model(asp_stats, hsp_stats, park_factor=1.0):
    """
    NRFI probability using Poisson model for first inning.
    Expected runs per inning = xFIP / 9.
    P(NRFI) = P(away scores 0 in top 1) × P(home scores 0 in bot 1).
    Using Poisson: P(X=0) = e^(-lambda).
    """
    # Adjust xFIP for first-inning dynamics (SPs tend to be stronger early)
    away_1st_era = asp_stats["xfip"] * 0.92  # SPs ~8% better in 1st inning
    home_1st_era = hsp_stats["xfip"] * 0.92

    # Expected runs per inning (park-adjusted slightly for first inning)
    lam_away = (away_1st_era / 9) * park_factor
    lam_home = (home_1st_era / 9) * park_factor

    p_away_0 = math.exp(-lam_away)
    p_home_0 = math.exp(-lam_home)
    p_nrfi   = round(p_away_0 * p_home_0, 4)
    p_yrfi   = round(1 - p_nrfi, 4)

    return {
        "p_nrfi": p_nrfi,
        "p_yrfi": p_yrfi,
        "lam_away": round(lam_away, 3),
        "lam_home": round(lam_home, 3),
        "model_nrfi_ml": american(p_nrfi),
    }


def total_model(away_wrc, home_wrc, asp_stats, hsp_stats, park_factor=1.0):
    """
    Expected total runs using wRC+ and SP xFIP.
    Same formula as scout.py run expectancy model.
    """
    asp_ip = sp_ip_estimate(asp_stats["xfip"])
    hsp_ip = sp_ip_estimate(hsp_stats["xfip"])
    # Default BP ERA league avg
    bp_era = 4.50  # bullpen ERA slightly worse than overall

    def exp_runs(wrc, opp_sp_xfip, opp_sp_ip, pf):
        bp_ip = 9 - opp_sp_ip
        combined_era = (opp_sp_xfip * opp_sp_ip + bp_era * bp_ip) / 9
        return max((wrc / 100) * LG_RPG * (combined_era / LG_ERA) * pf, 0.5)

    home_rs = exp_runs(home_wrc, asp_stats["xfip"], asp_ip, park_factor) * HOME_ADV
    away_rs = exp_runs(away_wrc, hsp_stats["xfip"], hsp_ip, park_factor)
    total   = round(home_rs + away_rs, 2)
    return total, round(home_rs, 2), round(away_rs, 2)


# ─── ANALYSIS ─────────────────────────────────────────────────────────────────

def analyze_games(schedule, pitcher_db, wrc_db, totals_raw):
    results = []

    # Build totals lookup
    totals_lkp = {}
    for game in totals_raw:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        ha = MLB_TEAM_MAP.get(home, home[:3].upper())
        aa = MLB_TEAM_MAP.get(away, away[:3].upper())
        for bk in game.get("bookmakers", []):
            for mkt in bk.get("markets", []):
                if mkt.get("key") != "totals":
                    continue
                for o in mkt.get("outcomes", []):
                    pt = o.get("point")
                    pr = o.get("price")
                    side = o.get("name", "").lower()
                    if pt is None or pr is None:
                        continue
                    key = f"{aa}@{ha}"
                    if key not in totals_lkp:
                        totals_lkp[key] = {}
                    k2 = f"{side}_{pt}"
                    if k2 not in totals_lkp[key]:
                        totals_lkp[key][k2] = {"side": side, "line": pt, "price": pr}

    for date_entry in schedule.get("dates", []):
        for game in date_entry.get("games", []):
            state = game.get("status", {}).get("abstractGameState", "")
            if state not in ("Preview", "Scheduled", "Pre-Game"):
                continue

            home_t   = game["teams"]["home"]
            away_t   = game["teams"]["away"]
            home_n   = home_t["team"]["name"]
            away_n   = away_t["team"]["name"]
            home_a   = MLB_TEAM_MAP.get(home_n, home_n[:3].upper())
            away_a   = MLB_TEAM_MAP.get(away_n, away_n[:3].upper())
            hsp_name = home_t.get("probablePitcher", {}).get("fullName", "TBA")
            asp_name = away_t.get("probablePitcher", {}).get("fullName", "TBA")
            game_pk  = game.get("gamePk", "")

            try:
                from datetime import datetime as _dt
                utc_dt = _dt.strptime(game.get("gameDate","")[:19], "%Y-%m-%dT%H:%M:%S")
                utc_dt = utc_dt.replace(tzinfo=pytz.utc)
                game_time = utc_dt.astimezone(ET).strftime("%-I:%M %p ET")
            except Exception:
                game_time = ""

            hsp = get_sp(hsp_name, pitcher_db)
            asp = get_sp(asp_name, pitcher_db)
            home_wrc = wrc_db.get(home_a, 100)
            away_wrc = wrc_db.get(away_a, 100)
            pf = PARK_FACTORS.get(home_a, 1.0)

            game_entry = {
                "away": away_a, "home": home_a,
                "away_name": away_n, "home_name": home_n,
                "time": game_time,
                "asp": asp_name, "hsp": hsp_name,
                "asp_xfip": round(asp["xfip"], 2), "hsp_xfip": round(hsp["xfip"], 2),
                "asp_k9": round(asp["k9"], 1), "hsp_k9": round(hsp["k9"], 1),
                "away_wrc": round(away_wrc), "home_wrc": round(home_wrc),
                "game_pk": game_pk,
                "props": [],
            }

            # ── K PROPS ──────────────────────────────────────────────────────
            # Fetch props for each SP and model expected Ks
            for sp_name_p, sp_stats_p, side_label in [
                (asp_name, asp, "away"), (hsp_name, hsp, "home")
            ]:
                if sp_name_p == "TBA":
                    continue

                sp_ip_p  = sp_ip_estimate(sp_stats_p["xfip"])
                exp_k    = k_model(sp_stats_p, sp_ip_p)

                # Check common K lines (half-integer lines around expected)
                for line_offset in [-1.5, -1.0, -0.5, 0.5, 1.0, 1.5]:
                    line = round(exp_k + line_offset - 0.5, 1)
                    if line < 0:
                        continue
                    p_over, p_under = poisson_over_under(exp_k, line)

                    # Try to get market odds (use -110 as default if no API data)
                    # Market line = nearest 0.5-increment
                    mkt_line = round(exp_k * 2) / 2  # nearest 0.5

                    prop = {
                        "type": "K_PROP",
                        "sp": sp_name_p,
                        "sp_side": side_label,
                        "expected_k": round(exp_k, 2),
                        "model_line": mkt_line,
                        "p_over": round(p_over, 4),
                        "p_under": round(p_under, 4),
                        "model_over_ml": american(p_over),
                        "model_under_ml": american(p_under),
                        "market_line": None,
                        "market_over_ml": None,
                        "market_under_ml": None,
                        "edge_pct": None,
                        "recommendation": None,
                    }

                    # Only add the main line analysis (not all offsets)
                    if abs(line_offset) == 0.5:
                        # P(over model_line)
                        p_o, p_u = poisson_over_under(exp_k, mkt_line)
                        prop.update({
                            "p_over": round(p_o, 4),
                            "p_under": round(p_u, 4),
                            "model_over_ml": american(p_o),
                            "model_under_ml": american(p_u),
                        })
                        game_entry["props"].append(prop)
                        break  # One K prop per SP

            # ── NRFI ──────────────────────────────────────────────────────────
            nrfi = nrfi_model(asp, hsp, pf)
            game_entry["nrfi"] = nrfi

            nrfi_prop = {
                "type": "NRFI",
                "p_nrfi": nrfi["p_nrfi"],
                "p_yrfi": nrfi["p_yrfi"],
                "model_nrfi_ml": nrfi["model_nrfi_ml"],
                "model_yrfi_ml": american(nrfi["p_yrfi"]),
                "market_nrfi_ml": None,
                "edge_pct": None,
                "recommendation": (
                    "NRFI" if nrfi["p_nrfi"] > 0.55 else
                    "YRFI" if nrfi["p_yrfi"] > 0.55 else
                    "LEAN NRFI" if nrfi["p_nrfi"] > 0.50 else "PASS"
                ),
            }
            game_entry["props"].append(nrfi_prop)

            # ── TOTALS ────────────────────────────────────────────────────────
            model_total, home_rs, away_rs = total_model(away_wrc, home_wrc, asp, hsp, pf)
            game_entry["model_total"] = model_total
            game_entry["home_exp_rs"] = home_rs
            game_entry["away_exp_rs"] = away_rs

            t_key = f"{away_a}@{home_a}"
            t_mkt = totals_lkp.get(t_key, {})

            total_prop = {
                "type": "TOTAL",
                "model_total": model_total,
                "home_exp_rs": home_rs,
                "away_exp_rs": away_rs,
                "market_line": None,
                "market_over_ml": None,
                "market_under_ml": None,
                "edge_pct": None,
                "recommendation": None,
            }

            # Find closest market total line
            for k, v in t_mkt.items():
                if "over" in k:
                    mkt_line = v["line"]
                    mkt_over_ml = v["price"]
                    p_o_raw = ip_prob(mkt_over_ml)
                    # Look for corresponding under
                    under_key = f"under_{mkt_line}"
                    mkt_under_ml = t_mkt.get(under_key, {}).get("price")
                    p_u_raw = ip_prob(mkt_under_ml) if mkt_under_ml else None

                    if p_o_raw and p_u_raw:
                        mkt_p_over, _ = devig(p_o_raw, p_u_raw)
                    else:
                        mkt_p_over = p_o_raw or 0.5

                    diff = model_total - mkt_line
                    if diff > 0.5:
                        model_over_p = 0.5 + min(diff * 0.15, 0.35)
                        rec = f"OVER {mkt_line}" if model_over_p - mkt_p_over > EDGE_THRESHOLD else f"LEAN OVER {mkt_line}"
                    elif diff < -0.5:
                        model_over_p = 0.5 - min(abs(diff) * 0.15, 0.35)
                        rec = f"UNDER {mkt_line}" if mkt_p_over - model_over_p > EDGE_THRESHOLD else f"LEAN UNDER {mkt_line}"
                    else:
                        model_over_p = 0.5
                        rec = "PASS"

                    edge = round((model_over_p - mkt_p_over) * 100, 2)
                    total_prop.update({
                        "market_line": mkt_line,
                        "market_over_ml": mkt_over_ml,
                        "market_under_ml": mkt_under_ml,
                        "mkt_p_over": round(mkt_p_over, 4),
                        "model_p_over": round(model_over_p, 4),
                        "edge_pct": edge,
                        "recommendation": rec,
                    })
                    break

            game_entry["props"].append(total_prop)
            results.append(game_entry)

    return results


def find_top_props(results):
    """Extract props with absolute edge ≥ threshold."""
    top = []
    for game in results:
        for prop in game.get("props", []):
            edge = prop.get("edge_pct")
            if edge and abs(edge) >= EDGE_THRESHOLD * 100:
                top.append({
                    "game": f"{game['away']} @ {game['home']}",
                    "time": game.get("time", ""),
                    **prop,
                })
    top.sort(key=lambda x: abs(x.get("edge_pct", 0)), reverse=True)
    return top


def format_message(results, top_props):
    lines = [f"PARLAY OS — PROPS MODEL — {DATE}", ""]

    # Top edges first
    if top_props:
        lines.append(f"TOP PROPS ({len(top_props)} edges ≥ 5%):")
        for p in top_props[:5]:
            lines.append(f"  [{p['type']}] {p['game']} {p['time']}")
            if p["type"] == "K_PROP":
                lines.append(f"  {p.get('sp','')} | Exp K: {p.get('expected_k','')} | Line: {p.get('model_line','')}")
                lines.append(f"  Model: O{p.get('model_over_ml','')} / U{p.get('model_under_ml','')}")
                lines.append(f"  Edge: {p.get('edge_pct',0):+.1f}% → {p.get('recommendation','')}")
            elif p["type"] == "NRFI":
                lines.append(f"  NRFI {p.get('p_nrfi',0)*100:.1f}% | Model: {p.get('model_nrfi_ml','')}")
                lines.append(f"  Edge: {p.get('edge_pct',0):+.1f}% → {p.get('recommendation','')}")
            elif p["type"] == "TOTAL":
                lines.append(f"  Model total: {p.get('model_total','')} | Market: {p.get('market_line','')}")
                lines.append(f"  Edge: {p.get('edge_pct',0):+.1f}% → {p.get('recommendation','')}")
            lines.append("")

    lines.append("─────────────────────────────")
    lines.append("GAME BREAKDOWN:")
    for game in results:
        lines.append(f"\n{game['away']} @ {game['home']} — {game.get('time','')}")
        lines.append(f"  SP: {game['asp']} (K/9 {game['asp_k9']}, xFIP {game['asp_xfip']}) vs {game['hsp']} (K/9 {game['hsp_k9']}, xFIP {game['hsp_xfip']})")
        lines.append(f"  Model total: {game.get('model_total','?')} ({game['away']} {game.get('away_exp_rs','?')} + {game['home']} {game.get('home_exp_rs','?')})")
        nrfi = game.get("nrfi", {})
        if nrfi:
            lines.append(f"  NRFI: {nrfi.get('p_nrfi',0)*100:.1f}% ({nrfi.get('model_nrfi_ml','')})")

    lines.append(f"\nGenerated {NOW.strftime('%I:%M %p ET')} — Parlay OS Props Model")
    return "\n".join(lines)


def main():
    print(f"[{NOW.strftime('%H:%M ET')}] Props model running — {DATE}")

    schedule    = fetch_mlb_schedule()
    fg_pitchers = fetch_fg_pitchers()
    fg_batting  = fetch_fg_team_batting()
    totals_raw  = fetch_totals_odds()

    pitcher_db = parse_pitchers(fg_pitchers)
    wrc_db     = parse_team_wrc(fg_batting)

    print(f"  Pitchers: {len(pitcher_db)} | wRC+ teams: {len(wrc_db)} | Totals games: {len(totals_raw)}")

    results    = analyze_games(schedule, pitcher_db, wrc_db, totals_raw)
    top_props  = find_top_props(results)

    output = {
        "date": DATE,
        "timestamp": NOW.isoformat(),
        "games": results,
        "top_props": top_props,
        "model_version": "props_v1",
    }

    with open(PROPS_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Saved {PROPS_FILE}")

    msg = format_message(results, top_props)
    send_telegram(msg)
    print("Done.")


if __name__ == "__main__":
    main()
