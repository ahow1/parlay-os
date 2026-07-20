"""PARLAY OS — daily pick archive (photographic memory project, Step 2).

Pulls every pick logged for a given date -- staked and over_cap -- with
its diagnostic factor breakdown (Step 1's diagnostic_json), result, and
CLV once available. Used by `python brain.py --archive YYYY-MM-DD` and
by api.py's /api/picks/<date> day view.
"""

import json
import os
from datetime import datetime

import db as _db

ARCHIVE_DIR = "archives"


def _parse_diagnostics(raw: str | None) -> dict | None:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _pick_summary(b: dict) -> dict:
    """Shape one bets-table row into an archive pick record."""
    return {
        "id":           b.get("id"),
        "event":        b.get("game"),
        "bet":          b.get("bet"),
        "bet_type":     b.get("type"),
        "conviction":   b.get("conviction"),
        "odds":         b.get("bet_odds"),
        "model_p":      b.get("model_prob"),
        "market_p":     b.get("market_prob"),
        "edge_pct":     b.get("edge_pct"),
        "stake":        b.get("stake"),
        "over_cap":     bool(b.get("over_cap")),
        "result":       b.get("result"),
        "closing_odds": b.get("closing_odds"),
        "clv_pct":      b.get("clv_pct"),
        "profit":       b.get("profit"),
        "diagnostics":  _parse_diagnostics(b.get("diagnostic_json")),
    }


def _diag_highlight(p: dict) -> str:
    """One-line summary of the diagnostic factors for the text view --
    the full breakdown is always in the JSON file, this is just the
    human-readable digest."""
    d = p.get("diagnostics")
    if not d:
        return "no diagnostics captured"
    bt = d.get("bet_type", "")
    if bt == "ML" and d.get("factors"):
        top = max(d["factors"], key=lambda f: abs(f["weight"] * (f["p"] - 0.5)))
        lean = "favors pick" if top["p"] >= 0.5 else "works against pick"
        flags = d.get("flags") or {}
        flagged = [k for k, v in flags.items() if v and "missing" in k]
        flag_s = f" | flags: {', '.join(flagged)}" if flagged else ""
        return f"top factor: {top['name']} ({lean}, contrib {top['contribution']:+.3f}){flag_s}"
    if bt in ("K_PROP", "ER_PROP"):
        m = d.get("model") or {}
        conf = m.get("confidence")
        return f"model conf={conf} | market_source={d.get('market_source','')}"
    if bt == "HITTER_PROP":
        return f"lam={d.get('lam')} n_games={d.get('n_games')} | market_source={d.get('market_source','')}"
    if bt == "NRFI":
        n = d.get("nrfi") or {}
        return f"lam_away={n.get('lam_away_bats')} lam_home={n.get('lam_home_bats')}"
    if bt == "TOTAL":
        t = d.get("total") or {}
        return f"xR away={d.get('away_xr')} home={d.get('home_xr')} lam={t.get('lam')}"
    if bt == "RUNLINE":
        return f"side={d.get('side')} probs={d.get('run_line_probs')}"
    if bt in ("PARLAY_ML", "PARLAY_PROPS"):
        return f"{len(d.get('legs') or [])} legs"
    if bt == "SGP":
        sgp = d.get("sgp") or {}
        return f"type={sgp.get('type','SGP')} joint_prob={sgp.get('joint_prob')}"
    return f"bet_type={bt}"


def build_daily_archive(date_str: str) -> dict:
    """Every pick logged for date_str (staked + over_cap), with diagnostics,
    result, and CLV. date_str must be YYYY-MM-DD."""
    datetime.strptime(date_str, "%Y-%m-%d")  # raises ValueError on bad format
    bets  = _db.get_bets(date=date_str)
    picks = [_pick_summary(b) for b in bets]

    staked   = [p for p in picks if not p["over_cap"]]
    over_cap = [p for p in picks if p["over_cap"]]
    settled  = [p for p in picks if p["result"]]
    with_clv = [p for p in picks if p["clv_pct"] is not None]

    return {
        "date":            date_str,
        "generated_at":    datetime.utcnow().isoformat() + "Z",
        "total_picks":     len(picks),
        "staked_count":    len(staked),
        "over_cap_count":  len(over_cap),
        "settled_count":   len(settled),
        "with_clv_count":  len(with_clv),
        "picks":           picks,
    }


def format_archive_text(archive: dict) -> str:
    """Readable stdout summary of a build_daily_archive() result."""
    lines = [
        f"=== PARLAY OS DAILY ARCHIVE — {archive['date']} ===",
        f"{archive['total_picks']} picks total "
        f"({archive['staked_count']} staked, {archive['over_cap_count']} over_cap) — "
        f"{archive['settled_count']} settled, {archive['with_clv_count']} with CLV",
        "",
    ]
    if not archive["picks"]:
        lines.append("No picks logged for this date.")
        return "\n".join(lines)

    by_type: dict[str, list] = {}
    for p in archive["picks"]:
        by_type.setdefault(p["bet_type"] or "UNKNOWN", []).append(p)

    for bet_type in sorted(by_type):
        picks = by_type[bet_type]
        lines.append(f"── {bet_type} ({len(picks)}) " + "─" * max(1, 40 - len(bet_type)))
        for p in picks:
            cap_flag = " [OVER_CAP]" if p["over_cap"] else ""
            result_s = f" | result={p['result']}" if p["result"] else " | pending"
            clv_s    = f" | CLV={p['clv_pct']:+.2f}%" if p["clv_pct"] is not None else ""
            model_s  = f"{p['model_p']:.3f}" if isinstance(p["model_p"], (int, float)) else "?"
            market_s = f"{p['market_p']:.3f}" if isinstance(p["market_p"], (int, float)) else "?"
            edge_s   = f"{p['edge_pct']:+.1f}%" if isinstance(p["edge_pct"], (int, float)) else "?"
            stake_s  = f"${p['stake']:.2f}" if isinstance(p["stake"], (int, float)) else "?"
            lines.append(
                f"  {p['bet']}{cap_flag}\n"
                f"    {p['event']} | odds={p['odds']} | model={model_s} market={market_s} "
                f"edge={edge_s} | stake={stake_s}{result_s}{clv_s}\n"
                f"    {_diag_highlight(p)}"
            )
        lines.append("")

    return "\n".join(lines)


def write_archive_json(archive: dict) -> str:
    """Write the archive to archives/YYYY-MM-DD.json. Returns the path."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    path = os.path.join(ARCHIVE_DIR, f"{archive['date']}.json")
    with open(path, "w") as f:
        json.dump(archive, f, indent=2, default=str)
    return path
