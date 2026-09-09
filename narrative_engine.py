"""PARLAY OS — narrative_engine.py
Generates real, plain-English "Why" narratives for the Telegram slip using
Claude Haiku -- ONE API call per SLIP (not per pick), so ~13 picks/slip
costs well under a cent. Replaces the old template-only narrative text
(brain.py's _pick_narrative()), which still exists and is used verbatim
as the fallback whenever this module's call fails, times out, is skipped
(no API key), or a specific pick's narrative is missing/malformed in the
response.

Grounding: this module is only ever given each pick's already-distilled
factor breakdown (label/value/contribution/fallback-flag, computed by
brain.py's _pick_narrative()/_rank_ml_factors()/_game_bet_top_drivers())
plus its odds/probability context -- never the model's raw internal
diagnostics blob. The system prompt below additionally instructs Claude
to restate only what it's given; the deterministic template fallback (and
the always-rendered "Neutral fallbacks" line in brain.py's _play_block())
is the backstop if the model's prose ever doesn't hold that line.

SAFETY: generate_slip_narratives() never raises. Any failure returns {},
so every pick falls back to the deterministic template narrative -- a
slow, failed, or malformed LLM response must never delay or block the
slip send. The request itself carries a short client-side timeout
(REQUEST_TIMEOUT_SEC) for the same reason: even in the worst case this
can't eat into the scout's overall GH-Actions time budget (see brain.py's
run_daily_scout()'s _time_budget_exhausted()/A1 guard).
"""

import os
import json
import time

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL       = "claude-haiku-4-5-20251001"
MAX_TOKENS  = 4096
REQUEST_TIMEOUT_SEC = 20   # short on purpose -- see module docstring's SAFETY note

# Haiku 4.5 pricing (per Anthropic's published rates): $1.00/MTok in, $5.00/MTok out.
_PRICE_PER_INPUT_TOKEN  = 1.00 / 1_000_000
_PRICE_PER_OUTPUT_TOKEN = 5.00 / 1_000_000

_SYSTEM_PROMPT = """You are writing short betting-slip explanations for a sharp, experienced bettor -- not marketing copy.

You will be given a JSON list of picks. For each pick you receive ONLY:
- bet_type, selection, event, odds_str, model_p, market_p, edge_pct
- drivers: the model's own ranked list of factors that produced this pick, each
  with a label, a plain-language value, and (when applicable) its signed
  percentage-point contribution
- neutral_fallbacks: which of those drivers, if any, used a fallback/neutral
  value instead of real data

HARD GROUNDING RULE — read this twice:
- You may ONLY reference facts present in the drivers/context given for THAT
  pick. Never introduce a fact, stat, name, injury, weather note, or storyline
  that isn't in the data you were given.
- You are translating the model's own reasoning into plain English. You are
  not adding new analysis, you are not speculating beyond the given data, and
  you are not hyping the pick. Never use words like "lock", "smash", "easy
  money", "can't lose", "free money", "guaranteed", or similar hype language.
- If a driver's is_fallback is true and you use it, you MUST say plainly that
  it's a fallback/estimated value, not real data (e.g. "using a league-average
  fallback since real bullpen data wasn't available").
- If the drivers list is empty or very thin for a pick, keep the narrative
  short and honest about that instead of inventing detail to fill space.

FORMAT — for each pick return:
- "narrative": 2-4 sentences.
  1. Lead with the single biggest driver, in plain terms a bettor would say
     out loud -- not the raw stat.
  2. State the market vs. model gap in ODDS terms ("market has them at +172,
     the model sees it closer to +130"), not bare percentages, using the
     odds_str/model_p/market_p given for that pick.
  3. End with ONE sentence on what would change the call (an injury/lineup/
     weather/bullpen-usage scenario, etc.) -- but ONLY if something in the
     given data actually supports it; if nothing does, end on the case
     itself instead of inventing a hedge.
- "drivers": a short list of humanized one-line versions of the SAME driver
  data, e.g. "Bullpen: MIN late innings fatigued (+1.2pp)" instead of the raw
  "bullpen fatigue 1.5/10 vs 7.2/10". Same facts, plainer words, referencing
  team/player names from the pick's own event/selection where that helps
  readability. One line per driver you were given for that pick, same order.

Every pick is a different game with different real drivers -- do not write
generic, repetitive, or templated-sounding language across picks. Reflect
what's actually distinct about each one's OWN driver data; a thin-data pick
and a rich-data pick should read differently, and no two picks should share
boilerplate phrasing just because their bet_type matches.

Return ONLY pure JSON, no markdown fences, no commentary:
{"narratives": {"0": {"narrative": "...", "drivers": ["...", "..."]}, "1": {...}}}
Use the pick's 0-based index (as a string key) from the input list. Include an
entry for every pick you were given."""


def _build_user_content(picks: list) -> str:
    return "PICKS:\n" + json.dumps(picks, indent=2, default=str)


def _call_claude(user_content: str) -> tuple:
    """One Claude API call for the whole slip. Returns (parsed_dict_or_None, usage_dict)."""
    import anthropic
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=REQUEST_TIMEOUT_SEC)
    response = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    usage = {
        "input_tokens":  getattr(response.usage, "input_tokens", None),
        "output_tokens": getattr(response.usage, "output_tokens", None),
    }
    raw = "".join(b.text for b in response.content if b.type == "text")
    raw = raw.replace("```json", "").replace("```", "").strip()
    s, e = raw.find("{"), raw.rfind("}")
    if s == -1 or e == -1:
        return None, usage
    try:
        return json.loads(raw[s:e + 1]), usage
    except Exception:
        return None, usage


def generate_slip_narratives(picks: list) -> dict:
    """picks: list of {"index","bet_type","selection","event","odds_str",
    "model_p","market_p","edge_pct","drivers","neutral_fallbacks"} dicts,
    in slip display order -- the exact structured data brain.py's
    _pick_narrative() already computed, never raw internal diagnostics.
    ONE Claude Haiku call for the entire slip, not per pick.

    Returns {index: {"narrative": str, "drivers": [str,...]}} keyed by
    each pick's position in `picks`, present only for picks the model
    actually returned a usable entry for. Never raises -- any failure
    (no API key, timeout, network error, malformed JSON, an index missing
    from the response) is logged and yields {} or a partial dict; the
    caller (brain.py's _render_slip_picks()) falls back to the
    deterministic template narrative for any pick with no entry here."""
    if not picks:
        return {}
    if not ANTHROPIC_API_KEY:
        print("[NARRATIVE] ANTHROPIC_API_KEY not set — using template narratives for this slip")
        return {}

    start = time.monotonic()
    try:
        user_content = _build_user_content(picks)
        parsed, usage = _call_claude(user_content)
    except Exception as e:
        print(f"[NARRATIVE] Claude call failed ({time.monotonic() - start:.1f}s) — falling back to template narratives for all {len(picks)} pick(s): {e}")
        return {}
    elapsed = time.monotonic() - start

    in_tok  = usage.get("input_tokens")
    out_tok = usage.get("output_tokens")
    cost_str = ""
    if in_tok is not None and out_tok is not None:
        cost = in_tok * _PRICE_PER_INPUT_TOKEN + out_tok * _PRICE_PER_OUTPUT_TOKEN
        cost_str = f", est. cost=${cost:.5f}"
    print(
        f"[NARRATIVE] {len(picks)} pick(s), {elapsed:.1f}s, model={MODEL}, "
        f"input_tokens={in_tok} output_tokens={out_tok}{cost_str}"
    )

    if not parsed or "narratives" not in parsed:
        print(f"[NARRATIVE] Malformed/empty response — falling back to template narratives for all {len(picks)} pick(s)")
        return {}

    result: dict = {}
    raw_narratives = parsed.get("narratives") or {}
    for i in range(len(picks)):
        entry = raw_narratives.get(str(i))
        if not entry or not entry.get("narrative"):
            continue
        result[i] = {
            "narrative": str(entry["narrative"]).strip(),
            "drivers": [str(d).strip() for d in (entry.get("drivers") or []) if str(d).strip()],
        }

    missing = len(picks) - len(result)
    if missing:
        print(f"[NARRATIVE] {missing}/{len(picks)} pick(s) missing from LLM response — those fall back to template narratives")
    return result
