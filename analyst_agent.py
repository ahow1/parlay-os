"""PARLAY OS — analyst_agent.py
Agent 2 (THE ANALYST): a daily, Claude-powered reflection agent. Once per day
(after the slate has settled) it reads its own prior findings and open
questions, reads today's picks with results/CLV/diagnostics, makes exactly
ONE Claude API call to analyze what happened, and writes its conclusions to a
local append-only memory (agent_memory/), a mirrored analyst_findings DB
table, and a condensed Telegram summary.

The Analyst OBSERVES and BUILDS EVIDENCE. It never recommends or makes model
weight/threshold changes — that stays out of scope until a future Validation
agent (not yet built) reviews accumulated findings with a real sample size.
A defense-in-depth filter (contains_weight_change_language / see run_analyst_once)
redacts and logs anything that reads like a weight-change recommendation
regardless of what the system prompt says, so this constraint doesn't depend
solely on Claude following instructions.

Toggle with ANALYST_ENABLED=false (default true).

Usage:
    python brain.py --debrief-agent       # one-shot CLI mode
    import analyst_agent
    analyst_agent.run_analyst_once()          # one full cycle, for manual/testing use
    analyst_agent.run_analyst_daily_loop()     # blocking, fires once/day at 1:30am ET
"""

import os
import re
import json
import threading
from datetime import datetime, timedelta, date

import pytz
import requests

import db as _db
import error_logger

ET = pytz.timezone("America/New_York")

MEMORY_DIR          = "agent_memory"
KNOWLEDGE_BASE_FILE = os.path.join(MEMORY_DIR, "knowledge_base.json")
OPEN_QUESTIONS_FILE = os.path.join(MEMORY_DIR, "open_questions.json")
DEBRIEFS_DIR        = os.path.join(MEMORY_DIR, "daily_debriefs")

# The user's spec names this exact model string twice. It is a real, current,
# non-deprecated model ID ($3/$15 per MTok, 1M context) — kept as specified
# rather than substituted, since the user explicitly named it by exact string.
CLAUDE_MODEL = "claude-sonnet-4-6"
MAX_TOKENS   = 3000

# Cost control (spec 2.3): once the on-disk knowledge base passes this many
# entries, only feed the most recent N + every high-confidence entry
# (regardless of age) into the prompt. The FILE itself is never trimmed —
# knowledge_base.json stays append-only forever; this only bounds what goes
# into each day's Claude call.
KB_CONTEXT_CAP_THRESHOLD = 100
KB_CONTEXT_RECENT_KEEP   = 50

BOT_TOKEN         = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALERT_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

DAILY_RUN_HOUR_ET   = 1    # 1:30am ET — after Railway's last settle pass so
DAILY_RUN_MINUTE_ET = 30   # results/CLV for the slate that just finished are final
LOOP_POLL_SEC        = 900  # 15 min — matches the Monitor's cadence


def is_enabled() -> bool:
    return os.getenv("ANALYST_ENABLED", "true").strip().lower() not in ("false", "0", "no")


def _et_now() -> datetime:
    return datetime.now(ET)


def _ensure_memory_dir() -> None:
    os.makedirs(DEBRIEFS_DIR, exist_ok=True)


# ── Memory: knowledge_base.json (findings — append-only, never edited) ──────

def load_knowledge_base() -> list:
    _ensure_memory_dir()
    if not os.path.exists(KNOWLEDGE_BASE_FILE):
        return []
    try:
        with open(KNOWLEDGE_BASE_FILE) as f:
            return json.load(f)
    except Exception as e:
        error_logger.log_error("analyst_agent.load_knowledge_base", e)
        return []


def save_knowledge_base(entries: list) -> None:
    _ensure_memory_dir()
    with open(KNOWLEDGE_BASE_FILE, "w") as f:
        json.dump(entries, f, indent=2)


def append_knowledge_base_entries(new_entries: list) -> list:
    """Append-only: existing entries are never edited or removed here."""
    entries = load_knowledge_base()
    entries.extend(new_entries)
    save_knowledge_base(entries)
    return entries


def get_knowledge_base_context() -> list:
    """What actually goes into the daily prompt. Below the cap threshold,
    every entry goes in. Above it, only the most recent KB_CONTEXT_RECENT_KEEP
    plus any high-confidence entry regardless of age (deduped, original order
    preserved)."""
    entries = load_knowledge_base()
    if len(entries) <= KB_CONTEXT_CAP_THRESHOLD:
        return entries
    recent = entries[-KB_CONTEXT_RECENT_KEEP:]
    high_conf = [e for e in entries if (e.get("confidence") or "").lower() == "high"]
    out, seen = [], set()
    for e in recent + high_conf:
        marker = json.dumps(e, sort_keys=True)
        if marker not in seen:
            seen.add(marker)
            out.append(e)
    return out


# ── Memory: open_questions.json (mutated in place: open -> answered) ────────

def load_open_questions() -> list:
    _ensure_memory_dir()
    if not os.path.exists(OPEN_QUESTIONS_FILE):
        return []
    try:
        with open(OPEN_QUESTIONS_FILE) as f:
            return json.load(f)
    except Exception as e:
        error_logger.log_error("analyst_agent.load_open_questions", e)
        return []


def save_open_questions(entries: list) -> None:
    _ensure_memory_dir()
    with open(OPEN_QUESTIONS_FILE, "w") as f:
        json.dump(entries, f, indent=2)


def append_open_questions(new_questions: list) -> list:
    entries = load_open_questions()
    entries.extend(new_questions)
    save_open_questions(entries)
    return entries


def get_open_questions_only() -> list:
    """status == 'open' questions — what goes into the prompt. Answered
    questions stay in the file (audit trail) but don't need to be re-asked."""
    return [q for q in load_open_questions() if q.get("status", "open") == "open"]


def mark_question_answered(question_text: str, answer: str, answer_date: str) -> bool:
    """Find the first open question matching this text (case-insensitive,
    exact) and mark it answered in place. Returns True if a match was found."""
    entries = load_open_questions()
    target = (question_text or "").strip().lower()
    if not target:
        return False
    for q in entries:
        if q.get("status", "open") == "open" and q.get("question", "").strip().lower() == target:
            q["status"] = "answered"
            q["answer"] = answer
            q["answer_date"] = answer_date
            save_open_questions(entries)
            return True
    return False


# ── Memory: daily_debriefs/YYYY-MM-DD.md ─────────────────────────────────────

def debrief_path_for(d: date) -> str:
    return os.path.join(DEBRIEFS_DIR, f"{d.isoformat()}.md")


def load_yesterdays_debrief() -> str | None:
    yesterday = (_et_now().date() - timedelta(days=1))
    path = debrief_path_for(yesterday)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return f.read()
    except Exception as e:
        error_logger.log_error("analyst_agent.load_yesterdays_debrief", e)
        return None


def write_debrief_file(d: date, content: str) -> None:
    _ensure_memory_dir()
    with open(debrief_path_for(d), "w") as f:
        f.write(content)


# ── Today's data ─────────────────────────────────────────────────────────────

def _analysis_target_date() -> str:
    """Which betting-day's picks the Analyst reviews. Bets are dated by
    ET-calendar-day at scout time (same convention brain.py's _run_debrief
    uses). The Analyst deliberately runs at 1:30am ET — already the next ET
    calendar day relative to the slate that just finished — so 'today' in ET
    terms would be empty; 'yesterday' is the slate whose results just became
    final."""
    return (_et_now().date() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_todays_picks(target_date: str) -> list:
    """Every pick that reached a human decision point that day — staked
    (stake>0) or over_cap (qualified but capped) — with diagnostics and
    results/CLV where already available. Pending (unsettled) bets are
    included with result=None; the Analyst evaluates what's known so far."""
    bets = _db.get_bets(date=target_date)
    picks = [b for b in bets if (b.get("stake") or 0) > 0 or b.get("over_cap")]
    clv_by_bet = {c["bet"]: c for c in _db.get_clv_log(days=2) if c.get("date") == target_date}

    out = []
    for b in picks:
        diag = None
        if b.get("diagnostic_json"):
            try:
                diag = json.loads(b["diagnostic_json"])
            except Exception:
                diag = None
        out.append({
            "bet": b.get("bet"),
            "type": b.get("type"),
            "game": b.get("game"),
            "stake": b.get("stake"),
            "over_cap": bool(b.get("over_cap")),
            "conviction": b.get("conviction"),
            "edge_pct": b.get("edge_pct"),
            "model_prob": b.get("model_prob"),
            "market_prob": b.get("market_prob"),
            "result": b.get("result"),
            "profit": b.get("profit"),
            "clv_pct": clv_by_bet.get(b.get("bet"), {}).get("clv_pct"),
            "diagnostics": diag,
        })
    return out


def get_monitor_health_context() -> dict | None:
    """Agent 1's last known state, if the Monitor module is importable and
    has run at least once. Optional context — never blocks the Analyst."""
    try:
        import monitor_agent
        status = monitor_agent.get_monitor_status()
        if status.get("status") == "not yet run":
            return None
        return status
    except Exception:
        return None


# ── Safety filter — defense in depth beyond the system prompt ───────────────

_WEIGHT_VERB = r"(chang|adjust|increas|decreas|modify|updat|tweak|lower|raise)\w*"
_FILLER      = r"(the\s+)?(\w+\s+)?(\w+\s+)?"  # up to two descriptor words, e.g. "the bullpen fatigue"

_WEIGHT_CHANGE_PATTERNS = [re.compile(p, re.IGNORECASE) for p in [
    rf"\b{_WEIGHT_VERB}\s+{_FILLER}weights?\b",
    rf"\b{_WEIGHT_VERB}\s+{_FILLER}threshold\b",
    r"\bweights?\s+should\s+be\s+(changed|adjusted|increased|decreased|updated)",
    r"\bthresholds?\s+should\s+be\s+(changed|adjusted|increased|decreased|updated)",
    r"\brecommend\w*\s+(changing|adjusting|updating)\s+.*(weight|threshold)",
    r"\bset\s+min_edge",
]]


def contains_weight_change_language(text: str) -> bool:
    if not text:
        return False
    return any(p.search(text) for p in _WEIGHT_CHANGE_PATTERNS)


def _sanitize_weight_change_language(parsed: dict) -> tuple:
    """Scan every free-text field Claude returned and redact anything that
    reads like a weight/threshold change recommendation. The Analyst is only
    ever allowed to observe and flag — a human, or a future Validation agent,
    decides on changes. Returns (parsed, list_of_violation_descriptions)."""
    violations = []

    def _check(text, where):
        if contains_weight_change_language(text):
            violations.append(f"{where}: {text!r}")
            return ("[REDACTED — flagged as a weight/threshold change "
                     "recommendation, which the Analyst is not permitted to make]")
        return text

    for pa in parsed.get("pick_analyses") or []:
        pa["analysis"] = _check(pa.get("analysis", ""), f"pick_analyses[{pa.get('bet')}]")
    for aq in parsed.get("answered_questions") or []:
        aq["answer"] = _check(aq.get("answer", ""), f"answered_questions[{aq.get('question')}]")
    for nf in parsed.get("new_findings") or []:
        nf["finding"] = _check(nf.get("finding", ""), "new_findings.finding")
        nf["evidence"] = _check(nf.get("evidence", ""), "new_findings.evidence")
    for nq in parsed.get("new_questions") or []:
        nq["question"] = _check(nq.get("question", ""), "new_questions.question")
        nq["what_to_check_tomorrow"] = _check(nq.get("what_to_check_tomorrow", ""), "new_questions.what_to_check_tomorrow")
    parsed["daily_summary"] = _check(parsed.get("daily_summary", ""), "daily_summary")

    if violations:
        error_logger.log_error(
            "analyst_agent.safety_filter",
            Exception("weight/threshold-change language detected in Analyst output and redacted"),
            extra="; ".join(violations),
        )
    return parsed, violations


# ── Prompt assembly + the one Claude API call ────────────────────────────────

_SYSTEM_PROMPT = """You are the Analyst for Parlay OS, a live MLB betting model that
recommends real-money bets to a single user (Aidan). You run once per day, after
that day's slate has settled, and your job is to build a slow, careful,
evidence-based understanding of what's working and what isn't — nothing more.

Do the following, in order:

SECTION 1 — Pick-by-pick analysis. For each pick given to you, write 2-3 sentences
explaining what happened using the diagnostic factors provided (SP, bullpen,
offense, weather, etc.) and the actual result/CLV. Explicitly flag any pick where
the model's conviction level looks disconnected from the quality of its underlying
factors — e.g. a HIGH-conviction pick built mostly on neutral-fallback data.

SECTION 2 — Answer your own open questions honestly using today's data, including
"not enough data yet" when that's the truth. Don't force an answer you don't have
evidence for.

SECTION 3 — New findings, each with a confidence rating (low/medium/high). Be
conservative: one day of data is noise. Reserve "high" confidence for findings
that already have several days of consistent supporting evidence in your prior
findings below — a pattern seen today for the first time is "low" almost by
definition.

SECTION 4 — Write 1 to 3 new, specific, data-answerable questions for tomorrow's
run to investigate. Not vague ("is the model good?") — specific and checkable
("do TIRED-bullpen picks underperform their model_prob more than fresh-bullpen
picks over the next 10 games?").

SECTION 5 — A 3-sentence daily summary of the day, for a human skimming Telegram.

CRITICAL RULES (never break these):
- Never recommend changing model weights or thresholds, in any section, in any
  wording. You OBSERVE and BUILD EVIDENCE ONLY. Changes to the model happen only
  after a future Validation agent (not yet built) reviews your accumulated
  findings with a large enough sample size — that is not your job.
- Your memory is append-only. If you now think a past finding was wrong, say so
  as a NEW finding that references it — never claim to edit or retract a prior
  one.
- Never fabricate numbers you weren't given. If you don't have enough data to
  support a claim, say so instead of guessing.

Respond with ONLY a single JSON object, no prose before or after, no markdown
code fences, matching exactly this shape:

{
  "pick_analyses": [
    {"bet": "<exact bet text from input>", "analysis": "<2-3 sentences>", "conviction_disconnect": true or false}
  ],
  "answered_questions": [
    {"question": "<exact question text from the open questions given to you>", "answer": "<honest answer, or 'not enough data yet'>"}
  ],
  "new_findings": [
    {"finding": "<specific claim>", "evidence": "<what data supports it>", "confidence": "low" or "medium" or "high", "category": "SP" or "bullpen" or "offense" or "weather" or "props" or "market" or "model" or "system", "related_questions": ["<optional question text this relates to>"]}
  ],
  "new_questions": [
    {"question": "<specific, checkable question>", "category": "SP" or "bullpen" or "offense" or "weather" or "props" or "market" or "model" or "system", "priority": 1 to 5, "what_to_check_tomorrow": "<what data to pull>"}
  ],
  "daily_summary": "<exactly 3 sentences>"
}

Use empty lists for sections with nothing to report — do not omit keys, and do
not invent a finding just to fill the field."""


def _build_user_content(target_date, picks, kb_context, open_qs, yesterday_debrief, monitor_health) -> str:
    parts = [f"Betting day under review: {target_date}", ""]
    parts.append(f"TODAY'S PICKS ({len(picks)}):")
    parts.append(json.dumps(picks, indent=2, default=str))
    parts.append("")
    parts.append(f"YOUR PRIOR FINDINGS ({len(kb_context)} shown — recent + all high-confidence):")
    parts.append(json.dumps(kb_context, indent=2, default=str) if kb_context else "(none yet — this may be an early run)")
    parts.append("")
    parts.append(f"YOUR OPEN QUESTIONS ({len(open_qs)}):")
    parts.append(json.dumps(open_qs, indent=2, default=str) if open_qs else "(none open)")
    parts.append("")
    parts.append("YESTERDAY'S DEBRIEF:")
    parts.append(yesterday_debrief if yesterday_debrief else "(no debrief file for yesterday)")
    parts.append("")
    parts.append("SYSTEM MONITOR HEALTH (Agent 1, if available):")
    parts.append(json.dumps(monitor_health, indent=2, default=str) if monitor_health else "(not available)")
    return "\n".join(parts)


def _call_claude(user_content: str) -> tuple:
    """The ONE Claude API call per day. Returns (parsed_json_or_None, usage_dict)."""
    import anthropic
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=MAX_TOKENS,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    usage = {
        "input_tokens": getattr(response.usage, "input_tokens", None),
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


# ── Writing outputs ───────────────────────────────────────────────────────────

def _write_outputs(target_date: str, parsed: dict) -> dict:
    new_findings = parsed.get("new_findings") or []
    kb_entries = [{
        "date": target_date,
        "finding": nf.get("finding", ""),
        "evidence": nf.get("evidence", ""),
        "confidence": nf.get("confidence", "low"),
        "category": nf.get("category", "system"),
        "status": "open",
        "related_questions": nf.get("related_questions", []),
    } for nf in new_findings]

    if kb_entries:
        append_knowledge_base_entries(kb_entries)
        for e in kb_entries:
            try:
                _db.log_analyst_finding(
                    date=e["date"], finding=e["finding"], evidence=e["evidence"],
                    confidence=e["confidence"], category=e["category"],
                    status=e["status"], related_questions=e["related_questions"],
                )
            except Exception as exc:
                error_logger.log_error("analyst_agent.log_analyst_finding_db", exc)

    answered_count = 0
    for aq in parsed.get("answered_questions") or []:
        if mark_question_answered(aq.get("question", ""), aq.get("answer", ""), target_date):
            answered_count += 1

    new_questions = parsed.get("new_questions") or []
    if new_questions:
        append_open_questions([{
            "date_asked": target_date,
            "question": nq.get("question", ""),
            "category": nq.get("category", "system"),
            "priority": nq.get("priority", 3),
            "what_to_check_tomorrow": nq.get("what_to_check_tomorrow", ""),
            "status": "open",
            "answer": None,
            "answer_date": None,
        } for nq in new_questions])

    return {
        "kb_entries_written": len(kb_entries),
        "questions_answered": answered_count,
        "new_questions_written": len(new_questions),
    }


def _render_debrief_markdown(target_date, picks, parsed, usage) -> str:
    lines = [f"# Analyst Debrief — {target_date}", ""]
    lines.append("## Pick-by-pick analysis")
    for pa in parsed.get("pick_analyses") or []:
        flag = " ⚠️ conviction/factor disconnect" if pa.get("conviction_disconnect") else ""
        lines.append(f"- **{pa.get('bet', '?')}**{flag}: {pa.get('analysis', '')}")
    lines.append("")
    lines.append("## Answered questions")
    for aq in parsed.get("answered_questions") or []:
        lines.append(f"- Q: {aq.get('question', '')}\n  A: {aq.get('answer', '')}")
    lines.append("")
    lines.append("## New findings")
    for nf in parsed.get("new_findings") or []:
        lines.append(f"- [{(nf.get('confidence') or '?').upper()}] ({nf.get('category', '?')}) "
                      f"{nf.get('finding', '')} — {nf.get('evidence', '')}")
    lines.append("")
    lines.append("## New questions for tomorrow")
    for nq in parsed.get("new_questions") or []:
        lines.append(f"- (P{nq.get('priority', '?')}, {nq.get('category', '?')}) "
                      f"{nq.get('question', '')} — check: {nq.get('what_to_check_tomorrow', '')}")
    lines.append("")
    lines.append("## Summary")
    lines.append(parsed.get("daily_summary", ""))
    lines.append("")
    lines.append("---")
    lines.append(f"Model: {CLAUDE_MODEL} | Input tokens: {usage.get('input_tokens')} | "
                  f"Output tokens: {usage.get('output_tokens')} | Picks analyzed: {len(picks)}")
    return "\n".join(lines)


def _render_telegram_summary(target_date, parsed, kb_written, new_q_written) -> str:
    high_conf = [nf for nf in (parsed.get("new_findings") or [])
                 if (nf.get("confidence") or "").lower() == "high"]
    lines = [f"🔬 ANALYST DEBRIEF — {target_date}", ""]
    lines.append((parsed.get("daily_summary") or "").strip())
    if high_conf:
        lines.append("")
        lines.append("High-confidence findings:")
        for nf in high_conf[:5]:
            lines.append(f"• {nf.get('finding', '')}")
    answered = parsed.get("answered_questions") or []
    if answered:
        lines.append("")
        lines.append(f"Answered {len(answered)} open question(s).")
    new_qs = parsed.get("new_questions") or []
    if new_qs:
        lines.append("")
        lines.append(f"New question(s) for tomorrow ({len(new_qs)}):")
        for nq in new_qs[:3]:
            lines.append(f"• {nq.get('question', '')}")
    return "\n".join(lines)


def _send_telegram(text: str) -> bool:
    if not BOT_TOKEN or not ALERT_CHAT_ID:
        print(f"[ANALYST] (no Telegram configured)\n{text}")
        return False
    text = text.replace("<", "&lt;").replace(">", "&gt;")
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": ALERT_CHAT_ID, "text": text},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception as e:
        print(f"[ANALYST] telegram send failed: {e}")
        return False


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_analyst_once() -> dict:
    """One full cycle: read prior context + today's data, one Claude call,
    write memory + DB + debrief file, send a condensed Telegram summary.
    Never raises — caller (CLI or scheduled loop) gets a result dict either
    way, with ok=False and an error string on failure."""
    target_date = _analysis_target_date()
    try:
        picks = get_todays_picks(target_date)
        if not picks:
            print(f"[ANALYST] no staked/over_cap picks for {target_date} — skipping Claude call")
            return {"ok": True, "skipped": True, "reason": "no picks", "date": target_date}

        kb_context = get_knowledge_base_context()
        open_qs = get_open_questions_only()
        yesterday_debrief = load_yesterdays_debrief()
        monitor_health = get_monitor_health_context()

        user_content = _build_user_content(
            target_date, picks, kb_context, open_qs, yesterday_debrief, monitor_health)
        parsed, usage = _call_claude(user_content)
        print(f"[ANALYST] Claude call: {usage.get('input_tokens')} in / {usage.get('output_tokens')} out tokens")

        if parsed is None:
            error_logger.log_error(
                "analyst_agent.run_analyst_once",
                Exception("Claude response did not parse as JSON"))
            return {"ok": False, "error": "unparseable Claude response", "date": target_date, "usage": usage}

        parsed, violations = _sanitize_weight_change_language(parsed)
        write_result = _write_outputs(target_date, parsed)

        debrief_md = _render_debrief_markdown(target_date, picks, parsed, usage)
        write_debrief_file(datetime.strptime(target_date, "%Y-%m-%d").date(), debrief_md)

        summary_text = _render_telegram_summary(
            target_date, parsed, write_result["kb_entries_written"], write_result["new_questions_written"])
        _send_telegram(summary_text)

        return {
            "ok": True,
            "date": target_date,
            "usage": usage,
            "kb_entries_written": write_result["kb_entries_written"],
            "questions_answered": write_result["questions_answered"],
            "new_questions_written": write_result["new_questions_written"],
            "safety_violations": violations,
        }
    except Exception as e:
        error_logger.log_error("analyst_agent.run_analyst_once", e)
        return {"ok": False, "error": str(e), "date": target_date}


def run_analyst_daily_loop(stop_event=None) -> None:
    """Background loop: fires run_analyst_once() once per ET calendar day at
    DAILY_RUN_HOUR_ET:DAILY_RUN_MINUTE_ET (1:30am ET — after Railway's last
    settle pass so results/CLV are final). Meant to run as a daemon thread
    started by brain.py in --bot mode."""
    _stop = stop_event or threading.Event()
    print("[ANALYST] loop started")
    last_run_date = None
    while not _stop.is_set():
        try:
            now = _et_now()
            due = (now.hour, now.minute) >= (DAILY_RUN_HOUR_ET, DAILY_RUN_MINUTE_ET)
            today_str = now.strftime("%Y-%m-%d")
            if due and last_run_date != today_str:
                result = run_analyst_once()
                print(f"[ANALYST] daily run result: {result}")
                last_run_date = today_str
        except Exception as e:
            print(f"[ANALYST] loop error: {e}")
        _stop.wait(LOOP_POLL_SEC)


if __name__ == "__main__":
    import pprint
    pprint.pprint(run_analyst_once())
