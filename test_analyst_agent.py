"""Tests for analyst_agent.py (Agent 2 -- THE ANALYST).

Run: python -m pytest test_analyst_agent.py -v
"""

import os
import json
from datetime import date, datetime, timedelta
from unittest.mock import patch, MagicMock

import pytest

import db
import analyst_agent as aa


@pytest.fixture(autouse=True)
def _isolated_env(tmp_path, monkeypatch):
    """Every test gets its own cwd (so agent_memory/ never touches the real
    repo) and its own SQLite file (so analyst_findings rows don't leak
    between tests or into the real parlay_os.db)."""
    monkeypatch.chdir(tmp_path)
    tmp_db = str(tmp_path / "analyst_test.db")
    with patch.object(db, "DB_PATH", tmp_db):
        db.init_db()
        yield db


def _log(d, bet="NYY ML", game="NYY @ BOS", bet_type="ML", date="2026-07-27",
         stake=25.0, over_cap=0, diagnostic_json=None, **overrides):
    kwargs = dict(
        date=date, bet=bet, bet_type=bet_type, game=game,
        sp="Cole", park="Fenway", umpire="Ump", bet_odds="-150",
        model_prob=0.62, market_prob=0.58, edge_pct=4.0,
        conviction="HIGH", stake=stake, over_cap=over_cap,
        diagnostic_json=diagnostic_json,
    )
    kwargs.update(overrides)
    d.log_bet(**kwargs)


class FakeUsage:
    def __init__(self, in_tok=1000, out_tok=300):
        self.input_tokens = in_tok
        self.output_tokens = out_tok


class FakeBlock:
    type = "text"

    def __init__(self, text):
        self.text = text


class FakeResponse:
    def __init__(self, text, in_tok=1000, out_tok=300):
        self.usage = FakeUsage(in_tok, out_tok)
        self.content = [FakeBlock(text)]


def _valid_claude_json():
    return json.dumps({
        "pick_analyses": [{"bet": "NYY ML", "analysis": "Cole cruised, model tracked well.",
                            "conviction_disconnect": False}],
        "answered_questions": [],
        "new_findings": [{"finding": "test finding", "evidence": "n=1", "confidence": "low",
                           "category": "system", "related_questions": []}],
        "new_questions": [{"question": "does this hold up over more games?", "category": "system",
                            "priority": 2, "what_to_check_tomorrow": "check again"}],
        "daily_summary": "Quiet day. One pick tracked well. No issues found.",
    })


# ── Memory read/write cycle ──────────────────────────────────────────────────

class TestKnowledgeBaseMemory:
    def test_starts_empty_and_creates_dir_structure(self):
        assert aa.load_knowledge_base() == []
        assert os.path.isdir("agent_memory/daily_debriefs")

    def test_append_is_additive_not_destructive(self):
        aa.append_knowledge_base_entries([{"date": "2026-07-26", "finding": "f1",
                                            "evidence": "e1", "confidence": "low",
                                            "category": "system", "status": "open",
                                            "related_questions": []}])
        aa.append_knowledge_base_entries([{"date": "2026-07-27", "finding": "f2",
                                            "evidence": "e2", "confidence": "medium",
                                            "category": "bullpen", "status": "open",
                                            "related_questions": []}])
        kb = aa.load_knowledge_base()
        assert [e["finding"] for e in kb] == ["f1", "f2"]

    def test_size_cap_trims_context_not_the_file(self):
        entries = [{"date": "d", "finding": f"f{i}", "evidence": "", "confidence": "low",
                    "category": "system", "status": "open", "related_questions": []}
                   for i in range(150)]
        entries[10]["confidence"] = "high"   # old, but high-confidence -- must survive
        aa.save_knowledge_base(entries)

        ctx = aa.get_knowledge_base_context()
        assert len(aa.load_knowledge_base()) == 150, "the on-disk file must stay append-only, never trimmed"
        assert entries[10] in ctx, "high-confidence entries survive regardless of age"
        assert entries[-1] in ctx, "most recent entries are always kept"
        assert entries[0] not in ctx, "old low-confidence entries are excluded from the prompt"

    def test_under_threshold_loads_everything(self):
        entries = [{"date": "d", "finding": f"f{i}", "evidence": "", "confidence": "low",
                    "category": "system", "status": "open", "related_questions": []}
                   for i in range(10)]
        aa.save_knowledge_base(entries)
        assert aa.get_knowledge_base_context() == entries


class TestOpenQuestionsMemory:
    def test_append_and_read_open_only(self):
        aa.append_open_questions([
            {"date_asked": "2026-07-27", "question": "q1", "category": "system",
             "priority": 3, "what_to_check_tomorrow": "check", "status": "open",
             "answer": None, "answer_date": None},
        ])
        assert [q["question"] for q in aa.get_open_questions_only()] == ["q1"]

    def test_answering_a_question_removes_it_from_open_but_keeps_it_in_file(self):
        aa.append_open_questions([
            {"date_asked": "2026-07-27", "question": "does X happen?", "category": "system",
             "priority": 3, "what_to_check_tomorrow": "check X", "status": "open",
             "answer": None, "answer_date": None},
        ])
        found = aa.mark_question_answered("does X happen?", "yes, confirmed", "2026-07-28")
        assert found is True
        assert aa.get_open_questions_only() == []
        all_qs = aa.load_open_questions()
        assert len(all_qs) == 1
        assert all_qs[0]["status"] == "answered"
        assert all_qs[0]["answer"] == "yes, confirmed"

    def test_answering_unknown_question_is_a_noop(self):
        found = aa.mark_question_answered("nonexistent question", "answer", "2026-07-28")
        assert found is False

    def test_unanswered_questions_roll_over(self):
        aa.append_open_questions([
            {"date_asked": "2026-07-26", "question": "q1", "category": "system",
             "priority": 3, "what_to_check_tomorrow": "c", "status": "open",
             "answer": None, "answer_date": None},
            {"date_asked": "2026-07-26", "question": "q2", "category": "system",
             "priority": 2, "what_to_check_tomorrow": "c", "status": "open",
             "answer": None, "answer_date": None},
        ])
        aa.mark_question_answered("q1", "done", "2026-07-27")
        remaining = [q["question"] for q in aa.get_open_questions_only()]
        assert remaining == ["q2"]


class TestDebriefFiles:
    def test_write_then_read_yesterday(self):
        yesterday = (aa._et_now().date() - timedelta(days=1))
        aa.write_debrief_file(yesterday, "# Debrief content")
        assert aa.load_yesterdays_debrief() == "# Debrief content"

    def test_no_debrief_file_returns_none(self):
        assert aa.load_yesterdays_debrief() is None

    def test_never_overwritten_means_one_file_per_date(self):
        aa.write_debrief_file(date(2026, 7, 20), "day 1")
        aa.write_debrief_file(date(2026, 7, 21), "day 2")
        assert os.path.exists("agent_memory/daily_debriefs/2026-07-20.md")
        assert os.path.exists("agent_memory/daily_debriefs/2026-07-21.md")


# ── Prompt assembly ───────────────────────────────────────────────────────────

class TestPromptAssembly:
    def test_get_todays_picks_includes_staked_and_over_cap_only(self, _isolated_env):
        _log(_isolated_env, bet="NYY ML", stake=25.0, over_cap=0)
        _log(_isolated_env, bet="BOS ML", stake=0.0, over_cap=1)
        _log(_isolated_env, bet="TB ML", stake=0.0, over_cap=0)  # neither staked nor over_cap
        picks = aa.get_todays_picks("2026-07-27")
        bets = {p["bet"] for p in picks}
        assert bets == {"NYY ML", "BOS ML"}

    def test_get_todays_picks_parses_diagnostics(self, _isolated_env):
        _log(_isolated_env, bet="NYY ML", diagnostic_json=json.dumps({"factors": [{"name": "sp"}]}))
        picks = aa.get_todays_picks("2026-07-27")
        assert picks[0]["diagnostics"] == {"factors": [{"name": "sp"}]}

    def test_build_user_content_includes_all_sections_with_realistic_data(self):
        picks = [{"bet": "NYY ML", "type": "ML", "result": "W", "clv_pct": 2.1}]
        kb_context = [{"finding": "prior finding", "confidence": "high"}]
        open_qs = [{"question": "open q1"}]
        content = aa._build_user_content(
            "2026-07-27", picks, kb_context, open_qs,
            "yesterday's debrief text", {"all_ok": True})
        assert "2026-07-27" in content
        assert "NYY ML" in content
        assert "prior finding" in content
        assert "open q1" in content
        assert "yesterday's debrief text" in content
        assert '"all_ok": true' in content

    def test_build_user_content_handles_empty_context_gracefully(self):
        content = aa._build_user_content("2026-07-27", [], [], [], None, None)
        assert "none yet" in content
        assert "none open" in content
        assert "no debrief file" in content
        assert "not available" in content


# ── Enable/disable toggle ─────────────────────────────────────────────────────

class TestToggle:
    def test_enabled_by_default(self, monkeypatch):
        monkeypatch.delenv("ANALYST_ENABLED", raising=False)
        assert aa.is_enabled() is True

    def test_disabled_via_env_var(self, monkeypatch):
        monkeypatch.setenv("ANALYST_ENABLED", "false")
        assert aa.is_enabled() is False

    def test_various_falsy_values(self, monkeypatch):
        for v in ("false", "False", "0", "no", "NO"):
            monkeypatch.setenv("ANALYST_ENABLED", v)
            assert aa.is_enabled() is False, v


# ── Critical safety test: no weight-change language ever survives ───────────

class TestWeightChangeSafety:
    @pytest.mark.parametrize("text", [
        "we should change the weight of the bullpen factor",
        "recommend adjusting the SP xwOBA threshold",
        "consider changing the threshold for MIN_EDGE",
        "increase the weight on offense to 15%",
        "the weights should be updated to reflect this",
        "I'd modify the bullpen fatigue weight going forward",
    ])
    def test_detects_weight_change_language(self, text):
        assert aa.contains_weight_change_language(text) is True

    @pytest.mark.parametrize("text", [
        "the SP had a rough outing, gave up 4 runs in 5 innings",
        "bullpen fatigue flagged correctly, model_p moved 1.2pp as expected",
        "this pick lost on a walk-off, nothing model-related",
        "",
    ])
    def test_does_not_false_positive_on_normal_analysis(self, text):
        assert aa.contains_weight_change_language(text) is False

    def test_sanitize_redacts_every_free_text_field(self):
        parsed = {
            "pick_analyses": [{"bet": "NYY ML",
                                "analysis": "we should increase the weight of bullpen fatigue",
                                "conviction_disconnect": False}],
            "answered_questions": [{"question": "q1",
                                     "answer": "yes -- change the threshold to fix it"}],
            "new_findings": [{"finding": "recommend changing the SP weight",
                               "evidence": "modify the offense weight too", "confidence": "low",
                               "category": "model"}],
            "new_questions": [{"question": "should we adjust the bullpen weight?",
                                "what_to_check_tomorrow": "adjust the threshold and see"}],
            "daily_summary": "Normal day.",
        }
        sanitized, violations = aa._sanitize_weight_change_language(parsed)
        assert len(violations) >= 5
        assert "REDACTED" in sanitized["pick_analyses"][0]["analysis"]
        assert "REDACTED" in sanitized["answered_questions"][0]["answer"]
        assert "REDACTED" in sanitized["new_findings"][0]["finding"]
        assert "REDACTED" in sanitized["new_findings"][0]["evidence"]
        assert "REDACTED" in sanitized["new_questions"][0]["question"]
        assert "REDACTED" in sanitized["new_questions"][0]["what_to_check_tomorrow"]
        assert sanitized["daily_summary"] == "Normal day."

    def test_clean_output_passes_through_unmodified(self):
        parsed = json.loads(_valid_claude_json())
        sanitized, violations = aa._sanitize_weight_change_language(parsed)
        assert violations == []
        assert sanitized["daily_summary"] == "Quiet day. One pick tracked well. No issues found."

    def test_end_to_end_run_never_lets_weight_change_language_reach_telegram_or_memory(self, _isolated_env):
        """The critical safety test (spec 2.6): if Claude's raw output contains
        weight-change language, it must never survive to Telegram or to the
        persisted knowledge base -- FAIL this test if it does."""
        _log(_isolated_env, bet="NYY ML", date="2026-07-27")
        adversarial_text = json.dumps({
            "pick_analyses": [],
            "answered_questions": [],
            "new_findings": [{
                "finding": "we should change the weight on the SP factor from 18% to 25%",
                "evidence": "n=1", "confidence": "high", "category": "model",
            }],
            "new_questions": [],
            "daily_summary": "Recommend adjusting the bullpen threshold immediately.",
        })

        sent_messages = []

        def _capture_send(text):
            sent_messages.append(text)
            return True

        with patch.object(aa, "ANTHROPIC_API_KEY", "fake-key"), \
             patch.object(aa, "_analysis_target_date", return_value="2026-07-27"), \
             patch("anthropic.Anthropic") as mock_anthropic_cls, \
             patch.object(aa, "_send_telegram", side_effect=_capture_send):
            mock_client = MagicMock()
            mock_client.messages.create.return_value = FakeResponse(adversarial_text)
            mock_anthropic_cls.return_value = mock_client

            result = aa.run_analyst_once()

        assert result["ok"] is True
        assert len(result["safety_violations"]) >= 1

        for msg in sent_messages:
            assert "change the weight" not in msg.lower()
            assert "adjusting the bullpen threshold" not in msg.lower()

        kb = aa.load_knowledge_base()
        assert len(kb) == 1
        assert "change the weight" not in kb[0]["finding"].lower()
        assert "REDACTED" in kb[0]["finding"]

        db_findings = db.get_analyst_findings()
        assert len(db_findings) == 1
        assert "change the weight" not in db_findings[0]["finding"].lower()


# ── Mocked Claude API call (no real tokens burned) ───────────────────────────

class TestMockedClaudeCall:
    def test_call_claude_parses_valid_json_response(self):
        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_client.messages.create.return_value = FakeResponse(_valid_claude_json(), 1234, 567)
            mock_cls.return_value = mock_client
            with patch.object(aa, "ANTHROPIC_API_KEY", "fake-key"):
                parsed, usage = aa._call_claude("some prompt content")
        assert parsed["daily_summary"] == "Quiet day. One pick tracked well. No issues found."
        assert usage == {"input_tokens": 1234, "output_tokens": 567}

    def test_call_claude_strips_markdown_fences(self):
        fenced = "```json\n" + _valid_claude_json() + "\n```"
        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_client.messages.create.return_value = FakeResponse(fenced)
            mock_cls.return_value = mock_client
            with patch.object(aa, "ANTHROPIC_API_KEY", "fake-key"):
                parsed, _ = aa._call_claude("prompt")
        assert parsed is not None
        assert "pick_analyses" in parsed

    def test_call_claude_returns_none_on_unparseable_response(self):
        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_client.messages.create.return_value = FakeResponse("not json at all")
            mock_cls.return_value = mock_client
            with patch.object(aa, "ANTHROPIC_API_KEY", "fake-key"):
                parsed, usage = aa._call_claude("prompt")
        assert parsed is None
        assert usage["input_tokens"] == 1000

    def test_call_claude_raises_without_api_key(self):
        with patch.object(aa, "ANTHROPIC_API_KEY", ""):
            with pytest.raises(RuntimeError):
                aa._call_claude("prompt")

    def test_uses_the_specified_model_and_capped_max_tokens(self):
        with patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_client.messages.create.return_value = FakeResponse(_valid_claude_json())
            mock_cls.return_value = mock_client
            with patch.object(aa, "ANTHROPIC_API_KEY", "fake-key"):
                aa._call_claude("prompt")
        _, kwargs = mock_client.messages.create.call_args
        assert kwargs["model"] == aa.CLAUDE_MODEL == "claude-sonnet-4-6"
        assert kwargs["max_tokens"] == 3000


class TestRunAnalystOnce:
    def test_skips_claude_call_when_no_picks(self, _isolated_env):
        with patch.object(aa, "_analysis_target_date", return_value="2026-07-27"), \
             patch("anthropic.Anthropic") as mock_cls:
            result = aa.run_analyst_once()
        assert result == {"ok": True, "skipped": True, "reason": "no picks", "date": "2026-07-27"}
        mock_cls.assert_not_called()

    def test_full_cycle_writes_memory_db_and_sends_telegram(self, _isolated_env):
        _log(_isolated_env, bet="NYY ML", date="2026-07-27")
        with patch.object(aa, "_analysis_target_date", return_value="2026-07-27"), \
             patch.object(aa, "ANTHROPIC_API_KEY", "fake-key"), \
             patch("anthropic.Anthropic") as mock_cls, \
             patch.object(aa, "_send_telegram", return_value=True) as mock_send:
            mock_client = MagicMock()
            mock_client.messages.create.return_value = FakeResponse(_valid_claude_json())
            mock_cls.return_value = mock_client
            result = aa.run_analyst_once()

        assert result["ok"] is True
        assert result["kb_entries_written"] == 1
        assert result["new_questions_written"] == 1
        assert result["safety_violations"] == []
        assert mock_send.called
        assert os.path.exists("agent_memory/daily_debriefs/2026-07-27.md")
        assert len(db.get_analyst_findings()) == 1

    def test_unparseable_response_does_not_crash_or_write_partial_state(self, _isolated_env):
        _log(_isolated_env, bet="NYY ML", date="2026-07-27")
        with patch.object(aa, "_analysis_target_date", return_value="2026-07-27"), \
             patch.object(aa, "ANTHROPIC_API_KEY", "fake-key"), \
             patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_client.messages.create.return_value = FakeResponse("garbage, not json")
            mock_cls.return_value = mock_client
            result = aa.run_analyst_once()
        assert result["ok"] is False
        assert aa.load_knowledge_base() == []
        assert db.get_analyst_findings() == []

    def test_api_exception_is_caught_and_reported(self, _isolated_env):
        _log(_isolated_env, bet="NYY ML", date="2026-07-27")
        with patch.object(aa, "_analysis_target_date", return_value="2026-07-27"), \
             patch.object(aa, "ANTHROPIC_API_KEY", "fake-key"), \
             patch("anthropic.Anthropic") as mock_cls:
            mock_client = MagicMock()
            mock_client.messages.create.side_effect = RuntimeError("API down")
            mock_cls.return_value = mock_client
            result = aa.run_analyst_once()
        assert result["ok"] is False
        assert "API down" in result["error"]


class TestDbFindingsTable:
    def test_log_and_get_analyst_findings(self, _isolated_env):
        db.log_analyst_finding(date="2026-07-27", finding="f1", evidence="e1",
                                confidence="high", category="bullpen",
                                related_questions=["q1"])
        rows = db.get_analyst_findings()
        assert len(rows) == 1
        assert rows[0]["finding"] == "f1"
        assert json.loads(rows[0]["related_questions"]) == ["q1"]
