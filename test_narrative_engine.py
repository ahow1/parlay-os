"""Tests for narrative_engine.py -- the one-call-per-slip Claude Haiku
narrative generator that replaces the template "Why" text on the
Telegram slip, with a deterministic template fallback on any failure.

Run: python -m pytest test_narrative_engine.py -v
"""

import json
from unittest.mock import patch, MagicMock

import pytest

import narrative_engine as ne


def _fake_response(payload: dict, input_tokens=500, output_tokens=200):
    block = MagicMock(type="text", text=json.dumps(payload))
    resp = MagicMock()
    resp.content = [block]
    resp.usage = MagicMock(input_tokens=input_tokens, output_tokens=output_tokens)
    return resp


def _sample_picks(n=2):
    return [
        {
            "index": i, "bet_type": "ML", "selection": f"Team{i} ML",
            "event": f"Team{i} @ Opp{i}", "odds_str": "+150",
            "model_p": 0.55, "market_p": 0.48, "edge_pct": 7.0,
            "drivers": [{"label": "Bullpen", "value": "fatigue 1.5 vs 7.2", "edge_pct": 1.5, "is_fallback": False}],
            "neutral_fallbacks": [],
        }
        for i in range(n)
    ]


class TestNoOpPaths:
    def test_empty_picks_returns_empty_without_any_call(self):
        with patch("anthropic.Anthropic") as mock_client:
            result = ne.generate_slip_narratives([])
        assert result == {}
        mock_client.assert_not_called()

    def test_missing_api_key_returns_empty_without_any_call(self, monkeypatch, capsys):
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "")
        with patch("anthropic.Anthropic") as mock_client:
            result = ne.generate_slip_narratives(_sample_picks())
        assert result == {}
        mock_client.assert_not_called()
        assert "ANTHROPIC_API_KEY not set" in capsys.readouterr().out


class TestSuccessfulCall:
    def test_parses_narratives_keyed_by_index(self, monkeypatch):
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        payload = {
            "narratives": {
                "0": {"narrative": "Team0's bullpen is gassed after last night.", "drivers": ["Bullpen: gassed"]},
                "1": {"narrative": "Team1 has a clean pen advantage tonight.", "drivers": ["Bullpen: fresh"]},
            }
        }
        mock_instance = MagicMock()
        mock_instance.messages.create.return_value = _fake_response(payload)
        with patch("anthropic.Anthropic", return_value=mock_instance):
            result = ne.generate_slip_narratives(_sample_picks(2))
        assert result[0]["narrative"] == "Team0's bullpen is gassed after last night."
        assert result[0]["drivers"] == ["Bullpen: gassed"]
        assert result[1]["narrative"] == "Team1 has a clean pen advantage tonight."

    def test_uses_haiku_model_and_short_timeout(self, monkeypatch):
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        mock_instance = MagicMock()
        mock_instance.messages.create.return_value = _fake_response(
            {"narratives": {"0": {"narrative": "x", "drivers": []}}}
        )
        with patch("anthropic.Anthropic", return_value=mock_instance) as mock_ctor:
            ne.generate_slip_narratives(_sample_picks(1))
        assert mock_ctor.call_args.kwargs["api_key"] == "sk-test"
        assert mock_ctor.call_args.kwargs["timeout"] == ne.REQUEST_TIMEOUT_SEC
        create_kwargs = mock_instance.messages.create.call_args.kwargs
        assert create_kwargs["model"] == "claude-haiku-4-5-20251001"
        assert create_kwargs["system"] == ne._SYSTEM_PROMPT

    def test_makes_exactly_one_call_for_the_whole_slip(self, monkeypatch):
        """Cost control: N picks must still be exactly 1 API call, not N."""
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        payload = {"narratives": {str(i): {"narrative": f"n{i}", "drivers": []} for i in range(13)}}
        mock_instance = MagicMock()
        mock_instance.messages.create.return_value = _fake_response(payload)
        with patch("anthropic.Anthropic", return_value=mock_instance):
            result = ne.generate_slip_narratives(_sample_picks(13))
        assert mock_instance.messages.create.call_count == 1
        assert len(result) == 13

    def test_logs_token_usage_and_estimated_cost(self, monkeypatch, capsys):
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        mock_instance = MagicMock()
        mock_instance.messages.create.return_value = _fake_response(
            {"narratives": {"0": {"narrative": "x", "drivers": []}}},
            input_tokens=1000, output_tokens=500,
        )
        with patch("anthropic.Anthropic", return_value=mock_instance):
            ne.generate_slip_narratives(_sample_picks(1))
        out = capsys.readouterr().out
        assert "input_tokens=1000" in out
        assert "output_tokens=500" in out
        # 1000 * $1/1M + 500 * $5/1M = $0.001 + $0.0025 = $0.0035
        assert "0.0035" in out

    def test_cost_scales_correctly_with_realistic_token_counts(self, monkeypatch, capsys):
        """Arithmetic check with token counts in a realistic range for a
        13-pick slip (see test_narrative_engine_cost.py for the actual
        cost estimate against the real system prompt + a realistic
        payload, which is the real "confirm ~13 picks/slip is well under
        a cent" evidence -- this test only checks the pricing math)."""
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        payload = {"narratives": {str(i): {"narrative": f"n{i}", "drivers": []} for i in range(13)}}
        mock_instance = MagicMock()
        mock_instance.messages.create.return_value = _fake_response(payload, input_tokens=2000, output_tokens=1000)
        with patch("anthropic.Anthropic", return_value=mock_instance):
            ne.generate_slip_narratives(_sample_picks(13))
        out = capsys.readouterr().out
        cost_line = next(l for l in out.split("\n") if "est. cost=" in l)
        cost = float(cost_line.split("est. cost=$")[1])
        # 2000 * $1/1M + 1000 * $5/1M = $0.002 + $0.005 = $0.007
        assert cost == pytest.approx(0.007, abs=1e-6)


class TestFailureFallsBackSafely:
    def test_api_exception_returns_empty_and_logs(self, monkeypatch, capsys):
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        mock_instance = MagicMock()
        mock_instance.messages.create.side_effect = ConnectionError("network down")
        with patch("anthropic.Anthropic", return_value=mock_instance):
            result = ne.generate_slip_narratives(_sample_picks(2))
        assert result == {}
        out = capsys.readouterr().out
        assert "falling back to template narratives" in out
        assert "network down" in out

    def test_malformed_json_returns_empty_and_logs(self, monkeypatch, capsys):
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        mock_instance = MagicMock()
        block = MagicMock(type="text", text="not json at all, no braces")
        resp = MagicMock(content=[block], usage=MagicMock(input_tokens=10, output_tokens=5))
        mock_instance.messages.create.return_value = resp
        with patch("anthropic.Anthropic", return_value=mock_instance):
            result = ne.generate_slip_narratives(_sample_picks(1))
        assert result == {}
        assert "Malformed/empty response" in capsys.readouterr().out

    def test_missing_narratives_key_returns_empty(self, monkeypatch):
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        mock_instance = MagicMock()
        mock_instance.messages.create.return_value = _fake_response({"something_else": True})
        with patch("anthropic.Anthropic", return_value=mock_instance):
            result = ne.generate_slip_narratives(_sample_picks(1))
        assert result == {}

    def test_partial_response_falls_back_only_for_missing_indices(self, monkeypatch, capsys):
        """3 picks sent, LLM only returns pick 1 -- picks 0 and 2 must be
        absent from the result (caller falls back to template for those),
        pick 1 must be present."""
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        mock_instance = MagicMock()
        mock_instance.messages.create.return_value = _fake_response(
            {"narratives": {"1": {"narrative": "only this one", "drivers": []}}}
        )
        with patch("anthropic.Anthropic", return_value=mock_instance):
            result = ne.generate_slip_narratives(_sample_picks(3))
        assert 0 not in result
        assert 1 in result and result[1]["narrative"] == "only this one"
        assert 2 not in result
        assert "2/3 pick(s) missing from LLM response" in capsys.readouterr().out

    def test_entry_with_empty_narrative_is_skipped(self, monkeypatch):
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        mock_instance = MagicMock()
        mock_instance.messages.create.return_value = _fake_response(
            {"narratives": {"0": {"narrative": "", "drivers": []}}}
        )
        with patch("anthropic.Anthropic", return_value=mock_instance):
            result = ne.generate_slip_narratives(_sample_picks(1))
        assert result == {}

    def test_never_raises_even_on_totally_unexpected_response_shape(self, monkeypatch):
        monkeypatch.setattr(ne, "ANTHROPIC_API_KEY", "sk-test")
        mock_instance = MagicMock()
        mock_instance.messages.create.return_value = "not a real response object"
        with patch("anthropic.Anthropic", return_value=mock_instance):
            result = ne.generate_slip_narratives(_sample_picks(1))  # must not raise
        assert result == {}


class TestGroundingPromptContract:
    """The system prompt is the enforcement mechanism for the hard
    grounding rule -- assert the load-bearing constraints are actually in
    it, so a future edit can't silently drop them."""

    def test_prompt_forbids_hype_language(self):
        assert "lock" in ne._SYSTEM_PROMPT.lower()
        assert "hype" in ne._SYSTEM_PROMPT.lower()

    def test_prompt_requires_grounding_to_given_data_only(self):
        assert "ONLY reference facts present" in ne._SYSTEM_PROMPT

    def test_prompt_requires_disclosing_fallback_usage(self):
        assert "is_fallback" in ne._SYSTEM_PROMPT
        assert "fallback" in ne._SYSTEM_PROMPT.lower()

    def test_prompt_requires_odds_terms_not_bare_percentages(self):
        assert "ODDS terms" in ne._SYSTEM_PROMPT

    def test_prompt_requires_a_whatwouldchange_closer(self):
        assert "what would change the call" in ne._SYSTEM_PROMPT.lower()

    def test_prompt_requires_per_pick_differentiation(self):
        assert "different" in ne._SYSTEM_PROMPT.lower()
        assert "generic" in ne._SYSTEM_PROMPT.lower() or "repetitive" in ne._SYSTEM_PROMPT.lower()
