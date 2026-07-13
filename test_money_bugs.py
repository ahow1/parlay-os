"""Tests for TIER 1 money-bug audit fixes (AUDIT.md B1, B2, B10, B3, B4).
Run: python -m pytest test_money_bugs.py -v
"""

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest


# ── B1: K-prop fabricated math ────────────────────────────────────────────────

class TestKPropRealModelP:
    """B1: analyze_k_prop() must return a real model_p (Poisson-derived), and
    edge_pct must come from that probability vs. a market baseline — not
    gap (a strikeout-count differential) multiplied by 10."""

    def _sp_stats(self, k9=10.0):
        # pitcher_id=None avoids any live Savant HTTP call in get_pitcher_whiff_rate
        return {
            "name": "Test SP", "pitcher_id": None,
            "k9": k9, "ip": 100, "gs": 15, "ttop": True,
            "hand": "R", "rolling_k9_3": None,
        }

    def test_returns_real_model_p_not_hardcoded_fallback(self):
        """model_p must be a real probability in (0,1), not the 0.55 brain.py
        used to silently fall back to."""
        from strikeout_engine import analyze_k_prop
        result = analyze_k_prop(self._sp_stats(), None, "XXX", market_line=6.0)
        assert result is not None
        assert "model_p" in result
        assert 0.0 < result["model_p"] < 1.0

    def test_model_p_favors_over_when_projection_beats_line(self):
        """Large projected-K surplus over the line -> model_p for OVER > 0.5."""
        from strikeout_engine import analyze_k_prop
        result = analyze_k_prop(self._sp_stats(), None, "XXX", market_line=6.0)
        assert result["direction"] == "OVER"
        assert result["model_p"] > 0.5

    def test_model_p_favors_under_when_projection_below_line(self):
        """Projection well below the line -> model_p for UNDER > 0.5."""
        from strikeout_engine import analyze_k_prop
        result = analyze_k_prop(self._sp_stats(), None, "XXX", market_line=9.0)
        assert result["direction"] == "UNDER"
        assert result["model_p"] > 0.5

    def test_edge_pct_derived_from_probability_not_gap_times_ten(self):
        """edge_pct must equal a real probability edge vs. the 0.5 market
        baseline (matching the existing -110 K-prop convention elsewhere in
        brain.py), not gap*10."""
        from strikeout_engine import analyze_k_prop, project_strikeouts
        from props_engine import prob_over

        sp = self._sp_stats()
        result = analyze_k_prop(sp, None, "XXX", market_line=6.0)
        assert "edge_pct" in result

        # Recompute independently via the same Poisson model to verify the formula
        proj = project_strikeouts(sp, None, "XXX", 1.0, None)
        lam = proj["projected_k"]
        model_p_over = prob_over(lam, 6.0)
        expected_edge_pct = round((model_p_over - 0.5) * 100, 2)

        assert result["edge_pct"] == expected_edge_pct
        gap_times_ten = round(result["gap"] * 10, 2)
        assert result["edge_pct"] != gap_times_ten, (
            "edge_pct must not be derived from gap*10 (a K-count differential, "
            "not a probability-point edge)"
        )
