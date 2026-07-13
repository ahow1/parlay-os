"""Tests for TIER 2 silent-edge-killer audit fixes (AUDIT.md B7, B8, M1, M2, M3, M4).
Run: python -m pytest test_edge_killers.py -v
"""

import inspect
from unittest.mock import patch

import pytest


# ── B7: situations never applied to win prob ──────────────────────────────────

class TestSituationsWiredIntoWinProb:
    """B7: brain.py imported a nonexistent get_active_situations() (silently
    failing every game), and check_situations()'s total_away_adj/
    total_home_adj were computed but never applied to the win-prob blend —
    only a flat +8 confidence bump fired on a 3+ situation stack."""

    def test_dead_get_active_situations_import_removed(self):
        import brain
        src = inspect.getsource(brain.analyze_game)
        assert "import get_active_situations" not in src, (
            "get_active_situations() doesn't exist in situations_engine.py — "
            "the import must be removed, not left to silently except:pass"
        )

    def test_total_adj_applied_before_edge_is_computed(self):
        """total_away_adj/total_home_adj must be added to away_model_p/
        home_model_p BEFORE away_edge/home_edge are derived from them —
        otherwise the adjustment never reaches edge, stake, or conviction."""
        import brain
        src = inspect.getsource(brain.analyze_game)
        apply_idx = src.index('situations_result.get("total_away_adj"')
        edge_idx  = src.index("away_edge = round((away_model_p - away_nv)")
        assert apply_idx < edge_idx, (
            "total_away_adj must be applied to away_model_p before away_edge "
            "is computed from it"
        )

    def test_only_one_check_situations_call_remains(self):
        """The duplicate late-game recomputation must be removed now that
        situations_result is computed once, early, and reused."""
        import brain
        src = inspect.getsource(brain.analyze_game)
        assert src.count("check_situations(") == 1
