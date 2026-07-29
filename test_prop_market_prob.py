"""
FIX #2 (2026-07-29 prop-slip review): brain.py's _norm_k() hardcoded
"market_p": 0.5 unconditionally when normalizing K-props for the slip,
discarding the real SGO odds-implied market_p that the K-prop collection
loop upstream (brain.py, player_prop_market_prob) had already fetched and
used to compute edge_pct correctly. The result: edge_pct shown/sent was
right, but the market_prob value written to the bets table was always a
flat 0.5 — corrupting anything reading that column directly later
(backtesting, calibration audits) instead of recomputing from raw odds.

Run with: python -m pytest test_prop_market_prob.py -v
"""

from unittest.mock import patch

import pytest


class TestKPropMarketProbPreserved:
    @pytest.fixture(autouse=True)
    def _isolated_db(self, tmp_path):
        import db
        tmp_db = str(tmp_path / "prop_market_prob.db")
        with patch.object(db, "DB_PATH", tmp_db):
            db.init_db()
            yield db

    def _mk_ml_analysis(self, away_name, home_name, side, odds, model_p, stake, confidence=70):
        return {
            "away_name": away_name, "home_name": home_name,
            "away": away_name[:3].upper(), "home": home_name[:3].upper(),
            f"{side}_name": away_name if side == "away" else home_name,
            f"best_{side}_odds": odds,
            f"{side}_model_p": model_p,
            f"{side}_nv": 0.45,
            f"{side}_edge": 8.0,
            f"{side}_stake": stake,
            f"{side}_conv": "HIGH",
            f"{side}_confidence_score": confidence,
            "away_lineup_confirmed": True,
            "home_lineup_confirmed": True,
            "h2h": {},
        }

    def _call_slip(self, all_k_props):
        import brain
        locks = [(self._mk_ml_analysis("Team A", "Team B", "away", "-150", 0.60, 20.0), "away")]
        return brain._daily_bet_slip(
            locks, [], [], [], 1000.0,
            all_k_props=all_k_props,
        )

    def test_real_sgo_market_p_is_stored_not_overwritten(self, _isolated_db):
        """A K-prop with a real SGO-derived market_p (e.g. from
        player_prop_market_prob()) must keep that exact value all the way
        into the bets table's market_prob column."""
        all_k_props = [{
            "sp": "Logan Webb", "team": "SF", "game": "SF @ LAD", "line": 6.5,
            "p_over": 0.65, "market_p": 0.42, "edge_pct": 15.0, "stake": 9.0,
            "statcast_2025": False,
        }]
        self._call_slip(all_k_props)
        rows = [b for b in _isolated_db.get_bets() if b.get("type") == "PROP"]
        assert len(rows) == 1
        assert rows[0]["market_prob"] == pytest.approx(0.42)

    def test_missing_market_p_still_falls_back_to_baseline(self, _isolated_db):
        """When SGO genuinely has no market for this exact prop/line, the
        K-prop dict has no market_p key at all — the 0.5 -110 baseline
        fallback must still apply (this is the legitimate use of the
        default, as opposed to discarding a real value that was present)."""
        all_k_props = [{
            "sp": "Logan Webb", "team": "SF", "game": "SF @ LAD", "line": 6.5,
            "p_over": 0.65, "edge_pct": 15.0, "stake": 9.0,
            "statcast_2025": False,
        }]
        self._call_slip(all_k_props)
        rows = [b for b in _isolated_db.get_bets() if b.get("type") == "PROP"]
        assert len(rows) == 1
        assert rows[0]["market_prob"] == pytest.approx(0.5)
