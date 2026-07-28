"""Regression tests for CHANGE 1: the ML lock/flip bucketing gap.

_conviction() (brain.py:1170) and the old inline bucketing at brain.py:4046
used disconnected rules -- a MEDIUM-conviction pick with edge >= 7% (clears
HIGH's edge bar but not its model_p bar) fell through both the "HIGH and
edge>=7" and "MEDIUM and 4<=edge<7" branches. It still got staked and
logged to the bets table, but never appeared in all_locks or all_flips --
invisible in the Telegram slip and uncounted in the ML bet total. Tonight's
concrete case: Tampa Bay Rays, edge +9.2%, model_p 0.494, staked $23,
silently dropped.

Fix: _ml_bucket(conv) is now the single source of truth for tier -> bucket,
so the split can never re-derive itself from edge_pct downstream.

Run: python -m pytest test_ml_bucketing.py -v
"""
from unittest.mock import patch

import brain


class TestConvictionToBucketMapping:
    """Pure unit tests -- _conviction() decides the tier, _ml_bucket() maps
    tier to slip bucket. No re-derivation from edge_pct should be possible."""

    def test_rays_case_medium_high_edge_lands_in_flip(self):
        """The concrete bug: edge +9.2%, model_p 0.494 clears HIGH's edge
        bar (7.0) but not its model_p bar (0.52), so _conviction() correctly
        downgrades to MEDIUM. Before the fix this fell through to neither
        bucket despite being staked and logged."""
        conv = brain._conviction(9.2, 0.494, {}, {})
        assert conv == "MEDIUM"
        assert brain._ml_bucket(conv) == "FLIP"

    def test_high_on_both_bars_lands_in_lock(self):
        conv = brain._conviction(8.0, 0.55, {}, {})
        assert conv == "HIGH"
        assert brain._ml_bucket(conv) == "LOCK"

    def test_medium_edge_far_above_seven_still_flips_not_dropped(self):
        """High edge but model_p too low for HIGH -- must never fall through
        regardless of how far edge_pct is above the old 7% upper bound.
        edge=12 is in the value-dog band [10, 14) with model_p=0.50, which
        clears the value-dog floor (0.40) but not the value-dog HIGH cutoff
        (14%), so _conviction() returns MEDIUM here too."""
        conv = brain._conviction(12.0, 0.50, {}, {})
        assert conv == "MEDIUM"
        assert brain._ml_bucket(conv) == "FLIP"

    def test_edge_below_medium_floor_lands_in_neither(self):
        # 3.5% clears MIN_EDGE_PCT (3.0) but not CONVICTION_MEDIUM_EDGE_MIN (4.0)
        conv = brain._conviction(3.5, 0.55, {}, {})
        assert conv == "PASS"
        assert brain._ml_bucket(conv) is None

    def test_edge_below_min_floor_lands_in_neither(self):
        conv = brain._conviction(2.0, 0.55, {}, {})
        assert conv == "PASS"
        assert brain._ml_bucket(conv) is None

    def test_ml_bucket_never_returns_a_third_value(self):
        for conv in ("HIGH", "MEDIUM", "PASS", "", "UNKNOWN"):
            assert brain._ml_bucket(conv) in ("LOCK", "FLIP", None)


class TestSlipFormattingIncludesHighEdgeMedium:
    """End-to-end: a MEDIUM pick with edge >= 7% must actually render in the
    slip's COIN FLIPS section -- not just pass a unit-level classification
    check. Exercises the real _daily_bet_slip() text-building code."""

    def _mk_analysis(self, edge, model_p, stake=23.0, side="away"):
        return {
            "away_name": "Tampa Bay Rays", "home_name": "Toronto Blue Jays",
            f"{side}_name": "Tampa Bay Rays",
            f"best_{side}_odds": "+143",
            f"{side}_edge": edge,
            f"{side}_model_p": model_p,
            f"{side}_stake": stake,
            f"{side}_confidence_score": 80,
            "away_lineup_confirmed": True,
            "home_lineup_confirmed": True,
        }

    def _bucket_into(self, analysis, side, conv):
        locks, flips = [], []
        bucket = brain._ml_bucket(conv)
        if bucket == "LOCK":
            locks.append((analysis, side))
        elif bucket == "FLIP":
            flips.append((analysis, side))
        return locks, flips

    def test_medium_high_edge_pick_appears_with_medium_conviction_tag(self):
        analysis = self._mk_analysis(edge=9.2, model_p=0.494)
        conv = brain._conviction(9.2, 0.494, {}, {})
        assert conv == "MEDIUM"  # sanity: this is the Rays case
        locks, flips = self._bucket_into(analysis, "away", conv)
        assert flips and not locks

        sent = []
        with patch("brain._send_telegram", side_effect=lambda m: (sent.append(m), True)[1]):
            brain._daily_bet_slip(locks, flips, [], [], 1000.0)

        full_text = "\n".join(sent)
        assert "Tampa Bay Rays" in full_text, (
            "a MEDIUM pick with edge >= 7% must render on the slip, not "
            "silently vanish the way the Rays pick did"
        )
        play_line = next(l for l in full_text.split("\n") if "PLAY #" in l)
        assert "MEDIUM" in play_line
        assert "HIGH" not in play_line

    def test_high_pick_appears_with_high_conviction_tag(self):
        analysis = self._mk_analysis(edge=8.0, model_p=0.55)
        conv = brain._conviction(8.0, 0.55, {}, {})
        assert conv == "HIGH"
        locks, flips = self._bucket_into(analysis, "away", conv)
        assert locks and not flips

        sent = []
        with patch("brain._send_telegram", side_effect=lambda m: (sent.append(m), True)[1]):
            brain._daily_bet_slip(locks, flips, [], [], 1000.0)

        full_text = "\n".join(sent)
        assert "Tampa Bay Rays" in full_text
        play_line = next(l for l in full_text.split("\n") if "PLAY #" in l)
        assert "HIGH" in play_line
        assert "MEDIUM" not in play_line
