"""Tests for real ML pool cap enforcement (Step 2.1 audit follow-up).

Before this fix, ml_pool_rem was computed (for the slip header display) but
never consulted at admission time — ML picks were only gated by the flat
daily cap, in game-analysis order rather than edge order. This meant
ml_pool_rem was purely decorative.

run_daily_scout() is a huge end-to-end orchestrator never executed directly
in tests (see test_runline.py / test_money_bugs.py) — these are source-level
regression guards, matching that existing convention.

Run: python -m pytest test_ml_pool_enforcement.py -v
"""

import inspect

import brain


class TestMlPoolIsReallyEnforced:
    def test_ml_pool_remaining_gates_admission(self):
        """ml_pool_rem (_ml_rem) must actually block admission, not just
        appear in a print/header string."""
        src = inspect.getsource(brain.run_daily_scout)
        assert '_ml_pool_spent + proposed_stake > _ml_rem' in src

    def test_flat_daily_cap_still_gates_admission(self):
        """Double-gated: POOL_ML (200% of daily_budget) is currently looser
        than the flat cap, so the flat cap must remain a binding constraint
        too — this is what keeps today's real ML ceiling unchanged while
        making ml_pool_rem a real (if currently non-binding) constraint."""
        src = inspect.getsource(brain.run_daily_scout)
        assert 'accumulated_risk + proposed_stake > _cap' in src

    def test_admission_is_edge_sorted_not_game_order(self):
        """Candidates must be admitted highest-edge-first across the whole
        slate, mirroring the RUNLINE admission pattern — not in whichever
        order games happened to be analyzed."""
        src = inspect.getsource(brain.run_daily_scout)
        assert 'sorted(_ml_candidates, key=lambda c: c["edge"], reverse=True)' in src

    def test_candidates_collected_before_admission_not_admitted_inline(self):
        """The per-game side loop must only qualify + collect candidates;
        it must not log/admit inline anymore (that's the bug being fixed)."""
        src = inspect.getsource(brain.run_daily_scout)
        collect_idx = src.index("_ml_candidates.append({")
        admit_idx = src.index("for _mc in sorted(_ml_candidates")
        assert collect_idx < admit_idx

    def test_over_cap_picks_still_logged_when_pool_blocks(self):
        """A candidate blocked by either gate must still be logged as
        over_cap (visible for audit) rather than silently dropped."""
        src = inspect.getsource(brain.run_daily_scout)
        block_section = src[src.index("_blocked_reason = None"):src.index("# Validate bet type before logging")]
        assert "_log_bet_with_retry(today, analysis, side, conv, over_cap=True)" in block_section

    def test_persist_check_still_precedes_telegram_queueing(self):
        """Regression guard for the existing money-bug fix (B4): the retry/
        suppress check must still run before the pick is queued into
        all_locks/all_flips or scout_out['bets'], even after moving
        admission to a post-loop pass."""
        src = inspect.getsource(brain.run_daily_scout)
        persist_idx = src.index("_log_bet_with_retry(today, analysis, side, conv):")
        locks_idx = src.index("all_locks.append((analysis, side))")
        bets_idx = src.index('scout_out["bets"].append({')
        assert persist_idx < locks_idx
        assert persist_idx < bets_idx

    def test_pass_tracking_reflects_final_admission_not_qualification(self):
        """A game must only land in all_pass/scout_out['passes'] if none of
        its candidates were actually admitted — matching prior behavior
        where a cap-blocked pick still counted as a 'pass' game."""
        src = inspect.getsource(brain.run_daily_scout)
        assert "_ml_admitted_ids = {id(a) for a in all_bets}" in src
        assert "if id(_pg_analysis) not in _ml_admitted_ids:" in src
