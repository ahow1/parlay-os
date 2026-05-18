"""seed_bets.py — Insert historical bets into the database.
Run directly or triggered via POST /api/seed.
Uses INSERT OR IGNORE so re-runs are safe.
"""
import db as _db

COLS = (
    "date", "timestamp", "bet", "type", "game", "sp", "park", "umpire",
    "bet_odds", "model_prob", "market_prob", "edge_pct", "conviction",
    "stake", "closing_odds", "clv_pct", "result", "game_score", "notes",
    "verify_hash", "profit"
)

ROWS = [
    ('2026-05-12', '2026-05-11T21:04:47.290837-04:00', 'SF', 'ML', 'SF @ opponent', '', 'SF', '', '+162', None, None, None, 'MEDIUM', 7.83, '109', -20.23, 'win', 'SF 6-2 LAD', '', None, 7.83),
    ('2026-05-12', '2026-05-11T21:04:47.290837-04:00', 'BAL', 'ML', 'BAL @ opponent', '', 'BAL', '', '+141', None, None, None, 'MEDIUM', 4.64, '', None, 'win', '', '', None, 4.64),
    ('2026-05-12', '2026-05-11T21:04:47.290837-04:00', 'TEX', 'ML', 'TEX @ opponent', '', 'TEX', '', '-125', None, None, None, 'MEDIUM', 3.94, '', None, 'loss', '', '', None, -3.94),
    ('2026-05-12', '2026-05-18 18:00:00', 'TEX', 'ML', 'AZ vs TEX', None, None, None, None, None, None, 5.0, 'HIGH', 3.94, None, None, 'loss', None, None, None, -3.94),
    ('2026-05-12', '2026-05-18 18:00:00', 'BAL', 'ML', 'NYY vs BAL', None, None, None, None, None, None, 5.0, 'HIGH', 11.0, None, None, 'win', None, None, None, 11.0),
    ('2026-05-12', '2026-05-18 18:00:00', 'SF', 'ML', 'SF vs LAD', None, None, None, None, None, None, 5.0, 'HIGH', 20.0, None, None, 'win', None, None, None, 20.0),
    ('2026-05-12', '2026-05-18 18:00:00', 'TB_TOR_OVER', 'TOTAL', 'TB vs TOR O6.5', None, None, None, None, None, None, 5.0, 'HIGH', 6.0, None, None, 'win', None, None, None, 6.0),
    ('2026-05-13', '2026-05-18 18:00:00', 'CLE', 'ML', 'LAA vs CLE', None, None, None, None, None, None, 5.0, 'HIGH', 3.0, None, None, 'win', None, None, None, 3.0),
    ('2026-05-13', '2026-05-18 18:00:00', 'NYY', 'ML', 'NYY vs BAL', None, None, None, None, None, None, 5.0, 'HIGH', 5.0, None, None, 'win', None, None, None, 5.0),
    ('2026-05-13', '2026-05-18 18:00:00', 'PHI', 'ML', 'PHI vs BOS', None, None, None, None, None, None, 5.0, 'HIGH', 8.0, None, None, 'win', None, None, None, 8.0),
    ('2026-05-13', '2026-05-18 18:00:00', 'WAS', 'ML', 'WSH vs CIN', None, None, None, None, None, None, 5.0, 'HIGH', 9.0, None, None, 'win', None, None, None, 9.0),
    ('2026-05-13', '2026-05-18 18:00:00', 'PHI_BOS_UNDER', 'TOTAL', 'PHI vs BOS U8.5', None, None, None, None, None, None, 5.0, 'HIGH', 6.0, None, None, 'win', None, None, None, 6.0),
    ('2026-05-14', '2026-05-18 18:00:00', 'ATH', 'ML', 'STL vs ATH', None, None, None, None, None, None, 5.0, 'HIGH', 7.0, None, None, 'win', None, None, None, 7.0),
    ('2026-05-14', '2026-05-18 18:00:00', 'TEX', 'ML', 'AZ vs TEX', None, None, None, None, None, None, 5.0, 'HIGH', 8.0, None, None, 'win', None, None, None, 8.0),
    ('2026-05-14', '2026-05-18 18:00:00', 'CWS', 'ML', 'KC vs CWS', None, None, None, None, None, None, 5.0, 'HIGH', 5.0, None, None, 'win', None, None, None, 5.0),
    ('2026-05-14', '2026-05-18 18:00:00', 'WAS', 'ML', 'WSH vs CIN', None, None, None, None, None, None, 5.0, 'HIGH', 12.0, None, None, 'win', None, None, None, 12.0),
    ('2026-05-14', '2026-05-18 18:00:00', 'ATL', 'ML', 'CHC vs ATL', None, None, None, None, None, None, 5.0, 'HIGH', 11.0, None, None, 'win', None, None, None, 11.0),
    ('2026-05-14', '2026-05-18 18:00:00', 'MIL', 'ML', 'SD vs MIL', None, None, None, None, None, None, 5.0, 'HIGH', 1.2, None, None, 'loss', None, None, None, -1.2),
    ('2026-05-14', '2026-05-18 18:00:00', 'PHI', 'ML', 'PHI vs BOS', None, None, None, None, None, None, 5.0, 'HIGH', 5.09, None, None, 'loss', None, None, None, -5.09),
    ('2026-05-14', '2026-05-18 18:00:00', 'YRFI_AZ_COL', 'NRFI', 'AZ vs COL YRFI', None, None, None, None, None, None, 5.0, 'HIGH', 0.34, None, None, 'loss', None, None, None, -0.34),
    ('2026-05-14', '2026-05-18 18:00:00', 'YRFI_CIN_CLE', 'NRFI', 'CIN vs CLE YRFI', None, None, None, None, None, None, 5.0, 'HIGH', 1.9, None, None, 'loss', None, None, None, -1.9),
    ('2026-05-14', '2026-05-18 18:00:00', 'YRFI_BAL_WAS', 'NRFI', 'BAL vs WAS YRFI', None, None, None, None, None, None, 5.0, 'HIGH', 1.89, None, None, 'loss', None, None, None, -1.89),
    ('2026-05-14', '2026-05-18 18:00:00', 'YRFI_SF_ATH', 'NRFI', 'SF vs ATH YRFI', None, None, None, None, None, None, 5.0, 'HIGH', 1.89, None, None, 'loss', None, None, None, -1.89),
    ('2026-05-15', '2026-05-18 18:00:00', 'CWS', 'ML', 'KC vs CWS', None, None, None, None, None, None, 5.0, 'HIGH', 11.0, None, None, 'win', None, None, None, 11.0),
    ('2026-05-15', '2026-05-18 18:00:00', 'PHI', 'ML', 'PHI vs BOS', None, None, None, None, None, None, 5.0, 'HIGH', 10.0, None, None, 'win', None, None, None, 10.0),
    ('2026-05-15', '2026-05-18 18:00:00', 'ATH', 'ML', 'STL vs ATH', None, None, None, None, None, None, 5.0, 'HIGH', 3.92, None, None, 'loss', None, None, None, -3.92),
    ('2026-05-15', '2026-05-18 18:00:00', 'PHI_BOS_UNDER', 'TOTAL', 'PHI vs BOS U8.5', None, None, None, None, None, None, 5.0, 'HIGH', 6.0, None, None, 'win', None, None, None, 6.0),
    ('2026-05-16', '2026-05-18 18:00:00', 'WAS', 'ML', 'BAL vs WAS', None, None, None, None, None, None, 5.0, 'HIGH', 11.14, None, None, 'win', None, None, None, 11.14),
    ('2026-05-16', '2026-05-18 18:00:00', 'TB', 'ML', 'MIA vs TB', None, None, None, None, None, None, 5.0, 'HIGH', 7.71, None, None, 'win', None, None, None, 7.71),
    ('2026-05-16', '2026-05-18 18:00:00', 'ATL', 'ML', 'BOS vs ATL', None, None, None, None, None, None, 5.0, 'HIGH', 6.08, None, None, 'win', None, None, None, 6.08),
    ('2026-05-16', '2026-05-18 18:00:00', 'PIT', 'ML', 'PHI vs PIT', None, None, None, None, None, None, 5.0, 'HIGH', 5.14, None, None, 'loss', None, None, None, -5.14),
    ('2026-05-16', '2026-05-18 18:00:00', 'KC', 'ML', 'KC vs STL', None, None, None, None, None, None, 5.0, 'HIGH', 3.21, None, None, 'loss', None, None, None, -3.21),
    ('2026-05-16', '2026-05-18 18:00:00', 'KC2', 'ML', 'KC vs STL', None, None, None, None, None, None, 5.0, 'HIGH', 2.46, None, None, 'loss', None, None, None, -2.46),
    ('2026-05-16', '2026-05-18 18:00:00', 'BAL_WAS_UNDER', 'TOTAL', 'BAL vs WAS U8.5', None, None, None, None, None, None, 5.0, 'HIGH', 1.69, None, None, 'win', None, None, None, 1.69),
    ('2026-05-16', '2026-05-18 18:00:00', 'BOS_ATL_UNDER', 'TOTAL', 'BOS vs ATL U7.5', None, None, None, None, None, None, 5.0, 'HIGH', 2.35, None, None, 'win', None, None, None, 2.35),
    ('2026-05-16', '2026-05-18 18:00:00', 'PARLAY_WAS_TB_ATL', 'PARLAY', 'WAS+TB+ATL', None, None, None, None, None, None, 5.0, 'HIGH', 22.22, None, None, 'win', None, None, None, 22.22),
    ('2026-05-16', '2026-05-18 18:00:00', 'PARLAY_HARPER_SCHWARBER', 'PARLAY', 'Harper+Schwarber HR', None, None, None, None, None, None, 5.0, 'HIGH', 0.99, None, None, 'loss', None, None, None, -0.99),
    ('2026-05-16', '2026-05-18 18:00:00', 'PARLAY_ATL_NYY_CHC_AZ', 'PARLAY', 'ATL+NYY+CHC+AZ RL', None, None, None, None, None, None, 5.0, 'HIGH', 1.0, None, None, 'loss', None, None, None, -1.0),
    ('2026-05-16', '2026-05-18 18:00:00', 'PARLAY_WAS_PHI_TB', 'PARLAY', 'WAS+PHI+TB', None, None, None, None, None, None, 5.0, 'HIGH', 3.78, None, None, 'loss', None, None, None, -3.78),
    ('2026-05-16', '2026-05-18 18:00:00', 'PARLAY_WAS_TB_CWS', 'PARLAY', 'WAS+TB+CWS', None, None, None, None, None, None, 5.0, 'HIGH', 4.46, None, None, 'loss', None, None, None, -4.46),
    ('2026-05-17', '2026-05-18 18:00:00', 'WAS', 'ML', 'BAL vs WAS', None, None, None, None, None, None, 5.0, 'HIGH', 10.15, None, None, 'win', None, None, None, 10.15),
    ('2026-05-17', '2026-05-18 18:00:00', 'PHI', 'ML', 'PHI vs PIT', None, None, None, None, None, None, 5.0, 'HIGH', 6.79, None, None, 'win', None, None, None, 6.79),
    ('2026-05-17', '2026-05-18 18:00:00', 'LAD', 'ML', 'LAD vs LAA', None, None, None, None, None, None, 5.0, 'HIGH', 3.3, None, None, 'win', None, None, None, 3.3),
    ('2026-05-17', '2026-05-18 18:00:00', 'CWS', 'ML', 'CHC vs CWS', None, None, None, None, None, None, 5.0, 'HIGH', 14.18, None, None, 'win', None, None, None, 14.18),
    ('2026-05-17', '2026-05-18 18:00:00', 'MIN', 'ML', 'MIL vs MIN', None, None, None, None, None, None, 5.0, 'HIGH', 15.95, None, None, 'win', None, None, None, 15.95),
    ('2026-05-17', '2026-05-18 18:00:00', 'TB', 'ML', 'MIA vs TB', None, None, None, None, None, None, 5.0, 'HIGH', 13.74, None, None, 'loss', None, None, None, -13.74),
    ('2026-05-17', '2026-05-18 18:00:00', 'TEX_HOU_UNDER', 'TOTAL', 'TEX vs HOU U8.5', None, None, None, None, None, None, 5.0, 'HIGH', 2.61, None, None, 'win', None, None, None, 2.61),
    ('2026-05-17', '2026-05-18 18:00:00', 'NYY_NYM_OVER', 'TOTAL', 'NYY vs NYM O6.5', None, None, None, None, None, None, 5.0, 'HIGH', 1.7, None, None, 'loss', None, None, None, -1.7),
    ('2026-05-17', '2026-05-18 18:00:00', 'PARLAY_TOR_SD_SF_UNDER', 'PARLAY', 'TOR+SD+SF Under', None, None, None, None, None, None, 5.0, 'HIGH', 0.28, None, None, 'loss', None, None, None, -0.28),
    ('2026-05-17', '2026-05-18 18:00:00', 'PARLAY_TB_PROPS', 'PARLAY', 'TB Props', None, None, None, None, None, None, 5.0, 'HIGH', 0.98, None, None, 'loss', None, None, None, -0.98),
    ('2026-05-18', '2026-05-18 18:00:00', 'ATL', 'ML', 'BOS vs ATL', None, None, None, None, None, None, 5.0, 'HIGH', 9.13, None, None, 'win', None, None, None, 9.13),
    ('2026-05-18', '2026-05-18 18:00:00', 'PHI', 'ML', 'PHI vs PIT', None, None, None, None, None, None, 5.0, 'HIGH', 12.37, None, None, 'win', None, None, None, 12.37),
    ('2026-05-18', '2026-05-18 18:00:00', 'CWS', 'ML', 'CHC vs CWS', None, None, None, None, None, None, 5.0, 'HIGH', 10.56, None, None, 'win', None, None, None, 10.56),
    ('2026-05-18', '2026-05-18 18:00:00', 'SEA', 'ML', 'SD vs SEA', None, None, None, None, None, None, 5.0, 'HIGH', 8.55, None, None, 'loss', None, None, None, -8.55),
    ('2026-05-18', '2026-05-18 18:00:00', 'ATH', 'ML', 'SF vs ATH', None, None, None, None, None, None, 5.0, 'HIGH', 11.62, None, None, 'loss', None, None, None, -11.62),
    ('2026-05-18', '2026-05-18 18:00:00', 'WAS', 'ML', 'BAL vs WAS', None, None, None, None, None, None, 5.0, 'HIGH', 19.1, None, None, 'loss', None, None, None, -19.1),
    ('2026-05-18', '2026-05-18 18:00:00', 'HOU', 'ML', 'TEX vs HOU', None, None, None, None, None, None, 5.0, 'HIGH', 14.1, None, None, 'loss', None, None, None, -14.1),
    ('2026-05-18', '2026-05-18 18:00:00', 'MIN', 'ML', 'MIL vs MIN', None, None, None, None, None, None, 5.0, 'HIGH', 5.7, None, None, 'loss', None, None, None, -5.7),
    ('2026-05-18', '2026-05-18 18:00:00', 'PARLAY_K_PROPS', 'PARLAY', 'K Props Combo', None, None, None, None, None, None, 5.0, 'HIGH', 2.98, None, None, 'loss', None, None, None, -2.98),
    ('2026-05-18', '2026-05-18 18:00:00', 'PARLAY_UNDER_COMBO', 'PARLAY', 'Under Combo', None, None, None, None, None, None, 5.0, 'HIGH', 2.47, None, None, 'loss', None, None, None, -2.47),
]

def seed():
    placeholders = ", ".join("?" * len(COLS))
    col_list     = ", ".join(COLS)
    sql = f"INSERT OR IGNORE INTO bets ({col_list}) VALUES ({placeholders})"
    with _db._conn() as conn:
        conn.executemany(sql, ROWS)
    print(f"[seed] inserted up to {len(ROWS)} bets (duplicates skipped)")

if __name__ == "__main__":
    seed()
