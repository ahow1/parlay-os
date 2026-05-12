"""PARLAY OS — shared constants. Import from here, never re-define elsewhere."""

LG_RPG   = 4.35
LG_ERA   = 4.35
PYTH_EXP = 1.83
HOME_ADV = 1.035

PARK_FACTORS = {
    "COL": 1.13, "BOS": 1.07, "CIN": 1.05, "PHI": 1.03, "CHC": 1.03,
    "NYY": 1.01, "BAL": 1.01, "MIN": 1.01, "KC":  1.00, "WAS": 1.00,
    "TEX": 1.00, "TOR": 1.00, "NYM": 0.98, "HOU": 0.97, "ATL": 0.97,
    "DET": 0.97, "MIA": 0.97, "STL": 0.99, "MIL": 0.99, "LAD": 0.99,
    "AZ":  1.02, "CLE": 0.98, "LAA": 0.98, "PIT": 0.98, "CWS": 0.99,
    "OAK": 0.95, "ATH": 0.95, "SF":  0.93, "SD":  0.95, "SEA": 0.96, "TB": 0.95,
}

MLB_TEAM_MAP = {
    "Arizona Diamondbacks": "AZ",   "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",     "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",          "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",       "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",      "Detroit Tigers": "DET",
    "Houston Astros": "HOU",        "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",         "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",       "New York Mets": "NYM",
    "New York Yankees": "NYY",      "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",       "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",      "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",         "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",     "Washington Nationals": "WAS",
    "Athletics": "ATH",
}

MLB_TEAM_NAMES = {
    "AZ":  ["Arizona Diamondbacks","Diamondbacks","D-backs"],
    "ATL": ["Atlanta Braves","Braves"],
    "BAL": ["Baltimore Orioles","Orioles"],
    "BOS": ["Boston Red Sox","Red Sox"],
    "CHC": ["Chicago Cubs","Cubs"],
    "CWS": ["Chicago White Sox","White Sox"],
    "CIN": ["Cincinnati Reds","Reds"],
    "CLE": ["Cleveland Guardians","Guardians"],
    "COL": ["Colorado Rockies","Rockies"],
    "DET": ["Detroit Tigers","Tigers"],
    "HOU": ["Houston Astros","Astros"],
    "KC":  ["Kansas City Royals","Royals"],
    "LAA": ["Los Angeles Angels","Angels"],
    "LAD": ["Los Angeles Dodgers","Dodgers"],
    "MIA": ["Miami Marlins","Marlins"],
    "MIL": ["Milwaukee Brewers","Brewers"],
    "MIN": ["Minnesota Twins","Twins"],
    "NYM": ["New York Mets","Mets"],
    "NYY": ["New York Yankees","Yankees"],
    "ATH": ["Oakland Athletics","Athletics","Oakland"],
    "PHI": ["Philadelphia Phillies","Phillies"],
    "PIT": ["Pittsburgh Pirates","Pirates"],
    "SD":  ["San Diego Padres","Padres"],
    "SF":  ["San Francisco Giants","Giants"],
    "SEA": ["Seattle Mariners","Mariners"],
    "STL": ["St. Louis Cardinals","Cardinals"],
    "TB":  ["Tampa Bay Rays","Rays"],
    "TEX": ["Texas Rangers","Rangers"],
    "TOR": ["Toronto Blue Jays","Blue Jays"],
    "WAS": ["Washington Nationals","Nationals"],
}

MLB_TEAM_IDS = {
    "AZ": 109, "ATL": 144, "BAL": 110, "BOS": 111, "CHC": 112,
    "CWS": 145, "CIN": 113, "CLE": 114, "COL": 115, "DET": 116,
    "HOU": 117, "KC":  118, "LAA": 108, "LAD": 119, "MIA": 146,
    "MIL": 158, "MIN": 142, "NYM": 121, "NYY": 147, "ATH": 133,
    "OAK": 133, "PHI": 143, "PIT": 134, "SD":  135, "SF":  137,
    "SEA": 136, "STL": 138, "TB":  139, "TEX": 140, "TOR": 141, "WAS": 120,
}

BALLPARK_CITIES = {
    "AZ":  "Phoenix",       "ATL": "Atlanta",       "BAL": "Baltimore",
    "BOS": "Boston",        "CHC": "Chicago",        "CWS": "Chicago",
    "CIN": "Cincinnati",    "CLE": "Cleveland",      "COL": "Denver",
    "DET": "Detroit",       "HOU": "Houston",        "KC":  "Kansas City",
    "LAA": "Anaheim",       "LAD": "Los Angeles",    "MIA": "Miami",
    "MIL": "Milwaukee",     "MIN": "Minneapolis",    "NYM": "New York",
    "NYY": "New York",      "ATH": "Sacramento",     "OAK": "Oakland",
    "PHI": "Philadelphia",  "PIT": "Pittsburgh",     "SD":  "San Diego",
    "SF":  "San Francisco", "SEA": "Seattle",        "STL": "St. Louis",
    "TB":  "St. Petersburg","TEX": "Arlington",      "TOR": "Toronto",
    "WAS": "Washington DC",
}

TEAM_SLUGS = {
    "AZ":"ari","ATL":"atl","BAL":"bal","BOS":"bos","CHC":"chc","CWS":"cws",
    "CIN":"cin","CLE":"cle","COL":"col","DET":"det","HOU":"hou","KC":"kc",
    "LAA":"laa","LAD":"lad","MIA":"mia","MIL":"mil","MIN":"min","NYM":"nym",
    "NYY":"nyy","ATH":"ath","PHI":"phi","PIT":"pit","SD":"sd","SF":"sf",
    "SEA":"sea","STL":"stl","TB":"tb","TEX":"tex","TOR":"tor","WAS":"was",
}

# (k_factor, run_factor, note)
UMPIRE_TENDENCIES = {
    "CB Bucknor":        (0.90, 0.93, "small/erratic zone — YRFI lean"),
    "Angel Hernandez":   (0.92, 0.95, "below-avg zone — hitter-friendly"),
    "Laz Diaz":          (0.93, 0.94, "tight zone — YRFI tendency"),
    "Doug Eddings":      (0.96, 0.97, "slightly tight zone"),
    "Ron Kulpa":         (0.98, 0.97, "slightly tight zone"),
    "Chris Guccione":    (0.98, 0.98, "near neutral, slightly tight"),
    "Vic Carapazza":     (1.06, 1.04, "large zone — K-friendly, under lean"),
    "Lance Barrett":     (1.04, 1.02, "above-avg zone — K-friendly"),
    "Mark Carlson":      (1.04, 1.02, "above-avg zone"),
    "Dan Bellino":       (1.05, 1.03, "above-avg zone — K-friendly"),
    "Fieldin Culbreth":  (1.03, 1.01, "above-avg zone"),
    "Alfonso Marquez":   (1.02, 1.01, "slight above avg"),
    "Jim Reynolds":      (1.02, 1.01, "slight above avg"),
    "Paul Emmel":        (1.02, 1.01, "slight above avg"),
    "Tripp Gibson":      (1.02, 1.01, "slight above avg"),
    "Hunter Wendelstedt":(1.01, 1.01, "near neutral"),
    "Bruce Dreckman":    (1.03, 1.02, "solid zone"),
    "Bill Welke":        (1.02, 1.01, "standard zone"),
    "Tom Hallion":       (1.01, 1.00, "neutral"),
    "Brian Gorman":      (1.00, 1.00, "neutral"),
    "Ted Barrett":       (1.00, 1.00, "neutral"),
    "Dana DeMuth":       (1.01, 1.00, "neutral"),
    "John Tumpane":      (0.99, 0.99, "near neutral"),
    "Marvin Hudson":     (0.97, 0.98, "slightly tight"),
    "Bill Miller":       (0.98, 0.98, "slightly tight"),
}

# % of PAs from LHBs — affects platoon adjustment
TEAM_LHB_PCT = {
    "AZ":0.42,"ATL":0.44,"BAL":0.40,"BOS":0.48,"CHC":0.46,"CWS":0.38,
    "CIN":0.43,"CLE":0.40,"COL":0.45,"DET":0.42,"HOU":0.46,"KC":0.40,
    "LAA":0.45,"LAD":0.48,"MIA":0.39,"MIL":0.44,"MIN":0.44,"NYM":0.43,
    "NYY":0.46,"ATH":0.41,"PHI":0.44,"PIT":0.42,"SD":0.42,"SF":0.46,
    "SEA":0.47,"STL":0.43,"TB":0.45,"TEX":0.41,"TOR":0.46,"WAS":0.41,
}

PLATOON_WRCPLUS_DELTA = {
    ("R","R"): -4, ("R","L"): +6, ("L","R"): +5, ("L","L"): -5,
    ("S","R"): +2, ("S","L"): +2,
}

# ABS robot umpire: command pitchers +edge, fastball-heavy −edge (wOBA ≈47pt gap)
ABS_COMMAND_BONUS   =  0.03   # run factor boost for command SPs
ABS_FB_HEAVY_MALUS  = -0.02   # run factor penalty for fastball-only SPs

BOOK_PRIORITY = ["pinnacle", "draftkings", "fanduel", "betmgm", "caesars", "pointsbet", "betrivers"]
