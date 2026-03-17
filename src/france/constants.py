"""
French racing constants for the speed-figures pipeline.

Mirrors the constant structure of ``src/speed_figures.py`` but with
France-specific values for going descriptions, class mappings, courses,
and beaten-length codes.
"""

import re

# ─────────────────────────────────────────────────────────────────────
# CORE PIPELINE CONSTANTS  (shared with UK — imported or duplicated
# here so the French module is self-contained)
# ─────────────────────────────────────────────────────────────────────

BASE_RATING = 100          # 100-rated horse = standard time on good ground
BASE_WEIGHT_LBS = 126      # 9st 0lb — flat racing base weight
SECONDS_PER_LENGTH = 0.2   # BHA standard
LBS_PER_SECOND_5F = 20     # Empirical (matches UK pipeline)
BENCHMARK_FURLONGS = 5.0   # Anchor distance

# Surface-specific LPL multipliers (same as UK)
LPL_SURFACE_MULTIPLIER = {
    "Turf": 1.0,
    "All Weather": 1.10,
}

# Beaten-length attenuation (same as UK)
BL_ATTENUATION_THRESHOLD = 20.0
BL_ATTENUATION_FACTOR = 0.5

# Minimum sample sizes (same as UK)
MIN_RACES_STANDARD_TIME = 20
MIN_RACES_GOING_ALLOWANCE = 3

# Going-allowance robustness parameters (same as UK)
GA_OUTLIER_ZSCORE = 3.0
GA_SHRINKAGE_K = 3.0
GA_NONLINEAR_THRESHOLD = 0.30
GA_NONLINEAR_BETA = 0.25
GA_CONVERGENCE_TOL = 0.005
INTERPOLATED_GA_WEIGHT = 0.7   # Discount weight for interpolated standard times in GA

# Recency weighting for iterative standard times (same as UK)
RECENCY_HALF_LIFE_YEARS = 4.0

# Class adjustments — constant baseline (same finding as UK: varying
# class adjustments hurt accuracy).
CLASS_ADJUSTMENT_PER_MILE = {
    "1": -3.6,
    "2": -4.8,
    "3": -6.0,
    "4": -7.2,
    "5": -8.4,
    "6": -9.6,
    "7": -10.8,
}

# Sex allowance (not applied — same finding as UK)
SEX_ALLOWANCE_SUMMER = 3   # lbs, May–September
SEX_ALLOWANCE_WINTER = 5   # lbs, October–April
FEMALE_GENDERS = {"f", "m"}

# ─────────────────────────────────────────────────────────────────────
# UNIT CONVERSION
# ─────────────────────────────────────────────────────────────────────

METERS_PER_FURLONG = 201.168
KG_TO_LBS = 2.20462

# ─────────────────────────────────────────────────────────────────────
# FRENCH GOING DESCRIPTIONS
# ─────────────────────────────────────────────────────────────────────

# Going descriptions considered "Good/Standard" for standard-time
# compilation (mirrors GOOD_GOING in UK pipeline).
FRANCE_GOOD_GOING = {
    "Bon", "Bon Léger", "Bon Leger", "Bon léger", "Léger",
    "PSF STANDARD", "PSF RAPIDE",
}

# Going-description GA priors for Bayesian shrinkage (seeds — to be
# refined empirically after initial backfill, same methodology as UK
# GOING_GA_PRIOR derived from 10,625 meetings).
FRANCE_GOING_GA_PRIOR = {
    # Turf going descriptions (actual values from PMU data)
    "Très Sec":      -0.25,
    "Tres Sec":      -0.25,
    "Très leger":    -0.09,
    "Tres leger":    -0.09,
    "Sec":           -0.21,
    "Bon Léger":     -0.09,
    "Bon Leger":     -0.09,
    "Bon léger":     -0.09,
    "Léger":         -0.09,
    "Bon":            0.05,
    "Bon Souple":     0.25,
    "Bon souple":     0.25,
    "Souple":         0.51,
    "Très Souple":    0.65,
    "Tres Souple":    0.65,
    "Très souple":    0.65,
    "Collant":        0.82,
    "Lourd":          0.82,
    # PSF (artificial surface) — actual descriptions from data
    "PSF STANDARD":   0.04,
    "PSF RAPIDE":    -0.03,
    "PSF LENTE":      0.06,
    "PSF":            0.04,
    "Standard":       0.04,
    # Unknown/empty
    "Inconnu":        0.05,
    "":               0.05,
}

# Ordinal encoding for potential future ML use
FRANCE_GOING_ORDINAL = {
    "Très Sec": 0, "Tres Sec": 0,
    "Sec": 1,
    "Bon Léger": 2, "Bon Leger": 2,
    "Bon": 3,
    "Bon Souple": 4,
    "Souple": 5,
    "Très Souple": 6, "Tres Souple": 6,
    "Collant": 6,
    "Lourd": 7,
    "Standard": 3,
}

# ─────────────────────────────────────────────────────────────────────
# FRENCH CLASS MAPPING  (race name + prize money → class 1-7)
# ─────────────────────────────────────────────────────────────────────

# Regex patterns for Group/Listed detection (applied to race_name)
_GROUP1_RE = re.compile(r"GROUP?E?\s*I(?:\b|$)|GR\.?\s*I(?:\b|$)", re.I)
_GROUP2_RE = re.compile(r"GROUP?E?\s*II(?:\b|$)|GR\.?\s*II(?:\b|$)", re.I)
_GROUP3_RE = re.compile(r"GROUP?E?\s*III(?:\b|$)|GR\.?\s*III(?:\b|$)", re.I)
_LISTED_RE = re.compile(r"LIST[ÉEe]{1,2}", re.I)
_CLAIMING_RE = re.compile(r"R[ÉEe]CLAM|CLAIMING", re.I)

# Prize money thresholds for non-pattern races (EUR)
FRANCE_CLASS_FROM_PRIZE = [
    (500_000, "1"),   # Group-level prize money
    (200_000, "1"),
    (100_000, "2"),
    (50_000,  "3"),
    (25_000,  "4"),
    (15_000,  "5"),
    (8_000,   "6"),
    (0,       "7"),
]


def classify_french_race(race_name: str, prize_money) -> str:
    """Map a French race to class 1-7 using name patterns + prize money."""
    name = str(race_name or "")
    if _GROUP1_RE.search(name):
        return "1"
    if _GROUP2_RE.search(name):
        return "1"
    if _GROUP3_RE.search(name):
        return "2"
    if _LISTED_RE.search(name):
        return "2"
    if _CLAIMING_RE.search(name):
        return "6"

    try:
        pm = float(prize_money or 0)
    except (TypeError, ValueError):
        pm = 0.0

    for threshold, cls in FRANCE_CLASS_FROM_PRIZE:
        if pm >= threshold:
            return cls
    return "7"


# ─────────────────────────────────────────────────────────────────────
# PSF (ALL-WEATHER) TRACK IDENTIFICATION
# ─────────────────────────────────────────────────────────────────────

# Hippodromes known to host PSF (Piste en Sable Fibré) racing.
# Courses with both Turf and PSF use the parcours field to distinguish.
FRANCE_PSF_HIPPODROMES = {
    "DEAUVILLE",
    "CHANTILLY",
    "PAU",
    "PORNICHET",
    "MARSEILLE PONT DE VIVAUX",
    "CAGNES-SUR-MER",
}

# Parcours keywords that indicate PSF surface
_PSF_KEYWORDS = {"PSF", "FIBRE", "FIBRESAND", "POLYTRACK", "SABLE"}


def detect_surface(hippodrome_code: str, parcours: str, going: str = "") -> str:
    """Return 'All Weather' if the race is on PSF, else 'Turf'."""
    parc = str(parcours or "").upper()
    for kw in _PSF_KEYWORDS:
        if kw in parc:
            return "All Weather"
    # Check going description — PMU uses "PSF STANDARD", "PSF RAPIDE", etc.
    go = str(going or "").upper()
    if go.startswith("PSF"):
        return "All Weather"
    return "Turf"


# ─────────────────────────────────────────────────────────────────────
# MAIDEN & 2YO-ONLY DETECTION
# ─────────────────────────────────────────────────────────────────────

FRANCE_MAIDEN_KEYWORDS = [
    "MAIDEN", "DEBUTANT", "DÉBUTANT",
    "N'AYANT PAS COURU", "N'AYANT JAMAIS GAGN",
    "N'AYANT JAMAIS COURU",
]


def is_maiden_race(race_name: str) -> bool:
    """Heuristic: detect maiden races from race_name."""
    name = str(race_name or "").upper()
    return any(kw.upper() in name for kw in FRANCE_MAIDEN_KEYWORDS)


# ─────────────────────────────────────────────────────────────────────
# SEX CODE MAPPING  (PMU → pipeline)
# ─────────────────────────────────────────────────────────────────────

FRANCE_SEX_MAP = {
    "MALES":    "c",    # colt
    "FEMELLES": "f",    # filly
    "HONGRES":  "g",    # gelding
    "M":        "c",
    "F":        "f",
    "H":        "g",
}


# ─────────────────────────────────────────────────────────────────────
# WFA TABLES  (reused from UK — same biological curve)
# ─────────────────────────────────────────────────────────────────────
# Imported at use-time from src.speed_figures to avoid circular imports
# or duplication of large tables.  The French pipeline calls
# ``get_wfa_allowance`` from the UK module directly.
