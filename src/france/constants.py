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
LBS_PER_SECOND_BENCHMARK = 20   # Empirical (matches UK pipeline, anchor at ~1006m)
BENCHMARK_METRES = 1005.84      # 5 furlongs in metres (5 × 201.168)

# Surface-specific LPL multipliers (same as UK)
LPL_SURFACE_MULTIPLIER = {
    "Turf": 1.0,
    "All Weather": 1.10,
}

# Beaten-length attenuation (same as UK)
BL_ATTENUATION_THRESHOLD = 20.0
BL_ATTENUATION_FACTOR = 0.5

# ── Class-based raw figure compression ──
# Standard times are course/distance/surface medians across ALL classes.
# Lower-class races have slower standards that inflate raw figures for
# horses that beat them.  Rather than adjusting the standard time itself
# (which gives a flat lbs penalty regardless of performance), we compress
# the *excess above BASE_RATING* for lower-class races.
#
# Formula:
#   excess = max(0, raw_figure - BASE_RATING)
#   compress = max(0, 1 - CLASS_EXCESS_FACTOR * (raceClass - CLASS_REF))
#   raw_adjusted = BASE_RATING + excess * compress  (if raw >= BASE)
#   raw_adjusted = raw_figure                       (if raw < BASE)
#
# This only reduces inflated figures (raw > 100).  Horses that ran near
# or below the standard are unaffected.
#
# Fitted to: TONNANT (CHA C3)=88 unchanged, CENTRICAL (LYO C5)≈80.
CLASS_EXCESS_FACTOR = 0.354   # compression rate per class level below reference
CLASS_EXCESS_REFERENCE = 3    # Class 3 = no compression

# Minimum sample sizes (same as UK)
MIN_RACES_STANDARD_TIME = 20
MIN_RACES_GOING_ALLOWANCE = 3

# Going-allowance robustness parameters (same as UK, converted to s/m)
GA_OUTLIER_ZSCORE = 3.0
GA_SHRINKAGE_K = 3.0
GA_NONLINEAR_THRESHOLD = 0.30 / 201.168   # ~0.001491 s/m (was 0.30 s/f)
GA_NONLINEAR_BETA = 0.25 * 201.168        # ~50.292 (scaled for s/m²→s/m)
GA_CONVERGENCE_TOL = 0.005 / 201.168      # ~0.0000249 s/m
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
    "Bon", "Bon Léger", "Bon Leger", "Bon léger",
    "PSF STANDARD", "PSF RAPIDE",
}
# NOTE: "Léger" removed — empirically rides as soft ground (+0.39 s/f),
# despite the name meaning "light".  Including it in GOOD_GOING was
# contaminating standard times with slow-ground races.

# Going-description GA priors for Bayesian shrinkage (in seconds per metre).
# Updated 2026-03-30 from batch-computed empirical means (638k runners,
# ~12,000 meetings).  Previous values were stale seeds that diverged
# significantly from the data — notably "Très leger" (lowercase) was
# 7.5x too low, and "Léger" was 1.4x too low.
# All values converted from s/f to s/m (÷ 201.168).
_M = 201.168  # metres per furlong — conversion factor
FRANCE_GOING_GA_PRIOR = {
    # Turf going descriptions — empirical values from batch pipeline.
    # Casing variants can have different empirical values (different
    # PMU data sources / regional conventions).
    "Très Sec":      -0.25 / _M,
    "Tres Sec":      -0.25 / _M,
    "Sec":           -0.21 / _M,
    "Très leger":     0.375 / _M,  # empirical 0.375 s/f (was 0.05 — seed was wrong)
    "Tres leger":     0.375 / _M,
    "Très Leger":     0.05 / _M,   # seed — below MIN_MEETINGS threshold
    "Tres Leger":     0.05 / _M,
    "Bon Léger":      0.02 / _M,   # seed — below MIN_MEETINGS threshold
    "Bon Leger":      0.02 / _M,
    "Bon léger":      0.195 / _M,  # empirical 0.195 s/f (was 0.11)
    "Bon leger":      0.195 / _M,
    "Léger":          0.386 / _M,  # empirical 0.386 s/f (was 0.27) — soft ground despite name
    "Leger":          0.386 / _M,
    "Bon":            0.118 / _M,  # empirical 0.118 s/f (was 0.10)
    "Bon Souple":     0.14 / _M,   # seed — below MIN_MEETINGS threshold
    "Bon souple":     0.196 / _M,  # empirical 0.196 s/f (was 0.16)
    "Souple":         0.332 / _M,  # empirical 0.332 s/f (was 0.30)
    "Très Souple":    0.52 / _M,   # seed — below MIN_MEETINGS threshold
    "Tres Souple":    0.52 / _M,
    "Très souple":    0.569 / _M,  # empirical 0.569 s/f (was 0.52)
    "Tres souple":    0.52 / _M,
    "Collant":        0.919 / _M,  # empirical 0.919 s/f (was 0.86)
    "Lourd":          0.705 / _M,  # empirical 0.705 s/f (was 0.67)
    "Très lourd":     1.534 / _M,  # empirical 1.534 s/f (was 1.31)
    "Tres lourd":     1.31 / _M,   # seed — below MIN_MEETINGS threshold
    "Très Lourd":     1.31 / _M,   # seed — below MIN_MEETINGS threshold
    "Tres Lourd":     1.31 / _M,
    # PSF (artificial surface) — empirical values
    "PSF STANDARD":   0.087 / _M,  # empirical 0.087 s/f (was 0.06)
    "PSF RAPIDE":     0.019 / _M,  # empirical 0.019 s/f (was 0.01)
    "PSF LENTE":      0.125 / _M,  # empirical 0.125 s/f (was 0.13)
    "PSF":            0.012 / _M,  # empirical 0.012 s/f (was 0.02)
    "Standard":       0.06 / _M,   # seed
    # Unknown/empty — fall back to Bon empirical
    "Inconnu":        0.143 / _M,  # empirical 0.143 s/f (was 0.10)
    "":               0.133 / _M,  # empirical 0.133 s/f (was 0.10)
}

# Ordinal encoding for potential future ML use
FRANCE_GOING_ORDINAL = {
    "Très Sec": 0, "Tres Sec": 0,
    "Sec": 1,
    "Bon Léger": 2, "Bon Leger": 2,
    "Bon": 3,
    "Bon Souple": 4, "Bon souple": 4,
    "Léger": 5, "Leger": 5,          # empirically rides as soft ground
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
# NON-FRENCH COURSE BLOCKLIST
# ─────────────────────────────────────────────────────────────────────
# Course codes identified in the QA audit (french_standard_times_audit.md §3)
# that are not French racecourses.  These leak in from UK, Irish, German,
# and Swiss fixtures and contaminate French standard times.
NON_FRENCH_COURSE_CODES = {
    # British
    "ASC", "DON", "EPS", "GOO", "HAY", "KEM", "MKT", "NBU", "SDW", "WAR", "YOR",
    "WVD",  # Wolverhampton
    # Irish
    "CUR", "DUB", "GAL", "LEO",
    # German
    "BAD", "DUS", "FRA", "KOE",
    # Swiss
    "ZUR",
    # Hong Kong
    "HPV", "SHT",
    # North African (Morocco / international)
    "F03",
    # Spanish / Argentine (international)
    "VPR",
}


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
# WFA TABLES  (empirical, derived from GBR Timeform timefigures 2021-2025)
# ─────────────────────────────────────────────────────────────────────
# Measures the actual timefigure gap between 3yo and 4-5yo runners
# across ~270k rated GBR flat runners.  These values are significantly
# lower than the original UK WFA table which over-estimated by ~3.5 lbs
# on average (and up to +13 lbs at 8-10f in Apr/May).

# 3yo WFA table: rows = months 1-12, columns keyed by distance in furlongs.
# Values in lbs.  No surface split — sample sizes too thin when split.
_EMPIRICAL_WFA_3YO = {
    # Months 1-5 updated 2026-03 to align with BHA/Timeform WFA scales.
    # Previous empirical values were too low in early season (3-9 lbs
    # vs BHA 10-13), causing systematic under-rating of 3yo performances.
    # Months 6-12 retain empirical values.
    #        5f   6f   7f   8f  10f  12f
    1:  [13,  13,  13,  12,  10,  9],
    2:  [13,  13,  13,  12,  10,  9],
    3:  [13,  13,  13,  12,  10,  8],
    4:  [12,  12,  11,  11,  10,  8],
    5:  [10,  10,  10,  10,   9,  7],
    6:  [1,  7,  4,  3,  2,  0],
    7:  [2,  4,  6,  2,  2,  0],
    8:  [2,  4,  4,  3,  1,  0],
    9:  [2,  3,  2,  3,  1,  0],
    10: [2,  2,  3,  1,  0,  0],
    11: [3,  3,  3,  2,  1,  1],
    12: [4,  0,  5,  1,  3,  0],
}
_WFA_DIST_COLS = [5, 6, 7, 8, 10, 12]

# 2yo allowance: use a flat 12 lbs (conservative; insufficient French
# 2yo mixed-age data to derive empirically).
_WFA_2YO_FLAT = 12


def get_france_wfa_allowance(age, month, distance_metres):
    """Return empirical WFA allowance in lbs for a French flat runner.

    Parameters
    ----------
    age : int
        Horse age (2, 3, 4, …)
    month : int
        Calendar month 1-12.
    distance_metres : float
        Race distance in metres.

    Returns
    -------
    float  (always >= 0)
    """
    if age >= 4:
        return 0.0
    if age == 2:
        return float(_WFA_2YO_FLAT)
    # age == 3
    row = _EMPIRICAL_WFA_3YO.get(int(month))
    if row is None:
        return 0.0
    # Convert metres to furlongs for WFA table lookup
    d = float(distance_metres) / METERS_PER_FURLONG
    # Linear interpolation between bracketing distances
    for i in range(len(_WFA_DIST_COLS) - 1):
        lo, hi = _WFA_DIST_COLS[i], _WFA_DIST_COLS[i + 1]
        if d <= lo:
            return float(row[i])
        if d < hi:
            frac = (d - lo) / (hi - lo)
            return row[i] + frac * (row[i + 1] - row[i])
    return float(row[-1])
