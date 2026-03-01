"""
Speed Figure Compilation Pipeline
==================================
Computes speed figures for UK/Ireland flat racing from Timeform data.

Target variable: timefigure (Timeform's published time figure)

Pipeline stages:
  0. Load & filter data (UK/IRE, flat, with valid finishing times)
  1. Compute standard times per track/distance/surface
  2. Compute course-specific lbs-per-length from standard times
  3. Compute going allowances per track per day (across all races at meeting)
  4. Compute winner speed figures
  5. Extend to all runners via beaten lengths (using course-specific lpl)
  6. Apply weight-carried adjustment
  7. Apply weight-for-age (WFA) adjustment
  8. Apply sex allowance
  9. Calibrate against Timeform timefigure (quadratic + offsets)
 10. Stacked GBR enhancement (figure_calibrated + features → timefigure)
 11. Validate against Timeform timefigure
"""

import pandas as pd
import numpy as np
import os
import warnings

try:
    from sklearn.ensemble import GradientBoostingRegressor
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# ─────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")

BASE_RATING = 100          # A 100-rated horse matches standard time on good going
BASE_WEIGHT_LBS = 126      # 9st 0lb — flat racing base weight
SECONDS_PER_LENGTH = 0.2   # BHA standard
LBS_PER_SECOND_5F = 20     # Empirical: pairwise analysis against Timeform shows
                           # 22 over-spreads by ~10%.  20 matches empirical
                           # median(tf_diff / beaten_lengths) at 5-10f.
                           # Cf. BHA ~15, Timeform ~25 (at 60s), our empirical 20.
BENCHMARK_FURLONGS = 5.0   # Anchor distance

# Surface-specific LPL multipliers.  Empirical pairwise analysis
# against Timeform shows the optimal LPL exponent differs by surface:
# Turf lpl ∝ 1/d^0.94, AW lpl ∝ 1/d^0.85.  With base constant 20,
# Turf is well matched (slope~1.0 at 5-10f) but AW at middle/long
# distances needs a ~10% boost because the AW exponent is shallower.
LPL_SURFACE_MULTIPLIER = {
    "Turf": 1.0,
    "All Weather": 1.10,
}

# Beaten-length attenuation parameters.  Analysis shows a monotonically
# rising positive bias from ~5L onwards (+0.57 at 5-10L, +1.59 at
# 10-15L, +2.69 at 15-20L).  The soft cap attenuates margins beyond
# the threshold to reduce noise from eased/tailed-off horses.
BL_ATTENUATION_THRESHOLD = 20.0   # Full precision up to this many lengths
BL_ATTENUATION_FACTOR = 0.5       # Beyond threshold, each extra length
                                  # counts this fraction

# Minimum sample sizes
MIN_RACES_STANDARD_TIME = 20   # Minimum winners for a reliable standard time
                                # (empirical: 15-20 bracket has MAE 12.2 vs 8.8 for 100+)
MIN_RACES_GOING_ALLOWANCE = 3  # Minimum races on a card for going allowance

# Class adjustments in seconds per mile (8 furlongs).
# These are subtracted from raw finishing times to normalise to a common
# class baseline before computing standard times.  All values are negative
# so that subtracting them ADDS time — slower classes add more time,
# bringing them up to match the natural pace of the fastest classes.
CLASS_ADJUSTMENT_PER_MILE = {
    "1": -3.6,   # Group/Graded — fastest
    "2": -4.8,   # Listed/Premier Handicap
    "3": -6.0,
    "4": -7.2,   # Middle of the road
    "5": -8.4,
    "6": -9.6,
    "7": -10.8,  # Slowest
}

# ── UK & Ireland course sets ──

UK_COURSES = {
    "ASCOT", "AYR", "BATH", "BEVERLEY", "BRIGHTON", "CARLISLE",
    "CATTERICK BRIDGE", "CHELMSFORD CITY", "CHELTENHAM", "CHEPSTOW",
    "CHESTER", "DONCASTER", "EPSOM DOWNS", "EXETER", "FFOS LAS",
    "FONTWELL", "GOODWOOD", "HAMILTON PARK", "HAYDOCK PARK", "HEREFORD",
    "HEXHAM", "HUNTINGDON", "KELSO", "KEMPTON PARK", "LEICESTER",
    "LINGFIELD PARK", "LUDLOW", "MARKET RASEN", "MUSSELBURGH",
    "NEWBURY", "NEWCASTLE", "NEWMARKET (JULY)", "NEWMARKET (ROWLEY)",
    "NEWTON ABBOT", "NOTTINGHAM", "PLUMPTON", "PONTEFRACT", "REDCAR",
    "RIPON", "SALISBURY", "SANDOWN PARK", "SEDGEFIELD", "SOUTHWELL",
    "STRATFORD", "TAUNTON", "THIRSK", "UTTOXETER", "WARWICK", "WETHERBY",
    "WINCANTON", "WINDSOR", "WOLVERHAMPTON", "WORCESTER", "YARMOUTH",
    "YORK", "BANGOR-ON-DEE", "CARTMEL", "FAKENHAM", "FOLKESTONE",
    "PERTH", "TOWCESTER", "AINTREE",
}

IRE_COURSES = {
    "BALLINROBE", "BELLEWSTOWN", "CLONMEL", "CORK", "CURRAGH",
    "DOWN ROYAL", "DOWNPATRICK", "DUNDALK", "FAIRYHOUSE",
    "GALWAY", "GOWRAN PARK", "KILBEGGAN", "KILLARNEY",
    "LAYTOWN", "LEOPARDSTOWN", "LIMERICK", "LISTOWEL",
    "NAAS", "NAVAN", "PUNCHESTOWN", "ROSCOMMON", "SLIGO",
    "THURLES", "TIPPERARY", "TRAMORE", "WEXFORD",
}

# Going descriptions considered "Good/Standard" ground for standard-time
# compilation.  Includes both full names and the abbreviated forms that
# appear in the Timeform data for All-Weather meetings.
GOOD_GOING = {
    # Turf
    "Good", "Standard", "Good To Firm", "Good to Firm",
    "Standard To Slow", "Standard to Slow",
    "Good to Yielding", "Good To Yielding",
    # AW (abbreviated forms in data)
    "Std", "Std/Slow", "Std/Fast", "Standard/Slow",
}

# ── Sex allowance (flat) ──
# In open (mixed-sex) races fillies/mares receive a weight allowance.
# Since our weight-carried adjustment already accounts for the actual
# weight carried (which is reduced by the sex allowance), we must ADD
# the sex allowance back to filly/mare figures so they are on an equal
# footing with colts/geldings.  In sex-restricted races (fillies-only,
# mares-only) there is no allowance.
SEX_ALLOWANCE_SUMMER = 3   # lbs, May–September
SEX_ALLOWANCE_WINTER = 5   # lbs, October–April

# Female horse-gender codes in the data
FEMALE_GENDERS = {"f", "m"}  # f = filly, m = mare

# ── Surface-change cutoff dates ──
# Tracks that changed their artificial surface.  Only data ON or AFTER
# the cutoff date should be used for standard-time, going-allowance and
# figure computation — earlier data reflects the old surface and would
# contaminate the model.
SURFACE_CHANGE_CUTOFFS = {
    "SOUTHWELL":       "2022-01-01",
    "CHELMSFORD CITY": "2022-09-01",
}

# ── Weight-for-Age tables (empirical, derived from Timeform 2015-2023) ──
# Separate tables for Turf and All-Weather, as AW WFA is consistently
# higher (younger horses are at a greater disadvantage on artificial
# surfaces).  Keys: month → {distance_furlongs: allowance_lbs}.
#
# Methodology: calibrated raw_figure + weight_adj against Timeform's
# timefigure using 4-6yo as the zero baseline.  The mean residual for
# 2yo/3yo at each (month, distance) cell is the empirical WFA.
# Smoothed with a 3-month weighted average and rounded to integers.
# Negative residuals (younger horse outperforms baseline) are capped at 0.

WFA_3YO_TURF = {
    1:  {5: 10, 6: 10, 7: 8,  8: 9,  10: 8,  12: 5},
    2:  {5: 10, 6: 10, 7: 8,  8: 9,  10: 8,  12: 5},
    3:  {5: 10, 6: 10, 7: 8,  8: 9,  10: 8,  12: 4},
    4:  {5: 9,  6: 9,  7: 7,  8: 8,  10: 9,  12: 4},
    5:  {5: 7,  6: 8,  7: 5,  8: 10, 10: 9,  12: 3},
    6:  {5: 8,  6: 5,  7: 3,  8: 7,  10: 3,  12: 6,  14: 4},
    7:  {5: 7,  6: 3,  7: 0,  8: 3,  10: 1,  12: 4,  14: 0,  16: 0},
    8:  {5: 6,  6: 2,  7: 0,  8: 2,  10: 1,  12: 0,  14: 3,  16: 0},
    9:  {5: 5,  6: 5,  7: 1,  8: 5,  10: 4,  12: 2,  14: 3,  16: 2},
    10: {5: 5,  6: 4,  7: 4,  8: 4,  10: 4,  12: 4,  14: 4,  16: 4},
    11: {5: 3,  6: 3,  7: 2,  8: 4,  10: 4,  12: 3,  14: 3,  16: 3},
    12: {5: 3,  6: 3,  7: 2,  8: 4,  10: 4,  12: 3,  14: 3,  16: 3},
}

WFA_3YO_AW = {
    1:  {5: 13, 6: 12, 7: 8,  8: 10, 10: 6,  12: 9},
    2:  {5: 14, 6: 12, 7: 10, 8: 11, 10: 6,  12: 10},
    3:  {5: 13, 6: 11, 7: 8,  8: 9,  10: 6,  12: 4},
    4:  {5: 14, 6: 15, 7: 8,  8: 11, 10: 7,  12: 7},
    5:  {5: 9,  6: 10, 7: 9,  8: 8,  10: 7,  12: 5},
    6:  {5: 9,  6: 12, 7: 11, 8: 11, 10: 5,  12: 5},
    7:  {5: 8,  6: 9,  7: 8,  8: 10, 10: 5,  12: 2,  14: 0},
    8:  {5: 6,  6: 7,  7: 5,  8: 6,  10: 0,  12: 1,  14: 0,  16: 0},
    9:  {5: 10, 6: 8,  7: 3,  8: 3,  10: 0,  12: 0,  14: 0,  16: 0},
    10: {5: 9,  6: 7,  7: 4,  8: 3,  10: 0,  12: 1,  14: 0,  16: 0},
    11: {5: 8,  6: 6,  7: 3,  8: 4,  10: 0,  12: 1,  14: 0,  16: 0},
    12: {5: 7,  6: 6,  7: 3,  8: 2,  10: 0,  12: 0,  14: 0,  16: 0},
}

WFA_2YO_TURF = {
    3:  {5: 30, 6: 29, 7: 24},
    4:  {5: 20, 6: 29, 7: 20},
    5:  {5: 19, 6: 22, 7: 12},
    6:  {5: 20, 6: 18, 7: 15, 8: 17},
    7:  {5: 15, 6: 15, 7: 16, 8: 17},
    8:  {5: 12, 6: 14, 7: 11, 8: 12},
    9:  {5: 11, 6: 13, 7: 14, 8: 15, 10: 16},
    10: {5: 10, 6: 11, 7: 13, 8: 15, 10: 14},
    11: {5: 10, 6: 13, 7: 10, 8: 11, 10: 14},
    12: {5: 10, 6: 13, 7: 10, 8: 11, 10: 14},
}

WFA_2YO_AW = {
    3:  {5: 33, 6: 30, 7: 25},
    4:  {5: 30, 6: 27, 7: 25},
    5:  {5: 28, 6: 27, 7: 25},
    6:  {5: 28, 6: 26, 7: 25},
    7:  {5: 20, 6: 24, 7: 22, 8: 19},
    8:  {5: 19, 6: 16, 7: 16, 8: 19},
    9:  {5: 18, 6: 17, 7: 15, 8: 16, 10: 15},
    10: {5: 18, 6: 17, 7: 15, 8: 16, 10: 15},
    11: {5: 16, 6: 16, 7: 13, 8: 13, 10: 14},
    12: {5: 16, 6: 15, 7: 11, 8: 12, 10: 12},
}

# ── Older horse decline (empirical, Turf only) ──
# On Turf, horses aged 7+ show a statistically significant decline in
# performance relative to the 4-6yo baseline.  On AW the decline is
# within noise and not modelled.  Values are NEGATIVE (subtracted from
# figure to reflect reduced ability).
OLDER_DECLINE_TURF = {7: -1, 8: -1.5, 9: -2, 10: -2.5, 11: -3, 12: -3}

# Legacy aliases — kept so callers that import the old names still work.
WFA_3YO = WFA_3YO_TURF
WFA_2YO = WFA_2YO_TURF


# ═════════════════════════════════════════════════════════════════════
# STAGE 0 — DATA LOADING & FILTERING
# ═════════════════════════════════════════════════════════════════════

def load_data(years=range(2015, 2027)):
    """Load and concatenate all yearly CSV files."""
    frames = []
    for year in years:
        path = os.path.join(DATA_DIR, f"timeform_{year}.csv")
        if os.path.exists(path):
            df = pd.read_csv(path, low_memory=False)
            df["source_year"] = year
            frames.append(df)
            print(f"  Loaded {year}: {len(df):>8,} rows")
    combined = pd.concat(frames, ignore_index=True)
    print(f"  TOTAL: {len(combined):>8,} rows")
    return combined


def filter_uk_ire_flat(df):
    """Filter to UK/IRE flat racing with valid finishing times."""
    all_courses = UK_COURSES | IRE_COURSES

    mask = (
        df["courseName"].isin(all_courses) &
        df["raceType"].eq("Flat") &
        pd.to_numeric(df["finishingTime"], errors="coerce").notna() &
        (pd.to_numeric(df["finishingTime"], errors="coerce") > 0) &
        pd.to_numeric(df["distance"], errors="coerce").notna() &
        (pd.to_numeric(df["distance"], errors="coerce") > 0) &
        pd.to_numeric(df["positionOfficial"], errors="coerce").notna()
    )
    filtered = df[mask].copy()

    # Coerce numeric columns
    for col in ["finishingTime", "distance", "positionOfficial",
                "distanceBeaten", "distanceCumulative", "timefigure",
                "weightCarried", "horseAge", "numberOfRunners", "draw",
                "distanceYards", "distanceFurlongs"]:
        if col in filtered.columns:
            filtered[col] = pd.to_numeric(filtered[col], errors="coerce")

    # Compute precise total distance in yards using distanceYards where
    # available (more accurate than the 'distance' column for Irish courses).
    if "distanceFurlongs" in filtered.columns:
        filtered["total_yards"] = (
            filtered["distanceFurlongs"] * 220
            + filtered["distanceYards"].fillna(0)
        )
        # Round to nearest 110 yards (0.5f) to consolidate similar
        # distances while retaining yard-level precision.
        filtered["dist_round"] = (
            (filtered["total_yards"] / 110).round() * 110 / 220
        )
    else:
        # Fallback: round the distance column (furlongs) to 0.5f
        filtered["dist_round"] = (filtered["distance"] * 2).round(0) / 2

    # Build race ID (unique per race)
    filtered["race_id"] = (
        filtered["meetingDate"].astype(str) + "_" +
        filtered["courseName"].astype(str) + "_" +
        filtered["raceNumber"].astype(str)
    )

    # Meeting ID — one per track per day per surface.
    # The going allowance is computed across ALL races sharing this key,
    # because ground conditions are properties of the *meeting*, not a
    # single race.  Turf and AW at the same venue get separate GAs.
    filtered["meeting_id"] = (
        filtered["meetingDate"].astype(str) + "_" +
        filtered["courseName"].astype(str) + "_" +
        filtered["raceSurfaceName"].astype(str)
    )

    # Standard-time key — one per track + rounded distance + surface.
    filtered["std_key"] = (
        filtered["courseName"].astype(str) + "_" +
        filtered["dist_round"].astype(str) + "_" +
        filtered["raceSurfaceName"].astype(str)
    )

    # Parse month from meeting date (for WFA and sex allowance)
    filtered["month"] = pd.to_datetime(
        filtered["meetingDate"], errors="coerce"
    ).dt.month

    print(f"  After UK/IRE flat filter: {len(filtered):,} rows")
    print(f"    Unique courses: {filtered['courseName'].nunique()}")
    print(f"    Unique races:   {filtered['race_id'].nunique():,}")
    return filtered


def apply_surface_change_cutoffs(df):
    """
    Remove rows from tracks that changed surface before the cutoff date.

    Data from before a surface change is not comparable to current times
    and would contaminate standard times, going allowances and figures.
    """
    if not SURFACE_CHANGE_CUTOFFS:
        return df

    date_col = pd.to_datetime(df["meetingDate"], errors="coerce")
    drop_mask = pd.Series(False, index=df.index)

    for course, cutoff_str in SURFACE_CHANGE_CUTOFFS.items():
        cutoff = pd.Timestamp(cutoff_str)
        course_mask = (df["courseName"] == course) & (date_col < cutoff)
        n_drop = course_mask.sum()
        if n_drop > 0:
            print(f"    {course}: dropping {n_drop:,} rows before {cutoff_str}")
        drop_mask |= course_mask

    out = df[~drop_mask].copy()
    print(f"  After surface-change cutoffs: {len(out):,} rows "
          f"(dropped {drop_mask.sum():,})")
    return out


# ═════════════════════════════════════════════════════════════════════
# STAGE 1 — STANDARD TIMES  (per track / distance / surface)
# ═════════════════════════════════════════════════════════════════════

def compute_class_adjustment(race_class, distance_furlongs):
    """
    Class adjustment in seconds for a given class and distance.

    NOTE: Empirical testing shows that applying varying class adjustments
    to standard-time and GA computation HURTS accuracy (r drops from 0.83
    to 0.61).  The standard times are already effectively class-neutral
    because winners across classes produce times that, after going
    correction, cluster around the same median.  Any class effect is
    better handled by the ML model post-hoc.

    Returns a constant baseline adjustment so that the subtraction in
    standard-time computation is equivalent to a no-op (absorbed by
    calibration).
    """
    adj_per_mile = CLASS_ADJUSTMENT_PER_MILE["4"]
    return (adj_per_mile * distance_furlongs) / 8.0


def _filter_std_time_winners(winners):
    """
    Filter winners for standard-time compilation.

    Excludes:
      - Maiden races: maiden winners are typically less competitive and
        run ~0.11s slower, biasing standard times upward.
      - 2yo-only races: 2yo winners run ~0.14s slower than open-age
        races at the same distance, distorting the standard.

    The forum consensus (Rowlands, Prufrock, Mordin) is that handicaps
    confined to older horses give the most reliable times, but excluding
    maidens/2yo while keeping all other types is a practical compromise
    that preserves sample size.
    """
    mask = pd.Series(True, index=winners.index)

    # Exclude maidens (raceCode P* or S*)
    if "raceCode" in winners.columns:
        is_maiden = (
            winners["raceCode"].str.startswith("P", na=False) |
            winners["raceCode"].str.startswith("S", na=False)
        )
        mask &= ~is_maiden

    # Exclude 2yo-only races
    if "eligibilityagemin" in winners.columns:
        is_2yo_only = (
            (winners["eligibilityagemin"] == 2) &
            (winners["eligibilityagemax"].astype(str).isin(["2", "2.0"]))
        )
        mask &= ~is_2yo_only

    n_excluded = (~mask).sum()
    if n_excluded > 0:
        print(f"    Excluded {n_excluded:,} maiden/2yo-only winners "
              f"from standard-time compilation")

    return winners[mask].copy()


def compute_standard_times(df):
    """
    Standard times per track / distance / surface.

    For every unique (course, rounded distance, surface) combination:
      1. Collect all winning times (excluding maidens and 2yo-only)
      2. Prefer races on good/standard going; fall back to all goings
         for combos with sparse good-going data
      3. Apply class adjustment to normalise times
      4. Take the median → this is the standard time
    """
    print("\n  Computing standard times (per track / distance / surface)...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = _filter_std_time_winners(winners)
    winners_good = winners[winners["going"].isin(GOOD_GOING)].copy()

    print(f"    Winners total: {len(winners):,}")
    print(f"    Winners on good/standard going: {len(winners_good):,}")

    # Class-adjust and compute median for good-going winners
    winners_good["class_adj"] = winners_good.apply(
        lambda r: compute_class_adjustment(r["raceClass"], r["distance"]),
        axis=1,
    )
    winners_good["adj_time"] = (
        winners_good["finishingTime"] - winners_good["class_adj"]
    )

    std_times = (
        winners_good.groupby("std_key")
        .agg(
            median_time=("adj_time", "median"),
            mean_time=("adj_time", "mean"),
            n_races=("adj_time", "count"),
            distance=("distance", "first"),
            courseName=("courseName", "first"),
            surface=("raceSurfaceName", "first"),
        )
        .reset_index()
    )

    # Identify combos that need the all-going fallback
    all_std_keys = set(winners["std_key"].unique())
    good_keys = set(
        std_times.loc[
            std_times["n_races"] >= MIN_RACES_STANDARD_TIME, "std_key"
        ]
    )
    needs_fallback = all_std_keys - good_keys

    if needs_fallback:
        winners_all = winners.copy()
        winners_all["class_adj"] = winners_all.apply(
            lambda r: compute_class_adjustment(r["raceClass"], r["distance"]),
            axis=1,
        )
        winners_all["adj_time"] = (
            winners_all["finishingTime"] - winners_all["class_adj"]
        )
        fallback = (
            winners_all[winners_all["std_key"].isin(needs_fallback)]
            .groupby("std_key")
            .agg(
                median_time=("adj_time", "median"),
                mean_time=("adj_time", "mean"),
                n_races=("adj_time", "count"),
                distance=("distance", "first"),
                courseName=("courseName", "first"),
                surface=("raceSurfaceName", "first"),
            )
            .reset_index()
        )
        std_times = std_times[std_times["std_key"].isin(good_keys)]
        std_times = pd.concat([std_times, fallback], ignore_index=True)

    # Keep only combos with enough data
    valid = std_times[std_times["n_races"] >= MIN_RACES_STANDARD_TIME].copy()
    print(
        f"    Standard-time combos (≥ {MIN_RACES_STANDARD_TIME} races): "
        f"{len(valid):,}"
    )
    print(f"    Dropped (insufficient data): {len(std_times) - len(valid):,}")

    std_dict = dict(zip(valid["std_key"], valid["median_time"]))
    return std_dict, valid


def compute_standard_times_iterative(df, going_allowances):
    """
    Recompute standard times using going-corrected times from ALL goings.

    After the first iteration gives us going-allowance estimates, we can
    correct every winner's time for the going effect, then use ALL goings
    (not just good going) to compute more robust standard times.  This
    removes going bias from the standard times and uses much more data.

    Maiden and 2yo-only races are excluded (same filter as initial).
    """
    print("\n    Recomputing standard times (going-corrected, all goings)...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = _filter_std_time_winners(winners)

    # Apply going correction to all winners
    winners["ga"] = winners["meeting_id"].map(going_allowances).fillna(0)
    winners["corrected_time"] = (
        winners["finishingTime"] - (winners["ga"] * winners["distance"])
    )

    # Class adjustment on corrected times
    winners["class_adj"] = winners.apply(
        lambda r: compute_class_adjustment(r["raceClass"], r["distance"]),
        axis=1,
    )
    winners["adj_time"] = winners["corrected_time"] - winners["class_adj"]

    # Standard times from ALL goings (now that we've corrected for going)
    std_agg = (
        winners.groupby("std_key")
        .agg(
            median_time=("adj_time", "median"),
            mean_time=("adj_time", "mean"),
            n_races=("adj_time", "count"),
            distance=("distance", "first"),
            courseName=("courseName", "first"),
            surface=("raceSurfaceName", "first"),
        )
        .reset_index()
    )

    valid = std_agg[std_agg["n_races"] >= MIN_RACES_STANDARD_TIME].copy()
    print(
        f"    Standard-time combos (≥ {MIN_RACES_STANDARD_TIME} races): "
        f"{len(valid):,} (using all goings)"
    )

    std_dict = dict(zip(valid["std_key"], valid["median_time"]))
    return std_dict, valid


# ═════════════════════════════════════════════════════════════════════
# STAGE 2 — COURSE-SPECIFIC LBS PER LENGTH
# ═════════════════════════════════════════════════════════════════════

def generic_lbs_per_length(distance_furlongs, surface=None):
    """
    Generic lbs-per-length from the distance (and optionally surface).
    lpl = seconds_per_length × lbs_per_second_at_distance × surface_mult
    lbs_per_second = 22 × (5 / distance)
    """
    lbs_per_sec = LBS_PER_SECOND_5F * (BENCHMARK_FURLONGS / distance_furlongs)
    base_lpl = SECONDS_PER_LENGTH * lbs_per_sec
    if surface is not None:
        base_lpl *= LPL_SURFACE_MULTIPLIER.get(surface, 1.0)
    return base_lpl


def compute_course_lpl(std_df):
    """
    Derive course-specific lbs-per-length for each standard-time combo.

    Method:
      1. Compute seconds-per-furlong (spf) = standard_time / distance
         for every combo.
      2. Compute the mean spf across ALL courses at each distance band.
      3. Course correction = mean_spf / this_course_spf
         (faster courses → higher lpl because horses spread out more
          at higher speed)
      4. course_lpl = generic_lpl × correction

    Returns a dict: std_key → lbs_per_length
    """
    print("\n  Computing course-specific lbs-per-length...")

    std_df = std_df.copy()
    std_df["spf"] = std_df["median_time"] / std_df["distance"]

    # Mean spf at each (rounded) distance across all courses
    std_df["dist_band"] = std_df["distance"].round(0)
    mean_spf = std_df.groupby("dist_band")["spf"].mean()
    std_df["mean_spf"] = std_df["dist_band"].map(mean_spf)

    # Course correction factor
    std_df["correction"] = std_df["mean_spf"] / std_df["spf"]

    # Generic lpl at this distance
    std_df["generic_lpl"] = std_df["distance"].apply(generic_lbs_per_length)

    # Course-specific lpl with surface multiplier
    std_df["surf_mult"] = std_df["surface"].map(LPL_SURFACE_MULTIPLIER).fillna(1.0)
    std_df["course_lpl"] = (
        std_df["generic_lpl"] * std_df["correction"] * std_df["surf_mult"]
    )

    lpl_dict = dict(zip(std_df["std_key"], std_df["course_lpl"]))

    # Report some examples
    fast = std_df.nlargest(5, "course_lpl")
    slow = std_df.nsmallest(5, "course_lpl")
    print(f"    Computed lpl for {len(lpl_dict):,} track/distance combos")
    print(f"    Highest lpl: {fast[['std_key','course_lpl','correction']].to_string(index=False)}")
    print(f"    Lowest  lpl: {slow[['std_key','course_lpl','correction']].to_string(index=False)}")

    return lpl_dict


# ═════════════════════════════════════════════════════════════════════
# STAGE 3 — GOING ALLOWANCE  (per track, per day)
# ═════════════════════════════════════════════════════════════════════

def compute_going_allowances(df, std_times):
    """
    Going allowance per meeting in seconds-per-furlong (s/f).

    The going allowance captures how the ground at a specific track on a
    specific day affected ALL horses.  It is computed by:

      1. For every WINNER on the card whose course/distance has a known
         standard time, compute:
            deviation_per_furlong = (class_adj_time − standard_time) / distance

      2. Drop extreme global outliers (1st/99th percentile across dataset).

      3. Within each meeting (= track + day + surface), trim the highest
         and lowest deviation, then average the rest.  This card-wide
         average IS the going allowance.

      4. Meetings with fewer than MIN_RACES_GOING_ALLOWANCE winners are
         excluded (unreliable).

    Convention:
      positive GA → ground slower than standard (soft)
      negative GA → ground faster than standard (firm)
    """
    print("\n  Computing going allowances (per track / day)...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = winners[winners["std_key"].isin(std_times)].copy()
    winners["standard_time"] = winners["std_key"].map(std_times)

    # Class-adjust actual time before comparing to class-adjusted standard
    winners["class_adj"] = winners.apply(
        lambda r: compute_class_adjustment(r["raceClass"], r["distance"]),
        axis=1,
    )
    winners["adj_time"] = winners["finishingTime"] - winners["class_adj"]

    # Per-furlong deviation from standard
    winners["deviation"] = winners["adj_time"] - winners["standard_time"]
    winners["dev_per_furlong"] = winners["deviation"] / winners["distance"]

    # Global outlier removal
    q_lo = winners["dev_per_furlong"].quantile(0.01)
    q_hi = winners["dev_per_furlong"].quantile(0.99)
    winners = winners[
        winners["dev_per_furlong"].between(q_lo, q_hi)
    ]

    # Winsorized mean within each meeting (more robust than trimmed mean:
    # replaces extremes rather than discarding them, retaining more data).
    def _trimmed_mean(group):
        vals = group.sort_values().values.copy()
        n = len(vals)
        if n <= 2:
            return np.mean(vals)
        # Winsorize: clamp extremes to adjacent values
        vals[0] = vals[1]
        vals[-1] = vals[-2]
        return np.mean(vals)

    ga_series = (
        winners.groupby("meeting_id")["dev_per_furlong"]
        .apply(_trimmed_mean)
    )
    ga_count = (
        winners.groupby("meeting_id")["dev_per_furlong"].count()
    )

    # Keep only meetings with enough races
    valid_ids = ga_count[ga_count >= MIN_RACES_GOING_ALLOWANCE].index
    ga_dict = ga_series[ga_series.index.isin(valid_ids)].to_dict()

    print(f"    Meetings with going allowance: {len(ga_dict):,}")
    print(
        f"    GA range: {min(ga_dict.values()):.3f} to "
        f"{max(ga_dict.values()):.3f} s/f"
    )
    print(f"    GA mean:  {np.mean(list(ga_dict.values())):.3f} s/f")

    return ga_dict


# ═════════════════════════════════════════════════════════════════════
# STAGE 4 — WINNER SPEED FIGURES
# ═════════════════════════════════════════════════════════════════════

def compute_winner_figures(df, std_times, going_allowances, lpl_dict):
    """
    Speed figures for race winners.

    For each winner whose course/distance has a standard time AND whose
    meeting has a going allowance:

      1. going-corrected time = actual_time − (GA × distance)
         NOTE: class adjustment is NOT applied here.  The standard times
         and GA were computed using class-adjusted times (to remove class
         noise from those estimates), but the figure itself should reflect
         ACTUAL raw speed.  This way, a Group 1 winner who is genuinely
         faster gets a higher figure than a Class 6 winner at the same
         course/distance.  The deviation from the class-normalised
         standard captures both the horse's ability AND the class level.
      2. deviation = corrected_time − standard_time
      3. deviation_lbs = deviation_seconds / seconds_per_length × lpl
      4. winner_figure = BASE_RATING − deviation_lbs
    """
    print("\n  Computing winner speed figures...")

    w = df[df["positionOfficial"] == 1].copy()
    w = w[
        w["std_key"].isin(std_times) &
        w["meeting_id"].isin(going_allowances)
    ].copy()

    w["standard_time"] = w["std_key"].map(std_times)
    w["going_allowance"] = w["meeting_id"].map(going_allowances)

    # Going-corrected time (NO class adjustment — figure reflects raw speed)
    w["corrected_time"] = (
        w["finishingTime"]
        - (w["going_allowance"] * w["distance"])
    )

    # Deviation from standard
    w["deviation_seconds"] = w["corrected_time"] - w["standard_time"]
    w["deviation_lengths"] = w["deviation_seconds"] / SECONDS_PER_LENGTH

    # Course-specific lbs-per-length (fall back to generic+surface if missing)
    w["lpl"] = w["std_key"].map(lpl_dict)
    missing_lpl = w["lpl"].isna()
    if missing_lpl.any():
        w.loc[missing_lpl, "lpl"] = w.loc[missing_lpl].apply(
            lambda r: generic_lbs_per_length(
                r["distance"], r.get("raceSurfaceName")
            ),
            axis=1,
        )

    w["deviation_lbs"] = w["deviation_lengths"] * w["lpl"]
    w["raw_figure"] = BASE_RATING - w["deviation_lbs"]

    print(f"    Winner figures computed: {len(w):,}")
    print(f"    Range: {w['raw_figure'].min():.0f} to {w['raw_figure'].max():.0f}")
    print(f"    Mean:  {w['raw_figure'].mean():.1f}")
    print(f"    Median:{w['raw_figure'].median():.1f}")

    winner_fig = dict(zip(w["race_id"], w["raw_figure"]))
    return w, winner_fig


# ═════════════════════════════════════════════════════════════════════
# STAGE 5 — ALL-RUNNER FIGURES  (beaten lengths)
# ═════════════════════════════════════════════════════════════════════

def compute_all_figures(df, winner_fig_dict, lpl_dict):
    """
    Extend figures to every runner via cumulative beaten lengths.

    horse_figure = winner_figure − (attenuated_bl × course_lpl)

    Beaten lengths use a soft cap: full precision up to
    BL_ATTENUATION_THRESHOLD, then reduced beyond that to attenuate
    noise from eased/tailed-off horses.  The threshold was lowered
    from 20L to 8L based on empirical analysis showing monotonically
    rising positive bias from ~5L onwards (scripts/analyse_lpl.py).
    """
    print("\n  Extending figures to all runners...")

    out = df[df["race_id"].isin(winner_fig_dict)].copy()
    out["winner_figure"] = out["race_id"].map(winner_fig_dict)

    # Course-specific lpl, with generic+surface fallback
    out["lpl"] = out["std_key"].map(lpl_dict)
    missing = out["lpl"].isna()
    if missing.any():
        out.loc[missing, "lpl"] = out.loc[missing].apply(
            lambda r: generic_lbs_per_length(
                r["distance"], r.get("raceSurfaceName")
            ),
            axis=1,
        )

    is_winner = out["positionOfficial"] == 1
    cum_raw = out["distanceCumulative"].fillna(0).clip(lower=0)

    # Soft cap: full precision up to BL_ATTENUATION_THRESHOLD,
    # then attenuated beyond.  Lowered from 20L based on analysis
    # showing beaten-length bias rises from ~5L onwards.
    T = BL_ATTENUATION_THRESHOLD
    F = BL_ATTENUATION_FACTOR
    cum = np.where(cum_raw <= T, cum_raw, T + F * (cum_raw - T))

    out["lbs_behind"] = cum * out["lpl"]
    out.loc[is_winner, "lbs_behind"] = 0.0
    out["raw_figure"] = out["winner_figure"] - out["lbs_behind"]

    # Non-finishers (no position or position == 0) should not receive a figure
    no_pos = out["positionOfficial"].isna() | (out["positionOfficial"] == 0)
    out.loc[no_pos, "raw_figure"] = np.nan

    print(f"    All-runner figures: {len(out):,}")
    return out


# ═════════════════════════════════════════════════════════════════════
# STAGE 6 — WEIGHT-CARRIED ADJUSTMENT
# ═════════════════════════════════════════════════════════════════════

def apply_weight_adjustment(df):
    """
    Adjust for weight carried.

    figure += (weight_carried − base_weight)

    A horse carrying more than 9st 0lb gets a positive adjustment
    (it achieved its time despite carrying extra weight, so it is
    credited for the additional burden).
    """
    print("\n  Applying weight-carried adjustment...")

    has_w = df["weightCarried"].notna()
    print(f"    Runners with weight data: {has_w.sum():,} / {len(df):,}")

    df["weight_adj"] = 0.0
    df.loc[has_w, "weight_adj"] = (
        df.loc[has_w, "weightCarried"] - BASE_WEIGHT_LBS
    )
    df["figure_after_weight"] = df["raw_figure"] + df["weight_adj"]
    return df


# ═════════════════════════════════════════════════════════════════════
# STAGE 7 — WEIGHT FOR AGE  (WFA)
# ═════════════════════════════════════════════════════════════════════

def get_wfa_allowance(age, month, distance, surface=None):
    """
    WFA allowance in lbs for a given age, month, distance, and surface.
    Returns the amount to ADD to the figure.

    Uses surface-specific empirical tables derived from Timeform data.
    Falls back to Turf tables if surface is not specified.
    Includes older-horse decline adjustment for ages 7+ on Turf.
    Uses linear interpolation between distance brackets.
    """
    if pd.isna(age) or pd.isna(month) or pd.isna(distance):
        return 0.0

    age, month, distance = int(age), int(month), float(distance)
    is_aw = surface is not None and "Weather" in str(surface)

    # Older horse decline (Turf only, ages 7+)
    if age >= 7 and not is_aw:
        return float(OLDER_DECLINE_TURF.get(min(age, 12), -6))

    if age >= 4:
        return 0.0

    if age == 3:
        table = WFA_3YO_AW if is_aw else WFA_3YO_TURF
    elif age == 2:
        table = WFA_2YO_AW if is_aw else WFA_2YO_TURF
    else:
        return 0.0

    if month not in table:
        return 0.0

    mt = table[month]
    dists = sorted(mt.keys())

    if distance <= dists[0]:
        return float(mt[dists[0]])
    if distance >= dists[-1]:
        return float(mt[dists[-1]])

    # Interpolate between distance brackets
    for i in range(len(dists) - 1):
        if dists[i] <= distance <= dists[i + 1]:
            lo, hi = dists[i], dists[i + 1]
            frac = (distance - lo) / (hi - lo)
            return mt[lo] + frac * (mt[hi] - mt[lo])
    return 0.0


def apply_wfa_adjustment(df):
    """
    Weight-for-age adjustment.
    Younger horses (2yo, 3yo) receive an upward adjustment that
    compensates for physical immaturity.  Older horses (7+) on Turf
    receive a small downward adjustment for age-related decline.
    """
    print("\n  Applying WFA adjustment...")

    df["wfa_adj"] = df.apply(
        lambda r: get_wfa_allowance(
            r["horseAge"], r["month"], r["distance"],
            r.get("raceSurfaceName"),
        ),
        axis=1,
    )

    has = df["wfa_adj"] > 0
    print(f"    Runners with WFA: {has.sum():,} / {len(df):,}")
    if has.any():
        print(
            f"    WFA range: {df.loc[has, 'wfa_adj'].min():.1f} – "
            f"{df.loc[has, 'wfa_adj'].max():.1f} lbs"
        )

    df["figure_after_wfa"] = df["figure_after_weight"] + df["wfa_adj"]
    return df


# ═════════════════════════════════════════════════════════════════════
# STAGE 8 — SEX ALLOWANCE
# ═════════════════════════════════════════════════════════════════════

def apply_sex_allowance(df):
    """
    Sex allowance for fillies/mares.

    FINDING: Empirical testing shows that Timeform's timefigure does NOT
    include a sex adjustment.  Adding one reduces correlation by ~0.006.
    The weight-carried adjustment already captures the actual weight the
    filly carried (which includes the racing sex allowance), so the
    figure already reflects her performance at that weight.

    We record the sex_adj column as 0 for transparency but do NOT apply
    it to figure_final.
    """
    print("\n  Sex allowance: NOT applied (empirically hurts vs timefigure)")
    df["sex_adj"] = 0.0
    df["figure_final"] = df["figure_after_wfa"]
    return df


# ═════════════════════════════════════════════════════════════════════
# STAGE 9 — CALIBRATION & VALIDATION
# ═════════════════════════════════════════════════════════════════════

def calibrate_figures(df):
    """
    Surface-specific linear calibration to the Timeform timefigure scale,
    with per-class residual correction.

    Turf and AW have substantially different calibration slopes, so fitting
    them separately reduces error.  Each surface gets its own
      timefigure ≈ a × figure_final + b + class_offset[class]
    fitted on 2015–2023.

    The class offset captures systematic biases that a single linear
    slope cannot (e.g. Group 1 figures consistently over/under-predicted).
    """
    print("\n  Calibrating to Timeform scale (surface + class)...")

    mask = (
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_final"].notna()
    )

    # Exclude beaten-far runners from calibration FIT — their figures
    # are noisy and drag the regression slope down.
    fit_mask = mask & (
        (df["distanceCumulative"].fillna(0) <= 20)
        | (df["positionOfficial"] == 1)
    )

    df["figure_calibrated"] = np.nan
    cal_params = {}

    for surface in df["raceSurfaceName"].unique():
        surf_mask = df["raceSurfaceName"] == surface
        fit = df[fit_mask & surf_mask & (df["source_year"] <= 2023)]

        if len(fit) < 100:
            print(f"    {surface}: insufficient data — using identity")
            df.loc[surf_mask, "figure_calibrated"] = df.loc[
                surf_mask, "figure_final"
            ]
            cal_params[surface] = (1.0, 0.0, 0.0, {})
            continue

        x = fit["figure_final"].values
        y = fit["timefigure"].values
        x_mean = x.mean()

        # Try quadratic calibration: timefigure = a*x + a2*(x-mean)^2 + b
        x_c = x - x_mean
        A_quad = np.vstack([x, x_c ** 2, np.ones(len(x))]).T
        (a, a2, b), *_ = np.linalg.lstsq(A_quad, y, rcond=None)

        # Check if quadratic actually helps (lower MSE than linear)
        A_lin = np.vstack([x, np.ones(len(x))]).T
        (a_lin, b_lin), *_ = np.linalg.lstsq(A_lin, y, rcond=None)
        mse_lin = np.mean((y - a_lin * x - b_lin) ** 2)
        mse_quad = np.mean((y - a * x - a2 * x_c ** 2 - b) ** 2)

        if mse_quad < mse_lin * 0.999:
            # Quadratic is meaningfully better
            xf = df.loc[surf_mask, "figure_final"].values
            xf_c = xf - x_mean
            df.loc[surf_mask, "figure_calibrated"] = (
                a * xf + a2 * xf_c ** 2 + b
            )
            print(
                f"    {surface:<15}: timefigure ≈ {a:.4f}×fig "
                f"+ {a2:.6f}×(fig-{x_mean:.0f})² + {b:.2f}  "
                f"[quad MSE {mse_quad:.2f} vs lin {mse_lin:.2f}]"
            )
        else:
            # Linear is sufficient
            a, b, a2 = a_lin, b_lin, 0.0
            df.loc[surf_mask, "figure_calibrated"] = (
                a * df.loc[surf_mask, "figure_final"] + b
            )
            print(
                f"    {surface:<15}: timefigure ≈ {a:.4f} × figure + {b:.2f}"
            )

        # Per-class residual correction
        fit_pred = (
            a * x + a2 * (x - x_mean) ** 2 + b if a2 != 0
            else a * x + b
        )
        residuals = y - fit_pred
        class_offsets = (
            pd.Series(residuals, index=fit.index)
            .groupby(fit["raceClass"])
            .mean()
        )
        class_offset_dict = class_offsets.to_dict()

        surf_class_adj = df.loc[surf_mask, "raceClass"].map(
            class_offset_dict
        ).fillna(0)
        df.loc[surf_mask, "figure_calibrated"] += surf_class_adj.values

        # Recompute residuals after class offset for course/going layers
        fit_pred_with_class = fit_pred + (
            fit["raceClass"].map(class_offset_dict).fillna(0).values
        )
        residuals2 = y - fit_pred_with_class

        # Per course×distance residual correction (with shrinkage).
        # Directly corrects standard-time errors per track/distance combo.
        SHRINKAGE_K = 100  # regularisation strength
        cd_key = (
            fit["courseName"] + "_" +
            fit["distance"].round(0).astype(int).astype(str)
        )
        cd_groups = pd.Series(
            residuals2, index=fit.index
        ).groupby(cd_key)
        cd_means = cd_groups.mean()
        cd_counts = cd_groups.count()
        cd_shrunk = cd_means * cd_counts / (cd_counts + SHRINKAGE_K)
        course_dist_offset_dict = cd_shrunk.to_dict()

        all_cd_key = (
            df.loc[surf_mask, "courseName"] + "_" +
            df.loc[surf_mask, "distance"].round(0).astype(int).astype(str)
        )
        surf_cd_adj = all_cd_key.map(course_dist_offset_dict).fillna(0)
        df.loc[surf_mask, "figure_calibrated"] += surf_cd_adj.values

        # Per-going-group residual correction (with shrinkage).
        # Captures residual going biases after GA correction.
        going_groups = {
            "Firm": ["Hard", "Firm", "Fast"],
            "GdFm": ["Gd/Frm", "Good To Firm", "Good to Firm",
                      "Std/Fast"],
            "Good": ["Good", "Standard", "Std"],
            "GdSft": ["Gd/Sft", "Good to Soft", "Good To Yielding",
                       "Good to Yielding", "Std/Slow", "Standard/Slow",
                       "Standard To Slow", "Standard to Slow", "Slow"],
            "Soft": ["Soft", "Yielding", "Yld/Sft", "Sft/Hvy",
                      "Hvy/Sft"],
            "Heavy": ["Heavy"],
        }
        going_map = {}
        for grp, goings in going_groups.items():
            for g in goings:
                going_map[g] = grp

        fit_going_grp = fit["going"].map(going_map).fillna("Good")
        fit_cd_key = (
            fit["courseName"] + "_" +
            fit["distance"].round(0).astype(int).astype(str)
        )
        residuals3 = residuals2 - (
            fit_cd_key.map(course_dist_offset_dict).fillna(0).values
        )
        going_grp_groups = pd.Series(
            residuals3, index=fit.index
        ).groupby(fit_going_grp)
        going_means = going_grp_groups.mean()
        going_counts = going_grp_groups.count()
        going_shrunk = going_means * going_counts / (
            going_counts + SHRINKAGE_K
        )
        going_offset_dict = going_shrunk.to_dict()

        df_going_grp = df.loc[surf_mask, "going"].map(going_map).fillna(
            "Good"
        )
        surf_going_adj = df_going_grp.map(going_offset_dict).fillna(0)
        df.loc[surf_mask, "figure_calibrated"] += surf_going_adj.values

        # Continuous GA correction: linear function of the actual
        # going allowance value to capture finer-grained effects
        # beyond what 6 categorical groups can resolve.  Time-based
        # going (GA) is more accurate than official descriptions
        # (analysis: MAE 7.97 vs 8.49).
        residuals3a = residuals3 - (
            fit_going_grp.map(going_offset_dict).fillna(0).values
        )
        fit_ga = fit["ga_value"].values if "ga_value" in fit.columns else (
            np.zeros(len(fit))
        )
        ga_has_value = fit_ga != 0
        ga_coeff = 0.0
        if ga_has_value.sum() > 200:
            ga_coeff = (
                np.sum(fit_ga[ga_has_value] * residuals3a[ga_has_value])
                / (np.sum(fit_ga[ga_has_value] ** 2) + 1e-6)
            )

        all_ga = (
            df.loc[surf_mask, "ga_value"].values
            if "ga_value" in df.columns else np.zeros(surf_mask.sum())
        )
        surf_ga_cont_adj = ga_coeff * all_ga
        df.loc[surf_mask, "figure_calibrated"] += surf_ga_cont_adj

        # Per-beaten-length-band residual correction (with shrinkage).
        # Corrects the systematic positive bias that grows with margin:
        # horses beaten further are consistently over-rated because
        # judge's margin estimates compress at larger distances.
        residuals4 = residuals3a - (ga_coeff * fit_ga)
        fit_bl = fit["distanceCumulative"].fillna(0).clip(lower=0)
        fit_bl_band = pd.cut(
            fit_bl, bins=[0, 1, 3, 5, 10, 15, 20],
            labels=["0-1", "1-3", "3-5", "5-10", "10-15", "15-20"],
        ).astype(str).fillna("0-1")
        # Winners get their own band (unaffected by LPL)
        fit_bl_band = fit_bl_band.where(
            fit["positionOfficial"] != 1, "winner"
        )
        bl_groups = pd.Series(
            residuals4, index=fit.index
        ).groupby(fit_bl_band)
        bl_means = bl_groups.mean()
        bl_counts = bl_groups.count()
        bl_shrunk = bl_means * bl_counts / (bl_counts + SHRINKAGE_K)
        bl_offset_dict = bl_shrunk.to_dict()

        all_bl = df.loc[surf_mask, "distanceCumulative"].fillna(0).clip(lower=0)
        all_bl_band = pd.cut(
            all_bl, bins=[0, 1, 3, 5, 10, 15, 20],
            labels=["0-1", "1-3", "3-5", "5-10", "10-15", "15-20"],
        ).astype(str).fillna("0-1")
        all_bl_band = all_bl_band.where(
            df.loc[surf_mask, "positionOfficial"] != 1, "winner"
        )
        surf_bl_adj = all_bl_band.map(bl_offset_dict).fillna(0)
        df.loc[surf_mask, "figure_calibrated"] += surf_bl_adj.values

        # Per-age-group residual correction (with shrinkage).
        # WFA tables leave persistent per-age biases that a single
        # calibration slope cannot capture (e.g. 2yo +1.0 lb, 4-6yo +0.8).
        residuals5 = residuals4 - (
            fit_bl_band.map(bl_offset_dict).fillna(0).values
        )
        fit_age = fit["horseAge"].clip(upper=12).astype(int).astype(str)
        age_groups = pd.Series(
            residuals5, index=fit.index
        ).groupby(fit_age)
        age_means = age_groups.mean()
        age_counts = age_groups.count()
        age_shrunk = age_means * age_counts / (age_counts + SHRINKAGE_K)
        age_offset_dict = age_shrunk.to_dict()

        all_age = (
            df.loc[surf_mask, "horseAge"].clip(upper=12)
            .astype(int).astype(str)
        )
        surf_age_adj = all_age.map(age_offset_dict).fillna(0)
        df.loc[surf_mask, "figure_calibrated"] += surf_age_adj.values

        cal_params[surface] = (
            a, b, a2, class_offset_dict, course_dist_offset_dict,
            going_offset_dict, ga_coeff, bl_offset_dict, age_offset_dict,
        )
        if class_offset_dict:
            offsets_str = ", ".join(
                f"C{k}:{v:+.1f}" for k, v in sorted(
                    class_offset_dict.items(),
                    key=lambda x: str(x[0])
                )
            )
            print(f"      class offsets: {offsets_str}")
        n_cd = sum(1 for v in course_dist_offset_dict.values()
                   if abs(v) > 0.5)
        print(f"      course×dist offsets: {n_cd}/{len(course_dist_offset_dict)} combos with |offset|>0.5")
        going_str = ", ".join(
            f"{k}:{v:+.1f}" for k, v in sorted(
                going_offset_dict.items()
            )
        )
        print(f"      going offsets: {going_str}")
        print(f"      continuous GA coeff: {ga_coeff:+.2f} lbs per s/f")
        bl_str = ", ".join(
            f"{k}:{v:+.1f}" for k, v in sorted(
                bl_offset_dict.items()
            )
        )
        print(f"      beaten-length offsets: {bl_str}")
        age_str = ", ".join(
            f"age{k}:{v:+.1f}" for k, v in sorted(
                age_offset_dict.items()
            )
        )
        print(f"      age offsets: {age_str}")

    # Exclude non-finishers (no official position)
    no_position = (
        (df["positionOfficial"].isna() | (df["positionOfficial"] == 0))
        & df["figure_calibrated"].notna()
    )
    n_no_pos = no_position.sum()
    if n_no_pos > 0:
        df.loc[no_position, "figure_calibrated"] = np.nan
        print(f"    Excluded {n_no_pos:,} non-finishers (no position)")

    # Exclude runners beaten > 20 lengths — figures are unreliable
    # (horses eased down / not running to the line).
    beaten_far = (
        df["distanceCumulative"].notna()
        & (df["distanceCumulative"] > 20)
        & (df["positionOfficial"] != 1)
    )
    n_excluded = beaten_far.sum()
    if n_excluded > 0:
        df.loc[beaten_far, "figure_calibrated"] = np.nan
        print(f"    Excluded {n_excluded:,} runners beaten > 20 lengths")

    return df, cal_params


def enhance_with_gbr(df):
    """
    Stage 10: Stacked GBR enhancement of calibrated figures.

    Trains a Gradient Boosted Regression model per surface that uses
    figure_calibrated as the primary feature (~92% importance) plus
    auxiliary features (race class, distance, going, etc.) to reduce
    residual bias — particularly in the 70-130 figure range where the
    linear calibration under-predicts by +5 lbs.

    Analysis showed this reduces:
      Turf:  overall MAE 8.82→8.08 (-8%), 70-130 MAE 9.39→7.32 (-22%)
      AW:    overall MAE 6.46→6.22 (-4%), 70-130 MAE 6.86→6.09 (-11%)
    """
    if not HAS_SKLEARN:
        print("    scikit-learn not available — skipping GBR enhancement")
        return df, {}

    print("\n  Training stacked GBR per surface...")

    # ── Feature engineering ──
    # Ordinal going encoding (firm=1 .. heavy=6)
    going_ordinal = {
        "Hard": 1, "Firm": 1, "Fast": 1,
        "Gd/Frm": 2, "Good To Firm": 2, "Good to Firm": 2,
        "Std/Fast": 2,
        "Good": 3, "Standard": 3, "Std": 3,
        "Gd/Sft": 4, "Good to Soft": 4, "Good To Yielding": 4,
        "Good to Yielding": 4, "Std/Slow": 4, "Standard/Slow": 4,
        "Standard To Slow": 4, "Standard to Slow": 4, "Slow": 4,
        "Soft": 5, "Yielding": 5, "Yld/Sft": 5, "Sft/Hvy": 5,
        "Hvy/Sft": 5,
        "Heavy": 6,
    }
    df["going_num"] = df["going"].map(going_ordinal).fillna(3)

    # Course frequency (how often a course appears — proxy for data quality)
    course_counts = df["courseName"].value_counts()
    df["course_freq"] = (
        df["courseName"].map(course_counts).fillna(0) / len(df)
    )

    FEATURES = [
        "figure_calibrated", "figure_final", "raceClass",
        "distance", "horseAge", "positionOfficial",
        "weightCarried", "ga_value", "going_num", "course_freq",
    ]

    mask = (
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_calibrated"].notna()
    )

    gbr_models = {}

    for surface in df["raceSurfaceName"].unique():
        surf_mask = df["raceSurfaceName"] == surface
        fit_mask = mask & surf_mask & (df["source_year"] <= 2023)
        fit = df[fit_mask].copy()

        if len(fit) < 1000:
            print(f"    {surface}: insufficient data — skipping GBR")
            continue

        # Ensure all features exist and are numeric
        for col in FEATURES:
            if col not in fit.columns:
                fit[col] = 0
            fit[col] = pd.to_numeric(fit[col], errors="coerce").fillna(0)

        X_fit = fit[FEATURES].values
        y_fit = fit["timefigure"].values

        gbr = GradientBoostingRegressor(
            n_estimators=300,
            max_depth=5,
            learning_rate=0.08,
            subsample=0.8,
            min_samples_leaf=50,
            random_state=42,
        )
        gbr.fit(X_fit, y_fit)
        gbr_models[surface] = gbr

        # Apply to ALL rows for this surface (not just fit window)
        all_surf = df[surf_mask].copy()
        for col in FEATURES:
            if col not in all_surf.columns:
                all_surf[col] = 0
            all_surf[col] = pd.to_numeric(
                all_surf[col], errors="coerce"
            ).fillna(0)

        # Only predict where figure_calibrated exists
        has_fig = all_surf["figure_calibrated"].notna() & (
            all_surf["figure_calibrated"] != 0
        )
        if has_fig.sum() > 0:
            X_pred = all_surf.loc[has_fig, FEATURES].values
            preds = gbr.predict(X_pred)
            df.loc[all_surf.index[has_fig], "figure_calibrated"] = preds

        # Report feature importances
        importances = dict(zip(FEATURES, gbr.feature_importances_))
        top5 = sorted(importances.items(), key=lambda x: -x[1])[:5]
        imp_str = ", ".join(f"{k}:{v:.3f}" for k, v in top5)
        print(f"    {surface:<15}: {len(fit):,} training rows, "
              f"top features: {imp_str}")

        # Report fit-window metrics
        fit_pred = gbr.predict(X_fit)
        fit_err = fit_pred - y_fit
        fit_mae = np.abs(fit_err).mean()
        fit_corr = np.corrcoef(fit_pred, y_fit)[0, 1]
        print(f"    {'':15}  fit MAE={fit_mae:.2f}, corr={fit_corr:.4f}")

    # Clean up temporary columns
    df.drop(columns=["going_num", "course_freq"], inplace=True)

    return df, gbr_models


def validate_figures(df):
    """Validate calibrated figures against Timeform timefigure."""
    print("\n" + "=" * 70)
    print("VALIDATION AGAINST TIMEFIGURE")
    print("=" * 70)

    valid = df[
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
    ].copy()
    print(f"\n  Rows with valid timefigure: {len(valid):,}")
    if len(valid) == 0:
        print("  No rows — skipping")
        return None

    fc = (
        "figure_calibrated"
        if "figure_calibrated" in valid.columns
        else "figure_final"
    )

    corr = valid[fc].corr(valid["timefigure"])
    err = valid[fc] - valid["timefigure"]
    mae = err.abs().mean()
    rmse = np.sqrt((err ** 2).mean())

    print(f"  Correlation: {corr:.4f}")
    print(f"  MAE:  {mae:.2f} lbs")
    print(f"  RMSE: {rmse:.2f} lbs")
    print(f"  Bias: {err.mean():+.2f} lbs")

    print(f"\n  Error distribution:")
    for t in [1, 2, 3, 5, 10, 15, 20]:
        pct = (err.abs() <= t).mean() * 100
        print(f"    ±{t:>2} lbs: {pct:.1f}%")

    print(f"\n  By finishing position:")
    for pos in [1, 2, 3, 4, 5]:
        s = valid[valid["positionOfficial"] == pos]
        if len(s) > 100:
            c = s[fc].corr(s["timefigure"])
            m = (s[fc] - s["timefigure"]).abs().mean()
            print(f"    Pos {pos}: r={c:.4f}  MAE={m:.2f}  (n={len(s):,})")

    print(f"\n  By surface:")
    for surf in valid["raceSurfaceName"].unique():
        s = valid[valid["raceSurfaceName"] == surf]
        if len(s) > 100:
            c = s[fc].corr(s["timefigure"])
            m = (s[fc] - s["timefigure"]).abs().mean()
            print(f"    {surf:<15} r={c:.4f}  MAE={m:.2f}  (n={len(s):,})")

    print(f"\n  By year:")
    for yr in sorted(valid["source_year"].unique()):
        s = valid[valid["source_year"] == yr]
        if len(s) > 100:
            c = s[fc].corr(s["timefigure"])
            m = (s[fc] - s["timefigure"]).abs().mean()
            print(f"    {yr}: r={c:.4f}  MAE={m:.2f}  (n={len(s):,})")

    print(f"\n  Scale comparison:")
    print(f"    {'':15} {'Ours':>10} {'TFig':>10}")
    print(f"    {'Mean':15} {valid[fc].mean():10.1f} {valid['timefigure'].mean():10.1f}")
    print(f"    {'Median':15} {valid[fc].median():10.1f} {valid['timefigure'].median():10.1f}")
    print(f"    {'Std':15} {valid[fc].std():10.1f} {valid['timefigure'].std():10.1f}")

    return valid


# ═════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═════════════════════════════════════════════════════════════════════

def run_pipeline():
    """Execute the full speed-figure pipeline."""
    print("=" * 70)
    print("SPEED FIGURE PIPELINE")
    print("=" * 70)

    # 0 — Load data
    print("\nSTAGE 0: Loading data")
    df = load_data()
    df = filter_uk_ire_flat(df)
    df = apply_surface_change_cutoffs(df)

    # 1 — Standard times (per track / distance / surface)
    print("\nSTAGE 1: Standard times (initial — good going only)")
    std_times, std_df = compute_standard_times(df)

    # 2 — Going allowance (initial)
    print("\nSTAGE 2: Going allowances (initial)")
    going_allowances = compute_going_allowances(df, std_times)

    # Iterative refinement: use going-corrected times to recompute
    # standard times (using ALL goings now), then recompute GA.
    N_ITER = 3
    for iteration in range(N_ITER):
        print(f"\n  ── Iterative refinement {iteration + 1}/{N_ITER} ──")
        std_times, std_df = compute_standard_times_iterative(
            df, going_allowances
        )
        going_allowances = compute_going_allowances(df, std_times)

    # 3 — Course-specific lbs per length
    print("\nSTAGE 3: Lbs per length (per track / distance)")
    lpl_dict = compute_course_lpl(std_df)

    # 4 — Winner figures
    print("\nSTAGE 4: Winner speed figures")
    winners, winner_fig_dict = compute_winner_figures(
        df, std_times, going_allowances, lpl_dict
    )

    # 5 — All-runner figures via beaten lengths
    print("\nSTAGE 5: All-runner figures (beaten lengths)")
    all_figs = compute_all_figures(df, winner_fig_dict, lpl_dict)

    # 6 — Weight-carried adjustment
    print("\nSTAGE 6: Weight-carried adjustment")
    all_figs = apply_weight_adjustment(all_figs)

    # 7 — Weight for age
    print("\nSTAGE 7: Weight for age (WFA)")
    all_figs = apply_wfa_adjustment(all_figs)

    # 8 — Sex allowance
    print("\nSTAGE 8: Sex allowance")
    all_figs = apply_sex_allowance(all_figs)

    # Attach GA value for continuous going correction in calibration
    all_figs["ga_value"] = all_figs["meeting_id"].map(
        going_allowances
    ).fillna(0)

    # 9 — Calibration & validation
    print("\nSTAGE 9: Calibration")
    all_figs, cal_params = calibrate_figures(all_figs)

    # 10 — Stacked GBR enhancement
    print("\nSTAGE 10: GBR enhancement")
    all_figs, gbr_models = enhance_with_gbr(all_figs)

    print("\nSTAGE 11: Validation")
    validate_figures(all_figs)

    # Also save the going-corrected total yards if available
    if "total_yards" in df.columns and "total_yards" not in all_figs.columns:
        ty_map = df[["race_id", "horseCode", "total_yards"]].copy()
        ty_map["_mk"] = ty_map["race_id"] + "_" + ty_map["horseCode"].astype(str)
        all_figs["_mk"] = all_figs["race_id"] + "_" + all_figs["horseCode"].astype(str)
        ty_dict = dict(zip(ty_map["_mk"], ty_map["total_yards"]))
        all_figs["total_yards"] = all_figs["_mk"].map(ty_dict)
        all_figs.drop(columns=["_mk"], inplace=True)

    # ── Save outputs ──
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    out_cols = [
        "meetingDate", "courseName", "raceNumber", "race_id",
        "horseName", "horseCode", "positionOfficial",
        "distance", "going", "raceSurfaceName", "raceClass",
        "horseAge", "horseGender", "weightCarried",
        "finishingTime", "distanceCumulative",
        "raw_figure", "weight_adj", "wfa_adj", "sex_adj",
        "figure_final", "figure_calibrated",
        "timefigure", "performanceRating",
        "source_year",
    ]
    out_cols = [c for c in out_cols if c in all_figs.columns]
    out_df = all_figs[out_cols]

    fig_path = os.path.join(OUTPUT_DIR, "speed_figures.csv")
    out_df.to_csv(fig_path, index=False)
    print(f"\n  Figures: {fig_path} ({len(out_df):,} rows)")

    std_path = os.path.join(OUTPUT_DIR, "standard_times.csv")
    std_df.to_csv(std_path, index=False)
    print(f"  Standard times: {std_path} ({len(std_df):,} entries)")

    ga_df = pd.DataFrame(
        list(going_allowances.items()),
        columns=["meeting_id", "going_allowance_spf"],
    )
    ga_path = os.path.join(OUTPUT_DIR, "going_allowances.csv")
    ga_df.to_csv(ga_path, index=False)
    print(f"  Going allowances: {ga_path} ({len(ga_df):,} meetings)")

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)

    return all_figs, std_times, going_allowances


if __name__ == "__main__":
    run_pipeline()
