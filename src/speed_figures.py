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
  9. Calibrate & validate against Timeform timefigure
"""

import pandas as pd
import numpy as np
import os
import warnings

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# ─────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")

BASE_RATING = 100          # A 100-rated horse matches standard time on good going
BASE_WEIGHT_LBS = 126      # 9st 0lb — flat racing base weight
SECONDS_PER_LENGTH = 0.2   # BHA standard
LBS_PER_SECOND_5F = 22     # Industry standard at 5 furlongs
BENCHMARK_FURLONGS = 5.0   # Anchor distance

# Minimum sample sizes
MIN_RACES_STANDARD_TIME = 15   # Minimum winners for a reliable standard time
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
OLDER_DECLINE_TURF = {7: -1, 8: -2, 9: -3, 10: -4, 11: -6, 12: -6}

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


def compute_standard_times(df):
    """
    Standard times per track / distance / surface.

    For every unique (course, rounded distance, surface) combination:
      1. Collect all winning times
      2. Prefer races on good/standard going; fall back to all goings
         for combos with sparse good-going data
      3. Apply class adjustment to normalise times
      4. Take the median → this is the standard time
    """
    print("\n  Computing standard times (per track / distance / surface)...")

    winners = df[df["positionOfficial"] == 1].copy()
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
    """
    print("\n    Recomputing standard times (going-corrected, all goings)...")

    winners = df[df["positionOfficial"] == 1].copy()

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

def generic_lbs_per_length(distance_furlongs):
    """
    Generic lbs-per-length from the distance alone.
    lpl = seconds_per_length × lbs_per_second_at_distance
    lbs_per_second = 22 × (5 / distance)
    """
    lbs_per_sec = LBS_PER_SECOND_5F * (BENCHMARK_FURLONGS / distance_furlongs)
    return SECONDS_PER_LENGTH * lbs_per_sec


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

    # Course-specific lpl
    std_df["course_lpl"] = std_df["generic_lpl"] * std_df["correction"]

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

    # Trimmed mean within each meeting
    def _trimmed_mean(group):
        vals = group.sort_values().values
        n = len(vals)
        if n <= 2:
            return np.mean(vals)
        # Drop highest and lowest
        return np.mean(vals[1:-1])

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

    # Course-specific lbs-per-length (fall back to generic if missing)
    w["lpl"] = w["std_key"].map(lpl_dict)
    missing_lpl = w["lpl"].isna()
    if missing_lpl.any():
        w.loc[missing_lpl, "lpl"] = w.loc[missing_lpl, "distance"].apply(
            generic_lbs_per_length
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

    horse_figure = winner_figure − (cum_beaten_lengths × course_lpl)

    Beaten lengths are capped at 30 to limit noise from extreme values.
    Runners beaten more than 20 lengths receive no final figure (unreliable
    due to easing / not running to the line).
    """
    print("\n  Extending figures to all runners...")

    out = df[df["race_id"].isin(winner_fig_dict)].copy()
    out["winner_figure"] = out["race_id"].map(winner_fig_dict)

    # Course-specific lpl, with generic fallback
    out["lpl"] = out["std_key"].map(lpl_dict)
    missing = out["lpl"].isna()
    if missing.any():
        out.loc[missing, "lpl"] = out.loc[missing, "distance"].apply(
            generic_lbs_per_length
        )

    is_winner = out["positionOfficial"] == 1
    cum = out["distanceCumulative"].fillna(0).clip(lower=0, upper=30)

    out["lbs_behind"] = cum * out["lpl"]
    out.loc[is_winner, "lbs_behind"] = 0.0
    out["raw_figure"] = out["winner_figure"] - out["lbs_behind"]

    # Non-finishers (no position) should not receive a figure
    no_pos = out["positionOfficial"].isna()
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
    Surface-specific linear calibration to the Timeform timefigure scale.

    Turf and AW have substantially different calibration slopes (0.77 vs
    0.89), so fitting them separately reduces error.  Each surface gets
    its own  timefigure ≈ a × figure_final + b  fitted on 2015–2023.
    """
    print("\n  Calibrating to Timeform scale (surface-specific)...")

    mask = (
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_final"].notna()
    )

    df["figure_calibrated"] = np.nan
    cal_params = {}

    for surface in df["raceSurfaceName"].unique():
        surf_mask = df["raceSurfaceName"] == surface
        fit = df[mask & surf_mask & (df["source_year"] <= 2023)]

        if len(fit) < 100:
            print(f"    {surface}: insufficient data — using identity")
            df.loc[surf_mask, "figure_calibrated"] = df.loc[
                surf_mask, "figure_final"
            ]
            cal_params[surface] = (1.0, 0.0)
            continue

        x = fit["figure_final"].values
        y = fit["timefigure"].values
        A = np.vstack([x, np.ones(len(x))]).T
        (a, b), *_ = np.linalg.lstsq(A, y, rcond=None)

        df.loc[surf_mask, "figure_calibrated"] = (
            a * df.loc[surf_mask, "figure_final"] + b
        )
        cal_params[surface] = (a, b)
        print(f"    {surface:<15}: timefigure ≈ {a:.4f} × figure + {b:.2f}")

    # Exclude non-finishers (no official position)
    no_position = df["positionOfficial"].isna() & df["figure_calibrated"].notna()
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

    # 1 — Standard times (per track / distance / surface)
    print("\nSTAGE 1: Standard times (initial — good going only)")
    std_times, std_df = compute_standard_times(df)

    # 2 — Going allowance (initial)
    print("\nSTAGE 2: Going allowances (initial)")
    going_allowances = compute_going_allowances(df, std_times)

    # Iterative refinement: use going-corrected times to recompute
    # standard times (using ALL goings now), then recompute GA.
    N_ITER = 2
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

    # 9 — Calibration & validation
    print("\nSTAGE 9: Calibration")
    all_figs, cal_params = calibrate_figures(all_figs)

    print("\nSTAGE 10: Validation")
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
