"""
Speed Figure Compilation Pipeline
==================================
Computes speed figures for UK/Ireland flat racing from Timeform data.

Target variable: timefigure (Timeform's published time figure)

Pipeline stages:
  1. Load & filter data (UK/IRE, flat, with valid finishing times)
  2. Compute standard times per course/distance/surface
  3. Compute going allowances per meeting
  4. Compute winner speed figures
  5. Extend to all runners via beaten lengths
  6. Apply weight-carried adjustment
  7. Apply weight-for-age (WFA) adjustment
  8. Validate against Timeform timefigure
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
MIN_RACES_STANDARD_TIME = 15   # Minimum winners for a standard time
MIN_RACES_GOING_ALLOWANCE = 3  # Minimum races on a card for going allowance

# Class adjustments in seconds per mile (8 furlongs) — subtract from raw times
# to normalise to a "Class 4" baseline before computing standard times
CLASS_ADJUSTMENT_PER_MILE = {
    "1": -3.6,   # Group/Graded
    "2": -4.8,   # Listed/Premier Hcap
    "3": -6.0,
    "4": -7.2,   # Reference class
    "5": -8.4,
    "6": -9.6,
    "7": -10.8,
}

# UK and Ireland course sets
UK_COURSES = {
    "ASCOT", "AYR", "BATH", "BEVERLEY", "BRIGHTON", "CARLISLE",
    "CATTERICK", "CHELMSFORD CITY", "CHELTENHAM", "CHEPSTOW", "CHESTER",
    "DONCASTER", "EPSOM", "EXETER", "FFOS LAS", "FONTWELL", "GOODWOOD",
    "HAMILTON", "HAYDOCK", "HEREFORD", "HEXHAM", "HUNTINGDON", "KELSO",
    "KEMPTON", "LEICESTER", "LINGFIELD", "LUDLOW", "MARKET RASEN",
    "MUSSELBURGH", "NEWBURY", "NEWCASTLE", "NEWMARKET", "NEWTON ABBOT",
    "NOTTINGHAM", "PLUMPTON", "PONTEFRACT", "REDCAR", "RIPON",
    "SALISBURY", "SANDOWN", "SEDGEFIELD", "SOUTHWELL", "STRATFORD",
    "TAUNTON", "THIRSK", "UTTOXETER", "WARWICK", "WETHERBY",
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

# Going descriptions considered "Good/Standard" ground (for standard time compilation)
# Includes both full names and abbreviations used in the data
GOOD_GOING = {
    # Turf
    "Good", "Standard", "Good To Firm", "Good to Firm",
    "Standard To Slow", "Standard to Slow",
    "Good to Yielding", "Good To Yielding",
    # AW (abbreviated forms in data)
    "Std", "Std/Slow", "Std/Fast", "Standard/Slow",
}

# WFA table: 3yo vs 4+ (approximate BHA 2025, in lbs)
# Keys: month number. Values: dict of distance_furlongs → allowance_lbs
WFA_3YO = {
    1:  {5: 11, 6: 12, 7: 13, 8: 14, 10: 15, 12: 16, 14: 17, 16: 18},
    2:  {5: 11, 6: 12, 7: 13, 8: 14, 10: 15, 12: 16, 14: 17, 16: 18},
    3:  {5: 10, 6: 11, 7: 12, 8: 13, 10: 14, 12: 15, 14: 16, 16: 17},
    4:  {5: 9,  6: 10, 7: 11, 8: 12, 10: 13, 12: 14, 14: 15, 16: 16},
    5:  {5: 7,  6: 8,  7: 9,  8: 10, 10: 11, 12: 12, 14: 13, 16: 14},
    6:  {5: 5,  6: 6,  7: 7,  8: 8,  10: 9,  12: 10, 14: 11, 16: 12},
    7:  {5: 3,  6: 4,  7: 5,  8: 7,  10: 8,  12: 9,  14: 10, 16: 11},
    8:  {5: 2,  6: 3,  7: 4,  8: 5,  10: 6,  12: 7,  14: 8,  16: 9},
    9:  {5: 0,  6: 1,  7: 2,  8: 3,  10: 5,  12: 6,  14: 7,  16: 8},
    10: {5: 0,  6: 0,  7: 1,  8: 2,  10: 3,  12: 4,  14: 5,  16: 6},
    11: {5: 0,  6: 0,  7: 0,  8: 1,  10: 2,  12: 3,  14: 4,  16: 5},
    12: {5: 0,  6: 0,  7: 0,  8: 1,  10: 2,  12: 3,  14: 4,  16: 5},
}

# WFA for 2yo vs 4+ (approximate, May-Nov only — 2yos don't race Jan-Apr)
WFA_2YO = {
    5:  {5: 24, 6: 26, 7: 28, 8: 30},
    6:  {5: 20, 6: 22, 7: 24, 8: 26},
    7:  {5: 15, 6: 17, 7: 19, 8: 21},
    8:  {5: 12, 6: 13, 7: 15, 8: 17},
    9:  {5: 9,  6: 10, 7: 12, 8: 14},
    10: {5: 6,  6: 8,  7: 10, 8: 12},
    11: {5: 5,  6: 7,  7: 9,  8: 11},
    12: {5: 5,  6: 7,  7: 9,  8: 11},
}


# ─────────────────────────────────────────────────────────────────────
# STAGE 0: DATA LOADING & FILTERING
# ─────────────────────────────────────────────────────────────────────

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
    """Filter to UK/IRE flat racing with valid data."""
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
                "weightCarried", "horseAge", "numberOfRunners", "draw"]:
        if col in filtered.columns:
            filtered[col] = pd.to_numeric(filtered[col], errors="coerce")

    # Build race ID
    filtered["race_id"] = (
        filtered["meetingDate"].astype(str) + "_" +
        filtered["courseName"].astype(str) + "_" +
        filtered["raceNumber"].astype(str)
    )

    # Meeting ID (for going allowance) — separate by surface so turf/AW get own GA
    filtered["meeting_id"] = (
        filtered["meetingDate"].astype(str) + "_" +
        filtered["courseName"].astype(str) + "_" +
        filtered["raceSurfaceName"].astype(str)
    )

    # Standard time key: course + rounded distance + surface
    filtered["dist_round"] = filtered["distance"].round(1)
    filtered["std_key"] = (
        filtered["courseName"].astype(str) + "_" +
        filtered["dist_round"].astype(str) + "_" +
        filtered["raceSurfaceName"].astype(str)
    )

    # Parse month from meeting date for WFA
    filtered["month"] = pd.to_datetime(filtered["meetingDate"], errors="coerce").dt.month

    print(f"  After UK/IRE flat filter: {len(filtered):,} rows")
    print(f"    Unique courses: {filtered['courseName'].nunique()}")
    print(f"    Unique races:   {filtered['race_id'].nunique():,}")
    return filtered


# ─────────────────────────────────────────────────────────────────────
# STAGE 1: STANDARD TIMES
# ─────────────────────────────────────────────────────────────────────

def compute_class_adjustment(race_class, distance_furlongs):
    """Compute class adjustment in seconds for a given class and distance."""
    cls_str = str(race_class).strip()
    # Try to extract numeric class
    if cls_str in CLASS_ADJUSTMENT_PER_MILE:
        adj_per_mile = CLASS_ADJUSTMENT_PER_MILE[cls_str]
    else:
        # Default to class 4 (no adjustment relative to reference)
        adj_per_mile = CLASS_ADJUSTMENT_PER_MILE["4"]
    return (adj_per_mile * distance_furlongs) / 8.0


def compute_standard_times(df):
    """
    Compute standard times per course/distance/surface.

    Method: Take winners on good going, apply class adjustment to normalise
    times, then take the median. This gives the expected time for an average
    horse on good going at each course/distance.
    """
    print("\n  Computing standard times...")

    # Filter to winners only
    winners = df[df["positionOfficial"] == 1].copy()

    # Prefer good going, but include all if insufficient data
    winners_good = winners[winners["going"].isin(GOOD_GOING)].copy()
    print(f"    Winners total: {len(winners):,}")
    print(f"    Winners on good going: {len(winners_good):,}")

    # Apply class adjustment: normalise times to a "class 4" baseline
    winners_good["class_adj"] = winners_good.apply(
        lambda r: compute_class_adjustment(r["raceClass"], r["distance"]),
        axis=1
    )
    winners_good["adj_time"] = winners_good["finishingTime"] - winners_good["class_adj"]

    # Compute median adjusted time per course/distance/surface
    std_times = (
        winners_good
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

    # For combos with insufficient good-going data (or none at all), fall back to all goings
    all_std_keys = set(winners["std_key"].unique())
    good_std_keys = set(std_times[std_times["n_races"] >= MIN_RACES_STANDARD_TIME]["std_key"])
    sparse_or_missing = all_std_keys - good_std_keys

    if sparse_or_missing:
        winners_all = winners.copy()
        winners_all["class_adj"] = winners_all.apply(
            lambda r: compute_class_adjustment(r["raceClass"], r["distance"]),
            axis=1
        )
        winners_all["adj_time"] = winners_all["finishingTime"] - winners_all["class_adj"]

        fallback = (
            winners_all[winners_all["std_key"].isin(sparse_or_missing)]
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
        # Keep good-going entries that met the threshold, add fallback entries
        std_times = std_times[std_times["std_key"].isin(good_std_keys)]
        std_times = pd.concat([std_times, fallback], ignore_index=True)

    # Filter to combos with enough data
    valid = std_times[std_times["n_races"] >= MIN_RACES_STANDARD_TIME].copy()
    print(f"    Standard time combos (>= {MIN_RACES_STANDARD_TIME} races): {len(valid):,}")
    print(f"    Dropped (insufficient data): {len(std_times) - len(valid):,}")

    # Build lookup dict
    std_dict = dict(zip(valid["std_key"], valid["median_time"]))
    return std_dict, valid


# ─────────────────────────────────────────────────────────────────────
# STAGE 2: GOING ALLOWANCE
# ─────────────────────────────────────────────────────────────────────

def compute_going_allowances(df, std_times):
    """
    Compute going allowance per meeting (seconds per furlong).

    For each race on the card, compute deviation from standard time per furlong.
    Remove the highest and lowest deviations, then average the rest.

    Positive = ground faster than standard (firm)
    Negative = ground slower than standard (soft/heavy)
    """
    print("\n  Computing going allowances...")

    # Only use winners with valid standard times
    winners = df[df["positionOfficial"] == 1].copy()
    winners = winners[winners["std_key"].isin(std_times)].copy()
    winners["standard_time"] = winners["std_key"].map(std_times)

    # Apply class adjustment to actual time before comparing to standard
    winners["class_adj"] = winners.apply(
        lambda r: compute_class_adjustment(r["raceClass"], r["distance"]),
        axis=1
    )
    winners["adj_time"] = winners["finishingTime"] - winners["class_adj"]

    # Deviation per furlong
    winners["deviation"] = winners["adj_time"] - winners["standard_time"]
    winners["dev_per_furlong"] = winners["deviation"] / winners["distance"]

    # Drop extreme outlier deviations (likely timing errors or non-triers)
    q_low = winners["dev_per_furlong"].quantile(0.01)
    q_high = winners["dev_per_furlong"].quantile(0.99)
    winners = winners[
        (winners["dev_per_furlong"] >= q_low) &
        (winners["dev_per_furlong"] <= q_high)
    ]

    # Compute going allowance per meeting with outlier trimming
    def trimmed_mean(group):
        vals = group.sort_values().values
        n = len(vals)
        if n <= 2:
            return np.mean(vals)
        elif n <= 4:
            # Remove single highest and lowest
            return np.mean(vals[1:-1])
        else:
            # Remove top and bottom
            return np.mean(vals[1:-1])

    ga_series = (
        winners.groupby("meeting_id")["dev_per_furlong"]
        .apply(trimmed_mean)
    )

    # Also track how many races contributed
    ga_count = winners.groupby("meeting_id")["dev_per_furlong"].count()

    # Filter to meetings with enough races
    valid_meetings = ga_count[ga_count >= MIN_RACES_GOING_ALLOWANCE].index
    ga_dict = ga_series[ga_series.index.isin(valid_meetings)].to_dict()

    print(f"    Meetings with going allowance: {len(ga_dict):,}")
    print(f"    GA range: {min(ga_dict.values()):.3f} to {max(ga_dict.values()):.3f} s/f")
    print(f"    GA mean:  {np.mean(list(ga_dict.values())):.3f} s/f")

    return ga_dict


# ─────────────────────────────────────────────────────────────────────
# STAGE 3: LBS PER LENGTH
# ─────────────────────────────────────────────────────────────────────

def lbs_per_length(distance_furlongs):
    """
    Compute lbs per length at a given distance.

    Formula: lbs_per_length = seconds_per_length × lbs_per_second_at_distance
    lbs_per_second_at_distance = 22 × (5 / distance_in_furlongs)
    """
    lbs_per_second = LBS_PER_SECOND_5F * (BENCHMARK_FURLONGS / distance_furlongs)
    return SECONDS_PER_LENGTH * lbs_per_second


def lbs_per_second(distance_furlongs):
    """Lbs per second at a given distance."""
    return LBS_PER_SECOND_5F * (BENCHMARK_FURLONGS / distance_furlongs)


# ─────────────────────────────────────────────────────────────────────
# STAGE 4: WINNER SPEED FIGURES
# ─────────────────────────────────────────────────────────────────────

def compute_winner_figures(df, std_times, going_allowances):
    """
    Compute raw speed figures for race winners.

    Steps:
      1. Subtract going allowance from actual time → going-corrected time
      2. Subtract class adjustment → class-normalised time
      3. Compute deviation from standard time
      4. Convert deviation to lbs
      5. winner_figure = BASE_RATING - deviation_lbs
    """
    print("\n  Computing winner speed figures...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = winners[
        winners["std_key"].isin(std_times) &
        winners["meeting_id"].isin(going_allowances)
    ].copy()

    winners["standard_time"] = winners["std_key"].map(std_times)
    winners["going_allowance"] = winners["meeting_id"].map(going_allowances)

    # Class adjustment
    winners["class_adj"] = winners.apply(
        lambda r: compute_class_adjustment(r["raceClass"], r["distance"]),
        axis=1
    )

    # Going-corrected, class-adjusted time
    winners["corrected_time"] = (
        winners["finishingTime"]
        - winners["class_adj"]
        - (winners["going_allowance"] * winners["distance"])
    )

    # Deviation from standard (negative = faster than standard = better)
    winners["deviation_seconds"] = winners["corrected_time"] - winners["standard_time"]

    # Convert to lengths
    winners["deviation_lengths"] = winners["deviation_seconds"] / SECONDS_PER_LENGTH

    # Convert to lbs (using distance-specific lbs_per_length)
    winners["lpl"] = winners["distance"].apply(lbs_per_length)
    winners["deviation_lbs"] = winners["deviation_lengths"] * winners["lpl"]

    # Raw winner figure: BASE_RATING minus deviation (faster = higher figure)
    winners["raw_figure"] = BASE_RATING - winners["deviation_lbs"]

    print(f"    Winner figures computed: {len(winners):,}")
    print(f"    Raw figure range: {winners['raw_figure'].min():.0f} to {winners['raw_figure'].max():.0f}")
    print(f"    Raw figure mean:  {winners['raw_figure'].mean():.1f}")
    print(f"    Raw figure median:{winners['raw_figure'].median():.1f}")

    # Build race_id → winner_figure lookup
    winner_fig_dict = dict(zip(winners["race_id"], winners["raw_figure"]))

    return winners, winner_fig_dict


# ─────────────────────────────────────────────────────────────────────
# STAGE 5: ALL-RUNNER SPEED FIGURES (BEATEN LENGTHS)
# ─────────────────────────────────────────────────────────────────────

def compute_all_figures(df, winner_fig_dict):
    """
    Extend speed figures to all runners using cumulative beaten lengths.

    For non-winners: figure = winner_figure - (cumulative_beaten_lengths × lbs_per_length)
    For winners: figure = winner_figure
    """
    print("\n  Extending figures to all runners...")

    # Filter to races that have a winner figure
    df_valid = df[df["race_id"].isin(winner_fig_dict)].copy()

    # Map winner figure to every runner in that race
    df_valid["winner_figure"] = df_valid["race_id"].map(winner_fig_dict)

    # Lbs per length at this distance
    df_valid["lpl"] = df_valid["distance"].apply(lbs_per_length)

    # For winners (pos=1): figure = winner_figure
    # For non-winners: figure = winner_figure - (cumulative_beaten_lengths * lpl)
    is_winner = df_valid["positionOfficial"] == 1
    cum_beaten = df_valid["distanceCumulative"].fillna(0).clip(lower=0)

    # Cap extreme beaten distances at 30 lengths (unreliable beyond that)
    cum_beaten = cum_beaten.clip(upper=30)

    df_valid["lbs_behind"] = cum_beaten * df_valid["lpl"]
    df_valid.loc[is_winner, "lbs_behind"] = 0

    df_valid["raw_figure"] = df_valid["winner_figure"] - df_valid["lbs_behind"]

    print(f"    All-runner figures: {len(df_valid):,}")
    return df_valid


# ─────────────────────────────────────────────────────────────────────
# STAGE 6: WEIGHT-CARRIED ADJUSTMENT
# ─────────────────────────────────────────────────────────────────────

def apply_weight_adjustment(df):
    """
    Adjust figures for weight carried.

    Method: figure += (weight_carried - base_weight)
    A horse carrying more than 9st 0lb gets a positive adjustment.
    """
    print("\n  Applying weight-carried adjustment...")

    has_weight = df["weightCarried"].notna()
    print(f"    Runners with weight data: {has_weight.sum():,} / {len(df):,}")

    df["weight_adj"] = 0.0
    df.loc[has_weight, "weight_adj"] = df.loc[has_weight, "weightCarried"] - BASE_WEIGHT_LBS

    df["figure_after_weight"] = df["raw_figure"] + df["weight_adj"]

    return df


# ─────────────────────────────────────────────────────────────────────
# STAGE 7: WEIGHT FOR AGE (WFA) ADJUSTMENT
# ─────────────────────────────────────────────────────────────────────

def get_wfa_allowance(age, month, distance):
    """
    Look up the WFA allowance in lbs for a given age, month, and distance.
    Returns the allowance to ADD to the figure.
    """
    if pd.isna(age) or pd.isna(month) or pd.isna(distance):
        return 0.0

    age = int(age)
    month = int(month)
    distance = float(distance)

    # 4+ year olds: no WFA adjustment
    if age >= 4:
        return 0.0

    # Choose table
    if age == 3:
        table = WFA_3YO
    elif age == 2:
        table = WFA_2YO
    else:
        return 0.0

    if month not in table:
        return 0.0

    month_table = table[month]
    distances = sorted(month_table.keys())

    # Find closest distance bracket
    if distance <= distances[0]:
        return float(month_table[distances[0]])
    elif distance >= distances[-1]:
        return float(month_table[distances[-1]])
    else:
        # Linear interpolation between brackets
        for i in range(len(distances) - 1):
            if distances[i] <= distance <= distances[i + 1]:
                d_lo, d_hi = distances[i], distances[i + 1]
                v_lo, v_hi = month_table[d_lo], month_table[d_hi]
                frac = (distance - d_lo) / (d_hi - d_lo)
                return v_lo + frac * (v_hi - v_lo)
    return 0.0


def apply_wfa_adjustment(df):
    """
    Apply weight-for-age adjustment.

    Younger horses get an upward adjustment to compensate for immaturity.
    """
    print("\n  Applying WFA adjustment...")

    df["wfa_adj"] = df.apply(
        lambda r: get_wfa_allowance(r["horseAge"], r["month"], r["distance"]),
        axis=1
    )

    # Count adjustments
    has_wfa = df["wfa_adj"] > 0
    print(f"    Runners with WFA adjustment: {has_wfa.sum():,} / {len(df):,}")
    if has_wfa.sum() > 0:
        print(f"    WFA range: {df.loc[has_wfa, 'wfa_adj'].min():.1f} to {df.loc[has_wfa, 'wfa_adj'].max():.1f} lbs")

    df["figure_final"] = df["figure_after_weight"] + df["wfa_adj"]

    return df


# ─────────────────────────────────────────────────────────────────────
# STAGE 8: VALIDATION AGAINST TIMEFIGURE
# ─────────────────────────────────────────────────────────────────────

def calibrate_figures(df):
    """
    Calibrate figures to match Timeform's timefigure scale.

    Uses ordinary least squares on the training set (years 2015-2023)
    to find the optimal linear mapping: calibrated = a * figure + b
    """
    print("\n  Calibrating to Timeform scale...")

    # Use rows with valid, non-extreme timefigure for fitting
    # Filter out timefigure outliers (data errors in some years)
    mask = (
        df["timefigure"].notna() &
        (df["timefigure"] != 0) &
        (df["timefigure"] > -200) &
        (df["timefigure"] < 200) &
        df["figure_final"].notna()
    )
    fit_data = df[mask].copy()

    # Temporal split: train on 2015-2023, test on 2024+
    train = fit_data[fit_data["source_year"] <= 2023]
    print(f"    Calibration training set: {len(train):,} rows")

    if len(train) < 100:
        print("    Insufficient data for calibration — skipping")
        df["figure_calibrated"] = df["figure_final"]
        return df, 1.0, 0.0

    # Fit linear regression: timefigure = a * figure_final + b
    x = train["figure_final"].values
    y = train["timefigure"].values
    A = np.vstack([x, np.ones(len(x))]).T
    result = np.linalg.lstsq(A, y, rcond=None)
    a, b = result[0]

    print(f"    Calibration: timefigure ≈ {a:.4f} × figure + {b:.2f}")

    # Apply calibration
    df["figure_calibrated"] = a * df["figure_final"] + b

    return df, a, b


def validate_figures(df):
    """Validate computed figures against Timeform's timefigure."""
    print("\n" + "=" * 70)
    print("VALIDATION AGAINST TIMEFIGURE")
    print("=" * 70)

    # Filter to rows with non-zero timefigure, excluding extreme outliers
    valid = df[
        (df["timefigure"].notna()) &
        (df["timefigure"] != 0) &
        (df["timefigure"] > -200) &
        (df["timefigure"] < 200)
    ].copy()
    print(f"\n  Rows with valid timefigure: {len(valid):,}")

    if len(valid) == 0:
        print("  No valid timefigure rows for validation!")
        return

    # Use calibrated figure for comparison
    fig_col = "figure_calibrated" if "figure_calibrated" in valid.columns else "figure_final"

    # Correlation
    corr = valid[fig_col].corr(valid["timefigure"])
    print(f"  Correlation ({fig_col} vs timefigure): {corr:.4f}")

    # Error metrics
    errors = valid[fig_col] - valid["timefigure"]
    mae = errors.abs().mean()
    rmse = np.sqrt((errors ** 2).mean())
    median_ae = errors.abs().median()
    mean_error = errors.mean()

    print(f"  MAE:         {mae:.2f} lbs")
    print(f"  RMSE:        {rmse:.2f} lbs")
    print(f"  Median AE:   {median_ae:.2f} lbs")
    print(f"  Mean error:  {mean_error:+.2f} lbs (bias)")

    # Distribution of errors
    print(f"\n  Error distribution:")
    for threshold in [1, 2, 3, 5, 10, 15, 20]:
        pct = (errors.abs() <= threshold).mean() * 100
        print(f"    Within ±{threshold:>2} lbs: {pct:.1f}%")

    # By position
    print(f"\n  Correlation by finishing position:")
    for pos in [1, 2, 3, 4, 5]:
        sub = valid[valid["positionOfficial"] == pos]
        if len(sub) > 100:
            c = sub[fig_col].corr(sub["timefigure"])
            m = (sub[fig_col] - sub["timefigure"]).abs().mean()
            print(f"    Pos {pos}: corr={c:.4f}, MAE={m:.2f} ({len(sub):,} runners)")

    # By surface
    print(f"\n  Correlation by surface:")
    for surf in valid["raceSurfaceName"].unique():
        sub = valid[valid["raceSurfaceName"] == surf]
        if len(sub) > 100:
            c = sub[fig_col].corr(sub["timefigure"])
            m = (sub[fig_col] - sub["timefigure"]).abs().mean()
            print(f"    {surf:<15}: corr={c:.4f}, MAE={m:.2f} ({len(sub):,} runners)")

    # By year
    print(f"\n  Correlation by year:")
    for year in sorted(valid["source_year"].unique()):
        sub = valid[valid["source_year"] == year]
        if len(sub) > 100:
            c = sub[fig_col].corr(sub["timefigure"])
            m = (sub[fig_col] - sub["timefigure"]).abs().mean()
            print(f"    {year}: corr={c:.4f}, MAE={m:.2f} ({len(sub):,} runners)")

    # Summary stats comparison
    print(f"\n  Summary comparison:")
    print(f"    {'Metric':<15} {'Our Figure':>12} {'Timefigure':>12}")
    print(f"    {'Mean':<15} {valid[fig_col].mean():>12.1f} {valid['timefigure'].mean():>12.1f}")
    print(f"    {'Median':<15} {valid[fig_col].median():>12.1f} {valid['timefigure'].median():>12.1f}")
    print(f"    {'Std':<15} {valid[fig_col].std():>12.1f} {valid['timefigure'].std():>12.1f}")
    print(f"    {'Min':<15} {valid[fig_col].min():>12.1f} {valid['timefigure'].min():>12.1f}")
    print(f"    {'Max':<15} {valid[fig_col].max():>12.1f} {valid['timefigure'].max():>12.1f}")

    return valid


# ─────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────

def run_pipeline():
    """Execute the full speed figure pipeline."""
    print("=" * 70)
    print("SPEED FIGURE PIPELINE")
    print("=" * 70)

    # Stage 0: Load data
    print("\nSTAGE 0: Loading data...")
    df = load_data()
    df = filter_uk_ire_flat(df)

    # Stage 1: Standard times
    print("\nSTAGE 1: Standard times")
    std_times, std_df = compute_standard_times(df)

    # Stage 2: Going allowances
    print("\nSTAGE 2: Going allowances")
    going_allowances = compute_going_allowances(df, std_times)

    # Stage 3: Winner figures
    print("\nSTAGE 3: Winner speed figures")
    winners, winner_fig_dict = compute_winner_figures(df, std_times, going_allowances)

    # Stage 4: All-runner figures
    print("\nSTAGE 4: All-runner figures")
    all_figs = compute_all_figures(df, winner_fig_dict)

    # Stage 5: Weight adjustment
    print("\nSTAGE 5: Weight adjustment")
    all_figs = apply_weight_adjustment(all_figs)

    # Stage 6: WFA adjustment
    print("\nSTAGE 6: WFA adjustment")
    all_figs = apply_wfa_adjustment(all_figs)

    # Stage 7: Calibration
    print("\nSTAGE 7: Calibration to Timeform scale")
    all_figs, cal_a, cal_b = calibrate_figures(all_figs)

    # Stage 8: Validation
    print("\nSTAGE 8: Validation")
    valid = validate_figures(all_figs)

    # Save output
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_cols = [
        "meetingDate", "courseName", "raceNumber", "race_id",
        "horseName", "horseCode", "positionOfficial",
        "distance", "going", "raceSurfaceName", "raceClass",
        "horseAge", "horseGender", "weightCarried",
        "finishingTime", "distanceCumulative",
        "raw_figure", "weight_adj", "wfa_adj", "figure_final",
        "figure_calibrated",
        "timefigure", "performanceRating",
        "source_year",
    ]
    # Only include columns that exist
    output_cols = [c for c in output_cols if c in all_figs.columns]
    output_df = all_figs[output_cols].copy()

    output_path = os.path.join(OUTPUT_DIR, "speed_figures.csv")
    output_df.to_csv(output_path, index=False)
    print(f"\n  Output saved: {output_path} ({len(output_df):,} rows)")

    # Save standard times
    std_path = os.path.join(OUTPUT_DIR, "standard_times.csv")
    std_df.to_csv(std_path, index=False)
    print(f"  Standard times saved: {std_path} ({len(std_df):,} entries)")

    # Save going allowances
    ga_df = pd.DataFrame(
        list(going_allowances.items()),
        columns=["meeting_id", "going_allowance_spf"]
    )
    ga_path = os.path.join(OUTPUT_DIR, "going_allowances.csv")
    ga_df.to_csv(ga_path, index=False)
    print(f"  Going allowances saved: {ga_path} ({len(ga_df):,} meetings)")

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)

    return all_figs, std_times, going_allowances


if __name__ == "__main__":
    run_pipeline()
