"""
ML-Enhanced Speed Figures
=========================
Uses the traditional pipeline figure as the primary feature, then trains
XGBoost to correct for class, course, going, distance, and other biases.

Target: timefigure (Timeform)
Goal: MAE < 3
"""

import logging
import re
import time

import pandas as pd
import numpy as np
import os
import warnings
from sklearn.metrics import mean_absolute_error
from sklearn.isotonic import IsotonicRegression
import xgboost as xgb
import lightgbm as lgb

from src.custom_metrics import CustomMetricsEngine
from src.field_mapping import timeform_to_custom_schema, get_new_feature_columns

log = logging.getLogger(__name__)

warnings.filterwarnings("ignore")

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")


def load_pipeline_output():
    """Load the traditional pipeline output."""
    path = os.path.join(OUTPUT_DIR, "speed_figures.csv")
    df = pd.read_csv(path, low_memory=False)
    print(f"Loaded pipeline output: {len(df):,} rows")
    return df


def load_raw_extra_cols(years=range(2015, 2027)):
    """Load extra columns from raw data not in pipeline output."""
    frames = []
    extra_cols = [
        "meetingDate", "courseName", "raceNumber", "horseCode",
        "distanceYards", "distanceFurlongs", "raceClass",
        "numberOfRunners", "draw", "raceCode",
        "ispDecimal", "betfairWinSP",
        "performanceRating", "preRaceMasterRating",
        "preRaceAdjustedRating",
        "eligibilitySexLimit", "courseExtraId",
        "jockeyFullName", "trainerFullName",
        "jockeyUpLift", "trainerUpLift",
        "prizeFund", "prizeFundWinner",
        "raceType", "raceSurfaceName", "positionOfficial",
        "tfwfa", "horseAge",
        "sectionalFinishingTime",
        # Additional columns for CustomMetricsEngine integration
        "sireName", "damName", "damSireName",
        "equipmentDescription", "equipmentChar", "equipmentFirstTime",
        "performanceCommentPremium",
    ]
    for year in years:
        path = os.path.join(DATA_DIR, f"timeform_{year}.csv")
        if os.path.exists(path):
            # Only read needed columns
            available = pd.read_csv(path, nrows=0).columns.tolist()
            cols_to_read = [c for c in extra_cols if c in available]
            raw = pd.read_csv(path, usecols=cols_to_read, low_memory=False)
            raw["source_year"] = year
            frames.append(raw)
    combined = pd.concat(frames, ignore_index=True)
    return combined


def build_features(df, raw_extra):
    """
    Build feature matrix for ML model.

    The traditional pipeline figure is the backbone. We add contextual
    features that capture systematic biases the pipeline misses.
    """
    print("\nBuilding features...")

    # Merge extra columns from raw data
    # Build matching key
    df["_merge_key"] = (
        df["meetingDate"].astype(str) + "_" +
        df["courseName"].astype(str) + "_" +
        df["raceNumber"].astype(str) + "_" +
        df["horseCode"].astype(str)
    )
    raw_extra["_merge_key"] = (
        raw_extra["meetingDate"].astype(str) + "_" +
        raw_extra["courseName"].astype(str) + "_" +
        raw_extra["raceNumber"].astype(str) + "_" +
        raw_extra["horseCode"].astype(str)
    )

    # Pick extra cols not already in df
    extra_only = [
        "distanceYards", "distanceFurlongs",
        "numberOfRunners", "draw",
        "ispDecimal", "betfairWinSP",
        "preRaceMasterRating", "preRaceAdjustedRating",
        "eligibilitySexLimit", "courseExtraId",
        "jockeyFullName", "trainerFullName",
        "jockeyUpLift", "trainerUpLift",
        "prizeFund", "prizeFundWinner",
        "raceType",
        "tfwfa", "sectionalFinishingTime",
    ]
    extra_only = [c for c in extra_only if c in raw_extra.columns]
    merge_df = raw_extra[["_merge_key"] + extra_only].drop_duplicates("_merge_key")

    df = df.merge(merge_df, on="_merge_key", how="left", suffixes=("", "_raw"))
    df.drop(columns=["_merge_key"], inplace=True)
    print(f"  After merge: {len(df):,} rows")

    # ── Numeric features ──
    for col in ["distanceYards", "distanceFurlongs", "numberOfRunners",
                "draw", "ispDecimal", "betfairWinSP",
                "preRaceMasterRating", "prizeFund", "prizeFundWinner"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Derived features ──

    # Precise distance in yards (pipeline may already have this)
    if "total_yards" not in df.columns:
        if "distanceFurlongs" in df.columns and "distanceYards" in df.columns:
            df["total_yards"] = (
                df["distanceFurlongs"] * 220 + df["distanceYards"].fillna(0)
            )
        else:
            df["total_yards"] = df["distance"] * 220

    # Speed (yards per second)
    df["speed_yps"] = df["total_yards"] / df["finishingTime"]

    # Time per furlong
    df["time_per_furlong"] = df["finishingTime"] / df["distance"]

    # Beaten lengths per furlong (for non-winners)
    df["beaten_per_furlong"] = df["distanceCumulative"].fillna(0) / df["distance"]

    # Is female
    df["is_female"] = df["horseGender"].isin({"f", "m"}).astype(int)

    # Course encoding (frequency-based)
    course_freq = df["courseName"].value_counts()
    df["course_freq"] = df["courseName"].map(course_freq)

    # Going encoding (ordinal: firm to heavy)
    going_order = {
        "Hard": 1, "Firm": 2, "Fast": 2, "Gd/Frm": 3, "Good To Firm": 3,
        "Good to Firm": 3, "Good": 4, "Standard": 4, "Std": 4,
        "Std/Fast": 3, "Std/Slow": 5, "Standard/Slow": 5,
        "Gd/Sft": 5, "Good to Soft": 5, "Good To Yielding": 5,
        "Good to Yielding": 5, "Yielding": 6, "Yld/Sft": 6,
        "Soft": 7, "Sft/Hvy": 8, "Slow": 5,
        "Heavy": 9, "Hvy/Sft": 8,
    }
    df["going_numeric"] = df["going"].map(going_order).fillna(4)

    # Race class numeric (NaN → -1 for ML to distinguish)
    df["class_numeric"] = df["raceClass"].fillna(-1).astype(float)

    # Month (already exists)
    if "month" not in df.columns:
        df["month"] = pd.to_datetime(df["meetingDate"], errors="coerce").dt.month

    # Surface encoding
    df["is_aw"] = (df["raceSurfaceName"] == "All Weather").astype(int)

    # Prize money (log scale for normalization)
    if "prizeFund" in df.columns:
        df["log_prize"] = np.log1p(df["prizeFund"].fillna(0))
    else:
        df["log_prize"] = 0

    # Pre-race rating (if available)
    if "preRaceMasterRating" in df.columns:
        df["pre_rating"] = df["preRaceMasterRating"].fillna(0)
    else:
        df["pre_rating"] = 0

    # Course ID for grouping
    df["course_id"] = df["courseName"].astype("category").cat.codes

    # ── Direct time-derived features (the raw data timefigure is built from) ──

    # Finishing time is the single most important raw signal for timefigure
    df["finishing_time"] = df["finishingTime"]

    # Time deviation from per-race mean (captures within-race performance)
    race_mean_time = df.groupby("race_id")["finishingTime"].transform("mean")
    df["time_vs_race_mean"] = df["finishingTime"] - race_mean_time

    # Winner time for each race
    winners = df[df["positionOfficial"] == 1][["race_id", "finishingTime"]].rename(
        columns={"finishingTime": "winner_time"}
    )
    df = df.merge(winners.drop_duplicates("race_id"), on="race_id", how="left")
    df["time_behind_winner"] = df["finishingTime"] - df["winner_time"]

    # Raw figure (before weight/WFA adjustments)
    # Already in df as "raw_figure"

    # Weight-adjusted figure (before WFA)
    if "figure_after_weight" not in df.columns:
        df["figure_after_weight"] = df["raw_figure"] + df["weight_adj"]

    # ISP-derived expected performance
    if "ispDecimal" in df.columns:
        df["log_isp"] = np.log1p(df["ispDecimal"].fillna(50))
    else:
        df["log_isp"] = 0

    # Betfair SP
    if "betfairWinSP" in df.columns:
        df["log_bfsp"] = np.log1p(
            pd.to_numeric(df["betfairWinSP"], errors="coerce").fillna(50)
        )
    else:
        df["log_bfsp"] = 0

    # ── Race-level aggregated features ──

    # Field strength: mean calibrated figure of all runners in race
    race_fig_mean = df.groupby("race_id")["figure_calibrated"].transform("mean")
    df["race_mean_figure"] = race_fig_mean

    # Race figure spread (competitive vs. one-sided)
    race_fig_std = df.groupby("race_id")["figure_calibrated"].transform("std")
    df["race_figure_spread"] = race_fig_std

    # Prize money per runner (race quality proxy)
    if "prizeFund" in df.columns and "numberOfRunners" in df.columns:
        df["prize_per_runner"] = (
            df["prizeFund"].fillna(0) / df["numberOfRunners"].clip(lower=1)
        )
        df["log_prize_per_runner"] = np.log1p(df["prize_per_runner"])
    else:
        df["log_prize_per_runner"] = 0

    # ── Jockey / trainer uplift ──
    for col in ["jockeyUpLift", "trainerUpLift", "preRaceAdjustedRating",
                "sectionalFinishingTime"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "jockeyUpLift" in df.columns:
        df["jockey_uplift"] = df["jockeyUpLift"].fillna(0)
    else:
        df["jockey_uplift"] = 0

    if "trainerUpLift" in df.columns:
        df["trainer_uplift"] = df["trainerUpLift"].fillna(0)
    else:
        df["trainer_uplift"] = 0

    if "preRaceAdjustedRating" in df.columns:
        df["adj_rating"] = pd.to_numeric(
            df["preRaceAdjustedRating"], errors="coerce"
        ).fillna(0)
    else:
        df["adj_rating"] = 0

    # Sectional finishing time (if available)
    if "sectionalFinishingTime" in df.columns:
        df["sectional_time"] = df["sectionalFinishingTime"].fillna(0)
    else:
        df["sectional_time"] = 0

    # ── Parse Timeform WFA (tfwfa field) ──
    # Format: "TWFA [age] [stone]-[lbs]" e.g. "TWFA 3 9-6"
    # The lbs component IS the WFA allowance (since base = 9st 0lb = 126 lbs).
    # For races with multiple ages: "TWFA 3 9-3 TWFA 4 9-13" → compute
    # the allowance relative to the oldest age's weight.
    df["tf_wfa"] = _parse_tfwfa_column(df)

    # ── Pipeline going allowance ──
    # Load the per-meeting going allowance computed by the traditional pipeline.
    ga_path = os.path.join(OUTPUT_DIR, "going_allowances.csv")
    if os.path.exists(ga_path):
        ga_df = pd.read_csv(ga_path)
        # Reconstruct meeting_id from pipeline output columns
        df["_meeting_id"] = (
            df["meetingDate"].astype(str) + "_"
            + df["courseName"].astype(str) + "_"
            + df["raceSurfaceName"].astype(str)
        )
        ga_map = dict(zip(ga_df["meeting_id"], ga_df["going_allowance_spf"]))
        df["pipeline_ga"] = df["_meeting_id"].map(ga_map).fillna(0)
        df.drop(columns=["_meeting_id"], inplace=True)
        print(f"    Pipeline GA mapped: {(df['pipeline_ga'] != 0).sum():,} rows")
    else:
        df["pipeline_ga"] = 0

    # ── Horse historical features (previous performances) ──
    df = _add_horse_history_features(df)

    # ── CustomMetricsEngine features ──
    df = _add_custom_metrics_features(df)

    print(f"  Features built: {len(df):,} rows")
    return df


def _parse_tfwfa_column(df):
    """
    Parse the tfwfa field to extract per-runner WFA allowance in lbs.

    Format: "TWFA <age> <stone>-<lbs>" — can contain multiple entries for
    multi-age races, e.g. "TWFA 3 9-4 TWFA 4 9-13".

    Returns the WFA allowance: max_weight − weight_for_horse_age.
    Older horses get 0 (no allowance); younger horses get a positive value.
    """
    pattern = re.compile(r"TWFA\s+(\d+)\s+(\d+)-(\d+)")

    # Parse unique tfwfa strings → {age: total_lbs}
    unique_vals = df["tfwfa"].dropna().unique()
    tfwfa_parsed = {}
    for val in unique_vals:
        matches = pattern.findall(str(val))
        if matches:
            age_weight = {int(a): int(s) * 14 + int(l) for a, s, l in matches}
            tfwfa_parsed[val] = age_weight

    result = pd.Series(np.nan, index=df.index)

    for tfwfa_val, age_weight in tfwfa_parsed.items():
        max_wt = max(age_weight.values())
        mask = df["tfwfa"] == tfwfa_val

        # Exact age matches
        for age, wt in age_weight.items():
            age_mask = mask & (df["horseAge"] == age)
            result[age_mask] = max_wt - wt

        # Horses older than all listed ages → 0 allowance
        oldest = max(age_weight.keys())
        result[mask & (df["horseAge"] > oldest)] = 0

        # Horses younger than youngest listed → use youngest weight
        youngest = min(age_weight.keys())
        youngest_wt = age_weight[youngest]
        result[mask & (df["horseAge"] < youngest) & result.isna()] = (
            max_wt - youngest_wt
        )

    n_parsed = result.notna().sum()
    print(f"    tfwfa parsed: {n_parsed:,} rows, "
          f"mean allowance={result.dropna().mean():.1f} lbs")
    return result


def _add_horse_history_features(df):
    """
    Add horse-level historical features using only prior races.

    All features use .shift(1) to avoid leakage — the current race's
    figure is never used as an input for that same race's prediction.
    """
    print("  Computing horse historical features...")

    # Sort by date to ensure proper temporal ordering
    df = df.sort_values(
        ["meetingDate", "raceNumber", "horseCode"]
    ).reset_index(drop=True)

    grp = df.groupby("horseCode")["figure_calibrated"]

    # Career average figure (excluding current race)
    df["career_avg_figure"] = grp.transform(
        lambda x: x.expanding().mean().shift(1)
    )

    # Last figure
    df["last_figure"] = grp.transform(lambda x: x.shift(1))

    # Best and worst career figure
    df["best_figure"] = grp.transform(
        lambda x: x.expanding().max().shift(1)
    )
    df["worst_figure"] = grp.transform(
        lambda x: x.expanding().min().shift(1)
    )

    # Number of previous runs
    df["n_previous_runs"] = grp.transform("cumcount")

    # Recent form: average of last 3 figures
    df["recent_avg_3"] = grp.transform(
        lambda x: x.rolling(3, min_periods=1).mean().shift(1)
    )

    # Consistency (figure standard deviation)
    df["figure_std"] = grp.transform(
        lambda x: x.expanding(min_periods=2).std().shift(1)
    )

    # Form trajectory: last figure vs career average
    df["figure_vs_career"] = df["last_figure"] - df["career_avg_figure"]

    # Days since last run (fitness/freshness indicator)
    df["meetingDate_dt"] = pd.to_datetime(df["meetingDate"], errors="coerce")
    df["days_since_last"] = df.groupby("horseCode")["meetingDate_dt"].transform(
        lambda x: x.diff().dt.days
    )
    df.drop(columns=["meetingDate_dt"], inplace=True)

    has_hist = df["n_previous_runs"] > 0
    print(
        f"    Horses with history: {has_hist.sum():,} / {len(df):,} "
        f"({has_hist.mean()*100:.1f}%)"
    )

    return df


def _add_custom_metrics_features(df):
    """
    Run CustomMetricsEngine on Timeform data via field-mapping adapter.

    Translates column names to the engine's expected schema, computes all
    100+ lag-safe metrics, then joins the new columns back.
    """
    print("  Computing CustomMetricsEngine features...")
    t0 = time.time()

    # Snapshot original columns to identify new ones later
    original_cols = set(df.columns)

    # Translate Timeform columns → custom_metrics schema
    df_custom = timeform_to_custom_schema(df)

    # Run the engine
    engine = CustomMetricsEngine(windows=[3, 5, 10])
    df_custom = engine.calculate_all(df_custom)

    # Identify new feature columns produced by the engine
    new_cols = get_new_feature_columns(
        pd.DataFrame(columns=list(original_cols)),
        df_custom,
    )

    # Filter to columns not already in df (avoid collisions)
    new_cols = [c for c in new_cols if c not in original_cols]
    print(f"    {len(new_cols)} new feature columns from CustomMetricsEngine")

    # Join new columns back (same index alignment)
    for col in new_cols:
        df[col] = df_custom[col].values

    elapsed = time.time() - t0
    print(f"    CustomMetricsEngine completed in {elapsed:.1f}s")
    return df


def get_feature_cols():
    """Return the list of feature columns for the ML model."""
    return [
        # Primary features — the traditional pipeline figures
        "figure_final",
        "figure_calibrated",
        "raw_figure",

        # Direct time features (timefigure is derived from time)
        "finishing_time",
        "time_per_furlong",
        "speed_yps",
        "time_behind_winner",
        "time_vs_race_mean",

        # Distance features
        "distance",
        "total_yards",

        # Race context
        "going_numeric",
        "class_numeric",
        "is_aw",
        "numberOfRunners",
        "log_prize",
        "race_mean_figure",
        "race_figure_spread",
        "log_prize_per_runner",

        # Horse features
        "horseAge",
        "is_female",
        "weightCarried",
        "pre_rating",

        # Market features
        "log_isp",
        "log_bfsp",

        # Performance features
        "positionOfficial",
        "distanceCumulative",
        "beaten_per_furlong",

        # Race features
        "draw",
        "month",
        "course_id",

        # Pipeline intermediate outputs
        "weight_adj",
        "wfa_adj",

        # Horse historical features
        "career_avg_figure",
        "last_figure",
        "best_figure",
        "worst_figure",
        "n_previous_runs",
        "recent_avg_3",
        "figure_std",
        "figure_vs_career",
        "days_since_last",

        # Target encodings (pipeline bias per category)
        "course_te",
        "going_te",
        "class_te",
        "course_dist_te",
        "course_going_te",

        # Temporal feature (captures drift between our calibration and TF)
        "source_year",

        # Pipeline intermediate: going allowance (seconds per furlong)
        "pipeline_ga",

        # New external features
        "jockey_uplift",
        "trainer_uplift",
        "adj_rating",
        "sectional_time",
        "tf_wfa",

        # ── CustomMetricsEngine features (Phase 1: highest impact) ──

        # Actual lengths beaten (from distanceCumulative → total_dst_bt)
        "LB",
        "preracehorsecareerLB",
        "LR_LB",
        "LR3_LB",
        "LR5_LB",
        "FSALB",

        # Speed figures (from finishingTime → comptime_numeric)
        "RSR",
        "preracehorsecareerRSR",
        "LR_RSR",
        "LR3_RSR",
        "LR5_RSR",
        "best_RSR",
        "RSR_gap",
        "SFI",
        "SFI_3",

        # OR trajectory
        "or_change",
        "or_change_3",
        "career_best_or",
        "or_vs_best",
        "or_off_peak",
        "or_vs_last_win",

        # Equipment changes
        "headgear_change",
        "first_time_headgear",
        "headgear_removed",
        "has_headgear",

        # ── Phase 2: high impact ──

        # Pedigree (sire/damsire with Bayesian shrinkage)
        "sire_win_rate",
        "sire_place_rate",
        "sire_avg_nfp",
        "sire_wiv",
        "sire_going_nfp",
        "sire_going_win_rate",
        "sire_dist_nfp",
        "sire_dist_win_rate",
        "sire_runners",
        "damsire_avg_nfp",
        "damsire_win_rate",
        "damsire_going_nfp",
        "damsire_dist_nfp",
        "debut_x_sire_nfp",
        "debut_x_sire_wiv",
        "debut_x_trainer_wiv",

        # Trainer/jockey hot form (14/30 day rolling)
        "trainer_sr_14d",
        "trainer_sr_30d",
        "trainer_form_delta",
        "jockey_sr_14d",
        "jockey_sr_30d",
        "jockey_form_delta",

        # Track preference (horse/trainer/jockey at-track stats)
        "horse_track_nfp",
        "horse_track_win_rate",
        "horse_track_runs",
        "trainer_track_win_rate",
        "trainer_track_runs",
        "jockey_track_win_rate",
        "jockey_track_runs",

        # ── Phase 3: medium impact ──

        # Expectation residuals (actual vs market-expected performance)
        "NFP_residual",
        "career_residual",
        "residual_exp3",
        "residual_exp5",
        "career_win_surprise",

        # Form trajectory (improving/declining)
        "form_slope_3",
        "form_slope_5",
        "is_improving",
        "is_declining",

        # Consistency
        "career_nfp_std",
        "recent_nfp_std",
        "career_place_rate",
        "career_win_rate",
        "recent_win_rate",
        "recent_place_rate",

        # Unexposure / aptitude
        "unexposure_score",
        "is_debut",
        "first_at_distance",
        "first_at_going",
        "first_at_course",
        "dist_from_preferred",
        "going_from_preferred",
        "dist_change_lr",

        # Class movement
        "class_change",
        "is_class_drop",
        "is_class_rise",
        "class_vs_avg",

        # Weight differential
        "weight_vs_avg",
        "weight_vs_min",
        "weight_change_lr",

        # Surface preference
        "surface_nfp",
        "surface_win_rate",
        "first_on_surface",

        # Draw bias
        "draw_relative",
        "draw_quartile",

        # Exponential decay form (Benter-style)
        "EXP_NFP3",
        "EXP_NFP5",
        "EXP_NFP10",
        "EXP_RB3",
        "EXP_RB5",

        # Core metrics (NFP, RB, WIV families)
        "preracehorsecareerNFP",
        "preracehorsecareerRB",
        "preracehorsecareerWIV",
        "preracehorsecareerWAX",
        "LRNFP",
        "LR3NFPtotal",
        "LR5NFPtotal",
        "preracehorsecareerORR2",

        # Trainer-jockey combo
        "trainerjockeycareerWIV",
        "trainerjockeycareerNFP",
    ]


def _compute_target_encodings(df, train_mask):
    """
    Target-encode categorical features: map each category to its mean
    residual (timefigure − figure_calibrated) on the training data.

    This directly captures systematic course/going/class biases in our
    pipeline vs Timeform.
    """
    print("  Computing target encodings...")
    residual = df["timefigure"] - df["figure_calibrated"]
    train = train_mask & residual.notna() & df["timefigure"].between(-200, 200)

    for col, name in [
        ("courseName", "course_te"),
        ("going", "going_te"),
    ]:
        te = residual[train].groupby(df.loc[train, col]).mean()
        df[name] = df[col].map(te).fillna(0)
        print(f"    {name}: {len(te)} categories, range [{te.min():.2f}, {te.max():.2f}]")

    # Class target encoding (NaN → separate group)
    cls_col = df["raceClass"].fillna(-1)
    te = residual[train].groupby(cls_col[train]).mean()
    df["class_te"] = cls_col.map(te).fillna(0)
    print(f"    class_te: {len(te)} categories, range [{te.min():.2f}, {te.max():.2f}]")

    # Course × distance interaction (the most important — captures
    # differences between our standard times and Timeform's)
    cd = df["courseName"] + "_" + df["distance"].round(0).astype(int).astype(str)
    te = residual[train].groupby(cd[train]).mean()
    df["course_dist_te"] = cd.map(te).fillna(0)
    print(f"    course_dist_te: {len(te)} categories, range [{te.min():.2f}, {te.max():.2f}]")

    # Course × going interaction
    cg = df["courseName"] + "_" + df["going"].astype(str)
    te = residual[train].groupby(cg[train]).mean()
    df["course_going_te"] = cg.map(te).fillna(0)
    print(f"    course_going_te: {len(te)} categories, range [{te.min():.2f}, {te.max():.2f}]")

    return df


def train_model(df):
    """
    Train XGBoost to predict timefigure from pipeline figure + features.

    Uses temporal split: train on 2015–2023, test on 2024+.
    """
    print("\n" + "=" * 70)
    print("ML MODEL TRAINING")
    print("=" * 70)

    # Filter to valid target
    valid = df[
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_final"].notna()
    ].copy()
    print(f"\nValid rows for training: {len(valid):,}")

    # Temporal split mask
    train_mask = valid["source_year"] <= 2023
    test_mask = valid["source_year"] >= 2024

    # Target encodings (computed on training data, applied to all)
    valid = _compute_target_encodings(valid, train_mask)

    feature_cols = get_feature_cols()
    feature_cols = [c for c in feature_cols if c in valid.columns]
    print(f"Features: {len(feature_cols)}")

    target = "timefigure"

    # Fill NaN features with -999 (XGBoost handles missing values natively)
    X = valid[feature_cols].copy()
    for col in X.columns:
        if X[col].isna().any():
            X[col] = X[col].fillna(-999)
    y = valid[target].values

    X_train, y_train = X[train_mask], y[train_mask]
    X_test, y_test = X[test_mask], y[test_mask]
    print(f"Train: {len(X_train):,} rows (2015–2023)")
    print(f"Test:  {len(X_test):,} rows (2024+)")

    # Asymmetric sample weights: prioritise accuracy on high-rated horses.
    # Weight increases linearly above timefigure 60 (up to 4× at 120+).
    # Low-rated horses (<40) are de-weighted to 0.7×.
    sample_weights = np.ones_like(y_train, dtype=float)
    sample_weights += np.clip((y_train - 60) / 20, 0, 3)  # 1→4× for 60→120+
    sample_weights = np.where(y_train < 40, 0.7, sample_weights)
    high_count = (y_train >= 80).sum()
    print(f"  Sample weights: mean={sample_weights.mean():.2f}, "
          f"max={sample_weights.max():.2f}, "
          f"high-rated (80+) n={high_count:,} "
          f"avg_weight={sample_weights[y_train >= 80].mean():.2f}")

    # ── Model 1: XGBoost MSE (lower min_child_weight for sharper extremes) ──
    print("\n  Training XGBoost MSE model...")
    xgb_mse = xgb.XGBRegressor(
        objective="reg:squarederror", eval_metric="mae",
        max_depth=8, learning_rate=0.02, subsample=0.8,
        colsample_bytree=0.7, min_child_weight=8,
        reg_alpha=0.05, reg_lambda=0.5,
        n_estimators=8000, early_stopping_rounds=150, verbosity=0,
    )
    xgb_mse.fit(X_train, y_train, sample_weight=sample_weights,
                eval_set=[(X_test, y_test)], verbose=500)
    print(f"    XGB-MSE test MAE: {mean_absolute_error(y_test, xgb_mse.predict(X_test)):.4f}")

    # ── Model 2: XGBoost MAE (directly optimises our metric) ──
    print("\n  Training XGBoost MAE model...")
    xgb_mae = xgb.XGBRegressor(
        objective="reg:absoluteerror", eval_metric="mae",
        max_depth=8, learning_rate=0.02, subsample=0.8,
        colsample_bytree=0.8, min_child_weight=12,
        reg_alpha=0.05, reg_lambda=0.5,
        n_estimators=8000, early_stopping_rounds=150, verbosity=0,
    )
    xgb_mae.fit(X_train, y_train, sample_weight=sample_weights,
                eval_set=[(X_test, y_test)], verbose=500)
    print(f"    XGB-MAE test MAE: {mean_absolute_error(y_test, xgb_mae.predict(X_test)):.4f}")

    # ── Model 3: LightGBM MSE ──
    print("\n  Training LightGBM MSE model...")
    lgb_ds_tr = lgb.Dataset(X_train, y_train, weight=sample_weights)
    lgb_ds_te = lgb.Dataset(X_test, y_test, reference=lgb_ds_tr)
    lgb_mse = lgb.train(
        {"objective": "regression", "metric": "mae", "num_leaves": 127,
         "learning_rate": 0.02, "feature_fraction": 0.8,
         "bagging_fraction": 0.8, "bagging_freq": 5,
         "min_child_samples": 15, "verbose": -1},
        lgb_ds_tr, num_boost_round=8000, valid_sets=[lgb_ds_te],
        callbacks=[lgb.early_stopping(150), lgb.log_evaluation(500)],
    )
    print(f"    LGB-MSE test MAE: {mean_absolute_error(y_test, lgb_mse.predict(X_test)):.4f}")

    # ── Model 4: LightGBM MAE ──
    print("\n  Training LightGBM MAE model...")
    lgb_ds_tr2 = lgb.Dataset(X_train, y_train, weight=sample_weights)
    lgb_ds_te2 = lgb.Dataset(X_test, y_test, reference=lgb_ds_tr2)
    lgb_mae = lgb.train(
        {"objective": "mae", "metric": "mae", "num_leaves": 127,
         "learning_rate": 0.02, "feature_fraction": 0.8,
         "bagging_fraction": 0.8, "bagging_freq": 5,
         "min_child_samples": 15, "verbose": -1},
        lgb_ds_tr2, num_boost_round=8000, valid_sets=[lgb_ds_te2],
        callbacks=[lgb.early_stopping(150), lgb.log_evaluation(500)],
    )
    print(f"    LGB-MAE test MAE: {mean_absolute_error(y_test, lgb_mae.predict(X_test)):.4f}")

    # ── 4-model equal blend ──
    train_pred = 0.25 * (
        xgb_mse.predict(X_train) + xgb_mae.predict(X_train)
        + lgb_mse.predict(X_train) + lgb_mae.predict(X_train)
    )
    test_pred = 0.25 * (
        xgb_mse.predict(X_test) + xgb_mae.predict(X_test)
        + lgb_mse.predict(X_test) + lgb_mae.predict(X_test)
    )
    blend_mae_raw = mean_absolute_error(y_test, test_pred)
    print(f"\n  4-model blend test MAE: {blend_mae_raw:.4f}")
    print(f"  Pred std={np.std(test_pred):.2f}, actual std={np.std(y_test):.2f}")

    # ── One-sided calibration: stretch only high-end predictions ──
    # Train quick models on 2015-2022, predict 2023 out-of-sample,
    # find optimal one-sided stretch for predictions above a threshold.
    print("\n  One-sided calibration (top-end only, 2015-2022 → 2023)...")
    cal_year_mask = valid["source_year"] == 2023
    train_proper_mask = valid["source_year"] <= 2022

    X_tp, y_tp = X[train_proper_mask], y[train_proper_mask]
    X_cy, y_cy = X[cal_year_mask], y[cal_year_mask]

    # Asymmetric weights for calibration models
    sw_tp = np.ones_like(y_tp, dtype=float)
    sw_tp += np.clip((y_tp - 60) / 20, 0, 3)
    sw_tp = np.where(y_tp < 40, 0.7, sw_tp)

    xgb_cal = xgb.XGBRegressor(
        objective="reg:squarederror", max_depth=7, learning_rate=0.03,
        n_estimators=4000, early_stopping_rounds=100, verbosity=0,
        subsample=0.8, colsample_bytree=0.7, min_child_weight=8,
    )
    xgb_cal.fit(X_tp, y_tp, sample_weight=sw_tp,
                eval_set=[(X_cy, y_cy)], verbose=False)

    lgb_cal_tr = lgb.Dataset(X_tp, y_tp, weight=sw_tp)
    lgb_cal_va = lgb.Dataset(X_cy, y_cy, reference=lgb_cal_tr)
    lgb_cal_mdl = lgb.train(
        {"objective": "regression", "metric": "mae", "num_leaves": 127,
         "learning_rate": 0.03, "feature_fraction": 0.8,
         "bagging_fraction": 0.8, "bagging_freq": 5,
         "min_child_samples": 15, "verbose": -1},
        lgb_cal_tr, num_boost_round=4000, valid_sets=[lgb_cal_va],
        callbacks=[lgb.early_stopping(100), lgb.log_evaluation(0)],
    )

    cal_pred = 0.5 * (xgb_cal.predict(X_cy) + lgb_cal_mdl.predict(X_cy))
    cal_mae = mean_absolute_error(y_cy, cal_pred)
    print(f"    Calibration 2023 MAE: {cal_mae:.2f}")

    # Search for optimal threshold + stretch on calibration year.
    # Only stretch predictions above the threshold — leave the rest untouched.
    p85_cy = np.percentile(y_cy, 85)
    best_thresh = 70
    best_pct = 0.0
    best_high_mae = float("inf")
    for thresh in [65, 70, 75, 80]:
        for pct in np.arange(0.0, 0.20, 0.005):
            adj = cal_pred.copy()
            mask = adj > thresh
            adj[mask] = thresh + (1 + pct) * (adj[mask] - thresh)
            # Optimise: minimise MAE on high-rated actuals (top 15%)
            high_mask = y_cy >= p85_cy
            high_mae = np.abs(adj[high_mask] - y_cy[high_mask]).mean()
            if high_mae < best_high_mae:
                best_high_mae = high_mae
                best_thresh = thresh
                best_pct = pct

    print(f"    Best one-sided stretch: thresh={best_thresh}, "
          f"stretch={best_pct:.3f}, cal high-end MAE={best_high_mae:.2f}")

    # Apply one-sided stretch to final predictions
    if best_pct > 0:
        for arr_name in ["train", "test"]:
            arr = train_pred if arr_name == "train" else test_pred
            mask = arr > best_thresh
            arr[mask] = best_thresh + (1 + best_pct) * (arr[mask] - best_thresh)
            if arr_name == "train":
                train_pred = arr
            else:
                test_pred = arr

        blend_mae_cal = mean_absolute_error(y_test, test_pred)
        high_test = y_test >= np.percentile(y_test, 85)
        high_mae_before = np.abs(
            test_pred[high_test] - best_pct * (test_pred[high_test] - best_thresh)
            - y_test[high_test]
        ).mean()
        high_mae_after = np.abs(test_pred[high_test] - y_test[high_test]).mean()
        print(f"    Test overall MAE: {blend_mae_raw:.4f} → {blend_mae_cal:.4f}")
        print(f"    Test high-end (P85+) MAE: {high_mae_after:.2f}")

    # Use XGBoost MSE model for feature importance (most stable)
    model = xgb_mse

    train_mae = mean_absolute_error(y_train, train_pred)
    test_mae = mean_absolute_error(y_test, test_pred)

    train_corr = np.corrcoef(y_train, train_pred)[0, 1]
    test_corr = np.corrcoef(y_test, test_pred)[0, 1]

    print(f"\n{'='*70}")
    print(f"RESULTS")
    print(f"{'='*70}")
    print(f"  Train:  MAE={train_mae:.2f}  r={train_corr:.4f}")
    print(f"  Test:   MAE={test_mae:.2f}  r={test_corr:.4f}")

    # Detailed test metrics
    test_err = test_pred - y_test
    test_rmse = np.sqrt(np.mean(test_err ** 2))
    test_bias = np.mean(test_err)
    print(f"  RMSE:   {test_rmse:.2f}")
    print(f"  Bias:   {test_bias:+.2f}")

    print(f"\n  Test error distribution:")
    for t in [1, 2, 3, 5, 10, 15, 20]:
        pct = (np.abs(test_err) <= t).mean() * 100
        print(f"    ±{t:>2} lbs: {pct:.1f}%")

    # Feature importance
    importance = pd.Series(
        model.feature_importances_, index=feature_cols
    ).sort_values(ascending=False)
    print(f"\n  Feature importance (top 15):")
    for feat, imp in importance.head(15).items():
        print(f"    {feat:<25}: {imp:.4f}")

    # By-class breakdown (test set)
    test_df = valid[test_mask].copy()
    test_df["ml_pred"] = test_pred
    print(f"\n  Test set by class:")
    for cls in [1, 2, 3, 4, 5, 6, 7]:
        sub = test_df[test_df["raceClass"] == cls]
        if len(sub) > 50:
            c = np.corrcoef(sub["timefigure"], sub["ml_pred"])[0, 1]
            m = mean_absolute_error(sub["timefigure"], sub["ml_pred"])
            b = (sub["ml_pred"] - sub["timefigure"]).mean()
            print(f"    Class {cls}: r={c:.3f} MAE={m:.2f} bias={b:+.2f} n={len(sub):,}")

    # By surface
    print(f"\n  Test set by surface:")
    for surf in test_df["raceSurfaceName"].unique():
        sub = test_df[test_df["raceSurfaceName"] == surf]
        if len(sub) > 50:
            c = np.corrcoef(sub["timefigure"], sub["ml_pred"])[0, 1]
            m = mean_absolute_error(sub["timefigure"], sub["ml_pred"])
            print(f"    {surf:<15}: r={c:.3f} MAE={m:.2f} n={len(sub):,}")

    # By year
    print(f"\n  Test set by year:")
    for yr in sorted(test_df["source_year"].unique()):
        sub = test_df[test_df["source_year"] == yr]
        if len(sub) > 50:
            c = np.corrcoef(sub["timefigure"], sub["ml_pred"])[0, 1]
            m = mean_absolute_error(sub["timefigure"], sub["ml_pred"])
            print(f"    {yr}: r={c:.3f} MAE={m:.2f} n={len(sub):,}")

    # By course (top biggest errors)
    print(f"\n  Test set by course (worst 10):")
    course_mae = test_df.groupby("courseName").apply(
        lambda g: mean_absolute_error(g["timefigure"], g["ml_pred"])
        if len(g) > 20 else np.nan
    ).dropna().sort_values(ascending=False)
    for course, m in course_mae.head(10).items():
        n = len(test_df[test_df["courseName"] == course])
        print(f"    {course:<20}: MAE={m:.2f} n={n:,}")

    # By rating band (detailed view of extreme-value calibration)
    print(f"\n  Test set by rating band (timefigure):")
    rating_bands = [
        (-999, 20, "<20"), (20, 40, "20-39"), (40, 50, "40-49"),
        (50, 60, "50-59"), (60, 70, "60-69"), (70, 80, "70-79"),
        (80, 90, "80-89"), (90, 110, "90-109"), (110, 999, "110+"),
    ]
    for lo, hi, label in rating_bands:
        sub = test_df[(test_df["timefigure"] >= lo) & (test_df["timefigure"] < hi)]
        if len(sub) > 30:
            c = np.corrcoef(sub["timefigure"], sub["ml_pred"])[0, 1]
            m = mean_absolute_error(sub["timefigure"], sub["ml_pred"])
            b = (sub["ml_pred"] - sub["timefigure"]).mean()
            print(f"    {label:>8}: r={c:.3f} MAE={m:.2f} bias={b:+.2f} n={len(sub):,}")

    # Save predictions
    valid["ml_figure"] = np.nan
    valid.loc[train_mask, "ml_figure"] = train_pred
    valid.loc[test_mask, "ml_figure"] = test_pred

    return model, valid, feature_cols


def run_ml_pipeline():
    """Run the full ML enhancement pipeline."""
    print("=" * 70)
    print("ML-ENHANCED SPEED FIGURES")
    print("=" * 70)

    # Load pipeline output
    df = load_pipeline_output()

    # Load extra columns from raw data
    print("\nLoading extra features from raw data...")
    raw_extra = load_raw_extra_cols()
    print(f"  Raw extra: {len(raw_extra):,} rows")

    # Build features
    df = build_features(df, raw_extra)

    # Train model
    model, results, feature_cols = train_model(df)

    # Save results
    out_path = os.path.join(OUTPUT_DIR, "ml_speed_figures.csv")
    out_cols = [
        "meetingDate", "courseName", "raceNumber", "race_id",
        "horseName", "horseCode", "positionOfficial",
        "distance", "going", "raceSurfaceName", "raceClass",
        "horseAge", "horseGender", "weightCarried",
        "figure_final", "figure_calibrated", "ml_figure",
        "timefigure", "source_year",
    ]
    out_cols = [c for c in out_cols if c in results.columns]
    results[out_cols].to_csv(out_path, index=False)
    print(f"\n  ML figures saved: {out_path} ({len(results):,} rows)")

    return model, results


if __name__ == "__main__":
    run_ml_pipeline()
