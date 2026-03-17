"""
Speed Figure Compilation Pipeline — France
============================================
Computes speed figures for French flat racing from PMU data stored in
the SQLite database.

Pipeline stages (mirroring the UK pipeline in ``src/speed_figures.py``):
  0. Load & filter data (French flat, valid finishing times)
  1. Compute standard times per track / distance / surface
  2. Compute course-specific lbs-per-length from standard times
  3. Compute going allowances per track per day
  4. Compute winner speed figures
  5. Extend to all runners via beaten lengths
  6. Apply weight-carried adjustment
  7. Apply weight-for-age (WFA) adjustment
  8. Apply sex allowance (not applied — same finding as UK)
  9. Self-calibration (distribution matching by class to UK reference)
  9b. Beaten-length band correction
  9c. Empirical GA prior refinement
"""

import logging
import os
import pickle
from collections import defaultdict

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from . import constants as C
from .constants import (
    BASE_RATING,
    BASE_WEIGHT_LBS,
    BENCHMARK_FURLONGS,
    BL_ATTENUATION_FACTOR,
    BL_ATTENUATION_THRESHOLD,
    CLASS_ADJUSTMENT_PER_MILE,
    FRANCE_GOING_GA_PRIOR,
    FRANCE_GOOD_GOING,
    GA_NONLINEAR_BETA,
    GA_NONLINEAR_THRESHOLD,
    GA_OUTLIER_ZSCORE,
    GA_SHRINKAGE_K,
    INTERPOLATED_GA_WEIGHT,
    LBS_PER_SECOND_5F,
    LPL_SURFACE_MULTIPLIER,
    MIN_RACES_GOING_ALLOWANCE,
    RECENCY_HALF_LIFE_YEARS,
    SECONDS_PER_LENGTH,
)
from .database import DailyFigureRow, RunnerRow
from .field_mapping import load_france_dataframe

log = logging.getLogger(__name__)


# ── UK reference distributions for self-calibration (by class) ──
# Mean and std of Timeform timefigures per class from UK data (2015-2025).
# Used as the target distribution for French self-calibration.
# Derived from the UK pipeline's calibrated output.
UK_CLASS_DISTRIBUTION = {
    "1": {"mean": 108.0, "std": 11.0},  # Group races
    "2": {"mean": 99.0,  "std": 10.5},  # Listed / Premier
    "3": {"mean": 91.0,  "std": 10.0},
    "4": {"mean": 83.0,  "std": 10.0},
    "5": {"mean": 75.0,  "std": 10.0},
    "6": {"mean": 68.0,  "std": 10.0},
    "7": {"mean": 60.0,  "std": 10.5},  # Lowest class
}


# ═════════════════════════════════════════════════════════════════════
# STAGE 1 — STANDARD TIMES  (per track / distance / surface)
# ═════════════════════════════════════════════════════════════════════

def compute_class_adjustment(race_class, distance_furlongs):
    """
    Class adjustment in seconds for a given class and distance.

    Same finding as UK: varying class adjustments hurt accuracy.
    Returns a constant baseline (class 4) so the subtraction is
    effectively a no-op absorbed by calibration.
    """
    adj_per_mile = CLASS_ADJUSTMENT_PER_MILE["4"]
    return (adj_per_mile * distance_furlongs) / 8.0


def _filter_std_time_winners(winners):
    """
    Filter winners for standard-time compilation.

    Excludes:
      - Maiden races (detected heuristically from race_name)
      - 2yo-only races (all runners in the race have age == 2)
    """
    mask = pd.Series(True, index=winners.index)

    # Exclude maidens
    if "is_maiden" in winners.columns:
        mask &= ~winners["is_maiden"].fillna(False)

    # Exclude 2yo-only races: if all runners in the race are age 2
    if "horseAge" in winners.columns and "race_id" in winners.columns:
        race_max_age = winners.groupby("race_id")["horseAge"].transform("max")
        race_min_age = winners.groupby("race_id")["horseAge"].transform("min")
        is_2yo_only = (race_max_age == 2) & (race_min_age == 2)
        n_2yo = is_2yo_only.sum()
        if n_2yo > 0:
            log.info("    Excluded %s 2yo-only winners from standard-time compilation",
                     f"{n_2yo:,}")
        mask &= ~is_2yo_only

    n_excluded = (~mask).sum()
    if n_excluded > 0:
        log.info("    Excluded %s maiden/2yo-only winners from standard-time compilation",
                 f"{n_excluded:,}")

    return winners[mask].copy()


def compute_standard_times(df):
    """
    Standard times per track / distance / surface.

    Same algorithm as UK ``compute_standard_times()``:
      1. Collect winning times (excluding maidens + 2yo-only)
      2. Prefer races on good going; fall back to all goings
      3. Take the median → standard time
    """
    log.info("  Computing standard times (per track / distance / surface)...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = _filter_std_time_winners(winners)
    winners_good = winners[winners["going"].isin(FRANCE_GOOD_GOING)].copy()

    log.info("    Winners total: %s", f"{len(winners):,}")
    log.info("    Winners on good going: %s", f"{len(winners_good):,}")

    # No class adjustment for France — the constant offset cancels between
    # standard-time and figure computation (audit confirmed this is correct).
    winners_good["adj_time"] = winners_good["finishingTime"]

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
            std_times["n_races"] >= C.MIN_RACES_STANDARD_TIME, "std_key"
        ]
    )
    needs_fallback = all_std_keys - good_keys

    if needs_fallback:
        winners_all = winners.copy()
        winners_all["adj_time"] = winners_all["finishingTime"]
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
    valid = std_times[std_times["n_races"] >= C.MIN_RACES_STANDARD_TIME].copy()
    log.info("    Standard-time combos (>= %d races): %s",
             C.MIN_RACES_STANDARD_TIME, f"{len(valid):,}")
    log.info("    Dropped (insufficient data): %s",
             f"{len(std_times) - len(valid):,}")

    std_dict = dict(zip(valid["std_key"], valid["median_time"]))
    return std_dict, valid


def compute_standard_times_iterative(df, going_allowances):
    """
    Recompute standard times using going-corrected times from ALL goings.

    Now includes recency weighting (half-life 4yr) matching the UK pipeline,
    so more recent data is weighted more heavily.
    """
    log.info("    Recomputing standard times (going-corrected, all goings, recency-weighted)...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = _filter_std_time_winners(winners)

    # Apply going correction
    winners["ga"] = winners["meeting_id"].map(going_allowances).fillna(0)
    winners["corrected_time"] = (
        winners["finishingTime"] - (winners["ga"] * winners["distance"])
    )

    # No class adjustment for France (see compute_standard_times comment)
    winners["adj_time"] = winners["corrected_time"]

    # Recency weighting: half-life of 4 years (same as UK pipeline).
    # More recent data counts more, tracking surface/configuration drift.
    if "source_year" in winners.columns:
        max_year = winners["source_year"].max()
        winners["years_ago"] = max_year - winners["source_year"]
        winners["recency_weight"] = 0.5 ** (winners["years_ago"] / RECENCY_HALF_LIFE_YEARS)
    else:
        winners["recency_weight"] = 1.0

    def _weighted_median(group):
        times = group["adj_time"].values
        weights = group["recency_weight"].values
        sorted_idx = np.argsort(times)
        times = times[sorted_idx]
        weights = weights[sorted_idx]
        cum_w = np.cumsum(weights)
        mid = cum_w[-1] / 2.0
        idx = np.searchsorted(cum_w, mid)
        return times[min(idx, len(times) - 1)]

    std_rows = []
    for key, grp in winners.groupby("std_key"):
        std_rows.append({
            "std_key": key,
            "median_time": _weighted_median(grp),
            "mean_time": np.average(grp["adj_time"], weights=grp["recency_weight"]),
            "n_races": len(grp),
            "distance": grp["distance"].iloc[0],
            "courseName": grp["courseName"].iloc[0],
            "surface": grp["raceSurfaceName"].iloc[0],
        })
    std_agg = pd.DataFrame(std_rows)

    valid = std_agg[std_agg["n_races"] >= C.MIN_RACES_STANDARD_TIME].copy()

    # Shrinkage for combos with 10+ but < MIN_RACES_STANDARD_TIME races:
    # blend their median with the overall distance median (same as UK
    # Irish shrinkage, applied to all French combos).
    SHRINKAGE_K = 10
    below_threshold = std_agg[
        (std_agg["n_races"] >= 10)
        & (std_agg["n_races"] < C.MIN_RACES_STANDARD_TIME)
    ].copy()

    if len(below_threshold) > 0 and len(valid) > 0:
        dist_median = valid.groupby(valid["distance"].round(0))["median_time"].median()
        for idx, row in below_threshold.iterrows():
            d_round = round(row["distance"])
            if d_round in dist_median.index:
                generic_std = dist_median[d_round]
                n = row["n_races"]
                blended = (n * row["median_time"] + SHRINKAGE_K * generic_std) / (n + SHRINKAGE_K)
                below_threshold.loc[idx, "median_time"] = blended
        valid = pd.concat([valid, below_threshold], ignore_index=True)
        log.info("    Shrinkage combos added: %s", f"{len(below_threshold):,}")

    log.info("    Standard-time combos (>= %d races): %s (using all goings, recency-weighted)",
             C.MIN_RACES_STANDARD_TIME, f"{len(valid):,}")

    std_dict = dict(zip(valid["std_key"], valid["median_time"]))
    return std_dict, valid


# ═════════════════════════════════════════════════════════════════════
# STAGE 2 — COURSE-SPECIFIC LBS PER LENGTH
# ═════════════════════════════════════════════════════════════════════

def generic_lbs_per_length(distance_furlongs, surface=None):
    """
    Generic lbs-per-length from distance (and optionally surface).
    Same formula as UK pipeline.
    """
    lbs_per_sec = LBS_PER_SECOND_5F * (BENCHMARK_FURLONGS / distance_furlongs)
    base_lpl = SECONDS_PER_LENGTH * lbs_per_sec
    if surface is not None:
        base_lpl *= LPL_SURFACE_MULTIPLIER.get(surface, 1.0)
    return base_lpl


def compute_course_lpl(std_df):
    """
    Course-specific lbs-per-length for each standard-time combo.
    Same algorithm as UK ``compute_course_lpl()``.
    """
    log.info("  Computing course-specific lbs-per-length...")

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
    log.info("    Computed lpl for %s track/distance combos", f"{len(lpl_dict):,}")

    return lpl_dict


# ═════════════════════════════════════════════════════════════════════
# STAGE 3 — GOING ALLOWANCE  (per track, per day)
# ═════════════════════════════════════════════════════════════════════

def _build_interpolated_std_times(std_times):
    """Interpolate standard times for missing distances at each course/surface.

    For GA inference only — NOT used for final figure computation.
    Ported from UK pipeline: uses linear interpolation between adjacent
    known distances at the same course/surface.  Only interpolates WITHIN
    the range of known distances (no extrapolation).

    Returns: dict of std_key → interpolated_time (excludes keys already
    in std_times).
    """
    course_surface_dists = defaultdict(dict)
    for key, time_val in std_times.items():
        parts = key.rsplit("_", 2)
        if len(parts) != 3:
            continue
        course, dist_str, surface = parts
        try:
            dist = float(dist_str)
        except ValueError:
            continue
        course_surface_dists[(course, surface)][dist] = time_val

    interpolated = {}
    for (course, surface), dist_time in course_surface_dists.items():
        if len(dist_time) < 2:
            continue
        dists = sorted(dist_time.keys())
        times = [dist_time[d] for d in dists]

        # Generate 0.5f increments within [min, max] known distances
        d = dists[0]
        while d <= dists[-1]:
            key = f"{course}_{d}_{surface}"
            if key not in std_times:
                # Find bracketing known distances
                lo_idx = 0
                for i, kd in enumerate(dists):
                    if kd <= d:
                        lo_idx = i
                    else:
                        break
                hi_idx = min(lo_idx + 1, len(dists) - 1)
                if lo_idx != hi_idx:
                    lo_d, hi_d = dists[lo_idx], dists[hi_idx]
                    lo_t, hi_t = times[lo_idx], times[hi_idx]
                    frac = (d - lo_d) / (hi_d - lo_d)
                    interpolated[key] = lo_t + frac * (hi_t - lo_t)
            d = round(d + 0.5, 1)

    return interpolated


def _temporal_neighbor_ga(undersized_meetings, ga_dict):
    """For meetings without GA, borrow from same course/surface ±1 day.

    Ported from UK pipeline.
    Returns: dict of meeting_id → inferred GA for recovered meetings.
    """
    recovered = {}
    for meeting_id in undersized_meetings:
        parts = meeting_id.split("_", 2)
        if len(parts) != 3:
            continue
        date_str, course, surface = parts
        try:
            date_dt = pd.to_datetime(date_str)
        except Exception:
            continue

        neighbor_gas = []
        for delta in [-1, +1]:
            neighbor_date = (date_dt + pd.Timedelta(days=delta)).strftime("%Y-%m-%d")
            neighbor_id = f"{neighbor_date}_{course}_{surface}"
            if neighbor_id in ga_dict:
                neighbor_gas.append(ga_dict[neighbor_id])

        if neighbor_gas:
            recovered[meeting_id] = np.mean(neighbor_gas)

    return recovered


def compute_going_allowances(df, std_times):
    """
    Going allowance per meeting in seconds-per-furlong (s/f).

    Now includes (ported from UK pipeline):
      - Interpolated standard times for GA inference (more winners contribute)
      - Weighted winsorized mean (interpolated deviations get lower weight)
      - Temporal neighbor pooling (meetings with too few races borrow ±1 day)
    """
    log.info("  Computing going allowances (per track / day)...")

    # Layer 1: Interpolated standard times for GA inference
    interp_std = _build_interpolated_std_times(std_times)
    all_std_for_ga = {**interp_std, **std_times}  # exact takes precedence

    winners = df[df["positionOfficial"] == 1].copy()
    winners = winners[winners["std_key"].isin(all_std_for_ga)].copy()
    winners["standard_time"] = winners["std_key"].map(all_std_for_ga)
    winners["ga_weight"] = np.where(
        winners["std_key"].isin(std_times), 1.0, INTERPOLATED_GA_WEIGHT
    )

    n_exact = winners["std_key"].isin(std_times).sum()
    n_interp = len(winners) - n_exact
    log.info("    Winners for GA: %s exact + %s interpolated",
             f"{n_exact:,}", f"{n_interp:,}")

    # No class adjustment for France (consistent with standard times)
    # Per-furlong deviation from standard
    winners["deviation"] = winners["finishingTime"] - winners["standard_time"]
    winners["dev_per_furlong"] = winners["deviation"] / winners["distance"]

    # ── Per-meeting outlier removal ──
    meeting_medians = winners.groupby("meeting_id")["dev_per_furlong"].median()
    meeting_stds = winners.groupby("meeting_id")["dev_per_furlong"].std()
    winners["_meeting_med"] = winners["meeting_id"].map(meeting_medians)
    winners["_meeting_std"] = winners["meeting_id"].map(meeting_stds)
    has_std = winners["_meeting_std"].notna() & (winners["_meeting_std"] > 0)
    z_scores = (
        (winners["dev_per_furlong"] - winners["_meeting_med"])
        / winners["_meeting_std"]
    ).abs()
    winners = winners[~has_std | (z_scores <= GA_OUTLIER_ZSCORE)].copy()
    winners.drop(columns=["_meeting_med", "_meeting_std"], inplace=True)

    # ── Split-card going detection ──
    split_meetings = set()
    for mid, group in winners.groupby("meeting_id"):
        if len(group) < 6:
            continue
        ordered = group.sort_values("raceNumber")
        n = len(ordered)
        half = n // 2
        first_half = ordered.iloc[:half]["dev_per_furlong"]
        second_half = ordered.iloc[half:]["dev_per_furlong"]

        n1, n2 = len(first_half), len(second_half)
        if n1 < 2 or n2 < 2:
            continue
        m1, m2 = first_half.mean(), second_half.mean()
        s1, s2 = first_half.std(), second_half.std()

        if s1 == 0 and s2 == 0:
            continue
        se = np.sqrt(s1**2 / n1 + s2**2 / n2)
        if se == 0:
            continue
        t_stat = abs(m1 - m2) / se

        if t_stat > 2.5 and abs(m1 - m2) > 0.10:
            winners.loc[ordered.index[:half], "meeting_id"] = mid + "_early"
            winners.loc[ordered.index[half:], "meeting_id"] = mid + "_late"
            split_meetings.add(mid)

    if split_meetings:
        log.info("    Split-card going detected: %d meetings split",
                 len(split_meetings))

    # ── Weighted winsorized mean within each meeting ──
    # Interpolated deviations get lower weight (INTERPOLATED_GA_WEIGHT).
    def _weighted_winsorized_mean(group):
        sorted_g = group.sort_values("dev_per_furlong")
        devs = sorted_g["dev_per_furlong"].values.copy()
        weights = sorted_g["ga_weight"].values.copy()
        n = len(devs)
        if n <= 2:
            return np.average(devs, weights=weights)
        # Winsorize: clamp extremes to adjacent values
        devs[0] = devs[1]
        devs[-1] = devs[-2]
        return np.average(devs, weights=weights)

    ga_series = (
        winners.groupby("meeting_id")[["dev_per_furlong", "ga_weight"]]
        .apply(_weighted_winsorized_mean)
    )
    ga_count = (
        winners.groupby("meeting_id")["dev_per_furlong"].count()
    )

    # ── GA standard error per meeting ──
    def _winsorized_se(group):
        vals = group.sort_values().values.copy()
        n = len(vals)
        if n <= 1:
            return np.nan
        if n > 2:
            vals[0] = vals[1]
            vals[-1] = vals[-2]
        return np.std(vals, ddof=1) / np.sqrt(n)

    ga_se_series = (
        winners.groupby("meeting_id")["dev_per_furlong"]
        .apply(_winsorized_se)
    )

    # Keep only meetings with enough races
    valid_ids = ga_count[ga_count >= MIN_RACES_GOING_ALLOWANCE].index
    ga_dict = ga_series[ga_series.index.isin(valid_ids)].to_dict()
    n_exact_ga = len(ga_dict)
    ga_se_dict = ga_se_series[ga_se_series.index.isin(valid_ids)].to_dict()
    ga_n = ga_count[ga_count.index.isin(valid_ids)].to_dict()

    # Layer 2: Temporal neighbor pooling for meetings still missing GA
    all_meeting_ids = set(df["meeting_id"].unique())
    undersized = all_meeting_ids - set(ga_dict.keys())
    temporal_ga = _temporal_neighbor_ga(undersized, ga_dict)
    n_temporal = len(temporal_ga)
    ga_dict.update(temporal_ga)

    log.info("    Meetings with going allowance: %s (direct)", f"{n_exact_ga:,}")
    if n_temporal > 0:
        log.info("    Meetings recovered via temporal neighbors: +%s", f"{n_temporal:,}")
    log.info("    Total meetings with GA: %s", f"{len(ga_dict):,}")

    # ── Bayesian shrinkage toward French going-description prior ──
    meeting_going = (
        df.groupby("meeting_id")["going"].first().to_dict()
    )

    shrunk_dict = {}
    for mid, raw_ga in ga_dict.items():
        n = ga_n.get(mid, MIN_RACES_GOING_ALLOWANCE)
        going_desc = meeting_going.get(
            mid.replace("_early", "").replace("_late", ""), "Bon"
        )
        prior_ga = FRANCE_GOING_GA_PRIOR.get(going_desc, 0.0)

        k = GA_SHRINKAGE_K
        shrunk_ga = (n * raw_ga + k * prior_ga) / (n + k)
        shrunk_dict[mid] = shrunk_ga

    ga_dict = shrunk_dict

    # ── Non-linear correction for extreme going ──
    corrected_dict = {}
    for mid, ga in ga_dict.items():
        abs_ga = abs(ga)
        if abs_ga > GA_NONLINEAR_THRESHOLD:
            sign = 1.0 if ga > 0 else -1.0
            excess = abs_ga - GA_NONLINEAR_THRESHOLD
            correction = GA_NONLINEAR_BETA * excess ** 2
            corrected_dict[mid] = ga + sign * correction
        else:
            corrected_dict[mid] = ga

    ga_dict = corrected_dict

    log.info("    Meetings with going allowance: %s", f"{len(ga_dict):,}")
    if ga_dict:
        log.info("    GA range: %.3f to %.3f s/f",
                 min(ga_dict.values()), max(ga_dict.values()))
        log.info("    GA mean:  %.3f s/f", np.mean(list(ga_dict.values())))

    return ga_dict, ga_se_dict


# ═════════════════════════════════════════════════════════════════════
# STAGE 4 — WINNER SPEED FIGURES
# ═════════════════════════════════════════════════════════════════════

def compute_winner_figures(df, std_times, going_allowances, lpl_dict):
    """
    Speed figures for race winners.
    Same formula as UK ``compute_winner_figures()``.
    """
    log.info("  Computing winner speed figures...")

    w = df[df["positionOfficial"] == 1].copy()
    w = w[
        w["std_key"].isin(std_times)
        & w["meeting_id"].isin(going_allowances)
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

    log.info("    Winner figures computed: %s", f"{len(w):,}")
    if len(w) > 0:
        log.info("    Range: %.0f to %.0f", w["raw_figure"].min(), w["raw_figure"].max())
        log.info("    Mean:  %.1f", w["raw_figure"].mean())

    winner_fig = dict(zip(w["race_id"], w["raw_figure"]))
    return w, winner_fig


# ═════════════════════════════════════════════════════════════════════
# STAGE 5 — ALL-RUNNER FIGURES  (beaten lengths)
# ═════════════════════════════════════════════════════════════════════

def compute_all_figures(df, winner_fig_dict, lpl_dict, std_times=None,
                        going_allowances=None):
    """
    Extend figures to every runner via cumulative beaten lengths.
    Same algorithm as UK ``compute_all_figures()``.
    """
    log.info("  Extending figures to all runners...")

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

    # Velocity-weighted LPL
    if std_times is not None:
        winners = out[out["positionOfficial"] == 1][["race_id", "finishingTime"]].copy()
        winners = winners.rename(columns={"finishingTime": "winner_time"})
        winners = winners.drop_duplicates(subset="race_id")
        out = out.merge(winners, on="race_id", how="left")

        out["standard_time"] = out["std_key"].map(std_times)
        has_both = (
            out["standard_time"].notna()
            & out["winner_time"].notna()
            & (out["winner_time"] > 0)
        )
        velocity_ratio = (out["standard_time"] / out["winner_time"]).clip(0.85, 1.15)
        out.loc[has_both, "lpl"] = out.loc[has_both, "lpl"] * velocity_ratio[has_both]
        n_adjusted = has_both.sum()
        log.info("    Velocity-weighted LPL applied to %s runners", f"{n_adjusted:,}")
        out.drop(columns=["winner_time", "standard_time"], inplace=True, errors="ignore")

    is_winner = out["positionOfficial"] == 1
    cum_raw = out["distanceCumulative"].fillna(0).clip(lower=0)

    # Going-dependent beaten-length attenuation (same as UK)
    F = BL_ATTENUATION_FACTOR
    if going_allowances is not None:
        ga = out["meeting_id"].map(going_allowances).fillna(0)
        T = np.clip(BL_ATTENUATION_THRESHOLD + (ga * -8), 10, 30)
    else:
        T = BL_ATTENUATION_THRESHOLD
    cum = np.where(cum_raw <= T, cum_raw, T + F * (cum_raw - T))

    out["lbs_behind"] = cum * out["lpl"]
    out.loc[is_winner, "lbs_behind"] = 0.0
    out["raw_figure"] = out["winner_figure"] - out["lbs_behind"]

    # Non-finishers
    no_pos = out["positionOfficial"].isna() | (out["positionOfficial"] == 0)
    out.loc[no_pos, "raw_figure"] = np.nan

    log.info("    All-runner figures: %s", f"{len(out):,}")
    return out


# ═════════════════════════════════════════════════════════════════════
# STAGE 6 — WEIGHT-CARRIED ADJUSTMENT
# ═════════════════════════════════════════════════════════════════════

def apply_weight_adjustment(df):
    """
    Adjust for weight carried.
    Same formula as UK: figure += (weight_carried − base_weight).
    """
    log.info("  Applying weight-carried adjustment...")

    has_w = df["weightCarried"].notna()
    log.info("    Runners with weight data: %s / %s",
             f"{has_w.sum():,}", f"{len(df):,}")

    df["weight_adj"] = 0.0
    df.loc[has_w, "weight_adj"] = (
        df.loc[has_w, "weightCarried"] - BASE_WEIGHT_LBS
    )
    df["figure_after_weight"] = df["raw_figure"] + df["weight_adj"]
    return df


# ═════════════════════════════════════════════════════════════════════
# STAGE 7 — WEIGHT FOR AGE  (WFA)
# ═════════════════════════════════════════════════════════════════════

def apply_wfa_adjustment(df):
    """
    Weight-for-age adjustment.
    Imports ``get_wfa_allowance`` from the UK pipeline (same biological
    curve; reused for France initially).
    """
    from src.speed_figures import get_wfa_allowance

    log.info("  Applying WFA adjustment...")

    df["wfa_adj"] = df.apply(
        lambda r: get_wfa_allowance(
            r["horseAge"], r["month"], r["distance"],
            r.get("raceSurfaceName"),
        ),
        axis=1,
    )

    has = df["wfa_adj"] > 0
    log.info("    Runners with WFA: %s / %s",
             f"{has.sum():,}", f"{len(df):,}")
    if has.any():
        log.info("    WFA range: %.1f - %.1f lbs",
                 df.loc[has, "wfa_adj"].min(), df.loc[has, "wfa_adj"].max())

    df["figure_after_wfa"] = df["figure_after_weight"] + df["wfa_adj"]
    return df


# ═════════════════════════════════════════════════════════════════════
# STAGE 8 — SEX ALLOWANCE (not applied)
# ═════════════════════════════════════════════════════════════════════

def apply_sex_allowance(df):
    """
    Sex allowance for fillies/mares.
    NOT applied — same finding as UK (empirically hurts accuracy).
    """
    log.info("  Sex allowance: NOT applied (same finding as UK)")
    df["sex_adj"] = 0.0
    df["figure_after_sex"] = df["figure_after_wfa"]
    return df


# ═════════════════════════════════════════════════════════════════════
# STAGE 9 — SELF-CALIBRATION (distribution matching by class)
# ═════════════════════════════════════════════════════════════════════

def calibrate_to_uk_scale(df):
    """
    Self-calibration: align French figure distribution to UK reference
    distribution by class.

    Since no Timeform timefigure exists for French racing, we use the
    known UK class distributions as calibration targets.  For each class,
    we compute the French mean and std, then apply a linear transform:

        calibrated = UK_mean + (raw - FR_mean) × (UK_std / FR_std)

    This ensures French Group 1 figures have the same mean/spread as UK
    Group 1 figures, French Class 4 matches UK Class 4, etc.

    Additionally applies:
      - Beaten-length band correction (horses beaten further systematically
        over-rated due to margin estimation noise)
      - Per-going-group residual correction
      - Continuous GA correction
    """
    log.info("  Calibrating to UK scale (distribution matching by class)...")

    has_fig = df["figure_after_sex"].notna()
    if not has_fig.any():
        df["figure_calibrated"] = df["figure_after_sex"]
        return df, {}

    df["figure_calibrated"] = df["figure_after_sex"].copy()
    cal_params = {}

    # ── Per-class distribution matching ──
    for cls, uk_dist in UK_CLASS_DISTRIBUTION.items():
        cls_mask = (df["raceClass"] == cls) & has_fig
        if cls_mask.sum() < 30:
            continue

        fr_vals = df.loc[cls_mask, "figure_after_sex"]
        fr_mean = fr_vals.mean()
        fr_std = fr_vals.std()

        if fr_std == 0 or pd.isna(fr_std):
            continue

        uk_mean = uk_dist["mean"]
        uk_std = uk_dist["std"]

        # Linear transform: map FR distribution → UK distribution
        scale = uk_std / fr_std
        shift = uk_mean - fr_mean * scale

        df.loc[cls_mask, "figure_calibrated"] = (
            df.loc[cls_mask, "figure_after_sex"] * scale + shift
        )

        cal_params[cls] = {
            "fr_mean": fr_mean, "fr_std": fr_std,
            "uk_mean": uk_mean, "uk_std": uk_std,
            "scale": scale, "shift": shift,
            "n_runners": int(cls_mask.sum()),
        }
        log.info("    Class %s: FR(%.1f±%.1f) → UK(%.1f±%.1f)  scale=%.3f shift=%+.1f  n=%s",
                 cls, fr_mean, fr_std, uk_mean, uk_std, scale, shift,
                 f"{cls_mask.sum():,}")

    # For classes not matched (insufficient data), apply the global transform
    unmatched = has_fig & df["figure_calibrated"].eq(df["figure_after_sex"])
    if unmatched.any() and cal_params:
        # Use the weighted average transform across all matched classes
        total_n = sum(p["n_runners"] for p in cal_params.values())
        avg_scale = sum(p["scale"] * p["n_runners"] for p in cal_params.values()) / total_n
        avg_shift = sum(p["shift"] * p["n_runners"] for p in cal_params.values()) / total_n
        df.loc[unmatched, "figure_calibrated"] = (
            df.loc[unmatched, "figure_after_sex"] * avg_scale + avg_shift
        )
        log.info("    Unmatched classes: applied global scale=%.3f shift=%+.1f to %s runners",
                 avg_scale, avg_shift, f"{unmatched.sum():,}")

    # ── Beaten-length band correction ──
    # Horses beaten further are systematically over-rated because beaten-length
    # estimates compress at larger margins.  Compute mean figure by BL band
    # for winners vs beaten horses and apply correction.
    bl_corrections = _compute_bl_band_corrections(df)
    if bl_corrections:
        cum_bl = df["distanceCumulative"].fillna(0).clip(lower=0)
        bl_band = pd.cut(
            cum_bl, bins=[0, 1, 3, 5, 10, 15, 20, 999],
            labels=["0-1", "1-3", "3-5", "5-10", "10-15", "15-20", "20+"],
            include_lowest=True,
        ).astype(str).fillna("0-1")
        # Winners get their own band
        bl_band = bl_band.where(df["positionOfficial"] != 1, "winner")

        bl_adj = bl_band.map(bl_corrections).fillna(0)
        df.loc[has_fig, "figure_calibrated"] += bl_adj[has_fig]

        bl_str = ", ".join(f"{k}:{v:+.1f}" for k, v in sorted(bl_corrections.items()))
        log.info("    BL band corrections: %s", bl_str)

    # ── Per-going-group residual correction ──
    going_corrections = _compute_going_corrections(df)
    if going_corrections:
        going_group_map = _french_going_group_map()
        df_going_grp = df["going"].map(going_group_map).fillna("Bon")
        going_adj = df_going_grp.map(going_corrections).fillna(0)
        df.loc[has_fig, "figure_calibrated"] += going_adj[has_fig]

        going_str = ", ".join(f"{k}:{v:+.1f}" for k, v in sorted(going_corrections.items()))
        log.info("    Going corrections: %s", going_str)

    # ── Continuous GA correction ──
    if "ga_value" in df.columns:
        ga_coeff = _compute_ga_correction_coeff(df)
        if abs(ga_coeff) > 0.01:
            ga_adj = ga_coeff * df["ga_value"].fillna(0)
            df.loc[has_fig, "figure_calibrated"] += ga_adj[has_fig]
            log.info("    Continuous GA coeff: %+.2f lbs per s/f", ga_coeff)
            cal_params["ga_coeff"] = ga_coeff

    # ── Exclude runners beaten > 20 lengths (unreliable) ──
    beaten_far = (
        df["distanceCumulative"].notna()
        & (df["distanceCumulative"] > 20)
        & (df["positionOfficial"] != 1)
    )
    n_excluded = beaten_far.sum()
    if n_excluded > 0:
        df.loc[beaten_far, "figure_calibrated"] = np.nan
        log.info("    Excluded %s runners beaten > 20 lengths", f"{n_excluded:,}")

    # Non-finishers
    no_pos = df["positionOfficial"].isna() | (df["positionOfficial"] == 0)
    df.loc[no_pos, "figure_calibrated"] = np.nan

    return df, cal_params


def _compute_bl_band_corrections(df):
    """Compute beaten-length band corrections using internal consistency.

    Winners are the anchor (correction=0).  For each BL band, compute
    the systematic residual (how much beaten horses' figures deviate from
    what the winner-based race quality suggests).

    Uses shrinkage (K=100) to regularise small-sample bands.
    """
    has_fig = df["figure_calibrated"].notna()
    if not has_fig.any():
        return {}

    # Get winner figure per race
    winners = df[has_fig & (df["positionOfficial"] == 1)][["race_id", "figure_calibrated"]].copy()
    winners = winners.rename(columns={"figure_calibrated": "race_quality"})
    winners = winners.drop_duplicates("race_id")

    beaten = df[has_fig & (df["positionOfficial"] > 1)].copy()
    if beaten.empty:
        return {}

    beaten = beaten.merge(winners, on="race_id", how="inner")
    if beaten.empty:
        return {}

    # Expected figure = race_quality - lbs_behind (already computed)
    # Residual = actual figure - expected
    # But since figure_calibrated = winner_figure - lbs_behind + adjustments,
    # we approximate the residual as figure_calibrated vs race_quality expectation
    beaten["residual"] = beaten["figure_calibrated"] - beaten["race_quality"]

    cum_bl = beaten["distanceCumulative"].fillna(0).clip(lower=0)
    beaten["bl_band"] = pd.cut(
        cum_bl, bins=[0, 1, 3, 5, 10, 15, 20, 999],
        labels=["0-1", "1-3", "3-5", "5-10", "10-15", "15-20", "20+"],
        include_lowest=True,
    ).astype(str).fillna("0-1")

    SHRINKAGE_K = 100
    band_groups = beaten.groupby("bl_band")["residual"]
    band_means = band_groups.mean()
    band_counts = band_groups.count()

    # Shrunk corrections: positive residual means over-rated → subtract
    corrections = {}
    for band in band_means.index:
        n = band_counts[band]
        raw = band_means[band]
        shrunk = raw * n / (n + SHRINKAGE_K)
        # We want to SUBTRACT the over-rating (negative correction)
        corrections[band] = -shrunk

    # Winner band gets no correction
    corrections["winner"] = 0.0

    return corrections


def _french_going_group_map():
    """Map French going descriptions to groups for correction."""
    return {
        "Très Sec": "Sec", "Tres Sec": "Sec", "Sec": "Sec",
        "Très leger": "BonLeger", "Tres leger": "BonLeger",
        "Bon Léger": "BonLeger", "Bon Leger": "BonLeger",
        "Bon léger": "BonLeger", "Léger": "BonLeger",
        "Bon": "Bon",
        "Bon Souple": "BonSouple", "Bon souple": "BonSouple",
        "Souple": "Souple",
        "Très Souple": "Lourd", "Tres Souple": "Lourd",
        "Très souple": "Lourd", "Collant": "Lourd", "Lourd": "Lourd",
        "PSF STANDARD": "PSF", "PSF RAPIDE": "PSF",
        "PSF LENTE": "PSF", "PSF": "PSF", "Standard": "PSF",
        "Inconnu": "Bon", "": "Bon",
    }


def _compute_going_corrections(df):
    """Compute per-going-group residual correction with shrinkage."""
    has_fig = df["figure_calibrated"].notna()
    if not has_fig.any():
        return {}

    going_map = _french_going_group_map()
    going_grp = df.loc[has_fig, "going"].map(going_map).fillna("Bon")

    # We don't have an external target, so use the class-adjusted mean:
    # the residual is figure_calibrated - class_expected_mean
    class_means = {}
    for cls, uk_dist in UK_CLASS_DISTRIBUTION.items():
        class_means[cls] = uk_dist["mean"]

    expected = df.loc[has_fig, "raceClass"].map(class_means)
    residual = df.loc[has_fig, "figure_calibrated"] - expected
    residual = residual.dropna()

    if residual.empty:
        return {}

    going_grp_aligned = going_grp.loc[residual.index]
    SHRINKAGE_K = 200  # heavier shrinkage — no external target
    grp_groups = residual.groupby(going_grp_aligned)
    grp_means = grp_groups.mean()
    grp_counts = grp_groups.count()

    corrections = {}
    for grp in grp_means.index:
        n = grp_counts[grp]
        raw = grp_means[grp]
        # Negative correction: if going group is over-rated, subtract
        corrections[grp] = -(raw * n / (n + SHRINKAGE_K))

    return corrections


def _compute_ga_correction_coeff(df):
    """Compute continuous GA correction coefficient."""
    has_fig = df["figure_calibrated"].notna() & df["ga_value"].notna()
    if has_fig.sum() < 200:
        return 0.0

    # Residual from class expected mean
    class_means = {}
    for cls, uk_dist in UK_CLASS_DISTRIBUTION.items():
        class_means[cls] = uk_dist["mean"]

    expected = df.loc[has_fig, "raceClass"].map(class_means)
    residual = df.loc[has_fig, "figure_calibrated"] - expected
    ga_vals = df.loc[has_fig, "ga_value"]

    both_valid = residual.notna() & ga_vals.notna() & (ga_vals != 0)
    if both_valid.sum() < 200:
        return 0.0

    r = residual[both_valid].values
    g = ga_vals[both_valid].values
    coeff = np.sum(g * r) / (np.sum(g ** 2) + 1e-6)

    return float(coeff)


# ═════════════════════════════════════════════════════════════════════
# STAGE 9c — EMPIRICAL GA PRIOR REFINEMENT
# ═════════════════════════════════════════════════════════════════════

def compute_empirical_ga_priors(df, ga_dict):
    """
    Compute empirical going-allowance priors from actual French data.

    After sufficient data has been accumulated, this replaces the seed
    values in FRANCE_GOING_GA_PRIOR with data-driven estimates.

    Returns a dict of going_description → mean_ga.
    """
    log.info("  Computing empirical GA priors from French data...")

    meeting_going = df.groupby("meeting_id")["going"].first()
    meeting_ga = pd.Series(ga_dict)

    # Join: for each meeting that has both a going description and a GA
    combined = pd.DataFrame({
        "going": meeting_going,
        "ga": meeting_ga,
    }).dropna()

    if combined.empty:
        log.info("    No data for empirical priors — keeping seeds")
        return dict(FRANCE_GOING_GA_PRIOR)

    empirical = combined.groupby("going")["ga"].agg(["mean", "count"])
    MIN_MEETINGS = 20  # need enough data to be reliable

    updated_priors = dict(FRANCE_GOING_GA_PRIOR)
    n_updated = 0
    for going_desc, row in empirical.iterrows():
        if row["count"] >= MIN_MEETINGS:
            updated_priors[going_desc] = float(row["mean"])
            n_updated += 1

    log.info("    Updated %d going priors empirically (from %d total descriptions)",
             n_updated, len(empirical))
    if n_updated > 0:
        for going_desc, row in empirical[empirical["count"] >= MIN_MEETINGS].iterrows():
            seed = FRANCE_GOING_GA_PRIOR.get(going_desc, "N/A")
            log.info("      %s: seed=%.3f → empirical=%.3f (n=%d)",
                     going_desc, seed if isinstance(seed, float) else 0.0,
                     row["mean"], int(row["count"]))

    return updated_priors


# ═════════════════════════════════════════════════════════════════════
# ORCHESTRATOR — FULL PIPELINE
# ═════════════════════════════════════════════════════════════════════

def run_pipeline(session: Session, start_date=None, end_date=None,
                 n_iterations: int = 2, return_artifacts: bool = False):
    """
    Execute the full French speed-figure pipeline (stages 0-9c).

    Parameters
    ----------
    session : SQLAlchemy session
    start_date, end_date : optional date bounds
    n_iterations : number of standard-time / GA iterations (default 2,
                   same as UK pipeline converges in 2-3 iterations)
    return_artifacts : if True, return a dict with intermediate lookup
        tables alongside the DataFrame (for artifact persistence).
        If False, return just the DataFrame (backward compatible).

    Returns
    -------
    pd.DataFrame (if return_artifacts=False)
    dict with keys 'df', 'std_dict', 'std_df', 'lpl_dict', 'ga_dict',
        'ga_se_dict', 'cal_params', 'empirical_ga_priors'
        (if return_artifacts=True)
    """
    log.info("=" * 70)
    log.info("FRENCH SPEED FIGURES PIPELINE")
    log.info("=" * 70)

    # Stage 0: Load data
    df = load_france_dataframe(session, start_date, end_date)
    if df.empty:
        log.warning("No data loaded — aborting pipeline.")
        return df

    # Stage 1: Initial standard times (good going only)
    std_dict, std_df = compute_standard_times(df)
    if not std_dict:
        log.warning("No standard times computed — aborting pipeline.")
        return df

    # Stage 2: Course-specific LPL
    lpl_dict = compute_course_lpl(std_df)

    # Stage 3: Going allowances
    ga_dict, ga_se_dict = compute_going_allowances(df, std_dict)

    # Iterate: recompute standard times with going-corrected data
    for i in range(1, n_iterations):
        log.info("  --- Iteration %d ---", i + 1)
        std_dict, std_df = compute_standard_times_iterative(df, ga_dict)
        lpl_dict = compute_course_lpl(std_df)
        ga_dict, ga_se_dict = compute_going_allowances(df, std_dict)

    # Stage 9c: Compute empirical GA priors (for future use / artifact persistence)
    empirical_ga_priors = compute_empirical_ga_priors(df, ga_dict)

    # Store GA value on the main DataFrame for calibration stages
    df["ga_value"] = df["meeting_id"].map(ga_dict).fillna(0)

    # Stage 4: Winner figures
    winner_df, winner_fig_dict = compute_winner_figures(
        df, std_dict, ga_dict, lpl_dict
    )

    # Stage 5: All-runner figures
    df = compute_all_figures(
        df, winner_fig_dict, lpl_dict,
        std_times=std_dict, going_allowances=ga_dict,
    )

    # Stage 6: Weight adjustment
    df = apply_weight_adjustment(df)

    # Stage 7: WFA adjustment
    df = apply_wfa_adjustment(df)

    # Stage 8: Sex allowance (no-op)
    df = apply_sex_allowance(df)

    # Stage 9: Self-calibration (distribution matching + corrections)
    df, cal_params = calibrate_to_uk_scale(df)

    # Set final figure to calibrated output
    df["figure_final"] = df["figure_calibrated"]

    # Summary stats
    has_fig = df["figure_final"].notna()
    log.info("=" * 70)
    log.info("PIPELINE COMPLETE")
    log.info("  Runners with figures: %s / %s",
             f"{has_fig.sum():,}", f"{len(df):,}")
    if has_fig.any():
        log.info("  Figure range: %.0f to %.0f",
                 df.loc[has_fig, "figure_final"].min(),
                 df.loc[has_fig, "figure_final"].max())
        log.info("  Figure mean:  %.1f", df.loc[has_fig, "figure_final"].mean())
        log.info("  Figure std:   %.1f", df.loc[has_fig, "figure_final"].std())
    log.info("=" * 70)

    if return_artifacts:
        return {
            "df": df,
            "std_dict": std_dict,
            "std_df": std_df,
            "lpl_dict": lpl_dict,
            "ga_dict": ga_dict,
            "ga_se_dict": ga_se_dict,
            "cal_params": cal_params,
            "empirical_ga_priors": empirical_ga_priors,
        }
    return df


# ═════════════════════════════════════════════════════════════════════
# ARTIFACT PERSISTENCE (for daily live ratings)
# ═════════════════════════════════════════════════════════════════════

FRANCE_OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "output", "france"
)


def save_artifacts(std_dict, std_df, lpl_dict, ga_dict, ga_se_dict,
                   cal_params=None, empirical_ga_priors=None,
                   output_dir=None):
    """
    Save pipeline lookup tables as files for daily live ratings.

    Produces:
      - standard_times.csv  (std_key, median_time, course_lpl, ...)
      - going_allowances.csv (meeting_id, going_allowance_spf)
      - france_artifacts.pkl (all dicts for fast loading)
    """
    output_dir = output_dir or FRANCE_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    # 1. Standard times CSV (with course_lpl column)
    std_out = std_df.copy()
    std_out["course_lpl"] = std_out["std_key"].map(lpl_dict)
    std_path = os.path.join(output_dir, "standard_times.csv")
    std_out.to_csv(std_path, index=False)
    log.info("  Saved standard times: %s (%d entries)", std_path, len(std_out))

    # 2. Going allowances CSV
    ga_df = pd.DataFrame(
        list(ga_dict.items()),
        columns=["meeting_id", "going_allowance_spf"],
    )
    ga_path = os.path.join(output_dir, "going_allowances.csv")
    ga_df.to_csv(ga_path, index=False)
    log.info("  Saved going allowances: %s (%d meetings)", ga_path, len(ga_df))

    # 3. Pickle with all dicts for fast loading
    artifacts = {
        "std_times": std_dict,
        "std_df": std_df,
        "lpl_dict": lpl_dict,
        "ga_dict": ga_dict,
        "ga_se_dict": ga_se_dict,
        "cal_params": cal_params or {},
        "empirical_ga_priors": empirical_ga_priors or {},
    }
    pkl_path = os.path.join(output_dir, "france_artifacts.pkl")
    with open(pkl_path, "wb") as f:
        pickle.dump(artifacts, f)
    log.info("  Saved artifacts pickle: %s", pkl_path)

    return {
        "standard_times_path": std_path,
        "going_allowances_path": ga_path,
        "pickle_path": pkl_path,
    }


def load_artifacts(output_dir=None):
    """
    Load pipeline artifacts from disk.

    Returns dict with keys: std_times, std_df, lpl_dict, ga_dict, ga_se_dict,
    cal_params, empirical_ga_priors.
    Tries pickle first (fast), falls back to CSVs.
    """
    output_dir = output_dir or FRANCE_OUTPUT_DIR

    # Fast path: pickle
    pkl_path = os.path.join(output_dir, "france_artifacts.pkl")
    if os.path.exists(pkl_path):
        log.info("  Loading artifacts from pickle: %s", pkl_path)
        with open(pkl_path, "rb") as f:
            artifacts = pickle.load(f)
        log.info("  Loaded: %d std_times, %d lpl, %d ga",
                 len(artifacts.get("std_times", {})),
                 len(artifacts.get("lpl_dict", {})),
                 len(artifacts.get("ga_dict", {})))
        # Ensure new keys exist for backward compatibility
        artifacts.setdefault("cal_params", {})
        artifacts.setdefault("empirical_ga_priors", {})
        return artifacts

    # Fallback: CSVs
    log.info("  No pickle found, loading from CSVs...")
    std_path = os.path.join(output_dir, "standard_times.csv")
    ga_path = os.path.join(output_dir, "going_allowances.csv")

    if not os.path.exists(std_path):
        raise FileNotFoundError(
            f"Standard times not found at {std_path}. "
            "Run 'build-artifacts' first."
        )

    std_df = pd.read_csv(std_path)
    std_times = dict(zip(std_df["std_key"], std_df["median_time"]))
    lpl_dict = dict(zip(std_df["std_key"], std_df["course_lpl"])) if "course_lpl" in std_df.columns else {}

    ga_dict = {}
    if os.path.exists(ga_path):
        ga_df = pd.read_csv(ga_path)
        ga_dict = dict(zip(ga_df["meeting_id"], ga_df["going_allowance_spf"]))

    log.info("  Loaded from CSV: %d std_times, %d lpl, %d ga",
             len(std_times), len(lpl_dict), len(ga_dict))

    return {
        "std_times": std_times,
        "std_df": std_df,
        "lpl_dict": lpl_dict,
        "ga_dict": ga_dict,
        "ga_se_dict": {},
        "cal_params": {},
        "empirical_ga_priors": {},
    }


# ═════════════════════════════════════════════════════════════════════
# PERSISTENCE (database)
# ═════════════════════════════════════════════════════════════════════

def persist_figures(session: Session, df: pd.DataFrame) -> int:
    """
    Write computed figures to RunnerRow.speed_figure and DailyFigureRow.

    Returns the number of runners updated.
    """
    if df.empty or "figure_final" not in df.columns:
        return 0

    has_fig = df["figure_final"].notna() & df["db_runner_id"].notna()
    rows_to_update = df[has_fig]
    count = 0

    for _, row in rows_to_update.iterrows():
        runner_id = int(row["db_runner_id"])
        figure = float(row["figure_final"])

        # Update RunnerRow.speed_figure
        runner = session.get(RunnerRow, runner_id)
        if runner is not None:
            runner.speed_figure = figure
            count += 1

        # Insert/update DailyFigureRow
        daily = DailyFigureRow(
            race_date=pd.to_datetime(row["meetingDate"]).date(),
            runner_id=runner_id,
            speed_figure=figure,
            weight_adjusted_figure=float(row.get("figure_after_weight", figure)),
            distance_m=int(row.get("distance_m_raw", 0)) if pd.notna(row.get("distance_m_raw")) else None,
            hippodrome_code=row.get("courseName"),
        )
        session.merge(daily)

    session.commit()
    log.info("  Persisted figures for %s runners.", f"{count:,}")
    return count
