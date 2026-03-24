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
    BENCHMARK_METRES,
    BL_ATTENUATION_FACTOR,
    BL_ATTENUATION_THRESHOLD,

    FRANCE_GOING_GA_PRIOR,
    FRANCE_GOOD_GOING,
    GA_NONLINEAR_BETA,
    GA_NONLINEAR_THRESHOLD,
    GA_OUTLIER_ZSCORE,
    GA_SHRINKAGE_K,
    LBS_PER_SECOND_BENCHMARK,
    LPL_SURFACE_MULTIPLIER,
    MIN_RACES_GOING_ALLOWANCE,
    RECENCY_HALF_LIFE_YEARS,
    SECONDS_PER_LENGTH,
)
from .database import DailyFigureRow, RunnerRow
from .field_mapping import load_france_dataframe

log = logging.getLogger(__name__)



# ── Default calibration parameters ──
# Global scale+shift mapping French raw figures onto the UK Timeform
# scale.  Class-independent: a single transform for all runners.
# Used when batch cal_params are unavailable (e.g., no france.db).
# Scale = target_std / fr_std; shift = target_mean - fr_mean * scale.
DEFAULT_CAL_PARAMS = {
    "global": {
        "scale": 0.700, "shift": -15.0,
        "fr_mean": 100.0, "fr_std": 60.0,
        "fr_robust_std": 25.7, "fr_median": 100.0,
        "target_mean": 55.0, "target_std": 18.0,
        "n_runners": 0,
    },
}


# ═════════════════════════════════════════════════════════════════════
# STAGE 1 — STANDARD TIMES  (per track / distance / surface)
# ═════════════════════════════════════════════════════════════════════


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


def _trimmed_median(series, trim_frac=0.05):
    """Trimmed median: drop the fastest and slowest ``trim_frac`` of values,
    then return the median of what remains.

    Methodology §Step 4: "Trim the fastest 5% and slowest 5%".

    If the group is too small for the trim to remove at least one value
    from each tail, falls back to the plain median.
    """
    vals = series.dropna().values
    n = len(vals)
    k = int(n * trim_frac)
    if k < 1 or n - 2 * k < 3:
        return np.median(vals)
    sorted_vals = np.sort(vals)
    return np.median(sorted_vals[k: n - k])


def _flag_divergence(std_df, threshold=0.03, exclude_threshold=0.10):
    """Flag and optionally exclude standard times with high median-mean divergence.

    Methodology QA check: alert if median-mean divergence exceeds 3%.
    Audit §2 identified 25 rows with >5% divergence between median and mean,
    indicating severe outlier contamination in the underlying race-time
    distribution.  Rows above ``exclude_threshold`` (default 10%) are dropped;
    rows between ``threshold`` (default 3%) and ``exclude_threshold`` are
    flagged as provisional.

    Parameters
    ----------
    std_df : pd.DataFrame
        Standard times DataFrame with median_time and mean_time columns.
    threshold : float
        Divergence ratio above which rows are flagged as provisional.
    exclude_threshold : float
        Divergence ratio above which rows are excluded entirely.

    Returns
    -------
    pd.DataFrame with ``divergence`` and ``provisional`` columns added,
    and extreme-divergence rows removed.
    """
    std_df = std_df.copy()
    std_df["divergence"] = (
        (std_df["mean_time"] - std_df["median_time"]) / std_df["median_time"]
    ).abs()

    extreme = std_df["divergence"] >= exclude_threshold
    n_extreme = extreme.sum()
    if n_extreme > 0:
        log.warning("    Excluding %d standard-time combos with >%.0f%% median-mean divergence: %s",
                     n_extreme, exclude_threshold * 100,
                     list(std_df.loc[extreme, "std_key"]))
        std_df = std_df[~extreme].copy()

    provisional = std_df["divergence"] >= threshold
    std_df["provisional"] = provisional
    n_prov = provisional.sum()
    if n_prov > 0:
        log.info("    Flagged %d standard-time combos as provisional (%.0f%%–%.0f%% divergence)",
                 n_prov, threshold * 100, exclude_threshold * 100)

    return std_df


def _flag_going_dominance(winners_df, std_df, dominance_threshold=0.70):
    """Flag standard-time combos where a single going description dominates.

    Methodology QA: "No single going type should dominate >70% of a
    standard's sample."  If one going description accounts for more than
    ``dominance_threshold`` of the races, the standard may not generalise
    well across conditions despite the prior-based correction.

    Adds a ``going_dominance`` column (True if flagged).
    """
    std_df = std_df.copy()
    std_df["going_dominance"] = False

    if "going" not in winners_df.columns or "std_key" not in winners_df.columns:
        return std_df

    flagged = []
    for key in std_df["std_key"]:
        grp = winners_df.loc[winners_df["std_key"] == key, "going"]
        if grp.empty:
            continue
        counts = grp.value_counts(normalize=True)
        if counts.iloc[0] > dominance_threshold:
            flagged.append(key)

    if flagged:
        std_df.loc[std_df["std_key"].isin(flagged), "going_dominance"] = True
        log.info("    Going dominance (>%.0f%% single going): %d combos flagged",
                 dominance_threshold * 100, len(flagged))

    return std_df


def _validate_monotonicity(std_df):
    """Log warnings for course/surface combos where a longer distance has a
    faster standard time than a shorter one.

    Methodology QA: "Time must increase with distance within a course."
    """
    violations = []
    for (course, surface), grp in std_df.groupby(["courseName", "surface"]):
        if len(grp) < 2:
            continue
        ordered = grp.sort_values("distance")
        dists = ordered["distance"].values
        times = ordered["median_time"].values
        for i in range(1, len(dists)):
            if times[i] < times[i - 1]:
                violations.append(
                    f"{course} {surface}: {dists[i-1]:.0f}m={times[i-1]:.1f}s > "
                    f"{dists[i]:.0f}m={times[i]:.1f}s"
                )
    if violations:
        log.warning("    Monotonicity violations (%d):", len(violations))
        for v in violations[:10]:
            log.warning("      %s", v)
        if len(violations) > 10:
            log.warning("      ... and %d more", len(violations) - 10)
    else:
        log.info("    Monotonicity check: OK (all courses pass)")


def _flag_cross_course_outliers(std_df, z_threshold=2.5):
    """Flag standards whose seconds-per-metre is >``z_threshold`` standard
    deviations from the distance-band peer mean.

    Methodology QA: "Flag any standard >2.5σ from the distance-band peer
    mean."  Adds a ``cross_course_outlier`` column.
    """
    std_df = std_df.copy()
    std_df["cross_course_outlier"] = False

    std_df["spm"] = std_df["median_time"] / std_df["distance"]
    std_df["dist_band"] = std_df["distance"].round(-2)  # 100m bands

    band_stats = std_df.groupby(["dist_band", "surface"])["spm"].agg(["mean", "std"])
    band_stats.columns = ["band_mean", "band_std"]

    std_df = std_df.merge(
        band_stats, left_on=["dist_band", "surface"], right_index=True, how="left"
    )

    outlier_mask = (
        (std_df["band_std"] > 0)
        & (((std_df["spm"] - std_df["band_mean"]) / std_df["band_std"]).abs() > z_threshold)
    )
    n_outliers = outlier_mask.sum()
    if n_outliers > 0:
        std_df.loc[outlier_mask, "cross_course_outlier"] = True
        flagged = std_df.loc[outlier_mask, ["std_key", "spm", "band_mean", "band_std"]]
        log.warning("    Cross-course outliers (>%.1fσ): %d combos", z_threshold, n_outliers)
        for _, row in flagged.head(10).iterrows():
            z = (row["spm"] - row["band_mean"]) / row["band_std"]
            log.warning("      %s: spm=%.4f, band_mean=%.4f (z=%.1f)",
                        row["std_key"], row["spm"], row["band_mean"], z)
    else:
        log.info("    Cross-course outlier check: OK (none flagged)")

    std_df.drop(columns=["spm", "dist_band", "band_mean", "band_std"], inplace=True)
    return std_df


def compute_standard_times(df):
    """
    Standard times per track / distance / surface.

    Uses ALL goings with going-description prior adjustments to calibrate
    standards to median ground conditions (GA audit §2: reduces GA corrective
    burden from +0.25 spf to ~+0.08 spf).

    For each winner, applies a prior-based going correction using the
    FRANCE_GOING_GA_PRIOR table before computing the trimmed median (5%
    trim each tail, per STANDARD_TIMES_METHODOLOGY.md §Step 4).  This
    ensures the initial standard times approximate "average conditions"
    rather than "good going" conditions.

    QA checks applied (per methodology):
      - Median-mean divergence alerting at >3%
      - Going distribution: flag if single going >70% of sample
      - Monotonicity: time must increase with distance per course
      - Cross-course z-score: flag >2.5σ from distance-band peer mean
    """
    log.info("  Computing standard times (per track / distance / surface)...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = _filter_std_time_winners(winners)

    log.info("    Winners total: %s", f"{len(winners):,}")

    # Apply prior-based going correction to ALL winners so that standards
    # reflect median ground conditions, not just good-going conditions.
    # This reduces GA corrective burden (GA audit §2).
    winners["prior_ga"] = winners["going"].map(FRANCE_GOING_GA_PRIOR).fillna(
        FRANCE_GOING_GA_PRIOR.get("Bon", 0.04 / 201.168)
    )
    winners["adj_time"] = (
        winners["finishingTime"] - (winners["prior_ga"] * winners["distance"])
    )

    std_rows = []
    for key, grp in winners.groupby("std_key"):
        adj = grp["adj_time"]
        std_rows.append({
            "std_key": key,
            "median_time": _trimmed_median(adj),
            "mean_time": adj.mean(),
            "n_races": len(adj),
            "distance": grp["distance"].iloc[0],
            "courseName": grp["courseName"].iloc[0],
            "surface": grp["raceSurfaceName"].iloc[0],
        })
    std_times = pd.DataFrame(std_rows)

    # Keep only combos with enough data
    valid = std_times[std_times["n_races"] >= C.MIN_RACES_STANDARD_TIME].copy()
    log.info("    Standard-time combos (>= %d races): %s",
             C.MIN_RACES_STANDARD_TIME, f"{len(valid):,}")
    log.info("    Dropped (insufficient data): %s",
             f"{len(std_times) - len(valid):,}")

    # Flag high median-mean divergence (methodology: alert at >3%)
    valid = _flag_divergence(valid)

    # QA: going distribution check — flag combos where a single going
    # description dominates >70% of the sample (methodology §QA).
    valid = _flag_going_dominance(winners, valid)

    # QA: monotonicity — time must increase with distance per course
    _validate_monotonicity(valid)

    # QA: cross-course outliers — flag standards >2.5σ from peer mean
    valid = _flag_cross_course_outliers(valid)

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

    def _weighted_trimmed_median(group, trim_frac=0.05):
        """Weighted median with 5% trimming on each tail.

        Methodology §Step 4: trim the fastest/slowest 5% before computing
        the weighted median.  This resists outliers even better than
        plain median, especially when recency weights amplify recent
        anomalous races.
        """
        times = group["adj_time"].values
        weights = group["recency_weight"].values
        sorted_idx = np.argsort(times)
        times = times[sorted_idx]
        weights = weights[sorted_idx]
        # Trim tails
        n = len(times)
        k = int(n * trim_frac)
        if k >= 1 and n - 2 * k >= 3:
            times = times[k: n - k]
            weights = weights[k: n - k]
        # Weighted median on trimmed data
        cum_w = np.cumsum(weights)
        mid = cum_w[-1] / 2.0
        idx = np.searchsorted(cum_w, mid)
        return times[min(idx, len(times) - 1)]

    std_rows = []
    for key, grp in winners.groupby("std_key"):
        std_rows.append({
            "std_key": key,
            "median_time": _weighted_trimmed_median(grp),
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
        # Group by integer metres (PMU distances are naturally integer)
        dist_median = valid.groupby(valid["distance"].astype(int))["median_time"].median()
        for idx, row in below_threshold.iterrows():
            d_int = int(row["distance"])
            if d_int in dist_median.index:
                generic_std = dist_median[d_int]
                n = row["n_races"]
                blended = (n * row["median_time"] + SHRINKAGE_K * generic_std) / (n + SHRINKAGE_K)
                below_threshold.loc[idx, "median_time"] = blended
        valid = pd.concat([valid, below_threshold], ignore_index=True)
        log.info("    Shrinkage combos added: %s", f"{len(below_threshold):,}")

    # Flag high median-mean divergence (methodology: alert at >3%)
    valid = _flag_divergence(valid)

    # QA: monotonicity — time must increase with distance per course
    _validate_monotonicity(valid)

    # QA: cross-course outliers — flag standards >2.5σ from peer mean
    valid = _flag_cross_course_outliers(valid)

    log.info("    Standard-time combos (>= %d races): %s (using all goings, recency-weighted)",
             C.MIN_RACES_STANDARD_TIME, f"{len(valid):,}")

    std_dict = dict(zip(valid["std_key"], valid["median_time"]))
    return std_dict, valid


# ═════════════════════════════════════════════════════════════════════
# STAGE 2 — COURSE-SPECIFIC LBS PER LENGTH
# ═════════════════════════════════════════════════════════════════════

def generic_lbs_per_length(distance_metres, surface=None):
    """
    Generic lbs-per-length from distance in metres (and optionally surface).
    Same formula as UK pipeline, adapted for metre distances.
    """
    lbs_per_sec = LBS_PER_SECOND_BENCHMARK * (BENCHMARK_METRES / distance_metres)
    base_lpl = SECONDS_PER_LENGTH * lbs_per_sec
    if surface is not None:
        base_lpl *= LPL_SURFACE_MULTIPLIER.get(surface, 1.0)
    return base_lpl


def compute_course_lpl(std_df):
    """
    Course-specific lbs-per-length for each standard-time combo.
    Same algorithm as UK ``compute_course_lpl()``, using metre distances.
    """
    log.info("  Computing course-specific lbs-per-length...")

    std_df = std_df.copy()
    std_df["spm"] = std_df["median_time"] / std_df["distance"]

    # Mean spm at each distance across all courses
    # (PMU distances are integer metres so grouping is natural)
    std_df["dist_band"] = std_df["distance"].astype(int)
    mean_spm = std_df.groupby("dist_band")["spm"].mean()
    std_df["mean_spm"] = std_df["dist_band"].map(mean_spm)

    # Course correction factor
    std_df["correction"] = std_df["mean_spm"] / std_df["spm"]

    # Generic lpl at this distance (metres)
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

def _parse_std_keys(lookup_dict):
    """Parse a std_key → value dict into {(course, surface): sorted [(dist, val), ...]}.

    Reusable helper for standard-time and LPL interpolation.
    """
    cs_map = defaultdict(list)
    for key, val in lookup_dict.items():
        parts = key.rsplit("_", 2)
        if len(parts) != 3:
            continue
        course, dist_str, surface = parts
        try:
            dist = float(dist_str)
        except ValueError:
            continue
        cs_map[(course, surface)].append((dist, val))
    # Sort by distance within each course/surface
    for k in cs_map:
        cs_map[k].sort()
    return dict(cs_map)


def _interp_single(actual_dist, dist_val_pairs):
    """Linearly interpolate a value for actual_dist given sorted (dist, val) pairs.

    Returns NaN if the actual distance is more than 20% beyond the known range
    (prevents e.g. a 1600m standard time being used for a 2300m race).
    Clamps to nearest known value for small extrapolations within the 20% guard.
    """
    if len(dist_val_pairs) == 1:
        only_dist = dist_val_pairs[0][0]
        if abs(actual_dist - only_dist) / only_dist > 0.20:
            return np.nan
        return dist_val_pairs[0][1]
    dists = [dv[0] for dv in dist_val_pairs]
    vals = [dv[1] for dv in dist_val_pairs]
    if actual_dist <= dists[0]:
        if (dists[0] - actual_dist) / dists[0] > 0.20:
            return np.nan
        return vals[0]
    if actual_dist >= dists[-1]:
        if (actual_dist - dists[-1]) / dists[-1] > 0.20:
            return np.nan
        return vals[-1]
    # Find bracketing pair
    for i in range(len(dists) - 1):
        if dists[i] <= actual_dist <= dists[i + 1]:
            frac = (actual_dist - dists[i]) / (dists[i + 1] - dists[i])
            return vals[i] + frac * (vals[i + 1] - vals[i])
    return vals[-1]


def interpolate_lookup(df, lookup_dict, course_col="courseName",
                       surface_col="raceSurfaceName", dist_col="distance"):
    """Interpolate values from a std_key-keyed dict using actual distances.

    Instead of rounding distances to 0.5f buckets and doing an exact lookup,
    this linearly interpolates between the two nearest known distance buckets
    at each course/surface.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain course_col, surface_col, and dist_col columns.
    lookup_dict : dict
        std_key → value (e.g., std_times or lpl_dict).

    Returns
    -------
    pd.Series aligned with df.index, with NaN where interpolation is impossible.
    """
    cs_map = _parse_std_keys(lookup_dict)
    result = pd.Series(np.nan, index=df.index)

    for (course, surface), dist_val_pairs in cs_map.items():
        mask = (df[course_col] == course) & (df[surface_col] == surface)
        if not mask.any():
            continue
        actual_dists = df.loc[mask, dist_col]
        result.loc[mask] = actual_dists.apply(
            lambda d: _interp_single(d, dist_val_pairs)
        )

    return result


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


def _apply_split_meetings(df, split_map):
    """Propagate split-card meeting IDs to the main DataFrame.

    ``split_map`` is ``{original_meeting_id: first_race_number_of_late_half}``.
    Rows with ``raceNumber < split_race`` get ``_early``; the rest get ``_late``.
    """
    for mid, split_race in split_map.items():
        mask = df["meeting_id"] == mid
        early = mask & (df["raceNumber"] < split_race)
        late = mask & (df["raceNumber"] >= split_race)
        df.loc[early, "meeting_id"] = mid + "_early"
        df.loc[late, "meeting_id"] = mid + "_late"


def _log_systematic_ga_bias(ga_dict, min_meetings=20,
                            threshold_spm=0.30 / 201.168):
    """Log courses where the GA is systematically extreme, suggesting
    the standard time is miscalibrated (GA audit §7).

    Parameters
    ----------
    ga_dict : dict
        meeting_id → GA in s/m.
    min_meetings : int
        Minimum meetings at a course to report.
    threshold_spm : float
        Mean |GA| above which a course is flagged (default 0.30 s/f in s/m).
    """
    course_gas = defaultdict(list)
    for mid, ga in ga_dict.items():
        # Parse course from meeting_id: "date_COURSE_surface[_early|_late]"
        parts = mid.replace("_early", "").replace("_late", "").split("_", 2)
        if len(parts) >= 2:
            course = parts[1]
            course_gas[course].append(ga)

    flagged = []
    for course, gas in course_gas.items():
        if len(gas) < min_meetings:
            continue
        mean_ga = np.mean(gas)
        if abs(mean_ga) > threshold_spm:
            direction = "fast" if mean_ga < 0 else "slow"
            spf = mean_ga * 201.168
            flagged.append((course, len(gas), spf, direction))

    if flagged:
        flagged.sort(key=lambda x: abs(x[2]), reverse=True)
        log.warning("    GA bias diagnostic — courses with systematically extreme GAs "
                    "(standard may need recalibration):")
        for course, n, spf, direction in flagged[:10]:
            log.warning("      %s: %d meetings, mean GA %+.3f s/f (%s standard ~%.0fs %s at 8f)",
                        course, n, spf, "too" if abs(spf) > 0.3 else "",
                        abs(spf) * 8, direction)


def compute_going_allowances(df, std_times):
    """
    Going allowance per meeting in seconds-per-metre (s/m).

    Now includes (ported from UK pipeline):
      - Interpolated standard times for GA inference (more winners contribute)
      - Weighted winsorized mean (interpolated deviations get lower weight)
      - Temporal neighbor pooling (meetings with too few races borrow ±1 day)
    """
    log.info("  Computing going allowances (per track / day)...")

    # Interpolate standard times to actual distances
    winners = df[df["positionOfficial"] == 1].copy()
    winners["standard_time"] = interpolate_lookup(winners, std_times)
    winners = winners[winners["standard_time"].notna()].copy()
    # All winners now use distance-interpolated standard times
    winners["ga_weight"] = 1.0

    log.info("    Winners for GA: %s (distance-interpolated)",
             f"{len(winners):,}")

    # No class adjustment for France (consistent with standard times)
    # Per-metre deviation from standard
    winners["deviation"] = winners["finishingTime"] - winners["standard_time"]
    winners["dev_per_metre"] = winners["deviation"] / winners["distance"]

    # ── Per-meeting outlier removal ──
    meeting_medians = winners.groupby("meeting_id")["dev_per_metre"].median()
    meeting_stds = winners.groupby("meeting_id")["dev_per_metre"].std()
    winners["_meeting_med"] = winners["meeting_id"].map(meeting_medians)
    winners["_meeting_std"] = winners["meeting_id"].map(meeting_stds)
    has_std = winners["_meeting_std"].notna() & (winners["_meeting_std"] > 0)
    z_scores = (
        (winners["dev_per_metre"] - winners["_meeting_med"])
        / winners["_meeting_std"]
    ).abs()
    winners = winners[~has_std | (z_scores <= GA_OUTLIER_ZSCORE)].copy()
    winners.drop(columns=["_meeting_med", "_meeting_std"], inplace=True)

    # ── Split-card going detection ──
    # Record {original_meeting_id: split_race_number} so the caller can
    # propagate splits to the main DataFrame.
    split_meetings = {}  # mid → raceNumber of first race in second half
    for mid, group in winners.groupby("meeting_id"):
        # Skip already-split meetings to avoid double-suffixing
        # (GA audit §5: malformed IDs like "SIO_Turf_early_early")
        if mid.endswith("_early") or mid.endswith("_late"):
            continue
        if len(group) < 6:
            continue
        ordered = group.sort_values("raceNumber")
        n = len(ordered)
        half = n // 2
        first_half = ordered.iloc[:half]["dev_per_metre"]
        second_half = ordered.iloc[half:]["dev_per_metre"]

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

        # Threshold converted to s/m (0.10 s/f ÷ 201.168)
        if t_stat > 2.5 and abs(m1 - m2) > 0.10 / 201.168:
            winners.loc[ordered.index[:half], "meeting_id"] = mid + "_early"
            winners.loc[ordered.index[half:], "meeting_id"] = mid + "_late"
            split_race = int(ordered.iloc[half]["raceNumber"])
            split_meetings[mid] = split_race

    if split_meetings:
        log.info("    Split-card going detected: %d meetings split",
                 len(split_meetings))

    # ── Weighted winsorized mean within each meeting ──
    def _weighted_winsorized_mean(group):
        sorted_g = group.sort_values("dev_per_metre")
        devs = sorted_g["dev_per_metre"].values.copy()
        weights = sorted_g["ga_weight"].values.copy()
        n = len(devs)
        if n <= 2:
            return np.average(devs, weights=weights)
        # Winsorize: clamp extremes to adjacent values
        devs[0] = devs[1]
        devs[-1] = devs[-2]
        return np.average(devs, weights=weights)

    ga_series = (
        winners.groupby("meeting_id")[["dev_per_metre", "ga_weight"]]
        .apply(_weighted_winsorized_mean)
    )
    ga_count = (
        winners.groupby("meeting_id")["dev_per_metre"].count()
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
        winners.groupby("meeting_id")["dev_per_metre"]
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
        prior_ga = FRANCE_GOING_GA_PRIOR.get(going_desc, 0.05)

        k = GA_SHRINKAGE_K
        shrunk_ga = (n * raw_ga + k * prior_ga) / (n + k)
        shrunk_dict[mid] = shrunk_ga

    ga_dict = shrunk_dict

    # ── Non-linear correction for extreme going ──
    # On extreme going (|GA| > threshold), the linear s/f model
    # underestimates the true effect.  Apply a quadratic boost.
    # Matches UK pipeline: ga + sign * correction.
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

    # ── Soft-cap GA using Winsorisation (GA audit §3) ──
    # Hard clipping at −1.5/+2.5 spf masked miscalibrated standards and
    # discarded information at extremes.  Instead, use percentile-based
    # soft caps: values beyond the 1st/99th percentile are Winsorised
    # (clamped to the percentile boundary), preserving more signal while
    # still preventing clearly erroneous values from propagating.
    if ga_dict:
        ga_vals = np.array(list(ga_dict.values()))
        p1, p99 = np.percentile(ga_vals, [1, 99])
        # Ensure a minimum sane range (in s/m) — don't let thin data
        # produce absurdly tight caps
        GA_FLOOR = -1.5 / 201.168
        GA_CEIL = 2.5 / 201.168
        ga_min = min(p1, GA_FLOOR)
        ga_max = max(p99, GA_CEIL)
        n_clipped = sum(1 for v in ga_dict.values() if v < ga_min or v > ga_max)
        ga_dict = {mid: max(ga_min, min(ga_max, v)) for mid, v in ga_dict.items()}
        if n_clipped > 0:
            log.info("    GA soft-capped %d meetings (range: %.6f to %.6f s/m)",
                     n_clipped, ga_min, ga_max)

    log.info("    Meetings with going allowance: %s", f"{len(ga_dict):,}")
    if ga_dict:
        ga_vals_list = list(ga_dict.values())
        log.info("    GA range: %.6f to %.6f s/m",
                 min(ga_vals_list), max(ga_vals_list))
        log.info("    GA mean:  %.6f s/m", np.mean(ga_vals_list))

        # ── Diagnostic: flag courses with systematically extreme GAs ──
        # (GA audit §7) — identifies standards that need recalibration.
        _log_systematic_ga_bias(ga_dict)

    return ga_dict, ga_se_dict, split_meetings


# ═════════════════════════════════════════════════════════════════════
# STAGE 4 — WINNER SPEED FIGURES
# ═════════════════════════════════════════════════════════════════════

def _quality_check_winners(df, std_times):
    """
    Identify races that fail quality checks and should not produce figures.

    Returns a set of race_ids that failed, plus a dict of
    {race_id: comment} explaining the failure.

    Checks:
      QC-1: No standard time for the winner's course/distance.
      QC-2: Impossible winner finishing time (pace outside 10–18 s/f).
      QC-3: Broken beaten lengths (all non-winners share identical BL).
    """
    failed = {}   # race_id → comment
    winners = df[df["positionOfficial"] == 1]

    # Plausible pace in seconds per metre (converted from 10–18 s/f)
    MIN_PACE_SPM = 10.0 / 201.168
    MAX_PACE_SPM = 18.0 / 201.168

    # Pre-compute which course/surface combos have standard times
    cs_with_std = set()
    for key in std_times:
        parts = key.rsplit("_", 2)
        if len(parts) == 3:
            cs_with_std.add((parts[0], parts[2]))

    for _, row in winners.iterrows():
        rid = row["race_id"]
        # QC-1: No standard time for course/surface (interpolation requires
        # at least one known distance bucket at this course/surface)
        if (row.get("courseName"), row.get("raceSurfaceName")) not in cs_with_std:
            failed[rid] = "no historical data to generate figures"
            continue
        # QC-2: Impossible winner finishing time
        ft = row.get("finishingTime")
        dist = row.get("distance")
        if pd.notna(ft) and pd.notna(dist) and dist > 0:
            pace_spm = ft / dist
            if pace_spm < MIN_PACE_SPM or pace_spm > MAX_PACE_SPM:
                failed[rid] = "no historical data to generate figures"
                log.warning("    QC: Race %s failed — impossible pace %.4f s/m "
                            "(time=%.2f, dist=%.0fm)", rid, pace_spm, ft, dist)

    # QC-3: Broken beaten lengths
    for rid, grp in df.groupby("race_id"):
        if rid in failed:
            continue
        non_winners = grp[grp["positionOfficial"] != 1]
        if len(non_winners) >= 3:
            bl_vals = non_winners["distanceCumulative"].dropna()
            if len(bl_vals) >= 3 and bl_vals.nunique() == 1:
                failed[rid] = "no historical data to generate figures"
                log.warning("    QC: Race %s failed — all beaten lengths "
                            "identical (%.2f)", rid, bl_vals.iloc[0])

    if failed:
        log.info("    QC: %d races failed quality checks", len(failed))

    return failed


def compute_winner_figures(df, std_times, going_allowances, lpl_dict):
    """
    Speed figures for race winners.
    Same formula as UK ``compute_winner_figures()``.
    """
    log.info("  Computing winner speed figures...")

    # ── Quality checks — exclude races that should not produce figures ──
    qc_failed = _quality_check_winners(df, std_times)

    w = df[df["positionOfficial"] == 1].copy()

    # Interpolate standard times to actual distances instead of using rounded buckets
    w["standard_time"] = interpolate_lookup(w, std_times)
    w = w[
        w["standard_time"].notna()
        & w["meeting_id"].isin(going_allowances)
        & ~w["race_id"].isin(qc_failed)
    ].copy()

    w["going_allowance"] = w["meeting_id"].map(going_allowances)

    # Going-corrected time (NO class adjustment — figure reflects raw speed)
    w["corrected_time"] = (
        w["finishingTime"]
        - (w["going_allowance"] * w["distance"])
    )

    # Deviation from standard
    w["deviation_seconds"] = w["corrected_time"] - w["standard_time"]
    w["deviation_lengths"] = w["deviation_seconds"] / SECONDS_PER_LENGTH

    # Course-specific lbs-per-length interpolated to actual distance
    w["lpl"] = interpolate_lookup(w, lpl_dict)
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

    # Cap extreme outlier figures to prevent nonsensical values
    # (e.g. bad standard times or timing errors producing -10000 or +1400).
    # Reasonable flat racing figures range from roughly -50 to 250.
    n_outliers = ((w["raw_figure"] < -50) | (w["raw_figure"] > 250)).sum()
    if n_outliers > 0:
        log.info("    Capping %s extreme winner figures to [-50, 250]", f"{n_outliers:,}")
    w["raw_figure"] = w["raw_figure"].clip(lower=-50, upper=250)

    log.info("    Winner figures computed: %s", f"{len(w):,}")
    if len(w) > 0:
        log.info("    Range: %.0f to %.0f", w["raw_figure"].min(), w["raw_figure"].max())
        log.info("    Mean:  %.1f", w["raw_figure"].mean())

    winner_fig = dict(zip(w["race_id"], w["raw_figure"]))
    return w, winner_fig, qc_failed


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

    # Course-specific lpl interpolated to actual distance
    out["lpl"] = interpolate_lookup(out, lpl_dict)
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

        out["standard_time"] = interpolate_lookup(out, std_times)
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

    # Cap extreme outlier figures — same bounds as winner figures (Stage 4).
    # Without this, garbage values from clamped/missing standard times leak
    # into calibration and distort the global scale+shift.
    extreme = out["raw_figure"].notna() & (
        (out["raw_figure"] < -50) | (out["raw_figure"] > 250)
    )
    if extreme.any():
        log.info("    Capping %s extreme all-runner figures to [-50, 250]",
                 f"{extreme.sum():,}")
        out.loc[extreme, "raw_figure"] = out.loc[extreme, "raw_figure"].clip(
            lower=-50, upper=250
        )

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
    Weight-for-age adjustment using empirical WFA table derived from
    GBR Timeform timefigure gaps (2021-2025, ~270k runners).

    WFA is only applied to runners in mixed-age races.  In age-restricted
    races (e.g. all 3yos) there is no older-horse benchmark, so the WFA
    would artificially inflate figures.
    """
    from .constants import get_france_wfa_allowance

    log.info("  Applying WFA adjustment (empirical)...")

    # Detect single-age races: all runners share the same age
    race_age_nunique = df.groupby("race_id")["horseAge"].transform("nunique")
    is_mixed_age = race_age_nunique > 1

    df["wfa_adj"] = df.apply(
        lambda r: get_france_wfa_allowance(
            r["horseAge"], r["month"], r["distance"],
        ),
        axis=1,
    )
    # Zero out WFA for single-age races (no older-horse benchmark)
    single_age_mask = ~is_mixed_age
    n_zeroed = (single_age_mask & (df["wfa_adj"] > 0)).sum()
    df.loc[single_age_mask, "wfa_adj"] = 0.0

    has = df["wfa_adj"] > 0
    log.info("    Runners with WFA: %s / %s (zeroed %s in single-age races)",
             f"{has.sum():,}", f"{len(df):,}", f"{n_zeroed:,}")
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
# STAGE 9 — SELF-CALIBRATION (global scale to Timeform-equivalent)
# ═════════════════════════════════════════════════════════════════════

# Global calibration target — the overall UK Timeform distribution for
# all flat runners (all classes combined).  Speed figures should be
# class-independent: a single global scale+shift maps French raw figures
# onto the Timeform scale without artificially anchoring each class to
# its expected mean.
GLOBAL_TARGET_MEAN = 55.0   # French figure target mean (lowered from 72 — see QA audit 2026-03-23)
GLOBAL_TARGET_STD  = 18.0   # overall UK flat Timeform std  (all classes)


def calibrate_to_uk_scale(df):
    """
    Global calibration: align French figure distribution to the UK
    Timeform scale using a single scale+shift applied to ALL runners
    regardless of class.

    This preserves class-independence — a Class 5 horse that genuinely
    runs fast will get a high figure, not one anchored to a Class 5
    target mean.

    Additionally applies:
      - Beaten-length band correction
      - Per-going-group residual correction
      - Continuous GA correction
    """
    log.info("  Calibrating to Timeform scale (global scale+shift)...")

    has_fig = df["figure_after_sex"].notna()
    if not has_fig.any():
        df["figure_calibrated"] = df["figure_after_sex"]
        return df, {}

    df["figure_calibrated"] = df["figure_after_sex"].copy()
    cal_params = {}

    # ── Global calibration: single scale+shift for all runners ──
    # Use robust statistics (IQR-based) to prevent outliers from
    # inflating std and crushing the scale factor.  Raw std can be
    # 80+ due to extreme outliers, yielding scale ~0.22 which makes
    # beaten-length and weight adjustments nearly invisible.
    fr_vals = df.loc[has_fig, "figure_after_sex"]

    # Pre-filter: exclude extreme outliers before computing IQR.
    # Figures outside [-50, 250] are almost certainly from bad standard
    # times or timing errors and should not influence calibration.
    sane_mask = fr_vals.between(-50, 250)
    fr_vals_sane = fr_vals[sane_mask]
    if len(fr_vals_sane) < 20:
        fr_vals_sane = fr_vals  # fall back if too few sane values

    fr_mean = fr_vals_sane.mean()
    fr_std = fr_vals_sane.std()

    q25 = fr_vals_sane.quantile(0.25)
    q75 = fr_vals_sane.quantile(0.75)
    iqr = q75 - q25
    robust_std = iqr / 1.349  # IQR-based std estimator for normal distribution
    robust_center = fr_vals_sane.median()

    # Use robust std for scaling (prevents outlier-driven compression)
    cal_std = robust_std if robust_std > 0 else fr_std
    cal_center = robust_center

    if cal_std > 0 and not pd.isna(cal_std):
        scale = GLOBAL_TARGET_STD / cal_std
        # Clamp to prevent collapse or explosion
        scale = float(np.clip(scale, 0.3, 2.0))
        shift = GLOBAL_TARGET_MEAN - cal_center * scale

        df.loc[has_fig, "figure_calibrated"] = (
            df.loc[has_fig, "figure_after_sex"] * scale + shift
        )

        cal_params["global"] = {
            "fr_mean": float(fr_mean), "fr_std": float(fr_std),
            "fr_robust_std": float(robust_std), "fr_median": float(robust_center),
            "target_mean": GLOBAL_TARGET_MEAN, "target_std": GLOBAL_TARGET_STD,
            "scale": float(scale), "shift": float(shift),
            "n_runners": int(has_fig.sum()),
        }
        log.info("    Global: FR(median=%.1f, IQR_std=%.1f, raw_std=%.1f) → UK(%.1f±%.1f)  scale=%.4f shift=%+.1f  n=%s",
                 robust_center, robust_std, fr_std, GLOBAL_TARGET_MEAN, GLOBAL_TARGET_STD,
                 scale, shift, f"{has_fig.sum():,}")

    # ── NOTE: BL band corrections REMOVED ──
    # The _compute_bl_band_corrections() function measures the gap between
    # beaten horses and the winner (residual = beaten_cal - winner_cal).
    # This residual is almost entirely the BL penalty itself, so the
    # "correction" just undoes beaten lengths — producing absurd results
    # where horses finishing 10th rate higher than the winner.
    # With the robust IQR-based scale, the BL extension works correctly
    # and no band correction is needed.

    # ── NOTE: Per-going-group and continuous GA corrections REMOVED ──
    # These post-calibration adjustments inflated figures by +3-5 lbs
    # (PSF going correction +2.67, GA coeff up to +0.8) and obscured
    # the base calibration.  The per-meeting going allowance already
    # accounts for going variation at the race level.
    # See QA audit 2026-03-23.

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

    # Residual = figure_calibrated - global mean (class-independent)
    global_mean = df.loc[has_fig, "figure_calibrated"].mean()
    residual = df.loc[has_fig, "figure_calibrated"] - global_mean
    residual = residual.dropna()

    if residual.empty:
        return {}

    going_grp_aligned = going_grp.loc[residual.index]
    SHRINKAGE_K = 200  # heavier shrinkage — no external target
    grp_groups = residual.groupby(going_grp_aligned)
    grp_means = grp_groups.mean()
    grp_counts = grp_groups.count()

    # Cap per-group corrections: France going corrections are computed from
    # global residuals (figure_calibrated - global_mean) which capture
    # confounded class/track/distance effects — unlike UK which computes
    # going offsets after class+course+distance corrections.  Raised from
    # ±3 to ±6 lbs because the prior correction now uses empirical values
    # (reducing systematic bias), but residual going-group effects of
    # 4-5 lbs remain and were previously clamped.
    MAX_GOING_CORRECTION = 6.0

    corrections = {}
    for grp in grp_means.index:
        n = grp_counts[grp]
        raw = grp_means[grp]
        # Negative correction: if going group is over-rated, subtract
        shrunk = -(raw * n / (n + SHRINKAGE_K))
        corrections[grp] = float(np.clip(shrunk, -MAX_GOING_CORRECTION, MAX_GOING_CORRECTION))

    return corrections


def _compute_ga_correction_coeff(df):
    """Compute continuous GA correction coefficient."""
    has_fig = df["figure_calibrated"].notna() & df["ga_value"].notna()
    if has_fig.sum() < 200:
        return 0.0

    # Residual from global mean (class-independent)
    global_mean = df.loc[has_fig, "figure_calibrated"].mean()
    residual = df.loc[has_fig, "figure_calibrated"] - global_mean
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


def _log_within_race_spread(df):
    """Log within-race spread statistics for QA validation."""
    race_stats = df.groupby("race_id")["figure_final"].agg(["max", "min", "count", "std"])
    multi_runner = race_stats[race_stats["count"] >= 3]
    if multi_runner.empty:
        return
    spreads = multi_runner["max"] - multi_runner["min"]
    log.info("  Within-race spread (races with 3+ runners):")
    log.info("    Median winner-to-last gap: %.1f lbs", spreads.median())
    log.info("    Mean winner-to-last gap:   %.1f lbs", spreads.mean())
    log.info("    Mean within-race std:      %.1f lbs", multi_runner["std"].mean())
    log.info("    Races with <5 lbs spread:  %d / %d (%.0f%%)",
             (spreads < 5).sum(), len(spreads),
             100 * (spreads < 5).sum() / len(spreads) if len(spreads) > 0 else 0)


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

    # Stage 1: Initial standard times (all goings, prior-corrected)
    std_dict, std_df = compute_standard_times(df)
    if not std_dict:
        log.warning("No standard times computed — aborting pipeline.")
        return df

    # Stage 2: Course-specific LPL
    lpl_dict = compute_course_lpl(std_df)

    # Stage 3: Going allowances
    ga_dict, ga_se_dict, split_map = compute_going_allowances(df, std_dict)

    # Propagate split-card meeting IDs to the main DataFrame so that
    # downstream stages (winner figures, calibration) use the correct GA.
    _apply_split_meetings(df, split_map)

    # Iterate: recompute standard times with going-corrected data
    for i in range(1, n_iterations):
        log.info("  --- Iteration %d ---", i + 1)
        std_dict, std_df = compute_standard_times_iterative(df, ga_dict)
        lpl_dict = compute_course_lpl(std_df)
        ga_dict, ga_se_dict, split_map = compute_going_allowances(df, std_dict)
        _apply_split_meetings(df, split_map)

    # Stage 9c: Compute empirical GA priors (for future use / artifact persistence)
    empirical_ga_priors = compute_empirical_ga_priors(df, ga_dict)

    # Store GA value on the main DataFrame for calibration stages
    df["ga_value"] = df["meeting_id"].map(ga_dict).fillna(0)

    # Stage 4: Winner figures
    winner_df, winner_fig_dict, qc_failed = compute_winner_figures(
        df, std_dict, ga_dict, lpl_dict
    )

    # Annotate races that failed quality checks:
    # no historical data to generate figures
    df["figure_comment"] = ""
    if qc_failed:
        failed_mask = df["race_id"].isin(qc_failed)
        df.loc[failed_mask, "figure_comment"] = (
            "no historical data to generate figures"
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

        # Within-race spread validation: report median and mean gap between
        # winner and last-placed horse per race (QA for beaten-length spread).
        _log_within_race_spread(df[has_fig])

    log.info("=" * 70)

    # Save audit files
    save_france_batch_audit(df, std_dict, ga_dict)

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
# AUDIT OUTPUT (batch pipeline)
# ═════════════════════════════════════════════════════════════════════

FRANCE_AUDIT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "output", "france_audit"
)


def save_france_batch_audit(df, std_dict, ga_dict):
    """Save audit files for the France batch pipeline run.

    Creates ``output/france_audit/batch/`` containing:
      - ``audit_full_batch.csv``  — all intermediate columns for row-by-row
        verification of every calculation step.
      - ``audit_calc_logic_batch.txt``  — formula reference and summary
        statistics with the constants used.
    """
    from datetime import datetime, timezone

    audit_dir = os.path.join(FRANCE_AUDIT_DIR, "batch")
    os.makedirs(audit_dir, exist_ok=True)

    # 1. Full audit CSV
    audit_cols = [c for c in [
        "meetingDate", "courseName", "raceNumber", "race_id",
        "horseName", "positionOfficial",
        "distance", "going", "raceSurfaceName", "raceClass",
        "horseAge", "horseGender", "weightCarried",
        "finishingTime", "distanceCumulative",
        # Lookup / intermediate values
        "standard_time", "going_allowance", "lpl",
        "corrected_time", "deviation_seconds",
        # Figure chain
        "raw_figure", "weight_adj", "figure_after_weight",
        "wfa_adj", "figure_after_wfa",
        "figure_calibrated", "figure_final",
        # QA
        "figure_comment", "ga_value",
    ] if c in df.columns]

    csv_path = os.path.join(audit_dir, "audit_full_batch.csv")
    df[audit_cols].sort_values(
        ["meetingDate", "courseName", "raceNumber", "positionOfficial"]
    ).to_csv(csv_path, index=False, float_format="%.4f")
    log.info("Audit CSV: %s (%d rows)", csv_path, len(df))

    # 2. Calculation logic reference
    logic_path = os.path.join(audit_dir, "audit_calc_logic_batch.txt")
    lines = []
    lines.append("France Speed Figures — Batch Pipeline Calculation Logic Audit")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    lines.append("=" * 80)
    lines.append("")
    lines.append("CONSTANTS")
    lines.append("-" * 40)
    lines.append(f"  BASE_RATING            = {BASE_RATING}")
    lines.append(f"  BASE_WEIGHT_LBS        = {BASE_WEIGHT_LBS}")
    lines.append(f"  SECONDS_PER_LENGTH     = {SECONDS_PER_LENGTH}")
    lines.append(f"  BENCHMARK_METRES       = {C.BENCHMARK_METRES}")
    lines.append(f"  LBS_PER_SECOND_BENCH   = {C.LBS_PER_SECOND_BENCHMARK}")
    lines.append(f"  LPL_SURFACE_MULTIPLIER = {C.LPL_SURFACE_MULTIPLIER}")
    lines.append(f"  GA_SHRINKAGE_K         = {GA_SHRINKAGE_K}")
    lines.append(f"  GA_OUTLIER_ZSCORE      = {GA_OUTLIER_ZSCORE}")
    lines.append(f"  BL_ATTENUATION_THRESH  = {BL_ATTENUATION_THRESHOLD}")
    lines.append(f"  BL_ATTENUATION_FACTOR  = {BL_ATTENUATION_FACTOR}")
    lines.append("")
    lines.append("FORMULA CHAIN")
    lines.append("-" * 40)
    lines.append("  Stage 1:  standard_time         = trimmed_median(class_adjusted winner times)")
    lines.append("  Stage 2:  going_allowance (GA)   = bayesian_shrunk(winsorized_median(deviations))")
    lines.append("  Stage 3:  lpl                    = generic_lpl * course_correction * surface_mult")
    lines.append("  Stage 4:  corrected_time         = finishingTime - (GA * distance)")
    lines.append("            deviation_seconds      = corrected_time - standard_time")
    lines.append("            deviation_lbs          = (deviation_seconds / SPL) * lpl")
    lines.append("            raw_figure (winner)    = BASE_RATING - deviation_lbs")
    lines.append("  Stage 5:  raw_figure (others)    = winner_figure - (beaten_lengths * lpl)")
    lines.append("  Stage 6:  figure_after_weight    = raw_figure + (weightCarried - BASE_WEIGHT_LBS)")
    lines.append("  Stage 7:  figure_after_wfa       = figure_after_weight + wfa_adj")
    lines.append("  Stage 8:  sex_adj                (not applied)")
    lines.append("  Stage 9:  figure_calibrated      = self_calibration(figure_final)")
    lines.append("")

    # Summary by surface
    has_fig = df["figure_final"].notna()
    lines.append("SUMMARY BY SURFACE")
    lines.append("-" * 40)
    if has_fig.any():
        for surf, grp in df[has_fig].groupby("raceSurfaceName"):
            lines.append(f"  {surf}: {len(grp):>6,} runners  "
                          f"mean={grp['figure_final'].mean():.1f}  "
                          f"std={grp['figure_final'].std():.1f}  "
                          f"range=[{grp['figure_final'].min():.0f}, "
                          f"{grp['figure_final'].max():.0f}]")
    lines.append("")

    # Summary by class
    lines.append("SUMMARY BY CLASS")
    lines.append("-" * 40)
    if has_fig.any() and "raceClass" in df.columns:
        for cls, grp in df[has_fig].groupby("raceClass"):
            lines.append(f"  Class {cls}: {len(grp):>6,} runners  "
                          f"mean={grp['figure_final'].mean():.1f}")
    lines.append("")

    lines.append(f"GOING ALLOWANCES: {len(ga_dict):,} meetings")
    lines.append(f"STANDARD TIMES: {len(std_dict):,} entries")
    lines.append("")
    lines.append(f"Total runners: {len(df):,}")
    lines.append(f"Runners with figures: {has_fig.sum():,}")

    with open(logic_path, "w") as f:
        f.write("\n".join(lines))
    log.info("Audit logic: %s", logic_path)

    return audit_dir


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
      - going_allowances.csv (meeting_id, going_allowance_spm)
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
        columns=["meeting_id", "going_allowance_spm"],
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
        ga_dict = dict(zip(ga_df["meeting_id"], ga_df["going_allowance_spm"]))

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
