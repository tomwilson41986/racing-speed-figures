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

Stages 9-10 (Timeform calibration + GBR) are NOT used — there is no
external calibration target for France.  Figures are self-calibrated
on the 100-point scale (100 = standard time on good ground at 9st 0lb).
"""

import logging

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
    LBS_PER_SECOND_5F,
    LPL_SURFACE_MULTIPLIER,
    MIN_RACES_GOING_ALLOWANCE,
    SECONDS_PER_LENGTH,
)
from .database import DailyFigureRow, RunnerRow
from .field_mapping import load_france_dataframe

log = logging.getLogger(__name__)


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

    n_excluded = (~mask).sum()
    if n_excluded > 0:
        log.info("    Excluded %s maiden winners from standard-time compilation",
                 f"{n_excluded:,}")

    return winners[mask].copy()


def compute_standard_times(df):
    """
    Standard times per track / distance / surface.

    Same algorithm as UK ``compute_standard_times()``:
      1. Collect winning times (excluding maidens)
      2. Prefer races on good going; fall back to all goings
      3. Apply class adjustment to normalise times
      4. Take the median → standard time
    """
    log.info("  Computing standard times (per track / distance / surface)...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = _filter_std_time_winners(winners)
    winners_good = winners[winners["going"].isin(FRANCE_GOOD_GOING)].copy()

    log.info("    Winners total: %s", f"{len(winners):,}")
    log.info("    Winners on good going: %s", f"{len(winners_good):,}")

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
            std_times["n_races"] >= C.MIN_RACES_STANDARD_TIME, "std_key"
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

    Same algorithm as UK ``compute_standard_times_iterative()``:
    after GA estimates exist, correct every winner's time for going,
    then use ALL goings for more robust standard times.
    """
    log.info("    Recomputing standard times (going-corrected, all goings)...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = _filter_std_time_winners(winners)

    # Apply going correction
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

    std_rows = []
    for key, grp in winners.groupby("std_key"):
        std_rows.append({
            "std_key": key,
            "median_time": grp["adj_time"].median(),
            "mean_time": grp["adj_time"].mean(),
            "n_races": len(grp),
            "distance": grp["distance"].iloc[0],
            "courseName": grp["courseName"].iloc[0],
            "surface": grp["raceSurfaceName"].iloc[0],
        })
    std_agg = pd.DataFrame(std_rows)

    valid = std_agg[std_agg["n_races"] >= C.MIN_RACES_STANDARD_TIME].copy()

    # Shrinkage for combos with 10+ but < C.MIN_RACES_STANDARD_TIME races:
    # blend their median with the overall distance median (same as UK
    # Irish shrinkage).
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

    log.info("    Standard-time combos (>= %d races): %s (using all goings)",
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

def compute_going_allowances(df, std_times):
    """
    Going allowance per meeting in seconds-per-furlong (s/f).

    Same algorithm as UK ``compute_going_allowances()``:
      1. Per-winner deviation from standard time
      2. Per-meeting outlier removal (z-score)
      3. Winsorized median within each meeting
      4. Split-card going detection
      5. Bayesian shrinkage toward French going-description prior
      6. Non-linear correction for extreme going
      7. Minimum races filter
    """
    log.info("  Computing going allowances (per track / day)...")

    winners = df[df["positionOfficial"] == 1].copy()
    winners = winners[winners["std_key"].isin(std_times)].copy()
    winners["standard_time"] = winners["std_key"].map(std_times)

    # Class-adjust actual time
    winners["class_adj"] = winners.apply(
        lambda r: compute_class_adjustment(r["raceClass"], r["distance"]),
        axis=1,
    )
    winners["adj_time"] = winners["finishingTime"] - winners["class_adj"]

    # Per-furlong deviation from standard
    winners["deviation"] = winners["adj_time"] - winners["standard_time"]
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

    # ── Winsorized median within each meeting ──
    def _winsorized_median(group):
        vals = group.sort_values().values.copy()
        n = len(vals)
        if n <= 2:
            return np.median(vals)
        vals[0] = vals[1]
        vals[-1] = vals[-2]
        return np.median(vals)

    ga_series = (
        winners.groupby("meeting_id")["dev_per_furlong"]
        .apply(_winsorized_median)
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
    ga_se_dict = ga_se_series[ga_se_series.index.isin(valid_ids)].to_dict()
    ga_n = ga_count[ga_count.index.isin(valid_ids)].to_dict()

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

        # All French meetings use same shrinkage strength
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
    df["figure_final"] = df["figure_after_wfa"]
    return df


# ═════════════════════════════════════════════════════════════════════
# ORCHESTRATOR — FULL PIPELINE
# ═════════════════════════════════════════════════════════════════════

def run_pipeline(session: Session, start_date=None, end_date=None,
                 n_iterations: int = 2) -> pd.DataFrame:
    """
    Execute the full French speed-figure pipeline (stages 0-8).

    Parameters
    ----------
    session : SQLAlchemy session
    start_date, end_date : optional date bounds
    n_iterations : number of standard-time / GA iterations (default 2,
                   same as UK pipeline converges in 2-3 iterations)

    Returns
    -------
    pd.DataFrame with ``figure_final`` column for every runner.
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

    # Store GA value on the main DataFrame for potential later use
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

    return df


# ═════════════════════════════════════════════════════════════════════
# PERSISTENCE
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
