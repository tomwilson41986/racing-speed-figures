"""
Live Daily Ratings — France
============================
Computes same-day speed figures for French flat races using pre-built
lookup tables (artifacts) from the batch pipeline.

Mirrors the UK ``src/live_ratings.py`` pattern:
  1. Load artifacts (standard times, LPL, going allowances)
  2. Ingest today's results from PMU via the existing backfill machinery
  3. Compute speed figures for all runners
  4. Output ratings DataFrame

Usage:
  python -m src.france.live_ratings                     # Today
  python -m src.france.live_ratings --date 2026-03-17   # Specific date
  python -m src.france.live_ratings --no-email           # Compute only
"""

import argparse
import datetime
import logging
import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import numpy as np
import pandas as pd

from .constants import (
    BASE_RATING,
    BASE_WEIGHT_LBS,
    BL_ATTENUATION_FACTOR,
    BL_ATTENUATION_THRESHOLD,
    FRANCE_GOING_GA_PRIOR,
    GA_NONLINEAR_BETA,
    GA_NONLINEAR_THRESHOLD,
    GA_OUTLIER_ZSCORE,
    GA_SHRINKAGE_K,
    LPL_SURFACE_MULTIPLIER,
    SECONDS_PER_LENGTH,
)
from .speed_figures import (
    generic_lbs_per_length,
    interpolate_lookup,
    load_artifacts,
    FRANCE_OUTPUT_DIR,
)

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
LIVE_DIR = ROOT_DIR / "data" / "france_live"

# ─── Email configuration ────────────────────────────────────────────
RECIPIENTS = [
    "racingsquared@gmail.com",
]
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")


class FranceLiveRatingEngine:
    """Compute French speed figures using pre-built artifact lookup tables."""

    def __init__(self, artifact_dir=None):
        self.artifact_dir = artifact_dir or FRANCE_OUTPUT_DIR
        self.std_times = {}
        self.lpl_dict = {}
        self.ga_dict = {}
        self.ga_se_dict = {}
        self.cal_params = {}
        self._loaded = False

    def load(self):
        """Load pre-computed artifacts from disk."""
        log.info("Loading France artifacts from %s", self.artifact_dir)
        artifacts = load_artifacts(self.artifact_dir)
        self.std_times = artifacts["std_times"]
        self.lpl_dict = artifacts["lpl_dict"]
        self.ga_dict = artifacts["ga_dict"]
        self.ga_se_dict = artifacts.get("ga_se_dict", {})
        self.cal_params = artifacts.get("cal_params", {})
        self._loaded = True
        log.info("  Loaded: %d std_times, %d lpl, %d ga, cal_params=%s",
                 len(self.std_times), len(self.lpl_dict), len(self.ga_dict),
                 "yes" if self.cal_params else "no")

    def estimate_going_allowance(self, going_desc):
        """Estimate GA from going description when no computed GA exists."""
        return FRANCE_GOING_GA_PRIOR.get(going_desc, 0.05)

    def _compute_realtime_ga(self, df):
        """Compute going allowances from same-day results for meetings
        not covered by pre-computed artifacts.

        Mirrors the UK live_ratings approach:
          1. For each meeting with >=3 winners that have standard times,
             compute per-metre deviations (no class adjustment — France).
          2. Per-meeting z-score outlier removal.
          3. Winsorized median.
          4. Bayesian shrinkage toward going-description prior.
          5. Non-linear correction for extreme going.

        Returns dict of meeting_id → GA (s/m) for newly computed meetings.
        """
        winners = df[df["positionOfficial"] == 1].copy()
        winners["_std_time"] = interpolate_lookup(winners, self.std_times)
        winners = winners[
            winners["finishingTime"].notna()
            & (winners["finishingTime"] > 0)
            & winners["_std_time"].notna()
        ].copy()

        if len(winners) == 0:
            return {}

        # No class adjustment for France — deviation = raw finish - standard
        winners["dev_per_metre"] = (
            (winners["finishingTime"] - winners["_std_time"])
            / winners["distance"]
        )

        meetings = df.groupby("meeting_id").first()[
            ["going", "courseName", "raceSurfaceName"]
        ]
        ga_dict = {}

        for mid, group in winners.groupby("meeting_id"):
            if len(group) < 3:
                continue
            vals = group["dev_per_metre"].sort_values().values.copy()
            n = len(vals)

            # Per-meeting z-score outlier removal
            if n > 2:
                med = np.median(vals)
                std = np.std(vals, ddof=1)
                if std > 0:
                    z = np.abs((vals - med) / std)
                    vals = vals[z <= GA_OUTLIER_ZSCORE]
                    n = len(vals)

            if n < 3:
                continue

            # Winsorized median
            if n > 2:
                vals = np.sort(vals)
                vals[0] = vals[1]
                vals[-1] = vals[-2]
            raw_ga = float(np.median(vals))

            # Bayesian shrinkage toward going-description prior (s/m)
            going_desc = (
                meetings.loc[mid, "going"]
                if mid in meetings.index
                else "Bon"
            )
            if pd.isna(going_desc):
                going_desc = "Bon"
            prior_ga = FRANCE_GOING_GA_PRIOR.get(going_desc, 0.05 / 201.168)
            ga = (n * raw_ga + GA_SHRINKAGE_K * prior_ga) / (n + GA_SHRINKAGE_K)

            # Non-linear correction for extreme going
            abs_ga = abs(ga)
            if abs_ga > GA_NONLINEAR_THRESHOLD:
                sign = 1.0 if ga > 0 else -1.0
                excess = abs_ga - GA_NONLINEAR_THRESHOLD
                ga += sign * GA_NONLINEAR_BETA * excess ** 2

            ga_dict[mid] = ga
            log.info(
                "  %s: computed GA = %+.6f s/m "
                "(%d winners, raw=%+.6f)",
                mid, ga, len(group), raw_ga,
            )

        return ga_dict

    def compute_figures(self, df):
        """
        Compute speed figures for a DataFrame of runners.

        The DataFrame should have columns matching the France field_mapping
        output: std_key, meeting_id, finishingTime, distance, positionOfficial,
        distanceCumulative, weightCarried, horseAge, month, raceClass,
        raceSurfaceName, going, race_id.

        Returns the DataFrame with figure_final column added.
        """
        if not self._loaded:
            self.load()

        if df.empty:
            return df

        df = df.copy()
        df["figure_comment"] = ""

        # Interpolate standard times and LPL to actual distances
        df["standard_time"] = interpolate_lookup(df, self.std_times)
        df["lpl"] = interpolate_lookup(df, self.lpl_dict)

        # Fallback LPL for unmapped keys
        missing_lpl = df["lpl"].isna()
        if missing_lpl.any():
            df.loc[missing_lpl, "lpl"] = df.loc[missing_lpl].apply(
                lambda r: generic_lbs_per_length(
                    r["distance"], r.get("raceSurfaceName")
                ),
                axis=1,
            )

        # Velocity-weighted LPL (matches batch pipeline)
        winners_for_vw = df[df["positionOfficial"] == 1][["race_id", "finishingTime"]].copy()
        winners_for_vw = winners_for_vw.rename(columns={"finishingTime": "winner_time"})
        winners_for_vw = winners_for_vw.drop_duplicates(subset="race_id")
        df = df.merge(winners_for_vw, on="race_id", how="left")

        has_both = (
            df["standard_time"].notna()
            & df["winner_time"].notna()
            & (df["winner_time"] > 0)
        )
        if has_both.any():
            velocity_ratio = (df["standard_time"] / df["winner_time"]).clip(0.85, 1.15)
            df.loc[has_both, "lpl"] = df.loc[has_both, "lpl"] * velocity_ratio[has_both]
            log.info("  Velocity-weighted LPL applied to %d runners", has_both.sum())
        df.drop(columns=["winner_time"], inplace=True, errors="ignore")

        # Going allowance: prefer real-time computation (uses interpolated
        # standard times), fall back to pre-computed artifacts, then estimate
        realtime_ga = self._compute_realtime_ga(df)
        if realtime_ga:
            log.info("  Real-time GA computed for %d meetings", len(realtime_ga))
        df["going_allowance"] = df["meeting_id"].map(realtime_ga)

        # Fill gaps with pre-computed artifact GA
        missing_ga = df["going_allowance"].isna()
        if missing_ga.any():
            artifact_mapped = df.loc[missing_ga, "meeting_id"].map(self.ga_dict)
            df.loc[missing_ga, "going_allowance"] = artifact_mapped

            # Final fallback: going description estimate
            still_missing = df["going_allowance"].isna()
            if still_missing.any():
                df.loc[still_missing, "going_allowance"] = df.loc[
                    still_missing, "going"
                ].map(lambda g: self.estimate_going_allowance(g))

        # ── Quality checks ──────────────────────────────────────────
        # Mark races that fail quality checks so figures are not
        # generated.  Affected runners get figure_final = NaN and a
        # descriptive comment instead.

        failed_race_ids = {}  # {race_id: qc_code}

        # QC-1: No standard time for winner's course/distance
        winners_all = df[df["positionOfficial"] == 1]
        for race_id, row in winners_all.iterrows():
            rid = row["race_id"]
            if pd.isna(row.get("standard_time")):
                failed_race_ids[rid] = "QC1"

        # QC-2: Impossible winner finishing time
        #   Plausible pace in seconds per metre (converted from 10–18 s/f).
        #   Anything outside this range indicates bad source data.
        MIN_PACE_SPM = 10.0 / 201.168
        MAX_PACE_SPM = 18.0 / 201.168
        for race_id, row in winners_all.iterrows():
            rid = row["race_id"]
            ft = row.get("finishingTime")
            dist = row.get("distance")
            if pd.notna(ft) and pd.notna(dist) and dist > 0:
                pace_spm = ft / dist
                if pace_spm < MIN_PACE_SPM or pace_spm > MAX_PACE_SPM:
                    failed_race_ids[rid] = "QC2"
                    log.warning("  QC: Race %s failed — impossible pace %.4f s/m "
                                "(time=%.2f, dist=%.0fm)", rid, pace_spm, ft, dist)

        # QC-3: Broken beaten lengths (all non-winners share identical
        #   cumulative BL — indicates parser failure on source data)
        for rid, grp in df.groupby("race_id"):
            non_winners = grp[grp["positionOfficial"] != 1]
            if len(non_winners) >= 3:
                bl_vals = non_winners["distanceCumulative"].dropna()
                if len(bl_vals) >= 3 and bl_vals.nunique() == 1:
                    failed_race_ids[rid] = "QC3"
                    log.warning("  QC: Race %s failed — all beaten lengths "
                                "identical (%.2f)", rid, bl_vals.iloc[0])

        # QC-4: Arabian / non-TB race detection
        #   Winners finishing >7% slower than standard time (after GA
        #   correction) are likely Arabian races misclassified as PLAT.
        #   Even on heavy ground, TB deviations rarely exceed 5%.
        #   Arabian TBs run ~8-12% slower than regular thoroughbreds.
        QC4_DEVIATION_THRESHOLD = 0.07
        for race_id, row in winners_all.iterrows():
            rid = row["race_id"]
            if rid in failed_race_ids:
                continue
            ft = row.get("finishingTime")
            std = row.get("standard_time")
            ga = row.get("going_allowance") if "going_allowance" in row.index else None
            if pd.notna(ft) and pd.notna(std) and std > 0:
                dist = row.get("distance", 0)
                ga_val = ga if pd.notna(ga) else 0
                corrected = ft - (ga_val * dist)
                deviation_pct = (corrected - std) / std
                if deviation_pct > QC4_DEVIATION_THRESHOLD:
                    failed_race_ids[rid] = "QC4"
                    log.warning(
                        "  QC: Race %s failed — winner %.1f%% slower than "
                        "standard (likely Arabian or non-TB race, "
                        "time=%.2f, std=%.2f)",
                        rid, deviation_pct * 100, ft, std,
                    )

        # QC-5: Arabian breed detection via horse name suffix
        #   Arabian-bred horses carry the " AA" suffix on their registered
        #   name.  If >= 50% of runners have this suffix the race is almost
        #   certainly an Arabian race.
        QC5_AA_FRACTION = 0.50
        for rid, grp in df.groupby("race_id"):
            if rid in failed_race_ids:
                continue
            names = grp["horseName"].dropna()
            if len(names) == 0:
                continue
            aa_count = sum(1 for n in names if str(n).strip().endswith(" AA"))
            if aa_count / len(names) >= QC5_AA_FRACTION:
                failed_race_ids[rid] = "QC5"
                log.warning("  QC: Race %s failed — %d/%d runners have AA suffix "
                            "(likely Arabian race)", rid, aa_count, len(names))

        QC_COMMENTS = {
            "QC1": "no standard time for this course/distance",
            "QC2": "impossible finishing time (bad source data)",
            "QC3": "broken beaten-length data (all identical)",
            "QC5": "likely Arabian race (majority of runners have AA suffix)",
        }
        if failed_race_ids:
            failed_mask = df["race_id"].isin(failed_race_ids)
            for rid, qc_code in failed_race_ids.items():
                rid_mask = df["race_id"] == rid
                if qc_code == "QC4":
                    # Preserve the >10% / >7% distinction for QC-4
                    winner_rows = df[rid_mask & (df["positionOfficial"] == 1)]
                    comment = "likely Arabian/non-TB race (winner >7% slower than standard)"
                    if len(winner_rows) > 0:
                        w = winner_rows.iloc[0]
                        std = w.get("standard_time")
                        ft = w.get("finishingTime")
                        if pd.notna(ft) and pd.notna(std) and std > 0:
                            ga_val = w.get("going_allowance", 0)
                            if pd.isna(ga_val):
                                ga_val = 0
                            corrected = ft - (ga_val * w.get("distance", 0))
                            if (corrected - std) / std > 0.10:
                                comment = "likely Arabian/non-TB race (winner >10% slower than standard)"
                    df.loc[rid_mask, "figure_comment"] = comment
                else:
                    df.loc[rid_mask, "figure_comment"] = QC_COMMENTS.get(qc_code, "QC failure")
            log.info("  QC: %d races (%d runners) failed quality checks",
                     len(failed_race_ids), failed_mask.sum())

        # Only process runners with standard times AND passing QC
        has_std = df["standard_time"].notna() & ~df["race_id"].isin(failed_race_ids)
        log.info("  Runners with standard times: %d / %d", has_std.sum(), len(df))

        # --- Winner figures ---
        winners = df[has_std & (df["positionOfficial"] == 1)].copy()

        # Per-race going attenuation: when the meeting GA substantially
        # overshoots a race's own deviation from standard, the uniform
        # meeting-level correction inflates that race's figure.  Attenuate
        # the GA for such races to prevent over-correction.
        #
        # Example: meeting GA = 0.32 s/f (Souple) but a 1800m race ran at
        # near-standard pace (deviation 0.025 s/f).  Without attenuation
        # the correction makes the race appear 13 lengths faster than
        # standard → raw figure 130.  With attenuation the correction is
        # reduced proportionally to the excess.
        race_dev = (
            (winners["finishingTime"] - winners["standard_time"])
            / winners["distance"]
        )
        meeting_ga = winners["going_allowance"]
        # Only attenuate when meeting GA is positive (soft going) and the
        # race deviation is smaller — i.e., the race was less affected by
        # the going than the meeting average.
        excess = (meeting_ga - race_dev).clip(lower=0)
        GA_RACE_ATTENUATION = 0.5  # remove 50% of the excess
        attenuated_ga = meeting_ga - GA_RACE_ATTENUATION * excess

        winners["corrected_time"] = (
            winners["finishingTime"]
            - (attenuated_ga * winners["distance"])
        )
        winners["deviation_seconds"] = winners["corrected_time"] - winners["standard_time"]
        winners["deviation_lengths"] = winners["deviation_seconds"] / SECONDS_PER_LENGTH
        winners["deviation_lbs"] = winners["deviation_lengths"] * winners["lpl"]
        winners["raw_figure"] = BASE_RATING - winners["deviation_lbs"]

        # Store the effective GA used for this race (for QA audit)
        winners["going_allowance_effective"] = attenuated_ga

        winner_fig = dict(zip(winners["race_id"], winners["raw_figure"]))
        log.info("  Winner figures computed: %d", len(winner_fig))

        # Propagate winner calculation columns back to main DataFrame
        # so QA output can display the full calculation chain.
        winner_calc_cols = ["corrected_time", "deviation_seconds",
                           "deviation_lengths", "deviation_lbs",
                           "going_allowance_effective"]
        for col in winner_calc_cols:
            if col not in df.columns:
                df[col] = np.nan
            df.loc[winners.index, col] = winners[col]

        # Propagate effective GA to all runners in each race so that
        # the ga_coeff calibration step uses the attenuated value.
        race_eff_ga = dict(zip(winners["race_id"],
                               winners["going_allowance_effective"]))
        race_ga_mapped = df["race_id"].map(race_eff_ga)
        has_eff = race_ga_mapped.notna()
        df.loc[has_eff, "going_allowance"] = race_ga_mapped[has_eff]

        # --- All-runner figures via beaten lengths ---
        df["winner_figure"] = df["race_id"].map(winner_fig)
        has_winner = df["winner_figure"].notna()

        is_winner = df["positionOfficial"] == 1
        cum_raw = df["distanceCumulative"].fillna(0).clip(lower=0)

        # Beaten-length attenuation
        ga = df["going_allowance"].fillna(0)
        T = np.clip(BL_ATTENUATION_THRESHOLD + (ga * -8), 10, 30)
        cum = np.where(cum_raw <= T, cum_raw, T + BL_ATTENUATION_FACTOR * (cum_raw - T))

        df["bl_effective"] = cum  # attenuated BL for QA display
        df["lbs_behind"] = cum * df["lpl"]
        df.loc[is_winner, "lbs_behind"] = 0.0
        df["raw_figure"] = df["winner_figure"] - df["lbs_behind"]

        # Non-finishers
        no_pos = df["positionOfficial"].isna() | (df["positionOfficial"] == 0)
        df.loc[no_pos, "raw_figure"] = np.nan

        # --- Weight adjustment ---
        has_w = df["weightCarried"].notna()
        df["weight_adj"] = 0.0
        df.loc[has_w, "weight_adj"] = df.loc[has_w, "weightCarried"] - BASE_WEIGHT_LBS
        df["figure_after_weight"] = df["raw_figure"] + df["weight_adj"]

        # --- WFA adjustment (empirical, derived from GBR timefigures) ---
        # Always applied regardless of race age composition so that figures
        # are comparable across the entire population.
        from .constants import get_france_wfa_allowance
        df["wfa_adj"] = df.apply(
            lambda r: get_france_wfa_allowance(
                r["horseAge"], r["month"], r["distance"],
            ),
            axis=1,
        )
        df["figure_after_wfa"] = df["figure_after_weight"] + df["wfa_adj"]

        # --- Global calibration using batch-derived parameters ---
        # Single scale+shift for all runners (class-independent).
        # Falls back to DEFAULT_CAL_PARAMS if artifacts unavailable.
        from .speed_figures import DEFAULT_CAL_PARAMS
        df["figure_calibrated"] = df["figure_after_wfa"].copy()
        has_wfa = df["figure_after_wfa"].notna()

        cal = self.cal_params if self.cal_params else DEFAULT_CAL_PARAMS

        # Apply global scale+shift
        global_params = cal.get("global")
        if not (global_params and isinstance(global_params, dict)):
            # Artifacts pre-date global migration (class-based keys only).
            # Class-based calibration is fundamentally wrong for speed figures:
            # it adds +30 lbs shift for Group races, making every Group winner
            # rate ~100 regardless of actual speed.  Fall back to defaults.
            log.warning("  Artifacts missing 'global' key — using DEFAULT_CAL_PARAMS")
            global_params = DEFAULT_CAL_PARAMS["global"]

        scale = global_params["scale"]
        shift = global_params["shift"]
        # Sanity-check: scale < 0.3 means outliers inflated fr_std
        # during calibration, crushing beaten-length/weight adjustments
        if scale < 0.3:
            log.warning("  Calibration scale %.4f too low (outlier-driven); "
                        "clamping to 0.3", scale)
            scale = 0.3
            shift = global_params.get("target_mean", 72.0) - (
                global_params.get("fr_median", global_params.get("fr_mean", 100.0)) * scale
            )
        df.loc[has_wfa, "figure_calibrated"] = (
            df.loc[has_wfa, "figure_after_wfa"] * scale + shift
        )
        log.info("  Global calibration: scale=%.4f shift=%+.1f  n=%d",
                 scale, shift, has_wfa.sum())

        # NOTE: BL band corrections intentionally NOT applied.
        # The _compute_bl_band_corrections() function measures the expected
        # BL gap (beaten_cal - winner_cal) and treats it as a "residual" to
        # correct.  This undoes the beaten-length penalty, producing absurd
        # results (horses finishing 10th rated higher than the winner).
        # With the robust IQR-based calibration scale, the BL extension
        # works correctly and no band correction is needed.

        # NOTE: Per-going-group corrections and continuous GA coeff corrections
        # are NOT applied.  These post-calibration adjustments were inflating
        # figures by +3-5 lbs (PSF going correction +2.67, GA coeff up to +0.8)
        # and were computed against the old target_mean=72 calibration.
        # The per-meeting going allowance already accounts for going variation
        # at the race level.  See QA audit 2026-03-23.

        # Exclude runners beaten > 20 lengths
        beaten_far = (
            df["distanceCumulative"].notna()
            & (df["distanceCumulative"] > 20)
            & (df["positionOfficial"] != 1)
        )
        df.loc[beaten_far, "figure_calibrated"] = np.nan
        df.loc[beaten_far, "figure_comment"] = "excluded: beaten >20 lengths"

        df["figure_final"] = df["figure_calibrated"]

        has_fig = df["figure_final"].notna()
        log.info("  Runners with figures: %d / %d", has_fig.sum(), len(df))
        if has_fig.any():
            log.info("  Figure range: %.0f to %.0f",
                     df.loc[has_fig, "figure_final"].min(),
                     df.loc[has_fig, "figure_final"].max())

        return df

    def rate_day(self, session, race_date):
        """
        Ingest and rate all flat races for a given date.

        Parameters
        ----------
        session : SQLAlchemy session (france.db)
        race_date : datetime.date

        Returns
        -------
        pd.DataFrame with figure_final for all runners.
        """
        from .field_mapping import load_france_dataframe

        log.info("Rating French races for %s", race_date.isoformat())
        df = load_france_dataframe(session, start_date=race_date, end_date=race_date)
        if df.empty:
            log.warning("No data for %s", race_date)
            return df

        return self.compute_figures(df)


def _fig_class(fig):
    """CSS class name for a figure value."""
    if pd.isna(fig):
        return ""
    if fig >= 110:
        return "high"
    if fig >= 85:
        return "good"
    return "avg"


def format_email_html(df, target_date, run_time):
    """Format the France ratings as a styled HTML email."""
    df = df.copy()
    df["_sort_pos"] = df["positionOfficial"].where(
        df["positionOfficial"].notna() & (df["positionOfficial"] > 0), other=9999
    )
    df = df.sort_values(["courseName", "raceNumber", "_sort_pos"])
    df = df.drop(columns=["_sort_pos"])

    css = """
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; color: #333;
               max-width: 800px; margin: 0 auto; padding: 10px; }
        h1 { color: #1a3a5c; border-bottom: 3px solid #c8102e;
             padding-bottom: 10px; }
        h2 { color: #1a3a5c; margin-top: 30px; }
        h3 { color: #555; margin-top: 20px;
             border-left: 4px solid #c8102e; padding-left: 10px; }
        table { border-collapse: collapse; width: 100%;
                margin: 10px 0; font-size: 14px; }
        th { background: #1a3a5c; color: white;
             padding: 8px 12px; text-align: left; }
        td { padding: 6px 12px; border-bottom: 1px solid #ddd; }
        tr:nth-child(even) { background: #f8f9fa; }
        .high { color: #c8102e; font-weight: bold; }
        .good { color: #1a5c3a; font-weight: bold; }
        .avg  { color: #555; }
        .box  { background: #f0f4f8; border: 1px solid #d0d8e0;
                border-radius: 8px; padding: 15px; margin: 15px 0; }
        .meta { color: #666; font-size: 13px; margin-bottom: 5px; }
        .note { color: #888; font-size: 12px; font-style: italic; }
        .foot { color: #888; font-size: 12px; margin-top: 30px;
                border-top: 1px solid #ddd; padding-top: 10px; }
    </style>
    """

    html = f"""<html><head>{css}</head><body>
    <h1>France Speed Figures &mdash; {target_date}</h1>
    <p>Generated at {run_time} GMT</p>
    """

    # ── Top performers ──
    rated = df[df["figure_final"].notna() & (df["figure_final"] >= 0)]
    top = rated.nlargest(10, "figure_final")
    if len(top) > 0:
        html += '<div class="box"><h2>Top Performers</h2><table>'
        html += (
            "<tr><th>#</th><th>Horse</th><th>Course</th>"
            "<th>Race</th><th>Pos</th><th>Figure</th></tr>"
        )
        for i, (_, r) in enumerate(top.iterrows(), 1):
            fig = r["figure_final"]
            cls = _fig_class(fig)
            html += (
                f'<tr><td>{i}</td><td><b>{r.get("horseName", "?")}</b></td>'
                f'<td>{r.get("courseName", "")}</td>'
                f'<td>R{int(r.get("raceNumber", 0))}</td>'
                f'<td>{int(r.get("positionOfficial", 0))}</td>'
                f'<td class="{cls}">{fig:.0f}</td></tr>'
            )
        html += "</table></div>"

    # ── Race-by-race breakdown ──
    html += "<h2>Full Results</h2>"

    for (course, race_num), race_df in df.groupby(
        ["courseName", "raceNumber"], sort=True
    ):
        first = race_df.iloc[0]
        dist = first.get("distance", "?")
        going = first.get("going", "?")
        surface = first.get("raceSurfaceName", "?")
        ga = first.get("going_allowance", 0)

        # Format distance in metres
        if pd.notna(dist):
            dist_str = f"{int(dist)}m"
        else:
            dist_str = "?"

        html += f"<h3>{course} &mdash; Race {int(race_num)}</h3>"
        html += (
            f'<p class="meta">{dist_str} &middot; {going} &middot; '
            f'{surface}</p>'
        )
        if pd.notna(ga):
            html += f'<p class="note">Going allowance: {ga:+.6f} s/m</p>'

        html += (
            "<table><tr><th>Pos</th><th>Horse</th><th>Age</th>"
            "<th>Wgt</th><th>Beaten</th><th>Time</th>"
            "<th>Figure</th></tr>"
        )

        for _, r in race_df.iterrows():
            pos = (
                int(r["positionOfficial"])
                if pd.notna(r.get("positionOfficial"))
                and r["positionOfficial"] > 0
                else "-"
            )
            horse = r.get("horseName", "?")
            age = (
                int(r["horseAge"])
                if pd.notna(r.get("horseAge"))
                else "?"
            )
            wgt = (
                f'{int(r["weightCarried"])}'
                if pd.notna(r.get("weightCarried"))
                else "-"
            )
            beaten = (
                f'{r["distanceCumulative"]:.2f}'
                if pd.notna(r.get("distanceCumulative"))
                and r.get("distanceCumulative", 0) > 0
                else "-"
            )
            fin_time = (
                f'{r["finishingTime"]:.2f}'
                if pd.notna(r.get("finishingTime"))
                else "-"
            )
            fig = r.get("figure_final")
            if pd.notna(fig) and fig >= 0:
                fig_str = f"{fig:.0f}"
                cls = _fig_class(fig)
            else:
                fig_str = "-"
                cls = ""

            html += (
                f"<tr><td>{pos}</td><td><b>{horse}</b></td>"
                f"<td>{age}</td><td>{wgt}</td>"
                f"<td>{beaten}</td>"
                f"<td>{fin_time}</td>"
                f'<td class="{cls}">{fig_str}</td></tr>'
            )

        html += "</table>"

    # ── Footer ──
    total = rated.shape[0]
    races = df["race_id"].nunique() if "race_id" in df.columns else "?"
    n_courses = len(rated["courseName"].unique()) if len(rated) > 0 else 0
    html += f"""
    <div class="foot">
        <p>{total} runners rated across {races} races</p>
        <p>Figures calibrated to Timeform scale using {n_courses}
           course standard times from French racing dataset.</p>
        <p>Racing Speed Figures &mdash; France Ratings Engine</p>
    </div></body></html>
    """
    return html


def send_email(html, target_date, run_time, recipients=None):
    """Send the France ratings email via SMTP."""
    if recipients is None:
        recipients = RECIPIENTS

    log.info(
        "Email config: SMTP_USER=%s, SMTP_PASS=%s, host=%s:%s, recipients=%s",
        "set" if SMTP_USER else "EMPTY",
        "set" if SMTP_PASS else "EMPTY",
        SMTP_HOST, SMTP_PORT, recipients,
    )

    if not SMTP_USER or not SMTP_PASS:
        log.error(
            "Email not configured. Set SMTP_USER and SMTP_PASS env vars. "
            "For Gmail, use an App Password "
            "(Google Account > Security > 2FA > App Passwords)."
        )
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"France Speed Figures — {target_date} ({run_time} GMT)"
    )
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)

    text = (
        f"France Speed Figures for {target_date}. "
        "View this email in HTML format for full results."
    )
    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        log.info("Connecting to %s:%s...", SMTP_HOST, SMTP_PORT)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            log.info("Authenticating as %s...", SMTP_USER)
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, recipients, msg.as_string())
        log.info("Email sent successfully to %s", recipients)
        return True
    except smtplib.SMTPAuthenticationError as e:
        log.error(
            "SMTP authentication failed: %s. "
            "Check that SMTP_USER is a valid Gmail address and "
            "SMTP_PASS is a 16-character Gmail App Password.", e,
        )
        return False
    except Exception as e:
        log.error("Failed to send email: %s", e, exc_info=True)
        return False


# ═════════════════════════════════════════════════════════════════════
# QA OUTPUT — save full calculation chain for review
# ═════════════════════════════════════════════════════════════════════

QA_DIR = ROOT_DIR / "output" / "france_qa"


def save_qa_output(df, race_date, run_source="manual"):
    """Save a comprehensive QA package for a ratings run.

    Creates ``output/france_qa/{date}/`` containing:
      - ``qa_full_{date}.csv``  — every intermediate column so each
        calculation step can be verified row-by-row.
      - ``qa_calc_logic_{date}.txt``  — a per-race breakdown showing the
        formula applied at each stage with actual values.

    Parameters
    ----------
    df : pd.DataFrame
        The rated DataFrame (output of ``compute_figures``).
    race_date : datetime.date
        Target race date.
    run_source : str
        Label for the run ("live", "workflow", "manual").
    """
    date_str = race_date.isoformat()
    qa_run_dir = QA_DIR / date_str
    os.makedirs(qa_run_dir, exist_ok=True)

    # ── 1. Full QA CSV with all intermediate columns ────────────────
    qa_cols = [c for c in [
        "meetingDate", "courseName", "raceNumber", "race_id",
        "horseName", "positionOfficial", "distance", "going",
        "raceSurfaceName", "raceClass", "horseAge", "weightCarried",
        "finishingTime", "distanceCumulative",
        # Lookup values
        "standard_time", "lpl", "going_allowance",
        # Winner calculation chain
        "corrected_time", "deviation_seconds", "deviation_lengths",
        "deviation_lbs",
        # All-runner extension
        "winner_figure", "bl_effective", "lbs_behind", "raw_figure",
        # Adjustments
        "weight_adj", "figure_after_weight",
        "wfa_adj", "figure_after_wfa",
        # Calibration
        "figure_calibrated", "figure_final",
        # QA
        "figure_comment",
    ] if c in df.columns]

    qa_csv_path = qa_run_dir / f"qa_full_{date_str}.csv"
    df[qa_cols].sort_values(
        ["courseName", "raceNumber", "positionOfficial"]
    ).to_csv(str(qa_csv_path), index=False, float_format="%.4f")
    log.info("QA full CSV saved: %s", qa_csv_path)

    # ── 2. Calculation logic breakdown (human-readable) ─────────────
    logic_path = qa_run_dir / f"qa_calc_logic_{date_str}.txt"
    lines = []
    lines.append(f"France Speed Figures — Calculation Logic QA")
    lines.append(f"Date: {date_str}   Run source: {run_source}")
    lines.append(f"Generated: {datetime.datetime.utcnow().isoformat(timespec='seconds')}Z")
    lines.append("=" * 80)
    lines.append("")
    lines.append("FORMULA REFERENCE")
    lines.append("-" * 40)
    lines.append(f"  BASE_RATING          = {BASE_RATING}")
    lines.append(f"  BASE_WEIGHT_LBS      = {BASE_WEIGHT_LBS}")
    lines.append(f"  SECONDS_PER_LENGTH   = {SECONDS_PER_LENGTH}")
    lines.append(f"  BL_ATTENUATION_THRESH= {BL_ATTENUATION_THRESHOLD}")
    lines.append(f"  BL_ATTENUATION_FACTOR= {BL_ATTENUATION_FACTOR}")
    lines.append("")
    lines.append("  corrected_time       = finishingTime - (going_allowance * distance)")
    lines.append("  deviation_seconds    = corrected_time - standard_time")
    lines.append("  deviation_lengths    = deviation_seconds / SECONDS_PER_LENGTH")
    lines.append("  deviation_lbs        = deviation_lengths * lpl")
    lines.append("  raw_figure (winner)  = BASE_RATING - deviation_lbs")
    lines.append("  raw_figure (others)  = winner_figure - lbs_behind")
    lines.append("  figure_after_weight  = raw_figure + (weightCarried - BASE_WEIGHT_LBS)")
    lines.append("  figure_after_wfa     = figure_after_weight + wfa_adj")
    lines.append("  figure_calibrated    = figure_after_wfa * scale + shift")
    lines.append("  figure_final         = figure_calibrated")
    lines.append("")

    # Per-race breakdown
    sorted_df = df.sort_values(["courseName", "raceNumber", "positionOfficial"])
    for (course, rnum), race_df in sorted_df.groupby(
        ["courseName", "raceNumber"], sort=False
    ):
        race_row = race_df.iloc[0]
        rid = race_row.get("race_id", "?")
        lines.append("=" * 80)
        lines.append(f"RACE: {course} Race {int(rnum)}  (id={rid})")
        dist_val = race_row.get('distance', '?')
        dist_display = f"{int(dist_val)}m" if pd.notna(dist_val) else "?"
        lines.append(f"  Distance: {dist_display}  "
                      f"Going: {race_row.get('going', '?')}  "
                      f"Surface: {race_row.get('raceSurfaceName', '?')}  "
                      f"Class: {race_row.get('raceClass', '?')}")

        comment = race_row.get("figure_comment", "")
        if comment:
            lines.append(f"  ** QC: {comment} **")
            lines.append("")
            continue

        std_t = race_row.get("standard_time")
        ga = race_row.get("going_allowance")
        lpl_val = race_row.get("lpl")
        lines.append(f"  standard_time={_fmt(std_t)}s  "
                      f"going_allowance={_fmt(ga, dp=6)} s/m  "
                      f"lpl={_fmt(lpl_val)} lbs/L")
        lines.append("-" * 60)

        # Winner calculation
        winner_rows = race_df[race_df["positionOfficial"] == 1]
        if len(winner_rows) > 0:
            w = winner_rows.iloc[0]
            lines.append(f"  WINNER: {w.get('horseName', '?')}")
            ft = w.get("finishingTime")
            ct = w.get("corrected_time")
            dev_s = w.get("deviation_seconds")
            dev_l = w.get("deviation_lengths")
            dev_lbs = w.get("deviation_lbs")
            raw = w.get("raw_figure")
            lines.append(f"    finishingTime        = {_fmt(ft)}s")
            lines.append(f"    corrected_time       = {_fmt(ft)} - ({_fmt(ga)} * {_fmt(w.get('distance'))}) = {_fmt(ct)}")
            lines.append(f"    deviation_seconds    = {_fmt(ct)} - {_fmt(std_t)} = {_fmt(dev_s)}")
            lines.append(f"    deviation_lengths    = {_fmt(dev_s)} / {SECONDS_PER_LENGTH} = {_fmt(dev_l)}")
            lines.append(f"    deviation_lbs        = {_fmt(dev_l)} * {_fmt(lpl_val)} = {_fmt(dev_lbs)}")
            lines.append(f"    raw_figure           = {BASE_RATING} - {_fmt(dev_lbs)} = {_fmt(raw)}")
            _append_adjustment_lines(lines, w)
            lines.append("")

        # Other runners
        others = race_df[race_df["positionOfficial"] != 1]
        for _, r in others.iterrows():
            pos = r.get("positionOfficial")
            pos_str = str(int(pos)) if pd.notna(pos) and pos > 0 else "DNF"
            lines.append(f"  {pos_str:>4s}. {r.get('horseName', '?')}")
            wf = r.get("winner_figure")
            lb = r.get("lbs_behind")
            raw = r.get("raw_figure")
            cum = r.get("distanceCumulative")
            bl_eff = r.get("bl_effective")
            lines.append(f"    beaten_lengths       = {_fmt(cum)}L")
            if (pd.notna(cum) and pd.notna(bl_eff)
                    and abs(cum - bl_eff) > 0.01):
                lines.append(f"    bl_attenuated        = {_fmt(bl_eff)}L  (threshold exceeded)")
                lines.append(f"    lbs_behind           = {_fmt(bl_eff)} * {_fmt(lpl_val)} = {_fmt(lb)}")
            else:
                lines.append(f"    lbs_behind           = {_fmt(cum)} * {_fmt(lpl_val)} = {_fmt(lb)}")
            lines.append(f"    raw_figure           = {_fmt(wf)} - {_fmt(lb)} = {_fmt(raw)}")
            _append_adjustment_lines(lines, r)
            lines.append("")

        lines.append("")

    with open(logic_path, "w") as f:
        f.write("\n".join(lines))
    log.info("QA calculation logic saved: %s", logic_path)

    return qa_run_dir


def _fmt(val, dp=4):
    """Format a value for the QA text file."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    if isinstance(val, float):
        return f"{val:.{dp}f}"
    return str(val)


def _append_adjustment_lines(lines, row):
    """Append weight/WFA/calibration lines for a runner."""
    w_adj = row.get("weight_adj")
    fig_w = row.get("figure_after_weight")
    wfa = row.get("wfa_adj")
    fig_wfa = row.get("figure_after_wfa")
    fig_cal = row.get("figure_calibrated")
    fig_final = row.get("figure_final")
    lines.append(f"    weight_adj           = {_fmt(w_adj)}")
    lines.append(f"    figure_after_weight  = {_fmt(fig_w)}")
    lines.append(f"    wfa_adj              = {_fmt(wfa)}")
    lines.append(f"    figure_after_wfa     = {_fmt(fig_wfa)}")
    lines.append(f"    figure_calibrated    = {_fmt(fig_cal)}")
    lines.append(f"    figure_final         = {_fmt(fig_final)}")


def main():
    """CLI entry point for France live ratings."""
    parser = argparse.ArgumentParser(description="France Live Daily Ratings")
    parser.add_argument("--date", type=str, default=None,
                        help="Target date (YYYY-MM-DD), default today")
    parser.add_argument("--db", type=str, default="sqlite:///france.db",
                        help="Database connection string")
    parser.add_argument("--artifact-dir", type=str, default=None,
                        help="Artifact directory (default: output/france)")
    parser.add_argument("--output-csv", type=str, default=None,
                        help="Output CSV path (default: data/france_live/ratings_DATE.csv)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    target_date = (
        datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
        if args.date else datetime.date.today()
    )

    # Set up database
    from .database import get_engine, get_session, init_db
    engine = get_engine(args.db)
    init_db(engine)
    session = get_session(engine)

    try:
        engine_obj = FranceLiveRatingEngine(artifact_dir=args.artifact_dir)
        engine_obj.load()

        df = engine_obj.rate_day(session, target_date)

        if df.empty:
            log.warning("No results for %s", target_date)
            sys.exit(0)

        has_fig = df["figure_final"].notna()
        print(f"\n=== France Live Ratings: {target_date} ===")
        print(f"Runners rated: {has_fig.sum()} / {len(df)}")

        if has_fig.any():
            print(f"Figure range: {df.loc[has_fig, 'figure_final'].min():.0f} "
                  f"to {df.loc[has_fig, 'figure_final'].max():.0f}")

            # Top 10
            top = df[has_fig].nlargest(10, "figure_final")
            print("\nTop 10:")
            for _, r in top.iterrows():
                pos = int(r["positionOfficial"]) if pd.notna(r["positionOfficial"]) else 0
                print(f"  {r.get('horseName', '?'):25s}  "
                      f"{r.get('courseName', '?'):20s}  "
                      f"Pos {pos}  Fig {r['figure_final']:.0f}")

        # Save CSV
        os.makedirs(LIVE_DIR, exist_ok=True)
        csv_path = args.output_csv or str(
            LIVE_DIR / f"ratings_{target_date.isoformat()}.csv"
        )
        out_cols = [c for c in [
            "meetingDate", "courseName", "raceNumber", "race_id",
            "horseName", "positionOfficial", "distance", "going",
            "raceSurfaceName", "raceClass", "horseAge", "weightCarried",
            "finishingTime", "distanceCumulative", "going_allowance",
            "raw_figure", "weight_adj", "wfa_adj", "figure_calibrated", "figure_final",
            "figure_comment",
        ] if c in df.columns]
        df[out_cols].to_csv(csv_path, index=False)
        print(f"\nSaved: {csv_path}")

        # Save audit CSV with all intermediate columns
        audit_dir = ROOT_DIR / "output" / "france_audit"
        os.makedirs(audit_dir, exist_ok=True)
        audit_path = audit_dir / f"audit_{target_date.isoformat()}.csv"
        audit_cols = [c for c in [
            "meetingDate", "courseName", "raceNumber", "race_id",
            "horseName", "positionOfficial", "distance", "going",
            "raceSurfaceName", "raceClass", "horseAge", "weightCarried",
            "finishingTime", "distanceCumulative",
            "standard_time", "lpl", "going_allowance",
            "raw_figure", "weight_adj", "figure_after_weight",
            "wfa_adj", "figure_after_wfa",
            "figure_calibrated", "figure_final", "figure_comment",
        ] if c in df.columns]
        df[audit_cols].to_csv(str(audit_path), index=False)
        print(f"Audit CSV: {audit_path}")

        # Save QA output for review (full calculation chain + logic breakdown)
        qa_dir = save_qa_output(df, target_date, run_source="live")
        print(f"QA output: {qa_dir}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
