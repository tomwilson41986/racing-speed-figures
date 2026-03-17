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
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from .constants import (
    BASE_RATING,
    BASE_WEIGHT_LBS,
    BL_ATTENUATION_FACTOR,
    BL_ATTENUATION_THRESHOLD,
    FRANCE_GOING_GA_PRIOR,
    LPL_SURFACE_MULTIPLIER,
    SECONDS_PER_LENGTH,
)
from .speed_figures import (
    compute_class_adjustment,
    generic_lbs_per_length,
    load_artifacts,
    FRANCE_OUTPUT_DIR,
)

log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
LIVE_DIR = ROOT_DIR / "data" / "france_live"


class FranceLiveRatingEngine:
    """Compute French speed figures using pre-built artifact lookup tables."""

    def __init__(self, artifact_dir=None):
        self.artifact_dir = artifact_dir or FRANCE_OUTPUT_DIR
        self.std_times = {}
        self.lpl_dict = {}
        self.ga_dict = {}
        self.ga_se_dict = {}
        self._loaded = False

    def load(self):
        """Load pre-computed artifacts from disk."""
        log.info("Loading France artifacts from %s", self.artifact_dir)
        artifacts = load_artifacts(self.artifact_dir)
        self.std_times = artifacts["std_times"]
        self.lpl_dict = artifacts["lpl_dict"]
        self.ga_dict = artifacts["ga_dict"]
        self.ga_se_dict = artifacts.get("ga_se_dict", {})
        self._loaded = True
        log.info("  Loaded: %d std_times, %d lpl, %d ga",
                 len(self.std_times), len(self.lpl_dict), len(self.ga_dict))

    def estimate_going_allowance(self, going_desc):
        """Estimate GA from going description when no computed GA exists."""
        return FRANCE_GOING_GA_PRIOR.get(going_desc, 0.05)

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

        # Map standard times and LPL
        df["standard_time"] = df["std_key"].map(self.std_times)
        df["lpl"] = df["std_key"].map(self.lpl_dict)

        # Fallback LPL for unmapped keys
        missing_lpl = df["lpl"].isna()
        if missing_lpl.any():
            df.loc[missing_lpl, "lpl"] = df.loc[missing_lpl].apply(
                lambda r: generic_lbs_per_length(
                    r["distance"], r.get("raceSurfaceName")
                ),
                axis=1,
            )

        # Going allowance: use pre-computed if available, else estimate
        df["going_allowance"] = df["meeting_id"].map(self.ga_dict)
        missing_ga = df["going_allowance"].isna()
        if missing_ga.any():
            df.loc[missing_ga, "going_allowance"] = df.loc[missing_ga, "going"].map(
                lambda g: self.estimate_going_allowance(g)
            )

        # Only process runners with standard times
        has_std = df["standard_time"].notna()
        log.info("  Runners with standard times: %d / %d", has_std.sum(), len(df))

        # --- Winner figures ---
        winners = df[has_std & (df["positionOfficial"] == 1)].copy()
        winners["corrected_time"] = (
            winners["finishingTime"]
            - (winners["going_allowance"] * winners["distance"])
        )
        winners["deviation_seconds"] = winners["corrected_time"] - winners["standard_time"]
        winners["deviation_lengths"] = winners["deviation_seconds"] / SECONDS_PER_LENGTH
        winners["deviation_lbs"] = winners["deviation_lengths"] * winners["lpl"]
        winners["raw_figure"] = BASE_RATING - winners["deviation_lbs"]

        winner_fig = dict(zip(winners["race_id"], winners["raw_figure"]))
        log.info("  Winner figures computed: %d", len(winner_fig))

        # --- All-runner figures via beaten lengths ---
        df["winner_figure"] = df["race_id"].map(winner_fig)
        has_winner = df["winner_figure"].notna()

        is_winner = df["positionOfficial"] == 1
        cum_raw = df["distanceCumulative"].fillna(0).clip(lower=0)

        # Beaten-length attenuation
        ga = df["going_allowance"].fillna(0)
        T = np.clip(BL_ATTENUATION_THRESHOLD + (ga * -8), 10, 30)
        cum = np.where(cum_raw <= T, cum_raw, T + BL_ATTENUATION_FACTOR * (cum_raw - T))

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

        # --- WFA adjustment ---
        from src.speed_figures import get_wfa_allowance
        df["wfa_adj"] = df.apply(
            lambda r: get_wfa_allowance(
                r["horseAge"], r["month"], r["distance"],
                r.get("raceSurfaceName"),
            ),
            axis=1,
        )
        df["figure_after_wfa"] = df["figure_after_weight"] + df["wfa_adj"]
        df["figure_final"] = df["figure_after_wfa"]

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
            "finishingTime", "distanceCumulative",
            "raw_figure", "weight_adj", "wfa_adj", "figure_final",
        ] if c in df.columns]
        df[out_cols].to_csv(csv_path, index=False)
        print(f"\nSaved: {csv_path}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
