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
    LPL_SURFACE_MULTIPLIER,
    SECONDS_PER_LENGTH,
)
from .speed_figures import (
    UK_CLASS_DISTRIBUTION,
    generic_lbs_per_length,
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

        # --- Self-calibration: apply class-based distribution matching ---
        df["figure_calibrated"] = df["figure_after_wfa"].copy()
        has_wfa = df["figure_after_wfa"].notna()

        if self.cal_params:
            # Use pre-computed calibration params from batch pipeline
            for cls, params in self.cal_params.items():
                if cls == "ga_coeff":
                    continue
                if not isinstance(params, dict) or "scale" not in params:
                    continue
                cls_mask = (df["raceClass"] == cls) & has_wfa
                if cls_mask.any():
                    df.loc[cls_mask, "figure_calibrated"] = (
                        df.loc[cls_mask, "figure_after_wfa"] * params["scale"]
                        + params["shift"]
                    )
            # Apply GA correction if available
            ga_coeff = self.cal_params.get("ga_coeff", 0)
            if ga_coeff and "going_allowance" in df.columns:
                df.loc[has_wfa, "figure_calibrated"] += (
                    ga_coeff * df.loc[has_wfa, "going_allowance"].fillna(0)
                )
        else:
            # No pre-computed params — apply live shift-primary calibration
            # Clamp scale to [0.90, 1.10] to preserve within-race spreads
            for cls, uk_dist in UK_CLASS_DISTRIBUTION.items():
                cls_mask = (df["raceClass"] == cls) & has_wfa
                if cls_mask.sum() < 5:
                    continue
                fr_vals = df.loc[cls_mask, "figure_after_wfa"]
                fr_mean = fr_vals.mean()
                fr_std = fr_vals.std()
                if fr_std == 0 or pd.isna(fr_std):
                    continue
                raw_scale = uk_dist["std"] / fr_std
                scale = float(np.clip(raw_scale, 0.90, 1.10))
                shift = uk_dist["mean"] - fr_mean * scale
                df.loc[cls_mask, "figure_calibrated"] = (
                    df.loc[cls_mask, "figure_after_wfa"] * scale + shift
                )

        # Exclude runners beaten > 20 lengths
        beaten_far = (
            df["distanceCumulative"].notna()
            & (df["distanceCumulative"] > 20)
            & (df["positionOfficial"] != 1)
        )
        df.loc[beaten_far, "figure_calibrated"] = np.nan

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
        rc = first.get("raceClass", "?")
        ga = first.get("going_allowance", 0)

        # Format distance in furlongs
        if pd.notna(dist):
            dist_str = f"{dist:.1f}f"
        else:
            dist_str = "?"

        html += f"<h3>{course} &mdash; Race {int(race_num)}</h3>"
        html += (
            f'<p class="meta">{dist_str} &middot; {going} &middot; '
            f'{surface} &middot; Class {rc}</p>'
        )
        if pd.notna(ga):
            html += f'<p class="note">Going allowance: {ga:+.3f} s/f</p>'

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
            "raw_figure", "weight_adj", "wfa_adj", "figure_final",
        ] if c in df.columns]
        df[out_cols].to_csv(csv_path, index=False)
        print(f"\nSaved: {csv_path}")

    finally:
        session.close()


if __name__ == "__main__":
    main()
