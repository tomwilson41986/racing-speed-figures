"""
Comprehensive Model Accuracy Audit
====================================
Audits speed figure accuracy against Timeform timefigure, broken down by:
  - Track (per course)
  - Ground/going conditions
  - Ratings spread (figure bands)
  - Distance bands
  - Age groups
  - Surface (Turf vs AW)
  - In-sample vs out-of-sample years
  - Country (UK vs Ireland)

Identifies the biggest sources of inaccuracy and quantifies each.

Usage:
    python scripts/audit_model_accuracy.py
"""

import pandas as pd
import numpy as np
import os
import sys

# ─────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")
FIGURES_PATH = os.path.join(OUTPUT_DIR, "speed_figures.csv")
REPORT_PATH = os.path.join(OUTPUT_DIR, "audit_report.txt")

# Out-of-sample years (model trained on 2015-2023)
OOS_YEARS = {2024, 2025, 2026}

# Irish courses (for UK vs IRE split)
IRE_COURSES = {
    "BALLINROBE", "BELLEWSTOWN", "CLONMEL", "CORK", "CURRAGH",
    "DOWN ROYAL", "DOWNPATRICK", "DUNDALK", "FAIRYHOUSE",
    "GALWAY", "GOWRAN PARK", "KILBEGGAN", "KILLARNEY",
    "LAYTOWN", "LEOPARDSTOWN", "LIMERICK", "LISTOWEL",
    "NAAS", "NAVAN", "PUNCHESTOWN", "ROSCOMMON", "SLIGO",
    "THURLES", "TIPPERARY", "TRAMORE", "WEXFORD",
}


# ─────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────

def metrics(predicted, actual):
    """Compute accuracy metrics for a group."""
    err = predicted - actual
    n = len(err)
    if n == 0:
        return {}
    mae = err.abs().mean()
    rmse = np.sqrt((err ** 2).mean())
    bias = err.mean()
    corr = np.corrcoef(predicted, actual)[0, 1] if n > 2 else np.nan
    within_5 = (err.abs() <= 5).mean() * 100
    within_10 = (err.abs() <= 10).mean() * 100
    return {
        "n": n,
        "mae": mae,
        "rmse": rmse,
        "bias": bias,
        "corr": corr,
        "within_5": within_5,
        "within_10": within_10,
    }


def metrics_df(df, group_col, fig_col="figure_calibrated", target="timefigure"):
    """Compute metrics per group and return as DataFrame."""
    rows = []
    for name, grp in df.groupby(group_col):
        m = metrics(grp[fig_col], grp[target])
        if m:
            m["group"] = name
            rows.append(m)
    return pd.DataFrame(rows).set_index("group").sort_values("mae", ascending=False)


class Report:
    """Accumulates report text and prints simultaneously."""
    def __init__(self):
        self.lines = []

    def print(self, text=""):
        print(text)
        self.lines.append(text)

    def heading(self, text, char="="):
        bar = char * 70
        self.print(f"\n{bar}")
        self.print(text)
        self.print(bar)

    def subheading(self, text):
        self.heading(text, "-")

    def table(self, df, title=None, float_fmt="{:.2f}"):
        if title:
            self.print(f"\n  {title}")
            self.print(f"  {'─' * 66}")
        header = f"  {'Group':<30} {'N':>7} {'MAE':>6} {'RMSE':>6} {'Bias':>7} {'Corr':>6} {'±5':>5} {'±10':>5}"
        self.print(header)
        self.print(f"  {'─' * 78}")
        for idx, row in df.iterrows():
            name = str(idx)[:30]
            line = (
                f"  {name:<30} {int(row['n']):>7,} {row['mae']:>6.2f} "
                f"{row['rmse']:>6.2f} {row['bias']:>+7.2f} {row['corr']:>6.3f} "
                f"{row['within_5']:>5.1f} {row['within_10']:>5.1f}"
            )
            self.print(line)

    def save(self, path):
        with open(path, "w") as f:
            f.write("\n".join(self.lines))
        self.print(f"\nReport saved to {path}")


# ─────────────────────────────────────────────────────────────────────
# MAIN AUDIT
# ─────────────────────────────────────────────────────────────────────

def run_audit():
    rpt = Report()
    rpt.heading("COMPREHENSIVE MODEL ACCURACY AUDIT")
    rpt.print(f"Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")
    rpt.print(f"Data: {FIGURES_PATH}")

    # Load data
    df = pd.read_csv(FIGURES_PATH, low_memory=False)
    rpt.print(f"Total rows: {len(df):,}")

    # Filter to rows with valid figures and timefigures
    valid = df[
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_calibrated"].notna()
    ].copy()
    rpt.print(f"Rows with valid timefigure + figure: {len(valid):,}")

    # Derived columns
    valid["error"] = valid["figure_calibrated"] - valid["timefigure"]
    valid["abs_error"] = valid["error"].abs()
    valid["country"] = valid["courseName"].apply(
        lambda x: "IRE" if x in IRE_COURSES else "UK"
    )
    valid["is_oos"] = valid["source_year"].isin(OOS_YEARS)

    # Going groups
    going_map = {
        "Hard": "Firm", "Firm": "Firm", "Fast": "Firm",
        "Gd/Frm": "Good/Firm", "Good To Firm": "Good/Firm",
        "Good to Firm": "Good/Firm", "Std/Fast": "Good/Firm",
        "Good": "Good/Std", "Standard": "Good/Std", "Std": "Good/Std",
        "Gd/Sft": "Good/Soft", "Good to Soft": "Good/Soft",
        "Good To Yielding": "Good/Soft", "Good to Yielding": "Good/Soft",
        "Std/Slow": "Good/Soft", "Standard/Slow": "Good/Soft",
        "Standard To Slow": "Good/Soft", "Standard to Slow": "Good/Soft",
        "Slow": "Good/Soft",
        "Soft": "Soft", "Yielding": "Soft", "Yld/Sft": "Soft",
        "Sft/Hvy": "Soft", "Hvy/Sft": "Soft",
        "Heavy": "Heavy",
    }
    valid["going_group"] = valid["going"].map(going_map).fillna("Unknown")

    # Distance bands
    valid["dist_band"] = pd.cut(
        valid["distance"],
        bins=[0, 6, 8, 10, 12, 16, 25],
        labels=["5f-6f", "7f-8f", "9f-10f", "11f-12f", "13f-16f", "17f+"],
    )

    # Figure bands (using timefigure as ground truth)
    valid["figure_band"] = pd.cut(
        valid["timefigure"],
        bins=[-200, 20, 40, 60, 80, 100, 120, 200],
        labels=["<20", "20-40", "40-60", "60-80", "80-100", "100-120", "120+"],
    )

    # ═══════════════════════════════════════════════════════════════
    # 1. OVERALL METRICS
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("1. OVERALL METRICS")
    overall = metrics(valid["figure_calibrated"], valid["timefigure"])
    rpt.print(f"  Correlation:  {overall['corr']:.4f}")
    rpt.print(f"  MAE:          {overall['mae']:.2f} lbs")
    rpt.print(f"  RMSE:         {overall['rmse']:.2f} lbs")
    rpt.print(f"  Bias:         {overall['bias']:+.2f} lbs")
    rpt.print(f"  Within ±5:    {overall['within_5']:.1f}%")
    rpt.print(f"  Within ±10:   {overall['within_10']:.1f}%")

    # Scale comparison
    rpt.print(f"\n  Scale comparison:")
    rpt.print(f"    Our std dev: {valid['figure_calibrated'].std():.1f}")
    rpt.print(f"    TFig std dev: {valid['timefigure'].std():.1f}")
    rpt.print(f"    Compression ratio: {valid['figure_calibrated'].std() / valid['timefigure'].std():.3f}")

    # In-sample vs OOS
    rpt.subheading("1a. IN-SAMPLE vs OUT-OF-SAMPLE")
    is_df = metrics_df(valid, valid["is_oos"].map({True: "Out-of-sample (2024-26)", False: "In-sample (2015-23)"}))
    rpt.table(is_df)

    # ═══════════════════════════════════════════════════════════════
    # 2. BY TRACK (COURSE)
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("2. ACCURACY BY TRACK")

    track_df = metrics_df(valid, "courseName")
    rpt.print(f"\n  Total tracks: {len(track_df)}")

    # Worst 15 tracks
    rpt.table(track_df.head(15), title="WORST 15 TRACKS (by MAE)")

    # Best 10 tracks
    rpt.table(track_df.tail(10).iloc[::-1], title="BEST 10 TRACKS (by MAE)")

    # Worst tracks OOS only
    oos = valid[valid["is_oos"]]
    if len(oos) > 0:
        track_oos = metrics_df(oos, "courseName")
        # Only tracks with decent sample
        track_oos_sig = track_oos[track_oos["n"] >= 100]
        rpt.table(track_oos_sig.head(15), title="WORST 15 TRACKS — OUT-OF-SAMPLE ONLY (n≥100)")

    # Track + surface split
    valid["track_surface"] = valid["courseName"] + " (" + valid["raceSurfaceName"] + ")"
    ts_df = metrics_df(valid, "track_surface")
    rpt.table(ts_df.head(15), title="WORST 15 TRACK × SURFACE COMBOS")

    # ═══════════════════════════════════════════════════════════════
    # 3. BY GOING / GROUND
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("3. ACCURACY BY GOING (GROUND)")

    # By going group
    going_df = metrics_df(valid, "going_group")
    rpt.table(going_df, title="BY GOING GROUP (all data)")

    # Going × surface
    valid["going_surface"] = valid["going_group"] + " (" + valid["raceSurfaceName"] + ")"
    gs_df = metrics_df(valid, "going_surface")
    rpt.table(gs_df, title="BY GOING GROUP × SURFACE")

    # Going OOS
    if len(oos) > 0:
        going_oos = metrics_df(oos, "going_group")
        rpt.table(going_oos, title="BY GOING GROUP — OUT-OF-SAMPLE ONLY")

    # Extreme going analysis
    rpt.subheading("3a. EXTREME GOING DEEP DIVE")
    for go in ["Heavy", "Firm"]:
        subset = valid[valid["going_group"] == go]
        if len(subset) > 100:
            rpt.print(f"\n  {go} going: n={len(subset):,}")
            rpt.print(f"    MAE: {(subset['error']).abs().mean():.2f}")
            rpt.print(f"    Bias: {subset['error'].mean():+.2f}")
            rpt.print(f"    % over-rated: {(subset['error'] > 0).mean()*100:.1f}%")
            # By track on extreme going
            if len(subset) > 500:
                track_extreme = metrics_df(subset, "courseName")
                worst = track_extreme[track_extreme["n"] >= 50].head(10)
                if len(worst) > 0:
                    rpt.table(worst, title=f"  WORST TRACKS ON {go.upper()} GOING (n≥50)")

    # ═══════════════════════════════════════════════════════════════
    # 4. BY RATINGS SPREAD (FIGURE BANDS)
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("4. ACCURACY BY RATINGS SPREAD")

    band_df = metrics_df(valid, "figure_band")
    rpt.table(band_df, title="BY TIMEFIGURE BAND")

    # Compression analysis
    rpt.subheading("4a. SCALE COMPRESSION ANALYSIS")
    rpt.print("\n  Compression = how much our scale is squished vs Timeform")
    rpt.print(f"  {'Band':<15} {'Our Mean':>10} {'TF Mean':>10} {'Diff':>8} {'Our Std':>10} {'TF Std':>10}")
    rpt.print(f"  {'─' * 68}")
    for band in ["<20", "20-40", "40-60", "60-80", "80-100", "100-120", "120+"]:
        subset = valid[valid["figure_band"] == band]
        if len(subset) > 100:
            our_mean = subset["figure_calibrated"].mean()
            tf_mean = subset["timefigure"].mean()
            our_std = subset["figure_calibrated"].std()
            tf_std = subset["timefigure"].std()
            diff = our_mean - tf_mean
            rpt.print(
                f"  {band:<15} {our_mean:>10.1f} {tf_mean:>10.1f} {diff:>+8.1f} "
                f"{our_std:>10.1f} {tf_std:>10.1f}"
            )

    # Band × surface
    valid["band_surface"] = valid["figure_band"].astype(str) + " (" + valid["raceSurfaceName"] + ")"
    bs_df = metrics_df(valid, "band_surface")
    rpt.table(bs_df, title="BY FIGURE BAND × SURFACE")

    # Band OOS
    if len(oos) > 0:
        oos["figure_band"] = valid.loc[oos.index, "figure_band"]
        band_oos = metrics_df(oos, "figure_band")
        rpt.table(band_oos, title="BY FIGURE BAND — OUT-OF-SAMPLE ONLY")

    # ═══════════════════════════════════════════════════════════════
    # 5. BY DISTANCE
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("5. ACCURACY BY DISTANCE")

    dist_df = metrics_df(valid, "dist_band")
    rpt.table(dist_df, title="BY DISTANCE BAND")

    # Distance × surface
    valid["dist_surface"] = valid["dist_band"].astype(str) + " (" + valid["raceSurfaceName"] + ")"
    ds_df = metrics_df(valid, "dist_surface")
    rpt.table(ds_df, title="BY DISTANCE × SURFACE")

    # ═══════════════════════════════════════════════════════════════
    # 6. BY AGE
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("6. ACCURACY BY AGE")

    valid["age_group"] = valid["horseAge"].clip(upper=10).astype(int).astype(str)
    valid.loc[valid["horseAge"] > 10, "age_group"] = "11+"
    age_df = metrics_df(valid, "age_group")
    rpt.table(age_df, title="BY AGE")

    # Age × surface
    valid["age_surface"] = valid["age_group"] + " (" + valid["raceSurfaceName"] + ")"
    as_df = metrics_df(valid, "age_surface")
    worst_age_surface = as_df[as_df["n"] >= 200].head(15)
    rpt.table(worst_age_surface, title="WORST AGE × SURFACE COMBOS (n≥200)")

    # ═══════════════════════════════════════════════════════════════
    # 7. BY RACE CLASS
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("7. ACCURACY BY RACE CLASS")

    valid["class_label"] = "C" + valid["raceClass"].fillna(0).astype(int).astype(str)
    class_df = metrics_df(valid, "class_label")
    rpt.table(class_df, title="BY RACE CLASS")

    # Class × surface
    valid["class_surface"] = valid["class_label"] + " (" + valid["raceSurfaceName"] + ")"
    cs_df = metrics_df(valid, "class_surface")
    rpt.table(cs_df, title="BY CLASS × SURFACE")

    # ═══════════════════════════════════════════════════════════════
    # 8. BY COUNTRY (UK vs IRELAND)
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("8. ACCURACY BY COUNTRY")

    country_df = metrics_df(valid, "country")
    rpt.table(country_df, title="UK vs IRELAND")

    # Country × surface
    valid["country_surface"] = valid["country"] + " (" + valid["raceSurfaceName"] + ")"
    cntry_surf = metrics_df(valid, "country_surface")
    rpt.table(cntry_surf, title="COUNTRY × SURFACE")

    # ═══════════════════════════════════════════════════════════════
    # 9. BY YEAR
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("9. ACCURACY BY YEAR")

    year_df = metrics_df(valid, "source_year")
    rpt.table(year_df, title="BY YEAR")

    # ═══════════════════════════════════════════════════════════════
    # 10. SURFACE COMPARISON
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("10. ACCURACY BY SURFACE")

    surf_df = metrics_df(valid, "raceSurfaceName")
    rpt.table(surf_df, title="BY SURFACE")

    # ═══════════════════════════════════════════════════════════════
    # 11. WORST INDIVIDUAL TRACK×DISTANCE COMBOS
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("11. WORST TRACK × DISTANCE COMBOS")

    valid["track_dist"] = valid["courseName"] + " " + valid["distance"].round(0).astype(int).astype(str) + "f"
    td_df = metrics_df(valid, "track_dist")
    td_sig = td_df[td_df["n"] >= 100]
    rpt.table(td_sig.head(20), title="WORST 20 TRACK × DISTANCE COMBOS (n≥100)")

    # ═══════════════════════════════════════════════════════════════
    # 12. ERROR CONTRIBUTION ANALYSIS
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("12. ERROR CONTRIBUTION ANALYSIS")
    rpt.print("\n  Which factors contribute most to total MAE?")
    rpt.print("  (Weighted MAE contribution = group_MAE × group_share)")

    # Compute weighted contributions by factor
    factors = {
        "Going": "going_group",
        "Track": "courseName",
        "Figure band": "figure_band",
        "Distance": "dist_band",
        "Surface": "raceSurfaceName",
        "Age": "age_group",
        "Class": "class_label",
        "Country": "country",
    }

    overall_mae = overall["mae"]
    factor_contributions = []

    for factor_name, col in factors.items():
        groups = valid.groupby(col)
        total_excess = 0
        worst_group = None
        worst_excess = 0
        for name, grp in groups:
            m = metrics(grp["figure_calibrated"], grp["timefigure"])
            if m:
                share = m["n"] / len(valid)
                excess = (m["mae"] - overall_mae) * share
                total_excess += abs(excess)
                if m["mae"] > worst_excess and m["n"] >= 100:
                    worst_excess = m["mae"]
                    worst_group = name
        factor_contributions.append({
            "factor": factor_name,
            "dispersion": total_excess,
            "worst_group": worst_group,
            "worst_mae": worst_excess,
        })

    fc_df = pd.DataFrame(factor_contributions).sort_values("dispersion", ascending=False)
    rpt.print(f"\n  {'Factor':<15} {'Dispersion':>12} {'Worst Group':<30} {'Worst MAE':>10}")
    rpt.print(f"  {'─' * 72}")
    for _, row in fc_df.iterrows():
        rpt.print(
            f"  {row['factor']:<15} {row['dispersion']:>12.3f} "
            f"{str(row['worst_group']):<30} {row['worst_mae']:>10.2f}"
        )

    # ═══════════════════════════════════════════════════════════════
    # 13. BIAS PATTERN ANALYSIS
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("13. SYSTEMATIC BIAS PATTERNS")

    rpt.subheading("13a. BIAS BY FIGURE LEVEL (10-pt bands)")
    rpt.print(f"\n  {'TFig Band':<15} {'N':>8} {'Mean Bias':>10} {'Direction':>12}")
    rpt.print(f"  {'─' * 50}")
    for lo in range(-10, 140, 10):
        hi = lo + 10
        subset = valid[(valid["timefigure"] >= lo) & (valid["timefigure"] < hi)]
        if len(subset) >= 50:
            bias = subset["error"].mean()
            direction = "over-rated" if bias > 0 else "under-rated"
            rpt.print(f"  {lo:>3}–{hi:<10} {len(subset):>8,} {bias:>+10.2f} {direction:>12}")

    rpt.subheading("13b. BIAS BY GOING (showing over/under-rating direction)")
    for going_grp in ["Firm", "Good/Firm", "Good/Std", "Good/Soft", "Soft", "Heavy"]:
        subset = valid[valid["going_group"] == going_grp]
        if len(subset) >= 50:
            bias = subset["error"].mean()
            direction = "over-rated" if bias > 0 else "under-rated"
            rpt.print(f"  {going_grp:<15} n={len(subset):>7,}  bias={bias:>+.2f}  ({direction})")

    rpt.subheading("13c. BIAS BY DISTANCE")
    for dist_band in ["5f-6f", "7f-8f", "9f-10f", "11f-12f", "13f-16f", "17f+"]:
        subset = valid[valid["dist_band"] == dist_band]
        if len(subset) >= 50:
            bias = subset["error"].mean()
            rpt.print(f"  {dist_band:<15} n={len(subset):>7,}  bias={bias:>+.2f}")

    # ═══════════════════════════════════════════════════════════════
    # 14. OOS DEGRADATION ANALYSIS
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("14. OUT-OF-SAMPLE DEGRADATION")

    in_sample = valid[~valid["is_oos"]]
    oos_data = valid[valid["is_oos"]]
    is_metrics = metrics(in_sample["figure_calibrated"], in_sample["timefigure"])
    oos_metrics = metrics(oos_data["figure_calibrated"], oos_data["timefigure"])

    rpt.print(f"\n  {'Metric':<20} {'In-sample':>12} {'OOS':>12} {'Degradation':>14}")
    rpt.print(f"  {'─' * 62}")
    for m in ["mae", "rmse", "corr", "bias"]:
        is_val = is_metrics[m]
        oos_val = oos_metrics[m]
        if m == "corr":
            deg = f"{oos_val - is_val:+.4f}"
        else:
            deg = f"{oos_val - is_val:+.2f}"
        rpt.print(f"  {m.upper():<20} {is_val:>12.3f} {oos_val:>12.3f} {deg:>14}")

    # What degrades most OOS?
    rpt.subheading("14a. OOS DEGRADATION BY GOING")
    rpt.print(f"\n  {'Going':<15} {'IS MAE':>8} {'OOS MAE':>8} {'Δ MAE':>8}")
    rpt.print(f"  {'─' * 44}")
    for going_grp in ["Firm", "Good/Firm", "Good/Std", "Good/Soft", "Soft", "Heavy"]:
        is_sub = in_sample[in_sample["going_group"] == going_grp]
        oos_sub = oos_data[oos_data["going_group"] == going_grp]
        if len(is_sub) >= 50 and len(oos_sub) >= 50:
            is_mae = (is_sub["error"]).abs().mean()
            oos_mae = (oos_sub["error"]).abs().mean()
            rpt.print(f"  {going_grp:<15} {is_mae:>8.2f} {oos_mae:>8.2f} {oos_mae - is_mae:>+8.2f}")

    rpt.subheading("14b. OOS DEGRADATION BY FIGURE BAND")
    rpt.print(f"\n  {'Band':<15} {'IS MAE':>8} {'OOS MAE':>8} {'Δ MAE':>8}")
    rpt.print(f"  {'─' * 44}")
    for band in ["<20", "20-40", "40-60", "60-80", "80-100", "100-120", "120+"]:
        is_sub = in_sample[in_sample["figure_band"] == band]
        oos_sub = oos_data[oos_data["figure_band"] == band]
        if len(is_sub) >= 50 and len(oos_sub) >= 50:
            is_mae = (is_sub["error"]).abs().mean()
            oos_mae = (oos_sub["error"]).abs().mean()
            rpt.print(f"  {band:<15} {is_mae:>8.2f} {oos_mae:>8.2f} {oos_mae - is_mae:>+8.2f}")

    # ═══════════════════════════════════════════════════════════════
    # 15. SUMMARY: TOP INACCURACY SOURCES
    # ═══════════════════════════════════════════════════════════════
    rpt.heading("15. SUMMARY — TOP SOURCES OF INACCURACY")

    # Collect all significant problem areas (MAE > overall + 2 AND n >= 200)
    problem_areas = []

    # Tracks
    for idx, row in track_df.iterrows():
        if row["mae"] > overall_mae + 2 and row["n"] >= 200:
            problem_areas.append({
                "category": "Track",
                "group": idx,
                "mae": row["mae"],
                "excess": row["mae"] - overall_mae,
                "n": int(row["n"]),
                "bias": row["bias"],
            })

    # Going
    for idx, row in going_df.iterrows():
        if row["mae"] > overall_mae + 1 and row["n"] >= 200:
            problem_areas.append({
                "category": "Going",
                "group": idx,
                "mae": row["mae"],
                "excess": row["mae"] - overall_mae,
                "n": int(row["n"]),
                "bias": row["bias"],
            })

    # Figure bands
    for idx, row in band_df.iterrows():
        if row["mae"] > overall_mae + 1 and row["n"] >= 200:
            problem_areas.append({
                "category": "Rating Band",
                "group": idx,
                "mae": row["mae"],
                "excess": row["mae"] - overall_mae,
                "n": int(row["n"]),
                "bias": row["bias"],
            })

    # Distance
    for idx, row in dist_df.iterrows():
        if row["mae"] > overall_mae + 1 and row["n"] >= 200:
            problem_areas.append({
                "category": "Distance",
                "group": idx,
                "mae": row["mae"],
                "excess": row["mae"] - overall_mae,
                "n": int(row["n"]),
                "bias": row["bias"],
            })

    # Age
    for idx, row in age_df.iterrows():
        if row["mae"] > overall_mae + 1 and row["n"] >= 200:
            problem_areas.append({
                "category": "Age",
                "group": idx,
                "mae": row["mae"],
                "excess": row["mae"] - overall_mae,
                "n": int(row["n"]),
                "bias": row["bias"],
            })

    prob_df = pd.DataFrame(problem_areas).sort_values("excess", ascending=False)
    rpt.print(f"\n  Problem areas (MAE > overall + threshold, n≥200):")
    rpt.print(f"  Overall MAE: {overall_mae:.2f}")
    rpt.print(f"\n  {'Category':<15} {'Group':<25} {'MAE':>6} {'Excess':>8} {'Bias':>7} {'N':>8}")
    rpt.print(f"  {'─' * 74}")
    for _, row in prob_df.iterrows():
        rpt.print(
            f"  {row['category']:<15} {str(row['group']):<25} {row['mae']:>6.2f} "
            f"{row['excess']:>+8.2f} {row['bias']:>+7.2f} {int(row['n']):>8,}"
        )

    rpt.print(f"\n  Total problem areas identified: {len(prob_df)}")

    # ═══════════════════════════════════════════════════════════════
    # SAVE REPORT
    # ═══════════════════════════════════════════════════════════════
    rpt.save(REPORT_PATH)

    return valid, rpt


if __name__ == "__main__":
    run_audit()
