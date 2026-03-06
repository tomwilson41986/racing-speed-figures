"""
Analyse OOS (2024-2026) patterns to identify specific improvement opportunities.

Focus areas:
1. Meeting-level consistency: are all races at a meeting biased by the same factor?
2. Distance-specific bias: systematic over/under-rating at certain distances
3. Going estimate accuracy: how well do our going allowances work live?
4. Course x distance x going interactions
"""

import pandas as pd
import numpy as np
import os

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")


def load_data():
    df = pd.read_csv(os.path.join(OUTPUT_DIR, "speed_figures.csv"), low_memory=False)
    mask = (
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_calibrated"].notna()
    )
    df = df[mask].copy()
    df["error"] = df["figure_calibrated"] - df["timefigure"]
    df["abs_error"] = df["error"].abs()
    df["dist_round"] = df["distance"].round(0).astype(int)
    return df


def analyse_meeting_level_bias(df):
    """Check if meeting-level bias is consistent (all races at a meeting off by same factor)."""
    print("=" * 80)
    print("1. MEETING-LEVEL BIAS ANALYSIS")
    print("=" * 80)

    oos = df[df["source_year"] >= 2024].copy()
    oos["meeting_id"] = (
        oos["meetingDate"].astype(str) + "_"
        + oos["courseName"].astype(str) + "_"
        + oos["raceSurfaceName"].astype(str)
    )

    # Per-meeting mean error
    meeting_stats = oos.groupby("meeting_id").agg(
        mean_error=("error", "mean"),
        std_error=("error", "std"),
        mae=("abs_error", "mean"),
        n_runners=("error", "count"),
        mean_abs_error_per_race=("error", lambda x: x.abs().mean()),
    ).reset_index()

    meeting_stats = meeting_stats[meeting_stats["n_runners"] >= 5]

    print(f"\n  Meetings with >=5 runners: {len(meeting_stats)}")
    print(f"  Meeting-level mean bias: {meeting_stats['mean_error'].mean():+.2f}")
    print(f"  Meeting-level mean |bias|: {meeting_stats['mean_error'].abs().mean():.2f}")
    print(f"  Meeting-level std of bias: {meeting_stats['mean_error'].std():.2f}")

    # How often is the meeting-level bias >3 lbs?
    big_bias = (meeting_stats["mean_error"].abs() > 3).mean() * 100
    print(f"  Meetings with |mean bias| > 3 lbs: {big_bias:.1f}%")

    # The key insight: if within-meeting std is LOW relative to between-meeting bias,
    # then the error is systematic (meeting-wide) rather than per-horse random.
    avg_within = meeting_stats["std_error"].mean()
    avg_between = meeting_stats["mean_error"].std()
    print(f"\n  Avg within-meeting error std: {avg_within:.2f}")
    print(f"  Between-meeting bias std: {avg_between:.2f}")
    print(f"  Ratio (between/within): {avg_between/avg_within:.2f}")
    print(f"  --> If ratio > 0.5, meeting-level corrections could help significantly")

    # Meeting-level bias by surface
    for surface in ["Turf", "All Weather"]:
        surf_meetings = meeting_stats[
            meeting_stats["meeting_id"].str.contains(surface)
        ]
        if len(surf_meetings) > 10:
            print(f"\n  {surface}:")
            print(f"    N meetings: {len(surf_meetings)}")
            print(f"    Mean bias: {surf_meetings['mean_error'].mean():+.2f}")
            print(f"    Std of bias: {surf_meetings['mean_error'].std():.2f}")
            print(f"    Mean within-meeting std: {surf_meetings['std_error'].mean():.2f}")

    # Meeting-level bias by going group
    oos["going_group"] = oos["going"].map({
        "Hard": "Firm", "Firm": "Firm", "Fast": "Firm",
        "Gd/Frm": "GdFm", "Good To Firm": "GdFm", "Good to Firm": "GdFm", "Std/Fast": "GdFm",
        "Good": "Good", "Standard": "Good", "Std": "Good",
        "Gd/Sft": "GdSft", "Good to Soft": "GdSft", "Good To Yielding": "GdSft",
        "Good to Yielding": "GdSft", "Std/Slow": "GdSft", "Standard/Slow": "GdSft",
        "Standard To Slow": "GdSft", "Standard to Slow": "GdSft", "Slow": "GdSft",
        "Soft": "Soft", "Yielding": "Soft", "Yld/Sft": "Soft", "Sft/Hvy": "Soft", "Hvy/Sft": "Soft",
        "Heavy": "Heavy",
    }).fillna("Unknown")

    print(f"\n  Meeting-level bias by going group (OOS):")
    for going_grp in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
        sub = oos[oos["going_group"] == going_grp]
        if len(sub) > 50:
            print(f"    {going_grp:<8}: bias={sub['error'].mean():+.2f}, MAE={sub['abs_error'].mean():.2f}, n={len(sub)}")

    return meeting_stats


def analyse_distance_bias(df):
    """Detailed distance bias analysis."""
    print("\n" + "=" * 80)
    print("2. DISTANCE-SPECIFIC BIAS ANALYSIS")
    print("=" * 80)

    # Overall distance bias
    print("\n  By exact distance (furlongs), all data:")
    print(f"  {'Dist':>5} {'N':>8} {'Bias':>7} {'MAE':>6} {'OOS Bias':>9} {'OOS MAE':>8}")
    print(f"  {'─' * 50}")

    oos = df[df["source_year"] >= 2024]
    ins = df[df["source_year"] <= 2023]

    for dist in sorted(df["dist_round"].unique()):
        sub = df[df["dist_round"] == dist]
        sub_oos = oos[oos["dist_round"] == dist]
        if len(sub) >= 500:
            bias = sub["error"].mean()
            mae = sub["abs_error"].mean()
            oos_bias = sub_oos["error"].mean() if len(sub_oos) > 50 else np.nan
            oos_mae = sub_oos["abs_error"].mean() if len(sub_oos) > 50 else np.nan
            print(
                f"  {dist:>5}f {len(sub):>8,} {bias:>+7.2f} {mae:>6.2f} "
                f"{oos_bias:>+9.2f} {oos_mae:>8.2f}"
            )

    # Distance bias by surface
    for surface in ["Turf", "All Weather"]:
        print(f"\n  Distance bias on {surface}:")
        sub = df[df["raceSurfaceName"] == surface]
        sub_oos = oos[oos["raceSurfaceName"] == surface]
        print(f"  {'Dist':>5} {'N':>8} {'Bias':>7} {'MAE':>6} {'OOS Bias':>9} {'OOS MAE':>8}")
        print(f"  {'─' * 50}")
        for dist in sorted(sub["dist_round"].unique()):
            s = sub[sub["dist_round"] == dist]
            s_oos = sub_oos[sub_oos["dist_round"] == dist]
            if len(s) >= 200:
                bias = s["error"].mean()
                mae = s["abs_error"].mean()
                oos_bias = s_oos["error"].mean() if len(s_oos) > 30 else np.nan
                oos_mae = s_oos["abs_error"].mean() if len(s_oos) > 30 else np.nan
                print(
                    f"  {dist:>5}f {len(s):>8,} {bias:>+7.2f} {mae:>6.2f} "
                    f"{oos_bias:>+9.2f} {oos_mae:>8.2f}"
                )


def analyse_going_estimation_accuracy(df):
    """Analyse going allowance accuracy in OOS period."""
    print("\n" + "=" * 80)
    print("3. GOING ALLOWANCE ESTIMATION ACCURACY")
    print("=" * 80)

    # Load going allowances
    ga_path = os.path.join(OUTPUT_DIR, "going_allowances.csv")
    ga_df = pd.read_csv(ga_path)

    oos = df[df["source_year"] >= 2024].copy()
    oos["meeting_id"] = (
        oos["meetingDate"].astype(str) + "_"
        + oos["courseName"].astype(str) + "_"
        + oos["raceSurfaceName"].astype(str)
    )

    # Parse GA
    ga_dict = dict(zip(ga_df["meeting_id"], ga_df["going_allowance_spf"]))
    oos["ga_value"] = oos["meeting_id"].map(ga_dict)

    has_ga = oos["ga_value"].notna()
    print(f"\n  OOS runners with computed GA: {has_ga.sum():,} / {len(oos):,}")
    print(f"  OOS runners without GA: {(~has_ga).sum():,}")

    # Compare error for meetings WITH computed GA vs estimated GA
    with_ga = oos[has_ga]
    without_ga = oos[~has_ga]

    print(f"\n  With computed GA:    MAE={with_ga['abs_error'].mean():.2f}, Bias={with_ga['error'].mean():+.2f}, n={len(with_ga)}")
    if len(without_ga) > 50:
        print(f"  Without computed GA: MAE={without_ga['abs_error'].mean():.2f}, Bias={without_ga['error'].mean():+.2f}, n={len(without_ga)}")

    # GA value vs meeting error correlation
    meeting_ga = oos.groupby("meeting_id").agg(
        mean_error=("error", "mean"),
        ga=("ga_value", "first"),
        going=("going", "first"),
        surface=("raceSurfaceName", "first"),
        n=("error", "count"),
    ).dropna()

    if len(meeting_ga) > 20:
        corr = meeting_ga["ga"].corr(meeting_ga["mean_error"])
        print(f"\n  GA vs meeting mean error correlation: {corr:.3f}")
        print(f"  (Positive = slower going → over-rated, Negative = slower → under-rated)")

        # Residual meeting error AFTER GA correction - is there a systematic GA scaling issue?
        # If we're using too much/too little GA, there'll be a correlation
        for surface in ["Turf", "All Weather"]:
            sub = meeting_ga[meeting_ga["surface"] == surface]
            if len(sub) > 20:
                c = sub["ga"].corr(sub["mean_error"])
                print(f"    {surface}: GA vs error corr = {c:.3f} (n={len(sub)} meetings)")


def analyse_year_drift(df):
    """Analyse if there's a systematic drift over time."""
    print("\n" + "=" * 80)
    print("4. TEMPORAL DRIFT ANALYSIS")
    print("=" * 80)

    df["year"] = df["source_year"]
    print(f"\n  {'Year':>6} {'N':>8} {'Bias':>7} {'MAE':>6} {'Turf Bias':>10} {'AW Bias':>8}")
    print(f"  {'─' * 55}")
    for yr in sorted(df["year"].unique()):
        sub = df[df["year"] == yr]
        turf = sub[sub["raceSurfaceName"] == "Turf"]
        aw = sub[sub["raceSurfaceName"] == "All Weather"]
        print(
            f"  {yr:>6} {len(sub):>8,} {sub['error'].mean():>+7.2f} {sub['abs_error'].mean():>6.2f} "
            f"{turf['error'].mean():>+10.2f} {aw['error'].mean():>+8.2f}"
        )

    # Trend test: is bias getting worse?
    yearly = df.groupby("year")["error"].mean()
    oos_years = yearly[yearly.index >= 2024]
    if len(oos_years) >= 2:
        slope = np.polyfit(oos_years.index, oos_years.values, 1)[0]
        print(f"\n  OOS bias trend (lbs/year): {slope:+.2f}")


def analyse_course_distance_interactions(df):
    """Find worst course x distance combos in OOS to identify standard time issues."""
    print("\n" + "=" * 80)
    print("5. WORST COURSE × DISTANCE COMBOS (OOS ONLY)")
    print("=" * 80)

    oos = df[df["source_year"] >= 2024].copy()
    oos["cd_key"] = oos["courseName"] + " " + oos["dist_round"].astype(str) + "f"

    cd_stats = oos.groupby("cd_key").agg(
        n=("error", "count"),
        bias=("error", "mean"),
        mae=("abs_error", "mean"),
        std=("error", "std"),
    )
    cd_stats = cd_stats[cd_stats["n"] >= 20].sort_values("mae", ascending=False)

    print(f"\n  {'Course×Dist':<35} {'N':>5} {'Bias':>7} {'MAE':>6} {'Std':>6}")
    print(f"  {'─' * 65}")
    for idx, row in cd_stats.head(30).iterrows():
        print(
            f"  {str(idx):<35} {int(row['n']):>5} {row['bias']:>+7.2f} "
            f"{row['mae']:>6.2f} {row['std']:>6.2f}"
        )


def simulate_distance_correction(df):
    """
    Test: what if we add per-distance bias corrections?
    Fit on 2015-2023, evaluate improvement on 2024-2026.
    """
    print("\n" + "=" * 80)
    print("6. SIMULATED DISTANCE BIAS CORRECTION")
    print("=" * 80)

    ins = df[df["source_year"] <= 2023].copy()
    oos = df[df["source_year"] >= 2024].copy()

    # Per-distance × surface bias on training data
    corrections = {}
    for surface in ["Turf", "All Weather"]:
        surf_ins = ins[ins["raceSurfaceName"] == surface]
        for dist in sorted(surf_ins["dist_round"].unique()):
            sub = surf_ins[surf_ins["dist_round"] == dist]
            if len(sub) >= 100:
                # Shrunk correction (regularised toward 0)
                k = 500  # shrinkage
                raw_bias = sub["error"].mean()
                n = len(sub)
                correction = raw_bias * n / (n + k)
                corrections[(surface, dist)] = correction

    # Apply corrections to OOS
    oos["correction"] = oos.apply(
        lambda r: corrections.get((r["raceSurfaceName"], r["dist_round"]), 0),
        axis=1,
    )
    oos["corrected_fig"] = oos["figure_calibrated"] - oos["correction"]
    oos["corrected_error"] = oos["corrected_fig"] - oos["timefigure"]

    print(f"\n  OOS before correction:")
    print(f"    MAE={oos['abs_error'].mean():.3f}, Bias={oos['error'].mean():+.3f}")
    print(f"\n  OOS after distance correction:")
    new_mae = oos["corrected_error"].abs().mean()
    new_bias = oos["corrected_error"].mean()
    print(f"    MAE={new_mae:.3f}, Bias={new_bias:+.3f}")
    print(f"    MAE change: {new_mae - oos['abs_error'].mean():+.3f}")

    # By surface
    for surface in ["Turf", "All Weather"]:
        sub = oos[oos["raceSurfaceName"] == surface]
        old_mae = sub["abs_error"].mean()
        new_mae = sub["corrected_error"].abs().mean()
        old_bias = sub["error"].mean()
        new_bias = sub["corrected_error"].mean()
        print(f"\n    {surface}:")
        print(f"      Before: MAE={old_mae:.3f}, Bias={old_bias:+.3f}")
        print(f"      After:  MAE={new_mae:.3f}, Bias={new_bias:+.3f}")

    # Show the corrections that would be applied
    print(f"\n  Distance corrections (from IS fit):")
    print(f"  {'Surface':<15} {'Dist':>5} {'Correction':>11} {'N':>6}")
    print(f"  {'─' * 42}")
    for (surface, dist), corr in sorted(corrections.items()):
        if abs(corr) > 0.1:
            n = len(ins[(ins["raceSurfaceName"] == surface) & (ins["dist_round"] == dist)])
            print(f"  {surface:<15} {dist:>5}f {corr:>+11.2f} {n:>6}")


def simulate_oos_bias_correction(df):
    """
    Test: what if we correct the overall OOS drift?
    Use 2024 to calibrate, 2025-2026 to validate.
    """
    print("\n" + "=" * 80)
    print("7. SIMULATED OOS BIAS DRIFT CORRECTION")
    print("=" * 80)

    # The OOS bias is +2.21. Can we learn this from early OOS data?
    cal_year = df[df["source_year"] == 2024].copy()
    val_years = df[df["source_year"] >= 2025].copy()

    if len(cal_year) < 100 or len(val_years) < 100:
        print("  Insufficient data for temporal drift test")
        return

    # Simple overall bias correction from 2024
    bias_2024 = cal_year["error"].mean()
    print(f"  2024 bias: {bias_2024:+.2f}")

    # Per-surface bias
    for surface in ["Turf", "All Weather"]:
        sub_cal = cal_year[cal_year["raceSurfaceName"] == surface]
        sub_val = val_years[val_years["raceSurfaceName"] == surface]
        if len(sub_cal) > 50 and len(sub_val) > 50:
            bias = sub_cal["error"].mean()
            val_before = sub_val["abs_error"].mean()
            sub_val["corrected"] = sub_val["figure_calibrated"] - bias
            val_after = (sub_val["corrected"] - sub_val["timefigure"]).abs().mean()
            val_bias_before = sub_val["error"].mean()
            val_bias_after = (sub_val["corrected"] - sub_val["timefigure"]).mean()
            print(f"\n  {surface}:")
            print(f"    2024 bias (used as correction): {bias:+.2f}")
            print(f"    2025-26 before: MAE={val_before:.3f}, Bias={val_bias_before:+.3f}")
            print(f"    2025-26 after:  MAE={val_after:.3f}, Bias={val_bias_after:+.3f}")


def simulate_combined_corrections(df):
    """
    Test: combine distance + temporal bias corrections.
    Fit all corrections on 2015-2023 + 2024 drift, evaluate on 2025-2026.
    """
    print("\n" + "=" * 80)
    print("8. COMBINED CORRECTIONS (distance + temporal + going bias)")
    print("=" * 80)

    ins = df[df["source_year"] <= 2023].copy()
    cal = df[df["source_year"] == 2024].copy()
    val = df[df["source_year"] >= 2025].copy()

    if len(val) < 100:
        print("  Insufficient validation data")
        return

    # 1. Distance corrections (from IS)
    dist_corrections = {}
    for surface in ["Turf", "All Weather"]:
        surf_ins = ins[ins["raceSurfaceName"] == surface]
        for dist in sorted(surf_ins["dist_round"].unique()):
            sub = surf_ins[surf_ins["dist_round"] == dist]
            if len(sub) >= 100:
                k = 500
                raw_bias = sub["error"].mean()
                n = len(sub)
                dist_corrections[(surface, dist)] = raw_bias * n / (n + k)

    # 2. Temporal drift (from 2024)
    temporal_corrections = {}
    for surface in ["Turf", "All Weather"]:
        sub = cal[cal["raceSurfaceName"] == surface]
        if len(sub) > 50:
            # Correct distance first, then measure residual temporal drift
            sub_corr = sub.copy()
            sub_corr["dist_adj"] = sub_corr.apply(
                lambda r: dist_corrections.get((r["raceSurfaceName"], r["dist_round"]), 0),
                axis=1,
            )
            sub_corr["residual"] = sub_corr["error"] - sub_corr["dist_adj"]
            temporal_corrections[surface] = sub_corr["residual"].mean()

    # 3. Going-group bias residual (from IS, after distance correction)
    going_corrections = {}
    going_map = {
        "Hard": "Firm", "Firm": "Firm", "Fast": "Firm",
        "Gd/Frm": "GdFm", "Good To Firm": "GdFm", "Good to Firm": "GdFm", "Std/Fast": "GdFm",
        "Good": "Good", "Standard": "Good", "Std": "Good",
        "Gd/Sft": "GdSft", "Good to Soft": "GdSft", "Good To Yielding": "GdSft",
        "Good to Yielding": "GdSft", "Std/Slow": "GdSft", "Standard/Slow": "GdSft",
        "Standard To Slow": "GdSft", "Standard to Slow": "GdSft", "Slow": "GdSft",
        "Soft": "Soft", "Yielding": "Soft", "Yld/Sft": "Soft", "Sft/Hvy": "Soft", "Hvy/Sft": "Soft",
        "Heavy": "Heavy",
    }
    ins["going_group"] = ins["going"].map(going_map).fillna("Good")
    for surface in ["Turf", "All Weather"]:
        surf_ins = ins[ins["raceSurfaceName"] == surface]
        for gg in surf_ins["going_group"].unique():
            sub = surf_ins[surf_ins["going_group"] == gg]
            if len(sub) >= 200:
                # After distance correction residual
                sub_adj = sub.copy()
                sub_adj["dist_adj"] = sub_adj.apply(
                    lambda r: dist_corrections.get((r["raceSurfaceName"], r["dist_round"]), 0),
                    axis=1,
                )
                residual = (sub_adj["error"] - sub_adj["dist_adj"]).mean()
                k = 500
                going_corrections[(surface, gg)] = residual * len(sub) / (len(sub) + k)

    # Apply all corrections to validation set
    val = val.copy()
    val["going_group"] = val["going"].map(going_map).fillna("Good")
    val["dist_adj"] = val.apply(
        lambda r: dist_corrections.get((r["raceSurfaceName"], r["dist_round"]), 0),
        axis=1,
    )
    val["temporal_adj"] = val["raceSurfaceName"].map(temporal_corrections).fillna(0)
    val["going_adj"] = val.apply(
        lambda r: going_corrections.get((r["raceSurfaceName"], r["going_group"]), 0),
        axis=1,
    )
    val["total_adj"] = val["dist_adj"] + val["temporal_adj"] + val["going_adj"]
    val["corrected_fig"] = val["figure_calibrated"] - val["total_adj"]
    val["corrected_error"] = val["corrected_fig"] - val["timefigure"]

    print(f"\n  Validation set (2025-2026): {len(val):,} runners")
    print(f"\n  Before corrections:")
    print(f"    MAE={val['abs_error'].mean():.4f}, Bias={val['error'].mean():+.3f}")
    print(f"    Corr={val['figure_calibrated'].corr(val['timefigure']):.4f}")
    print(f"\n  After combined corrections:")
    new_mae = val["corrected_error"].abs().mean()
    new_bias = val["corrected_error"].mean()
    new_corr = val["corrected_fig"].corr(val["timefigure"])
    print(f"    MAE={new_mae:.4f}, Bias={new_bias:+.3f}")
    print(f"    Corr={new_corr:.4f}")
    print(f"    MAE change: {new_mae - val['abs_error'].mean():+.4f}")

    # By surface
    for surface in ["Turf", "All Weather"]:
        sub = val[val["raceSurfaceName"] == surface]
        if len(sub) > 50:
            old = sub["abs_error"].mean()
            new = sub["corrected_error"].abs().mean()
            old_b = sub["error"].mean()
            new_b = sub["corrected_error"].mean()
            print(f"\n  {surface}:")
            print(f"    Before: MAE={old:.3f}, Bias={old_b:+.3f}")
            print(f"    After:  MAE={new:.3f}, Bias={new_b:+.3f}")
            print(f"    Δ MAE: {new - old:+.3f}")

    # By distance band
    print(f"\n  By distance (validation):")
    for dband, (lo, hi) in [
        ("5-6f", (5, 6)), ("7-8f", (7, 8)), ("9-10f", (9, 10)),
        ("11-12f", (11, 12)), ("13-16f", (13, 16)), ("17f+", (17, 30)),
    ]:
        sub = val[(val["dist_round"] >= lo) & (val["dist_round"] <= hi)]
        if len(sub) > 30:
            old = sub["abs_error"].mean()
            new = sub["corrected_error"].abs().mean()
            print(f"    {dband:<8}: MAE {old:.2f} → {new:.2f} ({new - old:+.2f})")

    # By going group
    print(f"\n  By going group (validation):")
    for gg in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
        sub = val[val["going_group"] == gg]
        if len(sub) > 30:
            old = sub["abs_error"].mean()
            new = sub["corrected_error"].abs().mean()
            old_b = sub["error"].mean()
            new_b = sub["corrected_error"].mean()
            print(f"    {gg:<8}: MAE {old:.2f} → {new:.2f} ({new - old:+.2f}), Bias {old_b:+.2f} → {new_b:+.2f}")

    # Print the correction values
    print(f"\n  Temporal corrections:")
    for surface, val_corr in temporal_corrections.items():
        print(f"    {surface}: {val_corr:+.2f}")

    print(f"\n  Distance corrections (|adj| > 0.2):")
    for (surface, dist), corr in sorted(dist_corrections.items()):
        if abs(corr) > 0.2:
            print(f"    {surface:<15} {dist:>3}f: {corr:+.2f}")

    print(f"\n  Going corrections (|adj| > 0.1):")
    for (surface, gg), corr in sorted(going_corrections.items()):
        if abs(corr) > 0.1:
            print(f"    {surface:<15} {gg:<8}: {corr:+.2f}")


if __name__ == "__main__":
    df = load_data()
    analyse_meeting_level_bias(df)
    analyse_distance_bias(df)
    analyse_going_estimation_accuracy(df)
    analyse_year_drift(df)
    analyse_course_distance_interactions(df)
    simulate_distance_correction(df)
    simulate_oos_bias_correction(df)
    simulate_combined_corrections(df)
