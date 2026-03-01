#!/usr/bin/env python3
"""
Analyse going allowances and figure compression.

Part 1: Going Allowance Analysis
  - Current GA accuracy by going group
  - Per-race vs per-meeting GA comparison
  - Distance-dependent GA within meetings
  - GA by number of races on card
  - Time-based going vs official going description

Part 2: Compression Analysis
  - Residuals by figure quartile (are tails compressed?)
  - Calibrated vs Timeform at different rating levels
  - Scale compression diagnosis
"""

import os
import sys
import numpy as np
import pandas as pd
from scipy import stats

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
sys.path.insert(0, os.path.join(BASE_DIR, "src"))

# ─── Load data ────────────────────────────────────────────────────────

print("=" * 70)
print("GOING ALLOWANCE & COMPRESSION ANALYSIS")
print("=" * 70)

df = pd.read_csv(os.path.join(OUTPUT_DIR, "speed_figures.csv"))
std_df = pd.read_csv(os.path.join(OUTPUT_DIR, "standard_times.csv"))
ga_df = pd.read_csv(os.path.join(OUTPUT_DIR, "going_allowances.csv"))

print(f"  Loaded {len(df):,} runners, {len(std_df)} standard times, {len(ga_df):,} going allowances")

# Reconstruct meeting_id (same logic as speed_figures.py line 292)
df["meeting_id"] = (
    df["meetingDate"].astype(str) + "_" +
    df["courseName"].astype(str) + "_" +
    df["raceSurfaceName"].astype(str)
)

# Reconstruct dist_round and std_key
df["dist_round"] = (df["distance"] * 2).round(0) / 2
df["std_key"] = (
    df["courseName"].astype(str) + "_" +
    df["dist_round"].astype(str) + "_" +
    df["raceSurfaceName"].astype(str)
)

# Merge GA onto runners
ga_map = dict(zip(ga_df["meeting_id"], ga_df["going_allowance_spf"]))
df["going_allowance"] = df["meeting_id"].map(ga_map)

valid = df[
    df["timefigure"].notna()
    & (df["timefigure"] != 0)
    & df["timefigure"].between(-200, 200)
    & df["figure_calibrated"].notna()
].copy()
valid["residual"] = valid["figure_calibrated"] - valid["timefigure"]
print(f"  Valid for evaluation: {len(valid):,}")

# Going groups
going_map = {}
for grp, goings in {
    "Firm": ["Hard", "Firm", "Fast"],
    "GdFm": ["Gd/Frm", "Good To Firm", "Good to Firm", "Std/Fast"],
    "Good": ["Good", "Standard", "Std"],
    "GdSft": ["Gd/Sft", "Good to Soft", "Good To Yielding",
              "Good to Yielding", "Std/Slow", "Standard/Slow",
              "Standard To Slow", "Standard to Slow", "Slow"],
    "Soft": ["Soft", "Yielding", "Yld/Sft", "Sft/Hvy", "Hvy/Sft"],
    "Heavy": ["Heavy"],
}.items():
    for g in goings:
        going_map[g] = grp

valid["going_group"] = valid["going"].map(going_map).fillna("Good")

# ══════════════════════════════════════════════════════════════════════
# PART 1: GOING ALLOWANCE ANALYSIS
# ══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("PART 1: GOING ALLOWANCE ANALYSIS")
print("=" * 70)

# ── 1.1: GA distribution by going description ─────────────────────────
print("\n" + "-" * 70)
print("1.1 GA DISTRIBUTION BY OFFICIAL GOING DESCRIPTION")
print("-" * 70)

has_ga = valid["going_allowance"].notna()
print(f"  Runners with GA: {has_ga.sum():,} / {len(valid):,}")

ga_by_going = valid[has_ga].groupby("going_group")["going_allowance"]
print(f"\n  {'Going':>8} {'Mean GA':>8} {'Std':>6} {'Min':>7} {'Max':>7} {'N':>8}")
for going_grp in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
    if going_grp in ga_by_going.groups:
        g = ga_by_going.get_group(going_grp)
        print(f"  {going_grp:>8} {g.mean():>+8.3f} {g.std():>6.3f} "
              f"{g.min():>7.3f} {g.max():>7.3f} {len(g):>8,}")

# ── 1.2: Does GA predict residuals? ──────────────────────────────────
print("\n" + "-" * 70)
print("1.2 RESIDUALS BY GA QUARTILE (is GA over/under-correcting?)")
print("-" * 70)

ga_valid = valid[has_ga].copy()
ga_valid["ga_quartile"] = pd.qcut(ga_valid["going_allowance"], 4,
                                   labels=["Q1 (fast)", "Q2", "Q3", "Q4 (slow)"])

print(f"  {'GA Quartile':>15} {'Mean GA':>8} {'Bias':>7} {'MAE':>7} {'RMSE':>7} {'N':>8}")
for q in ["Q1 (fast)", "Q2", "Q3", "Q4 (slow)"]:
    s = ga_valid[ga_valid["ga_quartile"] == q]
    bias = s["residual"].mean()
    mae = s["residual"].abs().mean()
    rmse = np.sqrt((s["residual"] ** 2).mean())
    ga_mean = s["going_allowance"].mean()
    print(f"  {q:>15} {ga_mean:>+8.3f} {bias:>+7.2f} {mae:>7.2f} {rmse:>7.2f} {len(s):>8,}")

# ── 1.3: Residuals by official going × surface ───────────────────────
print("\n" + "-" * 70)
print("1.3 RESIDUALS BY GOING GROUP × SURFACE")
print("-" * 70)

for surf in ["Turf", "All Weather"]:
    print(f"\n  {surf}:")
    print(f"  {'Going':>8} {'Bias':>7} {'MAE':>7} {'RMSE':>7} {'Corr':>7} {'N':>8}")
    sv = valid[valid["raceSurfaceName"] == surf]
    for going_grp in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
        s = sv[sv["going_group"] == going_grp]
        if len(s) < 100:
            continue
        bias = s["residual"].mean()
        mae = s["residual"].abs().mean()
        rmse = np.sqrt((s["residual"] ** 2).mean())
        corr = s["figure_calibrated"].corr(s["timefigure"])
        print(f"  {going_grp:>8} {bias:>+7.2f} {mae:>7.2f} {rmse:>7.2f} {corr:>7.4f} {len(s):>8,}")

# ── 1.4: Per-race GA vs per-meeting GA ────────────────────────────────
print("\n" + "-" * 70)
print("1.4 PER-RACE GA VS PER-MEETING GA")
print("-" * 70)

from speed_figures import compute_class_adjustment

# Build standard time lookup
std_map = dict(zip(std_df["std_key"], std_df["median_time"]))

winners_for_ga = valid[(valid["positionOfficial"] == 1) & has_ga].copy()
winners_for_ga["standard_time"] = winners_for_ga["std_key"].map(std_map)
winners_for_ga = winners_for_ga[winners_for_ga["standard_time"].notna()].copy()

winners_for_ga["class_adj"] = winners_for_ga.apply(
    lambda r: compute_class_adjustment(r["raceClass"], r["distance"]), axis=1
)
winners_for_ga["adj_time"] = winners_for_ga["finishingTime"] - winners_for_ga["class_adj"]
winners_for_ga["per_race_dev"] = (
    (winners_for_ga["adj_time"] - winners_for_ga["standard_time"])
    / winners_for_ga["distance"]
)
winners_for_ga["dev_from_meeting"] = (
    winners_for_ga["per_race_dev"] - winners_for_ga["going_allowance"]
)

print(f"  Winners with per-race GA: {len(winners_for_ga):,}")
print(f"  Per-race deviation std: {winners_for_ga['per_race_dev'].std():.4f} s/f")
print(f"  Meeting GA std:         {winners_for_ga['going_allowance'].std():.4f} s/f")
print(f"  Deviation from meeting: {winners_for_ga['dev_from_meeting'].std():.4f} s/f")

# How much of within-meeting variance is there?
meeting_grps = winners_for_ga.groupby("meeting_id")["per_race_dev"]
within_var = meeting_grps.apply(lambda x: x.var() if len(x) > 1 else np.nan).dropna()
between_var = winners_for_ga.groupby("meeting_id")["per_race_dev"].mean().var()
print(f"\n  Between-meeting variance: {between_var:.6f}")
print(f"  Mean within-meeting var: {within_var.mean():.6f}")
print(f"  Ratio (between/within): {between_var / within_var.mean():.2f}")
print(f"  → {'Meeting-level GA captures most variance' if between_var > within_var.mean() else 'Significant within-meeting variation — per-race GA may help'}")

# ── 1.5: Does GA vary by distance within a meeting? ──────────────────
print("\n" + "-" * 70)
print("1.5 WITHIN-MEETING GA VARIATION BY DISTANCE")
print("-" * 70)

# For meetings with both short and long races, check if GA differs
winners_for_ga["dist_group"] = pd.cut(
    winners_for_ga["distance"],
    bins=[0, 7, 10, 20],
    labels=["sprint (5-7f)", "middle (8-10f)", "long (11f+)"]
)

# Count meetings with multiple distance groups
meeting_dist = winners_for_ga.groupby("meeting_id")["dist_group"].nunique()
multi_dist = meeting_dist[meeting_dist >= 2].index

mw = winners_for_ga[winners_for_ga["meeting_id"].isin(multi_dist)]
print(f"  Meetings with multiple distance groups: {len(multi_dist):,}")

# Compute per-distance-group mean GA for each meeting
pivot = mw.groupby(["meeting_id", "dist_group"])["per_race_dev"].mean().unstack()
if "sprint (5-7f)" in pivot.columns and "long (11f+)" in pivot.columns:
    both = pivot.dropna(subset=["sprint (5-7f)", "long (11f+)"])
    diff = both["sprint (5-7f)"] - both["long (11f+)"]
    print(f"  Sprint vs Long GA difference:")
    print(f"    Mean: {diff.mean():+.4f} s/f")
    print(f"    Std:  {diff.std():.4f} s/f")
    print(f"    N:    {len(diff):,} meetings")
    if abs(diff.mean()) > 0.01:
        print(f"    → Sprints are {'faster' if diff.mean() < 0 else 'slower'} relative to standard")
    else:
        print(f"    → No significant distance-dependent going effect")

# Also check by surface
for surf in ["Turf"]:
    surf_w = winners_for_ga[
        (winners_for_ga["raceSurfaceName"] == surf) &
        winners_for_ga["meeting_id"].isin(multi_dist)
    ]
    if len(surf_w) < 100:
        continue
    pivot_s = surf_w.groupby(["meeting_id", "dist_group"])["per_race_dev"].mean().unstack()
    if "sprint (5-7f)" in pivot_s.columns and "long (11f+)" in pivot_s.columns:
        both_s = pivot_s.dropna(subset=["sprint (5-7f)", "long (11f+)"])
        diff_s = both_s["sprint (5-7f)"] - both_s["long (11f+)"]
        print(f"\n  {surf} only:")
        print(f"    Sprint vs Long: mean={diff_s.mean():+.4f}, std={diff_s.std():.4f}, N={len(diff_s)}")

# ── 1.6: Number of races on card impact ──────────────────────────────
print("\n" + "-" * 70)
print("1.6 GA RELIABILITY BY NUMBER OF RACES ON CARD")
print("-" * 70)

# Count winners per meeting used for GA
races_per_meeting = winners_for_ga.groupby("meeting_id").size()
winners_for_ga["races_on_card"] = winners_for_ga["meeting_id"].map(races_per_meeting)

# Merge onto all runners
card_size_map = races_per_meeting.to_dict()
ga_valid["races_on_card"] = ga_valid["meeting_id"].map(card_size_map)

bins = [0, 3, 4, 5, 6, 7, 100]
labels = ["1-3", "4", "5", "6", "7", "8+"]
ga_valid["card_bin"] = pd.cut(ga_valid["races_on_card"].fillna(0), bins=bins, labels=labels)

print(f"  {'Card Size':>10} {'Bias':>7} {'MAE':>7} {'RMSE':>7} {'N':>8}")
for b in labels:
    s = ga_valid[ga_valid["card_bin"] == b]
    if len(s) < 100:
        continue
    bias = s["residual"].mean()
    mae = s["residual"].abs().mean()
    rmse = np.sqrt((s["residual"] ** 2).mean())
    print(f"  {b:>10} {bias:>+7.2f} {mae:>7.2f} {rmse:>7.2f} {len(s):>8,}")

# ── 1.7: Time-based going vs official going ──────────────────────────
print("\n" + "-" * 70)
print("1.7 TIME-BASED GOING CLASSIFICATION")
print("-" * 70)
print("  Using GA to classify going into groups, compare with official.\n")

# Define time-based going groups from GA
ga_valid["time_going"] = pd.cut(
    ga_valid["going_allowance"],
    bins=[-np.inf, -0.15, -0.05, 0.05, 0.15, 0.30, np.inf],
    labels=["Fast", "GdFm", "Good", "GdSft", "Soft", "Heavy"],
)

# Cross-tabulate: official going × time-based going
print("  Official going × Time-based going (row %):")
print(f"  {'':>8}", end="")
for col in ["Fast", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
    print(f"  {col:>6}", end="")
print()

ct_pct = pd.crosstab(
    ga_valid["going_group"],
    ga_valid["time_going"],
    normalize="index"
) * 100
for idx in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
    if idx in ct_pct.index:
        row = "  " + f"{idx:>8}"
        for col in ["Fast", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
            if col in ct_pct.columns:
                row += f"  {ct_pct.loc[idx, col]:>5.1f}%"
            else:
                row += f"  {'0.0':>5}%"
        print(row)

# Agreement rate
agreement = (ga_valid["going_group"] == ga_valid["time_going"].astype(str)).mean()
print(f"\n  Exact agreement (official = time-based): {agreement:.1%}")

# Which is better at predicting residuals?
print("\n  Residual MAE using official vs time-based going groups:")
off_mae = ga_valid.groupby("going_group")["residual"].apply(
    lambda x: x.abs().mean()
).mean()
time_mae = ga_valid.groupby("time_going")["residual"].apply(
    lambda x: x.abs().mean()
).mean()
print(f"    Official going groups: {off_mae:.3f}")
print(f"    Time-based groups:    {time_mae:.3f}")

# ── 1.8: Per-race GA impact simulation ───────────────────────────────
print("\n" + "-" * 70)
print("1.8 SIMULATED PER-RACE GA: WOULD IT IMPROVE FIGURES?")
print("-" * 70)

# For races where we have a winner with a per-race dev, recompute
# raw figure using per-race GA instead of meeting GA
sim_winners = winners_for_ga.copy()
# Current figure uses meeting GA; difference is proportional to (meeting_GA - per_race_GA) × distance
sim_winners["ga_diff"] = sim_winners["going_allowance"] - sim_winners["per_race_dev"]
# The figure correction: going_correction = GA × distance × lbs_per_sec
# Changing GA by ga_diff changes figure by ga_diff × distance × lbs_per_sec

from speed_figures import LBS_PER_SECOND_5F
lps_base = 0.2 * LBS_PER_SECOND_5F  # lps at 5f = 4.0

sim_winners["lpl"] = lps_base * 5.0 / sim_winners["distance"]
sim_winners["fig_adj"] = sim_winners["ga_diff"] * sim_winners["distance"] * lps_base * 5.0 / sim_winners["distance"]
sim_winners["sim_figure"] = sim_winners["figure_calibrated"] + sim_winners["fig_adj"]
sim_winners["sim_residual"] = sim_winners["sim_figure"] - sim_winners["timefigure"]

curr_mae = sim_winners["residual"].abs().mean()
sim_mae = sim_winners["sim_residual"].abs().mean()
curr_rmse = np.sqrt((sim_winners["residual"] ** 2).mean())
sim_rmse = np.sqrt((sim_winners["sim_residual"] ** 2).mean())

print(f"  Winners only (N={len(sim_winners):,}):")
print(f"    Current (meeting GA):  MAE={curr_mae:.3f}, RMSE={curr_rmse:.3f}")
print(f"    Simulated (race GA):   MAE={sim_mae:.3f}, RMSE={sim_rmse:.3f}")
print(f"    Change:                MAE={sim_mae - curr_mae:+.3f}, RMSE={sim_rmse - curr_rmse:+.3f}")

# ══════════════════════════════════════════════════════════════════════
# PART 2: COMPRESSION ANALYSIS
# ══════════════════════════════════════════════════════════════════════

print("\n\n" + "=" * 70)
print("PART 2: COMPRESSION ANALYSIS")
print("=" * 70)
print(f"  Our Std={valid['figure_calibrated'].std():.1f} vs Timeform Std={valid['timefigure'].std():.1f}\n")

# ── 2.1: Residuals by figure decile ─────────────────────────────────
print("-" * 70)
print("2.1 RESIDUALS BY FIGURE DECILE")
print("-" * 70)

valid["fig_decile"] = pd.qcut(
    valid["figure_calibrated"], 10,
    labels=[f"D{i}" for i in range(1, 11)]
)

print(f"  {'Decile':>8} {'Our Mean':>9} {'TF Mean':>8} {'Bias':>7} {'MAE':>7} {'N':>8}")
for d in [f"D{i}" for i in range(1, 11)]:
    s = valid[valid["fig_decile"] == d]
    our_mean = s["figure_calibrated"].mean()
    tf_mean = s["timefigure"].mean()
    bias = s["residual"].mean()
    mae = s["residual"].abs().mean()
    print(f"  {d:>8} {our_mean:>9.1f} {tf_mean:>8.1f} {bias:>+7.2f} {mae:>7.2f} {len(s):>8,}")

# ── 2.2: Residuals by Timeform rating level ──────────────────────────
print("\n" + "-" * 70)
print("2.2 RESIDUALS BY TIMEFORM RATING LEVEL")
print("-" * 70)

valid["tf_band"] = pd.cut(
    valid["timefigure"],
    bins=[-100, 0, 20, 40, 60, 80, 100, 120, 200],
    labels=["<0", "0-20", "20-40", "40-60", "60-80", "80-100", "100-120", "120+"]
)

print(f"  {'TF Band':>10} {'Our Mean':>9} {'TF Mean':>8} {'Diff':>7} {'Bias':>7} {'MAE':>7} {'N':>8}")
for band in ["<0", "0-20", "20-40", "40-60", "60-80", "80-100", "100-120", "120+"]:
    s = valid[valid["tf_band"] == band]
    if len(s) < 50:
        continue
    our_mean = s["figure_calibrated"].mean()
    tf_mean = s["timefigure"].mean()
    diff = our_mean - tf_mean
    bias = s["residual"].mean()
    mae = s["residual"].abs().mean()
    print(f"  {band:>10} {our_mean:>9.1f} {tf_mean:>8.1f} {diff:>+7.1f} {bias:>+7.2f} {mae:>7.2f} {len(s):>8,}")

# ── 2.3: Pairwise slope analysis by figure level ─────────────────────
print("\n" + "-" * 70)
print("2.3 COMPRESSION: CALIBRATION SLOPE BY FIGURE RANGE")
print("-" * 70)

# Split into bands and compute local slope
for surf in ["Turf", "All Weather"]:
    sv = valid[valid["raceSurfaceName"] == surf]
    print(f"\n  {surf}:")
    print(f"  {'Figure Range':>15} {'Local Slope':>12} {'R²':>7} {'N':>8}")
    bands = [(0, 40), (40, 60), (60, 80), (80, 100), (100, 120), (120, 200)]
    for lo, hi in bands:
        s = sv[sv["figure_calibrated"].between(lo, hi)]
        if len(s) < 200:
            continue
        slope, inter, r, _, _ = stats.linregress(
            s["figure_calibrated"], s["timefigure"]
        )
        print(f"  {f'{lo}-{hi}':>15} {slope:>12.4f} {r**2:>7.4f} {len(s):>8,}")

# ── 2.4: Figure at extremes ──────────────────────────────────────────
print("\n" + "-" * 70)
print("2.4 EXTREME PERFORMANCE: TOP AND BOTTOM FIGURES")
print("-" * 70)

# Top performers
top = valid.nlargest(500, "timefigure")
top_our = top["figure_calibrated"].mean()
top_tf = top["timefigure"].mean()
print(f"  Top 500 by Timeform: TF mean={top_tf:.1f}, Our mean={top_our:.1f}, "
      f"gap={top_our - top_tf:+.1f}")

# Bottom performers
bot = valid.nsmallest(500, "timefigure")
bot_our = bot["figure_calibrated"].mean()
bot_tf = bot["timefigure"].mean()
print(f"  Bottom 500 by TF:    TF mean={bot_tf:.1f}, Our mean={bot_our:.1f}, "
      f"gap={bot_our - bot_tf:+.1f}")

# Middle
mid = valid[(valid["timefigure"].between(45, 55))]
mid_our = mid["figure_calibrated"].mean()
mid_tf = mid["timefigure"].mean()
print(f"  Middle (TF 45-55):   TF mean={mid_tf:.1f}, Our mean={mid_our:.1f}, "
      f"gap={mid_our - mid_tf:+.1f}")

print(f"\n  Compression ratio at extremes:")
print(f"    Top:    Our range / TF range = "
      f"{top['figure_calibrated'].std():.1f} / {top['timefigure'].std():.1f} = "
      f"{top['figure_calibrated'].std() / top['timefigure'].std():.3f}")
print(f"    Bottom: Our range / TF range = "
      f"{bot['figure_calibrated'].std():.1f} / {bot['timefigure'].std():.1f} = "
      f"{bot['figure_calibrated'].std() / bot['timefigure'].std():.3f}")

# ── 2.5: What drives the compression? ────────────────────────────────
print("\n" + "-" * 70)
print("2.5 COMPRESSION BY RACE CLASS")
print("-" * 70)

print(f"  {'Class':>6} {'Our Mean':>9} {'TF Mean':>8} {'Diff':>7} {'Our Std':>8} {'TF Std':>7} {'Ratio':>6} {'N':>8}")
for cls in sorted(valid["raceClass"].dropna().unique()):
    s = valid[valid["raceClass"] == cls]
    if len(s) < 500:
        continue
    our_m = s["figure_calibrated"].mean()
    tf_m = s["timefigure"].mean()
    our_s = s["figure_calibrated"].std()
    tf_s = s["timefigure"].std()
    ratio = our_s / tf_s if tf_s > 0 else 0
    print(f"  {cls:>6} {our_m:>9.1f} {tf_m:>8.1f} {our_m - tf_m:>+7.1f} "
          f"{our_s:>8.1f} {tf_s:>7.1f} {ratio:>6.3f} {len(s):>8,}")

# ── 2.6: Would quadratic/polynomial calibration help? ─────────────────
print("\n" + "-" * 70)
print("2.6 WOULD QUADRATIC CALIBRATION HELP?")
print("-" * 70)

for surf in ["Turf", "All Weather"]:
    sv = valid[valid["raceSurfaceName"] == surf].copy()
    x = sv["figure_calibrated"].values
    y = sv["timefigure"].values

    # Linear
    slope_lin, inter_lin, _, _, _ = stats.linregress(x, y)
    pred_lin = slope_lin * x + inter_lin
    mae_lin = np.mean(np.abs(y - pred_lin))

    # Quadratic
    x_c = x - x.mean()
    A = np.vstack([x, x_c**2, np.ones(len(x))]).T
    (a, a2, b), *_ = np.linalg.lstsq(A, y, rcond=None)
    pred_quad = a * x + a2 * x_c**2 + b
    mae_quad = np.mean(np.abs(y - pred_quad))

    print(f"\n  {surf}:")
    print(f"    Linear:    slope={slope_lin:.4f}, MAE={mae_lin:.3f}")
    print(f"    Quadratic: a={a:.4f}, a2={a2:.6f}, MAE={mae_quad:.3f}")
    print(f"    Improvement: MAE {mae_lin - mae_quad:+.3f}")

    # Show effect at extremes
    for fig_val in [20, 40, 60, 80, 100, 120]:
        lin_pred = slope_lin * fig_val + inter_lin
        quad_pred = a * fig_val + a2 * (fig_val - x.mean())**2 + b
        print(f"      At fig={fig_val}: linear→{lin_pred:.1f}, quad→{quad_pred:.1f}, diff={quad_pred-lin_pred:+.1f}")

# ── 2.7: Pre-calibration compression check ───────────────────────────
print("\n" + "-" * 70)
print("2.7 PRE-CALIBRATION VS POST-CALIBRATION COMPRESSION")
print("-" * 70)

if "figure_final" in valid.columns:
    pre = valid["figure_final"]
    post = valid["figure_calibrated"]
    tf = valid["timefigure"]

    pre_slope, _, _, _, _ = stats.linregress(pre, tf)
    post_slope, _, _, _, _ = stats.linregress(post, tf)

    print(f"  Pre-calibration  (figure_final):      std={pre.std():.1f}, slope vs TF={pre_slope:.4f}")
    print(f"  Post-calibration (figure_calibrated):  std={post.std():.1f}, slope vs TF={post_slope:.4f}")
    print(f"  Timeform:                              std={tf.std():.1f}")
    print(f"\n  Pre-cal spread ratio:  {pre.std() / tf.std():.3f}")
    print(f"  Post-cal spread ratio: {post.std() / tf.std():.3f}")

    # Check if calibration itself is compressing
    for surf in ["Turf", "All Weather"]:
        sv = valid[valid["raceSurfaceName"] == surf]
        pre_s = sv["figure_final"].std()
        post_s = sv["figure_calibrated"].std()
        tf_s = sv["timefigure"].std()
        print(f"\n  {surf}: pre={pre_s:.1f}, post={post_s:.1f}, TF={tf_s:.1f}")
        print(f"    Pre→Post change: {post_s - pre_s:+.1f} (calibration {'compresses' if post_s < pre_s else 'expands'})")


# ══════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

overall_bias = valid["residual"].mean()
overall_mae = valid["residual"].abs().mean()
print(f"\n  Overall: bias={overall_bias:+.2f}, MAE={overall_mae:.2f}")
print(f"  Std comparison: Ours={valid['figure_calibrated'].std():.1f}, "
      f"TF={valid['timefigure'].std():.1f}")
print(f"  Compression ratio: {valid['figure_calibrated'].std() / valid['timefigure'].std():.3f}")
