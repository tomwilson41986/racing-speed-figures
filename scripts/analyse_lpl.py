#!/usr/bin/env python3
"""
Analyse lbs-per-length (LPL) methodology and test improvements.

Tests:
  1. Current LPL accuracy by position (do beaten horses have higher residuals?)
  2. Going-dependent LPL (soft going → lower LPL, fast going → higher)
  3. Alternative base constants (22 vs 25 lbs/sec at 5f)
  4. Improved beaten-length attenuation curves
  5. Surface-specific LPL multiplier (fibresand ×0.8 per Rowlands)
  6. Empirical LPL derivation from re-opposing horses
"""

import os
import sys
import numpy as np
import pandas as pd
from scipy import stats

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
SRC_DIR = os.path.join(BASE_DIR, "src")
sys.path.insert(0, SRC_DIR)

from speed_figures import (
    generic_lbs_per_length,
    compute_course_lpl,
    SECONDS_PER_LENGTH,
    LBS_PER_SECOND_5F,
    BENCHMARK_FURLONGS,
    BASE_RATING,
)

# ─── Load data ────────────────────────────────────────────────────────

print("=" * 70)
print("LBS-PER-LENGTH ANALYSIS")
print("=" * 70)

df = pd.read_csv(os.path.join(OUTPUT_DIR, "speed_figures.csv"))
std_df = pd.read_csv(os.path.join(OUTPUT_DIR, "standard_times.csv"))
ga_df = pd.read_csv(os.path.join(OUTPUT_DIR, "going_allowances.csv"))

print(f"  Loaded {len(df):,} runners, {len(std_df)} standard times, {len(ga_df):,} going allowances")

# Filter to rows with valid timefigure for evaluation
valid = df[
    df["timefigure"].notna()
    & (df["timefigure"] != 0)
    & df["timefigure"].between(-200, 200)
    & df["figure_calibrated"].notna()
].copy()
print(f"  Valid for evaluation: {len(valid):,}")

# ─── Current LPL values ──────────────────────────────────────────────

print("\n" + "=" * 70)
print("1. CURRENT COURSE-SPECIFIC LPL DISTRIBUTION")
print("=" * 70)

lpl_dict = compute_course_lpl(std_df)

lpl_series = pd.Series(lpl_dict)
print(f"\n  LPL values computed: {len(lpl_series)}")
print(f"  Mean:   {lpl_series.mean():.3f}")
print(f"  Median: {lpl_series.median():.3f}")
print(f"  Std:    {lpl_series.std():.3f}")
print(f"  Min:    {lpl_series.min():.3f} ({lpl_series.idxmin()})")
print(f"  Max:    {lpl_series.max():.3f} ({lpl_series.idxmax()})")

# Show LPL by distance band
std_df_copy = std_df.copy()
std_df_copy["dist_band"] = std_df_copy["distance"].round(0).astype(int)
std_df_copy["course_lpl"] = std_df_copy["std_key"].map(lpl_dict)
std_df_copy["generic_lpl"] = std_df_copy["distance"].apply(generic_lbs_per_length)

print("\n  LPL by distance band:")
print(f"  {'Dist':>5} {'Generic':>8} {'Mean':>8} {'Std':>6} {'Min':>8} {'Max':>8} {'N':>4}")
for dist, grp in std_df_copy.groupby("dist_band"):
    g = grp["course_lpl"]
    gen = grp["generic_lpl"].iloc[0]
    print(f"  {dist:>5}f {gen:>8.3f} {g.mean():>8.3f} {g.std():>6.3f} {g.min():>8.3f} {g.max():>8.3f} {len(g):>4}")


# ─── Test 1: Residuals by finishing position ──────────────────────────

print("\n" + "=" * 70)
print("2. RESIDUALS BY FINISHING POSITION (LPL accuracy test)")
print("=" * 70)
print("  If LPL is correct, beaten horses should not have systematically")
print("  different residuals than winners.\n")

valid["residual"] = valid["figure_calibrated"] - valid["timefigure"]
valid["abs_residual"] = valid["residual"].abs()

print(f"  {'Pos':>4} {'MAE':>7} {'Bias':>7} {'RMSE':>7} {'Corr':>7} {'N':>8}")
for pos in range(1, 16):
    s = valid[valid["positionOfficial"] == pos]
    if len(s) < 100:
        continue
    mae = s["abs_residual"].mean()
    bias = s["residual"].mean()
    rmse = np.sqrt((s["residual"] ** 2).mean())
    corr = s["figure_calibrated"].corr(s["timefigure"])
    print(f"  {pos:>4} {mae:>7.2f} {bias:>+7.2f} {rmse:>7.2f} {corr:>7.4f} {len(s):>8,}")

# Group by beaten-length ranges
print("\n  By cumulative beaten lengths:")
valid["bl_band"] = pd.cut(
    valid["distanceCumulative"].fillna(0),
    bins=[0, 1, 2, 3, 5, 10, 15, 20, 100],
    labels=["0-1", "1-2", "2-3", "3-5", "5-10", "10-15", "15-20", "20+"],
    right=True,
)
print(f"  {'BL Range':>8} {'MAE':>7} {'Bias':>7} {'RMSE':>7} {'N':>8}")
for band, grp in valid.groupby("bl_band", observed=True):
    if len(grp) < 100:
        continue
    mae = grp["abs_residual"].mean()
    bias = grp["residual"].mean()
    rmse = np.sqrt((grp["residual"] ** 2).mean())
    print(f"  {band:>8} {mae:>7.2f} {bias:>+7.2f} {rmse:>7.2f} {len(grp):>8,}")


# ─── Test 2: Going-dependent LPL ─────────────────────────────────────

print("\n" + "=" * 70)
print("3. RESIDUALS BY GOING (testing if LPL should vary with going)")
print("=" * 70)
print("  If going affects LPL, we'd see systematic bias patterns.\n")

going_groups = {
    "Firm": ["Hard", "Firm", "Fast"],
    "GdFm": ["Gd/Frm", "Good To Firm", "Good to Firm", "Std/Fast"],
    "Good": ["Good", "Standard", "Std"],
    "GdSft": ["Gd/Sft", "Good to Soft", "Good To Yielding",
              "Good to Yielding", "Std/Slow", "Standard/Slow",
              "Standard To Slow", "Standard to Slow", "Slow"],
    "Soft": ["Soft", "Yielding", "Yld/Sft", "Sft/Hvy", "Hvy/Sft"],
    "Heavy": ["Heavy"],
}
going_map = {}
for grp, goings in going_groups.items():
    for g in goings:
        going_map[g] = grp

valid["going_group"] = valid["going"].map(going_map).fillna("Good")

# For beaten horses only (winners unaffected by LPL)
beaten = valid[valid["positionOfficial"] > 1].copy()

print("  BEATEN HORSES ONLY (positions 2+):")
print(f"  {'Going':>8} {'MAE':>7} {'Bias':>7} {'RMSE':>7} {'N':>8}")
for going_grp in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
    s = beaten[beaten["going_group"] == going_grp]
    if len(s) < 100:
        continue
    mae = s["abs_residual"].mean()
    bias = s["residual"].mean()
    rmse = np.sqrt((s["residual"] ** 2).mean())
    print(f"  {going_grp:>8} {mae:>7.2f} {bias:>+7.2f} {rmse:>7.2f} {len(s):>8,}")

# Cross-tabulate: going × beaten length range
print("\n  Going × beaten length BIAS (beaten horses):")
print(f"  {'Going':>8} {'0-3L':>7} {'3-5L':>7} {'5-10L':>7} {'10+L':>7}")
beaten["bl_coarse"] = pd.cut(
    beaten["distanceCumulative"].fillna(0),
    bins=[0, 3, 5, 10, 100],
    labels=["0-3", "3-5", "5-10", "10+"],
)
for going_grp in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
    vals = []
    for bl in ["0-3", "3-5", "5-10", "10+"]:
        s = beaten[(beaten["going_group"] == going_grp) & (beaten["bl_coarse"] == bl)]
        if len(s) >= 50:
            vals.append(f"{s['residual'].mean():>+7.2f}")
        else:
            vals.append(f"{'n/a':>7}")
    print(f"  {going_grp:>8} {'  '.join(vals)}")

# Winners only for comparison
print("\n  WINNERS ONLY (unaffected by LPL):")
winners = valid[valid["positionOfficial"] == 1]
print(f"  {'Going':>8} {'MAE':>7} {'Bias':>7} {'N':>8}")
for going_grp in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
    s = winners[winners["going_group"] == going_grp]
    if len(s) < 50:
        continue
    mae = s["abs_residual"].mean()
    bias = s["residual"].mean()
    print(f"  {going_grp:>8} {mae:>7.2f} {bias:>+7.2f} {len(s):>8,}")


# ─── Test 3: Surface-specific residuals ───────────────────────────────

print("\n" + "=" * 70)
print("4. SURFACE-SPECIFIC LPL ANALYSIS")
print("=" * 70)
print("  Rowlands suggests fibresand needs ×0.8 multiplier.\n")

print("  BEATEN HORSES by surface:")
print(f"  {'Surface':>20} {'MAE':>7} {'Bias':>7} {'N':>8}")
for surf in beaten["raceSurfaceName"].unique():
    s = beaten[beaten["raceSurfaceName"] == surf]
    if len(s) < 100:
        continue
    mae = s["abs_residual"].mean()
    bias = s["residual"].mean()
    print(f"  {surf:>20} {mae:>7.2f} {bias:>+7.2f} {len(s):>8,}")

# Check if specific AW tracks have different LPL accuracy
print("\n  AW beaten horses by course:")
aw_beaten = beaten[beaten["raceSurfaceName"] == "All Weather"]
print(f"  {'Course':>25} {'MAE':>7} {'Bias':>7} {'RMSE':>7} {'N':>6}")
for course in sorted(aw_beaten["courseName"].unique()):
    s = aw_beaten[aw_beaten["courseName"] == course]
    if len(s) < 200:
        continue
    mae = s["abs_residual"].mean()
    bias = s["residual"].mean()
    rmse = np.sqrt((s["residual"] ** 2).mean())
    print(f"  {course:>25} {mae:>7.2f} {bias:>+7.2f} {rmse:>7.2f} {len(s):>6,}")


# ─── Test 4: Beaten-length attenuation curves ────────────────────────

print("\n" + "=" * 70)
print("5. BEATEN-LENGTH ATTENUATION ANALYSIS")
print("=" * 70)
print("  Current: full precision ≤20L, halved beyond.")
print("  Testing alternative curves.\n")

# For each beaten-length range, compute what the "ideal" LPL multiplier
# would be to minimise residuals.
# The idea: if actual residual is positive (our figure too high),
# we're under-penalising beaten margins → LPL too low.

beaten_with_bl = beaten[
    beaten["distanceCumulative"].notna()
    & (beaten["distanceCumulative"] > 0.5)
].copy()

beaten_with_bl["std_key"] = (
    beaten_with_bl["courseName"] + "_" +
    beaten_with_bl["distance"].round(0).astype(str) + "_" +
    beaten_with_bl["raceSurfaceName"]
)

# Reconstruct the lbs_behind from known values
# raw_figure = winner_figure - lbs_behind
# We can back out: lbs_behind ≈ winner_figure - raw_figure
# But we don't have winner_figure in the CSV. Let's use what we have.

# Instead, look at residual vs beaten length to see if there's a trend
print("  Residual vs beaten length (all beaten horses):")
bl_fine_bins = [0, 0.5, 1, 2, 3, 5, 7, 10, 13, 16, 20, 25, 30, 50, 100]
beaten_with_bl["bl_fine"] = pd.cut(
    beaten_with_bl["distanceCumulative"],
    bins=bl_fine_bins,
)
print(f"  {'BL Range':>12} {'Mean Resid':>10} {'MAE':>7} {'N':>7}")
for band, grp in beaten_with_bl.groupby("bl_fine", observed=True):
    if len(grp) < 50:
        continue
    print(f"  {str(band):>12} {grp['residual'].mean():>+10.2f} {grp['abs_residual'].mean():>7.2f} {len(grp):>7,}")


# ─── Test 5: Empirical LPL from re-opposing horses ───────────────────

print("\n" + "=" * 70)
print("6. EMPIRICAL LPL FROM RE-OPPOSING HORSES")
print("=" * 70)
print("  Compare same-race figures with Timeform to find optimal LPL.\n")

# For each race, look at pairs of horses. If our LPL is correct, the
# *difference* between two horses' figures should match the difference
# in their Timeform timefigures.
#
# Specifically: for two horses in the same race,
#   our_diff = our_fig_A - our_fig_B
#   tf_diff  = tf_fig_A - tf_fig_B
#
# If LPL is too high, our_diff is too spread (we over-penalise margins).
# If LPL is too low, our_diff is too compressed.
#
# We can measure this as the regression slope: tf_diff = β × our_diff + ε
# If β > 1, our LPL is too low (we under-spread).
# If β < 1, our LPL is too high (we over-spread).

pair_data = valid[
    valid["figure_calibrated"].notna()
    & valid["timefigure"].notna()
    & valid["positionOfficial"].between(1, 10)
].copy()

# Sample pairs: for each race, compare winner vs each beaten horse
races_with_multiple = pair_data.groupby("race_id").filter(lambda x: len(x) >= 2)

# Build pairs: winner vs 2nd, winner vs 3rd, etc.
winners_df = races_with_multiple[races_with_multiple["positionOfficial"] == 1][
    ["race_id", "figure_calibrated", "timefigure", "raw_figure", "distance",
     "raceSurfaceName", "going", "distanceCumulative"]
].rename(columns={
    "figure_calibrated": "cal_w", "timefigure": "tf_w",
    "raw_figure": "raw_w",
}).drop_duplicates("race_id")

beaten_df = races_with_multiple[races_with_multiple["positionOfficial"] > 1][
    ["race_id", "positionOfficial", "figure_calibrated", "timefigure",
     "raw_figure", "distanceCumulative"]
].rename(columns={
    "figure_calibrated": "cal_b", "timefigure": "tf_b",
    "raw_figure": "raw_b", "distanceCumulative": "bl_beaten",
})

pairs = beaten_df.merge(winners_df[["race_id", "cal_w", "tf_w", "raw_w",
                                     "distance", "raceSurfaceName", "going"]],
                        on="race_id", how="inner")

pairs["our_diff"] = pairs["cal_w"] - pairs["cal_b"]
pairs["tf_diff"] = pairs["tf_w"] - pairs["tf_b"]
pairs["raw_diff"] = pairs["raw_w"] - pairs["raw_b"]

# Filter to reasonable cases
pairs = pairs[
    pairs["our_diff"].notna() & pairs["tf_diff"].notna()
    & pairs["our_diff"].between(-50, 80)
    & pairs["tf_diff"].between(-50, 80)
    & pairs["bl_beaten"].between(0.1, 20)
]

print(f"  Pairs analysed: {len(pairs):,}")

# Overall regression: tf_diff = β × our_diff
slope, intercept, r, p, se = stats.linregress(pairs["our_diff"], pairs["tf_diff"])
print(f"\n  Overall: tf_diff = {slope:.4f} × our_diff + {intercept:.2f}")
print(f"  R²={r**2:.4f}, p={p:.2e}")
print(f"  Interpretation: β={slope:.4f} → ", end="")
if slope > 1.02:
    print("our LPL is TOO LOW (we under-spread beaten horses)")
elif slope < 0.98:
    print("our LPL is TOO HIGH (we over-spread beaten horses)")
else:
    print("our LPL is approximately correct")

# Same analysis on raw figures (before calibration stretches things)
slope_r, intercept_r, r_r, _, _ = stats.linregress(
    pairs["raw_diff"], pairs["tf_diff"]
)
print(f"\n  Raw (pre-calibration): tf_diff = {slope_r:.4f} × raw_diff + {intercept_r:.2f}")
print(f"  R²={r_r**2:.4f}")

# By distance band
print(f"\n  By distance (calibrated):")
pairs["dist_band"] = pairs["distance"].round(0).astype(int)
print(f"  {'Dist':>5} {'Slope':>7} {'R²':>7} {'N':>7}")
for dist, grp in pairs.groupby("dist_band"):
    if len(grp) < 200:
        continue
    s, i, r2, _, _ = stats.linregress(grp["our_diff"], grp["tf_diff"])
    print(f"  {dist:>5}f {s:>7.4f} {r2**2:>7.4f} {len(grp):>7,}")

# By surface
print(f"\n  By surface (calibrated):")
for surf in pairs["raceSurfaceName"].unique():
    grp = pairs[pairs["raceSurfaceName"] == surf]
    if len(grp) < 500:
        continue
    s, i, r2, _, _ = stats.linregress(grp["our_diff"], grp["tf_diff"])
    print(f"  {surf:<20} slope={s:.4f}  R²={r2**2:.4f}  N={len(grp):,}")

# By going
pairs["going_group"] = pairs["going"].map(going_map).fillna("Good")
print(f"\n  By going (calibrated):")
for going_grp in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
    grp = pairs[pairs["going_group"] == going_grp]
    if len(grp) < 200:
        continue
    s, i, r2, _, _ = stats.linregress(grp["our_diff"], grp["tf_diff"])
    print(f"  {going_grp:<10} slope={s:.4f}  R²={r2**2:.4f}  N={len(grp):,}")

# By beaten-length band
print(f"\n  By beaten-length range (does LPL accuracy degrade with margin?):")
bl_bins = [0.1, 1, 2, 3, 5, 10, 20]
pairs["bl_band"] = pd.cut(pairs["bl_beaten"], bins=bl_bins)
print(f"  {'BL Range':>10} {'Slope':>7} {'R²':>7} {'N':>7}")
for band, grp in pairs.groupby("bl_band", observed=True):
    if len(grp) < 200:
        continue
    s, i, r2, _, _ = stats.linregress(grp["our_diff"], grp["tf_diff"])
    print(f"  {str(band):>10} {s:>7.4f} {r2**2:>7.4f} {len(grp):>7,}")


# ─── Test 6: Alternative LPL constants ───────────────────────────────

print("\n" + "=" * 70)
print("7. ALTERNATIVE BASE LPL CONSTANTS")
print("=" * 70)
print("  Testing different lbs/sec at 5f values.\n")

# Compute what the optimal lbs/sec would be to match Timeform pairwise
# For raw figures: raw_diff = (bl_beaten × lpl)
# We want: tf_diff ≈ calibration(raw_diff)
# But we can look at raw_diff vs tf_diff slope to infer ideal LPL scaling.

# The raw diff is dominated by the LPL × beaten_lengths term.
# We can directly test: for given beaten_lengths, what LPL makes
# (winner_raw - horse_raw) best predict (winner_tf - horse_tf)?

# Since winner_raw - horse_raw ≈ bl × lpl (approximately),
# and we want this to align with tf_diff after calibration,
# we can estimate the optimal multiplier on lpl.

# The slope from the pairwise regression tells us:
# If slope > 1 on calibrated → our final spread is too narrow
# The calibration slope (0.76 for turf, 0.88 for AW) already compresses,
# so the raw figures need to over-spread to compensate.

print(f"  Current: LBS_PER_SECOND_5F = {LBS_PER_SECOND_5F}")
print(f"  Current generic 5f LPL = {generic_lbs_per_length(5.0):.2f}")
print(f"  Current generic 8f LPL = {generic_lbs_per_length(8.0):.2f}")
print(f"  Current generic 12f LPL = {generic_lbs_per_length(12.0):.2f}")

for test_lbs_sec in [18, 20, 22, 24, 25, 26, 28]:
    lpl_5f = SECONDS_PER_LENGTH * test_lbs_sec
    lpl_8f = SECONDS_PER_LENGTH * test_lbs_sec * (5.0 / 8.0)
    lpl_12f = SECONDS_PER_LENGTH * test_lbs_sec * (5.0 / 12.0)
    print(f"  LBS/SEC={test_lbs_sec}: 5f={lpl_5f:.2f}  8f={lpl_8f:.2f}  12f={lpl_12f:.2f}")

# Timeform uses 25 lbs/sec at 60 seconds.
# At 5f (55-60 sec), this would be ~25 lbs/sec → LPL = 0.2 × 25 = 5.0
# Compare: our current 22 → LPL = 0.2 × 22 = 4.4
# If Rowlands says Timeform is 25-30% higher than BHA, and BHA is ~3 lbs/L
# at 5f, then Timeform uses ~3.75-3.9 lbs/L at 5f.  But our 4.4 is already
# above that.  The 22 lbs/sec figure likely already accounts for this.

print("\n  Note: The calibration layer (slope 0.76-0.88) compresses the")
print("  spread, so changing the base LPL would mostly be absorbed by")
print("  the calibration slope.  The key question is whether going-")
print("  dependent or position-dependent LPL reduces within-cell variance.")


# ─── Test 7: Going-adjusted LPL simulation ───────────────────────────

print("\n" + "=" * 70)
print("8. GOING-ADJUSTED LPL SIMULATION")
print("=" * 70)
print("  Estimate optimal going multiplier on LPL.\n")

# On soft going, horses bunch up → lengths represent more lbs.
# On firm going, horses spread out → lengths represent fewer lbs per length?
# Actually it's the opposite: faster surfaces → horses finish faster →
# a length represents less time → less weight equivalent.
#
# Wait: lpl = seconds_per_length × lbs_per_second.
# On soft going: times are slower, horses are more bunched.
# The margins (in lengths) between horses tend to be SMALLER on soft going,
# but each length might represent MORE ability difference (because it's
# harder to gain ground in deep going).
#
# The empirical test: do beaten horses on soft going have different residual
# patterns than on fast going?

# For beaten horses, compute the margin contribution to their figure:
# figure = winner_figure - bl × lpl
# If lpl should be higher on soft, then on soft beaten horses would
# have figures that are too HIGH (positive residual = over-rated).

# Group: going × position (2nd, 3rd-5th, 6th-10th)
beaten_groups = beaten.copy()
beaten_groups["pos_group"] = pd.cut(
    beaten_groups["positionOfficial"],
    bins=[1, 2, 5, 10, 100],
    labels=["2nd", "3rd-5th", "6th-10th", "11th+"],
)

print("  Residual by going × position group (beaten horses):")
print(f"  {'Going':>8} {'2nd':>8} {'3rd-5th':>8} {'6th-10th':>8} {'11th+':>8}")
for going_grp in ["Firm", "GdFm", "Good", "GdSft", "Soft", "Heavy"]:
    vals = []
    for pg in ["2nd", "3rd-5th", "6th-10th", "11th+"]:
        s = beaten_groups[
            (beaten_groups["going_group"] == going_grp) &
            (beaten_groups["pos_group"] == pg)
        ]
        if len(s) >= 50:
            vals.append(f"{s['residual'].mean():>+8.2f}")
        else:
            vals.append(f"{'n/a':>8}")
    print(f"  {going_grp:>8} {''.join(vals)}")

print("\n  If going-adjusted LPL is needed, we'd see beaten horses on")
print("  soft going systematically over- or under-rated relative to")
print("  beaten horses on firm going, especially for larger margins.")


# ─── Summary ──────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("SUMMARY & RECOMMENDATIONS")
print("=" * 70)

# Compute key metrics
winners_mae = valid[valid["positionOfficial"] == 1]["abs_residual"].mean()
beaten_mae = valid[valid["positionOfficial"] > 1]["abs_residual"].mean()
close_beaten = valid[
    (valid["positionOfficial"] > 1) &
    (valid["distanceCumulative"].fillna(0) <= 3)
]["abs_residual"].mean()
far_beaten = valid[
    (valid["positionOfficial"] > 1) &
    (valid["distanceCumulative"].fillna(0) > 10)
]["abs_residual"].mean()

print(f"\n  Winner MAE:            {winners_mae:.2f} lbs")
print(f"  Beaten MAE:            {beaten_mae:.2f} lbs")
print(f"  Close beaten (≤3L):    {close_beaten:.2f} lbs")
print(f"  Far beaten (>10L):     {far_beaten:.2f} lbs")
print(f"  Winner-beaten gap:     {beaten_mae - winners_mae:+.2f} lbs")
print(f"  Close-far gap:         {far_beaten - close_beaten:+.2f} lbs")
print(f"\n  Pairwise slope (calibrated): {slope:.4f}")
print(f"  Pairwise slope (raw):        {slope_r:.4f}")
