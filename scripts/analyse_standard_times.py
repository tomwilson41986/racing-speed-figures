#!/usr/bin/env python3
"""
Analyse standard times methodology and test improvements.

Tests:
  1. Current standard time quality (residuals per course×dist)
  2. Race type filtering: handicaps-only vs all winners vs excluding maidens
  3. Excluding 2yo races from standard time compilation
  4. Outlier removal: trimming vs winsorizing vs IQR
  5. Class adjustment variants: constant vs varying vs none
  6. Sample size thresholds: 15 vs 20 vs 30
  7. Recency weighting: more recent winners weighted higher
  8. Standard time stability over years
"""

import os
import sys
import numpy as np
import pandas as pd
from scipy import stats

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
SRC_DIR = os.path.join(BASE_DIR, "src")
sys.path.insert(0, SRC_DIR)

from speed_figures import (
    GOOD_GOING,
    MIN_RACES_STANDARD_TIME,
    CLASS_ADJUSTMENT_PER_MILE,
    compute_class_adjustment,
    filter_uk_ire_flat,
    apply_surface_change_cutoffs,
    load_data,
)

# ─── Load data ────────────────────────────────────────────────────────

print("=" * 70)
print("STANDARD TIMES ANALYSIS")
print("=" * 70)

df = load_data()
df = filter_uk_ire_flat(df)
df = apply_surface_change_cutoffs(df)

# Load existing output for comparison
fig_df = pd.read_csv(os.path.join(OUTPUT_DIR, "speed_figures.csv"))
std_df = pd.read_csv(os.path.join(OUTPUT_DIR, "standard_times.csv"))

# ─── 1: Current standard time quality ────────────────────────────────

print("\n" + "=" * 70)
print("1. CURRENT STANDARD TIMES OVERVIEW")
print("=" * 70)

print(f"\n  Standard times: {len(std_df)}")
print(f"  Median sample size: {std_df['n_races'].median():.0f}")
print(f"  Min sample size: {std_df['n_races'].min()}")
print(f"  Max sample size: {std_df['n_races'].max()}")

# Distribution by sample size
print("\n  Sample size distribution:")
for threshold in [15, 20, 30, 50, 100, 200]:
    n = (std_df["n_races"] >= threshold).sum()
    print(f"    ≥{threshold:>3}: {n:>4} combos ({100*n/len(std_df):.0f}%)")

# Which combos have fewest races?
print("\n  Combos with fewest races (potential reliability issues):")
smallest = std_df.nsmallest(10, "n_races")
for _, row in smallest.iterrows():
    print(f"    {row['std_key']:<40} n={row['n_races']:>3}  "
          f"median={row['median_time']:.2f}s")


# ─── 2: Race type analysis ───────────────────────────────────────────

print("\n" + "=" * 70)
print("2. RACE TYPE ANALYSIS FOR STANDARD TIMES")
print("=" * 70)

winners = df[df["positionOfficial"] == 1].copy()

# Classify race types from raceCode
winners["race_type"] = "other"
winners.loc[winners["raceCode"].str.startswith("I", na=False), "race_type"] = "handicap"
winners.loc[winners["raceCode"].str.startswith("P", na=False) |
            winners["raceCode"].str.startswith("S", na=False), "race_type"] = "maiden"
winners.loc[winners["raceCode"].str.startswith("G", na=False) |
            winners["raceCode"].str.startswith("L", na=False), "race_type"] = "group_listed"
winners.loc[winners["raceCode"].str.startswith("E", na=False), "race_type"] = "claimer"

print(f"\n  Winner breakdown by race type:")
for rt in ["handicap", "maiden", "group_listed", "claimer", "other"]:
    n = (winners["race_type"] == rt).sum()
    print(f"    {rt:<15} {n:>7,} ({100*n/len(winners):.1f}%)")

# Check if 2yo races exist
if "eligibilityagemin" in winners.columns:
    two_yo = winners[winners["eligibilityagemin"] == 2]
    older = winners[winners["eligibilityagemin"] >= 3]
    print(f"\n  Age breakdown:")
    print(f"    2yo eligible:  {len(two_yo):>7,} ({100*len(two_yo)/len(winners):.1f}%)")
    print(f"    3yo+ only:     {len(older):>7,} ({100*len(older)/len(winners):.1f}%)")

# Compute good-going winners only
winners_good = winners[winners["going"].isin(GOOD_GOING)].copy()
print(f"\n  Good going winners: {len(winners_good):,} of {len(winners):,} "
      f"({100*len(winners_good)/len(winners):.1f}%)")

# ─── Compare standard times under different race selections ──────────

print("\n" + "=" * 70)
print("3. STANDARD TIMES UNDER DIFFERENT RACE SELECTIONS")
print("=" * 70)
print("  Comparing median times for each course×distance using different")
print("  winner subsets. If handicap-only or excluding-maidens gives")
print("  different standard times, that tells us about selection bias.\n")

# Class adjustment (constant baseline — matching current pipeline)
winners_good["class_adj"] = winners_good.apply(
    lambda r: compute_class_adjustment(r.get("raceClass", 4), r["distance"]),
    axis=1,
)
winners_good["adj_time"] = winners_good["finishingTime"] - winners_good["class_adj"]

# Compute standard times under different selections
selections = {
    "all_winners": winners_good,
    "handicap_only": winners_good[winners_good["race_type"] == "handicap"],
    "no_maidens": winners_good[winners_good["race_type"] != "maiden"],
    "hcp_grp_only": winners_good[winners_good["race_type"].isin(["handicap", "group_listed"])],
    "no_2yo": winners_good[winners_good["eligibilityagemin"] >= 3]
        if "eligibilityagemin" in winners_good.columns else winners_good,
    "3yo+_hcp": winners_good[
        (winners_good["race_type"] == "handicap") &
        (winners_good["eligibilityagemin"] >= 3)
    ] if "eligibilityagemin" in winners_good.columns else winners_good[
        winners_good["race_type"] == "handicap"
    ],
}

std_times_by_sel = {}
for name, subset in selections.items():
    agg = (
        subset.groupby("std_key")
        .agg(
            median_time=("adj_time", "median"),
            mean_time=("adj_time", "mean"),
            n_races=("adj_time", "count"),
        )
        .reset_index()
    )
    valid = agg[agg["n_races"] >= MIN_RACES_STANDARD_TIME]
    std_times_by_sel[name] = dict(zip(valid["std_key"], valid["median_time"]))
    print(f"  {name:<20} {len(valid):>4} combos, "
          f"median n_races={agg['n_races'].median():.0f}")

# Compare each selection to "all_winners" baseline
base = std_times_by_sel["all_winners"]
print("\n  Deviation from all-winners standard (seconds):")
print(f"  {'Selection':<20} {'Common':>6} {'Mean Δ':>8} {'MAD':>8} {'Max |Δ|':>8}")
for name, times in std_times_by_sel.items():
    if name == "all_winners":
        continue
    common = set(base.keys()) & set(times.keys())
    if len(common) < 10:
        print(f"  {name:<20} {len(common):>6}  too few common combos")
        continue
    diffs = [times[k] - base[k] for k in common]
    mean_d = np.mean(diffs)
    mad = np.mean(np.abs(diffs))
    max_d = max(abs(d) for d in diffs)
    print(f"  {name:<20} {len(common):>6} {mean_d:>+8.3f} {mad:>8.3f} {max_d:>8.3f}")


# ─── 4: Test race-type filtering impact on figure accuracy ──────────

print("\n" + "=" * 70)
print("4. IMPACT ON FIGURE ACCURACY (simulated)")
print("=" * 70)
print("  For each selection, recompute winner deviation using that")
print("  selection's standard times, then correlate with timefigure.\n")

# Use the output figures to test: we can simulate what happens if the
# standard time was different by adjusting the deviation.
valid_figs = fig_df[
    fig_df["timefigure"].notna()
    & (fig_df["timefigure"] != 0)
    & fig_df["timefigure"].between(-200, 200)
    & fig_df["figure_calibrated"].notna()
    & (fig_df["positionOfficial"] == 1)
].copy()

# Get current std_key
valid_figs["std_key"] = (
    valid_figs["courseName"].astype(str) + "_" +
    valid_figs["distance"].round(0).astype(str) + "_" +
    valid_figs["raceSurfaceName"].astype(str)
)

current_std = dict(zip(std_df["std_key"], std_df["median_time"]))
valid_figs["current_std"] = valid_figs["std_key"].map(current_std)
valid_figs = valid_figs[valid_figs["current_std"].notna()].copy()

print(f"  Winners with valid timefigure: {len(valid_figs):,}")

for name, alt_std in std_times_by_sel.items():
    valid_figs[f"alt_std_{name}"] = valid_figs["std_key"].map(alt_std)
    # Change in standard time → change in figure
    valid_figs[f"delta_std_{name}"] = (
        valid_figs["current_std"] - valid_figs[f"alt_std_{name}"]
    ).fillna(0)

# For each selection, the adjusted figure would be:
# new_figure = current_figure + delta_std * lpl_conversion
# Since this is a proportional adjustment, we can look at correlation
# of the delta with the residual to see if it would help
print(f"  {'Selection':<20} {'Corr(Δ,resid)':>14} {'Mean Δ':>8}")
valid_figs["residual"] = valid_figs["figure_calibrated"] - valid_figs["timefigure"]
for name in std_times_by_sel:
    delta = valid_figs[f"delta_std_{name}"]
    corr = delta.corr(valid_figs["residual"])
    mean_d = delta.mean()
    print(f"  {name:<20} {corr:>14.4f} {mean_d:>+8.3f}")


# ─── 5: Outlier analysis ─────────────────────────────────────────────

print("\n" + "=" * 70)
print("5. OUTLIER ANALYSIS IN WINNING TIMES")
print("=" * 70)

# For each std_key, compute how much the mean differs from median
# Large gaps indicate outlier sensitivity
winners_good_with_key = winners_good.copy()
key_stats = (
    winners_good_with_key.groupby("std_key")["adj_time"]
    .agg(["median", "mean", "std", "count", "min", "max"])
    .reset_index()
)
key_stats = key_stats[key_stats["count"] >= 15]
key_stats["mean_median_diff"] = key_stats["mean"] - key_stats["median"]
key_stats["cv"] = key_stats["std"] / key_stats["median"]
key_stats["range_ratio"] = (key_stats["max"] - key_stats["min"]) / key_stats["median"]

print(f"\n  Combos analysed: {len(key_stats)}")
print(f"  Mean-median difference: {key_stats['mean_median_diff'].mean():+.3f}s "
      f"(std: {key_stats['mean_median_diff'].std():.3f}s)")
print(f"  Coefficient of variation: {key_stats['cv'].mean():.4f} "
      f"(std: {key_stats['cv'].std():.4f})")
print(f"  Range ratio (max-min)/median: {key_stats['range_ratio'].mean():.4f}")

# Combos with high outlier impact
print("\n  Combos where mean-median gap is largest (outlier-sensitive):")
high_diff = key_stats.nlargest(10, "mean_median_diff")
for _, row in high_diff.iterrows():
    print(f"    {row['std_key']:<40} gap={row['mean_median_diff']:>+.3f}s  "
          f"n={row['count']:>3}  CV={row['cv']:.4f}")

# Test trimmed mean vs median vs winsorized mean
print("\n  Central tendency comparison (across all combos):")

def trimmed_mean(vals, pct=0.1):
    """Trim top/bottom pct of values before taking mean."""
    n = len(vals)
    k = max(1, int(n * pct))
    s = sorted(vals)
    return np.mean(s[k:-k]) if 2*k < n else np.mean(s)

def winsorized_mean(vals, pct=0.1):
    """Replace top/bottom pct with adjacent values, then mean."""
    n = len(vals)
    k = max(1, int(n * pct))
    s = sorted(vals)
    for i in range(k):
        s[i] = s[k]
        s[-(i+1)] = s[-(k+1)]
    return np.mean(s)

central_methods = {}
for name, func in [("median", np.median), ("mean", np.mean),
                    ("trimmed_10%", lambda v: trimmed_mean(v, 0.1)),
                    ("winsorized_10%", lambda v: winsorized_mean(v, 0.1))]:
    results = {}
    for key, grp in winners_good_with_key.groupby("std_key"):
        vals = grp["adj_time"].values
        if len(vals) >= 15:
            results[key] = func(vals)
    central_methods[name] = results

# Compare each method to current median
print(f"  {'Method':<20} {'Common':>6} {'vs Median MAD':>14} {'Max |Δ|':>8}")
for name, times in central_methods.items():
    common = set(central_methods["median"].keys()) & set(times.keys())
    diffs = [times[k] - central_methods["median"][k] for k in common]
    mad = np.mean(np.abs(diffs))
    max_d = max(abs(d) for d in diffs)
    print(f"  {name:<20} {len(common):>6} {mad:>14.4f} {max_d:>8.4f}")


# ─── 6: Class adjustment analysis ────────────────────────────────────

print("\n" + "=" * 70)
print("6. CLASS ADJUSTMENT IMPACT ON STANDARD TIMES")
print("=" * 70)
print("  Current: constant Class 4 baseline (varying adj hurt accuracy).")
print("  Testing whether the data supports this.\n")

# Only meaningful for GB data where raceClass exists
gb_winners = winners_good[winners_good["raceClass"].notna()].copy()
print(f"  GB winners with raceClass: {len(gb_winners):,}")

if len(gb_winners) > 1000:
    # Method 1: No class adjustment
    gb_winners["adj_none"] = gb_winners["finishingTime"]

    # Method 2: Constant (current — Class 4 baseline)
    gb_winners["adj_const"] = gb_winners["adj_time"]  # Already computed

    # Method 3: Varying class adjustment
    gb_winners["adj_varying"] = gb_winners.apply(
        lambda r: r["finishingTime"] - (
            CLASS_ADJUSTMENT_PER_MILE.get(str(int(r["raceClass"])), -7.2) *
            r["distance"] / 8.0
        ),
        axis=1,
    )

    # Compare standard times under each method
    for method_name, col in [("no_adjustment", "adj_none"),
                              ("constant_c4", "adj_const"),
                              ("varying_class", "adj_varying")]:
        agg = (
            gb_winners.groupby("std_key")[col]
            .agg(["median", "std", "count"])
            .reset_index()
        )
        agg = agg[agg["count"] >= 15]
        print(f"  {method_name:<20}: {len(agg)} combos, "
              f"median CV={agg['std'].divide(agg['median']).mean():.5f}")

    # Show what varying class adj does to specific combos
    print("\n  Varying vs constant class adj (per combo):")
    for method_col, method_name in [("adj_varying", "varying"), ("adj_none", "none")]:
        agg_alt = (
            gb_winners.groupby("std_key")[method_col]
            .median()
            .reset_index()
            .rename(columns={method_col: "alt_median"})
        )
        agg_base = (
            gb_winners.groupby("std_key")["adj_const"]
            .median()
            .reset_index()
            .rename(columns={"adj_const": "base_median"})
        )
        merged = agg_base.merge(agg_alt, on="std_key")
        merged["diff"] = merged["alt_median"] - merged["base_median"]
        print(f"    {method_name} vs constant: "
              f"mean diff={merged['diff'].mean():+.3f}s, "
              f"MAD={merged['diff'].abs().mean():.3f}s, "
              f"max={merged['diff'].abs().max():.3f}s")

    # What does varying class adj look like by class?
    print("\n  Mean adj_time by class (GB good-going winners):")
    print(f"    {'Class':>5} {'N':>6} {'Mean Raw':>10} {'Mean Const':>10} "
          f"{'Mean Varying':>12} {'Diff':>8}")
    for cls in sorted(gb_winners["raceClass"].dropna().unique()):
        sub = gb_winners[gb_winners["raceClass"] == cls]
        if len(sub) < 50:
            continue
        print(f"    {cls:>5.0f} {len(sub):>6} {sub['adj_none'].mean():>10.2f} "
              f"{sub['adj_const'].mean():>10.2f} {sub['adj_varying'].mean():>12.2f} "
              f"{sub['adj_varying'].mean() - sub['adj_const'].mean():>+8.2f}")


# ─── 7: Recency weighting ────────────────────────────────────────────

print("\n" + "=" * 70)
print("7. RECENCY ANALYSIS — DO STANDARD TIMES DRIFT?")
print("=" * 70)

# For each std_key, compute the median adj_time per year
# to see if standards drift over time
winners_good_with_key["year"] = winners_good_with_key["source_year"]

# Pick a few representative combos
popular_keys = (
    winners_good_with_key.groupby("std_key")["adj_time"]
    .count()
    .nlargest(15)
    .index
)

print("\n  Annual median adj_time for top combos (drift check):")
for key in popular_keys:
    sub = winners_good_with_key[winners_good_with_key["std_key"] == key]
    yearly = sub.groupby("year")["adj_time"].median()
    overall = sub["adj_time"].median()
    drift = yearly.iloc[-1] - yearly.iloc[0] if len(yearly) > 1 else 0
    trend = stats.linregress(yearly.index.astype(float), yearly.values)
    print(f"  {key:<40} overall={overall:.2f}  "
          f"slope={trend.slope:+.3f}s/yr  R²={trend.rvalue**2:.3f}  "
          f"n_yrs={len(yearly)}")

# Overall drift assessment
all_drifts = []
for key in std_df["std_key"]:
    sub = winners_good_with_key[winners_good_with_key["std_key"] == key]
    if len(sub) < 30:
        continue
    yearly = sub.groupby("year")["adj_time"].median()
    if len(yearly) >= 3:
        trend = stats.linregress(yearly.index.astype(float), yearly.values)
        all_drifts.append({
            "std_key": key,
            "slope": trend.slope,
            "r2": trend.rvalue**2,
            "n": len(sub),
        })

drift_df = pd.DataFrame(all_drifts)
print(f"\n  Overall drift analysis ({len(drift_df)} combos):")
print(f"    Mean slope: {drift_df['slope'].mean():+.4f} s/yr")
print(f"    Median slope: {drift_df['slope'].median():+.4f} s/yr")
print(f"    Combos with |slope| > 0.1 s/yr: {(drift_df['slope'].abs() > 0.1).sum()}")
print(f"    Combos with significant drift (R²>0.3): "
      f"{(drift_df['r2'] > 0.3).sum()}")

# Test weighted standard times (exponential decay)
print("\n  Testing recency-weighted standard times:")
half_lives = [2, 3, 5, 10, None]  # None = no weighting (current)
for hl in half_lives:
    weighted_stds = {}
    for key in std_df["std_key"]:
        sub = winners_good_with_key[winners_good_with_key["std_key"] == key]
        if len(sub) < 15:
            continue
        if hl is not None:
            max_year = sub["year"].max()
            weights = np.exp(-0.693 * (max_year - sub["year"]) / hl)
            # Weighted median approximation: weight × sort
            sorted_idx = sub["adj_time"].argsort()
            sorted_vals = sub["adj_time"].iloc[sorted_idx].values
            sorted_w = weights.iloc[sorted_idx].values
            cum_w = np.cumsum(sorted_w) / np.sum(sorted_w)
            idx = np.searchsorted(cum_w, 0.5)
            idx = min(idx, len(sorted_vals) - 1)
            weighted_stds[key] = sorted_vals[idx]
        else:
            weighted_stds[key] = sub["adj_time"].median()

    # Compare to current
    common = set(current_std.keys()) & set(weighted_stds.keys())
    if len(common) < 50:
        continue
    diffs = [weighted_stds[k] - current_std[k] for k in common]
    label = f"hl={hl}yr" if hl else "no_weight"
    print(f"    {label:<12}: MAD from current={np.mean(np.abs(diffs)):.3f}s, "
          f"mean Δ={np.mean(diffs):+.3f}s, combos={len(common)}")


# ─── 8: Sample size threshold ────────────────────────────────────────

print("\n" + "=" * 70)
print("8. MINIMUM SAMPLE SIZE IMPACT")
print("=" * 70)

# Current threshold is 15. Would raising it improve accuracy?
# Check: do combos with fewer races have worse residuals in the output?
fig_valid = fig_df[
    fig_df["timefigure"].notna()
    & (fig_df["timefigure"] != 0)
    & fig_df["timefigure"].between(-200, 200)
    & fig_df["figure_calibrated"].notna()
].copy()
fig_valid["residual"] = fig_valid["figure_calibrated"] - fig_valid["timefigure"]
fig_valid["abs_residual"] = fig_valid["residual"].abs()

fig_valid["std_key"] = (
    fig_valid["courseName"].astype(str) + "_" +
    fig_valid["distance"].round(0).astype(str) + "_" +
    fig_valid["raceSurfaceName"].astype(str)
)
std_n_map = dict(zip(std_df["std_key"], std_df["n_races"]))
fig_valid["std_n_races"] = fig_valid["std_key"].map(std_n_map)

print(f"\n  MAE by standard-time sample size:")
bins = [0, 15, 20, 30, 50, 100, 200, 500, 10000]
fig_valid["n_bin"] = pd.cut(fig_valid["std_n_races"], bins=bins)
print(f"  {'N Races':>12} {'MAE':>7} {'Bias':>7} {'RMSE':>7} {'N Runners':>10}")
for band, grp in fig_valid.groupby("n_bin", observed=True):
    if len(grp) < 100:
        continue
    mae = grp["abs_residual"].mean()
    bias = grp["residual"].mean()
    rmse = np.sqrt((grp["residual"] ** 2).mean())
    print(f"  {str(band):>12} {mae:>7.2f} {bias:>+7.2f} {rmse:>7.2f} {len(grp):>10,}")

# Would dropping low-n combos help?
for min_n in [15, 20, 30, 50]:
    sub = fig_valid[fig_valid["std_n_races"] >= min_n]
    mae = sub["abs_residual"].mean()
    corr = sub["figure_calibrated"].corr(sub["timefigure"])
    print(f"    min_n={min_n:>3}: MAE={mae:.3f}  r={corr:.4f}  "
          f"coverage={len(sub):,} ({100*len(sub)/len(fig_valid):.1f}%)")


# ─── Summary ──────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("SUMMARY & RECOMMENDATIONS")
print("=" * 70)
print("""
  Current setup:
  - All winners on good going (with all-going fallback)
  - Constant class adjustment (Class 4 baseline)
  - Median for central tendency
  - Min 15 races per combo
  - No race type filtering
  - No recency weighting
  - 413 standard times covering all course×distance×surface combos
""")
