#!/usr/bin/env python3
"""
Analyse Weight-for-Age (WFA) methodology and test improvements.

Tests:
  1. Current WFA residuals by age × month × distance × surface
  2. Compare our empirical WFA to BHA official scale
  3. Non-monotonic distance issues in current tables
  4. Re-derive WFA tables from latest data (2015-2025)
  5. Test fortnightly vs monthly resolution
  6. Older horse decline validation
  7. 2yo early-season reliability
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
    WFA_3YO_TURF, WFA_3YO_AW, WFA_2YO_TURF, WFA_2YO_AW,
    OLDER_DECLINE_TURF,
    get_wfa_allowance,
)

# ─── Load data ────────────────────────────────────────────────────────

print("=" * 70)
print("WEIGHT-FOR-AGE ANALYSIS")
print("=" * 70)

df = pd.read_csv(os.path.join(OUTPUT_DIR, "speed_figures.csv"))
print(f"  Loaded {len(df):,} runners")

# Filter to valid rows
valid = df[
    df["timefigure"].notna()
    & (df["timefigure"] != 0)
    & df["timefigure"].between(-200, 200)
    & df["figure_calibrated"].notna()
    & df["horseAge"].notna()
].copy()
valid["residual"] = valid["figure_calibrated"] - valid["timefigure"]
valid["abs_residual"] = valid["residual"].abs()
valid["month"] = pd.to_datetime(valid["meetingDate"], errors="coerce").dt.month
valid["dist_round"] = valid["distance"].round(0).astype(int)

print(f"  Valid for evaluation: {len(valid):,}")

# ─── 1: Overall WFA accuracy by age ──────────────────────────────────

print("\n" + "=" * 70)
print("1. RESIDUALS BY AGE GROUP")
print("=" * 70)
print("  If WFA is correct, all age groups should have similar residuals.\n")

print(f"  {'Age':>4} {'MAE':>7} {'Bias':>7} {'RMSE':>7} {'Corr':>7} {'N':>9}")
for age in range(2, 13):
    s = valid[valid["horseAge"] == age]
    if len(s) < 500:
        continue
    mae = s["abs_residual"].mean()
    bias = s["residual"].mean()
    rmse = np.sqrt((s["residual"] ** 2).mean())
    corr = s["figure_calibrated"].corr(s["timefigure"])
    print(f"  {age:>4} {mae:>7.2f} {bias:>+7.2f} {rmse:>7.2f} {corr:>7.4f} {len(s):>9,}")


# ─── 2: 3yo residuals by month and distance ──────────────────────────

print("\n" + "=" * 70)
print("2. 3YO RESIDUALS BY MONTH × DISTANCE (Turf)")
print("=" * 70)
print("  Positive bias → WFA too high (we over-compensate).")
print("  Negative bias → WFA too low (we under-compensate).\n")

three_yo_turf = valid[
    (valid["horseAge"] == 3) &
    (valid["raceSurfaceName"] == "Turf")
]

dists_to_show = [5, 6, 7, 8, 10, 12, 14, 16]
print(f"  {'Month':>5} " + " ".join(f"{d:>6}f" for d in dists_to_show) + f" {'All':>7}")
for month in range(1, 13):
    vals = []
    for dist in dists_to_show:
        s = three_yo_turf[
            (three_yo_turf["month"] == month) &
            (three_yo_turf["dist_round"] == dist)
        ]
        if len(s) >= 30:
            vals.append(f"{s['residual'].mean():>+6.1f}")
        else:
            vals.append(f"{'':>6}")
    all_m = three_yo_turf[three_yo_turf["month"] == month]
    all_bias = f"{all_m['residual'].mean():>+7.2f}" if len(all_m) >= 50 else f"{'':>7}"
    print(f"  {month:>5} " + " ".join(vals) + f" {all_bias}")

# Same for AW
print(f"\n  3YO RESIDUALS BY MONTH × DISTANCE (All Weather)")
three_yo_aw = valid[
    (valid["horseAge"] == 3) &
    (valid["raceSurfaceName"] == "All Weather")
]

print(f"  {'Month':>5} " + " ".join(f"{d:>6}f" for d in dists_to_show[:6]) + f" {'All':>7}")
for month in range(1, 13):
    vals = []
    for dist in dists_to_show[:6]:
        s = three_yo_aw[
            (three_yo_aw["month"] == month) &
            (three_yo_aw["dist_round"] == dist)
        ]
        if len(s) >= 30:
            vals.append(f"{s['residual'].mean():>+6.1f}")
        else:
            vals.append(f"{'':>6}")
    all_m = three_yo_aw[three_yo_aw["month"] == month]
    all_bias = f"{all_m['residual'].mean():>+7.2f}" if len(all_m) >= 50 else f"{'':>7}"
    print(f"  {month:>5} " + " ".join(vals) + f" {all_bias}")


# ─── 3: 2yo residuals by month ───────────────────────────────────────

print("\n" + "=" * 70)
print("3. 2YO RESIDUALS BY MONTH × DISTANCE")
print("=" * 70)

two_yo_turf = valid[
    (valid["horseAge"] == 2) &
    (valid["raceSurfaceName"] == "Turf")
]

print(f"\n  2YO TURF:")
print(f"  {'Month':>5} " + " ".join(f"{d:>6}f" for d in [5, 6, 7, 8, 10]) + f" {'All':>7}")
for month in range(3, 13):
    vals = []
    for dist in [5, 6, 7, 8, 10]:
        s = two_yo_turf[
            (two_yo_turf["month"] == month) &
            (two_yo_turf["dist_round"] == dist)
        ]
        if len(s) >= 20:
            vals.append(f"{s['residual'].mean():>+6.1f}")
        else:
            vals.append(f"{'':>6}")
    all_m = two_yo_turf[two_yo_turf["month"] == month]
    all_bias = f"{all_m['residual'].mean():>+7.2f}" if len(all_m) >= 30 else f"{'':>7}"
    print(f"  {month:>5} " + " ".join(vals) + f" {all_bias}")

two_yo_aw = valid[
    (valid["horseAge"] == 2) &
    (valid["raceSurfaceName"] == "All Weather")
]
print(f"\n  2YO ALL WEATHER:")
print(f"  {'Month':>5} " + " ".join(f"{d:>6}f" for d in [5, 6, 7, 8]) + f" {'All':>7}")
for month in range(3, 13):
    vals = []
    for dist in [5, 6, 7, 8]:
        s = two_yo_aw[
            (two_yo_aw["month"] == month) &
            (two_yo_aw["dist_round"] == dist)
        ]
        if len(s) >= 20:
            vals.append(f"{s['residual'].mean():>+6.1f}")
        else:
            vals.append(f"{'':>6}")
    all_m = two_yo_aw[two_yo_aw["month"] == month]
    all_bias = f"{all_m['residual'].mean():>+7.2f}" if len(all_m) >= 20 else f"{'':>7}"
    print(f"  {month:>5} " + " ".join(vals) + f" {all_bias}")


# ─── 4: Compare our WFA to BHA scale ─────────────────────────────────

print("\n" + "=" * 70)
print("4. OUR WFA vs BHA OFFICIAL SCALE (3yo)")
print("=" * 70)

# BHA scale (approximate, from yumpu extract and BHA PDFs)
# These are half-month averages; I'll use the mid-month values
BHA_3YO = {
    # month: {dist: lbs}  (BHA uses fortnightly; these are mid-month approx)
    1:  {5: 15, 6: 15, 7: 14, 8: 14, 10: 12, 12: 10, 14: 9, 16: 9},
    2:  {5: 15, 6: 15, 7: 14, 8: 14, 10: 12, 12: 10, 14: 9, 16: 9},
    3:  {5: 14, 6: 14, 7: 13, 8: 13, 10: 11, 12: 10, 14: 9, 16: 9},
    4:  {5: 13, 6: 13, 7: 12, 8: 12, 10: 11, 12: 10, 14: 9, 16: 9},
    5:  {5: 11, 6: 11, 7: 10, 8: 10, 10: 9,  12: 9,  14: 8, 16: 8},
    6:  {5: 9,  6: 9,  7: 8,  8: 8,  10: 7,  12: 7,  14: 7, 16: 7},
    7:  {5: 7,  6: 6,  7: 6,  8: 6,  10: 6,  12: 6,  14: 6, 16: 6},
    8:  {5: 4,  6: 4,  7: 4,  8: 4,  10: 5,  12: 5,  14: 5, 16: 5},
    9:  {5: 2,  6: 2,  7: 2,  8: 2,  10: 3,  12: 4,  14: 4, 16: 4},
    10: {5: 1,  6: 1,  7: 1,  8: 1,  10: 2,  12: 3,  14: 3, 16: 3},
    11: {5: 0,  6: 0,  7: 0,  8: 0,  10: 1,  12: 2,  14: 2, 16: 2},
    12: {5: 0,  6: 0,  7: 0,  8: 0,  10: 0,  12: 1,  14: 1, 16: 1},
}

print("\n  Difference: OURS − BHA (positive = we give more WFA)")
print(f"  {'Month':>5} " + " ".join(f"{d:>5}f" for d in [5, 6, 7, 8, 10, 12]))
for month in range(1, 13):
    vals = []
    for dist in [5, 6, 7, 8, 10, 12]:
        our = WFA_3YO_TURF.get(month, {}).get(dist, 0)
        bha = BHA_3YO.get(month, {}).get(dist, 0)
        vals.append(f"{our - bha:>+5}")
    print(f"  {month:>5} " + " ".join(vals))


# ─── 5: Non-monotonic distance issues ────────────────────────────────

print("\n" + "=" * 70)
print("5. NON-MONOTONIC DISTANCE ISSUES")
print("=" * 70)
print("  WFA should generally increase with distance (younger horses")
print("  are more disadvantaged over longer distances).\n")

for table_name, table in [("3YO_TURF", WFA_3YO_TURF),
                           ("3YO_AW", WFA_3YO_AW),
                           ("2YO_TURF", WFA_2YO_TURF),
                           ("2YO_AW", WFA_2YO_AW)]:
    issues = []
    for month, dists in table.items():
        sorted_d = sorted(dists.items())
        for i in range(len(sorted_d) - 1):
            d1, v1 = sorted_d[i]
            d2, v2 = sorted_d[i + 1]
            if v2 < v1:
                issues.append((month, d1, v1, d2, v2))
    if issues:
        print(f"  {table_name}: {len(issues)} non-monotonic pairs")
        for month, d1, v1, d2, v2 in issues[:5]:
            print(f"    Month {month}: {d1}f={v1} > {d2}f={v2}")
    else:
        print(f"  {table_name}: OK (monotonic)")


# ─── 6: Re-derive WFA tables empirically ─────────────────────────────

print("\n" + "=" * 70)
print("6. RE-DERIVE WFA FROM RESIDUALS (optimal values)")
print("=" * 70)
print("  Computing what WFA SHOULD be to zero out residuals.\n")

# For each (age, surface, month, distance), compute the mean residual.
# The optimal WFA adjustment = current_wfa - residual
# (positive residual means we over-compensate, so reduce WFA)

# Get current WFA for each runner
valid["current_wfa"] = valid.apply(
    lambda r: get_wfa_allowance(
        r["horseAge"], r["month"], r["distance"],
        r.get("raceSurfaceName"),
    ),
    axis=1,
)

for age_group, age_val, surface in [
    ("3yo Turf", 3, "Turf"),
    ("3yo AW", 3, "All Weather"),
    ("2yo Turf", 2, "Turf"),
    ("2yo AW", 2, "All Weather"),
]:
    sub = valid[
        (valid["horseAge"] == age_val) &
        (valid["raceSurfaceName"] == surface)
    ]
    if len(sub) < 100:
        continue

    print(f"\n  {age_group} — Optimal WFA (current WFA − residual):")
    if age_val == 3:
        dist_list = [5, 6, 7, 8, 10, 12, 14, 16]
    else:
        dist_list = [5, 6, 7, 8, 10]

    print(f"  {'Month':>5} " + " ".join(f"{d:>5}f" for d in dist_list))
    for month in range(1, 13):
        vals = []
        for dist in dist_list:
            cell = sub[
                (sub["month"] == month) &
                (sub["dist_round"] == dist)
            ]
            if len(cell) >= 20:
                cur_wfa = cell["current_wfa"].mean()
                residual = cell["residual"].mean()
                optimal = cur_wfa - residual
                vals.append(f"{optimal:>5.1f}")
            else:
                vals.append(f"{'':>5}")
        print(f"  {month:>5} " + " ".join(vals))


# ─── 7: Older horse decline ──────────────────────────────────────────

print("\n" + "=" * 70)
print("7. OLDER HORSE DECLINE ANALYSIS")
print("=" * 70)

# Check residuals for 4-6yo (baseline) vs 7+ on both surfaces
baseline = valid[valid["horseAge"].between(4, 6)]
print(f"\n  4-6yo baseline: MAE={baseline['abs_residual'].mean():.2f}, "
      f"bias={baseline['residual'].mean():+.2f}, n={len(baseline):,}")

for surface in ["Turf", "All Weather"]:
    print(f"\n  {surface}:")
    print(f"  {'Age':>4} {'MAE':>7} {'Bias':>7} {'Curr WFA':>9} {'Optimal':>8} {'N':>8}")
    for age in range(4, 13):
        s = valid[(valid["horseAge"] == age) & (valid["raceSurfaceName"] == surface)]
        if len(s) < 100:
            continue
        mae = s["abs_residual"].mean()
        bias = s["residual"].mean()
        cur_wfa = s["current_wfa"].mean()
        optimal = cur_wfa - bias
        print(f"  {age:>4} {mae:>7.2f} {bias:>+7.2f} {cur_wfa:>+9.1f} {optimal:>+8.1f} {len(s):>8,}")


# ─── 8: Fortnightly resolution test ──────────────────────────────────

print("\n" + "=" * 70)
print("8. FORTNIGHTLY RESOLUTION TEST")
print("=" * 70)
print("  Testing whether first-half vs second-half of month differ.\n")

valid["day"] = pd.to_datetime(valid["meetingDate"], errors="coerce").dt.day
valid["half_month"] = np.where(valid["day"] <= 15, "1st half", "2nd half")

three_yo = valid[valid["horseAge"] == 3]
print("  3YO: First half vs second half bias by month")
print(f"  {'Month':>5} {'1st half':>9} {'2nd half':>9} {'Diff':>7} {'N_1st':>7} {'N_2nd':>7}")
for month in range(1, 13):
    m = three_yo[three_yo["month"] == month]
    h1 = m[m["half_month"] == "1st half"]
    h2 = m[m["half_month"] == "2nd half"]
    if len(h1) >= 100 and len(h2) >= 100:
        b1 = h1["residual"].mean()
        b2 = h2["residual"].mean()
        print(f"  {month:>5} {b1:>+9.2f} {b2:>+9.2f} {b2-b1:>+7.2f} {len(h1):>7,} {len(h2):>7,}")

# Same for 2yo
two_yo = valid[valid["horseAge"] == 2]
print(f"\n  2YO: First half vs second half bias by month")
print(f"  {'Month':>5} {'1st half':>9} {'2nd half':>9} {'Diff':>7} {'N_1st':>7} {'N_2nd':>7}")
for month in range(5, 13):
    m = two_yo[two_yo["month"] == month]
    h1 = m[m["half_month"] == "1st half"]
    h2 = m[m["half_month"] == "2nd half"]
    if len(h1) >= 50 and len(h2) >= 50:
        b1 = h1["residual"].mean()
        b2 = h2["residual"].mean()
        print(f"  {month:>5} {b1:>+9.2f} {b2:>+9.2f} {b2-b1:>+7.2f} {len(h1):>7,} {len(h2):>7,}")


# ─── 9: Year-over-year stability ─────────────────────────────────────

print("\n" + "=" * 70)
print("9. WFA STABILITY OVER YEARS")
print("=" * 70)
print("  Check if our WFA tables (derived from 2015-2023) still work.\n")

three_yo_all = valid[valid["horseAge"] == 3]
print("  3YO bias by year:")
print(f"  {'Year':>5} {'Bias':>7} {'MAE':>7} {'N':>8}")
for yr in sorted(three_yo_all["source_year"].unique()):
    s = three_yo_all[three_yo_all["source_year"] == yr]
    if len(s) < 200:
        continue
    print(f"  {yr:>5} {s['residual'].mean():>+7.2f} {s['abs_residual'].mean():>7.2f} {len(s):>8,}")

two_yo_all = valid[valid["horseAge"] == 2]
print(f"\n  2YO bias by year:")
print(f"  {'Year':>5} {'Bias':>7} {'MAE':>7} {'N':>8}")
for yr in sorted(two_yo_all["source_year"].unique()):
    s = two_yo_all[two_yo_all["source_year"] == yr]
    if len(s) < 100:
        continue
    print(f"  {yr:>5} {s['residual'].mean():>+7.2f} {s['abs_residual'].mean():>7.2f} {len(s):>8,}")


# ─── Summary ──────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

for age in [2, 3]:
    s = valid[valid["horseAge"] == age]
    print(f"  Age {age}: bias={s['residual'].mean():+.2f}, "
          f"MAE={s['abs_residual'].mean():.2f}, n={len(s):,}")

for age in range(4, 7):
    s = valid[valid["horseAge"] == age]
    print(f"  Age {age}: bias={s['residual'].mean():+.2f}, "
          f"MAE={s['abs_residual'].mean():.2f}, n={len(s):,}")

for age in range(7, 11):
    s = valid[valid["horseAge"] == age]
    if len(s) >= 100:
        print(f"  Age {age}: bias={s['residual'].mean():+.2f}, "
              f"MAE={s['abs_residual'].mean():.2f}, n={len(s):,}")
