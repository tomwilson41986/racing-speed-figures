#!/usr/bin/env python3
"""
Generate an XLSX showing the lbs-per-length (LPL) breakdown for every track
and distance in the full Timeform database (2015–2026).

Uses the same pipeline stages as speed_figures.py:
  1. Load & filter to UK/IRE flat racing
  2. Compute standard times (iteratively refined)
  3. Derive course-specific LPL via compute_course_lpl()
  4. Audit coverage: which track/distance combos are covered vs fallback

Output: output/weight_length_breakdown.xlsx (5 sheets)
"""

import os
import sys

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(BASE_DIR, "src")
sys.path.insert(0, SRC_DIR)

from speed_figures import (
    load_data,
    filter_uk_ire_flat,
    apply_surface_change_cutoffs,
    compute_standard_times,
    compute_standard_times_iterative,
    compute_going_allowances,
    compute_course_lpl,
    generic_lbs_per_length,
    UK_COURSES,
    IRE_COURSES,
    MIN_RACES_STANDARD_TIME,
    SECONDS_PER_LENGTH,
    LBS_PER_SECOND_5F,
    BENCHMARK_FURLONGS,
    LPL_SURFACE_MULTIPLIER,
)

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ═════════════════════════════════════════════════════════════════════
# STAGE 0 — Load and filter data (reuses pipeline functions)
# ═════════════════════════════════════════════════════════════════════

print("=" * 70)
print("LPL AUDIT — WEIGHT / LENGTH BREAKDOWN")
print("=" * 70)

print("\nLoading Timeform data...")
df = load_data()
df = filter_uk_ire_flat(df)
df = apply_surface_change_cutoffs(df)

# ═════════════════════════════════════════════════════════════════════
# STAGE 1–3 — Standard times → Going allowances → Course LPL
# ═════════════════════════════════════════════════════════════════════

print("\nComputing standard times (initial)...")
std_times, std_df = compute_standard_times(df)

print("\nComputing going allowances (initial)...")
going_allowances = compute_going_allowances(df, std_times)

N_ITER = 3
for iteration in range(N_ITER):
    print(f"\n  Iterative refinement {iteration + 1}/{N_ITER}")
    std_times, std_df = compute_standard_times_iterative(df, going_allowances)
    going_allowances = compute_going_allowances(df, std_times)

print("\nComputing course-specific LPL...")
lpl_dict = compute_course_lpl(std_df)

# ═════════════════════════════════════════════════════════════════════
# BUILD OUTPUT TABLES
# ═════════════════════════════════════════════════════════════════════

# std_df has columns: std_key, median_time, mean_time, n_races, distance,
#                     courseName, surface
# Add LPL columns
std_df["course_lpl"] = std_df["std_key"].map(lpl_dict)
std_df["generic_lpl"] = std_df.apply(
    lambda r: generic_lbs_per_length(r["distance"], r.get("surface")),
    axis=1,
)
std_df["spf"] = std_df["median_time"] / std_df["distance"]
std_df["has_course_lpl"] = std_df["course_lpl"].notna()

# For combos WITHOUT course-specific LPL, compute generic fallback
std_df.loc[~std_df["has_course_lpl"], "course_lpl"] = std_df.loc[
    ~std_df["has_course_lpl"], "generic_lpl"
]

# ── Sheet 1: Covered combos (have course-specific LPL) ──────────────

covered = std_df[std_df["has_course_lpl"]].copy()
covered = covered.sort_values(["courseName", "distance"]).reset_index(drop=True)

sheet1 = covered[[
    "courseName", "distance", "surface", "n_races",
    "course_lpl", "generic_lpl", "median_time", "spf",
]].copy()
sheet1.columns = [
    "Track", "Distance (f)", "Surface", "N Races",
    "Course-Specific LPL", "Generic LPL", "Median Time (s)", "Secs/Furlong",
]
# Add derived columns
sheet1["Correction Factor"] = (
    sheet1["Course-Specific LPL"] / sheet1["Generic LPL"]
).round(4)
sheet1["Surface Multiplier"] = sheet1["Surface"].map(
    LPL_SURFACE_MULTIPLIER
).fillna(1.0)
for col in ["Course-Specific LPL", "Generic LPL", "Median Time (s)", "Secs/Furlong"]:
    sheet1[col] = sheet1[col].round(3)

# ── Sheet 2: Coverage gaps (below threshold) ─────────────────────────

# Find ALL track/distance/surface combos in the data (not just those
# with enough races for a standard time)
all_combos = (
    df.groupby(["std_key"])
    .agg(
        n_races_total=("race_id", "nunique"),
        n_winners=("positionOfficial", lambda x: (x == 1).sum()),
        courseName=("courseName", "first"),
        distance=("distance", "first"),
        surface=("raceSurfaceName", "first"),
    )
    .reset_index()
)

# Combos that do NOT have course-specific LPL
covered_keys = set(lpl_dict.keys())
gaps = all_combos[~all_combos["std_key"].isin(covered_keys)].copy()
gaps = gaps.sort_values(["courseName", "distance"]).reset_index(drop=True)
gaps["generic_lpl"] = gaps.apply(
    lambda r: generic_lbs_per_length(r["distance"], r.get("surface")),
    axis=1,
)

sheet2 = gaps[[
    "courseName", "distance", "surface", "n_races_total", "n_winners",
    "generic_lpl",
]].copy()
sheet2.columns = [
    "Track", "Distance (f)", "Surface", "Total Races", "N Winners",
    "Generic LPL (Fallback)",
]
sheet2["Generic LPL (Fallback)"] = sheet2["Generic LPL (Fallback)"].round(3)
sheet2["Status"] = "Below " + str(MIN_RACES_STANDARD_TIME) + " winner threshold"

# ── Sheet 3: Missing tracks (no data at all) ────────────────────────

all_expected = UK_COURSES | IRE_COURSES
tracks_in_data = set(df["courseName"].unique())
missing_tracks = sorted(all_expected - tracks_in_data)

sheet3_rows = []
for track in missing_tracks:
    origin = "UK" if track in UK_COURSES else "IRE"
    sheet3_rows.append({
        "Track": track,
        "Region": origin,
        "Status": "No data in Timeform archive",
    })
sheet3 = pd.DataFrame(sheet3_rows)

# ── Sheet 4: Pivot table ────────────────────────────────────────────

pivot = sheet1.pivot_table(
    values="Course-Specific LPL",
    index="Track",
    columns="Distance (f)",
    aggfunc="first",
)

# ── Sheet 5: Generic LPL reference ──────────────────────────────────

all_distances = sorted(set(
    list(sheet1["Distance (f)"].unique()) +
    list(sheet2["Distance (f)"].unique())
))
generic_ref = pd.DataFrame({
    "Distance (f)": all_distances,
    "Generic LPL (Turf)": [
        round(generic_lbs_per_length(d, "Turf"), 3) for d in all_distances
    ],
    "Generic LPL (AW)": [
        round(generic_lbs_per_length(d, "All Weather"), 3) for d in all_distances
    ],
    "Lbs/Second": [
        round(LBS_PER_SECOND_5F * (BENCHMARK_FURLONGS / d), 3)
        for d in all_distances
    ],
    "Secs/Length": [SECONDS_PER_LENGTH] * len(all_distances),
})

# ═════════════════════════════════════════════════════════════════════
# WRITE XLSX
# ═════════════════════════════════════════════════════════════════════

xlsx_path = os.path.join(OUTPUT_DIR, "weight_length_breakdown.xlsx")

with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
    sheet1.to_excel(writer, sheet_name="LPL by Track & Distance", index=False)
    if len(sheet2) > 0:
        sheet2.to_excel(writer, sheet_name="Coverage Gaps", index=False)
    if len(sheet3) > 0:
        sheet3.to_excel(writer, sheet_name="Missing Tracks", index=False)
    pivot.to_excel(writer, sheet_name="LPL Pivot (Track x Dist)")
    generic_ref.to_excel(writer, sheet_name="Generic LPL Reference", index=False)

    # Auto-size columns
    for sheet_name in writer.sheets:
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = 0
            col_letter = col_cells[0].column_letter
            for cell in col_cells:
                try:
                    cell_len = len(str(cell.value))
                    if cell_len > max_len:
                        max_len = cell_len
                except Exception:
                    pass
            ws.column_dimensions[col_letter].width = min(max_len + 3, 30)

# ═════════════════════════════════════════════════════════════════════
# AUDIT SUMMARY
# ═════════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("LPL COVERAGE AUDIT SUMMARY")
print("=" * 70)

n_expected = len(all_expected)
tracks_with_lpl = set(covered["courseName"].unique())
tracks_with_gaps_only = set(gaps["courseName"].unique()) - tracks_with_lpl
n_covered = len(tracks_with_lpl)
n_missing = len(missing_tracks)
n_gaps_only = len(tracks_with_gaps_only)

print(f"\n  TRACKS")
print(f"    Expected (UK + IRE):              {n_expected}")
print(f"    With course-specific LPL:         {n_covered}")
print(f"    Data but below threshold only:    {n_gaps_only}")
print(f"    No data in archive:               {n_missing}")
if missing_tracks:
    print(f"    Missing: {', '.join(missing_tracks)}")

n_combos_covered = len(sheet1)
n_combos_gaps = len(sheet2)
n_combos_total = n_combos_covered + n_combos_gaps
print(f"\n  TRACK / DISTANCE COMBOS")
print(f"    Total in data:                    {n_combos_total}")
print(f"    With course-specific LPL:         {n_combos_covered} "
      f"({100 * n_combos_covered / max(n_combos_total, 1):.1f}%)")
print(f"    Using generic fallback:           {n_combos_gaps} "
      f"({100 * n_combos_gaps / max(n_combos_total, 1):.1f}%)")
print(f"    Min races for course LPL:         {MIN_RACES_STANDARD_TIME}")

print(f"\n  OUTPUT")
print(f"    {xlsx_path}")
print(f"    Sheet 1: LPL by Track & Distance  ({len(sheet1)} combos)")
print(f"    Sheet 2: Coverage Gaps             ({len(sheet2)} combos)")
print(f"    Sheet 3: Missing Tracks            ({len(sheet3)} tracks)")
print(f"    Sheet 4: LPL Pivot (Track × Dist)")
print(f"    Sheet 5: Generic LPL Reference     ({len(generic_ref)} distances)")
print()
