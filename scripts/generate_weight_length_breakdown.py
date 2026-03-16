#!/usr/bin/env python3
"""
Generate an XLSX showing the lbs-per-length breakdown for each track and distance.

Computes:
  - Generic LPL from the formula: lpl = seconds_per_length × lbs_per_second_at_distance
  - Course-specific LPL adjusted by how fast/slow a track runs vs the average
  - Surface multiplier (AW gets ×1.10)

Output: output/weight_length_breakdown.xlsx
"""

import os
import sys
import glob

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(BASE_DIR, "src")
sys.path.insert(0, SRC_DIR)

from speed_figures import (
    generic_lbs_per_length,
    SECONDS_PER_LENGTH,
    LBS_PER_SECOND_5F,
    BENCHMARK_FURLONGS,
    LPL_SURFACE_MULTIPLIER,
)

OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Load all historic result CSVs ────────────────────────────────────

data_files = sorted(glob.glob(os.path.join(BASE_DIR, "data", "historic", "results_*.csv")))
if not data_files:
    print("No historic CSV files found in data/historic/")
    sys.exit(1)

frames = []
for f in data_files:
    frames.append(pd.read_csv(f))
df = pd.concat(frames, ignore_index=True)
print(f"Loaded {len(df):,} rows from {len(data_files)} files")

# ── Derive fields ────────────────────────────────────────────────────

df["comptime_numeric"] = pd.to_numeric(df["comptime_numeric"], errors="coerce")
df["Dist_Furlongs"] = pd.to_numeric(df["Dist_Furlongs"], errors="coerce")
df["pounds"] = pd.to_numeric(df["pounds"], errors="coerce")
df["Yards"] = pd.to_numeric(df["Yards"], errors="coerce")
df["placing_numerical"] = pd.to_numeric(df["placing_numerical"], errors="coerce")

# Map RCode to surface label matching the pipeline's convention
surface_map = {"All Weather": "All Weather", "National Hunt": "Turf"}
# For flat races on turf tracks, also "Turf"
# RCode captures the race type, but surfacetype is the track surface.
# Use RCode to determine if it's AW, otherwise Turf.
df["surface"] = df["RCode"].map(surface_map).fillna("Turf")

# ── Compute winners-only median times per track/distance ─────────────

winners = df[
    (df["placing_numerical"] == 1)
    & df["comptime_numeric"].notna()
    & (df["comptime_numeric"] > 0)
    & df["Dist_Furlongs"].notna()
    & (df["Dist_Furlongs"] > 0)
].copy()

# Standard-time key: track + distance + surface
winners["std_key"] = (
    winners["track"].astype(str) + "_"
    + winners["Dist_Furlongs"].astype(str) + "_"
    + winners["surface"].astype(str)
)

std_times = (
    winners.groupby("std_key")
    .agg(
        median_time=("comptime_numeric", "median"),
        mean_time=("comptime_numeric", "mean"),
        n_races=("comptime_numeric", "count"),
        distance=("Dist_Furlongs", "first"),
        track=("track", "first"),
        surface=("surface", "first"),
    )
    .reset_index()
)

print(f"Track/distance combos with winner times: {len(std_times)}")

# ── Compute course-specific LPL ─────────────────────────────────────

# Seconds per furlong for each combo
std_times["spf"] = std_times["median_time"] / std_times["distance"]

# Mean spf across all tracks at each rounded distance
std_times["dist_band"] = std_times["distance"].round(0)
mean_spf = std_times.groupby("dist_band")["spf"].mean()
std_times["mean_spf"] = std_times["dist_band"].map(mean_spf)

# Course correction: faster track → higher LPL (horses spread more at speed)
std_times["correction"] = std_times["mean_spf"] / std_times["spf"]

# Generic LPL at this distance
std_times["generic_lpl"] = std_times["distance"].apply(generic_lbs_per_length)

# Surface multiplier
std_times["surf_mult"] = std_times["surface"].map(LPL_SURFACE_MULTIPLIER).fillna(1.0)

# Course-specific LPL
std_times["course_lpl"] = (
    std_times["generic_lpl"] * std_times["correction"] * std_times["surf_mult"]
)

# ── Build the output table ───────────────────────────────────────────

# Also compute race-level stats for context
race_stats = (
    df.groupby(["track", "Dist_Furlongs", "surface"])
    .agg(
        total_runners=("horse_name", "count"),
        avg_weight_lbs=("pounds", "mean"),
        min_weight_lbs=("pounds", "min"),
        max_weight_lbs=("pounds", "max"),
        avg_yards=("Yards", "mean"),
    )
    .reset_index()
)

# Merge
output = std_times.merge(
    race_stats,
    left_on=["track", "distance", "surface"],
    right_on=["track", "Dist_Furlongs", "surface"],
    how="left",
)

# Select and order columns for the spreadsheet
result = output[[
    "track",
    "distance",
    "surface",
    "n_races",
    "total_runners",
    "generic_lpl",
    "course_lpl",
    "correction",
    "surf_mult",
    "median_time",
    "spf",
    "mean_spf",
    "avg_weight_lbs",
    "min_weight_lbs",
    "max_weight_lbs",
    "avg_yards",
]].copy()

result.columns = [
    "Track",
    "Distance (f)",
    "Surface",
    "Winner Races",
    "Total Runners",
    "Generic LPL",
    "Course-Specific LPL",
    "Course Correction",
    "Surface Multiplier",
    "Median Time (s)",
    "Secs/Furlong",
    "Mean Secs/Furlong (all tracks)",
    "Avg Weight (lbs)",
    "Min Weight (lbs)",
    "Max Weight (lbs)",
    "Avg Distance (yards)",
]

result = result.sort_values(["Track", "Distance (f)"]).reset_index(drop=True)

# Round for readability
for col in ["Generic LPL", "Course-Specific LPL", "Course Correction",
            "Median Time (s)", "Secs/Furlong", "Mean Secs/Furlong (all tracks)",
            "Avg Weight (lbs)", "Avg Distance (yards)"]:
    if col in result.columns:
        result[col] = result[col].round(3)

# ── Write to XLSX with formatting ────────────────────────────────────

xlsx_path = os.path.join(OUTPUT_DIR, "weight_length_breakdown.xlsx")

with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
    # Sheet 1: Full breakdown
    result.to_excel(writer, sheet_name="LPL by Track & Distance", index=False)

    # Sheet 2: Pivot — tracks as rows, distances as columns, LPL as values
    pivot = result.pivot_table(
        values="Course-Specific LPL",
        index="Track",
        columns="Distance (f)",
        aggfunc="first",
    )
    pivot.to_excel(writer, sheet_name="LPL Pivot (Track x Dist)")

    # Sheet 3: Generic LPL reference table
    distances = sorted(result["Distance (f)"].unique())
    generic_ref = pd.DataFrame({
        "Distance (f)": distances,
        "Generic LPL (Turf)": [generic_lbs_per_length(d, "Turf") for d in distances],
        "Generic LPL (AW)": [generic_lbs_per_length(d, "All Weather") for d in distances],
        "Lbs/Second": [LBS_PER_SECOND_5F * (BENCHMARK_FURLONGS / d) for d in distances],
        "Secs/Length": [SECONDS_PER_LENGTH] * len(distances),
    })
    for col in ["Generic LPL (Turf)", "Generic LPL (AW)", "Lbs/Second"]:
        generic_ref[col] = generic_ref[col].round(3)
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

print(f"\nWrote {xlsx_path}")
print(f"  Sheet 1: LPL by Track & Distance ({len(result)} rows)")
print(f"  Sheet 2: LPL Pivot (Track × Distance)")
print(f"  Sheet 3: Generic LPL Reference ({len(generic_ref)} distances)")
