"""Audit figure accuracy at Chelmsford and Southwell by year."""
import pandas as pd
import numpy as np

df = pd.read_csv("output/speed_figures.csv", low_memory=False)
df["meetingDate"] = pd.to_datetime(df["meetingDate"])
df["year"] = df["meetingDate"].dt.year

# Filter to tracks of interest with valid calibrated figure AND timefigure
tracks = ["CHELMSFORD CITY", "SOUTHWELL"]
mask = (
    df["courseName"].isin(tracks)
    & df["figure_calibrated"].notna()
    & df["timefigure"].notna()
    & (df["timefigure"] != 0)
)
sub = df[mask].copy()
sub["error"] = sub["figure_calibrated"] - sub["timefigure"]
sub["abs_error"] = sub["error"].abs()

print("=" * 90)
print("ACCURACY AUDIT: CHELMSFORD CITY & SOUTHWELL vs Timeform timefigure")
print("=" * 90)

# --- Per track, per year ---
for track in tracks:
    t = sub[sub["courseName"] == track]
    print(f"\n{'─' * 90}")
    print(f"  {track}  (surface: {t['raceSurfaceName'].unique()})")
    print(f"{'─' * 90}")
    print(f"{'Year':>6} {'N':>7} {'MAE':>7} {'RMSE':>7} {'Bias':>7} {'Corr':>7} {'±3':>6} {'±5':>6} {'±10':>6}")
    for year in sorted(t["year"].unique()):
        y = t[t["year"] == year]
        n = len(y)
        mae = y["abs_error"].mean()
        rmse = np.sqrt((y["error"] ** 2).mean())
        bias = y["error"].mean()
        corr = y["figure_calibrated"].corr(y["timefigure"]) if n > 5 else np.nan
        within3 = (y["abs_error"] <= 3).mean() * 100
        within5 = (y["abs_error"] <= 5).mean() * 100
        within10 = (y["abs_error"] <= 10).mean() * 100
        print(f"{year:>6} {n:>7} {mae:>7.2f} {rmse:>7.2f} {bias:>+7.2f} {corr:>7.3f} {within3:>5.1f}% {within5:>5.1f}% {within10:>5.1f}%")

    # Total for track
    n = len(t)
    mae = t["abs_error"].mean()
    rmse = np.sqrt((t["error"] ** 2).mean())
    bias = t["error"].mean()
    corr = t["figure_calibrated"].corr(t["timefigure"])
    within3 = (t["abs_error"] <= 3).mean() * 100
    within5 = (t["abs_error"] <= 5).mean() * 100
    within10 = (t["abs_error"] <= 10).mean() * 100
    print(f"{'TOTAL':>6} {n:>7} {mae:>7.2f} {rmse:>7.2f} {bias:>+7.2f} {corr:>7.3f} {within3:>5.1f}% {within5:>5.1f}% {within10:>5.1f}%")

# --- Compare to all AW tracks ---
print(f"\n{'─' * 90}")
print("  COMPARISON: ALL ALL-WEATHER TRACKS (excluding Chelmsford & Southwell)")
print(f"{'─' * 90}")
aw = df[
    (df["raceSurfaceName"] == "All Weather")
    & df["figure_calibrated"].notna()
    & df["timefigure"].notna()
    & (df["timefigure"] != 0)
    & ~df["courseName"].isin(tracks)
].copy()
aw["error"] = aw["figure_calibrated"] - aw["timefigure"]
aw["abs_error"] = aw["error"].abs()
aw["year"] = aw["meetingDate"].dt.year

print(f"{'Year':>6} {'N':>7} {'MAE':>7} {'RMSE':>7} {'Bias':>7} {'Corr':>7} {'±3':>6} {'±5':>6} {'±10':>6}")
for year in sorted(aw["year"].unique()):
    y = aw[aw["year"] == year]
    n = len(y)
    mae = y["abs_error"].mean()
    rmse = np.sqrt((y["error"] ** 2).mean())
    bias = y["error"].mean()
    corr = y["figure_calibrated"].corr(y["timefigure"]) if n > 5 else np.nan
    within3 = (y["abs_error"] <= 3).mean() * 100
    within5 = (y["abs_error"] <= 5).mean() * 100
    within10 = (y["abs_error"] <= 10).mean() * 100
    print(f"{year:>6} {n:>7} {mae:>7.2f} {rmse:>7.2f} {bias:>+7.2f} {corr:>7.3f} {within3:>5.1f}% {within5:>5.1f}% {within10:>5.1f}%")

n = len(aw)
mae = aw["abs_error"].mean()
rmse = np.sqrt((aw["error"] ** 2).mean())
bias = aw["error"].mean()
corr = aw["figure_calibrated"].corr(aw["timefigure"])
within3 = (aw["abs_error"] <= 3).mean() * 100
within5 = (aw["abs_error"] <= 5).mean() * 100
within10 = (aw["abs_error"] <= 10).mean() * 100
print(f"{'TOTAL':>6} {n:>7} {mae:>7.2f} {rmse:>7.2f} {bias:>+7.2f} {corr:>7.3f} {within3:>5.1f}% {within5:>5.1f}% {within10:>5.1f}%")

# --- Standard times used for these tracks ---
print(f"\n{'─' * 90}")
print("  STANDARD TIMES for Chelmsford & Southwell")
print(f"{'─' * 90}")
st = pd.read_csv("output/standard_times.csv")
st_sub = st[st["courseName"].isin(tracks)]
print(st_sub[["courseName", "distance", "surface", "median_time", "n_races"]].to_string(index=False))

# --- Going allowance distribution ---
print(f"\n{'─' * 90}")
print("  GOING ALLOWANCE DISTRIBUTION (s/f) per track per year")
print(f"{'─' * 90}")
ga = pd.read_csv("output/going_allowances.csv")
# Parse meeting_id to get date and course
ga[["date", "course", "surface"]] = ga["meeting_id"].str.extract(
    r"^(\d{4}-\d{2}-\d{2})_(.+?)_(Turf|All Weather)$"
)
ga["date"] = pd.to_datetime(ga["date"])
ga["year"] = ga["date"].dt.year
ga_sub = ga[ga["course"].isin(tracks)]
for track in tracks:
    t = ga_sub[ga_sub["course"] == track]
    print(f"\n  {track}:")
    print(f"  {'Year':>6} {'N_meets':>8} {'Mean GA':>8} {'Median GA':>9} {'Std GA':>7} {'Min':>7} {'Max':>7}")
    for year in sorted(t["year"].unique()):
        y = t[t["year"] == year]
        n = len(y)
        print(f"  {year:>6} {n:>8} {y['going_allowance_spf'].mean():>+8.4f} {y['going_allowance_spf'].median():>+9.4f} {y['going_allowance_spf'].std():>7.4f} {y['going_allowance_spf'].min():>+7.4f} {y['going_allowance_spf'].max():>+7.4f}")
