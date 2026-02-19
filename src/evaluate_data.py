"""
Comprehensive evaluation of the Timeform racing dataset (2015-2026).
Assesses data quality, coverage, and suitability for speed figure compilation.
"""

import pandas as pd
import numpy as np
import os
from collections import defaultdict

DATA_DIR = "/tmp/racing_data"


def load_all_data():
    """Load and concatenate all yearly CSV files."""
    frames = []
    for year in range(2015, 2027):
        path = os.path.join(DATA_DIR, f"timeform_{year}.csv")
        if os.path.exists(path):
            df = pd.read_csv(path, low_memory=False)
            df["source_year"] = year
            frames.append(df)
            print(f"  {year}: {len(df):>8,} rows, {df.columns.size} cols")
    combined = pd.concat(frames, ignore_index=True)
    print(f"  TOTAL: {len(combined):>8,} rows")
    return combined


def overview(df):
    """High-level dataset overview."""
    print("\n" + "=" * 70)
    print("1. DATASET OVERVIEW")
    print("=" * 70)
    print(f"Total rows (runner-level):  {len(df):,}")
    print(f"Columns:                    {df.columns.size}")
    print(f"Date range:                 {df['meetingDate'].min()} to {df['meetingDate'].max()}")
    print(f"Unique courses:             {df['courseName'].nunique()}")
    print(f"Unique horses:              {df['horseName'].nunique()}")
    print(f"Unique meeting dates:       {df['meetingDate'].nunique()}")

    # Identify unique races
    df["race_id"] = df["meetingDate"] + "_" + df["courseName"] + "_" + df["raceNumber"].astype(str)
    n_races = df["race_id"].nunique()
    print(f"Unique races:               {n_races:,}")
    print(f"Avg runners/race:           {len(df) / n_races:.1f}")


def column_quality(df):
    """Check completeness and types of all columns."""
    print("\n" + "=" * 70)
    print("2. COLUMN COMPLETENESS & DATA TYPES")
    print("=" * 70)

    key_cols = [
        "positionOfficial", "horseName", "courseName", "courseId",
        "meetingDate", "raceNumber", "distance", "going",
        "raceSurfaceName", "raceType", "raceClass", "numberOfRunners",
        "finishingTime", "distanceBeaten", "distanceCumulative",
        "timefigure", "performanceRating", "horseAge", "horseGender",
        "draw", "jockeyFullName", "trainerFullName",
        "leaderSectional", "winnerSectional", "distanceSectional",
        "sectionalFinishingTime", "prizeFund", "ispDecimal",
        "betfairWinSP", "preRaceAdjustedRating", "preRaceMasterRating",
        "horseCode", "foalingDate"
    ]

    print(f"\n{'Column':<30} {'Non-null':>10} {'Pct':>7} {'Dtype':>10} {'Sample values'}")
    print("-" * 90)
    for col in key_cols:
        if col in df.columns:
            non_null = df[col].notna().sum()
            pct = non_null / len(df) * 100
            dtype = str(df[col].dtype)
            # Get sample non-null values
            samples = df[col].dropna().unique()[:3]
            sample_str = str(list(samples))[:40]
            print(f"{col:<30} {non_null:>10,} {pct:>6.1f}% {dtype:>10} {sample_str}")
        else:
            print(f"{col:<30} {'MISSING':>10}")


def race_type_analysis(df):
    """Analyze race types, surfaces, and going descriptions."""
    print("\n" + "=" * 70)
    print("3. RACE TYPES, SURFACES & GOING")
    print("=" * 70)

    print("\nRace Types:")
    print(df["raceType"].value_counts().to_string())

    print("\nSurfaces:")
    print(df["raceSurfaceName"].value_counts().to_string())

    print("\nGoing Descriptions:")
    going_counts = df["going"].value_counts()
    for going, count in going_counts.items():
        print(f"  {going:<20} {count:>8,} ({count/len(df)*100:>5.1f}%)")

    print("\nRace Classes:")
    class_counts = df["raceClass"].value_counts().sort_index()
    for cls, count in class_counts.items():
        label = cls if cls != "" else "(empty)"
        print(f"  Class {label:<10} {count:>8,} ({count/len(df)*100:>5.1f}%)")


def geography_analysis(df):
    """Analyze geographic coverage — UK, Ireland, international."""
    print("\n" + "=" * 70)
    print("4. GEOGRAPHIC COVERAGE")
    print("=" * 70)

    # Known UK courses
    uk_courses = {
        "ASCOT", "AYR", "BATH", "BEVERLEY", "BRIGHTON", "CARLISLE",
        "CATTERICK", "CHELMSFORD CITY", "CHELTENHAM", "CHEPSTOW", "CHESTER",
        "DONCASTER", "EPSOM", "EXETER", "FFOS LAS", "FONTWELL", "GOODWOOD",
        "HAMILTON", "HAYDOCK", "HEREFORD", "HEXHAM", "HUNTINGDON", "KELSO",
        "KEMPTON", "LEICESTER", "LINGFIELD", "LUDLOW", "MARKET RASEN",
        "MUSSELBURGH", "NEWBURY", "NEWCASTLE", "NEWMARKET", "NEWTON ABBOT",
        "NOTTINGHAM", "PLUMPTON", "PONTEFRACT", "REDCAR", "RIPON",
        "SALISBURY", "SANDOWN", "SEDGEFIELD", "SOUTHWELL", "STRATFORD",
        "TAUNTON", "THIRSK", "UTTOXETER", "WARWICK", "WETHERBY",
        "WINCANTON", "WINDSOR", "WOLVERHAMPTON", "WORCESTER", "YARMOUTH", "YORK",
        "BANGOR-ON-DEE", "CARTMEL", "FAKENHAM", "FOLKESTONE", "PERTH",
        "TOWCESTER", "AINTREE"
    }

    ire_courses = {
        "BALLINROBE", "BELLEWSTOWN", "CLONMEL", "CORK", "CURRAGH",
        "DOWN ROYAL", "DOWNPATRICK", "DUNDALK", "FAIRYHOUSE",
        "GALWAY", "GOWRAN PARK", "KILBEGGAN", "KILLARNEY",
        "LAYTOWN", "LEOPARDSTOWN", "LIMERICK", "LISTOWEL",
        "NAAS", "NAVAN", "PUNCHESTOWN", "ROSCOMMON", "SLIGO",
        "THURLES", "TIPPERARY", "TRAMORE", "WEXFORD"
    }

    all_courses = set(df["courseName"].unique())

    uk_matched = all_courses & uk_courses
    ire_matched = all_courses & ire_courses
    other = all_courses - uk_courses - ire_courses

    uk_rows = df[df["courseName"].isin(uk_courses)]
    ire_rows = df[df["courseName"].isin(ire_courses)]
    other_rows = df[~df["courseName"].isin(uk_courses | ire_courses)]

    print(f"\nUK courses matched:      {len(uk_matched):>4} ({len(uk_rows):>8,} rows, {len(uk_rows)/len(df)*100:.1f}%)")
    print(f"Ireland courses matched: {len(ire_matched):>4} ({len(ire_rows):>8,} rows, {len(ire_rows)/len(df)*100:.1f}%)")
    print(f"Other/International:     {len(other):>4} ({len(other_rows):>8,} rows, {len(other_rows)/len(df)*100:.1f}%)")

    if other:
        print(f"\nInternational courses (sample): {sorted(other)[:30]}")

    # Rows per year by region
    print(f"\n{'Year':<6} {'UK':>8} {'IRE':>8} {'Other':>8} {'Total':>8}")
    print("-" * 42)
    for year in sorted(df["source_year"].unique()):
        yr = df[df["source_year"] == year]
        uk_n = yr[yr["courseName"].isin(uk_courses)].shape[0]
        ire_n = yr[yr["courseName"].isin(ire_courses)].shape[0]
        oth_n = yr.shape[0] - uk_n - ire_n
        print(f"{year:<6} {uk_n:>8,} {ire_n:>8,} {oth_n:>8,} {yr.shape[0]:>8,}")


def timing_analysis(df):
    """Analyze finishing times — the core input for speed figures."""
    print("\n" + "=" * 70)
    print("5. FINISHING TIME ANALYSIS")
    print("=" * 70)

    ft = pd.to_numeric(df["finishingTime"], errors="coerce")
    print(f"\nfinishingTime non-null: {ft.notna().sum():,} / {len(df):,} ({ft.notna().mean()*100:.1f}%)")
    print(f"  Range: {ft.min():.2f} - {ft.max():.2f} seconds")
    print(f"  Mean:  {ft.mean():.2f}s, Median: {ft.median():.2f}s")

    # Check if all runners in a race share the same finishingTime (i.e. it's winner time)
    sample_races = df.groupby("race_id").agg(
        ft_nunique=("finishingTime", "nunique"),
        n_runners=("positionOfficial", "count")
    )
    all_same = (sample_races["ft_nunique"] == 1).mean()
    print(f"\n  Races where all runners share same finishingTime: {all_same*100:.1f}%")
    print(f"  (Confirms finishingTime = RACE winning time, not individual horse time)")

    # Finishing time by distance
    df["_ft"] = ft
    df["_dist_round"] = df["distance"].round(0)
    dist_times = df.groupby("_dist_round")["_ft"].agg(["median", "count", "std"])
    dist_times = dist_times[dist_times["count"] > 100]
    print(f"\n  Median finishing time by distance (furlongs):")
    print(f"  {'Dist':>6} {'Median(s)':>10} {'Std':>8} {'Count':>8}")
    for d, row in dist_times.iterrows():
        print(f"  {d:>6.0f} {row['median']:>10.2f} {row['std']:>8.2f} {row['count']:>8,.0f}")
    df.drop(columns=["_ft", "_dist_round"], inplace=True)


def timefigure_analysis(df):
    """Analyze the target variable — Timeform's timefigure (TFig)."""
    print("\n" + "=" * 70)
    print("6. TIMEFIGURE (TFig) ANALYSIS — TARGET VARIABLE")
    print("=" * 70)

    tf = pd.to_numeric(df["timefigure"], errors="coerce")
    print(f"\nTotal rows:           {len(df):,}")
    print(f"TFig non-null:        {tf.notna().sum():,}")
    print(f"TFig == 0:            {(tf == 0).sum():,} ({(tf == 0).mean()*100:.1f}%)")
    print(f"TFig > 0:             {(tf > 0).sum():,} ({(tf > 0).mean()*100:.1f}%)")
    print(f"TFig < 0:             {(tf < 0).sum():,} ({(tf < 0).mean()*100:.1f}%)")

    valid = tf[tf != 0]
    print(f"\nNon-zero TFig stats:")
    print(f"  Count: {valid.count():,}")
    print(f"  Mean:  {valid.mean():.1f}")
    print(f"  Median:{valid.median():.1f}")
    print(f"  Std:   {valid.std():.1f}")
    print(f"  Min:   {valid.min():.1f}")
    print(f"  Max:   {valid.max():.1f}")

    # Distribution buckets
    print(f"\nTFig distribution (non-zero):")
    buckets = [(-9999, -50), (-50, 0), (0, 30), (30, 50), (50, 70),
               (70, 90), (90, 110), (110, 130), (130, 200)]
    for lo, hi in buckets:
        n = ((valid >= lo) & (valid < hi)).sum()
        print(f"  [{lo:>5}, {hi:>4}): {n:>8,} ({n/valid.count()*100:>5.1f}%)")

    # TFig by year
    df["_tf"] = tf
    print(f"\nTFig by year (non-zero stats):")
    print(f"  {'Year':<6} {'Count':>8} {'Zero%':>7} {'Mean':>7} {'Median':>7} {'Max':>5}")
    for year in sorted(df["source_year"].unique()):
        yr_tf = df[df["source_year"] == year]["_tf"]
        nz = yr_tf[yr_tf != 0]
        zero_pct = (yr_tf == 0).mean() * 100
        print(f"  {year:<6} {nz.count():>8,} {zero_pct:>6.1f}% {nz.mean():>7.1f} {nz.median():>7.1f} {nz.max():>5.0f}")
    df.drop(columns=["_tf"], inplace=True)


def beaten_distance_analysis(df):
    """Analyze beaten distances — critical for non-winner figures."""
    print("\n" + "=" * 70)
    print("7. BEATEN DISTANCE ANALYSIS")
    print("=" * 70)

    db = pd.to_numeric(df["distanceBeaten"], errors="coerce")
    dc = pd.to_numeric(df["distanceCumulative"], errors="coerce")
    pos = pd.to_numeric(df["positionOfficial"], errors="coerce")

    print(f"\ndistanceBeaten non-null:     {db.notna().sum():,} ({db.notna().mean()*100:.1f}%)")
    print(f"distanceCumulative non-null: {dc.notna().sum():,} ({dc.notna().mean()*100:.1f}%)")

    # Winners
    winners = df[pos == 1]
    print(f"\nWinners (pos=1): {len(winners):,}")
    print(f"  distanceBeaten == 0: {(winners['distanceBeaten'].astype(float) == 0).sum():,}")
    print(f"  distanceCumulative for winners: mean={winners['distanceCumulative'].astype(float).mean():.2f}")
    print(f"  (This is the winning margin — distance to 2nd place)")

    # Non-winners
    non_winners = df[pos > 1]
    dc_nw = pd.to_numeric(non_winners["distanceCumulative"], errors="coerce")
    print(f"\nNon-winners distanceCumulative:")
    print(f"  Mean:   {dc_nw.mean():.2f} lengths")
    print(f"  Median: {dc_nw.median():.2f} lengths")
    print(f"  Max:    {dc_nw.max():.2f} lengths")
    print(f"  >30L:   {(dc_nw > 30).sum():,} ({(dc_nw > 30).mean()*100:.1f}%)")


def sectional_analysis(df):
    """Analyze sectional timing data availability."""
    print("\n" + "=" * 70)
    print("8. SECTIONAL TIMING DATA")
    print("=" * 70)

    for col in ["leaderSectional", "winnerSectional", "distanceSectional", "sectionalFinishingTime"]:
        vals = pd.to_numeric(df[col], errors="coerce")
        non_null = vals.notna().sum()
        non_zero = (vals.notna() & (vals != 0)).sum()
        print(f"\n{col}:")
        print(f"  Non-null: {non_null:,} ({non_null/len(df)*100:.1f}%)")
        print(f"  Non-zero: {non_zero:,} ({non_zero/len(df)*100:.1f}%)")
        if non_zero > 0:
            valid = vals[vals.notna() & (vals != 0)]
            print(f"  Range: {valid.min():.1f} - {valid.max():.1f}")
            print(f"  Mean:  {valid.mean():.1f}")


def course_distance_coverage(df):
    """Analyze course/distance combinations for standard time computation."""
    print("\n" + "=" * 70)
    print("9. COURSE/DISTANCE COVERAGE (for standard times)")
    print("=" * 70)

    # Focus on UK/IRE
    uk_ire = df  # include all for now

    # Count unique races per course/distance
    winners = uk_ire[pd.to_numeric(uk_ire["positionOfficial"], errors="coerce") == 1].copy()
    winners["_dist_round"] = winners["distance"].astype(float).round(1)

    cd_counts = winners.groupby(["courseName", "_dist_round", "raceSurfaceName"]).size().reset_index(name="n_races")
    cd_counts = cd_counts.sort_values("n_races", ascending=False)

    print(f"\nTotal course/distance/surface combos: {len(cd_counts):,}")
    print(f"Combos with >= 20 races:              {(cd_counts['n_races'] >= 20).sum():,}")
    print(f"Combos with >= 50 races:              {(cd_counts['n_races'] >= 50).sum():,}")
    print(f"Combos with >= 100 races:             {(cd_counts['n_races'] >= 100).sum():,}")

    print(f"\nTop 20 course/distance combos by race count:")
    print(f"  {'Course':<25} {'Dist':>5} {'Surface':<12} {'Races':>6}")
    print("  " + "-" * 55)
    for _, row in cd_counts.head(20).iterrows():
        print(f"  {row['courseName']:<25} {row['_dist_round']:>5.1f} {row['raceSurfaceName']:<12} {row['n_races']:>6}")

    # Courses with most races overall
    print(f"\nTop 20 courses by total winners:")
    course_counts = winners.groupby("courseName").size().sort_values(ascending=False)
    for course, n in course_counts.head(20).items():
        print(f"  {course:<25} {n:>6,}")


def missing_data_assessment(df):
    """Assess what's missing for speed figure computation."""
    print("\n" + "=" * 70)
    print("10. MISSING DATA ASSESSMENT FOR SPEED FIGURES")
    print("=" * 70)

    print("\nData AVAILABLE for speed figures:")
    available = {
        "Finishing times (winner)": "finishingTime",
        "Beaten distances (cumulative)": "distanceCumulative",
        "Distance (furlongs)": "distance",
        "Going description": "going",
        "Course name/ID": "courseName",
        "Surface type": "raceSurfaceName",
        "Race class": "raceClass",
        "Horse age": "horseAge",
        "Horse gender": "horseGender",
        "Draw/stall": "draw",
        "Field size": "numberOfRunners",
        "Meeting date": "meetingDate",
        "Race number": "raceNumber",
        "Sectional times (partial)": "winnerSectional",
        "Timeform figure (target)": "timefigure",
        "Performance rating": "performanceRating",
        "Jockey": "jockeyFullName",
        "Trainer": "trainerFullName",
        "Horse ID": "horseCode",
        "SP odds": "ispDecimal",
        "Betfair SP": "betfairWinSP",
    }
    for desc, col in available.items():
        if col in df.columns:
            pct = df[col].notna().mean() * 100
            # Also check for empty strings
            if df[col].dtype == object:
                non_empty = ((df[col].notna()) & (df[col] != "")).mean() * 100
                print(f"  [OK]  {desc:<35} {non_empty:>5.1f}% populated")
            else:
                print(f"  [OK]  {desc:<35} {pct:>5.1f}% populated")

    print("\nData NOT in dataset (must derive or source externally):")
    missing = [
        "Weight carried (lbs)",
        "Going stick readings",
        "Rail movement data",
        "Wind speed/direction",
        "Run-up distances",
        "Course configuration (straight/round)",
        "Equipment changes (blinkers etc.)",
        "In-running positions",
    ]
    for item in missing:
        print(f"  [--]  {item}")


def key_findings(df):
    """Summarize key findings and recommendations."""
    print("\n" + "=" * 70)
    print("11. KEY FINDINGS & RECOMMENDATIONS")
    print("=" * 70)

    tf = pd.to_numeric(df["timefigure"], errors="coerce")
    valid_tf = (tf != 0) & tf.notna()

    print(f"""
DATASET SUMMARY:
  - {len(df):,} runner-level records across {df['source_year'].nunique()} years (2015-2026)
  - {df['race_id'].nunique():,} unique races
  - {df['courseName'].nunique()} courses ({df['horseName'].nunique():,} unique horses)
  - Flat racing only (no jump racing in this dataset)

STRENGTHS:
  + Finishing times available for nearly all rows
  + Cumulative beaten distances available
  + Timeform timefigure (TFig) available as validation target
  + {valid_tf.sum():,} rows ({valid_tf.mean()*100:.1f}%) have non-zero TFig
  + Sectional timing data available for many races
  + Good coverage of UK and Irish courses
  + 11 years of data provides ample sample for standard times
  + Horse IDs (horseCode) enable tracking individual horses across runs
  + Pre-race ratings available for iterative methods

LIMITATIONS:
  - NO weight carried column — cannot do weight adjustment
  - Race class is empty for many rows (especially Irish races)
  - TFig = 0 for {(tf==0).sum():,} rows ({(tf==0).mean()*100:.1f}%) — likely races without timing
  - International data mixed in (need to filter to UK/IRE for pipeline)
  - No going stick readings, rail movements, or wind data
  - No course configuration info (straight vs round)

RECOMMENDED APPROACH:
  1. Filter to UK/IRE courses with Turf and AW surfaces
  2. Build standard times from races with non-zero TFig (confirmed timed races)
  3. Compute going allowances per meeting from winner time deviations
  4. Calculate speed figures: winner figure from going-corrected time deviation
  5. Extend to all runners via cumulative beaten lengths
  6. Skip weight adjustment (data unavailable) — figures reflect actual performance
  7. Apply WFA from BHA scale where age data permits
  8. Validate correlation with TFig as target
""")


if __name__ == "__main__":
    print("TIMEFORM RACING DATA EVALUATION")
    print("=" * 70)
    print("\nLoading data...")
    df = load_all_data()

    overview(df)
    column_quality(df)
    race_type_analysis(df)
    geography_analysis(df)
    timing_analysis(df)
    timefigure_analysis(df)
    beaten_distance_analysis(df)
    sectional_analysis(df)
    course_distance_coverage(df)
    missing_data_assessment(df)
    key_findings(df)
