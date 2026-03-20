# Fix Plan: UK Ratings Issues from 2026-03-19 Audit

## Issue 1 (CRITICAL): Race 1 figures ~55 lbs below ORs — NEWCASTLE_10 course-distance offset

### Root Cause
The `NEWCASTLE_10` course-distance calibration offset is -15.40 lbs (largest negative AW offset). This is learned from residuals in `speed_figures.py:1444-1457` with Bayesian shrinkage (k=100). The raw mean residual for this combo must have been approximately -80 to -100 lbs, indicating the standard time at Newcastle AW 10f is significantly miscalibrated (approximately 5 seconds too slow).

### Fix Options

**Option A — Cap course-distance offsets (Quick, defensive)**
File: `src/speed_figures.py`, after line 1456
```python
cd_shrunk = cd_shrunk.clip(-10, 10)  # Cap at ±10 lbs
```
- Prevents extreme offsets while preserving valid corrections
- Minimal code change, no recalibration needed
- Downside: Doesn't fix the underlying standard time error; Race 1 figures would still be low (just less extreme)

**Option B — Increase shrinkage for sparse combos (Moderate)**
File: `src/speed_figures.py`, line 1446
```python
SHRINKAGE_K = 300  # was 100
```
- Forces all course-distance offsets closer to zero
- NEWCASTLE_10 with ~20 samples: offset becomes ~20/(20+300) × raw_mean ≈ -5 lbs (vs current -15)
- Downside: Weakens valid corrections everywhere

**Option C — Add minimum sample size (Targeted)**
File: `src/speed_figures.py`, after line 1456
```python
cd_shrunk = cd_shrunk.where(cd_counts >= 50, 0.0)  # Require n≥50
```
- Eliminates offsets for sparse combos entirely
- Downside: Loses legitimate corrections for valid but low-volume combos

**Option D — Recalibrate standard times (Root cause fix, requires pipeline re-run)**
- Investigate whether Newcastle AW 10f standard time needs updating
- May need to split by time period if there was a configuration change
- Requires re-running the full pipeline

### Recommendation
**Option A (cap at ±10 lbs)** as an immediate fix, combined with Option D as a follow-up investigation. The ±10 cap is defensive and won't harm well-calibrated combos (most offsets are <5 lbs).

---

## Issue 2 (HIGH): Finishers nulled by >20L beaten-length hard cutoff

### Root Cause
`src/live_ratings.py:1311-1320` sets `figure_calibrated = NaN` for any runner beaten >20 lengths. This is a hard exclusion applied AFTER calibration, contradicting the batch pipeline which uses soft attenuation (0.5x factor beyond 20L) and clips at 30L but **never nulls figures for finishers**.

### Affected Runners (2026-03-19)
- William Dewhirst (IRE): Race 3, 7th place, 24.15L beaten
- Antique Blue (IRE): Race 4, 11th place, 29.18L beaten

### Fix
File: `src/live_ratings.py`, lines 1311-1320

**Option A — Remove the hard cutoff entirely (Recommended)**
Delete lines 1311-1320. The attenuation in `_extend_to_all_runners()` already handles margin uncertainty, and the batch pipeline doesn't use this cutoff.

**Option B — Replace with confidence flag**
Replace the nulling with a flag column:
```python
beaten_far = (
    df["distanceCumulative"].notna()
    & (df["distanceCumulative"] > 20)
    & (df["positionOfficial"] != 1)
)
df.loc[beaten_far, "figure_confidence"] = "low"
```

### Recommendation
**Option A** — remove the cutoff. It contradicts the batch pipeline's design and destroys valid data. The beaten-length band offsets in calibration already correct for the known bias at large margins.

---

## Issue 3 (HIGH): Beaten-length ordering anomalies

### Root Cause
Horses beaten further sometimes get higher figures due to weight adjustments. This is **mathematically correct** — a horse carrying 142lb beaten 16L should rate higher than a horse carrying 122lb beaten 14L if the weight difference exceeds the BL difference.

### Assessment
Most anomalies are explained by legitimate weight differentials:
- **Race 5** (Emerald Harmony): 2nd carries +5lb, beaten 0.03L → fig correctly higher. **Not a bug.**
- **Race 4** (El Pinto): 4yo carrying 142lb (+20 vs 3yo field at 122lb) → +16lb weight adj overwhelms 1.75L extra BL. **Correct but counter-intuitive.**
- **Race 1** anomalies: Symptomatic of the NEWCASTLE_10 calibration issue; fixing Issue 1 should resolve these.

### Fix
No code fix needed — the calculations are correct. However, adding a diagnostic column would aid QA:

File: `src/live_ratings.py`, in the output preparation section
```python
df["rank_vs_position"] = df.groupby("race_id")["figure_calibrated"].rank(ascending=False) - df["positionOfficial"]
```

### Recommendation
Add the rank-vs-position diagnostic column for QA, but no formula changes.

---

## Issue 4 (MEDIUM, revised): Figure-OR gap is NOT a calibration bug

### Analysis
The initial audit flagged a -25.3 lbs mean gap between figures and official ratings. However, investigation of the batch pipeline's audit report shows:
- **AW bias against Timeform timefigure: -0.60 to -0.93 lbs** (excellent)
- **Overall correlation: 0.925, MAE: 6.73 lbs**
- The OOS corrections are applied in the correct direction

The -25.3 gap is because **official ratings and speed figures measure different things**:
- Speed figures = one-day time-based performance
- Official ratings = handicapper's assessment of longer-term ability
- A horse having a below-par day will get a low speed figure but retain its OR

### Fix
No code change needed. The audit report should note that figure-OR comparison is informational, not a validation metric. The true validation target is Timeform timefigure.

---

## Implementation Order

1. **Fix Issue 2** (remove >20L cutoff) — 1 line deletion, zero risk
2. **Fix Issue 1** (cap course-distance offsets) — 1 line addition, low risk
3. **Fix Issue 3** (add diagnostic column) — 1 line addition, zero risk
4. **Investigate** Newcastle AW 10f standard time for root cause fix
