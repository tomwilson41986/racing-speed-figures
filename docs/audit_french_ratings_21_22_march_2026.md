# Audit: French Ratings — 21 & 22 March 2026

**Date:** 2026-03-23
**Issue:** Calibrated figures consistently too high across both dates.

---

## Executive Summary

French ratings for 21-03-26 and 22-03-26 are systematically inflated. Class 6 winners
average 83–89 (expected ~55–65 on the Timeform scale); Class 5 winners average 85–86
(expected ~70–80). Three root causes were identified:

1. **Missing standard time** for A33 2300m Turf causes garbage figures (−265 to −290)
   that contaminate the March 22 calibration pool.
2. **Distance clamping** in `_interp_single()` silently returns the nearest known
   standard time instead of NaN when a race distance is far outside the known range.
3. **All-runner figures are uncapped**, allowing extreme outlier values to pass through
   to calibration even though winner figures are clipped to [−50, 250].

---

## Data Summary

### 22 March 2026

| Metric | Value |
|--------|-------|
| Total runners | 295 |
| Runners with figures | 272 |
| Runners with no data | 13 |
| Sane figures (−50 < f < 150) | 261, mean=72.8, median=74.2 |
| Insane figures | 11 (all from A33 R9 2300m: −265 to −291) |

**Winners by class (calibrated):**

| Class | Winner figures | Mean |
|-------|---------------|------|
| 1 | 76.3, 81.0, 75.4 | 77.6 |
| 4 | 87.4 | 87.4 |
| 5 | 85.4, 85.2, 94.6, 92.4, 86.8, 74.6, 94.1, 78.2, 83.5, 88.1 | **86.3** |
| 6 | 94.6, 84.0, 96.2, 71.5, 70.4 | **83.4** |
| 7 | 82.7, 77.9, 85.1, 86.0, 86.4, 86.6, 74.3 | **82.7** |

### 21 March 2026

| Metric | Value |
|--------|-------|
| Total runners | 264 |
| Runners with figures | 221 |
| Runners with no data | 22 (GPD — no standard times) |
| Sane figures | 221, mean=76.5, median=76.1 |

**Winners by class (calibrated):**

| Class | Winner figures | Mean |
|-------|---------------|------|
| 2 | 68.3 | 68.3 |
| 4 | 62.4, 85.8 | 74.1 |
| 5 | 75.7, 86.5, 71.7, 95.8, 69.8, 91.2, 92.1, 76.8, 90.9, 104.4 | **85.5** |
| 6 | 100.4, 103.4, 89.4, 90.7, 87.1, 70.4, 82.4, 88.3 | **89.0** |

---

## Root Cause Analysis

### CRITICAL 1 — Missing Standard Time: A33 2300m Turf

**File:** `output/france/standard_times.csv`

A33 only has standard times for 1000m, 1200m, 1400m, 1500m, 1600m on Turf.
No entry exists for 2300m. The `_interp_single()` function (line 538 of
`speed_figures.py`) **clamps** to the nearest known value (A33_1600_Turf =
97.97s) instead of returning NaN.

A 2300m race naturally finishes in ~146s. Using the 1600m standard (97.97s)
creates a deviation of +48s, which maps to a raw figure of approximately −400.

**Impact:**
- A33 R9 (11 runners) all received figures between −265 and −291.
- These 11 extreme outliers narrowed the March 22 IQR to 20.3 (vs 30.9 on
  March 21), producing a calibration scale of **1.195** — figures are
  *expanded* instead of compressed.
- Without the outliers, the IQR would be wider, producing a scale of ~0.7–0.8
  and bringing figures down by ~15–20 lbs.

**Fix:** Add an extrapolation guard in `_interp_single()` — return NaN when
the requested distance is more than 20% beyond the known range.

### CRITICAL 2 — All-Runner Figures Uncapped

**File:** `src/france/speed_figures.py`, lines 997–1003 vs Stage 5

Winner figures are clipped to [−50, 250] at line 1003, but the all-runner
extension (`compute_all_figures`) does not apply the same cap. The A33 R9
runners pass through uncapped, entering the calibration pool with figures
of −400+.

**Fix:** Apply the same [−50, 250] clip in `compute_all_figures()` after
beaten-length extension.

### HIGH 3 — Going Allowance Under-Compensation on Soft Ground

**Meeting:** LAT, 21-03-26, going = Souple

| Parameter | Value |
|-----------|-------|
| Empirical GA prior (Souple) | 0.30 s/f = 0.001491 s/m |
| Computed GA (shrunk) | 0.000941 s/m ≈ 0.189 s/f |
| Expected range | 0.25–0.40 s/f |

The Bayesian shrinkage formula `(n × raw + k × prior) / (n + k)` with `k = 3`
is working correctly, but the **raw GA is only 0.15 s/f** — roughly half the
prior. This means the meeting data shows winners systematically beating
standard times, consistent with either:

- Standard times for LAT_1900 being slightly slow (set from all-going data)
- Unusually fast ground despite "Souple" classification
- Data quality issues in finishing times

The result is that LAT 1900m winners (Class 6) receive raw figures of 124–130,
calibrating to 95–103 — figures appropriate for Group/Listed quality, not
Class 6 handicaps.

### HIGH 4 — Volatile Daily Calibration

| Date | Raw IQR | Robust Std | Scale | Shift |
|------|---------|-----------|-------|-------|
| 21-03-26 | 30.9 | 22.9 | 0.787 | +1.9 |
| 22-03-26 | 20.3 | 15.1 | 1.195 | −34.9 |

The March 22 scale exceeds 1.0 because the A33 R9 outliers compress the
middle of the distribution, making the IQR artificially narrow. A narrow IQR
relative to the target std of 18.0 produces scale > 1, which **expands**
figures rather than compressing them.

Even on March 21 (no outlier contamination), scale = 0.787 is insufficient.
A raw figure of 130 becomes 104.2 — still far too high for Class 6.

---

## Specific Race Flags

| Race | Issue | Raw → Cal | Expected |
|------|-------|-----------|----------|
| A33 R9 2300m Bon | No standard time → garbage | −402 → −266 | Should be suppressed |
| LAT R4 1900m Souple Cl6 | Winner 100.4 | 128.0 → 100.4 | ~55–65 |
| LAT R5 1900m Souple Cl6 | Winner 103.4 | 130.3 → 103.4 | ~55–65 |
| LAT R8 1900m Souple Cl5 | Winner 95.8 | 124.6 → 95.8 | ~65–75 |
| VIL R5 2400m PSF Cl5 | Winner 104.4 | 118.4 → 104.4 | ~70–80 |
| PON R4 1500m PSF Cl6 | Winner 96.2 | 116.4 → 96.2 | ~55–65 |
| MXD R8 1200m Bon Cl6 | Winner 94.6 | 112.3 → 94.6 | ~55–65 |
| SIO R1 2000m Collant Cl6 | Winner 89.4 | 116.2 → 89.4 | ~55–65 |

---

## Recommended Fixes

### Fix 1: Extrapolation guard in `_interp_single()` (CRITICAL)

Return NaN when the actual distance is more than 20% beyond the nearest known
standard time distance. This prevents the 1600m standard time from being used
for a 2300m race.

### Fix 2: Cap all-runner figures (CRITICAL)

Apply `clip(lower=-50, upper=250)` to all-runner figures after the beaten-length
extension, matching the existing winner-figure cap.

### Fix 3: Filter outliers before calibration (HIGH)

Before computing the IQR for calibration, exclude figures outside [−50, 250]
(or use only figures within 3 IQR of the median). This prevents extreme values
from distorting the scale/shift computation.

### Fix 4: Consider increasing GA shrinkage strength (MEDIUM)

With `k = 3`, the prior has limited influence when a meeting has 8+ races.
For going descriptions with strong empirical priors (Souple, Collant, Lourd),
consider a higher `k` (e.g., 5–6) to prevent under-compensation.

---

## Appendix: Calibration Parameters

**Target:** UK Timeform distribution (mean=72.0, std=18.0)

**Default fallback:** scale=0.700, shift=2.0

**March 21 computed:** scale=0.787, shift=+1.9 (221 runners)

**March 22 computed:** scale=1.195, shift=−34.9 (272 runners, contaminated by 11 outliers)
