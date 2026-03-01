# Model Baseline Performance Review

**Date:** 2026-03-01
**Pipeline:** `src/speed_figures.py` (11-stage pipeline with GBR enhancement)
**Dataset:** Timeform 2015–2026 (793,065 UK/IRE flat runners → 750,204 after surface-change cutoffs)
**Target:** Timeform published timefigure
**Calibration window:** 2015–2023 (fit), 2024–2026 (out-of-sample)

---

## Overall Metrics (684,322 rows with valid timefigure)

| Metric | Value |
|---|---|
| **Correlation** | 0.9211 |
| **MAE** | 6.64 lbs |
| **RMSE** | 8.74 lbs |
| **Bias** | +0.45 lbs |

### Error Distribution

| Tolerance | % Within |
|---|---|
| ±1 lb | 9.7% |
| ±2 lbs | 19.1% |
| ±3 lbs | 28.2% |
| ±5 lbs | 44.6% |
| ±10 lbs | 72.9% |
| ±15 lbs | 85.6% |
| ±20 lbs | 90.5% |

---

## Breakdown by Surface

| Surface | Correlation | MAE | N |
|---|---|---|---|
| **Turf** | 0.9092 | 7.32 | 425,857 |
| **All Weather** | 0.9376 | 5.54 | 258,465 |

All Weather outperforms Turf by ~1.8 lbs MAE, likely because AW surfaces are more consistent (no going variation).

---

## Breakdown by Finishing Position

| Position | Correlation | MAE | N |
|---|---|---|---|
| 1st (winners) | 0.8891 | 6.79 | 72,921 |
| 2nd | 0.8892 | 6.72 | 72,762 |
| 3rd | 0.8925 | 6.71 | 72,700 |
| 4th | 0.8962 | 6.72 | 72,016 |
| 5th | 0.9000 | 6.71 | 69,794 |

Notably flat across positions — no significant degradation for beaten horses. Mid-field runners actually have marginally *better* MAE, possibly because extreme winners/losers have more variance.

---

## Breakdown by Year

| Year | Correlation | MAE | N |
|---|---|---|---|
| 2015 | 0.9254 | 6.79 | 55,785 |
| 2016 | 0.9229 | 6.61 | 57,410 |
| 2017 | 0.9270 | 6.71 | 60,489 |
| 2018 | 0.9310 | 6.44 | 60,766 |
| 2019 | 0.9312 | 6.31 | 60,548 |
| 2020 | 0.9302 | 6.13 | 52,796 |
| 2021 | 0.9281 | 6.35 | 64,309 |
| 2022 | 0.9312 | 6.31 | 65,475 |
| 2023 | 0.9218 | 6.66 | 67,632 |
| **2024** | **0.9076** | **7.19** | **69,255** |
| **2025** | **0.9020** | **7.38** | **64,692** |
| **2026** | **0.9040** | **6.68** | **5,165** |

2024–2026 are **out-of-sample** (calibration/GBR fitted on ≤2023). Performance is stable — the OOS degradation of ~0.6 lbs vs in-sample reflects expected generalisation loss from the GBR layer.

---

## Scale Comparison

| Statistic | Our Figures | Timeform |
|---|---|---|
| Mean | 53.9 | 50.1 |
| Median | 54.7 | 52.0 |
| Std Dev | 20.4 | 26.0 |

Our distribution is compressed compared to Timeform (lower std: 20.4 vs 26.0, ratio 0.785), meaning
we under-spread the tails. High-rated horses are under-rated and low-rated horses over-rated
relative to Timeform's scale. The GBR enhancement reduces overall MAE but doesn't fully resolve
compression because regression-based models inherently attenuate extreme predictions.
See the Compression Analysis section for detailed findings.

---

## All-Weather Track Audit

### Chelmsford City (post-surface change, from Sept 2022)

| Year | N | MAE | RMSE | Bias | Corr | ±3 | ±5 | ±10 |
|---|---|---|---|---|---|---|---|---|
| 2022 | 1,222 | 4.33 | 5.75 | -0.30 | 0.955 | 43.2% | 66.9% | 90.3% |
| 2023 | 2,937 | 5.47 | 7.12 | +0.49 | 0.934 | 38.5% | 55.6% | 84.9% |
| 2024 | 2,708 | 5.72 | 7.32 | +0.40 | 0.924 | 33.6% | 53.2% | 85.3% |
| 2025 | 2,242 | 6.20 | 8.19 | +0.05 | 0.915 | 31.6% | 52.3% | 80.7% |
| 2026 | 366 | 6.34 | 7.98 | +1.47 | 0.925 | 28.7% | 44.3% | 78.7% |
| **Total** | **9,475** | **5.60** | **7.32** | **+0.30** | **0.929** | **35.7%** | **55.2%** | **84.5%** |

### Southwell (post-surface change, from Jan 2022)

| Year | N | MAE | RMSE | Bias | Corr | ±3 | ±5 | ±10 |
|---|---|---|---|---|---|---|---|---|
| 2022 | 3,273 | 5.81 | 7.41 | +0.84 | 0.934 | 31.5% | 52.8% | 84.9% |
| 2023 | 3,233 | 5.88 | 7.81 | +0.91 | 0.923 | 34.9% | 55.2% | 83.8% |
| 2024 | 4,339 | 6.25 | 8.01 | -0.87 | 0.915 | 30.1% | 47.4% | 82.3% |
| 2025 | 4,173 | 6.83 | 8.91 | -3.08 | 0.917 | 29.3% | 46.1% | 77.9% |
| 2026 | 889 | 6.38 | 8.21 | -1.44 | 0.921 | 29.2% | 47.4% | 80.5% |
| **Total** | **15,907** | **6.24** | **8.11** | **-0.77** | **0.920** | **31.1%** | **49.8%** | **81.9%** |

### Other AW Tracks (Kempton, Lingfield, Newcastle, Wolverhampton)

| Year | N | MAE | RMSE | Bias | Corr | ±3 | ±5 | ±10 |
|---|---|---|---|---|---|---|---|---|
| Total | 219,800 | 6.74 | 8.75 | +0.56 | 0.907 | 29.2% | 46.5% | 77.4% |

---

## Calibration Details

### Turf
- `timefigure ≈ 0.8230×fig + -0.000733×(fig-181)² + -92.51` (quadratic)
- Class offsets: C1 +20.1, C2 +15.4, C3 +5.9, C4 -0.2, C5 -5.2, C6 -11.8, C7 -19.7
- Going offsets: Firm -2.7, GdFm -0.8, Good -0.2, GdSft +0.5, Soft +0.5, Heavy +3.1
- Continuous GA coefficient: -2.33 lbs per s/f
- BL offsets: winner +2.8, 0-1L +1.7, 1-3L +1.3, 3-5L +0.8, 5-10L -0.2, 10-15L -1.5, 15-20L -2.9
- Age offsets: age2 -0.6, age3 +0.9, age4 -0.6, age5 -0.8, age6 -0.6, age7 +0.2,
  age8 +0.7, age9 +1.1, age10 +1.4, age11 +1.6, age12 +1.5
- 268/297 course×distance combos with |offset| > 0.5 lbs

### All Weather
- `timefigure ≈ 0.8753×fig + -0.000450×(fig-192)² + -119.09` (quadratic)
- Class offsets: C1 +14.2, C2 +10.5, C3 +5.7, C4 +3.0, C5 +0.4, C6 -3.1, C7 -3.8
- Going offsets: Firm +1.1, GdFm +0.3, Good +0.2, GdSft -0.5
- Continuous GA coefficient: -10.88 lbs per s/f
- BL offsets: winner +1.1, 0-1L +0.5, 1-3L +0.6, 3-5L +0.7, 5-10L +0.9, 10-15L +0.9, 15-20L +1.3
- Age offsets: age2 -0.8, age3 +0.1, age4 +0.6, age5 -0.2, age6 -0.1, age7 -0.1,
  age8 -0.1, age9 -0.0, age10 -0.2, age11 -0.5, age12 -0.6
- 50/53 course×distance combos with |offset| > 0.5 lbs

---

## Weight-for-Age (WFA) Analysis

### Current Implementation

The pipeline uses **empirical WFA tables** derived from Timeform 2015–2023 data, separate
for Turf and All Weather (Stage 7 in `speed_figures.py`). The methodology:

- Baseline: 4–6 year-olds (zero allowance)
- Calibrated the mean residual (pipeline figure vs Timeform timefigure) for 2yo/3yo at
  each (month, distance) cell
- Smoothed with a 3-month weighted average, rounded to integers
- Negative residuals capped at 0
- Older-horse decline (7+) modelled on Turf only

### Empirical Tables vs BHA Official Scale

The BHA 2025 unified European scale (reviewed by Dominic Gardiner-Hill, approved by the
European Pattern Committee in August 2024) sets the following 3yo allowances for
representative distances:

| Month | BHA 5f | Ours (Turf) | BHA 8f | Ours (Turf) | BHA 12f | Ours (Turf) |
|-------|--------|-------------|--------|-------------|---------|-------------|
| Jan   | ~15    | 10          | ~14    | 9           | ~11     | 5           |
| Mar   | ~12    | 10          | ~12    | 9           | ~10     | 4           |
| May   | ~9     | 7           | ~10    | 10          | ~7      | 3           |
| Jul   | ~5     | 7           | ~7     | 3           | ~5      | 4           |
| Sep   | ~1     | 5           | ~3     | 5           | ~3      | 2           |
| Nov   | 0      | 3           | ~1     | 4           | ~2      | 3           |

**Key differences:**

1. **Our scale is flatter** — lower allowances in winter (Jan–Mar) and higher in autumn
   (Sep–Nov) compared to the BHA scale. This is expected: we are fitting against
   Timeform's timefigure, which already incorporates Timeform's own WFA adjustments,
   so our empirical table captures the *residual* WFA not already reflected in the target.

2. **Non-monotonic patterns** — Several cells show unexpected reversals (e.g., 3yo Turf
   at 8f: month 5 = 10 lbs but month 6 = 7 lbs, then month 7 = 3 lbs). These likely
   reflect sample-size noise in specific (month, distance) cells rather than genuine
   maturation patterns.

3. **AW allowances are higher** — Our AW 3yo allowances are 2–5 lbs greater than Turf
   (e.g., Jan 5f: 13 vs 10), consistent with industry understanding that younger horses
   are at a greater disadvantage on artificial surfaces.

### Comparison with Industry Approaches

| Approach | Description | Our Position |
|----------|-------------|--------------|
| **BHA Official** | Unified European scale, fortnightly intervals, ~15 lbs at 5f in Jan for 3yo | Not used directly — our empirical table replaces it |
| **Timeform (Rowlands)** | Proprietary WFA scale used internally; Timeform states "a 120-rated 3yo is equivalent to a 120-rated 4yo" after their WFA | Our target (timefigure) already incorporates Timeform's WFA, so our adjustment captures the residual |
| **Racing Post (Edwards)** | Less generous to 2yo early season; RP WFA not publicly available | Aligns with our finding of lower 2yo allowances vs BHA |
| **Raceform** | Simple monthly scale in lengths (e.g., 2yo May/Jun: -10 lengths) | Less granular than our distance-specific approach |
| **Empirical (best practice)** | Derive from own data by comparing same-horse performances across ages | Partially what we do, though via residual fitting rather than paired comparisons |

### Empirical WFA Residual Analysis

Comprehensive analysis of residuals by age group, month, distance, and surface
(`scripts/analyse_wfa.py` on 638,110 valid runners):

**Residuals by age group (after WFA):**

| Age | Bias | MAE | N |
|-----|------|-----|---|
| 2 | +1.03 | 7.74 | 117,009 |
| 3 | -0.12 | 7.96 | 200,120 |
| 4 | +0.61 | 7.97 | 122,422 |
| 5 | +1.05 | 7.98 | 75,896 |
| 6 | +0.98 | 7.97 | 49,409 |
| 7 | +0.58 | 7.90 | 33,021 |
| 8 | +0.12 | 7.98 | 20,360 |
| 9 | -0.25 | 7.82 | 11,443 |
| 10 | -0.53 | 7.76 | 5,412 |
| 11 | -1.31 | 8.10 | 2,128 |
| 12 | -1.11 | 7.88 | 701 |

**3yo month × distance patterns:**
- Turf sprints (5-6f): positive bias +1 to +6 lbs (WFA too generous)
- Turf stayers (12f+): negative bias -2 to -10 lbs (WFA too low)
- AW sprints (5-6f): consistent +3 to +5 bias across all months
- AW stayers (10-12f): consistent -2 to -6 bias across all months
- This distance gradient reflects the "stamina assumption" flaw noted by drawbias.com

**2yo patterns:**
- Turf: overall +1.0 bias, strongest at sprints
- AW: consistent +1 to +4 bias, especially at 5-6f

**Non-monotonic WFA table entries:** 107 pairs across all 4 tables where shorter
distances have higher WFA than longer distances. This is physically implausible
(younger horses are MORE disadvantaged at longer distances).

**Year-over-year drift:**
- 3yo bias drifted from -2.15 (2015) to +2.35 (2023), then +1.82 (2024)
- 2yo bias drifted from -4.76 (2015) to +4.80 (2023), then +4.01 (2024)
- This suggests 2yo/3yo horses have been getting relatively stronger over time,
  or that Timeform's own WFA adjustments have drifted

**Fortnightly resolution test:** Within-month 1st-half vs 2nd-half bias differences
are moderate (typically 0.5-2.0 lbs), with the largest at 3yo February (-3.3 lbs)
and 2yo July (+1.5 lbs). Monthly resolution is adequate for most cells.

### WFA Improvements Implemented

1. **Reduced older horse Turf decline** — The original decline
   `{7: -1, 8: -2, 9: -3, 10: -4, 11: -6, 12: -6}` was too aggressive at ages
   9-12, producing strongly negative residuals (-1.0 to -3.5 lbs). Updated to
   `{7: -1, 8: -1.5, 9: -2, 10: -2.5, 11: -3, 12: -3}`. The optimal values
   (computed as current_decline − residual_bias) are: 7→-1.6, 8→-1.7, 9→-2.0,
   10→-2.3, 11→-2.8, 12→-2.5.

2. **Per-age calibration offset** — Added a per-age-group shrinkage-regularised
   residual correction in the calibration layer (after the beaten-length offset).
   This captures age-specific biases that the WFA tables don't fully correct:
   - Turf: ages 4-6 get -0.7 to -0.9 (figures were too high), ages 9-12 get
     +0.8 to +1.2 (figures were too low even after decline fix)
   - AW: 2yo gets -0.7 (WFA was too generous), ages 11-12 get -0.5 to -0.6
     (mild AW decline captured implicitly)

### Identified WFA Issues (Remaining)

1. **3yo distance gradient** — The month×distance bias pattern (sprint over-
   compensation, stayer under-compensation) is an age×distance interaction that
   cannot be captured by a per-age offset alone. Fixing the WFA tables directly
   would require re-derivation with monotonic distance constraints.

2. **2yo early-season inflation** — The BHA scale allows up to 41 lbs for 2yo in
   March; our 2yo Turf table shows 30 lbs at 5f. Very few 2yo races exist before
   May, so early-season cells have minimal data and should be treated with caution.

3. **Non-monotonic WFA tables** — 107 non-monotonic pairs across all 4 tables.
   A monotonic distance constraint would produce more physically plausible values.

4. **Year-over-year drift** — The 2yo bias has drifted by ~9 lbs from 2015 to 2023.
   A temporal decay or recency weighting when deriving WFA tables could help.

5. **3yo at 14f+ under-corrected** — The 2025 BHA review found persistent 3yo
   overperformance at 14f+ and reduced allowances by ~1 lb. Our empirical table
   shows biases of -4 to -12 lbs at 14-16f on Turf, confirming under-correction.

### Recommendations

1. **Re-derive WFA tables with monotonic distance constraint** — Enforce that WFA
   allowances are non-decreasing with distance (at fixed age/month). Use isotonic
   regression on the optimal values from the residual analysis.

2. **Use paired-horse methodology** — Supplement the residual-fitting approach with
   direct paired comparisons of horses that raced as both 2yo/3yo and 4yo+. This
   is more robust to target-scale artefacts.

3. **Validate against Timeform's `tfwfa` field** — Compare our empirical WFA
   allowances against Timeform's published per-race WFA weights.

4. **Consider recency-weighted WFA derivation** — Weight recent years more heavily
   when computing WFA tables, to account for the significant year-over-year drift.

5. **Audit 2yo tables before May** — Flag or suppress cells with < 100 runners, as
   early-season 2yo data is too sparse for reliable empirical derivation.

---

## Standard Times Analysis

### Current Implementation

Standard times are the bedrock of the pipeline — every figure is a deviation from
the standard time at that course × distance × surface. The current method:

1. **Winners only** — all winners from 2015-2026 (excluding maiden and 2yo-only races)
2. **Good going preferred** — initial computation uses only good/standard going winners;
   iterative refinement then uses going-corrected times from ALL goings
3. **Class adjustment** — constant Class 4 baseline applied (empirically, varying class
   adjustments hurt accuracy: CV 0.0216 vs 0.0142 for constant)
4. **Median** for central tendency (robust to outliers)
5. **Minimum 20 races** per combo (raised from 15 based on analysis)
6. **3 iterations** of standard time ↔ going allowance refinement

### What Races Are Used?

**Current filtering (after improvements):**
- All winners EXCEPT maiden races (raceCode `P*`, `S*`) and 2yo-only races
- Maidens run ~0.11s slower than non-maiden winners at the same course/distance
- 2yo-only races run ~0.14s slower than open-age races

**Winner breakdown by race type:**

| Race Type | Count | % of Winners |
|-----------|-------|--------------|
| Handicap (I*) | 51,276 | 65.6% |
| Group/Listed (G*, L*) | 14,716 | 18.8% |
| Maiden (P*, S*) — excluded | 10,707 | 13.7% |
| Claimer (E*) | 749 | 1.0% |
| Other | 724 | 0.9% |

**Industry best practice** (Rowlands, Mordin, Racing Forum consensus) recommends
handicaps confined to older horses as the most reliable source.  Our approach of
excluding maidens and 2yo-only races while keeping all other types is a practical
compromise that preserves sample size while removing the two noisiest categories.

### Standard Time Comparison Under Different Race Selections

Tested six race selections on good-going winners. Deviation from the all-winners
baseline (before the maiden exclusion was implemented):

| Selection | Combos | Mean Δ (seconds) | MAD | Max |Δ| |
|-----------|--------|-------------------|-----|---------|
| All winners (baseline) | 324 | — | — | — |
| Handicap only | 263 | -0.110 | 0.215 | 1.43 |
| No maidens | 303 | -0.112 | 0.145 | 1.38 |
| Hcp + Group only | 300 | -0.123 | 0.163 | 1.47 |
| No 2yo | 306 | -0.138 | 0.140 | 1.29 |
| 3yo+ handicap only | 257 | -0.146 | 0.243 | 1.43 |

All filtered selections produce **faster** standard times (negative Δ), confirming
that maidens and 2yo races slow the standards.  "No maidens" gives the best trade-off:
low MAD (0.145) with high coverage (303 combos).

### Sample Size Impact

MAE degrades sharply for combos with few races in the standard:

| N Races | MAE | RMSE | Runners |
|---------|-----|------|---------|
| 15-20 | 12.21 | 23.58 | 1,535 |
| 20-30 | 11.18 | 14.59 | 3,463 |
| 30-50 | 10.93 | 15.99 | 6,108 |
| 50-100 | 9.60 | 12.70 | 35,486 |
| 100-200 | 8.81 | 11.49 | 104,401 |
| 200-500 | 8.06 | 10.58 | 260,846 |
| 500+ | 6.66 | 8.61 | 165,177 |

Raising MIN_RACES from 15 to 20 drops only 19 unreliable combos (mostly small Irish
tracks like Bellewstown, Clonmel, Sligo) with marginal MAE improvement.

### Standard Time Drift Over Time

87 of 231 analysed combos show annual drift > 0.1 s/yr; 55 have R² > 0.3.

Notable drifting tracks:
- **Wolverhampton** — getting 0.06-0.11 s/yr faster (R² 0.36-0.59), likely surface wear
- **Lingfield Park** — 5f getting 0.05 s/yr faster (R² 0.44)
- **Kempton Park 8f** — 0.09 s/yr slower (R² 0.14), possibly new rail positions

Recency-weighted standard times (exponential half-life) were tested but showed only
marginal benefit (MAD ~0.4s from unweighted), suggesting the current approach of
using all years pooled is adequate.  A future improvement could be a rolling 5-year
window for tracks with significant drift.

### Class Adjustment Validation

Three approaches tested on 25,232 GB winners with raceClass:

| Method | Median CV | Notes |
|--------|-----------|-------|
| No adjustment | 0.0152 | Worse than constant |
| **Constant C4 (current)** | **0.0142** | Best — lowest CV |
| Varying by class | 0.0216 | Significantly worse |

The varying class adjustment introduces MORE noise because the raw class labels don't
perfectly capture ability differences.  The constant approach is correct — winners across
classes, after going correction, naturally cluster around the same median.

### Outlier Analysis

| Central Tendency | MAD vs Median | Max |Δ| |
|-----------------|---------------|---------|
| Mean | 0.215 | 2.28 |
| Trimmed mean 10% | 0.159 | 1.20 |
| Winsorized mean 10% | 0.187 | 1.29 |
| **Median (current)** | **0.000** | **0.000** |

The median remains the best choice. Mean is sensitive to outliers (Musselburgh 16f
shows a 2.28s gap between mean and median). Trimmed/winsorized means offer no advantage
over the simpler median.

### Impact of Standard Time Improvements

| Metric | Before Std Times | After Std Times | Change |
|--------|------------------|-----------------|--------|
| **Correlation** | 0.8862 | **0.8866** | +0.0004 |
| **MAE** | 7.93 | **7.92** | -0.01 |
| **RMSE** | 10.50 | **10.47** | -0.03 |
| **Std combos** | 413 | 381 | -32 |

---

## Lbs-Per-Length (LPL) Analysis

### Current Implementation

LPL is computed **per course × distance × surface** (381 combos). The method:

1. Generic LPL from distance: `lpl = 0.2 × 20 × (5 / distance_furlongs)` (reduced
   from 22 to 20 lbs/sec based on empirical analysis)
2. Surface multiplier: Turf ×1.0, AW ×1.10 (AW has shallower distance decline)
3. Course correction: `mean_spf / this_course_spf` (faster courses → higher lpl)
4. Beaten lengths converted via: `lbs_behind = cumulative_bl × course_lpl`

### LPL Distribution

| Distance | Generic (Turf) | Generic (AW) | Empirical (TF pairwise) |
|----------|----------------|--------------|------------------------|
| 5f | 4.00 | 4.40 | 4.00 |
| 8f | 2.50 | 2.75 | 2.50 |
| 12f | 1.67 | 1.83 | 1.72 |
| 16f | 1.25 | 1.38 | 1.41 |

### International Comparison

| Source | 5f lpl | 8f lpl | Formula | Notes |
|--------|--------|--------|---------|-------|
| **Our pipeline** | 4.00 | 2.50 | 20/distance | Empirical fit to Timeform |
| **BHA handicapper** | ~3.00 | ~1.88 | 15/distance | Official, conservative |
| **Timeform (Rowlands)** | ~4.17 | ~2.60 | 1500/(time×LPS) | 25 lbs/sec at 60s |
| **UK forum (200/time)** | ~3.37 | ~2.00 | 200/std_time | Course-specific |
| **Beyer (US)** | ~3 pts/L | ~2 pts/L | LPSF from time | Sprint/route split |
| **Ragozin (US)** | ~2.5 | ~1.5 | Proprietary | Different scale |
| **HKJC** | 3.0 | 2.0 | Step table | HK rating scale (0-130) |

**BHA LPS (Lengths Per Second) Scale — going-dependent:**

| Going (Flat Turf) | LPS | Sec/Length |
|---|---|---|
| Firm / Good to Firm | 6 | 0.167 |
| Good to Soft | 5.5 | 0.182 |
| Soft / Heavy | 5 | 0.200 |
| AW Polytrack | 6 | 0.167 |
| AW Fibresand | 5 | 0.200 |

Note: official beaten lengths are ALREADY converted using going-dependent LPS before
we receive them, so our LPL does not need going adjustment.

### Empirical LPL Derivation

Computed optimal LPL from 505K winner-vs-beaten pairs as median(tf_diff / bl):

| Distance | Empirical | Old (22/d) | Old Error | New (20/d Turf) | New Error |
|----------|-----------|------------|-----------|-----------------|-----------|
| 5f | 4.00 | 4.40 | +10.0% | 4.00 | 0.0% |
| 6f | 3.33 | 3.67 | +10.0% | 3.33 | 0.0% |
| 8f | 2.50 | 2.75 | +10.0% | 2.50 | 0.0% |
| 10f | 2.00 | 2.20 | +10.0% | 2.00 | 0.0% |
| 12f | 1.72 | 1.83 | +6.4% | 1.67 | -3.3% |
| 14f | 1.50 | 1.57 | +4.7% | 1.43 | -4.7% |

Power law fit: Turf `lpl = 17.84 / distance^0.941` (R²=0.986), AW `lpl = 14.97 /
distance^0.849` (R²=0.983). The AW exponent (0.849) is lower, meaning AW LPL declines
less steeply with distance — hence the AW ×1.10 multiplier.

### Pairwise Spread Analysis

| Level | Old Slope | New Slope | Interpretation |
|-------|-----------|-----------|----------------|
| **Raw Turf** | 0.93 | **1.02** | Was 7% over-spread → now ~correct |
| **Raw AW** | 0.93 | **0.94** | Preserved (AW ×1.10 restores old effective LPL) |
| **Calibrated overall** | 1.077 | **1.071** | Closer to 1.0 |
| **Calibrated Turf** | 1.104 | **1.093** | Closer to 1.0 |

Going-dependent LPL was tested empirically: raw pairwise slopes are identical across
all going conditions (0.92-0.94), confirming that going-dependent LPL is NOT needed.
This is because official beaten lengths already embed the BHA's going-dependent LPS
conversion (6 LPS on firm, 5 on soft).

### Beaten-Length Bias (Before/After Correction)

Analysis revealed systematic position-dependent bias: horses beaten further were
consistently over-rated (positive residual), while winners were under-rated.

**Before correction (bias by beaten lengths):**

| BL Range | Bias | MAE |
|----------|------|-----|
| 0-1L | -0.11 | 8.03 |
| 3-5L | +0.00 | 7.95 |
| 5-10L | +0.57 | 7.86 |
| 10-15L | +1.59 | 8.03 |
| 15-20L | +2.69 | 8.36 |

**After correction (per-BL-band residual offsets in calibration):**

| BL Range | Bias | MAE |
|----------|------|-----|
| 0-1L | +0.76 | 7.98 |
| 3-5L | +0.39 | 7.93 |
| 5-10L | +0.45 | 7.85 |
| 10-15L | +0.46 | 7.92 |
| 15-20L | +0.42 | 7.99 |

The correction flattened the bias from a -0.11 to +2.69 range → uniformly ~+0.4.

**Calibration BL offsets (Turf):** winner +2.2, 0-1L +1.2, 1-3L +1.0, 3-5L +0.5,
5-10L -0.3, 10-15L -1.8, 15-20L -3.4

**AW BL offsets:** near zero across all bands (AW LPL already well-calibrated).

### Impact on Overall Metrics

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| **Correlation** | 0.8836 | **0.8862** | +0.0026 |
| **MAE** | 7.99 | **7.93** | -0.06 |
| **RMSE** | 10.57 | **10.50** | -0.07 |
| **Turf MAE** | 8.81 | **8.71** | -0.10 |
| **AW MAE** | 6.67 | **6.66** | -0.01 |
| **Std Dev** | 20.8 | **21.3** | +0.5 (closer to TFig 26.0) |

### Root Cause of the Turf BL Bias

The Turf beaten-length bias likely comes from two sources:

1. **Judge's margin compression** — Official margins are conversions of time lapses
   (since 1997), but the judge's estimation of physical margins at the finish compresses
   at larger distances.  A "20 lengths" call may actually represent 22-24 lengths of
   time lapse.  Rowlands has noted this discrepancy.

2. **Eased horses** — Horses beaten 10+ lengths are often eased by their jockeys in the
   final furlong.  The official margin understates what the true ability gap was.

On AW, the effect is minimal because AW fields are more compressed (tighter ability
range, more consistent surfaces).

### LPL Improvement Impact

| Metric | Before LPL | After LPL | Change |
|--------|-----------|-----------|--------|
| **Correlation** | 0.8871 | **0.8876** | +0.0005 |
| **MAE** | 7.90 | **7.88** | -0.02 |
| **RMSE** | 10.44 | **10.42** | -0.02 |
| **Turf MAE** | 8.68 | **8.65** | -0.03 |
| **AW MAE** | 6.65 | **6.65** | 0.00 |

### Remaining Improvement Opportunities

1. **Non-linear calibration for Turf** — The quadratic term was not selected for Turf
   (linear MSE was not sufficiently worse).  The remaining 9% pairwise under-spread
   after calibration is driven by the linear slope (0.76) compressing the distribution.

2. **Distance exponent refinement** — The empirical power law exponent is 0.941 (Turf)
   and 0.849 (AW) vs the current 1.0.  At 14f+ the `20/distance` formula under-
   estimates by 5-10%.  A future improvement could use the power law directly.

3. **Velocity-based LPL** — Computing `lpl = K / standard_time` per course×distance
   automatically adjusts for course speed but K is not constant (increases from
   258 at 5f to 296 at 12f).  The course correction factor already captures most
   of this effect.

---

## Going Allowance Analysis

### Current Implementation

Going allowances (GA) are computed per meeting (track × date × surface) as the winsorized
mean of per-furlong deviations from standard time across winners on the card. Convention:
positive GA = ground slower than standard (soft), negative = faster (firm).

### GA Distribution by Official Going

| Going | Mean GA (s/f) | Std | N |
|-------|---------------|-----|---|
| Firm | -0.147 | 0.133 | 20,731 |
| Good to Firm | -0.093 | 0.124 | 129,701 |
| Good | +0.049 | 0.123 | 223,798 |
| Good to Soft | +0.176 | 0.157 | 195,947 |
| Soft | +0.514 | 0.195 | 43,856 |
| Heavy | +0.818 | 0.202 | 24,077 |

The GA values align well with official going descriptions, showing a clear monotonic
progression from firm (-0.15 s/f) to heavy (+0.82 s/f).

### Per-Race vs Per-Meeting GA

| Metric | Value |
|--------|-------|
| Between-meeting variance | 0.0714 |
| Mean within-meeting variance | 0.0274 |
| Ratio (between/within) | 2.60 |

Meeting-level GA captures the majority of going variance. Simulating per-race GA
(using individual race deviations instead of meeting average) **worsened** figures by
+0.55 MAE. Per-meeting is the correct granularity — per-race corrections collapse
into "form ratings" by adjusting each race individually.

### Distance-Dependent Going

No significant distance-dependent going effect detected. Sprint vs long race GA
difference: mean -0.002 s/f (N=8,222 meetings). The convention of expressing GA
in seconds-per-furlong already scales correctly with distance.

### Card Size Reliability

| Card Size | Bias | MAE | N |
|-----------|------|-----|---|
| 1-3 races | -2.85 | 9.46 | 2,753 |
| 4 races | -0.12 | 8.52 | 5,020 |
| 5 races | -0.19 | 8.87 | 10,488 |
| 6 races | +1.03 | 8.46 | 77,599 |
| 7 races | +0.53 | 8.20 | 263,145 |
| 8+ races | +0.35 | 7.14 | 279,105 |

Cards with 1-3 races show significant negative bias (-2.85 lbs) and higher MAE, but
represent only 0.4% of runners. The current MIN_RACES=3 threshold is adequate.

### Time-Based vs Official Going

| Metric | Official Going Groups | Time-Based Groups |
|--------|----------------------|-------------------|
| Residual MAE | 8.425 | 7.870 |
| Exact agreement | — | 30.4% |

Time-based going classifications (from GA values) are significantly more accurate than
official going descriptions. Only 30.4% of runners have identical official and time-based
going classifications. The pipeline already uses time-derived GA, not official descriptions.

### Continuous GA Calibration Correction

**Key improvement implemented:** Added a continuous linear correction based on the actual
GA value in the calibration layer, supplementing the categorical going group offsets.

**Before:** Residual bias by GA quartile ranged from -1.30 (fast ground, Q1) to +1.82
(slow ground, Q4) — a spread of 3.12 lbs.

**After:** Bias range reduced to -0.35 (Q1) to +1.01 (Q3) — spread of 1.36 lbs (56%
reduction).

| GA Quartile | Mean GA | Bias (before) | Bias (after) |
|-------------|---------|---------------|--------------|
| Q1 (fast) | -0.146 | -1.30 | -0.35 |
| Q2 | +0.013 | +0.17 | +0.68 |
| Q3 | +0.132 | +1.31 | +1.01 |
| Q4 (slow) | +0.456 | +1.82 | +0.58 |

Coefficients: Turf -2.33 lbs per s/f, AW -10.88 lbs per s/f. The larger AW coefficient
reflects the fact that AW going variation (Standard to Slow) has a larger residual effect
that the 4-category going offset (Firm, GdFm, Good, GdSft) doesn't capture.

### Going Allowance Improvement Impact

| Metric | Before GA | After GA | Change |
|--------|-----------|----------|--------|
| **Correlation** | 0.8876 | **0.8896** | +0.0020 |
| **MAE** | 7.88 | **7.79** | -0.09 |
| **RMSE** | 10.42 | **10.33** | -0.09 |
| **Turf MAE** | 8.65 | **8.62** | -0.03 |
| **AW MAE** | 6.65 | **6.45** | -0.20 |

### What Was Tested But Didn't Help

1. **Excluding maiden/2yo races from GA** — Reduced GA meetings from 10,625 to 10,463,
   losing coverage. MAE worsened by +0.03.

2. **Per-race GA** — Simulated per-race corrections worsened MAE by +0.55. Meeting-level
   GA is optimal.

3. **GA shrinkage for small cards** — Adding Bayesian shrinkage toward zero for meetings
   with few races worsened overall MAE despite reducing small-card bias.

### Research Context (International Best Practices)

Based on research into Timeform (UK), Beyer (US), and HKJC approaches:

- **Timeform** derives GA mathematically from race times vs expected ratings (not official
  descriptions). Uses 6-7 races with 4+ from established horses. Applies wind vector
  analysis and rail movement corrections. Abandons figures when conditions are too extreme.

- **Beyer** computes a "track variant" per surface per day. Uses projected figures (horse
  histories) rather than class pars. Separate dirt and turf variants always. Human judgment
  plays a significant role.

- **HKJC** uses a fixed going scale with step tables. Less sophisticated than UK/US.

- **Consensus:** Per-meeting is the correct granularity. Time-based going is universally
  preferred over official descriptions. Wind and rail corrections are the next frontier
  for accuracy but require course-specific data we don't currently have.

---

## Compression Analysis

### Overview

Our figures have std 21.3 vs Timeform's 22.4 (compression ratio 0.948). This means
high-rated horses are systematically under-rated and low-rated horses over-rated.

### Compression by Timeform Rating Level

| TF Band | Our Mean | TF Mean | Diff | Bias | N |
|---------|----------|---------|------|------|---|
| <0 | 0.5 | -12.0 | +12.5 | +12.51 | 8,365 |
| 0-20 | 19.7 | 12.9 | +6.8 | +6.82 | 38,821 |
| 20-40 | 35.7 | 32.0 | +3.6 | +3.63 | 124,571 |
| 40-60 | 51.6 | 50.9 | +0.8 | +0.76 | 220,473 |
| 60-80 | 67.5 | 69.4 | -1.9 | -1.89 | 177,906 |
| 80-100 | 83.5 | 88.0 | -4.5 | -4.48 | 59,205 |
| 100-120 | 97.2 | 106.5 | -9.3 | -9.29 | 8,568 |
| 120+ | 109.5 | 123.6 | -14.1 | -14.06 | 171 |

The compression is nearly perfectly symmetric around TF 40-60 (the centre of the distribution).
At the top (TF 120+), we under-rate by 14.1 lbs. At the bottom (TF <0), we over-rate by 12.5 lbs.

### Local Calibration Slopes

The regression slope between our figures and Timeform decreases at higher figure levels,
confirming the compression is worse at the tails:

**Turf:** 0-40: 0.93, 40-60: 0.96, 60-80: 0.90, 80-100: 0.81, 100-120: 0.61
**AW:** 0-40: 1.00, 40-60: 0.99, 60-80: 0.95, 80-100: 0.87, 100-120: 0.86

AW slopes are consistently closer to 1.0 than Turf, reflecting AW's inherently lower noise.
The continuous GA correction particularly improved AW 80-100 (0.816→0.872) and AW 100-120
(0.726→0.859).

### Compression by Race Class

| Class | Our Std | TF Std | Ratio | N |
|-------|---------|--------|-------|---|
| 1 | 16.0 | 18.6 | 0.863 | 22,901 |
| 2 | 17.6 | 20.1 | 0.878 | 41,556 |
| 3 | 17.3 | 19.8 | 0.876 | 38,310 |
| 4 | 17.8 | 19.9 | 0.898 | 102,218 |
| 5 | 18.2 | 20.0 | 0.910 | 171,201 |
| 6 | 17.0 | 17.9 | 0.949 | 147,544 |
| 7 | 14.1 | 14.5 | 0.974 | 1,504 |

Class 1 is the most compressed (ratio 0.863), Class 7 the least (0.974). Higher-class
races — with smaller, more competitive fields and less beaten-length spread — are
inherently harder to differentiate.

### Pre- vs Post-Calibration Spread

| Stage | Std | Slope vs TF |
|-------|-----|-------------|
| Pre-calibration (figure_final) | 21.2 | 0.752 |
| Post-calibration (figure_calibrated) | 21.3 | 0.938 |
| Timeform | 22.4 | 1.000 |

Calibration dramatically improves the slope (0.75→0.94) but barely changes the standard
deviation. The compression exists at the raw figure level, before calibration is applied.

### Root Cause

The compression is a fundamental property of our raw figure computation:

1. **Measurement noise → attenuation bias:** The calibration regression slope is < 1 because
   our raw figures contain noise (from lack of wind corrections, rail movement corrections,
   and limitations of the going allowance). This is standard regression-to-the-mean: a noisy
   x-variable produces a slope that attenuates the predicted y.

2. **Information asymmetry:** Timeform uses wind vector analysis, rail movement corrections,
   and expert human judgment to refine their figures. These reduce noise and allow higher
   effective spread. Without these additional data sources, our figures have a lower signal-
   to-noise ratio at the extremes.

3. **Small-field effect at high classes:** Class 1-2 races have fewer runners, tighter
   finishing margins, and less beaten-length spread — all of which reduce figure precision.

### What Was Tested But Didn't Help

All post-hoc compression corrections **worsened** overall MAE:

| Approach | MAE Change | Notes |
|----------|------------|-------|
| Per-figure-band offsets (K=100) | +0.23 | Tail correction helps extremes but adds noise to middle |
| Simple stretch factor (1.03-1.08) | +0.07 to +0.26 | Uniform stretch hurts well-calibrated centre |
| Tail-only offsets (<30, >80) | +0.19 | Still degrades middle |
| Graduated stretch (alpha=0.001-0.005) | +0.10 to +0.85 | All worsen |
| Quadratic calibration | +0.02 (marginal) | Already implemented; helps at tails by ~2 lbs at fig=120 |

**Why post-hoc fixes fail:** The calibration is already optimal (least squares) given the
data. Any post-hoc adjustment moves figures away from the regression optimum, adding variance
to the well-calibrated majority in exchange for marginal tail improvements.

### What Would Help (Future)

1. **Wind corrections** — Would reduce raw figure noise, naturally increasing the calibration
   slope. Requires weather data and course-specific wind models (as used by Timeform and Ragozin).

2. **Rail movement corrections** — Published on the BHA website; could be incorporated to
   adjust advertised distances.

3. **Projected-figure-based going allowances** — Using horse histories (like Beyer and
   Timeform) instead of class-adjusted times would improve GA accuracy and reduce noise.

4. **Split-card going detection** — Detecting within-meeting going changes (rain, watering)
   and applying segment-specific GA would reduce noise for affected meetings.

---

## Model Layer: Stacked GBR Enhancement (Stage 10)

### Problem Statement

The linear/quadratic calibration (Stage 9) systematically under-predicts high-rated figures.
In the 70-130 Timeform range, Turf bias was +5.27 lbs (our figures too low) and 70-130 MAE
was 9.39 lbs — 22% worse than overall MAE. The pre-calibration slope in the 70-130 range
was only 0.23 (Turf), meaning raw figures explain only 5% of variance at the high end.

### Approaches Tested

| Approach | Overall MAE | 70-130 MAE | 70-130 Bias | Notes |
|----------|-------------|------------|-------------|-------|
| **Baseline (quadratic + offsets)** | 8.82 | 9.39 | +5.27 | Current at that time |
| Per-class calibration | 9.00 | 8.77 | +0.80 | Near-eliminates bias but worse overall |
| Cubic polynomial | 12.32 | — | — | Negligible improvement over quadratic |
| Piecewise linear (4 knots) | 12.31 | — | — | Marginal |
| Cubic spline | 37.96 | — | — | Extrapolation catastrophe |
| Quantile mapping | 13.35 | — | — | CDF matching; worse |
| Linear + interactions | 9.91 | — | — | Multivariate linear |
| GBR (9 features, standalone) | 8.22 | 7.34 | +2.17 | Strong improvement |
| Random Forest | 9.28 | — | — | Worse than GBR |
| **Stacked (cal + GBR)** | **8.08** | **7.32** | **+2.66** | **Winner** |
| Residual GBR (current + GBR on residual) | 8.26 | 7.64 | +3.04 | Good but stacking better |
| Blend (50% baseline + 50% GBR) | 8.28 | 7.93 | +3.87 | Conservative |

All numbers are out-of-sample (2024-2026 Turf test set, 72,441 rows).

### Chosen Approach: Two-Stage Stacking

1. **Stage 9** (unchanged): Quadratic calibration with per-class, course×distance, going,
   continuous GA, beaten-length, and age offsets → produces `figure_calibrated`
2. **Stage 10** (new): GBR trained on `figure_calibrated` + auxiliary features → predicts
   `timefigure`, overwrites `figure_calibrated`

**GBR configuration:**
- 300 trees, max depth 5, learning rate 0.08, subsample 0.8, min_samples_leaf 50
- Separate model per surface (Turf, AW)
- Training window: 2015-2023 (same as calibration)

**Features (10):**
`figure_calibrated`, `figure_final`, `raceClass`, `distance`, `horseAge`,
`positionOfficial`, `weightCarried`, `ga_value`, `going_num`, `course_freq`

### Feature Importances

**Turf stacked model:**
| Feature | Importance |
|---------|-----------|
| figure_calibrated | 0.917 |
| ga_value | 0.022 |
| course_freq | 0.013 |
| distance | 0.012 |
| raceClass | 0.011 |

**AW stacked model:**
| Feature | Importance |
|---------|-----------|
| figure_calibrated | 0.938 |
| ga_value | 0.026 |
| distance | 0.008 |
| raceClass | 0.007 |
| course_freq | 0.006 |

The calibrated figure dominates (>91%), confirming the GBR is a refinement layer, not a
replacement. The auxiliary features provide marginal but real corrections: GA captures
residual going effects, course_freq adjusts for data quality, and raceClass captures
class-dependent compression.

### Impact on 70-130 Range (Out-of-Sample)

| Surface | Before MAE | After MAE | Before Bias | After Bias |
|---------|-----------|-----------|-------------|------------|
| Turf | 9.39 | 6.93 | +5.27 | -2.22 |
| AW | 6.86 | 6.51 | +2.98 | -3.86 |

The bias has flipped sign (from under-prediction to mild over-prediction) but magnitude
is reduced. The 70-130 MAE improved by 26% on Turf and 5% on AW.

### Overfitting Assessment

| Window | MAE | Corr |
|--------|-----|------|
| In-sample (2015-2023) | 6.45 | 0.9282 |
| Out-of-sample (2024-2026) | 7.26 | 0.9051 |
| Gap | +0.81 | -0.023 |

The OOS degradation of ~0.8 lbs is reasonable for a 300-tree GBR. The model generalises
well, with stable year-over-year performance (2024: 7.19, 2025: 7.38, 2026: 6.68).

---

## Key Observations & Improvement Opportunities

1. **Compressed scale (Std 20.4 vs 26.0):** The pipeline under-spreads figures at the
   tails (ratio 0.785). Root cause is raw figure noise from lack of wind/rail corrections,
   compounded by regression-to-mean in both calibration and GBR layers. The path forward
   is reducing upstream noise (wind data, projected-figure GA, split-card detection).

2. **Large class offsets (C1: +20.1 on Turf):** The class adjustment in Stage 1 isn't
   fully capturing class-level pace differences — the calibration layer is doing heavy
   lifting. However, varying class adjustments empirically HURT accuracy (CV 0.0216 vs
   0.0142), so this is best addressed by other means.

3. **Going allowance works well:** The continuous GA calibration correction significantly
   reduced going-dependent bias (Q1-Q4 spread: 3.12 → 1.36 lbs). Per-meeting GA is the
   correct granularity. No distance-dependent going effect detected.

4. **Standard time drift:** 87 of 231 combos show drift > 0.1 s/yr, with Wolverhampton
   and Lingfield getting measurably faster. A rolling 5-year window could improve these.

5. **GBR enhancement delivers biggest single improvement:** The stacked GBR (Stage 10)
   reduced overall MAE from 7.79 to 6.64 (-15%). The 70-130 Turf range improved from
   MAE 9.39 to 6.93 (-26%). Feature importances confirm figure_calibrated dominates
   (>91%), with auxiliary features providing marginal corrections.

6. **Out-of-sample stability is good:** 2024–2026 show ~0.8 lbs degradation vs in-sample
   years, which is expected for the GBR layer. Year-over-year variation is stable.

---

## Cumulative Improvement Tracker

| Stage | Corr | MAE | RMSE | Turf MAE | AW MAE |
|-------|------|-----|------|----------|--------|
| **Original baseline** | 0.8836 | 7.99 | 10.57 | 8.81 | 6.67 |
| + Std times (exclude maiden/2yo, min 20) | 0.8862 | 7.93 | 10.50 | — | — |
| + BL bias correction, LPL analysis | 0.8866 | 7.92 | 10.47 | 8.68 | 6.65 |
| + WFA: reduce decline, per-age offset | 0.8871 | 7.90 | 10.44 | — | — |
| + LPL: base 22→20, AW ×1.10 | 0.8876 | 7.88 | 10.42 | 8.65 | 6.65 |
| + Continuous GA calibration correction | 0.8896 | 7.79 | 10.33 | 8.62 | 6.45 |
| + Stacked GBR enhancement (Stage 10) | **0.9211** | **6.64** | **8.74** | **7.32** | **5.54** |

Total improvement: corr +0.0375, MAE -1.35, RMSE -1.83, Turf -1.49, AW -1.13.
