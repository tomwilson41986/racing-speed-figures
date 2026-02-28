# Model Baseline Performance Review

**Date:** 2026-02-28
**Pipeline:** `src/speed_figures.py` (10-stage traditional pipeline)
**Dataset:** Timeform 2015–2026 (793,065 UK/IRE flat runners → 750,204 after surface-change cutoffs)
**Target:** Timeform published timefigure
**Calibration window:** 2015–2023 (fit), 2024–2026 (out-of-sample)

---

## Overall Metrics (684,322 rows with valid timefigure)

| Metric | Value |
|---|---|
| **Correlation** | 0.8866 |
| **MAE** | 7.92 lbs |
| **RMSE** | 10.47 lbs |
| **Bias** | +0.49 lbs |

### Error Distribution

| Tolerance | % Within |
|---|---|
| ±1 lb | 8.2% |
| ±2 lbs | 16.2% |
| ±3 lbs | 24.0% |
| ±5 lbs | 38.5% |
| ±10 lbs | 65.9% |
| ±15 lbs | 80.8% |
| ±20 lbs | 87.8% |

---

## Breakdown by Surface

| Surface | Correlation | MAE | N |
|---|---|---|---|
| **Turf** | 0.8710 | 8.71 | 425,857 |
| **All Weather** | 0.9085 | 6.66 | 258,465 |

All Weather outperforms Turf by ~2 lbs MAE, likely because AW surfaces are more consistent (no going variation).

---

## Breakdown by Finishing Position

| Position | Correlation | MAE | N |
|---|---|---|---|
| 1st (winners) | 0.8410 | 8.07 | 72,921 |
| 2nd | 0.8395 | 8.07 | 72,762 |
| 3rd | 0.8440 | 8.06 | 72,700 |
| 4th | 0.8515 | 8.02 | 72,016 |
| 5th | 0.8585 | 7.96 | 69,794 |

Notably flat across positions — no significant degradation for beaten horses. Mid-field runners actually have marginally *better* MAE, possibly because extreme winners/losers have more variance.

---

## Breakdown by Year

| Year | Correlation | MAE | N |
|---|---|---|---|
| 2015 | 0.8706 | 8.57 | 57,052 |
| 2016 | 0.8735 | 8.31 | 57,632 |
| 2017 | 0.8882 | 8.12 | 60,583 |
| 2018 | 0.8975 | 7.75 | 61,025 |
| 2019 | 0.8960 | 7.71 | 60,752 |
| 2020 | 0.8922 | 7.55 | 52,947 |
| 2021 | 0.8910 | 7.76 | 64,494 |
| 2022 | 0.8932 | 7.85 | 65,619 |
| 2023 | 0.8784 | 8.23 | 68,235 |
| **2024** | **0.8871** | **7.87** | **69,513** |
| **2025** | **0.8803** | **8.20** | **64,939** |
| **2026** | **0.8910** | **7.23** | **5,165** |

2024–2026 are **out-of-sample** (calibration fitted on ≤2023). Performance is stable — no significant degradation on unseen data.

---

## Scale Comparison

| Statistic | Our Figures | Timeform |
|---|---|---|
| Mean | 53.9 | 50.1 |
| Median | 54.1 | 52.0 |
| Std Dev | 20.8 | 26.0 |

Our distribution is slightly compressed compared to Timeform (lower std: 20.8 vs 26.0), meaning we under-spread the tails. High-rated horses may be under-rated and low-rated horses over-rated relative to Timeform's scale.

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
- `timefigure ≈ 0.7616 × figure_final - 87.43`
- Class offsets: C1 +19.9, C2 +15.1, C3 +5.7, C4 -0.1, C5 -5.1, C6 -11.9, C7 -20.3
- Going offsets: Firm -3.1, GdFm -0.8, Good -0.2, GdSft +0.6, Soft +0.5, Heavy +3.0
- 279/312 course×distance combos with |offset| > 0.5 lbs

### All Weather
- `timefigure ≈ 0.8835 × figure_final - 120.98`
- Class offsets: C1 +14.0, C2 +10.3, C3 +5.5, C4 +2.9, C5 +0.3, C6 -3.1, C7 -3.9
- Going offsets: Firm +1.2, GdFm +0.3, Good +0.3, GdSft -0.5
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

### Identified WFA Issues

1. **2yo early-season inflation** — The BHA scale allows up to 41 lbs for 2yo in March;
   Racing Post and industry commentators (Dave Edwards, Timeform) consider this too
   generous. Our 2yo Turf table shows 30 lbs in March at 5f — more conservative than
   BHA but still potentially inflated. Very few 2yo races exist before May, so
   early-season cells have minimal data and should be treated with caution.

2. **Smoothing artefacts** — The 3-month weighted average smoothing produces non-monotonic
   values at distance boundaries (e.g., 3yo Turf 7f drops to 0 in July while 5f remains
   at 7 and 8f is at 3). A monotonic constraint on the distance axis would produce more
   physically plausible tables.

3. **No fortnightly resolution** — The BHA scale uses fortnightly intervals; ours uses
   monthly. For 3yo in April–June when maturation is fastest, this means up to 2 lbs
   of within-month variation is averaged out.

4. **Older horse decline only on Turf** — The AW dataset may be too noisy to detect the
   signal, but it could also represent a genuine surface interaction. Worth revisiting
   with the larger 2024–2026 dataset.

5. **3yo at 14f+ under-corrected** — The 2025 BHA review found persistent 3yo
   overperformance at 14f+ and reduced allowances by ~1 lb. Our empirical table has
   limited data at these distances (few cells exist beyond 12f for 3yo Turf). This
   edge case should be audited.

### Recommendations

1. **Add monotonic distance constraint** — Enforce that WFA allowances are
   non-decreasing with distance (at fixed age/month) when deriving empirical tables.
   This prevents implausible reversals.

2. **Use paired-horse methodology** — Supplement the residual-fitting approach with
   direct paired comparisons of horses that raced as both 2yo/3yo and 4yo+ (per the
   framework recommendation in §6.5). This is more robust to target-scale artefacts.

3. **Validate against Timeform's `tfwfa` field** — The ML pipeline already parses the
   `tfwfa` column from Timeform data. Compare our empirical WFA allowances against
   Timeform's published per-race WFA weights to quantify divergence.

4. **Consider fortnightly intervals** for April–July when maturation is fastest, falling
   back to monthly for the rest of the year.

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

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| **Correlation** | 0.8862 | **0.8866** | +0.0004 |
| **MAE** | 7.93 | **7.92** | -0.01 |
| **RMSE** | 10.50 | **10.47** | -0.03 |
| **Std combos** | 413 | 381 | -32 |

---

## Lbs-Per-Length (LPL) Analysis

### Current Implementation

LPL is computed **per course × distance × surface** (381 combos). The method:

1. Generic LPL from distance: `lpl = 0.2 × 22 × (5 / distance_furlongs)`
2. Course correction: `mean_spf / this_course_spf` (faster courses → higher lpl)
3. Beaten lengths converted via: `lbs_behind = cumulative_bl × course_lpl`

### LPL Distribution

| Distance | Generic | Mean (course-adjusted) | Min | Max | N Courses |
|----------|---------|------------------------|-----|-----|-----------|
| 5f | 4.40 | 4.37 | 4.09 | 4.70 (Epsom) | 47 |
| 8f | 2.75 | 2.74 | 2.53 | 3.03 | 54 |
| 12f | 1.83 | 1.83 | 1.61 | 1.93 | 48 |
| 16f | 1.38 | 1.37 | 1.29 | 1.43 | 27 |

Course-specific corrections are small (±7% at most), dominated by Epsom (+6.9%,
downhill → horses spread more) and tight tracks like Chester (-3%).

### Industry Comparison

| Source | 5f lpl | 8f lpl | Notes |
|--------|--------|--------|-------|
| **Our pipeline** | 4.40 | 2.75 | 22 lbs/sec at 5f |
| **BHA handicapper** | ~3.00 | ~1.88 | 15 ÷ distance formula |
| **Timeform (Rowlands)** | ~4.0+ | ~2.5+ | "25-30% higher than BHA" |
| **Raceform** | ~3.20 | ~2.00 | 16 ÷ distance formula |
| **Beyer (US)** | ~3 pts/L | ~2 pts/L | Different scale |

Our 22 lbs/sec sits between BHA (low) and Timeform (high), which is appropriate since
we calibrate against Timeform's timefigure as target.

### Pairwise Spread Analysis

Tested whether our figure differences between horses in the same race match Timeform's
differences (507K winner-vs-beaten pairs):

| Level | Slope (tf_diff = β × our_diff) | Interpretation |
|-------|-------------------------------|----------------|
| **Calibrated** | 1.07 | 7% under-spread (after BL correction) |
| **Raw (pre-cal)** | 0.93 | 7% over-spread |

The raw figures over-spread by 7%, then calibration (linear slope 0.76 Turf) compresses
by ~24%, resulting in net 7% under-spread.  This is a calibration artefact, not an LPL
error — increasing raw LPL was tested (Turf ×1.15) and **degraded** MAE from 7.99 to
8.50 because it forced even more calibration compression.

By surface: Turf slope = 1.19 (significant under-spread), AW slope = 1.02 (correct).
The Turf calibration slope (0.76) is the bottleneck.

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

### Remaining Improvement Opportunities

1. **Non-linear calibration for Turf** — The quadratic term was not selected for Turf
   (linear MSE was not sufficiently worse).  A higher-order calibration or separate
   slopes for the upper/lower halves of the figure range could reduce the pairwise
   under-spread without inflating raw LPL.

2. **Going-dependent LPL** — The pairwise slope varies by going (Soft: 1.19, GdSft:
   1.07, Good: 1.10).  Soft-going margins may need a different conversion, but the
   sample is smaller and the effect is partially captured by the going offset layer.

3. **Fortnightly LPL smoothing** — Standard times have seasonal variation (summer track
   is faster than winter).  Seasonal LPL adjustment could capture this.

---

## Key Observations & Improvement Opportunities

1. **Compressed scale (Std 21.2 vs 26.0):** The pipeline under-spreads figures at the tails. A non-linear calibration could help close the gap to Timeform's 26.0 std dev.

2. **Large class offsets (C1: +19.9 on Turf):** The class adjustment in Stage 1 isn't fully capturing class-level pace differences — the calibration layer is doing heavy lifting. However, varying class adjustments empirically HURT accuracy (CV 0.0216 vs 0.0142), so this is best addressed by other means.

3. **Going offsets (Heavy: +3.0 on Turf):** Residual going bias after the going allowance stage suggests the GA computation underestimates the impact of extreme ground.

4. **Standard time drift:** 87 of 231 combos show drift > 0.1 s/yr, with Wolverhampton and Lingfield getting measurably faster. A rolling 5-year window could improve these.

5. **MAE of ~7.92 lbs overall is above the stated goal of MAE < 3.** The ML enhancement layer (`ml_figures.py`) is designed to close this gap using the pipeline figure as its backbone feature.

6. **Out-of-sample stability is good:** 2024–2026 show no significant degradation vs in-sample years, confirming the calibration generalises well.
