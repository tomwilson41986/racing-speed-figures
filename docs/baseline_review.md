# Model Baseline Performance Review

**Date:** 2026-02-28
**Pipeline:** `src/speed_figures.py` (10-stage traditional pipeline)
**Dataset:** Timeform 2015–2026 (793,065 UK/IRE flat runners → 750,204 after surface-change cutoffs)
**Target:** Timeform published timefigure
**Calibration window:** 2015–2023 (fit), 2024–2026 (out-of-sample)

---

## Overall Metrics (687,956 rows with valid timefigure)

| Metric | Value |
|---|---|
| **Correlation** | 0.8836 |
| **MAE** | 7.99 lbs |
| **RMSE** | 10.57 lbs |
| **Bias** | +0.48 lbs |

### Error Distribution

| Tolerance | % Within |
|---|---|
| ±1 lb | 8.0% |
| ±2 lbs | 16.0% |
| ±3 lbs | 23.7% |
| ±5 lbs | 38.1% |
| ±10 lbs | 65.5% |
| ±15 lbs | 80.6% |
| ±20 lbs | 87.7% |

---

## Breakdown by Surface

| Surface | Correlation | MAE | N |
|---|---|---|---|
| **Turf** | 0.8659 | 8.81 | 429,491 |
| **All Weather** | 0.9084 | 6.67 | 258,465 |

All Weather outperforms Turf by ~2 lbs MAE, likely because AW surfaces are more consistent (no going variation).

---

## Breakdown by Finishing Position

| Position | Correlation | MAE | N |
|---|---|---|---|
| 1st (winners) | 0.8382 | 8.20 | 73,312 |
| 2nd | 0.8379 | 8.13 | 73,153 |
| 3rd | 0.8429 | 8.09 | 73,085 |
| 4th | 0.8502 | 8.03 | 72,406 |
| 5th | 0.8569 | 7.98 | 70,164 |

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

## Key Observations & Improvement Opportunities

1. **Compressed scale (Std 20.8 vs 26.0):** The pipeline under-spreads figures at the tails. A stretch/compression recalibration for extreme ratings could close this gap.

2. **Large class offsets (C1: +19.9 on Turf):** The class adjustment in Stage 1 isn't fully capturing class-level pace differences — the calibration layer is doing heavy lifting. Refining the `CLASS_ADJUSTMENT_PER_MILE` constants could reduce reliance on post-hoc correction.

3. **Going offsets (Heavy: +3.0 on Turf):** Residual going bias after the going allowance stage suggests the GA computation underestimates the impact of extreme ground. The Heavy going penalty of +3.0 lbs is significant.

4. **Southwell 2025 drift (Bias -3.08):** Southwell shows a growing negative bias in recent years, suggesting the standard times or surface characteristics may be drifting and need recalibration with a shorter lookback window.

5. **MAE of ~8 lbs overall is above the stated goal of MAE < 3.** The ML enhancement layer (`ml_figures.py`) is designed to close this gap using the pipeline figure as its backbone feature.

6. **Out-of-sample stability is good:** 2024–2026 show no significant degradation vs in-sample years, confirming the calibration generalises well.
