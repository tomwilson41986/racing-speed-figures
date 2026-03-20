# QA Audit Report: UK Ratings 2026-03-19

**Date reviewed:** 2026-03-20
**File:** `output/daily_ratings/uk_ratings_2026-03-19.xlsx`
**Meeting:** Newcastle (All Weather), 7 races, 71 runners
**Going:** Standard | **Surface:** All Weather

---

## Executive Summary

The UK ratings for 19 March 2026 contain **1 critical issue**, **3 high-priority issues**, and **3 medium-priority observations**. The meeting was a single-venue Newcastle AW card with 7 flat races (Classes 2-6). Of 71 runners, 64 received calibrated figures, 5 were correctly excluded as non-finishers (position=0), and **2 finishers were incorrectly excluded** due to the >20L beaten-length hard cutoff.

| Severity | Count | Summary |
|----------|-------|---------|
| CRITICAL | 1 | Race 1 figures ~55 lbs below official ratings (standard time miscalibration at 10f) |
| HIGH | 3 | Figures nulled for finishers; beaten-length ordering anomalies; weak OR correlation |
| MEDIUM | 3 | Missing ORs for maidens; class par deviations; distance precision |

---

## 1. CRITICAL: Race 1 Figures Systematically Depressed (~55 lbs Below OR)

**Race 1** (1m 2f 42y / 10.19f, Class 6, 10 runners) produces figures that are **massively below** all other races on the card and far below official ratings:

| Horse | Position | OR | Figure | Diff |
|-------|----------|-----|--------|------|
| Mao Shang Wong (IRE) | 1st | 65 | 10.1 | -54.9 |
| Dingwall | 2nd | 63 | 1.4 | -61.6 |
| Golspie (IRE) | 4th | 65 | 3.6 | -61.4 |
| Prince Achille | 8th | 64 | -3.7 | -67.7 |
| Toronto Raptor (FR) | 10th | 46 | -58.4 | -104.4 |

**Mean figure-OR gap:** -67.0 lbs (vs -4.0 to -20.4 for other races)

**Root cause:** The `NEWCASTLE_10` course-distance calibration offset is **-15.40 lbs** — the largest negative offset of all Newcastle AW configurations. This compounds with the AW base intercept (b = -83.12) and Class 6 offset (-4.78) to produce a total constant-term sum of approximately -99.8, severely compressing all figures at this distance.

The implied standard time (back-calculated from the winner's figure) is approximately 142.0s for 10.19f, but the actual winning time was 137.27s — suggesting the standard time used in the pipeline is roughly 5 seconds too slow for this course/distance, which then gets over-corrected by the large negative calibration offset.

**Recommendation:**
- Re-examine the standard time for Newcastle AW at 10-10.5 furlongs
- Investigate whether the -15.40 course-distance offset was fitted on sparse data
- Consider capping course-distance offsets at +-10 lbs or applying stronger shrinkage

---

## 2. HIGH: Figures Nulled for Finishers (Beaten >20L Hard Cutoff)

Two horses that **completed the race** received null figures due to the hard >20L beaten-length exclusion at `live_ratings.py:1311-1320`:

| Horse | Race | Position | Beaten Lengths | Figure |
|-------|------|----------|---------------|--------|
| William Dewhirst (IRE) | 3 | 7th (last finisher) | 24.15L | None |
| Antique Blue (IRE) | 4 | 11th (last finisher) | 29.18L | None |

The pipeline already implements beaten-length attenuation (0.5x factor beyond 20L), so these horses would receive attenuated but still meaningful figures. The hard cutoff at 20L after attenuation is applied discards valid information.

**Recommendation:** Either:
- Remove the >20L hard cutoff (rely on the attenuation alone), or
- Raise the threshold to 30L to accommodate tailed-off finishers, or
- Flag but do not null — add a `low_confidence` flag column instead

---

## 3. HIGH: Beaten-Length Ordering Anomalies (10 instances)

In 6 of 7 races, at least one horse beaten further had a **higher figure** than a horse beaten less. While some reversals are expected (weight adjustments), several are large:

| Race | Horse | Pos | BL | Fig | Reversal vs |
|------|-------|-----|-----|-----|-------------|
| R1 | Riyadh Gem (5th) | 5 | 3.90 | 6.1 | Hashtagnotions (3rd, 2.30L, -1.1) |
| R2 | Teddy Shaw (10th) | 10 | 14.15 | 9.8 | Supremissy (9th, 13.65L, -0.2) |
| R4 | El Pinto (7th) | 7 | 16.53 | 34.7 | Eva The Deeva (6th, 14.78L, 28.4) |
| R5 | Emerald Harmony (2nd) | 2 | 0.03 | 64.1 | Juan Les Pins (1st, 0L, 59.3) |

**Analysis:**
- **Race 5 (Emerald Harmony):** CORRECT — 2nd carries 5lb more than winner and beaten only 0.03L (a nose). Weight adjustment legitimately pushes 2nd's figure above winner.
- **Race 2 (Teddy Shaw):** Teddy Shaw (5yo, 140lb) gets +14lb weight adjustment while Supremissy (3yo, 124lb) gets -2lb, explaining the ~10-point swing despite being beaten further. This is borderline acceptable but highlights how extreme weight differentials can distort relative ordering.
- **Race 4 (El Pinto):** El Pinto (4yo, 142lb, OR 68) carries 20lb more than the 3yo maidens (122lb) and receives a +16lb weight adjustment plus no WFA penalty. The +8.5 point reversal over Eva The Deeva is driven entirely by the weight gap. Mathematically correct but produces a counter-intuitive result.
- **Race 1 ordering anomalies** are symptomatic of the Race 1 calibration issue (see Finding 1).

**Recommendation:** Add a `figure_rank` column to the output and flag cases where figure rank diverges from finish position by >3 places, for manual review.

---

## 4. HIGH: Weak Correlation Between Figures and Official Ratings

| Race | n | Mean(fig-OR) | Correlation |
|------|---|-------------|-------------|
| R1 | 10 | -67.0 | 0.933 |
| R3 | 6 | -21.7 | -0.652 |
| R5 | 6 | -20.4 | -0.614 |
| R6 | 11 | -4.0 | 0.765 |
| R7 | 10 | -12.7 | 0.329 |

- **Race 1** has excellent internal correlation (0.933) but massive bias — confirming a systematic calibration shift, not noisy figures.
- **Race 3** shows **negative correlation** (-0.652): higher-OR horses get lower figures. The highest-rated runner (Beauty Destiny, OR 102) gets only 62.0 (beaten 9.15L) while carrying 137lb (+11lb adj). The weight adjustment partially compensates but the BL penalty dominates. This may indicate the LPL at 6f AW is too aggressive, or the calibration favours lightly-raced improvers over established handicappers.
- **Race 5** also shows negative correlation — driven by the heaviest-weighted runners (Secret Guest 135lb OR 82, fig 53.1; Bell Conductor 134lb OR 81, fig 40.8) receiving figures well below their ORs despite carrying top weight.
- **Race 6** (0.765) and **Race 7** (0.329) show positive but low correlations. Race 6 is the best-calibrated race on the card with mean(fig-OR) of only -4.0.

**Note:** Speed figures and official ratings measure different things (one-day performance vs longer-term ability), so moderate discrepancy is expected. However, the systematic bias of figures below ORs across all races (mean -25.3 lbs overall) suggests the AW calibration may be running cool.

---

## 5. MEDIUM: Missing Official Ratings (21 of 71 runners)

21 runners (29.6%) have zero or missing official ratings. These fall into two categories:

- **Race 2** (10 of 11): Novice/maiden race where most runners are unrated 3-year-olds. The only rated runner is the winner Farandaway (OR 70, fig 59.2). This is normal for early-season novice races.
- **Race 4** (10 of 12): Similar pattern — 3yo maiden/novice with only El Pinto (4yo, OR 68) having a rating.

The figures for these runners cannot be cross-validated against ORs. Their figures appear reasonable relative to the winner and beaten margins. Races 2 and 4 are best assessed via class par alignment rather than OR comparison.

---

## 6. MEDIUM: Class Par Deviations

| Race | Class | Winner Fig | Par | Deviation | Assessment |
|------|-------|-----------|-----|-----------|------------|
| R1 | 6 | 10.1 | 50 | -39.9 | ANOMALOUS (see Finding 1) |
| R2 | 4 | 59.2 | 70 | -10.8 | Slightly below par |
| R3 | 2 | 84.5 | 90 | -5.5 | Near par |
| R4 | 4 | 77.7 | 70 | +7.7 | Above par (strong race) |
| R5 | 4 | 59.3 | 70 | -10.7 | Below par |
| R6 | 6 | 55.8 | 50 | +5.8 | At par |
| R7 | 6 | 60.8 | 50 | +10.8 | Above par |

**Excluding Race 1**, the average Class 6 winner figure is 58.3 (vs par 50, +8.3) and Class 4 is 65.4 (vs par 70, -4.6). The Class 2 race is near par. The overall calibration is reasonable but slightly depressed for Class 4 races.

---

## 7. MEDIUM: Distance Precision

Race distances are stored as raw furlong decimals rather than human-readable formats:

| Race | Stored Distance | Actual Distance |
|------|----------------|-----------------|
| R1 | 10.190909 | 1m 2f 42y |
| R4, R6, R7 | 7.063636 | 7f 14y |
| R2, R3 | 6.000000 | 6f |
| R5 | 5.000000 | 5f |

This is not a calculation error (the pipeline handles continuous distances correctly via interpolation), but the output would benefit from a human-readable distance column for QA purposes (e.g., "1m2f42y" or "7f14y").

---

## 8. Positive Findings

1. **Non-finisher handling is correct:** All 5 position-0 runners (Golden Redemption, Paddys Day, Tigers Nest, Alondra, Lauras Breeze) correctly receive null figures.

2. **Weight-for-age adjustments are correct:** Race 7 (all 3yo, 7f AW, March) shows the WFA boost working as intended — Fickle Mcselfish (3yo winner, 88.22s) rates 5.0 lbs above Yorkstone (6yo winner of Race 6, 88.31s) from the same distance/class, which is consistent with the 3yo AW March WFA allowance of ~11 lbs.

3. **Race 6 internal consistency is excellent:** The closest-finishing race (top 7 separated by 2.05L) shows figure separations of 0.3-1.5 lbs per position — tightly calibrated and consistent.

4. **Race 3 (Class 2) produces the highest figures (84.5)** from the fastest time (71.26s at 6f), correctly identifying this as the strongest race on the card. The 2.41s time gap between Race 3 and Race 2 (same distance) translates to a 25.3 figure gap, which is reasonable.

5. **Sex allowance is correctly not applied**, consistent with the pipeline's empirical finding that explicit sex adjustment hurts accuracy.

---

## Recommendations Summary

| Priority | Action | Impact |
|----------|--------|--------|
| P0 | Investigate and recalibrate NEWCASTLE_10 AW standard time / course-distance offset | Fixes Race 1 (~10 runners) |
| P1 | Remove or raise the >20L beaten-length hard cutoff | Recovers 2 lost figures |
| P2 | Add `figure_rank` divergence flag to output | Aids manual QA |
| P2 | Add human-readable distance column to output | Aids readability |
| P3 | Review overall AW calibration intercept (bias of -25.3 vs OR suggests running cool) | Systemic accuracy |

---

## Data Quality Summary

| Metric | Value | Status |
|--------|-------|--------|
| Total runners | 71 | OK |
| Figures computed | 64 (90.1%) | OK |
| Non-finishers excluded | 5 (7.0%) | CORRECT |
| Finishers wrongly excluded | 2 (2.8%) | BUG |
| Figure-OR correlation (excl R1) | 0.496 | BELOW TARGET (>0.7) |
| Figure-OR correlation (R6 only) | 0.765 | ACCEPTABLE |
| Mean figure-OR bias | -25.3 lbs | HIGH |
| Mean figure-OR bias (excl R1) | -13.3 lbs | MODERATE |
| Class par alignment (excl R1) | +-10.8 lbs | ACCEPTABLE |
