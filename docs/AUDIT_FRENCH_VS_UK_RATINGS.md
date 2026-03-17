# Audit: French vs UK Speed Figure Ratings

**Date:** 2026-03-17
**Standard:** UK pipeline (`src/speed_figures.py`) taken as the gold standard
**Subject:** French pipeline (`src/france/speed_figures.py`) rating too high

---

## Executive Summary

The French daily figures are rating too high because the French pipeline **stops at Stage 8** (raw physics-based figures) while the UK pipeline continues through **Stages 9-10c** which include calibration, GBR enhancement, quantile mapping, and OOS corrections. The UK's Stages 9-10c systematically **compress and centre** the raw figures toward the Timeform scale. Without these stages, French figures operate on an uncalibrated "raw physics" scale where the mean figure is inflated relative to the UK output.

**Root causes of French figures rating too high (in order of impact):**

| # | Issue | Estimated Impact | Difficulty |
|---|-------|-----------------|------------|
| 1 | No calibration stages (9-10c) | +15-25 lbs systematic inflation | High |
| 2 | No class adjustment in standard times | +3-8 lbs for lower-class races | Medium |
| 3 | Missing recency weighting in iterative std times | +2-5 lbs drift | Low |
| 4 | No interpolated standard times for GA | Reduced GA precision → +1-3 lbs | Medium |
| 5 | No temporal neighbor GA recovery | Missing GA → ~0 assumed → +1-2 lbs | Low |
| 6 | French going priors are seed values, not empirical | +1-3 lbs noise | Medium |
| 7 | Missing beaten-length band corrections | +1-5 lbs for beaten horses | Medium |

---

## 1. Column Name Mapping: UK ↔ French

### 1.1 Direct Column Equivalences

The French `field_mapping.py` already maps DB fields to UK pipeline column names. Here is the complete mapping:

| UK Pipeline Column | French DB Source | Conversion Applied |
|---|---|---|
| `meetingDate` | `MeetingRow.race_date` | `strftime("%Y-%m-%d")` |
| `courseName` | `MeetingRow.hippodrome_code` | `.upper().strip()` |
| `raceNumber` | `RaceRow.course_num` | Direct |
| `raceType` | (hardcoded) | Always `"Flat"` (filtered to PLAT) |
| `raceSurfaceName` | `RaceRow.parcours` + `RaceRow.going` | `detect_surface()` → "Turf" or "All Weather" |
| `raceClass` | `RaceRow.race_name` + `RaceRow.prize_money` | `classify_french_race()` → "1"-"7" |
| `going` | `RaceRow.going` | Kept as French text (e.g. "Bon", "Souple") |
| `numberOfRunners` | `RaceRow.num_starters` | `pd.to_numeric()` |
| `prizeFund` | `RaceRow.prize_money` | `pd.to_numeric()` |
| `distance` | `RaceRow.distance_m` | `÷ 201.168` (meters → furlongs) |
| `positionOfficial` | `RunnerRow.finish_position` | `pd.to_numeric()` |
| `finishingTime` | `RaceRow.winner_time_s` | Winners only; non-winners get `NaN` |
| `weightCarried` | `RunnerRow.weight_kg` | `× 2.20462` (kg → lbs) |
| `horseAge` | `RunnerRow.age` | `pd.to_numeric()` |
| `horseName` | `RunnerRow.horse_name` | Direct |
| `horseGender` | `RunnerRow.sex` | `FRANCE_SEX_MAP` → c/f/g |
| `jockeyFullName` | `RunnerRow.jockey` | Direct |
| `trainerFullName` | `RunnerRow.trainer` | Direct |
| `sireName` | `RunnerRow.sire` | Direct |
| `damName` | `RunnerRow.dam` | Direct |
| `distanceCumulative` | `RunnerRow.beaten_lengths` | Parsed via `beaten_lengths.py` → cumulative sum |
| `month` | Derived from `race_date` | `.dt.month` |
| `race_id` | Derived | `"{date}_{course}_{raceNum}"` |
| `meeting_id` | Derived | `"{date}_{course}_{surface}"` |
| `std_key` | Derived | `"{course}_{dist_round}_{surface}"` |
| `dist_round` | Derived from `distance` | `round(distance * 2) / 2` (nearest 0.5f) |

### 1.2 Columns Present in UK but MISSING in French

| UK Column | Purpose | Impact of Absence |
|---|---|---|
| `timefigure` | Timeform's published figure (calibration target) | **CRITICAL** — No calibration target exists |
| `source_year` | Year of data (for recency weighting) | Recency weighting disabled |
| `raceCode` | Race type code (P*/S* = maiden) | Maiden detection uses heuristic keywords instead |
| `eligibilityagemin/max` | Age restrictions | 2yo-only exclusion not possible |
| `distanceBeaten` | Official beaten distance | Uses PMU beaten lengths text instead |
| `distanceFurlongs` | Precise furlong distance | Uses converted meters (slightly less precise) |
| `distanceYards` | Yard-level precision | Uses converted meters |
| `draw` | Stall position | Not available from PMU |
| `ga_value` | Going allowance on main df | Added during pipeline, but not in GBR features |

---

## 2. Stage-by-Stage Comparison

### Stage 0: Data Loading & Filtering

| Aspect | UK | French | Discrepancy |
|---|---|---|---|
| Data source | Timeform CSV files (2015-2026) | SQLite via SQLAlchemy | OK — structural difference only |
| Race filter | `raceType == "Flat"` + UK/IRE courses | `discipline == "PLAT"` | OK — equivalent |
| Time filter | `finishingTime > 0` per runner | `winner_time_s > 0` per race | **MINOR** — FR only has winner times |
| Surface cutoffs | Southwell/Chelmsford date filters | None | OK — no French surface changes known |
| Distance precision | `distanceFurlongs × 220 + distanceYards` → round to 110yd | `distance_m / 201.168` → round to 0.5f | **MINOR** — FR slightly less precise |

**Verdict:** Broadly equivalent. No significant inflation source here.

### Stage 1: Standard Times

| Aspect | UK | French | Discrepancy |
|---|---|---|---|
| Winner filter | Exclude maidens (raceCode P*/S*) + 2yo-only | Exclude maidens (keyword heuristic) | **MINOR** — FR keywords may miss some maidens |
| Going filter | `GOOD_GOING` set (14 values) | `FRANCE_GOOD_GOING` set (7 values) | OK if empirically correct |
| Class adjustment | `compute_class_adjustment()` applied (constant class 4 baseline) | **NOT applied** (raw `finishingTime` used directly) | **SIGNIFICANT** — see analysis below |
| Metric | Class-adjusted median | Raw median | Related to above |
| Iterative recompute | Recency-weighted median (half-life 4yr) | Unweighted median | **MODERATE** — older data given equal weight in FR |
| Irish shrinkage | Applied for Irish courses with 10-20 races | Applied for ALL French combos with 10-20 races | OK — appropriate adaptation |

**Class Adjustment Analysis:**

The UK pipeline applies a constant class-4 baseline adjustment to all winners before computing standard times:
```
adj_time = finishingTime - (CLASS_ADJUSTMENT_PER_MILE["4"] × distance / 8)
         = finishingTime - (-7.2 × distance / 8)
         = finishingTime + 0.9 × distance
```

The French pipeline comment says "the adjustment uses a fixed class ('4') producing a constant offset that inflates figures by ~100 pts because no Timeform calibration step exists to absorb the bias." This is **partially correct** — the offset IS constant and WOULD be absorbed by calibration in the UK. But the absence of this constant offset means French standard times are systematically **lower** (faster) than UK standard times by `0.9 × distance` seconds.

**However**, since the SAME offset is absent from both standard-time computation AND winner-figure computation, the deviation calculation cancels out:
```
UK:  deviation = (time - class_adj) - (std_time computed with class_adj) ← class_adj cancels
FR:  deviation = time - std_time_raw ← no class_adj on either side, same result
```

**This is actually correct** — the French pipeline correctly identified that the constant class adjustment is a no-op that cancels. **This is NOT a source of inflation.**

### Stage 2: Course-Specific LPL

| Aspect | UK | French | Discrepancy |
|---|---|---|---|
| Formula | `generic_lpl × correction × surface_mult` | Identical | None |
| Base constants | `LBS_PER_SECOND_5F=20`, `SECONDS_PER_LENGTH=0.2` | Identical | None |
| Surface multiplier | Turf=1.0, AW=1.10 | Identical | None |

**Verdict:** Identical. No inflation source.

### Stage 3: Going Allowances

| Aspect | UK | French | Discrepancy |
|---|---|---|---|
| Interpolated std times | Yes — fills gaps for unusual distances | **No** | **MODERATE** — fewer winners contribute to GA |
| Temporal neighbor pooling | Yes — borrows GA from ±1 day same course | **No** | **MINOR** — some meetings get no GA |
| Outlier removal | z-score ≤ 3.0 per meeting | Same | None |
| Split-card detection | t > 2.5 and |diff| > 0.10 s/f | Same | None |
| Winsorized median | Weighted (interpolated get 0.7) | Unweighted (no interpolated) | Related to interpolation absence |
| Shrinkage | Irish courses get k/2 | All French courses get full k | **MINOR** — may over-shrink unreliable French going descriptions |
| Non-linear correction | Same formula | Same formula | None |
| GA priors | Empirical from 10,625 UK meetings | **Seed values** ("to be refined empirically") | **MODERATE** — may bias GA |
| Min races filter | 3 | 3 | None |

**Impact on inflation:** Missing interpolated standard times and temporal neighbors means some meetings get no GA (defaulting to 0), which on soft-going days would UNDER-correct times, making figures TOO HIGH.

### Stage 4: Winner Speed Figures

| Aspect | UK | French | Discrepancy |
|---|---|---|---|
| Formula | `BASE_RATING - deviation_lbs` | Identical | None |
| Class adjustment | NOT applied to figure (correct) | NOT applied (correct) | None |

**Verdict:** Identical formula. No inflation source at this stage.

### Stage 5: All-Runner Figures (Beaten Lengths)

| Aspect | UK | French | Discrepancy |
|---|---|---|---|
| Beaten length source | `distanceCumulative` from Timeform | Parsed from PMU text + cumulative sum | **NEEDS VALIDATION** |
| Velocity-weighted LPL | Yes | Yes | None |
| Going-dependent attenuation | `T = clip(20 + ga × -8, 10, 30)` | Same | None |
| Soft cap factor | 0.5 | 0.5 | None |

**Beaten-length parsing concern:** The French parser converts text like "Courte Tete" (0.05L), "Encolure" (0.25L), "Tete" (0.1L) etc. These values look reasonable but `"Loin" = 30.0` lengths is very aggressive — a horse beaten "Loin" (far) could be 10-50 lengths behind. Using 30.0 systematically may deflate figures for tailed-off runners, but this wouldn't cause overall INFLATION.

### Stage 6: Weight-Carried Adjustment

| Aspect | UK | French | Discrepancy |
|---|---|---|---|
| Formula | `figure += weightCarried - 126` | Identical | None |
| Weight source | Timeform `weightCarried` (lbs) | PMU `weight_kg × 2.20462` | **NEEDS VALIDATION** |

**Weight conversion concern:** French races typically use weights in kilograms. The PMU `poids` field may represent the weight carried (jockey + gear) or just the declared weight. If PMU weights are **allocated weight** (not actual carried weight including gear), French weights could be ~2-3 lbs lighter than UK equivalents, leading to a **lower** weight adjustment (less positive). This would NOT cause inflation — it would slightly deflate figures.

### Stage 7: WFA Adjustment

| Aspect | UK | French | Discrepancy |
|---|---|---|---|
| WFA tables | Surface-specific empirical (Turf/AW) | **Reuses UK tables** | **MODERATE** |
| Older horse decline | Applied for Turf ages 7+ | Applied (via UK function) | None |

**Concern:** French racing has a different age-distribution and seasonal pattern. The UK WFA tables were calibrated against Timeform's timefigure for UK racing. Applying them to French racing may not be perfectly accurate, but the direction of error is unclear — could inflate OR deflate.

### Stage 8: Sex Allowance

| Aspect | UK | French | Discrepancy |
|---|---|---|---|
| Applied? | No | No | None |

**Verdict:** Identical (both no-op).

### Stages 9-10c: Calibration, GBR, Quantile Mapping, OOS Corrections

| Stage | UK | French | Impact |
|---|---|---|---|
| **9. Calibration** | Quadratic fit to Timeform + class/course-dist/going/GA/beaten-length/age offsets | **ABSENT** | **CRITICAL** |
| **10. GBR Enhancement** | Gradient Boosted Regressor per surface | **ABSENT** | **HIGH** |
| **10b. Quantile Mapping** | PCHIP interpolation to match Timeform distribution | **ABSENT** | **HIGH** |
| **10c. OOS Corrections** | Distance + going + temporal drift corrections | **ABSENT** | **MODERATE** |

**This is the primary source of French figures rating too high.**

The UK pipeline's raw figures (after Stage 8) have an **inflated mean** relative to Timeform. The calibration stages bring them down:
- Stage 9 applies a linear/quadratic fit that typically has slope < 1.0, compressing the scale
- Stage 10 (GBR) further refines predictions toward the training target
- Stage 10b (quantile mapping) corrects distribution compression from GBR
- Stage 10c (OOS) fixes systematic distance/going/temporal biases

The French pipeline outputs `figure_final = figure_after_wfa` (Stage 7 output), which is equivalent to the UK's **pre-calibration** figure. In the UK pipeline, this pre-calibration figure is typically 10-25 lbs higher than the final calibrated figure for above-average horses.

---

## 3. Detailed Inflation Analysis

### 3.1 Where the Inflation Comes From

The French `figure_final` is equivalent to the UK's `figure_after_wfa` — the raw physics-based figure before any calibration. In the UK pipeline:

```
figure_after_wfa  →  calibrate_figures()  →  figure_calibrated
                                              (typically 5-15 lbs lower for fig > 90)
                  →  enhance_with_gbr()   →  figure_calibrated (refined)
                  →  expand_scale()       →  figure_calibrated (distribution matched)
                  →  oos_corrections()    →  figure_calibrated (drift corrected)
```

The net effect of Stages 9-10c in the UK is:
- **High figures (>100):** Reduced by ~10-20 lbs
- **Average figures (60-100):** Reduced by ~5-10 lbs
- **Low figures (<60):** Increased by ~0-5 lbs

This means French figures are on a **different, inflated scale** compared to UK output.

### 3.2 Specific Mechanisms of Inflation

1. **No beaten-length band correction (UK Stage 9):** UK calibration includes per-beaten-length-band offsets that correct for judge's margin compression. Without this, horses beaten 5-20L get figures that are systematically 1-5 lbs too high.

2. **No continuous GA correction (UK Stage 9):** UK calibration includes a `ga_coeff × ga_value` term that corrects residual going bias. Without this, figures on extreme going are biased.

3. **No course×distance correction (UK Stage 9):** UK calibration corrects standard-time errors per track/distance. Without this, tracks with systematically fast/slow standard times bias all figures.

4. **No age offset correction (UK Stage 9):** UK calibration includes per-age offsets that correct WFA table imperfections. Since French racing uses UK WFA tables uncalibrated, age-specific biases persist.

---

## 4. Recommendations

### 4.1 High Priority — Self-Calibration Scale Alignment

Since no Timeform timefigure exists for French racing, implement a **self-calibration** approach:

**Option A: Cross-border calibration (Recommended)**
- Identify horses that have raced in BOTH France and UK (many French Group horses ship to Ascot, Goodwood, York, etc.)
- Use their UK Timeform figures as anchoring points
- Fit a calibration function: `FR_calibrated = f(FR_raw)` where `f` minimises the difference with UK Timeform figures for dual-runners
- This gives a principled mapping from French raw scale to Timeform scale
- **Expected improvement: 15-25 lbs systematic bias correction**

**Option B: Distribution matching**
- Match the French figure distribution to the UK distribution by class
- French Group 1 winners should have similar figure distributions to UK Group 1 winners
- Use quantile mapping per class to align distributions
- Less principled than Option A but requires no cross-matching
- **Expected improvement: 10-20 lbs**

**Option C: Constant offset subtraction (Quick fix)**
- Compute the mean figure for French races by class
- Compare to UK mean figure by class
- Apply the difference as a constant offset
- Crude but immediately effective
- **Expected improvement: 10-15 lbs**

### 4.2 Medium Priority — Pipeline Parity Fixes

These changes bring the French pipeline closer to UK methodology:

1. **Add recency weighting to iterative standard times** (`compute_standard_times_iterative`):
   - Add a `source_year` equivalent derived from `meetingDate`
   - Apply half-life=4yr exponential decay weighting (same as UK)
   - **File:** `src/france/speed_figures.py:171-233`

2. **Add interpolated standard times for GA computation:**
   - Port `_build_interpolated_std_times()` from UK pipeline
   - Use interpolated + exact standard times when computing going allowances
   - Weight interpolated at 0.7 (same as UK's `INTERPOLATED_GA_WEIGHT`)
   - **File:** `src/france/speed_figures.py:289-440`

3. **Add temporal neighbor GA recovery:**
   - Port `_temporal_neighbor_ga()` from UK pipeline
   - For meetings without enough races for GA, borrow from same course ±1 day
   - **File:** `src/france/speed_figures.py:289-440`

4. **Empirically calibrate French going priors:**
   - After sufficient data accumulation, compute actual mean GA per going description
   - Replace seed values in `FRANCE_GOING_GA_PRIOR` with empirical means
   - **File:** `src/france/constants.py:81-110`

5. **Add beaten-length band corrections:**
   - Without Timeform as target, use internal consistency: compute mean residual
     by beaten-length band (0-1L, 1-3L, 3-5L, 5-10L, 10-15L, 15-20L)
   - Apply shrunk corrections (same methodology as UK Stage 9)
   - **File:** `src/france/speed_figures.py` (new stage)

### 4.3 Low Priority — Quality Improvements

1. **Validate beaten-length parsing:** Audit a sample of French beaten-length text against known results to ensure `compute_cumulative_bl()` is accurate.

2. **Validate weight conversion:** Confirm PMU `poids` field represents carried weight (with gear) not just allocated weight.

3. **French-specific WFA tables:** Once enough data exists, derive French-specific WFA tables rather than reusing UK tables.

4. **2yo-only race exclusion:** Add detection for 2yo-only French races (currently not excluded from standard time compilation).

---

## 5. Implementation Priority Order

| Step | Action | Files | Complexity |
|------|--------|-------|------------|
| 1 | **Quick fix: Add constant class-based offset** | `speed_figures.py` | Low |
| 2 | **Add recency weighting** | `speed_figures.py:171-233` | Low |
| 3 | **Port interpolated std times for GA** | `speed_figures.py:289-440` | Medium |
| 4 | **Port temporal neighbor GA** | `speed_figures.py:289-440` | Low |
| 5 | **Implement cross-border calibration** | New file or `speed_figures.py` | High |
| 6 | **Add self-calibrated beaten-length corrections** | `speed_figures.py` | Medium |
| 7 | **Empirically derive French GA priors** | `constants.py` | Medium |
| 8 | **French-specific WFA tables** | `constants.py` | High |

---

## 6. Appendix: Shared Constants Verification

All core physics constants are identical between UK and French pipelines:

| Constant | UK Value | French Value | Match |
|---|---|---|---|
| `BASE_RATING` | 100 | 100 | Yes |
| `BASE_WEIGHT_LBS` | 126 | 126 | Yes |
| `SECONDS_PER_LENGTH` | 0.2 | 0.2 | Yes |
| `LBS_PER_SECOND_5F` | 20 | 20 | Yes |
| `BENCHMARK_FURLONGS` | 5.0 | 5.0 | Yes |
| `LPL_SURFACE_MULTIPLIER` (Turf) | 1.0 | 1.0 | Yes |
| `LPL_SURFACE_MULTIPLIER` (AW) | 1.10 | 1.10 | Yes |
| `BL_ATTENUATION_THRESHOLD` | 20.0 | 20.0 | Yes |
| `BL_ATTENUATION_FACTOR` | 0.5 | 0.5 | Yes |
| `MIN_RACES_STANDARD_TIME` | 20 | 20 | Yes |
| `MIN_RACES_GOING_ALLOWANCE` | 3 | 3 | Yes |
| `GA_OUTLIER_ZSCORE` | 3.0 | 3.0 | Yes |
| `GA_SHRINKAGE_K` | 3.0 | 3.0 | Yes |
| `GA_NONLINEAR_THRESHOLD` | 0.30 | 0.30 | Yes |
| `GA_NONLINEAR_BETA` | 0.25 | 0.25 | Yes |

The constants are not a source of discrepancy. The inflation comes entirely from **missing post-processing stages** (9-10c) and **missing GA refinements** (interpolation, temporal neighbors).
