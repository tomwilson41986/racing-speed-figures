# Model Accuracy Audit — Findings & Recommendations

**Date:** 2026-03-01
**Pipeline version:** Post GBR enhancement (Stage 10)
**Data:** 638,110 runners with valid timefigure (2015–2026)

---

## Executive Summary

Overall accuracy: **MAE 6.64 lbs, r=0.9211** against Timeform timefigure. This is strong, but the audit reveals three structural weaknesses that together account for the majority of excess error:

| Rank | Source | Impact | Root Cause |
|------|--------|--------|------------|
| 1 | **Scale compression** (ratings spread) | MAE 9–12+ at extremes | Our std=20.4 vs Timeform std=22.4; systematic under-rating of 100+ and over-rating of <20 |
| 2 | **Irish Turf tracks** | MAE 8.6–10.0 at worst | Fewer races per track×distance, more variable ground, less AW data to anchor |
| 3 | **OOS bias drift** | +2.18 lbs bias in 2024–26 | GBR/calibration trained on 2015–23 doesn't generalise to new data |
| 4 | **Heavy/Soft going** | MAE 7.6–8.0 | GA underestimates extreme going; non-linear speed/ground relationship |
| 5 | **Long-distance Turf** (12f+) | MAE 8.7 at 13–16f | Fewer standard-time samples, wind/pace variance higher |

---

## 1. Ratings Spread — The Biggest Accuracy Problem

### The Compression Problem

Our scale is compressed relative to Timeform's. The bias is **perfectly monotonic**:

| TFig Band | Mean Bias | Direction | MAE |
|-----------|-----------|-----------|-----|
| <20 | **+7.62** | Over-rated | 9.53 |
| 20–40 | +3.69 | Over-rated | 7.00 |
| 40–60 | +0.93 | Near zero | 6.06 |
| 60–80 | -2.02 | Under-rated | 6.03 |
| 80–100 | **-5.12** | Under-rated | 7.16 |
| 100–120 | **-8.59** | Under-rated | 9.16 |
| 120+ | **-12.47** | Under-rated | 12.47 |

The pattern: low-figure horses get pulled up, high-figure horses get pulled down. This is classic **regression to the mean** introduced by:

1. **GBR regression attenuation** — ML models regress toward the training mean (~54). This compresses extremes.
2. **Quadratic calibration** — The `−0.000733 × (fig − 181)²` term amplifies compression at tails.
3. **Insufficient scale in the raw figure** — Our raw std is already narrower than Timeform before calibration.

### Impact

- **47,216 runners** rated <20 have MAE 9.53 (vs 6.64 overall)
- **8,568 runners** rated 100–120 have MAE 9.16
- Top-rated horses (120+) are under-rated by 12.5 lbs on average
- The compression ratio (our std / TF std) is **0.912**

### Ideas to Address

1. **Post-GBR scale expansion**: Apply a simple linear stretch: `adjusted = mean + (pred - mean) * (TF_std / our_std)`. This directly fixes the compression ratio. Risk: may worsen MAE at the center of the distribution.

2. **Quantile-matched calibration**: Instead of quadratic regression, map our figure quantiles to Timeform quantiles. This preserves rank order while fixing the distribution shape. This is what Beyer and other professional speed figure compilers use.

3. **Separate models for extreme bands**: Train supplementary GBR models or apply different calibration coefficients for figures <30 and >90, where the linear/quadratic approximation breaks down most.

4. **Remove quadratic term from calibration**: The quadratic term (`a2`) adds compression by design. Test replacing it with a simple linear fit plus per-band offsets (already partially implemented).

5. **Constrain GBR predictions**: During GBR training, apply a monotonicity constraint or clip predictions to maintain the spread of `figure_calibrated` rather than compressing toward the mean.

---

## 2. Tracks — Irish Turf is a Major Weakness

### The Pattern

All 7 worst tracks are **Irish Turf courses**:

| Track | MAE | Excess | N |
|-------|-----|--------|---|
| Cork | 10.02 | +3.38 | 6,037 |
| Tipperary | 9.43 | +2.79 | 3,866 |
| Killarney | 9.13 | +2.49 | 448 |
| Naas | 9.09 | +2.45 | 9,460 |
| Curragh | 9.05 | +2.41 | 18,735 |
| Navan | 8.65 | +2.01 | 7,819 |
| Galway | 7.96 | +1.32 | 3,401 |

**Ireland Turf aggregate: MAE 8.64** vs UK Turf MAE 7.04 (Δ = +1.60 lbs)

Interestingly, **Dundalk (IRE AW): MAE 4.97** — one of the *best* tracks overall. So it's specifically Irish Turf that struggles.

### Root Causes

1. **Fewer races per track×distance**: Irish tracks have smaller datasets, meaning standard times and course×distance offsets are less reliable. Many Irish distance combos barely meet the 20-race minimum.

2. **More going variability**: Irish ground changes rapidly (Atlantic weather), so the going description often doesn't match reality. Within-meeting going changes are more common.

3. **Course configuration variability**: Several Irish courses (Curragh, Leopardstown) have multiple course configurations (inner/outer/straight) that are not distinguished in the data. Different configurations have materially different standard times.

4. **Specific track×distance failures**: Cork 12f (MAE 13.74), Naas 10f (MAE 12.26), Curragh 10f (MAE 11.10) are catastrophically bad — suggesting the standard times or LPL for these combos are wrong.

### Ideas to Address

1. **Course configuration modeling**: If Timeform data includes draw/stalls information, use it to infer inner vs outer track usage. Compute separate standard times per configuration.

2. **Lower the standard-time threshold for Irish courses** to 15 races, but with stronger shrinkage toward the overall distance average. This gives more combos a standard time while regularising the noisy ones.

3. **Irish-specific going model**: Irish going descriptions are less calibrated than UK ones. Consider a separate going ordinal mapping for Irish tracks, or weight the time-based GA more heavily vs official descriptions.

4. **Regional GBR**: Train a separate GBR for Irish Turf (70,279 runners — enough data). The UK-trained GBR may learn UK-specific patterns that don't transfer.

5. **Per-track standard-time audit**: For the worst 7 Irish tracks, manually review the standard times and check whether specific distance combos are unreasonable. Cork 12f, Naas 10f, and Curragh 10f should be investigated.

---

## 3. Going (Ground) — Extremes Cause Most Error

### The Pattern

| Going | MAE | Bias | N |
|-------|-----|------|---|
| Heavy | 8.01 | +0.23 | 24,077 |
| Soft | 7.61 | +0.34 | 43,856 |
| Good/Firm | 6.96 | +0.74 | 129,701 |
| Good/Std | 6.55 | +0.34 | 223,798 |
| Good/Soft | 6.22 | +0.42 | 195,947 |
| Firm | 5.91 | +0.52 | 20,731 |

Counterintuitively, **Firm going has the lowest MAE** (5.91). Heavy (8.01) and Soft (7.61) are worst. This makes sense: on extreme going, horse ability is partially masked by the ground effect, and going allowances are harder to estimate.

### OOS Degradation by Going

| Going | IS MAE | OOS MAE | Δ |
|-------|--------|---------|---|
| Firm | 5.68 | 7.59 | **+1.92** |
| Soft | 7.40 | 8.66 | +1.27 |
| Good/Firm | 6.78 | 7.79 | +1.01 |

**Firm going degrades the most OOS** (+1.92 MAE). This suggests the firm-going calibration offset is overfit to in-sample data, or firm going has changed character in 2024–2026 (e.g. different watering policies).

### Worst Track × Going Combos

- Ascot on Heavy: MAE 11.00 (n=67)
- Gowran Park on Heavy: MAE 10.47, bias +6.39 (n=390)
- Goodwood on Heavy: MAE 9.92 (n=763)
- Nottingham on Firm: MAE 10.85, bias -4.35 (n=144)
- Newmarket (Rowley) on Firm: bias **-7.83** (n=115) — severely under-rated

### Ideas to Address

1. **Non-linear going adjustment**: The current GA is linear (seconds per furlong). On extreme going, the relationship between going and time is likely non-linear. Test a quadratic GA model: `GA = a × going + b × going²`.

2. **Going-dependent LPL**: Horses bunch up on heavy going (closing up beaten lengths). The LPL should decrease on soft/heavy ground. Analysis in `analyse_lpl.py` found no effect, but this should be re-tested with the current pipeline.

3. **Split-card going detection**: When it rains during a meeting, early and late races have different going. Currently the GA averages across the whole card. Detecting within-meeting going changes (via race-by-race deviation patterns) would reduce GA error.

4. **Track-specific going adjustments**: Some tracks ride materially differently on the same official going (e.g. Goodwood on heavy is notoriously unusual). Per-track going corrections would capture this.

5. **Weather data integration**: Wind speed/direction significantly affects finishing times at exposed courses. Rail movement data (published by the BHA) affects the distance horses actually run. Both are currently unmodeled.

---

## 4. Out-of-Sample Degradation

### The Core Issue

| Metric | In-sample | OOS | Δ |
|--------|-----------|-----|---|
| MAE | 6.48 | 7.26 | +0.78 |
| Bias | +0.00 | **+2.18** | +2.18 |
| Corr | 0.926 | 0.905 | -0.021 |

The **+2.18 lbs positive bias** OOS means we systematically over-rate horses in 2024–2026. The bias is near-zero in-sample by construction (the calibration fits to it), but doesn't hold out-of-sample.

### What Drives OOS Degradation?

By figure band, the worst OOS degradation is at the extremes:
- **<20 band**: IS MAE 9.08 → OOS MAE 11.20 (Δ +2.12)
- **20–40 band**: IS MAE 6.73 → OOS MAE 8.02 (Δ +1.29)

This confirms the compression problem is worse OOS — the GBR model's mean-regression is stronger when it encounters horses outside its training distribution.

### Ideas to Address

1. **Rolling calibration window**: Instead of training on all 2015–2023, use a rolling 5-year window (e.g. 2019–2023 for 2024 predictions). More recent calibration coefficients will be more relevant.

2. **Temporal validation in GBR**: Use time-based cross-validation during GBR training (e.g. train on 2015–2021, validate on 2022–2023). This prevents the GBR from memorising historical patterns that don't hold forward.

3. **Online calibration updates**: When new Timeform data arrives, refit the calibration layer (class offsets, going offsets, course×distance offsets) on the most recent 3 years. This is lightweight and could run weekly.

4. **Reduce GBR overfitting**: The current GBR (300 trees, depth 5) may be too complex. Try reducing to 150 trees, depth 3, or adding stronger regularisation (higher min_samples_leaf).

---

## 5. Distance — Long-Distance Turf is Weak

### The Pattern

| Distance | Turf MAE | AW MAE |
|----------|----------|--------|
| 5f–6f | 6.90 | 5.85 |
| 7f–8f | 7.11 | 5.10 |
| 9f–10f | 7.62 | 5.55 |
| 11f–12f | 8.06 | 5.93 |
| **13f–16f** | **8.67** | 6.00 |
| 17f+ | 7.32 | 7.67 |

Long-distance Turf (13–16f) has **MAE 8.67** — 1.77 lbs worse than the mid-range (7f–8f). The worst individual combos are catastrophic:
- Ascot 14f: MAE **18.72** (n=324) — likely due to the course layout or draw bias
- Haydock 14f: MAE **12.14** (n=773)
- Thirsk 12f: MAE **11.12** (n=1,147)

### Root Causes

1. **Fewer standard-time samples** at long distances → less reliable standard times
2. **Higher pace variance** — long-distance races have more tactical variation (front-running vs hold-up), leading to wider time distributions
3. **Wind exposure** — longer races spend more time on exposed sections, amplifying wind effects
4. **LPL at long distances** may be wrong — the power-law extrapolation is less well-anchored beyond 12f

### Ideas to Address

1. **Investigate Ascot 14f specifically**: MAE 18.72 is an extreme outlier. This is likely a data issue (e.g. mixing straight course vs round course distances). Examine whether this distance maps to multiple course configurations.

2. **Distance-dependent shrinkage**: Apply stronger shrinkage in course×distance offsets at long distances where sample sizes are small.

3. **Sectional times**: For courses with sectional timing data, use sectional splits rather than overall time. This partially neutralises pace variation.

---

## 6. Race Class — C0/Unclassified Races

Class C0 (unclassified) has MAE 7.35 overall, but **8.60 on Turf** — the worst class×surface combination. These are likely:
- Irish pattern races without UK-style class labels
- Conditions stakes
- Apprentice/amateur races

Since the calibration applies per-class offsets, C0 gets a generic offset that may not suit this heterogeneous group.

### Ideas to Address

1. **Reclassify C0 races**: Use prize money, race conditions text, or Timeform's own classification to assign these races to an equivalent class.
2. **Split C0 by country**: Irish C0 and UK C0 likely represent different race types.

---

## Priority Improvement Roadmap

Based on impact × feasibility:

### Quick Wins (< 1 day effort)
1. **Post-GBR scale expansion** — simple linear stretch to fix compression ratio
2. **Reduce GBR complexity** — test fewer trees / shallower depth to reduce OOS overfitting
3. **Investigate Ascot 14f** — likely a data / course configuration issue

### Medium-Term (1–3 days)
4. **Rolling calibration window** — retrain calibration on 2019–2023 instead of 2015–2023
5. **Per-track standard-time audit** for worst Irish courses (Cork, Curragh, Naas)
6. **Quantile-matched calibration** to replace quadratic + GBR compression

### Longer-Term (1–2 weeks)
7. **Course configuration modeling** for multi-layout tracks
8. **Non-linear going model** for extreme ground
9. **Regional GBR** for Irish Turf
10. **Wind/rail data integration** for exposed long-distance courses
