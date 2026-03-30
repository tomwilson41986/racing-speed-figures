# QA Review: French Figures — 29-03 (March 2026)

> Reviewed 2026-03-30 against daily ratings output and French pipeline artifacts.

## Overview

| Metric | Value |
|--------|-------|
| Courses | 7 (AGE, CAV, LYO, MXD, PMO, SAI, SHT) |
| Races | 40 |
| Total runners | 416 |
| Figures produced | 358 (86%) |
| Figures missing | 58 (13 "no historical data", 45 back-marker threshold) |
| Figure range | 9.5 – 138.1 |
| Figure mean | 52.9 |

| Course | Runners | Figs | Min | Max | Mean | Verdict |
|--------|---------|------|-----|-----|------|---------|
| AGE | 31 | 12 | 44.3 | 77.2 | 63.6 | **2 critical issues** (Arabian + bad data) |
| CAV | 31 | 27 | 33.2 | 70.1 | 51.5 | OK |
| LYO | 91 | 85 | 9.5 | 138.1 | 60.7 | **1 critical issue** (R1 inflated) |
| MXD | 89 | 67 | 10.3 | 84.3 | 45.6 | OK |
| PMO | 38 | 34 | 18.9 | 70.1 | 45.4 | OK |
| SAI | 94 | 91 | 11.5 | 78.3 | 51.3 | OK |
| SHT | 42 | 42 | 35.3 | 70.0 | 55.9 | **Non-French venue (Sha Tin)** |

---

## CRITICAL Issues

### 1. LYO Race 1 (900m Souple): Inflated figures — all runners 78–138

Winner **TIMUR** has `finishingTime=48.65s` for a **900m** race. The pace
(~10.87 s/f) is reasonable for a sprint. However, no `LYO_900_Turf` standard
time exists in `standard_times.csv`. The pipeline fell back to `LYO_1000_Turf`
(`standard_time=56.89s`, n=17).

Using a 1000m standard for a 900m race creates an artificial 8.24s "advantage":

| Field | Value |
|---|---|
| finishingTime | 48.65s |
| standard_time (used) | 56.89s (LYO_1000_Turf) |
| distance mismatch | **100m shorter → ~8.2s faster** |
| raw_figure (winner) | **202.7** |
| figure_calibrated | **138.1** |
| Affected runners | All 9 (range 78–138) |

This is a 2yo-only race (all runners age 2, March). Even with correct standard
times, these would likely be low-figure juveniles. Instead they appear as the
highest-rated horses of the day by a wide margin.

**The 6 figures > 100 on this day all come from this single race.**

**Recommendation:** When no exact standard time exists for a distance, the
pipeline should either (a) exclude the race and log a warning, or (b) use
distance-proportional interpolation from the nearest available standard (e.g.
`LYO_1000_Turf × 900/1000`). Applying a longer-distance standard to a shorter
race will always produce inflated figures.

---

### 2. SHT (Sha Tin, Hong Kong): Non-French venue — 42 runners, 3 races

SHT races 5, 6, and 7 are almost certainly from **Sha Tin, Hong Kong**,
imported by PMU for French betting markets. Horse names are clearly not French:

> EVER LUCK, RUN RUN SUNRISE, PACKING PHOENIX, SYNERGY EXPRESS, JOY CAPITAL,
> MIGHTY FIGHTER, STATE SECURITY, KING DANCE, PRESTIGE EMPEROR

Unlike the HPV (Happy Valley) issue flagged in the [17-18 March QA](qa_french_figures_17-18_march_2026.md),
the beaten-length data for SHT parses correctly (varied, realistic margins).
The figures therefore look plausible individually (35–70 range) but are for a
non-French venue using French standard times and going allowances, making them
unreliable.

SHT is **not** currently in `NON_FRENCH_COURSE_CODES` in `constants.py`.
Neither is HPV, which was flagged 12 days ago.

**Fix applied:** Added `"SHT"` and `"HPV"` to `NON_FRENCH_COURSE_CODES` under
a new "Hong Kong" section in `src/france/constants.py`.

---

### 3. AGE Race 1: Arabian race rated as thoroughbred

All 6 runners have the **"AA"** suffix (Anglo-Arabian / Arabian breed):

| Pos | Horse | Figure |
|-----|-------|--------|
| 1 | ROSE DE BOSDA AA | 58.7 |
| 2 | MONAGHAN BOY FOR LIFE AA | 66.8 |
| 3 | LAVANDE AA | 65.5 |
| 4 | MOZARD DE LA BRUNIE AA | 64.6 |
| 5 | LAMIGA D'OCCITANIA AA | 44.3 |
| 6 | MATADOR AA | N/A (back-marker) |

Arabian races have fundamentally different performance standards. These figures
are computed using thoroughbred standard times and calibration, so the numbers
are not meaningful in a thoroughbred context.

**Recommendation:** Detect Arabian races by checking if all (or a majority of)
runners have the "AA" suffix, and exclude or flag them. The detection from
[QC-4 (PR #99)](https://github.com/tomwilson41986/racing-speed-figures/pull/99)
should cover this — verify it is active in the live pipeline.

---

## MEDIUM Issues

### 4. AGE Race 3 (1850m): Impossible finishing time — 38.0s for 1850m

Winner **MOONZIKNUIT** has `finishingTime=38.0s` for an 1850m race.
Expected time is ~118s (`standard_time=117.78s`). A time of 38.0s implies
48.7 m/s (109 mph) — physically impossible.

The pipeline correctly excluded all 13 runners with the comment
`"no historical data to generate figures"`, though the actual reason is bad
PMU source data rather than missing historical data.

| Field | Value |
|---|---|
| finishingTime | **38.0s** (should be ~118s) |
| standard_time | 117.78s (AGE_1850_Turf, n=46) |
| Affected runners | 13 (entire field) |
| Pipeline action | Correctly excluded |

**Recommendation:** The exclusion comment should be more specific (e.g.
"impossible finishing time: 38.0s for 1850m"). The pre-GA sanity filter
recommended in the 17-18 March QA would catch this automatically.

---

## Minor / Informational

### 5. Back-marker exclusions — 45 runners with no figure and no comment

45 runners across multiple races have `figure_final` empty but no
`figure_comment` explaining why. These are all back-markers with large beaten
lengths whose calibrated figure falls below the production threshold.

Sample from AGE R2 (cutoff between pos 7 at 16.25L and pos 8 at 25.75L):

| Pos | Horse | Beaten | Raw Figure | Calibrated |
|-----|-------|--------|------------|------------|
| 7 | MAHALA | 16.25L | 91.7 | 58.4 |
| 8 | BULGROOM | 25.75L | 80.6 | N/A |

This is expected pipeline behaviour (figures below a threshold are unreliable
and excluded) but the missing comment makes QA harder to trace.

**Recommendation:** Add a `figure_comment` like "excluded: below calibration
threshold" for these runners.

### 6. Going allowances — verified reasonable

| Course | Going | GA (s/f) | Assessment |
|--------|-------|----------|------------|
| AGE | Léger | -0.093 | Reasonable (fast-ish ground) |
| CAV | Bon souple | +0.197 to +0.209 | Reasonable |
| LYO | Souple (R1-R2) | -0.865 to -0.083 | **R1 GA contaminated by 900m mismatch** |
| LYO | Bon souple (R3-R8) | +0.003 to +0.111 | Reasonable |
| MXD | PSF LENTE | +0.095 to +0.150 | Reasonable for slow AW |
| MXD | Léger | +0.300 to +0.341 | Reasonable |
| PMO | PSF LENTE | +0.143 to +0.202 | Reasonable |
| SAI | Souple | +0.184 to +0.316 | Reasonable |
| SHT | Bon souple | +0.103 to +0.131 | N/A (non-French) |

LYO R1's GA of **-0.865 s/f** for "Souple" is an artefact of the 900m/1000m
standard-time mismatch. The negative GA implies fast ground, contradicting the
soft going description. LYO R2 (also Souple) has a more moderate -0.083 s/f.

### 7. Calibration — verified consistent

```
figure_calibrated = figure_after_wfa × 0.691258 + (-3.2377)
```

Verified across all 358 runners with figures. Formula applied consistently.

### 8. Weight adjustments — correct

`weight_adj = weightCarried − 126` applied consistently. Spot-checked:
CAV R2 winner LUCKY YOU: weight=120.15 → adj=-5.85 ✓

### 9. WFA adjustments — all zero (correct)

All WFA values are 0.0. This is correct: the single-age-race WFA skip
(implemented 2026-03-22) applies to all today's races where WFA would
otherwise trigger. LYO R3 (all 3yo, Class 5) correctly receives no WFA
adjustment.

---

## Recommended Actions

| Priority | Action | Status |
|----------|--------|--------|
| **Immediate** | Add SHT and HPV to `NON_FRENCH_COURSE_CODES` | ✅ Done |
| **Short-term** | Tighten distance extrapolation tolerance from 20% to 5% in `_interp_single()` to prevent wrong-distance standard time lookups | ✅ Done |
| **Short-term** | Add QC-5: Arabian breed detection via " AA" horse name suffix (≥50% threshold) | ✅ Done |
| **Short-term** | Add specific `figure_comment` for each QC failure type (QC1–QC5) instead of generic fallback | ✅ Done |
| **Short-term** | Add `figure_comment = "excluded: beaten >20 lengths"` for back-marker exclusions | ✅ Done |
