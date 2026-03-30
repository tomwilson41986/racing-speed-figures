# QA Review: French Figures — 29 March 2026

> Reviewed 2026-03-30 against daily ratings output (`ratings_2026-03-29.csv`)
> and QA calculation logic (`qa_calc_logic_2026-03-29.txt`).

## Overview

| Metric | Value |
|--------|-------|
| Courses | 6 (AGE, CAV, LYO, MXD, PMO, SAI) |
| Races | 38 |
| Races rated | 32 (3 excluded by QC, 3 missing data) |
| Total runners | ~340 |
| Figure range | 11.8 – 87.3 (calibrated) |
| Calibration params | scale=0.6677, shift=-0.986 (from batch artifacts) |

---

## BUG FIX: WFA not applied in single-age races

**Severity: HIGH**

The pipeline was zeroing out WFA adjustments for races where all runners share the same age. This is incorrect — WFA must always be applied so that figures are comparable across the entire population (e.g. a 3yo-only Class 5 figure should be on the same scale as a mixed-age handicap).

**Root cause:** `single_age_mask` logic in both `src/france/live_ratings.py` (line 471-473) and `src/france/speed_figures.py` (line 1156-1159) set `wfa_adj=0.0` when `race_age_nunique <= 1`.

**Impact on this card:** 7 races were all-3yo (CAV R4, LYO R3, PMO R3, PMO R5, SAI R1, SAI R2, SAI R4) and 1 race was all-2yo (LYO R1). All runners in these races received wfa_adj=0.0 instead of ~9-13 lbs.

**Fix applied:** Removed the single-age zeroing in both files. WFA is now always applied.

**Corrected 3yo winner figures** (approximate, pending artifact rebuild + re-run):

| Race | Horse | Old Figure | WFA (lbs) | Corrected Figure | Change |
|------|-------|-----------|-----------|-----------------|--------|
| CAV R4 | LIGHTIANA | 55.5 | +12.0 | ~63.5 | +8.0 |
| LYO R3 | ZULU CHANT | 25.0 | +9.1 | ~31.1 | +6.1 |
| PMO R3 | LIVE YOUR LIFE | 59.5 | +12.5 | ~67.9 | +8.3 |
| PMO R5 | THE GREAT TERMS | 59.0 | +13.0 | ~67.7 | +8.7 |
| SAI R1 | ERDENALI | 52.2 | +12.0 | ~60.3 | +8.0 |
| SAI R2 | YMEEELIH | 43.5 | +12.0 | ~51.5 | +8.0 |
| SAI R4 | IMPERIOR | 48.0 | +13.0 | ~56.7 | +8.7 |

> Note: Exact corrected figures will differ slightly after artifact rebuild, since calibration params will be re-fitted with WFA always applied in the batch pipeline.

---

## Excluded Races (3 QC failures)

| Race | QC Reason | Correct? |
|------|-----------|----------|
| AGE R1 (2500m Léger) | Arabian race — all runners have "AA" suffix | Yes — QC-5 detection working |
| AGE R3 (1850m Léger) | Impossible finishing time (bad PMU source data) | Yes — correctly excluded |
| LYO R1 (900m Souple) | No standard time for LYO 900m Turf | Yes — previous run used wrong 1000m standard; now correctly excluded after tolerance tightening |

---

## Winner-by-Winner Rating Logic

### How the formula works

Every winner's figure follows the same pipeline:

1. **Correct for going**: `corrected_time = finishingTime - (going_allowance × distance)`
2. **Compare to standard**: `deviation_seconds = corrected_time - standard_time`
3. **Convert to lengths**: `deviation_lengths = deviation_seconds / 0.2`
4. **Convert to lbs**: `deviation_lbs = deviation_lengths × lpl`
5. **Raw figure**: `raw_figure = 100 - deviation_lbs` (100 = standard time on good ground)
6. **Weight adjustment**: `figure_after_weight = raw_figure + (weightCarried - 126)`
7. **WFA adjustment**: `figure_after_wfa = figure_after_weight + wfa_adj`
8. **Calibrate**: `figure_final = figure_after_wfa × 0.6677 - 0.986`

A figure of 100 raw means the horse ran exactly standard time after going correction. Higher = faster.

---

### AGE — Agen (Turf, Léger)

**AGE R2 — DALYNSKA** (2500m, Class 6, figure **76.7**)
Ran 158.10s over 2500m on slightly fast ground (GA=-0.0014 s/m). After going correction, her time was 161.51s — **2.26s faster than standard** (163.77s). That converts to 11.3 lengths, or 18.8 lbs via the LPL (1.671). Raw figure 118.8. After weight penalty (-2.5 lbs for carrying light) and calibration, final figure 76.7. A strong performance for the grade — comfortably the best winner at Agen.

---

### CAV — Craon (Turf, Bon souple)

**CAV R1 — CALA ROSSA** (1600m, Class 6, figure **65.6**)
Ran 97.39s over 1600m on easing ground (GA=+0.0010 s/m). After going correction, her time (95.82s) was almost exactly standard time (95.91s) — just 0.09s faster. Raw figure 101.2, essentially par for the course/distance. Light weight penalty (-1.4 lbs), calibrates to 65.6. A standard-level performance.

**CAV R2 — LUCKY YOU** (2200m, Class 7, figure **48.4**)
A 10-year-old veteran. Ran 144.64s over 2200m on easing ground. After correction, 2.25s **slower** than standard — raw figure just 79.9. Significant light-weight penalty (-5.8 lbs, carrying ~120 lbs). Final 48.4. Weak winning performance in a low-grade race.

**CAV R4 — LIGHTIANA** (1600m, Class 6, figure **55.5** → ~63.5 with WFA fix)
A 3yo in an all-3yo race. Ran 98.34s, correcting to 0.77s slower than standard. Raw figure 90.4. Significant light-weight penalty (-5.8 lbs). **Missing WFA of +12.0 lbs** for a 3yo in March at 1600m would lift this to ~63.5. Moderate performance for the class.

**CAV R6 — NOBLEMAN** (2200m, Class 6, figure **69.9**)
Ran 140.50s over 2200m on easing ground (GA=+0.0006 s/m). After correction, 0.94s faster than standard. Raw figure 108.7, well above par. Weight penalty (-2.5 lbs). Calibrates to 69.9. The best figure at Craon — a solid Class 6 performance over middle distances.

---

### LYO — Lyon-Parilly (Turf, Souple / Bon souple)

**LYO R2 — CENTRICAL** (2200m, Class 5, figure **87.3** — day's best)
Ran 137.49s over 2200m on ground reading "Souple" but with a negative GA (-0.0003 s/m), meaning the ground was actually riding slightly fast. After correction, 2.16s faster than standard — 10.8 lengths, 20.4 lbs. Raw figure 120.4. **Very large weight credit (+11.8 lbs)** — carrying ~138 lbs as top weight. An 8-year-old veteran running an exceptional time under heavy weight. This is a standout figure for Class 5, warranting closer inspection if the time data is reliable. Beaten 0.05L by HUNGRY HEART (81.3) and 0.3L by WALDKAUZ (78.0), suggesting the field was genuinely high-quality.

**LYO R3 — ZULU CHANT** (2200m, Class 5, figure **25.0** → ~31.1 with WFA fix)
A 3yo in an all-3yo race. Ran 149.07s — a full 7.25s slower than standard after going correction. Raw figure just 37.1. Even with WFA (+9.1 lbs), this is very weak (~31). The GA for this race (Bon souple, +0.0007 s/m) may not fully capture genuinely deteriorating ground later in the card, or this was simply a very weak field.

**LYO R4 — STAR CREEK** (2000m, Class 5, figure **72.0**)
Ran 132.02s, correcting to 0.55s faster than standard. Raw 105.2 plus significant weight credit (+4.1 lbs). Final 72.0. Solid Class 5 performance.

**LYO R5 — PISORNO** (2000m, Class 6, figure **70.5**)
An 8-year-old. Ran 132.71s, correcting to just 0.20s faster than standard. Raw 101.9, boosted by weight credit (+5.2 lbs). Final 70.5. Strong for Class 6.

**LYO R6 — TERREDEGUERRE** (1600m, Class 6, figure **71.3**)
Ran 98.30s with a near-zero GA (+0.0001 s/m), meaning ground was essentially true. Corrected time 0.95s faster than standard. Raw 111.8 minus weight penalty (-3.6 lbs). Final 71.3. The best Class 6 winner at Lyon — ran to a figure that would be competitive in Class 5.

**LYO R7 — RIASSOU** (1600m, Class 6, figure **53.1**)
Ran 101.64s — 1.45s slower than standard after going correction. Raw figure just 82.5. Modest winning performance. The going here (Bon souple, GA=+0.0007 s/m) had deteriorated from earlier in the card.

**LYO R8 — SINDBAD** (2400m, Class 6, figure **66.4**)
Ran 155.88s over a staying trip, correcting to 0.41s faster than standard. Raw 103.4, minus weight penalty (-2.5 lbs). Final 66.4. Fair for a Class 6 staying race.

---

### MXD — Montevideo (Mixed: PSF + Turf, Léger / PSF LENTE)

**MXD R1 — EL TUBER STORM** (1400m AW, Class 7, figure **63.7**)
Ran 85.10s on the All Weather (PSF LENTE = slow synthetic surface). After going correction, 0.38s faster than standard. Raw 106.0, but heavy light-weight penalty (-9.2 lbs — carrying ~117 lbs). Calibrates to 63.7. Decent for Class 7 AW.

**MXD R2 — FUEGO NEGRO** (1600m AW, Class 6, figure **49.6**)
Ran 100.18s — 1.77s slower than standard after going correction. Raw figure just 76.0. Final 49.6. Below-par winner, potentially a weak field.

**MXD R3 — MANDOR** (1500m Turf, Class 6, figure **66.9**)
Ran 91.31s on Turf going "Léger" (GA=+0.0015 s/m, moderately soft). After correction, 0.31s faster than standard. Raw 104.2 minus weight penalty (-2.5 lbs). Final 66.9. Good Class 6 performance on Turf.

**MXD R4 — TAPAJOS** (1400m Turf, Class 7, figure **60.6**)
Ran 86.31s. After going correction, 0.53s slower than standard. Raw 92.6 with minimal weight adj (-0.3 lbs). Final 60.6. Average Class 7.

**MXD R5 — GHOST VH** (1400m Turf, Class 7, figure **60.2**)
Very similar profile to MXD R4 — ran 86.35s, 0.57s slow after correction. Raw 92.0. Final 60.2. Almost identical figure to TAPAJOS, consistent with similar conditions/class.

**MXD R6 — SIEMPRE FELIZ** (2000m Turf, Class 6, figure **70.3**)
Ran 125.47s over 2000m on Léger ground. After correction, 0.27s faster than standard. Raw 102.7 plus weight credit (+4.1 lbs). Final 70.3. Solid middle-distance performance.

**MXD R7 — SI SENOR** (1600m AW, Class 5, figure **83.6** — 2nd highest of day)
Ran 95.53s on the AW — 1.44s faster than standard after going correction. Raw 120.4, plus large weight credit (+6.3 lbs, carrying ~132 lbs). Calibrates to 83.6. An outstanding performance for Class 5 AW. The 3.5L gap to the runner-up suggests this horse was well above this grade.

**MXD R8 — KODIAK DANCER** (1400m AW, Class 7, figure **55.5**)
Ran 86.85s — 0.98s slower than standard. Raw 84.9. Final 55.5. Fair Class 7 AW.

**MXD R9 — GAL COSTA LUZ** (1500m Turf, Class 6, figure **57.1**)
Ran 92.23s. After correction, 0.29s slower than standard. Raw 96.1, but heavy light-weight penalty (-9.2 lbs). Final 57.1. Below-average winner dragged down by the weight adjustment — carried very little weight.

---

### PMO — Palermo (AW, PSF LENTE)

**PMO R1 — HOT CAT** (1800m, Class 7, figure **50.5**)
A 7-year-old. Ran 113.83s — 1.33s slower than standard. Raw 84.0, minus weight penalty (-7.0 lbs). Final 50.5. Moderate Class 7.

**PMO R2 — PRIVATE BLEND** (1800m, Class 6, figure **69.9**)
Ran 111.45s — 0.53s faster than standard after correction. Raw 106.5 with minimal weight adj (-0.3 lbs). Final 69.9. The best figure at Palermo — strong Class 6 performance and the only PMO winner to beat standard time.

**PMO R3 — LIVE YOUR LIFE** (1500m, Class 6, figure **59.5** → ~67.9 with WFA fix)
A 3yo in all-3yo race. Ran 93.05s, 0.32s slow after correction. Raw 95.4, minus weight penalty (-4.7 lbs). **Missing WFA of +12.5 lbs** for a March 3yo at 1500m would lift this to ~67.9. A much better performance than the uncorrected figure suggests.

**PMO R4 — CHICO LOCO** (1200m, Class 7, figure **66.1**)
Ran 72.77s over the sprint trip — almost exactly standard time (just 0.04s faster after correction). Raw 100.8. Final 66.1. Par-level sprint.

**PMO R5 — THE GREAT TERMS** (1400m, Class 5, figure **59.0** → ~67.7 with WFA fix)
A 3yo in all-3yo race. Ran 86.59s, 0.35s slow. Raw 94.6 minus weight penalty (-4.7 lbs). **Missing WFA of +13.0 lbs** at 1400m would lift this to ~67.7. Respectable for Class 5 once WFA-corrected.

---

### SAI — Saint-Cloud (Turf, Souple)

**SAI R1 — ERDENALI** (1600m, Class 5, figure **52.2** → ~60.3 with WFA fix)
A 3yo. Ran 105.63s on soft ground (GA=+0.0016 s/m — the highest GA on the card). After correction, 1.90s slower than standard. Raw 77.8 plus small weight credit (+1.9 lbs). **Missing WFA +12.0 lbs** lifts to ~60.3. The soft ground significantly slowed times at Saint-Cloud — all SAI figures should be interpreted in that context.

**SAI R2 — YMEEELIH** (1600m, Class 5, figure **43.5** → ~51.5 with WFA fix)
A 3yo. Ran 106.79s — 3.06s slower than standard after correction. Raw figure just 64.7. Even with WFA (+12.0 lbs), only ~51.5. Either the going was even worse than the GA captured for this race, or this was a weak 3yo field.

**SAI R3 — DRUMARD** (2000m, Class 3, figure **77.8**)
Ran 129.35s over 2000m on soft ground (GA=+0.0009 s/m). After correction, 1.30s faster than standard. Raw 112.8 plus weight credit (+5.2 lbs). Final 77.8. **Best figure at Saint-Cloud** and the highest-rated winner in a Class 3 (Listed-quality) contest. Appropriate figure for the class level.

**SAI R4 — IMPERIOR** (1400m, Class 4, figure **48.0** → ~56.7 with WFA fix)
A 3yo. Ran 90.45s — 1.96s slower than standard after correction. Raw 73.7. Even with WFA (+13.0 lbs), ~56.7 is modest for Class 4. The soft ground penalised sprinters particularly hard at Saint-Cloud.

**SAI R5 — DREAMLINER** (1600m, Class 3, figure **69.9**)
Ran 102.64s. After going correction (GA=+0.0012 s/m — slightly less soft than R1/R2), 0.54s faster than standard. Raw 106.5. Final 69.9. Solid for Class 3, though below DRUMARD.

**SAI R6 — ASMARANI** (3000m, Class 4, figure **70.6**)
Ran 201.41s over the marathon trip on soft ground. After correction, 1.49s faster than standard over 3000m. Raw 109.7 minus weight penalty (-2.5 lbs). Final 70.6. Strong stayer's performance — the long distance and low LPL (1.302) mean each second of deviation converts to fewer lbs.

**SAI R7 — CLICCLIC PANPANPAN** (2000m, Class 4, figure **60.3**)
Ran 131.97s. After going correction, the corrected time was almost exactly on standard (0.01s slow). Raw 99.9 minus weight penalty (-8.1 lbs). Final 60.3. Par-level performance — the weight penalty (carrying very little) pulls the figure down despite good raw speed.

**SAI R8 — SPANISH PRINCE** (2400m, Class 5, figure **63.4**)
Ran 159.32s. After going correction, just 0.12s slow — essentially standard time. Raw 99.0 minus weight penalty (-2.5 lbs). Final 63.4. Average Class 5 stayer.

---

## Arithmetic Verification

Spot-checked 5 winners. All calculations verified to within floating-point precision:

| Winner | corrected_time | deviation_sec | raw_figure | figure_final | Status |
|--------|---------------|---------------|------------|-------------|--------|
| DALYNSKA | 161.51 | -2.26 | 118.84 | 76.67 | ✓ |
| CENTRICAL | 138.15 | -2.16 | 120.37 | 87.26 | ✓ |
| CALA ROSSA | 95.82 | -0.09 | 101.15 | 65.60 | ✓ |
| EL TUBER STORM | 84.44 | -0.38 | 106.03 | 63.70 | ✓ |
| NOBLEMAN | 139.16 | -0.94 | 108.70 | 69.90 | ✓ |

---

## Going Allowance Review

| Venue | Going | GA (s/f) | Assessment |
|-------|-------|----------|------------|
| AGE | Léger | -0.093 | Fast ground, reasonable |
| CAV | Bon souple | +0.197 to +0.209 | Mildly soft, reasonable |
| LYO | Souple (R2) | -0.060 | Slightly fast for "Souple" — but per-meeting empirical |
| LYO | Bon souple (R3-R8) | +0.019 to +0.138 | Variable within meeting, reasonable range |
| MXD | PSF LENTE (AW) | -0.031 to +0.150 | Reasonable spread for AW |
| MXD | Léger (Turf) | +0.294 to +0.336 | Moderately soft Turf |
| PMO | PSF LENTE (AW) | +0.143 to +0.202 | Consistent slow AW |
| SAI | Souple | +0.197 to +0.316 | Genuinely soft — highest GAs on card |

LYO R6 has a notably low GA (+0.019 s/f) compared to other LYO Bon souple races (+0.083 to +0.138). This may reflect split-card detection or natural meeting GA variation.

---

## Summary of Findings

| Priority | Finding | Action |
|----------|---------|--------|
| **HIGH** | WFA zeroed for single-age races (7 affected races, ~80 runners) | Fixed in live_ratings.py + speed_figures.py |
| **INFO** | 3 races correctly excluded (Arabian, bad data, no standard) | QC working as expected |
| **INFO** | CENTRICAL (87.3) and SI SENOR (83.6) are standout figures | Plausible given weight carried + fast times |
| **INFO** | ZULU CHANT (25.0) very weak even with WFA correction (~31) | Likely genuinely slow race |
| **ACTION** | Rebuild artifacts then re-run ratings to get corrected figures | Needed to apply WFA fix end-to-end |
