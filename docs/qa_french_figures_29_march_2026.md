# QA Review: French Figures — 29 March 2026

> Reviewed 2026-03-30 against daily ratings output (`ratings_2026-03-29.csv`)
> and QA calculation logic (`qa_calc_logic_2026-03-29.txt`).
> Updated 2026-03-30 after calibration and GA attenuation fixes.

## Overview

| Metric | Value |
|--------|-------|
| Courses | 6 (AGE, CAV, LYO, MXD, PMO, SAI) |
| Races | 38 |
| Races rated | 32 (3 excluded by QC, 3 missing data) |
| Total runners | ~340 |
| Calibration params | scale=0.6677, shift=+16.0 (target_mean=72) |

---

## Findings Summary

| # | Finding | Severity | Action |
|---|---------|----------|--------|
| 1 | Arithmetic / formula chain | PASS | Verified to floating-point precision |
| 2 | WFA bug (single-age races) | FIXED | Code fixed, CSV re-run — WFA now always applied |
| 3 | QC exclusions (3 races) | CORRECT | Arabian, bad data, no std time |
| 4 | **GLOBAL_TARGET_MEAN = 55 (too low)** | **HIGH** | Reverted to 72.0 |
| 5 | **Per-race GA attenuation creates class bias** | **HIGH** | Removed entirely |
| 6 | SAI standard times | PASS | Well-established (n=59-612) |

---

## Fix 1: Calibration Target Reverted (GLOBAL_TARGET_MEAN 55 → 72)

The calibration target was lowered from 72 to 55 on 2026-03-24, systematically suppressing
all figures by ~17 points. The scale factor (0.6677) was correct — it properly compresses
the broader French raw figure distribution (~27 lbs std) to match the UK Timeform distribution
(~18 lbs std). The problem was purely the shift.

**Files changed:** `src/france/speed_figures.py` (lines 65, 1188)

---

## Fix 2: Per-Race GA Attenuation Removed (Class Bias)

The live pipeline (`src/france/live_ratings.py:379-432`) was attenuating the meeting-level
going allowance for individual races where the winner ran closer to standard time than the
meeting average. This created a **systematic class bias**:

- Better horses in higher-class races naturally run faster
- Smaller deviation from standard → GA reduced by up to 50% of the "excess"
- Less going correction → lower figure for the faster horse

**Impact at SAI 2026-03-29:**

| Race | Class | Meeting GA | Attenuated GA | Reduction |
|------|-------|-----------|---------------|-----------|
| R1 (ERDENALI) | 5 | 0.001569 | 0.001569 | 0% |
| R3 (DRUMARD) | 3 | 0.001569 | 0.000917 | **-42%** |
| R5 (DREAMLINER) | 3 | 0.001569 | 0.001229 | **-22%** |
| R7 (CLICCLIC PANPANPAN) | 4 | 0.001569 | 0.001569 | 0% |

DRUMARD (Class 3 Listed winner) lost 42% of the going correction because he ran fast
— exactly what a good horse should do on soft ground.

**File changed:** `src/france/live_ratings.py` — attenuation block removed, uniform
meeting-level GA now used for all races.

---

## Fix 3: WFA in Single-Age Races (Previously Applied)

The `single_age_mask` logic that zeroed WFA for all-3yo and all-2yo races was removed
prior to this review. The current CSV already has correct WFA applied:

| Race | Horse | WFA (lbs) | Verified |
|------|-------|-----------|----------|
| CAV R4 | LIGHTIANA | +12.05 | Yes |
| LYO R3 | ZULU CHANT | +9.06 | Yes |
| PMO R3 | LIVE YOUR LIFE | +12.54 | Yes |
| PMO R5 | THE GREAT TERMS | +13.0 | Yes |
| SAI R1 | ERDENALI | +12.05 | Yes |
| SAI R2 | YMEEELIH | +12.05 | Yes |
| SAI R4 | IMPERIOR | +13.0 | Yes |

---

## Excluded Races (3 QC failures)

| Race | QC Reason | Correct? |
|------|-----------|----------|
| AGE R1 (2500m Léger) | Arabian race — all runners have "AA" suffix | Yes |
| AGE R3 (1850m Léger) | Impossible finishing time (bad PMU source data) | Yes |
| LYO R1 (900m Souple) | No standard time for LYO 900m Turf | Yes |

---

## SAI Standard Times — Verified

| Distance | Std Time (s) | n_races | Divergence | Status |
|----------|-------------|---------|------------|--------|
| 1400m | 86.29 | 166 | 0.04% | Excellent |
| 1600m | 101.22 | 612 | 0.16% | Excellent |
| 2000m | 128.82 | 373 | 0.28% | Excellent |
| 2400m | 155.43 | 414 | 0.04% | Excellent |
| 3000m | 199.68 | 59 | 0.21% | OK |

No issues with SAI standard times.

---

## Going Allowance Review

| Venue | Going | GA (s/m) | GA (s/f) | Assessment |
|-------|-------|----------|----------|------------|
| AGE | Léger | -0.0005 to -0.0014 | -0.09 to -0.27 | Fast ground, correct direction |
| CAV | Bon souple | +0.0006 to +0.0010 | +0.12 to +0.21 | Mildly soft, plausible |
| LYO | Souple/Bon souple | -0.0003 to +0.0007 | -0.06 to +0.14 | Variable within meeting |
| MXD | PSF LENTE / Léger | -0.0002 to +0.0017 | -0.03 to +0.34 | Reasonable spread |
| PMO | PSF LENTE | +0.0007 to +0.0010 | +0.14 to +0.20 | Consistently slow synthetic |
| SAI | Souple | +0.0016 (uniform post-fix) | +0.32 | Genuinely soft |

Post-fix, all SAI races now use the uniform meeting-level GA of +0.001569 s/m.

---

## Arithmetic Verification

Spot-checked 5 winners. All calculations verified to within floating-point precision:

| Winner | corrected_time | deviation_sec | raw_figure | Status |
|--------|---------------|---------------|------------|--------|
| DALYNSKA (AGE R2) | 161.51 | -2.26 | 118.84 | PASS |
| CENTRICAL (LYO R2) | 138.15 | -2.16 | 120.37 | PASS |
| CALA ROSSA (CAV R1) | 95.82 | -0.09 | 101.15 | PASS |
| EL TUBER STORM (MXD R1) | 84.44 | -0.38 | 106.03 | PASS |
| NOBLEMAN (CAV R6) | 139.16 | -0.94 | 108.70 | PASS |

---

## Winner-by-Winner Rating Logic

### Formula

Every winner's figure follows:

1. `corrected_time = finishingTime - (going_allowance × distance)`
2. `deviation_seconds = corrected_time - standard_time`
3. `deviation_lengths = deviation_seconds / 0.2`
4. `deviation_lbs = deviation_lengths × lpl`
5. `raw_figure = 100 - deviation_lbs`
6. `weight_adj = weightCarried - 126`
7. `figure_after_wfa = raw_figure + weight_adj + wfa_adj`
8. `figure_final = figure_after_wfa × scale + shift`

### AGE — Agen (Turf, Léger)

**AGE R2 — DALYNSKA** (2500m, Class 6)
Ran 158.10s on slightly fast ground (GA=-0.0014 s/m). After going correction, 2.26s faster
than standard (163.77s). Raw figure 118.8. Weight penalty -2.5 lbs. Strong for the grade.

### CAV — Craon (Turf, Bon souple)

**CAV R1 — CALA ROSSA** (1600m, Class 6)
Ran 97.39s on easing ground. After correction, essentially standard time (0.09s faster).
Raw 101.2. Standard-level performance.

**CAV R2 — LUCKY YOU** (2200m, Class 7)
10-year-old veteran. 2.25s slower than standard after correction. Raw 79.9 plus heavy
light-weight penalty (-5.8 lbs). Weak winning performance.

**CAV R4 — LIGHTIANA** (1600m, Class 6, 3yo)
All-3yo race. 0.77s slower than standard. Raw 90.4, weight penalty -5.8 lbs,
WFA credit +12.0 lbs. Moderate performance.

**CAV R6 — NOBLEMAN** (2200m, Class 6)
0.94s faster than standard. Raw 108.7. Best figure at Craon.

### LYO — Lyon-Parilly (Turf, Souple / Bon souple)

**LYO R2 — CENTRICAL** (2200m, Class 5)
Day's standout. 2.16s faster than standard despite "Souple" label (per-meeting GA was
actually slightly negative). Raw 120.4 with large weight credit (+11.8 lbs, carrying ~138 lbs).
An 8yo veteran running an exceptional time under heavy weight. Top 3 within 0.3L.

**LYO R3 — ZULU CHANT** (2200m, Class 5, 3yo)
7.25s slower than standard. Raw 37.1 even with WFA (+9.1 lbs). Weak field on
deteriorating ground.

**LYO R4 — STAR CREEK** (2000m, Class 5)
0.55s faster than standard. Raw 105.2 + weight credit (+4.1 lbs). Solid.

**LYO R5 — PISORNO** (2000m, Class 6)
8yo. 0.20s faster than standard. Raw 101.9 + weight credit (+5.2 lbs). Strong for Class 6.

**LYO R6 — TERREDEGUERRE** (1600m, Class 6)
0.95s faster than standard on essentially true ground (GA≈0). Raw 111.8. Best Class 6 at Lyon.

**LYO R7 — RIASSOU** (1600m, Class 6)
1.45s slower than standard. Raw 82.5. Modest.

**LYO R8 — SINDBAD** (2400m, Class 6)
0.41s faster than standard. Raw 103.4. Fair for a Class 6 stayer.

### MXD — Montevideo (Mixed: PSF + Turf)

**MXD R1 — EL TUBER STORM** (1400m AW, Class 7)
0.38s faster than standard. Raw 106.0 but heavy weight penalty (-9.2 lbs). Decent for Class 7 AW.

**MXD R2 — FUEGO NEGRO** (1600m AW, Class 6)
1.77s slower than standard. Raw 76.0. Below-par winner.

**MXD R3 — MANDOR** (1500m Turf, Class 6)
0.31s faster than standard. Raw 104.2. Good Class 6.

**MXD R4 — TAPAJOS** (1400m Turf, Class 7)
0.53s slower than standard. Raw 92.6. Average.

**MXD R5 — GHOST VH** (1400m Turf, Class 7)
0.57s slower than standard. Raw 92.0. Almost identical to TAPAJOS.

**MXD R6 — SIEMPRE FELIZ** (2000m Turf, Class 6)
0.27s faster than standard. Raw 102.7 + weight credit (+4.1 lbs). Solid.

**MXD R7 — SI SENOR** (1600m AW, Class 5)
1.44s faster than standard. Raw 120.4 + large weight credit (+6.3 lbs). Outstanding — won by 3.5L.

**MXD R8 — KODIAK DANCER** (1400m AW, Class 7)
0.98s slower than standard. Raw 84.9. Fair.

**MXD R9 — GAL COSTA LUZ** (1500m Turf, Class 6)
0.29s slower than standard. Raw 96.1 but heavy weight penalty (-9.2 lbs). Below average.

### PMO — Palermo (AW, PSF LENTE)

**PMO R1 — HOT CAT** (1800m, Class 7)
7yo. 1.33s slower than standard. Raw 84.0 - weight penalty (-7.0 lbs). Moderate.

**PMO R2 — PRIVATE BLEND** (1800m, Class 6)
0.53s faster than standard. Raw 106.5. Best figure at Palermo.

**PMO R3 — LIVE YOUR LIFE** (1500m, Class 6, 3yo)
All-3yo. 0.32s slow. Raw 95.4, weight penalty -4.7 lbs, WFA +12.5 lbs. Better than
uncorrected figure suggests.

**PMO R4 — CHICO LOCO** (1200m, Class 7)
Near-standard time (0.04s faster). Raw 100.8. Par-level sprint.

**PMO R5 — THE GREAT TERMS** (1400m, Class 5, 3yo)
All-3yo. 0.35s slow. Raw 94.6, weight penalty -4.7 lbs, WFA +13.0 lbs. Respectable for
Class 5 once WFA-corrected.

### SAI — Saint-Cloud (Turf, Souple)

**SAI R1 — ERDENALI** (1600m, Class 5, 3yo)
Ran 105.63s on soft ground (GA=+0.0016 s/m — highest GA on the card). After correction,
1.90s slower than standard. Raw 77.8 + weight credit (+1.9 lbs) + WFA (+12.0 lbs).
An eye-catching winner in testing conditions. Post-fix with uniform GA, figure should
rise further.

**SAI R2 — YMEEELIH** (1600m, Class 5, 3yo)
3.06s slower than standard after correction. Raw 64.7. Even with WFA (+12.0 lbs), weak.
Either ground was worse than GA captured, or a weak field.

**SAI R3 — DRUMARD** (2000m, Class 3/Listed)
Ran 129.35s on soft ground. After correction with uniform meeting GA (0.001569 s/m → 3.14s
correction over 2000m), corrected time = 126.21s, which is 2.61s faster than standard
(128.82s). Raw figure ~125.6, plus weight credit (+5.2 lbs). Best figure at Saint-Cloud and
appropriate for Listed level. Previously penalised by GA attenuation (0.000917 instead of
0.001569).

**SAI R4 — IMPERIOR** (1400m, Class 4, 3yo)
1.96s slower than standard after correction. Raw 73.7. Even with WFA (+13.0 lbs), modest
for Class 4 — soft ground penalised sprinters at Saint-Cloud.

**SAI R5 — DREAMLINER** (1600m, Class 3)
Ran 102.64s. After uniform GA correction (0.001569 s/m → 2.51s over 1600m), corrected time
= 100.13s, 1.09s faster than standard. Raw ~113.1 - weight penalty (-0.3 lbs). Solid
for Class 3. Previously penalised by GA attenuation (0.001229 instead of 0.001569).

**SAI R6 — ASMARANI** (3000m, Class 4)
Ran 201.41s over the marathon trip. 1.49s faster than standard after correction. Raw 109.7.
Strong stayer's performance.

**SAI R7 — CLICCLIC PANPANPAN** (2000m, Class 4)
Corrected time was almost exactly standard. Raw 99.9 - weight penalty (-8.1 lbs). Par-level
but weight pulls the figure down.

**SAI R8 — SPANISH PRINCE** (2400m, Class 5)
0.12s slow — essentially standard time. Raw 99.0. Average Class 5 stayer.
