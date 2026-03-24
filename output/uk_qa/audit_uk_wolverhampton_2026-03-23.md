# QA Audit Report: UK Ratings 2026-03-23 (Wolverhampton)

**Date reviewed:** 2026-03-24
**Data source:** HorseRaceBase CSV (auto-downloaded, user 10327)
**Meeting:** Wolverhampton (All Weather), 9 races, 67 runners (64 rated, 3 DNF)
**Surface:** All Weather | **Going:** Standard

---

## Executive Summary

The Wolverhampton ratings for 23 March 2026 contain **1 critical issue**, **2 high-priority issues**, and **3 medium-priority observations**. The meeting was a 9-race all-weather card spanning 6f–9.5f with a mix of Class 4–6 handicaps and novice events. Of 67 runners, 64 received figures and 3 were correctly excluded as non-finishers.

Overall figure-OR correlation is **0.329** (N=51 with ORs) with **MAE 12.4** — well below the target of 0.90 / 7.0. However, this is driven by a systematic **distance-dependent bias**: 6f races produce figures ~14–19 lbs *above* OR, while 8.6f–9.5f races produce figures ~21–30 lbs *below* OR. Within individual races, internal ordering is generally correct and weight-driven anomalies are all legitimate.

| Severity | Count | Summary |
|----------|-------|---------|
| CRITICAL | 1 | Distance-dependent calibration bias: 6f inflated (+14 to +19 vs OR), 8.6–9.5f depressed (-21 to -30 vs OR) |
| HIGH | 2 | R5 winner fig 30 below OR (39 vs 69); R4 winner fig 21 below OR (66 vs 87) |
| MEDIUM | 3 | R5 winner/2nd 16pt reversal (weight-correct); R1 novice unvalidatable (all OR=0); R3 winner 11 above OR |

---

## 1. CRITICAL: Distance-Dependent Calibration Bias

The most striking pattern across the card is a systematic relationship between race distance and figure-OR gap:

| Distance | Races | Winner Mean Fig-OR | Direction |
|----------|-------|--------------------|-----------|
| ~6.1f | R7, R8, R9 | **+13.0** | Inflated |
| ~7.2f | R2, R3 | **+5.5** | Slightly inflated |
| ~8.6f | R4 (R1 has no OR) | **-20.6** | Depressed |
| ~9.5f | R5, R6 | **-25.5** | Heavily depressed |

**Band-level bias (all rated runners with OR > 0):**

| OR Band | n | Mean Diff (Fig-OR) | MAE |
|---------|---|--------------------|-----|
| 40–55 | 27 | **0.0** | 8.8 |
| 55–70 | 13 | **-11.9** | 15.3 |
| 70–90 | 11 | **-17.7** | 18.0 |

The OR 70–90 horses (mostly in the longer 8.6f–9.5f Class 4 races) are systematically under-rated by ~18 lbs, while the OR 40–55 horses (mostly in the shorter 6f Class 6 races) are accurately centred.

### Cross-race evidence

**Same distance, near-identical times, very different figures:**

- **R5 winner** (9.5f, Class 5): Little Miss India, 120.31s, **Fig=39**, OR=69, age 5, wgt 124
- **R6 winner** (9.5f, Class 4): Hard To Believe, 120.35s, **Fig=56**, OR=77, age 3, wgt 133

The 17-point gap is explained by: weight adj (+9 swing) + WFA for 3yo (+6.8) ≈ +16. This is mechanically correct, but both runners are **25–30 lbs below their OR**, suggesting the Wolverhampton 9.5f standard time or calibration offset is too aggressive.

- **R7 winner** (6.1f, Class 6): Piperstown, 73.09s, **Fig=71**, OR=52 → **+19 above OR**
- **R9 winner** (6.1f, Class 4): Yorkshire Glory, 71.82s, **Fig=72**, OR=66 → **+6 above OR**

The R9 winner ran 1.27s faster than R7 winner but receives only +1 on figure. Meanwhile, R7's winner is rated 19 lbs above its official rating — an implausibly large positive deviation for a Class 6 horse.

### Root Cause Analysis

The going allowance for the entire meeting is **-0.382 s/f** (computed from 9 winners). At shorter distances, a negative GA has less absolute effect (6.1f × -0.38 = -2.3s), while at longer distances the effect compounds (9.5f × -0.38 = -3.6s). If the GA is even slightly too negative, it will systematically inflate short-distance figures and depress long-distance figures — exactly the pattern observed.

**Additionally**, the AW standard times may have a distance-dependent error at Wolverhampton specifically. The standard times used are:
- 6.1f: 82.57s
- 7.2f: 99.50s
- 8.6f: 121.39s
- 9.5f: 131.05s

### Recommendation

| Priority | Action |
|----------|--------|
| P0 | Audit the going allowance calculation — the single GA applied across all distances may be inappropriate when race distances span 6f–9.5f |
| P0 | Investigate whether Wolverhampton standard times at 8.6f and 9.5f are based on sufficient data; compare against recent AW standard times at other tracks |
| P1 | Consider distance-specific going allowances (short/medium/long) or at minimum validate that the global GA doesn't introduce systematic distance bias |

---

## 2. HIGH: Race 4 Figures Depressed 21 lbs Below OR

**Race 4** (3yo handicap, ~8.6f, Class 4, 4 runners) shows a consistent negative bias:

| Horse | Pos | OR | Figure | Diff |
|-------|-----|-----|--------|------|
| Laureate Crown (IRE) | 1st | 87 | 66 | -21 |
| Waterford Castle (IRE) | 2nd | 78 | 59 | -19 |
| Gorey Gold (IRE) | 3rd | 84 | 48 | -36 |
| Born A Star (IRE) | 4th | 77 | 34 | -43 |

**Mean diff: -29.9 | Correlation: 0.628 | MAE: 29.9**

All runners are 3yos with WFA adjustment (+8.0), but this is insufficient to compensate for the distance-related depression. The winner ran 109.29s — 1.6s faster than the R1 winner (110.88s) at the same distance — and the 28-point figure gap (66 vs 38) aligns with this time difference plus the 14lb weight difference. The internal ordering is reasonable but the absolute level is far below OR.

---

## 3. HIGH: Race 5 Figures Depressed 28 lbs Below OR

**Race 5** (~9.5f, Class 5, 5 runners) produces the most depressed figures:

| Horse | Pos | OR | Figure | Diff |
|-------|-----|-----|--------|------|
| Little Miss India (IRE) | 1st | 69 | 39 | -30 |
| Rose Cotton | 2nd | 75 | 56 | -19 |
| Samra Star (IRE) | 3rd | 60 | 31 | -29 |
| Maywedance (IRE) | 4th | 57 | 29 | -28 |
| Bella Bisbee | 5th | 65 | 32 | -33 |

**Mean diff: -27.8 | Correlation: 0.928 | MAE: 27.8**

Internal ordering correlation is excellent (0.928), confirming this is a systematic level shift, not noise. Rose Cotton (2nd) receives the highest figure despite finishing 2nd — see Medium finding #4 below.

---

## 4. MEDIUM: Race 5 Winner/2nd Reversal (+16 pts)

Rose Cotton (2nd, beaten 0.15L) receives fig=56 vs winner Little Miss India's fig=39 — a 16-point reversal. This is explained by:

- Rose Cotton carried 135 lbs vs Little Miss India's 124 lbs (+11 lb difference)
- Weight adjustment: Rose Cotton gets +9, Little Miss India gets -2 (net +11)
- Plus Rose Cotton received sex_allowance = +5

Only 0.15L separated them, so the raw figures are nearly identical. The weight adjustment then correctly elevates the heavier horse's figure. **This is mathematically correct behaviour** — Rose Cotton's figure reflects that she was carrying a significant burden and still ran essentially level.

**No action required.**

---

## 5. MEDIUM: Race 1 (Novices) Unvalidatable — All OR = 0

Race 1 (8.6f, Class 4 Novices, 10 runners) has no official ratings for any runner, making external validation impossible. Internal ordering follows finishing position exactly (excluding Laravie, a 7yo carrying 137 lbs who finished 9th but rates above the 8th-place finisher due to +14 lbs weight difference).

The winner Estissa (fig=38) ran 1.6s slower than R4 winner Laureate Crown (fig=66) at the same distance and class. The 28-point gap is consistent with the time difference and both being 3yo races. **Figures appear plausible but cannot be externally validated.**

---

## 6. MEDIUM: Race 3 Winner Romanovich 11 Above OR

Romanovich (IRE) won R3 (7.2f, Class 6) in 88.31s with fig=65 vs OR=54 (+11). This is the largest positive deviation for any winner. The horse is a 7yo carrying 134 lbs:
- Weight adj: +8
- No WFA (age 7)

For comparison, R2 winner Fistral Beach (IRE) at the same distance/class ran 89.33s with fig=55 vs OR=55 (exact match). Romanovich ran 1.02s faster and gets +10 on figure — proportionate to the time gap. The +11 above OR may simply reflect a career-best performance, or may indicate the 7.2f figures are slightly inflated (consistent with the distance-dependent bias pattern, though less extreme than the 6f inflation).

---

## 7. Positive Findings

1. **Race 2 is excellently calibrated:** Correlation 0.763, MAE 4.3, mean diff -4.3. The winner Fistral Beach's figure exactly matches its OR (55). Internal ordering is very good.

2. **Race 9 internal ordering is correct:** Despite the top two on figure (Betsen 78, Dyrholaey 76) finishing 4th and 5th, both carried 136 lbs (top weight, +10 adj) while the winner Yorkshire Glory carried only 116 lbs (-10 adj). The 20lb weight swing correctly explains the reversal. Betsen's OR of 86 is the highest in the race, and its fig of 78 reflects carrying that weight and still running competitively.

3. **Weight adjustments work correctly throughout:** All 17 ordering anomalies are explained by weight-carried differentials. No unexplained reversals.

4. **DNF handling is correct:** 3 non-finishers (No Nay Nevermind R2, Clarissa Eclipse R6, Poet R8) correctly received null figures.

5. **100% coverage of finishers:** 64/64 finishing runners received a calibrated figure.

6. **Cross-race consistency within same distance:** R7/R8 (both 6.1f Class 6) produce similar winner figures (71 vs 69) for similar times (73.09s vs 73.58s) — good internal consistency.

---

## Recommendations Summary

| Priority | Action | Impact |
|----------|--------|--------|
| P0 | Investigate distance-dependent GA bias — single GA across 6f–9.5f may be inappropriate | Affects all 9 races, root cause of distance skew |
| P0 | Audit Wolverhampton AW standard times at 8.6f and 9.5f | Fixes R4/R5/R6 depression (-21 to -30 vs OR) |
| P1 | Consider distance-band going allowances (short/medium/long) for single-venue cards | Systemic improvement for AW venues |
| P1 | Investigate why 6f figures inflate by +14 to +19 above OR — possible standard time too slow at 6f | Fixes R7/R8 inflation |
| P2 | Cross-validate against other recent Wolverhampton AW cards to confirm distance bias pattern | Determines if this is meeting-specific or systematic |

---

## Data Quality Summary

| Metric | Value | Status |
|--------|-------|--------|
| Total runners | 67 | OK |
| Figures computed | 64 (95.5%) | GOOD |
| Non-finishers excluded | 3 | CORRECT |
| Figure-OR correlation (overall) | 0.329 | **POOR** (target 0.90) |
| Figure-OR MAE (overall) | 12.4 | **ELEVATED** (target <7.0) |
| Mean bias (Fig - OR) | -6.9 | MODERATE negative |
| Scale ratio (Fig std / OR std) | 1.147 | SLIGHTLY EXPANDED |
| Figure-OR corr (R2 only, best race) | 0.763 | ACCEPTABLE |
| Figure-OR corr (R5, best internal) | 0.928 | GOOD (but shifted) |
| Ordering anomalies | 17 (all weight-driven) | CORRECT |
| DNFs correctly handled | 3/3 | GOOD |
| Unvalidatable races (no OR) | 1 (R1 novices) | EXPECTED |
