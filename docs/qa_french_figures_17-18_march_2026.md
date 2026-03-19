# QA Review: French Figures — 17-03 and 18-03 (March 2026)

> Reviewed 2026-03-19 against UK pipeline logic and French constants/calibration setup.

## Summary

| Date  | Course | Races | Verdict                              |
|-------|--------|-------|--------------------------------------|
| 17-03 | FON    | 5     | Mostly OK, GA slightly low           |
| 18-03 | BOR    | 8     | **3 critical issues**                |
| 18-03 | CEP    | 4     | OK                                   |
| 18-03 | HPV    | 3+    | **Suspicious — likely non-French**   |

---

## CRITICAL Issues

### 1. BOR Race 4 (18-03): Impossible finishing time — figures of 450+

Winner **FEL** has `finishingTime=73.98s` for a **9.94f** race (`standard_time=122.74s`).
That is ~7.4 s/f — sprint pace over nearly 1.25 miles — physically impossible.
This is clearly bad PMU source data.

| Field | Value |
|---|---|
| raw_figure | **623** |
| figure_calibrated | **450** |
| Affected runners | All 8 (range 442–451) |

**Cascade effect on BOR GA:**

The outlier `dev_per_furlong = -4.905` gets winsorized but still drags the
weighted meeting mean to ~-0.028. After Bayesian shrinkage with "Très souple"
prior (0.52) and K=3 over 7 races:

```
shrunk_ga = (7 × -0.028 + 3 × 0.52) / 10 = 0.137
```

This GA of **0.137 s/f** is wildly low for very soft ground.
All other BOR races inherit this deflated GA, making them **under-corrected
by ~5–15 lbs** (distance-dependent).

Without Race 4, the GA would be ~0.189 — still low vs 0.52 but more defensible.

**Recommendation:** Add a pre-GA sanity filter on winner finishing times.
Flag any time implying <10 s/f or >18 s/f pace, or producing raw_figure > 200.
Exclude from GA computation and mark the race as unreliable.

---

### 2. BOR Race 1 (18-03): No standard time → no figures produced

Race distance is ~4.97f, but `BOR_5.0_Turf` does not exist in
`standard_times.csv` (BOR's shortest entry is `BOR_6.0_Turf` at 68.75s).

All 6 runners produce **no figure**. The WFA (12 lbs for 2yo) is applied but
calibration cannot proceed without a standard time for the winner.

**Recommendation:** Consider adding nearest-distance interpolation for winner
figures (currently only used for GA inference), or flag standard-time gaps
during the artifact build.

---

### 3. HPV (18-03): Likely non-French venue — unreliable beaten lengths

HPV has 1,000+ historical races in the artifacts (enough for standard times).
However, horse names are clearly non-French: **WAH MAY WAI WAI**, **LUCKY
BLESSING**, **TOP TO SKY**, **ARMOR GOLDEN EAGLE**. This is almost certainly
**Happy Valley (Hong Kong)** imported by PMU for French betting markets.

**Data quality issues in HPV Race 1:**

- Two runners (GLORIOUS RYDER + SOARING BRONCO) share position 1.0 (dead heat) —
  handled correctly by the pipeline.
- Runners 3–11 **all** have `distanceCumulative = 0.5L`. Nine horses cannot all
  finish exactly 0.5L behind the winner. The beaten-length parser likely failed
  on the HK format and defaulted to a fixed value.
- Result: all non-winners get near-identical figures (~78–90), differentiated
  only by weight. These figures are **unreliable**.

**Recommendation:** Either filter non-French hippodromes from the live pipeline,
or explicitly whitelist international courses with verified beaten-length parsing.

---

## MEDIUM Issues

### 4. BOR meeting GA under-correction for "Très souple" (18-03)

| Metric | Value |
|---|---|
| Computed GA | 0.137 s/f |
| Prior (Très souple) | 0.52 s/f |
| Gap | **0.383 s/f** |

At 13f (Race 5), this means ~5 seconds of under-correction ≈ **25+ lbs** of
missing going adjustment. Even excluding the Race 4 outlier, the BOR meeting
data argues for fast ground despite the soft going description.

Bayesian shrinkage with K=3 is overwhelmed by 7 data points. For extreme going
descriptions, a higher K (e.g. 5) would make the prior more resistant.

### 5. FON GA under-correction for "Souple" (17-03)

| Metric | Value |
|---|---|
| Computed GA | 0.175 s/f |
| Prior (Souple) | 0.32 s/f |
| Gap | **0.145 s/f** |

With 5 races at FON, the data dominates K=3 shrinkage. Figures are consequently
**~5–14 lbs lower** than with the prior GA (distance-dependent).

At 9f (Race 5): `(0.32 - 0.175) × 8.95 / 0.2 × 2.22 ≈ 14 lbs` lower than
prior-based figures. Winner AMERICAN GLORY at 101.5 would be ~115 with the
prior GA.

This could be legitimate (actual going was better than described) or indicate
under-correction.

---

## Minor / Informational

### 6. Beaten >20L exclusions — all correct

| Race | Horse | Beaten | Excluded? |
|------|-------|--------|-----------|
| FON R1 | LOVER SONG | 20.8L | Yes |
| BOR R2 | LE DOC | 38.28L | Yes |
| BOR R3 | LOOK | 42.6L | Yes |
| BOR R7 | GIRAFFE | 24.85L | Yes |
| BOR R7 | MUNGASHA | 30.85L | Yes |
| CEP R8 | ALTXERRI | 20.53L | Yes |
| CEP R8 | MISS DARIANA | 23.53L | Yes |
| CEP R8 | COCOA | 23.56L | Yes |

### 7. Weight adjustments and WFA — correct

- FON R2 (3yo, ~6f, March): WFA = 4.07 lbs — matches interpolation
  (month 3: 5f→6, 6f→4, frac=0.97). Correct.
- BOR R1 (2yo, ~5f): WFA = 12.0 lbs — matches `_WFA_2YO_FLAT`. Correct.
- Weight adjustments: `weightCarried − 126` applied consistently. Correct.

### 8. Calibration scale+shift — verified

Global calibration compresses raw French scale (~100–140 for mid-tier) down to
UK-comparable range (~60–100). Formula `fig_calibrated = fig_after_wfa × scale +
shift` checks out across all races in both the ratings and audit CSVs.

### 9. FON R5: 2nd place rates higher than winner — correct behaviour

PRINCE DES VILLES (2nd, 0.15L behind, 123.5 lbs) rates 102.1 vs winner
AMERICAN GLORY (122.4 lbs) at 101.5. The weight penalty for carrying less
weight outweighs the tiny beaten margin. This is correct weight-adjusted
figure behaviour.

---

## Recommended Actions

| Priority | Action |
|----------|--------|
| **Immediate** | Add pre-GA sanity filter on winner finishing times (exclude pace <10 or >18 s/f, or raw_figure > 200) |
| **Short-term** | Investigate HPV data: exclude non-French venues or validate BL parsing for international imports |
| **Medium-term** | Increase `GA_SHRINKAGE_K` for extreme going (e.g. K=5 for Très souple/Très Lourd) |
| **Medium-term** | Add nearest-distance standard-time interpolation for winner figures (not just GA) |
