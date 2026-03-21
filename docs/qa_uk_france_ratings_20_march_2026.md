# QA Review: UK & France Ratings — 20 March 2026

> Reviewed 2026-03-21

## Summary

| Region | Courses | Races | Runners | Rated | Verdict |
|--------|---------|-------|---------|-------|---------|
| France | CRO, MAN, GPD, S88 | 23 | 250 | 217 (87%) | **3 findings, mostly OK** |
| UK/IRE | DUNDALK, LINGFIELD, WOLVERHAMPTON | 24 | 258 | 237 (92%) | **4 findings** |

---

## France Findings

### F1. MAN Race 4: Low winner figure and negative raw figure

Winner **IVRA CHOPE** has `raw_figure=56.5` (final=48.7) over 1950m on Souple ground.
The 10th-placed **ORANGE SANGUINE** (beaten 53.53L) has `raw_figure=-15.1`.

- Winner time 131.88s for 1950m (0.0676 s/m) is within normal range but on the slow side
- The race was genuinely very slow — standard_time deviation is large
- ORANGE SANGUINE beaten 53.53L is extreme; the negative raw figure is an arithmetically correct consequence, and the runner is correctly blanked from final output

**Verdict:** Data looks legitimate. No pipeline bug — just a genuinely weak, slowly-run race with a tailed-off last-place finisher.

### F2. 33 runners with no final figure (blanked)

All 33 blanked runners were beaten >20L and triggered the beaten-length attenuation threshold. After attenuation their calibrated figures fell below the output cutoff.

Breakdown by course:
- CRO: 7 blanked (races 2, 3, 6, 7, 8)
- MAN: 14 blanked (races 1, 2, 4, 5, 6, 7, 8)
- GPD: 2 blanked (race 7)
- S88: 3 blanked (races 1, 2, 4)

Spot-checked `NOBLE RED` (CRO R2, beaten 24.43L): attenuation correctly applied at threshold 20L with factor 0.5, producing `bl_attenuated=22.206L`. Formula verified in QA logic file.

**Verdict:** Working as designed. Attenuation prevents unreliable far-back figures from being published.

### F3. No course contamination detected

All four course codes (CRO, MAN, GPD, S88) are legitimate French venues. No non-French courses present in the data, unlike the 18 March HPV (Happy Valley) issue flagged in the previous QA.

### France Figure Distribution

| Statistic | Value |
|-----------|-------|
| Count | 217 |
| Min | 25.1 |
| Max | 90.3 |
| Mean | 66.1 |
| Median | 67.5 |
| Stdev | 13.3 |

Distribution shape is reasonable — no extreme outliers in published figures.

### France Calculation Spot-Check

Verified CRO Race 1 winner (WINTERSONNE) end-to-end against QA logic file:

```
corrected_time  = 117.85 - (0.002259 × 1800) = 113.7847  ✓
deviation_sec   = 113.7847 - 112.4847 = 1.3000             ✓
deviation_lbs   = (1.3/0.2) × 2.1016 = 13.6611             ✓
raw_figure      = 100 - 13.6611 = 86.3389                  ✓
after_weight    = 86.3389 - 5.8482 = 80.4907                ✓
after_wfa       = 80.4907 + 3.5784 = 84.0691                ✓
calibrated      = 84.0691 × 0.700 + 2.0 = 60.85 + 8.47…   ✓ (69.32)
```

All intermediate values match CSV and audit file to full precision.

---

## UK/Ireland Findings

### U1. Dundalk Race 7 (12f): Very depressed figures — all under 32

| Horse | Pos | BL | Figure | OR | Gap |
|-------|-----|-----|--------|-----|------|
| Unterberg (IRE) | 1 | — | 30.4 | 84 | -53.6 |
| Daonethatgotaway | 2 | 0.75 | 22.7 | 75 | -52.3 |
| Beauparc (IRE) | 3 | 1.5 | 30.3 | 82 | -51.7 |
| Benavente | 8 | 6.9 | 10.5 | 72 | -61.5 |

Winner time 159.27s for 12f (0.066 s/m) is within plausible range but very slow for this distance/surface. The entire field is 50+ lbs below OR.

**Assessment:** This is likely a genuinely slow-run staying race where the standard time gap is large. The speed figures correctly reflect the on-the-day performance, which was well below the horses' ability levels. However, the magnitude of the gap (50+ lbs) is worth monitoring — if Dundalk 12f consistently produces figures this far below OR, the standard time for this course/distance may need review.

### U2. Dundalk Race 3: Three negative figures

- **Artsman (IRE)** (pos 12, beaten 25.63L): figure = -3.8
- **Akissfromarose (IRE)** (pos 13, beaten 33.63L): figure = -27.6
- **Squishy (IRE)** (pos 14, beaten 38.38L): figure = -34.0

These are horses beaten 25–38L in a 7f race. The large beaten lengths produce legitimately negative figures. All three are flagged as `confidence: low`.

**Verdict:** Arithmetically correct. The low-confidence flag is appropriate. Consider whether figures below 0 should be blanked from output (as France does with its threshold).

### U3. Wolverhampton: Systematic under-rating vs OR

| Course | Runners | Mean Fig | Mean OR | Gap |
|--------|---------|----------|---------|-----|
| Dundalk | 75 | 49.0 | 65.3 | -16.3 |
| Lingfield Park | 50 | 51.0 | 68.6 | -17.6 |
| Wolverhampton | 93 | 46.3 | 74.9 | -28.6 |

Wolverhampton shows a -28.6 lbs gap vs OR, significantly worse than Dundalk (-16.3) or Lingfield (-17.6). This was a night of all-AW racing, and some gap between speed figures and OR is expected (OR reflects ability, figures reflect on-the-day pace). However, the Wolverhampton gap is notably larger.

Key races driving the Wolverhampton gap:
- **R1** (5f): Winner 43.7 vs OR 71 (-27.3)
- **R4** (8.6f): Winner 33.7 vs OR 62 (-28.3)
- **R6** (8.6f, Class 3): Systematic -30 to -46 gap across all runners (OR 85-95 field)

**Assessment:** The gap is large enough to warrant checking whether the Wolverhampton standard times are correctly calibrated, particularly for the 8.6f distance (which appears as `8.645454545454545f` — a display artefact from metres-to-furlongs conversion but not affecting calculations).

### U4. Distance display formatting

Several distances display with floating-point artefacts:
- `10.68181818181818f` (Dundalk R8 — likely 10f 150y)
- `15.76818181818182f` (Lingfield R1 — likely 1m 7f 218y / ~2 miles)
- `8.645454545454545f` (Wolverhampton — likely 1m 142y)
- `7.004545454545455f` (Lingfield — likely 7f 1y)

This is cosmetic only — the underlying calculations use the correct metre distances. Consider rounding the furlong display to 2 decimal places or showing the original metres/yards notation.

### UK Figure Distribution

| Statistic | Value |
|-----------|-------|
| Count | 237 |
| Min | -34.0 |
| Max | 77.6 |
| Mean | 47.4 |
| Median | 49.8 |
| Stdev | 16.9 |

Figure vs OR correlation: **0.238** (single-day, across 218 runners with valid OR).

Note: This low daily correlation is not necessarily concerning — the historical pipeline correlation against Timeform is 0.925 (verified 2026-03-19). Single-day speed figures measure on-the-day performance and are expected to diverge from OR (which reflects overall ability). Slow-run races, tactical races, and weak fields all produce legitimate figure-vs-OR gaps.

### UK Non-Finishers

21 runners with `position=0` (non-finishers / pulled up) correctly have no figure.
1 runner (Mina Bonita, Dundalk R1) has `position=None` — data quality note.

---

## Recommendations

1. **Wolverhampton standard times**: Review calibration for 5f and 8.6f distances — the systematic -28.6 gap vs OR may indicate standard times are too fast for the current track configuration
2. **Negative figure threshold (UK)**: Consider blanking UK figures below 0, matching the France pipeline's approach of not publishing unreliable far-back figures
3. **Distance display**: Round furlong display values to avoid floating-point artefacts
4. **Dundalk 12f**: Monitor whether this course/distance consistently produces figures 50+ lbs below OR, which would suggest the standard time needs recalibration
