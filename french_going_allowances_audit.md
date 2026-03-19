# French Racing Going Allowances — Full QA Audit

## Executive Summary

Your going allowances dataset contains **12,716 meeting-level records** spanning **Jan 2015 – Mar 2026** across **139 course codes**, expressed as seconds per furlong (spf). The methodology is sound in principle — per-meeting continuous calculation with early/late splits for changing ground — but the audit reveals several issues that would propagate directly into speed figures. The most critical are the **calibration bias** (standards are set too fast, so the GA is doing too much corrective work), the **hard clipping at ±1.5/2.5** (which throws away information), and the same **non-French course contamination** found in the standard times.

---

## 1. What the Data Tells Us About Methodology

The going allowances appear to be calculated as follows:

- **Unit**: Seconds per furlong (spf). Positive = ground slower than standard, negative = ground faster.
- **Granularity**: One value per meeting per course per surface, with optional early/late splits (~9.3% of meetings).
- **Calculation**: Continuous, calculated per-meeting (12,566 unique values across 12,716 rows — not a lookup table).
- **Clipping**: Hard caps at **−1.5 spf** (50 meetings) and **+2.5 spf** (16 meetings).
- **Likely method**: Median residual of all race times at the meeting vs their respective standard times, divided by race distance in furlongs — or something equivalent. The high decimal precision (~16 decimal places) suggests it's derived from arithmetic on race times, not from a categorical going description.

This is a solid foundational approach. The key question is whether the corrections are doing the right amount of work.

---

## 2. Calibration Bias — Standards Are Set Too Fast

**This is the single most important finding.**

| Surface | Mean GA | Median GA | Implied bias at 8f |
|---------|---------|-----------|-------------------|
| Turf | +0.248 spf | +0.170 spf | Standards ~2.0s fast |
| All Weather | +0.089 spf | +0.084 spf | Standards ~0.7s fast |

If your standard times represented true "average conditions", the mean going allowance should be approximately zero. Instead, the Turf mean is **+0.248 spf**, which means your standards are calibrated to roughly "Good to Firm" (Bon Léger) conditions — faster than the average meeting. The GA is then doing 2 seconds of corrective work on a typical 8f race before any individual horse performance is assessed.

This isn't necessarily wrong — some systems deliberately set standards at "Good" ground and let the GA handle everything above — but it means:

1. Your speed figures are more sensitive to GA accuracy than they need to be
2. Any systematic error in the GA gets multiplied across more race-lengths
3. You're asking one number (the meeting-level GA) to absorb variation that could be partially handled by better-calibrated standards

**Recommendation:** Recalibrate standard times to the median observed ground conditions. This would shift mean Turf GA from +0.248 to approximately +0.08 spf, reducing the corrective burden.

---

## 3. Hard Clipping at −1.5 / +2.5 spf

**66 meetings are clipped to exact boundary values**, meaning the true going allowance was more extreme but has been capped.

| Clip | Count | Effect |
|------|-------|--------|
| −1.5 spf | 50 | True fast-ground correction suppressed |
| +2.5 spf | 16 | True soft-ground correction suppressed |

At 8 furlongs, clipping at +2.5 spf means any correction above 20 seconds gets capped. A genuinely waterlogged 8f race might be 25–30 seconds slower than good-ground standard — the clipping truncates this.

More concerning: **the −1.5 clips are concentrated on non-French courses where the standard time itself appears miscalibrated** (MKT: 11 clips, DON: 4 clips, SDW: 3, ZUR: 2, EPS: 2). This strongly suggests that certain foreign courses have standard times so far off that the GA cannot compensate even at maximum range.

**Recommendation:** Instead of hard clipping, use a soft cap (e.g., Winsorisation at the 1st/99th percentile) or investigate why these extremes occur — most are symptoms of miscalibrated standards.

---

## 4. Non-French Course Contamination (Again)

The same 139 course codes appear in both the standard times and going allowances, including the non-French ones. Some patterns confirm these are clearly foreign:

| Course | Meetings | GA Pattern | Diagnosis |
|--------|----------|------------|-----------|
| **YOR** (York) | 30 | All negative, mean −0.49 spf | Standard is ~4s too slow at 8f — set from a different population |
| **MKT** (Market Rasen) | 18 | 61% clipped at −1.5 | Standard is catastrophically wrong |
| **DON** (Doncaster) | 4 | All clipped at −1.5 | Standard is catastrophically wrong |
| **EPS** (Epsom) | 2 | Both clipped at −1.5 | Standard is catastrophically wrong |
| **SDW** (Sandown) | 4 | Mean −1.34, 75% clipped | Standard is catastrophically wrong |
| **WAR** (Warwick) | 16 | Mean +0.79, all positive | Standard is set far too fast |
| **HAY** (Haydock) | 2 | Mean +1.93, one clipped at +2.5 | Standard is set far too fast |
| **DUB** (Dundalk) | 293 | Relatively stable, mean +0.09 | Consistent but shouldn't be here |
| **KEM** (Kempton) | 150 | Mean +0.12, low variance | Consistent but shouldn't be here |

**789 meetings (6.2%) are from non-French courses.** DUB and KEM alone account for 443 of these — these are clearly established AW tracks that have enough data to produce reasonable GAs, but they're contaminating a French racing system.

York is the smoking gun: every single meeting across 8 years of Ebor Festival data has a GA between −0.33 and −0.67 spf. The standard is systematically wrong because it was derived from a French course population that York doesn't belong to.

---

## 5. Early/Late Splits

**591 meetings have early/late going splits** — a good feature that captures mid-meeting ground changes. Key observations:

- Late cards are slightly drier on average (late GAs are 0.014 spf lower), but the median difference is more pronounced at −0.099 spf, suggesting afternoon drying is common.
- 61% of split meetings see the ground dry out; 39% see it get wetter (rain during racing).
- Some extreme swings exist: A25 on 2020-05-01 goes from +0.36 (early) to +2.50 (late) — a biblical downpour mid-meeting.

**Issues:**

1. **Two parsing errors**: `2023-02-18_SIO_Turf_early_early` and `2023-02-18_SIO_Turf_early_late` — the surface is being parsed as "early" because the meeting ID has an extra level of nesting. These should be `SIO_Turf` with early/late suffixes.

2. **Coverage question**: Only 9.3% of meetings have splits. Is this because going rarely changes mid-meeting, or because the splitting logic is conservative? In French racing, watering practices at courses like Longchamp and Chantilly mean mid-meeting going changes are common — particularly on summer days where the first race on watered ground is notably different from the last race on drying ground.

---

## 6. All Weather Going Allowances — Not Zero

AW surfaces should produce near-zero going allowances since the surface is theoretically weather-independent. The actual picture:

| Metric | Value |
|--------|-------|
| AW mean GA | +0.089 spf |
| AW std GA | 0.182 spf |
| AW meetings with \|GA\| > 0.3 | 5.2% |
| AW meetings with \|GA\| > 0.5 | 1.5% |

The mean of +0.089 isn't alarming — it's a ~0.7s correction at 8f, which could reflect temperature effects on Polytrack/PSF surfaces, or the AW standards being calibrated slightly fast. But some individual AW courses show concerning patterns:

- **CHD All Weather**: mean GA of **+1.25 spf** across 16 meetings — the standard is about 10 seconds fast at 8f. This is almost certainly a miscalibrated standard, not a surface effect.
- **E2D, CHA, CAG, DEA All Weather**: mean GAs of 0.12–0.15 spf — minor but consistent.

There's also a slight downward drift in AW mean GA over time (from +0.14 in 2015 to +0.07 in 2025), which could reflect either gradual surface degradation (slower over time) or improved calibration.

---

## 7. Courses With Systematically Wrong Standards

The GA data effectively audits your standard times. Courses where the GA is systematically extreme reveal miscalibrated standards:

### Standards set too fast (GA always highly positive):
| Course | Surface | Mean GA | Meetings | Diagnosis |
|--------|---------|---------|----------|-----------|
| CHD | AW | +1.25 | 16 | Standard ≈10s too fast at 8f |
| A25 | Turf | +1.03 | 17 | Standard ≈8s too fast at 8f |
| WAR | Turf | +0.79 | 16 | Standard ≈6s too fast at 8f |
| SAI | Turf | +0.55 | 362 | Standard ≈4s too fast at 8f |
| CRO | Turf | +0.54 | 94 | Standard ≈4s too fast at 8f |
| BOU | Turf | +0.52 | 150 | Standard ≈4s too fast at 8f |

### Standards set too slow (GA always negative):
| Course | Surface | Mean GA | Meetings | Diagnosis |
|--------|---------|---------|----------|-----------|
| YOR | Turf | −0.49 | 30 | Standard ≈4s too slow at 8f |
| MKT | Turf | −1.03 | 18 | Standard ≈8s too slow at 8f |
| DON | Turf | −1.50 | 4 | Completely wrong |
| BED | AW | −0.59 | 10 | Standard ≈5s too slow |
| HPV | Turf | −0.01 | 326 | Marginal but n=326 confirms |

SAI with 362 meetings at +0.55 mean GA is particularly notable — that's a real French course with ample data, and the standard is still ~4 seconds too fast at 8f. This can't be a going effect; it's a calibration issue.

---

## 8. Seasonal Patterns — Expected and Reasonable

The monthly Turf GA pattern shows a clear and plausible seasonal signal:

| Month | Mean GA | Interpretation |
|-------|---------|---------------|
| Jan | 0.153 | Winter — moderate soft |
| Feb | 0.226 | Late winter — wetter |
| Mar | 0.370 | Spring rain — peak soft |
| Apr | 0.295 | Drying but still soft |
| May | 0.214 | Transitioning to summer |
| Jun | 0.163 | Good ground season starts |
| Jul | 0.149 | **Peak good ground** |
| Aug | 0.229 | Late summer — variable |
| Sep | 0.192 | Autumn transition |
| Oct | 0.371 | **Autumn soft peak** |
| Nov | 0.427 | **Wettest month** |
| Dec | 0.201 | Winter |

This pattern is entirely consistent with French climate and racing calendar. The two peaks (March and October/November) align with the wettest periods. July is the lowest — good summer ground. This gives confidence that the GA calculation is fundamentally tracking real ground conditions.

---

## 9. Scale & Magnitude Concerns

At extreme values, the GA is doing enormous corrective work:

| GA (spf) | Correction at 8f | At 12f | Equivalent lengths (8f) |
|----------|-----------------|--------|------------------------|
| 0.10 | 0.8s | 1.2s | ~4L |
| 0.20 | 1.6s | 2.4s | ~8L |
| 0.50 | 4.0s | 6.0s | ~20L |
| 1.00 | 8.0s | 12.0s | ~40L |
| 2.50 | 20.0s | 30.0s | ~100L |

**12.9% of meetings have |GA| > 0.5 spf** — that's a 4+ second correction at 8f, equivalent to adjusting every horse's performance by 20+ lengths. At that magnitude, the GA is no longer a fine adjustment — it's the dominant factor in the speed figure.

**3.8% of meetings exceed |GA| > 1.0 spf** (8+ second corrections). At these levels, the speed figure is more a going estimate than a performance measure.

---

## 10. Data Quality Issues

1. **Sort order breaks at row 12,497** — the first 12,497 rows are chronological, then 219 rows at the end are unsorted (dates from 2015–2026 mixed). Likely late additions or backfills.

2. **Two malformed meeting IDs**: `2023-02-18_SIO_Turf_early_early` and `2023-02-18_SIO_Turf_early_late` — parsing produces course="SIO_Turf" and surface="early".

3. **No going description stored** — the file contains only the calculated GA, not the official going description (Bon, Souple, Lourd, etc.). This makes it impossible to audit whether the GA correlates correctly with published going without joining to another data source.

---

## 11. Priority Actions

| Priority | Action | Impact |
|----------|--------|--------|
| **P0** | Remove non-French courses from both standard times and going allowances | Eliminates systematically wrong GAs |
| **P0** | Recalibrate standard times to median ground conditions | Reduces GA corrective burden from +0.25 spf to ~+0.08 spf |
| **P1** | Investigate and fix standard times for SAI, CRO, BOU, A25, CHD | These produce systematically extreme GAs |
| **P1** | Replace hard clipping with soft caps or targeted standard recalibration | Preserves information at extremes |
| **P1** | Fix the 2 malformed SIO meeting IDs | Data integrity |
| **P2** | Add official going description to the dataset for cross-validation | Audit capability |
| **P2** | Investigate whether early/late split coverage should be expanded | Accuracy on watered/changing ground |
| **P3** | Evaluate whether AW needs surface-type-specific standards (PSF vs Polytrack) | AW figure accuracy |
| **P3** | Investigate the slight AW GA drift over time (2015→2025) | Temporal stability |
