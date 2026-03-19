# French Racing Standard Times — Full QA Audit

## Executive Summary

Your dataset contains **800 course/distance/surface configurations** across **139 course codes** (625 Turf, 175 All Weather). The dataset has real strengths — no nulls, median and mean both captured, a minimum threshold of n=10 applied — but the audit surfaced several structural issues that would materially affect speed figure accuracy. The most critical are the **distance bucketing problem**, the **non-French course contamination**, and the **thin sample sizes at the tail**.

---

## 1. Sample Size Concerns

**The single biggest threat to figure reliability.**

| Threshold | Rows Below | % of Dataset |
|-----------|-----------|--------------|
| n < 15    | 137       | 17.1%        |
| n < 20    | 227       | 28.4%        |
| n < 30    | 342       | 42.8%        |
| n < 50    | 454       | 56.8%        |

**37 rows sit at the absolute minimum of n=10.** At that level, a single abandoned race or voided time in your source data can shift the median by 1–2 seconds — which at a typical LPS rate translates to 5–10 lengths on the figure. The standard error of the median at n=10 is roughly 1.25× the standard error of the mean, so your median-based standard is noisier than it needs to be at low sample sizes.

**Courses with only 1 distance configuration (and usually n≤15):**
A38, AGL, BED (AW), CHD, D21, DON, EPS, GAL, HAY, JAG, KOE, N11, NBU, NIM, PDF, POM, SDW, TOK, VIL, VIT, VY3, ZUR — 22 course/surface combinations with a single distance point and essentially zero ability to cross-validate internally.

**Recommendation:** Flag any standard time with n < 20 as provisional and consider excluding n < 15 from production speed figure calculation entirely.

---

## 2. Median vs Mean Divergence (Skew & Outlier Contamination)

**25 rows show > 5% divergence between median and mean time**, indicating the underlying race-time distribution contains severe outliers or is materially skewed.

### Worst offenders (mean >> median = positive skew / slow outliers):

| Key | n | Median | Mean | Divergence |
|-----|---|--------|------|------------|
| MXD_5.0_Turf | 22 | 59.1s | 68.0s | **+15.0%** |
| S87_5.5_Turf | 69 | 64.6s | 73.1s | **+13.0%** |
| ARG_6.0_Turf | 25 | 77.4s | 85.0s | **+9.8%** |
| D14_7.0_Turf | 10 | 88.2s | 96.7s | +9.6% |
| D09_6.5_Turf | 10 | 78.4s | 84.3s | +7.5% |

### Worst offenders (mean << median = negative skew / fast outliers):

| Key | n | Median | Mean | Divergence |
|-----|---|--------|------|------------|
| LPA_8.0_Turf | 10 | 92.7s | 84.6s | **−8.7%** |
| PET_18.5_Turf | 12 | 241.7s | 223.9s | **−7.3%** |
| SA7_10.0_Turf | 16 | 119.1s | 110.6s | −7.1% |
| SA7_7.5_Turf | 36 | 87.2s | 81.3s | −6.7% |

The +15% divergence at MXD_5.0_Turf almost certainly means your source data contains abandoned/walked-in/voided races at that course/distance that weren't cleaned. The negative-skew cases (LPA, PET, SA7) may contain timing errors or false starts that recorded anomalously fast times.

**Recommendation:** Audit the underlying race-time populations for these 25 configurations. Consider using a trimmed mean (e.g., 10% trim) or Winsorised mean rather than raw median for robustness.

---

## 3. Non-French Course Contamination

**This is a data integrity issue.** The following course codes are almost certainly not French racecourses and appear to have leaked in from UK, Irish, German, or Swiss fixtures:

### Likely British
- **ASC** (Ascot) — 9 rows
- **DON** (Doncaster) — 1 row
- **EPS** (Epsom) — 1 row
- **GOO** (Goodwood) — 6 rows
- **HAY** (Haydock) — 1 row
- **KEM** (Kempton) — 11 rows
- **MKT** (Market Rasen) — 2 rows
- **NBU** (Newbury) — 1 row
- **SDW** (Sandown) — 1 row
- **WAR** (Warwick) — 3 rows
- **YOR** (York) — 2 rows

### Likely Irish
- **CUR** (Curragh) — 5 rows
- **DUB** (Dundalk/Dublin) — 16 rows
- **GAL** (Galway) — 1 row
- **LEO** (Leopardstown) — 2 rows

### Likely German
- **BAD** (Baden-Baden) — 8 rows
- **DUS** (Düsseldorf) — 5 rows
- **FRA** (Frankfurt) — 3 rows
- **KOE** (Köln/Cologne) — 7 rows

### Likely Swiss
- **ZUR** (Zurich) — 1 row

That's roughly **80+ rows from non-French racing** contaminating your French standard times. If these are being used in a French-only speed figure system, they will either produce incorrect figures for French runners or (worse) your model may be matching French race results against UK/Irish/German standards.

**Note:** DUB in particular has 16 rows including both Turf and AW configurations — this is almost certainly Dundalk, Ireland's all-weather track, not a French course.

**Recommendation:** Cross-reference all 139 course codes against the official PMU/France Galop course list and remove non-French entries.

---

## 4. Distance Bucketing Problem

**Different actual race distances are being grouped under the same label.** The `std_key` rounds to the nearest 0.5 furlongs, but the actual `distance` column reveals that courses run at materially different metres within each bucket.

### Critical examples:

| Label | Actual Distances | Spread |
|-------|-----------------|--------|
| 5.0f | 1000m, 1016m, 1050m | 50m |
| 6.5f | 1300m, 1350m | 50m |
| 8.0f | 1600m, 1618m, 1630m, 1650m | 50m |
| 8.5f | 1700m, 1740m, 1750m, 1760m | 60m |
| 12.0f | 2400m, 2450m, 2460m | 60m |
| 14.5f | 2900m, 2920m, 2950m | 50m |

**A 50m distance difference at sprint distances equates to roughly 3 seconds.** If you're comparing a 1350m race against a 1300m standard, every horse gets a ~3-second (12–15 length) gift/penalty on their speed figure before you've even started adjusting. This is probably the single most impactful methodological flaw.

**However**, note that your `distance` column does store the actual distance per row — so each row's standard time reflects the correct distance for that specific course configuration. The risk is in how the key is used downstream. If your speed figure engine matches races to standards by key (e.g., `CHA_8.0_Turf`), and a race at Chantilly over 1650m gets matched to a 1600m standard because both are labelled 8.0f, figures will be systematically wrong.

**Recommendation:** Use the actual distance in metres as the matching key, not the furlong label. Or at minimum, verify your downstream matching logic uses the `distance` column rather than parsing the `std_key`.

---

## 5. LPL Column Inconsistency

The `course_lpl` column does **not** follow a single consistent formula. Testing `distance × 40 / median_time` (standard 1 furlong = 40 lengths) gives ratios that range from 0.33 to 1.43, with a mean of only 0.78.

The discrepancy is worst for short-distance all-weather configs (4.5f–5.0f AW), where stored LPL values are 30–43% higher than the formula predicts. This suggests either:

1. A different lengths-per-furlong conversion is used for AW vs Turf
2. The LPL was calculated using a different distance or time field than what's stored
3. The formula changed over time or across sources

Since LPL is the fundamental unit of speed figure calculation (converting time differences to lengths), this inconsistency would propagate directly into every figure.

**Recommendation:** Recalculate LPL from scratch using a single, documented formula applied consistently to all rows.

---

## 6. Monotonicity Violations

Two cases where a **longer distance has a faster standard time** at the same course:

1. **ARG Turf**: 6.0f = 77.4s → 6.5f = 76.8s (n=25 and n=18)
2. **LAT Turf**: 9.5f = 122.6s → 10.0f = 122.0s (n=257 and n=129)

The ARG case is clearly contaminated (ARG_6.0_Turf also appears in the extreme divergence list at +9.8% median-mean gap). The LAT case is subtler — with healthy sample sizes, this likely reflects a genuine course configuration difference (different start positions, different going) but still indicates the standards don't form a coherent speed model for the course.

---

## 7. AW vs Turf Anomalies

At courses with both surfaces, the AW/Turf speed relationship is **inconsistent**:

- **AW much faster than Turf** (expected for some fibresand/polytrack): CAG 6.5f (−7.0%), CAG 12.0f (−5.8%), PON 7.5f (−5.4%)
- **AW much slower than Turf** (unexpected): SIO 5.0f (+10.1%), SIO 5.5f (+6.4%), SA7 5.5f (+7.0%), SHT 8.0f (+5.5%)

A 10% speed difference between Turf and AW at SIO 5.0f is extreme and almost certainly reflects either data quality issues or a very different AW surface type. In France, the AW surfaces vary (Polytrack, fibresand, PSF) and their speed characteristics differ significantly.

**Recommendation:** Tag each AW configuration with its surface type (Polytrack, PSF, etc.) rather than using a single "All Weather" label. Different synthetic surfaces produce materially different standard times.

---

## 8. Cross-Course Speed Outliers

Within the same surface and distance band, some courses are statistical outliers that warrant investigation:

### Suspiciously slow (Turf):
- **OST_8.0_Turf**: 108.1s vs ~97s peer average (14.8 m/s, z=−3.1)
- **LIG_12.0_Turf**: 178.1s vs ~155s peer average (13.5 m/s, z=−4.3)
- **LIG_8.0_Turf**: also an outlier at z=−2.7

### Suspiciously fast:
- **WAR_11.5_Turf**: 135.6s vs ~147s peer average (17.0 m/s, z=+3.0)

LIG (likely Lignieres-en-Berry or similar provincial course) being 15% slower than average across multiple distances suggests it may be a cross-country or obstacles course whose times have been mixed in, or a course with extreme undulation/configuration.

---

## 9. Best Practice Methodology for Standard Times

### 9.1 Data Collection & Cleaning

1. **Source only completed flat races** — exclude abandoned, void, walkovers, hurdle/chase/cross-country.
2. **Filter by race class** — standard times should ideally be based on a defined class band (e.g., Class 2–4 / Listed–Handicap), not mixing Group 1s with claimers. Elite races run faster; rock-bottom claimers run slower. Both distort your standard if pooled.
3. **Going correction before standardisation** — either build separate standards per going description (Bon/Bon Souple/Souple/Très Souple/Lourd/Collant) or apply a going correction to normalise all times to a reference going before calculating the standard.
4. **Minimum sample size of n=30**, ideally n=50. Below 30, the standard is too noisy for reliable figures.

### 9.2 Standard Time Calculation

**Best practice is a weighted, going-adjusted median:**

1. Take all race winning times for a specific course/distance/surface over a defined period (3–5 years is typical).
2. Apply a going adjustment to normalise each time to "Good" (Bon) going using per-course going multipliers.
3. Trim the fastest 5% and slowest 5% to remove outliers.
4. Calculate the **trimmed median** of the adjusted times.
5. Optionally weight more recent seasons slightly higher (e.g., exponential decay with half-life of 2 years) to capture track renovations.

### 9.3 Speed Figure Calculation

The standard methodology (Beyer / Timeform / Racing Post approach, adapted):

```
Raw Speed Figure = (Standard_Time − Actual_Time) × LPS × Scale_Factor + Base

Where:
  Standard_Time = your going-adjusted standard for this course/distance/surface
  Actual_Time   = the horse's actual race time (winner time + beaten-lengths adjustment)
  LPS           = Lengths Per Second at this distance (consistent formula)
  Scale_Factor  = converts lengths to your rating scale
  Base          = your scale's baseline (e.g., 100 for an average winner)
```

**LPS should be calculated as:**
```
LPS = Distance_metres / (Standard_Time_seconds × Length_metres)

Where Length_metres = 2.4m (the Jockey Club standard horse length)
```

This gives you a single, auditable formula producing consistent results.

### 9.4 Going Allowances

The most sophisticated approaches (Timeform, Racing Post) calculate **per-course going allowances** using:

- All race times at a course across all going descriptions
- Regressing time differences against going descriptions
- Producing a seconds-per-furlong allowance per going level per course

This is critical for French racing specifically because French going descriptions are more granular than UK/Irish ones, and provincial courses can have dramatically different speed characteristics on "Souple" vs "Très Souple" depending on soil type.

### 9.5 Ongoing QA Framework

Build automated checks that run on every refresh:
- Monotonicity: time must increase with distance within a course
- Median-mean divergence alert at > 3%
- Minimum sample size enforcement
- Cross-course z-score flagging at > 2.5σ from the distance-band mean
- Going distribution check: ensure no single going type dominates > 70% of a standard's sample

---

## 10. Priority Action Items

| Priority | Action | Impact |
|----------|--------|--------|
| **P0** | Remove non-French course codes | Data integrity |
| **P0** | Fix distance bucketing — match on actual metres, not furlong labels | Figure accuracy ±10+ lengths |
| **P0** | Recalculate LPL from a single consistent formula | Figure accuracy |
| **P1** | Exclude or flag n < 20 as provisional | Reliability |
| **P1** | Audit the 25 high-divergence rows for outlier contamination | Noise reduction |
| **P1** | Add going adjustment to the standard time methodology | Systematic bias removal |
| **P2** | Split AW by surface type (Polytrack / PSF / Fibresand) | AW figure accuracy |
| **P2** | Investigate LIG, OST, WAR anomalies | Outlier resolution |
| **P3** | Move from raw median to trimmed, going-adjusted median | Best practice |
| **P3** | Build automated QA pipeline for ongoing refreshes | Maintainability |
