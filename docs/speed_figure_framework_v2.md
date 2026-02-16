# Speed Figure Compilation Framework v2.0
## Complete Production Methodology for UK/Ireland Flat & Jump Racing

---

## 1. CORE ARCHITECTURE

### 1.1 Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DAILY SPEED FIGURE PIPELINE                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  DATA COLLECTION                                                    │
│  ├── Race results & finishing times                                 │
│  ├── Going reports & going stick readings                           │
│  ├── Rail movement data                                             │
│  ├── Weather data (wind speed, direction, precipitation)            │
│  ├── Stall positions & draw data                                    │
│  ├── Sectional timing data (where available)                        │
│  └── Equipment & jockey/trainer changes                             │
│                                                                     │
│  STAGE 1: RAW TIME PROCESSING                                      │
│  ├── Actual finishing time per runner                                │
│  ├── Distance correction (rail movements, run-up)                   │
│  └── Raw deviation from standard time                               │
│                                                                     │
│  STAGE 2: NORMALISATION                                             │
│  ├── Going allowance (daily track variant)                          │
│  ├── Weight carried adjustment (to base weight)                     │
│  ├── Weight for age adjustment                                      │
│  ├── Sex allowance adjustment                                       │
│  ├── Surface type calibration                                       │
│  └── Wind adjustment (where data available)                         │
│                                                                     │
│  STAGE 3: CONVERSION & REFINEMENT                                   │
│  ├── Time → lengths → lbs conversion                                │
│  ├── Beaten-lengths calculation for placed horses                   │
│  ├── Sectional timing upgrade/downgrade                             │
│  ├── Draw bias adjustment                                           │
│  ├── Class par sanity check                                         │
│  └── Inter-track variant alignment                                  │
│                                                                     │
│  STAGE 4: ML REFINEMENT (ADVANCED)                                  │
│  ├── Iterative variant recalculation using prior figures             │
│  ├── Non-linear weight/distance/going learning                      │
│  ├── Empirical WFA derivation                                       │
│  ├── Anomaly detection & flagging                                   │
│  └── Model validation against TFig target                           │
│                                                                     │
│  OUTPUT                                                             │
│  ├── Speed figure per runner (in lbs, on unified scale)             │
│  ├── Confidence indicator per figure                                │
│  ├── Upgrade/downgrade flag (from sectionals)                       │
│  ├── Going allowance per meeting/track config                       │
│  └── Updated class pars & standard times                            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 Design Principles

- **All figures expressed in pounds (lbs)** on a unified scale where 100 = a mature horse at 9st 0lb matching standard time on good going
- **5 furlongs as the benchmark distance** — all time-to-lbs conversions are anchored here (Timeform convention)
- **Figures are interchangeable** across courses, distances, going, and surfaces once all adjustments are applied
- **Higher = better** — a 110 figure represents a higher level of performance than a 90
- **Each component is independently tuneable** — the framework is modular so individual adjustments can be refined without rebuilding the entire system

### 1.3 Scale Calibration

| Rating Band | Flat Level | Jump Level |
|---|---|---|
| 130+ | Champion / exceptional Group 1 | Champion chaser |
| 120-129 | Group 1 | Grade 1 chaser / top hurdler |
| 110-119 | Group 2-3 / top listed | Graded chase / hurdle |
| 100-109 | Listed / top handicap | Good handicap chaser |
| 90-99 | Decent handicap | Average handicapper |
| 80-89 | Low-grade handicap | Moderate handicapper |
| 70-79 | Class 5-6 | Poor handicapper |
| 60-69 | Low-grade maiden | Maiden hurdler |
| Below 60 | Very poor | Below average |

**Base settings:**
- Flat: base weight = 9st 0lb, base rating = 100
- Jump: base weight = 11st 0lb, base rating = 130 (some compilers use 100 for both)

---

## 2. STANDARD TIMES

### 2.1 Purpose

Standard times are the bedrock of the entire system. Every other calculation depends on having accurate, course-specific, distance-specific benchmarks.

A standard time answers: "How fast would an average horse of average ability complete this distance at this course on good going?"

### 2.2 Construction Method

**Step 1: Data Collection**

Gather all winning times for each course/distance combination over the most recent 3-5 years. Minimum sample size: 20 races per combination (fewer than this and the standard is unreliable).

**Step 2: Class Adjustment**

Raw times must be adjusted for the class of race before computing the standard. Higher-class races are naturally faster.

Recommended class adjustments (seconds per mile / 8 furlongs):

| Race Class | Adjustment (s/mile) | Notes |
|---|---|---|
| Class 1 (Group/Graded) | -3.6 | Fastest — subtract from time |
| Class 2 (Listed/Premier Hcap) | -4.8 | |
| Class 3 | -6.0 | |
| Class 4 | -7.2 | |
| Class 5 | -8.4 | |
| Class 6 | -9.6 | Slowest runners |

For distances other than 1 mile, scale proportionally:
```
class_adjustment = (adjustment_per_mile × distance_in_furlongs) / 8
```

**Step 3: Going Adjustment for Standard Compilation**

Only include times from races run on "Good" or "Standard" (AW) going, OR apply a preliminary going correction to times from other ground conditions.

**Step 4: Calculate Median**

After class-adjusting all times, sort them and take the **median** (NOT the mean). The median eliminates the influence of extreme outliers — both freakishly fast and unusually slow races.

**Step 5: Distance Relationship Validation**

Standard times must have a logical relationship across distances. Verify that:
```
standard_per_furlong decreases as distance increases
```
This reflects the biological reality that horses cannot sustain maximum speed over longer distances. If your 7f standard is faster per furlong than your 6f standard at the same course, investigate.

**Step 6: Course Configuration Separation**

At courses with multiple configurations, maintain separate standards for each:
- Straight course vs round course (Ascot, Newbury, Newmarket, York, Doncaster, etc.)
- Old course vs new course (Newmarket Rowley Mile vs July Course)
- Different chase/hurdle courses at same track

### 2.3 Key Constants

| Constant | Value | Source |
|---|---|---|
| Seconds per length | 0.2 | BHA standard |
| Lengths per second | 5 | Inverse of above |
| Lbs per second at 5f | 22 | Racing Post/Topspeed |
| Furlongs per mile | 8 | - |
| Yards per furlong | 220 | - |
| Timeform conversion | 0.4 seconds = 1lb at 5f | Timeform |
| Base lbs per length at 5f | 3.0-3.4 | Varies by course |

### 2.4 Updating Standards

Standards should be reviewed annually. Use a rolling 3-5 year window, dropping the oldest year and adding the most recent. This captures gradual changes in track surfaces (e.g., a track that relays its turf) and timing technology.

### 2.5 Ireland & France Specifics

**Ireland:**
- Distances are often imprecise; the BHA/HRI have been slowly improving measurement
- Some courses have limited data at certain distances — small sample sizes require caution
- French-bred horses (particularly in NH) are subject to Southern Hemisphere birthday conventions in some contexts

**France (if cross-referencing):**
- Metric distances: 1000m ≈ 5f, 1200m ≈ 6f, 1600m ≈ 8f, 2000m ≈ 10f, 2400m ≈ 12f
- Different timing systems and going descriptions
- OR/RPR conversion tables exist but are approximate

---

## 3. LBS PER LENGTH TABLES

### 3.1 Purpose

Converts beaten distances (in lengths) to pounds of ability difference. This varies by distance because a "length" represents a different proportion of total race time at different distances.

### 3.2 Derivation

The lbs-per-length value at each distance is derived from:
```
lbs_per_length = seconds_per_length × lbs_per_second_at_distance
lbs_per_second_at_distance = 22 × (5 / distance_in_furlongs)
```

### 3.3 Reference Table (Flat)

| Distance | Distance (f) | Lbs/Second | Lbs/Length (at 0.2s/L) | Lbs/Length (at 0.18s/L) |
|---|---|---|---|---|
| 5f | 5.0 | 22.00 | 4.40 | 3.96 |
| 5f 34y | 5.15 | 21.36 | 4.27 | 3.84 |
| 6f | 6.0 | 18.33 | 3.67 | 3.30 |
| 6f 16y | 6.07 | 18.12 | 3.62 | 3.26 |
| 7f | 7.0 | 15.71 | 3.14 | 2.83 |
| 1m (8f) | 8.0 | 13.75 | 2.75 | 2.48 |
| 1m 1f | 9.0 | 12.22 | 2.44 | 2.20 |
| 1m 2f | 10.0 | 11.00 | 2.20 | 1.98 |
| 1m 3f | 11.0 | 10.00 | 2.00 | 1.80 |
| 1m 4f | 12.0 | 9.17 | 1.83 | 1.65 |
| 1m 5f | 13.0 | 8.46 | 1.69 | 1.52 |
| 1m 6f | 14.0 | 7.86 | 1.57 | 1.41 |
| 1m 7f | 15.0 | 7.33 | 1.47 | 1.32 |
| 2m | 16.0 | 6.88 | 1.38 | 1.24 |

### 3.4 Jump Racing Extensions

| Distance | Distance (f) | Lbs/Second | Lbs/Length |
|---|---|---|---|
| 2m | 16.0 | 6.88 | 1.38 |
| 2m 4f | 20.0 | 5.50 | 1.10 |
| 2m 5f | 21.0 | 5.24 | 1.05 |
| 3m | 24.0 | 4.58 | 0.92 |
| 3m 2f | 26.0 | 4.23 | 0.85 |
| 3m 5f | 29.0 | 3.79 | 0.76 |
| 4m+ (GN etc) | 32+ | 3.44 | 0.69 |

### 3.5 Course-Specific Lbs Per Length

The gold standard approach is to derive unique lbs-per-length values for each course/distance combination from your own standard times. Course topography significantly affects these values:

- **Epsom 1m 4f (Derby course):** Predominantly downhill final furlongs → horses separate more easily → lbs-per-length slightly lower than a flat track at same distance
- **Pontefract 1m 4f:** Uphill finish → horses compress → lbs-per-length slightly higher
- **Chester 5f:** Tight turns → unique speed dynamics

**Method:** For each course/distance, compute `lbs_per_length = (22 × 5) / (standard_time_in_seconds × lengths_per_second)`

This implicitly captures course configuration effects.

---

## 4. GOING ALLOWANCE (DAILY TRACK VARIANT)

### 4.1 Purpose

The going allowance is the cornerstone adjustment. It quantifies how much the ground conditions on a given day were speeding up or slowing down ALL horses, expressed in seconds per furlong.

Without an accurate going allowance, your figures are meaningless — you'd be conflating ground conditions with horse ability.

### 4.2 Standard Calculation Method

**For each race on the card:**

```
deviation = actual_winning_time - standard_time_for_this_course_distance
deviation_per_furlong = deviation / distance_in_furlongs
```

**Then across the card:**

1. Calculate `deviation_per_furlong` for every race
2. Remove the highest (fastest) and lowest (slowest) deviation — outlier elimination
3. If card has 6+ races, also consider removing the next most extreme values
4. Average the remaining values → this is your **going allowance** for that track configuration on that day

**Sign convention:**
- Positive going allowance (+): ground is faster than standard (firm)
- Negative going allowance (-): ground is slower than standard (soft/heavy)

### 4.3 Advanced Going Allowance — Iterative Method

The simple method has a flaw: it assumes standard times perfectly represent average ability at each distance. The iterative method uses each horse's **prior speed figures** instead:

**Step 1:** Calculate expected finishing time for each winner based on their previous best/average speed figures

**Step 2:** `deviation = actual_time - expected_time` for each race

**Step 3:** Average these deviations (with outlier removal) → going allowance

**Step 4:** Use this going allowance to compute today's speed figures

**Step 5:** Feed these new figures back into the database for future iterations

This is essentially what Beyer does using human judgment about each field's quality. With ML, you can automate this iterative refinement.

### 4.4 Split-Card Going Allowances

**When to split:**

- Rain during the meeting → ground conditions deteriorate progressively
- Different course configurations used (straight vs round)
- Early vs late meeting on a day where the going stick reading changes significantly
- Different surfaces at a dual-purpose meeting

**Detection method:** Plot `deviation_per_furlong` in race-time order. If you see a clear trend (e.g., deviations becoming progressively more negative through the card), split the card at the inflection point and calculate separate going allowances for each segment.

**At some meetings, you may need 2-4 separate going allowances.** This is particularly common at:
- Chester (heavy rain can dramatically change the ground mid-meeting)
- Haydock (exposed course, weather-sensitive)
- Any Irish meeting in winter

### 4.5 Going Allowance Validation

Cross-check your calculated going allowance against:

| Validation Source | Method |
|---|---|
| Official going description | Should broadly align with your calculated range (but yours is more precise) |
| Going stick readings | Quantitative measure of ground moisture — correlate with your allowance |
| Comparison-per-furlong (RP) | Racing Post publishes these — compare to your values |
| Dave Edwards/Topspeed | Published in Racing Post as a secondary reference |
| Class pars | If your figures after applying the going allowance produce class-appropriate ratings, the allowance is likely correct |

### 4.6 Ireland Going Allowance Specifics

Irish going descriptions are notoriously unreliable and often overly optimistic. Ground officially described as "Good" in Ireland frequently rides as "Good to Soft" or "Soft" by UK standards.

**Recommendations:**
- Never trust Irish going descriptions for figure calculation
- Always derive your own going allowance from the times
- Be cautious with small cards (4-5 races) — insufficient data for a reliable going allowance
- Consider using OR/RPR as an additional anchor when Irish cards are small

---

## 5. WEIGHT CARRIED ADJUSTMENT

### 5.1 Purpose

Adjusts each horse's figure to account for the weight it carried, placing all performances on a common scale (typically 9st 0lb for flat, 11st 0lb for jumps).

### 5.2 Methods

**Method A: Linear Adjustment (Topspeed/RP approach)**

```
adjusted_figure = raw_figure + (weight_carried_in_lbs - base_weight_in_lbs)
```

Where base weight is 126 lbs (9st 0lb) on the flat. A horse carrying 9st 5lb gets +5 added to their figure; one carrying 8st 11lb gets -3.

**Method B: Percentage Adjustment (Beyer-influenced)**

Beyer's research suggested 1lb ≈ 0.4 points on his scale. Apply a proportional adjustment:
```
adjusted_figure = raw_figure + (weight_over_base × 0.4)
```

**Method C: No Adjustment (Beyer US method)**

Beyer himself chose NOT to adjust for weight, arguing that the weight/speed relationship is too noisy and non-linear. His figures reflect actual performance regardless of impost.

**Method D: ML-Learned Adjustment (Recommended)**

Include weight carried as a feature in your model and let it learn the true, potentially non-linear relationship. Research suggests:
- Each 1lb extra → 0.04 points slower
- Each 1lb less → 0.03 points slower (paradoxically — lighter-weighted horses tend to be weaker)
- The relationship may vary by distance and class

### 5.3 Apprentice/Conditional Jockey Claims

When a claiming jockey rides, the horse's carried weight is reduced by the claim amount (3lb, 5lb, 7lb, or 10lb). This reduced weight IS the actual weight for figure calculation purposes. However, the claim exists because the jockey is less experienced — so the question is whether the weight reduction fully compensates for the skill deficit.

**Recommendation:** Use actual weight carried (after claim) for the weight adjustment. If you want to assess whether claims are correctly calibrated, that's a separate analytical question your data can answer.

---

## 6. WEIGHT FOR AGE (WFA)

### 6.1 Purpose

Compensates younger horses for their physical immaturity, allowing performances to be compared across age groups on an equal basis.

### 6.2 Current Official Scale (2025 Unified European — Flat)

The BHA, IHRB, France Galop, and German racing agreed a unified European scale effective from 2025. Key features:

- 2-year-olds receive the largest allowances, particularly early in the season and at longer distances
- 3-year-olds are presumed equal to 4+ year-olds by approximately October at sprint distances, later at longer trips
- The 2025 update reduced 3yo allowances by ~1lb at 14f+ to correct demonstrated overperformance

**2025 Scale Structure (Flat, abbreviated):**

For 2-year-olds vs 4+ year-olds (approximate lbs allowance):

| Month | 5f | 6f | 7f | 1m | 1m2f | 1m4f |
|---|---|---|---|---|---|---|
| Mar | - | - | - | - | - | - |
| Apr | - | - | - | - | - | - |
| May | 24 | 26 | 28 | 30 | - | - |
| Jun | 20 | 22 | 24 | 26 | 28 | - |
| Jul | 15 | 17 | 19 | 21 | 23 | 25 |
| Aug | 12 | 13 | 15 | 17 | 19 | 21 |
| Sep | 9 | 10 | 12 | 14 | 16 | 18 |
| Oct | 6 | 8 | 10 | 12 | 14 | 16 |
| Nov | 5 | 7 | 9 | 11 | 13 | 15 |

*(These are approximate — consult the full BHA published table for exact values)*

For 3-year-olds vs 4+ year-olds:

| Month | 5f | 6f | 7f | 1m | 1m2f | 1m4f | 1m6f | 2m |
|---|---|---|---|---|---|---|---|---|
| Jan | 11 | 12 | 13 | 14 | 15 | 16 | 17 | 18 |
| Mar | 10 | 11 | 12 | 13 | 14 | 15 | 16 | 17 |
| May | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 |
| Jul | 3 | 4 | 5 | 7 | 8 | 9 | 10 | 11 |
| Sep | 0 | 1 | 2 | 3 | 5 | 6 | 7 | 8 |
| Nov | 0 | 0 | 0 | 1 | 2 | 3 | 4 | 5 |

*(Approximate — full BHA table has fortnightly intervals)*

### 6.3 Jump Racing WFA (Unified Anglo-Irish Scale, from May 2021)

Key differences from flat:
- Jumps WFA operates monthly (not fortnightly)
- 4-year-old hurdlers are considered mature later in their season than flat horses
- 5-year-old chasers still receive an allowance early in their age year
- The BHA and IHRB unified their scales in 2021 (previously varied by up to 5-7lbs)

### 6.4 The Racing Post Alternative WFA

The Racing Post developed their own WFA scale approximately 20 years ago, which differs from the BHA scale in several respects:
- Generally less generous to 2-year-olds in the first half of the season
- Dave Edwards (Topspeed) has expressed that the BHA scale inflates 2yo ratings early in the year
- The RP scale is not publicly available in full but is embedded in Topspeed figure calculations

### 6.5 Recommended Approach: Empirical WFA from Your Data

With 10 years of data and TFig as a training target, the cutting-edge approach is to **derive your own empirical WFA**:

**Method:**
1. Calculate raw speed figures for all runners WITHOUT any WFA adjustment
2. For horses that ran in both their 2yo/3yo seasons AND their 4yo+ seasons, compare the raw figures
3. Group by month and distance
4. Calculate the median difference between the raw figure as a younger horse and the established figure as a mature horse
5. This empirical curve IS your WFA table

**Advantages:**
- Based on actual observed maturation in your dataset
- Captures any systematic bias in official scales
- Can be updated annually as new data comes in
- Accounts for changes in horse population (e.g., if modern horses mature faster due to training methods)

**Validation:** Compare your empirical WFA against both the BHA and RP scales. Where yours differs significantly, investigate whether the official scale or your data is the outlier.

---

## 7. SEX ALLOWANCES

### 7.1 Current Allowances

**Flat Racing (UK/Ireland):**
- Fillies/mares receive **3lbs** from colts/geldings in races from May to September
- Fillies/mares receive **5lbs** from colts/geldings in races from October to April
- In fillies-only or mares-only races, no sex allowance applies (all carry WFA only)

**Jump Racing (UK/Ireland):**
- Mares receive **7lbs** from geldings/entire horses in most open races
- In mares-only races, no sex allowance applies

### 7.2 Treatment in Speed Figures

**If adjusting to a base weight:** The sex allowance is already reflected in the weight carried. A mare receiving 7lbs carries 7lbs less → when you adjust to base weight, this is automatically captured.

**If NOT adjusting to base weight:** You need to explicitly add the sex allowance to mares' figures to compare them fairly against males.

### 7.3 Are Sex Allowances Correctly Calibrated?

This is a researchable question with your 10-year dataset:
- Do mares/fillies receiving the allowance win at the rate expected by random chance?
- If they overperform → the allowance is too generous (their figures are inflated)
- If they underperform → the allowance is insufficient

**Recommendation:** Include sex as a feature in your ML model and let it learn the actual performance differential. Compare the model's learned effect to the official allowances.

---

## 8. BEATEN-LENGTHS CONVERSION

### 8.1 Purpose

Converts the margin by which each horse was beaten into a time difference, then into a pounds rating relative to the winner.

### 8.2 Standard Margin Definitions

| Margin | Lengths Equivalent |
|---|---|
| Short head | 0.05 - 0.10 |
| Head | 0.10 - 0.20 |
| Neck | 0.25 - 0.30 |
| Half a length | 0.50 |
| Three-quarters | 0.75 |
| One length | 1.00 |
| 1¼ lengths | 1.25 |
| ... | ... |
| Distance (Flat) | 30+ |
| Distance (Jumps) | 30+ |

### 8.3 Conversion Formula

For each horse behind the winner:
```
time_behind = cumulative_beaten_lengths × seconds_per_length
lbs_behind = cumulative_beaten_lengths × lbs_per_length_at_distance
horse_figure = winner_figure - lbs_behind
```

**Cumulative beaten lengths:** For a horse finishing 3rd, you add its margin from the 2nd-place horse to the 2nd-place horse's margin from the winner.

### 8.4 Reliability Concerns

- Beaten distances are **estimated by the judge**, not measured electronically (except at photo-finish resolution)
- Larger margins (10+ lengths) are increasingly approximate
- In jump racing, distances can be very large (30+ lengths) and unreliable
- At very slow paces, beaten distances understate actual ability differences (horses bunch up)
- At very fast paces, beaten distances can overstate differences (field strings out)

**Recommendation:** Consider capping beaten-length adjustments at a certain threshold (e.g., 20 lengths for flat, 30 for jumps) and flagging any figure derived from larger margins as low-confidence.

### 8.5 BHA Historical Inconsistency

The BHA changed their official lengths-per-second conversion at one point. Ensure your 10-year historical dataset uses a consistent conversion throughout. If the data source changed their beaten-length calculations mid-dataset, you'll need to normalise.

---

## 9. PACE ANALYSIS & SECTIONAL TIMING

### 9.1 Why Pace Matters for Speed Figures

A horse's final time is the product of two factors:
1. **Its own ability** (what we want to measure)
2. **The pace scenario it encountered** (what we want to remove)

A slowly-run race produces slow final times that understate every runner's ability. A fast-paced race produces honest times that reflect true ability. Without a pace/sectional adjustment, speed figures systematically underrate horses who encountered slow paces and accurately rate (or slightly overrate) those who encountered fast paces.

### 9.2 Timeform Finishing Speed Percentage Method

This is the industry-leading approach:

**Step 1: Calculate finishing speed as a percentage of average race speed**

```
finishing_speed_pct = (100 × T × d) / (D × t)
```

Where:
- T = overall race time (seconds)
- t = sectional time for the measured segment (seconds)
- D = overall race distance (furlongs)
- d = sectional distance (furlongs)

Example: A 1m race (D=8f) run in T=99 seconds, with a last-3f (d=3f) sectional of t=35 seconds:
```
finishing_speed_pct = (100 × 99 × 3) / (8 × 35) = 106.1%
```

A value above 100% means the horse finished faster than its average race speed. A value below 100% means it slowed down.

**Step 2: Compare to sectional pars**

Each course/distance has a characteristic finishing speed pattern. Establish **sectional pars** by:
1. Identifying all races at that course/distance where the overall time was genuinely fast (relative to the field's ability)
2. Calculating the finishing speed % for those races
3. The median of these values is the **par** — the expected finishing speed when a race is truly run

**Step 3: Upgrade/downgrade calculation**

```
upgrade_lbs = (actual_finishing_speed_pct - par_finishing_speed_pct) × conversion_factor
```

If a horse's finishing speed was significantly higher than the par, it was likely held up by a slow early pace → **upgrade** the figure. If significantly lower, the horse may have been flattered by a slow pace → figures stands or **downgrade**.

### 9.3 Sectional-Adjusted Speed Ratings (SASRs)

The adjusted figure incorporating sectional analysis:
```
SASR = raw_speed_figure + upgrade_lbs
```

These are the gold standard in modern speed figures. They routinely identify horses that:
- Finished behind the winner but actually ran to a higher level
- Were compromised by slow pace and can be expected to improve on the figure
- Were flattered by an unsustainably fast pace

### 9.4 Visually Adjusted Ratings (VARs)

For races where sectional data is unavailable, or to supplement sectional analysis:
- Re-watch each race (side-on and head-on views)
- Identify horses that suffered significant interference, traffic problems, or were forced wide
- Estimate the time lost (in lengths) and upgrade accordingly

This is subjective and labour-intensive but adds value, particularly for:
- Jump racing (where sectional data is less available)
- Big-field handicaps where in-running incidents are common
- Irish racing where sectional timing is less prevalent

### 9.5 Data Sources for Sectionals

| Provider | Coverage | Granularity |
|---|---|---|
| Timeform | UK & Ireland flat, selected jumps | Last 2-3f, sometimes more |
| TotalPerformance Data | UK flat | Furlong-by-furlong at equipped courses |
| Racing TV / ITV Racing | Displayed during broadcast | Last 2f typically |
| Racing Post (Proform partnership) | UK flat & AW | Imported into Proform system |
| Individual racecourses | Varies | Some courses have their own timing systems |

### 9.6 Pace Position Analysis

Even without sectional times, knowing where a horse raced in the field is valuable:
- **Made all / led:** More vulnerable to pace pressure, figures may be slightly inflated
- **Tracked leader:** Optimal position in most scenarios
- **Held up:** May have been disadvantaged by slow pace, figures potentially understated
- **Prominent / handy:** Generally good position

Include race position (at various call points) as features in your ML model.

---

## 10. DAILY TRACK VARIANT — ADVANCED METHODS

### 10.1 Standard Method Recap

Average the deviation-per-furlong across the card with outlier removal (covered in Section 4).

### 10.2 Bayesian/Iterative Variant

Instead of comparing to static standard times, compare to **expected times** based on prior speed figures:

```python
# Pseudocode
for each race on card:
    expected_winning_time = estimate_from_prior_figures(winner)
    deviation = actual_time - expected_winning_time
    deviations.append(deviation / distance_in_furlongs)

# Remove outliers
trimmed = remove_highest_and_lowest(deviations)
going_allowance = mean(trimmed)
```

**Advantages:**
- Doesn't rely on static standard times (which may be slightly wrong)
- Self-correcting: as your figures improve, so do your variants
- Handles class differences naturally (a maiden is expected to run slower)

**Bootstrapping problem:** On day 1, you have no prior figures. Start with the standard method and switch to iterative once you have 2-3 months of data.

### 10.3 ML-Derived Variant

Train a model to predict expected finishing times using features like:
- Horse's prior speed figures
- Distance preferences
- Going preferences
- Trainer/jockey expected performance
- Class of race

The systematic residual across a card (predicted times vs actual times) IS the daily variant.

**This removes the biggest weakness of traditional speed figures**: the circular dependency where the going allowance depends on the standard time which depends on historical going allowances.

### 10.4 Confidence Weighting

Not all races contribute equally to the going allowance calculation:

| Race Type | Confidence Weight | Reason |
|---|---|---|
| Open handicap (0-100+) | Highest | Known quantities, predictable times |
| Handicap (Class 4-6) | High | Established horses, decent sample |
| Conditions/Listed | Medium-High | Good horses, but small fields |
| Novice/Maiden | Low | Unknown quantities, unpredictable times |
| 2yo maiden (early season) | Very Low | Horses with no prior form |

Weight your going allowance calculation accordingly — give more influence to races with known-quantity horses.

---

## 11. RUN-UP DISTANCE & RAIL MOVEMENTS

### 11.1 Run-Up Distance

The distance from the starting stalls to where the timing beam starts. This varies by course and by start position at the same course.

**Impact:** A longer run-up means horses reach full speed before timing begins → faster raw times → if not accounted for, figures will be inflated.

**Approach:**
- Maintain a database of run-up distances per course per start position
- This is implicitly captured in your standard times IF those standards are built from races at the same start position
- Problems arise when a start position changes (e.g., stalls repositioned for new ground) — the standard time may no longer be valid

### 11.2 Rail Movements (Dolling Out)

When running rails are moved to provide fresh ground, the actual race distance changes. The BHA publishes this data after each meeting.

**Impact on distance:**
- Rails moved OUT on a bend → longer actual distance → slower times
- Rails moved IN on a bend → shorter actual distance → faster times
- On the straight → minimal impact on distance but can affect draw bias

**Calculation:**
At some courses, rail movements can add 100+ yards to the race distance. For example:
```
extra_distance_yards = rail_offset_yards × bend_factor
extra_furlongs = extra_distance_yards / 220
time_adjustment = extra_furlongs × standard_time_per_furlong
```

**The "Constant 15" method** (developed by independent researchers): An empirical factor applied to rail movement distances to estimate the actual time impact. The precise factor varies by course geometry.

**Recommendation:** Incorporate rail movement data as a distance correction BEFORE computing the deviation from standard time. This is a significant source of error if ignored, particularly at tracks like:
- Haydock (recently added 107 yards to the 1m6f trip due to rail movements)
- Newbury (straight and round course configurations)
- Sandown (inner vs outer configurations)
- Most Irish courses during winter

---

## 12. DRAW BIAS ADJUSTMENT

### 12.1 Purpose

At certain courses and distances, the starting stall position confers a statistically significant advantage or disadvantage. This affects finishing times and therefore speed figures.

### 12.2 Known Significant Draw Biases (UK)

| Course | Distance | Bias | Notes |
|---|---|---|---|
| Chester | 5f - 1m 2f | Low draws dominant | Tight inner rail, difficult to make up ground |
| Beverley | 5f | High draws | Camber of straight favours stands' side |
| Musselburgh | 5f - 7f | Varies by going | Low draws on soft, high draws on fast |
| Carlisle | 5f - 6f | Low draws | Inside rail advantageous |
| Thirsk | 6f - 7f | Low draws | Moderate but consistent |
| Ascot | 5f (straight) | Varies by going | Far side (high) on soft ground |
| Newmarket (July) | 6f | Varies by going | Far side in big fields on soft ground |
| York | 5f - 6f | Varies by going | Stands' side on soft ground |
| Doncaster | 5f - 7f (str) | Varies by going | Stands' rail preferred in large fields |
| Goodwood | 5f - 7f | Low draws | Inside rail preferred |

### 12.3 Treatment in Speed Figures

**Conservative approach:** Don't adjust individual figures for draw. Instead, include draw as a feature in your predictive model.

**Advanced approach:** Calculate draw-adjusted figures:
1. For each course/distance, calculate the average beaten-lengths advantage per draw position (from your 10-year data)
2. Express as a lbs adjustment per stall position
3. Apply: `adjusted_figure = raw_figure + draw_adjustment`

**Challenges:**
- Draw bias changes with going conditions (many courses have no bias on good ground but significant bias on soft)
- Draw bias changes with field size (stall 1 might be advantageous in a 20-runner field but neutral in a 6-runner field)
- The BHA changed stall positioning conventions at some courses around 2010 — pre-2010 draw data may not apply
- Rail movements on the day can alter draw dynamics

### 12.4 Recommended Implementation

Include draw position, field size, and going as interaction features in your ML model. This allows the model to learn that draw 1 at Chester 7f on good ground is X points advantageous, while draw 1 at York 6f on good to firm ground is Y points advantageous (or disadvantageous). This is far more nuanced than a fixed per-course adjustment.

---

## 13. WIND ADJUSTMENT

### 13.1 Impact on Speed Figures

Wind significantly affects finishing times, particularly:
- **Sprint races** (5f-7f): highest impact per furlong
- **Exposed courses**: Newmarket, Epsom, Goodwood (downland courses)
- **Straight courses**: where the entire final furlong is into/with the wind

### 13.2 Wind Effects

| Wind Condition | Effect on Times | Effect on Figures (if unadjusted) |
|---|---|---|
| Headwind in straight | Slower times | Figures understated |
| Tailwind in straight | Faster times | Figures overstated |
| Crosswind | Minimal time effect | Can amplify draw bias |
| Variable/gusty | Inconsistent | Increases figure noise |

### 13.3 Quantifying Wind Impact

Research suggests:
- A **10mph headwind** can slow a 5f time by approximately 0.5-1.0 seconds
- A **10mph tailwind** can speed a 5f time by approximately 0.3-0.6 seconds (less than headwind due to aerodynamics)
- Impact scales roughly linearly with wind speed up to ~25mph, then increases non-linearly

### 13.4 Data Sources

| Source | Data Available | Update Frequency |
|---|---|---|
| Met Office API | Wind speed, direction, gusts | Hourly |
| OpenWeather API | Wind speed, direction | Hourly |
| Visual Crossing | Historical weather by location | Daily |
| Racing Post/RP | Sometimes noted in race comments | Race-by-race |

### 13.5 Implementation

**Method A: Direct adjustment**
1. Get wind speed and direction at race time
2. Calculate the component of wind in the direction of the final straight
3. Apply a per-furlong time adjustment based on wind speed
4. Add to going allowance calculation

**Method B: Absorbed into going allowance**
If the wind is consistent throughout the meeting, its effect is captured in the going allowance (all races systematically faster or slower). This is usually adequate.

**Method C: ML feature**
Include wind speed and direction as features. The model learns the relationship. This is the most flexible approach and handles changing wind conditions during a meeting.

**Recommendation:** Method C as primary, with Method B as fallback when weather data is unavailable. Wind adjustment is lower priority than going allowance and sectionals but adds measurable accuracy, particularly for sprint races.

---

## 14. SURFACE TYPE CALIBRATION

### 14.1 Surface Types in UK/Ireland

**Turf:** The primary surface. Highly variable — going can range from firm to heavy. Each course's turf has different characteristics (draining speed, soil composition, grass type).

**All-Weather surfaces:**

| Surface | Tracks (UK) | Characteristics |
|---|---|---|
| Polytrack | Kempton, Lingfield, Chelmsford | Consistent, drains well, closest to fast turf |
| Tapeta | Newcastle, Wolverhampton (from 2024) | Consistent, slightly different speed profile to Polytrack |
| Fibresand | Southwell | Deeper, slower, more stamina-sapping |

### 14.2 Cross-Surface Comparison

Turf and AW figures are NOT directly comparable without adjustment:
- AW figures are more **reliable** (surface doesn't change day-to-day)
- Some horses run to significantly different levels on different surfaces
- The same horse can have a 10-15lb difference between its best turf and best AW figures

### 14.3 Implementation

**Separate standard times** for each surface type at each course. This is essential — you cannot use a turf standard time for an AW race.

**Separate going allowance ranges** for each AW surface (see Section 4.3).

**For cross-surface comparison:** Build a surface conversion factor from horses that have run on both. Your 10-year data will contain thousands of horses with form on both turf and AW — the median difference in their figures IS your conversion factor.

**In your ML model:** Include surface type as a categorical feature. The model will learn surface preferences and conversion factors implicitly.

---

## 15. CLASS PARS

### 15.1 Purpose

Class pars represent the expected winning speed figure for each class of race. They serve three critical functions:
1. **Sanity checking** your daily figures
2. **Going allowance validation** — if a Class 5 winner gets a figure above Class 2 par, the going allowance is probably wrong
3. **Race-day projection** — what figure is needed to win today's race

### 15.2 Building Class Pars from Your Data

For each combination of: `Race Class × Distance Band × Surface`:

```python
# Pseudocode
class_par = median(winning_speed_figures for all races matching this combination)
```

### 15.3 Flat Racing Class Pars (approximate guide)

| Class | 5f | 6f | 7f | 1m | 10f | 12f | 14f | 2m |
|---|---|---|---|---|---|---|---|---|
| Group 1 | 118 | 118 | 118 | 120 | 120 | 120 | 118 | 116 |
| Group 2 | 112 | 112 | 112 | 114 | 114 | 114 | 112 | 110 |
| Group 3 | 108 | 108 | 108 | 110 | 110 | 110 | 108 | 106 |
| Listed | 104 | 104 | 104 | 106 | 106 | 106 | 104 | 102 |
| Class 2 Hcap | 95 | 95 | 96 | 97 | 97 | 97 | 96 | 94 |
| Class 3 Hcap | 85 | 85 | 86 | 87 | 87 | 87 | 86 | 84 |
| Class 4 Hcap | 75 | 75 | 76 | 77 | 77 | 77 | 76 | 74 |
| Class 5 Hcap | 65 | 65 | 66 | 67 | 67 | 67 | 66 | 64 |
| Class 6 Hcap | 55 | 55 | 56 | 57 | 57 | 57 | 56 | 54 |
| Novice/Maiden | 60 | 60 | 62 | 64 | 65 | 65 | 64 | 62 |

*(These are indicative — derive your own from your data. Pars vary by time of year and will differ for 2yo, 3yo, and older horse races.)*

### 15.4 Using Pars for Going Allowance Validation

If your calculated going allowance produces figures where:
- Multiple winners exceed their class par by 15+ lbs → going allowance is probably too generous (ground faster than you estimated)
- Multiple winners are 15+ lbs below class par → going allowance is probably too harsh (ground slower than you estimated)

Adjust iteratively until figures align with class expectations.

---

## 16. INTER-TRACK VARIANT (ITV)

### 16.1 Purpose

The ITV accounts for systematic speed differences between tracks that aren't fully captured by standard times alone. It answers: "If a horse ships from Track A to Track B, do its figures hold up?"

### 16.2 Method

Analyse horses that have raced at multiple tracks. If horses shipping from Track A to Track B consistently produce figures 3lbs higher at Track B, then either:
- Track A's standard times are too fast (setting the bar too high)
- Track B's standard times are too slow (setting the bar too low)
- There's a systematic measurement difference

### 16.3 TrackMaster's Three-Component System

TrackMaster (US) uses a three-part speed figure:
1. **Raw speed rating** — from time and distance
2. **Inter-Track Variant (ITV)** — annual adjustment for track-to-track differences
3. **Daily Track Variant (DTV)** — daily going/weather adjustment

The ITV is updated annually by analysing cross-track shippers. This is a sound methodology and worth replicating:

```python
# For each pair of tracks A and B:
# Find all horses that raced at both within a 60-day window
# Calculate: median(figure_at_A - figure_at_B) for same horse
# If consistently != 0, adjust one track's standard times
```

### 16.4 UK/Ireland Specifics

The ITV is particularly important for:
- **UK vs Ireland comparisons**: Irish standard times may be systematically different from UK
- **Major festival form**: Ascot vs Newmarket vs Goodwood vs York comparisons
- **AW vs specific turf tracks**: some AW tracks produce systematically higher/lower figures

**Recommendation:** Build an ITV matrix from your 10-year data. For each pair of courses with sufficient cross-track runners, calculate the systematic difference and adjust.

---

## 17. ML MODEL ARCHITECTURE

### 17.1 Objective

Use machine learning to:
1. **Refine the daily variant** beyond what traditional methods achieve
2. **Learn non-linear relationships** between weight, distance, going, and performance
3. **Derive an empirical WFA** from observed data
4. **Produce more accurate figures** by training against TFig

### 17.2 Feature Engineering

**Temporal features (from historical data):**

| Feature | Type | Description |
|---|---|---|
| `median_fig_last_3` | Continuous | Median of last 3 speed figures |
| `median_fig_last_6` | Continuous | Median of last 6 speed figures |
| `best_fig_career` | Continuous | Career-best speed figure |
| `best_fig_distance` | Continuous | Best figure at today's distance (±1f) |
| `best_fig_going` | Continuous | Best figure on similar going |
| `best_fig_course` | Continuous | Best figure at today's course |
| `fig_trend` | Continuous | Linear trend slope of last 5 figures |
| `fig_consistency` | Continuous | Std dev of last 5 figures |
| `days_since_run` | Continuous | Days since last race |
| `runs_this_season` | Integer | Number of starts this season |
| `career_runs` | Integer | Total career starts |

**Race-specific features:**

| Feature | Type | Description |
|---|---|---|
| `weight_carried_lbs` | Continuous | Actual weight carried |
| `weight_vs_base` | Continuous | Difference from 9st/11st base |
| `age` | Integer | Horse's age |
| `sex` | Categorical | Colt / Gelding / Filly / Mare |
| `distance_furlongs` | Continuous | Race distance |
| `distance_change` | Continuous | Change from last run |
| `class_numeric` | Ordinal | Race class (1-6, Group 1-3, Listed) |
| `class_change` | Integer | Change in class from last run |
| `going_description` | Categorical | Official going |
| `going_stick_reading` | Continuous | If available |
| `surface` | Categorical | Turf / Polytrack / Tapeta / Fibresand |
| `course_id` | Categorical | Unique course identifier |
| `draw` | Integer | Stall number |
| `field_size` | Integer | Number of runners |
| `race_type` | Categorical | Handicap / Conditions / Maiden / Listed / Group |

**Environmental features:**

| Feature | Type | Description |
|---|---|---|
| `wind_speed_mph` | Continuous | Wind speed at race time |
| `wind_direction` | Continuous | Wind direction (degrees) |
| `wind_straight_component` | Continuous | Wind component along the straight |
| `temperature_c` | Continuous | Temperature at race time |
| `precipitation_mm` | Continuous | Rainfall in previous 24h |
| `rail_movement_yards` | Continuous | Dolling-out distance |
| `month` | Ordinal | Month of year (seasonal effects) |

**Jockey/Trainer features:**

| Feature | Type | Description |
|---|---|---|
| `jockey_win_pct_90d` | Continuous | Jockey win % over last 90 days |
| `trainer_win_pct_14d` | Continuous | Trainer win % over last 14 days |
| `trainer_win_pct_course` | Continuous | Trainer win % at this course |
| `jockey_trainer_combo_pct` | Continuous | Win % for this jockey/trainer pair |
| `jockey_change` | Boolean | Different jockey from last run |
| `first_time_blinkers` | Boolean | First-time headgear application |
| `equipment_change` | Categorical | Any equipment change |

**Sectional features (where available):**

| Feature | Type | Description |
|---|---|---|
| `finishing_speed_pct` | Continuous | Timeform finishing speed % |
| `finishing_speed_vs_par` | Continuous | Finishing speed relative to course par |
| `upgrade_lbs` | Continuous | Sectional-derived upgrade |
| `pace_position_1f` | Ordinal | Position at 1f marker |
| `pace_position_half` | Ordinal | Position at halfway |

### 17.3 Model Selection

**Recommended: Gradient Boosting (XGBoost or LightGBM)**

Advantages for this problem:
- Handles mixed feature types (continuous, categorical, ordinal)
- Naturally handles missing values (common in racing data)
- Learns non-linear relationships and feature interactions
- Interpretable through feature importance and SHAP values
- Fast inference for daily production use
- Robust to outliers

**Alternative: Neural Network (for large datasets)**

If your dataset is very large (millions of rows), a deep learning approach could capture more complex patterns. However, gradient boosting typically outperforms for tabular racing data.

### 17.4 Training Strategy

**Target variable:** TFig (your existing time figure)

**Training approach:**
1. **Temporal split**: Train on years 1-8, validate on year 9, test on year 10
2. **Walk-forward validation**: Sequentially train on expanding windows, never using future data
3. **Hyperparameter tuning**: Use Bayesian optimisation (Optuna) on the validation set

**Loss function:** Mean Squared Error (MSE) for regression against TFig, or Mean Absolute Error (MAE) if you prefer robustness to outliers.

### 17.5 Two-Stage Model

**Stage 1: Traditional speed figures** — compute using the classical methodology (standard times, going allowance, weight adjustment, WFA)

**Stage 2: ML refinement** — use the traditional figure as one input feature, along with all other features, to predict TFig. The model learns the residual — what the traditional method misses.

```
Final_Figure = Traditional_Figure + ML_Residual
```

This two-stage approach is more robust than end-to-end ML because:
- The traditional figure captures most of the signal
- The ML only needs to learn the corrections
- It's more interpretable (you can examine what the ML is correcting)
- It degrades gracefully (if ML fails, you still have the traditional figure)

### 17.6 Validation Metrics

| Metric | Purpose | Target |
|---|---|---|
| Correlation with TFig | Primary accuracy | > 0.90 |
| MAE vs TFig | Average error magnitude | < 3 lbs |
| RMSE vs TFig | Penalises large errors | < 4 lbs |
| Class par alignment | Sanity check | Within ±3 lbs of expected pars |
| Cross-surface consistency | Same horse, different surfaces | Low systematic bias |
| Temporal stability | Figures don't drift over time | Year-on-year correlation > 0.85 |
| Predictive power | Can figures predict finishing order? | Higher ROC-AUC than existing ratings |

---

## 18. DAILY PRODUCTION WORKFLOW

### 18.1 Pre-Racing

```
1. CHECK DATA SOURCES
   ├── Results feed active
   ├── Weather API accessible
   ├── Going reports available
   └── Rail movement data published

2. PREPARE MEETING DATA
   ├── Identify course configurations in use
   ├── Note any rail movements
   ├── Record pre-race going descriptions & stick readings
   └── Capture wind speed/direction forecast
```

### 18.2 Post-Racing (For Each Meeting)

```
3. COLLECT RESULTS
   ├── Winning times (to 0.01s)
   ├── Beaten margins (all runners)
   ├── Weight carried (all runners)
   ├── Starting stall positions
   ├── Going description (any changes during meeting)
   ├── Sectional times (where available)
   ├── In-running positions
   └── Equipment worn

4. CALCULATE GOING ALLOWANCE
   ├── Compute deviation from standard for each race
   ├── Express as seconds per furlong
   ├── Check for split-card conditions
   ├── Remove outliers (highest and lowest)
   ├── Average remaining → going allowance
   ├── Validate against official going, stick readings, and class pars
   └── If iterative method: use prior figures for expected times

5. COMPUTE RAW SPEED FIGURES (WINNER)
   For each race:
   ├── corrected_time = actual_time - (going_allowance × distance_in_furlongs)
   ├── deviation_from_standard = corrected_time - standard_time
   ├── raw_figure_lengths = deviation_from_standard / seconds_per_length
   ├── raw_figure_lbs = raw_figure_lengths × lbs_per_length_at_distance
   └── winner_speed_figure = base_rating - raw_figure_lbs

6. COMPUTE PLACED HORSE FIGURES
   For each placed horse:
   ├── cumulative_beaten_lengths (from margins)
   ├── lbs_behind = cumulative_beaten_lengths × lbs_per_length_at_distance
   └── horse_speed_figure = winner_speed_figure - lbs_behind

7. APPLY ADJUSTMENTS
   For each horse:
   ├── Weight adjustment: figure += (weight_carried - base_weight)
   ├── WFA adjustment: figure += WFA_allowance (if using)
   ├── Sectional upgrade: figure += upgrade_lbs (if sectionals available)
   └── [Wind, draw adjustments if implemented]

8. SANITY CHECK
   ├── Compare winning figures to class pars (flag if >15lbs deviation)
   ├── Compare individual figures to horse's prior figures (flag if >15lbs change)
   ├── Check that going allowance produces consistent figures across the card
   ├── Review any flagged anomalies manually
   └── Adjust if timing errors or obvious data issues detected

9. STORE & UPDATE
   ├── Store all figures in database
   ├── Update rolling class pars
   ├── Update rolling standard times (if end of season)
   ├── Update inter-track variants (quarterly)
   └── Retrain ML model (monthly or quarterly)
```

### 18.3 Worked Example

**Race:** 3:15 Newbury, Class 3 Handicap, 1m 2f (10 furlongs), Good to Firm ground

**Data:**
- Standard time for Newbury 1m 2f (round course): 128.50 seconds
- Winning time: 127.80 seconds
- Going allowance for meeting: +0.22 s/f (ground riding slightly fast)
- Winner carried 9st 5lb (131 lbs)
- 2nd horse beaten 2 lengths, carried 9st 0lb (126 lbs)
- Lbs per length at 10f: 2.20

**Calculation (Winner):**

```
1. Going-corrected time:
   corrected = 127.80 - (0.22 × 10) = 127.80 - 2.20 = 125.60s

2. Deviation from standard:
   deviation = 125.60 - 128.50 = -2.90s (faster than standard)

3. Convert to lengths:
   lengths_faster = 2.90 / 0.2 = 14.5 lengths

4. Convert to lbs:
   lbs_faster = 14.5 × 2.20 = 31.9 lbs

5. Raw figure:
   raw_figure = 100 + 31.9 = 131.9 ≈ 132

6. Weight adjustment (to 9st 0lb base):
   weight_over_base = 131 - 126 = 5 lbs
   adjusted_figure = 132 + 5 = 137

   Wait — that's too high for a Class 3 handicap (par ≈ 87).
   This tells us the going allowance is probably wrong.
```

**This is exactly why the sanity check step matters.** If the going allowance produces a Class 3 winner at 137, either:
- The going allowance is too generous (ground was faster than +0.22 s/f)
- There was a timing error
- This was a truly exceptional performance (rare)

Re-examine the going allowance using other races on the card and adjust.

---

## 19. CONFIDENCE INDICATORS

### 19.1 Purpose

Not all speed figures are equally reliable. Attach a confidence indicator to each figure so users know how much weight to give it.

### 19.2 Confidence Factors

| Factor | High Confidence | Low Confidence |
|---|---|---|
| Going allowance reliability | 6+ races on card, consistent deviations | 3-4 races, high variance between deviations |
| Beaten distance | Close finish (< 5 lengths) | Large margin (20+ lengths) |
| Field size | 8+ runners | 3-4 runners |
| Race type | Open handicap | Maiden / 2yo early season |
| Timing quality | Electronic timing | Manual timing |
| Sectional data available | Yes → can validate pace context | No → raw time only |
| Rail movement data | Known and accounted for | Unknown or not accounted for |
| Wind conditions | Calm or known | Strong and variable |

### 19.3 Implementation

Assign a confidence score (1-5 or percentage) based on the above factors. Low-confidence figures should be flagged and treated with caution in predictive models.

---

## 20. JUMP RACING SPECIFICS

### 20.1 Additional Considerations for NH Racing

Jump racing introduces factors not present on the flat:

| Factor | Impact |
|---|---|
| Fence/hurdle negotiation time | Adds variable time per obstacle |
| Falls and unseated riders | Removes runners, affects going allowance calculation |
| Varied obstacle counts | Different number of fences/hurdles at different courses |
| Ground conditions more extreme | Heavy ground common in winter, dramatically slowing times |
| Longer distances | Greater cumulative error in beaten-length estimates |
| Conditional jockeys | Claim allowances more common and larger (7lb, 5lb, 3lb) |
| Age ranges | 4-12+ year-old horses competing |

### 20.2 Standard Times for Jump Racing

Jump standard times must account for:
- **Number of obstacles**: More obstacles = more time. A standard time for a 2m chase (8-10 fences) is slower than a 2m hurdle (8 flights)
- **Obstacle type**: Fences slow horses more than hurdles
- **Course-specific obstacle positions**: Some courses have fences on the hill, others on the flat

### 20.3 Fence/Hurdle Time Allowance

Approximate time additions per obstacle:
- **Hurdle flight**: 0.3-0.5 seconds per flight
- **Standard fence**: 0.5-0.8 seconds per fence
- **Open ditch/water jump**: 0.8-1.2 seconds

These can be refined from your data by comparing chase vs hurdle times at the same course and distance (where available).

### 20.4 Jump Racing WFA

The unified Anglo-Irish jump WFA scale (effective May 2021) should be used. Key differences from flat:
- 4-year-old hurdlers are considered fully mature later in their season
- 5-year-old chasers still receive a small allowance early in the season
- Monthly intervals (not fortnightly)

### 20.5 Going Allowance in Jump Racing

Going conditions in NH are more extreme and variable:
- Heavy going can slow times by 2+ seconds per furlong
- Waterlogged ground creates massive deviations from standard
- Going can change dramatically during a meeting (especially in Ireland)
- Use more aggressive outlier removal for NH going allowances

---

## 21. DATA REQUIREMENTS & SOURCES

### 21.1 Minimum Data Required Per Race

| Field | Required | Nice to Have |
|---|---|---|
| Race date & time | ✓ | |
| Course | ✓ | |
| Distance (exact, in yards) | ✓ | |
| Course configuration | ✓ | |
| Race class | ✓ | |
| Race type (hcap/conditions/maiden) | ✓ | |
| Going description | ✓ | Going stick reading |
| Winning time (to 0.01s) | ✓ | |
| Beaten margins (all runners) | ✓ | |
| Finishing positions | ✓ | |
| Weight carried (all runners) | ✓ | |
| Age (all runners) | ✓ | |
| Sex (all runners) | ✓ | |
| Draw / stall position | ✓ | |
| Field size | ✓ | |
| Jockey | | ✓ |
| Trainer | | ✓ |
| Equipment worn | | ✓ |
| In-running positions | | ✓ |
| Sectional times | | ✓ (high value) |
| Rail movements | | ✓ |
| Wind data | | ✓ |
| OR / RPR / TFig | | ✓ (for training) |

### 21.2 Data Sources

| Source | Coverage | Data Available |
|---|---|---|
| Racing Post / Raceform | UK & Ireland | Times, margins, OR, RPR, Topspeed, sectionals (paid) |
| Timeform | UK & Ireland | TFig, sectionals, finishing speed %, ratings (paid) |
| Arion Pedigrees | Australasia | Limited UK/Ire data |
| Breednet | Australasia | Limited UK/Ire data |
| BHA | UK | Official results, rail movements, going reports |
| HRI / IHRB | Ireland | Official results, going reports |
| RacingAPI / Betfair | UK & Ireland | Results, prices, in-running data |
| Open Weather / Met Office | UK & Ireland | Weather data |
| Proform Racing | UK | Speed figures, sectionals, system builder |

---

## 22. SYSTEM ARCHITECTURE FOR DAILY PRODUCTION

### 22.1 Recommended Tech Stack

```
DATA LAYER
├── PostgreSQL / SQLite: Race results, standard times, class pars
├── Parquet files: Historical 10-year dataset for ML training
└── CSV/JSON: Daily data ingestion

PROCESSING LAYER
├── Python: Core speed figure calculations
│   ├── pandas: Data manipulation
│   ├── numpy: Numerical calculations
│   ├── scipy: Statistical functions
│   └── Custom modules: Going allowance, lbs-per-length, WFA
├── XGBoost / LightGBM: ML model
└── Optuna: Hyperparameter tuning

OUTPUT LAYER
├── Daily speed figures → database
├── Going allowances → database
├── Confidence scores → database
├── Anomaly flags → review queue
└── Dashboard / reporting (optional)
```

### 22.2 Claude Code Workflow

Since the data is too large for Claude.ai, use Claude Code with local filesystem access:

```
project/
├── data/
│   ├── raw/                    # 10 years of race data
│   ├── standard_times/         # Per course/distance
│   ├── class_pars/             # Per class/distance/surface
│   ├── lbs_per_length/         # Per course/distance
│   ├── wfa_tables/             # Weight for age scales
│   └── going_allowances/       # Historical going allowances
├── models/
│   ├── speed_figure_model.pkl  # Trained XGBoost model
│   └── training_logs/
├── src/
│   ├── standard_times.py       # Standard time calculation
│   ├── going_allowance.py      # Going allowance calculation
│   ├── speed_figures.py        # Core figure calculation
│   ├── sectional_adjustment.py # Sectional timing upgrades
│   ├── ml_refinement.py        # ML model training & inference
│   ├── validation.py           # Sanity checks & validation
│   └── daily_pipeline.py       # End-to-end daily runner
├── notebooks/
│   ├── EDA.ipynb               # Exploratory data analysis
│   ├── standard_time_analysis.ipynb
│   ├── wfa_empirical.ipynb
│   └── model_training.ipynb
└── output/
    ├── daily_figures/           # Daily output files
    └── reports/                 # Validation reports
```

---

## 23. IMPLEMENTATION ROADMAP

### Phase 1: Foundation (Weeks 1-3)
- [ ] Load and explore the 10-year dataset
- [ ] Build standard times for all course/distance combinations
- [ ] Build lbs-per-length tables
- [ ] Implement basic going allowance calculation
- [ ] Implement beaten-lengths conversion
- [ ] Produce first set of raw speed figures
- [ ] Compare to TFig — establish baseline correlation

### Phase 2: Core Adjustments (Weeks 4-6)
- [ ] Implement weight carried adjustment
- [ ] Implement WFA (start with BHA scale, plan empirical derivation)
- [ ] Implement sex allowance handling
- [ ] Implement surface-specific calibration
- [ ] Build class pars from your figures
- [ ] Build inter-track variant matrix
- [ ] Refine going allowance (iterative method)

### Phase 3: Advanced Features (Weeks 7-10)
- [ ] Integrate sectional timing data (if available)
- [ ] Implement sectional-adjusted speed ratings
- [ ] Incorporate rail movement data
- [ ] Incorporate wind data
- [ ] Build draw bias analysis
- [ ] Implement confidence scoring
- [ ] Build jump-racing specific adjustments

### Phase 4: ML Refinement (Weeks 11-14)
- [ ] Feature engineering for ML model
- [ ] Train XGBoost/LightGBM against TFig
- [ ] Derive empirical WFA from model
- [ ] Implement two-stage model (traditional + ML residual)
- [ ] Validate: correlation, MAE, class par alignment, temporal stability
- [ ] Hyperparameter optimisation

### Phase 5: Production (Weeks 15+)
- [ ] Build daily production pipeline
- [ ] Automate data collection
- [ ] Automate figure calculation
- [ ] Build anomaly detection and flagging
- [ ] Build reporting / dashboard
- [ ] Ongoing model retraining schedule
- [ ] Continuous validation against TFig

---

## 24. COMPONENT CHECKLIST

| # | Component | Priority | Section | Status |
|---|---|---|---|---|
| 1 | Standard times (per course/distance/config) | **Critical** | §2 | To build |
| 2 | Lbs per length tables | **Critical** | §3 | To build |
| 3 | Going allowance (daily track variant) | **Critical** | §4 | To build |
| 4 | Weight carried adjustment | **Critical** | §5 | To build |
| 5 | Weight for age scale | **Critical** | §6 | To build |
| 6 | Sex allowances | **High** | §7 | To build |
| 7 | Beaten-lengths conversion | **Critical** | §8 | To build |
| 8 | Pace analysis & sectional timing | **High** | §9 | To build (if data available) |
| 9 | Sectional-adjusted speed ratings | **High** | §9 | To build (if data available) |
| 10 | Advanced daily track variant (iterative) | **High** | §10 | To build (Phase 2) |
| 11 | Run-up distance adjustment | **Medium** | §11 | To build |
| 12 | Rail movement adjustment | **Medium-High** | §11 | To build |
| 13 | Draw bias analysis & adjustment | **Medium** | §12 | To build |
| 14 | Wind adjustment | **Medium** | §13 | To build |
| 15 | Surface type calibration | **High** | §14 | To build |
| 16 | Class pars | **High** | §15 | To derive from figures |
| 17 | Inter-track variant | **Medium-High** | §16 | To build |
| 18 | ML model (variant refinement) | **High** | §17 | To build (Phase 4) |
| 19 | Empirical WFA derivation | **High** | §6 & §17 | To build (Phase 4) |
| 20 | Confidence indicators | **Medium** | §19 | To build |
| 21 | Jump racing specifics | **High** | §20 | To build |
| 22 | Visual adjustment (race review) | **Low-Medium** | §9 | Manual process |
| 23 | Daily production pipeline | **Critical** | §18 & §22 | To build (Phase 5) |

---

## 25. KEY REFERENCES

| Source | Contribution |
|---|---|
| **Timeform** (Simon Rowlands) | Sectional timing methodology, finishing speed %, sectional pars, TFig system |
| **Racing Post Topspeed** (Dave Edwards) | Going allowances, WFA application, 5f benchmark, 22lbs/second at 5f |
| **Andrew Beyer** | Track variant calculation, parallel time charts, human judgment overlay |
| **TrackMaster** (AXCIS) | ITV + DTV three-component system, wind integration, class ratings |
| **BHA** (Dominic Gardiner-Hill) | 2025 unified European WFA scale, handicapping methodology |
| **Stars of the Clock** | Detailed standard time construction, going allowance workshopping |
| **The Helpful Punter** | SASR methodology, par finishing speeds, visual adjustments |
| **BRIS / TimeformUS** | Multi-pace figure system (E1, E2, Late Pace) |
| **Proform Racing** | Sectional metrics module, static/dynamic speed ratings, system builder |
| **chehsam (UK Betting Forum)** | Course-specific lbs-per-length derivation, going allowance tables, practical compilation |
| **Bill Benter (academic)** | ML for horse racing prediction, feature engineering, conditional logistic regression |
| **Charles Spencer (Medium/LinkedIn)** | ML speed figure engineering, relative features, Singapore model |
