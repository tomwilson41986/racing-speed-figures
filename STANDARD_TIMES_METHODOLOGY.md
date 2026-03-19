# Standard Times & Speed Figures — Methodology

> Best practice methodology for calculating standard times, going allowances, and speed figures for French flat racing.

---

## Core Principle

Standard times should be derived from **all races across all ground conditions**, with each race time going-adjusted to a common reference level before inclusion. This maximises sample sizes, eliminates seasonal bias, and produces standards calibrated to median observed conditions rather than an arbitrary "good ground" definition.

The alternative — filtering to "Bon" ground only — is rejected because:

- French Turf racing runs on soft ground for large parts of the year (Oct/Nov mean GA ~+0.40 spf)
- Provincial courses racing through winter may have only 15–20% of fixtures on genuinely good ground
- Small samples (n < 20) produce noisy, unreliable standards
- "Bon" at a watered summer meeting ≠ "Bon" on natural autumn ground — the description is an imprecise proxy for actual surface speed

---

## Iterative Calibration Process

The standard time calculation is an iterative loop. Each pass refines both the standards and the going allowances until convergence.

```
┌─────────────────────────────────────────────┐
│           ITERATIVE CALIBRATION             │
│                                             │
│  ┌──────────┐    ┌──────────┐    ┌───────┐ │
│  │  Seed    │───▶│Calculate │───▶│Adjust │ │
│  │Standards │    │   GAs    │    │ Times │ │
│  └──────────┘    └──────────┘    └───┬───┘ │
│       ▲                              │     │
│       │    ┌──────────────┐          │     │
│       └────│ Recalculate  │◀─────────┘     │
│            │  Standards   │                │
│            └──────────────┘                │
│                                             │
│  Repeat 3–5 passes until convergence        │
│  (standards change < 0.1s between passes)   │
└─────────────────────────────────────────────┘
```

### Step 1 — Seed Standards

Use the best-sampled course/distance/surface combinations (n > 100). Filter these to "Bon" or "Bon Léger" only. Calculate initial standards using the trimmed median (5% trim each tail).

These seed standards will be accurate for major courses and noisy for provincial ones — that's fine, the iteration corrects it.

### Step 2 — Calculate Initial Going Allowances

For every meeting in the dataset, calculate a going allowance:

```
GA_spf = median( (actual_time_i - standard_time_i) / distance_furlongs_i )
```

Where the median is taken across all races at that meeting. This gives one GA value per meeting per course per surface, expressed in seconds per furlong.

### Step 3 — Going-Adjust All Race Times

For every race in the dataset, normalise the winning time to the reference ground level:

```
adjusted_time = actual_time - (GA_spf × distance_furlongs)
```

This strips out the ground effect, leaving only the course/distance/surface characteristic and the race-quality component.

### Step 4 — Recalculate Standards

Using the full population of going-adjusted times (all goings, all seasons):

1. Group by course / distance / surface
2. Trim the fastest 5% and slowest 5%
3. Calculate the **trimmed median**
4. Optionally apply recency weighting (exponential decay, half-life ~2 years) to reflect track renovations

Sample sizes will be 2–3× larger than a good-ground-only filter.

### Step 5 — Iterate

Repeat Steps 2–4 until convergence. Typically 3–5 passes. Convergence criterion: no standard time changes by more than 0.1 seconds between consecutive passes.

---

## Going Allowance Calculation

### Per-Meeting GA

Each meeting gets a single GA value (or early/late split if going changes mid-meeting):

```
GA_spf = median( (winner_time_i - standard_time_i) / distance_furlongs_i )
```

**Requirements:**
- Minimum 3 races at a meeting to calculate a GA (fewer → use course seasonal average as fallback)
- Use median, not mean, to resist single-race outliers
- Express as seconds per furlong (spf)

### Early/Late Splits

If going changes mid-meeting (rain, drying, watering between races), split the card:

- Calculate separate GAs for the first and second halves of the card
- Only split if the two halves produce materially different GAs (> 0.1 spf difference)
- Tag each race with the appropriate split GA

### Soft Caps (Not Hard Clips)

Do not hard-clip GA values. Instead:

- **Investigate** any GA outside ±1.5 spf — this usually indicates a miscalibrated standard, not genuinely extreme ground
- If the GA is confirmed genuine, retain it
- If it's an artifact of a bad standard, fix the standard

---

## Speed Figure Calculation

### Formula

```
speed_figure = ((standard_time - adjusted_race_time) × LPS × scale) + base
```

Where:

| Variable | Definition |
|----------|-----------|
| `standard_time` | Going-adjusted standard for this course/distance/surface |
| `adjusted_race_time` | Winner time corrected for going: `actual_time - (GA × dist_f)` |
| `LPS` | Lengths per second at this distance |
| `scale` | Converts lengths to rating points (calibrated to your scale) |
| `base` | Baseline rating (e.g., 100 for an average winner) |

### Lengths Per Second (LPS)

```
LPS = distance_metres / (standard_time_seconds × 2.4)
```

Where 2.4m is the Jockey Club standard horse length. This must be calculated per course/distance/surface — not globally — because speed decreases with distance.

### Beaten-Distance Adjustment

For non-winners, convert official beaten distances to time:

```
horse_time = winner_time + (beaten_lengths / LPS)
```

Then apply the same speed figure formula to `horse_time`.

---

## Data Quality Requirements

### Minimum Sample Sizes

| Threshold | Status |
|-----------|--------|
| n ≥ 50 | Production-ready |
| 30 ≤ n < 50 | Usable with caution |
| 20 ≤ n < 30 | Provisional — flag in output |
| n < 20 | Exclude from production figures |

### Race Filters

Only include in standard time populations:

- ✅ Completed flat races
- ✅ Races with valid electronic timing
- ❌ Abandoned / void / walkovers
- ❌ Steeplechase / hurdle / cross-country
- ❌ Races with known timing failures

### Ongoing QA Checks

Run automatically on every data refresh:

| Check | Trigger |
|-------|---------|
| Monotonicity | Time must increase with distance within a course |
| Median-mean divergence | Alert if > 3% — indicates outlier contamination |
| Cross-course z-score | Flag any standard > 2.5σ from the distance-band peer mean |
| Mean GA drift | Per-course mean GA should be stable year-on-year; drift signals a changing track |
| GA calibration | Overall mean GA should be near zero after iterative calibration |
| Minimum sample size | Enforce n ≥ 20 for production, flag provisions |
| Going distribution | No single going type should dominate > 70% of a standard's sample |

---

## French Racing Specifics

### Distance Handling

French races are measured in metres. Store and match on **actual distance in metres**, not rounded furlong labels. Key distances:

| Metres | Furlongs (approx) | Common at |
|--------|-------------------|-----------|
| 1000m | 5.0f | Sprints |
| 1200m | 6.0f | Sprints |
| 1400m | 7.0f | Sprint/mile |
| 1600m | 8.0f | Mile |
| 1800m | 9.0f | Intermediate |
| 2000m | 10.0f | Intermediate |
| 2100m | 10.5f | Classic distance |
| 2400m | 12.0f | Derby distance |
| 2500m | 12.5f | Cup distance |
| 3000m | 15.0f | Staying |

Some courses run at non-standard distances (1050m, 1350m, 1650m, etc.). These must have their own standards — do not bucket 1300m and 1350m together.

### Going Descriptions

French going descriptions, from fastest to slowest:

| French | English Equivalent | Expected GA direction |
|--------|-------------------|----------------------|
| Bon Léger | Good to Firm | Slightly negative |
| Bon | Good | Near zero (reference) |
| Bon Souple | Good to Soft | Slightly positive |
| Souple | Soft | Positive |
| Très Souple | Very Soft | Strongly positive |
| Lourd | Heavy | Strongly positive |
| Collant | Holding/Sticky | Strongly positive |

Note: "Bon" on watered summer ground ≠ "Bon" on natural autumn ground. The per-meeting GA captures this distinction; the going description does not.

### All Weather Surfaces

French AW tracks use different synthetic surfaces. Where possible, tag each AW configuration by surface type:

| Surface | Tracks |
|---------|--------|
| Polytrack | Chantilly, Deauville |
| PSF (Piste en Sable Fibré) | Pau, others |
| Fibresand | Various provincial |

Different surfaces produce different speed characteristics and should ideally have separate standards.

---

## Calibration Validation

After completing the iterative calibration, validate with these checks:

1. **Mean GA ≈ 0**: Overall Turf mean GA should be within ±0.05 spf of zero. AW mean should be within ±0.03 spf.

2. **No systematic course bias**: Per-course mean GA should be within ±0.15 spf of zero for courses with 50+ meetings. Anything outside this range suggests the standard needs recalibration.

3. **Seasonal pattern intact**: Monthly Turf mean GA should show the expected pattern (higher in winter/autumn, lower in summer) but centered around zero rather than shifted positive.

4. **Year-on-year stability**: Annual mean GA should not drift by more than ±0.05 spf per year unless a track has been physically altered.

5. **No clipping required**: If the calibration is correct, no GA should routinely exceed ±1.5 spf. Persistent extremes indicate a standard time problem, not a going problem.
