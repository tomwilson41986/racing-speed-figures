# How close can our figures get to Timeform, and should they?

Fitted and measured against the full Timeform archive: **639,230 paired runners,
73,210 races, 2015–2026.** Written up because the answer is a negative result
that is expensive to rediscover, and because the obvious next step — tuning
harder against Tfig — makes the figures worse.

## Where we stand

| measure | value |
|---|---|
| per-runner MAE vs Tfig | 6.67 |
| **top figure per race vs Timeform's top Tfig** | **6.83** (within 3: 29.6%) |
| winner only (no beaten-lengths step) | 6.84 |
| within-race scatter about the race mean | 1.50 |
| whole-race level shift | 8.78 sd |

Two things follow immediately. The top-per-race and winner-only numbers are
identical, so the beaten-lengths stage contributes nothing to this gap — it is
entirely the race-level anchor. And within a race we are accurate to 1.5 lb, so
**the whole gap is the race level and nothing else.**

All Weather (5.88) beats Turf (7.39), which is what you would expect if the
residual is going and pace.

## Why 2–3 is not reachable against Tfig

### The ceiling, measured

| | MAE | within 3 |
|---|---|---|
| now | 6.83 | 29.6% |
| perfect same-card going allowance | 6.47 | 31.1% |
| **oracle** meeting mean (uses the answer) | 5.56 | 35.7% |

Even cheating at meeting level lands at 5.56. Reaching 2–3 requires the
**per-race** level, exactly.

### Nothing observable predicts the per-race level

Fitting on years ≤2023 and testing 2024–26, every correction to the residual
race offset is negative out of sample: course+distance −2.8%, going −6.4%, class
−0.1%, distance band −0.2%. Those residual offsets split-half correlate −0.30 —
anti-predictive. No production-available feature correlates above |r| = 0.12
(distance, class, field size, prize, pre-race ratings, going allowance). It is a
constant ~0.85% of race time at every distance, is not reduced by more races per
card, and does not drift through the card.

Timeform's own pace read (`ipHintsOverallPace`) explains **0.7%**.

### The runners do not think the race is mis-levelled

If a race were 10 lb too high, every horse in it would look 10 lb better than
its own history. Across 70,550 races the correlation between the field's mean
deviation-from-own-form and the true race offset is **+0.133** (1.8% of
variance), unchanged for horses with three or more prior runs.

### Timeform disagrees with itself by more

Timeform publish two numbers per runner. Their time figure sits **13.90 lb** from
their own performance rating at race level (712,910 runners, 80,574 races),
stable at 13.3–14.5 every year for twelve years. We sit 8.78 from their Tfig.
Against their performance rating we behave almost exactly as their own Tfig does
(12.38 vs 11.94), with a *tighter* race-level spread (11.98 vs 13.98).

A time figure measures how fast the race was run. A performance rating measures
how good the horses are. Those legitimately differ — which is why Timeform sell
both, and why they decline to publish a Tfig for 43% of runners.

## Chasing Tfig would make our figures worse

Judged on a yardstick neither figure is derived from — the same horse's rating
at its **next** run — across 571,213 runs and 58,704 horses:

| predicting next-run… | our figure | Timeform Tfig |
|---|---|---|
| TFR (performance rating) | **+0.5951** | +0.5820 |
| Tfig (Timeform's own next number) | **+0.5345** | +0.5259 |
| figure on our scale | **+0.5503** | +0.5304 |

Our figure predicts Timeform's *own next Tfig* better than Timeform's current
Tfig does. The 6.83 gap is their race-to-race movement, and we are on the better
side of it. Closing it means importing that movement.

## What the reconciliation should therefore report

Not "we are 6.8 off Timeform". Useful measures instead:

- **86.4%** of races have the same horse topping both lists.
- Within-race accuracy of **1.5 lb** — we rank and space a field tightly.
- Race-level agreement as a **band**, not a target: half of races land within
  4.6 lb, three quarters within 10.

## What is left to gain, and what is not

Gained and shipped (six year-holdout folds, improving in all six): splitting the
course×distance offset by class and by season, MAE 8.459 → 8.085 out of sample,
within-10 67.4% → 69.3%. Shrinkage is already at its optimum (k = 50). A third
split (× going) was tested and made it fractionally worse.

Not available: any further race-level correction from time data. If a number
accurate to 2–3 on *how good the horse is* is wanted, that is a performance
rating anchored on form, not a time figure — a different model, which is
precisely why Timeform publish both.
