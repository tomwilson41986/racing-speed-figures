# Ratings Accuracy Review — July 2026

How accurate have the ratings been so far, and how is accuracy now surfaced in
the daily emails. This refreshes and extends the March verification
(`docs/uk_ratings_verification_report.md`) and adds France and a forward-looking
plan. Reproduce any figure with `python scripts/audit_model_accuracy.py`
(reads `output/speed_figures.csv`, ground truth = Timeform `timefigure`).

---

## 1. Headline

| Jurisdiction | Ground truth | Correlation | MAE | Assessment |
|---|---|---|---|---|
| UK / Ireland | Timeform timefigure (600,708 runners, 2015–2026) | **0.925** | **6.7 lb** | Strong, stable, generalising |
| France | none available (no Timeform) | — | — | Assessed by distribution-match + QA (§3) |

The UK/IRE figure sits within ~7 lb of Timeform for the median runner, with
near-zero bias (**-0.6 lb**) and a scale that matches Timeform's spread
(compression ratio 1.015). **Out-of-sample (2024–26) is better than in-sample**
(MAE 6.40 vs 6.82, r 0.929 vs 0.924) — the model is not over-fit — and accuracy
has improved year-on-year (best year 2024: MAE 6.29, r 0.931).

These are the numbers now shown live in the **accuracy panel** at the head of
every email (§4).

---

## 2. UK / Ireland — accuracy by dimension

Summarised from the full audit (`scripts/audit_model_accuracy.py`,
`output/audit_report.txt`):

- **Surface** — All-Weather (MAE 5.7, r 0.938) is materially tighter than Turf
  (MAE 7.3, r 0.915), as expected from more consistent ground.
- **Going** — bias is < 1 lb across Firm → Heavy, i.e. the going-allowance stage
  is doing its job; softest/heaviest ground is a touch noisier (MAE ~7.5).
- **Country** — UK MAE 6.6 (r 0.928) vs Ireland MAE 7.2 (r 0.907). Irish tracks
  are the weakest area (below), reflecting fewer meetings and less consistent
  timing.
- **Class** — small positive bias at Group level (+2.6 lb, we slightly over-rate
  the very best) and small negative bias in the lowest classes (-2.0 lb).
- **Distance** — sprints (5–8f) tightest (MAE ~6.4); staying trips (13–16f) the
  weakest (MAE ~8.0) from greater pace variation.
- **Regression to the mean** — a known residual: very high figures are shaded
  down a few pounds and very low figures nudged up, from the GBR stage's mean
  compression (partly corrected by quantile mapping).

### Known weak spots (unchanged, pre-existing)
Irish tracks (Tipperary, Cork, Killarney), staying distances (13–16f), and
extreme low ratings (< 20). These are data/timing limitations, not regressions —
the **sectional layer (below) directly targets the pace-variation weakness** at
middle/staying distances by adjusting on finishing speed vs par.

---

## 3. France — how accuracy is judged without Timeform

France has no Timeform equivalent, so accuracy is assessed two ways, both already
in the repo:

1. **Distribution-match to the UK scale** — the French figures are calibrated so
   their distribution lines up with the (Timeform-anchored) UK output. The daily
   QA lives in `output/france_qa/` and `docs/qa_french_figures_*.md`.
2. **Cross-pipeline audit** — `docs/AUDIT_FRENCH_VS_UK_RATINGS.md` (Mar 2026) is
   the reference. Its key finding: the French pipeline historically **ran hot**
   (figures inflated) where it stopped short of the UK's calibration/GBR stages;
   the fix is to carry French raw figures through the same
   calibration → compression → quantile-mapping. Treat French figures as
   **provisional relative to UK/IRE** until a like-for-like benchmark exists.

**Recommended next step for France:** benchmark the French `figure_final` against
official French ratings (valeur) and/or Racing Post France ratings for a sample
of races, to get a France-native MAE/bias comparable to the UK number.

---

## 4. Recurring accuracy panel (now live in the emails)

`src/reporting/accuracy.py` computes the model-vs-Timeform headline
(`corr`, `mae`, `bias`, `within ±10 lb`, `n`) from `output/speed_figures.csv`
and caches `output/accuracy/panel_latest.json`. The combined live email and the
day-after email both render this as a small panel at the top. It is **refreshed
every run** in `daily_combined_ratings.yml` (`python -m src.reporting.accuracy`)
so the number tracks the live pipeline, not a stale snapshot.

Seed value (last verified): r 0.925 · MAE 6.7 lb · within ±10 lb 78% · 601k runners.

---

## 5. Forward-looking predictiveness (the next measurement)

The vs-Timeform number measures how well a figure *describes* a run. The more
valuable question for punting is whether a figure *predicts the next* run. We now
retain every daily rating (`data/live/`, `data/france_live/`, `output/uk_audit/`),
which is the substrate to measure:

- **Next-time-out strike/place rate** of horses whose latest figure tops their
  race, and the **figure edge** vs the eventual result;
- **Sectional-flagged horses** (the day-after upgrades) followed to their next
  start — the direct test of whether the sectional layer adds signal.

This is a cross-race join (horse identity → subsequent result) rather than a
single-file metric; it is the recommended follow-up workstream and will feed a
second, predictive stat into the panel once enough post-launch data has
accumulated.

---

## 6. Reproduce

```bash
python src/speed_figures.py            # regenerates output/speed_figures.csv
python scripts/audit_model_accuracy.py # full breakdown -> output/audit_report.txt
python -m src.reporting.accuracy       # refresh the in-email panel JSON
```
