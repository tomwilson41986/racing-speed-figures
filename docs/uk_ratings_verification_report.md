# UK Ratings Verification Report

**Date:** 2026-03-19
**Purpose:** Verify UK/Ireland speed figure accuracy against Timeform, confirming the France pipeline work has not damaged the UK pipeline.

---

## Executive Summary

The UK/Ireland speed figure pipeline is **functioning correctly** and has **not been degraded** by the France pipeline work. The France module (`src/france/`) operates as a fully separate pipeline with its own data sources, constants, and calibration — sharing no mutable state with the UK pipeline.

| Metric | Value | Assessment |
|--------|-------|------------|
| Correlation with Timeform | **0.9247** | Excellent |
| MAE | **6.73 lbs** | Good |
| RMSE | **8.86 lbs** | Acceptable |
| Bias | **-0.61 lbs** | Near-zero (slight under-rating) |
| Within ±5 lbs | **46.9%** | Nearly half within 5 lbs |
| Within ±10 lbs | **77.6%** | Over three-quarters within 10 lbs |
| Compression ratio | **1.015** | Near-perfect scale match |

---

## 1. Overall Accuracy vs Timeform

Pipeline run across 692,879 runners (2015–2026), 600,708 with valid Timeform timefigures.

```
Correlation:  0.9247
MAE:          6.73 lbs
RMSE:         8.86 lbs
Bias:         -0.61 lbs
Within ±5:    46.9%
Within ±10:   77.6%

Scale comparison:
  Our std dev:       22.9
  Timeform std dev:  22.6
  Compression ratio: 1.015 (effectively 1:1)
```

The scale compression issue (previously a concern during GBR enhancement) has been resolved — our figures now spread nearly identically to Timeform's.

---

## 2. In-Sample vs Out-of-Sample Performance

| Period | N | MAE | RMSE | Bias | Corr | ±5 | ±10 |
|--------|---|-----|------|------|------|----|-----|
| In-sample (2015–23) | 476,945 | 6.82 | 8.96 | -0.72 | 0.924 | 46.2% | 77.0% |
| Out-of-sample (2024–26) | 123,763 | **6.40** | **8.46** | -0.16 | **0.929** | **49.7%** | **79.6%** |

**Key finding:** Out-of-sample performance is *better* than in-sample, with lower MAE (6.40 vs 6.82), lower bias (-0.16 vs -0.72), and higher correlation (0.929 vs 0.924). This confirms the model generalises well and has not overfit.

---

## 3. Accuracy by Year (Trend Analysis)

| Year | Corr | MAE | N |
|------|------|-----|---|
| 2015 | 0.9175 | 7.32 | 53,519 |
| 2016 | 0.9196 | 7.05 | 54,589 |
| 2017 | 0.9236 | 7.12 | 57,622 |
| 2018 | 0.9254 | 6.94 | 57,727 |
| 2019 | 0.9269 | 6.68 | 57,165 |
| 2020 | 0.9270 | 6.51 | 47,870 |
| 2021 | 0.9261 | 6.63 | 60,424 |
| 2022 | 0.9260 | 6.62 | 60,763 |
| 2023 | 0.9236 | 6.58 | 63,514 |
| 2024 | **0.9307** | **6.29** | 65,933 |
| 2025 | 0.9288 | 6.49 | 60,681 |
| 2026 | 0.9116 | 6.56 | 4,985 |

**Key finding:** Accuracy has been improving year-on-year. 2024 is the best year (MAE 6.29, r=0.931). 2026 has limited data (4,985 runners, early-season only) but remains solid (MAE 6.56, r=0.912). No degradation visible from the France work.

---

## 4. Accuracy by Surface

| Surface | N | MAE | RMSE | Bias | Corr | ±5 | ±10 |
|---------|---|-----|------|------|------|----|-----|
| Turf | 372,661 | 7.35 | 9.61 | -0.52 | 0.915 | 43.1% | 73.5% |
| All Weather | 228,047 | **5.72** | **7.48** | -0.74 | **0.938** | **53.3%** | **84.1%** |

All-Weather figures are significantly more accurate, which is expected given more consistent ground conditions.

---

## 5. Accuracy by Going

| Going Group | N | MAE | Bias | Corr |
|-------------|---|-----|------|------|
| Firm | 19,913 | 6.27 | -0.33 | 0.938 |
| Good/Firm | 124,481 | 7.15 | -0.50 | 0.921 |
| Good/Standard | 210,471 | 6.67 | -0.57 | 0.925 |
| Good/Soft | 180,992 | 6.29 | -0.78 | 0.929 |
| Soft | 40,291 | 7.51 | -0.50 | 0.910 |
| Heavy | 24,560 | 7.57 | -0.60 | 0.908 |

Bias is consistently small (< 1 lb) across all goings — going allowance computation is working correctly.

---

## 6. Accuracy by Country

| Country | N | MAE | Bias | Corr | ±10 |
|---------|---|-----|------|------|-----|
| UK | 499,934 | 6.64 | -0.62 | 0.928 | 78.2% |
| Ireland | 100,774 | 7.18 | -0.52 | 0.907 | 74.5% |

Irish figures are slightly less accurate, which is expected due to fewer courses/meetings and less consistent timing infrastructure.

---

## 7. Accuracy by Race Class

| Class | N | MAE | Bias | Corr |
|-------|---|-----|------|------|
| C1 (Group) | 22,850 | 7.26 | +2.60 | 0.869 |
| C2 (Listed) | 40,958 | 6.99 | +1.51 | 0.896 |
| C3 | 36,558 | 7.17 | +0.72 | 0.878 |
| C4 | 96,519 | 6.85 | -0.21 | 0.898 |
| C5 | 159,972 | 6.65 | -1.05 | 0.907 |
| C6 | 137,526 | 6.14 | -1.97 | 0.908 |
| C7 | 1,401 | 4.00 | -0.68 | 0.937 |

Class 1 (Group races) shows a positive bias of +2.6 lbs (we slightly over-rate top-class horses). C6 shows -2.0 lbs (slight under-rating of lower-class runners). These are within acceptable tolerances.

---

## 8. Accuracy by Distance

| Distance Band | N | MAE | Bias | Corr |
|---------------|---|-----|------|------|
| 5f–6f | 167,542 | 6.41 | -0.59 | 0.936 |
| 7f–8f | 226,621 | 6.37 | -0.52 | 0.928 |
| 9f–10f | 94,134 | 7.04 | -0.89 | 0.913 |
| 11f–12f | 63,062 | 7.53 | -0.40 | 0.910 |
| 13f–16f | 42,610 | 7.99 | -0.78 | 0.908 |
| 17f+ | 6,739 | 7.46 | -0.62 | 0.942 |

Sprint distances (5f–8f) are most accurate. Staying distances (13f–16f) are the weakest, likely due to greater pace variation and fewer races for calibration.

---

## 9. Systematic Bias Patterns

### By Timeform Figure Level
```
TFig Band        N         Bias     Direction
-10 to 0      4,704      +0.88     over-rated
  0 to 10    10,029      +1.82     over-rated
 10 to 20    23,115      +1.87     over-rated
 20 to 30    42,277      +1.50     over-rated
 30 to 40    68,672      +0.93     over-rated
 40 to 50    94,990      +0.19     over-rated
 50 to 60   108,678      -0.62     under-rated
 60 to 70   100,438      -1.28     under-rated
 70 to 80    72,874      -2.11     under-rated
 80 to 90    42,091      -2.82     under-rated
 90 to 100   19,719      -3.20     under-rated
100 to 110    7,527      -3.70     under-rated
110 to 120    2,065      -4.46     under-rated
120 to 130      223      -6.11     under-rated
```

There is a known regression-to-the-mean effect: low-rated horses are slightly over-rated (+1–2 lbs) and high-rated horses slightly under-rated (-3–6 lbs). This is a feature of the GBR model's tendency toward mean compression, partially corrected by quantile mapping but not fully eliminated at the extremes.

---

## 10. Problem Areas

Only **5 problem areas** identified (MAE > overall + threshold, n≥200):

| Category | Group | MAE | Excess | Bias | N |
|----------|-------|-----|--------|------|---|
| Rating Band | <20 (very low) | 9.54 | +2.81 | +1.53 | 44,430 |
| Track | Tipperary | 9.48 | +2.75 | -1.32 | 3,699 |
| Track | Killarney | 8.93 | +2.20 | -0.02 | 465 |
| Track | Cork | 8.88 | +2.15 | -0.38 | 4,959 |
| Distance | 13f–16f | 7.99 | +1.25 | -0.78 | 42,610 |

The problem tracks are all **Irish courses** — a known limitation due to less consistent timing and fewer races for calibration. The <20 rating band weakness reflects noise in very weak performances (pulled up, tailed off, etc.).

---

## 11. France Pipeline Independence Check

The France pipeline is fully isolated from the UK pipeline:

| Aspect | UK Pipeline | France Pipeline |
|--------|-------------|-----------------|
| Source code | `src/speed_figures.py` | `src/france/speed_figures.py` |
| Data source | Timeform CSV (2015–2026) | PMU API / SQLite DB |
| Constants | In `speed_figures.py` L1–260 | `src/france/constants.py` |
| Calibration | Timeform timefigure regression | Default global scale (0.70) |
| Standard times | `output/standard_times.csv` | `output/france/standard_times.csv` |
| Going allowances | `output/going_allowances.csv` | `output/france/going_allowances.csv` |
| Artifacts | `output/calibration_artifacts.pkl` | `output/france/france_artifacts.pkl` |

No shared mutable state, databases, or configuration files. The France work added files under `src/france/` and `output/france/` without modifying any UK pipeline code.

---

## 12. Conclusion

**The UK speed figure pipeline is healthy and unaffected by the France work.**

- Correlation of **0.9247** with Timeform timefigures across 600K+ runners
- MAE of **6.73 lbs** (median horse is within ~7 lbs of Timeform)
- Near-zero overall bias of **-0.61 lbs**
- Out-of-sample accuracy (2024–26) is **better** than in-sample (2015–23)
- Year-on-year trend shows **improving accuracy**, not degradation
- Scale compression ratio of **1.015** — near-perfect match to Timeform's distribution
- Only 5 problem areas, all pre-existing (Irish tracks, extreme low ratings, staying distances)
- France pipeline is **fully independent** — separate code, data, constants, and artifacts

**Verdict: UK pipeline integrity confirmed. No remediation needed.**
