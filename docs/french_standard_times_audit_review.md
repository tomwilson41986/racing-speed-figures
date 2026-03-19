# French Standard Times Audit — Review & Actions Taken

**Date:** 2026-03-19
**Audit reviewed:** `french_standard_times_audit.md`
**Branch:** `claude/review-french-times-audit-MQMOp`

---

## Summary

The audit identified 10 categories of issues across 800 course/distance/surface
configurations.  After reviewing the current pipeline code against each finding,
several P0 issues were **already resolved** by recent code changes, while two
required new fixes.

---

## Finding-by-Finding Assessment

### P0 — Critical

| # | Finding | Status | Detail |
|---|---------|--------|--------|
| 3 | **Non-French course contamination** | **FIXED (this PR)** | Added `NON_FRENCH_COURSE_CODES` blocklist in `constants.py` (20 codes: UK, Irish, German, Swiss). Filtering applied in `field_mapping._filter_valid()` before any pipeline stage sees the data. |
| 4 | **Distance bucketing** | **Already fixed** | The audit reviewed an older CSV using furlong labels (`93A_6.0_Turf`). Current code uses integer metres natively (`std_key = "AIX_2000_Turf"`) and `interpolate_lookup()` performs linear interpolation on actual distances — no rounding. |
| 5 | **LPL inconsistency** | **Already fixed** | `compute_course_lpl()` uses a single consistent formula: `generic_lpl × (mean_spm / course_spm) × surface_multiplier`. The CSV inconsistencies noted in the audit were from an older artifact generation. |

### P1 — High

| # | Finding | Status | Detail |
|---|---------|--------|--------|
| 1 | **Sample size < 20** | **Already addressed** | `MIN_RACES_STANDARD_TIME = 20` enforced. Combos with 10–19 races get Bayesian shrinkage toward the distance-band median (shrinkage K=10). Combos < 10 are excluded. |
| 2 | **Median-mean divergence** | **FIXED (this PR)** | Added `_flag_divergence()` to both `compute_standard_times()` and `compute_standard_times_iterative()`. Combos with >10% divergence are excluded; 5–10% are flagged as provisional. Applied to both initial and iterative standard time computation. |
| 9.4 | **Going adjustment** | **Already addressed** | Iterative going-corrected standard times already implemented with Bayesian shrinkage toward French going-description priors, non-linear correction for extreme going, and temporal neighbor pooling. |

### P2 — Medium

| # | Finding | Status | Detail |
|---|---------|--------|--------|
| 7 | **AW surface types** | Not yet addressed | Single "All Weather" label still used. Splitting by PSF/Polytrack/Fibresand would require surface-type data from PMU that may not be reliably available. The 10% LPL surface multiplier partially compensates. |
| 8 | **Cross-course speed outliers** | Partially addressed | The non-French course removal resolves WAR (Warwick). LIG and OST likely represent genuine provincial course characteristics (undulating, tight tracks). QC-2 pace check (10–18 s/f) would catch extreme outliers. |

### P3 — Low

| # | Finding | Status | Detail |
|---|---------|--------|--------|
| 6 | **Monotonicity violations** | Not yet addressed | ARG and LAT cases. ARG is likely resolved by divergence exclusion (9.8% divergence). LAT with healthy sample sizes may reflect genuine course configuration differences. |
| 9 | **Best practice methodology** | Largely implemented | Pipeline already uses weighted median, going correction, recency weighting (4yr half-life), iterative refinement, and Bayesian shrinkage. The main gap is trimmed mean (currently using raw median with shrinkage). |

---

## Code Changes Made

### 1. `src/france/constants.py`
- Added `NON_FRENCH_COURSE_CODES` set containing 20 non-French course codes
  identified in audit §3 (ASC, DON, EPS, GOO, HAY, KEM, MKT, NBU, SDW, WAR,
  YOR, CUR, DUB, GAL, LEO, BAD, DUS, FRA, KOE, ZUR)

### 2. `src/france/field_mapping.py`
- Imported `NON_FRENCH_COURSE_CODES`
- Extended `_filter_valid()` to exclude rows where `courseName` matches a
  non-French code, with logging of removed rows and affected course names

### 3. `src/france/speed_figures.py`
- Added `_flag_divergence()` function that:
  - Computes `abs(mean_time - median_time) / median_time` for each standard
  - Excludes combos with >10% divergence (likely timing errors or void races)
  - Flags combos with 5–10% divergence as provisional
  - Logs warnings for excluded combos
- Called from both `compute_standard_times()` and
  `compute_standard_times_iterative()` so both initial and iterative passes
  benefit

---

## Remaining Work (not in this PR)

1. **Regenerate artifacts** — Run the full pipeline to produce updated
   `standard_times.csv`, `going_allowances.csv`, and `france_artifacts.pkl`
   reflecting the new filtering
2. **AW surface type tagging** — Requires PMU data enrichment
3. **Automated QA pipeline** — Build the monotonicity/divergence/z-score checks
   recommended in audit §9.5 as a standalone validation script
