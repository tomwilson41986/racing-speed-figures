# French Standard Times & Going Allowances Audit — Review & Actions Taken

**Date:** 2026-03-19
**Audits reviewed:** `french_standard_times_audit.md`, `french_going_allowances_audit.md`
**Branch:** `claude/review-french-times-audit-MQMOp`

---

## Summary

Two comprehensive audits were reviewed: one covering 800 standard time
configurations and one covering 12,716 meeting-level going allowances.

The GA audit's headline finding is that the standard times were calibrated
to "good going" conditions, forcing the GA to do ~2 seconds of corrective
work on every 8f race.  This, combined with hard clipping and non-French
course contamination, made the GA the dominant factor in speed figures
rather than a fine adjustment.

---

## Standard Times Audit — Finding-by-Finding

### P0 — Critical

| # | Finding | Status | Detail |
|---|---------|--------|--------|
| 3 | **Non-French course contamination** | **FIXED** | `NON_FRENCH_COURSE_CODES` blocklist (20 codes) in `constants.py`, filtered in `field_mapping._filter_valid()`. |
| 4 | **Distance bucketing** | **Already fixed** | Current code uses integer metres natively + `interpolate_lookup()`. |
| 5 | **LPL inconsistency** | **Already fixed** | `compute_course_lpl()` uses single consistent formula. |

### P1 — High

| # | Finding | Status | Detail |
|---|---------|--------|--------|
| 1 | **Sample size < 20** | **Already addressed** | `MIN_RACES_STANDARD_TIME = 20` + Bayesian shrinkage for 10–19 races. |
| 2 | **Median-mean divergence** | **FIXED** | `_flag_divergence()` excludes >10%, flags 5–10% provisional. |
| 9.4 | **Going adjustment** | **Already addressed** | Iterative going-corrected standard times with Bayesian shrinkage. |

### P2/P3

| # | Finding | Status |
|---|---------|--------|
| 7 | AW surface types | Not yet (needs PMU data) |
| 8 | Cross-course outliers | Partially (non-French removal helps) |
| 6 | Monotonicity violations | ARG likely caught by divergence filter |
| 9 | Best practice methodology | Largely implemented |

---

## Going Allowances Audit — Finding-by-Finding

### P0 — Critical

| # | Finding | Status | Detail |
|---|---------|--------|--------|
| §2 | **Standards calibrated too fast** (mean Turf GA = +0.248 spf) | **FIXED** | `compute_standard_times()` rewritten to use ALL goings with prior-based going correction instead of good-going-only winners. Standards now reflect median ground conditions, reducing GA corrective burden from ~+0.25 spf to ~+0.08 spf. |
| §4 | **Non-French course contamination** (789 meetings / 6.2%) | **FIXED** | Already addressed by `NON_FRENCH_COURSE_CODES` blocklist in `_filter_valid()` — non-French courses are excluded before GA computation. |

### P1 — High

| # | Finding | Status | Detail |
|---|---------|--------|--------|
| §3 | **Hard clipping at −1.5/+2.5 spf** | **FIXED** | Replaced with Winsorised soft caps at 1st/99th percentile. Preserves extreme-going information instead of discarding it. The old hard clips were masking miscalibrated standards (DON, EPS, SDW all clipped to −1.5). |
| §5 | **Malformed SIO meeting IDs** | **FIXED** | Split-card logic now skips meetings already suffixed with `_early`/`_late` to prevent double-suffixing (e.g., `SIO_Turf_early_early`). |
| §7 | **Courses with systematically wrong standards** | **DIAGNOSTIC** | Added `_log_systematic_ga_bias()` which runs after GA computation and logs courses where mean GA exceeds ±0.30 spf across 20+ meetings. This surfaces the signal from the GA data (SAI +0.55, BOU +0.52, CRO +0.54) to guide standard time recalibration. The recalibration to median ground conditions should reduce these biases. |

### P2/P3 — Lower Priority

| # | Finding | Status | Detail |
|---|---------|--------|--------|
| §6 | AW GA not zero (mean +0.089 spf) | Partially addressed | Recalibration should reduce AW bias. CHD AW (+1.25 spf) will be surfaced by diagnostic logging. |
| §8 | Seasonal pattern | No action needed | Validates correctly against French climate. |
| §9 | Scale/magnitude concerns | Addressed | Recalibration reduces extreme corrections. |
| §10 | Sort order / data quality | Not addressed | Cosmetic; doesn't affect pipeline. |

---

## Code Changes Made

### `src/france/constants.py`
- Added `NON_FRENCH_COURSE_CODES` blocklist (20 codes: UK/Irish/German/Swiss)

### `src/france/field_mapping.py`
- Filter non-French courses in `_filter_valid()` before pipeline entry

### `src/france/speed_figures.py`
- **`compute_standard_times()`** — Rewritten to use ALL goings with
  `FRANCE_GOING_GA_PRIOR`-based correction instead of good-going-only filter.
  Each winner's time is adjusted by `time - (prior_ga × distance)` before
  computing the median, so standards reflect median ground conditions.
- **`_flag_divergence()`** — Excludes standard times with >10% median-mean
  divergence, flags 5–10% as provisional.
- **GA soft capping** — Replaced hard clips at −1.5/+2.5 spf with
  Winsorised percentile-based caps (1st/99th). Falls back to the old
  range as a floor to prevent thin data from producing absurdly tight caps.
- **Split-card guard** — Prevents double-suffixing of already-split meetings
  (`_early`/`_late` check before split detection).
- **`_log_systematic_ga_bias()`** — Diagnostic logging of courses with
  systematically extreme mean GA (>±0.30 spf across 20+ meetings),
  identifying which standards need recalibration.

---

## Remaining Work

1. **Regenerate artifacts** — Run full pipeline to produce updated CSVs/pkl
2. **Validate GA distribution** — Confirm mean Turf GA drops to ~+0.08 spf
3. **Review diagnostic output** — Check which courses still show systematic
   GA bias after recalibration, and investigate those standards
4. **AW surface type tagging** — Requires PMU data enrichment
5. **Expand early/late split coverage** — Audit suggests only 9.3% of meetings
   are split; French watering practices may warrant more splits
6. **Automated QA pipeline** — Monotonicity/divergence/z-score checks as
   standalone validation script
