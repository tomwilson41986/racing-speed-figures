"""
Focused test: optimize the best distribution mapping approaches from round 1.

Key findings from round 1:
- CDF transfer (KDE): Best 100+ MAE (6.73), best corr (0.9210), 100+ bias -3.75
- PCHIP quantile mapping n=50: Best 100+ bias (-3.33), corr 0.9208, 100+ MAE 6.86
- Isotonic/binned approaches barely moved the needle
- The residual bias is due to GBR many-to-one clipping at extremes

This script tests:
1. PCHIP with fewer quantiles (20, 30, 40) for more aggressive tail correction
2. Tail-enriched quantile grids (more resolution at extremes)
3. CDF transfer + residual tail bias correction
4. Hybrid: PCHIP body + extrapolated tail correction
"""
import os, sys
import numpy as np
import pandas as pd
from scipy.interpolate import PchipInterpolator
from scipy.stats import gaussian_kde

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "output")


def evaluate(pred, truth, label, detail=True):
    err = pred - truth
    mae = np.abs(err).mean()
    bias = err.mean()
    corr = np.corrcoef(pred, truth)[0, 1]
    print(f"\n  {label}")
    print(f"    Overall:  MAE={mae:.3f}  Bias={bias:+.3f}  r={corr:.4f}")
    if detail:
        bands = [(-999, 20), (20, 40), (40, 60), (60, 80), (80, 100), (100, 120), (120, 999)]
        labels = ["<20", "20-40", "40-60", "60-80", "80-100", "100-120", "120+"]
        for (lo, hi), bl in zip(bands, labels):
            m = (truth >= lo) & (truth < hi)
            if m.sum() < 10:
                continue
            be = err[m]
            print(f"    {bl:>7}: n={m.sum():>6,}  MAE={np.abs(be).mean():.2f}  Bias={be.mean():+.2f}")
    return {"mae": mae, "bias": bias, "corr": corr}


def pchip_quantile_mapping(fit_pred, fit_truth, pred_all, quantile_levels):
    """PCHIP quantile mapping with custom quantile grid."""
    pred_q = np.percentile(fit_pred, quantile_levels)
    tf_q = np.percentile(fit_truth, quantile_levels)

    # Remove duplicates in pred_q (can happen at extremes)
    unique_mask = np.concatenate([[True], np.diff(pred_q) > 0.001])
    pred_q = pred_q[unique_mask]
    tf_q = tf_q[unique_mask]

    mapper = PchipInterpolator(pred_q, tf_q, extrapolate=True)
    return mapper(pred_all)


def cdf_transfer(fit_pred, fit_truth, pred_all, bw=0.5, n_grid=2000):
    """CDF-to-CDF transfer via KDE."""
    kde_pred = gaussian_kde(fit_pred, bw_method=bw)
    kde_truth = gaussian_kde(fit_truth, bw_method=bw)

    grid_min = min(fit_pred.min(), fit_truth.min()) - 20
    grid_max = max(fit_pred.max(), fit_truth.max()) + 20
    grid = np.linspace(grid_min, grid_max, n_grid)
    dx = grid[1] - grid[0]

    cdf_pred = np.cumsum(kde_pred(grid)) * dx
    cdf_pred = cdf_pred / cdf_pred[-1]
    cdf_truth = np.cumsum(kde_truth(grid)) * dx
    cdf_truth = cdf_truth / cdf_truth[-1]

    # Ensure monotonicity for inverse
    unique_mask = np.concatenate([[True], np.diff(cdf_truth) > 0])
    cdf_truth_u = cdf_truth[unique_mask]
    grid_u = grid[unique_mask]

    pred_to_cdf = PchipInterpolator(grid, cdf_pred, extrapolate=True)
    cdf_to_truth = PchipInterpolator(cdf_truth_u, grid_u, extrapolate=True)

    p_values = np.clip(pred_to_cdf(pred_all), 0.0001, 0.9999)
    return cdf_to_truth(p_values)


def get_pre_expansion_data():
    """Run pipeline with expand_scale disabled to get raw GBR output."""
    import speed_figures as sf

    def noop_expand(df):
        print("\n  [TEST] Skipping expansion for testing.")
        return df

    original = sf.expand_scale
    sf.expand_scale = noop_expand
    all_figs, _, _ = sf.run_pipeline()
    sf.expand_scale = original

    valid = all_figs[
        all_figs["timefigure"].notna()
        & (all_figs["timefigure"] != 0)
        & all_figs["timefigure"].between(-200, 200)
        & all_figs["figure_calibrated"].notna()
    ].copy()
    return valid


def main():
    print("=" * 70)
    print("DISTRIBUTION MAPPING — FOCUSED OPTIMIZATION")
    print("=" * 70)

    valid = get_pre_expansion_data()
    print(f"\nValid rows: {len(valid):,}")

    pred_all = valid["figure_calibrated"].values.astype(float)
    truth_all = valid["timefigure"].values.astype(float)
    surfaces = valid["raceSurfaceName"].values
    train_mask = valid["source_year"].values <= 2023
    oos_mask = valid["source_year"].values > 2023

    # ── Baseline ──
    print("\n" + "=" * 70)
    print("BASELINE (no expansion)")
    evaluate(pred_all, truth_all, "All")
    evaluate(pred_all[oos_mask], truth_all[oos_mask], "OOS")

    # Helper to apply per-surface mapping
    def apply_per_surface(mapping_fn, **kwargs):
        result = pred_all.copy()
        for surface in np.unique(surfaces):
            sm = surfaces == surface
            fit_pred = pred_all[sm & train_mask]
            fit_truth = truth_all[sm & train_mask]
            if len(fit_pred) < 1000:
                continue
            result[sm] = mapping_fn(fit_pred, fit_truth, pred_all[sm], **kwargs)
        return result

    # ── Test 1: PCHIP with varying quantile counts ──
    for nq in [20, 30, 40, 50, 75, 100]:
        print("\n" + "=" * 70)
        print(f"PCHIP uniform quantiles (n={nq})")
        ql = np.linspace(0, 100, nq + 1)
        mapped = apply_per_surface(pchip_quantile_mapping, quantile_levels=ql)
        evaluate(mapped, truth_all, "All")
        evaluate(mapped[oos_mask], truth_all[oos_mask], "OOS")

    # ── Test 2: Tail-enriched quantile grids ──
    # More quantile points at the tails where the non-linear compression is worst
    print("\n" + "=" * 70)
    print("PCHIP tail-enriched grid (dense at 0-5% and 95-100%)")
    # 0-5%: every 0.25%, 5-95%: every 2%, 95-100%: every 0.25%
    ql_tail = np.concatenate([
        np.arange(0, 5, 0.25),      # 20 points at bottom
        np.arange(5, 95, 2),         # 45 points in middle
        np.arange(95, 100.01, 0.25)  # 21 points at top
    ])
    mapped = apply_per_surface(pchip_quantile_mapping, quantile_levels=ql_tail)
    evaluate(mapped, truth_all, "All")
    evaluate(mapped[oos_mask], truth_all[oos_mask], "OOS")

    print("\n" + "=" * 70)
    print("PCHIP tail-enriched grid v2 (even denser at extremes)")
    ql_tail2 = np.concatenate([
        np.arange(0, 2, 0.1),        # 20 points at very bottom
        np.arange(2, 10, 0.5),       # 16 points at lower tail
        np.arange(10, 90, 2),        # 40 points in middle
        np.arange(90, 98, 0.5),      # 16 points at upper tail
        np.arange(98, 100.01, 0.1),  # 21 points at very top
    ])
    mapped = apply_per_surface(pchip_quantile_mapping, quantile_levels=ql_tail2)
    evaluate(mapped, truth_all, "All")
    evaluate(mapped[oos_mask], truth_all[oos_mask], "OOS")

    # ── Test 3: CDF transfer with varying bandwidths ──
    for bw in [0.2, 0.3, 0.5]:
        print("\n" + "=" * 70)
        print(f"CDF transfer (KDE bw={bw})")
        mapped = apply_per_surface(cdf_transfer, bw=bw)
        evaluate(mapped, truth_all, "All")
        evaluate(mapped[oos_mask], truth_all[oos_mask], "OOS")

    # ── Test 4: CDF transfer + residual tail bias correction ──
    def cdf_plus_tail_correction(fit_pred, fit_truth, pred_all_s, bw=0.3):
        """CDF transfer followed by a small residual bias correction at tails."""
        mapped = cdf_transfer(fit_pred, fit_truth, pred_all_s, bw=bw)

        # Compute residual bias in upper and lower tail on training data
        # Use the fit data to measure remaining bias after CDF transfer
        mapped_fit = cdf_transfer(fit_pred, fit_truth, fit_pred, bw=bw)

        # Upper tail correction: for predictions above 95th percentile
        p95 = np.percentile(fit_pred, 95)
        upper = fit_pred >= p95
        if upper.sum() > 100:
            residual_bias = (mapped_fit[upper] - fit_truth[upper]).mean()
            # Apply correction to mapped values above p95 threshold
            mapped_p95 = np.percentile(mapped, 95)  # use mapped scale
            above = pred_all_s >= p95
            # Smooth transition: alpha goes from 0 at p95 to 1 at p99
            p99 = np.percentile(fit_pred, 99)
            alpha = np.clip((pred_all_s - p95) / max(p99 - p95, 1), 0, 1)
            mapped = mapped - residual_bias * alpha

        # Lower tail correction
        p5 = np.percentile(fit_pred, 5)
        lower = fit_pred <= p5
        if lower.sum() > 100:
            residual_bias = (mapped_fit[lower] - fit_truth[lower]).mean()
            p1 = np.percentile(fit_pred, 1)
            alpha = np.clip((p5 - pred_all_s) / max(p5 - p1, 1), 0, 1)
            mapped = mapped - residual_bias * alpha

        return mapped

    print("\n" + "=" * 70)
    print("CDF transfer (bw=0.3) + tail bias correction")
    mapped = apply_per_surface(cdf_plus_tail_correction, bw=0.3)
    evaluate(mapped, truth_all, "All")
    evaluate(mapped[oos_mask], truth_all[oos_mask], "OOS")

    print("\n" + "=" * 70)
    print("CDF transfer (bw=0.5) + tail bias correction")
    mapped = apply_per_surface(cdf_plus_tail_correction, bw=0.5)
    evaluate(mapped, truth_all, "All")
    evaluate(mapped[oos_mask], truth_all[oos_mask], "OOS")

    # ── Test 5: PCHIP quantile mapping + tail boost ──
    def pchip_plus_tail_boost(fit_pred, fit_truth, pred_all_s, n_quantiles=50, boost=0.5):
        """PCHIP quantile mapping + extra tail stretch beyond 95th/below 5th pctile."""
        ql = np.linspace(0, 100, n_quantiles + 1)
        mapped = pchip_quantile_mapping(fit_pred, fit_truth, pred_all_s, ql)

        # Measure residual bias in training upper tail
        mapped_fit = pchip_quantile_mapping(fit_pred, fit_truth, fit_pred, ql)
        residual = mapped_fit - fit_truth

        # Upper tail: apply correction proportional to distance above p90
        p90 = np.percentile(fit_pred, 90)
        p99 = np.percentile(fit_pred, 99)
        upper_residual = residual[fit_pred >= p90].mean()
        alpha = np.clip((pred_all_s - p90) / max(p99 - p90, 1), 0, 1)
        mapped = mapped - upper_residual * alpha * boost

        # Lower tail
        p10 = np.percentile(fit_pred, 10)
        p1 = np.percentile(fit_pred, 1)
        lower_residual = residual[fit_pred <= p10].mean()
        alpha = np.clip((p10 - pred_all_s) / max(p10 - p1, 1), 0, 1)
        mapped = mapped - lower_residual * alpha * boost

        return mapped

    for boost in [0.3, 0.5, 0.7, 1.0]:
        print("\n" + "=" * 70)
        print(f"PCHIP n=50 + tail boost (factor={boost})")
        mapped = apply_per_surface(pchip_plus_tail_boost, n_quantiles=50, boost=boost)
        evaluate(mapped, truth_all, "All")
        evaluate(mapped[oos_mask], truth_all[oos_mask], "OOS")

    # ── Test 6: Log-spaced quantile grid (Beta distribution spacing) ──
    # Use Beta(0.5, 0.5) CDF to get more points at tails (U-shaped density)
    from scipy.stats import beta as beta_dist
    for a_param in [0.3, 0.5, 0.7]:
        print("\n" + "=" * 70)
        print(f"PCHIP Beta({a_param},{a_param})-spaced quantiles (100 total)")
        uniform_grid = np.linspace(0, 1, 102)[1:-1]  # exclude 0 and 1
        beta_grid = beta_dist.cdf(uniform_grid, a_param, a_param) * 100
        # Add 0 and 100
        ql_beta = np.concatenate([[0], beta_grid, [100]])
        mapped = apply_per_surface(pchip_quantile_mapping, quantile_levels=ql_beta)
        evaluate(mapped, truth_all, "All")
        evaluate(mapped[oos_mask], truth_all[oos_mask], "OOS")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
