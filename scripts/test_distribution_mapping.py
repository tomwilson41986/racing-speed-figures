"""
Test non-linear distribution mapping techniques for Stage 10b.

Compares:
1. Current approach: linear variance-matching with tanh modulation
2. Smoothed quantile mapping with monotonic cubic interpolation (PCHIP)
3. Isotonic regression (monotonic non-parametric mapping)
4. Kernel-smoothed CDF transfer
5. Spline-based quantile mapping with varying smoothness

The goal is to find a technique that:
- Corrects the non-linear GBR tail compression (100+ bias from -8.4 to ~0)
- Preserves centre accuracy (40-60 band)
- Maintains correlation > 0.92
- Generalises OOS (2024-2026)
"""
import os, sys
import numpy as np
import pandas as pd
from scipy.interpolate import PchipInterpolator, UnivariateSpline
from scipy.stats import gaussian_kde
from sklearn.isotonic import IsotonicRegression

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "output")


def load_data():
    df = pd.read_csv(os.path.join(DATA_DIR, "speed_figures.csv"))
    # We need the raw figure_calibrated BEFORE the current expand_scale
    # Since the pipeline already applied it, we need to re-run from scratch
    # OR we can reverse-engineer it. Let's just use the current data and test
    # different mapping approaches on the GBR output.
    # Actually, the saved CSV has the post-expansion figure_calibrated.
    # We need the pre-expansion version. Let's load and run the pipeline up to Stage 10.
    return df


def evaluate(pred, truth, label, detail=True):
    """Evaluate predictions against truth."""
    err = pred - truth
    mae = np.abs(err).mean()
    bias = err.mean()
    corr = np.corrcoef(pred, truth)[0, 1]
    rmse = np.sqrt((err ** 2).mean())
    print(f"\n  {label}")
    print(f"    Overall:  MAE={mae:.3f}  Bias={bias:+.3f}  r={corr:.4f}  RMSE={rmse:.3f}")

    if detail:
        bands = [(-999, 20), (20, 40), (40, 60), (60, 80), (80, 100), (100, 999)]
        band_labels = ["<20", "20-40", "40-60", "60-80", "80-100", "100+"]
        for (lo, hi), bl in zip(bands, band_labels):
            m = (truth >= lo) & (truth < hi)
            if m.sum() < 10:
                continue
            be = err[m]
            print(
                f"    {bl:>6}: n={m.sum():>6,}  MAE={np.abs(be).mean():.2f}  "
                f"Bias={be.mean():+.2f}"
            )

    return {"mae": mae, "bias": bias, "corr": corr, "rmse": rmse}


def approach_current_tanh(pred, truth, pred_all, train_mask):
    """Current approach: variance-matching with tanh modulation."""
    TANH_K = 1.5
    fit_pred = pred[train_mask]
    fit_truth = truth[train_mask]

    pred_mean = fit_pred.mean()
    pred_std = fit_pred.std()
    tf_mean = fit_truth.mean()
    tf_std = fit_truth.std()
    base_stretch = tf_std / pred_std

    dev = pred_all - pred_mean
    norm_dev = np.abs(dev) / pred_std
    alpha = np.tanh(norm_dev * TANH_K)
    local_stretch = 1.0 + (base_stretch - 1.0) * alpha
    return tf_mean + dev * local_stretch


def approach_quantile_pchip(pred, truth, pred_all, train_mask, n_quantiles=200):
    """
    Smoothed quantile mapping with PCHIP (monotonic cubic) interpolation.

    For each quantile level q in [0, 1]:
      - Find the q-th percentile of our predictions (training set)
      - Find the q-th percentile of Timeform timefigures (training set)
    Build a monotonic interpolation from pred_quantile -> tf_quantile.
    Apply to all predictions.
    """
    fit_pred = pred[train_mask]
    fit_truth = truth[train_mask]

    # Compute quantile pairs
    quantile_levels = np.linspace(0, 100, n_quantiles + 1)
    pred_quantiles = np.percentile(fit_pred, quantile_levels)
    tf_quantiles = np.percentile(fit_truth, quantile_levels)

    # Build monotonic interpolation
    # PCHIP guarantees monotonicity between data points
    mapper = PchipInterpolator(pred_quantiles, tf_quantiles, extrapolate=True)

    return mapper(pred_all)


def approach_isotonic(pred, truth, pred_all, train_mask):
    """
    Isotonic regression: fit a monotonically increasing step function
    that maps predictions to Timeform values with minimum squared error.

    This is the optimal monotonic mapping in MSE sense.
    """
    fit_pred = pred[train_mask]
    fit_truth = truth[train_mask]

    iso = IsotonicRegression(increasing=True, out_of_bounds="clip")
    iso.fit(fit_pred, fit_truth)

    return iso.predict(pred_all)


def approach_isotonic_smoothed(pred, truth, pred_all, train_mask, smooth_factor=0.01):
    """
    Isotonic regression + spline smoothing.

    Isotonic gives a step function. We smooth it with a univariate spline
    to get a continuous, smooth mapping that still captures the non-linear shape.
    """
    fit_pred = pred[train_mask]
    fit_truth = truth[train_mask]

    iso = IsotonicRegression(increasing=True, out_of_bounds="clip")
    iso.fit(fit_pred, fit_truth)

    # Get the isotonic mapping at the training points
    iso_pred = iso.predict(fit_pred)

    # Create a smooth version by sampling the isotonic function on a grid
    grid = np.linspace(fit_pred.min() - 10, fit_pred.max() + 10, 500)
    grid_iso = iso.predict(grid)

    # Fit a smooth spline through the isotonic mapping
    # s controls smoothness: higher = smoother
    n = len(grid)
    spline = UnivariateSpline(grid, grid_iso, s=smooth_factor * n, k=3)

    return spline(pred_all)


def approach_quantile_spline(pred, truth, pred_all, train_mask, n_quantiles=100,
                              smooth_factor=0.005):
    """
    Quantile mapping with smoothing spline.

    Like PCHIP quantile mapping but uses a smoothing spline instead of
    exact interpolation. This allows the mapping to be smoother (less
    sensitive to noise in individual quantile bins) while still capturing
    the overall non-linear shape.
    """
    fit_pred = pred[train_mask]
    fit_truth = truth[train_mask]

    quantile_levels = np.linspace(0, 100, n_quantiles + 1)
    pred_q = np.percentile(fit_pred, quantile_levels)
    tf_q = np.percentile(fit_truth, quantile_levels)

    # Ensure strictly increasing x for spline fitting
    # Remove duplicate x values (can happen at distribution tails)
    mask = np.diff(pred_q, prepend=pred_q[0] - 1) > 0
    pred_q = pred_q[mask]
    tf_q = tf_q[mask]

    n = len(pred_q)
    spline = UnivariateSpline(pred_q, tf_q, s=smooth_factor * n, k=3)

    return spline(pred_all)


def approach_binned_bias_correction(pred, truth, pred_all, train_mask, n_bins=20):
    """
    Non-parametric binned bias correction with smooth interpolation.

    1. Bin predictions into n_bins equal-frequency bins on training data
    2. Compute the mean bias in each bin
    3. Smooth-interpolate the bias function
    4. Apply: corrected = pred - smooth_bias(pred)

    This directly targets the bias curve rather than the full distribution.
    """
    fit_pred = pred[train_mask]
    fit_truth = truth[train_mask]

    # Equal-frequency bins
    bin_edges = np.percentile(fit_pred, np.linspace(0, 100, n_bins + 1))
    bin_centres = []
    bin_biases = []

    for i in range(n_bins):
        lo, hi = bin_edges[i], bin_edges[i + 1]
        if i == n_bins - 1:
            m = (fit_pred >= lo) & (fit_pred <= hi)
        else:
            m = (fit_pred >= lo) & (fit_pred < hi)
        if m.sum() < 50:
            continue
        bin_centres.append(fit_pred[m].mean())
        bin_biases.append((fit_pred[m] - fit_truth[m]).mean())

    bin_centres = np.array(bin_centres)
    bin_biases = np.array(bin_biases)

    # Smooth interpolation of the bias curve
    mapper = PchipInterpolator(bin_centres, bin_biases, extrapolate=True)

    return pred_all - mapper(pred_all)


def approach_cdf_transfer(pred, truth, pred_all, train_mask, bw_factor=0.5):
    """
    CDF transfer (probability integral transform).

    1. Estimate smooth CDF of predictions (training) using KDE
    2. Estimate smooth CDF of Timeform (training) using KDE
    3. For each prediction x: find p = CDF_pred(x), then return CDF_tf^{-1}(p)

    This is the theoretically correct distribution-matching approach.
    Uses kernel density estimation for smooth CDFs.
    """
    fit_pred = pred[train_mask]
    fit_truth = truth[train_mask]

    # KDE for both distributions
    kde_pred = gaussian_kde(fit_pred, bw_method=bw_factor)
    kde_truth = gaussian_kde(fit_truth, bw_method=bw_factor)

    # Build numerical CDFs on a fine grid
    grid_min = min(fit_pred.min(), fit_truth.min()) - 20
    grid_max = max(fit_pred.max(), fit_truth.max()) + 20
    grid = np.linspace(grid_min, grid_max, 2000)

    cdf_pred = np.cumsum(kde_pred(grid)) * (grid[1] - grid[0])
    cdf_pred = cdf_pred / cdf_pred[-1]  # normalise to [0, 1]

    cdf_truth = np.cumsum(kde_truth(grid)) * (grid[1] - grid[0])
    cdf_truth = cdf_truth / cdf_truth[-1]

    # For each prediction, find its CDF value, then invert through truth CDF
    # Use interpolation for both directions
    pred_to_cdf = PchipInterpolator(grid, cdf_pred, extrapolate=True)
    cdf_to_truth = PchipInterpolator(cdf_truth, grid, extrapolate=True)

    p_values = pred_to_cdf(pred_all)
    p_values = np.clip(p_values, 0.0001, 0.9999)

    return cdf_to_truth(p_values)


def main():
    print("=" * 70)
    print("DISTRIBUTION MAPPING TECHNIQUE COMPARISON")
    print("=" * 70)

    # Load the speed figures
    df = pd.read_csv(os.path.join(DATA_DIR, "speed_figures.csv"))
    print(f"Loaded {len(df):,} rows")

    # We need the pre-expansion figure_calibrated. Since the pipeline already
    # applied expand_scale, we need to re-run from the GBR output.
    # Let's check if we have the data we need.
    # The figure_calibrated in the CSV is POST-expansion. We need PRE-expansion.
    # We'll re-run the pipeline stages 1-10 (without 10b) to get the raw GBR output.

    # Actually, a simpler approach: re-run just the expand_scale logic in reverse
    # to recover the pre-expansion values. But that's fragile.

    # Better: run the pipeline fresh and capture the intermediate state.
    # Let's import and use the pipeline directly.
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
    from speed_figures import run_pipeline

    print("\nRunning full pipeline to get pre-expansion figures...")
    all_figs, std_times, ga = run_pipeline()

    # At this point, all_figs has the POST-expansion figure_calibrated.
    # We need PRE-expansion. Let's modify the approach: we'll monkeypatch
    # expand_scale to be a no-op and re-run.
    # Actually, that won't work easily. Let me instead run the pipeline
    # stages manually.

    # Simpler approach: the pipeline saves output. Let's use the saved data
    # and just reverse the expansion. Actually the cleanest approach is to
    # monkeypatch and re-run.

    # Even simpler: save the pre-expansion data from within the pipeline.
    # But we can't modify the pipeline for testing purposes.

    # OK, let's take a different tack. We know expand_scale is per-surface
    # variance-matching with tanh. We can reverse it given the parameters.
    # But we don't have the exact parameters saved.

    # Most pragmatic: use the existing figure_calibrated as-is and just test
    # different mapping approaches against timefigure. The current approach
    # is already applied, so we'll compare:
    # - The identity (current post-expansion values)
    # - Applying a *new* mapping on top (which would be wrong, double-correcting)
    #
    # NO - we need the raw pre-expansion values. Let me think...
    #
    # The pipeline runs: GBR -> expand_scale -> save.
    # We want: GBR output (before expand_scale).
    # The best approach is to compute it directly.

    # Actually, let me just manually reverse the current tanh expansion.
    # For each surface, we know: expanded = tf_mean + dev * local_stretch
    # where dev = raw - pred_mean, local_stretch = 1 + (stretch-1)*tanh(...)
    # This is invertible if we know the parameters.
    # But we don't save them. Let me just re-compute them.

    valid = all_figs[
        all_figs["timefigure"].notna()
        & (all_figs["timefigure"] != 0)
        & all_figs["timefigure"].between(-200, 200)
        & all_figs["figure_calibrated"].notna()
    ].copy()

    print(f"\nValid rows for analysis: {len(valid):,}")

    # Since the pipeline already applied expand_scale, and we can't easily
    # reverse it, let's instead add a hook to save pre-expansion data.
    # For now, let's read the raw columns. The pipeline saves figure_final
    # (pre-calibration) and figure_calibrated (post-everything).
    #
    # What we really want is figure post-GBR but pre-expand_scale.
    # That column doesn't exist in the output.
    #
    # SOLUTION: modify the pipeline temporarily to save the pre-expansion
    # figure, run it, and then compare approaches.

    print("\n\n*** Re-running pipeline with expand_scale disabled to get raw GBR output ***")

    # Monkeypatch expand_scale to save pre-expansion data
    import speed_figures as sf
    original_expand = sf.expand_scale

    pre_expansion_data = {}

    def capture_expand(df):
        """Capture pre-expansion data then skip expansion."""
        pre_expansion_data["figure_calibrated"] = df["figure_calibrated"].copy()
        print("\n  [TEST] Captured pre-expansion figure_calibrated, skipping expansion.")
        return df  # return WITHOUT applying expansion

    sf.expand_scale = capture_expand

    all_figs2, _, _ = run_pipeline()

    # Restore
    sf.expand_scale = original_expand

    # Now all_figs2 has the PRE-expansion figure_calibrated
    valid2 = all_figs2[
        all_figs2["timefigure"].notna()
        & (all_figs2["timefigure"] != 0)
        & all_figs2["timefigure"].between(-200, 200)
        & all_figs2["figure_calibrated"].notna()
    ].copy()

    print(f"\nPre-expansion valid rows: {len(valid2):,}")

    pred = valid2["figure_calibrated"].values.astype(float)
    truth = valid2["timefigure"].values.astype(float)
    train_mask = valid2["source_year"].values <= 2023
    oos_mask = valid2["source_year"].values > 2023

    print(f"Training (<=2023): {train_mask.sum():,}")
    print(f"OOS (2024-2026): {oos_mask.sum():,}")

    # Baseline: no expansion
    print("\n" + "=" * 70)
    print("BASELINE: No expansion (raw GBR output)")
    evaluate(pred, truth, "All data")
    evaluate(pred[oos_mask], truth[oos_mask], "OOS only (2024-2026)")

    # Approach 1: Current tanh
    print("\n" + "=" * 70)
    print("APPROACH 1: Current tanh modulation")
    for surface in valid2["raceSurfaceName"].unique():
        sm = valid2["raceSurfaceName"].values == surface
        mapped = approach_current_tanh(pred[sm], truth[sm], pred[sm], train_mask[sm])
        pred_a1 = pred.copy()
        pred_a1[sm] = mapped
    evaluate(pred_a1, truth, "All data")
    evaluate(pred_a1[oos_mask], truth[oos_mask], "OOS only")

    # Approach 2: Quantile mapping with PCHIP (various n_quantiles)
    for nq in [50, 100, 200, 500]:
        print("\n" + "=" * 70)
        print(f"APPROACH 2: PCHIP quantile mapping (n={nq})")
        pred_a2 = pred.copy()
        for surface in valid2["raceSurfaceName"].unique():
            sm = valid2["raceSurfaceName"].values == surface
            mapped = approach_quantile_pchip(pred[sm], truth[sm], pred[sm], train_mask[sm],
                                             n_quantiles=nq)
            pred_a2[sm] = mapped
        evaluate(pred_a2, truth, "All data")
        evaluate(pred_a2[oos_mask], truth[oos_mask], "OOS only")

    # Approach 3: Isotonic regression
    print("\n" + "=" * 70)
    print("APPROACH 3: Isotonic regression (raw)")
    pred_a3 = pred.copy()
    for surface in valid2["raceSurfaceName"].unique():
        sm = valid2["raceSurfaceName"].values == surface
        mapped = approach_isotonic(pred[sm], truth[sm], pred[sm], train_mask[sm])
        pred_a3[sm] = mapped
    evaluate(pred_a3, truth, "All data")
    evaluate(pred_a3[oos_mask], truth[oos_mask], "OOS only")

    # Approach 4: Isotonic + spline smoothing
    for sf_val in [0.001, 0.005, 0.01, 0.05]:
        print("\n" + "=" * 70)
        print(f"APPROACH 4: Isotonic + spline (smooth={sf_val})")
        pred_a4 = pred.copy()
        for surface in valid2["raceSurfaceName"].unique():
            sm = valid2["raceSurfaceName"].values == surface
            mapped = approach_isotonic_smoothed(pred[sm], truth[sm], pred[sm], train_mask[sm],
                                                smooth_factor=sf_val)
            pred_a4[sm] = mapped
        evaluate(pred_a4, truth, "All data")
        evaluate(pred_a4[oos_mask], truth[oos_mask], "OOS only")

    # Approach 5: Quantile mapping with smoothing spline
    for nq, sf_val in [(100, 0.005), (100, 0.01), (200, 0.005), (200, 0.01)]:
        print("\n" + "=" * 70)
        print(f"APPROACH 5: Quantile + smoothing spline (nq={nq}, s={sf_val})")
        pred_a5 = pred.copy()
        for surface in valid2["raceSurfaceName"].unique():
            sm = valid2["raceSurfaceName"].values == surface
            mapped = approach_quantile_spline(pred[sm], truth[sm], pred[sm], train_mask[sm],
                                              n_quantiles=nq, smooth_factor=sf_val)
            pred_a5[sm] = mapped
        evaluate(pred_a5, truth, "All data")
        evaluate(pred_a5[oos_mask], truth[oos_mask], "OOS only")

    # Approach 6: Binned bias correction
    for nb in [10, 20, 30, 50]:
        print("\n" + "=" * 70)
        print(f"APPROACH 6: Binned bias correction (n_bins={nb})")
        pred_a6 = pred.copy()
        for surface in valid2["raceSurfaceName"].unique():
            sm = valid2["raceSurfaceName"].values == surface
            mapped = approach_binned_bias_correction(pred[sm], truth[sm], pred[sm], train_mask[sm],
                                                     n_bins=nb)
            pred_a6[sm] = mapped
        evaluate(pred_a6, truth, "All data")
        evaluate(pred_a6[oos_mask], truth[oos_mask], "OOS only")

    # Approach 7: CDF transfer
    for bw in [0.3, 0.5, 0.8]:
        print("\n" + "=" * 70)
        print(f"APPROACH 7: CDF transfer (KDE bw={bw})")
        pred_a7 = pred.copy()
        for surface in valid2["raceSurfaceName"].unique():
            sm = valid2["raceSurfaceName"].values == surface
            try:
                mapped = approach_cdf_transfer(pred[sm], truth[sm], pred[sm], train_mask[sm],
                                               bw_factor=bw)
                pred_a7[sm] = mapped
            except Exception as e:
                print(f"  CDF transfer failed for {surface}: {e}")
        evaluate(pred_a7, truth, "All data")
        evaluate(pred_a7[oos_mask], truth[oos_mask], "OOS only")

    print("\n" + "=" * 70)
    print("COMPARISON COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
