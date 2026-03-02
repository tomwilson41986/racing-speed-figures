"""
Test GBR hyperparameter changes + quantile mapping combinations to minimise
100+ bias. Runs stages 0-9 once, then tests multiple GBR configs + QM settings.

Key hypothesis: min_samples_leaf=50 forces GBR to average 100+ horses with
lower-rated neighbours. Reducing this + adjusting quantile mapping should
substantially reduce the -3.47 bias at 100-120.
"""
import os, sys
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from sklearn.ensemble import GradientBoostingRegressor
from scipy.interpolate import PchipInterpolator


def evaluate(pred, truth, label, bands=None):
    """Evaluate predictions vs truth with optional band detail."""
    err = pred - truth
    mae = np.abs(err).mean()
    bias = err.mean()
    corr = np.corrcoef(pred, truth)[0, 1]

    if bands is None:
        bands = [(-999, 20), (20, 40), (40, 60), (60, 80), (80, 100), (100, 120), (120, 999)]
        band_labels = ["<20", "20-40", "40-60", "60-80", "80-100", "100-120", "120+"]

    print(f"  {label}: MAE={mae:.3f} Bias={bias:+.3f} r={corr:.4f}")
    results = {}
    for (lo, hi), bl in zip(bands, band_labels):
        m = (truth >= lo) & (truth < hi)
        if m.sum() < 10:
            continue
        be = err[m]
        band_mae = np.abs(be).mean()
        band_bias = be.mean()
        print(f"    {bl:>7}: n={m.sum():>6,}  MAE={band_mae:.2f}  Bias={band_bias:+.2f}")
        results[bl] = {"mae": band_mae, "bias": band_bias, "n": int(m.sum())}
    results["overall"] = {"mae": mae, "bias": bias, "corr": corr}
    return results


def apply_quantile_mapping(pred, truth, pred_all, train_mask, n_quantiles=50):
    """Apply PCHIP quantile mapping."""
    fit_pred = pred[train_mask]
    fit_truth = truth[train_mask]

    ql = np.linspace(0, 100, n_quantiles + 1)
    pred_q = np.percentile(fit_pred, ql)
    tf_q = np.percentile(fit_truth, ql)

    # Remove duplicate x-values
    unique_mask = np.concatenate([[True], np.diff(pred_q) > 0.001])
    pred_q = pred_q[unique_mask]
    tf_q = tf_q[unique_mask]

    mapper = PchipInterpolator(pred_q, tf_q, extrapolate=True)
    return mapper(pred_all)


def train_and_predict_gbr(df, fit_mask, surf_mask, features, **gbr_params):
    """Train GBR with given params and return predictions for all surf rows."""
    fit = df[fit_mask].copy()
    for col in features:
        if col not in fit.columns:
            fit[col] = 0
        fit[col] = pd.to_numeric(fit[col], errors="coerce").fillna(0)

    X_fit = fit[features].values
    y_fit = fit["timefigure"].values

    gbr = GradientBoostingRegressor(**gbr_params)
    gbr.fit(X_fit, y_fit)

    # Predict for all rows in this surface
    all_surf = df[surf_mask].copy()
    for col in features:
        if col not in all_surf.columns:
            all_surf[col] = 0
        all_surf[col] = pd.to_numeric(all_surf[col], errors="coerce").fillna(0)

    has_fig = all_surf["figure_calibrated"].notna() & (all_surf["figure_calibrated"] != 0)
    preds = np.full(surf_mask.sum(), np.nan)
    if has_fig.sum() > 0:
        X_pred = all_surf.loc[has_fig, features].values
        preds[has_fig.values] = gbr.predict(X_pred)

    return preds, gbr


def main():
    import speed_figures as sf

    print("=" * 70)
    print("GBR HYPERPARAMETER + QUANTILE MAPPING OPTIMISATION")
    print("=" * 70)

    # ── Run pipeline up to Stage 9 (calibration) ──
    # Monkeypatch to skip GBR and expansion
    original_gbr = sf.enhance_with_gbr
    original_expand = sf.expand_scale

    pre_gbr_data = {}

    def skip_gbr(df):
        pre_gbr_data["df"] = df.copy()
        print("\n  [TEST] Captured pre-GBR data, skipping GBR.")
        return df, {}

    def skip_expand(df):
        print("\n  [TEST] Skipping expansion.")
        return df

    sf.enhance_with_gbr = skip_gbr
    sf.expand_scale = skip_expand
    sf.run_pipeline()
    sf.enhance_with_gbr = original_gbr
    sf.expand_scale = original_expand

    df = pre_gbr_data["df"]
    print(f"\nCaptured pre-GBR dataframe: {len(df):,} rows")

    # ── Feature engineering (same as in enhance_with_gbr) ──
    going_ordinal = {
        "Hard": 1, "Firm": 1, "Fast": 1,
        "Gd/Frm": 2, "Good To Firm": 2, "Good to Firm": 2, "Std/Fast": 2,
        "Good": 3, "Standard": 3, "Std": 3,
        "Gd/Sft": 4, "Good to Soft": 4, "Good To Yielding": 4,
        "Good to Yielding": 4, "Std/Slow": 4, "Standard/Slow": 4,
        "Standard To Slow": 4, "Standard to Slow": 4, "Slow": 4,
        "Soft": 5, "Yielding": 5, "Yld/Sft": 5, "Sft/Hvy": 5, "Hvy/Sft": 5,
        "Heavy": 6,
    }
    df["going_num"] = df["going"].map(going_ordinal).fillna(3)
    course_counts = df["courseName"].value_counts()
    df["course_freq"] = df["courseName"].map(course_counts).fillna(0) / len(df)

    FEATURES = [
        "figure_calibrated", "figure_final", "raceClass",
        "distance", "horseAge", "positionOfficial",
        "weightCarried", "ga_value", "going_num", "course_freq",
    ]

    mask = (
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_calibrated"].notna()
    )

    # Save the pre-GBR figure_calibrated
    pre_gbr_fig = df["figure_calibrated"].copy()

    # ── GBR configurations to test ──
    configs = [
        ("CURRENT: d5/msl50/lr0.08/n300",
         dict(n_estimators=300, max_depth=5, learning_rate=0.08,
              subsample=0.8, min_samples_leaf=50, random_state=42)),

        ("d5/msl20/lr0.08/n300",
         dict(n_estimators=300, max_depth=5, learning_rate=0.08,
              subsample=0.8, min_samples_leaf=20, random_state=42)),

        ("d5/msl10/lr0.08/n300",
         dict(n_estimators=300, max_depth=5, learning_rate=0.08,
              subsample=0.8, min_samples_leaf=10, random_state=42)),

        ("d6/msl50/lr0.08/n300",
         dict(n_estimators=300, max_depth=6, learning_rate=0.08,
              subsample=0.8, min_samples_leaf=50, random_state=42)),

        ("d6/msl20/lr0.08/n300",
         dict(n_estimators=300, max_depth=6, learning_rate=0.08,
              subsample=0.8, min_samples_leaf=20, random_state=42)),

        ("d6/msl10/lr0.08/n300",
         dict(n_estimators=300, max_depth=6, learning_rate=0.08,
              subsample=0.8, min_samples_leaf=10, random_state=42)),

        ("d7/msl20/lr0.06/n400",
         dict(n_estimators=400, max_depth=7, learning_rate=0.06,
              subsample=0.8, min_samples_leaf=20, random_state=42)),

        ("d7/msl10/lr0.06/n400",
         dict(n_estimators=400, max_depth=7, learning_rate=0.06,
              subsample=0.8, min_samples_leaf=10, random_state=42)),

        # Huber loss (more robust to outliers)
        ("d5/msl20/lr0.08/n300/huber",
         dict(n_estimators=300, max_depth=5, learning_rate=0.08,
              subsample=0.8, min_samples_leaf=20, loss="huber",
              random_state=42)),

        ("d6/msl20/lr0.08/n300/huber",
         dict(n_estimators=300, max_depth=6, learning_rate=0.08,
              subsample=0.8, min_samples_leaf=20, loss="huber",
              random_state=42)),

        # More trees with lower learning rate
        ("d5/msl20/lr0.05/n500",
         dict(n_estimators=500, max_depth=5, learning_rate=0.05,
              subsample=0.8, min_samples_leaf=20, random_state=42)),

        ("d6/msl15/lr0.05/n500",
         dict(n_estimators=500, max_depth=6, learning_rate=0.05,
              subsample=0.8, min_samples_leaf=15, random_state=42)),
    ]

    # Quantile mapping settings to try
    qm_settings = [50, 30, 20]

    # ── Test each configuration ──
    all_results = []

    for config_name, gbr_params in configs:
        print(f"\n{'=' * 70}")
        print(f"GBR CONFIG: {config_name}")
        print(f"{'=' * 70}")

        # Restore pre-GBR figures
        df["figure_calibrated"] = pre_gbr_fig.copy()

        # Train and predict per surface
        for surface in df["raceSurfaceName"].unique():
            surf_mask = df["raceSurfaceName"] == surface
            fit_mask = mask & surf_mask & (df["source_year"] <= 2023)

            if fit_mask.sum() < 1000:
                continue

            preds, gbr = train_and_predict_gbr(
                df, fit_mask, surf_mask, FEATURES, **gbr_params
            )

            # Update figure_calibrated with GBR predictions
            surf_idx = df.index[surf_mask]
            valid_preds = ~np.isnan(preds)
            df.loc[surf_idx[valid_preds], "figure_calibrated"] = preds[valid_preds]

            # Show GBR compression on training data
            fit = df[fit_mask]
            pred_std = fit["figure_calibrated"].std()
            tf_std = fit["timefigure"].std()
            print(f"  {surface}: std ratio = {pred_std/tf_std:.4f} "
                  f"(pred={pred_std:.1f} vs tf={tf_std:.1f})")

        # Evaluate raw GBR (no QM)
        valid = df[mask].copy()
        pred = valid["figure_calibrated"].values.astype(float)
        truth = valid["timefigure"].values.astype(float)
        surfaces = valid["raceSurfaceName"].values
        train = valid["source_year"].values <= 2023
        oos = valid["source_year"].values > 2023

        print(f"\n  --- Raw GBR (no quantile mapping) ---")
        r_all = evaluate(pred, truth, "All")
        r_oos = evaluate(pred[oos], truth[oos], "OOS")

        # Now test different QM settings on top
        for nq in qm_settings:
            print(f"\n  --- + Quantile mapping (n={nq}) ---")
            pred_qm = pred.copy()
            for surface in np.unique(surfaces):
                sm = surfaces == surface
                pred_qm[sm] = apply_quantile_mapping(
                    pred[sm], truth[sm], pred[sm], train[sm], n_quantiles=nq
                )
            r_all_qm = evaluate(pred_qm, truth, f"All+QM{nq}")
            r_oos_qm = evaluate(pred_qm[oos], truth[oos], f"OOS+QM{nq}")

            all_results.append({
                "config": config_name,
                "qm": nq,
                "overall_mae": r_all_qm["overall"]["mae"],
                "overall_corr": r_all_qm["overall"]["corr"],
                "100_120_bias": r_all_qm.get("100-120", {}).get("bias", None),
                "100_120_mae": r_all_qm.get("100-120", {}).get("mae", None),
                "120_bias": r_all_qm.get("120+", {}).get("bias", None),
                "oos_overall_mae": r_oos_qm["overall"]["mae"],
                "oos_100_120_bias": r_oos_qm.get("100-120", {}).get("bias", None),
                "oos_100_120_mae": r_oos_qm.get("100-120", {}).get("mae", None),
            })

    # ── Also test pre-GBR blending approach ──
    print(f"\n{'=' * 70}")
    print("APPROACH: Blend pre-GBR + GBR at extremes")
    print(f"{'=' * 70}")

    # Restore pre-GBR and run current GBR
    df["figure_calibrated"] = pre_gbr_fig.copy()
    for surface in df["raceSurfaceName"].unique():
        surf_mask = df["raceSurfaceName"] == surface
        fit_mask = mask & surf_mask & (df["source_year"] <= 2023)
        if fit_mask.sum() < 1000:
            continue
        preds, _ = train_and_predict_gbr(
            df, fit_mask, surf_mask, FEATURES,
            n_estimators=300, max_depth=5, learning_rate=0.08,
            subsample=0.8, min_samples_leaf=50, random_state=42
        )
        surf_idx = df.index[surf_mask]
        valid_preds = ~np.isnan(preds)
        df.loc[surf_idx[valid_preds], "figure_calibrated"] = preds[valid_preds]

    valid = df[mask].copy()
    gbr_pred = valid["figure_calibrated"].values.astype(float)
    raw_pred = pre_gbr_fig[valid.index].values.astype(float)
    truth = valid["timefigure"].values.astype(float)
    surfaces = valid["raceSurfaceName"].values
    train = valid["source_year"].values <= 2023
    oos = valid["source_year"].values > 2023

    # Blend: at extremes (abs deviation from mean > 2σ), weight raw more
    for blend_threshold in [1.0, 1.5, 2.0]:
        for blend_max_weight in [0.3, 0.5, 0.7]:
            blended = gbr_pred.copy()
            for surface in np.unique(surfaces):
                sm = surfaces == surface
                gbr_mean = gbr_pred[sm & train].mean()
                gbr_std = gbr_pred[sm & train].std()
                dev = np.abs(gbr_pred[sm] - gbr_mean) / gbr_std
                # Weight raw prediction more at extremes
                alpha = np.clip((dev - blend_threshold) / (3.0 - blend_threshold), 0, 1) * blend_max_weight
                blended[sm] = (1 - alpha) * gbr_pred[sm] + alpha * raw_pred[sm]

            # Apply QM on top
            blended_qm = blended.copy()
            for surface in np.unique(surfaces):
                sm = surfaces == surface
                blended_qm[sm] = apply_quantile_mapping(
                    blended[sm], truth[sm], blended[sm], train[sm], n_quantiles=30
                )

            print(f"\n  Blend thresh={blend_threshold}σ, max_weight={blend_max_weight} + QM30")
            evaluate(blended_qm, truth, "All")
            evaluate(blended_qm[oos], truth[oos], "OOS")

    # ── Summary table ──
    print(f"\n{'=' * 70}")
    print("SUMMARY: Best configurations ranked by OOS 100-120 bias")
    print(f"{'=' * 70}")
    results_df = pd.DataFrame(all_results)
    results_df = results_df.sort_values("oos_100_120_bias", key=abs)
    print(results_df.to_string(index=False))


if __name__ == "__main__":
    main()
