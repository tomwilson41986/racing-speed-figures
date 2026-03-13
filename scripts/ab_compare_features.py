"""
A/B Comparison: Baseline vs CustomMetricsEngine-Enhanced ML Model
=================================================================
Trains two models on the same data:
  - Model A (baseline): Current feature set only
  - Model B (enhanced):  Current features + CustomMetricsEngine features

Compares accuracy on the held-out test set (2024+) to measure the
incremental value of the new features.

Usage:
    python scripts/ab_compare_features.py
"""

import os
import sys
import time

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
import xgboost as xgb
import lightgbm as lgb

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.ml_figures import (
    load_pipeline_output,
    load_raw_extra_cols,
    build_features,
    _compute_target_encodings,
)

# ── Baseline feature list (current model, no custom metrics) ──────────
BASELINE_FEATURES = [
    "figure_final", "figure_calibrated", "raw_figure",
    "finishing_time", "time_per_furlong", "speed_yps",
    "time_behind_winner", "time_vs_race_mean",
    "distance", "total_yards",
    "going_numeric", "class_numeric", "is_aw",
    "numberOfRunners", "log_prize",
    "race_mean_figure", "race_figure_spread", "log_prize_per_runner",
    "horseAge", "is_female", "weightCarried", "pre_rating",
    "log_isp", "log_bfsp",
    "positionOfficial", "distanceCumulative", "beaten_per_furlong",
    "draw", "month", "course_id",
    "weight_adj", "wfa_adj",
    "career_avg_figure", "last_figure", "best_figure", "worst_figure",
    "n_previous_runs", "recent_avg_3", "figure_std",
    "figure_vs_career", "days_since_last",
    "course_te", "going_te", "class_te",
    "course_dist_te", "course_going_te",
    "source_year", "pipeline_ga",
    "jockey_uplift", "trainer_uplift", "adj_rating",
    "sectional_time", "tf_wfa",
]

# ── New features from CustomMetricsEngine ─────────────────────────────
CUSTOM_METRICS_FEATURES = [
    # Phase 1: highest impact
    "LB", "preracehorsecareerLB", "LR_LB", "LR3_LB", "LR5_LB", "FSALB",
    "RSR", "preracehorsecareerRSR", "LR_RSR", "LR3_RSR", "LR5_RSR",
    "best_RSR", "RSR_gap", "SFI", "SFI_3",
    "or_change", "or_change_3", "career_best_or", "or_vs_best",
    "or_off_peak", "or_vs_last_win",
    "headgear_change", "first_time_headgear", "headgear_removed", "has_headgear",

    # Phase 2: high impact
    "sire_win_rate", "sire_place_rate", "sire_avg_nfp", "sire_wiv",
    "sire_going_nfp", "sire_going_win_rate", "sire_dist_nfp", "sire_dist_win_rate",
    "sire_runners",
    "damsire_avg_nfp", "damsire_win_rate", "damsire_going_nfp", "damsire_dist_nfp",
    "debut_x_sire_nfp", "debut_x_sire_wiv", "debut_x_trainer_wiv",
    "trainer_sr_14d", "trainer_sr_30d", "trainer_form_delta",
    "jockey_sr_14d", "jockey_sr_30d", "jockey_form_delta",
    "horse_track_nfp", "horse_track_win_rate", "horse_track_runs",
    "trainer_track_win_rate", "trainer_track_runs",
    "jockey_track_win_rate", "jockey_track_runs",

    # Phase 3: medium impact
    "NFP_residual", "career_residual", "residual_exp3", "residual_exp5",
    "career_win_surprise",
    "form_slope_3", "form_slope_5", "is_improving", "is_declining",
    "career_nfp_std", "recent_nfp_std", "career_place_rate",
    "career_win_rate", "recent_win_rate", "recent_place_rate",
    "unexposure_score", "is_debut", "first_at_distance",
    "first_at_going", "first_at_course",
    "dist_from_preferred", "going_from_preferred", "dist_change_lr",
    "class_change", "is_class_drop", "is_class_rise", "class_vs_avg",
    "weight_vs_avg", "weight_vs_min", "weight_change_lr",
    "surface_nfp", "surface_win_rate", "first_on_surface",
    "draw_relative", "draw_quartile",
    "EXP_NFP3", "EXP_NFP5", "EXP_NFP10", "EXP_RB3", "EXP_RB5",
    "preracehorsecareerNFP", "preracehorsecareerRB",
    "preracehorsecareerWIV", "preracehorsecareerWAX",
    "LRNFP", "LR3NFPtotal", "LR5NFPtotal",
    "preracehorsecareerORR2",
    "trainerjockeycareerWIV", "trainerjockeycareerNFP",
]


def _train_blend(X_train, y_train, X_test, y_test, sample_weights, label=""):
    """Train 4-model blend and return test predictions."""
    print(f"\n  Training {label} models ({X_train.shape[1]} features)...")

    xgb_mse = xgb.XGBRegressor(
        objective="reg:squarederror", eval_metric="mae",
        max_depth=8, learning_rate=0.02, subsample=0.8,
        colsample_bytree=0.7, min_child_weight=8,
        reg_alpha=0.05, reg_lambda=0.5,
        n_estimators=8000, early_stopping_rounds=150, verbosity=0,
    )
    xgb_mse.fit(X_train, y_train, sample_weight=sample_weights,
                eval_set=[(X_test, y_test)], verbose=False)

    xgb_mae = xgb.XGBRegressor(
        objective="reg:absoluteerror", eval_metric="mae",
        max_depth=8, learning_rate=0.02, subsample=0.8,
        colsample_bytree=0.8, min_child_weight=12,
        reg_alpha=0.05, reg_lambda=0.5,
        n_estimators=8000, early_stopping_rounds=150, verbosity=0,
    )
    xgb_mae.fit(X_train, y_train, sample_weight=sample_weights,
                eval_set=[(X_test, y_test)], verbose=False)

    lgb_ds_tr = lgb.Dataset(X_train, y_train, weight=sample_weights)
    lgb_ds_te = lgb.Dataset(X_test, y_test, reference=lgb_ds_tr)
    lgb_mse = lgb.train(
        {"objective": "regression", "metric": "mae", "num_leaves": 127,
         "learning_rate": 0.02, "feature_fraction": 0.8,
         "bagging_fraction": 0.8, "bagging_freq": 5,
         "min_child_samples": 15, "verbose": -1},
        lgb_ds_tr, num_boost_round=8000, valid_sets=[lgb_ds_te],
        callbacks=[lgb.early_stopping(150), lgb.log_evaluation(0)],
    )

    lgb_ds_tr2 = lgb.Dataset(X_train, y_train, weight=sample_weights)
    lgb_ds_te2 = lgb.Dataset(X_test, y_test, reference=lgb_ds_tr2)
    lgb_mae_mdl = lgb.train(
        {"objective": "mae", "metric": "mae", "num_leaves": 127,
         "learning_rate": 0.02, "feature_fraction": 0.8,
         "bagging_fraction": 0.8, "bagging_freq": 5,
         "min_child_samples": 15, "verbose": -1},
        lgb_ds_tr2, num_boost_round=8000, valid_sets=[lgb_ds_te2],
        callbacks=[lgb.early_stopping(150), lgb.log_evaluation(0)],
    )

    test_pred = 0.25 * (
        xgb_mse.predict(X_test) + xgb_mae.predict(X_test)
        + lgb_mse.predict(X_test) + lgb_mae_mdl.predict(X_test)
    )

    return test_pred, xgb_mse


def _report_metrics(y_true, y_pred, label, df_test=None):
    """Print comprehensive accuracy metrics."""
    err = y_pred - y_true
    mae = np.mean(np.abs(err))
    rmse = np.sqrt(np.mean(err ** 2))
    bias = np.mean(err)
    corr = np.corrcoef(y_true, y_pred)[0, 1]
    pred_std = np.std(y_pred)
    actual_std = np.std(y_true)

    print(f"\n  {label}:")
    print(f"    MAE:  {mae:.4f}")
    print(f"    RMSE: {rmse:.4f}")
    print(f"    Bias: {bias:+.4f}")
    print(f"    r:    {corr:.4f}")
    print(f"    Pred std: {pred_std:.2f}  Actual std: {actual_std:.2f}")

    print(f"    Error distribution:")
    for t in [1, 2, 3, 5, 10]:
        pct = (np.abs(err) <= t).mean() * 100
        print(f"      ±{t:>2} lbs: {pct:.1f}%")

    if df_test is not None:
        # By surface
        print(f"    By surface:")
        for surf in sorted(df_test["raceSurfaceName"].dropna().unique()):
            mask = df_test["raceSurfaceName"] == surf
            if mask.sum() > 50:
                m = np.mean(np.abs(err[mask]))
                c = np.corrcoef(y_true[mask], y_pred[mask])[0, 1]
                print(f"      {surf:<15}: MAE={m:.2f} r={c:.3f} n={mask.sum():,}")

        # By class
        print(f"    By class:")
        for cls in sorted(df_test["raceClass"].dropna().unique()):
            mask = df_test["raceClass"] == cls
            if mask.sum() > 50:
                m = np.mean(np.abs(err[mask]))
                c = np.corrcoef(y_true[mask], y_pred[mask])[0, 1]
                print(f"      Class {cls}: MAE={m:.2f} r={c:.3f} n={mask.sum():,}")

        # By rating band
        print(f"    By rating band:")
        bands = [(-999, 40, "<40"), (40, 60, "40-59"), (60, 80, "60-79"),
                 (80, 100, "80-99"), (100, 999, "100+")]
        for lo, hi, lbl in bands:
            mask = (y_true >= lo) & (y_true < hi)
            if mask.sum() > 30:
                m = np.mean(np.abs(err[mask]))
                b = np.mean(err[mask])
                print(f"      {lbl:>6}: MAE={m:.2f} bias={b:+.2f} n={mask.sum():,}")

    return {"mae": mae, "rmse": rmse, "bias": bias, "corr": corr}


def run_comparison():
    """Run A/B comparison between baseline and enhanced models."""
    print("=" * 70)
    print("A/B COMPARISON: Baseline vs CustomMetricsEngine-Enhanced")
    print("=" * 70)

    # Load and build features (this runs CustomMetricsEngine too)
    t0 = time.time()
    df = load_pipeline_output()
    print("\nLoading extra features from raw data...")
    raw_extra = load_raw_extra_cols()
    print(f"  Raw extra: {len(raw_extra):,} rows")
    df = build_features(df, raw_extra)
    print(f"  Data prep: {time.time() - t0:.1f}s")

    # Filter to valid target
    valid = df[
        df["timefigure"].notna()
        & (df["timefigure"] != 0)
        & df["timefigure"].between(-200, 200)
        & df["figure_final"].notna()
    ].copy()
    print(f"\nValid rows: {len(valid):,}")

    # Temporal split
    train_mask = valid["source_year"] <= 2023
    test_mask = valid["source_year"] >= 2024

    # Target encodings
    valid = _compute_target_encodings(valid, train_mask)

    target = "timefigure"
    y = valid[target].values
    y_train = y[train_mask]
    y_test = y[test_mask]

    # Sample weights
    sample_weights = np.ones_like(y_train, dtype=float)
    sample_weights += np.clip((y_train - 60) / 20, 0, 3)
    sample_weights = np.where(y_train < 40, 0.7, sample_weights)

    print(f"\nTrain: {train_mask.sum():,} rows (2015–2023)")
    print(f"Test:  {test_mask.sum():,} rows (2024+)")

    # ── Model A: Baseline features only ──
    baseline_cols = [c for c in BASELINE_FEATURES if c in valid.columns]
    print(f"\nModel A (baseline): {len(baseline_cols)} features")

    X_a = valid[baseline_cols].copy()
    for col in X_a.columns:
        X_a[col] = pd.to_numeric(X_a[col], errors="coerce").fillna(-999)

    pred_a, model_a = _train_blend(
        X_a[train_mask], y_train, X_a[test_mask], y_test,
        sample_weights, label="Model A (baseline)"
    )

    test_df = valid[test_mask].copy().reset_index(drop=True)
    metrics_a = _report_metrics(y_test, pred_a, "Model A (baseline)", test_df)

    # ── Model B: Baseline + CustomMetricsEngine features ──
    enhanced_features = CUSTOM_METRICS_FEATURES
    available_new = [c for c in enhanced_features if c in valid.columns]
    missing_new = [c for c in enhanced_features if c not in valid.columns]
    enhanced_cols = baseline_cols + available_new

    print(f"\nModel B (enhanced): {len(enhanced_cols)} features "
          f"({len(available_new)} new, {len(missing_new)} unavailable)")
    if missing_new:
        print(f"  Missing: {missing_new[:10]}{'...' if len(missing_new) > 10 else ''}")

    X_b = valid[enhanced_cols].copy()
    for col in X_b.columns:
        X_b[col] = pd.to_numeric(X_b[col], errors="coerce").fillna(-999)

    pred_b, model_b = _train_blend(
        X_b[train_mask], y_train, X_b[test_mask], y_test,
        sample_weights, label="Model B (enhanced)"
    )
    metrics_b = _report_metrics(y_test, pred_b, "Model B (enhanced)", test_df)

    # ── Comparison Summary ──
    print("\n" + "=" * 70)
    print("COMPARISON SUMMARY")
    print("=" * 70)

    for metric in ["mae", "rmse", "bias", "corr"]:
        a_val = metrics_a[metric]
        b_val = metrics_b[metric]
        if metric in ("mae", "rmse"):
            delta = b_val - a_val
            better = "BETTER" if delta < 0 else "WORSE" if delta > 0 else "SAME"
            print(f"  {metric.upper():>5}: A={a_val:.4f}  B={b_val:.4f}  "
                  f"Δ={delta:+.4f} ({better})")
        elif metric == "corr":
            delta = b_val - a_val
            better = "BETTER" if delta > 0 else "WORSE" if delta < 0 else "SAME"
            print(f"  {metric.upper():>5}: A={a_val:.4f}  B={b_val:.4f}  "
                  f"Δ={delta:+.4f} ({better})")
        else:
            print(f"  {metric.upper():>5}: A={a_val:+.4f}  B={b_val:+.4f}")

    # ── Feature importance for new features ──
    print(f"\n  Feature importance (Model B, top 30):")
    importance = pd.Series(
        model_b.feature_importances_, index=enhanced_cols
    ).sort_values(ascending=False)
    for feat, imp in importance.head(30).items():
        marker = " ★" if feat in available_new else ""
        print(f"    {feat:<35}: {imp:.4f}{marker}")

    print(f"\n  Top 15 NEW features by importance:")
    new_importance = importance[importance.index.isin(available_new)]
    for feat, imp in new_importance.head(15).items():
        print(f"    {feat:<35}: {imp:.4f}")

    # ── Feature coverage ──
    print(f"\n  Feature coverage (new features, non-null %):")
    for feat in new_importance.head(20).index:
        if feat in valid.columns:
            non_null = valid[feat].notna() & (valid[feat] != -999)
            pct = non_null.mean() * 100
            print(f"    {feat:<35}: {pct:.1f}%")


if __name__ == "__main__":
    run_comparison()
