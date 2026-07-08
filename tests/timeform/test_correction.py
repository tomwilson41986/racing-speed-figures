"""Guardrail tests for the rolling correction."""

import pandas as pd

from src.timeform.correction import CorrectionConfig, compute_correction

CFG = CorrectionConfig()  # defaults: window 14, min_days 7, min_sample 200, thr 3, cap 6


def _history(n_days: int, n_per_day: int, bias: float, start_day: int = 1) -> pd.DataFrame:
    return pd.DataFrame([
        {"date": f"2026-07-{start_day + i:02d}", "n_matched": n_per_day, "bias": bias}
        for i in range(n_days)
    ])


def test_empty_history():
    out = compute_correction(pd.DataFrame(columns=["date", "n_matched", "bias"]), CFG)
    assert out["apply"] is False


def test_below_min_days():
    out = compute_correction(_history(3, 100, 8.0), CFG)
    assert out["apply"] is False
    assert "days" in out["reason"]


def test_below_min_sample():
    # 8 days present (>= min_days) but only 80 matched runners total (< 200).
    out = compute_correction(_history(8, 10, 8.0), CFG)
    assert out["apply"] is False
    assert "matched" in out["reason"]


def test_below_threshold_is_noise():
    out = compute_correction(_history(10, 60, 1.5), CFG)
    assert out["apply"] is False
    assert "noise" in out["reason"]
    assert out["bias_hat"] == 1.5


def test_applies_and_caps_positive():
    out = compute_correction(_history(10, 60, 9.0), CFG)
    assert out["apply"] is True
    assert out["bias_applied"] == 6.0        # clipped to +cap
    assert out["bias_hat"] == 9.0
    assert out["scale_applied"] == 1.0


def test_applies_and_caps_negative():
    out = compute_correction(_history(10, 60, -8.0), CFG)
    assert out["apply"] is True
    assert out["bias_applied"] == -6.0       # clipped to -cap


def test_applies_within_cap_uncapped():
    out = compute_correction(_history(10, 60, 4.0), CFG)
    assert out["apply"] is True
    assert out["bias_applied"] == 4.0        # below cap, applied as-is


def test_window_limits_to_recent_days():
    # 20 days of bias 8, but only the last 14 are pooled; still applies.
    out = compute_correction(_history(20, 60, 8.0), CFG)
    assert out["days_used"] == 14
    assert out["apply"] is True
