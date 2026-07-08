"""Unit tests for the OOS-only accuracy panel."""

import numpy as np
import pandas as pd
import pytest

from src.reporting import accuracy


def _figures_csv(tmp_path, n_train=500, n_oos=200):
    rng = np.random.default_rng(7)
    rows = []
    # Training-window years: near-perfect agreement (in-sample flattery).
    for _ in range(n_train):
        tf = float(rng.uniform(20, 120))
        rows.append(
            {"timefigure": tf, "figure_calibrated": tf + rng.normal(0, 1),
             "source_year": 2023}
        )
    # Current (OOS) year: larger errors.
    for _ in range(n_oos):
        tf = float(rng.uniform(20, 120))
        rows.append(
            {"timefigure": tf, "figure_calibrated": tf + rng.normal(0, 8),
             "source_year": 2026}
        )
    path = tmp_path / "speed_figures.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


class TestComputeFromFigures:
    def test_headline_is_oos_only(self, tmp_path):
        path = _figures_csv(tmp_path)
        m = accuracy.compute_from_figures(path)
        assert m["window"] == "out-of-sample 2026"
        assert m["in_sample"] is False
        assert m["n"] == 200
        # MAE must reflect the noisy OOS year, not the flattering
        # in-sample years (σ=8 half-normal mean ≈ 6.4).
        assert m["mae"] > 3.0

    def test_falls_back_to_in_sample_when_oos_thin(self, tmp_path):
        path = _figures_csv(tmp_path, n_train=500, n_oos=10)
        m = accuracy.compute_from_figures(path)
        assert m["in_sample"] is True
        assert "in-sample" in m["window"]
        assert m["n"] == 510
