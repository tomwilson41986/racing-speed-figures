"""Tests for the top-end compression fixes.

High figures were being regressed toward the mean by: a level-target GBR
that replaced the figure (cannot extrapolate, leaf-averages elite rows),
unconditional quantile mapping with unsafe cubic extrapolation, a final
rescale that re-compressed by construction, unshrunk class offsets, class
signal absorbed into going allowances, and France's unclipped class-excess
factor / whole-race ceiling nulling.  These tests pin the fixed behaviour.
"""

import numpy as np
import pandas as pd
import pytest

from src.speed_figures import (
    apply_recal_map,
    compute_band_scorecard,
    compute_ga_class_gradient,
    enhance_with_gbr,
    expand_scale,
)


class TestApplyRecalMap:
    ANCHORS_X = [10.0, 30.0, 50.0, 70.0, 90.0]
    ANCHORS_Y = [12.0, 31.0, 50.0, 72.0, 95.0]

    def test_interpolates_through_anchors(self):
        out = apply_recal_map(self.ANCHORS_X, self.ANCHORS_X, self.ANCHORS_Y)
        assert np.allclose(out, self.ANCHORS_Y)

    def test_monotone_between_anchors(self):
        xs = np.linspace(10, 90, 500)
        out = apply_recal_map(xs, self.ANCHORS_X, self.ANCHORS_Y)
        assert (np.diff(out) >= -1e-9).all()

    def test_slope_one_above_top_anchor(self):
        # A career-best 25 lbs above the training range must be carried
        # through at full value, not bent by cubic extrapolation.
        out = apply_recal_map([90.0, 115.0], self.ANCHORS_X, self.ANCHORS_Y)
        assert out[1] - out[0] == pytest.approx(25.0)

    def test_slope_one_below_bottom_anchor(self):
        out = apply_recal_map([10.0, -10.0], self.ANCHORS_X, self.ANCHORS_Y)
        assert out[0] - out[1] == pytest.approx(20.0)


class TestConditionalRecalibration:
    def _synthetic(self, n=30000, seed=11):
        rng = np.random.default_rng(seed)
        truth = rng.normal(60, 18, n)
        # Compressed predictions: only 65% of the deviation transmitted
        pred = 60 + 0.65 * (truth - 60) + rng.normal(0, 2, n)
        years = rng.choice([2020, 2021, 2022], n)
        df = pd.DataFrame(
            {
                "timefigure": truth,
                "figure_calibrated": pred,
                "source_year": years,
                "raceSurfaceName": "Turf",
            }
        )
        # A handful of 2023 rows so max_year-1 = 2022 is the fit window
        df.loc[df.index[-50:], "source_year"] = 2023
        return df

    def test_top_decile_conditional_bias_removed(self):
        df = self._synthetic()
        pred0 = df["figure_calibrated"].copy()
        top = pred0 >= pred0.quantile(0.9)
        bias_before = (
            df.loc[top, "figure_calibrated"] - df.loc[top, "timefigure"]
        ).mean()

        out, params = expand_scale(df)
        bias_after = (
            out.loc[top, "figure_calibrated"] - out.loc[top, "timefigure"]
        ).mean()

        assert abs(bias_before) > 4.0  # compression was real
        assert abs(bias_after) < 1.2   # and is now substantially removed
        assert "Turf" in params

    def test_extrapolation_is_linear_slope_one(self):
        df = self._synthetic()
        extreme_val = float(df["figure_calibrated"].max()) + 30.0
        extra = pd.DataFrame(
            {
                "timefigure": [np.nan],
                "figure_calibrated": [extreme_val],
                "source_year": [2023],
                "raceSurfaceName": ["Turf"],
            }
        )
        df = pd.concat([df, extra], ignore_index=True)

        out, params = expand_scale(df)
        anchors_x = params["Turf"]["pred_quantiles"]
        anchors_y = params["Turf"]["tf_quantiles"]
        expected = anchors_y[-1] + (extreme_val - anchors_x[-1])
        got = out["figure_calibrated"].iloc[-1]
        assert got == pytest.approx(expected, abs=1e-6)


class TestGaClassGradient:
    def test_gradient_signs_and_shrinkage(self):
        n = 300
        dev = pd.Series(
            np.concatenate([np.full(n, -0.10), np.full(n, 0.00)])
        )
        cls = pd.Series(["1"] * n + ["5"] * n)
        grad = compute_ga_class_gradient(dev, cls, k=200)
        # Overall mean -0.05; class-1 raw gap -0.05 shrunk by 300/500
        assert grad["1"] == pytest.approx(-0.05 * 300 / 500, abs=1e-9)
        assert grad["5"] == pytest.approx(+0.05 * 300 / 500, abs=1e-9)

    def test_unknown_class_gets_no_gradient(self):
        dev = pd.Series([-0.1, 0.0, 0.1, np.nan]).dropna()
        cls = pd.Series([np.nan, "3", "3"])
        grad = compute_ga_class_gradient(dev, cls, k=1)
        assert "unknown" not in grad

    def test_centring_leaves_mean_unchanged(self):
        rng = np.random.default_rng(3)
        dev = pd.Series(rng.normal(0.05, 0.1, 1000))
        cls = pd.Series(rng.choice(["1", "3", "5"], 1000))
        grad = compute_ga_class_gradient(dev, cls, k=0)
        # With k=0 the gradient is exactly the centred class means, so
        # the count-weighted average of the gradient is ~0.
        weights = cls.value_counts()
        weighted = sum(grad[c] * weights[c] for c in grad) / weights.sum()
        assert weighted == pytest.approx(0.0, abs=1e-9)


class TestResidualGbr:
    def _synthetic(self, n=4000, seed=5):
        rng = np.random.default_rng(seed)
        fig = rng.normal(60, 18, n)
        # True timefigure: figure plus a class-dependent bias the GBR
        # should learn, plus noise
        race_class = rng.integers(1, 8, n)
        truth = fig + (4 - race_class) * 1.5 + rng.normal(0, 2, n)
        df = pd.DataFrame(
            {
                "figure_calibrated": fig,
                "figure_final": fig,
                "timefigure": truth,
                "raceClass": race_class,
                "distance": 8.0,
                "horseAge": 4,
                "positionOfficial": rng.integers(1, 10, n),
                "weightCarried": 126.0,
                "ga_value": 0.0,
                "going": "Good",
                "courseName": "ASCOT",
                "numberOfRunners": 10,
                "draw": 0,
                "raceSurfaceName": "Turf",
                "source_year": rng.choice([2020, 2021, 2022, 2023], n),
            }
        )
        return df

    def test_career_best_keeps_its_level(self):
        df = self._synthetic()
        max_tf_train = df.loc[df["source_year"] <= 2022, "timefigure"].max()

        # A same-scale career-best far above anything in training —
        # under the old level-target/replace design the GBR could not
        # predict above max_tf_train, hard-capping this performance.
        extreme = pd.DataFrame([{
            "figure_calibrated": 200.0, "figure_final": 200.0,
            "timefigure": np.nan, "raceClass": 1, "distance": 8.0,
            "horseAge": 4, "positionOfficial": 1, "weightCarried": 126.0,
            "ga_value": 0.0, "going": "Good", "courseName": "ASCOT",
            "numberOfRunners": 10, "draw": 0, "raceSurfaceName": "Turf",
            "source_year": 2023,
        }])
        df = pd.concat([df, extreme], ignore_index=True)

        out, models = enhance_with_gbr(df)
        got = out["figure_calibrated"].iloc[-1]

        assert "Turf" in models
        # Level preserved up to a bounded correction — far above the
        # training-target ceiling that the old design imposed.
        assert got > max_tf_train + 20
        assert abs(got - 200.0) < 25  # correction stays bounded

    def test_learns_systematic_bias(self):
        df = self._synthetic()
        before = (df["figure_calibrated"] - df["timefigure"]).abs().mean()
        out, _ = enhance_with_gbr(df)
        after = (out["figure_calibrated"] - out["timefigure"]).abs().mean()
        assert after < before


class TestBandScorecard:
    def test_structure_and_oos_scope(self):
        rng = np.random.default_rng(9)
        truth = rng.uniform(10, 130, 5000)
        df = pd.DataFrame(
            {
                "timefigure": truth,
                "figure_calibrated": truth + rng.normal(0, 5, 5000),
                "source_year": rng.choice([2022, 2023, 2026], 5000),
            }
        )
        sc = compute_band_scorecard(df)
        assert set(sc.columns) == {
            "scope", "band", "n", "mae", "bias", "truth_on_pred_slope",
        }
        assert (sc["scope"] == "oos_2026").any()
        assert "100-120" in set(sc["band"])

    def test_empty_input(self):
        df = pd.DataFrame(
            {"timefigure": [], "figure_calibrated": [], "source_year": []}
        )
        assert len(compute_band_scorecard(df)) == 0


class TestFranceClassExcess:
    def test_elite_classes_are_not_amplified(self):
        from src.france.live_ratings import class_excess_compress_factor

        cls = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        factor = class_excess_compress_factor(cls)
        # Class 1/2 previously got 1.708 / 1.354 — excess AMPLIFIED.
        assert factor.iloc[0] == pytest.approx(1.0)
        assert factor.iloc[1] == pytest.approx(1.0)
        assert factor.iloc[2] == pytest.approx(1.0)
        # Lower classes still compressed, floored at zero
        assert factor.iloc[4] == pytest.approx(1.0 - 0.354 * 2)
        assert factor.iloc[5] == pytest.approx(0.0, abs=1e-9)
        assert factor.iloc[6] == pytest.approx(0.0)
        assert (factor <= 1.0).all() and (factor >= 0.0).all()


class TestFranceSanityGuard:
    def _guard(self, df):
        from src.france.live_ratings import FranceLiveRatingEngine

        eng = FranceLiveRatingEngine.__new__(FranceLiveRatingEngine)
        return eng._sanity_guard(df)

    def _race(self, rid, figs):
        return pd.DataFrame(
            {
                "race_id": [rid] * len(figs),
                "figure_calibrated": figs,
                "figure_comment": [""] * len(figs),
            }
        )

    def test_moderate_excess_capped_and_flagged_not_nulled(self):
        df = self._guard(self._race("A", [128.0, 110.0, 95.0]))
        # Race survives: capped at the ceiling, flagged for review
        assert df["figure_calibrated"].iloc[0] == pytest.approx(120.0)
        assert df["figure_calibrated"].iloc[1] == pytest.approx(110.0)
        assert df["figure_calibrated"].notna().all()
        assert (df["figure_comment"].str.contains("capped")).all()

    def test_implausible_race_still_nulled(self):
        df = self._guard(self._race("B", [224.0, 180.0, 150.0]))
        assert df["figure_calibrated"].isna().all()
        assert (df["figure_comment"].str.contains("excluded")).all()

    def test_normal_race_untouched(self):
        df = self._guard(self._race("C", [95.0, 88.0, 70.0]))
        assert df["figure_calibrated"].tolist() == [95.0, 88.0, 70.0]
        assert (df["figure_comment"] == "").all()
