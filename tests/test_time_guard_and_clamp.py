"""Two guards that keep a single bad input from producing an absurd figure.

Both are anchored on races that actually reached the daily email:

* Fairyhouse 2026-05-28, 10f recorded in 322.50s against a 137.68s standard,
  and Leopardstown 2026-05-10, 8f in 74.17s — faster than any mile ever run.
  These became figures of -1748 and +541.
* Those figures then met an unclamped quadratic, which mapped them to roughly
  -11,956 and +236. The batch pipeline clamps the curvature term to the fitted
  range; live did not, and the range was never written to the artifacts.
"""

import numpy as np
import pandas as pd
import pytest

from src.speed_figures import MAX_TIME_DEVIATION_FRAC, drop_implausible_times


def _winners(rows):
    """Build the winner frame the guard sees, with deviation precomputed."""
    df = pd.DataFrame(rows)
    df["deviation_seconds"] = df["finishingTime"] - df["standard_time"]
    return df


NORMAL = {
    "courseName": "ASCOT", "distance": 8.0,
    "finishingTime": 100.0, "standard_time": 103.0,
}


class TestTimePlausibilityGuard:
    def test_keeps_an_ordinary_winner(self):
        out = drop_implausible_times(_winners([NORMAL]))
        assert len(out) == 1

    def test_keeps_a_genuinely_slow_race_on_heavy_ground(self):
        # +12% is inside the observed 0.1st-99.9th percentile band, so soft
        # ground must survive: the guard is for bad data, not slow races.
        slow = {**NORMAL, "finishingTime": 103.0 * 1.12}
        assert len(drop_implausible_times(_winners([slow]))) == 1

    def test_drops_the_fairyhouse_time(self):
        bad = {"courseName": "FAIRYHOUSE", "distance": 9.9545,
               "finishingTime": 322.50, "standard_time": 137.6764}
        assert len(drop_implausible_times(_winners([bad]))) == 0

    def test_drops_the_leopardstown_time(self):
        bad = {"courseName": "LEOPARDSTOWN", "distance": 8.0,
               "finishingTime": 74.17, "standard_time": 109.3513}
        assert len(drop_implausible_times(_winners([bad]))) == 0

    def test_drops_only_the_bad_row_from_a_mixed_card(self):
        bad = {"courseName": "FAIRYHOUSE", "distance": 9.9545,
               "finishingTime": 322.50, "standard_time": 137.6764}
        out = drop_implausible_times(_winners([NORMAL, bad, NORMAL]))
        assert len(out) == 2
        assert set(out["courseName"]) == {"ASCOT"}

    def test_threshold_is_symmetric(self):
        over = 103.0 * (1 + MAX_TIME_DEVIATION_FRAC + 0.01)
        under = 103.0 * (1 - MAX_TIME_DEVIATION_FRAC - 0.01)
        for t in (over, under):
            assert len(drop_implausible_times(_winners([{**NORMAL,
                                                         "finishingTime": t}]))) == 0

    def test_empty_and_missing_standard_time_are_passed_through(self):
        assert len(drop_implausible_times(pd.DataFrame())) == 0
        no_std = pd.DataFrame([{"courseName": "X", "deviation_seconds": 1.0}])
        assert len(drop_implausible_times(no_std)) == 1


def _calibrate(x, params):
    """The live curvature branch, isolated (mirrors _apply_full_calibration)."""
    a, a2, x_mean, b = params["a"], params["a2"], params["x_mean"], params["b"]
    lo = params.get("fit_lo", -np.inf)
    hi = params.get("fit_hi", np.inf)
    return a * x + a2 * (np.clip(x, lo, hi) - x_mean) ** 2 + b


# The shipped Turf calibration, whose parabola peaks at figure_final ~290.
TURF = {"a": 0.6266, "b": -56.12, "a2": -0.002904, "x_mean": 182.4,
        "fit_lo": 65.5, "fit_hi": 310.0}


class TestQuadraticClamp:
    def test_inside_the_fitted_range_is_unchanged_by_clamping(self):
        for x in (100.0, 182.4, 250.0, 309.0):
            unclamped = (TURF["a"] * x + TURF["a2"] * (x - TURF["x_mean"]) ** 2
                         + TURF["b"])
            assert _calibrate(x, TURF) == pytest.approx(unclamped)

    def test_beyond_the_range_the_figure_keeps_rising(self):
        # Unclamped, the parabola turns over past ~290, so a career best scores
        # LOWER than an ordinary run. Clamped, it stays monotone.
        assert _calibrate(541.2, TURF) > _calibrate(310.0, TURF)
        assert _calibrate(400.0, TURF) > _calibrate(300.0, TURF)

    def test_beyond_the_range_it_follows_the_linear_slope(self):
        step = _calibrate(420.0, TURF) - _calibrate(400.0, TURF)
        assert step == pytest.approx(TURF["a"] * 20.0)

    def test_the_runaway_negative_is_contained(self):
        # figure_final -1747.7 (Fairyhouse) mapped to about -11,956 unclamped.
        clamped = _calibrate(-1747.6942, TURF)
        unclamped = (TURF["a"] * -1747.6942
                     + TURF["a2"] * (-1747.6942 - TURF["x_mean"]) ** 2
                     + TURF["b"])
        assert unclamped < -10_000
        assert clamped > -1_200

    def test_artifacts_without_a_range_keep_the_old_behaviour(self):
        legacy = {k: v for k, v in TURF.items() if k not in ("fit_lo", "fit_hi")}
        x = 541.2
        expected = (TURF["a"] * x + TURF["a2"] * (x - TURF["x_mean"]) ** 2
                    + TURF["b"])
        assert _calibrate(x, legacy) == pytest.approx(expected)


def test_calibrate_figures_publishes_the_range_it_fitted_over():
    """The clamp is only possible if the range reaches the artifacts."""
    from src.speed_figures import calibrate_figures

    rng = np.random.default_rng(0)
    n = 8000
    df = pd.DataFrame({
        "figure_final": rng.normal(180, 25, n),
        "raceSurfaceName": "Turf",
        "raceClass": rng.integers(1, 7, n).astype(float),
        "courseName": "ASCOT",
        "distance": rng.choice([6.0, 8.0, 10.0], n),
        "going": "Good",
        "ga_value": rng.normal(0, 0.2, n),
        "distanceCumulative": rng.uniform(0, 15, n),
        "positionOfficial": rng.integers(1, 10, n).astype(float),
        "horseAge": rng.integers(2, 8, n).astype(float),
        "source_year": rng.integers(2015, 2021, n),
        "ga_is_segment": False,
    })
    x = df["figure_final"]
    # Genuinely curved, so the quadratic branch is the one taken.
    df["timefigure"] = 0.63 * x - 0.002 * (x - 180) ** 2 - 56 + rng.normal(0, 6, n)

    _, params = calibrate_figures(df.copy())
    p = params["Turf"]
    lo, hi = p["fit_lo"], p["fit_hi"]
    assert p["a2"] != 0, "expected the quadratic branch"
    assert np.isfinite(lo) and np.isfinite(hi) and lo < hi
    # The range must bound the data the quadratic was actually fitted on.
    trained = df[df["source_year"] <= df["source_year"].max() - 1]["figure_final"]
    assert lo <= trained.min() + 1e-6
    assert hi >= trained.max() - 1e-6


class TestCalibrationOffsetKeys:
    """Batch fits these offsets and live looks them up.

    A mismatch between the two is silent — every lookup misses, ``fillna(0)``
    swallows it, and the figures come out uncorrected. That has already happened
    once, when live matched class offsets on "4" against batch keys written as
    "4.0" and dropped all of them, a ±20 lb swing.
    """

    @pytest.fixture
    def frame(self):
        return pd.DataFrame({
            "courseName": ["ASCOT", "ASCOT", "YORK", "CURRAGH"],
            "distance": [7.6, 8.0, 12.0, 10.2],
            "raceClass": [4.0, 4.0, 2.0, np.nan],
            "meetingDate": ["2026-02-14", "2026-08-05", "2026-08-05",
                            "2026-11-30"],
        })

    def test_course_distance_key_matches_the_historic_format(self, frame):
        from src.speed_figures import calibration_offset_keys

        keys = calibration_offset_keys(frame)
        # The artifacts in the repo are keyed this way; changing it silently
        # orphans every offset fitted before the change.
        assert list(keys["course_dist"]) == ["ASCOT_8", "ASCOT_8", "YORK_12",
                                             "CURRAGH_10"]

    def test_class_split_buckets_a_missing_class_separately(self, frame):
        from src.speed_figures import calibration_offset_keys

        keys = calibration_offset_keys(frame)
        # Irish cards carry no class; they must not inherit some other class's
        # offset by defaulting to 0.
        assert list(keys["course_dist_class"]) == [
            "ASCOT_8|4", "ASCOT_8|4", "YORK_12|2", "CURRAGH_10|NA",
        ]

    def test_quarter_split_follows_the_calendar(self, frame):
        from src.speed_figures import calibration_offset_keys

        keys = calibration_offset_keys(frame)
        assert list(keys["course_dist_quarter"]) == [
            "ASCOT_8|Q1", "ASCOT_8|Q3", "YORK_12|Q3", "CURRAGH_10|Q4",
        ]

    def test_the_same_race_keys_identically_either_side(self, frame):
        """The batch fit and the live lookup must agree character for character."""
        from src.speed_figures import calibration_offset_keys as batch_side
        from src.live_ratings import calibration_offset_keys as live_side

        batch, live = batch_side(frame), live_side(frame)
        assert set(batch) == set(live)
        for name, series in batch.items():
            assert list(series) == list(live[name]), name

    def test_a_missing_distance_does_not_raise(self, frame):
        from src.speed_figures import calibration_offset_keys

        frame.loc[0, "distance"] = np.nan
        keys = calibration_offset_keys(frame)
        # Historically .astype(int) raised outright. Now the key is simply
        # null, so the lookup misses and the runner takes no offset — which is
        # the right answer for a race whose distance we do not know.
        assert pd.isna(keys["course_dist"].iloc[0])
        assert keys["course_dist"].iloc[1] == "ASCOT_8"
        assert pd.Series([np.nan]).map({"ASCOT_8": 1.0}).fillna(0).iloc[0] == 0.0
