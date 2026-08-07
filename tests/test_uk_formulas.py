"""Unit tests for the UK figure formula chain (lpl, WFA, interpolation)."""

import numpy as np
import pandas as pd
import pytest

from src.speed_figures import (
    BENCHMARK_FURLONGS,
    GOING_GA_PRIOR,
    LBS_PER_SECOND_5F,
    SECONDS_PER_LENGTH,
    generic_lbs_per_length,
    get_wfa_allowance,
    interpolate_lookup,
)


class TestGenericLbsPerLength:
    def test_anchor_distance(self):
        # At the 5f benchmark: 0.2 s/L × 20 lbs/s = 4 lbs per length.
        assert generic_lbs_per_length(5.0) == pytest.approx(
            SECONDS_PER_LENGTH * LBS_PER_SECOND_5F
        )

    def test_inverse_distance_scaling(self):
        assert generic_lbs_per_length(10.0) == pytest.approx(
            generic_lbs_per_length(5.0) / 2
        )

    def test_aw_surface_multiplier(self):
        turf = generic_lbs_per_length(8.0, "Turf")
        aw = generic_lbs_per_length(8.0, "All Weather")
        assert aw == pytest.approx(turf * 1.10)


class TestWfaAllowance:
    def test_mature_horse_no_allowance(self):
        assert get_wfa_allowance(4, 6, 8.0) == 0.0
        assert get_wfa_allowance(5, 1, 12.0) == 0.0

    def test_older_horse_decline_turf_only(self):
        assert get_wfa_allowance(7, 6, 8.0, "Turf") == -1
        assert get_wfa_allowance(7, 6, 8.0, "All Weather") == 0.0

    def test_3yo_early_season_matches_table(self):
        assert get_wfa_allowance(3, 1, 5.0, "Turf") == 13

    def test_3yo_distance_interpolation(self):
        # Month 1 turf: 8f=12, 10f=10 → 9f interpolates to 11.
        assert get_wfa_allowance(3, 1, 9.0, "Turf") == pytest.approx(11.0)

    def test_distance_clamped_to_table_range(self):
        # Below the shortest bracket → shortest bracket's value.
        assert get_wfa_allowance(3, 1, 4.5, "Turf") == 13

    def test_missing_inputs_are_zero(self):
        assert get_wfa_allowance(np.nan, 6, 8.0) == 0.0
        assert get_wfa_allowance(3, np.nan, 8.0) == 0.0
        assert get_wfa_allowance(3, 6, np.nan) == 0.0


class TestInterpolateLookup:
    def _df(self, distances):
        return pd.DataFrame(
            {
                "courseName": ["ASCOT"] * len(distances),
                "raceSurfaceName": ["Turf"] * len(distances),
                "distance": distances,
            }
        )

    def test_linear_interpolation_between_buckets(self):
        lookup = {"ASCOT_5.0_Turf": 60.0, "ASCOT_6.0_Turf": 72.0}
        out = interpolate_lookup(self._df([5.5]), lookup)
        assert out.iloc[0] == pytest.approx(66.0)

    def test_clamps_small_extrapolation_within_tolerance(self):
        # Within the 5% guard we still clamp to the nearest known value.
        lookup = {"ASCOT_5.0_Turf": 60.0, "ASCOT_6.0_Turf": 72.0}
        out = interpolate_lookup(self._df([4.8, 6.25]), lookup)
        assert out.iloc[0] == pytest.approx(60.0)
        assert out.iloc[1] == pytest.approx(72.0)

    def test_far_outside_known_range_is_nan(self):
        # Previously these CLAMPED, so a race far off the distance grid
        # silently borrowed the nearest standard time however distant it was
        # (e.g. a 2m race scored against a 1m6f standard).  Because
        # raw_figure = 100 - (corrected_time - standard_time)/0.2 * lpl, that
        # is a tens-to-hundreds-of-pounds error, not a rounding error.
        # Now guarded at 5%, matching src/france/speed_figures.py.
        lookup = {"ASCOT_5.0_Turf": 60.0, "ASCOT_6.0_Turf": 72.0}
        out = interpolate_lookup(self._df([4.0, 7.0]), lookup)
        assert np.isnan(out.iloc[0])
        assert np.isnan(out.iloc[1])

    def test_single_known_distance_is_guarded_too(self):
        # A course/surface with only one known distance must not supply that
        # value to an arbitrarily different distance.
        lookup = {"ASCOT_8.0_Turf": 100.0}
        out = interpolate_lookup(self._df([8.2, 16.0]), lookup)
        assert out.iloc[0] == pytest.approx(100.0)
        assert np.isnan(out.iloc[1])

    def test_uk_guard_matches_france(self):
        # The French pipeline has always had this guard; the UK one is now
        # identical.  live_ratings.py imports the UK version, so batch and
        # live share it.
        from src.france.speed_figures import _interp_single as fr_interp
        from src.speed_figures import _interp_single as uk_interp
        pairs = [(5.0, 60.0), (6.0, 72.0)]
        for d in (4.0, 4.8, 5.5, 6.25, 7.0):
            a, b = uk_interp(d, pairs), fr_interp(d, pairs)
            assert (np.isnan(a) and np.isnan(b)) or a == pytest.approx(b)

    def test_unknown_course_is_nan(self):
        lookup = {"YORK_5.0_Turf": 58.0}
        out = interpolate_lookup(self._df([5.0]), lookup)
        assert np.isnan(out.iloc[0])


class TestGoingPriorSingleSource:
    def test_live_estimates_alias_the_pipeline_prior(self):
        # live_ratings previously carried a hand-maintained copy of this
        # table; it must never drift from the pipeline prior.  (Equality,
        # not identity: live_ratings imports speed_figures under its own
        # module name via a sys.path insert, so the dict object differs.)
        from src.live_ratings import GOING_GA_ESTIMATES

        assert GOING_GA_ESTIMATES == GOING_GA_PRIOR


class TestNewmarketResolution:
    def test_spring_and_autumn_resolve_to_rowley(self):
        from src.live_ratings import _resolve_newmarket_course

        assert _resolve_newmarket_course("2026-05-02") == "NEWMARKET (ROWLEY)"
        assert _resolve_newmarket_course("2026-10-10") == "NEWMARKET (ROWLEY)"

    def test_summer_resolves_to_july_course(self):
        from src.live_ratings import _resolve_newmarket_course

        assert _resolve_newmarket_course("2026-07-11") == "NEWMARKET (JULY)"
        assert _resolve_newmarket_course("2026-08-01") == "NEWMARKET (JULY)"
        assert _resolve_newmarket_course("2026-06-25") == "NEWMARKET (JULY)"

    def test_unparseable_date_returns_none(self):
        from src.live_ratings import _resolve_newmarket_course

        assert _resolve_newmarket_course("not-a-date") is None
        assert _resolve_newmarket_course(None) is None
