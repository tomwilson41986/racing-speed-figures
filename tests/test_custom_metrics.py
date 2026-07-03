"""Unit tests for custom_metrics: form-trajectory sign and RSR lag-safety."""

import numpy as np
import pandas as pd
import pytest

from src.custom_metrics import CustomMetricsEngine


def _base_df(rows):
    df = pd.DataFrame(rows)
    df["race_time"] = df.get("race_time", 1)
    return df


class TestFormTrajectorySign:
    def _horse(self, nfps):
        rows = []
        for i, nfp in enumerate(nfps):
            rows.append(
                {
                    "horse_name": "TESTER",
                    "race_date": f"2026-01-{i + 1:02d}",
                    "race_time": 1,
                    "NFP": nfp,
                }
            )
        return _base_df(rows)

    def test_improving_horse_has_positive_slope(self):
        # NFP rising run by run: 0.1 → 0.5 → 0.9, then today's race.
        df = self._horse([0.1, 0.5, 0.9, 0.5])
        out = CustomMetricsEngine()._calc_form_trajectory(df)
        last = out.iloc[-1]
        assert last["form_slope_3"] > 0, (
            "an improving horse must have a positive form slope"
        )
        assert last["is_improving"] == 1
        assert last["is_declining"] == 0

    def test_declining_horse_has_negative_slope(self):
        df = self._horse([0.9, 0.5, 0.1, 0.5])
        out = CustomMetricsEngine()._calc_form_trajectory(df)
        last = out.iloc[-1]
        assert last["form_slope_3"] < 0
        assert last["is_declining"] == 1
        assert last["is_improving"] == 0


class TestRSRLagSafety:
    def _race_df(self, race_times, current_time=70.0):
        """One (track, dist, going) group; one race per day, 2 runners each."""
        rows = []
        for i, t in enumerate(race_times):
            for runner in ("A", "B"):
                rows.append(
                    {
                        "raceid": f"R{i}",
                        "horse_name": f"{runner}{i}",
                        "race_date": f"2026-01-{i + 1:02d}",
                        "race_time": 1,
                        "track": "Ascot",
                        "going_description": "Good",
                        "dist_furlongs": 8.0,
                        "comptime_numeric": t,
                    }
                )
        # The race under test, on a later day.
        for runner in ("X", "Y"):
            rows.append(
                {
                    "raceid": "RTEST",
                    "horse_name": runner,
                    "race_date": "2026-02-01",
                    "race_time": 1,
                    "track": "Ascot",
                    "going_description": "Good",
                    "dist_furlongs": 8.0,
                    "comptime_numeric": current_time,
                }
            )
        return _base_df(rows)

    def test_early_races_have_no_rsr(self):
        # With fewer prior races than the minimum par sample, no RSR.
        df = self._race_df([72.0, 71.0, 73.0])
        out = CustomMetricsEngine()._calc_speed_figures(df)
        assert out["RSR"].isna().all()

    def test_par_uses_only_prior_races(self):
        prior_times = [72.0, 71.0, 73.0, 72.5, 71.5]  # median 72.0
        df = self._race_df(prior_times, current_time=70.0)
        out = CustomMetricsEngine()._calc_speed_figures(df)
        test_rsr = out.loc[out["raceid"] == "RTEST", "RSR"]
        expected = (72.0 - 70.0) / 72.0 * 100
        assert test_rsr.iloc[0] == pytest.approx(expected)

    def test_own_race_time_does_not_move_own_par(self):
        # An extreme time in the current race must not change its own RSR
        # baseline — this is exactly the leak the old transform("median")
        # implementation had.
        prior_times = [72.0, 71.0, 73.0, 72.5, 71.5]
        slow = self._race_df(prior_times, current_time=100.0)
        out = CustomMetricsEngine()._calc_speed_figures(slow)
        test_rsr = out.loc[out["raceid"] == "RTEST", "RSR"]
        expected = (72.0 - 100.0) / 72.0 * 100
        assert test_rsr.iloc[0] == pytest.approx(expected)

    def test_future_races_do_not_leak_into_past_pars(self):
        prior_times = [72.0, 71.0, 73.0, 72.5, 71.5]
        df_a = self._race_df(prior_times, current_time=70.0)

        # Same data plus a wildly slow FUTURE race.
        extra = df_a.iloc[-2:].copy()
        extra["raceid"] = "RFUTURE"
        extra["race_date"] = "2026-03-01"
        extra["comptime_numeric"] = 200.0
        df_b = pd.concat([df_a, extra], ignore_index=True)

        rsr_a = (
            CustomMetricsEngine()
            ._calc_speed_figures(df_a)
            .set_index("raceid")["RSR"]
        )
        rsr_b = (
            CustomMetricsEngine()
            ._calc_speed_figures(df_b)
            .set_index("raceid")["RSR"]
        )
        assert rsr_a.loc["RTEST"].iloc[0] == pytest.approx(
            rsr_b.loc["RTEST"].iloc[0]
        )
