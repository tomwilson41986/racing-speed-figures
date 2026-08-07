"""Regression tests for two live-pipeline defects found in the Aug-2026
Timeform calibration review.

1. Day/month transposition in the HRB racedate parser.  `format="mixed"` with
   `dayfirst=True` made pandas infer "%Y-%d-%m" for ISO strings, so every
   raceday with day-of-month <= 12 was parsed with day and month swapped.
   The resulting `month` indexes the WFA table and the sex-allowance season
   band, so figures on 31 of 112 audited racedays were built on the wrong
   month's weight-for-age allowance.

2. Going descriptions emitted by the live feed that were missing from
   GOING_GROUPS / GOING_ORDINAL and therefore silently defaulted to
   "Good" / 3 (7.82% of audited runners).
"""
import numpy as np
import pandas as pd
import pytest

from src.live_ratings import (
    GOING_MAP,
    GOING_ORDINAL,
    _resolve_newmarket_course,
    going_group,
    going_ordinal,
)
from src.speed_figures import get_wfa_allowance


def _parse(values):
    """Mirror the parser in HRBScraper.transform (live_ratings.py)."""
    s = pd.Series(values).astype(str).str.strip()
    iso = pd.to_datetime(s, format="ISO8601", errors="coerce")
    return iso.fillna(pd.to_datetime(s, dayfirst=True, format="mixed", errors="coerce"))


class TestRacedateParsing:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            # ISO input — the layout HRB actually supplies.  Every one of
            # these was previously transposed because day <= 12.
            ("2026-08-03", "2026-08-03"),   # was 2026-03-08
            ("2026-08-01", "2026-08-01"),   # was 2026-01-08
            ("2026-12-05", "2026-12-05"),   # was 2026-05-12
            ("2026-01-02", "2026-01-02"),   # was 2026-02-01
            # ISO with day > 12 was always parsed correctly; must stay so.
            ("2026-08-25", "2026-08-25"),
            ("2026-02-19", "2026-02-19"),
        ],
    )
    def test_iso_dates_are_never_transposed(self, raw, expected):
        assert _parse([raw]).iloc[0].strftime("%Y-%m-%d") == expected

    @pytest.mark.parametrize(
        "raw,expected",
        [
            # DD/MM/YYYY really is day-first: 2 of the 9 raw HRB files on
            # disk use this layout (results_2026-2-19.csv, -2-20.csv), so
            # dayfirst=True must be retained for non-ISO strings.
            ("19/02/2026", "2026-02-19"),
            ("05/02/2026", "2026-02-05"),
            ("20/02/2026", "2026-02-20"),
        ],
    )
    def test_dayfirst_retained_for_slash_layout(self, raw, expected):
        assert _parse([raw]).iloc[0].strftime("%Y-%m-%d") == expected

    def test_every_day_of_month_round_trips(self):
        # The defect only bit for day <= 12, which is why it survived.
        days = pd.date_range("2026-08-01", "2026-08-31", freq="D")
        got = _parse([d.strftime("%Y-%m-%d") for d in days])
        assert (got.dt.month == 8).all()
        assert list(got.dt.day) == list(days.day)

    def test_mixed_layouts_in_one_batch(self):
        got = _parse(["2026-08-03", "19/02/2026", "2026-08-25"])
        assert [d.strftime("%Y-%m-%d") for d in got] == [
            "2026-08-03", "2026-02-19", "2026-08-25",
        ]

    def test_unparseable_is_nat_not_a_wrong_date(self):
        assert pd.isna(_parse(["not-a-date"]).iloc[0])

    def test_live_and_batch_month_agree(self):
        # Train/serve skew: the batch pipeline (speed_figures.py) uses a plain
        # to_datetime and always got the month right; the live path did not.
        raw = ["2026-08-03", "2026-08-01", "2026-12-05", "2026-08-25"]
        live = _parse(raw).dt.month
        batch = pd.to_datetime(pd.Series(raw), errors="coerce").dt.month
        assert list(live) == list(batch)


def _hrb_frame(racedates, tracks=None, goings=None):
    """Minimal HRB-shaped frame accepted by _transform_hrb_data."""
    n = len(racedates)
    return pd.DataFrame({
        "racedate": racedates,
        "track": tracks or ["NOTTINGHAM"] * n,
        "going_description": goings or ["Good"] * n,
        "racetime": ["2.00."] * n, "Yards": [1320] * n, "RailMove": [0] * n,
        "Dist_Furlongs": [6] * n, "race_class": ["Class 4"] * n,
        "number_of_runners": [8] * n, "horse_name": [f"H{i}" for i in range(n)],
        "horse_age": [3] * n, "HorseSex": ["F"] * n, "pounds": [126] * n,
        "comptime_numeric": [72.0] * n, "TotalDstBt": [0] * n, "CardNo": [1] * n,
        "stall": [1] * n, "official_rating": [70] * n, "MedianOR": [70] * n,
        "MaxORinRace": [80] * n, "placing_numerical": [1] * n, "odds": [3.0] * n,
        "race_name": ["x"] * n, "surfacetype": [0] * n, "RaceType": [0] * n,
    })


class TestTransformIntegration:
    """Exercise the real _transform_hrb_data, not a re-implementation of it."""

    def test_month_is_correct_through_the_real_transform(self):
        from src.live_ratings import _transform_hrb_data
        out = _transform_hrb_data(
            _hrb_frame(["2026-08-03", "19/02/2026", "2026-08-25", "2026-08-01"],
                       tracks=["NOTTINGHAM", "RIPON", "NEWMARKET", "NEWMARKET"])
        )
        assert list(out["meetingDate"]) == [
            "2026-08-03", "2026-02-19", "2026-08-25", "2026-08-01",
        ]

    def test_newmarket_resolves_by_the_corrected_date(self):
        # 2026-08-01 previously became month 1 and resolved to ROWLEY — a
        # different track with different standard times.
        from src.live_ratings import _transform_hrb_data
        out = _transform_hrb_data(
            _hrb_frame(["2026-08-01", "2026-08-25"], tracks=["NEWMARKET"] * 2)
        )
        assert list(out["courseName"]) == ["NEWMARKET (JULY)"] * 2

    def test_race_id_carries_the_correct_date(self):
        # The published audit for 3 Aug 2026 labelled its races
        # "2026-03-08_NOTTINGHAM_1" — the bug was visible in the artifact.
        from src.live_ratings import _transform_hrb_data
        out = _transform_hrb_data(_hrb_frame(["2026-08-03"]))
        ids = [c for c in out.columns if c.endswith("_id")]
        for c in ids:
            assert "2026-03-08" not in str(out[c].iloc[0])


class TestWfaMonthConsequence:
    def test_august_2yo_allowance_is_not_the_march_one(self):
        # The concrete client-visible symptom: a 2yo at Nottingham over 6f on
        # 2026-08-03 was given the MARCH allowance (~28.6 lb) instead of the
        # August one (~13.8 lb).
        march = get_wfa_allowance(2, 3, 6.0818, "Turf")
        august = get_wfa_allowance(2, 8, 6.0818, "Turf")
        assert march > august + 10
        month = _parse(["2026-08-03"]).iloc[0].month
        assert month == 8
        assert get_wfa_allowance(2, month, 6.0818, "Turf") == pytest.approx(august)

    def test_january_2yo_lookup_returns_zero(self):
        # On 2026-08-01 the bug produced month 1, which WFA_2YO_TURF has no
        # key for, so all 72 two-year-olds got exactly 0.0 WFA.
        assert get_wfa_allowance(2, 1, 6.0, "Turf") == 0.0
        assert get_wfa_allowance(2, 8, 6.0, "Turf") > 0.0


class TestNewmarketResolverConsequence:
    def test_august_resolves_to_july_course(self):
        # meetingDate also feeds the Rowley/July resolver.  With the bug,
        # 2026-08-01 became month 1 and a bare NEWMARKET resolved to ROWLEY —
        # a different track with different standard times.
        assert _resolve_newmarket_course("2026-08-01") == "NEWMARKET (JULY)"
        assert _resolve_newmarket_course("2026-01-08") == "NEWMARKET (ROWLEY)"


class TestGoingVocabulary:
    # Labels observed in output/uk_audit/* that used to be unmapped.
    OBSERVED_UNMAPPED = {
        "Good To Soft": ("GdSft", 4),
        "Soft To Heavy": ("Soft", 5),
        "Yielding To Soft": ("Soft", 5),
    }

    @pytest.mark.parametrize("label", sorted(OBSERVED_UNMAPPED))
    def test_previously_unmapped_labels_are_mapped(self, label):
        grp, ordv = self.OBSERVED_UNMAPPED[label]
        assert GOING_MAP[label] == grp
        assert GOING_ORDINAL[label] == ordv
        assert going_group(pd.Series([label])).iloc[0] == grp
        assert going_ordinal(pd.Series([label])).iloc[0] == ordv

    def test_soft_ground_is_not_scored_as_good(self):
        # The direction that mattered: soft ground was being fed to the
        # calibration as "Good" and to the GBR as ordinal 3.
        for label in self.OBSERVED_UNMAPPED:
            assert going_group(pd.Series([label])).iloc[0] != "Good"
            assert going_ordinal(pd.Series([label])).iloc[0] > 3

    @pytest.mark.parametrize(
        "variant", ["good to soft", "GOOD TO SOFT", "  Good To Soft  ", "Good to Soft"]
    )
    def test_lookup_is_case_and_whitespace_insensitive(self, variant):
        assert going_group(pd.Series([variant])).iloc[0] == "GdSft"
        assert going_ordinal(pd.Series([variant])).iloc[0] == 4

    def test_two_tables_cover_the_same_vocabulary(self):
        # A label present in one table but not the other is exactly how this
        # bug arose.
        assert set(GOING_MAP) == set(GOING_ORDINAL)

    def test_group_and_ordinal_are_consistent(self):
        order = {"Firm": 1, "GdFm": 2, "Good": 3, "GdSft": 4, "Soft": 5, "Heavy": 6}
        for label, grp in GOING_MAP.items():
            assert GOING_ORDINAL[label] == order[grp], label

    def test_unknown_label_still_defaults_but_is_reported(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger="live_ratings"):
            assert going_group(pd.Series(["Sloppy Mud"])).iloc[0] == "Good"
        assert "Sloppy Mud" in caplog.text

    def test_ga_prior_and_group_tables_agree_on_vocabulary(self):
        # GOING_GA_PRIOR already had all three labels, which is why the going
        # ALLOWANCE was right while the calibration group was wrong.
        from src.speed_figures import GOING_GA_PRIOR
        for label in GOING_GA_PRIOR:
            assert label in GOING_MAP, f"{label} in GA prior but not GOING_MAP"
