"""Unit tests for sectionals name/track/going/distance normalisation."""

import pytest

from src.sectionals.normalize import (
    distance_band_m,
    distance_to_m,
    going_group,
    normalise_name,
    normalise_track,
)


class TestNormaliseName:
    def test_strips_country_suffix_and_punctuation(self):
        # Punctuation is replaced with a space (both sides of every join
        # are normalised the same way, so internal consistency is what
        # matters here).
        assert normalise_name("Zarak's Dream (FR)") == "ZARAK S DREAM"

    def test_strips_accents(self):
        assert normalise_name("Élégante (IRE)") == "ELEGANTE"

    def test_empty(self):
        assert normalise_name("") == ""
        assert normalise_name(None) == ""


class TestGoingGroup:
    @pytest.mark.parametrize(
        "going,expected",
        [
            ("Good to Soft", "Good-Soft"),
            ("GOOD TO FIRM", "Good-Firm"),
            ("Good", "Good"),
            ("Heavy", "Heavy"),
            ("Lourd", "Heavy"),
            ("Tres Souple", "Heavy"),
            ("Souple", "Soft"),
            ("Standard To Slow", "Good"),
            ("", "Unknown"),
        ],
    )
    def test_groups(self, going, expected):
        assert going_group(going) == expected


class TestDistances:
    def test_furlongs_to_metres(self):
        assert distance_to_m(8, "f") == pytest.approx(1609.344)

    def test_yards_to_metres(self):
        assert distance_to_m(1000, "y") == pytest.approx(914.4)

    def test_banding(self):
        assert distance_band_m(1609.344) == 1600
        assert distance_band_m(1651.0) == 1700
        assert distance_band_m(None) is None


class TestNormaliseTrack:
    def test_upper_and_accent_free(self):
        assert normalise_track("Deauville") == "DEAUVILLE"
        assert normalise_track("Saint-Cloud") == "SAINT CLOUD"
