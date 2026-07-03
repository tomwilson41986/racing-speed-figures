"""Unit tests for the French beaten-length parser and cumulative margins."""

import numpy as np
import pandas as pd
import pytest

from src.france.beaten_lengths import compute_cumulative_bl, parse_beaten_length


class TestParseBeatenLength:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("NEZ", 0.03),
            ("TETE", 0.1),
            ("TÊTE", 0.1),
            ("CTE TETE", 0.05),
            ("C.T.", 0.05),
            ("ENCOLURE", 0.25),
            ("ENC", 0.25),
            ("CTE ENCOLURE", 0.15),
            ("LOIN", 30.0),
            ("1/2", 0.5),
            ("3/4", 0.75),
            ("1 L 1/2", 1.5),
            ("2 L", 2.0),
            ("5", 5.0),
            ("2.5", 2.5),
        ],
    )
    def test_known_tokens(self, text, expected):
        assert parse_beaten_length(text) == pytest.approx(expected)

    def test_winner_empty_is_zero(self):
        assert parse_beaten_length(None) == 0.0
        assert parse_beaten_length("") == 0.0
        assert parse_beaten_length("  ") == 0.0

    def test_explicit_zero_dead_heat(self):
        assert parse_beaten_length("0") == 0.0

    def test_garbage_is_nan(self):
        assert np.isnan(parse_beaten_length("GIBBERISH"))
        assert np.isnan(parse_beaten_length("-3"))


class TestComputeCumulativeBL:
    def _race(self, margins):
        return pd.DataFrame(
            {
                "race_id": ["R1"] * len(margins),
                "positionOfficial": list(range(1, len(margins) + 1)),
                "beaten_lengths_raw": margins,
            }
        )

    def test_simple_cumulative(self):
        df = compute_cumulative_bl(self._race([None, "1", "1/2", "2"]))
        assert df["distanceCumulative"].tolist() == pytest.approx(
            [0.0, 1.0, 1.5, 3.5]
        )

    def test_unparseable_margin_propagates_nan(self):
        # The horse behind an unparseable gap — and everyone behind it —
        # has an UNKNOWN cumulative margin.  Treating it as 0 handed the
        # whole tail of the field inflated figures.
        df = compute_cumulative_bl(self._race([None, "1", "???", "2"]))
        got = df["distanceCumulative"].tolist()
        assert got[0] == 0.0
        assert got[1] == pytest.approx(1.0)
        assert np.isnan(got[2])
        assert np.isnan(got[3])

    def test_nan_does_not_leak_across_races(self):
        df1 = self._race([None, "???", "1"])
        df2 = self._race([None, "2", "1"])
        df2["race_id"] = "R2"
        out = compute_cumulative_bl(pd.concat([df1, df2], ignore_index=True))
        r2 = out[out["race_id"] == "R2"]["distanceCumulative"].tolist()
        assert r2 == pytest.approx([0.0, 2.0, 3.0])
