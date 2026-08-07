"""Coordinate extraction of the ATR *Sectional Times* tab.

The saved PDFs are browser print-outs of the ATR race page — one file per tab —
so ``page.extract_text()`` interleaves the columns into soup and the original
line-based regexes matched nothing at all (188 PDFs ingested, 0 runner rows).
The Sectional Times tab is a clean coordinate table, so these tests pin the
geometry rather than the text: a header row of furlong sections, and per runner
a row of split times aligned under it with position and name a couple of points
below.

Geometry and values are taken from Brighton 14:30 on 2026-08-05.
"""

import pytest

from src.sectionals.parser import (
    _runner_blocks,
    _split_columns,
    parse_pdf,
)


# Real header/row geometry from that race, trimmed to what the parser reads.
HDR_TOP = 148.4
COL_X = [217.78, 241.63, 264.10, 286.67, 309.33, 330.13]
HEADER = ["Start-5f", "5f-4f", "4f-3f", "3f-2f", "2f-1f", "1f-Finish"]


def _w(text, x0, top, size=2.06):
    return {"text": text, "x0": x0, "x1": x0 + 6, "top": top,
            "bottom": top + 2, "size": size}


def _header_words():
    return [_w("Pos", 164.23, HDR_TOP), _w("SilkHorse", 171.0, HDR_TOP)] + [
        _w(t, x, HDR_TOP) for t, x in zip(HEADER, COL_X)
    ]


def _runner_words(splits, pos, cloth, name, top):
    out = [_w(f"{s:.2f}", x, top) for s, x in zip(splits, COL_X)]
    out.append(_w(str(pos), 165.0, top + 1.0))
    out.append(_w(f"{cloth}.", 177.0, top + 0.8))
    for i, part in enumerate(name.split()):
        out.append(_w(part, 179.0 + i * 6, top + 0.8))
    out.append(_w(f"({cloth})", 199.0, top + 1.1))
    return out


class TestSplitColumns:
    def test_finds_the_header_and_infers_distance(self):
        top, xs, furlongs = _split_columns(_header_words())
        assert top == pytest.approx(HDR_TOP)
        assert xs == pytest.approx(COL_X)
        # 'Start-5f' means five furlong markers remain, so the race is 6f.
        assert furlongs == 6

    def test_returns_none_when_there_is_no_split_table(self):
        # The other three tabs (Full Result, Sectional Tools, Stride Data) and
        # the Timeform/RacingTV families must fall through, not half-parse.
        plain = [_w("Full", 100.0, 200.0), _w("Result", 120.0, 200.0)]
        assert _split_columns(plain) is None

    def test_ignores_a_row_with_too_few_section_headers(self):
        assert _split_columns([_w("5f-4f", 241.63, 148.4)]) is None


class TestParseRealPdf:
    """End-to-end against the actual Brighton 14:30 Sectional Times PDF."""

    @pytest.fixture
    def race(self, atr_sectional_pdf):
        return parse_pdf(str(atr_sectional_pdf), track="Brighton",
                         race_date="2026-08-05", race_time="1430")

    def test_every_runner_is_recovered(self, race):
        assert race.error is None
        assert len(race.runners) == 5

    def test_splits_sum_to_the_stated_winning_time(self, race):
        # The page prints the winner's time as '1m 12.23s'.
        winner = next(r for r in race.runners if r.finish_pos == 1)
        assert sum(winner.splits) == pytest.approx(72.23, abs=0.01)
        assert winner.overall_time_s == pytest.approx(72.23, abs=0.01)

    def test_names_and_order(self, race):
        assert [r.horse for r in sorted(race.runners, key=lambda x: x.finish_pos)] == [
            "CAPE TORONADA", "BEACH PARTEE", "HAVANA JAG", "MISTER MOET",
            "LOVE ALIVE",
        ]

    def test_distance_is_recovered_from_the_section_headers(self, race):
        assert race.distance_m == pytest.approx(1005.84, abs=1.0)

    def test_finishing_speed_is_relative_to_the_horses_own_average(self, race):
        winner = next(r for r in race.runners if r.finish_pos == 1)
        expected = (winner.overall_time_s / len(winner.splits)) / winner.final_furlong_s
        assert winner.finishing_speed_pct == pytest.approx(expected * 100, abs=0.01)
        # Every runner slowed into the finish here, so all are under 100%.
        assert all(0 < r.finishing_speed_pct < 100 for r in race.runners)

    def test_final_furlong_is_the_closing_section_not_the_fastest(self, race):
        winner = next(r for r in race.runners if r.finish_pos == 1)
        assert winner.final_furlong_s == pytest.approx(winner.splits[-1])
        assert winner.final_furlong_s > min(winner.splits)


class TestNameRowAboveTheSplits:
    """Yarmouth 18:10 — the renderer does not keep a consistent row order.

    Five of these ten runners have their name below the split times and five
    have it above. Cross-checked against Timeform's own result PDF for the same
    race, which independently reports ten runners.
    """

    @pytest.fixture
    def race(self, atr_sectional_pdf_mixed_rows):
        return parse_pdf(str(atr_sectional_pdf_mixed_rows), track="Yarmouth",
                         race_date="2026-08-05", race_time="1810")

    def test_the_whole_field_is_recovered(self, race):
        assert len(race.runners) == 10

    def test_positions_are_one_to_ten_with_no_gaps(self, race):
        assert sorted(r.finish_pos for r in race.runners) == list(range(1, 11))

    def test_the_winner_is_first_and_matches_the_printed_time(self, race):
        winner = next(r for r in race.runners if r.finish_pos == 1)
        assert winner.horse == "SPEED OF SOUND"
        assert winner.overall_time_s == pytest.approx(70.81, abs=0.01)

    def test_overall_times_increase_down_the_finishing_order(self, race):
        by_pos = sorted(race.runners, key=lambda r: r.finish_pos)
        times = [r.overall_time_s for r in by_pos]
        assert times == sorted(times)

    def test_finishing_speed_separates_the_closers_from_the_faders(self, race):
        winner = next(r for r in race.runners if r.finish_pos == 1)
        last = next(r for r in race.runners if r.finish_pos == 10)
        assert winner.finishing_speed_pct > 100 > last.finishing_speed_pct


def test_a_name_sharing_the_split_row_is_still_read():
    """Yarmouth 18:40 puts TROUBLESOME GUEST's name on the split row itself.

    Excluding that row from the name search dropped the runner entirely, so the
    block window must include it — the x ranges keep the times out of the name.
    """
    words = _header_words()
    # Name on its own row below (the common case)...
    words += _runner_words([16.43, 10.71, 10.77, 10.90, 11.64, 11.20],
                           pos=1, cloth=2, name="BEAUTY BOX", top=154.5)
    # ...and a runner whose name shares the split row.
    same_row = [_w(f"{s:.2f}", x, 183.1)
                for s, x in zip([16.28, 10.69, 10.79, 11.04, 11.88, 11.30], COL_X)]
    same_row += [_w("4.", 177.0, 183.1), _w("TROUBLESOME", 179.0, 183.1),
                 _w("GUEST", 197.0, 183.1), _w("4", 165.0, 184.2)]
    words += same_row

    runners = _runner_blocks(words)
    assert {r.horse for r in runners} == {"BEAUTY BOX", "TROUBLESOME GUEST"}


def test_a_timeform_result_pdf_yields_no_sectionals():
    """Three unrelated PDF families share each Drive folder.

    The ingest hands every one of them to this parser, so a Timeform result —
    which also carries a table of numbers per runner — must produce nothing
    rather than a plausible-looking row of garbage.
    """
    import os

    fixture = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "timeform", "fixtures", "timeform_race_result_ripon_1524.pdf",
    )
    race = parse_pdf(fixture, track="Ripon", race_date="2026-08-03",
                     race_time="1524")
    assert race.runners == []
