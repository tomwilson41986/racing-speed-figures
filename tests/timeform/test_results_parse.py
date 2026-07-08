"""Offline parser tests against saved fixtures (no network)."""

from pathlib import Path

import pytest

from src.timeform import results
from src.timeform.models import RawCapture

FIX = Path(__file__).parent / "fixtures"


def _read(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("text,expected", [
    ("88", 88.0),
    ("72+", 72.0),      # provisional "+" annotation
    ("p95", 95.0),      # "promising" prefix handled by the fallback
    ("-3", -3.0),
    ("—", None),
    ("", None),
    (None, None),
    ("N/A", None),
])
def test_parse_tfig(text, expected):
    assert results.parse_tfig(text) == expected


@pytest.mark.parametrize("text,expected", [
    ("1", 1), ("2nd", 2), ("10", 10), ("PU", None), ("F", None), ("—", None),
])
def test_parse_finish_pos(text, expected):
    assert results.parse_finish_pos(text) == expected


def test_parse_yesterday_index():
    meetings = results.parse_yesterday_index(_read("yesterday_index.html"), date="2026-07-07")
    courses = sorted(m.course for m in meetings)
    assert courses == ["Ascot", "Newmarket"]
    assert all(m.result_url.startswith("https://www.timeform.com/") for m in meetings)
    # The "Yesterday" nav link must not be treated as a meeting.
    assert all("yesterday" not in m.result_url.lower() for m in meetings)


def test_parse_race_result_html_header_driven():
    rows = results.parse_race_result_html(_read("race_result.html"))
    by_horse = {r.horse: r for r in rows}
    assert by_horse["Ascot Star"].tfig == 88
    assert by_horse["Royal Blue"].tfig == 72     # "72+" -> 72
    assert by_horse["Slow Coach"].tfig == 40
    assert by_horse["Ascot Star"].finish_pos == 1
    assert by_horse["Never Ran"].tfig is None    # non-finisher, no figure
    assert all(r.source == "html" for r in rows)
    assert by_horse["Ascot Star"].course == "Ascot"


def test_parse_results_payload_json():
    rows = results.parse_results_payload(_read("results_payload.json"))
    by_horse = {r.horse: r for r in rows}
    assert by_horse["Ascot Star"].tfig == 88
    assert by_horse["Royal Blue"].tfig == 72
    assert by_horse["Slow Coach"].tfig == 40
    assert all(r.source == "json" for r in rows)
    assert by_horse["Ascot Star"].course == "Ascot"


def test_build_results_df_prefers_json():
    cap = RawCapture(
        date="2026-07-07",
        page_htmls={"Ascot": _read("race_result.html")},
        json_payloads=[_read("results_payload.json")],
    )
    df = results.build_results_df(cap)
    assert not df.empty
    assert set(df["source"]) == {"json"}          # JSON preferred over HTML
    assert set(df["horse"]) >= {"Ascot Star", "Royal Blue", "Slow Coach"}


def test_build_results_df_html_fallback():
    cap = RawCapture(date="2026-07-07", page_htmls={"Ascot": _read("race_result.html")})
    df = results.build_results_df(cap)
    assert not df.empty
    assert set(df["source"]) == {"html"}
