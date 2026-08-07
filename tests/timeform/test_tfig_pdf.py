"""Offline tests for the Timeform result-PDF parser (no network).

The fixture ``timeform_race_result_ripon_1524.pdf`` is a real Timeform "Race
Result" PDF (Ripon 15:24, 3 August 2026, 5 runners).  Its TFR/Tfig values were
verified by hand and are asserted below as ground truth.
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

from src.timeform import tfig_pdf
from src.timeform.tfig_pdf import (
    PdfRace,
    PdfRunner,
    build_results_df_from_pdfs,
    match_name,
    parse_result_pdf,
    races_to_df,
)

FIX = Path(__file__).parent / "fixtures"
RIPON_1524 = FIX / "timeform_race_result_ripon_1524.pdf"

def test_fixture_is_committed():
    """Fail loudly if the ground-truth PDF goes missing.

    Every test below parses this fixture, so were it merely skipped when absent
    the whole PDF parser would silently go untested in CI while staying green —
    the exact failure mode this pipeline keeps repeating.
    """
    assert RIPON_1524.exists(), (
        f"Missing fixture {RIPON_1524}. It must be committed: without it the "
        "Timeform PDF parser has NO test coverage."
    )

# horse, finish_pos, TFR, TFR flag, Tfig — verified by hand from the PDF.
GROUND_TRUTH = [
    ("ANGEL NUMBERS (IRE)", 1, 89.0, "+", 77.0),
    ("MARAJITO", 2, 71.0, "+", 61.0),
    ("CREATIVE QUEEN (USA)", 3, 72.0, None, 62.0),
    ("ZOUSTAR DREAMS", 4, 67.0, None, 58.0),
    ("NORTH WEST GAL", 5, 63.0, None, 54.0),
]


@pytest.fixture(scope="module")
def race() -> PdfRace:
    return parse_result_pdf(RIPON_1524)


# ─────────────────────────────────────────────────────────────────────
# Race-level metadata
# ─────────────────────────────────────────────────────────────────────
def test_race_metadata(race):
    assert race.ok is True
    assert race.error is None
    assert race.course == "Ripon"
    assert race.race_time == "15:24"
    assert race.race_date == "2026-08-03"
    assert race.race_class == 5
    assert race.distance == "5f"
    assert race.going == "Gd/Frm"
    assert race.surface == "Turf"
    assert race.race_name == "WEATHERBYS BLOODSTOCK PRO FILLIES' HANDICAP"
    assert race.source_file == "timeform_race_result_ripon_1524.pdf"


def test_card_times_and_race_number(race):
    # The meeting banner carries the whole card; race_no is this race's slot.
    assert race.card_times == ["14:24", "14:54", "15:24", "15:54", "16:24", "16:54"]
    assert race.race_no == 3


def test_footer(race):
    assert race.all_ran == 5
    assert race.non_runners is None
    assert race.winning_time == "58.32s"


# ─────────────────────────────────────────────────────────────────────
# Ground truth
# ─────────────────────────────────────────────────────────────────────
def test_runner_count(race):
    assert race.n_runners == 5
    assert race.n_tfig == 5


@pytest.mark.parametrize("horse,pos,tfr,flag,tfig", GROUND_TRUTH)
def test_ground_truth_runner(race, horse, pos, tfr, flag, tfig):
    by_pos = {r.finish_pos: r for r in race.runners}
    assert pos in by_pos, f"no runner finished {pos}"
    r = by_pos[pos]
    assert r.horse == horse
    assert r.tfr == tfr
    assert r.tfr_flag == flag
    assert r.tfig == tfig


def test_finish_order_is_complete(race):
    assert sorted(r.finish_pos for r in race.runners) == [1, 2, 3, 4, 5]


def test_tfr_is_not_confused_with_tfig(race):
    # TFR and Tfig are both bold size-2.62 tokens; they are told apart by
    # x-position, never by magnitude.  Here TFR > Tfig for every runner, but
    # the parser must not be relying on that.
    for r in race.runners:
        assert r.tfr is not None and r.tfig is not None
        assert r.tfr != r.tfig


# ─────────────────────────────────────────────────────────────────────
# Name quality — the critical bar: garbage in == no match downstream
# ─────────────────────────────────────────────────────────────────────
def test_horse_names_are_clean(race):
    """Names must not be contaminated by the interleaved pedigree column."""
    for r in race.runners:
        assert r.horse == r.horse.strip()
        assert "  " not in r.horse
        assert not any(c.isdigit() for c in r.horse), r.horse
        assert r.horse.upper() == r.horse, r.horse
        assert len(r.horse) <= 40, r.horse
        # The pedigree is parsed separately and must not leak into the name.
        assert r.pedigree
        assert r.pedigree not in r.horse


def test_country_suffix_survives(race):
    names = {r.horse for r in race.runners}
    assert "ANGEL NUMBERS (IRE)" in names
    assert "CREATIVE QUEEN (USA)" in names


def test_other_columns_are_separated(race):
    """Jockey / trainer / draw / btn land in their own fields, not the name."""
    by_pos = {r.finish_pos: r for r in race.runners}
    assert by_pos[1].jockey == "James Sullivan"
    assert by_pos[1].trainer == "Ruth Carr"
    assert by_pos[1].btn is None          # the winner has no beaten distance
    assert by_pos[2].btn == "2½"
    assert {r.draw for r in race.runners} == {1, 2, 3, 4, 5}
    assert {r.cloth for r in race.runners} == {1, 2, 3, 4, 5}


# ─────────────────────────────────────────────────────────────────────
# tf_df contract consumed by reconcile.match_rows
# ─────────────────────────────────────────────────────────────────────
def test_df_columns_and_dtypes(race):
    df = races_to_df([race])
    assert list(df.columns) == ["course", "horse", "tfig", "finish_pos",
                                "race_no", "race_time", "source"]
    # reconcile.py:74 calls a bare float(tfig) — it MUST already be numeric.
    assert pd.api.types.is_float_dtype(df["tfig"])
    assert str(df["finish_pos"].dtype) == "Int64"
    assert df["tfig"].notna().all()
    assert (df["source"] == "pdf").all()
    assert len(df) == 5


def test_df_course_is_the_full_course_name(race):
    df = races_to_df([race])
    # normalise_track("Notts") != "NOTTINGHAM" — the course must be the full
    # name from the PDF banner, never a filename stem.
    assert set(df["course"]) == {"Ripon"}


def test_df_values_match_ground_truth(race):
    df = races_to_df([race]).set_index("finish_pos")
    for horse, pos, _tfr, _flag, tfig in GROUND_TRUTH:
        assert df.loc[pos, "horse"] == match_name(horse)
        assert df.loc[pos, "tfig"] == tfig


def test_df_survives_reconcile_float_coercion(race):
    # The exact operation reconcile.match_rows performs on every row.
    for v in races_to_df([race])["tfig"]:
        assert isinstance(float(v), float)


def test_empty_frame_has_the_right_shape():
    df = races_to_df([])
    assert len(df) == 0
    assert list(df.columns) == ["course", "horse", "tfig", "finish_pos",
                                "race_no", "race_time", "source"]
    assert pd.api.types.is_float_dtype(df["tfig"])


# ─────────────────────────────────────────────────────────────────────
# Missing figures / annotated values
# ─────────────────────────────────────────────────────────────────────
def test_runners_without_a_figure_are_not_emitted():
    """Tailed-off runners have no TFR/Tfig at all; they must not be faked.

    ``reconcile`` counts every non-null tfig in ``n_tf``, so a placeholder row
    would corrupt the match-rate signal.
    """
    r = PdfRace(course="Ripon", race_time="15:24", runners=[
        PdfRunner(horse="WITH FIG", tfig=60.0, tfr=70.0, finish_pos=1),
        PdfRunner(horse="NO FIG", tfig=None, tfr=None, finish_pos=2),
    ])
    df = races_to_df([r])
    assert list(df["horse"]) == ["WITH FIG"]
    assert df["tfig"].notna().all()


def test_annotated_tfr_never_reaches_the_frame(race):
    """A '89+' string in the tfig column would kill the run at reconcile.py:74."""
    assert any(r.tfr_flag == "+" for r in race.runners)   # fixture exercises it
    df = races_to_df([race])
    for v in df["tfig"]:
        assert not isinstance(v, str)


def test_match_name_elides_apostrophes():
    # Our ratings feed spells "Harrys Pop"; Timeform prints "HARRY'S POP".
    # normalise_name turns punctuation into a space, so without this they
    # normalise to "HARRY S POP" vs "HARRYS POP" and never join.
    assert match_name("HARRY'S POP") == "HARRYS POP"
    assert match_name("VEGA’S VIRTUE (IRE)") == "VEGAS VIRTUE (IRE)"
    assert match_name("ANGEL NUMBERS (IRE)") == "ANGEL NUMBERS (IRE)"
    assert match_name("") == ""


def test_apostrophe_names_match_the_ratings_spelling():
    from src.sectionals.normalize import normalise_name
    assert normalise_name(match_name("HARRY'S POP")) == normalise_name("Harrys Pop")


# ─────────────────────────────────────────────────────────────────────
# Robustness: one bad file must never kill a day
# ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("bad", [
    b"",
    b"not a pdf at all",
    b"%PDF-1.4 truncated and broken",
    "/definitely/not/here.pdf",
    None,
    12345,
    io.BytesIO(b"garbage"),
])
def test_bad_input_returns_empty_never_raises(bad):
    r = parse_result_pdf(bad)
    assert isinstance(r, PdfRace)
    assert r.ok is False
    assert r.runners == []
    assert r.error


def test_truncated_real_pdf_is_handled():
    data = RIPON_1524.read_bytes()[:5000]
    r = parse_result_pdf(data)
    assert r.ok is False
    assert r.runners == []


def test_batch_skips_bad_files_and_keeps_good_ones():
    df = build_results_df_from_pdfs([b"junk", RIPON_1524, "/nope.pdf"])
    assert len(df) == 5
    assert set(df["course"]) == {"Ripon"}


def test_bytes_and_file_object_give_the_same_result(race):
    from_bytes = parse_result_pdf(RIPON_1524.read_bytes())
    assert [r.horse for r in from_bytes.runners] == [r.horse for r in race.runners]
    assert [r.tfig for r in from_bytes.runners] == [r.tfig for r in race.runners]
    with RIPON_1524.open("rb") as fh:
        from_file = parse_result_pdf(fh)
    assert [r.tfig for r in from_file.runners] == [r.tfig for r in race.runners]


# ─────────────────────────────────────────────────────────────────────
# Unit tests for the coordinate helpers (no PDF needed)
# ─────────────────────────────────────────────────────────────────────
def _w(text, x0, x1, top=100.0, font="ABCDEF+SourceSansPro-Bold", size=2.62):
    return {"text": text, "x0": x0, "x1": x1, "top": top, "doctop": top,
            "fontname": font, "size": size}


def test_font_helpers_reject_lookalike_fonts():
    # Helvetica-Bold is the rating-delta glyph; SemiBold is the course banner.
    assert tfig_pdf._is_bold(_w("89", 0, 1)) is True
    assert tfig_pdf._is_bold(_w("-6", 0, 1, font="XX+Helvetica-Bold")) is False
    assert tfig_pdf._is_bold(_w("Ripon", 0, 1, font="XX+SourceSansPro-SemiBold")) is False


def test_join_inserts_spaces_only_for_real_gaps():
    # Intra-word kerning is ~0pt; a rendered space is ~0.5pt.
    words = [_w("ANGEL", 10.0, 20.0), _w("NUMBERS", 20.5, 32.0),
             _w("(IRE)", 32.5, 38.0)]
    assert tfig_pdf._join(words) == "ANGEL NUMBERS (IRE)"
    assert tfig_pdf._join([_w("Sulli", 10.0, 15.0), _w("van", 15.0, 18.0)]) == "Sullivan"


def test_cluster_splits_two_rating_columns():
    centres = [303.4, 303.4, 303.41, 311.2, 311.2, 311.21]
    clusters = tfig_pdf._cluster(centres, gap=1.0)
    assert len(clusters) == 2
    assert len(clusters[0]) == len(clusters[1]) == 3


def test_assign_ratings_uses_position_not_magnitude():
    """TFR < Tfig genuinely occurs (e.g. MOONTUNE 51/52) — position decides."""
    runners = [PdfRunner(horse="A"), PdfRunner(horse="B")]
    raw = [
        [_w("51", 302.0, 304.8), _w("52", 310.0, 312.8)],
        [_w("89", 302.0, 304.8), _w("77", 310.0, 312.8)],
    ]
    tfig_pdf._assign_ratings(runners, raw, tfr_hdr_x0=301.0)
    assert (runners[0].tfr, runners[0].tfig) == (51.0, 52.0)
    assert (runners[1].tfr, runners[1].tfig) == (89.0, 77.0)


def test_assign_ratings_places_a_lone_token_by_column():
    """A block with only one rating token is assigned by learned column centre."""
    runners = [PdfRunner(horse="A"), PdfRunner(horse="B"), PdfRunner(horse="C")]
    raw = [
        [_w("51", 302.0, 304.8), _w("52", 310.0, 312.8)],
        [_w("60", 310.0, 312.8)],   # Tfig only
        [_w("70", 302.0, 304.8)],   # TFR only
    ]
    tfig_pdf._assign_ratings(runners, raw, tfr_hdr_x0=301.0)
    assert (runners[1].tfr, runners[1].tfig) == (None, 60.0)
    assert (runners[2].tfr, runners[2].tfig) == (70.0, None)


def test_assign_ratings_handles_a_block_with_no_tokens():
    runners = [PdfRunner(horse="A"), PdfRunner(horse="B")]
    raw = [[_w("51", 302.0, 304.8), _w("52", 310.0, 312.8)], []]
    tfig_pdf._assign_ratings(runners, raw, tfr_hdr_x0=301.0)
    assert runners[1].tfr is None and runners[1].tfig is None


def test_rating_regex_accepts_the_annotation_suffixes():
    for text, num in (("89", "89"), ("89+", "89"), ("95p", "95"),
                      ("101", "101"), ("7?", "7")):
        m = tfig_pdf._RATING_RE.match(text)
        assert m and m.group(1) == num, text
    for text in ("", "abc", "1234", "-6", "13/8f", "(1.51)"):
        assert tfig_pdf._RATING_RE.match(text) is None, text
