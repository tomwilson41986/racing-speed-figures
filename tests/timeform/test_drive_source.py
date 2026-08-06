"""Offline tests for the Drive-backed Timeform source.

The critical behaviour here is the **filename filter**. Three unrelated document
families share every ``<root>/DD.MM.YY/<Track>/`` folder and only one of them is
Timeform; ``drive_client.iter_race_pdfs`` has no filter at all, so feeding its
output to the Timeform parser would push Racing TV and At The Races PDFs through
it. These tests pin exactly which names are accepted and which are rejected.

No network: the Drive reader is a stub.
"""

from __future__ import annotations

import pytest

from src.timeform import drive_source
from src.timeform.drive_source import (
    is_timeform_result_pdf,
    iter_timeform_pdfs,
    parse_timeform_filename,
)

FOLDER_MIME = "application/vnd.google-apps.folder"
PDF_MIME = "application/pdf"


# ─────────────────────────────────────────────────────────────────────
# Accepted: the Timeform "Race Result" family
# ─────────────────────────────────────────────────────────────────────
ACCEPTED = [
    # The canonical shape.
    "Race Result 15_24 RIPON Monday 3 August.pdf",
    "Race Result 14_40 NOTTINGHAM Monday 3 August.pdf",
    # Single-digit hour.
    "Race Result 5_10 Nottingham Mon 3 Aug.pdf",
    # Multi-word / bracketed course names.
    "Race Result 19_05 NEWCASTLE (AW) Wednesday 12 November.pdf",
    "Race Result 14_15 MARKET RASEN Saturday 1 February.pdf",
    # Alternative time separators, from whatever sanitised the download.
    "Race Result 17.10 NOTTINGHAM Monday 3 August.pdf",
    "Race Result 17-10 NOTTINGHAM Monday 3 August.pdf",
    "Race Result 17:10 NOTTINGHAM Monday 3 August.pdf",
    # Duplicate-suffix variants the Drive folder actually contains.
    "Race Result 15_24 RIPON Monday 3 August2.pdf",
    "Race Result 15_24 RIPON Monday 3 August3.pdf",
    "Race Result 15_24 RIPON Monday 3 August (1).pdf",
    "Race Result 15_24 RIPON Monday 3 August-2.pdf",
    "Race Result 15_24 RIPON Monday 3 August_2.pdf",
    # Ordinal day, abbreviated weekday/month.
    "Race Result 16_54 RIPON Sun 14th Sept.pdf",
    # Case and extension case are not significant.
    "race result 15_24 ripon monday 3 august.PDF",
]


# ─────────────────────────────────────────────────────────────────────
# Rejected: the two other families in the same folder, plus near-misses
# ─────────────────────────────────────────────────────────────────────
REJECTED_RACINGTV = [
    "Ripon Racing Results _ 3 August 2026 15_24.pdf",
    "Nottingham Racing Results _ 03.08.2026 14_40.pdf",
    "Newcastle (AW) Racing Results _ 12 November 2026 19_05.pdf",
    "Racing Results _ 3 August 2026 15_24.pdf",
]

REJECTED_ATR = [
    "15_24 _ Ripon _ 3 August 2026 _ At The Races.pdf",
    "14_40 _ Nottingham _ 2026-08-03 _ At The Races (1).pdf",
    "16_54 _ Ripon _ 3 August 2026 _ At The Races Sectional Times.pdf",
]

REJECTED_OTHER = [
    "Race Card 15_24 RIPON Monday 3 August.pdf",          # racecard, not result
    "Racecard 15_24 RIPON Monday 3 August.pdf",
    "Results 15_24 RIPON Monday 3 August.pdf",            # missing 'Race'
    "Ripon Race Result 15_24 Monday 3 August.pdf",        # course leads
    "notes.pdf",
    "",
    "Race Result 15_24 RIPON Monday 3 August.txt",        # not a PDF at all
]


@pytest.mark.parametrize("name", ACCEPTED)
def test_accepts_timeform_family(name):
    assert is_timeform_result_pdf(name), name


@pytest.mark.parametrize("name", REJECTED_RACINGTV + REJECTED_ATR + REJECTED_OTHER)
def test_rejects_other_families(name):
    assert not is_timeform_result_pdf(name), name


def test_racing_tv_never_confused_with_race_result():
    """'Racing Results' must never satisfy the 'Race Result' prefix."""
    for name in REJECTED_RACINGTV:
        assert parse_timeform_filename(name) is None


def test_parsed_fields():
    got = parse_timeform_filename("Race Result 15_24 RIPON Monday 3 August.pdf")
    assert got == {"race_time": "15:24", "course": "RIPON", "day": 3,
                   "month": "August", "strict": True}


def test_single_digit_hour_zero_padded():
    assert parse_timeform_filename(
        "Race Result 9_05 AYR Friday 12 September.pdf")["race_time"] == "09:05"


def test_bracketed_course_kept_whole():
    assert parse_timeform_filename(
        "Race Result 19_05 NEWCASTLE (AW) Wednesday 12 November.pdf"
    )["course"] == "NEWCASTLE (AW)"


def test_duplicate_suffix_still_yields_course():
    """A trailing '2' must not be swallowed into the month or drop the course."""
    got = parse_timeform_filename("Race Result 15_24 RIPON Monday 3 August2.pdf")
    assert got["course"] == "RIPON"
    assert got["race_time"] == "15:24"
    assert got["strict"] is True


def test_lenient_path_accepts_unknown_tail():
    """Leading 'Race Result' is enough; the course comes from the PDF banner."""
    got = parse_timeform_filename("Race Result 15_24 RIPON.pdf")
    assert got is not None
    assert got["strict"] is False
    assert got["race_time"] == "15:24"
    assert got["course"] is None


def test_extension_stripped_not_required_to_be_lowercase():
    assert is_timeform_result_pdf("Race Result 15_24 RIPON Monday 3 August.Pdf")


# ─────────────────────────────────────────────────────────────────────
# Drive walk (stub reader — no network)
# ─────────────────────────────────────────────────────────────────────
class StubReader:
    """Minimal stand-in for GDriveReader over a two-track day folder."""

    def __init__(self, files, day_name="3.08.26"):
        self.files = files
        self.day_name = day_name
        self.downloaded = []

    def list_children(self, parent_id):
        if parent_id == "ROOT":
            yield {"id": "OTHERDAY", "name": "02.08.26", "mimeType": FOLDER_MIME}
            yield {"id": "DAY", "name": self.day_name, "mimeType": FOLDER_MIME}
            yield {"id": "STRAY", "name": "notes.txt", "mimeType": "text/plain"}

    def list_folder(self, folder_id, recursive=False):
        assert folder_id == "DAY" and recursive
        for i, (track, name) in enumerate(self.files):
            rel = f"{track}/{name}" if track else name
            yield {"id": f"f{i}", "name": name,
                   "mimeType": PDF_MIME if name.lower().endswith(".pdf") else "text/plain",
                   "relpath": rel}

    def download_file(self, file_id):
        self.downloaded.append(file_id)
        return b"not a pdf"


MIXED_FOLDER = [
    ("Ripon", "Race Result 15_24 RIPON Monday 3 August.pdf"),
    ("Ripon", "Ripon Racing Results _ 3 August 2026 15_24.pdf"),
    ("Ripon", "15_24 _ Ripon _ 3 August 2026 _ At The Races.pdf"),
    ("Nottingham", "Race Result 14_40 NOTTINGHAM Monday 3 August.pdf"),
    ("Nottingham", "Nottingham Racing Results _ 3 August 2026 14_40.pdf"),
    ("Nottingham", "14_40 _ Nottingham _ 3 August 2026 _ At The Races.pdf"),
    ("Nottingham", "readme.txt"),
]


def test_iter_selects_only_timeform_pdfs():
    reader = StubReader(MIXED_FOLDER)
    got = list(iter_timeform_pdfs(reader, "2026-08-03", root_id="ROOT"))
    assert [i.name for i in got] == [
        "Race Result 15_24 RIPON Monday 3 August.pdf",
        "Race Result 14_40 NOTTINGHAM Monday 3 August.pdf",
    ]
    assert [i.course for i in got] == ["RIPON", "NOTTINGHAM"]
    assert [i.folder_track for i in got] == ["Ripon", "Nottingham"]
    assert [i.race_time for i in got] == ["15:24", "14:40"]


def test_iter_tolerates_dropped_leading_zero_in_folder_name():
    """The Drive folder is '3.08.26' some days and '03.08.26' others."""
    for day_name in ("3.08.26", "03.08.26", "3.8.26", "03.8.26"):
        reader = StubReader(MIXED_FOLDER, day_name=day_name)
        assert len(list(iter_timeform_pdfs(reader, "2026-08-03", root_id="ROOT"))) == 2


def test_iter_returns_nothing_when_the_day_folder_is_missing():
    reader = StubReader(MIXED_FOLDER, day_name="01.01.99")
    assert list(iter_timeform_pdfs(reader, "2026-08-03", root_id="ROOT")) == []


def test_course_hint_falls_back_to_folder_track():
    reader = StubReader([("Ripon", "Race Result 15_24.pdf")])
    got = list(iter_timeform_pdfs(reader, "2026-08-03", root_id="ROOT"))
    assert len(got) == 1
    assert got[0].course is None          # lenient filename path
    assert got[0].course_hint == "Ripon"  # recovered from the folder


def test_loose_pdf_gets_no_bogus_track_from_the_date_folder():
    """A PDF sitting directly in the date folder must not take '3.08.26' as its
    track (which is what drive_client.iter_race_pdfs does)."""
    reader = StubReader([(None, "Race Result 15_24 RIPON Monday 3 August.pdf")])
    got = list(iter_timeform_pdfs(reader, "2026-08-03", root_id="ROOT"))
    assert got[0].folder_track is None
    assert got[0].course_hint == "RIPON"


# ─────────────────────────────────────────────────────────────────────
# Best-effort assembly
# ─────────────────────────────────────────────────────────────────────
def test_unparseable_pdf_does_not_abort_the_day(monkeypatch):
    """One bad file is skipped; the good ones still make it into the frame."""
    from src.timeform.tfig_pdf import PdfRace, PdfRunner

    reader = StubReader(MIXED_FOLDER)

    def fake_parse(raw):
        fake_parse.calls += 1
        if fake_parse.calls == 1:
            return PdfRace(ok=False, error="no runner rows")
        return PdfRace(ok=True, course="Nottingham", race_time="14:40",
                       runners=[PdfRunner(horse="SOME HORSE", tfig=71.0, finish_pos=1)])
    fake_parse.calls = 0
    monkeypatch.setattr(drive_source.tfig_pdf, "parse_result_pdf", fake_parse)

    df = drive_source.build_results_df_from_drive(
        "2026-08-03", reader=reader, root_id="ROOT")
    assert list(df["horse"]) == ["SOME HORSE"]
    assert df["tfig"].tolist() == [71.0]


def test_download_failure_does_not_abort_the_day(monkeypatch):
    from src.timeform.tfig_pdf import PdfRace, PdfRunner

    class BoomReader(StubReader):
        def download_file(self, file_id):
            if file_id == "f0":
                raise RuntimeError("403 quota")
            return b"x"

    monkeypatch.setattr(
        drive_source.tfig_pdf, "parse_result_pdf",
        lambda raw: PdfRace(ok=True, course="Nottingham",
                            runners=[PdfRunner(horse="A HORSE", tfig=60.0)]))
    df = drive_source.build_results_df_from_drive(
        "2026-08-03", reader=BoomReader(MIXED_FOLDER), root_id="ROOT")
    assert len(df) == 1


def test_empty_day_returns_the_contract_shaped_frame():
    reader = StubReader([("Ripon", "Ripon Racing Results _ 3 August 2026 15_24.pdf")])
    df = drive_source.build_results_df_from_drive(
        "2026-08-03", reader=reader, root_id="ROOT")
    assert df.empty
    assert list(df.columns) == ["course", "horse", "tfig", "finish_pos",
                                "race_no", "race_time", "source"]
    assert df["tfig"].dtype == "float64"
    assert df["finish_pos"].dtype == "Int64"


def test_no_google_import_needed_to_use_the_module():
    """Importing drive_source must not drag in the google client libraries —
    get_reader() imports them lazily, so offline tests stay offline."""
    import subprocess
    import sys
    out = subprocess.run(
        [sys.executable, "-c",
         "import sys; import src.timeform.drive_source as d; "
         "print([m for m in sys.modules if m.startswith('google')])"],
        capture_output=True, text=True, cwd=str(__import__("pathlib").Path(
            __file__).resolve().parents[2]),
        env={**__import__("os").environ, "PYTHONPATH": "."})
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "[]", out.stdout


# ─────────────────────────────────────────────────────────────────────
# CLI wiring
# ─────────────────────────────────────────────────────────────────────
def test_cli_run_defaults_to_the_drive_source(monkeypatch):
    """`run` with no --source must go to Drive, not to Playwright."""
    import pandas as pd
    from click.testing import CliRunner

    from src.timeform import cli as cli_mod

    seen = {}

    def fake_build(date):
        seen["date"] = date
        return pd.DataFrame([{"course": "RIPON", "horse": "A HORSE", "tfig": 77.0}])

    monkeypatch.setattr(drive_source, "build_results_df_from_drive", fake_build)
    monkeypatch.setattr(cli_mod, "_fetch_capture", lambda *a, **k:
                        pytest.fail("scrape path must not be reached"))
    monkeypatch.setattr(cli_mod, "reconcile", lambda *a, **k:
                        (_ for _ in ()).throw(SystemExit(0)))

    res = CliRunner().invoke(cli_mod.cli, ["run", "--date", "2026-08-03", "--no-email"])
    assert seen["date"] == "2026-08-03"
    assert res.exit_code == 0


def test_cli_run_fails_loudly_when_drive_has_nothing(monkeypatch):
    """An empty Drive day must not write a hollow zero-match row to history."""
    import pandas as pd
    from click.testing import CliRunner

    from src.timeform import cli as cli_mod

    monkeypatch.setattr(drive_source, "build_results_df_from_drive",
                        lambda date: pd.DataFrame(columns=["course", "horse", "tfig"]))
    monkeypatch.setattr(cli_mod, "reconcile", lambda *a, **k:
                        pytest.fail("must not reconcile an empty Drive day"))
    res = CliRunner().invoke(cli_mod.cli, ["run", "--date", "2026-08-03", "--no-email"])
    assert res.exit_code == 1
    assert "No TFig rows" in res.output


def test_cli_reports_a_missing_service_account_clearly(monkeypatch):
    from click.testing import CliRunner

    from src.timeform import cli as cli_mod

    def boom(date):
        raise RuntimeError("GDRIVE_SA_JSON not set (service-account JSON or path)")

    monkeypatch.setattr(drive_source, "build_results_df_from_drive", boom)
    res = CliRunner().invoke(cli_mod.cli, ["run", "--date", "2026-08-03"])
    assert res.exit_code == 1
    assert "GDRIVE_SA_JSON" in res.output


def test_cli_from_raw_flag_still_selects_the_raw_source(monkeypatch):
    from click.testing import CliRunner

    from src.timeform import cli as cli_mod

    monkeypatch.setattr(cli_mod.store, "read_raw_capture", lambda d: None)
    res = CliRunner().invoke(cli_mod.cli, ["run", "--date", "2026-08-03", "--from-raw"])
    assert res.exit_code == 1
    assert "No raw capture" in res.output


def test_cli_rejects_an_unknown_source():
    from click.testing import CliRunner

    from src.timeform import cli as cli_mod

    res = CliRunner().invoke(cli_mod.cli, ["run", "--source", "nonsense"])
    assert res.exit_code != 0
