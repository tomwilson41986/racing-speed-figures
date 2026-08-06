"""The reconciliation must never report a green, flattering day while the
underlying data is incomplete.

This pipeline has a documented history of "silently green while doing nothing":
its output feeds a correction applied to client-facing figures, so a partial or
unmatched day is worse than no day at all. Each test here reproduces a concrete
failure mode found in review.
"""

import pandas as pd
import pytest
from click.testing import CliRunner

from src.timeform import cli as cli_mod


def _tf_df(n=3, stats=None):
    df = pd.DataFrame({
        "course": ["Ripon"] * n,
        "horse": [f"HORSE {i}" for i in range(n)],
        "tfig": [70.0 + i for i in range(n)],
        "finish_pos": list(range(1, n + 1)),
        "race_no": [1] * n,
        "race_time": ["15:24"] * n,
        "source": ["pdf"] * n,
    })
    df.attrs["fetch_stats"] = stats or {}
    return df


class _Day:
    """Stand-in for a DayRecon."""
    def __init__(self, n_tf, n_matched):
        self.n_tf, self.n_matched = n_tf, n_matched
        self.mae, self.bias, self.n_outliers = 1.0, 0.5, 0


def _run(monkeypatch, tf_df, *, day=None, rated=True, args=()):
    monkeypatch.setattr(cli_mod, "_build_tf_df", lambda *a, **k: tf_df)
    monkeypatch.setattr(cli_mod, "_we_rated_races", lambda d: rated)
    if day is not None:
        monkeypatch.setattr(cli_mod, "reconcile", lambda *a, **k: day)
    # Nothing below should be reached in the failure cases; blow up if it is.
    monkeypatch.setattr(cli_mod.store, "write_day",
                        lambda *a, **k: pytest.fail("wrote a day it should have refused"))
    return CliRunner().invoke(cli_mod.cli, ["run", "--date", "2026-08-03",
                                            "--no-email", *args])


def test_partial_drive_failure_is_refused(monkeypatch):
    """10 of 12 PDFs failing must NOT publish a flattering 100%-match day."""
    df = _tf_df(stats={"n_found": 12, "n_parsed": 2, "n_failed": 10})
    res = _run(monkeypatch, df)
    assert res.exit_code == 1
    assert "partial" in res.output.lower()
    assert "10 of 12" in res.output


def test_complete_day_is_published(monkeypatch):
    """The happy path must still go through when nothing failed."""
    df = _tf_df(stats={"n_found": 12, "n_parsed": 12, "n_failed": 0})
    monkeypatch.setattr(cli_mod, "_build_tf_df", lambda *a, **k: df)
    monkeypatch.setattr(cli_mod, "reconcile", lambda *a, **k: _Day(3, 3))
    written = {}
    monkeypatch.setattr(cli_mod.store, "write_day", lambda day: written.setdefault("day", day))
    monkeypatch.setattr(cli_mod.store, "write_correction", lambda c: None)
    monkeypatch.setattr(cli_mod.store, "load_history", lambda: pd.DataFrame())
    monkeypatch.setattr(cli_mod, "compute_correction", lambda h: {})
    monkeypatch.setattr(cli_mod.report_mod, "render_markdown", lambda *a, **k: "md")
    monkeypatch.setattr(cli_mod.report_mod, "render_html", lambda *a, **k: "<html>")
    monkeypatch.setattr(cli_mod.report_mod, "send_email", lambda *a, **k: True)
    monkeypatch.setattr(cli_mod.store, "report_md_path", lambda d: __import__("pathlib").Path("/tmp/x.md"))
    monkeypatch.setattr(cli_mod.store, "report_html_path", lambda d: __import__("pathlib").Path("/tmp/x.html"))
    res = CliRunner().invoke(cli_mod.cli, ["run", "--date", "2026-08-03", "--no-email"])
    assert res.exit_code == 0, res.output
    assert written.get("day") is not None


def test_partial_failure_allowed_with_flag(monkeypatch):
    """--allow-partial is the explicit opt-in escape hatch."""
    df = _tf_df(stats={"n_found": 12, "n_parsed": 2, "n_failed": 10})
    monkeypatch.setattr(cli_mod, "_build_tf_df", lambda *a, **k: df)
    monkeypatch.setattr(cli_mod, "reconcile", lambda *a, **k: _Day(3, 3))
    seen = {}
    monkeypatch.setattr(cli_mod.store, "write_day", lambda day: seen.setdefault("w", True))
    monkeypatch.setattr(cli_mod.store, "write_correction", lambda c: None)
    monkeypatch.setattr(cli_mod.store, "load_history", lambda: pd.DataFrame())
    monkeypatch.setattr(cli_mod, "compute_correction", lambda h: {})
    monkeypatch.setattr(cli_mod.report_mod, "render_markdown", lambda *a, **k: "md")
    monkeypatch.setattr(cli_mod.report_mod, "render_html", lambda *a, **k: "<html>")
    monkeypatch.setattr(cli_mod.report_mod, "send_email", lambda *a, **k: True)
    monkeypatch.setattr(cli_mod.store, "report_md_path", lambda d: __import__("pathlib").Path("/tmp/x.md"))
    monkeypatch.setattr(cli_mod.store, "report_html_path", lambda d: __import__("pathlib").Path("/tmp/x.html"))
    res = CliRunner().invoke(cli_mod.cli, ["run", "--date", "2026-08-03",
                                           "--no-email", "--allow-partial"])
    assert res.exit_code == 0, res.output
    assert seen.get("w") is True


def test_zero_matches_is_refused(monkeypatch):
    """TFigs parsed but nothing joined => hollow row; must not be written."""
    df = _tf_df(stats={"n_found": 12, "n_parsed": 12, "n_failed": 0})
    res = _run(monkeypatch, df, day=_Day(n_tf=64, n_matched=0))
    assert res.exit_code == 1
    assert "matched none" in res.output.lower()


def test_no_pdfs_but_we_rated_races_is_an_error(monkeypatch):
    """We produced figures, so the PDFs should exist — that's a real failure."""
    res = _run(monkeypatch, _tf_df(0), rated=True)
    assert res.exit_code == 1
    assert "no timeform pdfs" in res.output.lower()


def test_no_pdfs_and_no_racing_is_not_an_error(monkeypatch):
    """A genuine no-racing day must not turn the scheduled job red."""
    res = _run(monkeypatch, _tf_df(0), rated=False)
    assert res.exit_code == 0
    assert "not an error" in res.output.lower()
