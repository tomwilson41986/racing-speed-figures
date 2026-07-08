"""Store tests: history upsert idempotency and raw-capture round-trip."""

import pytest

from src.timeform import store
from src.timeform.models import DayRecon, ReconRow


@pytest.fixture
def tmp_store(tmp_path, monkeypatch):
    recon = tmp_path / "timeform_recon"
    raw = recon / "raw"
    raw.mkdir(parents=True)
    monkeypatch.setattr(store, "RECON_DIR", recon)
    monkeypatch.setattr(store, "RAW_DIR", raw)
    monkeypatch.setattr(store, "HISTORY_CSV", recon / "history.csv")
    monkeypatch.setattr(store, "CORRECTION_JSON", recon / "correction.json")
    return recon


def _day(date="2026-07-07", bias_row_diff=2.0) -> DayRecon:
    rows = [ReconRow("Ascot", "Ascot Star", 90.0, 88.0, bias_row_diff, 1, "GB/IRE", False)]
    return DayRecon(date=date, n_tf=1, n_matched=1, n_unmatched=0, corr=1.0,
                    mae=abs(bias_row_diff), bias=bias_row_diff, within_10=100.0, rows=rows)


def test_write_day_and_history_upsert(tmp_store):
    store.write_day(_day())
    store.write_day(_day())  # same date again -> no duplicate row
    hist = store.load_history()
    assert list(hist["date"]) == ["2026-07-07"]
    assert (tmp_store / "2026-07-07.csv").exists()


def test_history_accumulates_distinct_days(tmp_store):
    store.write_day(_day("2026-07-06"))
    store.write_day(_day("2026-07-07"))
    hist = store.load_history()
    assert sorted(hist["date"]) == ["2026-07-06", "2026-07-07"]


def test_correction_round_trip(tmp_store):
    store.write_correction({"apply": False, "bias_hat": 1.2})
    assert store.load_correction()["bias_hat"] == 1.2


def test_read_raw_capture(tmp_store):
    day_dir = tmp_store / "raw" / "2026-07-07"
    day_dir.mkdir(parents=True)
    (day_dir / "index.html").write_text("<html>index</html>")
    (day_dir / "ascot.html").write_text("<html>ascot</html>")
    (day_dir / "payload_0.json").write_text('{"a":1}')
    (day_dir / "payload_1.json").write_text('{"b":2}')
    cap = store.read_raw_capture("2026-07-07")
    assert cap is not None
    assert cap.index_html == "<html>index</html>"
    assert "Ascot" in cap.page_htmls
    assert sorted(cap.json_payloads) == ['{"a":1}', '{"b":2}']


def test_read_raw_capture_missing(tmp_store):
    assert store.read_raw_capture("2099-01-01") is None
