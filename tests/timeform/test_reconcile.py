"""Reconciliation tests: join synthetic TFigs to a small ratings index."""

from pathlib import Path

import pandas as pd
import pytest

from src.sectionals.matching import RatingsIndex, _add_df
from src.timeform.reconcile import reconcile

FIX = Path(__file__).parent / "fixtures"


@pytest.fixture
def index() -> RatingsIndex:
    idx = RatingsIndex()
    df = pd.read_csv(FIX / "audit_full_sample.csv")
    _add_df(idx, df, "figure_calibrated", "GB/IRE", "f")
    # Add a France runner to confirm FR matches are excluded from the recon.
    fr = pd.DataFrame([{
        "courseName": "Deauville", "raceNumber": 1, "horseName": "Paris Flyer",
        "positionOfficial": 1, "distance": 1600, "going": "Bon", "raceSurfaceName": "Turf",
        "raceClass": "", "figure_final": 100.0,
    }])
    _add_df(idx, fr, "figure_final", "FR", "m")
    return idx.finalise()


def _tf_df(rows):
    return pd.DataFrame(rows, columns=["course", "horse", "tfig", "finish_pos"])


def test_reconcile_metrics(index):
    tf = _tf_df([
        ("Ascot", "Ascot Star", 88, 1),      # our 90 -> +2
        ("Ascot", "Royal Blue", 72, 2),      # our 70 -> -2
        ("Ascot", "Slow Coach", 40, 3),      # our 55 -> +15 (outlier)
        ("Newmarket", "Quick Silver", 80, 1),  # our 82 -> +2
        ("Ascot", "Unknown Horse", 60, 5),   # no match
    ])
    day = reconcile("2026-07-07", tf, index=index)

    assert day.n_tf == 5
    assert day.n_matched == 4
    assert day.n_unmatched == 1
    assert day.mae == pytest.approx(5.25)
    assert day.bias == pytest.approx(4.25)
    assert day.within_10 == pytest.approx(75.0)
    assert day.corr is not None and day.corr > 0.9
    assert day.n_outliers == 1
    outlier = next(r for r in day.rows if r.outlier)
    assert outlier.horse == "Slow Coach"
    assert outlier.diff == pytest.approx(15.0)


def test_unique_horse_fallback(index):
    # Wrong course spelling but a unique horse name still matches.
    tf = _tf_df([("Ascot Racecourse", "Quick Silver", 80, 1)])
    day = reconcile("2026-07-07", tf, index=index)
    assert day.n_matched == 1
    assert day.rows[0].course == "Newmarket"


def test_france_match_excluded(index):
    tf = _tf_df([("Deauville", "Paris Flyer", 95, 1)])
    day = reconcile("2026-07-07", tf, index=index)
    assert day.n_matched == 0  # FR figures are not reconciled against Timeform


def test_no_matches_is_safe(index):
    tf = _tf_df([("Nowhere", "Ghost Horse", 50, 1)])
    day = reconcile("2026-07-07", tf, index=index)
    assert day.n_matched == 0
    assert day.mae is None and day.bias is None and day.corr is None
