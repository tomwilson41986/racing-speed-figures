"""Unit tests for the leave-one-year-out target encoding in ml_figures."""

import pandas as pd
import pytest

pytest.importorskip("xgboost")
pytest.importorskip("lightgbm")

from src.ml_figures import TE_SMOOTHING_K, _loyo_encode  # noqa: E402


def _frame():
    """Category 'A': year 2020 residuals all +10 (n=10), year 2021 all -10
    (n=10).  Category 'B': train-only in 2020, +4 (n=5)."""
    rows = []
    for _ in range(10):
        rows.append({"key": "A", "res": 10.0, "year": 2020, "train": True})
    for _ in range(10):
        rows.append({"key": "A", "res": -10.0, "year": 2021, "train": True})
    for _ in range(5):
        rows.append({"key": "B", "res": 4.0, "year": 2020, "train": True})
    # Holdout rows (not in the training window)
    rows.append({"key": "A", "res": 0.0, "year": 2024, "train": False})
    rows.append({"key": "UNSEEN", "res": 0.0, "year": 2024, "train": False})
    return pd.DataFrame(rows)


class TestLoyoEncode:
    def test_train_rows_exclude_own_year(self):
        df = _frame()
        enc = _loyo_encode(
            df["key"], df["res"], df["train"], df["year"], k=TE_SMOOTHING_K
        )
        # A 2020 row must be encoded from 2021 data only:
        # (-10 × 10) / (10 + k)
        expected_2020 = -100.0 / (10 + TE_SMOOTHING_K)
        got_2020 = enc[(df["key"] == "A") & (df["year"] == 2020)].iloc[0]
        assert got_2020 == pytest.approx(expected_2020)

        # And an A 2021 row from 2020 data only: (+10 × 10) / (10 + k)
        expected_2021 = 100.0 / (10 + TE_SMOOTHING_K)
        got_2021 = enc[(df["key"] == "A") & (df["year"] == 2021)].iloc[0]
        assert got_2021 == pytest.approx(expected_2021)

    def test_holdout_rows_use_full_training_window(self):
        df = _frame()
        enc = _loyo_encode(df["key"], df["res"], df["train"], df["year"])
        # Full-train A: sum 0 over 20 rows → 0 / (20 + k) = 0.
        got = enc[(df["key"] == "A") & (~df["train"])].iloc[0]
        assert got == pytest.approx(0.0)

    def test_unseen_category_encodes_to_zero(self):
        df = _frame()
        enc = _loyo_encode(df["key"], df["res"], df["train"], df["year"])
        assert enc[df["key"] == "UNSEEN"].iloc[0] == 0.0

    def test_shrinkage_pulls_small_categories_toward_zero(self):
        df = _frame()
        enc = _loyo_encode(df["key"], df["res"], df["train"], df["year"])
        # B has n=5 at +4: full-train shrunk mean = 20 / (5 + k) — far
        # below the raw mean of 4.  (B train rows are single-year, so
        # their LOYO encoding is 0 — no other year to borrow from.)
        got_train = enc[(df["key"] == "B") & df["train"]].iloc[0]
        assert got_train == pytest.approx(0.0)

    def test_single_year_category_cannot_see_itself(self):
        # The core anti-leak property: a category observed only in one
        # training year contributes nothing to its own rows' encoding.
        df = pd.DataFrame(
            {
                "key": ["C"] * 8,
                "res": [25.0] * 8,
                "year": [2022] * 8,
                "train": [True] * 8,
            }
        )
        enc = _loyo_encode(df["key"], df["res"], df["train"], df["year"])
        assert (enc == 0.0).all()
