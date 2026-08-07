"""Make the repo root importable so tests can use ``src.*`` modules."""

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@pytest.fixture(scope="session")
def atr_sectional_pdf():
    """A real At The Races *Sectional Times* print-out (Brighton 14:30, 2026-08-05).

    Committed because the parser reads geometry, not text: only a genuine
    print-out pins the header/row coordinates the extractor keys off.
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "fixtures", "atr_sectional_brighton_1430.pdf")


@pytest.fixture(scope="session")
def atr_sectional_pdf_mixed_rows():
    """Yarmouth 18:10, 2026-08-05 — a ten-runner field.

    Five runners carry their name *below* the split times and five carry it
    *above*. Searching downwards only returned exactly half the field, silently,
    so this layout is worth a fixture of its own.
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "fixtures", "atr_sectional_yarmouth_1810.pdf")
