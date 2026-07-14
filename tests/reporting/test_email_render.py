"""Offline render tests for the house-style email template.

Guards the top-performers board column alignment: every row must carry the same
number of cells whether or not that runner matched a silk, otherwise the horse
name / figure columns stagger when silks are only partially matched.
"""

import re

from bs4 import BeautifulSoup

from src.reporting import render
from src.reporting.models import Race, ReportContext, Runner, Section, TopPerformer


def _ctx(top_silks):
    """Build a minimal context whose top performers have the given silk_cids."""
    tps = [
        TopPerformer(rank=i + 1, horse=f"Horse{i}", course="Ascot", race_number=1,
                     figure=110 - i, pos=i + 1, silk_cid=cid)
        for i, cid in enumerate(top_silks)
    ]
    race = Race(course="Ascot", race_number=1, distance_str="1m",
                runners=[Runner(horse="Horse0", pos=1, figure=110)])
    return ReportContext(title="T", date_str="D",
                         sections=[Section(title="UK", races=[race])],
                         top_performers=tps)


def _top_board_rows(html):
    """Return the performer <tr>s from the top-performers board.

    The board is a card <table> wrapping an inner <table> of one <tr> per
    performer; select that innermost (leaf) table's rows.
    """
    board = html.split("UK")[0]  # everything before the first section header
    soup = BeautifulSoup(board, "html.parser")
    leaf_tables = [t for t in soup.find_all("table") if t.find("table") is None]
    assert leaf_tables, "no top-board table rendered"
    return leaf_tables[0].find_all("tr")


def test_top_board_columns_consistent_with_partial_silks():
    # Mixed: some rows have a silk, some don't.
    html = render.render_html(_ctx(["silk-A", None, "silk-C"]))
    rows = _top_board_rows(html)
    counts = [len(tr.find_all("td", recursive=False)) for tr in rows]
    # Every performer row must have the same number of direct cells.
    assert len(set(counts)) == 1, f"ragged top-board rows: {counts}"


def test_top_board_no_silks_has_no_silk_column():
    html = render.render_html(_ctx([None, None]))
    # With zero silks, the silk column is omitted entirely (no fixed silk width).
    assert f"width:64px" not in html  # SILK_DISPLAY_W (56) + 8


def test_top_board_silk_cell_emitted_for_every_row_when_any_silk():
    html = render.render_html(_ctx(["silk-A", None, None]))
    # SILK_DISPLAY_W + 8 == 64: one silk cell per performer row (3), even the
    # two rows without a matched silk.
    assert html.count("width:64px") == 3
