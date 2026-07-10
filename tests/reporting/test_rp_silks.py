"""Offline tests for the Racing Post racecard silk parser (no network)."""

from pathlib import Path

import pandas as pd

from src.reporting import rp_silks

FIX = Path(__file__).parent / "fixtures"


def test_parse_card_links():
    html = (FIX / "rp_racecards_landing.html").read_text()
    links = rp_silks.parse_card_links(html, "2026-07-10")
    assert links == [
        "/racecards/2/ascot/2026-07-10/111",
        "/racecards/174/newmarket-july/2026-07-10/222",
    ]
    # A different date's card must not be picked up, and duplicates collapse.
    assert all("2026-07-11" not in l for l in links)


def test_parse_card_silks():
    html = (FIX / "rp_racecard.html").read_text()
    m = rp_silks.parse_card_silks(html)
    assert len(m) == 3
    assert m["ASCOT STAR"] == "https://www.rp-assets.com/svg/0/4/2/112240.svg"
    assert m["CAFE SOCIETY"].endswith("231453.svg")   # accent normalised away
    assert m["FISH CHIPS"].endswith("247330b.svg")    # "&" normalised away
    # A horse name that appears with no following silk is not mapped.
    assert "LATE WITHDRAWAL" not in m


def test_enrich_silks_matches_by_name():
    silk_map = {"ASCOT STAR": "https://www.rp-assets.com/svg/0/4/2/112240.svg"}
    df = pd.DataFrame({"horseName": ["Ascot Star", "Some Other Horse"]})
    out = rp_silks.enrich_silks(df, "2026-07-10", silk_map=silk_map)
    assert out.loc[0, "silk_url"] == "https://www.rp-assets.com/svg/0/4/2/112240.svg"
    assert pd.isna(out.loc[1, "silk_url"])


def test_enrich_silks_empty_map_is_noop():
    df = pd.DataFrame({"horseName": ["Ascot Star"]})
    out = rp_silks.enrich_silks(df, "2026-07-10", silk_map={})
    assert "silk_url" not in out.columns or out["silk_url"].isna().all()
