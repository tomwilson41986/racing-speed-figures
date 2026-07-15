"""Offline tests for France (PMU casaque) silks and the raster silk render path."""

import io

import pandas as pd
import pytest

from src.france import silks as fr_silks
from src.reporting import silks as rsilks
from src.reporting import theme


def test_is_france_filters_by_country():
    assert fr_silks._is_france({"pays": {"libelle": "FRANCE"}})
    assert fr_silks._is_france({"pays": "FR"})
    assert not fr_silks._is_france({"pays": {"libelle": "Belgique"}})
    assert not fr_silks._is_france({})


def test_enrich_silks_matches_by_name():
    silk_map = {"NYMPHENBURG": "https://assets.racingdata.pmu.fr/silks/x/AIX/5/a.png"}
    df = pd.DataFrame({"horseName": ["Nymphenburg", "Other Horse"]})
    out = fr_silks.enrich_silks(df, silk_map)
    assert out.loc[0, "silk_url"].endswith("a.png")
    assert pd.isna(out.loc[1, "silk_url"])


def test_enrich_silks_empty_map_is_noop():
    df = pd.DataFrame({"horseName": ["Nymphenburg"]})
    out = fr_silks.enrich_silks(df, {})
    assert "silk_url" not in out.columns or out["silk_url"].isna().all()


def test_fetch_silk_map_reads_casaque(monkeypatch):
    """A fake PMU client yields participants → {name: urlCasaque}."""
    class FakeClient:
        def get_programme(self, d):
            return {"programme": {"reunions": [
                {"numOfficiel": 1, "pays": {"libelle": "FRANCE"},
                 "courses": [{"numOrdre": 1}]},
                {"numOfficiel": 2, "pays": {"libelle": "Belgique"},   # skipped
                 "courses": [{"numOrdre": 1}]},
            ]}}

        def _build_url(self, d, *parts):
            return "/".join(parts)

        def _fetch_json(self, url):
            return {"participants": [
                {"nom": "Le Cheval", "urlCasaque": "http://x/a.png"},
                {"nom": "Sans Casaque"},  # no silk -> skipped
            ]}

    monkeypatch.setattr(fr_silks, "PMUClient", FakeClient, raising=False)
    # ensure the local import inside fetch_silk_map picks up the fake
    import src.france.pmu_client as pmu
    monkeypatch.setattr(pmu, "PMUClient", FakeClient, raising=False)
    m = fr_silks.fetch_silk_map("2026-07-14", client=FakeClient())
    assert m == {"LE CHEVAL": "http://x/a.png"}


def test_looks_svg_detection():
    assert rsilks._looks_svg(b"<svg xmlns=...>", "image/svg+xml", "x.svg")
    assert rsilks._looks_svg(b"<?xml ...", "", "x")
    assert not rsilks._looks_svg(b"\x89PNG\r\n", "image/png", "x.png")


def test_raster_to_png_normalises_to_render_box():
    pytest.importorskip("PIL")
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (40, 60), (200, 0, 0)).save(buf, format="PNG")
    out = rsilks._raster_to_png(buf.getvalue())
    assert out and out[:8] == b"\x89PNG\r\n\x1a\n"
    im = Image.open(io.BytesIO(out))
    assert im.size == (theme.SILK_RENDER_W, theme.SILK_RENDER_H)
