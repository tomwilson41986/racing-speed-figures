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


_RESULT_LANDING = """
<a href="/results/6/beverley/2026-07-14/923066">Beverley 2:00</a>
<a href="/results/6/beverley/2026-07-14/923066">dup</a>
<a href="/results/179/downpatrick/2026-07-14/924645">Downpatrick</a>
<a href="/racecards/2/ascot/2026-07-14/111">still upcoming</a>
"""

_RESULT_PAGE = """
<td class="rp-horseTable__horseCell">
  <a data-test-selector="link-silk">
    <img class="rp-horseTable__silk"
         src="https://www.rp-assets.com/svg/0/4/2/112240.svg" alt="Runner Jacket">
  </a>
  <div class="rp-horseTable__horse">
    <a href="/profile/horse/1/star-velocity"
       class="rp-horseTable__horse__name ui-link">Star Velocity</a>
</td>
<td class="rp-horseTable__horseCell">
  <img class="rp-horseTable__silk"
       src="https://www.rp-assets.com/svg/b/1/7/287171b.svg" alt="Runner Jacket">
  <a class="rp-horseTable__horse__name ui-link">Queen Sana</a>
</td>
"""


def test_parse_result_links():
    links = rp_silks.parse_result_links(_RESULT_LANDING, "2026-07-14")
    assert links == [
        "/results/6/beverley/2026-07-14/923066",
        "/results/179/downpatrick/2026-07-14/924645",
    ]
    # racecard links are not results
    assert all("/racecards/" not in l for l in links)


def test_parse_result_silks():
    m = rp_silks.parse_result_silks(_RESULT_PAGE)
    assert m["STAR VELOCITY"] == "https://www.rp-assets.com/svg/0/4/2/112240.svg"
    assert m["QUEEN SANA"].endswith("287171b.svg")


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


class _FakeResp:
    def __init__(self, status):
        self.status_code = status
        self.text = "ok" if status == 200 else ""


class _FakeSession:
    def __init__(self, statuses):
        self._statuses = list(statuses)
        self.calls = 0

    def get(self, url, timeout=20):
        self.calls += 1
        return _FakeResp(self._statuses.pop(0))


def test_get_with_retry_recovers_from_transient_block(monkeypatch):
    monkeypatch.setattr(rp_silks.time, "sleep", lambda *_: None)
    sess = _FakeSession([406, 406, 200])
    resp = rp_silks._get_with_retry(sess, "http://x", attempts=3)
    assert resp.status_code == 200
    assert sess.calls == 3


def test_get_with_retry_returns_last_block_when_never_recovers(monkeypatch):
    monkeypatch.setattr(rp_silks.time, "sleep", lambda *_: None)
    sess = _FakeSession([403, 403, 403])
    resp = rp_silks._get_with_retry(sess, "http://x", attempts=3)
    assert resp.status_code == 403
    assert sess.calls == 3


def test_silk_map_save_load_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(rp_silks, "_SILK_MAP_DIR", str(tmp_path))
    m = {"STAR VELOCITY": "https://x/a.svg", "QUEEN SANA": "https://x/b.svg"}
    path = rp_silks.save_silk_map("2026-07-16", m)
    assert path.endswith("rp_silks_2026-07-16.json")
    assert rp_silks.load_silk_map("2026-07-16") == m
    assert rp_silks.load_silk_map("1999-01-01") is None


def test_silk_map_for_prefers_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(rp_silks, "_SILK_MAP_DIR", str(tmp_path))
    cached = {"HORSE": "https://x/c.svg"}
    rp_silks.save_silk_map("2026-07-16", cached)
    # If a cache exists, silk_map_for must NOT hit the network.
    monkeypatch.setattr(rp_silks, "fetch_silk_map",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("network hit")))
    assert rp_silks.silk_map_for("2026-07-16") == cached


def test_silk_map_for_falls_back_to_live(tmp_path, monkeypatch):
    monkeypatch.setattr(rp_silks, "_SILK_MAP_DIR", str(tmp_path))  # empty → no cache
    monkeypatch.setattr(rp_silks, "fetch_silk_map", lambda *a, **k: {"LIVE": "https://x/d.svg"})
    assert rp_silks.silk_map_for("2026-07-16") == {"LIVE": "https://x/d.svg"}


def test_proxies_from_env(monkeypatch):
    monkeypatch.delenv("RP_SILKS_PROXY", raising=False)
    assert rp_silks._proxies() is None
    monkeypatch.setenv("RP_SILKS_PROXY", "http://user:pass@host:3128")
    assert rp_silks._proxies() == {
        "http": "http://user:pass@host:3128",
        "https": "http://user:pass@host:3128",
    }
