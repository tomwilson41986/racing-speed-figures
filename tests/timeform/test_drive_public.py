"""The credential-free Drive reader, and the fallback that selects it.

These run entirely offline: the Drive HTTP layer is stubbed, so what is under
test is the HTML parsing, the folder/file discrimination, the recursion and the
``get_reader`` fallback logic — not Drive itself.
"""

import re

import pytest

from src.sectionals import drive_client
from src.sectionals.drive_public import (
    FOLDER_MIME,
    PDF_MIME,
    PublicDriveReader,
    resolve_id,
)


def _entry(file_id: str, name: str, *, folder: bool) -> str:
    """One ``flip-entry`` block shaped like Drive's embedded folder view."""
    href = (
        f"https://drive.google.com/drive/folders/{file_id}"
        if folder
        else f"https://drive.google.com/file/d/{file_id}/view"
    )
    label = "Folder" if folder else "PDF"
    return (
        f'<div class="flip-entry" id="entry-{file_id}" tabindex="0" role="link">'
        f'<div class="flip-entry-info"><a href="{href}" target="_blank">'
        f'<div class="flip-entry-icon"><div aria-label="{label}"></div></div>'
        f'</a><div class="flip-entry-title">{name}</div></div></div>'
    )


def _page(*entries: str) -> str:
    return '<div class="flip-entries">' + "".join(entries) + "</div>"


class _FakeResponse:
    def __init__(self, text: str = "", content: bytes = b""):
        self.text = text
        self.content = content
        self.status_code = 200


@pytest.fixture
def reader(monkeypatch):
    """A PublicDriveReader whose HTTP layer serves a small fake Drive tree."""
    tree = {
        "root": _page(
            _entry("day1", "05.08.26", folder=True),
            _entry("day2", "06.08.26", folder=True),
        ),
        "day1": _page(
            _entry("trackA", "Kempton Park", folder=True),
            _entry("junk", ".DS_Store", folder=False),
        ),
        "day2": _page(),
        "trackA": _page(
            _entry("pdf1", "Race Result 17_20 KEMPTON PARK Wednesday 05 August.pdf",
                   folder=False),
            _entry("pdf2", "17_20 _ Kempton _ At The Races.pdf", folder=False),
        ),
    }
    blobs = {"pdf1": b"%PDF-1.4 timeform", "pdf2": b"%PDF-1.4 attheraces"}

    rdr = PublicDriveReader()

    def fake_get(url):
        folder = re.search(r"embeddedfolderview\?id=([\w-]+)", url)
        if folder:
            return _FakeResponse(text=tree[folder.group(1)])
        download = re.search(r"[?&]id=([\w-]+)", url)
        return _FakeResponse(content=blobs[download.group(1)])

    monkeypatch.setattr(rdr, "_get", fake_get)
    return rdr


def test_resolve_id_accepts_urls_and_bare_ids():
    assert resolve_id("https://drive.google.com/drive/folders/abc-123") == "abc-123"
    assert resolve_id("https://drive.google.com/file/d/xyz_9/view") == "xyz_9"
    assert resolve_id("plain-id") == "plain-id"


def test_list_children_separates_folders_from_files(reader):
    kids = {c["name"]: c for c in reader.list_children("day1")}
    assert kids["Kempton Park"]["mimeType"] == FOLDER_MIME
    assert kids["Kempton Park"]["id"] == "trackA"
    # A non-PDF file must not be mistaken for a folder, or recursion loops.
    assert kids[".DS_Store"]["mimeType"] == "application/octet-stream"


def test_pdf_children_are_typed_as_pdf(reader):
    kids = {c["name"]: c for c in reader.list_children("trackA")}
    assert all(c["mimeType"] == PDF_MIME for c in kids.values())


def test_recursive_listing_yields_relpaths_and_no_folders(reader):
    files = list(reader.list_folder("root", recursive=True))
    assert not any(f["mimeType"] == FOLDER_MIME for f in files)
    relpaths = sorted(f["relpath"] for f in files)
    assert relpaths == [
        "05.08.26/.DS_Store",
        "05.08.26/Kempton Park/17_20 _ Kempton _ At The Races.pdf",
        "05.08.26/Kempton Park/Race Result 17_20 KEMPTON PARK Wednesday 05 August.pdf",
    ]


def test_download_file_returns_bytes(reader):
    assert reader.download_file("pdf1") == b"%PDF-1.4 timeform"


def test_download_rejects_an_html_interstitial_without_a_token(monkeypatch):
    rdr = PublicDriveReader()
    monkeypatch.setattr(
        rdr, "_get", lambda url: _FakeResponse(text="<html>nope</html>",
                                               content=b"<html>nope</html>")
    )
    # Silently returning the HTML would poison the parser with a bogus "PDF".
    with pytest.raises(RuntimeError, match="anyone with the link"):
        rdr.download_file("secret")


def test_get_reader_falls_back_when_secret_is_absent(monkeypatch):
    monkeypatch.delenv("GDRIVE_SA_JSON", raising=False)
    assert isinstance(drive_client.get_reader(), PublicDriveReader)


def test_get_reader_falls_back_when_service_account_is_unusable(monkeypatch, tmp_path):
    key = tmp_path / "sa.json"
    key.write_text("{}")
    monkeypatch.setenv("GDRIVE_SA_JSON", str(key))
    monkeypatch.setattr(
        drive_client,
        "_sa_json_path",
        lambda: (_ for _ in ()).throw(RuntimeError("bad key")),
    )
    assert isinstance(drive_client.get_reader(), PublicDriveReader)
