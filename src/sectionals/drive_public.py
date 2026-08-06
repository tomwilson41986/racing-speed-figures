"""Credential-free reader for a *link-shared* Google Drive folder.

The service-account path in :mod:`gdrive_reader` needs a ``GDRIVE_SA_JSON``
secret.  When the folder is shared as "anyone with the link can view" — which
the Daily Sectional Reports root is — Drive will also serve it anonymously,
via two endpoints that need no OAuth at all:

``embeddedfolderview``
    The HTML folder listing Drive uses for embedding.  One ``flip-entry`` div
    per child, carrying the child's id, title and — through its ``aria-label``
    and link shape — whether it is a folder or a file.

``uc?export=download``
    Serves the file bytes for a given id.

This is a strict fallback, not a replacement: it can only see what the owner
has already made link-readable, and it exposes no field the service account
would not.  Its job is to keep the daily jobs running when the secret is
absent, rather than failing the whole workflow.

The public surface mirrors the subset of :class:`GDriveReader` the sectionals
and Timeform pipelines actually call, so the two are interchangeable.
"""

from __future__ import annotations

import html
import logging
import re
import time
from pathlib import Path
from typing import Iterator, Optional

import requests

log = logging.getLogger(__name__)

# Deliberately *not* imported from gdrive_reader: that module pulls in
# google-auth / google-api-python-client at import time, and the whole point of
# this fallback is to work on a runner where those are absent.
FOLDER_MIME = "application/vnd.google-apps.folder"
PDF_MIME = "application/pdf"

_FOLDER_ID_RE = re.compile(r"/folders/([\w-]+)")
_FILE_ID_RE = re.compile(r"/file/d/([\w-]+)")


def resolve_id(url_or_id: str) -> str:
    """Extract a Drive folder/file ID from a share URL, or pass an ID through."""
    for pattern in (_FOLDER_ID_RE, _FILE_ID_RE):
        match = pattern.search(url_or_id)
        if match:
            return match.group(1)
    return url_or_id

_FOLDER_VIEW = "https://drive.google.com/embeddedfolderview?id={id}#list"
_DOWNLOAD = "https://drive.google.com/uc?export=download&id={id}"

# One <div class="flip-entry" id="entry-<ID>"> per child; the anchor that
# follows is /drive/folders/<id> for folders and /file/d/<id> for files.
_ENTRY_RE = re.compile(
    r'<div class="flip-entry"[^>]*id="entry-([\w-]+)".*?'
    r'<a href="(https://drive\.google\.com/[^"]+)".*?'
    r'<div class="flip-entry-title">(.*?)</div>',
    re.S,
)

#: Drive serves an HTML interstitial instead of bytes for large files.
_HTML_SNIFF = (b"<!DOCTYPE html", b"<html", b"<HTML")


class PublicDriveReader:
    """Read-only, unauthenticated view of a link-shared Drive folder."""

    def __init__(self, *, timeout: int = 60, retries: int = 3) -> None:
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        # Drive serves the embed listing to ordinary browsers only.
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            }
        )

    # -- internals --------------------------------------------------------

    def _get(self, url: str) -> requests.Response:
        """GET with a bounded retry — Drive rate-limits bursts with 5xx."""
        last: Optional[Exception] = None
        for attempt in range(self.retries):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code < 400:
                    return resp
                last = RuntimeError(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as exc:  # pragma: no cover - network
                last = exc
            if attempt < self.retries - 1:
                time.sleep(2**attempt)
        raise RuntimeError(f"Drive request failed after {self.retries} tries: {last}")

    # -- listing ----------------------------------------------------------

    def list_children(self, parent_id: str) -> Iterator[dict]:
        """Yield direct children of a link-shared folder as metadata dicts."""
        resp = self._get(_FOLDER_VIEW.format(id=resolve_id(parent_id)))
        for file_id, href, raw_name in _ENTRY_RE.findall(resp.text):
            name = html.unescape(raw_name).strip()
            if "/drive/folders/" in href:
                mime = FOLDER_MIME
            elif name.lower().endswith(".pdf"):
                mime = PDF_MIME
            else:
                mime = "application/octet-stream"
            yield {"id": file_id, "name": name, "mimeType": mime}

    def list_folder(
        self, folder_url_or_id: str, *, recursive: bool = False
    ) -> Iterator[dict]:
        """Yield file metadata under a folder, mirroring ``GDriveReader``.

        With ``recursive``, descends into subfolders and adds a ``relpath``
        key; folders themselves are not yielded.
        """
        root_id = resolve_id(folder_url_or_id)
        if not recursive:
            yield from self.list_children(root_id)
            return

        stack: list[tuple[str, str]] = [(root_id, "")]
        while stack:
            parent_id, prefix = stack.pop()
            for child in self.list_children(parent_id):
                if child["mimeType"] == FOLDER_MIME:
                    stack.append((child["id"], f"{prefix}{child['name']}/"))
                    continue
                child["relpath"] = f"{prefix}{child['name']}"
                yield child

    # -- downloading ------------------------------------------------------

    def download_file(self, file_id: str) -> bytes:
        """Return the bytes of a link-shared Drive file."""
        resp = self._get(_DOWNLOAD.format(id=resolve_id(file_id)))
        data = resp.content
        if not data.lstrip()[:16].startswith(_HTML_SNIFF):
            return data
        # Large files get a "can't scan for viruses" interstitial first; the
        # confirm token in it unlocks the real bytes.
        token = re.search(r'name="confirm"\s+value="([^"]+)"', resp.text)
        if not token:
            raise RuntimeError(
                f"Drive returned HTML, not file bytes, for {file_id} — the file "
                "is probably not shared with 'anyone with the link'."
            )
        return self._get(
            _DOWNLOAD.format(id=resolve_id(file_id)) + f"&confirm={token.group(1)}"
        ).content

    def download_to(self, file_id: str, dest: str | Path) -> Path:
        """Download a file to ``dest`` (a full path) and return it."""
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.download_file(file_id))
        return dest
