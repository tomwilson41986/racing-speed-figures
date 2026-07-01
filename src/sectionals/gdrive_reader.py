"""Standalone Google Drive reader — service-account auth, list + download.

Drop-in, dependency-minimal module for pulling files out of a Google Drive
folder using a service-account credential. No CLI, throttling, or gdown
fallback — just the authenticated Drive API v3 core.

Dependencies:
    pip install google-api-python-client google-auth

Setup (one-time):
    1. GCP console -> create a project (free, no billing).
    2. APIs & Services -> Library -> enable "Google Drive API".
    3. IAM & Admin -> Service Accounts -> create one (no roles needed).
    4. On that service account: Keys -> Add key -> JSON -> download it.
    5. Share the target Drive folder with the service account's email
       (<name>@<project>.iam.gserviceaccount.com) as Viewer.

Usage:
    from gdrive_reader import GDriveReader

    reader = GDriveReader("service-account.json")

    # Iterate metadata without downloading:
    for f in reader.list_folder(folder_url_or_id, recursive=True):
        print(f["name"], f["id"], f["mimeType"])

    # Pull a whole folder tree to disk (skips already-downloaded files):
    reader.download_folder(folder_url_or_id, "out/")

    # Grab a single file's bytes:
    data = reader.download_file(file_id)
"""
from __future__ import annotations

import io
import re
from pathlib import Path
from typing import Iterator

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

FOLDER_MIME = "application/vnd.google-apps.folder"
_FOLDER_ID_RE = re.compile(r"/folders/([\w-]+)")
_FILE_ID_RE = re.compile(r"/file/d/([\w-]+)")


def resolve_id(url_or_id: str) -> str:
    """Extract a Drive folder/file ID from a share URL, or pass an ID through."""
    for pattern in (_FOLDER_ID_RE, _FILE_ID_RE):
        match = pattern.search(url_or_id)
        if match:
            return match.group(1)
    return url_or_id


class GDriveReader:
    """Read-only Google Drive client backed by a service account."""

    #: Extra fields requested for every file. Add e.g. "modifiedTime",
    #: "md5Checksum" here if you need them downstream.
    FILE_FIELDS = "id, name, mimeType, size, createdTime, modifiedTime"

    def __init__(
        self,
        service_account_json: str,
        *,
        scopes: list[str] | None = None,
    ) -> None:
        creds = service_account.Credentials.from_service_account_file(
            service_account_json,
            scopes=scopes or ["https://www.googleapis.com/auth/drive.readonly"],
        )
        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)

    # -- listing ----------------------------------------------------------

    def list_children(self, parent_id: str) -> Iterator[dict]:
        """Yield direct children of a folder, transparently paging results."""
        token = None
        while True:
            resp = (
                self.service.files()
                .list(
                    q=f"'{parent_id}' in parents and trashed = false",
                    fields=f"nextPageToken, files({self.FILE_FIELDS})",
                    pageSize=1000,
                    pageToken=token,
                    # Needed to see files in Shared Drives; harmless for My Drive.
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            yield from resp.get("files", [])
            token = resp.get("nextPageToken")
            if not token:
                break

    def list_folder(
        self, folder_url_or_id: str, *, recursive: bool = False
    ) -> Iterator[dict]:
        """Yield file metadata under a folder.

        When ``recursive`` is True, descends into subfolders and adds a
        ``relpath`` key (POSIX path relative to the root folder) to each
        non-folder file. Folders themselves are not yielded.
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
                    subprefix = f"{prefix}{child['name']}/"
                    stack.append((child["id"], subprefix))
                    continue
                child["relpath"] = f"{prefix}{child['name']}"
                yield child

    # -- downloading ------------------------------------------------------

    def download_file(self, file_id: str) -> bytes:
        """Return the full byte content of a Drive file."""
        request = self.service.files().get_media(
            fileId=file_id, supportsAllDrives=True
        )
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    def download_to(self, file_id: str, dest: str | Path) -> Path:
        """Download a file to ``dest`` (a full path) and return it."""
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.download_file(file_id))
        return dest

    def download_folder(
        self,
        folder_url_or_id: str,
        dest_dir: str | Path,
        *,
        skip_existing: bool = True,
        min_existing_bytes: int = 1,
        on_file=None,
    ) -> list[Path]:
        """Recursively download a folder tree into ``dest_dir``.

        Preserves the Drive subfolder structure. Returns the list of local
        paths written (excludes skipped/cached files). ``skip_existing``
        leaves a file alone when a local copy larger than
        ``min_existing_bytes`` already exists. Pass ``on_file(meta, path,
        status)`` to observe progress; ``status`` is "ok" or "skip".
        """
        dest_dir = Path(dest_dir)
        written: list[Path] = []
        for meta in self.list_folder(folder_url_or_id, recursive=True):
            target = dest_dir / meta["relpath"]
            if (
                skip_existing
                and target.exists()
                and target.stat().st_size > min_existing_bytes
            ):
                if on_file:
                    on_file(meta, target, "skip")
                continue
            self.download_to(meta["id"], target)
            written.append(target)
            if on_file:
                on_file(meta, target, "ok")
        return written


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Download a Google Drive folder via a service account."
    )
    parser.add_argument("folder", help="Drive folder URL or ID")
    parser.add_argument("dest", help="Local destination directory")
    parser.add_argument(
        "--service-account-json",
        required=True,
        help="Path to the service-account key JSON.",
    )
    args = parser.parse_args()

    reader = GDriveReader(args.service_account_json)
    reader.download_folder(
        args.folder,
        args.dest,
        on_file=lambda m, p, s: print(f"  {s:4} {p}"),
    )
