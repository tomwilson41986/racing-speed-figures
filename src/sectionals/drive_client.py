"""Read the 'Daily Sectional Reports' Google Drive folder from CI.

GitHub Actions cannot use the interactive Drive connection, so this uses a
Google **service account** (share the folder read-only with its email; store the
JSON as the ``GDRIVE_SA_JSON`` secret — either the JSON itself or a path to it).

Folder layout: ``<root>/DD.MM.YY/<Track>/<race>.pdf``.
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime
from typing import Iterator, List, Optional, Tuple

log = logging.getLogger(__name__)

# 'Daily Sectional Reports' root folder.
SECTIONALS_ROOT_ID = os.environ.get(
    "SECTIONALS_DRIVE_FOLDER_ID", "1tR8_Hhq3vBAuohj48fYtVT8arY8WlsDr"
)
_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def _load_sa_info() -> dict:
    raw = os.environ.get("GDRIVE_SA_JSON", "").strip()
    if not raw:
        raise RuntimeError("GDRIVE_SA_JSON not set (service-account JSON or path)")
    if raw.startswith("{"):
        return json.loads(raw)
    with open(raw) as fh:
        return json.load(fh)


def get_service():
    """Build an authenticated Drive v3 client from the service account."""
    from google.oauth2 import service_account            # lazy imports
    from googleapiclient.discovery import build
    creds = service_account.Credentials.from_service_account_info(
        _load_sa_info(), scopes=_SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_children(service, folder_id: str, mime: Optional[str] = None) -> List[dict]:
    q = f"'{folder_id}' in parents and trashed = false"
    if mime:
        q += f" and mimeType = '{mime}'"
    out, page = [], None
    while True:
        resp = service.files().list(
            q=q, fields="nextPageToken, files(id,name,mimeType)",
            pageSize=1000, pageToken=page,
            supportsAllDrives=True, includeItemsFromAllDrives=True,
        ).execute()
        out.extend(resp.get("files", []))
        page = resp.get("nextPageToken")
        if not page:
            break
    return out


def date_folder_name(date: str) -> str:
    """YYYY-MM-DD -> 'DD.MM.YY' (the Drive folder naming)."""
    return datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%y")


def find_date_folder(service, date: str, root_id: str = SECTIONALS_ROOT_ID) -> Optional[dict]:
    target = date_folder_name(date)
    folder_mime = "application/vnd.google-apps.folder"
    for f in _list_children(service, root_id, mime=folder_mime):
        # tolerate '4.05.26' vs '04.05.26'
        name = f["name"].strip()
        if name == target or name.replace(".0", ".").lstrip("0") == target.replace(".0", ".").lstrip("0"):
            return f
    return None


def iter_race_pdfs(service, date: str, root_id: str = SECTIONALS_ROOT_ID) -> Iterator[Tuple[str, dict]]:
    """Yield (track_name, pdf_file_dict) for every race PDF on ``date``."""
    day = find_date_folder(service, date, root_id)
    if not day:
        log.warning("No Drive folder for %s (%s)", date, date_folder_name(date))
        return
    folder_mime = "application/vnd.google-apps.folder"
    for track in _list_children(service, day["id"], mime=folder_mime):
        for pdf in _list_children(service, track["id"], mime="application/pdf"):
            yield track["name"], pdf


def download_pdf(service, file_id: str, dest_path: str) -> str:
    from googleapiclient.http import MediaIoBaseDownload
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    with io.FileIO(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return dest_path
