"""Read the 'Daily Sectional Reports' Google Drive folder from CI.

Thin, sectionals-specific wrapper over the service-account reader in
``gdrive_reader.py``. GitHub Actions cannot use the interactive Drive
connection, so this authenticates with a Google **service account** (share the
folder read-only with its email; store the JSON as the ``GDRIVE_SA_JSON``
secret — either the JSON itself or a path to a file).

Folder layout: ``<root>/DD.MM.YY/<Track>/<race>.pdf``.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Iterator, Optional, Tuple

log = logging.getLogger(__name__)

# 'Daily Sectional Reports' root folder.
SECTIONALS_ROOT_ID = os.environ.get(
    "SECTIONALS_DRIVE_FOLDER_ID", "1tR8_Hhq3vBAuohj48fYtVT8arY8WlsDr"
)
FOLDER_MIME = "application/vnd.google-apps.folder"
PDF_MIME = "application/pdf"


def _sa_json_path() -> str:
    """Resolve GDRIVE_SA_JSON to a file path (writing a temp file if it's JSON)."""
    raw = os.environ.get("GDRIVE_SA_JSON", "").strip()
    if not raw:
        raise RuntimeError("GDRIVE_SA_JSON not set (service-account JSON or path)")
    if raw.startswith("{"):
        json.loads(raw)  # validate
        fh = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        fh.write(raw)
        fh.close()
        return fh.name
    return raw


def get_reader():
    """Build a GDriveReader from the service account (lazy import of google libs)."""
    from .gdrive_reader import GDriveReader
    return GDriveReader(_sa_json_path())


def date_folder_name(date: str) -> str:
    """YYYY-MM-DD -> 'DD.MM.YY' (the Drive folder naming)."""
    return datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%y")


def _norm_folder(name: str) -> str:
    """Normalise 'DD.MM.YY' tolerating dropped leading zeros ('4.05.26')."""
    try:
        return ".".join(str(int(p)) for p in name.strip().split("."))
    except (ValueError, TypeError):
        return name.strip()


def find_date_folder(reader, date: str, root_id: str = SECTIONALS_ROOT_ID) -> Optional[dict]:
    target = _norm_folder(date_folder_name(date))
    for f in reader.list_children(root_id):
        if f.get("mimeType") == FOLDER_MIME and _norm_folder(f["name"]) == target:
            return f
    return None


def iter_race_pdfs(reader, date: str, root_id: str = SECTIONALS_ROOT_ID) -> Iterator[Tuple[str, dict]]:
    """Yield (track_name, pdf_file_dict) for every race PDF on ``date``."""
    day = find_date_folder(reader, date, root_id)
    if not day:
        log.warning("No Drive folder for %s (%s)", date, date_folder_name(date))
        return
    for meta in reader.list_folder(day["id"], recursive=True):
        name = meta.get("name", "")
        is_pdf = meta.get("mimeType") == PDF_MIME or name.lower().endswith(".pdf")
        if not is_pdf:
            continue
        rel = meta.get("relpath", name)
        track = rel.split("/")[0] if "/" in rel else day["name"]
        yield track, meta


def download_pdf(reader, file_id: str, dest_path: str) -> str:
    reader.download_to(file_id, dest_path)
    return dest_path
