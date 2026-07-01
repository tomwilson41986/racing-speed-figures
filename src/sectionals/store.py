"""Accumulating store of parsed sectional data (SQLite, S3-synced).

Mirrors the France DB pattern: a local ``sectionals.db`` synced to the existing
S3 bucket so the dataset grows across days. ``sectional_pdf`` tracks each PDF
ingested from the Drive folder; ``sectional_runner`` holds the parsed per-runner
splits used to compute pars and uplifts.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from sqlalchemy import (
    Float, Integer, String, Text, UniqueConstraint, create_engine, select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from .normalize import normalise_name, normalise_track

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_DB = ROOT / "data" / "sectionals.db"
S3_BUCKET = os.environ.get("SECTIONALS_S3_BUCKET", "frenchspeedfigures")
S3_KEY = os.environ.get("SECTIONALS_S3_KEY", "sectionals.db")


class Base(DeclarativeBase):
    pass


class SectionalPDF(Base):
    __tablename__ = "sectional_pdf"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[str] = mapped_column(String(128), index=True)      # Drive id
    file_name: Mapped[str] = mapped_column(String(512))
    race_date: Mapped[str] = mapped_column(String(10), index=True)     # YYYY-MM-DD
    track: Mapped[str] = mapped_column(String(128), index=True)
    s3_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    parsed_ok: Mapped[bool] = mapped_column(Integer, default=0)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ingested_at: Mapped[str] = mapped_column(String(32))
    __table_args__ = (UniqueConstraint("file_id", name="uq_pdf_file_id"),)


class SectionalRunner(Base):
    __tablename__ = "sectional_runner"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    race_date: Mapped[str] = mapped_column(String(10), index=True)
    track: Mapped[str] = mapped_column(String(128), index=True)
    track_norm: Mapped[str] = mapped_column(String(128), index=True)
    country: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    race_time: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    race_key: Mapped[str] = mapped_column(String(160), index=True)     # date_track_time
    distance_m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    distance_band: Mapped[Optional[int]] = mapped_column(Integer, index=True, nullable=True)
    going: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    going_group: Mapped[Optional[str]] = mapped_column(String(32), index=True, nullable=True)
    horse: Mapped[str] = mapped_column(String(128))
    horse_norm: Mapped[str] = mapped_column(String(128), index=True)
    finish_pos: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    overall_time_s: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    final_furlong_s: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    finishing_speed_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    stride_length_m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    stride_freq: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    splits_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_file: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    __table_args__ = (
        UniqueConstraint("race_key", "horse_norm", name="uq_runner"),
    )


def get_engine(db_path: Optional[str] = None):
    path = Path(db_path) if db_path else DEFAULT_DB
    path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{path}")
    Base.metadata.create_all(engine)
    return engine


def get_session(db_path: Optional[str] = None) -> Session:
    return sessionmaker(bind=get_engine(db_path))()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def record_pdf(session: Session, *, file_id: str, file_name: str, race_date: str,
               track: str, s3_key: Optional[str], parsed_ok: bool,
               error: Optional[str] = None) -> None:
    row = session.scalar(select(SectionalPDF).where(SectionalPDF.file_id == file_id))
    if row is None:
        row = SectionalPDF(file_id=file_id)
        session.add(row)
    row.file_name = file_name
    row.race_date = race_date
    row.track = track
    row.s3_key = s3_key
    row.parsed_ok = 1 if parsed_ok else 0
    row.error = error
    row.ingested_at = _now()


def upsert_runner(session: Session, data: dict) -> None:
    """Insert/update one parsed runner (keyed by race_key + normalised horse)."""
    data = dict(data)
    data.setdefault("horse_norm", normalise_name(data.get("horse", "")))
    data.setdefault("track_norm", normalise_track(data.get("track", "")))
    key = (data["race_key"], data["horse_norm"])
    row = session.scalar(
        select(SectionalRunner).where(
            SectionalRunner.race_key == key[0],
            SectionalRunner.horse_norm == key[1],
        )
    )
    if row is None:
        row = SectionalRunner(**{k: v for k, v in data.items() if hasattr(SectionalRunner, k)})
        session.add(row)
    else:
        for k, v in data.items():
            if hasattr(SectionalRunner, k):
                setattr(row, k, v)


def all_runners(session: Session) -> List[SectionalRunner]:
    return list(session.scalars(select(SectionalRunner)))


def runners_for_date(session: Session, race_date: str) -> List[SectionalRunner]:
    return list(
        session.scalars(select(SectionalRunner).where(SectionalRunner.race_date == race_date))
    )


# ── S3 sync (same bucket/pattern as france.db) ────────────────────────
def s3_download(db_path: Optional[str] = None, bucket: str = S3_BUCKET, key: str = S3_KEY) -> bool:
    try:
        import boto3
        path = Path(db_path) if db_path else DEFAULT_DB
        path.parent.mkdir(parents=True, exist_ok=True)
        boto3.client("s3").download_file(bucket, key, str(path))
        log.info("Downloaded s3://%s/%s -> %s", bucket, key, path)
        return True
    except Exception as e:  # first run: no object yet
        log.info("No sectionals DB in S3 (%s); starting fresh.", e)
        return False


def s3_upload(db_path: Optional[str] = None, bucket: str = S3_BUCKET, key: str = S3_KEY) -> bool:
    try:
        import boto3
        path = Path(db_path) if db_path else DEFAULT_DB
        boto3.client("s3").upload_file(str(path), bucket, key)
        log.info("Uploaded %s -> s3://%s/%s", path, bucket, key)
        return True
    except Exception as e:  # pragma: no cover
        log.warning("Sectionals DB S3 upload failed: %s", e)
        return False
