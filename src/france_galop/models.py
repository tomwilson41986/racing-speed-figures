"""SQLAlchemy models for France Galop PDF download tracking.

Extends the existing Base from src.france.database so that
init_db(engine) creates these tables alongside the main racing tables.
"""

from datetime import date, datetime

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Index,
    ForeignKey,
    func,
)
from sqlalchemy.orm import relationship

from src.france.database import Base


class FGMeetingPDF(Base):
    """Tracks a France Galop race meeting visited for PDF downloads."""

    __tablename__ = "fg_meetings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    race_date = Column(Date, nullable=False)
    venue = Column(String(120), nullable=False)
    meeting_url = Column(String(500))
    meeting_fg_id = Column(String(100))
    races_checked = Column(Integer, default=0)
    pdfs_found = Column(Integer, default=0)
    pdfs_downloaded = Column(Integer, default=0)
    checked_at = Column(DateTime, server_default=func.now())

    pdfs = relationship(
        "FGRacePDF", back_populates="meeting", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("race_date", "venue", name="uq_fg_meeting_date_venue"),
        Index("ix_fg_meeting_date", "race_date"),
    )


class FGRacePDF(Base):
    """Tracks an individual PDF download from France Galop."""

    __tablename__ = "fg_race_pdfs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    meeting_id = Column(Integer, ForeignKey("fg_meetings.id"), nullable=False)
    race_number = Column(Integer, nullable=False)
    race_name = Column(String(200))
    pdf_type = Column(String(50), default="sectional_times")
    pdf_url = Column(String(500))
    local_path = Column(String(500))
    file_size_bytes = Column(Integer)
    download_status = Column(String(20), default="pending")
    downloaded_at = Column(DateTime)
    error_message = Column(Text)

    meeting = relationship("FGMeetingPDF", back_populates="pdfs")

    __table_args__ = (
        UniqueConstraint(
            "meeting_id", "race_number", "pdf_type",
            name="uq_fg_pdf_meeting_race_type",
        ),
        Index("ix_fg_pdf_status", "download_status"),
        Index("ix_fg_pdf_meeting", "meeting_id"),
    )
