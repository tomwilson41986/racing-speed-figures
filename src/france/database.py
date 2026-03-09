"""
SQLAlchemy models for French racing data.

SQLite for development, PostgreSQL-ready via connection string.
Usage:
    engine = create_engine("sqlite:///france.db")
    init_db(engine)
    session = get_session(engine)
"""

import json
import logging
from datetime import date, datetime

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine as sa_create_engine,
    func,
    ForeignKey,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Session,
    relationship,
    sessionmaker,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class MeetingRow(Base):
    __tablename__ = "meetings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    race_date = Column(Date, nullable=False)
    reunion_num = Column(Integer, nullable=False)
    hippodrome_code = Column(String(60))
    hippodrome_name = Column(String(120))
    country = Column(String(10))
    created_at = Column(DateTime, server_default=func.now())

    races = relationship("RaceRow", back_populates="meeting", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("race_date", "reunion_num", name="uq_meeting_date_reunion"),
    )


class RaceRow(Base):
    __tablename__ = "races"

    id = Column(Integer, primary_key=True, autoincrement=True)
    meeting_id = Column(Integer, ForeignKey("meetings.id"), nullable=False)
    course_num = Column(Integer, nullable=False)
    race_name = Column(String(200))
    distance_m = Column(Integer)
    discipline = Column(String(20))
    specialite = Column(String(40))
    prize_money = Column(Integer)
    going = Column(String(40))
    parcours = Column(String(40))
    corde = Column(String(20))
    num_starters = Column(Integer)
    winner_time_s = Column(Float)
    standard_time_s = Column(Float)
    going_allowance = Column(Float)
    created_at = Column(DateTime, server_default=func.now())

    meeting = relationship("MeetingRow", back_populates="races")
    runners = relationship("RunnerRow", back_populates="race", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("meeting_id", "course_num", name="uq_race_meeting_course"),
        Index("ix_race_track_dist_going", "meeting_id", "distance_m", "going"),
    )


class RunnerRow(Base):
    __tablename__ = "runners"

    id = Column(Integer, primary_key=True, autoincrement=True)
    race_id = Column(Integer, ForeignKey("races.id"), nullable=False)
    num_pmu = Column(Integer)
    horse_name = Column(String(100))
    age = Column(Integer)
    sex = Column(String(10))
    finish_position = Column(Integer)
    temps_obtenu = Column(Integer)       # raw PMU encoding
    time_seconds = Column(Float)         # converted to seconds
    beaten_lengths = Column(String(20))  # ecart string from API
    weight_kg = Column(Float)
    jockey = Column(String(100))
    trainer = Column(String(100))
    sire = Column(String(100))
    dam = Column(String(100))
    odds = Column(Float)
    speed_figure = Column(Float)
    raw_json = Column(Text)              # full API response for this runner

    race = relationship("RaceRow", back_populates="runners")

    __table_args__ = (
        Index("ix_runner_race_finish", "race_id", "finish_position"),
        Index("ix_runner_horse_date", "horse_name", "race_id"),
    )


class StandardTimeRow(Base):
    """Median winner time for a track + distance + going combination."""
    __tablename__ = "standard_times"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hippodrome_code = Column(String(60), nullable=False)
    distance_m = Column(Integer, nullable=False)
    going = Column(String(40))
    sample_count = Column(Integer, default=0)
    standard_time_s = Column(Float)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("hippodrome_code", "distance_m", "going", name="uq_std_track_dist_going"),
        Index("ix_std_track_dist", "hippodrome_code", "distance_m"),
    )


class GoingAllowanceRow(Base):
    """Going adjustment (seconds) relative to the 'good' standard."""
    __tablename__ = "going_allowances"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hippodrome_code = Column(String(60), nullable=False)
    distance_m = Column(Integer, nullable=False)
    going = Column(String(40), nullable=False)
    allowance_s = Column(Float)
    sample_count = Column(Integer, default=0)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("hippodrome_code", "distance_m", "going", name="uq_ga_track_dist_going"),
    )


class DailyFigureRow(Base):
    """Pre-computed speed figure snapshot per runner per race day."""
    __tablename__ = "daily_figures"

    id = Column(Integer, primary_key=True, autoincrement=True)
    race_date = Column(Date, nullable=False)
    runner_id = Column(Integer, ForeignKey("runners.id"), nullable=False)
    speed_figure = Column(Float)
    weight_adjusted_figure = Column(Float)
    distance_m = Column(Integer)
    hippodrome_code = Column(String(60))
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_daily_date_hippo", "race_date", "hippodrome_code"),
    )


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def init_db(engine):
    """Create all tables if they don't exist."""
    Base.metadata.create_all(engine)
    log.info("Database tables initialised.")


def get_engine(connection_string: str = "sqlite:///france.db"):
    """Create a SQLAlchemy engine from a connection string."""
    return sa_create_engine(connection_string, echo=False)


def get_session(engine) -> Session:
    """Return a new Session bound to *engine*."""
    return sessionmaker(bind=engine)()
