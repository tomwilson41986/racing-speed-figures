"""
French DB → pipeline DataFrame mapping.

Queries the SQLAlchemy database (MeetingRow / RaceRow / RunnerRow),
applies unit conversions, and returns a pandas DataFrame with column
names matching those expected by the speed-figure pipeline.

This mirrors Stage 0 (``load_data`` + ``filter_uk_ire_flat``) in
``src/speed_figures.py``.
"""

import logging

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from .beaten_lengths import compute_cumulative_bl
from .constants import (
    FRANCE_SEX_MAP,
    KG_TO_LBS,
    METERS_PER_FURLONG,
    classify_french_race,
    detect_surface,
    is_maiden_race,
)
from .database import MeetingRow, RaceRow, RunnerRow

log = logging.getLogger(__name__)


def load_france_dataframe(
    session: Session,
    start_date=None,
    end_date=None,
) -> pd.DataFrame:
    """
    Query the French DB and return a pipeline-ready DataFrame.

    Mirrors ``load_data()`` + ``filter_uk_ire_flat()`` from the UK pipeline:
    loads raw data, filters to flat races with valid times, applies unit
    conversions, and builds derived columns (race_id, meeting_id, std_key,
    dist_round, month).

    Parameters
    ----------
    session : SQLAlchemy session
    start_date, end_date : optional date filters

    Returns
    -------
    pd.DataFrame with pipeline-compatible columns.
    """
    log.info("Loading French data from database...")

    # ── Query: join meetings → races → runners ──
    stmt = (
        select(
            MeetingRow.race_date,
            MeetingRow.hippodrome_code,
            MeetingRow.hippodrome_name,
            RaceRow.id.label("db_race_id"),
            RaceRow.course_num,
            RaceRow.race_name,
            RaceRow.distance_m,
            RaceRow.discipline,
            RaceRow.specialite,
            RaceRow.prize_money,
            RaceRow.going,
            RaceRow.parcours,
            RaceRow.num_starters,
            RaceRow.winner_time_s,
            RunnerRow.id.label("db_runner_id"),
            RunnerRow.num_pmu,
            RunnerRow.horse_name,
            RunnerRow.age,
            RunnerRow.sex,
            RunnerRow.finish_position,
            RunnerRow.time_seconds,
            RunnerRow.beaten_lengths,
            RunnerRow.weight_kg,
            RunnerRow.jockey,
            RunnerRow.trainer,
            RunnerRow.sire,
            RunnerRow.dam,
            RunnerRow.odds,
        )
        .join(RaceRow, RaceRow.meeting_id == MeetingRow.id)
        .join(RunnerRow, RunnerRow.race_id == RaceRow.id)
        .where(RaceRow.discipline == "PLAT")
    )

    if start_date is not None:
        stmt = stmt.where(MeetingRow.race_date >= start_date)
    if end_date is not None:
        stmt = stmt.where(MeetingRow.race_date <= end_date)

    rows = session.execute(stmt).all()
    if not rows:
        log.warning("No rows returned from query.")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[c.key for c in stmt.selected_columns])
    log.info("  Raw rows from DB: %s", f"{len(df):,}")

    # ── Map fields to pipeline column names ──
    df = _map_fields(df)

    # ── Filter to valid data ──
    df = _filter_valid(df)

    # ── Compute cumulative beaten lengths ──
    df = compute_cumulative_bl(df)

    # ── Build derived columns ──
    df = _build_derived_columns(df)

    log.info("  Final pipeline DataFrame: %s rows, %s races",
             f"{len(df):,}", f"{df['race_id'].nunique():,}")
    return df


def _map_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Apply unit conversions and rename columns to pipeline format."""
    out = df.copy()

    # ── Date handling ──
    out["meetingDate"] = pd.to_datetime(out["race_date"]).dt.strftime("%Y-%m-%d")
    out["month"] = pd.to_datetime(out["race_date"]).dt.month

    # ── Course ──
    out["courseName"] = out["hippodrome_code"].str.upper().str.strip()

    # ── Race fields ──
    out["raceNumber"] = out["course_num"]
    out["raceType"] = "Flat"

    # Surface detection
    out["raceSurfaceName"] = out.apply(
        lambda r: detect_surface(
            str(r.get("hippodrome_code", "")),
            str(r.get("parcours", "")),
        ),
        axis=1,
    )

    # Class mapping
    out["raceClass"] = out.apply(
        lambda r: classify_french_race(r.get("race_name"), r.get("prize_money")),
        axis=1,
    )

    # Going (keep French description as-is; pipeline uses it for GA priors)
    # No rename needed — column is already "going"

    # Number of runners
    out["numberOfRunners"] = pd.to_numeric(out["num_starters"], errors="coerce")

    # Prize fund
    out["prizeFund"] = pd.to_numeric(out["prize_money"], errors="coerce")

    # ── Distance: meters → furlongs ──
    out["distance_m_raw"] = pd.to_numeric(out["distance_m"], errors="coerce")
    out["distance"] = out["distance_m_raw"] / METERS_PER_FURLONG
    out["distanceFurlongs"] = out["distance"]

    # ── Runner fields ──
    out["positionOfficial"] = pd.to_numeric(out["finish_position"], errors="coerce")
    out["finishingTime"] = pd.to_numeric(out["time_seconds"], errors="coerce")
    out["beaten_lengths_raw"] = out["beaten_lengths"]  # raw text for BL parser
    out["horseName"] = out["horse_name"]
    out["horseAge"] = pd.to_numeric(out["age"], errors="coerce")
    out["jockeyFullName"] = out["jockey"]
    out["trainerFullName"] = out["trainer"]
    out["sireName"] = out["sire"]
    out["damName"] = out["dam"]

    # Weight: kg → lbs
    wt = pd.to_numeric(out["weight_kg"], errors="coerce")
    out["weightCarried"] = wt * KG_TO_LBS

    # Sex code mapping
    out["horseGender"] = out["sex"].map(FRANCE_SEX_MAP).fillna("")

    # Maiden flag (for standard-time filtering)
    out["is_maiden"] = out["race_name"].apply(is_maiden_race)

    return out


def _filter_valid(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to rows with valid finishing times, positions, and distances."""
    mask = (
        df["finishingTime"].notna()
        & (df["finishingTime"] > 0)
        & df["distance"].notna()
        & (df["distance"] > 0)
        & df["positionOfficial"].notna()
    )
    filtered = df[mask].copy()
    log.info("  After validity filter: %s rows", f"{len(filtered):,}")
    return filtered


def _build_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Build race_id, meeting_id, std_key, dist_round — mirrors UK pipeline."""
    # Distance rounding to nearest 0.5 furlongs (same as UK)
    df["dist_round"] = (df["distance"] * 2).round(0) / 2

    # Race ID (unique per race)
    df["race_id"] = (
        df["meetingDate"].astype(str) + "_"
        + df["courseName"].astype(str) + "_"
        + df["raceNumber"].astype(str)
    )

    # Meeting ID — one per track per day per surface (same as UK)
    df["meeting_id"] = (
        df["meetingDate"].astype(str) + "_"
        + df["courseName"].astype(str) + "_"
        + df["raceSurfaceName"].astype(str)
    )

    # Standard-time key — one per track + rounded distance + surface (same as UK)
    df["std_key"] = (
        df["courseName"].astype(str) + "_"
        + df["dist_round"].astype(str) + "_"
        + df["raceSurfaceName"].astype(str)
    )

    return df
