"""Jurisdiction-neutral data model for the reports.

The UK/IRE and France pipelines emit slightly different CSV columns
(``figure_calibrated`` + furlongs vs ``figure_final`` + metres). ``context.py``
normalises both into these dataclasses so a single template can render any
jurisdiction, the combined live email, or the day-after sectional email.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Runner:
    horse: str
    pos: Optional[int] = None          # finishing position; None = non-finisher
    field_size: Optional[int] = None
    age: Optional[int] = None
    weight: Optional[str] = None        # display string, e.g. "9-7"
    official_rating: Optional[int] = None
    beaten: Optional[float] = None      # cumulative beaten lengths
    time: Optional[str] = None          # finishing / estimated time display
    figure: Optional[float] = None      # the rating shown as the tile

    # Silks (resolved at send time; see silks.py)
    silk_url: Optional[str] = None
    silk_cid: Optional[str] = None      # set once the PNG is attached

    # Sectional day-after extras
    figure_original: Optional[float] = None
    uplift: Optional[float] = None
    reason: Optional[str] = None


@dataclass
class Race:
    course: str
    race_number: int
    distance_str: str = ""
    race_name: Optional[str] = None
    going: Optional[str] = None
    surface: Optional[str] = None
    race_class: Optional[str] = None
    going_allowance: Optional[float] = None
    median_or: Optional[int] = None
    max_or: Optional[int] = None
    video_url: Optional[str] = None     # At The Races review link (day-after)
    runners: List[Runner] = field(default_factory=list)


@dataclass
class Section:
    """A jurisdiction grouping, e.g. 'UK & Ireland' or 'France'."""
    title: Optional[str] = None
    subtitle: Optional[str] = None
    races: List[Race] = field(default_factory=list)

    @property
    def race_count(self) -> int:
        return len(self.races)

    @property
    def runner_count(self) -> int:
        return sum(len(r.runners) for r in self.races)


@dataclass
class TopPerformer:
    rank: int
    horse: str
    course: str
    race_number: int
    figure: float
    pos: Optional[int] = None
    note: Optional[str] = None
    silk_url: Optional[str] = None
    silk_cid: Optional[str] = None


@dataclass
class ReportContext:
    """Everything a template needs to render one email."""
    title: str
    date_str: str                       # "Wednesday 01 July 2026"
    subtitle_facts: List[str] = field(default_factory=list)
    sections: List[Section] = field(default_factory=list)
    top_performers: List[TopPerformer] = field(default_factory=list)
    accuracy_panel: Optional[dict] = None
    par_tables: List[dict] = field(default_factory=list)
    footer_lines: List[str] = field(default_factory=list)
    run_time: Optional[str] = None
    template: str = "email.html.j2"

    def iter_runners(self):
        """All runners across all sections (for silk resolution)."""
        for section in self.sections:
            for race in section.races:
                for runner in race.runners:
                    yield runner
