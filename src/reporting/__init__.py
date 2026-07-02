"""Shared house-style reporting engine for every email this project sends.

Modules:
    theme      design tokens + swatch selection
    models     jurisdiction-neutral dataclasses (ReportContext, Section, ...)
    context    build a ReportContext from a ratings CSV/DataFrame
    render     Jinja2 environment + render_html
    silks      Racing Post silk SVG -> PNG -> CID
    emailer    EmailMessage builder + SMTP send
    plaintext  multipart/alternative plaintext twin
"""

from .models import Race, ReportContext, Runner, Section, TopPerformer

__all__ = ["Race", "ReportContext", "Runner", "Section", "TopPerformer"]
