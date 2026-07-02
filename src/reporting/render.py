"""Jinja2 environment + HTML rendering for all emails."""

from __future__ import annotations

import os

from jinja2 import Environment, FileSystemLoader, select_autoescape

from . import theme
from .models import ReportContext

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

_env = None


def get_env() -> Environment:
    global _env
    if _env is None:
        env = Environment(
            loader=FileSystemLoader(_TEMPLATE_DIR),
            trim_blocks=True,
            lstrip_blocks=True,
            autoescape=select_autoescape(
                enabled_extensions=("html", "j2", "html.j2"), default=True
            ),
        )
        # Colour-band logic lives in theme.py; expose to macros.
        env.globals.update(
            figure_swatch=theme.figure_swatch,
            pos_swatch=theme.pos_swatch,
            uplift_swatch=theme.uplift_swatch,
            status_swatch=theme.status_swatch,
            T=theme.as_dict(),
            FONT_STACK=theme.FONT_STACK,
        )
        _env = env
    return _env


def render_html(context: ReportContext, template: str = None) -> str:
    tmpl = get_env().get_template(template or context.template)
    return tmpl.render(ctx=context)
