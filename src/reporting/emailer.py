"""Build and send the house-style emails.

multipart/alternative (plaintext + HTML) with silks nested as multipart/related
inside the HTML part, per the style guide. SMTP over STARTTLS with a Gmail app
password (reuses the existing SMTP_USER / SMTP_PASS env vars).
"""

from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import List, Optional

from . import plaintext, render, silks
from .models import ReportContext

log = logging.getLogger(__name__)

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")


def build_message(
    context: ReportContext,
    subject: str,
    recipients: List[str],
    sender: Optional[str] = None,
) -> EmailMessage:
    """Render the context and assemble a ready-to-send EmailMessage."""
    sender = sender or SMTP_USER or "racingsquared@gmail.com"

    html = render.render_html(context)
    text = plaintext.render_text(context)
    attachments = silks.resolve_silks(context)  # sets silk_cid on objects
    # Re-render HTML now that silk_cid values are populated.
    html = render.render_html(context)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="racingspeedfigures")

    msg.set_content(text)
    msg.add_alternative(html, subtype="html")

    if attachments:
        html_part = msg.get_payload()[-1]  # the text/html alternative
        for cid, png in attachments.items():
            html_part.add_related(
                png, maintype="image", subtype="png", cid=f"<{cid}>"
            )

    return msg


def send_message(msg: EmailMessage, recipients: List[str]) -> bool:
    if not SMTP_USER or not SMTP_PASS:
        log.error(
            "Email not configured. Set SMTP_USER and SMTP_PASS (Gmail App Password)."
        )
        return False
    try:
        log.info("Connecting to %s:%s...", SMTP_HOST, SMTP_PORT)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg, from_addr=SMTP_USER, to_addrs=recipients)
        log.info("Email sent to %s", recipients)
        return True
    except smtplib.SMTPAuthenticationError as e:
        log.error("SMTP auth failed: %s (use a 16-char Gmail App Password).", e)
        return False
    except Exception as e:  # pragma: no cover
        log.error("Failed to send email: %s", e, exc_info=True)
        return False


def send_report(
    context: ReportContext,
    subject: str,
    recipients: List[str],
    sender: Optional[str] = None,
    dry_run: bool = False,
    save_html_path: Optional[str] = None,
) -> bool:
    """Render + (optionally) send. Returns True on success (or dry-run)."""
    msg = build_message(context, subject, recipients, sender=sender)

    if save_html_path:
        html_part = None
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                html_part = part
        if html_part is not None:
            with open(save_html_path, "w", encoding="utf-8") as fh:
                fh.write(html_part.get_content())
            log.info("Saved HTML preview to %s", save_html_path)

    if dry_run:
        log.info("Dry run: not sending. Subject=%r To=%s", subject, recipients)
        return True

    return send_message(msg, recipients)
