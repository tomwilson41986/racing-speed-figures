"""The daily email must never silently vanish: if a send fails (e.g. Gmail's
500-attachment cap), send_report retries once without silk attachments."""

from src.reporting import emailer
from src.reporting.models import Race, ReportContext, Runner, Section, TopPerformer


def _ctx():
    tps = [TopPerformer(rank=i + 1, horse=f"H{i}", course="X", race_number=1,
                        figure=100 - i, silk_cid=f"silk-{i}") for i in range(3)]
    race = Race(course="X", race_number=1, runners=[Runner(horse="H0", figure=100)])
    return ReportContext(title="T", date_str="D",
                         sections=[Section(title="UK", races=[race])],
                         top_performers=tps)


def test_send_report_retries_without_silks_on_failure(monkeypatch):
    calls = []

    def fake_send(msg, recipients):
        # count image parts on each attempt
        imgs = sum(1 for p in msg.walk() if p.get_content_type() == "image/png")
        calls.append(imgs)
        return imgs == 0  # first (with silks) fails, second (no silks) succeeds

    monkeypatch.setattr(emailer, "send_message", fake_send)
    # avoid any network in resolve_silks: no silk_url on objects → no attachments,
    # so force an "attachment" on the first attempt via a stub.
    monkeypatch.setattr(emailer.silks, "resolve_silks",
                        lambda ctx: {"silk-0": b"\x89PNG\r\n\x1a\n"})

    ok = emailer.send_report(_ctx(), "subj", ["a@b.com"])
    assert ok is True
    assert calls == [1, 0], f"expected [with-silks fail, no-silks retry], got {calls}"


def test_build_message_without_silks_has_no_cid_refs():
    msg = emailer.build_message(_ctx(), "subj", ["a@b.com"], with_silks=False)
    html = [p.get_content() for p in msg.walk() if p.get_content_type() == "text/html"][0]
    assert "cid:" not in html
    assert sum(1 for p in msg.walk() if p.get_content_type() == "image/png") == 0
