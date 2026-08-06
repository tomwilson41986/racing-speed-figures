"""Offline tests for the Timeform cookie-reuse auth path.

The sign-in form has an image CAPTCHA, so the pipeline authenticates by reusing a
logged-in session cookie instead. Only the pure cookie-header parsing is tested
here — the browser/network login is CI-only (timeform.com is egress-blocked)."""

from src.timeform.auth import SITE_BASE, TimeformAuth, parse_cookie_header


def test_parse_cookie_header_full_header():
    cookies = parse_cookie_header("Cookie: .TFAUTH=abc123==; sid=xyz; ab=1")
    assert [c["name"] for c in cookies] == [".TFAUTH", "sid", "ab"]
    # a value containing '=' (base64 padding) is preserved
    assert cookies[0]["value"] == "abc123=="
    assert all(c["url"] == SITE_BASE for c in cookies)


def test_parse_cookie_header_tolerates_noise():
    assert parse_cookie_header("") == []
    assert parse_cookie_header("   ") == []
    # blank segments and a bare token with no '=' are skipped
    assert [c["name"] for c in parse_cookie_header("a=1; ; junk; b=2")] == ["a", "b"]


def test_cookie_only_auth_is_allowed(monkeypatch):
    """A cookie with no user/pass must satisfy the constructor (no ValueError)."""
    monkeypatch.delenv("TIMEFORM_USER", raising=False)
    monkeypatch.delenv("TIMEFORM_PASS", raising=False)
    monkeypatch.delenv("TF_EMAIL", raising=False)
    monkeypatch.delenv("TF_PASSWORD", raising=False)
    auth = TimeformAuth(cookie="sid=abc")
    assert auth._cookie == "sid=abc"


def test_missing_all_auth_raises(monkeypatch):
    for k in ("TIMEFORM_USER", "TIMEFORM_PASS", "TF_EMAIL", "TF_PASSWORD",
              "TIMEFORM_COOKIE"):
        monkeypatch.delenv(k, raising=False)
    import pytest
    with pytest.raises(ValueError):
        TimeformAuth()
