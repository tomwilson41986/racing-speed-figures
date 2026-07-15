"""Silks (jockey colours): Racing Post SVG -> PNG -> inline CID attachment.

Email clients block remote SVG and sanitise remote ``<img src>``, so the one
image in the design is rasterised at send time and attached by CID. Cards are
laid out silk-optional: if a URL is missing or a render fails, the ``<img>``
falls back to its alt text and the layout still reads.

Populating ``runner.silk_url`` is done upstream (feed ingestion); this module
just turns a URL into an attachable PNG. RP silks are keyed by a per-runner
code, so ``rp_silk_url`` is provided for when that code is available.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from typing import Dict, Optional

log = logging.getLogger(__name__)

from . import theme
from .models import ReportContext

try:  # optional at import time; degrades to alt-text if unavailable
    import resvg_py
except Exception:  # pragma: no cover
    resvg_py = None

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "silks_cache"
)

# Racing Post silk SVG asset host (per the style guide).
RP_SILK_BASE = "https://www.rp-assets.com/svg/{a}/{b}/{c}/{code}.svg"

_PT_DIM_RE = re.compile(r'\s(width|height)\s*=\s*"[^"]*(pt)?"', re.IGNORECASE)


def silk_cid(url: str) -> str:
    """Deterministic CID so the template can reference it before attaching."""
    return "silk-" + hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


def rp_silk_url(code: str) -> str:
    """Build a Racing Post silk SVG URL from a silk code (last 3 digits shard)."""
    code = str(code)
    tail = code[-3:].rjust(3, "0")
    return RP_SILK_BASE.format(a=tail[0], b=tail[1], c=tail[2], code=code)


def _strip_pt_dims(svg: str) -> str:
    """RP silk SVGs declare width/height in pt, which resvg rejects; drop them
    so it falls back to the viewBox + render size."""
    return _PT_DIM_RE.sub("", svg, count=2)


def render_silk_png(svg: str) -> Optional[bytes]:
    """Rasterise silk SVG markup to a white-backed PNG at 2x display size."""
    if resvg_py is None:
        return None
    try:
        data = resvg_py.svg_to_bytes(
            svg_string=_strip_pt_dims(svg),
            background="#ffffff",
            width=theme.SILK_RENDER_W,
            height=theme.SILK_RENDER_H,
        )
        return bytes(data) if data else None
    except Exception as e:  # pragma: no cover
        log.warning("silk render failed: %s", e)
        return None


def _http_get(url: str):
    if requests is None:
        return None
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200 and resp.content:
            return resp
    except Exception as e:  # pragma: no cover
        log.warning("silk fetch failed %s: %s", url, e)
    return None


def _looks_svg(content: bytes, ctype: str, url: str) -> bool:
    if "svg" in ctype or "xml" in ctype:
        return True
    if url.lower().split("?")[0].endswith(".svg"):
        return True
    head = content[:256].lstrip()
    return head.startswith(b"<?xml") or head.startswith(b"<svg")


def _raster_to_png(content: bytes) -> Optional[bytes]:
    """Normalise an already-raster silk (PNG/JPEG, e.g. a PMU casaque) to a
    white-backed PNG letterboxed into the silk render box. Falls back to the
    original bytes if Pillow is unavailable (the <img> tag still scales them)."""
    try:
        import io

        from PIL import Image

        W, H = theme.SILK_RENDER_W, theme.SILK_RENDER_H
        im = Image.open(io.BytesIO(content)).convert("RGBA")
        im.thumbnail((W, H))  # preserve aspect ratio
        canvas = Image.new("RGBA", (W, H), (255, 255, 255, 255))
        canvas.alpha_composite(im, ((W - im.width) // 2, (H - im.height) // 2))
        out = io.BytesIO()
        canvas.convert("RGB").save(out, format="PNG")
        return out.getvalue()
    except Exception as e:  # pragma: no cover - Pillow optional
        log.debug("raster silk normalise failed (%s); using original bytes", e)
        return content or None


def fetch_and_render(url: str) -> Optional[bytes]:
    """Fetch (or read cached) a silk and return PNG bytes, or None.

    Handles both SVG silks (Racing Post → rasterise via resvg) and already-raster
    silks (PMU casaques are PNG → normalise directly)."""
    os.makedirs(_CACHE_DIR, exist_ok=True)
    cache_png = os.path.join(_CACHE_DIR, silk_cid(url) + ".png")
    if os.path.exists(cache_png):
        try:
            with open(cache_png, "rb") as fh:
                return fh.read()
        except Exception:
            pass
    resp = _http_get(url)
    if resp is None:
        return None
    ctype = (resp.headers.get("content-type") or "").lower()
    if _looks_svg(resp.content, ctype, url):
        png = render_silk_png(resp.text)
    else:
        png = _raster_to_png(resp.content)
    if png:
        try:
            with open(cache_png, "wb") as fh:
                fh.write(png)
        except Exception:
            pass
    return png


def resolve_silks(context: ReportContext) -> Dict[str, bytes]:
    """Render all referenced silks, set ``silk_cid`` on each object, and return
    ``{cid: png_bytes}`` for the emailer to attach.

    Deduplicated by URL. Objects whose silk fails to render keep ``silk_cid``
    as None so the template shows alt text.
    """
    attachments: Dict[str, bytes] = {}
    rendered: Dict[str, Optional[str]] = {}  # url -> cid (or None if failed)

    def handle(obj):
        url = getattr(obj, "silk_url", None)
        if not url:
            return
        if url not in rendered:
            png = fetch_and_render(url)
            if png:
                cid = silk_cid(url)
                attachments[cid] = png
                rendered[url] = cid
            else:
                rendered[url] = None
        obj.silk_cid = rendered[url]

    for runner in context.iter_runners():
        handle(runner)
    for tp in context.top_performers:
        handle(tp)

    return attachments
