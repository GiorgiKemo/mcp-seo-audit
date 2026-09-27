"""Bounded same-origin sitemap inventory; the caller owns safe HTTP fetching.

The async fetch callback returns an httpx.Response-compatible object and MUST
validate redirect destinations before following them. This module never opens
connections itself. Sitemap rules: https://www.sitemaps.org/protocol.html
"""

from collections import deque
import gzip
from io import BytesIO
from urllib.parse import urlsplit, urlunsplit
from xml.etree import ElementTree


MAX_DOCUMENT_BYTES = 5 * 1024 * 1024
MAX_SKIP_EXAMPLES = 100


def _normalize_url(value):
    if not isinstance(value, str) or not value or any(char.isspace() for char in value) or "\\" in value:
        raise ValueError("Expected an absolute HTTP(S) URL without whitespace.")
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Expected an absolute HTTP(S) URL without credentials.")
    host = parsed.hostname.lower().encode("idna").decode("ascii")
    if ":" in host:
        host = f"[{host}]"
    port = parsed.port
    if port is not None and port != (443 if parsed.scheme.lower() == "https" else 80):
        host += f":{port}"
    return urlunsplit((parsed.scheme.lower(), host, parsed.path or "/", parsed.query, ""))


def _origin(url):
    parsed = urlsplit(url)
    return parsed.scheme, parsed.netloc


def _parse_document(body):
    if len(body) > MAX_DOCUMENT_BYTES:
        raise ValueError("Sitemap exceeds the compressed/downloaded document byte limit.")
    if body.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=BytesIO(body)) as stream:
            body = stream.read(MAX_DOCUMENT_BYTES + 1)
        if len(body) > MAX_DOCUMENT_BYTES:
            raise ValueError("Sitemap exceeds the decompressed document byte limit.")
    text = body.decode("utf-8-sig").strip()
    if not text:
        raise ValueError("Empty sitemap document.")
    if not text.startswith("<"):
        return "urlset", [line.strip() for line in text.splitlines() if line.strip()]
    if "<!DOCTYPE" in text.upper() or "<!ENTITY" in text.upper():
        raise ValueError("DTD and entity declarations are not allowed in sitemaps.")
    root = ElementTree.fromstring(text)
    name = root.tag.rsplit("}", 1)[-1]
    namespace = root.tag[:-len(name)]
    if name not in {"sitemapindex", "urlset"}:
        raise ValueError("Unsupported sitemap root element.")
    if namespace not in {"", "{http://www.sitemaps.org/schemas/sitemap/0.9}"}:
        raise ValueError("Unsupported sitemap XML namespace.")
    entry_name = "sitemap" if name == "sitemapindex" else "url"
    urls = []
    # Only direct <loc> children count: image/video extension URLs are not pages.
    for entry in root:
        if entry.tag != namespace + entry_name:
            continue
        location = ""
        for child in entry:
            if child.tag == namespace + "loc" and child.text:
                location = child.text.strip()
                break
        urls.append(location)
    return name, urls


async def discover_sitemap_urls(origin, fetch, seeds=None, max_sitemaps=20, max_urls=1000):
    """Return unique page candidates, sources, sitemap outcomes and honest coverage.

    Only same-origin absolute sitemap/page URLs are accepted. Query parameters
    survive normalization; fragments do not. Missing or invalid files are recorded
    without aborting independent files. Cancellation propagates to the caller.
    The smaller interactive 5 MiB byte limit is intentional, below the protocol's
    50 MiB maximum. ``seeds=None`` tries /sitemap.xml; ``seeds=[]`` tries no files.
    """
    if type(max_sitemaps) is not int or not 1 <= max_sitemaps <= 100:
        raise ValueError("max_sitemaps must be an integer between 1 and 100.")
    if type(max_urls) is not int or not 1 <= max_urls <= 50000:
        raise ValueError("max_urls must be an integer between 1 and 50000.")
    normalized_origin = _normalize_url(origin)
    scope = _origin(normalized_origin)
    root_url = f"{scope[0]}://{scope[1]}"
    queue, seen, fetched = deque(), set(), set()
    known, sources, documents, skipped = [], {}, [], []
    counts = {"skipped_off_origin": 0, "skipped_invalid": 0, "sitemaps_omitted": 0, "urls_omitted": 0}

    def scoped(value, kind):
        try:
            normalized = _normalize_url(value)
        except (ValueError, UnicodeError):
            reason = "invalid_url"
            counts["skipped_invalid"] += 1
        else:
            if _origin(normalized) == scope:
                return normalized
            reason = "off_origin"
            counts["skipped_off_origin"] += 1
        if len(skipped) < MAX_SKIP_EXAMPLES:
            # Avoid retaining userinfo from malformed/credential-bearing entries.
            display = value if isinstance(value, str) and "@" not in value else "[invalid URL]"
            skipped.append({"url": display, "kind": kind, "reason": reason})
        return None

    def enqueue(value):
        normalized = scoped(value, "sitemap")
        if normalized is None or normalized in seen:
            return
        seen.add(normalized)
        if len(queue) + len(documents) >= max_sitemaps:
            counts["sitemaps_omitted"] += 1
            return
        queue.append(normalized)

    for seed in ([root_url + "/sitemap.xml"] if seeds is None else seeds):
        enqueue(seed)
    while queue and len(known) < max_urls:
        requested = queue.popleft()
        record = {"url": requested}
        documents.append(record)
        try:
            response = await fetch(requested)
            record["status"] = response.status_code
            final = _normalize_url(str(response.url))
            if _origin(final) != scope:
                raise ValueError("Off-origin sitemap redirect rejected; caller must guard redirects before following.")
            record["final_url"] = final
            if not 200 <= response.status_code < 300:
                record["state"] = "http_error"
                continue
            if final in fetched:
                record["state"] = "duplicate_redirect"
                continue
            fetched.add(final)
            kind, entries = _parse_document(response.content)
            record.update({"state": "loaded", "type": kind, "entries": len(entries)})
            if kind == "sitemapindex":
                for entry in entries:
                    enqueue(entry)
            else:
                for entry in entries:
                    normalized = scoped(entry, "page")
                    if normalized is None:
                        continue
                    if normalized in sources:
                        if final not in sources[normalized]:
                            sources[normalized].append(final)
                        continue
                    if len(known) >= max_urls:
                        counts["urls_omitted"] += 1
                        continue
                    known.append(normalized)
                    sources[normalized] = [final]
        except Exception as exc:
            # Fetch exceptions can contain secret request headers/query strings.
            # Persist only the class; our own parser errors have fixed messages.
            record["state"] = "error"
            record["error"] = type(exc).__name__

    failed = sum(item["state"] in {"error", "http_error"} for item in documents)
    truncated = bool(queue or counts["sitemaps_omitted"] or counts["urls_omitted"])
    return {
        "known_urls": known, "sources": sources, "sitemaps": documents, "skipped": skipped,
        "coverage": {
            "attempted_sitemaps": len(documents), "loaded_sitemaps": sum(item["state"] == "loaded" for item in documents),
            "failed_sitemaps": failed, "discovered_urls": len(known), "pending_sitemaps": list(queue),
            "truncated": truncated, "complete_for_scope": bool(documents) and not truncated and not failed and not counts["skipped_invalid"],
            "max_sitemaps": max_sitemaps, "max_urls": max_urls, "max_document_bytes": MAX_DOCUMENT_BYTES, **counts,
        },
        "limitations": [
            "Sitemap discovery is bounded and restricted to the audited origin; cross-origin sitemap delegation is not followed.",
            "Inventory candidates are declarations, not proof of indexability, crawling or Google indexing.",
            "A page absent from sampled internal links is only an orphan candidate, not a proven site-wide orphan.",
        ],
    }
