"""
GSC MCP Server â€” Enhanced Fork
Google Search Console + Indexing API + Core Web Vitals integration for MCP.
"""

# â”€â”€ Auto-activate .venv if running from system Python (e.g. Glama Docker) â”€â”€â”€â”€
import os, sys, site, glob
_venv = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".venv")
if os.path.isdir(_venv) and _venv not in sys.prefix:
    # Find any site-packages dir in the venv (version may differ from running Python)
    for _sp in glob.glob(os.path.join(_venv, "lib", "python*", "site-packages")):
        site.addsitedir(_sp)
    # Windows layout
    _sp_win = os.path.join(_venv, "Lib", "site-packages")
    if os.path.isdir(_sp_win):
        site.addsitedir(_sp_win)

from typing import Any, Dict, List, Optional, Set, Tuple
import logging
import json
import asyncio
import time
import math
import gzip
import io
import re
import shutil
import subprocess
import ipaddress
import socket
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter, deque
from html import unescape
from urllib.parse import urlparse, urljoin, urlunsplit, urlsplit
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import httpx
from bs4 import BeautifulSoup
from seo_robots import RobotsPolicy
from seo_google import execute_google, google_service
from seo_network import safe_transport
from seo_sitemaps import discover_sitemap_urls
from seo_rendering import render_page, _run_lighthouse_process
from seo_storage import data_directory
from seo_monitoring import register_monitoring_tools
from seo_reporting import build_audit_report, compare_audit_reports

# Suppress the noisy file_cache warning from google-api-python-client.
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

mcp = FastMCP("mcp-seo-audit")

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Configuration
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

GSC_CREDENTIALS_PATH = os.environ.get("GSC_CREDENTIALS_PATH")
POSSIBLE_CREDENTIAL_PATHS = [
    GSC_CREDENTIALS_PATH,
    os.path.join(SCRIPT_DIR, "service_account_credentials.json"),
    os.path.join(os.getcwd(), "service_account_credentials.json"),
]

OAUTH_CLIENT_SECRETS_FILE = os.environ.get("GSC_OAUTH_CLIENT_SECRETS_FILE")
if not OAUTH_CLIENT_SECRETS_FILE:
    OAUTH_CLIENT_SECRETS_FILE = os.path.join(SCRIPT_DIR, "client_secrets.json")

TOKEN_FILE = os.environ.get("GSC_TOKEN_FILE") or str(data_directory() / "token.json")
LEGACY_TOKEN_FILE = os.path.join(SCRIPT_DIR, "token.json")
SKIP_OAUTH = os.environ.get("GSC_SKIP_OAUTH", "").lower() in ("true", "1", "yes")

_raw_data_state = os.environ.get("GSC_DATA_STATE", "all").lower().strip()
if _raw_data_state not in ("all", "final"):
    raise ValueError(
        f"Invalid GSC_DATA_STATE value '{_raw_data_state}'. "
        "Accepted values are 'all' (default, matches GSC dashboard) or 'final' (2-3 day lag)."
    )
DATA_STATE = _raw_data_state

# GSC API scope (read/write for sitemaps, site management)
GSC_SCOPES = ["https://www.googleapis.com/auth/webmasters"]

# Indexing API scope (separate from GSC)
INDEXING_SCOPES = ["https://www.googleapis.com/auth/indexing"]

# OAuth consent should cover every Google API tool exposed by this server.
ALL_SCOPES = GSC_SCOPES + INDEXING_SCOPES

# CrUX API key (free, no OAuth needed)
CRUX_API_KEY = os.environ.get("CRUX_API_KEY", "")

# PageSpeed Insights API key (optional, but recommended for stable quota)
PAGESPEED_API_KEY = os.environ.get("PAGESPEED_API_KEY", os.environ.get("GOOGLE_API_KEY", ""))

# Optional explicit Chrome path for local Lighthouse runs
LIGHTHOUSE_CHROME_PATH = os.environ.get("LIGHTHOUSE_CHROME_PATH", os.environ.get("CHROME_PATH", ""))

DEFAULT_FETCH_HEADERS = {
    "User-Agent": "mcp-seo-audit/2.1 (+https://github.com/GiorgiKemo/mcp-seo-audit)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.5",
}
DEFAULT_FETCH_TIMEOUT = httpx.Timeout(connect=10.0, read=20.0, write=20.0, pool=20.0)
LIGHTHOUSE_CATEGORIES = {"performance", "accessibility", "best-practices", "seo", "pwa"}
SEO_SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}
FETCH_ALLOWED_SCHEMES = {"http", "https"}
MAX_FETCH_BYTES = int(os.environ.get("SEO_AUDIT_MAX_FETCH_BYTES", str(5 * 1024 * 1024)))
MAX_REDIRECTS = int(os.environ.get("SEO_AUDIT_MAX_REDIRECTS", "5"))
MAX_SITEMAP_URLS = int(os.environ.get("SEO_AUDIT_MAX_SITEMAP_URLS", "50000"))
MAX_CRAWL_PAGES = int(os.environ.get("SEO_AUDIT_MAX_CRAWL_PAGES", "100"))
ALLOW_PRIVATE_URLS = os.environ.get("SEO_AUDIT_ALLOW_PRIVATE_URLS", "").lower() in ("true", "1", "yes")
ENABLE_WRITE_TOOLS = os.environ.get("SEO_AUDIT_ENABLE_WRITE_TOOLS", "").lower() in ("true", "1", "yes")
ALLOW_NPX_LIGHTHOUSE = os.environ.get("SEO_AUDIT_ALLOW_NPX_LIGHTHOUSE", "").lower() in ("true", "1", "yes")
ENABLE_LOCAL_LIGHTHOUSE = os.environ.get("SEO_AUDIT_ENABLE_LOCAL_LIGHTHOUSE", "").lower() in ("true", "1", "yes")
LIGHTHOUSE_BINARY = os.environ.get("LIGHTHOUSE_BINARY", "")
LIGHTHOUSE_NO_SANDBOX = os.environ.get("LIGHTHOUSE_NO_SANDBOX", "").lower() in ("true", "1", "yes")

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Authentication helpers
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_gsc_service_cache = None
_indexing_service_cache = None


def get_gsc_service():
    """Returns an authorized Search Console service object (cached)."""
    global _gsc_service_cache
    if _gsc_service_cache is not None:
        return _gsc_service_cache

    if not SKIP_OAUTH:
        try:
            svc = get_gsc_service_oauth()
            _gsc_service_cache = svc
            return svc
        except Exception:
            pass

    for cred_path in POSSIBLE_CREDENTIAL_PATHS:
        if cred_path and os.path.exists(cred_path):
            try:
                creds = service_account.Credentials.from_service_account_file(
                    cred_path, scopes=GSC_SCOPES
                )
                svc = build("searchconsole", "v1", credentials=creds, cache_discovery=False)
                _gsc_service_cache = svc
                return svc
            except Exception:
                continue

    raise FileNotFoundError(
        "Authentication failed. Please either:\n"
        "1. Set up OAuth by placing a client_secrets.json file in the script directory, or\n"
        "2. Set the GSC_CREDENTIALS_PATH environment variable or place a service account credentials file."
    )


def get_gsc_service_oauth():
    """Returns an authorized Search Console service object using OAuth."""
    creds = None

    token_source = TOKEN_FILE
    if not os.environ.get("GSC_TOKEN_FILE") and not os.path.exists(token_source) and os.path.exists(LEGACY_TOKEN_FILE):
        token_source = LEGACY_TOKEN_FILE
    if os.path.exists(token_source):
        try:
            creds = Credentials.from_authorized_user_file(token_source, ALL_SCOPES)
        except Exception:
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                _save_oauth_token(creds)
            except Exception:
                creds = None

        if not creds or not creds.valid:
            if not os.path.exists(OAUTH_CLIENT_SECRETS_FILE):
                raise FileNotFoundError(
                    "OAuth client secrets file not found. Please place a client_secrets.json "
                    "file in the script directory or set GSC_OAUTH_CLIENT_SECRETS_FILE."
                )
            flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CLIENT_SECRETS_FILE, ALL_SCOPES)
            creds = flow.run_local_server(port=0, timeout_seconds=180, authorization_prompt_message="")
            _save_oauth_token(creds)

    return build("searchconsole", "v1", credentials=creds, cache_discovery=False)


def get_indexing_service():
    """Returns an authorized Indexing API service object (cached).
    Uses OAuth credentials with indexing scope, or service account."""
    global _indexing_service_cache
    if _indexing_service_cache is not None:
        return _indexing_service_cache

    # Try service account first (recommended for Indexing API)
    for cred_path in POSSIBLE_CREDENTIAL_PATHS:
        if cred_path and os.path.exists(cred_path):
            try:
                creds = service_account.Credentials.from_service_account_file(
                    cred_path, scopes=INDEXING_SCOPES
                )
                svc = build("indexing", "v3", credentials=creds, cache_discovery=False)
                _indexing_service_cache = svc
                return svc
            except Exception:
                continue

    # Fall back to OAuth with the same authorized-user token used by Search Console.
    token_source = TOKEN_FILE
    if not os.environ.get("GSC_TOKEN_FILE") and not os.path.exists(token_source) and os.path.exists(LEGACY_TOKEN_FILE):
        token_source = LEGACY_TOKEN_FILE
    if os.path.exists(token_source):
        try:
            creds = Credentials.from_authorized_user_file(token_source, ALL_SCOPES)
            if not creds.valid and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                _save_oauth_token(creds)
            if not creds.valid:
                raise ValueError("OAuth token is invalid and cannot be refreshed.")
            svc = build("indexing", "v3", credentials=creds, cache_discovery=False)
            _indexing_service_cache = svc
            return svc
        except Exception:
            pass

    raise FileNotFoundError(
        "Indexing API authentication failed. The Indexing API requires a service account "
        "with indexing permissions, or OAuth credentials with the indexing scope."
    )


def _site_not_found_error(site_url: str) -> str:
    """Return a helpful message when a GSC property returns 404."""
    lines = [f"Property '{site_url}' not found (404). Possible causes:\n"]
    lines.append(
        "1. The site_url doesn't exactly match what is in GSC. "
        "Run list_properties to get the exact string to use."
    )
    if site_url.startswith("sc-domain:"):
        lines.append(
            "2. Domain properties require the service account to be explicitly added "
            "under GSC Settings > Users and permissions for that specific domain property."
        )
    else:
        lines.append(
            "2. If your property is a domain property (covers all subdomains), "
            "the correct format is 'sc-domain:example.com', not a full URL."
        )
    lines.append("3. The authenticated account may not have access to this property.")
    return "\n".join(lines)


def _ensure_https_url(url_or_origin: str) -> str:
    """Normalize a property/origin/url into a fetchable HTTPS URL."""
    value = (url_or_origin or "").strip()
    if not value:
        raise ValueError("A URL or origin is required.")
    if value.startswith("sc-domain:"):
        return f"https://{value.split(':', 1)[1].strip('/')}"
    parsed = urlparse(value)
    if not parsed.scheme:
        return f"https://{value.lstrip('/')}"
    return value


def _origin_from_url(url_or_origin: str) -> str:
    url = _ensure_https_url(url_or_origin)
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def _url_matches_origin(url: str, origin: str) -> bool:
    parsed_url = urlparse(_ensure_https_url(url))
    parsed_origin = urlparse(_ensure_https_url(origin))
    return (
        parsed_url.scheme == parsed_origin.scheme
        and parsed_url.hostname == parsed_origin.hostname
        and (parsed_url.port or (443 if parsed_url.scheme == "https" else 80)) == (parsed_origin.port or (443 if parsed_origin.scheme == "https" else 80))
    )


def _clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", unescape(value)).strip()


def _clip(value: str, limit: int = 120) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def _status_class(status_code: int) -> str:
    if 200 <= status_code < 300:
        return "ok"
    if 300 <= status_code < 400:
        return "redirect"
    if 400 <= status_code < 500:
        return "client_error"
    if status_code >= 500:
        return "server_error"
    return "unknown"


def _host_is_local_or_private(hostname: str) -> bool:
    host = (hostname or "").strip().lower()
    if not host:
        return False
    host = host.strip("[]")
    if host == "localhost" or host.endswith(".local"):
        return True
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved
    except ValueError:
        return False


def _url_is_local_or_private(url: str) -> bool:
    return _host_is_local_or_private(urlparse(url).hostname or "")


def _hostname_resolves_to_local_or_private(hostname: str) -> bool:
    host = (hostname or "").strip().strip("[]")
    if not host:
        return False
    if _host_is_local_or_private(host):
        return True
    try:
        resolved = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return False
    for family, _, _, _, sockaddr in resolved:
        address = sockaddr[0]
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            continue
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            return True
    return False


def _validate_fetchable_public_url(url: str, *, resolve_dns: bool = True) -> str:
    normalized = _ensure_https_url(url)
    parsed = urlparse(normalized)
    if parsed.scheme not in FETCH_ALLOWED_SCHEMES:
        raise ValueError("Only http and https URLs can be fetched by public SEO audit tools.")
    if not parsed.netloc:
        raise ValueError("A valid URL host is required.")
    if "@" in parsed.netloc:
        raise ValueError("URLs with embedded credentials are not allowed.")
    if not ALLOW_PRIVATE_URLS and (_host_is_local_or_private(parsed.hostname or "") or
                                  (resolve_dns and _hostname_resolves_to_local_or_private(parsed.hostname or ""))):
        raise ValueError(
            "Private, loopback, local, and reserved network targets are blocked by default. "
            "Set SEO_AUDIT_ALLOW_PRIVATE_URLS=true only for trusted local testing."
        )
    return normalized


def _write_tools_disabled(action: str) -> str:
    return (
        f"Write tool disabled: {action}. "
        "Set SEO_AUDIT_ENABLE_WRITE_TOOLS=true only when you intentionally want this MCP server "
        "to mutate Google Search Console or Indexing API state."
    )


def _require_write_tools_enabled(action: str) -> Optional[str]:
    if ENABLE_WRITE_TOOLS:
        return None
    return _write_tools_disabled(action)


def _build_visible_content_soup(html: str) -> BeautifulSoup:
    visible_soup = BeautifulSoup(html, "html.parser")
    for tag_name in ("script", "style", "noscript", "template"):
        for tag in visible_soup.find_all(tag_name):
            tag.decompose()
    return visible_soup


def _seo_findings_from_analysis(analysis: Dict[str, Any]) -> List[Tuple[str, str]]:
    findings: List[Tuple[int, str, str]] = []
    seen: Set[str] = set()

    def add(severity: str, message: str) -> None:
        key = f"{severity}:{message}"
        if key in seen:
            return
        seen.add(key)
        findings.append((SEO_SEVERITY_ORDER[severity], severity, message))

    for issue in analysis.get("issues", []):
        if issue.startswith("HTTP status"):
            add("critical", issue)
        elif issue == "Missing <title>":
            add("high", issue)
        elif issue == "Multiple canonical tags found":
            add("high", issue)
        elif issue == "Page is explicitly marked noindex":
            add("high", issue)
        elif issue == "Page instructs crawlers not to follow links":
            add("high", issue)
        elif issue.startswith("Invalid JSON-LD"):
            add("medium", issue)
        elif issue.startswith("Images missing alt text"):
            add("medium", issue)
        elif issue.startswith("Anchors without href"):
            add("medium", issue)
        elif issue.startswith("Canonical URL"):
            add("medium", issue)
        elif issue in {"Missing meta description", "Missing H1", "No visible body content", "Missing viewport meta tag"}:
            add("medium", issue)
        else:
            add("medium", issue)

    for note in analysis.get("notes", []):
        if note.startswith("Canonical points to another host") or note == "No canonical tag found":
            add("medium", note)
        elif note.startswith("Canonical URL uses http"):
            add("medium", note)
        elif (
            note.startswith("Multiple H1 tags found")
            or note.startswith("Thin visible content")
            or note.startswith("Images with empty alt")
            or note.startswith("Links with empty anchor text")
        ):
            add("low", note)
        elif (
            note.startswith("Title is ")
            or note.startswith("Meta description is ")
            or note.startswith("Missing og:")
            or note.startswith("Missing twitter:")
            or note.startswith("Missing html lang")
            or note.startswith("Meta keywords")
            or note.startswith("Images missing width")
            or note.startswith("Page limits search result snippets")
            or note.endswith("elements use data-nosnippet")
        ):
            add("low", note)

    findings.sort(key=lambda item: (item[0], item[2]))
    return [(severity, message) for _, severity, message in findings]


def _summarize_priority_findings(findings: List[Tuple[str, str]], limit: int = 8) -> List[str]:
    if not findings:
        return ["Priority findings: none"]
    lines = ["Priority findings:"]
    for severity, message in findings[:limit]:
        lines.append(f"  [{severity.upper()}] {message}")
    return lines


def _is_runtime_pagespeed_failure(result: str) -> bool:
    return result.startswith("PageSpeed Insights error") or result.startswith("Error running PageSpeed Insights")


def _is_runtime_lighthouse_failure(result: str) -> bool:
    return (
        result.startswith("Local Lighthouse failed")
        or result.startswith("Local Lighthouse timed out")
        or result.startswith("Local Lighthouse blocked URL")
        or result.startswith("Local Lighthouse returned invalid JSON output")
        or result.startswith("Error running local Lighthouse")
        or result.startswith("No Lighthouse runner available")
    )


async def _build_lighthouse_fallback(url: str, strategy: str, categories: str, reason: str) -> str:
    if not ENABLE_LOCAL_LIGHTHOUSE:
        return reason
    lighthouse_result = await run_lighthouse_audit(url, form_factor=strategy, categories=categories)
    if _is_runtime_lighthouse_failure(lighthouse_result):
        return reason
    return f"{reason}\nLocal Lighthouse fallback:\n{lighthouse_result}"


def _canonicalize_crawl_url(base_url: str, href: str) -> Optional[str]:
    if not href:
        return None
    absolute = urljoin(base_url, href)
    parsed = urlsplit(absolute)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return None
    return urlunsplit((parsed.scheme, parsed.netloc.lower(), parsed.path or "/", parsed.query, ""))


def _buffer_decoded_response(response: httpx.Response, content: bytes) -> httpx.Response:
    # aiter_bytes() has already decoded gzip, Brotli, and other transfer encodings.
    # Retaining those headers would make the cloned response decode the body twice.
    headers = [
        (name, value)
        for name, value in response.headers.multi_items()
        if name.lower() not in {"content-encoding", "content-length"}
    ]
    return httpx.Response(
        response.status_code,
        headers=headers,
        content=content,
        request=response.request,
        extensions=response.extensions,
    )


async def _fetch_url(
    url: str,
    *,
    method: str = "GET",
    follow_redirects: bool = True,
    headers: Optional[Dict[str, str]] = None,
    redirect_validator: Any = None,
    redirect_delay: float = 0,
) -> httpx.Response:
    request_headers = dict(DEFAULT_FETCH_HEADERS)
    if headers:
        request_headers.update(headers)

    async with httpx.AsyncClient(
        transport=safe_transport(ALLOW_PRIVATE_URLS),
        trust_env=False,
        follow_redirects=False,
        timeout=DEFAULT_FETCH_TIMEOUT,
        headers=request_headers,
    ) as client:
        current_url = _validate_fetchable_public_url(url, resolve_dns=False)
        redirects_followed = 0

        while True:
            request = client.build_request(method, current_url)
            response = await client.send(request, stream=True)
            try:
                if follow_redirects and response.is_redirect:
                    if redirects_followed >= MAX_REDIRECTS:
                        raise ValueError(f"Too many redirects while fetching {url}.")
                    location = response.headers.get("location")
                    if not location:
                        return _buffer_decoded_response(response, b"")
                    next_url = urljoin(str(response.url), location)
                    if redirect_validator is not None:
                        redirect_validator(next_url)
                    current_url = _validate_fetchable_public_url(next_url, resolve_dns=False)
                    redirects_followed += 1
                    if redirect_delay:
                        await asyncio.sleep(redirect_delay)
                    continue

                chunks = []
                total = 0
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > MAX_FETCH_BYTES:
                        raise ValueError(
                            f"Response exceeded SEO_AUDIT_MAX_FETCH_BYTES ({MAX_FETCH_BYTES} bytes)."
                        )
                    chunks.append(chunk)

                return _buffer_decoded_response(response, b"".join(chunks))
            finally:
                await response.aclose()


def _extract_json_ld_types(value: Any, types: Set[str]) -> None:
    if isinstance(value, dict):
        schema_type = value.get("@type")
        if isinstance(schema_type, list):
            for item in schema_type:
                if item:
                    types.add(str(item))
        elif schema_type:
            types.add(str(schema_type))
        for nested in value.values():
            _extract_json_ld_types(nested, types)
    elif isinstance(value, list):
        for item in value:
            _extract_json_ld_types(item, types)


def _parse_json_ld_types(soup: BeautifulSoup) -> List[str]:
    discovered: Set[str] = set()
    for script in soup.find_all("script"):
        script_type = (script.get("type") or "").lower()
        if "ld+json" not in script_type:
            continue
        raw = script.string or script.get_text(" ", strip=True)
        raw = raw.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            cleaned = raw.replace("<!--", "").replace("-->", "").strip()
            try:
                parsed = json.loads(cleaned)
            except Exception:
                continue
        _extract_json_ld_types(parsed, discovered)
    return sorted(discovered)


def _count_invalid_json_ld_scripts(soup: BeautifulSoup) -> int:
    invalid = 0
    for script in soup.find_all("script"):
        script_type = (script.get("type") or "").lower()
        if "ld+json" not in script_type:
            continue
        raw = (script.string or script.get_text(" ", strip=True) or "").strip()
        if not raw:
            continue
        try:
            json.loads(raw)
        except json.JSONDecodeError:
            cleaned = raw.replace("<!--", "").replace("-->", "").strip()
            try:
                json.loads(cleaned)
            except Exception:
                invalid += 1
    return invalid


def _parse_meta_tags(soup: BeautifulSoup) -> Dict[str, str]:
    meta: Dict[str, str] = {}
    for tag in soup.find_all("meta"):
        key = (tag.get("name") or tag.get("property") or "").strip().lower()
        value = _clean_text(tag.get("content"))
        if key and value:
            if key in {"robots", "googlebot"} and key in meta:
                meta[key] += ", " + value
            elif key not in meta:
                meta[key] = value
    return meta


def _json_ld_documents(soup: BeautifulSoup) -> List[Any]:
    documents = []
    for script in soup.find_all("script", type=re.compile(r"application/ld\+json", re.I)):
        try:
            documents.append(json.loads((script.string or script.get_text()).replace("<!--", "").replace("-->", "").strip()))
        except (ValueError, TypeError, RecursionError):
            continue
    return documents


def _googlebot_directives(robots_meta: str, googlebot_meta: str, x_robots: Any) -> Set[str]:
    """Combine generic and Googlebot rules without applying other bots' headers."""
    def tokens(value):
        # Parameters such as max-image-preview: none are not standalone rules.
        return {token for part in value.lower().split(",") if ":" not in part for token in part.split()}

    directives = tokens(robots_meta) | tokens(googlebot_meta)
    parameterized_rules = {"max-snippet", "max-image-preview", "max-video-preview", "unavailable_after"}
    for header in ([x_robots] if isinstance(x_robots, str) else x_robots):
        applies = True
        for part in header.lower().split(","):
            scoped = re.match(r"^\s*([a-z][a-z0-9_-]*)\s*:\s*(.*)$", part)
            if scoped and scoped[1] not in parameterized_rules:
                applies = scoped[1] == "googlebot"
                part = scoped[2]
            if applies:
                directives.update(tokens(part))
    if "none" in directives:
        directives.update({"noindex", "nofollow"})
    return directives


def _analyze_html_document(final_url: str, status_code: int, headers: Dict[str, str], html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    base_tag = soup.find("base", href=True)
    link_base = urljoin(final_url, base_tag["href"]) if base_tag else final_url
    visible_soup = _build_visible_content_soup(html)
    meta = _parse_meta_tags(soup)
    html_tag = soup.find("html")
    html_lang = _clean_text(html_tag.get("lang")) if html_tag else ""

    title_text = _clean_text(soup.title.get_text(" ", strip=True) if soup.title else "")
    canonical_tags = []
    for link in soup.find_all("link"):
        rel = link.get("rel") or []
        rel_values = [str(item).lower() for item in (rel if isinstance(rel, list) else [rel])]
        if "canonical" in rel_values:
            href = _clean_text(link.get("href"))
            if href:
                canonical_tags.append(href)

    hreflangs = []
    for link in soup.find_all("link"):
        hreflang = _clean_text(link.get("hreflang"))
        href = _clean_text(link.get("href"))
        if hreflang and href:
            hreflangs.append((hreflang, href))

    headings = {
        "h1": [_clean_text(tag.get_text(" ", strip=True)) for tag in visible_soup.find_all("h1") if _clean_text(tag.get_text(" ", strip=True))],
        "h2": [_clean_text(tag.get_text(" ", strip=True)) for tag in visible_soup.find_all("h2") if _clean_text(tag.get_text(" ", strip=True))],
    }

    # Head metadata is not visible page content. Fragments without <body> still work.
    if visible_soup.head:
        visible_soup.head.decompose()
    body_text = _clean_text((visible_soup.body or visible_soup).get_text(" ", strip=True))
    structured_types = _parse_json_ld_types(soup)
    invalid_json_ld_count = _count_invalid_json_ld_scripts(soup)
    x_robots_values = headers.get_list("x-robots-tag") if isinstance(headers, httpx.Headers) else [headers.get("x-robots-tag", "")]
    x_robots = _clean_text(", ".join(x_robots_values))
    robots_meta = meta.get("robots", "")
    googlebot_meta = meta.get("googlebot", "")
    viewport_meta = meta.get("viewport", "")
    meta_keywords = meta.get("keywords", "")
    data_nosnippet_count = len(visible_soup.select("[data-nosnippet]"))
    images = visible_soup.find_all("img")
    images_missing_alt = []
    images_empty_alt = []
    images_without_size = []
    for image in images:
        src = _clean_text(image.get("src") or image.get("data-src") or image.get("srcset") or "[inline image]")
        if not image.has_attr("alt"):
            images_missing_alt.append(src)
        elif not _clean_text(image.get("alt")):
            images_empty_alt.append(src)
        if not image.get("width") or not image.get("height"):
            images_without_size.append(src)

    anchors = visible_soup.find_all("a")
    anchors_missing_href = []
    anchors_empty_text = []
    internal_links = 0
    external_links = 0
    final_origin = _origin_from_url(final_url)
    for anchor in anchors:
        href = _clean_text(anchor.get("href"))
        image_alt = " ".join(image.get("alt", "") for image in anchor.find_all("img"))
        text = _clean_text(anchor.get_text(" ", strip=True) or anchor.get("aria-label") or image_alt or anchor.get("title"))
        if not href:
            anchors_missing_href.append(text or "[empty anchor]")
            continue
        normalized_href = _canonicalize_crawl_url(link_base, href)
        if normalized_href:
            if _url_matches_origin(normalized_href, final_origin):
                internal_links += 1
            else:
                external_links += 1
        if not text:
            anchors_empty_text.append(href)

    issues: List[str] = []
    notes: List[str] = []

    if status_code >= 400:
        issues.append(f"HTTP status {status_code}")
    if not title_text:
        issues.append("Missing <title>")
    elif len(title_text) > 60:
        notes.append(f"Title is long ({len(title_text)} chars)")
    elif len(title_text) < 15:
        notes.append(f"Title is short ({len(title_text)} chars)")

    description = meta.get("description", "")
    if not description:
        issues.append("Missing meta description")
    elif len(description) > 160:
        notes.append(f"Meta description is long ({len(description)} chars)")
    elif len(description) < 70:
        notes.append(f"Meta description is short ({len(description)} chars)")

    if len(canonical_tags) > 1:
        issues.append("Multiple canonical tags found")
    if canonical_tags:
        canonical_url = canonical_tags[0]
        parsed_canonical = urlparse(canonical_url)
        parsed_final = urlparse(final_url)
        if not parsed_canonical.scheme or not parsed_canonical.netloc:
            issues.append("Canonical URL is not absolute")
        if parsed_canonical.fragment:
            issues.append("Canonical URL contains a fragment")
        if parsed_final.scheme == "https" and parsed_canonical.scheme == "http":
            notes.append("Canonical URL uses http on an https page")
        if (
            parsed_canonical.netloc
            and parsed_canonical.netloc != parsed_final.netloc
            and not _url_is_local_or_private(final_url)
        ):
            notes.append(f"Canonical points to another host: {canonical_url}")
    else:
        notes.append("No canonical tag found")

    directives = _googlebot_directives(robots_meta, googlebot_meta, x_robots_values)
    if "noindex" in directives:
        issues.append("Page is explicitly marked noindex")
    if "nofollow" in directives:
        issues.append("Page instructs crawlers not to follow links")
    if "nosnippet" in directives:
        notes.append("Page limits search result snippets with nosnippet")

    if not headings["h1"]:
        issues.append("Missing H1")
    elif len(headings["h1"]) > 1:
        notes.append(f"Multiple H1 tags found ({len(headings['h1'])})")

    if not body_text:
        issues.append("No visible body content")
    elif len(body_text.split()) < 80:
        notes.append(f"Thin visible content ({len(body_text.split())} words)")

    if not viewport_meta:
        issues.append("Missing viewport meta tag")
    if not html_lang:
        notes.append("Missing html lang attribute")
    if meta_keywords:
        notes.append("Meta keywords tag is present; Google Search ignores it")
    if data_nosnippet_count:
        notes.append(f"{data_nosnippet_count} elements use data-nosnippet")
    if invalid_json_ld_count:
        issues.append(f"Invalid JSON-LD scripts found ({invalid_json_ld_count})")
    if images_missing_alt:
        issues.append(f"Images missing alt text ({len(images_missing_alt)})")
    if images_empty_alt:
        notes.append(f"Images with empty alt text ({len(images_empty_alt)})")
    if images_without_size:
        notes.append(f"Images missing width or height attributes ({len(images_without_size)})")
    if anchors_missing_href:
        issues.append(f"Anchors without href are not crawlable ({len(anchors_missing_href)})")
    if anchors_empty_text:
        notes.append(f"Links with empty anchor text ({len(anchors_empty_text)})")

    if not meta.get("og:title"):
        notes.append("Missing og:title")
    if not meta.get("og:description"):
        notes.append("Missing og:description")
    if not meta.get("og:image"):
        notes.append("Missing og:image")
    if not meta.get("twitter:card"):
        notes.append("Missing twitter:card")

    return {
        "title": title_text,
        "meta_description": description,
        "meta": meta,
        "canonicals": canonical_tags,
        "hreflangs": hreflangs,
        "headings": headings,
        "structured_types": structured_types,
        "x_robots_tag": x_robots,
        "viewport": viewport_meta,
        "html_lang": html_lang,
        "issues": issues,
        "notes": notes,
        "body_word_count": len(body_text.split()) if body_text else 0,
        "images": {
            "total": len(images),
            "missing_alt": images_missing_alt,
            "empty_alt": images_empty_alt,
            "missing_size": images_without_size,
        },
        "links": {
            "total": len(anchors),
            "internal": internal_links,
            "external": external_links,
            "missing_href": anchors_missing_href,
            "empty_text": anchors_empty_text,
        },
        "invalid_json_ld_count": invalid_json_ld_count,
        "nofollow": "nofollow" in directives,
        "structured_data": _json_ld_documents(soup),
        "alternate_languages": [
            {"language": _clean_text(link.get("hreflang")), "url": urljoin(final_url, link.get("href", "")),
             "raw_url": link.get("href", "")}
            for link in (soup.head.find_all("link", hreflang=True, href=True) if soup.head else [])
            if "alternate" in [str(rel).lower() for rel in link.get("rel", [])]
        ],
    }


def _iter_internal_links(final_url: str, html: str) -> List[str]:
    soup = _build_visible_content_soup(html)
    base_tag = soup.find("base", href=True)
    link_base = urljoin(final_url, base_tag["href"]) if base_tag else final_url
    parsed_final = urlparse(final_url)
    origin = f"{parsed_final.scheme}://{parsed_final.netloc}"
    links: List[str] = []
    seen: Set[str] = set()
    for anchor in soup.find_all("a", href=True):
        if "nofollow" in [str(rel).lower() for rel in anchor.get("rel", [])]:
            continue
        normalized = _canonicalize_crawl_url(link_base, anchor["href"])
        if not normalized:
            continue
        parsed = urlparse(normalized)
        if f"{parsed.scheme}://{parsed.netloc}" != origin:
            continue
        if normalized not in seen:
            seen.add(normalized)
            links.append(normalized)
    return links


def _extract_xml_text(response: httpx.Response) -> str:
    content = response.content
    # HTTP transfer decoding is already done by _fetch_url. Sitemap .gz files
    # can contain another gzip layer, whose expanded size also needs a bound.
    if content.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as compressed:
            content = compressed.read(MAX_FETCH_BYTES + 1)
    if len(content) > MAX_FETCH_BYTES:
        raise ValueError(
            f"Sitemap exceeded SEO_AUDIT_MAX_FETCH_BYTES ({MAX_FETCH_BYTES} bytes) after decoding."
        )
    return content.decode(response.encoding or "utf-8", errors="replace")


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _parse_sitemap_document(xml_text: str) -> Dict[str, Any]:
    sitemap_text = (xml_text or "").strip().lstrip("\ufeff").strip()
    if not sitemap_text:
        raise ValueError("Empty sitemap document.")

    if not sitemap_text.startswith("<"):
        urls = []
        for raw_line in sitemap_text.splitlines():
            loc = raw_line.strip()
            if not loc or loc.startswith("#"):
                continue
            urls.append({"loc": loc, "lastmod": None})
            if len(urls) >= MAX_SITEMAP_URLS:
                break
        if not urls:
            raise ValueError("Plain-text sitemap contains no URLs.")
        return {"type": "urlset", "sitemaps": [], "urls": urls, "lastmod_count": 0}

    root = ET.fromstring(sitemap_text)
    root_name = _local_name(root.tag)
    if root_name == "sitemapindex":
        sitemaps = []
        for node in root.findall(".//"):
            if _local_name(node.tag) == "loc" and node.text:
                sitemaps.append(node.text.strip())
                if len(sitemaps) >= MAX_SITEMAP_URLS:
                    break
        return {"type": "sitemapindex", "sitemaps": sitemaps, "urls": []}

    if root_name == "urlset":
        urls = []
        lastmods = 0
        for url_node in root.findall(".//"):
            if _local_name(url_node.tag) == "url":
                loc = None
                lastmod = None
                for child in list(url_node):
                    child_name = _local_name(child.tag)
                    if child_name == "loc" and child.text:
                        loc = child.text.strip()
                    elif child_name == "lastmod" and child.text:
                        lastmod = child.text.strip()
                if loc:
                    urls.append({"loc": loc, "lastmod": lastmod})
                    if lastmod:
                        lastmods += 1
                    if len(urls) >= MAX_SITEMAP_URLS:
                        break
        return {"type": "urlset", "sitemaps": [], "urls": urls, "lastmod_count": lastmods}

    raise ValueError(f"Unsupported sitemap root element: {root_name}")


def _format_loading_experience(experience: Dict[str, Any], label: str) -> List[str]:
    if not experience:
        return [f"{label}: no field data"]
    lines = [f"{label}:"]
    for metric_name, metric in experience.get("metrics", {}).items():
        percentile = metric.get("percentile")
        category = metric.get("category", "N/A")
        lines.append(f"  {metric_name}: p75={percentile} ({category})")
    return lines


def _summarize_lighthouse_payload(payload: Dict[str, Any], source_label: str) -> str:
    result = payload.get("lighthouseResult", payload)
    categories = result.get("categories", {})
    audits = result.get("audits", {})
    lines = [source_label]

    if categories:
        lines.append("Category scores:")
        for key in ["performance", "seo", "accessibility", "best-practices", "pwa"]:
            category = categories.get(key)
            if not category:
                continue
            score = category.get("score")
            score_text = "N/A" if score is None else f"{round(score * 100)}"
            lines.append(f"  {key}: {score_text}")

    metric_ids = [
        "first-contentful-paint",
        "largest-contentful-paint",
        "speed-index",
        "interactive",
        "total-blocking-time",
        "cumulative-layout-shift",
    ]
    lines.append("Key metrics:")
    for audit_id in metric_ids:
        audit = audits.get(audit_id)
        if not audit:
            continue
        value = audit.get("displayValue") or audit.get("numericValue") or "N/A"
        lines.append(f"  {audit.get('title', audit_id)}: {value}")

    opportunities = []
    for audit_id, audit in audits.items():
        score = audit.get("score")
        savings_ms = 0
        details = audit.get("details") or {}
        if isinstance(details, dict):
            savings_ms = details.get("overallSavingsMs") or 0
        numeric_value = audit.get("numericValue") or 0
        if score is not None and score < 0.9:
            opportunities.append(
                (
                    savings_ms,
                    numeric_value,
                    audit.get("title", audit_id),
                    audit.get("displayValue", ""),
                )
            )
    opportunities.sort(key=lambda item: (item[0], item[1]), reverse=True)
    if opportunities:
        lines.append("Top opportunities / failing audits:")
        for _, _, title, display_value in opportunities[:8]:
            suffix = f" ({display_value})" if display_value else ""
            lines.append(f"  {title}{suffix}")

    return "\n".join(lines)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Property Management Tools
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def list_properties() -> str:
    """Retrieves and returns the user's Search Console properties."""
    try:
        service = await google_service(get_gsc_service)
        site_list = await execute_google(service.sites().list())
        sites = site_list.get("siteEntry", [])

        if not sites:
            return "No Search Console properties found."

        lines = []
        for site in sites:
            site_url = site.get("siteUrl", "Unknown")
            permission = site.get("permissionLevel", "Unknown permission")
            lines.append(f"- {site_url} ({permission})")

        return "\n".join(lines)
    except Exception as e:
        return f"Error retrieving properties: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def add_site(site_url: str) -> str:
    """
    Add a site to your Search Console properties.

    Args:
        site_url: The URL of the site to add (e.g. https://example.com or sc-domain:example.com)
    """
    gate = _require_write_tools_enabled("add Search Console property")
    if gate:
        return gate
    try:
        service = await google_service(get_gsc_service)
        await execute_google(service.sites().add(siteUrl=site_url), read_only=False)
        return f"Site {site_url} has been added to Search Console."
    except HttpError as e:
        error_code = e.resp.status
        if error_code == 409:
            return f"Site {site_url} is already added to Search Console."
        return f"Error adding site (HTTP {error_code}): {str(e)}"
    except Exception as e:
        return f"Error adding site: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=True))
async def delete_site(site_url: str) -> str:
    """
    Remove a site from your Search Console properties.

    Args:
        site_url: The URL of the site to remove
    """
    gate = _require_write_tools_enabled("delete Search Console property")
    if gate:
        return gate
    try:
        service = await google_service(get_gsc_service)
        await execute_google(service.sites().delete(siteUrl=site_url), read_only=False)
        return f"Site {site_url} has been removed from Search Console."
    except HttpError as e:
        if e.resp.status == 404:
            return f"Site {site_url} was not found in Search Console."
        return f"Error removing site (HTTP {e.resp.status}): {str(e)}"
    except Exception as e:
        return f"Error removing site: {str(e)}"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Search Analytics Tools
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

GSC_DIMENSIONS = {"query", "page", "device", "country", "date", "searchAppearance"}
GSC_FILTER_DIMENSIONS = GSC_DIMENSIONS - {"date"}
GSC_FILTER_OPERATORS = {"contains", "equals", "notContains", "notEquals", "includingRegex", "excludingRegex"}


def _gsc_today():
    """Search Console dates use Pacific time, including daylight saving time."""
    return datetime.now(ZoneInfo("America/Los_Angeles")).date()


def _gsc_date_window(days: int) -> Tuple[Any, Any]:
    if not 1 <= days <= 500:
        raise ValueError("days must be between 1 and 500.")
    end_date = _gsc_today()
    return end_date - timedelta(days=days - 1), end_date


def _validate_gsc_dates(start_date: str, end_date: str) -> Tuple[Any, Any]:
    values = []
    for value in (start_date, end_date):
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            raise ValueError("Dates must use YYYY-MM-DD format.") from None
        if parsed.isoformat() != value:
            raise ValueError("Dates must use YYYY-MM-DD format.")
        values.append(parsed)
    if values[0] > values[1]:
        raise ValueError("start_date must be on or before end_date.")
    return values[0], values[1]


def _gsc_dimensions(dimensions: str) -> List[str]:
    values = [value.strip() for value in dimensions.split(",")] if dimensions.strip() else []
    if any(value not in GSC_DIMENSIONS for value in values) or len(values) != len(set(values)):
        raise ValueError("dimensions must contain unique values from query, page, device, country, date, searchAppearance.")
    return values


def _gsc_search_type(search_type: str) -> str:
    values = {"web": "web", "image": "image", "video": "video", "news": "news", "discover": "discover", "googlenews": "googleNews"}
    value = values.get(search_type.strip().lower())
    if value is None:
        raise ValueError("search_type must be WEB, IMAGE, VIDEO, NEWS, DISCOVER, or googleNews.")
    return value


def _gsc_coverage_notes(response: Dict[str, Any], row_limit: int, data_state: str = DATA_STATE) -> List[str]:
    notes = ["Coverage: returned Search Console rows only; query rows omit anonymized queries and API limits can omit additional rows."]
    if row_limit > 0 and len(response.get("rows", [])) >= row_limit:
        notes.append("Row limit reached; this result is a bounded sample, not a complete inventory.")
    incomplete = response.get("metadata", {}).get("first_incomplete_date")
    if incomplete:
        notes.append(f"Freshness: data from {incomplete} onward is incomplete and may change.")
    elif data_state == "all":
        notes.append("Freshness: data_state=all includes recent data that may be incomplete and change.")
    notes.append("Date boundaries are inclusive and use America/Los_Angeles (Pacific time).")
    return notes


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def get_search_analytics(
    site_url: str,
    days: int = 28,
    dimensions: str = "query",
    row_limit: int = 20,
    search_type: str = "WEB",
) -> str:
    """
    Get search analytics data for a specific property.

    Args:
        site_url: Exact GSC property URL (e.g. "sc-domain:example.com")
        days: Number of days to look back (default: 28)
        dimensions: Dimensions to group by, comma-separated (query, page, device, country, date, searchAppearance)
        row_limit: Number of rows to return (default: 20, max: 500)
        search_type: Type of search results (WEB, IMAGE, VIDEO, NEWS, DISCOVER)
    """
    try:
        start_date, end_date = _gsc_date_window(days)
        dimension_list = _gsc_dimensions(dimensions)
        resolved_type = _gsc_search_type(search_type)
        if row_limit < 1:
            return "row_limit must be positive."
        service = await google_service(get_gsc_service)

        request = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "dimensions": dimension_list,
            "rowLimit": min(max(1, row_limit), 500),
            "type": resolved_type,
            "dataState": DATA_STATE,
        }

        response = await execute_google(service.searchanalytics().query(siteUrl=site_url, body=request))

        if not response.get("rows"):
            return f"No search analytics data found for {site_url} in the last {days} days."

        result_lines = [f"Search analytics for {site_url} (last {days} days, type={search_type}):"]
        result_lines.append("-" * 80)

        header = [dim.capitalize() for dim in dimension_list] + ["Clicks", "Impressions", "CTR", "Position"]
        result_lines.append(" | ".join(header))
        result_lines.append("-" * 80)

        for row in response.get("rows", []):
            data = [v[:100] for v in row.get("keys", [])]
            data.append(str(row.get("clicks", 0)))
            data.append(str(row.get("impressions", 0)))
            data.append(f"{row.get('ctr', 0) * 100:.2f}%")
            data.append(f"{row.get('position', 0):.1f}")
            result_lines.append(" | ".join(data))

        result_lines.extend(_gsc_coverage_notes(response, request["rowLimit"] if dimension_list else 0))
        return "\n".join(result_lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error retrieving search analytics: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def get_advanced_search_analytics(
    site_url: str,
    start_date: str = None,
    end_date: str = None,
    dimensions: str = "query",
    search_type: str = "WEB",
    row_limit: int = 1000,
    start_row: int = 0,
    sort_by: str = "clicks",
    sort_direction: str = "descending",
    filter_dimension: str = None,
    filter_operator: str = "contains",
    filter_expression: str = None,
    filters: str = None,
    data_state: str = None,
) -> str:
    """
    Get advanced search analytics with sorting, filtering (including regex), and pagination.

    Args:
        site_url: Exact GSC property URL (e.g. "sc-domain:example.com")
        start_date: Inclusive start date YYYY-MM-DD (defaults to 28-day window ending at end_date)
        end_date: Inclusive end date YYYY-MM-DD (defaults to today in Pacific time)
        dimensions: Dimensions comma-separated (query,page,device,country,date,searchAppearance)
        search_type: WEB, IMAGE, VIDEO, NEWS, DISCOVER
        row_limit: Max rows (up to 25000)
        start_row: Starting row for pagination
        sort_by: Metric to sort returned API page locally (clicks, impressions, ctr, position)
        sort_direction: ascending or descending
        filter_dimension: Single filter dimension (query, page, country, device)
        filter_operator: contains, equals, notContains, notEquals, includingRegex, excludingRegex
        filter_expression: Filter value
        filters: JSON array of filter objects for AND logic. Each needs dimension, operator, expression.
        data_state: "all" (default) or "final" (confirmed only, 2-3 day lag)
    """
    try:
        if not end_date:
            end_date = _gsc_today().isoformat()
        if not start_date:
            _, parsed_end = _validate_gsc_dates(end_date, end_date)
            start_date = (parsed_end - timedelta(days=27)).isoformat()
        _validate_gsc_dates(start_date, end_date)
        if row_limit < 1 or start_row < 0:
            return "row_limit must be positive and start_row must be non-negative."

        resolved_data_state = (data_state or DATA_STATE).lower().strip()
        if resolved_data_state not in ("all", "final"):
            return f"Invalid data_state '{data_state}'. Use 'all' or 'final'."

        dimension_list = _gsc_dimensions(dimensions)
        resolved_type = _gsc_search_type(search_type)

        request = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimension_list,
            "rowLimit": min(row_limit, 25000),
            "startRow": start_row,
            "type": resolved_type,
            "dataState": resolved_data_state,
        }

        if sort_by not in {"clicks", "impressions", "ctr", "position"}:
            return "sort_by must be clicks, impressions, ctr, or position."
        direction = (sort_direction or "").lower().strip()
        if direction not in {"ascending", "asc", "descending", "desc"}:
            return "sort_direction must be ascending, asc, descending, or desc."
        descending = direction in {"descending", "desc"}

        active_filters = []
        if filters:
            try:
                filter_list = json.loads(filters)
            except json.JSONDecodeError:
                return "Invalid filters JSON."
            if not isinstance(filter_list, list) or not filter_list:
                return "Expected a non-empty JSON array of filter objects."
            for f in filter_list:
                if not isinstance(f, dict) or not all(k in f for k in ("dimension", "operator", "expression")):
                    return f"Each filter must have dimension, operator, expression. Invalid: {f}"
            request["dimensionFilterGroups"] = [{"filters": filter_list}]
            active_filters = filter_list
        elif filter_dimension is not None or filter_expression is not None:
            single = {"dimension": filter_dimension, "operator": filter_operator, "expression": filter_expression}
            request["dimensionFilterGroups"] = [{"filters": [single]}]
            active_filters = [single]

        for item in active_filters:
            if item["dimension"] not in GSC_FILTER_DIMENSIONS or item["operator"] not in GSC_FILTER_OPERATORS:
                return "Invalid filter dimension or operator."
            if not isinstance(item["expression"], str) or not item["expression"] or len(item["expression"]) > 4096:
                return "Filter expression must be a non-empty string of at most 4096 characters."
        service = await google_service(get_gsc_service)
        response = await execute_google(service.searchanalytics().query(siteUrl=site_url, body=request))

        if not response.get("rows"):
            return f"No data found for {site_url} with the specified parameters."

        result_lines = [f"Search analytics for {site_url} ({start_date} to {end_date}, type={search_type}):"]
        if active_filters:
            filter_desc = " AND ".join(f"{f['dimension']} {f['operator']} '{f['expression']}'" for f in active_filters)
            result_lines.append(f"Filters: {filter_desc}")
        result_lines.append(f"API rows {start_row + 1} to {start_row + len(response['rows'])}; locally sorted by {sort_by} {direction} within this returned page.")
        result_lines.append("API pagination follows clicks descending (date ascending when grouped by date); local sorting is not a global ranking.")
        result_lines.append("-" * 80)

        header = [d.capitalize() for d in dimension_list] + ["Clicks", "Impressions", "CTR", "Position"]
        result_lines.append(" | ".join(header))
        result_lines.append("-" * 80)

        for row in sorted(response["rows"], key=lambda item: item.get(sort_by, 0), reverse=descending):
            data = [v[:100] for v in row.get("keys", [])]
            data.append(str(row.get("clicks", 0)))
            data.append(str(row.get("impressions", 0)))
            data.append(f"{row.get('ctr', 0) * 100:.2f}%")
            data.append(f"{row.get('position', 0):.1f}")
            result_lines.append(" | ".join(data))

        if len(response["rows"]) == request["rowLimit"]:
            result_lines.append(f"\nMore results may be available. Use start_row: {start_row + request['rowLimit']}")
        result_lines.extend(_gsc_coverage_notes(response, request["rowLimit"] if dimension_list else 0, resolved_data_state))

        return "\n".join(result_lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def get_performance_overview(site_url: str, days: int = 28) -> str:
    """
    Get a performance overview with totals and daily trend.

    Args:
        site_url: Exact GSC property URL
        days: Number of days to look back (default: 28)
    """
    try:
        start_date, end_date = _gsc_date_window(days)
        service = await google_service(get_gsc_service)
        date_range = {"startDate": start_date.strftime("%Y-%m-%d"), "endDate": end_date.strftime("%Y-%m-%d")}

        total_response = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={**date_range, "dimensions": [], "rowLimit": 1, "dataState": DATA_STATE},
        ))

        date_response = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={**date_range, "dimensions": ["date"], "rowLimit": days, "dataState": DATA_STATE},
        ))

        result_lines = [f"Performance Overview for {site_url} (last {days} days):", "-" * 80]

        if total_response.get("rows"):
            row = total_response["rows"][0]
            result_lines.append(f"Total Clicks: {row.get('clicks', 0):,}")
            result_lines.append(f"Total Impressions: {row.get('impressions', 0):,}")
            result_lines.append(f"Average CTR: {row.get('ctr', 0) * 100:.2f}%")
            result_lines.append(f"Average Position: {row.get('position', 0):.1f}")
        else:
            return "No data available for the selected period."

        if date_response.get("rows"):
            result_lines.append("\nDaily Trend:")
            result_lines.append("Date | Clicks | Impressions | CTR | Position")
            result_lines.append("-" * 60)
            for row in sorted(date_response["rows"], key=lambda x: x["keys"][0]):
                d = row["keys"][0]
                result_lines.append(
                    f"{d} | {row.get('clicks', 0)} | {row.get('impressions', 0)} | "
                    f"{row.get('ctr', 0) * 100:.2f}% | {row.get('position', 0):.1f}"
                )

        result_lines.extend(_gsc_coverage_notes(date_response, 0))
        return "\n".join(result_lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def compare_search_periods(
    site_url: str,
    period1_start: str,
    period1_end: str,
    period2_start: str,
    period2_end: str,
    dimensions: str = "query",
    limit: int = 20,
) -> str:
    """
    Compare search analytics between two time periods.

    Args:
        site_url: Exact GSC property URL
        period1_start: Start date for period 1 (YYYY-MM-DD)
        period1_end: End date for period 1
        period2_start: Start date for period 2
        period2_end: End date for period 2
        dimensions: Dimensions to group by (default: query)
        limit: Top N results to compare (default: 20)
    """
    try:
        start1, end1 = _validate_gsc_dates(period1_start, period1_end)
        start2, end2 = _validate_gsc_dates(period2_start, period2_end)
        dimension_list = _gsc_dimensions(dimensions)
        if not 1 <= limit <= 1000:
            return "limit must be between 1 and 1000."
        service = await google_service(get_gsc_service)

        base = {"dimensions": dimension_list, "rowLimit": 1000, "dataState": DATA_STATE}
        p1 = await execute_google(service.searchanalytics().query(
            siteUrl=site_url, body={**base, "startDate": period1_start, "endDate": period1_end}
        ))
        p2 = await execute_google(service.searchanalytics().query(
            siteUrl=site_url, body={**base, "startDate": period2_start, "endDate": period2_end}
        ))

        p1_data = {tuple(r.get("keys", [])): r for r in p1.get("rows", [])}
        p2_data = {tuple(r.get("keys", [])): r for r in p2.get("rows", [])}
        all_keys = set(p1_data) | set(p2_data)

        comparisons = []
        for key in all_keys:
            r1 = p1_data.get(key)
            r2 = p2_data.get(key)
            click_diff = r2.get("clicks", 0) - r1.get("clicks", 0) if r1 is not None and r2 is not None else None
            p1_pos = r1.get("position") if r1 and r1.get("impressions", 0) > 0 else None
            p2_pos = r2.get("position") if r2 and r2.get("impressions", 0) > 0 else None
            pos_diff = p1_pos - p2_pos if p1_pos is not None and p2_pos is not None else None
            comparisons.append({"key": key, "p1_clicks": r1.get("clicks", 0) if r1 is not None else None,
                                "p2_clicks": r2.get("clicks", 0) if r2 is not None else None,
                                "click_diff": click_diff, "p1_pos": p1_pos,
                                "p2_pos": p2_pos, "pos_diff": pos_diff})

        comparisons.sort(key=lambda x: (x["click_diff"] is not None, abs(x["click_diff"] or 0), x["key"]), reverse=True)

        result_lines = [
            f"Comparison for {site_url}:",
            f"Period 1: {period1_start} to {period1_end}",
            f"Period 2: {period2_start} to {period2_end}",
            "-" * 100,
            f"{' | '.join(d.capitalize() for d in dimension_list)} | P1 Clicks | P2 Clicks | Change | P1 Pos | P2 Pos | Pos Change",
            "-" * 100,
        ]
        if (end1 - start1) != (end2 - start2):
            result_lines.append("Warning: periods have unequal lengths; click totals are not normalized per day.")
        result_lines.append("N/A means not returned or unavailable, not zero. Missing sampled rows do not prove a query was new or lost.")

        for item in comparisons[:limit]:
            key_str = " | ".join(str(k)[:80] for k in item["key"]) or "Property total"
            p1_clicks = f"{item['p1_clicks']:g}" if item["p1_clicks"] is not None else "N/A"
            p2_clicks = f"{item['p2_clicks']:g}" if item["p2_clicks"] is not None else "N/A"
            click_change = f"{item['click_diff']:+g}" if item["click_diff"] is not None else "N/A"
            p1_pos = f"{item['p1_pos']:.1f}" if item["p1_pos"] is not None else "N/A"
            p2_pos = f"{item['p2_pos']:.1f}" if item["p2_pos"] is not None else "N/A"
            pos_change = f"{item['pos_diff']:+.1f}" if item["pos_diff"] is not None else "N/A"
            result_lines.append(
                f"{key_str} | {p1_clicks} | {p2_clicks} | {click_change} | {p1_pos} | {p2_pos} | {pos_change}"
            )

        result_lines.append(f"Returned rows: period 1={len(p1_data)}, period 2={len(p2_data)}; maximum 1000 per period.")
        result_lines.extend(_gsc_coverage_notes(p1, 1000))
        if len(p2_data) >= 1000:
            result_lines.append("Period 2 row limit reached; comparison is a bounded sample.")
        return "\n".join(result_lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def get_search_by_page_query(site_url: str, page_url: str, days: int = 28, row_limit: int = 20) -> str:
    """
    Get search queries driving traffic to a specific page.

    Args:
        site_url: Exact GSC property URL
        page_url: The specific page URL to analyze
        days: Days to look back (default: 28)
        row_limit: Rows to return (default: 20, max: 500)
    """
    try:
        start_date, end_date = _gsc_date_window(days)
        if row_limit < 1:
            return "row_limit must be positive."
        service = await google_service(get_gsc_service)

        response = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d"),
                "dimensions": ["query"],
                "dimensionFilterGroups": [{"filters": [{"dimension": "page", "operator": "equals", "expression": page_url}]}],
                "rowLimit": min(max(1, row_limit), 500),
                "dataState": DATA_STATE,
            },
        ))

        if not response.get("rows"):
            return f"No search data found for {page_url} in the last {days} days."

        result_lines = [f"Queries for {page_url} (last {days} days):", "-" * 80,
                        "Query | Clicks | Impressions | CTR | Position", "-" * 80]

        for row in response["rows"]:
            q = row["keys"][0][:100]
            result_lines.append(
                f"{q} | {row.get('clicks', 0)} | {row.get('impressions', 0)} | "
                f"{row.get('ctr', 0) * 100:.2f}% | {row.get('position', 0):.1f}"
            )

        total_clicks = sum(r.get("clicks", 0) for r in response["rows"])
        total_imp = sum(r.get("impressions", 0) for r in response["rows"])
        result_lines.append("-" * 80)
        result_lines.append(f"RETURNED ROWS TOTAL | {total_clicks} | {total_imp} | {(total_clicks / total_imp * 100) if total_imp else 0:.2f}%")
        result_lines.extend(_gsc_coverage_notes(response, min(row_limit, 500)))

        return "\n".join(result_lines)
    except Exception as e:
        return f"Error: {str(e)}"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# URL Inspection Tools
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def inspect_url(site_url: str, page_url: str) -> str:
    """
    Inspect a URL for indexing status, rich results, and mobile usability.

    Args:
        site_url: Exact GSC property URL (e.g. "sc-domain:example.com")
        page_url: The specific URL to inspect
    """
    try:
        service = await google_service(get_gsc_service)
        response = await execute_google(service.urlInspection().index().inspect(
            body={"inspectionUrl": page_url, "siteUrl": site_url}
        ))

        if not response or "inspectionResult" not in response:
            return f"No inspection data found for {page_url}."

        inspection = response["inspectionResult"]
        index_status = inspection.get("indexStatusResult", {})

        result_lines = [f"URL Inspection for {page_url}:", "-" * 80]

        if "inspectionResultLink" in inspection:
            result_lines.append(f"GSC Link: {inspection['inspectionResultLink']}")

        result_lines.append(f"Verdict: {index_status.get('verdict', 'UNKNOWN')}")
        if "coverageState" in index_status:
            result_lines.append(f"Coverage: {index_status['coverageState']}")
        if "lastCrawlTime" in index_status:
            result_lines.append(f"Last Crawled: {index_status['lastCrawlTime'][:10]}")
        if "pageFetchState" in index_status:
            result_lines.append(f"Page Fetch: {index_status['pageFetchState']}")
        if "robotsTxtState" in index_status:
            result_lines.append(f"Robots.txt: {index_status['robotsTxtState']}")
        if "indexingState" in index_status:
            result_lines.append(f"Indexing: {index_status['indexingState']}")
        if "googleCanonical" in index_status:
            result_lines.append(f"Google Canonical: {index_status['googleCanonical']}")
        if "userCanonical" in index_status and index_status.get("userCanonical") != index_status.get("googleCanonical"):
            result_lines.append(f"User Canonical: {index_status['userCanonical']}")
        if "crawledAs" in index_status:
            result_lines.append(f"Crawled As: {index_status['crawledAs']}")

        referring = index_status.get("referringUrls", [])
        if referring:
            result_lines.append(f"\nReferring URLs ({len(referring)}):")
            for url in referring[:5]:
                result_lines.append(f"  - {url}")

        rich = inspection.get("richResultsResult", {})
        if rich:
            result_lines.append(f"\nRich Results: {rich.get('verdict', 'UNKNOWN')}")
            for item in rich.get("detectedItems", []):
                result_lines.append(f"  - {item.get('richResultType', 'Unknown')}")

        mobile = inspection.get("mobileUsabilityResult", {})
        if mobile and mobile.get("verdict") != "VERDICT_UNSPECIFIED":
            result_lines.append(f"\nMobile Usability: {mobile.get('verdict', 'UNKNOWN')}")

        return "\n".join(result_lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error inspecting URL: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def batch_inspect_urls(site_url: str, urls: str) -> str:
    """
    Inspect multiple URLs for indexing status. Handles rate limiting automatically.
    API limit: 2000/day, 600/minute. This tool handles up to 50 URLs per call.

    Args:
        site_url: Exact GSC property URL (e.g. "sc-domain:example.com")
        urls: List of URLs to inspect, one per line
    """
    try:
        service = await google_service(get_gsc_service)
        url_list = [u.strip() for u in urls.split("\n") if u.strip()]

        if not url_list:
            return "No URLs provided."
        if len(url_list) > 50:
            return f"Too many URLs ({len(url_list)}). Limit to 50 per batch."

        categories = {"indexed": [], "crawled_not_indexed": [], "not_found": [],
                      "blocked": [], "unknown": [], "error": []}
        details = []

        for i, page_url in enumerate(url_list):
            try:
                response = await execute_google(service.urlInspection().index().inspect(
                    body={"inspectionUrl": page_url, "siteUrl": site_url}
                ))

                idx = response.get("inspectionResult", {}).get("indexStatusResult", {})
                verdict = idx.get("verdict", "UNKNOWN")
                coverage = idx.get("coverageState", "Unknown")
                crawled = idx.get("lastCrawlTime", "never")
                crawl_date = crawled[:10] if crawled != "never" else "never"
                canonical = idx.get("googleCanonical", "")

                short = page_url.split("//", 1)[-1] if "//" in page_url else page_url
                details.append(f"{short} | {verdict} | {coverage} | crawled: {crawl_date}")

                if verdict == "PASS":
                    categories["indexed"].append(page_url)
                elif "not indexed" in coverage.lower():
                    categories["crawled_not_indexed"].append(page_url)
                elif "not found" in coverage.lower() or "404" in coverage.lower():
                    categories["not_found"].append(page_url)
                elif idx.get("robotsTxtState") == "BLOCKED":
                    categories["blocked"].append(page_url)
                else:
                    categories["unknown"].append(f"{page_url} ({coverage})")

                # Rate limiting: 600/min = 10/sec, be conservative
                if i < len(url_list) - 1:
                    await asyncio.sleep(0.15)

            except Exception as e:
                categories["error"].append(f"{page_url}: {str(e)[:80]}")

        result_lines = [f"Batch Inspection for {site_url} ({len(url_list)} URLs):", "-" * 80]
        result_lines.append(f"Indexed: {len(categories['indexed'])}")
        result_lines.append(f"Crawled not indexed: {len(categories['crawled_not_indexed'])}")
        result_lines.append(f"Not found (404): {len(categories['not_found'])}")
        result_lines.append(f"Blocked: {len(categories['blocked'])}")
        result_lines.append(f"Unknown/Other: {len(categories['unknown'])}")
        result_lines.append(f"Errors: {len(categories['error'])}")
        result_lines.append("-" * 80)
        result_lines.append("\nDetailed results:")
        result_lines.extend(details)

        for cat_name, cat_list in categories.items():
            if cat_list and cat_name not in ("indexed",):
                result_lines.append(f"\n{cat_name.upper().replace('_', ' ')}:")
                for item in cat_list:
                    result_lines.append(f"  - {item}")

        return "\n".join(result_lines)
    except Exception as e:
        return f"Error: {str(e)}"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Sitemap Tools
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def get_sitemaps(site_url: str) -> str:
    """
    List all sitemaps for a property with detailed info.

    Args:
        site_url: Exact GSC property URL
    """
    try:
        service = await google_service(get_gsc_service)
        sitemaps = await execute_google(service.sitemaps().list(siteUrl=site_url))

        if not sitemaps.get("sitemap"):
            return f"No sitemaps found for {site_url}."

        result_lines = [f"Sitemaps for {site_url}:", "-" * 100,
                        "Path | Last Downloaded | Type | URLs | Errors | Warnings", "-" * 100]

        for sm in sitemaps["sitemap"]:
            path = sm.get("path", "Unknown")
            last_dl = sm.get("lastDownloaded", "Never")
            if last_dl != "Never":
                try:
                    last_dl = datetime.fromisoformat(last_dl.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    pass

            sm_type = "Index" if sm.get("isSitemapsIndex", False) else "Sitemap"
            errors = int(sm.get("errors", 0))
            warnings = int(sm.get("warnings", 0))

            url_count = "N/A"
            for c in sm.get("contents", []):
                if c.get("type") == "web":
                    url_count = c.get("submitted", "0")
                    break

            result_lines.append(f"{path} | {last_dl} | {sm_type} | {url_count} | {errors} | {warnings}")

        return "\n".join(result_lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=True))
async def submit_sitemap(site_url: str, sitemap_url: str) -> str:
    """
    Submit or resubmit a sitemap to Google.

    Args:
        site_url: Exact GSC property URL
        sitemap_url: Full URL of the sitemap to submit
    """
    gate = _require_write_tools_enabled("submit sitemap")
    if gate:
        return gate
    try:
        service = await google_service(get_gsc_service)
        await execute_google(service.sitemaps().submit(siteUrl=site_url, feedpath=sitemap_url), read_only=False)
        return f"Successfully submitted sitemap: {sitemap_url}\nGoogle will queue it for processing."
    except Exception as e:
        return f"Error submitting sitemap: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=True))
async def delete_sitemap(site_url: str, sitemap_url: str) -> str:
    """
    Delete (unsubmit) a sitemap from Google Search Console.

    Args:
        site_url: Exact GSC property URL
        sitemap_url: Full URL of the sitemap to delete
    """
    gate = _require_write_tools_enabled("delete sitemap")
    if gate:
        return gate
    try:
        service = await google_service(get_gsc_service)
        await execute_google(service.sitemaps().delete(siteUrl=site_url, feedpath=sitemap_url), read_only=False)
        return f"Deleted sitemap: {sitemap_url}\nAlready-indexed URLs will remain in Google's index."
    except Exception as e:
        return f"Error deleting sitemap: {str(e)}"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# NEW: Google Indexing API Tools
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=True))
async def request_indexing(url: str) -> str:
    """
    Request Google to crawl and index a URL via the Indexing API.
    IMPORTANT: Only works for pages with JobPosting or BroadcastEvent structured data.
    Default quota: 200 requests/day.

    Args:
        url: The full URL to request indexing for
    """
    gate = _require_write_tools_enabled("request indexing")
    if gate:
        return gate
    try:
        service = await google_service(get_indexing_service)
        response = await execute_google(service.urlNotifications().publish(
            body={"url": url, "type": "URL_UPDATED"}
        ), read_only=False)

        notify_time = response.get("urlNotificationMetadata", {}).get("latestUpdate", {}).get("notifyTime", "unknown")
        return (
            f"Indexing requested for: {url}\nNotification time: {notify_time}\n"
            "Google accepted the notification. Crawling and indexing are not guaranteed."
        )
    except HttpError as e:
        if e.resp.status == 429:
            return "Rate limit exceeded. Check your project's Indexing API quotas and retry after the applicable limit resets."
        elif e.resp.status == 403:
            return (
                f"Permission denied for Indexing API. Ensure:\n"
                f"1. The Indexing API is enabled in Google Cloud Console\n"
                f"2. Your service account has 'Owner' permission in GSC for this site\n"
                f"3. The page has JobPosting or BroadcastEvent structured data"
            )
        return f"Error (HTTP {e.resp.status}): {str(e)}"
    except Exception as e:
        return f"Error requesting indexing: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=True))
async def request_removal(url: str) -> str:
    """
    Request Google to remove a URL from the index via the Indexing API.
    IMPORTANT: Only works for pages with JobPosting or BroadcastEvent structured data.

    Args:
        url: The full URL to request removal for
    """
    gate = _require_write_tools_enabled("request URL removal")
    if gate:
        return gate
    try:
        service = await google_service(get_indexing_service)
        response = await execute_google(service.urlNotifications().publish(
            body={"url": url, "type": "URL_DELETED"}
        ), read_only=False)

        return (
            f"Removal requested for: {url}\n"
            "Google accepted the deletion notification. This does not confirm removal from search results."
        )
    except HttpError as e:
        if e.resp.status == 429:
            return "Rate limit exceeded. Check your project's Indexing API quotas and retry after the applicable limit resets."
        elif e.resp.status == 403:
            return "Permission denied. Ensure the Indexing API is enabled and you have Owner permission."
        return f"Error (HTTP {e.resp.status}): {str(e)}"
    except Exception as e:
        return f"Error requesting removal: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=True))
async def batch_request_indexing(urls: str) -> str:
    """
    Request indexing for multiple URLs. Processes sequentially with rate limiting.
    Default quota: 200/day. Only for pages with JobPosting or BroadcastEvent structured data.

    Args:
        urls: List of URLs to index, one per line (max 100 per batch)
    """
    gate = _require_write_tools_enabled("batch request indexing")
    if gate:
        return gate
    try:
        url_list = [u.strip() for u in urls.split("\n") if u.strip()]

        if not url_list:
            return "No URLs provided."
        if len(url_list) > 100:
            return f"Too many URLs ({len(url_list)}). Max 100 per batch."

        service = await google_service(get_indexing_service)
        results = {"success": [], "failed": [], "not_attempted": []}

        for index, url in enumerate(url_list):
            try:
                await execute_google(service.urlNotifications().publish(
                    body={"url": url, "type": "URL_UPDATED"}
                ), read_only=False)
                results["success"].append(url)
                await asyncio.sleep(0.5)  # Rate limiting
            except HttpError as e:
                if e.resp.status == 429:
                    results["failed"].append(f"{url}: Rate limit exceeded")
                    results["not_attempted"] = url_list[index + 1:]
                    break  # Stop on rate limit
                results["failed"].append(f"{url}: HTTP {e.resp.status}")
            except Exception as e:
                results["failed"].append(f"{url}: {str(e)[:60]}")

        lines = [f"Batch Indexing Results:", "-" * 60,
                 f"Requested: {len(url_list)}", f"Submitted: {len(results['success'])}",
                 f"Failed: {len(results['failed'])}", f"Not attempted: {len(results['not_attempted'])}"]

        if results["failed"]:
            lines.append("\nFailed URLs:")
            for f in results["failed"]:
                lines.append(f"  - {f}")

        if results["not_attempted"]:
            lines.append("\nNot attempted after the rate limit response:")
            for pending_url in results["not_attempted"]:
                lines.append(f"  - {pending_url}")
        lines.append("\nSubmitted means Google accepted the notification; crawling and indexing are not guaranteed.")
        lines.append("Remaining project quota is not available from these responses.")
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def check_indexing_notification(url: str) -> str:
    """
    Check the latest indexing notification status for a URL.

    Args:
        url: The URL to check
    """
    try:
        service = await google_service(get_indexing_service)
        response = await execute_google(service.urlNotifications().getMetadata(url=url))

        lines = [f"Indexing notification status for: {url}", "-" * 60]

        latest_update = response.get("latestUpdate", {})
        if latest_update:
            lines.append(f"Latest update type: {latest_update.get('type', 'unknown')}")
            lines.append(f"Notify time: {latest_update.get('notifyTime', 'unknown')}")
            lines.append(f"URL: {latest_update.get('url', 'unknown')}")

        latest_remove = response.get("latestRemove", {})
        if latest_remove:
            lines.append(f"\nLatest removal type: {latest_remove.get('type', 'unknown')}")
            lines.append(f"Notify time: {latest_remove.get('notifyTime', 'unknown')}")

        return "\n".join(lines)
    except HttpError as e:
        if e.resp.status == 404:
            return f"No indexing notifications found for {url}. This URL hasn't been submitted via the Indexing API."
        return f"Error (HTTP {e.resp.status}): {str(e)}"
    except Exception as e:
        return f"Error: {str(e)}"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# NEW: Core Web Vitals (CrUX API)
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _format_crux_metric(metric_data: dict, name: str) -> str:
    """Format a single CrUX metric."""
    if not metric_data:
        return f"  {name}: No data"

    percentiles = metric_data.get("percentiles", {})
    p75 = percentiles.get("p75")

    histogram = metric_data.get("histogram", [])
    good = histogram[0].get("density", 0) * 100 if len(histogram) > 0 else 0
    needs_improvement = histogram[1].get("density", 0) * 100 if len(histogram) > 1 else 0
    poor = histogram[2].get("density", 0) * 100 if len(histogram) > 2 else 0

    return f"  {name}: p75={p75} | Good: {good:.0f}% | Needs Improvement: {needs_improvement:.0f}% | Poor: {poor:.0f}%"


def _redact_api_keys(message: str) -> str:
    """Redact configured API keys before clipping or returning provider errors."""
    safe_message = str(message)
    for api_key in (CRUX_API_KEY, PAGESPEED_API_KEY):
        if api_key:
            safe_message = safe_message.replace(api_key, "[REDACTED]")
    return re.sub(
        r"(?i)(\b(?:key|api_key|access_token|client_secret)=)[^&\s<>\"']+",
        r"\1[REDACTED]",
        safe_message,
    )


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def get_core_web_vitals(url_or_origin: str, form_factor: str = "PHONE") -> str:
    """
    Get Core Web Vitals (LCP, INP, CLS) from the Chrome UX Report (CrUX) API.
    Free API, no OAuth needed â€” just a CRUX_API_KEY env variable.

    Args:
        url_or_origin: Full URL or origin (e.g. "https://example.com" for origin-level)
        form_factor: PHONE, DESKTOP, or TABLET (default: PHONE)
    """
    if not CRUX_API_KEY:
        return (
            "CrUX API key not configured. Set the CRUX_API_KEY environment variable.\n"
            "Get a free key at: https://console.cloud.google.com/apis/credentials\n"
            "Enable the 'Chrome UX Report API' in your Google Cloud project."
        )
    form_factor_value = form_factor.upper().strip()
    if form_factor_value not in {"PHONE", "DESKTOP", "TABLET"}:
        return "Invalid form_factor. Use PHONE, DESKTOP, or TABLET."

    # Determine if it's a specific URL or an origin (no path beyond /)
    normalized_url = _ensure_https_url(url_or_origin)
    body = {"formFactor": form_factor_value}
    parsed = urlparse(normalized_url)
    has_path = parsed.path not in ("", "/")
    if has_path:
        body["url"] = normalized_url
    else:
        body["origin"] = _origin_from_url(normalized_url).rstrip("/")

    try:
        async with httpx.AsyncClient(timeout=DEFAULT_FETCH_TIMEOUT, headers=DEFAULT_FETCH_HEADERS) as client:
            response = await client.post(
                "https://chromeuxreport.googleapis.com/v1/records:queryRecord",
                params={"key": CRUX_API_KEY},
                json=body,
            )
        if response.status_code == 404:
            return f"No CrUX data available for {normalized_url}. The site may not have enough traffic for Chrome to collect data."
        response.raise_for_status()
        data = response.json()

        record = data.get("record", {})
        metrics = record.get("metrics", {})
        key = record.get("key", {})

        lines = [f"Core Web Vitals for {key.get('url') or key.get('origin', url_or_origin)}:"]
        lines.append(f"Form factor: {key.get('formFactor', form_factor)}")
        lines.append(f"Collection period: {data.get('record', {}).get('collectionPeriod', {}).get('firstDate', {}).get('year', '?')}-{data.get('record', {}).get('collectionPeriod', {}).get('firstDate', {}).get('month', '?')} to {data.get('record', {}).get('collectionPeriod', {}).get('lastDate', {}).get('year', '?')}-{data.get('record', {}).get('collectionPeriod', {}).get('lastDate', {}).get('month', '?')}")
        lines.append("-" * 60)

        lines.append(_format_crux_metric(metrics.get("largest_contentful_paint"), "LCP (Largest Contentful Paint)"))
        lines.append(_format_crux_metric(metrics.get("interaction_to_next_paint"), "INP (Interaction to Next Paint)"))
        lines.append(_format_crux_metric(metrics.get("cumulative_layout_shift"), "CLS (Cumulative Layout Shift)"))
        lines.append(_format_crux_metric(metrics.get("first_contentful_paint"), "FCP (First Contentful Paint)"))
        lines.append(_format_crux_metric(metrics.get("experimental_time_to_first_byte"), "TTFB (Time to First Byte)"))

        lines.append("\n--- Assessment ---")
        assessments = []
        for metric, label, threshold, unit in (
            ("largest_contentful_paint", "LCP", 2500, "ms"),
            ("interaction_to_next_paint", "INP", 200, "ms"),
            ("cumulative_layout_shift", "CLS", 0.1, ""),
        ):
            raw_p75 = metrics.get(metric, {}).get("percentiles", {}).get("p75")
            try:
                p75 = float(raw_p75)
                available = not isinstance(raw_p75, bool) and math.isfinite(p75) and p75 >= 0
            except (TypeError, ValueError):
                available = False
            assessment = ("GOOD" if p75 <= threshold else "NEEDS WORK") if available else "NO DATA"
            assessments.append(assessment)
            lines.append(f"{label}: {assessment} (threshold: {threshold}{unit})")

        if "NO DATA" in assessments:
            lines.append("\nOverall: INSUFFICIENT DATA to assess Core Web Vitals")
        elif all(assessment == "GOOD" for assessment in assessments):
            lines.append("\nOverall: PASSING Core Web Vitals")
        else:
            lines.append("\nOverall: FAILING Core Web Vitals")

        return "\n".join(lines)

    except httpx.HTTPStatusError as e:
        safe_body = _redact_api_keys(e.response.text)
        return f"CrUX API error (HTTP {e.response.status_code}): {safe_body[:200]}"
    except Exception as e:
        return f"Error fetching Core Web Vitals: {_redact_api_keys(str(e))}"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# NEW: SEO Analysis Tools
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def get_pagespeed_insights(
    url: str,
    strategy: str = "mobile",
    categories: str = "performance,seo,accessibility,best-practices",
) -> str:
    """
    Run Google's PageSpeed Insights API for a URL.
    Returns Lighthouse lab data plus available Chrome UX Report field data.

    Args:
        url: Full page URL
        strategy: mobile or desktop
        categories: Comma-separated Lighthouse categories
    """
    normalized_url = _ensure_https_url(url)
    strategy_value = strategy.lower().strip()
    if strategy_value not in {"mobile", "desktop"}:
        return "Invalid strategy. Use 'mobile' or 'desktop'."

    selected_categories = []
    for raw in categories.split(","):
        category = raw.strip().lower()
        if not category:
            continue
        if category not in LIGHTHOUSE_CATEGORIES:
            return f"Invalid Lighthouse category '{category}'. Allowed: {', '.join(sorted(LIGHTHOUSE_CATEGORIES))}"
        selected_categories.append(category)
    if not selected_categories:
        selected_categories = ["performance", "seo"]

    params = [("url", normalized_url), ("strategy", strategy_value)]
    for category in selected_categories:
        params.append(("category", category))
    if PAGESPEED_API_KEY:
        params.append(("key", PAGESPEED_API_KEY))

    try:
        async with httpx.AsyncClient(timeout=DEFAULT_FETCH_TIMEOUT, headers=DEFAULT_FETCH_HEADERS) as client:
            response = await client.get(
                "https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
                params=params,
            )
        response.raise_for_status()
        payload = response.json()

        lines = [f"PageSpeed Insights for {normalized_url} ({strategy_value})"]
        lines.append(_summarize_lighthouse_payload(payload, "Lighthouse / PSI summary"))
        lines.extend(_format_loading_experience(payload.get("loadingExperience", {}), "URL field data"))
        origin_data = payload.get("originLoadingExperience", {})
        if origin_data:
            lines.extend(_format_loading_experience(origin_data, "Origin field data"))
        return "\n".join(lines)
    except httpx.HTTPStatusError as exc:
        safe_body = _redact_api_keys(exc.response.text)[:300]
        if exc.response.status_code == 429:
            guidance = (
                " PageSpeed quota was exceeded. "
                "Set PAGESPEED_API_KEY (or GOOGLE_API_KEY) to use your own Google API quota."
            )
        else:
            guidance = ""
        return await _build_lighthouse_fallback(
            normalized_url,
            strategy_value,
            ",".join(selected_categories),
            f"PageSpeed Insights error (HTTP {exc.response.status_code}): {safe_body}{guidance}",
        )
    except Exception as exc:
        return await _build_lighthouse_fallback(
            normalized_url,
            strategy_value,
            ",".join(selected_categories),
            f"Error running PageSpeed Insights: {_redact_api_keys(str(exc))}",
        )


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def run_lighthouse_audit(
    url: str,
    form_factor: str = "mobile",
    categories: str = "performance,seo,accessibility,best-practices",
) -> str:
    """
    Run a local Lighthouse CLI audit for an explicitly trusted URL.
    Requires SEO_AUDIT_ENABLE_LOCAL_LIGHTHOUSE=true, Node.js and Chrome/Chromium.
    Lighthouse browser networking is not guarded by the public crawl transport.

    Args:
        url: Full page URL
        form_factor: mobile or desktop
        categories: Comma-separated Lighthouse categories
    """
    if not ENABLE_LOCAL_LIGHTHOUSE:
        return (
            "Local Lighthouse disabled. Set SEO_AUDIT_ENABLE_LOCAL_LIGHTHOUSE=true only for trusted sites; "
            "its browser can access the host network. Use PageSpeed Insights for untrusted public URLs."
        )
    normalized_url = _ensure_https_url(url)
    strategy_value = form_factor.lower().strip()
    if strategy_value not in {"mobile", "desktop"}:
        return "Invalid form_factor. Use 'mobile' or 'desktop'."

    selected_categories = []
    for raw in categories.split(","):
        category = raw.strip().lower()
        if not category:
            continue
        if category not in LIGHTHOUSE_CATEGORIES:
            return f"Invalid Lighthouse category '{category}'. Allowed: {', '.join(sorted(LIGHTHOUSE_CATEGORIES))}"
        selected_categories.append(category)
    if not selected_categories:
        selected_categories = ["performance", "seo"]

    try:
        normalized_url = _validate_fetchable_public_url(normalized_url)
    except ValueError as exc:
        return f"Local Lighthouse blocked URL: {str(exc)}"

    lighthouse_runner = []
    if LIGHTHOUSE_BINARY:
        lighthouse_runner = [LIGHTHOUSE_BINARY]
    else:
        lighthouse_path = shutil.which("lighthouse")
        if lighthouse_path:
            lighthouse_runner = [lighthouse_path]
        elif ALLOW_NPX_LIGHTHOUSE:
            npx_path = shutil.which("npx")
            if npx_path:
                lighthouse_runner = [npx_path, "--yes", "lighthouse"]

    if not lighthouse_runner:
        return (
            "No Lighthouse runner available. Install Lighthouse locally and set LIGHTHOUSE_BINARY, "
            "or set SEO_AUDIT_ALLOW_NPX_LIGHTHOUSE=true to allow npx to download/run Lighthouse."
        )

    command = [
        *lighthouse_runner,
        normalized_url,
        "--output=json",
        "--output-path=stdout",
        "--quiet",
        f"--only-categories={','.join(selected_categories)}",
    ]
    chrome_flags = ["--headless=new", "--disable-gpu"]
    if LIGHTHOUSE_NO_SANDBOX:
        chrome_flags.append("--no-sandbox")
    command.append(f"--chrome-flags={' '.join(chrome_flags)}")
    if strategy_value == "desktop":
        command.append("--preset=desktop")
    if LIGHTHOUSE_CHROME_PATH:
        command.append(f"--chrome-path={LIGHTHOUSE_CHROME_PATH}")

    try:
        result = await _run_lighthouse_process(command, timeout=180)
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            if not stderr and result.stdout:
                stderr = result.stdout.strip()
            return (
                "Local Lighthouse failed. "
                f"Exit code: {result.returncode}. "
                f"Details: {_clip(stderr or 'No stderr output', 400)}"
            )

        payload = json.loads(result.stdout)
        return _summarize_lighthouse_payload(payload, f"Local Lighthouse audit for {normalized_url} ({strategy_value})")
    except subprocess.TimeoutExpired:
        return "Local Lighthouse timed out after 180 seconds."
    except json.JSONDecodeError:
        return "Local Lighthouse returned invalid JSON output."
    except Exception as exc:
        return f"Error running local Lighthouse: {str(exc)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def inspect_robots_txt(url_or_origin: str) -> str:
    """
    Fetch and summarize the site's robots.txt file.

    Args:
        url_or_origin: Full URL, origin, or sc-domain property
    """
    robots_url = _origin_from_url(url_or_origin).rstrip("/") + "/robots.txt"
    try:
        response = await _fetch_url(robots_url)
        if response.status_code == 404:
            return (
                f"robots.txt not found at {robots_url} (HTTP 404).\n"
                "Google treats a missing robots.txt as no crawl restrictions."
            )

        text = response.text
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        sitemap_urls = []
        wildcard_disallows = []
        wildcard_allows = []
        warnings = []
        applies_to_wildcard = False

        for raw_line in lines:
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            key = key.strip().lower()
            value = value.strip()
            if key == "user-agent":
                applies_to_wildcard = value.lower() == "*"
            elif key == "disallow" and applies_to_wildcard:
                wildcard_disallows.append(value)
            elif key == "allow" and applies_to_wildcard:
                wildcard_allows.append(value)
            elif key == "sitemap":
                sitemap_urls.append(value)
            elif key == "noindex":
                warnings.append("Found noindex directive in robots.txt. Google no longer supports this.")

        blocked_all = any(path == "/" for path in wildcard_disallows)
        output = [
            f"robots.txt inspection for {robots_url}",
            f"HTTP status: {response.status_code}",
            f"User-agent * disallow rules: {len(wildcard_disallows)}",
            f"User-agent * allow rules: {len(wildcard_allows)}",
            f"Blocks all crawling for user-agent *: {'YES' if blocked_all else 'NO'}",
        ]

        if sitemap_urls:
            output.append("Declared sitemap URLs:")
            for item in sitemap_urls[:10]:
                output.append(f"  {item}")
        else:
            output.append("No sitemap directive found in robots.txt.")

        if wildcard_disallows:
            output.append("Sample disallow rules for user-agent *:")
            for item in wildcard_disallows[:10]:
                output.append(f"  {item or '[blank]'}")

        if warnings:
            output.append("Warnings:")
            for warning in warnings:
                output.append(f"  {warning}")

        return "\n".join(output)
    except Exception as exc:
        return f"Error inspecting robots.txt: {str(exc)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def analyze_sitemap(sitemap_url: str, sample_urls: int = 5) -> str:
    """
    Fetch and analyze an XML sitemap or sitemap index.

    Args:
        sitemap_url: Full sitemap URL
        sample_urls: Number of sitemap URLs to validate with GET requests
    """
    normalized_url = _ensure_https_url(sitemap_url)
    sample_urls = max(0, min(sample_urls, 25))
    try:
        response = await _fetch_url(normalized_url, headers={"Accept": "application/xml,text/xml;q=0.9,*/*;q=0.5"})
        response.raise_for_status()
        xml_text = _extract_xml_text(response)
        parsed = _parse_sitemap_document(xml_text)

        lines = [f"Sitemap analysis for {normalized_url}", f"Detected type: {parsed['type']}"]

        if parsed["type"] == "sitemapindex":
            sitemaps = parsed["sitemaps"]
            lines.append(f"Nested sitemaps: {len(sitemaps)}")
            for item in sitemaps[:10]:
                lines.append(f"  {item}")
            return "\n".join(lines)

        urls = parsed["urls"]
        lines.append(f"URLs listed: {len(urls)}")
        lastmod_count = parsed.get("lastmod_count", 0)
        lines.append(f"URLs with lastmod: {lastmod_count}")
        if urls:
            lines.append("Sample URLs:")
            for item in urls[:10]:
                lines.append(f"  {item['loc']}")

        sample_results = []
        for item in urls[: min(sample_urls, len(urls))]:
            target = item["loc"]
            try:
                sample_response = await _fetch_url(target, method="GET")
                sample_results.append((target, sample_response.status_code))
            except Exception:
                sample_results.append((target, "ERROR"))

        if sample_results:
            lines.append("Sample URL status checks:")
            for target, status in sample_results:
                lines.append(f"  {target} -> {status}")

        return "\n".join(lines)
    except Exception as exc:
        return f"Error analyzing sitemap: {str(exc)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def analyze_page_seo(url: str) -> str:
    """
    Fetch a page and analyze on-page SEO signals, structured data, and indexability hints.

    Args:
        url: Full page URL
    """
    normalized_url = _ensure_https_url(url)
    try:
        response = await _fetch_url(normalized_url)
        content_type = response.headers.get("content-type", "").lower()
        final_url = str(response.url)
        if "html" not in content_type and "xml" not in content_type and "<html" not in response.text[:500].lower():
            return (
                f"Page SEO analysis for {normalized_url}\n"
                f"Final URL: {final_url}\n"
                f"HTTP status: {response.status_code}\n"
                f"Content-Type: {content_type or 'unknown'}\n"
                "Response is not HTML, so on-page SEO analysis was skipped."
            )

        analysis = _analyze_html_document(final_url, response.status_code, response.headers, response.text)
        findings = _seo_findings_from_analysis(analysis)
        lines = [
            f"Page SEO analysis for {normalized_url}",
            f"Final URL: {final_url}",
            f"HTTP status: {response.status_code} ({_status_class(response.status_code)})",
            f"Title: {_clip(analysis['title'] or '[missing]', 120)}",
            f"Meta description: {_clip(analysis['meta_description'] or '[missing]', 180)}",
            f"Canonical: {_clip(analysis['canonicals'][0], 120) if analysis['canonicals'] else '[missing]'}",
            f"H1 count: {len(analysis['headings']['h1'])}",
            f"H2 count: {len(analysis['headings']['h2'])}",
            f"Word count: {analysis['body_word_count']}",
            f"Structured data types: {', '.join(analysis['structured_types']) if analysis['structured_types'] else '[none found]'}",
            f"Hreflang count: {len(analysis['hreflangs'])}",
            f"HTML lang: {analysis['html_lang'] or '[missing]'}",
            f"Viewport meta: {'present' if analysis['viewport'] else '[missing]'}",
            (
                "Images: "
                f"{analysis['images']['total']} total, "
                f"{len(analysis['images']['missing_alt'])} missing alt, "
                f"{len(analysis['images']['empty_alt'])} empty alt"
            ),
            (
                "Links: "
                f"{analysis['links']['total']} total, "
                f"{analysis['links']['internal']} internal, "
                f"{analysis['links']['external']} external, "
                f"{len(analysis['links']['missing_href'])} without href"
            ),
        ]
        lines.extend(_summarize_priority_findings(findings))
        if analysis["issues"]:
            lines.append("Detailed issues:")
            for issue in analysis["issues"]:
                lines.append(f"  {issue}")
        if analysis["notes"]:
            lines.append("Detailed notes:")
            for note in analysis["notes"]:
                lines.append(f"  {note}")
        if analysis["headings"]["h1"]:
            lines.append("H1 text:")
            for heading in analysis["headings"]["h1"][:3]:
                lines.append(f"  {heading}")
        return "\n".join(lines)
    except Exception as exc:
        return f"Error analyzing page SEO: {str(exc)}"


async def _load_crawl_robots(origin: str):
    """Fail closed for temporary errors; missing robots.txt does not prohibit crawling."""
    url = origin.rstrip("/") + "/robots.txt"
    try:
        response = await _fetch_url(url)
        status = response.status_code
        if status == 429 or status >= 500 or 300 <= status < 400:
            return None, {"url": url, "status": status, "state": "unavailable"}
        policy = RobotsPolicy(response.text if 200 <= status < 300 else "")
        return policy, {"url": url, "status": status, "state": "loaded" if status < 300 else "missing",
                        "truncated": policy.truncated, "sitemaps": [
                            line.strip().split(":", 1)[1].split("#", 1)[0].strip()
                            for line in response.text[:500 * 1024].splitlines()
                            if line.strip().lower().startswith("sitemap:")
                        ][:20] if 200 <= status < 300 else []}
    except Exception as exc:
        return None, {"url": url, "state": "unavailable", "error": str(exc)}


async def _crawl_site_data(start_url: str, max_pages: int, respect_robots: bool = True,
                          render_mode: str = "raw", include_sitemaps: bool = False,
                          max_seconds: int = 180) -> Dict[str, Any]:
    if render_mode not in {"raw", "rendered", "compare"}:
        raise ValueError("render_mode must be raw, rendered, or compare.")
    if not 5 <= max_seconds <= 600:
        raise ValueError("max_seconds must be between 5 and 600.")
    deadline = time.monotonic() + max_seconds
    normalized_start = _ensure_https_url(start_url)
    parsed_start = urlsplit(normalized_start)
    if parsed_start.scheme not in FETCH_ALLOWED_SCHEMES or not parsed_start.hostname or parsed_start.username:
        raise ValueError("A valid http(s) URL without embedded credentials is required.")
    normalized_start = urlunsplit((parsed_start.scheme, parsed_start.netloc.lower(), parsed_start.path or "/", parsed_start.query, ""))
    max_pages = max(1, min(max_pages, MAX_CRAWL_PAGES))
    origin = _origin_from_url(normalized_start)
    queue = deque([normalized_start])
    visited: Set[str] = set()
    discovered = {normalized_start}
    depths = {normalized_start: 0}
    inbound = {}
    final_seen = set()
    blocked = []
    omitted = 0
    termination = "frontier_exhausted"
    page_summaries = []
    policy = RobotsPolicy("")
    robots = {"state": "ignored_by_request"}
    if respect_robots:
        policy, robots = await _load_crawl_robots(origin)
        if policy is None:
            termination = "robots_unavailable"
        elif policy.crawl_delay is not None and policy.crawl_delay > 10:
            termination = "crawl_delay_exceeds_budget"
    delay = max(0.2, (policy.crawl_delay or 0)) if policy is not None else 0.2

    def validate_redirect(target):
        if not _url_matches_origin(target, origin):
            raise ValueError(f"External redirect blocked before fetch: {target}")
        if respect_robots and not policy.can_fetch(target):
            raise ValueError(f"Redirect blocked by robots.txt: {target}")

    sitemap_inventory = None
    sitemap_urls = set()
    if include_sitemaps and termination == "frontier_exhausted":
        async def fetch_sitemap(target):
            validate_redirect(target)
            await asyncio.sleep(delay)
            return await _fetch_url(target, redirect_validator=validate_redirect, redirect_delay=delay)
        try:
            async with asyncio.timeout(min(45, max(0.1, deadline - time.monotonic()))):
                sitemap_inventory = await discover_sitemap_urls(
                    origin, fetch_sitemap, seeds=robots.get("sitemaps") or None,
                    max_sitemaps=10, max_urls=max_pages * 20,
                )
            sitemap_urls = set(sitemap_inventory["known_urls"])
        except Exception as exc:
            sitemap_inventory = {"known_urls": [], "coverage": {"complete_for_scope": False}, "error": str(exc)}

    resource_policies = {origin: policy}
    resource_next_fetch = {}
    render_pace_lock = asyncio.Lock()
    async def validate_resource(target):
        if not respect_robots:
            return
        resource_origin = _origin_from_url(target)
        async with render_pace_lock:
            if resource_origin not in resource_policies:
                if len(resource_policies) >= 8:
                    raise ValueError("Rendering resource-origin limit reached.")
                resource_policy, _ = await _load_crawl_robots(resource_origin)
                resource_policies[resource_origin] = resource_policy
            resource_policy = resource_policies[resource_origin]
            if resource_policy is None or not resource_policy.can_fetch(target):
                raise ValueError("Resource blocked by robots.txt or unavailable robots policy.")
            resource_delay = max(0.2, resource_policy.crawl_delay or 0)
            if resource_delay > 10:
                raise ValueError("Resource crawl-delay exceeds rendering budget.")
            await asyncio.sleep(max(0, resource_next_fetch.get(resource_origin, 0) - time.monotonic()))
            resource_next_fetch[resource_origin] = time.monotonic() + resource_delay

    seeded = False

    while (queue or (include_sitemaps and not seeded)) and len(visited) < max_pages and termination == "frontier_exhausted":
        if not queue:
            seeded = True
            for candidate in sorted(sitemap_urls - discovered):
                discovered.add(candidate)
                depths[candidate] = None
                queue.append(candidate)
        if not queue:
            break
        if time.monotonic() >= deadline:
            termination = "time_budget"
            break
        current = queue.popleft()
        if current in visited:
            continue
        if respect_robots and not policy.can_fetch(current):
            blocked.append(current)
            continue
        if visited:
            await asyncio.sleep(delay)
        visited.add(current)

        try:
            async with asyncio.timeout(max(0.1, deadline - time.monotonic())):
                response = await _fetch_url(current, redirect_validator=validate_redirect, redirect_delay=delay)
            final_url = str(response.url)
            common = {"requested_url": current, "depth": depths[current], "linked_from": []}
            if not _url_matches_origin(final_url, origin):
                page_summaries.append({
                    **common, "state": "external_redirect",
                    "url": final_url,
                    "status": response.status_code,
                    "issues": [f"External redirect from {current}"],
                    "notes": [],
                    "title": "",
                    "description": "",
                    "word_count": 0,
                    "images_missing_alt": 0,
                    "links_missing_href": 0,
                })
                continue

            if final_url in final_seen:
                page_summaries.append({**common, "url": final_url, "status": response.status_code,
                                       "state": "redirect_alias", "issues": [], "title": "", "description": ""})
                continue
            final_seen.add(final_url)

            content_type = response.headers.get("content-type", "").lower()
            if "html" not in content_type and "<html" not in response.text[:500].lower():
                page_summaries.append({
                    **common, "state": "non_html",
                    "url": final_url,
                    "status": response.status_code,
                    "issues": [f"Non-HTML response ({content_type or 'unknown content type'})"],
                    "title": "",
                    "description": "",
                })
                continue

            page_html = response.text
            raw_analysis = _analyze_html_document(final_url, response.status_code, response.headers, page_html)
            render_evidence = None
            if render_mode != "raw" and 200 <= response.status_code < 300:
                try:
                    render_evidence = await render_page(
                        final_url, raw_response=response, fetch_callback=_fetch_url,
                        navigation_validator=validate_redirect, resource_validator=validate_resource,
                        allow_private=ALLOW_PRIVATE_URLS, timeout_seconds=min(30, max(0.1, deadline - time.monotonic())),
                    )
                    page_html = render_evidence.pop("rendered_html")
                    render_evidence.pop("raw_html", None)
                except Exception as exc:
                    render_evidence = {"error": str(exc), "status": "partial"}
            analysis = _analyze_html_document(final_url, response.status_code, response.headers, page_html) if render_mode != "raw" else raw_analysis
            if render_evidence is not None:
                render_evidence["raw_signals"] = {key: raw_analysis[key] for key in ("title", "meta_description", "canonicals", "body_word_count")}
                render_evidence["rendered_signals"] = {key: analysis[key] for key in ("title", "meta_description", "canonicals", "body_word_count")}
            is_noindex = "Page is explicitly marked noindex" in analysis["issues"]
            page_summaries.append({
                **common, "state": "html", "canonicals": analysis["canonicals"],
                "findings": _seo_findings_from_analysis(analysis),
                "url": final_url,
                "status": response.status_code,
                "issues": analysis["issues"],
                "notes": analysis["notes"],
                "noindex": is_noindex,
                "is_start_page": current == normalized_start,
                "title": analysis["title"],
                "description": analysis["meta_description"],
                "word_count": analysis["body_word_count"],
                "images_missing_alt": len(analysis["images"]["missing_alt"]),
                "links_missing_href": len(analysis["links"]["missing_href"]),
                "hreflangs": analysis["alternate_languages"], "structured_data": analysis["structured_data"],
                "rendering": render_evidence,
            })

            if response.status_code < 400 and not analysis["nofollow"]:
                for linked in _iter_internal_links(final_url, page_html):
                    if linked not in discovered and len(discovered) >= max_pages * 20:
                        omitted += 1
                        continue
                    inbound.setdefault(linked, set()).add(final_url)
                    if linked not in discovered:
                        discovered.add(linked)
                        depths[linked] = depths[current] + 1 if depths[current] is not None else None
                        queue.append(linked)
        except Exception as exc:
            page_summaries.append({
                "requested_url": current, "depth": depths[current], "linked_from": [], "state": "fetch_error",
                "url": current,
                "status": "ERROR",
                "issues": [f"Fetch error: {str(exc)}"],
                "title": "",
                "description": "",
                "word_count": 0,
            })

    for page in page_summaries:
        page["linked_from"] = sorted(inbound.get(page["requested_url"], set()) | inbound.get(page["url"], set()))
        page["in_sitemap"] = page["requested_url"] in sitemap_urls or page["url"] in sitemap_urls
        page["sitemap_only_candidate"] = page["in_sitemap"] and not page["linked_from"] and not page.get("is_start_page")
    unvisited_sitemap_urls = sorted(sitemap_urls - visited - set(blocked))
    if queue and termination == "frontier_exhausted":
        termination = "page_limit"
    if omitted and termination == "frontier_exhausted":
        termination = "discovery_limit"
    states = Counter(page["state"] for page in page_summaries)
    complete = not queue and not blocked and not omitted and not states["fetch_error"] and not robots.get("truncated") and termination == "frontier_exhausted"
    rendering_incomplete = [p["url"] for p in page_summaries if p.get("rendering") and p["rendering"].get("status") != "complete"]
    complete = complete and not rendering_incomplete and not unvisited_sitemap_urls and (sitemap_inventory is None or sitemap_inventory["coverage"].get("complete_for_scope", False))
    return {
        "start_url": normalized_start, "origin": origin,
        "settings": {"max_pages": max_pages, "respect_robots": respect_robots, "rendering": render_mode, "user_agent": "mcp-seo-audit",
                     "include_sitemaps": include_sitemaps, "max_seconds": max_seconds},
        "robots": robots, "pages": page_summaries, "sitemap_inventory": sitemap_inventory,
        "coverage": {"attempted": len(visited), "html_pages": states["html"], "discovered": len(discovered),
                     "remaining": len(queue), "blocked_urls": blocked, "fetch_errors": states["fetch_error"],
                     "discovery_links_omitted": omitted, "complete_for_discovered_links": complete,
                     "termination_reason": termination, "rendering_incomplete": rendering_incomplete,
                     "unvisited_sitemap_urls": unvisited_sitemap_urls},
    }


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def get_seo_audit_report(start_url: str, max_pages: int = 25, respect_robots: bool = True,
                             render_mode: str = "raw", include_sitemaps: bool = False,
                             max_seconds: int = 180) -> Dict[str, Any]:
    """Return a structured technical SEO action plan with stable rule IDs, affected URLs,
    evidence, fix guidance, crawl coverage and limits. No Google credentials required.
    Save the returned JSON in your MCP client to compare later with compare_seo_audits.
    Optional rendered/compare modes execute JavaScript in a guarded browser; include_sitemaps
    discovers nested sitemaps and checks unlinked candidates. max_seconds bounds the crawl.
    Preserves query strings. Respects robots.txt by
    default; turn this off only for a site you control. max_pages is capped by server configuration.
    """
    try:
        return build_audit_report(await _crawl_site_data(start_url, max_pages, respect_robots, render_mode, include_sitemaps, max_seconds))
    except Exception as exc:
        return {"error": str(exc)}


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False))
async def compare_seo_audits(baseline_json: str, current_json: str) -> Dict[str, Any]:
    """Compare two JSON outputs of get_seo_audit_report with matching crawl settings.
    Classifies new, resolved, persistent, newly observed and unverified issues without
    fetching URLs or writing files. Missing/failed pages never prove an issue resolved.
    """
    try:
        if len(baseline_json.encode()) > MAX_FETCH_BYTES or len(current_json.encode()) > MAX_FETCH_BYTES:
            raise ValueError("Each snapshot must fit within SEO_AUDIT_MAX_FETCH_BYTES.")
        return compare_audit_reports(json.loads(baseline_json), json.loads(current_json))
    except (ValueError, TypeError, KeyError, RecursionError) as exc:
        return {"error": str(exc)}


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def crawl_site_seo(start_url: str, max_pages: int = 10, respect_robots: bool = True) -> str:
    """Crawl same-origin raw HTML and summarize issues. Respects robots.txt by default.
    max_pages bounds fetch attempts, including errors/non-HTML. Use get_seo_audit_report
    for structured evidence and remediation. Disable robots only for a site you control.
    """
    try:
        crawl = await _crawl_site_data(start_url, max_pages, respect_robots)
    except Exception as exc:
        return f"Error crawling site: {exc}"
    normalized_start, origin = crawl["start_url"], crawl["origin"]
    max_pages = crawl["settings"]["max_pages"]
    page_summaries = [page for page in crawl["pages"] if page["state"] != "redirect_alias"]
    eligible = [page for page in page_summaries if page["state"] == "html" and 200 <= page["status"] < 300 and not page.get("noindex")]
    duplicate_titles = Counter(page["title"] for page in eligible if page["title"])
    duplicate_descriptions = Counter(page["description"] for page in eligible if page["description"])

    missing_titles = [page["url"] for page in page_summaries if not page.get("noindex") and "Missing <title>" in page.get("issues", [])]
    missing_descriptions = [page["url"] for page in page_summaries if not page.get("noindex") and "Missing meta description" in page.get("issues", [])]
    noindex_pages = [page["url"] for page in page_summaries if "Page is explicitly marked noindex" in page.get("issues", [])]
    start_noindex_pages = [page["url"] for page in page_summaries if page.get("noindex") and page.get("is_start_page")]
    thin_content_pages = [page["url"] for page in page_summaries if not page.get("noindex") and page.get("word_count", 0) > 0 and page.get("word_count", 0) < 80]
    duplicate_title_items = [(title, count) for title, count in duplicate_titles.items() if title and count > 1]
    duplicate_description_items = [(desc, count) for desc, count in duplicate_descriptions.items() if desc and count > 1]
    fetch_error_pages = [page["url"] for page in page_summaries if any(str(issue).startswith("Fetch error:") for issue in page.get("issues", []))]
    pages_with_missing_image_alt = [page["url"] for page in page_summaries if page.get("images_missing_alt", 0) > 0]
    pages_with_uncrawlable_anchors = [page["url"] for page in page_summaries if page.get("links_missing_href", 0) > 0]

    crawl_findings: List[Tuple[str, str]] = []
    if fetch_error_pages:
        crawl_findings.append(("high", f"{len(fetch_error_pages)} crawled pages failed to fetch"))
    if missing_titles:
        crawl_findings.append(("high", f"{len(missing_titles)} pages are missing a title tag"))
    if missing_descriptions:
        crawl_findings.append(("medium", f"{len(missing_descriptions)} pages are missing a meta description"))
    if start_noindex_pages:
        crawl_findings.append(("high", "The crawl start page is marked noindex"))
    if duplicate_title_items:
        crawl_findings.append(("medium", f"{len(duplicate_title_items)} duplicate title groups found"))
    if duplicate_description_items:
        crawl_findings.append(("medium", f"{len(duplicate_description_items)} duplicate meta description groups found"))
    if thin_content_pages:
        crawl_findings.append(("low", f"{len(thin_content_pages)} pages have thin visible content"))
    if pages_with_missing_image_alt:
        crawl_findings.append(("medium", f"{len(pages_with_missing_image_alt)} pages have images missing alt text"))
    if pages_with_uncrawlable_anchors:
        crawl_findings.append(("medium", f"{len(pages_with_uncrawlable_anchors)} pages have anchors without href"))
    crawl_findings.sort(key=lambda item: (SEO_SEVERITY_ORDER[item[0]], item[1]))

    lines = [
        f"Crawl SEO audit for {normalized_start}",
        f"Origin: {origin}",
        f"Pages crawled: {len(page_summaries)} / {max_pages}",
        f"Pages missing title: {len(missing_titles)}",
        f"Pages missing meta description: {len(missing_descriptions)}",
        f"Pages marked noindex: {len(noindex_pages)}",
        f"Pages with thin content: {len(thin_content_pages)}",
        f"Pages with images missing alt text: {len(pages_with_missing_image_alt)}",
        f"Pages with anchors without href: {len(pages_with_uncrawlable_anchors)}",
        f"Duplicate titles: {len(duplicate_title_items)}",
        f"Duplicate meta descriptions: {len(duplicate_description_items)}",
        f"Crawl stop reason: {crawl['coverage']['termination_reason']}",
        f"Robots-blocked URLs: {len(crawl['coverage']['blocked_urls'])}",
        f"Discovered URLs still pending: {crawl['coverage']['remaining']}",
        "Scope: bounded raw-HTML sample; JavaScript and orphan pages are not covered.",
    ]
    lines.extend(_summarize_priority_findings(crawl_findings))

    if missing_titles:
        lines.append("Missing titles:")
        for item in missing_titles[:10]:
            lines.append(f"  {item}")
    if missing_descriptions:
        lines.append("Missing meta descriptions:")
        for item in missing_descriptions[:10]:
            lines.append(f"  {item}")
    if noindex_pages:
        lines.append("Noindex pages:")
        for item in noindex_pages[:10]:
            lines.append(f"  {item}")
    if thin_content_pages:
        lines.append("Thin content pages:")
        for item in thin_content_pages[:10]:
            lines.append(f"  {item}")
    if pages_with_missing_image_alt:
        lines.append("Pages with images missing alt text:")
        for item in pages_with_missing_image_alt[:10]:
            lines.append(f"  {item}")
    if pages_with_uncrawlable_anchors:
        lines.append("Pages with anchors without href:")
        for item in pages_with_uncrawlable_anchors[:10]:
            lines.append(f"  {item}")
    if duplicate_title_items:
        lines.append("Duplicate titles:")
        for title, count in duplicate_title_items[:10]:
            lines.append(f"  {count} pages -> {_clip(title, 120)}")
    if duplicate_description_items:
        lines.append("Duplicate meta descriptions:")
        for description, count in duplicate_description_items[:10]:
            lines.append(f"  {count} pages -> {_clip(description, 140)}")

    issue_pages = [page for page in page_summaries if page.get("issues") and (not page.get("noindex") or page.get("is_start_page"))]
    if issue_pages:
        lines.append("Pages with issues:")
        for page in issue_pages[:10]:
            lines.append(f"  {page['url']} -> {', '.join(page['issues'])}")

    return "\n".join(lines)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def audit_live_site(url: str, crawl_pages: int = 5, include_lighthouse: bool = False) -> str:
    """
    Run a live SEO audit without requiring Search Console access.
    Combines page analysis, robots.txt inspection, sitemap discovery, PSI data, and a small same-origin crawl.

    Args:
        url: Full site/page URL
        crawl_pages: Number of pages to crawl for duplicate/missing-tag issues
        include_lighthouse: Whether to also run a local Lighthouse CLI audit
    """
    normalized_url = _ensure_https_url(url)
    origin = _origin_from_url(normalized_url)
    lines = [f"Live SEO audit for {normalized_url}", "=" * 80]

    lines.append(await analyze_page_seo(normalized_url))
    lines.append("\n" + "-" * 80)
    lines.append(await inspect_robots_txt(origin))
    lines.append("\n" + "-" * 80)

    robots_url = origin.rstrip("/") + "/robots.txt"
    discovered_sitemap = None
    try:
        robots_response = await _fetch_url(robots_url)
        for raw_line in robots_response.text.splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if line.lower().startswith("sitemap:"):
                discovered_sitemap = line.split(":", 1)[1].strip()
                break
    except Exception:
        discovered_sitemap = None

    if discovered_sitemap:
        lines.append(await analyze_sitemap(discovered_sitemap, sample_urls=3))
    else:
        lines.append("No sitemap discovered via robots.txt.")
    lines.append("\n" + "-" * 80)

    lines.append(await get_pagespeed_insights(normalized_url))
    lines.append("\n" + "-" * 80)
    lines.append(await crawl_site_seo(normalized_url, max_pages=max(1, min(crawl_pages, 25))))

    if include_lighthouse:
        lines.append("\n" + "-" * 80)
        lines.append(await run_lighthouse_audit(normalized_url))

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def find_striking_distance_keywords(site_url: str, days: int = 28, min_impressions: int = 10, row_limit: int = 50) -> str:
    """
    Find "striking distance" keywords â€” queries ranking at positions 5-20 with decent impressions.
    These are review candidates; positions alone do not establish the effort needed to improve rankings.

    Args:
        site_url: Exact GSC property URL
        days: Days to look back (default: 28)
        min_impressions: Minimum impressions to include (default: 10)
        row_limit: Max results (default: 50)
    """
    try:
        start_date, end_date = _gsc_date_window(days)
        if min_impressions < 1 or not 1 <= row_limit <= 5000:
            return "min_impressions must be positive and row_limit must be between 1 and 5000."
        service = await google_service(get_gsc_service)

        response = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d"),
                "dimensions": ["query", "page"],
                "rowLimit": 5000,
                "dataState": DATA_STATE,
            },
        ))

        if not response.get("rows"):
            return f"No data found for {site_url}."

        # Filter for striking distance: position 5-20, decent impressions
        candidates = []
        for row in response["rows"]:
            pos = row.get("position", 0)
            imp = row.get("impressions", 0)
            if 5 <= pos <= 20 and imp >= min_impressions:
                candidates.append({
                    "query": row["keys"][0],
                    "page": row["keys"][1],
                    "clicks": row.get("clicks", 0),
                    "impressions": imp,
                    "ctr": row.get("ctr", 0),
                    "position": pos,
                    "potential": max(0, imp * 0.3 - row.get("clicks", 0)),
                })

        candidates.sort(key=lambda x: x["potential"], reverse=True)

        lines = [
            f"Striking Distance Keywords for {site_url} (last {days} days):",
            f"Found {len(candidates)} query-page candidates in returned rows at positions 5-20 with {min_impressions}+ impressions",
            "-" * 100,
            "Query | Page | Pos | Impressions | Clicks | CTR | Additional clicks at assumed 30% CTR",
            "-" * 100,
        ]

        for item in candidates[:row_limit]:
            page_short = item["page"].split("//", 1)[-1] if "//" in item["page"] else item["page"]
            lines.append(
                f"{item['query'][:50]} | {page_short[:40]} | {item['position']:.1f} | "
                f"{item['impressions']} | {item['clicks']} | {item['ctr'] * 100:.1f}% | "
                f"+{max(0, item['potential']):.0f}"
            )

        if candidates:
            lines.append(f"\nTop opportunity: '{candidates[0]['query']}' at position {candidates[0]['position']:.1f}")
            lines.append(f"Currently getting {candidates[0]['clicks']} clicks from {candidates[0]['impressions']} impressions.")
            lines.append("The 30% CTR scenario is an illustrative assumption, not a forecast or guaranteed effect of ranking in the top 3.")

        lines.extend(_gsc_coverage_notes(response, 5000))
        return "\n".join(lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def detect_cannibalization(site_url: str, days: int = 28, min_impressions: int = 5) -> str:
    """
    Identify queries appearing for multiple pages as potential cannibalization candidates.
    Multiple pages can serve different intents; overlap alone does not prove harmful competition.

    Args:
        site_url: Exact GSC property URL
        days: Days to look back (default: 28)
        min_impressions: Minimum impressions per query-page pair (default: 5)
    """
    try:
        start_date, end_date = _gsc_date_window(days)
        if min_impressions < 1:
            return "min_impressions must be positive."
        service = await google_service(get_gsc_service)

        response = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d"),
                "dimensions": ["query", "page"],
                "rowLimit": 10000,
                "dataState": DATA_STATE,
            },
        ))

        if not response.get("rows"):
            return f"No data found for {site_url}."

        # Group by query
        query_pages = {}
        for row in response["rows"]:
            query = row["keys"][0]
            page = row["keys"][1]
            imp = row.get("impressions", 0)
            if imp >= min_impressions:
                if query not in query_pages:
                    query_pages[query] = []
                query_pages[query].append({
                    "page": page,
                    "clicks": row.get("clicks", 0),
                    "impressions": imp,
                    "position": row.get("position", 0),
                    "ctr": row.get("ctr", 0),
                })

        # Find queries with 2+ pages
        cannibalized = {q: pages for q, pages in query_pages.items() if len(pages) >= 2}

        # Sort by total impressions
        sorted_queries = sorted(
            cannibalized.items(),
            key=lambda x: sum(p["impressions"] for p in x[1]),
            reverse=True,
        )

        lines = [
            f"Keyword Cannibalization Report for {site_url} (last {days} days):",
            f"Found {len(cannibalized)} queries with multiple pages in returned rows",
            "These are overlap candidates, not confirmed harmful cannibalization. Review intent, canonicalization, and trends before consolidating pages.",
            "-" * 100,
        ]

        for query, pages in sorted_queries[:20]:
            total_imp = sum(p["impressions"] for p in pages)
            total_clicks = sum(p["clicks"] for p in pages)
            lines.append(f"\nQuery: '{query}' ({len(pages)} pages, {total_imp} total impressions, {total_clicks} clicks)")
            pages.sort(key=lambda x: x["impressions"], reverse=True)
            for p in pages:
                short_page = p["page"].split("//", 1)[-1] if "//" in p["page"] else p["page"]
                lines.append(
                    f"  {short_page[:60]} | pos: {p['position']:.1f} | imp: {p['impressions']} | "
                    f"clicks: {p['clicks']} | CTR: {p['ctr'] * 100:.1f}%"
                )

        if not cannibalized:
            lines.append("No keyword cannibalization candidates found within the returned sample and impression threshold.")

        lines.extend(_gsc_coverage_notes(response, 10000))
        return "\n".join(lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def split_branded_queries(site_url: str, brand_name: str, days: int = 28) -> str:
    """
    Split search performance into branded vs non-branded queries.
    Separates query-visible traffic; one period alone does not establish growth.

    Args:
        site_url: Exact GSC property URL
        brand_name: Your brand name to filter (e.g. "cdljobscenter")
        days: Days to look back (default: 28)
    """
    try:
        brand_pattern = re.escape(brand_name.strip())
        if not brand_pattern:
            return "Brand name must not be empty."

        start_date, end_date = _gsc_date_window(days)
        service = await google_service(get_gsc_service)
        date_range = {"startDate": start_date.strftime("%Y-%m-%d"), "endDate": end_date.strftime("%Y-%m-%d")}

        # Get branded queries
        branded = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={
                **date_range, "dimensions": ["query"], "rowLimit": 25000, "dataState": DATA_STATE,
                "dimensionFilterGroups": [{"filters": [
                    {"dimension": "query", "operator": "includingRegex", "expression": f"(?i){brand_pattern}"}
                ]}],
            },
        ))

        # Get non-branded queries
        non_branded = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={
                **date_range, "dimensions": ["query"], "rowLimit": 25000, "dataState": DATA_STATE,
                "dimensionFilterGroups": [{"filters": [
                    {"dimension": "query", "operator": "excludingRegex", "expression": f"(?i){brand_pattern}"}
                ]}],
            },
        ))

        # Also get totals
        total = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={**date_range, "dimensions": [], "rowLimit": 1, "dataState": DATA_STATE},
        ))

        def sum_metrics(rows):
            clicks = sum(r.get("clicks", 0) for r in rows)
            imp = sum(r.get("impressions", 0) for r in rows)
            ctr = (clicks / imp * 100) if imp > 0 else 0
            return clicks, imp, ctr

        branded_rows = branded.get("rows", [])
        non_branded_rows = non_branded.get("rows", [])
        total_rows = total.get("rows") or [{}]
        total_row = total_rows[0] if total_rows else {}

        b_clicks, b_imp, b_ctr = sum_metrics(branded_rows)
        nb_clicks, nb_imp, nb_ctr = sum_metrics(non_branded_rows)
        visible_clicks = b_clicks + nb_clicks
        visible_imp = b_imp + nb_imp
        visible_ctr = (visible_clicks / visible_imp * 100) if visible_imp else 0
        t_clicks = total_row.get("clicks", 0)
        t_imp = total_row.get("impressions", 0)

        lines = [
            f"Branded vs Non-Branded for {site_url} (last {days} days):",
            f"Brand filter: '{brand_name}'",
            "-" * 60,
            f"{'':20} | {'Clicks':>8} | {'Impressions':>12} | {'CTR':>6}",
            "-" * 60,
            f"{'Branded':20} | {b_clicks:>8,} | {b_imp:>12,} | {b_ctr:>5.1f}%",
            f"{'Non-Branded':20} | {nb_clicks:>8,} | {nb_imp:>12,} | {nb_ctr:>5.1f}%",
            f"{'Query-visible total':20} | {visible_clicks:>8,} | {visible_imp:>12,} | {visible_ctr:>5.1f}%",
            f"{'Property total':20} | {t_clicks:>8,} | {t_imp:>12,} | {(t_clicks / t_imp * 100) if t_imp else 0:>5.1f}%",
            "-" * 60,
            f"Non-branded share of query-visible data: {(nb_clicks / visible_clicks * 100) if visible_clicks else 0:.0f}% of clicks, {(nb_imp / visible_imp * 100) if visible_imp else 0:.0f}% of impressions",
            f"Query coverage vs property total: {(visible_clicks / t_clicks * 100) if t_clicks else 0:.0f}% of clicks, {(visible_imp / t_imp * 100) if t_imp else 0:.0f}% of impressions",
            "Note: Search Console omits anonymized queries and limits returned rows; these sampled sums can be lower than property totals.",
            "Brand classification matches the supplied literal text; brand variants and ambiguous names need separate review.",
        ]

        if non_branded_rows:
            lines.append(f"\nTop non-branded queries:")
            non_branded_rows.sort(key=lambda x: x.get("clicks", 0), reverse=True)
            for row in non_branded_rows[:10]:
                q = row["keys"][0][:50]
                lines.append(f"  {q} | clicks: {row.get('clicks', 0)} | imp: {row.get('impressions', 0)} | pos: {row.get('position', 0):.1f}")

        lines.extend(_gsc_coverage_notes(branded, 25000))
        if len(non_branded_rows) >= 25000:
            lines.append("Non-branded row limit reached; query-visible totals are a bounded sample.")
        return "\n".join(lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def site_audit(site_url: str, sitemap_url: str = None, max_inspect: int = 30) -> str:
    """
    Check sitemap submission health, performance, and a sample of top search pages for indexing issues.
    This sample cannot establish indexing coverage across the entire property.

    Args:
        site_url: Exact GSC property URL (e.g. "sc-domain:example.com")
        sitemap_url: Optional submitted sitemap to include in the GSC health report (not crawled here).
        max_inspect: Max URLs to inspect (0-100, default: 30, costs 1 API call each)
    """
    try:
        if not 0 <= max_inspect <= 100:
            return "max_inspect must be between 0 and 100."
        service = await google_service(get_gsc_service)
        lines = [f"Site Audit Report for {site_url}", "=" * 80, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"]

        # 1. Sitemap health
        lines.append("1. SITEMAP HEALTH")
        lines.append("-" * 40)
        sitemaps = await execute_google(service.sitemaps().list(siteUrl=site_url))
        sm_list = sitemaps.get("sitemap", [])
        if sitemap_url:
            sm_list = [item for item in sm_list if item.get("path") == sitemap_url]

        if not sm_list:
            lines.append("WARNING: No sitemaps found!")
        else:
            for sm in sm_list:
                errors = int(sm.get("errors", 0))
                warnings = int(sm.get("warnings", 0))
                url_count = "N/A"
                for c in sm.get("contents", []):
                    if c.get("type") == "web":
                        url_count = c.get("submitted", "0")
                status = "OK" if errors == 0 else f"ERRORS: {errors}"
                lines.append(f"  {sm['path']} | {url_count} URLs | {status} | Warnings: {warnings}")

                if not sitemap_url:
                    sitemap_url = sm["path"]

        # 2. Performance summary
        lines.append(f"\n2. PERFORMANCE SUMMARY (last 28 days)")
        lines.append("-" * 40)
        start_date, end_date = _gsc_date_window(28)
        date_range = {"startDate": start_date.strftime("%Y-%m-%d"), "endDate": end_date.strftime("%Y-%m-%d")}

        total = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={**date_range, "dimensions": [], "rowLimit": 1, "dataState": DATA_STATE},
        ))

        if total.get("rows"):
            r = total["rows"][0]
            lines.append(f"  Clicks: {r.get('clicks', 0):,}")
            lines.append(f"  Impressions: {r.get('impressions', 0):,}")
            lines.append(f"  Avg CTR: {r.get('ctr', 0) * 100:.2f}%")
            lines.append(f"  Avg Position: {r.get('position', 0):.1f}")

        # 3. Top pages
        lines.append(f"\n3. TOP PAGES BY CLICKS")
        lines.append("-" * 40)
        pages = await execute_google(service.searchanalytics().query(
            siteUrl=site_url,
            body={**date_range, "dimensions": ["page"], "rowLimit": max(20, max_inspect), "dataState": DATA_STATE},
        ))

        top_urls_to_inspect = []
        for row in pages.get("rows", []):
            page = row["keys"][0]
            short = page.split("//", 1)[-1] if "//" in page else page
            lines.append(f"  {short[:60]} | clicks: {row.get('clicks', 0)} | imp: {row.get('impressions', 0)} | pos: {row.get('position', 0):.1f}")
            top_urls_to_inspect.append(page)

        # 4. URL inspection of top pages
        lines.append(f"\n4. INDEXING STATUS (inspecting up to {max_inspect} URLs)")
        lines.append("-" * 40)

        urls_to_inspect = top_urls_to_inspect[:max_inspect]
        categories = {"indexed": 0, "crawled_not_indexed": 0, "not_found": 0, "other": 0}
        issues = []
        completed_inspections = 0
        lines.append("  Sample source: top pages returned by Search Console; URLs without search visibility may be absent.")

        for url in urls_to_inspect:
            try:
                result = await execute_google(service.urlInspection().index().inspect(
                    body={"inspectionUrl": url, "siteUrl": site_url}
                ))
                idx = result.get("inspectionResult", {}).get("indexStatusResult", {})
                completed_inspections += 1
                verdict = idx.get("verdict", "UNKNOWN")
                coverage = idx.get("coverageState", "")

                if verdict == "PASS":
                    categories["indexed"] += 1
                elif "crawled" in coverage.lower() and "not indexed" in coverage.lower():
                    categories["crawled_not_indexed"] += 1
                    issues.append(f"CRAWLED NOT INDEXED: {url}")
                elif "not found" in coverage.lower():
                    categories["not_found"] += 1
                    issues.append(f"NOT FOUND: {url}")
                else:
                    categories["other"] += 1
                    issues.append(f"{coverage}: {url}")

                # Check canonical mismatch
                gc = idx.get("googleCanonical", "")
                uc = idx.get("userCanonical", "")
                if gc and uc and gc != uc:
                    issues.append(f"CANONICAL MISMATCH: {url} (Google chose {gc}, you declared {uc})")

                await asyncio.sleep(0.15)
            except Exception as e:
                issues.append(f"INSPECT ERROR: {url} ({str(e)[:50]})")

        lines.append(f"  Indexed: {categories['indexed']}")
        lines.append(f"  Crawled not indexed: {categories['crawled_not_indexed']}")
        lines.append(f"  Not found: {categories['not_found']}")
        lines.append(f"  Other: {categories['other']}")
        lines.append(f"  Completed inspections: {completed_inspections} / {len(urls_to_inspect)} attempted")

        if issues:
            lines.append(f"\n5. ISSUES FOUND ({len(issues)})")
            lines.append("-" * 40)
            for issue in issues:
                lines.append(f"  {issue}")
        elif completed_inspections:
            lines.append("\n5. No indexing issues found in this inspected sample. This does not establish full-site coverage.")
        else:
            lines.append("\n5. No URLs were inspected; indexing status is unknown.")

        lines.extend(_gsc_coverage_notes(pages, max(20, max_inspect)))
        lines.append("\n" + "=" * 80)
        lines.append("End of audit report.")

        return "\n".join(lines)
    except Exception as e:
        if "404" in str(e):
            return _site_not_found_error(site_url)
        return f"Error running audit: {str(e)}"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Auth Management
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _save_oauth_token(creds: Any) -> None:
    """Replace the token only after a complete, durable write in the same directory."""
    token_dir = os.path.dirname(os.path.abspath(TOKEN_FILE))
    os.makedirs(token_dir, mode=0o700, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(prefix=".oauth-token-", suffix=".tmp", dir=token_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
            token.flush()
            os.fsync(token.fileno())
        os.replace(temporary_path, TOKEN_FILE)
    finally:
        if os.path.exists(temporary_path):
            os.remove(temporary_path)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=True))
async def reauthenticate() -> str:
    """
    Authenticate a new account, then atomically replace the saved OAuth token.
    Existing credentials and service caches are preserved if login or saving fails.
    """
    try:
        global _gsc_service_cache, _indexing_service_cache
        if not os.path.exists(OAUTH_CLIENT_SECRETS_FILE):
            return "Error: client_secrets.json not found. Cannot start auth flow."

        flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CLIENT_SECRETS_FILE, ALL_SCOPES)
        creds = await google_service(lambda: flow.run_local_server(port=0, timeout_seconds=180, authorization_prompt_message=""))
        _save_oauth_token(creds)
        _gsc_service_cache = None
        _indexing_service_cache = None

        return "Successfully authenticated with a new Google account."
    except Exception as e:
        return f"Error during reauthentication: {str(e)}"


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def get_search_analytics_snapshot(
    site_url: str, days: int = 28, dimensions: str = "page", max_rows: int = 25000,
    max_requests: int = 10, search_type: str = "web",
) -> Dict[str, Any]:
    """Fetch a bounded, paginated Search Console snapshot with explicit coverage.
    Returns at most 100,000 rows / 10 API pages within 180 seconds. Google may still
    omit anonymized/long-tail rows; completion is only for the returned API window.
    Partial results survive provider errors. Includes data-state/freshness metadata.
    """
    rows, seen = [], set()
    calls, duplicate_rows = 0, 0
    metadata = {}
    error = None
    reason = "row_limit"
    try:
        if not 1 <= max_rows <= 100000 or not 1 <= max_requests <= 10:
            raise ValueError("max_rows must be 1-100000 and max_requests 1-10.")
        start, end = _gsc_date_window(days)
        dims = _gsc_dimensions(dimensions)
        search = _gsc_search_type(search_type)
    except (ValueError, TypeError) as exc:
        return {"error": str(exc)}
    try:
        offset = 0
        async with asyncio.timeout(180):
            service = await google_service(get_gsc_service)
            while len(rows) < max_rows and calls < max_requests:
                page_size = min(25000, max_rows - len(rows))
                body = {"startDate": start.isoformat(), "endDate": end.isoformat(), "dimensions": dims,
                        "type": search, "dataState": DATA_STATE, "rowLimit": page_size, "startRow": offset}
                calls += 1
                result = await execute_google(service.searchanalytics().query(siteUrl=site_url, body=body))
                batch = result.get("rows", [])
                metadata.update(result.get("metadata", {}))
                new_count = 0
                for row in batch:
                    key = tuple(row.get("keys", []))
                    if key in seen:
                        duplicate_rows += 1
                    else:
                        seen.add(key)
                        rows.append(row)
                        new_count += 1
                offset += len(batch)
                if len(batch) < page_size:
                    reason = "api_window_exhausted"
                    break
                if not new_count:
                    reason = "repeated_page"
                    break
            else:
                reason = "row_limit" if len(rows) >= max_rows else "request_limit"
    except TimeoutError:
        reason, error = "time_budget", "180-second snapshot budget exceeded."
    except Exception as exc:
        reason, error = "api_error", _redact_api_keys(str(exc))
    return {
        "schema_version": "1.0", "site_url": site_url, "rows": rows,
        "start_date": start.isoformat(), "end_date": end.isoformat(), "dimensions": dims,
        "data_state": DATA_STATE, "metadata": metadata,
        "coverage": {"rows": len(rows), "requests": calls, "duplicate_rows": duplicate_rows,
                     "stop_reason": reason, "complete_for_api_window": reason == "api_window_exhausted"},
        "error": error,
        "limitations": ["Google returns top rows and may omit anonymized or long-tail data.",
                        "Rows can change between pages when provisional data is requested; duplicates are removed.",
                        "Requests counts API pages; retry attempts can consume additional quota."],
    }


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True))
async def prioritize_audit_issues(report_json: str, site_url: str, days: int = 28) -> Dict[str, Any]:
    """Add observed Search Console traffic to an audit action plan. Sort by severity,
    then affected-page clicks/impressions. Does not estimate revenue or ranking gains.
    Unmatched URLs are unknown, not zero. No page traffic is double-counted within a rule.
    """
    try:
        if len(report_json.encode()) > MAX_FETCH_BYTES:
            raise ValueError("Report exceeds SEO_AUDIT_MAX_FETCH_BYTES.")
        report = json.loads(report_json)
        if not isinstance(report, dict) or report.get("schema_version") != "1.0" or not isinstance(report.get("issues"), list):
            raise ValueError("Expected a schema_version 1.0 audit report.")
        for issue in report["issues"]:
            if (not isinstance(issue, dict) or not isinstance(issue.get("affected_urls"), list)
                    or any(not isinstance(url, str) for url in issue["affected_urls"])):
                raise ValueError("Each issue must include affected_urls as a list of URL strings.")
        snapshot = await get_search_analytics_snapshot(site_url, days=days, dimensions="page")
        if "rows" not in snapshot:
            return snapshot
        by_url = {row["keys"][0]: row for row in snapshot["rows"] if row.get("keys")}
        priorities = []
        for issue in report["issues"]:
            urls = sorted(set(issue["affected_urls"]))
            matched = [by_url[url] for url in urls if url in by_url]
            priorities.append({**issue, "traffic": {
                "observed_clicks": sum(row.get("clicks", 0) for row in matched),
                "observed_impressions": sum(row.get("impressions", 0) for row in matched),
                "matched_pages": len(matched), "unknown_pages": len(urls) - len(matched),
            }})
        priorities.sort(key=lambda item: (SEO_SEVERITY_ORDER.get(item.get("severity"), 4),
                                         -item["traffic"]["observed_clicks"], -item["traffic"]["observed_impressions"]))
        return {"schema_version": "1.0", "origin": report.get("origin"), "issues": priorities,
                "analytics_coverage": snapshot["coverage"], "analytics_error": snapshot["error"],
                "limitations": snapshot["limitations"] + ["Traffic is an observed prioritization signal, not predicted business impact.",
                    "URL matching is exact; redirects, aliases, or unmatched URLs need manual review."]}
    except (ValueError, TypeError, KeyError) as exc:
        return {"error": str(exc)}


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Entry point
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False))
async def get_server_status() -> Dict[str, Any]:
    """Inspect local setup without credential values, network calls, or starting jobs.
    Credential presence does not prove current Google permissions or quota.
    """
    import importlib.util
    return {
        "version": "2.1.0", "transport": "stdio", "tool_count": len(mcp._tool_manager._tools),
        "write_tools_enabled": ENABLE_WRITE_TOOLS, "private_urls_allowed": ALLOW_PRIVATE_URLS,
        "browser_package_installed": importlib.util.find_spec("playwright") is not None,
        "oauth_client_present": os.path.isfile(OAUTH_CLIENT_SECRETS_FILE),
        "oauth_token_present": os.path.isfile(TOKEN_FILE) or (not os.environ.get("GSC_TOKEN_FILE") and os.path.isfile(LEGACY_TOKEN_FILE)),
        "service_account_file_present": any(path and os.path.isfile(path) for path in POSSIBLE_CREDENTIAL_PATHS),
        "pagespeed_key_configured": bool(PAGESPEED_API_KEY), "crux_key_configured": bool(CRUX_API_KEY),
        "max_crawl_pages": MAX_CRAWL_PAGES, "max_fetch_bytes": MAX_FETCH_BYTES,
        "monitoring": "Requires explicit schedules and a separate mcp-seo-monitor worker.",
        "live_provider_access": "not_tested", "browser_execution": "verified only by a successful rendered audit",
    }


register_monitoring_tools(mcp, get_seo_audit_report)


def main():
    """Entry point for the MCP server (used by pyproject.toml [project.scripts])."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
