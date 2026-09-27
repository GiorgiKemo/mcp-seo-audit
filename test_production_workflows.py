"""Public-tool workflows: evidence extraction, bounded analytics and local rendering."""

import asyncio
import importlib.util
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import gsc_server as gs


ORIGIN = "https://example.com"


def run(coro):
    return asyncio.run(coro)


def html(title="Audit fixture", head="", body=""):
    return f'<html lang="en"><head><title>{title}</title>{head}</head><body><h1>Fixture</h1>{body}</body></html>'


def response(url, body, content_type="text/html", status=200):
    return httpx.Response(status, text=body, headers={"content-type": content_type}, request=httpx.Request("GET", url))


def fixture_fetch(monkeypatch, routes):
    calls = []

    async def fetch(url, **kwargs):
        calls.append(url)
        assert url in routes, f"Unexpected fetch: {url}"
        result = routes[url]
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(gs, "_fetch_url", fetch)
    monkeypatch.setattr(gs.asyncio, "sleep", AsyncMock())
    return calls


def issue_map(report):
    return {issue["rule_id"]: issue for issue in report["issues"]}


def test_public_report_inventories_nested_sitemap_and_unlinked_candidate(monkeypatch):
    root, child, candidate = ORIGIN + "/sitemap.xml", ORIGIN + "/pages.xml", ORIGIN + "/inventory?lang=en"
    calls = fixture_fetch(monkeypatch, {
        ORIGIN + "/robots.txt": response(ORIGIN + "/robots.txt", f"User-agent: *\nAllow: /\nSitemap: {root}", "text/plain"),
        root: response(root, f"<sitemapindex><sitemap><loc>{child}</loc></sitemap></sitemapindex>", "application/xml"),
        child: response(child, f"<urlset><url><loc>{candidate}</loc></url></urlset>", "application/xml"),
        ORIGIN + "/": response(ORIGIN + "/", html()),
        candidate: response(candidate, html("Inventory page")),
    })
    result = run(gs.get_seo_audit_report(ORIGIN, max_pages=5, include_sitemaps=True))
    assert "error" not in result, result
    assert calls == [ORIGIN + "/robots.txt", root, child, ORIGIN + "/", candidate]
    assert result["audit_status"] == "complete"
    assert result["sitemap_inventory"]["known_urls"] == [candidate]
    issue = issue_map(result)["sitemap_unlinked_candidate"]
    assert issue["affected_urls"] == [candidate]
    assert issue["evidence"][0]["sitemap_sources"] == [child]
    assert not issue["evidence"][0]["proven_orphan"]


def test_sitemap_page_budget_preserves_unvisited_inventory_evidence(monkeypatch):
    sitemap = ORIGIN + "/sitemap.xml"
    fixture_fetch(monkeypatch, {
        ORIGIN + "/robots.txt": response(ORIGIN + "/robots.txt", "", "text/plain"),
        sitemap: response(sitemap, ORIGIN + "/unvisited", "text/plain"),
        ORIGIN + "/": response(ORIGIN + "/", html()),
    })
    result = run(gs.get_seo_audit_report(ORIGIN, max_pages=1, include_sitemaps=True))
    assert result["audit_status"] == "partial"
    assert result["coverage"]["unvisited_sitemap_urls"] == [ORIGIN + "/unvisited"]
    assert "sitemap_unlinked_candidate" not in issue_map(result)


def test_public_report_parses_real_html_annotations_and_structured_documents(monkeypatch):
    head = f'''<link rel="alternate" hreflang="en" href="{ORIGIN}/" />
    <link rel="stylesheet" hreflang="fr" href="/not-an-alternate" />
    <link rel="alternate" hreflang="de" href="/de" />
    <script type="application/ld+json">{{"@context":"https://schema.org","@type":"Product"}}</script>
    <script type="application/ld+json">{{bad json}}</script>'''
    body = f'<link rel="alternate" hreflang="ka" href="{ORIGIN}/body-ignored">'
    fixture_fetch(monkeypatch, {
        ORIGIN + "/robots.txt": response(ORIGIN + "/robots.txt", "", "text/plain"),
        ORIGIN + "/": response(ORIGIN + "/", html(head=head, body=body)),
    })
    result = run(gs.get_seo_audit_report(ORIGIN))
    page = result["pages"][0]
    assert [entry["language"] for entry in page["hreflangs"]] == ["en", "de"]
    assert page["hreflangs"][1] == {"language": "de", "url": ORIGIN + "/de", "raw_url": "/de"}
    assert page["structured_data"] == [{"@context": "https://schema.org", "@type": "Product"}]
    assert {"structured_product_required", "invalid_json_ld", "hreflang_invalid_url"} <= issue_map(result).keys()


def test_public_compare_cannot_resolve_metadata_from_partial_browser_result(monkeypatch):
    fixture_fetch(monkeypatch, {
        ORIGIN + "/robots.txt": response(ORIGIN + "/robots.txt", "", "text/plain"),
        ORIGIN + "/": response(ORIGIN + "/", html()),
    })
    rendered = AsyncMock(side_effect=[
        {"rendered_html": html(title=""), "status": "complete"},
        {"rendered_html": html(title="Repaired title"), "status": "partial", "pending_requests": 1},
    ])
    monkeypatch.setattr(gs, "render_page", rendered)
    before = run(gs.get_seo_audit_report(ORIGIN, render_mode="compare"))
    after = run(gs.get_seo_audit_report(ORIGIN, render_mode="compare"))
    comparison = run(gs.compare_seo_audits(json.dumps(before), json.dumps(after)))
    assert after["audit_status"] == "partial"
    assert any(item["rule_id"] == "missing_title" for item in comparison["unverified"])
    assert not any(item["rule_id"] == "missing_title" for item in comparison["resolved"])
    assert rendered.await_count == 2


def traffic_row(url, clicks=10.0, impressions=100.0):
    return {"keys": [url], "clicks": clicks, "impressions": impressions, "ctr": clicks / impressions, "position": 5.0}


def google_responses(monkeypatch, *responses):
    service = MagicMock()
    service.searchanalytics().query().execute.side_effect = responses
    factory = MagicMock(return_value=service)
    monkeypatch.setattr(gs, "get_gsc_service", factory)
    return service, factory


def full_google_page():
    return [traffic_row(ORIGIN + f"/page/{index}") for index in range(25000)]


def test_snapshot_paginates_and_deduplicates_overlapping_provisional_rows(monkeypatch):
    first = full_google_page()
    service, _ = google_responses(monkeypatch, {"rows": first},
                                  {"rows": [first[-1], traffic_row(ORIGIN + "/last")],
                                   "metadata": {"first_incomplete_date": "2026-09-27"}})
    result = run(gs.get_search_analytics_snapshot("sc-domain:example.com", max_rows=30000))
    assert len(result["rows"]) == 25001
    assert result["coverage"] == {"rows": 25001, "requests": 2, "duplicate_rows": 1,
                                  "stop_reason": "api_window_exhausted", "complete_for_api_window": True}
    assert result["metadata"]["first_incomplete_date"] == "2026-09-27"
    requests = service.searchanalytics().query.call_args_list[-2:]
    assert requests[0].kwargs["body"]["startRow"] == 0
    assert requests[1].kwargs["body"]["startRow"] == 25000
    assert requests[1].kwargs["body"]["rowLimit"] == 5000
    assert all("orderBy" not in request.kwargs["body"] for request in requests)


@pytest.mark.parametrize("limit,requests,reason", [(25000, 10, "row_limit"), (50000, 1, "request_limit")])
def test_snapshot_budget_never_claims_complete_api_window(monkeypatch, limit, requests, reason):
    google_responses(monkeypatch, {"rows": full_google_page()})
    result = run(gs.get_search_analytics_snapshot("sc-domain:example.com", max_rows=limit, max_requests=requests))
    assert result["coverage"]["stop_reason"] == reason
    assert not result["coverage"]["complete_for_api_window"]


def test_repeated_api_page_stops_and_retains_first_observations(monkeypatch):
    first = full_google_page()
    service, _ = google_responses(monkeypatch, {"rows": first}, {"rows": first})
    result = run(gs.get_search_analytics_snapshot("sc-domain:example.com", max_rows=100000))
    assert result["coverage"]["stop_reason"] == "repeated_page"
    assert result["coverage"]["requests"] == 2
    assert result["coverage"]["duplicate_rows"] == 25000
    assert len(result["rows"]) == 25000
    assert not result["coverage"]["complete_for_api_window"]


def test_snapshot_preserves_rows_and_redacts_provider_error(monkeypatch):
    google_responses(monkeypatch, {"rows": full_google_page()}, RuntimeError("provider key=test-pagespeed-key failed"))
    result = run(gs.get_search_analytics_snapshot("sc-domain:example.com", max_rows=50000))
    assert len(result["rows"]) == 25000
    assert result["coverage"]["stop_reason"] == "api_error"
    assert not result["coverage"]["complete_for_api_window"]
    assert "test-pagespeed-key" not in json.dumps(result)
    assert result["error"]


def test_snapshot_missing_rows_is_empty_available_window(monkeypatch):
    google_responses(monkeypatch, {})
    result = run(gs.get_search_analytics_snapshot("sc-domain:example.com"))
    assert result["rows"] == []
    assert result["coverage"]["complete_for_api_window"]
    assert any("omit" in limitation for limitation in result["limitations"])


def test_snapshot_auth_error_is_unknown_and_never_complete(monkeypatch):
    factory = MagicMock(side_effect=RuntimeError("No configured credential"))
    monkeypatch.setattr(gs, "get_gsc_service", factory)
    result = run(gs.get_search_analytics_snapshot("sc-domain:example.com"))
    assert result["rows"] == []
    assert result["coverage"]["requests"] == 0
    assert not result["coverage"]["complete_for_api_window"]
    assert result["error"]


def test_snapshot_provider_type_error_keeps_already_fetched_rows(monkeypatch):
    google_responses(monkeypatch, {"rows": full_google_page()}, TypeError("Provider returned an invalid response"))
    result = run(gs.get_search_analytics_snapshot("sc-domain:example.com", max_rows=50000))
    assert len(result["rows"]) == 25000
    assert result["coverage"]["stop_reason"] == "api_error"
    assert not result["coverage"]["complete_for_api_window"]


def test_snapshot_timeout_includes_service_acquisition(monkeypatch):
    original_timeout = asyncio.timeout
    entered = []

    def short_timeout(seconds):
        entered.append(seconds)
        return original_timeout(0.01)

    async def slow_service(_factory):
        assert entered == [180]
        await asyncio.sleep(1)

    monkeypatch.setattr(gs.asyncio, "timeout", short_timeout)
    monkeypatch.setattr(gs, "google_service", slow_service)
    result = run(gs.get_search_analytics_snapshot("sc-domain:example.com"))
    assert result["coverage"]["stop_reason"] == "time_budget"
    assert result["coverage"]["requests"] == 0
    assert result["rows"] == []


@pytest.mark.parametrize("kwargs", [{"max_rows": 0}, {"max_requests": 11}, {"dimensions": "unknown"}, {"days": 0}])
def test_snapshot_bad_inputs_do_not_authenticate(monkeypatch, kwargs):
    _, factory = google_responses(monkeypatch)
    result = run(gs.get_search_analytics_snapshot("sc-domain:example.com", **kwargs))
    assert "error" in result
    factory.assert_not_called()


def report_json(issues):
    return json.dumps({"schema_version": "1.0", "origin": ORIGIN, "issues": issues})


def snapshot_mock(monkeypatch, rows, error=None):
    snapshot = {"rows": rows, "coverage": {"complete_for_api_window": not bool(error)}, "error": error, "limitations": ["Bounded provider sample."]}
    getter = AsyncMock(return_value=snapshot)
    monkeypatch.setattr(gs, "get_search_analytics_snapshot", getter)
    return getter


def test_prioritization_matches_exact_urls_and_deduplicates_within_each_rule(monkeypatch):
    snapshot_mock(monkeypatch, [traffic_row(ORIGIN + "/a", 5, 100), traffic_row(ORIGIN + "/a?lang=en", 2, 50), traffic_row(ORIGIN + "/a/", 99, 1000)])
    issue = {"rule_id": "missing_title", "severity": "high",
             "affected_urls": [ORIGIN + "/a", ORIGIN + "/a", ORIGIN + "/a?lang=en", ORIGIN + "/alias"]}
    result = run(gs.prioritize_audit_issues(report_json([issue]), "sc-domain:example.com"))
    assert result["issues"][0]["traffic"] == {"observed_clicks": 7, "observed_impressions": 150, "matched_pages": 2, "unknown_pages": 1}
    assert any("exact" in limitation for limitation in result["limitations"])


def test_prioritization_orders_severity_then_clicks_then_impressions(monkeypatch):
    snapshot_mock(monkeypatch, [traffic_row(ORIGIN + "/a", 3, 100), traffic_row(ORIGIN + "/b", 3, 200), traffic_row(ORIGIN + "/c", 100, 1000)])
    issues = [{"rule_id": "high_a", "severity": "high", "affected_urls": [ORIGIN + "/a"]},
              {"rule_id": "low_c", "severity": "low", "affected_urls": [ORIGIN + "/c"]},
              {"rule_id": "high_b", "severity": "high", "affected_urls": [ORIGIN + "/b"]},
              {"rule_id": "critical_unknown", "severity": "critical", "affected_urls": [ORIGIN + "/unknown"]}]
    result = run(gs.prioritize_audit_issues(report_json(issues), "sc-domain:example.com"))
    assert [issue["rule_id"] for issue in result["issues"]] == ["critical_unknown", "high_b", "high_a", "low_c"]
    assert result["issues"][0]["traffic"]["unknown_pages"] == 1
    assert result["issues"][0]["traffic"]["matched_pages"] == 0


def test_prioritization_preserves_partial_analytics_warning(monkeypatch):
    snapshot_mock(monkeypatch, [traffic_row(ORIGIN + "/a")], error="Provider unavailable after first page")
    result = run(gs.prioritize_audit_issues(report_json([{"rule_id": "x", "affected_urls": [ORIGIN + "/a", ORIGIN + "/b"]}]), "sc-domain:example.com"))
    assert result["analytics_error"]
    assert not result["analytics_coverage"]["complete_for_api_window"]
    assert result["issues"][0]["traffic"]["unknown_pages"] == 1


def test_prioritization_rejects_invalid_issue_before_spending_api_quota(monkeypatch):
    getter = snapshot_mock(monkeypatch, [])
    result = run(gs.prioritize_audit_issues(report_json([{"rule_id": "x", "affected_urls": [None]}]), "sc-domain:example.com"))
    assert "error" in result
    getter.assert_not_called()


def test_status_checks_only_presence_without_credentials_network_or_jobs(monkeypatch, tmp_path):
    token = tmp_path / "private-file-name.json"
    token.write_text('{"token":"private-test-token-value"}', encoding="utf-8")
    monkeypatch.setattr(gs, "TOKEN_FILE", str(token))
    monkeypatch.setattr(gs, "PAGESPEED_API_KEY", "private-pagespeed-value")
    monkeypatch.setattr(gs, "CRUX_API_KEY", "private-crux-value")
    service = MagicMock(side_effect=AssertionError("Health must not authenticate"))
    fetch = AsyncMock(side_effect=AssertionError("Health must not fetch"))
    monkeypatch.setattr(gs, "get_gsc_service", service)
    monkeypatch.setattr(gs, "_fetch_url", fetch)
    monkeypatch.setattr("builtins.open", MagicMock(side_effect=AssertionError("Health must not read credential values")))
    result = run(gs.get_server_status())
    encoded = json.dumps(result)
    assert result["oauth_token_present"]
    assert result["pagespeed_key_configured"] and result["crux_key_configured"]
    assert result["live_provider_access"] == "not_tested"
    assert all(secret not in encoded for secret in ("private-test-token-value", "private-pagespeed-value", "private-crux-value", str(token)))
    service.assert_not_called()
    fetch.assert_not_called()


@pytest.fixture
def browser_site():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            requests.append(self.path)
            content_type, status = "text/html", 200
            if self.path == "/robots.txt":
                content_type, body = "text/plain", "User-agent: *\nAllow: /\n"
            elif self.path == "/":
                body = '''<!doctype html><html lang="en"><head></head><body><h1>Waiting</h1>
                <script>document.title='Hydrated audit fixture';document.querySelector('h1').textContent='Rendered content';
                const link=document.createElement('a');link.href='/rendered-child';link.textContent='Rendered child';document.body.append(link);</script>
                </body></html>'''
            elif self.path == "/rendered-child":
                body = html("Rendered child page")
            else:
                status, content_type, body = 404, "text/plain", "Not found"
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.skipif(os.environ.get("SEO_AUDIT_TEST_BROWSER") != "1" or importlib.util.find_spec("playwright") is None,
                    reason="Set SEO_AUDIT_TEST_BROWSER=1 with Playwright Chromium installed.")
def test_public_report_real_browser_discovers_javascript_link_and_records_changes(browser_site, monkeypatch):
    origin, requests = browser_site
    monkeypatch.setattr(gs, "ALLOW_PRIVATE_URLS", True)
    result = run(gs.get_seo_audit_report(origin, max_pages=3, render_mode="compare", max_seconds=60))
    assert "error" not in result, result
    assert result["audit_status"] == "complete", result
    pages = {page["url"]: page for page in result["pages"]}
    assert set(pages) == {origin + "/", origin + "/rendered-child"}
    assert pages[origin + "/"]["title"] == "Hydrated audit fixture"
    assert pages[origin + "/"]["rendering"]["raw_signals"]["title"] == ""
    assert pages[origin + "/rendered-child"]["linked_from"] == [origin + "/"]
    assert "rendering_signal_difference" in issue_map(result)
    assert "missing_title" not in issue_map(result)
    assert "/robots.txt" in requests and "/rendered-child" in requests
