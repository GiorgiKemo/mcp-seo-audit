"""Verification suite for all fixes applied to mcp-seo-audit."""

import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock

# Mock third-party dependencies ONLY if not installed in current environment
def fake_tool(*args, **kwargs):
    def decorator(fn):
        return fn
    return decorator

for mod in ["httpx", "mcp", "mcp.types", "mcp.server", "mcp.server.fastmcp", "bs4", "google", "google.auth", "google.auth.transport", "google.auth.transport.requests", "google.oauth2", "google.oauth2.credentials", "google.oauth2.service_account", "googleapiclient", "googleapiclient.discovery", "googleapiclient.errors", "google_auth_oauthlib", "google_auth_oauthlib.flow"]:
    if importlib.util.find_spec(mod) is None and mod not in sys.modules:
        m = MagicMock()
        m.__path__ = []
        if mod in {"mcp", "mcp.server.fastmcp"}:
            m.FastMCP.return_value.tool = fake_tool
        sys.modules[mod] = m

import httpx

from seo_robots import RobotsPolicy
import seo_storage as storage
import seo_sitemaps as sitemaps
import seo_diagnostics as diagnostics
import seo_reporting as reporting
import gsc_server as gs


class TestRobotsFixes(unittest.TestCase):
    def test_numeric_and_hyphenated_bot_tokens(self):
        policy = RobotsPolicy("""
            User-agent: 360Spider
            Disallow: /spider-only
            
            User-agent: bot-v2
            Disallow: /v2-only

            User-agent: *
            Allow: /
        """, user_agent="360Spider")
        self.assertFalse(policy.can_fetch("https://example.com/spider-only"))
        self.assertTrue(policy.can_fetch("https://example.com/other"))

    def test_no_rogue_substring_matching(self):
        policy = RobotsPolicy("""
            User-agent: A6-Indexer
            Disallow: /

            User-agent: audit
            Disallow: /short

            User-agent: *
            Allow: /
        """, user_agent="mcp-seo-audit")
        # Neither A6-Indexer nor audit should match mcp-seo-audit
        self.assertTrue(policy.can_fetch("https://example.com/"))
        self.assertTrue(policy.can_fetch("https://example.com/short"))

    def test_crawl_delay_in_multi_agent_group(self):
        policy = RobotsPolicy("""
            User-agent: a
            Crawl-delay: 5
            User-agent: mcp-seo-audit
            Disallow: /x
        """, user_agent="mcp-seo-audit")
        self.assertEqual(policy.crawl_delay, 5.0)
        self.assertFalse(policy.can_fetch("https://example.com/x"))


class TestRedirectFixes(unittest.TestCase):
    def test_redirect_blocked_exception(self):
        resp = MagicMock()
        resp.status_code = 301
        exc = gs.RedirectBlocked("https://external.example.com/", "External redirect blocked", resp)
        self.assertEqual(exc.target, "https://external.example.com/")
        self.assertEqual(exc.response.status_code, 301)


class TestStorageAndMonitoringFixes(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.store = storage.AuditStore(os.path.join(self.temp_dir, "db"))

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_delete_project_cascades(self):
        self.store.create_project("test_proj", "https://example.com")
        self.assertIsNotNone(self.store.get_project("test_proj"))
        # Add a dummy audit and event directly into db
        with self.store._connection(write=True) as conn:
            conn.execute("INSERT INTO audits(audit_id,project_id,created_at,summary_json,report_json) VALUES(?,?,?,?,?)",
                         ("a" * 32, "test_proj", 100.0, "{}", "{}"))
            conn.execute("INSERT INTO events(project_id,created_at,kind,payload_json) VALUES(?,?,?,?)",
                         ("test_proj", 100.0, "audit_started", "{}"))
        result = self.store.delete_project("test_proj")
        self.assertTrue(result["deleted"])
        with self.assertRaises(ValueError):
            self.store.get_project("test_proj")
        with self.store._connection() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM audits WHERE project_id='test_proj'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT count(*) FROM events WHERE project_id='test_proj'").fetchone()[0], 0)


class TestHttpLinkHeaderAndCanonical(unittest.TestCase):
    def test_parse_http_link_headers(self):
        headers = {
            "link": '<https://example.com/canonical-page>; rel="canonical", <https://example.com/es>; rel="alternate"; hreflang="es"'
        }
        canonicals, hreflangs = gs._parse_http_link_headers(headers)
        self.assertEqual(canonicals, ["https://example.com/canonical-page"])
        self.assertEqual(hreflangs, [("es", "https://example.com/es")])


class TestBatchUrlParsing(unittest.TestCase):
    def test_parse_batch_urls_newlines_and_json(self):
        text = "https://example.com/a\nhttps://example.com/b?x=1,2"
        self.assertEqual(gs._parse_batch_urls(text), ["https://example.com/a", "https://example.com/b?x=1,2"])

        json_arr = '["https://example.com/1", "https://example.com/2"]'
        self.assertEqual(gs._parse_batch_urls(json_arr), ["https://example.com/1", "https://example.com/2"])


class TestSitemapNamespaces(unittest.TestCase):
    def test_legacy_and_https_namespaces_accepted(self):
        xml_084 = b"""<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.google.com/schemas/sitemap/0.84">
            <url><loc>https://example.com/</loc></url>
        </urlset>"""
        kind, urls = sitemaps._parse_document(xml_084)
        self.assertEqual(kind, "urlset")
        self.assertEqual(urls, ["https://example.com/"])

        xml_https = b"""<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="https://www.sitemaps.org/schemas/sitemap/0.9">
            <url><loc>https://example.com/https-ns</loc></url>
        </urlset>"""
        kind2, urls2 = sitemaps._parse_document(xml_https)
        self.assertEqual(kind2, "urlset")
        self.assertEqual(urls2, ["https://example.com/https-ns"])


class TestDiagnosticsBreadcrumbAndDate(unittest.TestCase):
    def test_breadcrumb_string_position(self):
        data = {
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": "1", "name": "Home", "item": "https://example.com/"},
                {"@type": "ListItem", "position": "2", "name": "Sub", "item": "https://example.com/sub"},
            ]
        }
        page = {"url": "https://example.com/sub", "state": "html", "status": 200, "structured_data": [data]}
        result = {"settings": {"rendering": "raw"}, "pages": [page]}
        report = diagnostics.enrich_report(result)
        rule_ids = [issue["rule_id"] for issue in report.get("issues", [])]
        self.assertNotIn("structured_breadcrumb_required", rule_ids)

    def test_breadcrumb_unicode_exponent_rejected_without_crash(self):
        data = {
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": "²", "name": "Home", "item": "https://example.com/"},
                {"@type": "ListItem", "position": 2, "name": "Sub", "item": "https://example.com/sub"},
            ]
        }
        page = {"url": "https://example.com/sub", "state": "html", "status": 200, "structured_data": [data]}
        result = {"settings": {"rendering": "raw"}, "pages": [page]}
        # Should not raise ValueError
        report = diagnostics.enrich_report(result)
        rule_ids = [issue["rule_id"] for issue in report.get("issues", [])]
        self.assertIn("structured_breadcrumb_required", rule_ids)

    def test_incomplete_rendering_not_eligible_for_diagnostics(self):
        for rendering in [
            {"status": "partial"},
            {"status": "complete", "pending_requests": 2},
            {"status": "complete", "resource_redirects": ["/script"]},
            {"status": "complete", "js_errors": ["error"]},
        ]:
            page = {"url": "https://example.com/", "state": "html", "status": 200, "rendering": rendering, "structured_data": [{"@type": "Product"}]}
            result = {"settings": {"rendering": "compare"}, "pages": [page]}
            report = diagnostics.enrich_report(result)
            rule_ids = [i["rule_id"] for i in report.get("issues", [])]
            self.assertNotIn("structured_product_required", rule_ids)


class TestReportingEdgeCases(unittest.TestCase):
    def test_canonical_malformed_port_no_crash(self):
        import seo_reporting
        crawl = {
            "pages": [
                {"url": "https://example.com:99999/page", "requested_url": "https://example.com:99999/page", "state": "html", "status": 200, "findings": [], "canonicals": ["https://example.com:bad/canonical"]},
            ],
            "settings": {},
            "robots": {"state": "loaded"},
            "coverage": {"termination_reason": "frontier_exhausted", "complete_for_discovered_links": True},
            "start_url": "https://example.com/page",
            "origin": "https://example.com",
        }
        report = seo_reporting.build_audit_report(crawl)
        self.assertIsInstance(report, dict)

    def test_canonical_port_handling_per_scheme(self):
        import seo_reporting
        crawl = {
            "pages": [
                {"url": "https://example.com/start", "requested_url": "https://example.com/start", "state": "html", "status": 200, "findings": [], "canonicals": ["https://example.com:80/target"]},
                {"url": "https://example.com/target", "requested_url": "https://example.com/target", "state": "html", "status": 404, "findings": []},
            ],
            "settings": {},
            "robots": {"state": "loaded"},
            "coverage": {"termination_reason": "frontier_exhausted", "complete_for_discovered_links": True},
            "start_url": "https://example.com/start",
            "origin": "https://example.com",
        }
        report = seo_reporting.build_audit_report(crawl)
        rule_ids = [i["rule_id"] for i in report.get("issues", [])]
        self.assertNotIn("canonical_unavailable", rule_ids)

    def test_report_includes_blocked_redirects(self):
        import seo_reporting
        crawl = {
            "pages": [
                {"url": "https://other.com", "requested_url": "https://example.com/ext", "state": "external_redirect", "status": 301, "issues": ["External redirect from https://example.com/ext"], "findings": []},
                {"url": "https://example.com/blocked", "requested_url": "https://example.com/int", "state": "robots_blocked_redirect", "status": 302, "issues": ["Redirect from https://example.com/int to https://example.com/blocked blocked by robots.txt"], "findings": []},
            ],
            "settings": {},
            "robots": {"state": "loaded"},
            "coverage": {"termination_reason": "frontier_exhausted", "complete_for_discovered_links": True},
            "start_url": "https://example.com/int",
            "origin": "https://example.com",
        }
        report = seo_reporting.build_audit_report(crawl)
        rule_ids = [i["rule_id"] for i in report.get("issues", [])]
        self.assertIn("external_redirect", rule_ids)
        self.assertIn("robots_blocked_redirect", rule_ids)


class TestInspectRobotsTxt(unittest.TestCase):
    def test_inspect_robots_multi_group_no_false_block(self):
        content = """
User-agent: *
Disallow: /private

User-agent: Googlebot
Disallow: /
"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = content
        import asyncio
        with unittest.mock.patch("gsc_server._fetch_url", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_resp
            output = asyncio.run(gs.inspect_robots_txt("https://example.com"))
            self.assertIn("Blocks all crawling for user-agent *: NO", output)
            self.assertIn("/private", output)

    def test_inspect_robots_disallow_root_with_allow_reports_no(self):
        content = """
User-agent: *
Disallow: /
Allow: /public
"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = content
        import asyncio
        with unittest.mock.patch("gsc_server._fetch_url", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_resp
            output = asyncio.run(gs.inspect_robots_txt("https://example.com"))
            self.assertIn("Blocks all crawling for user-agent *: NO", output)

    def test_inspect_robots_429_treated_as_temporary_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.text = "Rate limited"
        import asyncio
        with unittest.mock.patch("gsc_server._fetch_url", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_resp
            output = asyncio.run(gs.inspect_robots_txt("https://example.com"))
            self.assertIn("Google treats 429 and 5xx server errors on robots.txt as a full crawl block", output)

    def test_http_link_header_quoted_comma_splitting(self):
        header = '<https://example.com/other>; rel="canonical"; title="Some title, with a comma", <https://example.com/alt>; rel="alternate"; hreflang="es"'
        canonicals, hreflangs = gs._parse_http_link_headers({"link": header})
        self.assertEqual(canonicals, ["https://example.com/other"])
        self.assertEqual(hreflangs, [("es", "https://example.com/alt")])

    def test_audit_live_site_relative_sitemap_resolution(self):
        import asyncio
        robots_resp = MagicMock()
        robots_resp.status_code = 200
        robots_resp.text = "User-agent: *\nAllow: /\nSitemap: /sitemap.xml"

        with unittest.mock.patch("gsc_server._fetch_url", new_callable=AsyncMock) as mock_fetch, \
             unittest.mock.patch("gsc_server.analyze_page_seo", new_callable=AsyncMock) as mock_page, \
             unittest.mock.patch("gsc_server.inspect_robots_txt", new_callable=AsyncMock) as mock_robots, \
             unittest.mock.patch("gsc_server.analyze_sitemap", new_callable=AsyncMock) as mock_sitemap, \
             unittest.mock.patch("gsc_server.get_pagespeed_insights", new_callable=AsyncMock) as mock_psi, \
             unittest.mock.patch("gsc_server.crawl_site_seo", new_callable=AsyncMock) as mock_crawl:

            mock_fetch.return_value = robots_resp
            mock_page.return_value = "Page SEO"
            mock_robots.return_value = "Robots info"
            mock_sitemap.return_value = "Sitemap info"
            mock_psi.return_value = "PSI info"
            mock_crawl.return_value = "Crawl info"

            output = asyncio.run(gs.audit_live_site("https://example.com"))
            mock_sitemap.assert_called_once_with("https://example.com/sitemap.xml", sample_urls=3)
            self.assertIn("Live SEO audit for https://example.com", output)


if __name__ == "__main__":
    unittest.main()
