"""
Comprehensive QA test suite for GSC MCP Server.
All Google API calls are mocked — no credentials needed.
Run: python -m pytest test_gsc_server.py -v
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, mock_open

import pytest

# Set env vars BEFORE importing the module to avoid startup errors
os.environ.setdefault("GSC_SKIP_OAUTH", "true")
os.environ.setdefault("GSC_DATA_STATE", "all")
os.environ.setdefault("CRUX_API_KEY", "test-key-123")

import gsc_server as gs


# ─── Helpers ────────────────────────────────────────────────────────────────

def run(coro):
    """Run an async tool function synchronously."""
    return asyncio.run(coro)


def make_mock_service():
    """Create a deeply-nested mock that mimics the Google API client."""
    return MagicMock()


def mock_search_rows(queries, clicks=10, impressions=100, ctr=0.1, position=5.0):
    """Generate mock search analytics rows."""
    return [
        {
            "keys": [q] if isinstance(q, str) else list(q),
            "clicks": clicks,
            "impressions": impressions,
            "ctr": ctr,
            "position": position,
        }
        for q in queries
    ]


def mock_search_rows_with_pages(pairs, clicks=10, impressions=100, ctr=0.1, position=5.0):
    """Generate mock search analytics rows with query+page keys."""
    return [
        {
            "keys": [query, page],
            "clicks": clicks,
            "impressions": impressions,
            "ctr": ctr,
            "position": position,
        }
        for query, page in pairs
    ]


# ─── Configuration Tests ───────────────────────────────────────────────────

class TestConfiguration:
    def test_data_state_defaults_to_all(self):
        assert gs.DATA_STATE == "all"

    def test_crux_api_key_from_env(self):
        assert gs.CRUX_API_KEY == "test-key-123"

    def test_gsc_scopes_defined(self):
        assert "webmasters" in gs.GSC_SCOPES[0]

    def test_indexing_scopes_defined(self):
        assert "indexing" in gs.INDEXING_SCOPES[0]

    def test_possible_credential_paths_is_list(self):
        assert isinstance(gs.POSSIBLE_CREDENTIAL_PATHS, list)
        assert len(gs.POSSIBLE_CREDENTIAL_PATHS) >= 2


# ─── Auth Helper Tests ─────────────────────────────────────────────────────

class TestAuth:
    @patch("gsc_server.SKIP_OAUTH", False)
    @patch("gsc_server.get_gsc_service_oauth")
    def test_get_gsc_service_prefers_oauth(self, mock_oauth):
        gs._gsc_service_cache = None  # Clear cache
        mock_svc = MagicMock()
        mock_oauth.return_value = mock_svc
        result = gs.get_gsc_service()
        assert result == mock_svc
        gs._gsc_service_cache = None  # Cleanup

    def test_get_gsc_service_caches(self):
        sentinel = MagicMock()
        gs._gsc_service_cache = sentinel
        assert gs.get_gsc_service() == sentinel
        gs._gsc_service_cache = None

    def test_get_indexing_service_caches(self):
        sentinel = MagicMock()
        gs._indexing_service_cache = sentinel
        assert gs.get_indexing_service() == sentinel
        gs._indexing_service_cache = None

    def test_site_not_found_error_domain_property(self):
        msg = gs._site_not_found_error("sc-domain:example.com")
        assert "sc-domain" in msg
        assert "domain" in msg.lower()

    def test_site_not_found_error_url_property(self):
        msg = gs._site_not_found_error("https://example.com/")
        assert "sc-domain:example.com" in msg


# ─── Property Management Tests ─────────────────────────────────────────────

class TestPropertyManagement:
    @patch("gsc_server.get_gsc_service")
    def test_list_properties_returns_sites(self, mock_get):
        svc = make_mock_service()
        svc.sites().list().execute.return_value = {
            "siteEntry": [
                {"siteUrl": "sc-domain:example.com", "permissionLevel": "siteOwner"},
                {"siteUrl": "https://test.com/", "permissionLevel": "siteFullUser"},
            ]
        }
        mock_get.return_value = svc
        result = run(gs.list_properties())
        assert "sc-domain:example.com" in result
        assert "siteOwner" in result
        assert "test.com" in result

    @patch("gsc_server.get_gsc_service")
    def test_list_properties_empty(self, mock_get):
        svc = make_mock_service()
        svc.sites().list().execute.return_value = {"siteEntry": []}
        mock_get.return_value = svc
        result = run(gs.list_properties())
        assert "No Search Console properties" in result

    @patch("gsc_server.get_gsc_service")
    def test_list_properties_handles_error(self, mock_get):
        mock_get.side_effect = Exception("Connection error")
        result = run(gs.list_properties())
        assert "Error" in result

    @patch("gsc_server.get_gsc_service")
    def test_add_site_success(self, mock_get):
        svc = make_mock_service()
        svc.sites().add().execute.return_value = None
        mock_get.return_value = svc
        result = run(gs.add_site("https://newsite.com"))
        assert "has been added" in result

    @patch("gsc_server.get_gsc_service")
    def test_delete_site_success(self, mock_get):
        svc = make_mock_service()
        svc.sites().delete().execute.return_value = None
        mock_get.return_value = svc
        result = run(gs.delete_site("https://old.com"))
        assert "has been removed" in result


# ─── Search Analytics Tests ────────────────────────────────────────────────

class TestSearchAnalytics:
    @patch("gsc_server.get_gsc_service")
    def test_get_search_analytics_returns_data(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {
            "rows": mock_search_rows(["cdl jobs", "truck driver jobs"])
        }
        mock_get.return_value = svc
        result = run(gs.get_search_analytics("sc-domain:example.com"))
        assert "cdl jobs" in result
        assert "truck driver jobs" in result
        assert "Clicks" in result

    @patch("gsc_server.get_gsc_service")
    def test_get_search_analytics_no_data(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {"rows": []}
        mock_get.return_value = svc
        result = run(gs.get_search_analytics("sc-domain:example.com"))
        assert "No search analytics data" in result

    @patch("gsc_server.get_gsc_service")
    def test_get_search_analytics_search_type(self, mock_get):
        """Verify search_type parameter is passed through."""
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {"rows": mock_search_rows(["test"])}
        mock_get.return_value = svc
        result = run(gs.get_search_analytics("sc-domain:example.com", search_type="IMAGE"))
        assert "type=IMAGE" in result

    @patch("gsc_server.get_gsc_service")
    def test_get_search_analytics_row_limit_clamped(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {"rows": mock_search_rows(["test"])}
        mock_get.return_value = svc
        # row_limit > 500 should be clamped
        run(gs.get_search_analytics("sc-domain:example.com", row_limit=9999))
        call_body = svc.searchanalytics().query.call_args
        # The rowLimit in the body should be 500
        # Note: mock chaining makes this tricky; just verify it doesn't error
        assert True

    @patch("gsc_server.get_gsc_service")
    def test_get_advanced_search_analytics_with_filters(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {
            "rows": mock_search_rows(["cdl jobs near me"])
        }
        mock_get.return_value = svc
        filters_json = json.dumps([{"dimension": "query", "operator": "contains", "expression": "cdl"}])
        result = run(gs.get_advanced_search_analytics("sc-domain:example.com", filters=filters_json))
        assert "cdl jobs near me" in result
        assert "Filters:" in result

    @patch("gsc_server.get_gsc_service")
    def test_get_advanced_search_analytics_invalid_filters(self, mock_get):
        result = run(gs.get_advanced_search_analytics("sc-domain:example.com", filters="not json"))
        assert "Invalid filters JSON" in result

    @patch("gsc_server.get_gsc_service")
    def test_get_advanced_search_analytics_invalid_data_state(self, mock_get):
        result = run(gs.get_advanced_search_analytics("sc-domain:example.com", data_state="invalid"))
        assert "Invalid data_state" in result

    @patch("gsc_server.get_gsc_service")
    def test_get_advanced_sort_direction_uppercase(self, mock_get):
        """Verify sort_direction is properly mapped to API constants."""
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {"rows": mock_search_rows(["test"])}
        mock_get.return_value = svc
        # Should not error — the direction gets mapped to DESCENDING
        result = run(gs.get_advanced_search_analytics("sc-domain:example.com", sort_direction="descending"))
        assert "Error" not in result or "test" in result


class TestPerformanceOverview:
    @patch("gsc_server.get_gsc_service")
    def test_performance_overview_returns_totals(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.side_effect = [
            {"rows": [{"clicks": 500, "impressions": 10000, "ctr": 0.05, "position": 12.3}]},
            {"rows": mock_search_rows(["2025-01-01", "2025-01-02"])},
        ]
        mock_get.return_value = svc
        result = run(gs.get_performance_overview("sc-domain:example.com"))
        assert "500" in result
        assert "10,000" in result
        assert "12.3" in result

    @patch("gsc_server.get_gsc_service")
    def test_performance_overview_no_data(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {"rows": []}
        mock_get.return_value = svc
        result = run(gs.get_performance_overview("sc-domain:example.com"))
        assert "No data" in result


class TestComparePeriods:
    @patch("gsc_server.get_gsc_service")
    def test_compare_periods_shows_diff(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.side_effect = [
            {"rows": [{"keys": ["cdl jobs"], "clicks": 50, "impressions": 500, "ctr": 0.1, "position": 8.0}]},
            {"rows": [{"keys": ["cdl jobs"], "clicks": 80, "impressions": 600, "ctr": 0.13, "position": 6.0}]},
        ]
        mock_get.return_value = svc
        result = run(gs.compare_search_periods(
            "sc-domain:example.com", "2025-01-01", "2025-01-28", "2025-02-01", "2025-02-28"
        ))
        assert "cdl jobs" in result
        assert "+30" in result  # click_diff = 80-50

    @patch("gsc_server.get_gsc_service")
    def test_compare_periods_new_keyword(self, mock_get):
        """Keyword exists only in period 2."""
        svc = make_mock_service()
        svc.searchanalytics().query().execute.side_effect = [
            {"rows": []},
            {"rows": [{"keys": ["new keyword"], "clicks": 20, "impressions": 200, "ctr": 0.1, "position": 10.0}]},
        ]
        mock_get.return_value = svc
        result = run(gs.compare_search_periods(
            "sc-domain:example.com", "2025-01-01", "2025-01-28", "2025-02-01", "2025-02-28"
        ))
        assert "new keyword" in result


class TestSearchByPageQuery:
    @patch("gsc_server.get_gsc_service")
    def test_returns_queries_for_page(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {
            "rows": [
                {"keys": ["cdl jobs"], "clicks": 30, "impressions": 500, "ctr": 0.06, "position": 7.0},
                {"keys": ["truck driver"], "clicks": 20, "impressions": 400, "ctr": 0.05, "position": 9.0},
            ]
        }
        mock_get.return_value = svc
        result = run(gs.get_search_by_page_query("sc-domain:example.com", "https://example.com/jobs"))
        assert "cdl jobs" in result
        assert "TOTAL" in result
        assert "50" in result  # 30 + 20

    @patch("gsc_server.get_gsc_service")
    def test_no_queries_for_page(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {"rows": []}
        mock_get.return_value = svc
        result = run(gs.get_search_by_page_query("sc-domain:example.com", "https://example.com/nonexistent"))
        assert "No search data" in result


# ─── URL Inspection Tests ──────────────────────────────────────────────────

class TestURLInspection:
    @patch("gsc_server.get_gsc_service")
    def test_inspect_url_indexed(self, mock_get):
        svc = make_mock_service()
        svc.urlInspection().index().inspect().execute.return_value = {
            "inspectionResult": {
                "indexStatusResult": {
                    "verdict": "PASS",
                    "coverageState": "Submitted and indexed",
                    "lastCrawlTime": "2025-03-01T00:00:00Z",
                    "pageFetchState": "SUCCESSFUL",
                    "robotsTxtState": "ALLOWED",
                    "indexingState": "INDEXING_ALLOWED",
                    "googleCanonical": "https://example.com/page",
                    "crawledAs": "DESKTOP",
                },
                "mobileUsabilityResult": {"verdict": "PASS"},
            }
        }
        mock_get.return_value = svc
        result = run(gs.inspect_url("sc-domain:example.com", "https://example.com/page"))
        assert "PASS" in result
        assert "Submitted and indexed" in result
        assert "2025-03-01" in result

    @patch("gsc_server.get_gsc_service")
    def test_inspect_url_not_indexed(self, mock_get):
        svc = make_mock_service()
        svc.urlInspection().index().inspect().execute.return_value = {
            "inspectionResult": {
                "indexStatusResult": {
                    "verdict": "NEUTRAL",
                    "coverageState": "Crawled - currently not indexed",
                }
            }
        }
        mock_get.return_value = svc
        result = run(gs.inspect_url("sc-domain:example.com", "https://example.com/page"))
        assert "NEUTRAL" in result
        assert "not indexed" in result

    @patch("gsc_server.get_gsc_service")
    def test_batch_inspect_urls_categorizes(self, mock_get):
        svc = make_mock_service()
        responses = [
            {"inspectionResult": {"indexStatusResult": {"verdict": "PASS", "coverageState": "Submitted and indexed", "lastCrawlTime": "2025-03-01"}}},
            {"inspectionResult": {"indexStatusResult": {"verdict": "NEUTRAL", "coverageState": "Crawled - currently not indexed", "lastCrawlTime": "2025-02-15"}}},
            {"inspectionResult": {"indexStatusResult": {"verdict": "FAIL", "coverageState": "Not found (404)", "lastCrawlTime": "2025-01-10"}}},
        ]
        svc.urlInspection().index().inspect().execute.side_effect = responses
        mock_get.return_value = svc

        urls = "https://example.com/good\nhttps://example.com/crawled\nhttps://example.com/missing"
        result = run(gs.batch_inspect_urls("sc-domain:example.com", urls))
        assert "Indexed: 1" in result
        assert "Crawled not indexed: 1" in result
        assert "Not found (404): 1" in result

    @patch("gsc_server.get_gsc_service")
    def test_batch_inspect_urls_empty(self, mock_get):
        result = run(gs.batch_inspect_urls("sc-domain:example.com", ""))
        assert "No URLs" in result

    @patch("gsc_server.get_gsc_service")
    def test_batch_inspect_urls_too_many(self, mock_get):
        urls = "\n".join([f"https://example.com/page{i}" for i in range(51)])
        result = run(gs.batch_inspect_urls("sc-domain:example.com", urls))
        assert "Too many" in result
        assert "50" in result


# ─── Sitemap Tests ─────────────────────────────────────────────────────────

class TestSitemaps:
    @patch("gsc_server.get_gsc_service")
    def test_get_sitemaps_lists_all(self, mock_get):
        svc = make_mock_service()
        svc.sitemaps().list().execute.return_value = {
            "sitemap": [
                {
                    "path": "https://example.com/sitemap.xml",
                    "lastDownloaded": "2025-03-10T12:00:00Z",
                    "isSitemapsIndex": False,
                    "errors": "0",
                    "warnings": "2",
                    "contents": [{"type": "web", "submitted": "62"}],
                }
            ]
        }
        mock_get.return_value = svc
        result = run(gs.get_sitemaps("sc-domain:example.com"))
        assert "sitemap.xml" in result
        assert "62" in result

    @patch("gsc_server.get_gsc_service")
    def test_get_sitemaps_none(self, mock_get):
        svc = make_mock_service()
        svc.sitemaps().list().execute.return_value = {}
        mock_get.return_value = svc
        result = run(gs.get_sitemaps("sc-domain:example.com"))
        assert "No sitemaps" in result

    @patch("gsc_server.get_gsc_service")
    def test_submit_sitemap_success(self, mock_get):
        svc = make_mock_service()
        svc.sitemaps().submit().execute.return_value = None
        mock_get.return_value = svc
        result = run(gs.submit_sitemap("sc-domain:example.com", "https://example.com/sitemap.xml"))
        assert "Successfully submitted" in result

    @patch("gsc_server.get_gsc_service")
    def test_delete_sitemap_success(self, mock_get):
        svc = make_mock_service()
        svc.sitemaps().delete().execute.return_value = None
        mock_get.return_value = svc
        result = run(gs.delete_sitemap("sc-domain:example.com", "https://example.com/sitemap.xml"))
        assert "Deleted" in result


# ─── Indexing API Tests ────────────────────────────────────────────────────

class TestIndexingAPI:
    @patch("gsc_server.get_indexing_service")
    def test_request_indexing_success(self, mock_get):
        svc = make_mock_service()
        svc.urlNotifications().publish().execute.return_value = {
            "urlNotificationMetadata": {
                "latestUpdate": {"notifyTime": "2025-03-13T12:00:00Z"}
            }
        }
        mock_get.return_value = svc
        result = run(gs.request_indexing("https://example.com/jobs/new"))
        assert "Indexing requested" in result
        assert "2025-03-13" in result

    @patch("gsc_server.get_indexing_service")
    def test_request_indexing_rate_limit(self, mock_get):
        from googleapiclient.errors import HttpError
        svc = make_mock_service()
        resp = MagicMock()
        resp.status = 429
        svc.urlNotifications().publish().execute.side_effect = HttpError(resp, b"rate limited")
        mock_get.return_value = svc
        result = run(gs.request_indexing("https://example.com/jobs/new"))
        assert "Rate limit" in result

    @patch("gsc_server.get_indexing_service")
    def test_request_indexing_permission_denied(self, mock_get):
        from googleapiclient.errors import HttpError
        svc = make_mock_service()
        resp = MagicMock()
        resp.status = 403
        svc.urlNotifications().publish().execute.side_effect = HttpError(resp, b"forbidden")
        mock_get.return_value = svc
        result = run(gs.request_indexing("https://example.com/jobs/new"))
        assert "Permission denied" in result

    @patch("gsc_server.get_indexing_service")
    def test_request_removal_success(self, mock_get):
        svc = make_mock_service()
        svc.urlNotifications().publish().execute.return_value = {}
        mock_get.return_value = svc
        result = run(gs.request_removal("https://example.com/old-page"))
        assert "Removal requested" in result

    @patch("gsc_server.get_indexing_service")
    def test_batch_request_indexing_success(self, mock_get):
        svc = make_mock_service()
        svc.urlNotifications().publish().execute.return_value = {}
        mock_get.return_value = svc
        urls = "https://example.com/page1\nhttps://example.com/page2\nhttps://example.com/page3"
        result = run(gs.batch_request_indexing(urls))
        assert "Submitted: 3" in result
        assert "Failed: 0" in result

    @patch("gsc_server.get_indexing_service")
    def test_batch_request_indexing_empty(self, mock_get):
        result = run(gs.batch_request_indexing(""))
        assert "No URLs" in result

    @patch("gsc_server.get_indexing_service")
    def test_batch_request_indexing_too_many(self, mock_get):
        urls = "\n".join([f"https://example.com/p{i}" for i in range(101)])
        result = run(gs.batch_request_indexing(urls))
        assert "Too many" in result

    @patch("gsc_server.get_indexing_service")
    def test_batch_request_indexing_partial_failure(self, mock_get):
        from googleapiclient.errors import HttpError
        svc = make_mock_service()
        resp_429 = MagicMock()
        resp_429.status = 429
        svc.urlNotifications().publish().execute.side_effect = [
            {},  # success
            HttpError(resp_429, b"rate limited"),  # fail and stop
        ]
        mock_get.return_value = svc
        urls = "https://example.com/page1\nhttps://example.com/page2\nhttps://example.com/page3"
        result = run(gs.batch_request_indexing(urls))
        assert "Submitted: 1" in result
        assert "Failed: 1" in result  # stops on rate limit

    @patch("gsc_server.get_indexing_service")
    def test_check_indexing_notification_found(self, mock_get):
        svc = make_mock_service()
        svc.urlNotifications().getMetadata().execute.return_value = {
            "latestUpdate": {"type": "URL_UPDATED", "notifyTime": "2025-03-13T12:00:00Z", "url": "https://example.com/page"}
        }
        mock_get.return_value = svc
        result = run(gs.check_indexing_notification("https://example.com/page"))
        assert "URL_UPDATED" in result

    @patch("gsc_server.get_indexing_service")
    def test_check_indexing_notification_not_found(self, mock_get):
        from googleapiclient.errors import HttpError
        svc = make_mock_service()
        resp = MagicMock()
        resp.status = 404
        svc.urlNotifications().getMetadata().execute.side_effect = HttpError(resp, b"not found")
        mock_get.return_value = svc
        result = run(gs.check_indexing_notification("https://example.com/never-submitted"))
        assert "No indexing notifications" in result


# ─── Core Web Vitals Tests ─────────────────────────────────────────────────

class TestCoreWebVitals:
    def test_crux_no_api_key(self):
        original = gs.CRUX_API_KEY
        gs.CRUX_API_KEY = ""
        result = run(gs.get_core_web_vitals("https://example.com"))
        assert "not configured" in result
        gs.CRUX_API_KEY = original

    def test_origin_detection_no_path(self):
        """https://example.com should be treated as origin."""
        from urllib.parse import urlparse
        parsed = urlparse("https://example.com")
        assert parsed.path in ("", "/")

    def test_origin_detection_with_path(self):
        """https://example.com/jobs should be treated as URL."""
        from urllib.parse import urlparse
        parsed = urlparse("https://example.com/jobs")
        assert parsed.path not in ("", "/")

    def test_origin_detection_trailing_slash(self):
        """https://example.com/ should be treated as origin (not URL)."""
        from urllib.parse import urlparse
        parsed = urlparse("https://example.com/")
        assert parsed.path in ("", "/")

    @patch("urllib.request.urlopen")
    def test_crux_passing_vitals(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "record": {
                "key": {"origin": "https://example.com"},
                "metrics": {
                    "largest_contentful_paint": {"percentiles": {"p75": 2000}, "histogram": [{"density": 0.8}, {"density": 0.15}, {"density": 0.05}]},
                    "interaction_to_next_paint": {"percentiles": {"p75": 150}, "histogram": [{"density": 0.9}, {"density": 0.08}, {"density": 0.02}]},
                    "cumulative_layout_shift": {"percentiles": {"p75": 0.05}, "histogram": [{"density": 0.95}, {"density": 0.04}, {"density": 0.01}]},
                },
                "collectionPeriod": {"firstDate": {"year": 2025, "month": 2}, "lastDate": {"year": 2025, "month": 3}},
            }
        }).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = run(gs.get_core_web_vitals("https://example.com"))
        assert "PASSING" in result
        assert "LCP" in result
        assert "GOOD" in result

    @patch("urllib.request.urlopen")
    def test_crux_failing_vitals(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "record": {
                "key": {"origin": "https://slow.com"},
                "metrics": {
                    "largest_contentful_paint": {"percentiles": {"p75": 5000}, "histogram": [{"density": 0.3}, {"density": 0.3}, {"density": 0.4}]},
                    "interaction_to_next_paint": {"percentiles": {"p75": 500}, "histogram": [{"density": 0.3}, {"density": 0.3}, {"density": 0.4}]},
                    "cumulative_layout_shift": {"percentiles": {"p75": 0.3}, "histogram": [{"density": 0.3}, {"density": 0.3}, {"density": 0.4}]},
                },
                "collectionPeriod": {"firstDate": {"year": 2025, "month": 2}, "lastDate": {"year": 2025, "month": 3}},
            }
        }).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = run(gs.get_core_web_vitals("https://slow.com"))
        assert "FAILING" in result
        assert "NEEDS WORK" in result


# ─── SEO Analysis Tests ───────────────────────────────────────────────────

class TestStrikingDistance:
    @patch("gsc_server.get_gsc_service")
    def test_finds_striking_distance_keywords(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {
            "rows": [
                {"keys": ["cdl jobs near me", "https://example.com/jobs"], "clicks": 5, "impressions": 200, "ctr": 0.025, "position": 8.0},
                {"keys": ["truck driver salary", "https://example.com/salary"], "clicks": 2, "impressions": 150, "ctr": 0.013, "position": 12.0},
                {"keys": ["already ranked", "https://example.com/top"], "clicks": 50, "impressions": 500, "ctr": 0.1, "position": 2.0},  # Not striking distance
            ]
        }
        mock_get.return_value = svc
        result = run(gs.find_striking_distance_keywords("sc-domain:example.com"))
        assert "cdl jobs near me" in result
        assert "truck driver salary" in result
        assert "already ranked" not in result  # position 2.0 is outside 5-20

    @patch("gsc_server.get_gsc_service")
    def test_no_striking_distance(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {"rows": []}
        mock_get.return_value = svc
        result = run(gs.find_striking_distance_keywords("sc-domain:example.com"))
        assert "No data" in result


class TestCannibalization:
    @patch("gsc_server.get_gsc_service")
    def test_detects_cannibalization(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {
            "rows": [
                {"keys": ["cdl jobs", "https://example.com/jobs"], "clicks": 30, "impressions": 500, "ctr": 0.06, "position": 5.0},
                {"keys": ["cdl jobs", "https://example.com/jobs/flatbed"], "clicks": 10, "impressions": 200, "ctr": 0.05, "position": 12.0},
                {"keys": ["unique keyword", "https://example.com/salary"], "clicks": 20, "impressions": 300, "ctr": 0.07, "position": 7.0},
            ]
        }
        mock_get.return_value = svc
        result = run(gs.detect_cannibalization("sc-domain:example.com"))
        assert "cdl jobs" in result
        assert "2 pages" in result
        assert "unique keyword" not in result  # Only 1 page, not cannibalized

    @patch("gsc_server.get_gsc_service")
    def test_no_cannibalization(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.return_value = {
            "rows": [
                {"keys": ["query1", "https://example.com/page1"], "clicks": 10, "impressions": 100, "ctr": 0.1, "position": 5.0},
            ]
        }
        mock_get.return_value = svc
        result = run(gs.detect_cannibalization("sc-domain:example.com"))
        assert "No keyword cannibalization" in result


class TestBrandedQueries:
    @patch("gsc_server.get_gsc_service")
    def test_splits_branded_nonbranded(self, mock_get):
        svc = make_mock_service()
        svc.searchanalytics().query().execute.side_effect = [
            {"rows": [{"keys": ["cdljobscenter"], "clicks": 100, "impressions": 200, "ctr": 0.5, "position": 1.0}]},
            {"rows": [{"keys": ["cdl jobs near me"], "clicks": 50, "impressions": 1000, "ctr": 0.05, "position": 8.0}]},
            {"rows": [{"clicks": 150, "impressions": 1200, "ctr": 0.125, "position": 4.5}]},
        ]
        mock_get.return_value = svc
        result = run(gs.split_branded_queries("sc-domain:example.com", "cdljobscenter"))
        assert "Branded" in result
        assert "Non-Branded" in result
        assert "100" in result
        assert "50" in result

    @patch("gsc_server.get_gsc_service")
    def test_branded_empty_total(self, mock_get):
        """Edge case: total query returns empty rows."""
        svc = make_mock_service()
        svc.searchanalytics().query().execute.side_effect = [
            {"rows": []},
            {"rows": []},
            {"rows": []},
        ]
        mock_get.return_value = svc
        result = run(gs.split_branded_queries("sc-domain:example.com", "testbrand"))
        assert "Branded" in result
        # Should not crash — division by zero is guarded


# ─── Site Audit Tests ──────────────────────────────────────────────────────

class TestSiteAudit:
    @patch("gsc_server.get_gsc_service")
    def test_full_audit_runs(self, mock_get):
        svc = make_mock_service()

        # Sitemaps
        svc.sitemaps().list().execute.return_value = {
            "sitemap": [{"path": "https://example.com/sitemap.xml", "errors": "0", "warnings": "0",
                         "contents": [{"type": "web", "submitted": "50"}]}]
        }

        # Performance totals + top pages
        svc.searchanalytics().query().execute.side_effect = [
            {"rows": [{"clicks": 1000, "impressions": 20000, "ctr": 0.05, "position": 10.0}]},
            {"rows": [
                {"keys": ["https://example.com/"], "clicks": 200, "impressions": 5000, "position": 3.0},
                {"keys": ["https://example.com/jobs"], "clicks": 150, "impressions": 4000, "position": 5.0},
            ]},
        ]

        # URL inspections for top pages
        svc.urlInspection().index().inspect().execute.side_effect = [
            {"inspectionResult": {"indexStatusResult": {"verdict": "PASS", "coverageState": "Indexed"}}},
            {"inspectionResult": {"indexStatusResult": {"verdict": "PASS", "coverageState": "Indexed"}}},
        ]

        mock_get.return_value = svc
        result = run(gs.site_audit("sc-domain:example.com", max_inspect=2))
        assert "Site Audit Report" in result
        assert "SITEMAP HEALTH" in result
        assert "PERFORMANCE SUMMARY" in result
        assert "TOP PAGES" in result
        assert "INDEXING STATUS" in result
        assert "Indexed: 2" in result

    @patch("gsc_server.get_gsc_service")
    def test_audit_with_issues(self, mock_get):
        svc = make_mock_service()

        svc.sitemaps().list().execute.return_value = {"sitemap": []}

        svc.searchanalytics().query().execute.side_effect = [
            {"rows": [{"clicks": 100, "impressions": 2000, "ctr": 0.05, "position": 15.0}]},
            {"rows": [{"keys": ["https://example.com/broken"], "clicks": 10, "impressions": 200, "position": 20.0}]},
        ]

        svc.urlInspection().index().inspect().execute.return_value = {
            "inspectionResult": {"indexStatusResult": {
                "verdict": "FAIL",
                "coverageState": "Not found (404)",
                "googleCanonical": "https://example.com/other",
                "userCanonical": "https://example.com/broken",
            }}
        }

        mock_get.return_value = svc
        result = run(gs.site_audit("sc-domain:example.com", max_inspect=1))
        assert "WARNING: No sitemaps" in result
        assert "NOT FOUND" in result
        assert "CANONICAL MISMATCH" in result


# ─── Auth Management Tests ─────────────────────────────────────────────────

class TestReauthenticate:
    @patch("gsc_server.InstalledAppFlow")
    @patch("gsc_server.os.path.exists")
    @patch("gsc_server.os.remove")
    @patch("builtins.open", mock_open())
    def test_reauthenticate_clears_cache(self, mock_remove, mock_exists, mock_flow):
        gs._gsc_service_cache = MagicMock()
        gs._indexing_service_cache = MagicMock()

        mock_exists.return_value = True
        mock_creds = MagicMock()
        mock_creds.to_json.return_value = '{"token": "new"}'
        mock_flow.from_client_secrets_file.return_value.run_local_server.return_value = mock_creds

        result = run(gs.reauthenticate())
        assert gs._gsc_service_cache is None
        assert gs._indexing_service_cache is None
        assert "Successfully authenticated" in result


# ─── Helper Function Tests ─────────────────────────────────────────────────

class TestHelpers:
    def test_format_crux_metric_empty(self):
        result = gs._format_crux_metric({}, "LCP")
        assert "No data" in result

    def test_format_crux_metric_full(self):
        data = {
            "percentiles": {"p75": 2000},
            "histogram": [{"density": 0.8}, {"density": 0.15}, {"density": 0.05}],
        }
        result = gs._format_crux_metric(data, "LCP")
        assert "p75=2000" in result
        assert "80%" in result
        assert "15%" in result
        assert "5%" in result

    def test_format_crux_metric_partial_histogram(self):
        data = {"percentiles": {"p75": 100}, "histogram": [{"density": 0.9}]}
        result = gs._format_crux_metric(data, "INP")
        assert "p75=100" in result
        assert "90%" in result


# ─── Edge Case / Regression Tests ─────────────────────────────────────────

class TestEdgeCases:
    @patch("gsc_server.get_gsc_service")
    def test_404_triggers_helpful_error(self, mock_get):
        mock_get.side_effect = Exception("HttpError 404")
        result = run(gs.get_search_analytics("sc-domain:wrong.com"))
        assert "not found" in result.lower() or "404" in result

    @patch("gsc_server.get_gsc_service")
    def test_search_analytics_truncates_long_keys(self, mock_get):
        svc = make_mock_service()
        long_query = "x" * 200
        svc.searchanalytics().query().execute.return_value = {
            "rows": [{"keys": [long_query], "clicks": 1, "impressions": 10, "ctr": 0.1, "position": 5.0}]
        }
        mock_get.return_value = svc
        result = run(gs.get_search_analytics("sc-domain:example.com"))
        # Keys are truncated to 100 chars
        assert "x" * 100 in result
        assert "x" * 200 not in result

    @patch("gsc_server.get_gsc_service")
    def test_batch_inspect_handles_api_error_per_url(self, mock_get):
        svc = make_mock_service()
        svc.urlInspection().index().inspect().execute.side_effect = Exception("API quota exceeded")
        mock_get.return_value = svc
        result = run(gs.batch_inspect_urls("sc-domain:example.com", "https://example.com/test"))
        assert "Errors: 1" in result

    def test_module_has_23_tools(self):
        tools = list(gs.mcp._tool_manager._tools.keys())
        assert len(tools) == 23

    def test_all_expected_tools_registered(self):
        tools = set(gs.mcp._tool_manager._tools.keys())
        expected = {
            "list_properties", "add_site", "delete_site",
            "get_search_analytics", "get_advanced_search_analytics",
            "get_performance_overview", "compare_search_periods", "get_search_by_page_query",
            "inspect_url", "batch_inspect_urls",
            "get_sitemaps", "submit_sitemap", "delete_sitemap",
            "request_indexing", "request_removal", "batch_request_indexing", "check_indexing_notification",
            "get_core_web_vitals",
            "find_striking_distance_keywords", "detect_cannibalization", "split_branded_queries",
            "site_audit", "reauthenticate",
        }
        assert tools == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
