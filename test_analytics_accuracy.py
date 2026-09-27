"""Regression coverage for truthful, bounded Search Console analytics."""

import asyncio
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import gsc_server as gs


def run(coro):
    return asyncio.run(coro)


def row(query, clicks=10.0, impressions=100.0, position=5.0):
    return {"keys": [query], "clicks": clicks, "impressions": impressions,
            "position": position, "ctr": clicks / impressions if impressions else 0}


def service_with_responses(*responses):
    service = MagicMock()
    service.searchanalytics().query().execute.side_effect = responses
    return service


def test_window_has_exactly_requested_inclusive_days_across_year_boundary():
    with patch("gsc_server._gsc_today", return_value=date(2026, 1, 1)):
        assert gs._gsc_date_window(1) == (date(2026, 1, 1), date(2026, 1, 1))
        start, end = gs._gsc_date_window(28)
    assert start == date(2025, 12, 5)
    assert (end - start).days + 1 == 28


@pytest.mark.parametrize("moment", [
    datetime(2026, 1, 1, 7, 30, tzinfo=timezone.utc),
    datetime(2026, 7, 1, 6, 30, tzinfo=timezone.utc),
])
def test_date_uses_pacific_timezone_in_winter_and_summer(moment):
    with patch("gsc_server.datetime") as clock:
        clock.now.side_effect = lambda zone: moment.astimezone(zone)
        result = gs._gsc_today()
    assert result == moment.date().fromordinal(moment.date().toordinal() - 1)


@pytest.mark.parametrize("call", [
    lambda: gs.get_search_analytics("sc-domain:example.com", days=0),
    lambda: gs.get_search_analytics("sc-domain:example.com", dimensions="query,query"),
    lambda: gs.get_search_analytics("sc-domain:example.com", dimensions="nonsense"),
    lambda: gs.get_search_analytics("sc-domain:example.com", row_limit=0),
    lambda: gs.get_search_analytics("sc-domain:example.com", search_type="bad"),
    lambda: gs.get_performance_overview("sc-domain:example.com", days=-1),
    lambda: gs.get_search_by_page_query("sc-domain:example.com", "https://example.com", days=501),
    lambda: gs.get_advanced_search_analytics("sc-domain:example.com", start_date="2026-02-30"),
    lambda: gs.get_advanced_search_analytics("sc-domain:example.com", start_date="2026-05-01", end_date="2026-04-01"),
    lambda: gs.get_advanced_search_analytics("sc-domain:example.com", start_row=-1),
    lambda: gs.get_advanced_search_analytics("sc-domain:example.com", row_limit=0),
    lambda: gs.get_advanced_search_analytics("sc-domain:example.com", sort_direction="sideways"),
    lambda: gs.get_advanced_search_analytics("sc-domain:example.com", filters='[null]'),
    lambda: gs.get_advanced_search_analytics("sc-domain:example.com", filters='[{"dimension":"bogus","operator":"equals","expression":"test"}]'),
    lambda: gs.find_striking_distance_keywords("sc-domain:example.com", min_impressions=-1),
    lambda: gs.detect_cannibalization("sc-domain:example.com", days=0),
    lambda: gs.split_branded_queries("sc-domain:example.com", "example", days=0),
    lambda: gs.site_audit("sc-domain:example.com", max_inspect=-1),
])
def test_invalid_inputs_do_not_authenticate_or_call_google(call):
    with patch("gsc_server.get_gsc_service") as service:
        result = run(call())
    service.assert_not_called()
    assert result


def test_supported_type_and_exact_window_are_sent_to_google():
    service = service_with_responses({"rows": [row("test")]})
    with patch("gsc_server.get_gsc_service", return_value=service), patch("gsc_server._gsc_today", return_value=date(2026, 1, 28)):
        run(gs.get_search_analytics("sc-domain:example.com", search_type="GOOGLENEWS"))
    request = service.searchanalytics().query.call_args.kwargs["body"]
    assert request["type"] == "googleNews"
    assert "searchType" not in request
    assert request["startDate"] == "2026-01-01"
    assert request["endDate"] == "2026-01-28"


def test_advanced_sorting_changes_display_and_states_sample_limit():
    service = service_with_responses({"rows": [row("alpha", 100.0, 100.0), row("beta", 10.0, 1000.0)]})
    with patch("gsc_server.get_gsc_service", return_value=service):
        result = run(gs.get_advanced_search_analytics("sc-domain:example.com", row_limit=2, start_row=10, sort_by="impressions"))
    request = service.searchanalytics().query.call_args.kwargs["body"]
    assert "orderBy" not in request
    assert request["startRow"] == 10
    assert result.index("beta |") < result.index("alpha |")
    assert "not a global ranking" in result
    assert "bounded sample" in result
    assert "start_row: 12" in result


def test_advanced_default_start_is_relative_to_explicit_end():
    service = service_with_responses({"rows": [row("test")]})
    with patch("gsc_server.get_gsc_service", return_value=service):
        run(gs.get_advanced_search_analytics("sc-domain:example.com", end_date="2025-12-28"))
    assert service.searchanalytics().query.call_args.kwargs["body"]["startDate"] == "2025-12-01"


def test_compare_accepts_float_counts_and_does_not_invent_missing_rank():
    service = service_with_responses(
        {"rows": [row("existing", 50.0, 500.0, 8.0)]},
        {"rows": [row("existing", 80.0, 600.0, 6.0), row("new candidate", 20.0, 200.0, 10.0)]},
    )
    with patch("gsc_server.get_gsc_service", return_value=service):
        result = run(gs.compare_search_periods("sc-domain:example.com", "2026-01-01", "2026-01-28", "2026-02-01", "2026-02-28"))
    assert "existing | 50 | 80 | +30 | 8.0 | 6.0 | +2.0" in result
    assert "new candidate | N/A | 20 | N/A | N/A | 10.0 | N/A" in result
    assert "not zero" in result
    assert "Error" not in result


def test_comparison_warns_when_period_lengths_differ():
    service = service_with_responses({"rows": []}, {"rows": []})
    with patch("gsc_server.get_gsc_service", return_value=service):
        result = run(gs.compare_search_periods("sc-domain:example.com", "2026-01-01", "2026-01-31", "2026-02-01", "2026-02-28"))
    assert "unequal lengths" in result


def test_property_total_comparison_accepts_rows_without_dimension_keys():
    service = service_with_responses(
        {"rows": [{"clicks": 100.0, "impressions": 1000.0, "position": 4.0}]},
        {"rows": [{"clicks": 120.0, "impressions": 1200.0, "position": 3.0}]},
    )
    with patch("gsc_server.get_gsc_service", return_value=service):
        result = run(gs.compare_search_periods("sc-domain:example.com", "2026-01-01", "2026-01-28", "2026-02-01", "2026-02-28", dimensions=""))
    assert "Property total | 100 | 120 | +20 | 4.0 | 3.0 | +1.0" in result


def test_page_query_reports_only_returned_totals_and_no_unsupported_ordering():
    service = service_with_responses({"rows": [row("test")]})
    with patch("gsc_server.get_gsc_service", return_value=service):
        result = run(gs.get_search_by_page_query("sc-domain:example.com", "https://example.com"))
    assert "orderBy" not in service.searchanalytics().query.call_args.kwargs["body"]
    assert "RETURNED ROWS TOTAL" in result
    assert "omit anonymized queries" in result


def test_overview_exposes_incomplete_date_without_false_daily_truncation():
    service = service_with_responses(
        {"rows": [row("unused")]},
        {"rows": [row("2026-01-28")], "metadata": {"first_incomplete_date": "2026-01-28"}},
    )
    with patch("gsc_server.get_gsc_service", return_value=service):
        result = run(gs.get_performance_overview("sc-domain:example.com", days=1))
    assert "2026-01-28 onward is incomplete" in result
    assert "Row limit reached" not in result


def test_zero_inspections_are_unknown_not_success():
    service = service_with_responses({"rows": []}, {"rows": []})
    service.sitemaps().list().execute.return_value = {"sitemap": []}
    with patch("gsc_server.get_gsc_service", return_value=service):
        result = run(gs.site_audit("sc-domain:example.com", max_inspect=0))
    assert "No URLs were inspected; indexing status is unknown" in result
    assert "properly indexed" not in result
    service.urlInspection.assert_not_called()


def test_discovered_not_indexed_is_not_mislabeled_as_crawled():
    page = row("https://example.com/new")
    service = service_with_responses({"rows": []}, {"rows": [page]})
    service.sitemaps().list().execute.return_value = {"sitemap": []}
    service.urlInspection().index().inspect().execute.return_value = {
        "inspectionResult": {"indexStatusResult": {"verdict": "NEUTRAL", "coverageState": "Discovered - currently not indexed"}}
    }
    with patch("gsc_server.get_gsc_service", return_value=service), patch("gsc_server.asyncio.sleep"):
        result = run(gs.site_audit("sc-domain:example.com", max_inspect=1))
    assert "Crawled not indexed: 0" in result
    assert "Discovered - currently not indexed" in result
