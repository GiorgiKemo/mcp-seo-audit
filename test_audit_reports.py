"""Offline integration tests for crawl evidence and actionable reports."""

import asyncio
import copy
import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import gsc_server as gs


ORIGIN = "https://example.com"


def response(path="/", body="", status=200, content_type="text/html"):
    return httpx.Response(status, text=body, headers={"content-type": content_type},
                          request=httpx.Request("GET", ORIGIN + path))


def html(title="Example title", body="", head=""):
    return f'<html lang="en"><head><title>{title}</title>{head}</head><body><h1>Main heading</h1>{body}</body></html>'


def crawl(routes, max_pages=10, respect_robots=True):
    async def fetch(url, **kwargs):
        assert url in routes, f"Unexpected URL fetched: {url}"
        result = routes[url]
        if isinstance(result, Exception):
            raise result
        return result

    with patch.object(gs, "_fetch_url", side_effect=fetch) as mocked, patch.object(gs.asyncio, "sleep", new_callable=AsyncMock):
        result = asyncio.run(gs.get_seo_audit_report(ORIGIN, max_pages, respect_robots))
    assert "error" not in result, result
    return result, mocked


def routes_for(body="", robots=""):
    return {ORIGIN + "/robots.txt": response("/robots.txt", robots, content_type="text/plain"),
            ORIGIN + "/": response(body=html(body=body))}


def compare(before, after):
    return asyncio.run(gs.compare_seo_audits(json.dumps(before), json.dumps(after)))


def test_robots_disallow_is_not_fetched_and_coverage_disclosed():
    report, fetch = crawl(routes_for('<a href="/private">Private</a>', "User-agent: *\nDisallow: /private"))
    assert fetch.call_count == 2
    assert report["coverage"]["blocked_urls"] == [ORIGIN + "/private"]
    assert not report["coverage"]["complete_for_discovered_links"]


@pytest.mark.parametrize("status", [429, 500, 503])
def test_robots_unavailable_does_not_crawl_or_report_success(status):
    report, fetch = crawl({ORIGIN + "/robots.txt": response("/robots.txt", status=status)})
    assert fetch.call_count == 1
    assert report["coverage"]["termination_reason"] == "robots_unavailable"
    assert report["coverage"]["html_pages"] == 0
    assert not report["coverage"]["complete_for_discovered_links"]


def test_missing_robots_allows_crawl():
    routes = routes_for()
    routes[ORIGIN + "/robots.txt"] = response("/robots.txt", status=404)
    report, _ = crawl(routes)
    assert report["coverage"]["html_pages"] == 1
    assert report["robots"]["state"] == "missing"


def test_long_crawl_delay_defers_instead_of_ignoring():
    report, fetch = crawl(routes_for(robots="User-agent: *\nCrawl-delay: 60"))
    assert fetch.call_count == 1
    assert report["coverage"]["termination_reason"] == "crawl_delay_exceeds_budget"


def test_query_urls_kept_separate_and_broken_link_sources_reported():
    routes = routes_for('<a href="/?page=2">Two</a><a href="/missing">Missing</a>')
    routes[ORIGIN + "/?page=2"] = response("/?page=2", html("Second page", '<a href="/missing">Missing</a>'))
    routes[ORIGIN + "/missing"] = response("/missing", "Missing", status=404, content_type="text/plain")
    report, _ = crawl(routes)
    failure = next(i for i in report["issues"] if i["rule_id"] == "http_error")
    assert failure["affected_urls"] == [ORIGIN + "/missing"]
    assert failure["evidence"][0]["linked_from"] == [ORIGIN + "/", ORIGIN + "/?page=2"]
    assert failure["recommendation"]
    assert report["coverage"]["attempted"] == 3


def test_duplicate_groups_have_urls_and_no_redirect_double_counting():
    routes = routes_for('<a href="/two">Two</a><a href="/alias">Alias</a>')
    routes[ORIGIN + "/two"] = response("/two", html())
    routes[ORIGIN + "/alias"] = response("/two", html())
    report, _ = crawl(routes)
    duplicate = next(i for i in report["issues"] if i["rule_id"] == "duplicate_title")
    assert duplicate["affected_count"] == 2
    assert sum(p["state"] == "redirect_alias" for p in report["pages"]) == 1


def test_page_limit_exposes_remaining_frontier():
    report, fetch = crawl(routes_for('<a href="/two">Two</a>'), max_pages=1)
    assert fetch.call_count == 2
    assert report["coverage"]["remaining"] == 1
    assert report["coverage"]["termination_reason"] == "page_limit"


def test_nofollow_and_nonvisible_links_are_not_expanded():
    body = '<a rel="nofollow" href="/skip">Skip</a><noscript><a href="/fallback">Fallback</a></noscript>'
    report, fetch = crawl(routes_for(body))
    assert fetch.call_count == 2
    assert report["coverage"]["discovered"] == 1


def test_canonical_bad_target_action_includes_evidence():
    routes = routes_for()
    routes[ORIGIN + "/"] = response(body=html(body='<a href="/gone">Gone</a>', head='<link rel="canonical" href="https://example.com/gone">'))
    routes[ORIGIN + "/gone"] = response("/gone", status=410)
    report, _ = crawl(routes)
    issue = next(i for i in report["issues"] if i["rule_id"] == "canonical_unavailable")
    assert issue["evidence"][0]["target_status"] == 410


def test_report_roundtrip_and_fixed_title_comparison():
    routes = routes_for()
    routes[ORIGIN + "/"] = response(body=html(""))
    before, _ = crawl(routes)
    after, _ = crawl(routes_for())
    changes = compare(before, after)
    assert {i["rule_id"] for i in changes["resolved"]} == {"missing_title"}
    assert changes["counts"]["persistent"] > 0


def test_failed_current_fetch_is_unverified_not_resolved():
    before, _ = crawl(routes_for())
    routes = routes_for()
    routes[ORIGIN + "/"] = ValueError("Connection failed")
    after, _ = crawl(routes)
    changes = compare(before, after)
    assert changes["counts"]["resolved"] == 0
    assert changes["counts"]["unverified"] > 0


def test_mismatched_settings_and_invalid_snapshots_are_rejected():
    report, _ = crawl(routes_for())
    other = copy.deepcopy(report)
    other["settings"]["max_pages"] = 2
    assert "settings differ" in compare(report, other)["error"]
    assert "error" in asyncio.run(gs.compare_seo_audits("{}", "{}"))
    assert "error" in asyncio.run(gs.compare_seo_audits("broken", "{}"))


def test_parser_counts_only_visible_body_and_aggregates_robots():
    analysis = gs._analyze_html_document(ORIGIN, 200, {},
        '<html><head><title>Not body</title><meta name="robots" content="index">'
        '<meta name="robots" content="none"></head><body></body></html>')
    assert analysis["body_word_count"] == 0
    assert "Page is explicitly marked noindex" in analysis["issues"]
    assert analysis["nofollow"]


def test_parser_origin_and_image_link_alt_name():
    analysis = gs._analyze_html_document(ORIGIN, 200, {}, html(body=
        '<a href="https://example.com.evil.test/"><img alt="Other site" src="x"></a>'))
    assert analysis["links"]["internal"] == 0
    assert analysis["links"]["external"] == 1
    assert analysis["links"]["empty_text"] == []


def test_parser_ignores_images_hidden_from_the_accessibility_tree():
    analysis = gs._analyze_html_document(ORIGIN, 200, {}, html(body=
        '<picture aria-hidden="true"><img src="/decorative.jpg" alt=""></picture>'
        '<img src="/content.jpg" alt="">'))
    assert analysis["images"]["total"] == 2
    assert analysis["images"]["empty_alt"] == ["/content.jpg"]
    assert analysis["images"]["missing_size"] == ["/content.jpg"]
    assert not any("empty alt" in message.lower() for _, message in gs._seo_findings_from_analysis(analysis))


def test_redirect_validator_blocks_out_of_origin_and_disallowed_target():
    async def fetch(url, **kwargs):
        if url.endswith("robots.txt"):
            return response("/robots.txt", "User-agent: *\nDisallow: /private")
        validator = kwargs["redirect_validator"]
        with pytest.raises(ValueError, match="External redirect"):
            validator("https://other.example/")
        with pytest.raises(ValueError, match="robots.txt"):
            validator(ORIGIN + "/private")
        return response(body=html())

    with patch.object(gs, "_fetch_url", side_effect=fetch):
        report = asyncio.run(gs.get_seo_audit_report(ORIGIN))
    assert report["coverage"]["html_pages"] == 1


def test_explicit_robots_override_is_recorded():
    report, fetch = crawl({ORIGIN + "/": response(body=html())}, respect_robots=False)
    assert fetch.call_count == 1
    assert report["robots"]["state"] == "ignored_by_request"


def test_invalid_input_has_no_fetch():
    with patch.object(gs, "_fetch_url", new_callable=AsyncMock) as fetch:
        result = asyncio.run(gs.get_seo_audit_report("file:///secret"))
    assert "error" in result
    fetch.assert_not_called()


def test_crawl_normalization_preserves_path_parameters_and_query():
    assert gs._canonicalize_crawl_url(ORIGIN, "/page;lang=en?q=test#part") == ORIGIN + "/page;lang=en?q=test"


def test_base_url_resolves_relative_links_without_leaving_origin():
    assert gs._iter_internal_links(ORIGIN + "/docs/page", '<base href="/help/"><a href="faq">FAQ</a>') == [ORIGIN + "/help/faq"]
    assert gs._iter_internal_links(ORIGIN, '<base href="https://elsewhere.test/"><a href="faq">FAQ</a>') == []


def test_error_page_is_http_action_not_metadata_action():
    routes = routes_for()
    routes[ORIGIN + "/"] = response(body="<html><body>Error</body></html>", status=500)
    report, _ = crawl(routes)
    assert [i["rule_id"] for i in report["issues"]] == ["http_error"]


def test_robot_headers_only_apply_to_the_relevant_crawler():
    assert "noindex" not in gs._googlebot_directives("index", "", "bingbot: noindex, nofollow")
    assert "noindex" in gs._googlebot_directives("index", "", "bingbot: index, googlebot: noindex, nofollow")
    assert "noindex" in gs._googlebot_directives("", "", "max-snippet: 50, noindex")


@pytest.mark.parametrize("field", ["robots", "googlebot"])
def test_parameter_value_none_is_not_the_none_directive(field):
    page = html(head=f'<meta name="{field}" content="max-image-preview: none">')
    analysis = gs._analyze_html_document(ORIGIN, 200, {}, page)
    assert "Page is explicitly marked noindex" not in analysis["issues"]
    assert not analysis["nofollow"]


def test_separate_generic_header_applies_after_other_bot_header():
    headers = httpx.Headers([("x-robots-tag", "bingbot: nofollow"), ("x-robots-tag", "noindex")])
    analysis = gs._analyze_html_document(ORIGIN, 200, headers, html())
    assert "Page is explicitly marked noindex" in analysis["issues"]
    assert not analysis["nofollow"]
