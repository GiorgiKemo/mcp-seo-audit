"""Offline recursive inventory tests with explicit fetch and memory budgets."""

import asyncio
import gzip

import httpx
import pytest

import seo_sitemaps as sm


ORIGIN = "https://example.com"


def xml(kind, urls):
    entry = "sitemap" if kind == "sitemapindex" else "url"
    return f'<{kind} xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + "".join(
        f"<{entry}><loc>{url.replace('&', '&amp;')}</loc></{entry}>" for url in urls) + f"</{kind}>"


def discover(routes, **kwargs):
    calls = []

    async def fetch(url):
        calls.append(url)
        assert url in routes, f"Unexpected fetch: {url}"
        value = routes[url]
        if isinstance(value, Exception):
            raise value
        if isinstance(value, httpx.Response):
            return value
        return httpx.Response(200, content=value, request=httpx.Request("GET", url))

    return asyncio.run(sm.discover_sitemap_urls(ORIGIN, fetch, **kwargs)), calls


def test_nested_sitemaps_loops_gzip_sources_and_query_preservation():
    root, nested, one, two = [ORIGIN + value for value in ("/sitemap.xml", "/nested.xml", "/one.xml.gz", "/two.txt")]
    page, query = ORIGIN + "/page", ORIGIN + "/page?lang=en&page=2"
    result, calls = discover({
        root: xml("sitemapindex", [nested, one]),
        nested: xml("sitemapindex", [root, two, one]),
        one: gzip.compress(xml("urlset", [page, query]).encode()),
        two: page + "\n" + query + "\n" + ORIGIN + "/other#fragment",
    })
    assert calls == [root, nested, one, two]
    assert result["known_urls"] == [page, query, ORIGIN + "/other"]
    assert result["sources"][page] == [one, two]
    assert result["coverage"]["complete_for_scope"]
    assert not result["coverage"]["truncated"]


def test_off_origin_and_relative_children_are_never_fetched_or_seeded():
    root, child = ORIGIN + "/sitemap.xml", ORIGIN + "/child.xml"
    result, calls = discover({root: xml("sitemapindex", ["https://evil.com/a.xml", "/relative.xml", child]),
                             child: xml("urlset", ["https://evil.com/page", "http://example.com/page", "/relative", ORIGIN + "/ok"])})
    assert calls == [root, child]
    assert result["known_urls"] == [ORIGIN + "/ok"]
    assert result["coverage"]["skipped_off_origin"] == 3
    assert result["coverage"]["skipped_invalid"] == 2
    assert not result["coverage"]["complete_for_scope"]


def test_default_port_and_host_case_are_deduplicated():
    root = ORIGIN + "/sitemap.xml"
    result, calls = discover({root: xml("urlset", ["https://EXAMPLE.com:443/a", ORIGIN + "/a"])},
                             seeds=[root, "https://EXAMPLE.com:443/sitemap.xml#x"])
    assert calls == [root]
    assert result["known_urls"] == [ORIGIN + "/a"]


def test_sitemap_count_budget_reports_omitted_children():
    root, child = ORIGIN + "/sitemap.xml", ORIGIN + "/one.xml"
    result, calls = discover({root: xml("sitemapindex", [child, ORIGIN + "/two.xml", child]), child: ORIGIN + "/ok"}, max_sitemaps=2)
    assert calls == [root, child]
    assert result["coverage"]["sitemaps_omitted"] == 1
    assert result["coverage"]["truncated"]
    assert not result["coverage"]["complete_for_scope"]


def test_url_budget_stops_before_next_file_and_exposes_remaining():
    root, one, two = [ORIGIN + value for value in ("/sitemap.xml", "/one.txt", "/two.txt")]
    result, calls = discover({root: xml("sitemapindex", [one, two]), one: "\n".join(ORIGIN + f"/{i}" for i in range(4))}, max_urls=2)
    assert calls == [root, one]
    assert result["coverage"]["urls_omitted"] == 2
    assert result["coverage"]["pending_sitemaps"] == [two]
    assert result["coverage"]["truncated"]


def test_exact_final_url_budget_is_complete():
    result, _ = discover({ORIGIN + "/sitemap.xml": ORIGIN + "/a"}, max_urls=1)
    assert result["coverage"]["complete_for_scope"]


def test_failure_does_not_abort_independent_seed_or_disclose_exception_secrets():
    root, next_file = ORIGIN + "/sitemap.xml", ORIGIN + "/ok.xml"
    result, calls = discover({root: RuntimeError("secret=token123"), next_file: ORIGIN + "/page"}, seeds=[root, next_file])
    assert calls == [root, next_file]
    assert result["known_urls"] == [ORIGIN + "/page"]
    assert result["coverage"]["failed_sitemaps"] == 1
    assert "token123" not in str(result)


def test_http_failure_and_final_off_origin_response_are_not_parsed():
    root, next_file = ORIGIN + "/sitemap.xml", ORIGIN + "/other.xml"
    result, _ = discover({root: httpx.Response(404, request=httpx.Request("GET", root)),
                         next_file: httpx.Response(200, text=ORIGIN + "/bad", request=httpx.Request("GET", "https://evil.com/file"))}, seeds=[root, next_file])
    assert not result["known_urls"]
    assert result["coverage"]["failed_sitemaps"] == 2


@pytest.mark.parametrize("body", [b"", b"<html></html>", b"<urlset>", b'<!DOCTYPE x [<!ENTITY y "value">]><urlset/>', b"\xff\xfe<\x00u\x00r\x00l\x00s\x00e\x00t\x00/\x00>\x00"])
def test_invalid_documents_are_reported(body):
    result, _ = discover({ORIGIN + "/sitemap.xml": body})
    assert result["coverage"]["failed_sitemaps"] == 1
    assert not result["coverage"]["complete_for_scope"]


def test_decompression_and_download_limits(monkeypatch):
    monkeypatch.setattr(sm, "MAX_DOCUMENT_BYTES", 100)
    for content in (b"x" * 101, gzip.compress(b"x" * 101)):
        result, _ = discover({ORIGIN + "/sitemap.xml": content})
        assert result["coverage"]["failed_sitemaps"] == 1


def test_media_extension_loc_is_not_a_page():
    body = f'<urlset xmlns:image="urn:image"><url><loc>{ORIGIN}/page</loc><image:image><image:loc>{ORIGIN}/image.jpg</image:loc></image:image></url></urlset>'
    result, _ = discover({ORIGIN + "/sitemap.xml": body})
    assert result["known_urls"] == [ORIGIN + "/page"]


def test_missing_loc_is_invalid_inventory_evidence():
    result, _ = discover({ORIGIN + "/sitemap.xml": "<urlset><url><loc/></url></urlset>"})
    assert result["coverage"]["skipped_invalid"] == 1
    assert not result["coverage"]["complete_for_scope"]


def test_foreign_namespace_loc_cannot_impersonate_page_location():
    result, _ = discover({ORIGIN + "/sitemap.xml": f'<urlset xmlns:image="urn:image"><url><image:loc>{ORIGIN}/image.jpg</image:loc></url></urlset>'})
    assert not result["known_urls"]
    assert result["coverage"]["skipped_invalid"] == 1


def test_redirect_alias_sitemap_is_parsed_only_once():
    root, alias = ORIGIN + "/sitemap.xml", ORIGIN + "/alias.xml"
    result, _ = discover({root: ORIGIN + "/a", alias: httpx.Response(200, text=ORIGIN + "/ignored", request=httpx.Request("GET", root))}, seeds=[root, alias])
    assert result["known_urls"] == [ORIGIN + "/a"]
    assert result["sitemaps"][1]["state"] == "duplicate_redirect"


def test_empty_seeds_do_not_fetch_and_do_not_claim_complete_inventory():
    result, calls = discover({}, seeds=[])
    assert not calls
    assert not result["coverage"]["complete_for_scope"]


@pytest.mark.parametrize("kwargs", [{"max_sitemaps": 0}, {"max_sitemaps": True}, {"max_urls": 50001}, {"max_urls": 2.5}])
def test_invalid_budgets_rejected(kwargs):
    with pytest.raises(ValueError):
        discover({}, **kwargs)


def test_cancellation_is_not_reported_as_a_sitemap_failure():
    async def fetch(url):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(sm.discover_sitemap_urls(ORIGIN, fetch))
