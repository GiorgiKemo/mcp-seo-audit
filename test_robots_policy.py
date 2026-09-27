"""Offline conformance and resource-bound checks for the crawl robots policy."""

import pytest

from seo_robots import MAX_ROBOTS_BYTES, RobotsPolicy


def test_most_specific_case_insensitive_agent_overrides_wildcard():
    policy = RobotsPolicy("""
        User-agent: *
        Disallow: /
        User-agent: audit
        Disallow: /short
        User-agent: MCP-SEO-AUDIT/1.0
        Disallow: /private
    """)
    assert policy.can_fetch("https://example.com/")
    assert policy.can_fetch("https://example.com/short")
    assert not policy.can_fetch("https://example.com/private")


def test_equally_specific_groups_merge_and_allow_wins_ties():
    policy = RobotsPolicy("""
        User-agent: mcp-seo-audit
        Disallow: /private
        User-agent: MCP-SEO-AUDIT
        Disallow: /other
        Allow: /private/public
        Disallow: /private/public
    """)
    assert not policy.can_fetch("/private")
    assert not policy.can_fetch("/other")
    assert policy.can_fetch("/private/public")


def test_consecutive_agents_and_unknown_fields_do_not_split_group():
    policy = RobotsPolicy("""
        User-agent: mcp-seo-audit
        Sitemap: https://example.com/sitemap.xml
        Unknown: value

        User-agent: anotherbot
        Disallow: /private
    """)
    assert not policy.can_fetch("/private")


def test_empty_rules_end_a_group_and_do_not_block():
    policy = RobotsPolicy("""
        User-agent: mcp-seo-audit
        Disallow:
        Allow:
        User-agent: *
        Disallow: /
    """)
    assert policy.can_fetch("/anything")


@pytest.mark.parametrize("text", ["", "Disallow: /", "User-agent: otherbot\nDisallow: /"])
def test_no_applicable_rules_allow_crawling(text):
    assert RobotsPolicy(text).can_fetch("https://example.com/")


def test_wildcard_group_is_fallback_and_path_case_is_significant():
    policy = RobotsPolicy("User-agent: *\nDisallow: /Fish")
    assert not policy.can_fetch("/Fish/salmon")
    assert policy.can_fetch("/fish/salmon")


@pytest.mark.parametrize(("rules", "url", "allowed"), [
    ("Disallow: /*.pdf$", "/report.pdf", False),
    ("Disallow: /*.pdf$", "/report.pdf?download=1", True),
    ("Disallow: /*?sort=", "/shop?sort=price", False),
    ("Disallow: /catalog/*/private$", "/catalog//private", False),
    ("Disallow: /catalog/*/private$", "/catalog/a/private/private", False),
    ("Disallow: /catalog/*/private$", "/catalog/a/private/public", True),
    ("Disallow: /a*b*c$", "/axbyc", False),
    ("Disallow: /a*b*c$", "/acb", True),
    ("Disallow: /x*", "/xyz", False),
    ("Disallow: /x*$", "/xyz", False),
    ("Disallow: /$", "https://example.com", False),
    ("Disallow: /$", "/x", True),
    ("Disallow: /$", "/?", True),
    ("Disallow: /page$", "/page#section", False),
    ("Allow: /page\nDisallow: /*.htm", "/page.htm", False),
    ("Allow: /page\nDisallow: /*.ph", "/page.php5", True),
    ("Allow: /$\nDisallow: /", "/", True),
    ("Disallow: *.gif$", "/images/cat.gif", False),
])
def test_wildcards_queries_anchors_and_google_precedence_examples(rules, url, allowed):
    assert RobotsPolicy("User-agent: *\n" + rules).can_fetch(url) is allowed


@pytest.mark.parametrize(("rule", "url", "allowed"), [
    ("/café", "/caf%C3%A9", False),
    ("/caf%c3%a9", "/café", False),
    ("/%74ools", "/tools", False),
    ("/tools", "/%74ools", False),
    ("/a%2Fb", "/a/b", True),
    ("/a/b", "/a%2fb", True),
    ("/a%2fb", "/a%2Fb", False),
    ("/file%2A", "/file*", False),
    ("/file%2A", "/filename", True),
    ("/price%24", "/price$", False),
    ("/price$tag", "/price$tag", False),
    ("/hash%23tag", "/hash%23tag", False),
    ("/space%20here", "/space here", False),
    ("/a%3Fb", "/a?b", True),
])
def test_utf8_and_percent_encoded_paths(rule, url, allowed):
    assert RobotsPolicy("User-agent: *\nDisallow: " + rule).can_fetch(url) is allowed


def test_bom_comments_case_insensitive_fields_and_newlines():
    policy = RobotsPolicy("\ufeffUsEr-AgEnT: *\rDiSaLlOw: /private # comment\r\nAllow: /private/public\n")
    assert not policy.can_fetch("/private")
    assert policy.can_fetch("/private/public")


def test_invalid_rules_do_not_override_valid_rules():
    policy = RobotsPolicy("User-agent: *\nDisallow: /private\nAllow: private\nAllow: /private\x00")
    assert not policy.can_fetch("/private")


def test_robots_resource_itself_is_implicitly_allowed():
    policy = RobotsPolicy("User-agent: *\nDisallow: /")
    assert policy.can_fetch("https://example.com/robots.txt")
    assert policy.can_fetch("https://example.com/robots%2Etxt")
    assert not policy.can_fetch("https://example.com/other")


def test_largest_applicable_valid_delay_is_preserved_for_caller_budget():
    policy = RobotsPolicy("""
        User-agent: *
        Crawl-delay: 999
        Disallow:
        User-agent: mcp-seo-audit
        Crawl-delay: 0.5
        Crawl-delay: nan
        Crawl-delay: -2
        Crawl-delay: infinity
        Crawl-delay: invalid
        Disallow:
        User-agent: mcp-seo-audit
        Crawl-delay: 120
    """)
    assert policy.crawl_delay == 120
    assert RobotsPolicy("").crawl_delay is None
    assert RobotsPolicy("User-agent: *\nCrawl-delay: 0").crawl_delay == 0


def test_size_bound_and_truncation_are_exposed():
    prefix = "User-agent: *\nDisallow: /private\n#"
    text = prefix + "x" * (MAX_ROBOTS_BYTES - len(prefix)) + "\nDisallow: /outside"
    policy = RobotsPolicy(text)
    assert policy.truncated
    assert not policy.can_fetch("/private")
    assert policy.can_fetch("/outside")
    assert not RobotsPolicy(text[:MAX_ROBOTS_BYTES]).truncated


def test_byte_limit_does_not_mistake_unicode_characters_for_single_bytes():
    prefix = "User-agent: *\n#"
    policy = RobotsPolicy(prefix + "é" * (MAX_ROBOTS_BYTES // 2))
    assert policy.truncated


def test_many_wildcards_do_not_use_exponential_regex_backtracking():
    policy = RobotsPolicy("User-agent: *\nDisallow: /" + "a*" * 1000 + "z$")
    assert policy.can_fetch("/" + "a" * 2000)
