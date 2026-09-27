"""Rendering, sitemap and diagnostic evidence remain conservative on comparison."""

from copy import deepcopy

import pytest

from seo_reporting import build_audit_report, compare_audit_reports


ORIGIN = "https://example.com"


def page(path="/", **updates):
    value = {"url": ORIGIN + path, "requested_url": ORIGIN + path, "state": "html", "status": 200,
             "issues": [], "findings": [], "title": path, "description": "", "canonicals": [],
             "hreflangs": [], "structured_data": []}
    value.update(updates)
    return value


def report(pages, settings=None, inventory=None):
    return build_audit_report({"pages": pages, "start_url": ORIGIN + "/", "origin": ORIGIN,
                               "settings": settings or {}, "robots": {"state": "loaded"},
                               "coverage": {"termination_reason": "frontier_exhausted", "complete_for_discovered_links": True},
                               "sitemap_inventory": inventory})


def rules(snapshot):
    return {item["rule_id"]: item for item in snapshot["issues"]}


def bucket(comparison, name, rule):
    return [item for item in comparison[name] if item["rule_id"] == rule]


def annotations(*entries):
    return [{"language": code, "url": ORIGIN + path} for code, path in entries]


def test_build_calls_diagnostics_and_keeps_profile_coverage():
    result = report([page(structured_data=[{"@type": "Product"}])])
    assert "structured_product_required" in rules(result)
    assert result["diagnostics"]["structured_data"]["checked_entities"] == {"Product": 1}


def test_rendered_limitations_do_not_claim_javascript_was_not_rendered():
    result = report([page(rendering={"status": "complete"})], {"rendering": "rendered"})
    assert result["audit_status"] == "complete"
    assert not any("not rendered" in item for item in result["limitations"])
    assert any("bounded browser" in item for item in result["limitations"])


@pytest.mark.parametrize("rendering", [{"status": "partial"}, {"status": "complete", "pending_requests": 2},
                                      {"status": "complete", "resource_redirects": ["/script"]},
                                      {"status": "complete", "js_errors": ["error"]}, None])
def test_incomplete_rendering_overrides_overoptimistic_crawl_completion(rendering):
    result = report([page(rendering=rendering, structured_data=[{"@type": "Product"}])], {"rendering": "compare"})
    assert result["audit_status"] == "partial"
    assert not result["coverage"]["complete_for_discovered_links"]
    assert result["coverage"]["rendering_incomplete"] == [ORIGIN + "/"]
    assert "rendering_incomplete" in rules(result)
    assert "structured_product_required" not in rules(result)


def test_raw_rendered_differences_have_exact_field_evidence():
    rendering = {"status": "complete", "raw_signals": {"title": "", "body_word_count": 0},
                 "rendered_signals": {"title": "Rendered", "body_word_count": 500}}
    result = report([page(rendering=rendering)], {"rendering": "compare"})
    issue = rules(result)["rendering_signal_difference"]
    assert issue["severity"] == "low"
    assert issue["evidence"][0]["changed_fields"] == ["body_word_count", "title"]


def test_sitemap_only_pages_are_candidates_and_inventory_is_retained():
    inventory = {"known_urls": [ORIGIN + "/other"], "coverage": {"complete_for_scope": True},
                 "sources": {ORIGIN + "/other": [ORIGIN + "/sitemap.xml"]}, "limitations": ["Sitemap limitation"]}
    result = report([page("/other", sitemap_only_candidate=True)], {"include_sitemaps": True}, inventory)
    issue = rules(result)["sitemap_unlinked_candidate"]
    assert not issue["evidence"][0]["proven_orphan"]
    assert issue["evidence"][0]["sitemap_sources"] == [ORIGIN + "/sitemap.xml"]
    assert result["sitemap_inventory"] == inventory
    assert "Sitemap limitation" in result["limitations"]


def test_incomplete_sitemap_inventory_is_reported_even_if_links_complete():
    result = report([page()], {"include_sitemaps": True}, {"coverage": {"complete_for_scope": False}})
    assert result["audit_status"] == "partial"
    assert "sitemap_inventory_incomplete" in rules(result)


def test_partial_render_cannot_resolve_missing_title():
    settings = {"rendering": "compare"}
    before = report([page(rendering={"status": "complete"}, findings=[("high", "Missing <title>")])], settings)
    after = report([page(rendering={"status": "partial"})], settings)
    comparison = compare_audit_reports(before, after)
    assert not bucket(comparison, "resolved", "missing_title")
    assert bucket(comparison, "unverified", "missing_title")


def test_rendering_incomplete_resolves_after_complete_observation():
    settings = {"rendering": "compare"}
    before = report([page(rendering={"status": "partial"})], settings)
    after = report([page(rendering={"status": "complete"})], settings)
    assert bucket(compare_audit_reports(before, after), "resolved", "rendering_incomplete")


def test_missing_hreflang_target_cannot_resolve_return_link_issue():
    source = page("/en", hreflangs=annotations(("en", "/en"), ("de", "/de")))
    before = report([source, page("/de", hreflangs=annotations(("de", "/de")))])
    after = report([source])
    comparison = compare_audit_reports(before, after)
    assert bucket(comparison, "unverified", "hreflang_missing_return")
    assert not bucket(comparison, "resolved", "hreflang_missing_return")


def test_observed_return_link_repair_resolves_hreflang_issue():
    source = page("/en", hreflangs=annotations(("en", "/en"), ("de", "/de")))
    before = report([source, page("/de", hreflangs=annotations(("de", "/de")))])
    after = report([source, page("/de", hreflangs=source["hreflangs"])])
    assert bucket(compare_audit_reports(before, after), "resolved", "hreflang_missing_return")


def test_missing_dependency_evidence_keeps_hreflang_unverified():
    source = page("/en", hreflangs=annotations(("en", "/en"), ("de", "/de")))
    before = report([source, page("/de", hreflangs=annotations(("de", "/de")))])
    rules(before)["hreflang_missing_return"]["evidence"] = []
    after = report([source, page("/de", hreflangs=source["hreflangs"])])
    assert bucket(compare_audit_reports(before, after), "unverified", "hreflang_missing_return")


def test_unchecked_structured_context_cannot_resolve_required_fields():
    before = report([page(structured_data=[{"@type": "Product"}])])
    after = report([page(structured_data=[{"@context": {"alias": "name"}, "@type": "Product"}])])
    comparison = compare_audit_reports(before, after)
    assert bucket(comparison, "unverified", "structured_product_required")
    assert not bucket(comparison, "resolved", "structured_product_required")


def test_missing_diagnostic_fields_are_not_successful_rechecks():
    before = report([page(structured_data=[{"@type": "Product"}])])
    current_page = page()
    del current_page["structured_data"]
    after = report([current_page])
    assert bucket(compare_audit_reports(before, after), "unverified", "structured_product_required")


def test_sitemap_candidate_requires_observed_incoming_link_to_resolve():
    before = report([page("/other", sitemap_only_candidate=True)])
    after = report([page("/other", sitemap_only_candidate=False)])
    assert bucket(compare_audit_reports(before, after), "unverified", "sitemap_unlinked_candidate")
    linked = report([page("/other", sitemap_only_candidate=False, linked_from=[ORIGIN + "/"])])
    assert bucket(compare_audit_reports(before, linked), "resolved", "sitemap_unlinked_candidate")
    assert bucket(compare_audit_reports(linked, before), "newly_observed", "sitemap_unlinked_candidate")


def test_legacy_raw_html_and_new_raw_default_settings_are_comparable():
    before = report([page()], {"rendering": "raw_html"})
    after = report([page()], {"rendering": "raw", "include_sitemaps": False, "max_seconds": 180})
    assert not any(compare_audit_reports(before, after)["counts"].values())


def test_build_does_not_mutate_crawl_coverage_or_pages():
    pages = [page(rendering={"status": "partial"})]
    original = deepcopy(pages)
    report(pages, {"rendering": "compare"})
    assert pages == original
