"""Subset validation never turns missing evidence into full SEO eligibility."""

import copy

import pytest

from seo_diagnostics import enrich_report


ORIGIN = "https://example.com"


def page(path="/en", **fields):
    return {"url": ORIGIN + path, "requested_url": ORIGIN + path, "state": "html", "status": 200,
            "hreflangs": [], "structured_data": [], "canonicals": [], **fields}


def links(*entries):
    return [{"language": code, "url": ORIGIN + path} for code, path in entries]


def audit(*pages):
    return enrich_report({"pages": list(pages), "issues": [], "summary": {}})


def rules(result):
    return {issue["rule_id"]: issue for issue in result["issues"]}


def test_valid_reciprocal_cluster_and_case_insensitive_languages():
    annotations = links(("EN-us", "/en"), ("de-DE", "/de"), ("x-default", "/en"))
    result = audit(page(hreflangs=annotations), page("/de", hreflangs=annotations))
    assert not result["issues"]
    assert result["diagnostics"]["hreflang"]["checked_pairs"] == 1


@pytest.mark.parametrize("language", ["en_US", "zz", "en-UK", "es-419", "EN-us-extra", "", "english"])
def test_invalid_language_or_unsupported_region(language):
    result = audit(page(hreflangs=links((language, "/en"))))
    assert "hreflang_invalid_language" in rules(result)


@pytest.mark.parametrize("language", ["ka", "uk", "zh-Hant", "zh-Hans-US", "x-default"])
def test_supported_language_forms(language):
    result = audit(page(hreflangs=links((language, "/en"))))
    assert not result["issues"]


def test_missing_self_return_and_conflicting_cluster_have_dependencies():
    result = audit(page(hreflangs=links(("en", "/different"), ("de", "/de"))),
                   page("/de", hreflangs=links(("en", "/other"), ("de", "/de"))))
    issues = rules(result)
    assert {"hreflang_missing_self", "hreflang_missing_return", "hreflang_cluster_conflict"} <= issues.keys()
    assert issues["hreflang_missing_return"]["evidence"][0]["related_urls"] == [ORIGIN + "/en", ORIGIN + "/de"]


def test_unobserved_cross_domain_alternate_is_unchecked_not_broken():
    result = audit(page(hreflangs=links(("en", "/en")) + [{"language": "de", "url": "https://example.de/"}]))
    assert not result["issues"]
    assert result["diagnostics"]["hreflang"]["unchecked_targets"][0]["reason"] == "not_observed"


def test_missing_target_annotations_not_inferred_to_be_empty():
    target = page("/de")
    del target["hreflangs"]
    result = audit(page(hreflangs=links(("en", "/en"), ("de", "/de"))), target)
    assert "hreflang_missing_return" not in rules(result)
    assert result["diagnostics"]["hreflang"]["unchecked_targets"]


def test_noncanonical_noindex_and_redirect_targets_reported():
    result = audit(page(hreflangs=links(("en", "/en"), ("de", "/old"))),
                   page("/de", requested_url=ORIGIN + "/old", noindex=True, canonicals=["/canonical"],
                        hreflangs=links(("en", "/en"), ("de", "/de"))))
    assert {"hreflang_unavailable_target", "hreflang_noncanonical_target", "hreflang_redirect_target"} <= rules(result).keys()


def test_relative_raw_url_preserved_as_invalid_even_if_resolved():
    result = audit(page(hreflangs=[{"language": "en", "url": ORIGIN + "/en", "raw_url": "/en"}]))
    assert "hreflang_invalid_url" in rules(result)


def test_duplicate_language_target_conflicts_but_duplicate_tags_do_not():
    annotations = links(("en", "/en"), ("en", "/en"))
    assert not audit(page(hreflangs=annotations))["issues"]
    result = audit(page(hreflangs=annotations + links(("en", "/other"))))
    assert "hreflang_conflicting_language" in rules(result)


def test_product_required_fields_and_zero_price():
    invalid = {"@type": "Product"}
    valid = {"@type": "Product", "name": "Free sample", "offers": {"@type": "Offer", "price": 0}}
    result = audit(page(structured_data=[invalid, valid]))
    issue = rules(result)["structured_product_required"]
    assert issue["evidence"][0]["fields"] == ["name", "offers OR review OR aggregateRating"]
    assert "structured_product_offer" not in rules(result)


@pytest.mark.parametrize("price", [True, "NaN", "Infinity", "-3", "free", None])
def test_invalid_price_is_not_treated_as_valid_numeric(price):
    result = audit(page(structured_data=[{"@type": "Product", "name": "X", "offers": {"@type": "Offer", "price": price}}]))
    assert "structured_product_offer" in rules(result)


def test_product_uses_nested_price_and_local_graph_reference():
    product = {"@type": "Product", "name": "X", "offers": {"@id": "#offer"}}
    offer = {"@id": "#offer", "@type": "Offer", "priceSpecification": {"price": "12.50"}}
    result = audit(page(structured_data=[{"@context": "https://schema.org", "@graph": [product, offer]}]))
    assert not result["issues"]


def test_split_local_identity_merges_without_false_missing_fields():
    result = audit(page(structured_data=[{"@graph": [
        {"@id": "#product", "@type": "Product", "name": "X"},
        {"@id": "#product", "offers": {"@type": "Offer", "price": 3}},
    ]}]))
    assert not result["issues"]
    assert result["diagnostics"]["structured_data"]["checked_entities"] == {"Product": 1}


def test_unresolved_breadcrumb_reference_is_unchecked():
    result = audit(page(structured_data=[{"@type": "BreadcrumbList", "itemListElement": [
        {"@id": "https://other.com/#crumb"},
        {"@type": "ListItem", "position": 2, "name": "Page"},
    ]}]))
    assert not result["issues"]
    assert result["diagnostics"]["structured_data"]["unresolved_reference_pages"] == [ORIGIN + "/en"]


def test_article_multitype_runs_profile_once():
    result = audit(page(structured_data=[{"@type": ["Article", "NewsArticle"]}]))
    assert len(rules(result)["structured_article_recommended"]["evidence"]) == 1


def test_conflicting_identity_values_are_unchecked():
    result = audit(page(structured_data=[{"@graph": [
        {"@id": "#product", "@type": "Product", "name": "A"},
        {"@id": "#product", "name": "B"},
    ]}]))
    assert not result["issues"]
    assert result["diagnostics"]["structured_data"]["conflicting_identity_pages"] == [ORIGIN + "/en"]


def test_invalid_json_ld_identifier_does_not_crash_validation():
    result = audit(page(structured_data=[{"@type": "BreadcrumbList", "itemListElement": [
        {"@id": []}, {"@type": "ListItem", "position": 2, "name": "End"},
    ]}]))
    assert "structured_breadcrumb_required" in rules(result)


def test_aggregate_offer_requires_currency_but_simple_offer_does_not():
    result = audit(page(structured_data=[{"@type": "Product", "name": "X", "offers": {"@type": "AggregateOffer", "lowPrice": "4.5"}}]))
    assert rules(result)["structured_product_offer"]["evidence"][0]["fields"] == ["offers.priceCurrency"]


def test_breadcrumb_last_item_url_optional_and_nested_name_supported():
    breadcrumb = {"@type": "BreadcrumbList", "itemListElement": [
        {"@type": "ListItem", "position": 1, "item": {"@id": ORIGIN, "name": "Home"}},
        {"@type": "ListItem", "position": 2, "name": "Page"},
    ]}
    assert not audit(page(structured_data=[breadcrumb]))["issues"]


def test_breadcrumb_bad_items_and_duplicate_positions():
    breadcrumb = {"@type": "BreadcrumbList", "itemListElement": [
        {"@type": "ListItem", "position": 1, "name": "Home", "item": "/relative"},
        {"@type": "ListItem", "position": 1},
    ]}
    result = audit(page(structured_data=[breadcrumb]))
    fields = rules(result)["structured_breadcrumb_required"]["evidence"][0]["fields"]
    assert "itemListElement[0].item" in fields
    assert "itemListElement[1].name" in fields
    assert "itemListElement.position (duplicate positions)" in fields


def test_single_breadcrumb_insufficient():
    result = audit(page(structured_data=[{"@type": "BreadcrumbList", "itemListElement": []}]))
    assert "structured_breadcrumb_required" in rules(result)


def test_article_only_recommends_fields_and_validates_dates():
    result = audit(page(structured_data=[{"@type": "NewsArticle", "datePublished": "2026-02-30"}]))
    issues = rules(result)
    assert issues["structured_article_recommended"]["severity"] == "low"
    assert "not requirements" in issues["structured_article_recommended"]["evidence"][0]["message"]
    assert "structured_article_date" in issues


def test_unsupported_context_is_unchecked_instead_of_false_missing_fields():
    result = audit(page(structured_data=[{"@context": {"@vocab": "https://schema.org/", "label": "name"}, "@type": "Product", "label": "X"}]))
    assert not result["issues"]
    assert result["diagnostics"]["structured_data"]["unsupported_context_pages"] == [ORIGIN + "/en"]


def test_unknown_schema_and_no_eligibility_claim():
    result = audit(page(structured_data=[{"@type": "JobPosting"}]))
    assert not result["issues"]
    diagnostics = result["diagnostics"]["structured_data"]
    assert diagnostics["unsupported_types"] == ["JobPosting"]
    assert any("eligibility" in item for item in diagnostics["not_checked"])


def test_failed_pages_are_not_checked():
    result = audit(page(status=500, structured_data=[{"@type": "Product"}]))
    assert not result["issues"]
    assert result["diagnostics"]["structured_data"]["checked_pages"] == 0


def test_enrichment_is_pure_idempotent_and_preserves_existing_findings():
    source = {"pages": [page(structured_data=[{"@type": "Product"}])],
              "issues": [{"rule_id": "invalid_json_ld", "severity": "high", "affected_urls": [ORIGIN], "evidence": []}]}
    before = copy.deepcopy(source)
    result = enrich_report(source)
    assert source == before
    assert enrich_report(result) == result
    assert "invalid_json_ld" in rules(result)
