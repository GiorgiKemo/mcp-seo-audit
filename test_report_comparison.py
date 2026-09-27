"""Comparisons distinguish changed findings from changed crawl coverage."""

from seo_reporting import build_audit_report, compare_audit_reports


def page(path, **updates):
    result = {
        "url": "https://example.com" + path,
        "requested_url": "https://example.com" + path,
        "state": "html", "status": 200, "findings": [], "issues": [],
        "title": "Unique " + path, "description": "", "canonicals": [], "noindex": False,
    }
    result.update(updates)
    return result


def report(pages):
    return build_audit_report({
        "pages": pages, "start_url": "https://example.com/", "origin": "https://example.com",
        "settings": {}, "coverage": {"termination_reason": "frontier_exhausted", "complete_for_discovered_links": True},
        "robots": {"state": "loaded"},
    })


def finding_urls(comparison, bucket, rule):
    return {item["url"] for item in comparison[bucket] if item["rule_id"] == rule}


def test_canonical_issue_requires_target_to_be_checked_again():
    source = page("/source", canonicals=["/target"])
    baseline = report([source, page("/target", status=404)])
    comparison = compare_audit_reports(baseline, report([source]))
    assert not finding_urls(comparison, "resolved", "canonical_unavailable")
    assert finding_urls(comparison, "unverified", "canonical_unavailable") == {source["url"]}


def test_canonical_issue_resolves_after_source_and_target_succeed():
    source = page("/source", canonicals=["/target"])
    baseline = report([source, page("/target", status=404)])
    comparison = compare_audit_reports(baseline, report([source, page("/target")]))
    assert finding_urls(comparison, "resolved", "canonical_unavailable") == {source["url"]}


def test_canonical_target_redirect_recheck_counts_for_requested_url():
    source = page("/source", canonicals=["/target"])
    baseline = report([source, page("/target", status=404)])
    target = page("/replacement", requested_url="https://example.com/target")
    comparison = compare_audit_reports(baseline, report([source, target]))
    assert finding_urls(comparison, "resolved", "canonical_unavailable") == {source["url"]}


def test_newly_discovered_canonical_failure_is_not_proven_regression():
    source = page("/source", canonicals=["/target"])
    comparison = compare_audit_reports(report([source]), report([source, page("/target", status=404)]))
    assert not finding_urls(comparison, "new", "canonical_unavailable")
    assert finding_urls(comparison, "newly_observed", "canonical_unavailable") == {source["url"]}


def test_newly_sampled_duplicate_peer_does_not_establish_regression():
    first = page("/a", title="Shared")
    comparison = compare_audit_reports(report([first]), report([first, page("/b", title="Shared")]))
    assert not finding_urls(comparison, "new", "duplicate_title")
    assert finding_urls(comparison, "newly_observed", "duplicate_title") == {
        "https://example.com/a", "https://example.com/b",
    }


def test_new_duplicate_is_regression_when_all_peers_had_baseline_checks():
    baseline = report([page("/a", title="First"), page("/b", title="Second")])
    current = report([page("/a", title="Shared"), page("/b", title="Shared")])
    comparison = compare_audit_reports(baseline, current)
    assert finding_urls(comparison, "new", "duplicate_title") == {
        "https://example.com/a", "https://example.com/b",
    }


def test_duplicate_resolution_depends_only_on_its_actual_peer_group():
    baseline = report([page("/a", title="One"), page("/b", title="One"),
                       page("/c", title="Two"), page("/d", title="Two")])
    comparison = compare_audit_reports(baseline, report([page("/a"), page("/b")]))
    assert finding_urls(comparison, "resolved", "duplicate_title") == {
        "https://example.com/a", "https://example.com/b",
    }
    assert finding_urls(comparison, "unverified", "duplicate_title") == {
        "https://example.com/c", "https://example.com/d",
    }


def test_missing_duplicate_peer_leaves_issue_unverified():
    first, second = page("/a", title="Shared"), page("/b", title="Shared")
    comparison = compare_audit_reports(report([first, second]), report([first]))
    assert not finding_urls(comparison, "resolved", "duplicate_title")
    assert len(finding_urls(comparison, "unverified", "duplicate_title")) == 2


def test_noindex_suppression_is_not_proof_of_metadata_fix():
    baseline = report([page("/a", title="", findings=[("high", "Missing <title>")])])
    current = report([page("/a", title="", noindex=True, findings=[
        ("high", "Missing <title>"), ("high", "Page is explicitly marked noindex"),
    ])])
    comparison = compare_audit_reports(baseline, current)
    assert not finding_urls(comparison, "resolved", "missing_title")
    assert finding_urls(comparison, "unverified", "missing_title") == {"https://example.com/a"}
    reverse = compare_audit_reports(current, baseline)
    assert not finding_urls(reverse, "new", "missing_title")
    assert finding_urls(reverse, "newly_observed", "missing_title") == {"https://example.com/a"}


def test_noindex_does_not_prevent_http_error_resolution():
    baseline = report([page("/a", status=404)])
    current = report([page("/a", noindex=True)])
    comparison = compare_audit_reports(baseline, current)
    assert finding_urls(comparison, "resolved", "http_error") == {"https://example.com/a"}


def test_incomplete_canonical_evidence_cannot_establish_resolution():
    source = page("/source", canonicals=["/target"])
    baseline = report([source, page("/target", status=404)])
    for issue in baseline["issues"]:
        if issue["rule_id"] == "canonical_unavailable":
            issue.pop("evidence")
    comparison = compare_audit_reports(baseline, report([source, page("/target")]))
    assert not finding_urls(comparison, "resolved", "canonical_unavailable")
