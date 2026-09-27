"""Serializable, evidence-based crawl reports and conservative snapshot comparisons."""

from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
from urllib.parse import urljoin

from seo_diagnostics import enrich_report


SEVERITIES = {"critical": 0, "high": 1, "medium": 2, "low": 3}
LIMITATIONS = [
    "This is a bounded crawl sample, not a complete site inventory.",
    "Noindex and canonical signals do not prove Google's indexing status; use URL Inspection.",
    "Length, word-count and social metadata checks are review hints, not ranking requirements.",
    "Robots rules use the mcp-seo-audit user agent; Googlebot may receive different rules.",
    "Canonical target checks only cover targets observed in this crawl.",
    "International and JSON-LD diagnostics cover a documented subset; unchecked scope is listed in diagnostics.",
]


def _rendering_complete(page, settings):
    rendering = page.get("rendering")
    if not rendering:
        return settings.get("rendering", "raw") in {"raw", "raw_html"}
    return (rendering.get("status") == "complete" and not any(rendering.get(key) for key in
            ("error", "blocked_requests", "js_errors", "pending_requests", "resource_redirects")))

# Stable rule IDs allow comparisons even when counts or evidence text change.
RULES = [
    ("HTTP status", "http_error", "Restore the intended response or update links to a working replacement."),
    ("Fetch error:", "fetch_error", "Check availability, TLS and access restrictions, then rerun this URL."),
    ("Missing <title>", "missing_title", "Add a descriptive, unique title to the page head."),
    ("Missing meta description", "missing_description", "Write a useful page-specific summary for search snippets."),
    ("Missing H1", "missing_h1", "Add a clear heading that describes the page's main content."),
    ("Multiple H1", "multiple_h1", "Review the heading hierarchy; multiple H1s are not automatically an SEO error."),
    ("Page is explicitly marked noindex", "noindex", "Confirm exclusion is intentional before changing robots directives."),
    ("Page instructs crawlers not to follow", "nofollow", "Confirm the nofollow directive is intended for this page."),
    ("Multiple canonical", "multiple_canonicals", "Emit one consistent canonical URL."),
    ("No canonical", "missing_canonical", "Consider a canonical URL where duplicates or URL variants exist."),
    ("Canonical", "canonical_review", "Check the canonical destination and use a consistent absolute indexable URL."),
    ("Invalid JSON-LD", "invalid_json_ld", "Repair the JSON syntax and validate supported structured data separately."),
    ("Images missing alt", "missing_image_alt", "Add descriptive alt text to meaningful images; use empty alt for decoration."),
    ("Images missing width", "image_dimensions", "Reserve image space using dimensions or CSS aspect-ratio to reduce layout shifts."),
    ("Anchors without href", "uncrawlable_link", "Use a real href for navigation or a button for actions."),
    ("Links with empty anchor", "empty_anchor", "Give the link an accessible name, including appropriate image alt text."),
    ("No visible body", "empty_body", "Check the server response and rendered page for missing main content."),
    ("Thin visible content", "short_body", "Review whether this page answers its intended task; there is no minimum SEO word count."),
    ("Missing viewport", "missing_viewport", "Add a mobile viewport declaration and test responsive rendering."),
    ("Missing html lang", "missing_language", "Declare the document language for assistive technologies."),
    ("Title is", "title_length", "Review title clarity and search appearance; character counts are only a heuristic."),
    ("Meta description is", "description_length", "Review snippet usefulness; Google may choose a different snippet."),
    ("Missing og:", "open_graph", "Add relevant Open Graph metadata if social sharing matters."),
    ("Missing twitter:", "twitter_card", "Add card metadata if previews on X matter."),
    ("Meta keywords", "meta_keywords", "Remove obsolete keywords metadata when convenient; Google ignores it."),
    ("Page limits search result", "snippet_limit", "Confirm the snippet restriction is intentional."),
]


def build_audit_report(crawl):
    """Build an action plan from bounded crawl evidence; no network or file writes."""
    grouped = {}

    def add(rule, severity, message, url, recommendation, evidence=None):
        item = grouped.setdefault(rule, {
            "rule_id": rule, "severity": severity, "recommendation": recommendation,
            "affected_urls": [], "evidence": [],
        })
        if SEVERITIES[severity] < SEVERITIES[item["severity"]]:
            item["severity"] = severity
        if url not in item["affected_urls"]:
            item["affected_urls"].append(url)
        item["evidence"].append({"url": url, "message": message, **(evidence or {})})

    pages = crawl["pages"]
    settings = crawl["settings"]
    inventory = crawl.get("sitemap_inventory")
    rendering_incomplete = []
    if crawl["robots"]["state"] == "unavailable":
        add("robots_unavailable", "high", "Crawl deferred because robots.txt could not be checked safely.",
            crawl["start_url"], "Restore robots.txt availability or resolve the reported fetch error, then rerun.")
    if crawl["coverage"]["termination_reason"] == "crawl_delay_exceeds_budget":
        add("crawl_deferred", "low", "Requested crawl-delay exceeds this interactive tool's 10-second limit.",
            crawl["start_url"], "Use a crawler that supports the requested delay, or review your own site's policy.")
    if settings.get("include_sitemaps") and (inventory is None or not inventory.get("coverage", {}).get("complete_for_scope")):
        add("sitemap_inventory_incomplete", "low", "Sitemap inventory was not fully checked within this run's scope and budgets.",
            crawl["start_url"], "Review sitemap outcomes, skipped URLs and budget limits before interpreting inventory gaps.",
            {"coverage": inventory.get("coverage", {}) if inventory else {}})
    for page in pages:
        url = page["url"]
        if page["state"] == "fetch_error":
            add("fetch_error", "high", page["issues"][0], url,
                "Check availability, TLS and access restrictions, then rerun this URL.")
        if isinstance(page["status"], int) and page["status"] >= 400:
            add("http_error", "critical", f"HTTP status {page['status']}", url,
                "Restore the intended response or update links to a working replacement.",
                {"linked_from": page.get("linked_from", [])})
        if page["state"] != "html" or page["status"] >= 400:
            continue
        if not _rendering_complete(page, settings):
            rendering_incomplete.append(url)
            add("rendering_incomplete", "medium", "The requested rendered-page observation is incomplete.", url,
                "Review blocked or pending requests, JavaScript errors and rendering limits, then rerun.",
                {"rendering": page.get("rendering")})
        rendering = page.get("rendering") or {}
        raw, rendered = rendering.get("raw_signals"), rendering.get("rendered_signals")
        if isinstance(raw, dict) and isinstance(rendered, dict) and not rendering.get("error"):
            changed = sorted(key for key in raw.keys() & rendered.keys() if raw[key] != rendered[key])
            if changed:
                add("rendering_signal_difference", "low", "JavaScript changed observed SEO signals; this may be intentional.", url,
                    "Review whether important content and metadata should also be available in the server response.",
                    {"changed_fields": changed, "raw_signals": raw, "rendered_signals": rendered})
        if page.get("sitemap_only_candidate") and not page.get("noindex"):
            add("sitemap_unlinked_candidate", "low", "This sitemap URL has no incoming link in the crawled sample.", url,
                "Check the site's navigation and broader link inventory before treating this as an orphan page.",
                {"sitemap_sources": (inventory or {}).get("sources", {}).get(page["requested_url"], []),
                 "scope": "sampled_internal_links", "proven_orphan": False})
        for severity, message in page["findings"]:
            if message.startswith("HTTP status"):
                continue
            if page.get("noindex") and not page.get("is_start_page"):
                # Excluded utility pages are inventory, not missing-metadata action items.
                if message != "Page is explicitly marked noindex":
                    continue
                severity = "low"
            match = next((rule for rule in RULES if message.startswith(rule[0])), None)
            if match:
                _, rule, recommendation = match
            elif message.endswith("elements use data-nosnippet"):
                rule = "data_nosnippet"
                recommendation = "Confirm these elements should be excluded from search snippets."
            else:
                rule = "review_" + hashlib.sha256(message.encode()).hexdigest()[:12]
                recommendation = "Review the captured evidence in the context of this page's purpose."
            add(rule, severity, message, url, recommendation)

    candidates = [p for p in pages if p["state"] == "html" and 200 <= p["status"] < 300
                  and not p.get("noindex") and _rendering_complete(p, settings)]
    for key, rule in (("title", "duplicate_title"), ("description", "duplicate_description")):
        duplicates = defaultdict(list)
        for page in candidates:
            if page.get(key):
                duplicates[page[key]].append(page["url"])
        for value, urls in duplicates.items():
            if len(urls) > 1:
                for url in urls:
                    add(rule, "medium", f"Shared by {len(urls)} crawled pages: {value}", url,
                        "Differentiate metadata for distinct pages, or consolidate true duplicates.",
                        {"shared_with": [other for other in urls if other != url]})

    observed = {p["requested_url"]: p for p in pages if p["state"] != "redirect_alias"}
    observed.update({p["url"]: p for p in pages if p["state"] != "redirect_alias"})
    for page in candidates:
        for canonical in page.get("canonicals", []):
            target_url = urljoin(page["url"], canonical).split("#", 1)[0]
            target = observed.get(target_url)
            if target and (target.get("noindex") or (isinstance(target["status"], int) and target["status"] >= 400)):
                add("canonical_unavailable", "high", f"Canonical target is noindex or HTTP error: {target_url}",
                    page["url"], "Point canonical to an available, indexable version of this content.",
                    {"target": target_url, "target_status": target["status"], "target_noindex": target.get("noindex", False)})

    issues = sorted(grouped.values(), key=lambda i: (SEVERITIES[i["severity"]], -len(i["affected_urls"]), i["rule_id"]))
    for item in issues:
        item["affected_urls"].sort()
        item["affected_count"] = len(item["affected_urls"])
    coverage = {**crawl["coverage"], "rendering_incomplete": sorted(set(rendering_incomplete) | set(crawl["coverage"].get("rendering_incomplete", [])))}
    complete = coverage["complete_for_discovered_links"] and not coverage["rendering_incomplete"]
    if settings.get("include_sitemaps"):
        complete = complete and inventory is not None and inventory.get("coverage", {}).get("complete_for_scope", False)
    coverage["complete_for_discovered_links"] = bool(complete)
    limitations = list(LIMITATIONS)
    if settings.get("rendering", "raw") in {"raw", "raw_html"}:
        limitations.append("JavaScript is not rendered in this raw-HTML run.")
    else:
        limitations.append("Rendering uses bounded browser time/resources and may differ from Googlebot; partial renders cannot verify fixes.")
    if inventory is None:
        limitations.append("Sitemaps were not inventoried; sitemap-only pages may be absent.")
    else:
        limitations.extend(inventory.get("limitations", []))
    report = {
        "schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
        "audit_status": "complete" if complete else "partial",
        "start_url": crawl["start_url"], "origin": crawl["origin"], "settings": crawl["settings"],
        "coverage": coverage, "robots": crawl["robots"], "limitations": limitations,
        "summary": {"issue_groups": len(issues), "groups_by_severity": dict(Counter(i["severity"] for i in issues))},
        "issues": issues, "pages": pages, "sitemap_inventory": inventory,
    }
    return enrich_report(report)


def compare_audit_reports(baseline, current):
    """Only call findings resolved when their pages were successfully observed again."""
    for report in (baseline, current):
        if not isinstance(report, dict) or report.get("schema_version") != "1.0":
            raise ValueError("Both inputs must be schema_version 1.0 SEO audit reports.")
        if not isinstance(report.get("pages"), list) or not isinstance(report.get("issues"), list):
            raise ValueError("Reports must include pages and issues arrays.")
        if not isinstance(report.get("settings"), dict) or not report.get("origin"):
            raise ValueError("Reports must include origin and crawl settings.")
    if baseline["origin"] != current["origin"] or baseline.get("start_url") != current.get("start_url"):
        raise ValueError("Compare reports for the same origin and start URL.")
    def comparable_settings(report):
        settings = dict(report["settings"])
        settings.setdefault("rendering", "raw")
        if settings["rendering"] == "raw_html":
            settings["rendering"] = "raw"
        settings.setdefault("include_sitemaps", False)
        settings.setdefault("max_seconds", 180)
        return settings

    if comparable_settings(baseline) != comparable_settings(current):
        raise ValueError("Crawl settings differ. Rerun with matching settings before comparing.")

    def flatten(report):
        findings = {}
        dependencies = {}
        for issue in report["issues"]:
            if not isinstance(issue, dict) or not isinstance(issue.get("rule_id"), str) or not isinstance(issue.get("affected_urls"), list):
                raise ValueError("Each issue needs a rule_id and affected_urls array.")
            evidence = issue.get("evidence", [])
            if not isinstance(evidence, list) or any(not isinstance(item, dict) for item in evidence):
                raise ValueError("Issue evidence must be an array of objects.")
            for url in issue["affected_urls"]:
                if not isinstance(url, str):
                    raise ValueError("Affected URLs must be strings.")
                rule = issue["rule_id"]
                key = (rule, url)
                findings[key] = {"rule_id": rule, "url": url, "severity": issue.get("severity")}
                related = {url}
                relevant = [item for item in evidence if item.get("url") == url]
                links = [item["related_urls"] for item in relevant if "related_urls" in item]
                if any(not isinstance(group, list) or any(not isinstance(target, str) for target in group) for group in links):
                    raise ValueError("Related-page evidence must contain URL arrays.")
                related.update(target for group in links for target in group)
                if rule in {"hreflang_missing_return", "hreflang_unavailable_target", "hreflang_noncanonical_target", "hreflang_redirect_target", "hreflang_cluster_conflict", "hreflang_conflicting_language"} and not links:
                    dependencies[key] = None
                    continue
                if rule.startswith("duplicate_"):
                    peers = [item.get("shared_with") for item in relevant if "shared_with" in item]
                    if any(not isinstance(group, list) or any(not isinstance(peer, str) for peer in group) for group in peers):
                        raise ValueError("Duplicate shared_with evidence must contain URL arrays.")
                    # Older/minimal snapshots may omit evidence; all affected URLs
                    # are a conservative fallback, never proof of a smaller group.
                    related.update(peer for group in peers for peer in group)
                    if not peers:
                        if any(not isinstance(peer, str) for peer in issue["affected_urls"]):
                            raise ValueError("Affected URLs must be strings.")
                        related.update(issue["affected_urls"])
                elif rule == "canonical_unavailable":
                    targets = [item.get("target") for item in relevant]
                    if not targets or any(not isinstance(target, str) for target in targets):
                        dependencies[key] = None
                        continue
                    related.update(targets)
                dependencies[key] = related
        return findings, dependencies

    def observed(report):
        result = {}
        for page in report["pages"]:
            if not isinstance(page, dict) or not isinstance(page.get("url"), str):
                raise ValueError("Each page needs a URL.")
            if page.get("state") == "html" and isinstance(page.get("status"), int) and 200 <= page["status"] < 300:
                result[page["url"]] = page
                if isinstance(page.get("requested_url"), str):
                    result[page["requested_url"]] = page
        return result

    def checked(rule, urls, pages, report, resolving=False):
        if urls is None:
            return False
        if rule == "sitemap_inventory_incomplete":
            return bool((report.get("sitemap_inventory") or {}).get("coverage", {}).get("complete_for_scope"))
        structured = report.get("diagnostics", {}).get("structured_data", {})
        unchecked_structured = set(url for key in ("unsupported_context_pages", "truncated_pages", "unresolved_reference_pages", "conflicting_identity_pages") for url in structured.get(key, []))
        for url in urls:
            page = pages.get(url)
            if page is None:
                return False
            if not _rendering_complete(page, report["settings"]):
                return False
            if rule.startswith("hreflang_") and "hreflangs" not in page:
                return False
            if rule.startswith("structured_") and ("structured_data" not in page or page["url"] in unchecked_structured):
                return False
            if rule == "rendering_signal_difference" and not all(key in (page.get("rendering") or {}) for key in ("raw_signals", "rendered_signals")):
                return False
            if rule == "sitemap_unlinked_candidate":
                # An observed incoming link establishes a sample-level repair.
                # Its absence in another bounded sample cannot prove regression.
                if not resolving or not page.get("linked_from"):
                    return False
            if page.get("noindex") and rule not in {"http_error", "fetch_error", "noindex"}:
                # Noindex pages are excluded from duplicate/canonical evaluation;
                # ordinary page hints are also suppressed except on the start URL.
                if rule.startswith("duplicate_") or rule == "canonical_unavailable" or not page.get("is_start_page"):
                    return False
        return True

    before, before_dependencies = flatten(baseline)
    after, after_dependencies = flatten(current)
    before_seen, after_seen = observed(baseline), observed(current)
    result = {"new": [], "resolved": [], "persistent": [], "unverified": [], "newly_observed": []}
    for key in sorted(before.keys() | after.keys()):
        if key in before and key in after:
            result["persistent"].append(after[key])
        elif key in before:
            bucket = "resolved" if checked(key[0], before_dependencies[key], after_seen, current, resolving=True) else "unverified"
            result[bucket].append(before[key])
        else:
            bucket = "new" if checked(key[0], after_dependencies[key], before_seen, baseline) else "newly_observed"
            result[bucket].append(after[key])
    return {
        "schema_version": "1.0", "origin": current["origin"],
        "baseline_at": baseline.get("generated_at"), "current_at": current.get("generated_at"),
        "counts": {name: len(items) for name, items in result.items()}, **result,
        "limitations": [
            "Unverified findings lack a successful applicable recheck of the page, duplicate peers, or canonical target.",
            "Newly observed findings lack applicable baseline checks for the page or its dependencies; they are not proven regressions.",
            "Comparisons cover only the captured HTML sample, not the whole site or Google's index.",
            "Incomplete rendering and unchecked diagnostic profiles cannot establish a verified fix.",
        ],
    }
