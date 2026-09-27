"""Observed-page international SEO and a documented subset of JSON-LD checks.

No network requests or remote JSON-LD contexts are evaluated. A clean result is
not proof of Google rich-result eligibility. Rules checked against Google Search
Central on 2026-09-27; see SOURCES and each profile's explicit unchecked scope.
"""

from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from decimal import Decimal, InvalidOperation
import re
from urllib.parse import urljoin, urlsplit, urlunsplit


SOURCES = {
    "hreflang": "https://developers.google.com/search/docs/specialty/international/localized-versions",
    "Product": "https://developers.google.com/search/docs/appearance/structured-data/product-snippet",
    "BreadcrumbList": "https://developers.google.com/search/docs/appearance/structured-data/breadcrumb",
    "Article": "https://developers.google.com/search/docs/appearance/structured-data/article",
}
SEVERITY = {"critical": 0, "high": 1, "medium": 2, "low": 3}
MAX_JSON_NODES = 5000
# ISO 639-1 and ISO 3166-1 alpha-2. Script subtags receive syntax checks only.
LANGUAGES = frozenset("aa ab ae af ak am an ar as av ay az ba be bg bh bi bm bn bo br bs ca ce ch co cr cs cu cv cy da de dv dz ee el en eo es et eu fa ff fi fj fo fr fy ga gd gl gn gu gv ha he hi ho hr ht hu hy hz ia id ie ig ii ik io is it iu ja jv ka kg ki kj kk kl km kn ko kr ks ku kv kw ky la lb lg li ln lo lt lu lv mg mh mi mk ml mn mr ms mt my na nb nd ne ng nl nn no nr nv ny oc oj om or os pa pi pl ps pt qu rm rn ro ru rw sa sc sd se sg si sk sl sm sn so sq sr ss st su sv sw ta te tg th ti tk tl tn to tr ts tt tw ty ug uk ur uz ve vi vo wa wo xh yi yo za zh zu".split())
REGIONS = frozenset("ad ae af ag ai al am ao aq ar as at au aw ax az ba bb bd be bf bg bh bi bj bl bm bn bo bq br bs bt bv bw by bz ca cc cd cf cg ch ci ck cl cm cn co cr cu cv cw cx cy cz de dj dk dm do dz ec ee eg eh er es et fi fj fk fm fo fr ga gb gd ge gf gg gh gi gl gm gn gp gq gr gs gt gu gw gy hk hm hn hr ht hu id ie il im in io iq ir is it je jm jo jp ke kg kh ki km kn kp kr kw ky kz la lb lc li lk lr ls lt lu lv ly ma mc md me mf mg mh mk ml mm mn mo mp mq mr ms mt mu mv mw mx my mz na nc ne nf ng ni nl no np nr nu nz om pa pe pf pg ph pk pl pm pn pr ps pt pw py qa re ro rs ru rw sa sb sc sd se sg sh si sj sk sl sm sn so sr ss st sv sx sy sz tc td tf tg th tj tk tl tm tn to tr tt tv tw tz ua ug um us uy uz va vc ve vg vi vn vu wf ws ye yt za zm zw".split())


def _url(value, base=None):
    if not isinstance(value, str) or any(c.isspace() for c in value) or "\\" in value:
        return None
    try:
        parsed = urlsplit(urljoin(base, value) if base else value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            return None
        host = parsed.hostname.lower().encode("idna").decode("ascii")
        if ":" in host:
            host = f"[{host}]"
        if parsed.port is not None and parsed.port != (443 if parsed.scheme == "https" else 80):
            host += f":{parsed.port}"
        return urlunsplit((parsed.scheme, host, parsed.path or "/", parsed.query, ""))
    except (ValueError, UnicodeError):
        return None


def _language_valid(language):
    if language == "x-default":
        return True
    if not re.fullmatch(r"[a-z]{2}(?:-[a-z]{4})?(?:-[a-z]{2})?", language):
        return False
    parts = language.split("-")
    return parts[0] in LANGUAGES and (len(parts) == 1 or len(parts[-1]) != 2 or parts[-1] in REGIONS)


def _successful(page):
    return page.get("state") == "html" and isinstance(page.get("status"), int) and 200 <= page["status"] < 300


def _present(value):
    return value is not None and value != "" and value != [] and value != {}


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _numeric(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return False
    try:
        result = Decimal(str(value))
        return result.is_finite() and result >= 0
    except InvalidOperation:
        return False


def _types(node):
    raw = node.get("@type", [])
    return {t.removeprefix("https://schema.org/").removeprefix("http://schema.org/")
            for t in (raw if isinstance(raw, list) else [raw]) if isinstance(t, str)}


def _nodes(documents):
    """Iterative bounded walk; no recursive stack growth for hostile JSON."""
    pending, nodes, examined = list(reversed(documents)), [], 0
    while pending and examined < MAX_JSON_NODES:
        node = pending.pop()
        examined += 1
        if isinstance(node, dict):
            nodes.append(node)
            pending.extend(reversed([value for key, value in node.items() if key != "@context"]))
        elif isinstance(node, list):
            pending.extend(reversed(node))
    return nodes, bool(pending)


def enrich_report(report):
    """Return an enriched copy of a report, leaving the source snapshot untouched.

    Pages may provide ``hreflangs=[{language, url, raw_url?}]`` and
    ``structured_data=[parsed JSON-LD documents]``. Missing fields are unchecked,
    not equivalent to a verified empty collection. Existing JSON syntax findings
    are preserved. Related-page evidence uses ``related_urls`` for comparisons.
    """
    result = deepcopy(report)
    existing = result.setdefault("issues", [])
    grouped = {}

    def add(rule, severity, url, message, recommendation, **evidence):
        group = grouped.setdefault(rule, {"rule_id": rule, "severity": severity,
                                          "recommendation": recommendation, "affected_urls": [], "evidence": []})
        if SEVERITY[severity] < SEVERITY[group["severity"]]:
            group["severity"] = severity
        if url not in group["affected_urls"]:
            group["affected_urls"].append(url)
        group["evidence"].append({"url": url, "message": message, **evidence})

    pages = result.get("pages", [])
    def eligible(page):
        rendering = page.get("rendering")
        if result.get("settings", {}).get("rendering", "raw") not in {"raw", "raw_html"} and not rendering:
            return False
        if rendering and (rendering.get("status") != "complete" or any(rendering.get(key) for key in
                ("error", "blocked_requests", "js_errors", "pending_requests", "resource_redirects"))):
            return False
        return _successful(page)

    observed = {}
    for page in pages:
        if page.get("state") != "redirect_alias":
            for value in (page.get("url"), page.get("requested_url")):
                if normalized := _url(value):
                    observed[normalized] = page
    valid_links = {}
    unchecked_targets = []
    hreflang_pages = 0
    for page in pages:
        if not eligible(page) or "hreflangs" not in page:
            continue
        hreflang_pages += 1
        url = page["url"]
        links = []
        by_language = defaultdict(set)
        for entry in page["hreflangs"]:
            if not isinstance(entry, dict):
                continue
            language = str(entry.get("language", "")).strip().lower()
            target = _url(entry.get("url"))
            if not _language_valid(language):
                add("hreflang_invalid_language", "medium", url, f"Unsupported language/region syntax: {language}",
                    "Use an ISO 639-1 language, an optional script/ISO 3166-1 region, or x-default.", language=language)
            if target is None or ("raw_url" in entry and _url(entry["raw_url"]) is None):
                add("hreflang_invalid_url", "medium", url, "Alternate URL is not a fully qualified HTTP(S) URL.",
                    "Use a fully qualified alternate URL with the scheme and hostname.", language=language)
            if target is None or not _language_valid(language):
                continue
            by_language[language].add(target)
            if (language, target) not in links:
                links.append((language, target))
        valid_links[url] = links
        for language, targets in by_language.items():
            if len(targets) > 1:
                add("hreflang_conflicting_language", "medium", url, f"Multiple destinations for {language}.",
                    "Use one consistent URL for each language/region in this alternate set.", language=language,
                    targets=sorted(targets), related_urls=sorted(targets))
        if page["hreflangs"] and not any(target == _url(url) for _, target in links):
            add("hreflang_missing_self", "medium", url, "The alternate set has no valid self-reference.",
                "Include this page's fully qualified URL in its own alternate set.")

    checked_pairs = set()
    for url, links in valid_links.items():
        for language, target_url in links:
            if target_url == _url(url):
                continue
            target = observed.get(target_url)
            related = [url, target_url]
            if target is None:
                unchecked_targets.append({"url": url, "target": target_url, "reason": "not_observed"})
                continue
            if target.get("noindex") or (isinstance(target.get("status"), int) and target["status"] >= 400):
                add("hreflang_unavailable_target", "high", url, "An observed alternate is noindex or returns an HTTP error.",
                    "Use an available, indexable alternate or remove an obsolete annotation.",
                    target=target_url, related_urls=related, target_status=target.get("status"), target_noindex=target.get("noindex", False))
            if not eligible(target) or "hreflangs" not in target:
                unchecked_targets.append({"url": url, "target": target_url, "reason": "target_not_successfully_checked"})
                continue
            target_links = valid_links.get(target["url"], [])
            if not any(back == _url(url) for _, back in target_links):
                add("hreflang_missing_return", "medium", url, "The observed alternate has no valid return annotation.",
                    "Add reciprocal alternate links between the language versions.", target=target_url, related_urls=related)
            canonical_urls = {_url(canonical, target["url"]) for canonical in target.get("canonicals", [])}
            canonical_urls.discard(None)
            if canonical_urls and _url(target["url"]) not in canonical_urls:
                add("hreflang_noncanonical_target", "medium", url, "The observed alternate canonicalizes to another URL.",
                    "Review alternate/canonical consistency; intentional same-language regional consolidation may be valid.",
                    target=target_url, canonicals=sorted(canonical_urls), related_urls=related + sorted(canonical_urls))
            if _url(target["url"]) != target_url:
                add("hreflang_redirect_target", "low", url, "The alternate redirects to another observed URL.",
                    "Use the final canonical alternate URL where appropriate.", target=target_url,
                    final_url=target["url"], related_urls=related)
            pair = tuple(sorted((url, target["url"])))
            if pair in checked_pairs:
                continue
            checked_pairs.add(pair)
            own_map, target_map = defaultdict(set), defaultdict(set)
            for code, destination in links:
                own_map[code].add(destination)
            for code, destination in target_links:
                target_map[code].add(destination)
            conflicts = sorted(code for code in own_map.keys() & target_map.keys() if own_map[code] != target_map[code])
            if conflicts:
                add("hreflang_cluster_conflict", "medium", url, "Observed alternates disagree about language destinations.",
                    "Use consistent language-to-URL mappings across each alternate cluster.",
                    target=target_url, languages=conflicts, related_urls=related)

    checked_entities = Counter()
    unsupported_types = set()
    structured_pages, truncated_pages, unsupported_context_pages = 0, [], []
    unresolved_reference_pages, conflicting_identity_pages = [], []
    for page in pages:
        if not eligible(page) or "structured_data" not in page:
            continue
        structured_pages += 1
        url = page["url"]
        nodes, truncated = _nodes(page["structured_data"])
        if truncated:
            truncated_pages.append(url)
        # Custom aliases/context overrides need a full JSON-LD processor. Skipping
        # the page prevents false required-field failures from compacted keys.
        contexts = [node["@context"] for node in nodes if "@context" in node]
        if any(context not in ("https://schema.org", "http://schema.org", "https://schema.org/", "http://schema.org/") for context in contexts):
            unsupported_context_pages.append(url)
            continue
        identities = {}
        conflicting_identities = False
        for node in nodes:
            identity = node.get("@id")
            if not isinstance(identity, str) or len(node) == 1:
                continue
            merged = identities.setdefault(identity, {})
            if any(key in merged and merged[key] != value for key, value in node.items()):
                conflicting_identities = True
            merged.update(node)
        if conflicting_identities:
            # Merging conflicting values requires JSON-LD expansion semantics.
            conflicting_identity_pages.append(url)
            continue

        def unresolved(value):
            missing = (isinstance(value, dict) and set(value) == {"@id"}
                       and isinstance(value.get("@id"), str) and value["@id"] not in identities)
            if missing and url not in unresolved_reference_pages:
                unresolved_reference_pages.append(url)
            return missing

        def resolve(value):
            if isinstance(value, dict) and set(value) == {"@id"} and isinstance(value["@id"], str):
                return identities.get(value["@id"], value)
            return value

        checked_ids = set()
        for node in nodes:
            identity = node.get("@id")
            if isinstance(identity, str) and identity in identities:
                if identity in checked_ids:
                    continue
                checked_ids.add(identity)
                node = identities[identity]
            types = _types(node)
            profiles = types & {"Product", "BreadcrumbList", "Article", "NewsArticle", "BlogPosting"}
            unsupported_types.update(types - profiles)
            profiles = {"Article" if profile in {"NewsArticle", "BlogPosting"} else profile for profile in profiles}
            for profile in sorted(profiles):
                checked_entities[profile] += 1

                def finding(rule, severity, message, fields):
                    add(rule, severity, url, message, "Repair the listed fields against the linked Google documentation; verify the visible page and Rich Results Test.",
                        schema_type=profile, entity_id=node.get("@id"), fields=fields, source=SOURCES[profile])

                if profile == "Product":
                    missing = []
                    if not _text(node.get("name")):
                        missing.append("name")
                    if not any(_present(resolve(node.get(key))) for key in ("offers", "review", "aggregateRating")):
                        missing.append("offers OR review OR aggregateRating")
                    if missing:
                        finding("structured_product_required", "medium", "Product is missing basic product-snippet fields.", missing)
                    offers = resolve(node.get("offers", []))
                    for offer in (offers if isinstance(offers, list) else [offers]):
                        offer = resolve(offer)
                        if unresolved(offer):
                            continue
                        if not isinstance(offer, dict):
                            finding("structured_product_offer", "medium", "Product offers must contain structured Offer or AggregateOffer objects.", ["offers"])
                            continue
                        offer_types = _types(offer)
                        fields = []
                        if "AggregateOffer" in offer_types:
                            if not _numeric(offer.get("lowPrice")):
                                fields.append("offers.lowPrice")
                            if not _text(offer.get("priceCurrency")) or not re.fullmatch(r"[A-Za-z]{3}", offer["priceCurrency"]):
                                fields.append("offers.priceCurrency")
                        elif "Offer" in offer_types:
                            specification = resolve(offer.get("priceSpecification", {}))
                            specifications = specification if isinstance(specification, list) else [specification]
                            # Google prefers an explicit price even if a second value exists.
                            price_ok = _numeric(offer["price"]) if "price" in offer else any(isinstance(item, dict) and _numeric(item.get("price")) for item in specifications)
                            if not price_ok:
                                fields.append("offers.price OR offers.priceSpecification.price")
                        if fields:
                            finding("structured_product_offer", "medium", "Product offer has missing or invalid basic price fields.", fields)
                elif profile == "BreadcrumbList":
                    elements = node.get("itemListElement")
                    if not isinstance(elements, list) or len(elements) < 2:
                        finding("structured_breadcrumb_required", "medium", "BreadcrumbList needs at least two ListItem entries.", ["itemListElement"])
                        continue
                    fields, positions = [], []
                    for index, value in enumerate(elements):
                        entry = resolve(value)
                        prefix = f"itemListElement[{index}]"
                        if unresolved(entry):
                            continue
                        if not isinstance(entry, dict) or "ListItem" not in _types(entry):
                            fields.append(prefix + ".@type")
                            continue
                        position = entry.get("position")
                        if type(position) is not int or position < 1:
                            fields.append(prefix + ".position")
                        else:
                            positions.append(position)
                        item = resolve(entry.get("item"))
                        if not _text(entry.get("name")) and not unresolved(item) and not (isinstance(item, dict) and _text(item.get("name"))):
                            fields.append(prefix + ".name")
                        item_url = item.get("@id") if isinstance(item, dict) else item
                        if (index < len(elements) - 1 or item is not None) and _url(item_url) is None:
                            fields.append(prefix + ".item")
                    if len(positions) != len(set(positions)):
                        fields.append("itemListElement.position (duplicate positions)")
                    if fields:
                        finding("structured_breadcrumb_required", "medium", "Breadcrumb items have missing or invalid required fields.", fields)
                else:
                    # Google explicitly specifies NO required Article properties.
                    missing = [field for field in ("headline", "author", "image", "datePublished") if not _present(node.get(field))]
                    if missing:
                        finding("structured_article_recommended", "low", "Article omits recommended properties; these are not requirements.", missing)
                    for field in ("datePublished", "dateModified"):
                        if field not in node:
                            continue
                        try:
                            value = node[field]
                            if not isinstance(value, str) or not re.match(r"^\d{4}-\d{2}-\d{2}(?:T|$)", value):
                                raise ValueError
                            datetime.fromisoformat(value.replace("Z", "+00:00"))
                        except ValueError:
                            finding("structured_article_date", "low", "Article date does not use a supported ISO date/date-time form.", [field])

    # Idempotent enrichment is useful when an MCP client rebuilds a saved report.
    diagnostic_prefixes = ("hreflang_", "structured_")
    result["issues"] = [issue for issue in existing if not issue.get("rule_id", "").startswith(diagnostic_prefixes)]
    for group in grouped.values():
        group["affected_urls"].sort()
        group["affected_count"] = len(group["affected_urls"])
        result["issues"].append(group)
    result["issues"].sort(key=lambda issue: (SEVERITY[issue["severity"]], -len(issue["affected_urls"]), issue["rule_id"]))
    result["summary"] = {**result.get("summary", {}), "issue_groups": len(result["issues"]),
                         "groups_by_severity": dict(Counter(issue["severity"] for issue in result["issues"]))}
    result["diagnostics"] = {
        "rules_reviewed_at": "2026-09-27", "sources": SOURCES,
        "hreflang": {"checked_pages": hreflang_pages, "checked_pairs": len(checked_pairs),
                     "unchecked_targets": unchecked_targets,
                     "not_checked": ["HTTP Link-header and sitemap hreflang annotations", "Unobserved alternate pages", "Actual content language and translation equivalence", "ISO 15924 script registry membership"]},
        "structured_data": {"checked_pages": structured_pages, "checked_entities": dict(checked_entities),
                            "unsupported_types": sorted(unsupported_types), "truncated_pages": truncated_pages,
                            "unsupported_context_pages": unsupported_context_pages,
                            "unresolved_reference_pages": unresolved_reference_pages,
                            "conflicting_identity_pages": conflicting_identity_pages,
                            "not_checked": ["Full JSON-LD expansion, custom contexts and unresolved external @id references", "Review/aggregateRating nested requirements and advanced product/merchant-listing rules", "Content truthfulness, visibility, image accessibility, policy compliance and Google rich-result eligibility", "Microdata and RDFa", "Currency registry membership"]},
    }
    return result
