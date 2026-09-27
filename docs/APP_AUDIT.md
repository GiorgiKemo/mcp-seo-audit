# Application audit and improvement record

Date: 2026-09-27. Scope: the local Python MCP server, its 30 original tools, package configuration, and test suite. This is a code, protocol, and behavior audit; the project has no standalone graphical interface to audit.

## Verdict

The server has useful breadth across Search Console, page inspection and performance. Its biggest weakness was trust in the results: passing mocked tests did not catch unsupported API parameters, incorrect time windows, false missing-data conclusions, or incomplete crawl coverage. Text-only findings also made it difficult to turn an audit into a repair backlog and verify subsequent changes.

Version 2.1.0 addresses these foundations and the follow-up production backlog, bringing the server to 43 tools. The supported deployment is a trusted, single-user local stdio MCP process with an optional separately operated monitoring worker. This audit does not establish competitive superiority, hosted multi-tenant readiness, or live Google account access. No PyPI publication, Google write calls, account changes, or paid services were performed.

## Findings and implemented changes

| Priority | Evidence in original implementation | Change |
| --- | --- | --- |
| High | Advanced/page-query analytics sent `orderBy`, which is absent from Google's request schema. | Removed unsupported request fields; alternative sorting is local and explicitly limited to returned rows. |
| High | Date windows included `days + 1` dates and used the machine timezone. | Exact inclusive windows in America/Los_Angeles with cross-platform timezone data. |
| High | Period differences used integer formatting on API doubles and assigned position zero to absent rows. | Float-safe formatting, absent values shown as unavailable, validation and unequal-period warnings. |
| High | Crawler never consulted robots.txt and stripped query strings. | Bounded robots parser, crawler-specific rules, delays, temporary-error deferral, preserved query/path parameters and explicit coverage. |
| High | Findings were prose without reusable IDs, affected-page groups or a way to validate fixes. | Structured action plans and snapshot comparison with dependency-aware uncertainty. |
| High | Gzip sitemap expansion was unbounded after download. | Decompressed-size limit with regression tests. |
| High | Reauthentication removed an existing token before new login succeeded. | Atomic token replacement; preserve existing token and cached services on failure. |
| High | Generic CrUX/PSI exception text could include API keys. | Redaction before output truncation for both provider errors and generic exceptions. |
| Medium | Numeric-string CLS and missing performance data became failures. | Numeric conversion and an explicit insufficient-data outcome. |
| Medium | Lighthouse used a blocking subprocess in an async tool. | Async subprocess execution, bounded output, timeout/cancellation cleanup and Windows/POSIX descendant cleanup; local Lighthouse requires explicit opt-in. |
| Medium | Title text counted as body content; `none`/repeated robots tags were missed; host-prefix matches misclassified external links. | Visible-body counting, combined/scoped robots directives, exact origin checks and image-alt link names. |
| Medium | Redirect aliases could inflate duplicate groups; links with base URLs could target wrong pages. | Final-page deduplication and base-URL-aware discovery. |
| Medium | Indexing success implied future crawling; batch output guessed remaining daily quota and omitted unattempted URLs. | Distinguish accepted notifications from outcomes, remove quota guess and list pending URLs. |
| Medium | MCP clients lacked explicit read/write/destructive metadata. | Tool annotations on all tools, including local-only snapshot comparison. |
| Medium | Docker omitted README/LICENSE needed by package metadata; no CI matrix existed. | Correct package inputs, synchronized dependencies and Windows/Linux test/build workflow. |

## New audit workflow

1. Run `get_seo_audit_report` against a permitted target. Review `audit_status`, robots state and coverage before interpreting findings.
2. Work through prioritized rule groups using the affected URLs, evidence and fix guidance. Verify intentional noindex/canonical choices before changing them.
3. Use `create_audit_project` and `run_project_audit` to persist bounded snapshots, or save ad hoc report JSON in the MCP client.
4. Re-run with the same start URL and settings, then use `compare_project_audits` for stored history or `compare_seo_audits` for two JSON reports. Enable a schedule and run `mcp-seo-monitor` separately for recurring checks; review changes through `list_audit_events`.
5. Treat `resolved` as an applicable recheck, `unverified` as missing evidence, and `newly_observed` as newly sampled evidence rather than a demonstrated regression.

The comparison also accounts for duplicate peers and canonical targets. For example, a canonical problem is not declared fixed merely because its failing target dropped out of the next sample. Metadata suppressed by a new noindex directive is not automatically labeled repaired.

## Follow-up production backlog completed

| Capability | Implementation | Acceptance coverage |
| --- | --- | --- |
| Rendered crawling and raw/rendered comparison | Optional Chromium, guarded resource fulfillment, robots checks, request/byte/time budgets and raw/rendered signals. | Real JavaScript-created title/link fixture, private-target guards, cancellation, redirects and incomplete-resource metadata. |
| Sitemap inventory and candidate orphan pages | Nested XML/text/gzip inventory, deduplication and sitemap-only candidates after linked discovery. | Compression/URL/document limits, same-origin scope, partial inventory and conservative comparison. Candidates are not proven site-wide orphans. |
| Durable projects, scheduled audits and events | Local SQLite, immutable bounded snapshots, retention, opt-in schedules, worker leases, failure backoff and local change/failure/recovery events. | Isolation by project, retention, lease contention, timeout/cancellation, worker scheduling and meaningful-event suppression. |
| International and structured-data diagnostics | Observed hreflang clusters and Product/BreadcrumbList profiles; article recommendations. | Language/region syntax, reciprocal/self references, observed canonical/noindex targets and profile-required fields. No eligibility guarantees or remote-context fetching. |
| Paginated analytics and traffic priorities | Bounded Search Console pagination with partial results and severity/click/impression ranking. | Dedupe, limits, provider failures after collected rows, exact-URL weighting and input validation before API use. |
| Guarded network operation | DNS-to-socket pinning, original Host/SNI, public-address checks and environment-proxy bypass. | Literal/translated address handling, mixed DNS answers, TLS identity and redirect revalidation. |
| Async Google operations | Serialized worker, HTTP timeout and bounded retries for reads only. | Event-loop responsiveness, transport serialization, cancellation and no retry of mutations. |
| Release operations | Hash-pinned runtime, safe nonroot container, setup status tool, atomic per-user OAuth storage, security/contribution docs and MIT attribution. | Package build/metadata, dependency audit, real stdio MCP handshake and CI matrix. |

## Verification

- Baseline: 99 tests passed before changes, illustrating gaps in the original coverage.
- Local Windows/Python 3.14 suite: 446 tests passed with actual Chromium enabled. This includes real HTTP, public audit workflow, MCP subprocess, persistence and Windows child-process cleanup tests.
- The installed hash-pinned runtime passes `pip check`; wheel and source distributions build successfully with all ten runtime modules.
- GitHub Actions runs Python 3.11, 3.13 and 3.14 on Ubuntu and Windows, including actual Chromium on 3.13, package metadata validation, dependency auditing and a nonroot Docker smoke check. The badge and exact commit workflow are the authoritative remote results.
- Google integrations use mocked responses for repeatable validation. Additional live read-only checks succeeded for existing OAuth credentials: property listing and a one-row Search Analytics snapshot with the requested row-limit metadata. This verifies those operations for that configured account at test time, not every Google integration, property, quota or future provider availability. No Google write calls were made.

## Supported production boundary

- This release is for one trusted local user. It does not expose an authenticated hosted API, tenant isolation, a distributed scheduler, billing, outbound email/webhooks, or a public scanning service.
- Monitoring needs an explicitly enabled project schedule and a running separate worker. Events are stored locally; delivery to external channels is not implemented or implied.
- One-page HTML tools are direct inspections; recursive crawls and rendered resources honor the configured robots policy. Robots input is capped at 500 KiB with truncation reported.
- A completed discovered frontier is not proof of complete site coverage. Sitemap omissions, crawl limits, blocked resources, unknown targets and incomplete renderer signals remain explicit uncertainty.
- Guarded rendering is for analysis, with bounded network access. Local Lighthouse is a separate trusted-target browser path, disabled by default, and its subresources do not receive the guarded renderer's network policy.
- Chromium must be installed separately for rendered audits; the default container supports raw audits. Google tools need the user's own configured credentials, enabled APIs and appropriate property permissions.
- Dependency scan results are time-specific; the lockfile should be refreshed and re-audited for later releases.

## Source checks

Google documents inclusive Pacific-time dates, supported request fields, and the fact that Search Analytics returns top rows rather than guaranteeing every row. These informed the analytics corrections. [Search Analytics request reference](https://developers.google.com/webmaster-tools/v1/searchanalytics/query).

Crawler policy and directives were checked against primary specifications: [Google robots.txt interpretation](https://developers.google.com/crawling/docs/robots-txt/robots-txt-spec), [RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html), and [Google robots meta/header rules](https://developers.google.com/search/docs/crawling-indexing/robots-meta-tag).

The product priorities were informed by current vendor documentation, not a benchmark showing this app outperforms competitors. Screaming Frog documents JavaScript rendering, scheduling and crawl comparison; Semrush documents recurring audits and change reports. [Screaming Frog SEO Spider](https://www.screamingfrog.co.uk/seo-spider/), [Semrush audit workflow](https://www.semrush.com/kb/1184-audit-your-website).
