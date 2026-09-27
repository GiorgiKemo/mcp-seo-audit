# Changelog

## 2.1.0 - 2026-09-27

Source release preparation. PyPI/MCP Registry publication is separate from the GitHub update.

### Added

- Structured audit action plans with stable rule IDs, affected URLs, evidence, recommendations, coverage, and conservative snapshot comparisons.
- Optional sandboxed Chromium rendering, raw/rendered comparison, bounded resource fetching, and explicit rendering failures/partial coverage.
- Sitemap-index discovery and sitemap-seeded inventories with source tracking and sample-scoped orphan candidates.
- Hreflang syntax, reciprocal-link, cluster, availability, and canonical diagnostics for observed pages.
- Basic Product, BreadcrumbList, and Article-family JSON-LD checks with unsupported-context/type/reference disclosure.
- Paginated Search Console snapshots and severity/observed-traffic issue prioritization with request, row, time, and freshness metadata.
- SQLite projects, immutable audit history, retention, stored-report comparison, explicit schedules, and a local change/failure/recovery inbox.
- An opt-in `mcp-seo-monitor` worker with one-shot operation, cancellation, concurrent-worker leases, failure backoff, and crash recovery.
- Local server-readiness reporting through `get_server_status`.
- Cross-platform test/build automation, hash-pinned runtime dependencies, package checks, contributor guidance, and a documented local-deployment security boundary.

### Fixed

- Search Analytics request fields, inclusive Pacific-time date windows, missing-data handling, numeric performance metrics, and misleading quota/indexing claims.
- Canonical/duplicate comparisons that could infer fixes from missing pages, failed related checks, or changed crawl settings.
- Robots handling, crawler URL/origin classification, nofollow/indexability handling, link sources, body-text extraction, and redirect aliases.
- Unbounded sitemap decompression and unsafe gaps between DNS validation and the actual HTTP connection.
- Event-loop blocking in Google operations, with serialized provider calls and bounded retries for reads only; mutations are never automatically retried.
- Credential/error redaction, OAuth token replacement, token storage defaults, MCP tool annotations, and package runtime-module inclusion.

### Scope and compatibility

- The supported deployment remains a trusted local stdio MCP server. No hosted authentication, tenant isolation, public service, or external notification delivery is introduced.
- Browser rendering requires installed Chromium. Raw HTML audits remain available without it. Local Lighthouse now requires explicit `SEO_AUDIT_ENABLE_LOCAL_LIGHTHOUSE=true` for trusted targets.
- Scheduling is disabled until explicitly enabled and requires a separately running worker. Existing installations do not automatically begin monitoring.
- Reports use schema version `1.0`; comparison requires matching scope/settings and applicable evidence. Findings are audit guidance, not ranking guarantees or rich-result certification.
- Google access, provider quotas, browser availability, and publication status must be verified in the actual deployment; mocked tests do not establish those conditions.

## 2.0.2

Existing package baseline with Google Search Console, Indexing API, performance APIs, local Lighthouse, and technical SEO inspection tools. See repository history for changes before the 2.1.0 audit and release work.
