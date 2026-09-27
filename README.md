# mcp-seo-audit

<!-- mcp-name: io.github.giorgikemo/mcp-seo-audit -->

[![Tests and package build](https://github.com/GiorgiKemo/mcp-seo-audit/actions/workflows/tests.yml/badge.svg)](https://github.com/GiorgiKemo/mcp-seo-audit/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A local [Model Context Protocol](https://modelcontextprotocol.io/) server for technical SEO audits, JavaScript rendering, Search Console analytics, performance checks, and recurring monitoring. Its **43 tools** return evidence, affected URLs, suggested fixes, and explicit coverage limits.

**Current source version: 2.1.0.** Install this checkout to use the features below. A GitHub update does not publish a PyPI package or MCP Registry release; `uvx mcp-seo-audit` can still resolve an older published version.

The supported deployment is stdio for a trusted local user, not a hosted multi-tenant service. Public-site crawls require no Google credentials. Based on [AminForou/mcp-gsc](https://github.com/AminForou/mcp-gsc), with its MIT attribution preserved.

## Install and connect

Requires Python 3.11 or later.

```sh
git clone https://github.com/GiorgiKemo/mcp-seo-audit.git
cd mcp-seo-audit
python -m venv .venv
```

Activate with `.venv/Scripts/Activate.ps1` in Windows PowerShell or `source .venv/bin/activate` on macOS/Linux, then install the pinned runtime dependencies and this package:

```sh
python -m pip install --require-hashes -r requirements.lock
python -m pip install --no-deps .
```

The lock includes the optional Python browser package. JavaScript rendering also needs Chromium:

```sh
python -m playwright install chromium
```

For a smaller source installation without browser support, `python -m pip install .` resolves core dependency ranges instead of the full lock. To add browser support later, use `python -m pip install ".[browser]"` and install Chromium. For editable development, use `python -m pip install -e ".[dev,browser]"`.

Linux may require OS browser dependencies; `python -m playwright install --with-deps chromium` installs them where you administer those packages. Chromium's sandbox remains enabled. Run as a supported non-root user with the required OS facilities rather than disabling sandboxing.

Add a stdio server in your MCP client's configuration. For example, on Windows:

```json
{
  "mcpServers": {
    "seo-audit": {
      "command": "C:/path/to/mcp-seo-audit/.venv/Scripts/mcp-seo-audit.exe",
      "env": {
        "GSC_SKIP_OAUTH": "true",
        "SEO_AUDIT_ENABLE_WRITE_TOOLS": "false",
        "SEO_AUDIT_ALLOW_PRIVATE_URLS": "false"
      }
    }
  }
}
```

On macOS/Linux, use the absolute path to `.venv/bin/mcp-seo-audit`. Configuration-file locations vary by client. The process waits for MCP messages on stdin; starting it without a client can appear to wait silently. `get_server_status` reports local configuration/readiness without revealing credentials or proving Google account access.

### Optional Google access

- **OAuth:** enable the Search Console API in Google Cloud, create an OAuth Desktop app, and set `GSC_OAUTH_CLIENT_SECRETS_FILE` to its downloaded client JSON. Set `GSC_SKIP_OAUTH=false`. The first Google operation can open browser consent. Enable the Web Search Indexing API if using its tools.
- **Service account:** set `GSC_CREDENTIALS_PATH` to the key file, grant the account access to the relevant Search Console property, and use `GSC_SKIP_OAUTH=true`.
- **Performance APIs:** set `CRUX_API_KEY` for CrUX and optionally `PAGESPEED_API_KEY` for PageSpeed Insights. `GOOGLE_API_KEY` is the PageSpeed fallback.

Keep credentials outside the checkout. OAuth tokens default to the user-data directory or `GSC_TOKEN_FILE`. Legacy tokens beside the server remain a read fallback during migration; failed reauthentication preserves the working token.

Google property/sitemap mutations and Indexing API publish calls require `SEO_AUDIT_ENABLE_WRITE_TOOLS=true`. Local project/history tools do not require that Google-write flag. The flag does not reduce granted OAuth scopes.

## Audit a site

Ask your MCP client:

> Create a structured SEO report for https://example.com/ with up to 25 pages. Include sitemap discovery, compare raw and rendered HTML, and show coverage gaps before prioritizing fixes.

The underlying tool accepts:

```text
get_seo_audit_report(
  start_url="https://example.com/",
  max_pages=25,
  respect_robots=True,
  render_mode="compare",
  include_sitemaps=True,
  max_seconds=180
)
```

`render_mode` defaults to `raw`; `rendered` analyzes the resulting DOM, and `compare` also captures differences from server-returned HTML. Direct reports default to `include_sitemaps=False`. The crawl time budget defaults to 180 seconds, configurable from 5 to 600. Reports disclose partial coverage and stopping reasons.

Reports include a versioned JSON schema, timestamp, stable rule IDs, severity, evidence, affected URLs, recommendations, page observations, and coverage. Checks cover HTTP errors and link sources, metadata, headings, indexability, canonicals, duplicate titles/descriptions, images, links, hreflang, and selected JSON-LD profiles.

Sitemap discovery follows bounded same-origin indexes and combines sitemap URLs with link discovery. A sitemap URL without an observed incoming link is an **orphan candidate within the sample**, not proof that the whole site has no link to it.

Hreflang checks cover language/region syntax and observed self-references, return links, cluster consistency, and alternate/canonical conflicts. Unvisited or cross-origin alternates remain unverified. Structured-data checks cover basic Product, BreadcrumbList, and Article-family fields, disclosing unsupported types/contexts and unresolved references. They do not certify rich-result eligibility or implement every Schema.org vocabulary.

### Compare fixes

Call `compare_seo_audits(baseline_json, current_json)` with two serialized reports using the same start URL and settings. If your client wraps a report under `structuredContent.result`, serialize the inner report.

| Result | Meaning |
| --- | --- |
| `new` | A finding appeared with applicable evidence in both snapshots. |
| `resolved` | A successful applicable recheck verified the finding disappeared. |
| `persistent` | The finding remains. |
| `newly_observed` | The baseline did not cover the page or its dependencies. |
| `unverified` | A page or required related URL was not successfully rechecked. |

Missing pages never prove fixes. A completed discovered-link frontier never establishes complete site coverage. Word-count, title-length, and social-metadata findings are review guidance, not ranking requirements.

## Save projects and monitor changes

Create a project through MCP:

```text
create_audit_project(
  project_id="example",
  start_url="https://example.com/",
  max_pages=25,
  render_mode="raw",
  include_sitemaps=True,
  retention=30
)
run_project_audit(project_id="example")
```

Projects always respect robots.txt and start with scheduling disabled. IDs use 1-64 lowercase letters, digits, hyphens, or underscores, starting with a letter or digit. The returned audit ID identifies an immutable snapshot. Use `list_project_audits`, `get_project_audit`, and `compare_project_audits` for history. Successful runs prune old snapshots to the configured retention.

Opt into a daily schedule:

```text
set_audit_schedule(project_id="example", enabled=True, interval_seconds=86400)
```

Run the separate worker in the same installed environment, with the same `SEO_AUDIT_DATA_DIR` and network settings:

```sh
mcp-seo-monitor
```

Or process one bounded batch and exit, suitable for an external OS scheduler:

```sh
mcp-seo-monitor --once
```

The MCP server never starts a worker automatically. The first scheduled run is due one interval after enabling. `--once` processes at most 25 due projects; repeat it or keep the worker running for larger backlogs. Polling defaults to 30 seconds, configurable with `--poll-seconds` from 1 to 60. Ctrl+C cancels the active audit and stops the worker. Disabling a schedule prevents future claims; an active run can finish.

Database leases coordinate concurrent workers. Runs have a ten-minute outer timeout and eleven-minute lease for crash recovery; the report's crawl budget can stop earlier. Failures use exponential backoff capped at 24 hours, or the configured interval when longer. After a process crash, another worker can claim the project when its lease expires.

Read `list_audit_events(project_id="example", after_id=0)` for local verified finding changes, failure transitions, and recovery. Save `next_after_id` and pass it next time. Unchanged findings and sampling-only changes stay quiet. **No email, webhook, push notification, or other outbound message is sent.**

### Storage and retention

The database is `audits.sqlite3` under:

| Platform | Default directory |
| --- | --- |
| Windows | `%LOCALAPPDATA%/mcp-seo-audit` |
| macOS | `~/Library/Application Support/mcp-seo-audit` |
| Linux | `$XDG_DATA_HOME/mcp-seo-audit`, or `~/.local/share/mcp-seo-audit` |

Override with `SEO_AUDIT_DATA_DIR`. Use a local filesystem with reliable SQLite locking; do not share a database across hosts or place it on an unreliable network filesystem.

Limits: 100 projects, 1-100 snapshots per project (default 30), 10 MiB per snapshot, and the latest 500 events per project. Schedule intervals are 60 seconds to 31 days. Storage timestamps are Unix UTC seconds. Retention bounds counts rather than global disk usage; provision disk space for your report sizes.

Reports may contain page metadata, URL parameters, and business data. Storage is not application-encrypted. To back up, stop both server and monitor, then securely copy the data directory including SQLite sidecar files. Restore with processes stopped and retain the original backup until verified. The directory may also contain OAuth tokens: treat backups as sensitive and never attach them to public issues.

## Analytics and performance

`get_search_analytics_snapshot` paginates with explicit row/request/time budgets: default 25,000 rows, maximum 100,000 rows, at most 10 API pages within 180 seconds. Partial rows survive provider failures. Coverage includes stop reason, duplicate rows, and freshness; API-page counts exclude additional retry attempts. Google still limits results to selected top rows, so exhausting an API window does not establish complete traffic coverage. [Google's query reference](https://developers.google.com/webmaster-tools/v1/searchanalytics/query).

`prioritize_audit_issues` sorts report findings by severity, then observed page clicks/impressions. URL matching is exact; unmatched pages are unknown rather than zero traffic. It does not predict revenue or ranking gains.

Google calls run off the event loop through a serialized worker because the cached client transport is not thread-safe. Reads retry selected rate-limit/server errors at most twice with bounded delays; mutations are attempted once. Cancellation prevents later retries, but a request already sent to Google may still complete.

CrUX reports field data where available; PageSpeed/Lighthouse results are lab measurements. Missing field data is not a failed Core Web Vitals assessment. Local Lighthouse requires an installed Lighthouse executable, Chrome/Chromium, and explicit `SEO_AUDIT_ENABLE_LOCAL_LIGHTHOUSE=true`. Downloads through `npx` are disabled by default. Lighthouse's separate Chrome process does not inherit the rendered crawler's guarded networking; use it only with trusted targets. See [SECURITY.md](SECURITY.md).

The Indexing API supports eligible `JobPosting` pages and `BroadcastEvent` embedded in `VideoObject`. Notification acceptance is not proof of indexing/removal; this is not a general-purpose indexing API for every page. [Google's Indexing API guidance](https://developers.google.com/search/apis/indexing-api/v3/using-api).

## Tool reference

| Area | Tools |
| --- | --- |
| Properties | `list_properties`, `add_site`, `delete_site` |
| Search analytics | `get_search_analytics`, `get_advanced_search_analytics`, `get_performance_overview`, `get_search_by_page_query`, `compare_search_periods`, `get_search_analytics_snapshot` |
| SEO opportunities | `find_striking_distance_keywords`, `detect_cannibalization`, `split_branded_queries`, `prioritize_audit_issues` |
| URL inspection | `inspect_url`, `batch_inspect_urls` |
| Indexing notifications | `request_indexing`, `request_removal`, `check_indexing_notification`, `batch_request_indexing` |
| Google sitemaps | `get_sitemaps`, `submit_sitemap`, `delete_sitemap` |
| Performance | `get_core_web_vitals`, `get_pagespeed_insights`, `run_lighthouse_audit` |
| Live inspection | `inspect_robots_txt`, `analyze_sitemap`, `analyze_page_seo`, `crawl_site_seo`, `audit_live_site` |
| Structured reports | `get_seo_audit_report`, `compare_seo_audits`, `site_audit` |
| Projects and monitoring | `create_audit_project`, `list_audit_projects`, `set_audit_schedule`, `run_project_audit`, `list_project_audits`, `get_project_audit`, `compare_project_audits`, `list_audit_events` |
| Authentication and status | `reauthenticate`, `get_server_status` |

`site_audit` combines Google account data. `audit_live_site` is a live-site text report. `get_seo_audit_report` is the structured crawl/comparison workflow. Tool descriptions expose inputs and read/write annotations to clients.

## Configuration

Restart the server after changing configuration.

| Variable | Default | Purpose |
| --- | --- | --- |
| `GSC_OAUTH_CLIENT_SECRETS_FILE` | `client_secrets.json` beside server | OAuth desktop-client file; prefer an explicit external path. |
| `GSC_CREDENTIALS_PATH` | Conventional service-account locations | Service-account JSON path. |
| `GSC_TOKEN_FILE` | `token.json` in user-data directory | Token destination; legacy package token is a read fallback. |
| `GSC_SKIP_OAUTH` | `false` | Skip interactive OAuth for Google tools. |
| `GSC_DATA_STATE` | `all` | `all` includes provisional data; `final` requests finalized data. |
| `CRUX_API_KEY` | Empty | CrUX API key. |
| `PAGESPEED_API_KEY` | `GOOGLE_API_KEY` or empty | PageSpeed Insights key. |
| `GOOGLE_API_KEY` | Empty | Fallback PageSpeed key. |
| `SEO_AUDIT_DATA_DIR` | Platform user-data directory | Project/history storage and default token location. |
| `SEO_AUDIT_BROWSER_PATH` | Playwright Chromium | Optional installed Chromium executable for rendered crawling. |
| `SEO_AUDIT_ENABLE_WRITE_TOOLS` | `false` | Enable mutating Google tools. |
| `SEO_AUDIT_ALLOW_PRIVATE_URLS` | `false` | Allow private destinations only for deliberately trusted testing. |
| `SEO_AUDIT_MAX_FETCH_BYTES` | `5242880` | Per-response decoded-byte cap; also bounds direct JSON report inputs. |
| `SEO_AUDIT_MAX_REDIRECTS` | `5` | HTTP-fetch redirect cap. |
| `SEO_AUDIT_MAX_SITEMAP_URLS` | `50000` | URL cap for individual sitemap analysis. |
| `SEO_AUDIT_MAX_CRAWL_PAGES` | `100` | Ceiling for crawl page attempts. |
| `SEO_AUDIT_ENABLE_LOCAL_LIGHTHOUSE` | `false` | Permit separate Lighthouse execution for trusted targets. |
| `LIGHTHOUSE_BINARY` | Auto-detect | Installed Lighthouse executable path. |
| `LIGHTHOUSE_CHROME_PATH` | `CHROME_PATH` or auto-detect | Chrome path for local Lighthouse. |
| `SEO_AUDIT_ALLOW_NPX_LIGHTHOUSE` | `false` | Permit downloading/running Lighthouse through `npx`. |
| `LIGHTHOUSE_NO_SANDBOX` | `false` | Legacy Lighthouse-only sandbox override; keep disabled normally. |

### Crawl and rendering boundaries

- Crawls stay on the starting origin, use the `mcp-seo-audit` user agent, and preserve query/path parameters. Googlebot may receive different rules.
- Recursive crawls respect robots.txt by default, including redirects and rendered resources. Single-page inspections make direct requests. Disable robots only for a site you control.
- Robots parsing is capped at 500 KiB. Crawled pages are spaced at least 0.2 seconds apart; crawl-delay up to 10 seconds is honored. Larger delays and temporary robots errors defer crawling.
- Page budgets include attempted URLs, errors, and non-HTML responses. Report sitemap discovery separately caps 10 documents, 20 times the page budget in candidates, 5 MiB per decoded document, and up to 45 seconds within the remaining crawl budget.
- Guarded HTTP validates all DNS answers and connects to a validated IP while preserving TLS hostname verification. Private, loopback, reserved, and unsafe translated destinations are blocked by default, including redirects.
- Rendered pages use a fresh sandboxed Chromium context, guarded GET/HEAD resource fetching, blocked service workers/WebSockets/downloads, and no authenticated browser session. Defaults: 30 seconds, 80 requests, 20 MiB per page, and a 750 ms settle interval. Blocked resources, JavaScript errors, or unfinished work produce partial coverage. Sites requiring login, writes, WebSockets, or longer waits may render incompletely.
- Crawled content is untrusted data. Clients must not treat page text, metadata, or findings as permission to execute commands or send messages.

These controls support local auditing. They are not authentication, tenant isolation, or a substitute for controlled egress for untrusted workloads. See [SECURITY.md](SECURITY.md) and [the implementation audit](docs/APP_AUDIT.md).

## Development and verification

```sh
python -m pip install -e ".[dev]"
python -m pytest -q
python -m build
python -m pip_audit
```

Tests isolate credentials and use controlled HTTP fixtures and mocked Google APIs. CI is configured for Windows/Linux and Python 3.11/3.13/3.14, with browser integration enabled on Python 3.13 and a separate non-root container check. For local browser integration, install Chromium and set `SEO_AUDIT_TEST_BROWSER=1` before running pytest. Passing offline tests does not establish live Google permissions, quotas, remote CI completion, or package publication; release evidence belongs in [the audit record](docs/APP_AUDIT.md).

See [CONTRIBUTING.md](CONTRIBUTING.md) for change/release checks and [CHANGELOG.md](CHANGELOG.md) for changes.

## License

[MIT](LICENSE). Original work copyright 2025 Amin Foroutan; project contributions copyright 2025-2026 GiorgiKemo.
