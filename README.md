# mcp-seo-audit

A Model Context Protocol (MCP) server for SEO auditing, Google Search Console analytics, URL indexing, Core Web Vitals, and site analysis. Works with Claude Code, Claude Desktop, Cursor, and any MCP-compatible client.

Forked from [AminForou/mcp-gsc](https://github.com/AminForou/mcp-gsc) and rewritten with 23 tools, bug fixes, and a full test suite.

---

## What It Does

| Category | Tools | Description |
|----------|-------|-------------|
| **Property Management** | `list_properties`, `add_site`, `delete_site` | List, add, and remove GSC properties |
| **Search Analytics** | `get_search_analytics`, `get_advanced_search_analytics`, `get_performance_overview`, `get_search_by_page_query`, `compare_search_periods` | Query clicks, impressions, CTR, position with filtering, dimensions, and period comparison |
| **URL Inspection** | `inspect_url`, `batch_inspect_urls` | Check indexing status, crawl info, canonical, robots for one or many URLs |
| **Indexing API** | `request_indexing`, `request_removal`, `check_indexing_notification`, `batch_request_indexing` | Submit/remove URLs from Google's index via the Indexing API |
| **Sitemaps** | `get_sitemaps`, `submit_sitemap`, `delete_sitemap` | List, submit, and delete sitemaps |
| **Core Web Vitals** | `get_core_web_vitals` | LCP, FID, CLS, INP, TTFB via the Chrome UX Report (CrUX) API |
| **SEO Analysis** | `find_striking_distance_keywords`, `detect_cannibalization`, `split_branded_queries` | Find keywords at positions 5-20, detect pages competing for the same query, split branded vs non-branded traffic |
| **Site Audit** | `site_audit` | All-in-one report: sitemap health, indexing status, canonical mismatches, performance summary |
| **Auth** | `reauthenticate` | Switch Google accounts by clearing cached OAuth tokens |

**23 tools total.**

---

## Setup

### 1. Google API Credentials

#### OAuth (recommended)

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Enable the **Search Console API** and **Web Search Indexing API**
3. Create an **OAuth 2.0 Client ID** (Desktop app)
4. Download `client_secrets.json`

#### Service Account

1. Create a service account in Google Cloud Console
2. Download the JSON key file
3. Add the service account email to your GSC properties

### 2. Install

```bash
git clone https://github.com/GiorgiKemo/mcp-gsc.git
cd mcp-gsc
python -m venv .venv

# Activate:
# macOS/Linux: source .venv/bin/activate
# Windows:     .venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Configure Your MCP Client

#### Claude Code (`~/.claude/settings.json`)

```json
{
  "mcpServers": {
    "seo-audit": {
      "command": "/path/to/mcp-gsc/.venv/bin/python",
      "args": ["/path/to/mcp-gsc/gsc_server.py"],
      "env": {
        "GSC_OAUTH_CLIENT_SECRETS_FILE": "/path/to/client_secrets.json"
      }
    }
  }
}
```

#### Claude Desktop (`claude_desktop_config.json`)

Same JSON structure — see [Claude Desktop MCP docs](https://docs.anthropic.com/en/docs/claude-code/mcp) for config file location.

### 4. Optional: CrUX API Key

For Core Web Vitals data, set `CRUX_API_KEY` in the env block:

```json
"env": {
  "GSC_OAUTH_CLIENT_SECRETS_FILE": "/path/to/client_secrets.json",
  "CRUX_API_KEY": "your-google-api-key"
}
```

Get one from [Google AI Studio](https://makersuite.google.com/app/apikey) or the Cloud Console with CrUX API enabled.

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GSC_OAUTH_CLIENT_SECRETS_FILE` | OAuth | `client_secrets.json` | Path to OAuth client secrets |
| `GSC_CREDENTIALS_PATH` | Service account | `service_account_credentials.json` | Path to service account key |
| `GSC_SKIP_OAUTH` | No | `false` | Set to `true` to skip OAuth and use service account only |
| `GSC_DATA_STATE` | No | `all` | `all` = fresh data matching GSC dashboard, `final` = confirmed data (2-3 day lag) |
| `CRUX_API_KEY` | No | — | Google API key for Core Web Vitals (CrUX) |

---

## Example Prompts

```
"List my GSC properties"
"Show search analytics for cdljobscenter.com last 28 days"
"Find striking distance keywords for my site"
"Detect keyword cannibalization"
"Run a full site audit"
"Check Core Web Vitals for cdljobscenter.com"
"Inspect indexing status of these URLs: /jobs, /companies, /pricing"
"Request indexing for https://mysite.com/new-page"
"Compare search performance this month vs last month"
```

---

## Tests

71 tests covering all 23 tools with mocked Google API calls:

```bash
# Activate venv first
python -m pytest test_gsc_server.py -v
```

---

## What Changed From the Original

- **23 tools** (up from ~10) — added Indexing API, CrUX, SEO analysis, site audit
- **7 bug fixes** — sort direction mapping, origin/URL detection, empty rows crash, API key leak, blocking sleep, service caching, stale cache on reauth
- **71-test QA suite** — full coverage with mocked API calls
- **Security** — API keys redacted from error messages
- **Performance** — Google API service objects cached, async sleep instead of blocking

---

## License

MIT. See [LICENSE](LICENSE).

Based on [AminForou/mcp-gsc](https://github.com/AminForou/mcp-gsc).
