# Peer review of `AUDIT_REVIEW.md`

I checked each finding against the source by reading and tracing the code. I couldn't run anything: the robots-parser probe needed approval and there's no venv with Playwright, so the "Traced" verdicts below are worked through by hand, not executed.

**Bottom line:** about 12 of the 21 findings hold up, 4 are false positives (one with a fix that would introduce a bug), and the rest are design choices or overstated. The audit also missed the most serious robots bug, which partly overlaps with Issue 2.

## 1. Verdict per issue

| # | Verdict | Notes |
|---|---|---|
| 1 | **Half wrong, and arguably intended** | The "empty `Disallow:`" claim is false: `has_rule = True` is set at `seo_robots.py:101` *before* the empty-value `continue` on line 102, so the group does close. The crawl-delay-only merge is real, but it matches RFC 9309 and, as far as I recall, Google's reference parser (only allow/disallow end a group). The comment at line 112 says it's on purpose, and `test_robots_policy.py:132` adds `Disallow:` after each `Crawl-delay` for this reason. **Low**, should be documented rather than called Critical. |
| 2 | **Real, but the impact is misdiagnosed** | Dropping `360Spider` is harmless because that group never applied to our crawler, and `bingbot/2.0 → bingbot` is the intended suffix stripping. The real damage comes from combining this with substring matching (see Blind spot A). |
| 3 | **False positive; the fix is harmful** | Google's documented semantics are that `X-Robots-Tag: otherbot: noindex, nofollow` scopes *both* directives to `otherbot`. The current scope carry-over (`gsc_server.py:686-697`) is correct. The proposed fix would apply bingbot's `noarchive` to Googlebot. **Reject.** |
| 4 | **Confirmed, High** | `validate_redirect` raises inside `_fetch_url` (`gsc_server.py:574`), so the branch at 2637-2650 never runs in production. The test at `test_gsc_server.py:1272` mocks `_fetch_url` to return an off-origin response, which the real function never does, so the dead code is hidden. Side effects: a false `fetch_error` with `status: "ERROR"`, and `complete_for_discovered_links` is forced to False. |
| 5 | **Likely a false positive on the mechanism** | As far as I know, Playwright `route()` doesn't intercept `data:` or `blob:` URLs, so they never reach `_origin`. One browser test would settle it: a page with a `data:` image should give empty `blocked_requests`. The underlying concern is still real, though: `eligible()` (`seo_diagnostics.py:123`) drops a page for *any* blocked request or `pageerror`. A third-party origin that is robots-disallowed, a 9th resource origin (`gsc_server.py:2593`), or a single JS error removes the page from schema and hreflang checks. That should be reframed as over-strict eligibility. |
| 6 | Plausible, **Low** | Google's docs type `position` as Integer. Accepting digit strings reduces noise, but it's a leniency choice, not a spec violation. |
| 7 | **Check against the docs first** | The asymmetry looks deliberate: AggregateOffer requires `priceCurrency`, Offer doesn't. That matches Product-snippet guidance, where Merchant listings are the stricter case. Don't treat it as a bug without checking the current Google docs. |
| 8 | Valid, **Low** | Schema.org and Google say ISO 8601, which uses `T`. Accepting a space is a leniency choice. |
| 9 | Valid, but the proposed fix is wrong | A timeout or DNS error on our side doesn't prove the canonical target is unavailable, and a `fetch_error` finding is already raised for it. Emit something like a low-severity `canonical_target_unverified` instead of raising a High `canonical_unavailable`. |
| 10 | **Confirmed, Medium** | Four distinct messages map to `canonical_review` (`seo_reporting.py:41`), so the comparison keyed on `(rule_id, url)` merges them. |
| 11 | Valid feature gap, **Low/Medium** | Non-HTML responses are skipped anyway (`gsc_server.py:2661`), so PDFs don't matter here. The real gap is HTML pages that declare the canonical only in a `Link:` header, which get a false "No canonical", plus header/HTML conflicts going undetected. |
| 12 | Valid gap, **Medium** (not High) | Note that the foreign keys have no `ON DELETE CASCADE` (`seo_storage.py:91,99`), so a delete has to remove `audits` and `events` rows explicitly. |
| 13 | **Design choice** | Capping the retry at the interval means a failing daily audit just keeps its daily cadence, which is a sensible "never later than schedule". The proposed fix would make failing projects run *less* often than scheduled. **Informational.** |
| 14 | **Real crash, wrong trigger** | Projects don't store `max_seconds`, so the crawl uses the default 180 s budget, which includes sitemap discovery. The 600/660 s race is practically unreachable. The real defect: if the lease is lost, `finish_success`/`finish_failure` raise `ValueError` from inside the `except` handlers (`seo_monitoring.py:32-33`). The outer `except Exception` calls `finish_failure` again, it raises again, and `main()` exits with status 1. A plausible trigger is laptop sleep: the lease uses wall-clock `time.time()`. |
| 15 | **Confirmed, Medium** | It also misses one point: only 404 is special-cased, so a 403 or 5xx error page is parsed as robots content. Google treats 5xx as disallow-all and 4xx as allow-all. |
| 16 | Confirmed, **Low** | `/sitemap.xml` becomes `https://sitemap.xml`. RFC 9309 requires absolute sitemap URLs, so this is a non-standard input. |
| 17 | Valid, overstated | Discovery isolates errors per document (`seo_sitemaps.py:162`), so only that one file fails; the "entire inventory is discarded" claim is wrong. Also, `gsc_server._parse_sitemap_document` ignores namespaces entirely, so the two tools disagree on the same file. |
| 18 | Valid, **Low**; the fix is risky | Splitting on commas breaks valid URLs that contain commas. Accept a JSON array or newlines only. |
| 19 | Valid, **Low** | `mcp` is pinned `<2`. Prefer the public `len(await mcp.list_tools())` over `getattr` on private attributes. |
| 20 | **False positive** | `run_project_audit` deletes old snapshots to enforce retention (`seo_storage.py:266`, and its docstring says so). `destructiveHint=True` is correct. |
| 21 | **Not a defect** | Already declared in `pyproject.toml:6`, `.python-version`, and `README.md:16`. |

## 2. Blind spots the audit missed

**A. Robots user-agent substring matching (High).** `seo_robots.py:127` uses `agent in crawler`. RFC 9309 and Google compare the full product token, case-insensitively. Combined with the digit stripping from Issue 2, I traced these cases:
- `User-agent: A6-Indexer` (a real bot) becomes `"a"`, and `"a" in "mcp-seo-audit"` is true. That group has specificity 1, beats `*`, and a `Disallow: /` meant for that bot blocks the whole audit.
- `User-agent: seo` or `User-agent: audit` has the same effect.

Fix: take the product token with `[A-Za-z0-9_-]+` and compare it for **equality** with the crawler's token.

**B. Robots-blocked redirects are reported as fetch errors.** The same mechanism as Issue 4 turns "Redirect blocked by robots.txt" into `fetch_error` instead of adding the URL to `blocked`.

**C. Two divergent sitemap parsers.** `gsc_server._parse_sitemap_document` (used by the `analyze_sitemap` tool) has no DTD/ENTITY rejection, unlike `seo_sitemaps.py:52`. Modern expat limits entity-expansion attacks, but this is inconsistent hardening, and the namespace handling also differs (see Issue 17).

**D. Canonical lookup isn't normalized (plausible).** `seo_reporting.py:151-156` keys `observed` on raw URL strings, while diagnostics normalizes through `_url()`. A trailing slash, host case or default port difference means a broken canonical target goes unflagged.

## 3. Remediation priority

**P0 (correctness of the audit's own output):**
1. Blind spot A together with Issue 2: one fix to token extraction and equality matching.
2. Issue 4 together with blind spot B: return the unfollowed 3xx response, or raise a typed exception that the crawler records as `external_redirect` or `blocked`. Keep blocking the off-origin hop before it is fetched; that guard is correct. Fix the misleading mock in the test.
3. Issue 14 reframed: catch lease-loss `ValueError` around the `finish_*` calls in `execute_claim` and log it instead of crashing. A heartbeat is optional.

**P1:**
- Issue 15: reuse `RobotsPolicy` and handle the HTTP status correctly.
- Issue 10: split into granular rule IDs. This changes rule IDs, so expect one-time `new`/`resolved` noise when comparing against old baselines.
- Issue 5 reframed: decide which rendering imperfections should really disqualify a page.
- Issue 9: use the "unverified" variant.
- Issue 17 and blind spot C: share one sitemap parser.
- Issue 16.

**P2:** Issues 12, 11, 6, 8, 18 (JSON or newlines only), 19, and 7 after checking the docs.

**Drop or document:** Issue 3 (don't apply the fix), 20, 21, 13, and 1 (document that it's Google-compatible).
