# Follow-up review: `mcp-seo-audit` working-tree fixes

**Verdict: not ready to merge yet.** Most patches do what they set out to do. But one patch makes `inspect_robots_txt` give wrong answers in cases it used to get right, and two others cause new false positives. I reproduced the first and third robots findings (§2, items 1 and 3) by running the logic directly. I found the rest by reading the code.

**I couldn't run the test suites.** The project needs Python ≥3.11, and this machine only has the system Python 3.9.6 with no pytest, venv or `uv`. That means the claim that `test_fixes.py` and the updated `test_gsc_server.py` pass is unconfirmed. Also, `test_fixes.py` has only 9 tests, and none of them cover `inspect_robots_txt`.

---

## 1. Correctness: did the patches fix their targets?

| Area | Status | Notes |
|---|---|---|
| Robots product-token regex (`[a-z0-9_-]`) | ✅ | Fixed |
| Agent token exact match / parent matching | ✅ (see §2.3) | Exact match now scores 1000+len, ranking it above parent and `*` groups |
| `RedirectBlocked` for external and robots-blocked redirects | ✅ | Upstream handlers catch it correctly (`seo_sitemaps.py:168` catches `Exception`; rendering catches `Exception`). SSRF checks still sit outside the validator, so they still show up as `fetch_error`. Good. |
| Canonical scoped to `<head>` plus body-only warning | ✅ | Falls back to the whole document when there's no `<head>` |
| HTTP `Link` header extraction | ⚠️ | Works, but causes a false positive (§2.2) |
| `inspect_robots_txt`: 5xx handling and `RobotsPolicy` reuse | ❌ | The reuse is dead code, 429 is mishandled, and the group parsing got worse (§2.1) |
| Deadline on the initial robots load | ✅ | `asyncio.timeout` is fine given `requires-python >=3.11` |
| Sitemap DTD/entity rejection | ✅ | Added to both parsers (`gsc_server.py:1035` and `seo_sitemaps.py:52`) |
| Sitemap namespaces (0.84, https) | ✅ | |
| Batch URL parsing (JSON array or one per line) | ✅ | |
| `safe_finish_failure` / lease handling | ✅ | Matching `"lease"` in the exception text is fragile but works with the current message (`seo_storage.py:229`) |
| `delete_project` cascade | ✅ | Runs in one write transaction (see §2.6) |
| Lease 900s vs 600s audit timeout | ✅ | 300s buffer |
| Breadcrumb string `position`, article date with a space | ✅ | Minor edge cases (§2.8) |
| Granular canonical rule IDs | ⚠️ | 2 of the 6 new rules can never fire (§2.5) |
| `canonical_target_unverified` | ✅ | Added to the comparison dependency logic |

## 2. Regressions and new defects

### High

**1. `inspect_robots_txt` misreads robots groups and reports false "blocks all" results** (`gsc_server.py:2441-2443`, `2425-2426`)
- The line meant to reset the group never runs:
  ```python
  if any(k in {"allow","disallow"} for k in (raw_line.lower() for _ in [0])):
  ```
  This checks whether the whole line (e.g. `"user-agent: googlebot"`) is the string `"allow"` or `"disallow"`, which is never true. I confirmed it evaluates to `False`. So `current_agents` only ever grows, and once `User-agent: *` appears, every later group's rules get counted as wildcard rules.
- Example: `User-agent: *` / `Disallow: /a` / `User-agent: Googlebot` / `Disallow: /`. This now reports **"Blocks all crawling for user-agent *: YES"**, which is wrong. The old code got this case right.
- The `RobotsPolicy` reuse does nothing: `blocked_all = not policy.can_fetch("/")` at line 2426 is overwritten by the old heuristic at line 2457.
- 429 is excluded from the 4xx early return, so a 429 error page falls through and gets parsed as robots rules. `_load_crawl_robots` already treats 429 like a 5xx. This tool should do the same.
- **Fix:** delete the hand-written parser. Get `blocked_all` from `RobotsPolicy`, and have `RobotsPolicy` expose the rules from the selected group plus the sitemap list. Then the tool and the crawler can't disagree.

### Medium

**2. False "Multiple canonical tags" (high severity) when the header and HTML agree** (`gsc_server.py:780-782`)
Header and HTML canonicals are de-duplicated by exact string. A page with `<link rel=canonical href="/p">` and `Link: <https://e.com/p>; rel=canonical` therefore gets flagged as having two canonicals. **Fix:** resolve both with `urljoin(final_url, …)` before comparing, and keep the original HTML value for the "not absolute" check.

**3. `Crawl-delay` now splits robots groups, which is not what RFC 9309 or Google do** (`seo_robots.py:90`)
For `User-agent: a` / `Crawl-delay: 5` / `User-agent: mcp-seo-audit` / `Disallow: /x`, both agents should share `Disallow: /x`. Google ignores `Crawl-delay` when grouping. After this change, agent `a` can fetch `/x` (I confirmed `blocked=False`). **Fix:** go back to `if has_rule:` and attach the crawl-delay to the group's agents without closing the group.

**4. The relaxed URL matching for canonical targets can produce false `canonical_unavailable` (high severity)** (`seo_reporting.py:157-178`)
The fallback strips trailing slashes, but `/a` and `/a/` are different URLs. If a canonical points to `/a` (not crawled) and `/a/` was crawled and returned 404, a high-severity finding is raised against the wrong resource. **Fix:** lowercase only the scheme and host, drop default ports, and leave the path untouched. If you keep slash folding, downgrade such matches to `canonical_target_unverified`.

**5. Two new rules can never fire** (`seo_reporting.py:44-45` vs `gsc_server.py:879`, `885`)
"Canonical URL uses http on an https page" and "Canonical points to another host" go into `notes`, but the rule matching only looks at `findings` built from `issues`. So `canonical_http_downgrade` and `canonical_cross_domain` are dead. Either promote those messages to issues (at low severity) or remove the two rules.

**6. The `delete_audit_project` tool is marked non-destructive** (`seo_monitoring.py:96`, `write` at line 72)
It uses the `write` annotation, which has `destructiveHint=False`, for a permanent cascading delete. MCP clients rely on that hint to decide whether to ask for confirmation. Use an annotation with `destructiveHint=True`. Separately, deleting a project while an audit is running on it is allowed. The worker then fails safely on the lease check, but returning a "running" error would be cleaner.

### Low

7. **Relaxed `eligible()` check** (`seo_diagnostics.py:123`): pages with `blocked_requests`, `pending_requests` or a non-`complete` status are now diagnosed on a possibly incomplete DOM. That can produce false "structured data missing" findings. `coverage.rendering_incomplete` still flags these pages, which limits the impact. A middle ground would be to exclude only `blocked_requests` and `pending_requests` and allow `js_errors`.
8. `str.isdigit()` accepts characters like `"²"`, and `int("²")` raises `ValueError`. Check that this sits inside a guarded block, or use `re.fullmatch(r"\d+", s, re.ASCII)`. The date regex's `\s` also accepts tabs and newlines; a literal space is enough.
9. The `Link` header split on `,(?![^<]*>)` breaks on commas inside quoted parameters (e.g. `title="a, b"`). This is rare.
10. Robots agent matching only uses the leading product token. A full browser-style user-agent such as `Mozilla/5.0 (… MyBot/1.0)` would never match `User-agent: mybot`. This is fine today because only `mcp-seo-audit` and `*` are passed in, but the constraint should be documented.
11. The `fetch_error` record in the `RedirectBlocked` handler omits `notes`. That matches the existing generic handler, so it's harmless.

## 3. What to fix before merge

The codebase is otherwise in good shape: lease and cancellation handling, SSRF scoping, robots fail-closed behaviour and sitemap hardening are all solid. Fixes needed:

1. **Must fix:** item 1 (the `inspect_robots_txt` regression) and item 6 (the delete tool's annotation).
2. **Should fix:** items 2, 3, 4 and 5 (false positives and the RFC deviation).
3. Add tests for: a mixed-group `inspect_robots_txt` file; a 429 robots response; header plus relative HTML canonical; a crawl-delay between `User-agent` lines; and a canonical target that differs only by a trailing slash.
4. Run the full suite in a Python 3.11+ environment and confirm the 44-tool count.
