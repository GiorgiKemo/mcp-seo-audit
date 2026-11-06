# Round 3 review: not ready for production yet

Most of the Round 2 fixes are correct. But the patch set adds one confirmed crash and one behaviour regression, and it breaks at least three existing test files. I couldn't run the full suite because this machine only has Python 3.9 and the project requires 3.11 or newer. I confirmed the findings below by reading the code and running small scripts directly.

## 1. Round 2 issues

| # | Issue | Status |
|---|---|---|
| 1 | `urlsplit` NameError | ✅ Import added (`seo_reporting.py:6`) |
| 2 | Robots state machine, 429 handling, policy overwrite | ✅ Groups reset correctly, 429 and 5xx are treated as a full block, `blocked_all` now comes from `RobotsPolicy` |
| 3 | Relative vs absolute canonical dedup | ✅ Both are resolved with `urljoin(final_url, …)` before comparing |
| 4 | HTTP Link hreflangs dropped | ✅ `alternate_languages` is now built from the merged `hreflangs` list |
| 5 | Trailing slash conflating `/product` and `/product/` | ✅ The path is kept as-is when normalising |
| 6 | Unicode "²" in breadcrumb position | ✅ The `[1-9]\d*` check rejects "²" before `int()` runs |
| 7 | `delete_audit_project` hint | ✅ Now `destructiveHint=True`, but existing tests weren't updated (see below) |
| 8 | Crawl-delay splitting groups | ✅ `crawl-delay` no longer sets `has_rule` |
| 9 | Canonical rules that never fired | ✅ They are now added to `issues` and mapped in `RULES` |

The claim that 12 tests cover all of the above isn't accurate. `test_fixes.py` has no tests for items 1, 3, 4, 5, 7 or 9. A test for item 5 would have caught the crash below.

## 2. New problems

**Blocking: a malformed canonical crashes the whole audit report** (`seo_reporting.py:157-176`)
The new lookup code calls `urlsplit(target_url).port`, and Python raises an error for an out-of-range or non-numeric port. I reproduced it:
```
canonicals=["https://example.com:99999/"]  →  build_audit_report raises ValueError: Port out of range 0-65535
```
One page with a broken or hostile `<link rel=canonical>` makes `site_audit` fail. A monitored project containing such a page fails on every scheduled run. **Fix:** wrap the normalisation in a helper that returns `None` when it hits that error, or skip invalid ports. Add a test using the bad-port canonical.

**High: diagnostics now trusts incomplete renders** (`seo_diagnostics.py:122-123`)
The `status != "complete"`, `js_errors` and `resource_redirects` checks were removed from `eligible()`. This wasn't one of the Round 2 items. Now `seo_reporting._rendering_complete` marks a page as incomplete while diagnostics still runs rules against it. I confirmed that `structured_product_required` fires for `{"status": "partial"}`, for `resource_redirects` and for `js_errors`. This contradicts the regression test `test_report_extensions.py:53-61`, so three of its cases will fail. **Fix:** revert this hunk.

**High: the existing suite will be red**
- `test_reliability.py:221-234`: `delete_audit_project` isn't in `write_tools` or `destructive_tools`, so the readOnly and destructive annotation checks fail.
- `test_mcp_protocol.py:86`: still asserts 43 tools.
- `test_report_extensions.py:53`: the three failures above.

Only `test_gsc_server.py` was updated, which suggests just `test_fixes.py` and `test_gsc_server.py` were run.

**Medium: blocked redirects no longer show up in the action plan** (`gsc_server.py:2836-2875`)
A redirect blocked by robots.txt or by the external-redirect check used to become a high-severity `fetch_error` finding. It's now recorded under the states `external_redirect` or `robots_blocked_redirect`. `build_audit_report` only creates issues for `fetch_error`, so these pages are silently dropped from the findings. Either add rules for the new states or accept the change on purpose. Also, the fallback `fetch_error` dict in that branch is missing the `notes` key that other branches include.

**Low**
- `README.md:8` and `docs/APP_AUDIT.md:9` still say 43 tools.
- The canonical http-downgrade and cross-host messages are now added to both `issues` and `notes`, so they appear twice.
- The normalised key drops port 443 on `http://` and port 80 on `https://`, which are non-default ports for those schemes.
- `_parse_http_link_headers` splits on commas inside quoted parameters such as `title="a, b"`.
- `inspect_robots_txt` correctly treats a 429 as a block. But a 401 or 403 is reported as "no crawl restrictions", which matches Google's current documentation, so that one is fine.

## 3. What to do before release

1. Guard the `.port` call in `seo_reporting.py` and add a test for it.
2. Revert the `eligible()` change in `seo_diagnostics.py`.
3. Update `test_reliability.py` (add `delete_audit_project` to `write_tools` and `destructive_tools`) and `test_mcp_protocol.py` (44 tools). Then run the **full** suite on Python 3.11 or newer.
4. Decide how blocked redirects should be reported, then update the README tool count.

The first three are small changes. Once they're done and the full suite passes on 3.11+, I'd consider it ready to ship.
