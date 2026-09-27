# Contributing

Use issues for reproducible bugs and focused feature proposals. Follow [SECURITY.md](SECURITY.md) for vulnerabilities. Do not include credentials, private Search Console data, tokens, real audit databases, or customer page content in issues, fixtures, or pull requests.

## Local setup

Use Python 3.11 or later in a virtual environment:

```sh
python -m venv .venv
```

Activate `.venv/Scripts/Activate.ps1` in Windows PowerShell or `source .venv/bin/activate` on macOS/Linux, then:

```sh
python -m pip install -e ".[dev]"
python -m pytest -q
```

For changes to rendered crawling:

```sh
python -m pip install -e ".[dev,browser]"
python -m playwright install chromium
```

Set `SEO_AUDIT_TEST_BROWSER=1` before running pytest to opt into the installed-browser integration checks. Keep browser sandboxing enabled. Use temporary local fixtures for browser checks. The default test configuration isolates Google credentials and blocks uncontrolled network access; never replace those protections with a live key to make a test pass.

On Linux, install OS dependencies with `python -m playwright install --with-deps chromium`. For Ubuntu's `No usable sandbox` error, use [Chromium's per-executable AppArmor setup](https://chromium.googlesource.com/chromium/src/+/main/docs/security/apparmor-userns-restrictions.md): the administrator grants `userns` to the exact trusted Chromium/headless-shell paths and reloads that profile. [Our workflow](.github/workflows/tests.yml) generates the paths from its dedicated browser install directory, loads only the temporary profile, and removes it after testing. Keep `chromium_sandbox=True`; do not replace this with `--no-sandbox` or a global user-namespace restriction change. Browser upgrades may require updated paths.

## Change expectations

- Keep changes focused and preserve stable rule IDs and existing report semantics where possible.
- Add regression coverage for changed behavior, including meaningful negative cases and incomplete evidence. Missing data must not silently become a passing check or a zero metric.
- Keep every fetch bounded and validate redirects and subresources. Do not reintroduce unchecked browser/network paths into the rendered crawler.
- Treat provider/page exception text as potentially sensitive. Avoid logging raw request URLs containing keys or credentials.
- Apply truthful MCP annotations. Local retention can delete old snapshots, and Google-write retries can duplicate a mutation.
- Scheduling stays explicit. Importing a module or starting the stdio server must not start monitoring, create projects, send alerts, or modify Google accounts.
- Durable storage changes require a reviewed schema/version migration strategy and tests against old databases. Never silently reset an incompatible store.
- Update documentation and the changelog when public inputs, behavior, configuration, limitations, or packaging change.

The project is a local stdio tool. Hosted/multi-tenant features need a separate architecture and security proposal, rather than merely enabling an HTTP transport.

## Checks before a pull request

```sh
python -m pytest -q
python -m build
python -m pip_audit
git diff --check
```

Inspect the wheel/source archive to confirm all runtime modules and license files are included and no local credentials or databases are packaged. For browser changes, run the relevant integration fixtures with installed Chromium. For persistence changes, test retention, transaction rollback, concurrent claims, restart recovery, and cancellation.

Describe the user-visible problem, resulting behavior, checks actually run, and remaining limitations. Distinguish mocked provider tests from live account verification. A configured workflow is not evidence of a successful remote run; link the completed run when available.

## Release checklist

1. Synchronize the package version, changelog, registry metadata, and README instructions.
2. Update and review `requirements.lock` when runtime dependencies change, retaining hashes and supported-platform coverage. Verify a clean `pip install --require-hashes -r requirements.lock` followed by `pip install --no-deps .`.
3. Run tests, the package build, dependency audit, and applicable real-browser/Docker checks. Review skipped checks and disclose them.
4. Validate the MCP stdio workflow from the built package, not only the source checkout. Test both a clean data directory and existing supported storage.
5. Review staged files and package contents for credentials, tokens, customer data, generated files, and unrelated work.
6. Commit and push only authorized changes, then inspect the remote CI result.
7. Treat PyPI upload, MCP Registry publication, GitHub releases, and deployment as separate actions requiring the maintainer's release decision. Updating `server.json` or pushing a commit does not perform them.

The project uses the [MIT License](LICENSE). Preserve existing copyright and permission notices in redistributed copies.
