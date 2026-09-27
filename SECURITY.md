# Security policy

## Supported deployment

The supported application is a trusted-user, local stdio MCP server and an explicitly started local monitoring worker. Version 2.1.x is the current source line for fixes. A source update does not imply that a corresponding package or registry release has been published.

There is no HTTP authentication service, tenant boundary, user-management layer, or authorization mechanism for sharing one server/database across unrelated users. Do not expose stdio through an unauthenticated public bridge. A hosted deployment needs separately designed authentication, authorization, process/filesystem isolation, rate limits, controlled egress, operational monitoring, and an independent security review.

## Network and browser controls

The guarded HTTP fetcher validates resolved IP addresses and connects directly to an approved address with the original HTTP host and TLS server name. The default policy rejects private, loopback, reserved, and unsafe translation/tunneling destinations. Redirect hops are rechecked. Response sizes and crawl duration/page counts are bounded. Environment proxy settings are not used for guarded site fetches.

The optional rendered crawler uses a sandboxed Chromium instance with a fresh context. Browser resource requests are fulfilled through guarded HTTP fetching; unintended direct browser networking is blocked through a reserved non-listening proxy endpoint. Requests are limited to GET/HEAD, service workers and WebSockets are blocked, and browser cookies/authorization are not forwarded to resource fetches. Rendering has request, byte, timeout, and DOM-size limits. These are defense-in-depth controls, not a claim that arbitrary hostile browser code is risk-free.

Local Lighthouse is disabled unless `SEO_AUDIT_ENABLE_LOCAL_LIGHTHOUSE=true`. It starts a separate Chrome process and does not inherit the rendered crawler's resource interception or DNS-to-connection pinning. Run it only against trusted sites, preferably in an isolated environment with controlled egress. Keep `LIGHTHOUSE_NO_SANDBOX=false`. `SEO_AUDIT_ALLOW_NPX_LIGHTHOUSE=true` explicitly permits a package download and executable launch; installing a reviewed Lighthouse version ahead of time is more reproducible.

Keep `SEO_AUDIT_ALLOW_PRIVATE_URLS=false` except for deliberate trusted local testing. Enabling it expands access for the configured process and should not be combined with untrusted target input.

## Credentials and saved data

- Keep OAuth client files, service-account keys, API keys, tokens, and real audit databases outside version control and public issue attachments.
- Google mutations are gated by `SEO_AUDIT_ENABLE_WRITE_TOOLS`, which defaults to false. This application gate does not narrow already granted Google scopes.
- OAuth replacement is atomic and failed reauthentication preserves the existing token. Tokens default to the local user-data directory; `GSC_TOKEN_FILE` can select an explicit destination. Old package-local tokens can still be read during migration.
- SQLite snapshots may contain page metadata, URL parameters, and commercially sensitive information. Storage is not application-encrypted. Protect the data directory and its backups using OS access controls and disk encryption as appropriate.
- Local change events never trigger email, webhooks, or messages. External notification integration must be an explicit, separately reviewed addition.
- Treat remote page content and returned audit evidence as untrusted input, including instructions embedded in titles, scripts, metadata, or error responses. MCP tool metadata is advisory; the client controls approval and execution.

Google reads have bounded retries. Mutations are attempted once to avoid replaying an ambiguous write. Cancelling a request after it has reached Google cannot guarantee that Google did not apply it; verify provider state before retrying manually.

## Report a vulnerability

Use the repository's [Security tab](https://github.com/GiorgiKemo/mcp-seo-audit/security) to check for a private reporting option. If unavailable, open a minimal issue asking the maintainer for a private contact channel; do not post exploit details, keys, account data, or a real audit database publicly.

Include the affected commit/version, operating system, Python version, deployment mode, a minimal sanitized reproduction, and the expected versus observed boundary. Reports involving SSRF, credential disclosure, browser escape, unauthorized Google writes, or cross-project data access are particularly useful. No response-time or remediation SLA is promised by this volunteer-maintained repository.

## Dependency maintenance

Install in an isolated environment, use the checked-in hash-pinned runtime lock for reproducible deployments, keep supported Python/browser versions updated, and run the documented dependency audit before releases. An audit detects known advisories in the scanned dependency set at that time; it does not prove that the application or browser has no vulnerabilities. Browser binaries have their own update lifecycle alongside the Python Playwright package.
