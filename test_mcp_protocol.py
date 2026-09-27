"""Exercise the real stdio protocol and HTTP crawler without Google accounts."""

import asyncio
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import threading

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import pytest


@pytest.fixture
def audit_http_fixture():
    state = {"fixed_title": False, "requests": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["requests"].append(self.path)
            if self.path == "/robots.txt":
                status, content_type, body = 200, "text/plain", "User-agent: *\nAllow: /\n"
            elif self.path == "/":
                title = "<title>Protocol fixture with repaired page title</title>" if state["fixed_title"] else ""
                status, content_type, body = 200, "text/html", f"""<!doctype html>
<html lang="en"><head>{title}
<meta name="description" content="A local deterministic fixture for testing the SEO audit protocol, real HTTP fetching, and comparison of captured reports." />
<meta name="viewport" content="width=device-width, initial-scale=1" />
</head><body><h1>Protocol test fixture</h1><p>This local page exercises the complete audit workflow.</p></body></html>"""
            else:
                status, content_type, body = 404, "text/plain", "Not found"
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_stdio_audit_and_fix_comparison(audit_http_fixture, tmp_path):
    start_url, state = audit_http_fixture
    server_path = Path(__file__).resolve().with_name("gsc_server.py")
    parameters = StdioServerParameters(
        command=sys.executable,
        args=[str(server_path)],
        cwd=str(tmp_path),
        env={
            "GSC_SKIP_OAUTH": "true",
            "GSC_DATA_STATE": "all",
            "GSC_CREDENTIALS_PATH": str(tmp_path / "no-service-account.json"),
            "GSC_OAUTH_CLIENT_SECRETS_FILE": str(tmp_path / "no-client-secrets.json"),
            "GOOGLE_APPLICATION_CREDENTIALS": str(tmp_path / "no-application-credentials.json"),
            "SEO_AUDIT_ALLOW_PRIVATE_URLS": "true",
            "SEO_AUDIT_ENABLE_WRITE_TOOLS": "false",
            "SEO_AUDIT_ALLOW_NPX_LIGHTHOUSE": "false",
            "CRUX_API_KEY": "",
            "PAGESPEED_API_KEY": "",
            "GOOGLE_API_KEY": "",
            "NO_PROXY": "127.0.0.1,localhost",
            "PYTHONIOENCODING": "utf-8",
        },
    )

    async def scenario():
        async with stdio_client(parameters) as (read, write):
            async with ClientSession(read, write) as session:
                initialized = await session.initialize()
                assert initialized.serverInfo.name == "mcp-seo-audit"
                registered = {tool.name: tool for tool in (await session.list_tools()).tools}
                assert len(registered) == 43
                assert all(tool.annotations is not None for tool in registered.values())
                assert registered["get_seo_audit_report"].annotations.readOnlyHint is True
                assert registered["compare_seo_audits"].annotations.openWorldHint is False
                assert registered["request_removal"].annotations.destructiveHint is True
                assert registered["reauthenticate"].annotations.readOnlyHint is False

                async def call(name, arguments):
                    response = await session.call_tool(
                        name, arguments, read_timeout_seconds=timedelta(seconds=10)
                    )
                    assert not response.isError
                    assert isinstance(response.structuredContent, dict)
                    # FastMCP advertises Dict[str, Any] outputs under `result`.
                    payload = response.structuredContent["result"]
                    assert "error" not in payload
                    return payload

                arguments = {"start_url": start_url, "max_pages": 1, "respect_robots": True}
                baseline = await call("get_seo_audit_report", arguments)
                assert baseline["schema_version"] == "1.0"
                assert baseline["coverage"]["html_pages"] == 1
                assert baseline["pages"][0]["status"] == 200
                assert any(issue["rule_id"] == "missing_title" for issue in baseline["issues"])

                state["fixed_title"] = True
                current = await call("get_seo_audit_report", arguments)
                assert not any(issue["rule_id"] == "missing_title" for issue in current["issues"])
                assert current["pages"][0]["title"] == "Protocol fixture with repaired page title"
                comparison = await call("compare_seo_audits", {
                    "baseline_json": json.dumps(baseline), "current_json": json.dumps(current)
                })
                assert {"rule_id": "missing_title", "url": start_url, "severity": "high"} in comparison["resolved"]
                assert comparison["counts"]["unverified"] == 0

    asyncio.run(asyncio.wait_for(scenario(), timeout=30))
    assert state["requests"].count("/") == 2
    assert state["requests"].count("/robots.txt") == 2
    assert set(state["requests"]) == {"/", "/robots.txt"}
