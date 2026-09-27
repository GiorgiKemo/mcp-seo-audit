"""Security boundaries and actual JavaScript rendering, with no external requests."""

import asyncio
import gzip
import importlib.util
import os
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import AsyncMock

import httpx
import pytest

import seo_network as network
import seo_rendering as rendering


@pytest.mark.parametrize("address", [
    "127.0.0.1", "10.0.0.1", "169.254.169.254", "0.0.0.0", "224.0.0.1",
    "::1", "fc00::1", "fe80::1", "ff02::1", "::ffff:127.0.0.1", "192.0.2.1",
    "64:ff9b::7f00:1", "64:ff9b::a9fe:a9fe", "2002:7f00:1::",
])
def test_nonpublic_literal_ips_are_blocked(address):
    with pytest.raises(ValueError, match="non-public"):
        asyncio.run(network._resolve_addresses(address, 80, False))


def test_mixed_public_and_private_dns_fails_closed(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
    ])
    with pytest.raises(ValueError, match="non-public"):
        asyncio.run(network._resolve_addresses("example.com", 443, False))


@pytest.mark.parametrize("url,ip,host,sni", [
    ("https://example.com:8443/path?a=1", "93.184.216.34", "example.com:8443", "example.com"),
    ("https://example.com/path", "2606:4700:4700::1111", "example.com", "example.com"),
    ("http://[2606:4700:4700::1111]:8080/path", "2606:4700:4700::1111", "[2606:4700:4700::1111]:8080", "2606:4700:4700::1111"),
])
def test_transport_pins_ip_preserving_host_port_and_tls_name(monkeypatch, url, ip, host, sni):
    resolver = AsyncMock(return_value=[ip])
    monkeypatch.setattr(network, "_resolve_addresses", resolver)
    seen = []

    async def send(_self, request):
        seen.append(request)
        return httpx.Response(200, content=b"ok")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", send)

    async def run():
        async with httpx.AsyncClient(transport=network.safe_transport(), trust_env=False) as client:
            response = await client.get(url, headers={"Host": "attacker.example"})
            assert str(response.url) == url

    asyncio.run(run())
    assert resolver.await_count == 1
    assert seen[0].url.host == ip
    assert seen[0].headers["host"] == host
    assert seen[0].extensions["sni_hostname"] == sni
    assert seen[0].url.raw_path == httpx.URL(url).raw_path


def test_transport_does_not_pool_connections_across_tls_names():
    transport = network.safe_transport()
    assert transport._transport._pool._max_keepalive_connections == 0
    asyncio.run(transport.aclose())


@pytest.mark.parametrize("url", ["ftp://example.com/file", "http://user:pass@example.com/", "http://[fe80::1%25eth0]/"])
def test_invalid_fetch_url_is_blocked(url):
    with pytest.raises(ValueError):
        asyncio.run(network.safe_fetch_once(url))


@pytest.fixture
def local_server():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            requests.append((self.path, self.headers.get("Host")))
            self.send_response(302 if self.path == "/redirect" else 200)
            if self.path == "/redirect":
                self.send_header("Location", "/final")
                body = b""
            elif self.path == "/gzip":
                self.send_header("Content-Encoding", "gzip")
                body = gzip.compress(b"decoded body")
            else:
                body = b"safe response"
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", requests
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def test_real_fetch_pinning_no_proxy_and_no_automatic_redirects(local_server, monkeypatch):
    url, requests = local_server
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    response = asyncio.run(network.safe_fetch_once(url + "/redirect", allow_private=True))
    assert response.status_code == 302
    assert requests == [("/redirect", url.removeprefix("http://"))]


def test_real_fetch_decodes_once_and_caps_body(local_server):
    url, _ = local_server
    response = asyncio.run(network.safe_fetch_once(url + "/gzip", allow_private=True))
    assert response.text == "decoded body"
    assert "content-encoding" not in response.headers
    with pytest.raises(ValueError, match="limit"):
        asyncio.run(network.safe_fetch_once(url, allow_private=True, max_bytes=3))


@pytest.mark.parametrize("kwargs", [
    {"max_requests": 0}, {"max_requests": 501}, {"max_bytes": 0},
    {"timeout_seconds": 121}, {"settle_ms": -1},
])
def test_render_limits_are_validated(kwargs):
    with pytest.raises(ValueError):
        asyncio.run(rendering.render_page("https://example.com", **kwargs))


def browser_available():
    return os.environ.get("SEO_AUDIT_TEST_BROWSER") == "1" and importlib.util.find_spec("playwright") is not None


@pytest.mark.skipif(not browser_available(), reason="Set SEO_AUDIT_TEST_BROWSER=1 with Playwright Chromium installed.")
def test_real_browser_executes_js_and_guards_network():
    raw_html = """<!doctype html><html><head><title>Raw</title></head><body>
        <h1>Raw heading</h1><script src='/redirect.js'></script>
        <script>
        fetch('/api').then(r=>r.json()).then(data=>{document.querySelector('h1').textContent=data.heading});
        fetch('http://127.0.0.1/private').catch(()=>{});
        fetch('/write', {method:'POST', body:'mutation'}).catch(()=>{});
        const ws = new WebSocket('wss://example.com/socket');
        </script></body></html>"""
    raw = httpx.Response(200, text=raw_html, headers={"content-type": "text/html"}, request=httpx.Request("GET", "https://example.com/"))
    fetched = []

    async def validate(url):
        if urlsplit_host(url) == "127.0.0.1":
            raise ValueError("Private resource blocked by validator.")

    async def fetch(url, **kwargs):
        assert kwargs["follow_redirects"] is False
        fetched.append(url)
        request = httpx.Request("GET", url)
        if url.endswith("/redirect.js"):
            return httpx.Response(302, headers={"location": "/hydrate.js"}, request=request)
        if url.endswith("/hydrate.js"):
            return httpx.Response(200, text="document.title='Rendered title';", headers={"content-type": "application/javascript"}, request=request)
        if url.endswith("/api"):
            return httpx.Response(200, json={"heading": "Rendered heading"}, request=request)
        raise AssertionError(f"Unexpected fetch: {url}")

    result = asyncio.run(rendering.render_page(
        "https://example.com/", raw_response=raw, fetch_callback=fetch,
        resource_validator=validate, settle_ms=1000,
    ))
    assert "<title>Rendered title</title>" in result["rendered_html"]
    assert "<h1>Rendered heading</h1>" in result["rendered_html"]
    assert "<title>Raw</title>" in result["raw_html"]
    assert set(fetched) == {"https://example.com/api", "https://example.com/redirect.js", "https://example.com/hydrate.js"}
    assert result["status"] == "partial"
    reasons = " ".join(item["reason"] for item in result["blocked_requests"])
    assert "GET and HEAD" in reasons and "Private resource" in reasons and "WebSockets" in reasons


def urlsplit_host(url):
    from urllib.parse import urlsplit
    return urlsplit(url).hostname


@pytest.mark.skipif(not browser_available(), reason="Set SEO_AUDIT_TEST_BROWSER=1 with Playwright Chromium installed.")
def test_real_browser_limits_requests():
    raw = httpx.Response(200, text="""<html><body>
        <script>fetch('/resource').catch(()=>{});for(let i=0;i<8;i++)fetch('/extra'+i).catch(()=>{});</script>
        </body></html>""", headers={"content-type": "text/html"}, request=httpx.Request("GET", "https://example.com/"))
    fetched = []

    async def fetch(url, **kwargs):
        fetched.append(url)
        return httpx.Response(200, text="ok", request=httpx.Request("GET", url))

    result = asyncio.run(rendering.render_page(
        "https://example.com/", raw_response=raw, fetch_callback=fetch, max_requests=3, settle_ms=500,
    ))
    assert len(fetched) <= 2
    assert any("request limit" in item["reason"] for item in result["blocked_requests"])


@pytest.mark.skipif(not browser_available(), reason="Set SEO_AUDIT_TEST_BROWSER=1 with Playwright Chromium installed.")
def test_real_browser_blocks_document_and_private_redirects():
    raw = httpx.Response(200, text="""<html><body>
        <iframe src='/document'></iframe><script src='/redirect.js'></script>
        </body></html>""", headers={"content-type": "text/html"}, request=httpx.Request("GET", "https://example.com/"))
    fetched = []

    def resource_validator(url):
        if urlsplit_host(url) == "127.0.0.1":
            raise ValueError("Private redirect blocked.")

    async def fetch(url, **kwargs):
        fetched.append(url)
        location = "https://other.example/" if url.endswith("/document") else "http://127.0.0.1/private"
        return httpx.Response(302, headers={"location": location}, request=httpx.Request("GET", url))

    result = asyncio.run(rendering.render_page(
        "https://example.com/", raw_response=raw, fetch_callback=fetch,
        resource_validator=resource_validator, settle_ms=200,
    ))
    reasons = " ".join(item["reason"] for item in result["blocked_requests"])
    assert "Cross-origin" in reasons and "Private redirect" in reasons
    assert set(fetched) == {"https://example.com/document", "https://example.com/redirect.js"}


@pytest.mark.skipif(not browser_available(), reason="Set SEO_AUDIT_TEST_BROWSER=1 with Playwright Chromium installed.")
def test_real_browser_cancellation_closes_browser_and_pending_fetch(monkeypatch):
    from playwright.async_api import Browser, BrowserContext

    closed = []
    original_browser_close = Browser.close
    original_context_close = BrowserContext.close

    async def close_browser(self, *args, **kwargs):
        closed.append("browser")
        return await original_browser_close(self, *args, **kwargs)

    async def close_context(self, *args, **kwargs):
        closed.append("context")
        return await original_context_close(self, *args, **kwargs)

    monkeypatch.setattr(Browser, "close", close_browser)
    monkeypatch.setattr(BrowserContext, "close", close_context)
    raw = httpx.Response(200, text="<script src='/slow.js'></script>", headers={"content-type": "text/html"}, request=httpx.Request("GET", "https://example.com/"))

    async def run():
        fetching = asyncio.Event()
        cancelled = asyncio.Event()

        async def fetch(url, **kwargs):
            fetching.set()
            try:
                await asyncio.sleep(60)
            finally:
                cancelled.set()

        task = asyncio.create_task(rendering.render_page(
            "https://example.com/", raw_response=raw, fetch_callback=fetch,
        ))
        await asyncio.wait_for(fetching.wait(), timeout=15)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10)
        assert cancelled.is_set()

    asyncio.run(run())
    assert closed == ["context", "browser"]


@pytest.mark.skipif(not browser_available(), reason="Set SEO_AUDIT_TEST_BROWSER=1 with Playwright Chromium installed.")
def test_real_browser_pending_fetch_marks_snapshot_partial():
    raw = httpx.Response(200, text="<script>fetch('/slow').catch(()=>{})</script>", headers={"content-type": "text/html"}, request=httpx.Request("GET", "https://example.com/"))

    async def fetch(url, **kwargs):
        await asyncio.sleep(60)

    result = asyncio.run(rendering.render_page(
        "https://example.com/", raw_response=raw, fetch_callback=fetch, settle_ms=100,
    ))
    assert result["pending_requests"] == 1
    assert result["status"] == "partial"


@pytest.mark.skipif(not browser_available(), reason="Set SEO_AUDIT_TEST_BROWSER=1 with Playwright Chromium installed.")
def test_real_browser_history_url_change_marks_snapshot_partial():
    raw = httpx.Response(200, text="<script>history.replaceState({}, '', '/other')</script>", headers={"content-type": "text/html"}, request=httpx.Request("GET", "https://example.com/"))
    result = asyncio.run(rendering.render_page("https://example.com/", raw_response=raw, settle_ms=50))
    assert result["final_url"] == "https://example.com/other"
    assert result["document_url_changed"] is True
    assert result["status"] == "partial"


def test_local_lighthouse_requires_opt_in_and_never_falls_back_implicitly(monkeypatch):
    import gsc_server as gs

    monkeypatch.setattr(gs, "ENABLE_LOCAL_LIGHTHOUSE", False)
    runner = AsyncMock()
    monkeypatch.setattr(gs, "_run_lighthouse_process", runner)
    assert "Local Lighthouse disabled" in asyncio.run(gs.run_lighthouse_audit("https://example.com"))
    fallback = AsyncMock()
    monkeypatch.setattr(gs, "run_lighthouse_audit", fallback)
    assert asyncio.run(gs._build_lighthouse_fallback("https://example.com", "mobile", "seo", "Provider failed")) == "Provider failed"
    runner.assert_not_called()
    fallback.assert_not_called()


def test_lighthouse_process_captures_output_without_blocking_loop():
    async def run():
        ticks = 0

        async def tick():
            nonlocal ticks
            for _ in range(5):
                await asyncio.sleep(0.02)
                ticks += 1

        process, _ = await asyncio.gather(rendering._run_lighthouse_process(
            [sys.executable, "-c", "import time; time.sleep(0.2); print('result')"], timeout=5,
        ), tick())
        assert process.returncode == 0 and process.stdout.strip() == "result"
        assert ticks == 5

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["cancel", "timeout", "oversized"])
def test_lighthouse_process_is_terminated_on_cancel_timeout_or_output_limit(monkeypatch, mode):
    processes = []
    create_subprocess = asyncio.create_subprocess_exec

    async def observe(*args, **kwargs):
        process = await create_subprocess(*args, **kwargs)
        if args[0] == sys.executable:
            processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", observe)

    async def run():
        code = "print('x'*100000)" if mode == "oversized" else "import time; time.sleep(60)"
        task = asyncio.create_task(rendering._run_lighthouse_process(
            [sys.executable, "-c", code], timeout=0.1 if mode == "timeout" else 5,
            max_output_bytes=256,
        ))
        if mode == "cancel":
            while not processes:
                await asyncio.sleep(0.01)
            task.cancel()
        error = {"cancel": asyncio.CancelledError, "timeout": subprocess.TimeoutExpired, "oversized": ValueError}[mode]
        with pytest.raises(error):
            await task
        assert processes[0].returncode is not None

    asyncio.run(run())


@pytest.mark.skipif(os.name != "nt", reason="Windows Job Object lifecycle regression.")
def test_lighthouse_windows_job_cleans_child_after_cli_exits(tmp_path):
    import ctypes
    from ctypes import wintypes

    child_pid_file = tmp_path / "child.pid"
    code = (
        "import pathlib,subprocess,sys; "
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
        "pathlib.Path(sys.argv[1]).write_text(str(child.pid))"
    )
    with pytest.raises(subprocess.TimeoutExpired):
        asyncio.run(rendering._run_lighthouse_process(
            [sys.executable, "-c", code, str(child_pid_file)], timeout=0.5,
        ))
    child_pid = int(child_pid_file.read_text())
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    child = kernel.OpenProcess(0x00100000, False, child_pid)  # SYNCHRONIZE
    if child:
        try:
            assert kernel.WaitForSingleObject(child, 0) == 0
        finally:
            kernel.CloseHandle(child)
