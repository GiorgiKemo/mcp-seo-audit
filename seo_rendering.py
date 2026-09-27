"""Optional, bounded Chromium rendering through the same guarded HTTP fetcher."""

import asyncio
import inspect
import os
import socket
import signal
import subprocess
from contextlib import suppress
from urllib.parse import urljoin, urlsplit

import httpx

from seo_network import safe_fetch_once


def _windows_process_job(pid):
    """Assign our newly created trusted CLI to a kill-on-close Windows job."""
    import ctypes
    from ctypes import wintypes

    class BasicLimits(ctypes.Structure):
        _fields_ = [
            ("process_time", ctypes.c_longlong), ("job_time", ctypes.c_longlong),
            ("flags", wintypes.DWORD), ("minimum_working_set", ctypes.c_size_t),
            ("maximum_working_set", ctypes.c_size_t), ("active_processes", wintypes.DWORD),
            ("affinity", ctypes.c_size_t), ("priority", wintypes.DWORD), ("scheduling", wintypes.DWORD),
        ]

    class ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ("basic", BasicLimits), ("io_counters", ctypes.c_ulonglong * 6),
            ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
            ("peak_process_memory", ctypes.c_size_t), ("peak_job_memory", ctypes.c_size_t),
        ]

    class ProcessList(ctypes.Structure):
        _fields_ = [
            ("assigned", wintypes.DWORD), ("count", wintypes.DWORD),
            ("ids", ctypes.c_size_t * 4096),
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel.CreateJobObjectW.restype = wintypes.HANDLE
    kernel.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel.SetInformationJobObject.restype = wintypes.BOOL
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel.QueryInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p]
    kernel.QueryInformationJobObject.restype = wintypes.BOOL
    kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel.TerminateJobObject.restype = wintypes.BOOL
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    job = kernel.CreateJobObjectW(None, None)
    if not job:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        limits = ExtendedLimits()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not kernel.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            raise ctypes.WinError(ctypes.get_last_error())
        process_handle = kernel.OpenProcess(0x0101, False, pid)  # SET_QUOTA | TERMINATE
        if not process_handle:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            if not kernel.AssignProcessToJobObject(job, process_handle):
                raise ctypes.WinError(ctypes.get_last_error())
        finally:
            kernel.CloseHandle(process_handle)
    except BaseException:
        kernel.CloseHandle(job)
        raise
    async def terminate_and_wait():
        handles = {}

        def capture_members():
            members = ProcessList()
            if not kernel.QueryInformationJobObject(job, 3, ctypes.byref(members), ctypes.sizeof(members), None):
                raise ctypes.WinError(ctypes.get_last_error())
            for member in members.ids[:members.count]:
                if member in handles:
                    continue
                handle = kernel.OpenProcess(0x00100000, False, member)  # SYNCHRONIZE
                if handle:
                    handles[member] = handle
                elif ctypes.get_last_error() not in {87, 1168}:  # Already exited.
                    raise ctypes.WinError(ctypes.get_last_error())
            return members.count

        try:
            capture_members()
            if not kernel.TerminateJobObject(job, 1):
                raise ctypes.WinError(ctypes.get_last_error())
            # Job termination and pipe EOF do not mean all process objects are
            # signaled yet. Keep handles alive and await actual child exit.
            deadline = asyncio.get_running_loop().time() + 5
            while True:
                active = capture_members()
                waits = [kernel.WaitForSingleObject(handle, 0) for handle in handles.values()]
                if any(result not in {0, 258} for result in waits):
                    raise ctypes.WinError(ctypes.get_last_error())
                if not active and all(result == 0 for result in waits):
                    break
                if asyncio.get_running_loop().time() >= deadline:
                    raise RuntimeError("Windows Lighthouse process tree did not stop within the cleanup deadline.")
                await asyncio.sleep(0.01)
        finally:
            kernel.CloseHandle(job)
            for handle in handles.values():
                kernel.CloseHandle(handle)

    return terminate_and_wait


async def _run_lighthouse_process(command: list[str], *, timeout: float = 180, max_output_bytes: int = 20 * 1024 * 1024):
    """Run the explicitly trusted Lighthouse CLI with bounded, cancellable I/O."""
    options = {"stdout": asyncio.subprocess.PIPE, "stderr": asyncio.subprocess.PIPE}
    if os.name == "nt":
        options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    else:
        options["start_new_session"] = True
    process = await asyncio.create_subprocess_exec(*command, **options)
    readers = []
    close_job = None

    async def read_bounded(stream):
        chunks = []
        total = 0
        while chunk := await stream.read(65536):
            total += len(chunk)
            if total > max_output_bytes:
                raise ValueError("Lighthouse output exceeded the configured byte limit.")
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8", errors="replace")

    try:
        if os.name == "nt":
            # Assign immediately, before the trusted Node CLI launches Chromium.
            # This is lifecycle cleanup, not a sandbox for hostile executables.
            close_job = _windows_process_job(process.pid)
        readers = [asyncio.create_task(read_bounded(process.stdout)), asyncio.create_task(read_bounded(process.stderr))]
        async with asyncio.timeout(timeout):
            stdout, stderr = await asyncio.gather(*readers)
            await process.wait()
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    except TimeoutError as exc:
        raise subprocess.TimeoutExpired(command, timeout) from exc
    finally:
        for reader in readers:
            if not reader.done():
                reader.cancel()
        if readers:
            await asyncio.gather(*readers, return_exceptions=True)
        if close_job is not None:
            await close_job()
        if os.name == "nt":
            if close_job is None and process.returncode is None:
                # Kill only the tree rooted at the CLI we just created. A bare
                # terminate() kills Node but can leave its Chromium children.
                taskkill = os.path.join(os.environ.get("SystemRoot", "C:/Windows"), "System32", "taskkill.exe")
                with suppress(OSError, TimeoutError):
                    killer = await asyncio.create_subprocess_exec(
                        taskkill, "/PID", str(process.pid), "/T", "/F",
                        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                        creationflags=subprocess.CREATE_NO_WINDOW,
                    )
                    await asyncio.wait_for(killer.wait(), timeout=5)
        else:
            # The CLI has its own session, so this cannot target the server's
            # process group. Also catches descendants whose parent exited early.
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        if process.returncode is None:
            with suppress(ProcessLookupError):
                process.kill()
        # After killing, drain any buffered pipe bytes so asyncio's process
        # transport cannot deadlock waiting for a paused stdout/stderr pipe.
        with suppress(TimeoutError):
            await asyncio.wait_for(process.communicate(), timeout=5)


def _origin(url: str) -> tuple:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Rendering requires an HTTP(S) URL without embedded credentials.")
    return parsed.scheme, parsed.hostname.lower(), parsed.port or (443 if parsed.scheme == "https" else 80)


async def _validate(validator, url: str) -> None:
    if validator is not None:
        result = validator(url)
        if inspect.isawaitable(result):
            await result


async def render_page(
    url: str, *, raw_response: httpx.Response | None = None,
    fetch_callback=None, navigation_validator=None, resource_validator=None,
    allow_private: bool = False, timeout_seconds: float = 30,
    settle_ms: int = 750, max_requests: int = 80, max_bytes: int = 20 * 1024 * 1024,
) -> dict:
    """Return raw/rendered HTML and coverage metadata for a single page.

    ``fetch_callback`` must implement the guarded ``_fetch_url`` interface and
    honor ``follow_redirects=False``. Validators may be synchronous or async.
    Navigation stays on the starting origin; resource validators can enforce
    robots rules for each origin. Browser requests never use route.continue_().
    """
    if not (1 <= max_requests <= 500 and 1 <= max_bytes <= 100 * 1024 * 1024):
        raise ValueError("Rendering limits must be 1..500 requests and 1..100 MiB.")
    if not (0 < timeout_seconds <= 120 and 0 <= settle_ms <= 10000):
        raise ValueError("Rendering timeout must be <=120 seconds and settle time <=10000 ms.")
    starting_origin = _origin(url)
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise RuntimeError(
            "JavaScript rendering requires the optional browser dependency: "
            "pip install 'mcp-seo-audit[browser]' and python -m playwright install chromium."
        ) from exc

    blocked = []
    js_errors = []
    request_count = 0
    bytes_received = 0
    redirects = 0
    resource_redirects = []
    raw_used = False
    context = None
    browser = None
    proxy_guard = None
    active_routes = set()
    # Bound concurrent buffered callback responses as well as the delivered bytes.
    fetch_slots = asyncio.Semaphore(2)

    async def validate_target(target, navigation=False):
        if navigation and _origin(target) != starting_origin:
            raise ValueError("Cross-origin navigation is blocked.")
        _origin(target)
        await _validate(resource_validator, target)
        if navigation:
            await _validate(navigation_validator, target)

    async def fetch(target, method="GET", headers=None):
        if fetch_callback is not None:
            response = await fetch_callback(target, method=method, headers=headers, follow_redirects=False)
        else:
            response = await safe_fetch_once(
                target, method=method, headers=headers, allow_private=allow_private,
                max_bytes=min(max_bytes, 5 * 1024 * 1024), timeout=min(timeout_seconds, 20),
            )
        return response

    async def initial_response():
        current = url
        for _ in range(6):
            await validate_target(current, navigation=True)
            response = await fetch(current)
            if response.is_redirect and response.headers.get("location"):
                current = urljoin(current, response.headers["location"])
            else:
                return response
        raise ValueError("Too many redirects before rendering.")

    async def intercept(route):
        nonlocal request_count, bytes_received, raw_used, redirects
        task = asyncio.current_task()
        active_routes.add(task)
        request = route.request
        target = request.url
        request_count += 1
        try:
            if request_count > max_requests:
                raise ValueError("Rendering request limit reached.")
            if request.method not in {"GET", "HEAD"}:
                raise ValueError("Rendering only permits GET and HEAD requests.")
            await validate_target(target, request.is_navigation_request())
            async with fetch_slots:
                if bytes_received >= max_bytes:
                    raise ValueError("Rendering byte limit reached.")
                if not raw_used and target == str(raw_response.url) and request.is_navigation_request():
                    response = raw_response
                    raw_used = True
                else:
                    # Do not propagate browser cookies, authorization, or client hints.
                    request_headers = {"User-Agent": "mcp-seo-audit"}
                    if request.headers.get("accept"):
                        request_headers["Accept"] = request.headers["accept"]
                    response = await fetch(target, request.method, request_headers)
                while response.is_redirect and response.headers.get("location"):
                    redirects += 1
                    request_count += 1
                    if redirects > 10 or request_count > max_requests:
                        raise ValueError("Rendering redirect limit reached.")
                    destination = urljoin(target, response.headers["location"])
                    await validate_target(destination, request.is_navigation_request())
                    if request.is_navigation_request():
                        raise ValueError("Navigation redirect during rendering blocked; audit its destination separately.")
                    resource_redirects.append({"from": target[:2000], "to": destination[:2000]})
                    target = destination
                    # Chromium does not re-intercept redirected routes. Follow
                    # here so every hop uses the DNS-pinned fetcher, never Chrome.
                    response = await fetch(target, request.method, request_headers)
                bytes_received += len(response.content)
                if bytes_received > max_bytes:
                    raise ValueError("Rendering byte limit reached.")
                headers = {
                    name: value for name, value in response.headers.items()
                    if name.lower() not in {
                        "content-length", "content-encoding", "transfer-encoding", "connection", "set-cookie",
                    }
                }
                await route.fulfill(status=response.status_code, headers=headers, body=response.content)
        except asyncio.CancelledError:
            with suppress(Exception):
                await route.abort("aborted")
            raise
        except Exception as exc:
            if len(blocked) < 100:
                blocked.append({"url": target[:2000], "reason": str(exc)[:500]})
            with suppress(Exception):
                await route.abort("blockedbyclient")
        finally:
            active_routes.discard(task)

    async def block_socket(socket):
        if len(blocked) < 100:
            blocked.append({"url": socket.url[:2000], "reason": "WebSockets are disabled during rendering."})
        await socket.close()

    async with async_playwright() as playwright:
        try:
            async with asyncio.timeout(timeout_seconds):
                if raw_response is None:
                    raw_response = await initial_response()
                await validate_target(str(raw_response.url), navigation=True)
                if len(raw_response.content) > max_bytes:
                    raise ValueError("Raw HTML exceeded rendering byte limit.")
                browser_path = os.environ.get("SEO_AUDIT_BROWSER_PATH") or None
                # Reserve a non-listening local port for the lifetime of the
                # browser. No local service can accidentally become its proxy.
                proxy_guard = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                proxy_guard.bind(("127.0.0.1", 0))
                browser = await playwright.chromium.launch(
                    headless=True, chromium_sandbox=True, executable_path=browser_path,
                    # Defense in depth: requests missed by interception cannot
                    # reach the Internet or localhost through Chromium itself.
                    proxy={"server": f"http://127.0.0.1:{proxy_guard.getsockname()[1]}", "bypass": "<-loopback>"},
                    args=["--disable-quic", "--dns-prefetch-disable", "--disable-background-networking",
                          "--force-webrtc-ip-handling-policy=disable_non_proxied_udp"],
                )
                context = await browser.new_context(
                    service_workers="block", accept_downloads=False,
                    user_agent="mcp-seo-audit", viewport={"width": 1365, "height": 900},
                )
                await context.route("**/*", intercept)
                await context.route_web_socket("**/*", block_socket)
                await context.add_init_script("""
                    for (const name of ['RTCPeerConnection', 'webkitRTCPeerConnection', 'WebTransport']) {
                        Object.defineProperty(globalThis, name, {value: undefined, configurable: false});
                    }
                """)
                page = await context.new_page()
                page.on("pageerror", lambda error: js_errors.append(str(error)[:500]) if len(js_errors) < 50 else None)
                await page.goto(str(raw_response.url), wait_until="domcontentloaded", timeout=int(timeout_seconds * 1000))
                await page.wait_for_timeout(settle_ms)
                await validate_target(page.url, navigation=True)
                rendered_html = await page.evaluate("""limit => {
                    const html = document.documentElement.outerHTML;
                    return html.length > limit ? null : '<!DOCTYPE html>\\n' + html;
                }""", max_bytes)
                if rendered_html is None or len(rendered_html.encode("utf-8")) > max_bytes:
                    raise ValueError("Rendered HTML exceeded the rendering byte limit.")
                pending_requests = len(active_routes)
                original_parts = urlsplit(str(raw_response.url))
                rendered_parts = urlsplit(page.url)
                document_url_changed = (
                    (original_parts.path or "/", original_parts.query)
                    != (rendered_parts.path or "/", rendered_parts.query)
                )
                limitations = []
                if resource_redirects:
                    limitations.append("Redirected script/style resources retain the requested browser URL; relative imports may differ.")
                if document_url_changed:
                    limitations.append("JavaScript changed the document URL; audit the rendered destination separately.")
                return {
                    "raw_html": raw_response.text, "rendered_html": rendered_html,
                    "final_url": page.url,
                    "status": "partial" if blocked or js_errors or pending_requests or limitations else "complete",
                    "blocked_requests": blocked, "js_errors": js_errors,
                    "request_count": request_count, "bytes_received": bytes_received,
                    "settle_ms": settle_ms, "resource_redirects": resource_redirects,
                    "pending_requests": pending_requests,
                    "document_url_changed": document_url_changed, "limitations": limitations,
                }
        finally:
            for task in tuple(active_routes):
                task.cancel()
            if active_routes:
                await asyncio.gather(*tuple(active_routes), return_exceptions=True)
            if context is not None:
                with suppress(Exception):
                    await asyncio.wait_for(context.close(), timeout=5)
            if browser is not None:
                with suppress(Exception):
                    await asyncio.wait_for(browser.close(), timeout=5)
            if proxy_guard is not None:
                proxy_guard.close()
