"""HTTP fetching with DNS-pinned connections and bounded response bodies."""

import asyncio
import ipaddress
import socket
from typing import Optional

import httpx


async def _resolve_addresses(host: str, port: int, allow_private: bool) -> list[str]:
    if "%" in host:
        raise ValueError("Scoped IP addresses are not allowed.")
    try:
        addresses = [str(ipaddress.ip_address(host))]
    except ValueError:
        records = await asyncio.get_running_loop().getaddrinfo(
            host, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP,
        )
        addresses = list(dict.fromkeys(record[4][0] for record in records))
    if not addresses:
        raise ValueError("The host did not resolve to an IP address.")
    for address in addresses:
        ip = ipaddress.ip_address(address)
        # Translation/tunneling prefixes can hide an IPv4 destination from a
        # superficial IPv6 public-address check (including cloud metadata IPs).
        translated = ip.version == 6 and any(ip in prefix for prefix in (
            ipaddress.ip_network("64:ff9b::/96"), ipaddress.ip_network("64:ff9b:1::/48"),
            ipaddress.ip_network("2002::/16"), ipaddress.ip_network("2001::/32"),
        ))
        if not allow_private and (
            not ip.is_global or ip.is_multicast or ip.is_unspecified or ip.is_reserved
            or translated
            or (ip.version == 6 and ip.ipv4_mapped is not None and not ip.ipv4_mapped.is_global)
        ):
            raise ValueError("URL resolves to a non-public IP address; request blocked.")
    return addresses


class _PinnedTransport(httpx.AsyncBaseTransport):
    def __init__(self, allow_private: bool = False):
        self.allow_private = allow_private
        # The rewritten connection origin is an IP. Never reuse TLS connections
        # across original hostnames which happen to share that IP.
        self._transport = httpx.AsyncHTTPTransport(
            trust_env=False, verify=True, retries=0,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=0),
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        if url.scheme not in {"http", "https"} or not url.host or url.username or url.password:
            raise ValueError("Only HTTP(S) URLs without embedded credentials are allowed.")
        connect_timeout = request.extensions.get("timeout", {}).get("connect", 10.0)
        try:
            async with asyncio.timeout(connect_timeout):
                addresses = await _resolve_addresses(
                    url.host, url.port or (443 if url.scheme == "https" else 80), self.allow_private,
                )
        except TimeoutError as exc:
            raise httpx.ConnectTimeout("DNS lookup timed out.", request=request) from exc
        original_host = url.raw_host.decode("ascii")
        authority = f"[{original_host}]" if ":" in original_host else original_host
        if url.port is not None:
            authority += f":{url.port}"
        headers = httpx.Headers(request.headers)
        headers["Host"] = authority
        extensions = {**request.extensions, "sni_hostname": original_host}
        for index, address in enumerate(addresses):
            pinned = httpx.Request(
                request.method, url.copy_with(host=address), headers=headers,
                stream=request.stream, extensions=extensions,
            )
            try:
                response = await self._transport.handle_async_request(pinned)
                response.request = request
                return response
            except (httpx.ConnectError, httpx.ConnectTimeout):
                if index == len(addresses) - 1:
                    raise
        raise AssertionError("Address list was unexpectedly empty.")

    async def aclose(self) -> None:
        await self._transport.aclose()


def safe_transport(allow_private: bool = False) -> httpx.AsyncBaseTransport:
    """Use with an AsyncClient configured with trust_env=False.

    Every connection uses a validated literal IP with the original HTTP Host and
    TLS server name. The browser/OS cannot re-resolve the original DNS hostname
    between validation and connection. All DNS answers must be public.
    """
    return _PinnedTransport(allow_private)


async def safe_fetch_once(
    url: str, *, method: str = "GET", headers: Optional[dict[str, str]] = None,
    max_bytes: int = 5 * 1024 * 1024, timeout: float = 20,
    allow_private: bool = False,
) -> httpx.Response:
    """Fetch one response without following redirects or using environment proxies."""
    if max_bytes < 1 or timeout <= 0:
        raise ValueError("Response limit and timeout must be positive.")
    async with asyncio.timeout(timeout):
        async with httpx.AsyncClient(
            transport=safe_transport(allow_private), trust_env=False,
            follow_redirects=False, timeout=timeout, headers=headers,
        ) as client:
            async with client.stream(method, url) as response:
                chunks = []
                total = 0
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > max_bytes:
                        raise ValueError(f"Response exceeded the {max_bytes}-byte limit.")
                    chunks.append(chunk)
                decoded_headers = [
                    (name, value) for name, value in response.headers.multi_items()
                    if name.lower() not in {"content-length", "content-encoding"}
                ]
                return httpx.Response(
                    response.status_code, headers=decoded_headers,
                    content=b"".join(chunks), request=response.request,
                )
