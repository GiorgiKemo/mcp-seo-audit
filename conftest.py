"""Deterministic offline configuration applied before test modules import the server."""

import ipaddress
import os
import socket

import pytest


os.environ.update({
    "GSC_SKIP_OAUTH": "true",
    "GSC_DATA_STATE": "all",
    "GSC_CREDENTIALS_PATH": "test-credentials-not-present.json",
    "GSC_OAUTH_CLIENT_SECRETS_FILE": "test-client-not-present.json",
    "CRUX_API_KEY": "test-key-123",
    "PAGESPEED_API_KEY": "test-pagespeed-key",
    "GOOGLE_API_KEY": "test-google-key",
    "SEO_AUDIT_ENABLE_WRITE_TOOLS": "true",
    "SEO_AUDIT_ALLOW_NPX_LIGHTHOUSE": "true",
    "SEO_AUDIT_ALLOW_PRIVATE_URLS": "false",
    "SEO_AUDIT_MAX_FETCH_BYTES": "5242880",
    "SEO_AUDIT_MAX_REDIRECTS": "5",
    "SEO_AUDIT_MAX_SITEMAP_URLS": "50000",
    "SEO_AUDIT_MAX_CRAWL_PAGES": "100",
    "LIGHTHOUSE_BINARY": "",
    "LIGHTHOUSE_CHROME_PATH": "",
    "LIGHTHOUSE_NO_SANDBOX": "false",
})


@pytest.fixture(autouse=True)
def isolate_credentials_and_network(monkeypatch, tmp_path):
    import gsc_server as gs

    monkeypatch.setattr(gs, "TOKEN_FILE", str(tmp_path / "token.json"))
    monkeypatch.setattr(gs, "LEGACY_TOKEN_FILE", str(tmp_path / "legacy-token.json"))
    monkeypatch.setattr(gs, "OAUTH_CLIENT_SECRETS_FILE", str(tmp_path / "client.json"))
    monkeypatch.setattr(gs, "POSSIBLE_CREDENTIAL_PATHS", [
        str(tmp_path / "service-account.json"), str(tmp_path / "fallback-service-account.json"),
    ])
    monkeypatch.setattr(gs, "_gsc_service_cache", None)
    monkeypatch.setattr(gs, "_indexing_service_cache", None)
    # Performance fallback tests must never discover/download a real browser runner.
    # Tests of runner behavior explicitly replace this and mock subprocess.run.
    monkeypatch.setattr(gs.shutil, "which", lambda executable: None)

    original_lookup = socket.getaddrinfo
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex

    def is_loopback(host):
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return host == "localhost"

    def offline_lookup(host, port, *args, **kwargs):
        if host is None or is_loopback(host):
            return original_lookup(host, port, *args, **kwargs)
        # Public fetch validation needs a deterministic public DNS answer; tests
        # for other DNS outcomes override this mock explicitly.
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("93.184.216.34", port or 0))]

    def guarded_connect(connection, address):
        if not isinstance(address, tuple) or not is_loopback(address[0]):
            raise AssertionError("Live network access is disabled in unit tests; mock the provider.")
        return original_connect(connection, address)

    def guarded_connect_ex(connection, address):
        if not isinstance(address, tuple) or not is_loopback(address[0]):
            raise AssertionError("Live network access is disabled in unit tests; mock the provider.")
        return original_connect_ex(connection, address)

    monkeypatch.setattr(socket, "getaddrinfo", offline_lookup)
    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
