"""Offline regressions for resource limits, credentials, and provider results."""

import asyncio
import gzip
import json
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import gsc_server as gs


def test_gzip_sitemap_expansion_is_bounded(monkeypatch):
    monkeypatch.setattr(gs, "MAX_FETCH_BYTES", 128)
    response = httpx.Response(
        200,
        content=gzip.compress(b"x" * 256),
        request=httpx.Request("GET", "https://example.com/sitemap.xml.gz"),
    )
    assert len(response.content) < 128
    with pytest.raises(ValueError, match="after decoding"):
        gs._extract_xml_text(response)


@pytest.mark.parametrize("compressed", [True, False])
def test_gzip_sitemap_and_already_decoded_response(compressed):
    xml = b"<urlset><url><loc>https://example.com/</loc></url></urlset>"
    response = httpx.Response(
        200,
        content=gzip.compress(xml) if compressed else xml,
        headers={"content-type": "application/gzip"},
        request=httpx.Request("GET", "https://example.com/sitemap.xml.gz"),
    )
    assert gs._extract_xml_text(response) == xml.decode()


@pytest.fixture
def temporary_auth(monkeypatch, tmp_path):
    token = tmp_path / "test-token.json"
    token.write_text('{"token":"existing-test-value"}', encoding="utf-8")
    client = tmp_path / "test-client.json"
    client.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(gs, "TOKEN_FILE", str(token))
    monkeypatch.setattr(gs, "OAUTH_CLIENT_SECRETS_FILE", str(client))
    monkeypatch.setattr(gs, "_gsc_service_cache", object())
    monkeypatch.setattr(gs, "_indexing_service_cache", object())
    return token, client, gs._gsc_service_cache, gs._indexing_service_cache


@pytest.mark.parametrize("failure", ["missing_client", "cancelled_login", "save_failure"])
def test_reauthentication_preserves_existing_credentials(temporary_auth, failure, monkeypatch):
    token, client, gsc_cache, indexing_cache = temporary_auth
    original = token.read_text(encoding="utf-8")
    if failure == "missing_client":
        client.unlink()
    with patch.object(gs, "InstalledAppFlow") as flow:
        if failure == "cancelled_login":
            flow.from_client_secrets_file.return_value.run_local_server.side_effect = RuntimeError("Login cancelled")
        elif failure == "save_failure":
            flow.from_client_secrets_file.return_value.run_local_server.return_value.to_json.return_value = '{"token":"new-test-value"}'
            monkeypatch.setattr(gs.os, "replace", MagicMock(side_effect=OSError("Disk replacement denied")))
        result = asyncio.run(gs.reauthenticate())
    assert "Error" in result
    assert token.read_text(encoding="utf-8") == original
    assert gs._gsc_service_cache is gsc_cache
    assert gs._indexing_service_cache is indexing_cache
    assert not list(token.parent.glob(".oauth-token-*.tmp"))


def test_successful_reauthentication_replaces_token_then_clears_caches(temporary_auth):
    token, _, _, _ = temporary_auth
    with patch.object(gs, "InstalledAppFlow") as flow:
        flow.from_client_secrets_file.return_value.run_local_server.return_value.to_json.return_value = '{"token":"new-test-value"}'
        result = asyncio.run(gs.reauthenticate())
    assert "Successfully authenticated" in result
    assert json.loads(token.read_text(encoding="utf-8")) == {"token": "new-test-value"}
    assert gs._gsc_service_cache is None
    assert gs._indexing_service_cache is None
    assert not list(token.parent.glob(".oauth-token-*.tmp"))


def crux_response(metrics):
    return httpx.Response(
        200,
        json={"record": {"key": {"origin": "https://example.com"}, "metrics": metrics}},
        request=httpx.Request("POST", "https://chromeuxreport.googleapis.com/v1/records:queryRecord"),
    )


@pytest.mark.parametrize("cls", ["0.05", 0.05])
def test_crux_accepts_numeric_string_percentiles(monkeypatch, cls):
    monkeypatch.setattr(gs, "CRUX_API_KEY", "test-key")
    metrics = {
        "largest_contentful_paint": {"percentiles": {"p75": "2000"}},
        "interaction_to_next_paint": {"percentiles": {"p75": 150}},
        "cumulative_layout_shift": {"percentiles": {"p75": cls}},
    }
    with patch.object(gs.httpx, "AsyncClient") as client_cls:
        client_cls.return_value.__aenter__.return_value.post = AsyncMock(return_value=crux_response(metrics))
        result = asyncio.run(gs.get_core_web_vitals("https://example.com"))
    assert "CLS: GOOD" in result
    assert "PASSING Core Web Vitals" in result


@pytest.mark.parametrize("unknown", [None, "NaN", "invalid", -1, True])
def test_crux_missing_or_invalid_metric_is_not_a_failure(monkeypatch, unknown):
    monkeypatch.setattr(gs, "CRUX_API_KEY", "test-key")
    metrics = {
        "largest_contentful_paint": {"percentiles": {"p75": 2000}},
        "interaction_to_next_paint": {"percentiles": {"p75": 150}},
        "cumulative_layout_shift": {"percentiles": {"p75": unknown}},
    }
    with patch.object(gs.httpx, "AsyncClient") as client_cls:
        client_cls.return_value.__aenter__.return_value.post = AsyncMock(return_value=crux_response(metrics))
        result = asyncio.run(gs.get_core_web_vitals("https://example.com"))
    assert "CLS: NO DATA" in result
    assert "INSUFFICIENT DATA" in result
    assert "FAILING" not in result


@pytest.mark.parametrize("provider", ["crux", "pagespeed"])
def test_provider_transport_errors_redact_api_keys(monkeypatch, provider):
    secret = "TEST_SECRET_THAT_MUST_NOT_APPEAR"
    monkeypatch.setattr(gs, "CRUX_API_KEY", secret)
    monkeypatch.setattr(gs, "PAGESPEED_API_KEY", secret)
    with patch.object(gs.httpx, "AsyncClient") as client_cls, patch.object(
        gs, "run_lighthouse_audit", new=AsyncMock(return_value="No Lighthouse runner available")
    ):
        client = client_cls.return_value.__aenter__.return_value
        client.post = AsyncMock(side_effect=RuntimeError(f"request failed?key={secret}"))
        client.get = AsyncMock(side_effect=RuntimeError(f"request failed?key={secret}"))
        tool = gs.get_core_web_vitals if provider == "crux" else gs.get_pagespeed_insights
        result = asyncio.run(tool("https://example.com"))
    assert secret not in result
    assert "[REDACTED]" in result


def test_pagespeed_redacts_before_truncating(monkeypatch):
    secret = "TEST_SECRET_THAT_MUST_NOT_APPEAR"
    monkeypatch.setattr(gs, "PAGESPEED_API_KEY", secret)
    response = httpx.Response(
        500,
        text="x" * 295 + secret,
        request=httpx.Request("GET", "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"),
    )
    with patch.object(gs.httpx, "AsyncClient") as client_cls, patch.object(
        gs, "run_lighthouse_audit", new=AsyncMock(return_value="No Lighthouse runner available")
    ):
        client_cls.return_value.__aenter__.return_value.get = AsyncMock(return_value=response)
        result = asyncio.run(gs.get_pagespeed_insights("https://example.com"))
    assert "TEST_" not in result


def test_lighthouse_awaits_cancellable_process_runner(monkeypatch):
    monkeypatch.setattr(gs, "ENABLE_LOCAL_LIGHTHOUSE", True)
    monkeypatch.setattr(gs, "LIGHTHOUSE_BINARY", "test-lighthouse")
    monkeypatch.setattr(gs, "_validate_fetchable_public_url", lambda url: url)
    runner = AsyncMock(return_value=MagicMock(returncode=0, stdout='{"categories":{},"audits":{}}'))
    monkeypatch.setattr(gs, "_run_lighthouse_process", runner)
    result = asyncio.run(gs.run_lighthouse_audit("https://example.com"))
    assert "Local Lighthouse audit" in result
    runner.assert_awaited_once()
    assert runner.call_args.kwargs["timeout"] == 180


def test_indexing_notification_does_not_promise_indexing(monkeypatch):
    monkeypatch.setattr(gs, "ENABLE_WRITE_TOOLS", True)
    service = MagicMock()
    service.urlNotifications().publish().execute.return_value = {}
    with patch.object(gs, "get_indexing_service", return_value=service):
        result = asyncio.run(gs.request_indexing("https://example.com/job"))
    assert "accepted the notification" in result
    assert "not guaranteed" in result
    assert "will crawl" not in result


def test_deletion_notification_does_not_claim_confirmed_removal(monkeypatch):
    monkeypatch.setattr(gs, "ENABLE_WRITE_TOOLS", True)
    service = MagicMock()
    service.urlNotifications().publish().execute.return_value = {}
    with patch.object(gs, "get_indexing_service", return_value=service):
        result = asyncio.run(gs.request_removal("https://example.com/old-job"))
    assert "accepted the deletion notification" in result
    assert "does not confirm removal" in result


def test_batch_indexing_reports_unattempted_urls_after_rate_limit(monkeypatch):
    from googleapiclient.errors import HttpError

    monkeypatch.setattr(gs, "ENABLE_WRITE_TOOLS", True)
    response = MagicMock(status=429)
    service = MagicMock()
    service.urlNotifications().publish().execute.side_effect = [{}, HttpError(response, b"rate limited")]
    urls = [f"https://example.com/job-{index}" for index in range(4)]
    with patch.object(gs, "get_indexing_service", return_value=service), patch.object(gs.asyncio, "sleep", new=AsyncMock()):
        result = asyncio.run(gs.batch_request_indexing("\n".join(urls)))
    assert "Requested: 4" in result
    assert "Submitted: 1" in result
    assert "Failed: 1" in result
    assert "Not attempted: 2" in result
    assert urls[2] in result and urls[3] in result
    assert service.urlNotifications().publish().execute.call_count == 2
    assert "quota is not available" in result
    assert "Remaining daily quota" not in result
    assert "not guaranteed" in result


@pytest.mark.parametrize("urls", ["", "\n".join(f"https://example.com/{index}" for index in range(101))])
def test_invalid_indexing_batch_does_not_initialize_credentials(monkeypatch, urls):
    monkeypatch.setattr(gs, "ENABLE_WRITE_TOOLS", True)
    with patch.object(gs, "get_indexing_service") as service:
        result = asyncio.run(gs.batch_request_indexing(urls))
    service.assert_not_called()
    assert "No URLs" in result or "Too many URLs" in result


def test_registered_mcp_tools_expose_safety_annotations():
    tools = {tool.name: tool for tool in asyncio.run(gs.mcp.list_tools())}
    write_tools = {
        "add_site", "delete_site", "submit_sitemap", "delete_sitemap",
        "request_indexing", "request_removal", "batch_request_indexing", "reauthenticate",
        "create_audit_project", "set_audit_schedule", "run_project_audit",
    }
    destructive_tools = {"delete_site", "delete_sitemap", "request_removal", "reauthenticate", "run_project_audit"}
    local_tools = {"compare_seo_audits", "get_server_status", "create_audit_project", "list_audit_projects", "list_project_audits", "get_project_audit", "compare_project_audits", "list_audit_events"}
    repeatable_writes = {"add_site", "delete_site", "delete_sitemap"}
    assert write_tools <= tools.keys()
    for name, tool in tools.items():
        assert tool.annotations is not None, name
        assert tool.annotations.readOnlyHint is (name not in write_tools), name
        assert tool.annotations.destructiveHint is (name in destructive_tools), name
        assert tool.annotations.idempotentHint is (name not in write_tools or name in repeatable_writes), name
        assert tool.annotations.openWorldHint is (name not in local_tools), name
