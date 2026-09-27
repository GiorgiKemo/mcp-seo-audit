"""Explicit worker execution, failure handling, cancellation and tool registration."""

import asyncio
import importlib
import json
from unittest.mock import AsyncMock

from mcp.server.fastmcp import FastMCP
import pytest

import seo_monitoring as monitoring
from seo_storage import AuditStore
from test_storage import sample_report


@pytest.fixture
def store(tmp_path):
    result = AuditStore(tmp_path / "store")
    result.create_project("example", "https://example.com/")
    return result


def test_import_and_tool_registration_do_not_create_storage_or_run_audits(tmp_path, monkeypatch):
    target = tmp_path / "unused"
    monkeypatch.setenv("SEO_AUDIT_DATA_DIR", str(target))
    importlib.reload(monitoring)
    callback = AsyncMock()
    monitoring.register_monitoring_tools(FastMCP("test"), callback)
    assert not target.exists()
    callback.assert_not_called()


def test_disabled_projects_never_run(store):
    callback = AsyncMock(return_value=sample_report())
    assert asyncio.run(monitoring.run_due_audits(store, callback)) == []
    callback.assert_not_called()


def test_due_worker_runs_with_saved_settings_and_reopens_cleanly(store):
    store.set_schedule("example", True, 60, now=0)
    callback = AsyncMock(return_value=sample_report())
    result = asyncio.run(monitoring.run_due_audits(store, callback))
    assert len(result) == 1
    callback.assert_awaited_once_with(start_url="https://example.com/", max_pages=25, respect_robots=True, render_mode="raw", include_sitemaps=True)
    assert AuditStore(store.path.parent).load_audit("example", result[0]["audit_id"]) == sample_report()
    assert asyncio.run(monitoring.run_due_audits(store, callback)) == []


def test_worker_failure_does_not_save_an_error_baseline(store):
    store.set_schedule("example", True, 60, now=0)
    callback = AsyncMock(return_value={"error": "api_key=secret"})
    result = asyncio.run(monitoring.run_due_audits(store, callback))
    assert result[0]["error"] == "audit_failed"
    assert store.list_audits("example") == []
    assert "secret" not in json.dumps(store.list_events("example"))


@pytest.mark.parametrize("report", [sample_report(pages=[]), sample_report(pages=[{"url": "https://example.com/", "state": "fetch_error", "status": None}])])
def test_no_successfully_crawled_html_is_a_failed_run(store, report):
    result = asyncio.run(monitoring.execute_claim(store, store.claim("example"), AsyncMock(return_value=report)))
    assert result["error"] == "audit_failed"
    assert store.list_audits("example") == []


def test_invalid_report_is_not_saved_and_releases_lease(store):
    report = sample_report(schema_version="invalid")
    result = asyncio.run(monitoring.execute_claim(store, store.claim("example"), AsyncMock(return_value=report)))
    assert result["error"] == "invalid_report"
    assert store.get_project("example")["running"] is False
    assert store.list_audits("example") == []


def test_timeout_releases_lease_and_records_safe_category(store, monkeypatch):
    monkeypatch.setattr(monitoring, "AUDIT_TIMEOUT_SECONDS", 0.01)
    async def callback(**kwargs):
        await asyncio.sleep(60)
    result = asyncio.run(monitoring.execute_claim(store, store.claim("example"), callback))
    assert result["error"] == "audit_timeout"
    assert store.get_project("example")["running"] is False


def test_cancellation_releases_lease_and_stops_callback(store):
    async def exercise():
        started = asyncio.Event()
        cancelled = asyncio.Event()
        async def callback(**kwargs):
            started.set()
            try:
                await asyncio.sleep(60)
            finally:
                cancelled.set()
        task = asyncio.create_task(monitoring.execute_claim(store, store.claim("example"), callback))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()
    asyncio.run(exercise())
    assert store.get_project("example")["running"] is False
    assert store.get_project("example")["last_error"] == "cancelled"


def test_provider_exception_is_redacted(store):
    callback = AsyncMock(side_effect=RuntimeError("signed_url?token=secret"))
    result = asyncio.run(monitoring.execute_claim(store, store.claim("example"), callback))
    assert result["error"] == "audit_failed"
    assert "secret" not in json.dumps(store.list_events("example"))


def test_concurrent_due_workers_execute_one_audit(store):
    store.set_schedule("example", True, 60, now=0)
    callback = AsyncMock(return_value=sample_report())
    async def exercise():
        return await asyncio.gather(*(monitoring.run_due_audits(store, callback) for _ in range(5)))
    result = asyncio.run(exercise())
    assert sum(len(batch) for batch in result) == 1
    callback.assert_awaited_once()


def test_worker_batch_is_bounded(store):
    store.set_schedule("example", True, 60, now=0)
    store.create_project("second", "https://example.com/")
    store.set_schedule("second", True, 60, now=0)
    result = asyncio.run(monitoring.run_due_audits(store, AsyncMock(return_value=sample_report()), limit=1))
    assert len(result) == 1
    assert store.claim() is not None


def test_all_monitor_tools_have_truthful_metadata_and_run_end_to_end(store):
    mcp = FastMCP("test")
    monitoring.register_monitoring_tools(mcp, AsyncMock(return_value=sample_report()), store=store)
    async def exercise():
        tools = {tool.name: tool for tool in await mcp.list_tools()}
        assert len(tools) == 8
        assert tools["run_project_audit"].annotations.readOnlyHint is False
        assert tools["run_project_audit"].annotations.destructiveHint is True
        assert tools["set_audit_schedule"].annotations.openWorldHint is True
        assert tools["get_project_audit"].annotations.openWorldHint is False
        run = await mcp._tool_manager.call_tool("run_project_audit", {"project_id": "example"})
        saved = await mcp._tool_manager.call_tool("get_project_audit", {"project_id": "example", "audit_id": run["audit_id"]})
        assert saved == sample_report()
        projects = await mcp._tool_manager.call_tool("list_audit_projects", {})
        assert projects["projects"][0]["last_status"] == "success"
        invalid = await mcp._tool_manager.call_tool("run_project_audit", {"project_id": "../outside"})
        assert "error" in invalid
    asyncio.run(exercise())


def test_cli_once_and_poll_validation(monkeypatch, capsys):
    worker = AsyncMock()
    monkeypatch.setattr(monitoring, "_worker", worker)
    monkeypatch.setattr("sys.argv", ["mcp-seo-monitor", "--once"])
    monitoring.main()
    worker.assert_awaited_once_with(True, 30)
    monkeypatch.setattr("sys.argv", ["mcp-seo-monitor", "--poll-seconds", "0"])
    with pytest.raises(SystemExit) as exc:
        monitoring.main()
    assert exc.value.code == 2
