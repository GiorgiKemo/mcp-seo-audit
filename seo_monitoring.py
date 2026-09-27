"""Explicit local scheduled-audit worker and MCP project tools; no import-time jobs."""

import argparse
import asyncio
import json
import sqlite3
import sys

from mcp.types import ToolAnnotations

from seo_storage import AuditStore


AUDIT_TIMEOUT_SECONDS = 600


async def execute_claim(store, claim, audit_callback):
    """Run one claimed audit. Cancellation records state, releases its lease and propagates."""
    project, token = claim["project"], claim["token"]
    project_id = project["project_id"]
    try:
        report = await asyncio.wait_for(
            audit_callback(start_url=project["start_url"], **project["settings"]), timeout=AUDIT_TIMEOUT_SECONDS)
        if not isinstance(report, dict) or "error" in report:
            return await asyncio.to_thread(store.finish_failure, project_id, token, "audit_failed")
        # Unavailable robots or an entirely failed crawl is not a successful baseline.
        if not any(page.get("state") == "html" and isinstance(page.get("status"), int) and 200 <= page["status"] < 300
                   for page in report.get("pages", []) if isinstance(page, dict)):
            return await asyncio.to_thread(store.finish_failure, project_id, token, "audit_failed")
        try:
            return await asyncio.to_thread(store.finish_success, project_id, token, report)
        except (ValueError, TypeError, KeyError, RecursionError):
            return await asyncio.to_thread(store.finish_failure, project_id, token, "invalid_report")
    except asyncio.CancelledError:
        try:
            await asyncio.shield(asyncio.to_thread(store.finish_failure, project_id, token, "cancelled"))
        except (ValueError, sqlite3.Error):
            pass  # A replaced lease belongs to another worker; never disturb it.
        raise
    except TimeoutError:
        return await asyncio.to_thread(store.finish_failure, project_id, token, "audit_timeout")
    except Exception:
        # Provider exceptions can contain signed URLs or credentials; do not persist them.
        return await asyncio.to_thread(store.finish_failure, project_id, token, "audit_failed")


async def run_due_audits(store, audit_callback, limit=25):
    """Run a bounded batch sequentially; database leases coordinate other workers."""
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ValueError("limit must be an integer from 1 to 100.")
    results = []
    for _ in range(limit):
        claim = await asyncio.to_thread(store.claim)
        if claim is None:
            break
        results.append(await execute_claim(store, claim, audit_callback))
    return results


def register_monitoring_tools(mcp, audit_callback, store=None):
    """Register tools without touching disk or starting a worker until explicitly invoked."""
    read = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
    write = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False)

    async def current_store():
        return store if store is not None else await asyncio.to_thread(AuditStore)

    async def invoke(method, *args):
        try:
            active = await current_store()
            return await asyncio.to_thread(getattr(active, method), *args)
        except ValueError as exc:
            return {"error": str(exc)}
        except (sqlite3.Error, OSError):
            return {"error": "Local audit storage is unavailable; check SEO_AUDIT_DATA_DIR permissions and available disk space."}

    @mcp.tool(annotations=write)
    async def create_audit_project(project_id: str, start_url: str, max_pages: int = 25, render_mode: str = "raw", include_sitemaps: bool = True, retention: int = 30) -> dict:
        """Create a local project with robots-aware audit settings and scheduling disabled.
        IDs use lowercase letters/digits/hyphens/underscores. Retain 1-100 snapshots
        (default 30); max_pages 1-500 is further capped by server configuration.
        Files live under SEO_AUDIT_DATA_DIR or the platform user-data directory.
        Rendered modes require the optional browser dependency and installed Chromium.
        """
        return await invoke("create_project", project_id, start_url, max_pages, render_mode, include_sitemaps, retention)

    @mcp.tool(annotations=read)
    async def list_audit_projects() -> dict:
        """List local projects, settings, schedule state and latest run status."""
        result = await invoke("list_projects")
        return result if isinstance(result, dict) else {"projects": result}

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=True))
    async def set_audit_schedule(project_id: str, enabled: bool = False, interval_seconds: int = 86400) -> dict:
        """Explicitly enable/disable a project's recurring audit (60 seconds to 31 days).
        First run is due one interval after enabling. A separate mcp-seo-monitor worker
        must be running; the MCP stdio server does not launch background work. Failures
        back off; disabling prevents future scheduled claims, leaving an active run to finish.
        """
        return await invoke("set_schedule", project_id, enabled, interval_seconds)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=True))
    async def run_project_audit(project_id: str) -> dict:
        """Fetch a project now, save its immutable report, and record verified finding changes.
        Prunes oldest snapshots to project retention. One run per project; ten-minute
        timeout. Returns audit_id; use get_project_audit for the full snapshot. Alerts
        stay in the local event inbox; no email, messaging or webhook is sent.
        """
        try:
            active = await current_store()
            claim = await asyncio.to_thread(active.claim, project_id)
            return await execute_claim(active, claim, audit_callback)
        except ValueError as exc:
            return {"error": str(exc)}
        except (sqlite3.Error, OSError):
            return {"error": "Local audit storage is unavailable; check SEO_AUDIT_DATA_DIR permissions and available disk space."}

    @mcp.tool(annotations=read)
    async def list_project_audits(project_id: str, limit: int = 30) -> dict:
        """List up to 100 retained snapshots, newest first, with coverage and issue summaries."""
        result = await invoke("list_audits", project_id, limit)
        return result if isinstance(result, dict) else {"audits": result}

    @mcp.tool(annotations=read)
    async def get_project_audit(project_id: str, audit_id: str) -> dict:
        """Load an immutable retained audit snapshot by its project-scoped audit_id."""
        return await invoke("load_audit", project_id, audit_id)

    @mcp.tool(annotations=read)
    async def compare_project_audits(project_id: str, baseline_id: str, current_id: str) -> dict:
        """Compare two retained project snapshots using applicable rechecks and coverage.
        Missing pages do not count as resolved; differing crawl settings cannot be compared.
        """
        return await invoke("compare_audits", project_id, baseline_id, current_id)

    @mcp.tool(annotations=read)
    async def list_audit_events(project_id: str, after_id: int = 0, limit: int = 100) -> dict:
        """Read a local change/failure/recovery inbox without marking events as read.
        Pass next_after_id on the next call; retain the latest 500 events per project.
        Only verified new/resolved findings trigger change events; sampling changes
        and unchanged reports stay quiet. No external notification is transmitted.
        """
        return await invoke("list_events", project_id, after_id, limit)


async def _worker(once=False, poll_seconds=30):
    # Keep Google initialization and the main server out of library/tool import paths.
    from gsc_server import get_seo_audit_report

    store = await asyncio.to_thread(AuditStore)
    while True:
        results = await run_due_audits(store, get_seo_audit_report)
        if results or once:
            print(json.dumps({"runs": results}), flush=True)
        if once:
            return
        await asyncio.sleep(poll_seconds)


def main():
    parser = argparse.ArgumentParser(description="Run explicitly enabled local SEO audit schedules. Ctrl+C cancels the active audit and stops the worker.")
    parser.add_argument("--once", action="store_true", help="Run one batch of at most 25 due projects and exit.")
    parser.add_argument("--poll-seconds", type=int, default=30, help="Check for due schedules every 1-60 seconds (default 30).")
    args = parser.parse_args()
    if not 1 <= args.poll_seconds <= 60:
        parser.error("--poll-seconds must be from 1 to 60")
    try:
        asyncio.run(_worker(args.once, args.poll_seconds))
    except KeyboardInterrupt:
        print("SEO audit monitor stopped.", file=sys.stderr)
    except (ValueError, OSError, sqlite3.Error):
        print("SEO audit monitor failed; check the local database version, directory permissions and disk space.", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
