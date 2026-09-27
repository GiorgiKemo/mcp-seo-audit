"""Local SQLite projects, immutable audit snapshots and a bounded event inbox.

Creating a store is explicit: importing this module never creates files or workers.
The database is for a trusted local user, not an authentication/tenant boundary.
"""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
from urllib.parse import urlsplit
from uuid import uuid4

from seo_reporting import compare_audit_reports


MAX_REPORT_BYTES = 10 * 1024 * 1024
MAX_PROJECTS = 100
MAX_EVENTS_PER_PROJECT = 500
LEASE_SECONDS = 660
_PROJECT_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")
_AUDIT_ID = re.compile(r"[a-f0-9]{32}\Z")


def data_directory():
    """Return the configured directory, otherwise the platform user-data directory."""
    configured = os.environ.get("SEO_AUDIT_DATA_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    if sys.platform == "win32":
        base = Path(os.environ.get("LOCALAPPDATA") or Path.home() / "AppData" / "Local")
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local" / "share")
    return base / "mcp-seo-audit"


def validate_project_id(project_id):
    if not isinstance(project_id, str) or not _PROJECT_ID.fullmatch(project_id):
        raise ValueError("project_id must be 1-64 lowercase letters, digits, hyphens or underscores, starting with a letter or digit.")
    return project_id


def _integer(value, name, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer from {minimum} to {maximum}.")
    return value


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


class AuditStore:
    """Use a fresh connection per operation so instances work across worker threads."""

    def __init__(self, directory=None):
        directory = Path(directory) if directory is not None else data_directory()
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = directory / "audits.sqlite3"
        with self._connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ValueError("Unsupported audit database version; use a compatible server version.")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS projects (
                    project_id TEXT PRIMARY KEY,
                    start_url TEXT NOT NULL,
                    settings_json TEXT NOT NULL,
                    retention INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    schedule_enabled INTEGER NOT NULL DEFAULT 0,
                    interval_seconds INTEGER NOT NULL DEFAULT 86400,
                    next_run_at REAL,
                    lease_token TEXT,
                    lease_until REAL,
                    last_run_at REAL,
                    last_status TEXT,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                );
                CREATE TABLE IF NOT EXISTS audits (
                    audit_id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL REFERENCES projects(project_id),
                    created_at REAL NOT NULL,
                    summary_json TEXT NOT NULL,
                    report_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS audit_history ON audits(project_id, created_at DESC);
                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT NOT NULL REFERENCES projects(project_id),
                    created_at REAL NOT NULL,
                    kind TEXT NOT NULL,
                    audit_id TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS event_inbox ON events(project_id, event_id);
                PRAGMA user_version=1;
            """)
        if os.name != "nt":
            self.path.chmod(0o600)

    @contextmanager
    def _connection(self, write=False):
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            if write:
                conn.execute("BEGIN IMMEDIATE")
            yield conn
            if write:
                conn.commit()
        except BaseException:
            if write:
                conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _project(row):
        if row is None:
            raise ValueError("Project not found.")
        result = dict(row)
        result["settings"] = json.loads(result.pop("settings_json"))
        result["schedule_enabled"] = bool(result["schedule_enabled"])
        result["running"] = bool(result.get("lease_until") and result["lease_until"] > time.time())
        result.pop("lease_token", None)
        result.pop("lease_until", None)
        return result

    def create_project(self, project_id, start_url, max_pages=25, render_mode="raw", include_sitemaps=True, retention=30):
        validate_project_id(project_id)
        _integer(max_pages, "max_pages", 1, 500)
        _integer(retention, "retention", 1, 100)
        if render_mode not in {"raw", "rendered", "compare"}:
            raise ValueError("render_mode must be raw, rendered or compare.")
        if not isinstance(include_sitemaps, bool):
            raise ValueError("include_sitemaps must be a boolean.")
        if not isinstance(start_url, str) or len(start_url) > 2048 or any(ord(c) < 33 for c in start_url):
            raise ValueError("start_url must be an HTTP(S) URL of at most 2048 characters without whitespace.")
        try:
            parsed = urlsplit(start_url)
            port = parsed.port
        except ValueError:
            raise ValueError("start_url must be a valid HTTP(S) URL.") from None
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
            raise ValueError("start_url must be an HTTP(S) URL without credentials or a fragment.")
        if port == 0:
            raise ValueError("start_url must use a valid nonzero port.")
        settings = {"max_pages": max_pages, "respect_robots": True, "render_mode": render_mode, "include_sitemaps": include_sitemaps}
        now = time.time()
        with self._connection(write=True) as conn:
            if conn.execute("SELECT count(*) FROM projects").fetchone()[0] >= MAX_PROJECTS:
                raise ValueError(f"The local store supports at most {MAX_PROJECTS} projects.")
            try:
                conn.execute("INSERT INTO projects(project_id,start_url,settings_json,retention,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                             (project_id, start_url, _json(settings), retention, now, now))
            except sqlite3.IntegrityError:
                raise ValueError("Project already exists; choose a unique project_id.") from None
            return self._project(conn.execute("SELECT * FROM projects WHERE project_id=?", (project_id,)).fetchone())

    def list_projects(self):
        with self._connection() as conn:
            return [self._project(row) for row in conn.execute("SELECT * FROM projects ORDER BY project_id")]

    def get_project(self, project_id):
        validate_project_id(project_id)
        with self._connection() as conn:
            return self._project(conn.execute("SELECT * FROM projects WHERE project_id=?", (project_id,)).fetchone())

    def set_schedule(self, project_id, enabled=False, interval_seconds=86400, now=None):
        validate_project_id(project_id)
        _integer(interval_seconds, "interval_seconds", 60, 2678400)
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean.")
        now = time.time() if now is None else now
        with self._connection(write=True) as conn:
            changed = conn.execute("""UPDATE projects SET schedule_enabled=?,interval_seconds=?,next_run_at=?,updated_at=?
                WHERE project_id=?""", (int(enabled), interval_seconds, now + interval_seconds if enabled else None, now, project_id))
            if changed.rowcount != 1:
                raise ValueError("Project not found.")
            return self._project(conn.execute("SELECT * FROM projects WHERE project_id=?", (project_id,)).fetchone())

    def claim(self, project_id=None, now=None):
        """Claim one due project, or an explicit manual run, under an expiring lease."""
        if project_id is not None:
            validate_project_id(project_id)
        now = time.time() if now is None else now
        with self._connection(write=True) as conn:
            if project_id is None:
                row = conn.execute("""SELECT * FROM projects WHERE schedule_enabled=1 AND next_run_at<=?
                    AND (lease_until IS NULL OR lease_until<=?) ORDER BY next_run_at,project_id LIMIT 1""", (now, now)).fetchone()
                if row is None:
                    return None
            else:
                row = conn.execute("SELECT * FROM projects WHERE project_id=?", (project_id,)).fetchone()
                if row is None:
                    raise ValueError("Project not found.")
                if row["lease_until"] and row["lease_until"] > now:
                    raise ValueError("An audit is already running for this project.")
            token = uuid4().hex
            conn.execute("UPDATE projects SET lease_token=?,lease_until=? WHERE project_id=?", (token, now + LEASE_SECONDS, row["project_id"]))
            return {"project": self._project(row), "token": token}

    @staticmethod
    def _leased_project(conn, project_id, token, now):
        row = conn.execute("SELECT * FROM projects WHERE project_id=? AND lease_token=? AND lease_until>?", (project_id, token, now)).fetchone()
        if row is None:
            raise ValueError("Audit lease expired or was replaced; this worker cannot save a result.")
        return row

    @staticmethod
    def _event(conn, project_id, now, kind, audit_id, payload):
        conn.execute("INSERT INTO events(project_id,created_at,kind,audit_id,payload_json) VALUES(?,?,?,?,?)",
                     (project_id, now, kind, audit_id, _json(payload)))
        conn.execute("""DELETE FROM events WHERE project_id=? AND event_id NOT IN
            (SELECT event_id FROM events WHERE project_id=? ORDER BY event_id DESC LIMIT ?)""",
                     (project_id, project_id, MAX_EVENTS_PER_PROJECT))

    def finish_success(self, project_id, token, report, now=None):
        """Atomically save an immutable snapshot, changes, retention and schedule state."""
        validate_project_id(project_id)
        try:
            encoded = _json(report)
        except (TypeError, ValueError, RecursionError):
            raise ValueError("Audit result must contain finite JSON values.") from None
        if len(encoded.encode("utf-8")) > MAX_REPORT_BYTES:
            raise ValueError("Audit result exceeds the 10 MiB snapshot limit.")
        # Validate the full comparison input before allowing a malformed durable baseline.
        compare_audit_reports(report, report)
        now = time.time() if now is None else now
        audit_id = uuid4().hex
        with self._connection(write=True) as conn:
            project = self._leased_project(conn, project_id, token, now)
            previous = conn.execute("SELECT audit_id,report_json FROM audits WHERE project_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1", (project_id,)).fetchone()
            summary = {"audit_status": report.get("audit_status"), "summary": report.get("summary", {}), "coverage": report.get("coverage", {})}
            conn.execute("INSERT INTO audits(audit_id,project_id,created_at,summary_json,report_json) VALUES(?,?,?,?,?)",
                         (audit_id, project_id, now, _json(summary), encoded))
            change_counts = None
            if previous:
                try:
                    comparison = compare_audit_reports(json.loads(previous["report_json"]), report)
                except ValueError:
                    # Settings/schema changes require a new baseline, never inferred changes.
                    comparison = None
                if comparison:
                    change_counts = comparison["counts"]
                    if comparison["new"] or comparison["resolved"]:
                        self._event(conn, project_id, now, "findings_changed", audit_id, {
                            "baseline_id": previous["audit_id"], "counts": comparison["counts"],
                            "new": comparison["new"][:100], "resolved": comparison["resolved"][:100],
                            "details_truncated": len(comparison["new"]) > 100 or len(comparison["resolved"]) > 100,
                        })
            if project["failure_count"]:
                self._event(conn, project_id, now, "audit_recovered", audit_id, {"previous_failures": project["failure_count"]})
            conn.execute("""DELETE FROM audits WHERE project_id=? AND audit_id NOT IN
                (SELECT audit_id FROM audits WHERE project_id=? ORDER BY created_at DESC,rowid DESC LIMIT ?)""",
                         (project_id, project_id, project["retention"]))
            conn.execute("""UPDATE projects SET lease_token=NULL,lease_until=NULL,last_run_at=?,last_status='success',
                failure_count=0,last_error=NULL,next_run_at=?,updated_at=? WHERE project_id=?""",
                         (now, now + project["interval_seconds"] if project["schedule_enabled"] else None, now, project_id))
        return {"project_id": project_id, "audit_id": audit_id, "created_at": now, **summary, "change_counts": change_counts}

    def finish_failure(self, project_id, token, error, now=None):
        """Persist a safe failure category, with one event per transition and capped backoff."""
        validate_project_id(project_id)
        # Callers supply fixed categories rather than sensitive provider exception strings.
        if error not in {"audit_failed", "audit_timeout", "invalid_report", "cancelled"}:
            error = "audit_failed"
        now = time.time() if now is None else now
        with self._connection(write=True) as conn:
            project = self._leased_project(conn, project_id, token, now)
            failures = project["failure_count"] + 1
            delay = min(project["interval_seconds"] * 2 ** min(failures - 1, 10), max(project["interval_seconds"], 86400))
            if not project["failure_count"] or project["last_error"] != error:
                self._event(conn, project_id, now, "audit_failed", None, {"reason": error})
            conn.execute("""UPDATE projects SET lease_token=NULL,lease_until=NULL,last_run_at=?,last_status='failed',
                failure_count=?,last_error=?,next_run_at=?,updated_at=? WHERE project_id=?""",
                         (now, failures, error, now + delay if project["schedule_enabled"] else None, now, project_id))
        return {"project_id": project_id, "error": error, "failure_count": failures}

    def list_audits(self, project_id, limit=30):
        validate_project_id(project_id)
        _integer(limit, "limit", 1, 100)
        self.get_project(project_id)
        with self._connection() as conn:
            return [{"audit_id": row["audit_id"], "created_at": row["created_at"], **json.loads(row["summary_json"])} for row in conn.execute(
                "SELECT audit_id,created_at,summary_json FROM audits WHERE project_id=? ORDER BY created_at DESC,rowid DESC LIMIT ?", (project_id, limit))]

    def load_audit(self, project_id, audit_id):
        validate_project_id(project_id)
        if not isinstance(audit_id, str) or not _AUDIT_ID.fullmatch(audit_id):
            raise ValueError("audit_id must be the 32-character ID returned by a saved audit.")
        with self._connection() as conn:
            row = conn.execute("SELECT report_json FROM audits WHERE project_id=? AND audit_id=?", (project_id, audit_id)).fetchone()
            if row is None:
                raise ValueError("Audit not found for this project, or it was removed by retention.")
            return json.loads(row["report_json"])

    def compare_audits(self, project_id, baseline_id, current_id):
        return compare_audit_reports(self.load_audit(project_id, baseline_id), self.load_audit(project_id, current_id))

    def list_events(self, project_id, after_id=0, limit=100):
        validate_project_id(project_id)
        _integer(after_id, "after_id", 0, 2 ** 63 - 1)
        _integer(limit, "limit", 1, 100)
        self.get_project(project_id)
        with self._connection() as conn:
            oldest = conn.execute("SELECT min(event_id) FROM events WHERE project_id=?", (project_id,)).fetchone()[0]
            rows = conn.execute("SELECT * FROM events WHERE project_id=? AND event_id>? ORDER BY event_id LIMIT ?", (project_id, after_id, limit)).fetchall()
            events = [{k: v for k, v in dict(row).items() if k != "payload_json"} | {"details": json.loads(row["payload_json"])} for row in rows]
        return {"events": events, "next_after_id": events[-1]["event_id"] if events else after_id,
                "oldest_available_id": oldest, "retention_limit": MAX_EVENTS_PER_PROJECT}
