"""Durable snapshot, retention, lease and inbox behavior with real local SQLite."""

from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3

import pytest

import seo_storage as storage


def sample_report(issues=None, pages=None, **updates):
    result = {
        "schema_version": "1.0", "generated_at": "2026-09-27T00:00:00+00:00",
        "audit_status": "complete", "start_url": "https://example.com/", "origin": "https://example.com",
        "settings": {"max_pages": 25, "respect_robots": True}, "summary": {}, "coverage": {},
        "issues": issues or [], "pages": pages if pages is not None else [
            {"url": "https://example.com/", "state": "html", "status": 200, "is_start_page": True}],
    }
    result.update(updates)
    return result


def issue(rule="missing_title", url="https://example.com/"):
    return {"rule_id": rule, "affected_urls": [url], "severity": "high", "evidence": []}


@pytest.fixture
def store(tmp_path):
    result = storage.AuditStore(tmp_path / "data")
    result.create_project("example", "https://example.com/")
    return result


def save(store, report=None, now=1000):
    claim = store.claim("example", now=now)
    return store.finish_success("example", claim["token"], report or sample_report(), now=now + 1)


def test_explicit_store_location_and_disabled_schedule(tmp_path, monkeypatch):
    monkeypatch.setenv("SEO_AUDIT_DATA_DIR", str(tmp_path / "configured"))
    store = storage.AuditStore()
    project = store.create_project("example", "https://example.com/")
    assert store.path == tmp_path / "configured" / "audits.sqlite3"
    assert project["schedule_enabled"] is False
    assert project["next_run_at"] is None
    assert project["settings"]["respect_robots"] is True
    assert store.claim(now=10 ** 10) is None


@pytest.mark.parametrize("project_id", ["../bad", "UPPER", "a/b", "", "a" * 65, "-first", "foo' OR 1=1--"])
def test_rejects_invalid_project_ids(store, project_id):
    with pytest.raises(ValueError, match="project_id"):
        store.create_project(project_id, "https://example.com/")


@pytest.mark.parametrize("url", ["file:///etc/passwd", "https://u:p@example.com/", "https://example.com/#fragment", "https://example.com:99999", "https://example.com:0", "https://example.com/\nheader", "https://"])
def test_rejects_unsafe_or_invalid_project_urls(store, url):
    with pytest.raises(ValueError, match="start_url"):
        store.create_project("new", url)


@pytest.mark.parametrize("kwargs", [{"retention": 0}, {"retention": 101}, {"max_pages": True}, {"max_pages": 501}, {"render_mode": "unsafe"}, {"include_sitemaps": "true"}])
def test_bounded_project_settings(store, kwargs):
    with pytest.raises(ValueError):
        store.create_project("new", "https://example.com/", **kwargs)


def test_duplicate_project_does_not_replace_settings(store):
    with pytest.raises(ValueError, match="already exists"):
        store.create_project("example", "https://other.example/")
    assert store.get_project("example")["start_url"] == "https://example.com/"


def test_snapshots_survive_restart_and_are_project_scoped(store):
    report = sample_report([issue()])
    result = save(store, report)
    report["issues"].clear()
    reopened = storage.AuditStore(store.path.parent)
    assert reopened.load_audit("example", result["audit_id"])["issues"] == [issue()]
    reopened.create_project("other", "https://other.example/")
    with pytest.raises(ValueError, match="not found"):
        reopened.load_audit("other", result["audit_id"])
    assert len(reopened.list_audits("example")) == 1


def test_retention_prunes_only_oldest_project_snapshots(tmp_path):
    store = storage.AuditStore(tmp_path)
    store.create_project("example", "https://example.com/", retention=2)
    ids = [save(store, now=1000 + i * 10)["audit_id"] for i in range(3)]
    assert [item["audit_id"] for item in store.list_audits("example")] == ids[::-1][:2]
    with pytest.raises(ValueError, match="retention"):
        store.load_audit("example", ids[0])


def test_compare_history_and_only_meaningful_change_events(store):
    first = save(store, sample_report(), now=1000)
    second = save(store, sample_report([issue()]), now=1100)
    save(store, sample_report([issue()]), now=1200)
    fourth = save(store, sample_report(), now=1300)
    inbox = store.list_events("example")
    assert [event["kind"] for event in inbox["events"]] == ["findings_changed", "findings_changed"]
    assert inbox["events"][0]["details"]["counts"]["new"] == 1
    assert inbox["events"][1]["details"]["counts"]["resolved"] == 1
    assert store.compare_audits("example", first["audit_id"], second["audit_id"])["counts"]["new"] == 1
    assert store.compare_audits("example", second["audit_id"], fourth["audit_id"])["counts"]["resolved"] == 1
    assert store.list_events("example", after_id=inbox["next_after_id"])["events"] == []


def test_sampling_changes_are_not_change_events(store):
    save(store, sample_report([issue()]), now=1000)
    save(store, sample_report(pages=[{"url": "https://example.com/other", "state": "html", "status": 200}]), now=1100)
    assert store.list_events("example")["events"] == []


def test_changed_settings_create_new_baseline_without_false_alert(store):
    save(store, sample_report(), now=1000)
    save(store, sample_report([issue()], settings={"max_pages": 50}), now=1100)
    assert store.list_events("example")["events"] == []


def test_event_retention_and_cursor(store, monkeypatch):
    monkeypatch.setattr(storage, "MAX_EVENTS_PER_PROJECT", 2)
    for index in range(4):
        save(store, sample_report([issue()] if index % 2 else []), now=1000 + index * 10)
    inbox = store.list_events("example", limit=1)
    assert len(inbox["events"]) == 1
    assert inbox["oldest_available_id"] == 2
    assert store.list_events("example", inbox["next_after_id"])["events"][0]["event_id"] == 3


def test_report_size_and_shape_fail_without_partial_write(store, monkeypatch):
    claim = store.claim("example", now=1000)
    monkeypatch.setattr(storage, "MAX_REPORT_BYTES", 10)
    with pytest.raises(ValueError, match="limit"):
        store.finish_success("example", claim["token"], sample_report(), now=1001)
    assert store.list_audits("example") == []
    monkeypatch.setattr(storage, "MAX_REPORT_BYTES", 100000)
    with pytest.raises(ValueError, match="schema_version"):
        store.finish_success("example", claim["token"], {"error": "bad"}, now=1001)
    assert store.list_audits("example") == []


def test_transaction_rolls_back_snapshot_when_event_write_fails(store, monkeypatch):
    save(store, now=1000)
    claim = store.claim("example", now=1100)
    def fail_event(*args):
        raise sqlite3.OperationalError("disk full")
    monkeypatch.setattr(storage.AuditStore, "_event", fail_event)
    with pytest.raises(sqlite3.Error):
        store.finish_success("example", claim["token"], sample_report([issue()]), now=1101)
    assert len(store.list_audits("example")) == 1


def test_due_state_persists_and_concurrent_workers_claim_once(store):
    store.set_schedule("example", True, 60, now=1000)
    assert store.claim(now=1059) is None
    reopened = storage.AuditStore(store.path.parent)
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(lambda _: reopened.claim(now=1060), range(8)))
    assert sum(claim is not None for claim in claims) == 1
    assert store.claim(now=1061) is None


def test_expired_lease_recovers_and_old_worker_cannot_write(store):
    first = store.claim("example", now=1000)
    with pytest.raises(ValueError, match="already running"):
        store.claim("example", now=1001)
    second = store.claim("example", now=1000 + storage.LEASE_SECONDS + 1)
    with pytest.raises(ValueError, match="lease"):
        store.finish_success("example", first["token"], sample_report(), now=1662)
    store.finish_success("example", second["token"], sample_report(), now=1662)
    assert len(store.list_audits("example")) == 1


def test_failure_backoff_deduplication_and_recovery(store):
    store.set_schedule("example", True, 60, now=1000)
    first = store.claim(now=1060)
    store.finish_failure("example", first["token"], "audit_failed", now=1061)
    assert store.get_project("example")["next_run_at"] == 1121
    second = store.claim(now=1121)
    store.finish_failure("example", second["token"], "audit_failed", now=1122)
    assert store.get_project("example")["next_run_at"] == 1242
    assert len(store.list_events("example")["events"]) == 1
    third = store.claim(now=1242)
    store.finish_success("example", third["token"], sample_report(), now=1243)
    project = store.get_project("example")
    assert project["failure_count"] == 0
    assert project["next_run_at"] == 1303
    assert [event["kind"] for event in store.list_events("example")["events"]] == ["audit_failed", "audit_recovered"]


def test_provider_error_text_is_never_persisted(store):
    claim = store.claim("example", now=1000)
    store.finish_failure("example", claim["token"], "token=secret-value", now=1001)
    assert "secret-value" not in json.dumps(store.get_project("example"))
    assert "secret-value" not in json.dumps(store.list_events("example"))


def test_disable_during_active_run_prevents_future_scheduling(store):
    store.set_schedule("example", True, 60, now=1000)
    claim = store.claim(now=1060)
    store.set_schedule("example", False, 60, now=1061)
    store.finish_success("example", claim["token"], sample_report(), now=1062)
    assert store.get_project("example")["next_run_at"] is None
    assert store.claim(now=10000) is None


@pytest.mark.parametrize("interval", [0, 59, 2678401, True])
def test_schedule_rejects_unbounded_intervals(store, interval):
    with pytest.raises(ValueError, match="interval_seconds"):
        store.set_schedule("example", True, interval)


def test_future_database_schema_is_not_modified(tmp_path):
    connection = sqlite3.connect(tmp_path / "audits.sqlite3")
    connection.execute("PRAGMA user_version=999")
    connection.close()
    with pytest.raises(ValueError, match="database version"):
        storage.AuditStore(tmp_path)


def test_report_non_finite_numbers_rejected(store):
    report = sample_report()
    report["summary"]["bad"] = float("nan")
    claim = store.claim("example", now=1000)
    with pytest.raises(ValueError, match="finite JSON"):
        store.finish_success("example", claim["token"], report, now=1001)
