import asyncio
import threading
import time
from unittest.mock import MagicMock, patch

import httplib2
import pytest
from googleapiclient.errors import HttpError

from seo_google import execute_google


def error(status=429, retry_after="0"):
    return HttpError(httplib2.Response({"status": status, "retry-after": retry_after}), b'{"error":"limited"}')


def test_read_retries_transient_failure_but_write_does_not():
    read = MagicMock()
    read.execute.side_effect = [error(), {"rows": []}]
    assert asyncio.run(execute_google(read)) == {"rows": []}
    assert read.execute.call_count == 2
    write = MagicMock()
    write.execute.side_effect = error()
    with pytest.raises(HttpError):
        asyncio.run(execute_google(write, read_only=False))
    assert write.execute.call_count == 1


def test_non_retryable_and_long_retry_after_stop():
    for failure in (error(403), error(429, "600"), error(429, "Tue, 1 Jan")):
        request = MagicMock()
        request.execute.side_effect = failure
        with pytest.raises(HttpError):
            asyncio.run(execute_google(request))
        assert request.execute.call_count == 1


def test_requests_are_serialized_and_event_loop_remains_responsive():
    active = 0
    highest = 0
    def blocking():
        nonlocal active, highest
        active += 1
        highest = max(active, highest)
        time.sleep(0.03)
        active -= 1
        return "done"
    requests = [MagicMock(), MagicMock()]
    for request in requests:
        request.execute.side_effect = blocking
    async def scenario():
        work = asyncio.gather(*(execute_google(request) for request in requests))
        await asyncio.sleep(0.005)
        assert not work.done()
        assert await work == ["done", "done"]
    asyncio.run(scenario())
    assert highest == 1


def test_cancellation_does_not_retry_active_request():
    entered = threading.Event()
    release = threading.Event()
    request = MagicMock()
    def blocking():
        entered.set()
        release.wait(timeout=1)
        raise error()
    request.execute.side_effect = blocking
    async def scenario():
        task = asyncio.create_task(execute_google(request))
        while not entered.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
    asyncio.run(scenario())
    assert request.execute.call_count == 1
