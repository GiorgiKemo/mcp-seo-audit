"""Serialized, nonblocking Google requests with bounded read-only retries."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from googleapiclient.errors import HttpError


# google-api-python-client's cached httplib2 transports are not thread safe.
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="seo-google")


async def google_service(factory):
    """Keep authentication/discovery off the event loop and serialize transport use."""
    return await asyncio.get_running_loop().run_in_executor(_executor, factory)


def _execute(request):
    transport = getattr(request.http, "http", request.http)
    transport.timeout = 30
    return request.execute()


async def execute_google(request, *, read_only=True):
    """Retry only provider reads. Cancellation stops queued work and later retries.

    An HTTP request already running in a thread cannot be recalled; its transport
    timeout bounds it. Writes are attempted exactly once, since an ambiguous
    timeout is not proof that Google did not apply the mutation.
    """
    for attempt in range(3 if read_only else 1):
        try:
            return await asyncio.get_running_loop().run_in_executor(_executor, partial(_execute, request))
        except HttpError as exc:
            if not read_only or attempt == 2 or exc.resp.status not in {429, 500, 502, 503, 504}:
                raise
            retry_after = exc.resp.get("retry-after")
            try:
                delay = float(retry_after) if retry_after is not None else 2 ** attempt
            except (TypeError, ValueError):
                # A date-based or malformed hint is not permission to retry immediately.
                raise exc
            if not 0 <= delay <= 30:
                raise
            await asyncio.sleep(delay)
