import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import AsyncMock

import httpx
import pytest

from webants.downloader import Downloader
from webants.libs.exceptions import RequestError
from webants.libs.request import Request


def sequence_handler(*outcomes):
    """Return a MockTransport handler that yields outcomes in order.

    Outcomes may be status codes, exceptions, or callables taking the request.
    The last outcome repeats once the sequence is exhausted.
    """
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        outcome = outcomes[min(len(calls) - 1, len(outcomes) - 1)]
        if isinstance(outcome, Exception):
            raise outcome
        if callable(outcome):
            return outcome(request)
        return httpx.Response(outcome, request=request)

    handler.calls = calls
    return handler


async def install_handler(downloader: Downloader, handler) -> None:
    await downloader.client.aclose()
    downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.fixture
async def downloader():
    """Offline downloader with deterministic, zero-jitter retries."""
    dl = Downloader(concurrency=2, log_level=logging.DEBUG, retry_jitter=0)
    dl._sleep = AsyncMock()
    yield dl
    await dl.close()


@pytest.mark.asyncio
async def test_normal_request(downloader):
    handler = sequence_handler(200)
    await install_handler(downloader, handler)

    req = Request(url="http://example.com/get?name=downloader")
    resp = await downloader.fetch(req)

    assert resp.status_code == 200
    assert len(handler.calls) == 1
    assert downloader.stats["total_requests"] == 1
    assert downloader.stats["total_attempts"] == 1
    assert downloader.stats["successful_requests"] == 1
    assert downloader.stats["failed_requests"] == 0


@pytest.mark.asyncio
async def test_transport_retry_then_success(downloader):
    handler = sequence_handler(httpx.ConnectError("boom"), 200)
    await install_handler(downloader, handler)

    req = Request(url="http://retry.example.com", retries=3)
    resp = await downloader.fetch(req)

    assert resp.status_code == 200
    assert len(handler.calls) == 2
    assert req.retries == 3, "fetch must not mutate request.retries"
    assert downloader.stats["total_attempts"] == 2
    assert downloader.stats["total_retries"] == 1
    assert downloader.stats["retry_requests"] == 1
    assert downloader.stats["successful_requests"] == 1
    downloader._sleep.assert_awaited_once()
    assert downloader._sleep.await_args.args[0] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_status_retry_then_success(downloader):
    handler = sequence_handler(500, 500, 200)
    await install_handler(downloader, handler)

    req = Request(url="http://status.example.com", retries=2)
    resp = await downloader.fetch(req)

    assert resp.status_code == 200
    assert len(handler.calls) == 3
    assert downloader.stats["total_attempts"] == 3
    assert downloader.stats["total_retries"] == 2
    assert downloader.stats["retry_requests"] == 1
    assert [call.args[0] for call in downloader._sleep.await_args_list] == [
        pytest.approx(1.0),
        pytest.approx(2.0),
    ]


@pytest.mark.asyncio
async def test_retryable_status_exhaustion_raises(downloader):
    handler = sequence_handler(503)
    await install_handler(downloader, handler)

    req = Request(url="http://down.example.com", retries=1)
    with pytest.raises(RequestError) as exc_info:
        await downloader.fetch(req)

    error = exc_info.value
    assert error.code == "RETRY_EXHAUSTED"
    assert error.metadata["status_code"] == 503
    assert error.metadata["attempts"] == 2
    assert error.metadata["url"] == req.url
    assert downloader.stats["failed_requests"] == 1
    assert downloader.stats["retry_requests"] == 1
    assert downloader.stats["total_attempts"] == 2


@pytest.mark.asyncio
async def test_transport_error_exhaustion_raises(downloader):
    handler = sequence_handler(httpx.ConnectError("boom"))
    await install_handler(downloader, handler)

    req = Request(url="http://down.example.com", retries=1)
    with pytest.raises(RequestError) as exc_info:
        await downloader.fetch(req)

    error = exc_info.value
    assert error.code == "FETCH_FAILED"
    assert error.metadata["cause"] == "ConnectError"
    assert error.metadata["attempts"] == 2
    assert downloader.stats["failed_requests"] == 1
    assert downloader.stats["total_attempts"] == 2


@pytest.mark.asyncio
async def test_404_is_not_retried(downloader):
    handler = sequence_handler(404)
    await install_handler(downloader, handler)

    req = Request(url="http://missing.example.com", retries=3)
    resp = await downloader.fetch(req)

    assert resp.status_code == 404
    assert len(handler.calls) == 1
    assert downloader.stats["total_retries"] == 0
    assert downloader.stats["successful_requests"] == 1


@pytest.mark.asyncio
async def test_downloader_never_invokes_callback(downloader):
    called = []

    async def callback(response, **kwargs):
        called.append(response)
        return response

    handler = sequence_handler(200)
    await install_handler(downloader, handler)

    req = Request(url="http://callback.example.com", callback=callback)
    resp = await downloader.fetch(req)

    assert resp.status_code == 200
    assert called == [], "Downloader must not invoke request.callback"


@pytest.mark.asyncio
async def test_body_cookies_and_referer_are_sent(downloader):
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content"] = request.content
        captured["headers"] = request.headers
        return httpx.Response(200, request=request)

    await install_handler(downloader, handler)

    referer = Request(url="http://ref.example.com/")
    req = Request(
        url="http://example.com/post",
        method="POST",
        body={"name": "downloader"},
        cookies={"sid": "abc"},
        headers={"X-Test": "1"},
        referer=referer,
    )
    await downloader.fetch(req)

    assert json.loads(captured["content"]) == {"name": "downloader"}
    assert captured["headers"]["X-Test"] == "1"
    assert "sid=abc" in captured["headers"]["cookie"]
    assert captured["headers"]["referer"] == "http://ref.example.com/"


@pytest.mark.asyncio
async def test_retry_after_seconds_is_honored(downloader):
    def too_many_requests(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429, headers={"Retry-After": "7"}, request=request
        )

    handler = sequence_handler(too_many_requests, 200)
    await install_handler(downloader, handler)

    req = Request(url="http://rate-limited.example.com", retries=1)
    resp = await downloader.fetch(req)

    assert resp.status_code == 200
    delays = [call.args[0] for call in downloader._sleep.await_args_list]
    assert delays == [pytest.approx(7.0)]


def test_retry_after_http_date_is_parsed():
    future = datetime.now(timezone.utc) + timedelta(seconds=30)
    response = httpx.Response(
        503,
        headers={"Retry-After": format_datetime(future, usegmt=True)},
    )

    delay = Downloader._parse_retry_after(response)

    assert delay is not None
    assert 20 <= delay <= 40


@pytest.mark.asyncio
async def test_statistics_accuracy(downloader):
    handler = sequence_handler(200)
    await install_handler(downloader, handler)

    for index in range(5):
        await downloader.fetch(Request(url=f"http://stats.example.com/{index}"))

    assert downloader.stats["total_requests"] == 5
    assert downloader.stats["total_attempts"] == 5
    assert downloader.stats["successful_requests"] == 5
    assert downloader.stats["failed_requests"] == 0
    assert downloader.stats["min_response_time"] >= 0
    assert downloader.stats["max_response_time"] >= downloader.stats["min_response_time"]


@pytest.mark.asyncio
async def test_concurrency_control(downloader):
    sem = downloader.sem
    initial_value = sem._value

    async with sem:
        assert sem._value == initial_value - 1
        await asyncio.sleep(0)

    assert sem._value == initial_value


@pytest.mark.asyncio
async def test_fetch_retry_alias_delegates_to_fetch(downloader):
    handler = sequence_handler(200)
    await install_handler(downloader, handler)

    req = Request(url="http://alias.example.com")
    resp = await downloader.fetch_retry(req)

    assert resp.status_code == 200
    assert downloader.stats["total_attempts"] == 1
