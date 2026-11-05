import asyncio
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import httpx
import pytest

from webants.libs.exceptions import RequestError
from webants.libs.request import Request
from webants.libs.result import Result
from webants.scheduler import ScheduleReason
from webants.spider import Spider


def site_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text=f"page:{request.url.path}", request=request)


def failing_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(503, text="unavailable", request=request)


@asynccontextmanager
async def offline_spider(spider: Spider, handler):
    """Replace the downloader client with a MockTransport-backed client."""
    await spider.downloader.client.aclose()
    spider.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        yield spider
    finally:
        await spider.downloader.close()


async def run_request_chain(spider: Spider, request: Request) -> None:
    """Schedule and process one request, then drain requests it spawns."""
    await spider._schedule(request)
    slot = await spider.scheduler.get_request()
    await spider.process_request(slot.request, slot=slot)
    while not spider.scheduler.request_queue.empty():
        slot = await spider.scheduler.get_request()
        await spider.process_request(slot.request, slot=slot)


def collect_items(spider: Spider) -> list:
    items = []

    def on_item(sender, item):
        items.append(item)

    spider.signals.connect("item_scraped", on_item)
    return items


class DemoSpider(Spider):
    name = "demo"
    start_urls = ["http://example.com/"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parsed_paths = []

    async def parse(self, response, **kwargs):
        self.parsed_paths.append(response.url.path)
        if response.url.path == "/":
            yield Request(url="http://example.com/a", callback=self.parse)
            yield Request(url="http://example.com/b", callback=self.parse)
        yield Result(
            spider=self.name,
            fields={"path": response.url.path},
            url=str(response.url),
        )


class CoroutineSpider(Spider):
    name = "coroutine"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.token = None

    async def parse(self, response, token=None, **kwargs):
        self.token = token
        return Result(
            spider=self.name,
            fields={"path": response.url.path},
            url=str(response.url),
        )


class ListSpider(Spider):
    name = "list"

    async def parse(self, response, **kwargs):
        return [
            Result(spider=self.name, fields={"index": 1}, url=str(response.url)),
            Result(spider=self.name, fields={"index": 2}, url=str(response.url)),
        ]


class NoopSpider(Spider):
    name = "noop"

    async def parse(self, response, **kwargs):
        return None


class BadYieldSpider(Spider):
    name = "bad-yield"

    async def parse(self, response, **kwargs):
        yield "not-an-item"


class RaisingSpider(Spider):
    name = "raising"

    async def parse(self, response, **kwargs):
        raise ValueError("boom")


class SingleResultSpider(Spider):
    name = "single"

    async def parse(self, response, **kwargs):
        yield Result(
            spider=self.name,
            fields={"path": response.url.path},
            url=str(response.url),
        )


class Escaped(BaseException):
    """BaseException subclass: not caught by process_request's except Exception."""


class EscapingSpider(Spider):
    name = "escaping"
    start_urls = ["http://example.com/"]

    async def parse(self, response, **kwargs):
        raise Escaped("escaped from callback")
        yield  # pragma: no cover - keeps parse an async generator


@pytest.mark.asyncio
async def test_start_requests_accepts_async_generator_parse():
    spider = DemoSpider()

    requests = [request async for request in spider.start_requests()]

    assert len(requests) == 1
    assert requests[0].callback == spider.parse
    await spider.downloader.close()


@pytest.mark.asyncio
async def test_end_to_end_async_generator_callback():
    spider = DemoSpider(retry_delay=0)
    async with offline_spider(spider, site_handler):
        items = collect_items(spider)
        request = Request(
            url="http://example.com/",
            callback=spider.parse,
            errback=spider.handle_error,
        )

        await run_request_chain(spider, request)

        assert set(spider.parsed_paths) == {"/", "/a", "/b"}
        assert len(items) == 3
        assert spider.stats["successful_requests"] == 3
        assert spider.stats["failed_requests"] == 0
        assert spider.stats["completed_requests"] == 3
        assert spider.downloader.stats["total_requests"] == 3
        assert spider.scheduler.stats["total_scheduled"] == 3
        assert spider.scheduler.stats["current_requests"] == 0
        # Every slot taken during the crawl was handed back.
        audit = spider.scheduler.audit()
        assert audit["mismatch_count"] == 0
        assert audit["outstanding"] == 0
        assert audit["balanced"] is True
        assert audit["idle"] is True
        assert spider._inflight == {}


@pytest.mark.asyncio
async def test_coroutine_callback_receives_kwargs():
    spider = CoroutineSpider(retry_delay=0)
    async with offline_spider(spider, site_handler):
        items = collect_items(spider)
        request = Request(
            url="http://example.com/",
            callback=spider.parse,
            cb_kwargs={"token": "secret"},
        )

        await run_request_chain(spider, request)

        assert spider.token == "secret"
        assert len(items) == 1
        assert spider.stats["successful_requests"] == 1


@pytest.mark.asyncio
async def test_coroutine_callback_returning_list():
    spider = ListSpider(retry_delay=0)
    async with offline_spider(spider, site_handler):
        items = collect_items(spider)
        request = Request(url="http://example.com/", callback=spider.parse)

        await run_request_chain(spider, request)

        assert len(items) == 2
        assert spider.stats["successful_requests"] == 1


@pytest.mark.asyncio
async def test_coroutine_callback_returning_none():
    spider = NoopSpider(retry_delay=0)
    async with offline_spider(spider, site_handler):
        items = collect_items(spider)
        request = Request(url="http://example.com/", callback=spider.parse)

        await run_request_chain(spider, request)

        assert items == []
        assert spider.stats["successful_requests"] == 1


@pytest.mark.asyncio
async def test_invalid_yield_goes_to_errback_once():
    spider = BadYieldSpider(retry_delay=0)
    errors = []

    async def errback(error, request):
        errors.append(error)

    async with offline_spider(spider, site_handler):
        items = collect_items(spider)
        request = Request(url="http://example.com/", callback=spider.parse, errback=errback)

        await run_request_chain(spider, request)

        assert items == []
        assert len(errors) == 1
        assert isinstance(errors[0], TypeError)
        assert spider.stats["successful_requests"] == 0
        assert spider.stats["failed_requests"] == 1
        assert spider._inflight == {}


@pytest.mark.asyncio
async def test_callback_exception_goes_to_errback_once():
    spider = RaisingSpider(retry_delay=0)
    errors = []

    async def errback(error, request):
        errors.append(error)

    async with offline_spider(spider, site_handler):
        request = Request(url="http://example.com/", callback=spider.parse, errback=errback)

        await run_request_chain(spider, request)

        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)
        assert spider.stats["failed_requests"] == 1


@pytest.mark.asyncio
async def test_downloader_failure_reaches_errback():
    spider = SingleResultSpider(retry_delay=0)
    spider.downloader._sleep = AsyncMock()
    errors = []

    async def errback(error, request):
        errors.append(error)

    async with offline_spider(spider, failing_handler):
        request = Request(
            url="http://example.com/",
            callback=spider.parse,
            errback=errback,
            retries=1,
        )

        await run_request_chain(spider, request)

        assert len(errors) == 1
        assert isinstance(errors[0], RequestError)
        assert errors[0].code == "RETRY_EXHAUSTED"
        assert spider.stats["failed_requests"] == 1
        assert spider.stats["successful_requests"] == 0
        assert spider.downloader.stats["total_attempts"] == 2
        assert spider.downloader.stats["failed_requests"] == 1
        assert spider.scheduler.stats["current_requests"] == 0


@pytest.mark.asyncio
async def test_unscheduled_request_does_not_raise():
    """P1-01: processing without a slot must not touch scheduler accounting"""
    spider = NoopSpider(retry_delay=0)
    async with offline_spider(spider, site_handler):
        request = Request(url="http://example.com/", callback=spider.parse)

        await spider.process_request(request)

        assert spider.stats["successful_requests"] == 1
        assert spider._inflight == {}
        assert spider.scheduler.stats["slot_mismatches"] == 0
        assert spider.scheduler.audit()["balanced"] is True


@pytest.mark.asyncio
async def test_circuit_blocked_request_releases_resources():
    """P1-18: a request refused by the breaker still gives its slot back"""
    spider = NoopSpider(retry_delay=0, failure_threshold=1)
    async with offline_spider(spider, site_handler):
        domain = "example.com"
        request = Request(url="http://example.com/", callback=spider.parse)

        await spider._schedule(request)
        slot = await spider.scheduler.get_request()
        sem = spider.scheduler.domain_semaphores[domain]
        spider._get_circuit_breaker(domain).record_failure()

        await spider.process_request(slot.request, slot=slot)

        assert spider._inflight == {}
        assert sem._value == spider.scheduler.max_domain_concurrent
        assert spider.scheduler.stats["current_requests"] == 0
        assert spider.scheduler.stats["slot_mismatches"] == 0
        assert spider.scheduler.audit()["idle"] is True
        assert spider.stats["successful_requests"] == 0
        assert spider.stats["failed_requests"] == 0


@pytest.mark.asyncio
async def test_scheduled_signal_fires_only_for_queued_requests():
    """P2-22: request_scheduled tracks real enqueue, not process_request entry"""
    spider = DemoSpider(retry_delay=0)
    async with offline_spider(spider, site_handler):
        signals = []
        spider.signals.connect(
            "request_scheduled", lambda sender, request: signals.append(request.url)
        )

        await spider._schedule(Request(url="http://example.com/"))
        duplicate = await spider._schedule(Request(url="http://example.com/"))

        assert not duplicate
        assert duplicate.reason is ScheduleReason.DUPLICATED
        assert signals == ["http://example.com/"]


@pytest.mark.asyncio
async def test_inflight_keeps_same_url_requests_distinct():
    """E5: one finishing request must not empty the in-flight table"""
    spider = NoopSpider(retry_delay=0)
    gate = asyncio.Event()
    calls = {"count": 0}

    async def scripted_fetch(request):
        calls["count"] += 1
        if calls["count"] == 1:
            gate.set()
            await asyncio.sleep(0.2)
        elif calls["count"] == 2:
            await asyncio.sleep(0.05)
        return httpx.Response(200, request=httpx.Request("GET", request.url))

    async with offline_spider(spider, site_handler):
        spider.downloader.fetch = scripted_fetch
        holding = Request(url="http://example.com/dup", callback=spider.parse)
        finishing = Request(url="http://example.com/dup", callback=spider.parse)

        long_task = asyncio.create_task(spider.process_request(holding))
        await gate.wait()
        short_task = asyncio.create_task(spider.process_request(finishing))
        await asyncio.sleep(0)

        assert spider.get_stats()["inflight_count"] == 2

        await short_task
        assert short_task.done() and not long_task.done()
        assert spider.get_stats()["inflight_count"] == 1

        await long_task
        assert spider._inflight == {}


@pytest.mark.asyncio
async def test_start_runs_main_loop_to_completion():
    """The polling main loop drains the queue, releases slots, then exits"""
    spider = DemoSpider(retry_delay=0, concurrent_requests=2)
    async with offline_spider(spider, site_handler):
        items = collect_items(spider)

        await asyncio.wait_for(spider.start(), timeout=10)

        assert set(spider.parsed_paths) == {"/", "/a", "/b"}
        assert len(items) == 3
        assert spider._running is False
        assert spider._inflight == {}
        assert spider.get_stats()["inflight_count"] == 0
        audit = spider.scheduler.audit()
        assert audit["outstanding"] == 0
        assert audit["mismatch_count"] == 0
        assert audit["idle"] is True
        assert spider.downloader.client.is_closed is True


@pytest.mark.asyncio
async def test_start_with_empty_queue_exits_immediately():
    """No start URLs means no work: the loop must not hang"""
    spider = NoopSpider(retry_delay=0)
    spider.start_urls = []
    async with offline_spider(spider, site_handler):
        await asyncio.wait_for(spider.start(), timeout=5)

        assert spider.stats["total_requests"] == 0
        assert spider.scheduler.audit()["idle"] is True


@pytest.mark.asyncio
async def test_cancelled_request_releases_its_slot():
    """Cancelling an in-flight task must return the permit via finally"""
    spider = NoopSpider(retry_delay=0)
    started = asyncio.Event()

    async def hung_fetch(request):
        started.set()
        await asyncio.sleep(10)
        return httpx.Response(200, request=httpx.Request("GET", request.url))

    async with offline_spider(spider, site_handler):
        spider.downloader.fetch = hung_fetch
        await spider._schedule(Request(url="http://example.com/", callback=spider.parse))
        slot = await spider.scheduler.get_request()
        task = asyncio.create_task(spider.process_request(slot.request, slot=slot))

        await started.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        assert slot.released is True
        assert spider._inflight == {}
        audit = spider.scheduler.audit()
        assert audit["mismatch_count"] == 0
        assert audit["idle"] is True


@pytest.mark.asyncio
async def test_closing_releases_queued_slots():
    """E2 consumer side: discard_pending hands queued permits back"""
    spider = NoopSpider(retry_delay=0)
    async with offline_spider(spider, site_handler):
        for index in range(3):
            await spider._schedule(Request(url=f"http://example.com/{index}"))
        assert spider.scheduler.stats["current_requests"] == 3

        assert spider.scheduler.discard_pending() == 3

        assert spider.scheduler.audit()["idle"] is True


@pytest.mark.asyncio
async def test_close_before_start_releases_resources():
    """E1: close() must work without start() and announce exactly once"""
    spider = NoopSpider()
    stopped = []
    spider.signals.connect("spider_stopped", lambda sender: stopped.append(sender))

    assert spider.downloader.client.is_closed is False

    await spider.close()

    assert spider.downloader.client.is_closed is True
    assert spider.get_stats()["closed"] is True
    assert spider.stats["finish_time"] is not None
    assert stopped == [spider]


@pytest.mark.asyncio
async def test_close_cancels_inflight_and_drains_queue():
    """E2: close() cancels running work, drains queued slots, leaves audit idle"""
    spider = NoopSpider(retry_delay=0)
    started = asyncio.Event()

    async def hung_fetch(request):
        started.set()
        await asyncio.sleep(10)
        return httpx.Response(200, request=httpx.Request("GET", request.url))

    async with offline_spider(spider, site_handler):
        spider.downloader.fetch = hung_fetch
        for index in range(3):
            await spider._schedule(
                Request(url=f"http://example.com/{index}", callback=spider.parse)
            )

        slot = await spider.scheduler.get_request()
        task = asyncio.create_task(spider.process_request(slot.request, slot=slot))
        spider._inflight[task] = slot.request
        task.add_done_callback(spider._on_task_done)
        await started.wait()

        await spider.close()

        assert task.done() and task.cancelled()
        assert spider._inflight == {}
        assert spider.scheduler.request_queue.qsize() == 0
        assert spider.scheduler.stats["discarded_on_close"] == 2
        assert spider.stats["cancelled_requests"] == 1
        assert spider.stats["failed_requests"] == 0
        assert spider.get_stats()["closed"] is True

        audit = spider.scheduler.audit()
        assert audit["idle"] is True
        assert audit["mismatch_count"] == 0
        assert audit["current_requests"] == 0
        assert spider.downloader.client.is_closed is True


@pytest.mark.asyncio
async def test_close_is_idempotent_and_concurrent_safe():
    """I7: repeated and concurrent close() releases once and never raises"""
    spider = NoopSpider()
    stopped = []
    spider.signals.connect("spider_stopped", lambda sender: stopped.append(sender))

    calls = {"n": 0}
    original_close = spider.downloader.close

    async def counting_close():
        calls["n"] += 1
        await original_close()

    spider.downloader.close = counting_close

    await asyncio.gather(spider.close(), spider.close(), spider.close())
    await spider.close()

    assert calls["n"] == 1
    assert stopped == [spider]


@pytest.mark.asyncio
async def test_start_after_close_raises():
    """I8: shutdown is terminal"""
    spider = NoopSpider()
    await spider.close()

    with pytest.raises(RuntimeError, match="closed"):
        await spider.start()


@pytest.mark.asyncio
async def test_task_escape_is_counted_and_consumed():
    """E4: escaped BaseException is consumed by the done callback, not leaked"""
    import gc

    spider = EscapingSpider(retry_delay=0, idle_grace=0)
    loop = asyncio.get_running_loop()
    reported = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, context: reported.append(context))

    try:
        async with offline_spider(spider, site_handler):
            await asyncio.wait_for(spider.start(), timeout=5)
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)

    assert spider.stats["unhandled_task_errors"] == 1
    assert spider.stats["failed_requests"] == 0
    assert all("never retrieved" not in str(context.get("message", "")) for context in reported)


@pytest.mark.asyncio
async def test_idle_grace_controls_exit_delay():
    """E5: idle_grace=0 exits at once, a positive grace keeps the loop waiting"""
    fast_spider = DemoSpider(retry_delay=0, idle_grace=0)
    async with offline_spider(fast_spider, site_handler):
        started = time.monotonic()
        await asyncio.wait_for(fast_spider.start(), timeout=10)
        fast = time.monotonic() - started

    slow_spider = DemoSpider(retry_delay=0, idle_grace=0.5)
    async with offline_spider(slow_spider, site_handler):
        started = time.monotonic()
        await asyncio.wait_for(slow_spider.start(), timeout=10)
        slow = time.monotonic() - started

    assert fast < 0.2
    assert slow >= 0.5


@pytest.mark.asyncio
async def test_context_manager_closes_without_starting():
    """async with provides the minimal close guarantee"""
    async with NoopSpider() as spider:
        assert spider._running is False
        assert spider.downloader.client.is_closed is False

    assert spider.downloader.client.is_closed is True
    assert spider.get_stats()["closed"] is True


@pytest.mark.asyncio
async def test_schedule_after_close_returns_none():
    """Closed spiders drop new requests instead of enqueueing them"""
    spider = NoopSpider()
    await spider.close()

    result = await spider._schedule(Request(url="http://example.com/late"))

    assert result is None
    assert spider.scheduler.request_queue.qsize() == 0
    assert spider.scheduler.stats["current_requests"] == 0


class TimedSpider(Spider):
    name = "timed"

    async def parse(self, response, **kwargs):
        await asyncio.sleep(0.02 if response.url.path == "/fast" else 0.06)
        yield Result(spider=self.name, fields={}, url=str(response.url))


@pytest.mark.asyncio
async def test_avg_request_time_uses_completed_requests():
    """In-flight requests must not dilute the completed-request average."""
    spider = TimedSpider(concurrent_requests=2)
    async with offline_spider(spider, site_handler):
        await spider._schedule(Request(url="http://example.com/fast", callback=spider.parse))
        await spider._schedule(Request(url="http://example.com/slow", callback=spider.parse))
        first = await spider.scheduler.get_request()
        second = await spider.scheduler.get_request()
        await asyncio.gather(
            spider.process_request(first.request, slot=first),
            spider.process_request(second.request, slot=second),
        )

    stats = spider.get_stats()
    assert stats["completed_requests"] == 2
    assert stats["total_requests"] == 2
    assert stats["avg_request_time"] == pytest.approx(
        stats["total_request_time"] / stats["completed_requests"]
    )
