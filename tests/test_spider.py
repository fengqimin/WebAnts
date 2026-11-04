from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import httpx
import pytest

from webants.libs.exceptions import RequestError
from webants.libs.request import Request
from webants.libs.result import Result
from webants.spider import Spider


def site_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text=f"page:{request.url.path}", request=request)


def failing_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(503, text="unavailable", request=request)


@asynccontextmanager
async def offline_spider(spider: Spider, handler):
    """Replace the downloader client with a MockTransport-backed client."""
    await spider.downloader.client.aclose()
    spider.downloader.client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler)
    )
    try:
        yield spider
    finally:
        await spider.downloader.close()


async def run_request_chain(spider: Spider, request: Request) -> None:
    """Schedule and process one request, then drain requests it spawns."""
    await spider.scheduler.schedule_request(request)
    queued = await spider.scheduler.get_request()
    await spider.process_request(queued)
    while not spider.scheduler.request_queue.empty():
        queued = await spider.scheduler.get_request()
        await spider.process_request(queued)


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
        assert spider.downloader.stats["total_requests"] == 3
        assert spider.scheduler.stats["total_scheduled"] == 3
        assert spider.scheduler.stats["current_requests"] == 0


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
        request = Request(
            url="http://example.com/", callback=spider.parse, errback=errback
        )

        await run_request_chain(spider, request)

        assert items == []
        assert len(errors) == 1
        assert isinstance(errors[0], TypeError)
        assert spider.stats["successful_requests"] == 0
        assert spider.stats["failed_requests"] == 1
        assert spider._processing == set()


@pytest.mark.asyncio
async def test_callback_exception_goes_to_errback_once():
    spider = RaisingSpider(retry_delay=0)
    errors = []

    async def errback(error, request):
        errors.append(error)

    async with offline_spider(spider, site_handler):
        request = Request(
            url="http://example.com/", callback=spider.parse, errback=errback
        )

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
    spider = NoopSpider(retry_delay=0)
    async with offline_spider(spider, site_handler):
        request = Request(url="http://example.com/", callback=spider.parse)

        await spider.process_request(request)

        assert spider.stats["successful_requests"] == 1
        assert spider._processing == set()


@pytest.mark.asyncio
async def test_circuit_blocked_request_releases_resources():
    spider = NoopSpider(retry_delay=0, failure_threshold=1)
    async with offline_spider(spider, site_handler):
        domain = "example.com"
        request = Request(url="http://example.com/", callback=spider.parse)

        await spider.scheduler.schedule_request(request)
        queued = await spider.scheduler.get_request()
        sem = spider.scheduler.domain_semaphores[domain]
        spider._get_circuit_breaker(domain).record_failure()

        await spider.process_request(queued)

        assert spider._processing == set()
        assert sem._value == spider.scheduler.max_domain_concurrent
        assert spider.scheduler.stats["current_requests"] == 0
        assert spider.stats["successful_requests"] == 0
        assert spider.stats["failed_requests"] == 0
