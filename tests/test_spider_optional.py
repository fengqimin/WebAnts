"""Spider optional-layer wiring tests for ADR-0005 T4."""

from contextlib import asynccontextmanager

import httpx
import pytest

from webants.libs.config import Config
from webants.libs.middleware import (
    Middleware,
    MiddlewareManager,
    RequestDeduplicator,
    ResponseCache,
)
from webants.libs.persister import BasePersister, JsonPersister, PersisterManager
from webants.libs.request import Request
from webants.libs.result import Result
from webants.spider import Spider


def site_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text=f"page:{request.url.path}", request=request)


@asynccontextmanager
async def offline_spider(spider: Spider, handler=site_handler):
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
    await spider._schedule(request)
    slot = await spider.scheduler.get_request()
    await spider.process_request(slot.request, slot=slot)
    while not spider.scheduler.request_queue.empty():
        slot = await spider.scheduler.get_request()
        await spider.process_request(slot.request, slot=slot)


class SimpleSpider(Spider):
    name = "simple"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.seen_urls = []

    async def parse(self, response, **kwargs):
        self.seen_urls.append(str(response.url))
        yield Result(
            spider=self.name,
            fields={"path": response.url.path},
            url=str(response.url),
        )


class NoopSpider(Spider):
    name = "noop"

    async def parse(self, response, **kwargs):
        return None


class ErrbackSpider(SimpleSpider):
    name = "errback"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.errors = []

    async def handle_error(self, failure, request):
        self.errors.append(failure)


class DropRequestMiddleware(Middleware):
    async def process_request(self, request):
        return None

    async def process_response(self, response, request):
        return response

    async def process_exception(self, exception, request):
        return None


class DropResponseMiddleware(Middleware):
    async def process_request(self, request):
        return request

    async def process_response(self, response, request):
        return None

    async def process_exception(self, exception, request):
        return None


class RetryOnceMiddleware(Middleware):
    async def process_request(self, request):
        return request

    async def process_response(self, response, request):
        return response

    async def process_exception(self, exception, request):
        if request.meta.get("_middleware_retry"):
            return None
        return request.copy(dont_filter=True)


class AlwaysRetryMiddleware(RetryOnceMiddleware):
    async def process_exception(self, exception, request):
        return request.copy(dont_filter=True)


class SubstituteResponseMiddleware(Middleware):
    async def process_request(self, request):
        return request

    async def process_response(self, response, request):
        return response

    async def process_exception(self, exception, request):
        return httpx.Response(
            200,
            text="fallback",
            request=httpx.Request("GET", request.url),
        )


class CrossDomainMiddleware(Middleware):
    async def process_request(self, request):
        return request.copy(url="http://other.example/x")

    async def process_response(self, response, request):
        return response

    async def process_exception(self, exception, request):
        return None


class RewriteMiddleware(Middleware):
    async def process_request(self, request):
        return request.copy(url=f"{request.url}?from=middleware")

    async def process_response(self, response, request):
        return response

    async def process_exception(self, exception, request):
        return None


class RaisingExceptionMiddleware(Middleware):
    async def process_request(self, request):
        return request

    async def process_response(self, response, request):
        return response

    async def process_exception(self, exception, request):
        raise RuntimeError("middleware failure")


class FailingSavePersister(BasePersister):
    async def save(self, data):
        return False

    async def load(self, query=None):
        return []

    async def _write_batch(self):
        return 0


class FailingClosePersister(FailingSavePersister):
    async def close(self):
        raise RuntimeError("close failed")


def test_defaults_are_unchanged_without_optional_layers():
    spider = Spider()

    assert spider.middleware is None
    assert spider.persister is None
    assert spider.downloader.concurrency == 10
    assert spider.scheduler.max_domain_concurrent == 10
    assert spider.scheduler.domain_delay == 0
    assert spider.parser.cache_size == 1000
    assert spider.settings["CONCURRENT_REQUESTS"] == 10
    for key in (
        "filtered_requests",
        "filtered_responses",
        "middleware_retries",
        "cache_hits",
        "persist_failures",
    ):
        assert spider.stats[key] == 0
    assert "middleware_stats" not in spider.get_stats()
    assert "persister_stats" not in spider.get_stats()


async def test_cache_hit_bypasses_downloader():
    cache = ResponseCache()
    middleware = MiddlewareManager()
    middleware.add_middleware(cache)
    cached_request = Request("http://example.com/a")
    cached_response = httpx.Response(
        200,
        text="cached",
        request=httpx.Request("GET", cached_request.url),
    )
    await cache.process_response(cached_response, cached_request)

    spider = SimpleSpider(middleware=middleware)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text="network", request=request)

    async with offline_spider(spider, handler):
        await run_request_chain(
            spider, Request("http://example.com/a", callback=spider.parse)
        )

    assert calls == []
    assert spider.stats["cache_hits"] == 1
    assert spider.stats["successful_requests"] == 1
    assert spider.stats["failed_requests"] == 0
    assert "middleware_stats" in spider.get_stats()


async def test_middleware_filtered_request_releases_slot():
    middleware = MiddlewareManager()
    middleware.add_middleware(DropRequestMiddleware())
    spider = NoopSpider(middleware=middleware)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, request=request)

    async with offline_spider(spider, handler):
        await run_request_chain(
            spider, Request("http://example.com/a", callback=spider.parse)
        )

    assert calls == []
    assert spider.stats["filtered_requests"] == 1
    assert spider.stats["successful_requests"] == 0
    assert spider.stats["failed_requests"] == 0
    assert spider.scheduler.audit()["idle"] is True
    assert spider.scheduler.stats["slot_mismatches"] == 0


async def test_middleware_filtered_response_skips_callback():
    middleware = MiddlewareManager()
    middleware.add_middleware(DropResponseMiddleware())
    spider = SimpleSpider(middleware=middleware)

    async with offline_spider(spider):
        await run_request_chain(
            spider, Request("http://example.com/a", callback=spider.parse)
        )

    assert spider.stats["filtered_responses"] == 1
    assert spider.stats["successful_requests"] == 0
    assert spider.stats["failed_requests"] == 0
    assert spider.seen_urls == []
    assert spider.scheduler.audit()["idle"] is True


async def test_middleware_can_retry_a_failed_request():
    middleware = MiddlewareManager()
    middleware.add_middleware(RetryOnceMiddleware())
    spider = ErrbackSpider(middleware=middleware)
    calls = []

    def flaky(request):
        calls.append(request.url)
        if len(calls) == 1:
            return httpx.Response(503, request=request)
        return httpx.Response(200, text="ok", request=request)

    async with offline_spider(spider, flaky):
        await run_request_chain(
            spider,
            Request("http://example.com/a", callback=spider.parse, retries=0),
        )

    assert len(calls) == 2
    assert spider.stats["middleware_retries"] == 1
    assert spider.stats["successful_requests"] == 1
    assert spider.stats["failed_requests"] == 0
    assert spider.errors == []
    assert spider.scheduler.audit()["idle"] is True


async def test_middleware_retry_limit_falls_back_to_failure():
    middleware = MiddlewareManager()
    middleware.add_middleware(AlwaysRetryMiddleware())
    spider = ErrbackSpider(middleware=middleware, middleware_max_retries=1)
    calls = []

    def failing(request):
        calls.append(request.url)
        return httpx.Response(503, request=request)

    async with offline_spider(spider, failing):
        await run_request_chain(
            spider,
            Request(
                "http://example.com/a",
                callback=spider.parse,
                errback=spider.handle_error,
                retries=0,
            ),
        )

    assert len(calls) == 2
    assert spider.stats["middleware_retries"] == 1
    assert spider.stats["failed_requests"] == 1
    assert len(spider.errors) == 1


async def test_middleware_can_substitute_a_response():
    middleware = MiddlewareManager()
    middleware.add_middleware(SubstituteResponseMiddleware())
    spider = ErrbackSpider(middleware=middleware)

    def failing(request):
        return httpx.Response(503, request=request)

    async with offline_spider(spider, failing):
        await run_request_chain(
            spider,
            Request("http://example.com/a", callback=spider.parse, retries=0),
        )

    assert spider.stats["successful_requests"] == 1
    assert spider.stats["failed_requests"] == 0
    assert spider.seen_urls == ["http://example.com/a"]
    assert spider.errors == []


async def test_middleware_cannot_change_request_domain():
    middleware = MiddlewareManager()
    middleware.add_middleware(CrossDomainMiddleware())
    spider = ErrbackSpider(middleware=middleware)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, request=request)

    async with offline_spider(spider, handler):
        await run_request_chain(
            spider,
            Request(
                "http://example.com/a",
                callback=spider.parse,
                errback=spider.handle_error,
            ),
        )

    assert calls == []
    assert spider.stats["failed_requests"] == 1
    assert len(spider.errors) == 1
    assert isinstance(spider.errors[0], ValueError)


async def test_results_are_persisted_and_flushed_on_close(tmp_path):
    persister = JsonPersister(tmp_path / "results.json", batch_size=100)
    manager = PersisterManager(tmp_path)
    manager.add_persister("json", persister)
    spider = SimpleSpider(persister=manager)

    async with offline_spider(spider):
        await run_request_chain(
            spider, Request("http://example.com/a", callback=spider.parse)
        )

    assert not (tmp_path / "results.json").exists()

    await spider.close()

    data = await persister.load()
    assert len(data) == 1
    assert spider.stats["items_scraped"] == 1
    assert "persister_stats" in spider.get_stats()


async def test_persist_failure_does_not_fail_the_request(tmp_path):
    manager = PersisterManager(tmp_path)
    manager.add_persister("bad", FailingSavePersister(tmp_path / "bad.json"))
    spider = SimpleSpider(persister=manager)

    async with offline_spider(spider):
        await run_request_chain(
            spider, Request("http://example.com/a", callback=spider.parse)
        )

    assert spider.stats["successful_requests"] == 1
    assert spider.stats["failed_requests"] == 0
    assert spider.stats["persist_failures"] == 1


def test_sync_signal_rejects_coroutine_handler():
    spider = Spider()

    async def handler(sender):
        return None

    with pytest.raises(TypeError, match="synchronous"):
        spider.signals.connect("item_scraped", handler)


async def test_middleware_can_replace_the_request():
    middleware = MiddlewareManager()
    middleware.add_middleware(RewriteMiddleware())
    spider = SimpleSpider(middleware=middleware)
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(200, request=request)

    async with offline_spider(spider, handler):
        await run_request_chain(
            spider, Request("http://example.com/a", callback=spider.parse)
        )

    assert seen == ["http://example.com/a?from=middleware"]
    assert spider.seen_urls == ["http://example.com/a?from=middleware"]
    assert spider.stats["successful_requests"] == 1
    assert spider.scheduler.audit()["idle"] is True


async def test_middleware_exception_handler_error_falls_back_to_failure():
    middleware = MiddlewareManager()
    middleware.add_middleware(RaisingExceptionMiddleware())
    spider = ErrbackSpider(middleware=middleware)

    def failing(request):
        return httpx.Response(503, request=request)

    async with offline_spider(spider, failing):
        await run_request_chain(
            spider,
            Request(
                "http://example.com/a",
                callback=spider.parse,
                errback=spider.handle_error,
                retries=0,
            ),
        )

    assert spider.stats["failed_requests"] == 1
    assert spider.stats["middleware_retries"] == 0
    assert len(spider.errors) == 1


async def test_close_closes_injected_persister(tmp_path):
    persister = JsonPersister(tmp_path / "results.json")
    manager = PersisterManager(tmp_path)
    manager.add_persister("json", persister)
    spider = SimpleSpider(persister=manager)

    await spider.close()

    assert persister.closed is True
    assert spider.downloader.client.is_closed is True


async def test_persister_close_failure_does_not_block_downloader_close(tmp_path):
    manager = PersisterManager(tmp_path)
    manager.add_persister("bad", FailingClosePersister(tmp_path / "bad.json"))
    spider = SimpleSpider(persister=manager)

    await spider.close()

    assert spider.downloader.client.is_closed is True


async def test_assembly_with_config_middleware_and_persister(tmp_path):
    """ADR-0005 acceptance: a full offline assembly runs end to end."""
    config = Config()
    config.set("scheduler.domain_delay", 0)

    persister = JsonPersister(tmp_path / "results.json", batch_size=1)
    persisters = PersisterManager(tmp_path)
    persisters.add_persister("json", persister)

    middleware = MiddlewareManager()
    middleware.add_middleware(RequestDeduplicator())
    middleware.add_middleware(ResponseCache(ttl=60))

    spider = SimpleSpider(
        config=config,
        middleware=middleware,
        persister=persisters,
    )
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, text="ok", request=request)

    async with offline_spider(spider, handler):
        await run_request_chain(
            spider, Request("http://example.com/a", callback=spider.parse)
        )
        await run_request_chain(
            spider,
            Request(
                "http://example.com/a",
                callback=spider.parse,
                dont_filter=True,
            ),
        )

    await spider.close()

    assert calls == ["http://example.com/a"]
    # The second request is served by ResponseCache, before the deduplicator.
    assert spider.stats["successful_requests"] == 2
    assert spider.stats["cache_hits"] == 1
    assert spider.stats["filtered_requests"] == 0
    assert len(await persister.load()) == 2
    assert "middleware_stats" in spider.get_stats()
    assert "persister_stats" in spider.get_stats()
    assert spider.scheduler.audit()["idle"] is True
