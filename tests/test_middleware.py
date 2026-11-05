"""Middleware chain tests for ADR-0005 T3.

Covers the ``lookup`` phase, request-chain type validation and ResponseCache
statistics/TTL without touching the network.
"""

import httpx
import pytest

from webants.libs.middleware import (
    Middleware,
    MiddlewareManager,
    RequestDeduplicator,
    ResponseCache,
)
from webants.libs.request import Request


class FakeTime:
    """Minimal clock used to keep TTL tests deterministic."""

    def __init__(self) -> None:
        self.now = 1000.0

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    fake = FakeTime()
    monkeypatch.setattr("webants.libs.middleware.time", fake)
    return fake


class Recorder(Middleware):
    """Records the types reaching the request chain."""

    def __init__(self) -> None:
        super().__init__()
        self.seen = []

    async def process_request(self, request):
        self.seen.append(type(request).__name__)
        return request

    async def process_response(self, response, request):
        return response

    async def process_exception(self, exception, request):
        return None


class BadRequestMiddleware(Middleware):
    """Returns a response from process_request, which is now invalid."""

    async def process_request(self, request):
        return httpx.Response(200)

    async def process_response(self, response, request):
        return response

    async def process_exception(self, exception, request):
        return None


def _request(path: str = "a") -> Request:
    return Request(f"http://example.com/{path}")


async def test_lookup_miss_returns_none_and_counts():
    cache = ResponseCache()

    assert await cache.lookup(_request()) is None
    assert cache.stats == {"hits": 0, "misses": 1, "expired": 0, "evictions": 0}


async def test_lookup_hit_returns_cached_response():
    cache = ResponseCache()
    request = _request()
    response = httpx.Response(200, text="ok")

    await cache.process_response(response, request)
    assert await cache.lookup(request) is response
    assert cache.stats["hits"] == 1
    assert cache.stats["misses"] == 0


async def test_process_request_is_passthrough():
    cache = ResponseCache()
    request = _request()
    await cache.process_response(httpx.Response(200), request)

    assert await cache.process_request(request) is request


async def test_manager_lookup_does_not_run_request_chain():
    manager = MiddlewareManager()
    cache = ResponseCache()
    recorder = Recorder()
    manager.add_middleware(cache)
    manager.add_middleware(recorder)
    request = _request()
    response = httpx.Response(200, text="cached")

    assert await manager.lookup(request) is None
    await cache.process_response(response, request)

    assert await manager.lookup(request) is response
    assert recorder.seen == []

    # A miss still runs the request chain with Request, never a Response.
    assert await manager.process_request(request) is request
    assert recorder.seen == ["Request"]


async def test_manager_rejects_non_request_from_process_request():
    manager = MiddlewareManager()
    manager.add_middleware(BadRequestMiddleware())

    with pytest.raises(TypeError, match="process_request must return Request"):
        await manager.process_request(_request())


async def test_manager_lookup_returns_first_hit_only():
    manager = MiddlewareManager()
    first = ResponseCache()
    second = ResponseCache()
    manager.add_middleware(first)
    manager.add_middleware(second)
    request = _request()
    first_response = httpx.Response(200, text="first")
    second_response = httpx.Response(200, text="second")
    await first.process_response(first_response, request)
    await second.process_response(second_response, request)

    assert await manager.lookup(request) is first_response
    assert first.stats["hits"] == 1
    assert second.stats["hits"] == 0


async def test_ttl_expiry_returns_miss(clock):
    cache = ResponseCache(ttl=10)
    request = _request()
    await cache.process_response(httpx.Response(200), request)

    clock.advance(11)

    assert await cache.lookup(request) is None
    assert cache.stats["expired"] == 1
    assert cache.stats["misses"] == 1


async def test_ttl_is_sliding_on_lookup(clock):
    cache = ResponseCache(ttl=10)
    request = _request()
    response = httpx.Response(200)
    await cache.process_response(response, request)

    clock.advance(6)
    assert await cache.lookup(request) is response

    clock.advance(6)
    # 12s after the initial store, but only 6s after the last access.
    assert await cache.lookup(request) is response
    assert cache.stats["expired"] == 0


async def test_process_response_touches_cached_object(clock):
    cache = ResponseCache(ttl=10)
    request = _request()
    response = httpx.Response(200)
    await cache.process_response(response, request)

    clock.advance(6)
    await cache.process_response(response, request)
    clock.advance(6)

    assert await cache.lookup(request) is response
    assert cache.stats["hits"] == 1


async def test_lru_eviction(clock):
    cache = ResponseCache(max_size=2)
    responses = {}
    for path in ("a", "b", "c"):
        request = _request(path)
        response = httpx.Response(200, text=path)
        responses[path] = response
        await cache.process_response(response, request)
        clock.advance(1)

    assert cache.stats["evictions"] == 1
    assert await cache.lookup(_request("a")) is None
    assert await cache.lookup(_request("b")) is responses["b"]
    assert await cache.lookup(_request("c")) is responses["c"]


async def test_process_response_replaces_existing_url(clock):
    cache = ResponseCache(max_size=2)
    request = _request()
    first = httpx.Response(200, text="first")
    second = httpx.Response(200, text="second")
    await cache.process_response(first, request)
    clock.advance(1)

    await cache.process_response(second, request)

    assert await cache.lookup(request) is second
    assert cache.stats["evictions"] == 0


async def test_zero_max_size_disables_caching():
    cache = ResponseCache(max_size=0)
    request = _request()

    await cache.process_response(httpx.Response(200), request)

    assert await cache.lookup(request) is None
    assert cache.stats["misses"] == 1


async def test_manager_get_stats():
    manager = MiddlewareManager()
    cache = ResponseCache()
    manager.add_middleware(cache)
    await cache.lookup(_request())

    assert manager.get_stats() == {
        "ResponseCache": {"hits": 0, "misses": 1, "expired": 0, "evictions": 0}
    }


async def test_request_deduplicator_drops_duplicate():
    manager = MiddlewareManager()
    manager.add_middleware(RequestDeduplicator())
    request = _request()

    assert await manager.process_request(request) is request
    assert await manager.process_request(request) is None
