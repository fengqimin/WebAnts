import pytest
import asyncio
from webants.scheduler import Scheduler
from webants.libs.request import Request


@pytest.fixture
def scheduler():
    return Scheduler(
        max_requests=100,
        request_delay=0.01,
        domain_delay=0.02,
        max_domain_concurrent=3,
        max_queue_size=50,
        log_level=10,
    )


@pytest.mark.asyncio
async def test_scheduler_initialization(scheduler):
    """Test scheduler initialization and default values"""
    assert scheduler.max_requests == 100
    assert scheduler.request_delay == 0.01
    assert scheduler.domain_delay == 0.02
    assert scheduler.max_domain_concurrent == 3
    assert scheduler.max_queue_size == 50
    assert isinstance(scheduler.seen_urls, set)
    assert len(scheduler.seen_urls) == 0
    assert isinstance(scheduler.domain_stats, dict)
    assert len(scheduler.domain_stats) == 0


@pytest.mark.asyncio
async def test_schedule_single_request(scheduler):
    """Test scheduling a single request"""
    request = Request(url="http://example.com/page1")
    result = await scheduler.schedule_request(request)
    assert result is True
    assert "example.com" in scheduler.domain_stats
    assert scheduler.stats["total_scheduled"] == 1
    assert scheduler.stats["total_filtered"] == 0


@pytest.mark.asyncio
async def test_duplicate_request_filtering(scheduler):
    """Test duplicate request filtering"""
    request = Request(url="http://example.com/page1")
    result1 = await scheduler.schedule_request(request)
    result2 = await scheduler.schedule_request(request)
    assert result1 is True
    assert result2 is False
    assert scheduler.stats["total_filtered"] == 1


@pytest.mark.asyncio
async def test_max_requests_limit(scheduler):
    """Test maximum requests limit"""
    scheduler.max_requests = 2
    req1 = Request(url="http://example.com/page1")
    req2 = Request(url="http://example.com/page2")
    req3 = Request(url="http://example.com/page3")

    assert await scheduler.schedule_request(req1) is True
    assert await scheduler.schedule_request(req2) is True
    assert await scheduler.schedule_request(req3) is False


@pytest.mark.asyncio
async def test_domain_concurrency(scheduler):
    """Test domain concurrency limits"""
    scheduler.max_domain_concurrent = 2
    urls = [f"http://example.com/page{i}" for i in range(4)]
    requests = [Request(url=url) for url in urls]

    # Schedule requests simultaneously
    tasks = [asyncio.create_task(scheduler.schedule_request(req)) for req in requests]
    done, pending = await asyncio.wait(tasks, timeout=5)
    assert sum((t.result() for t in done)) == 2
    [t.cancel() for t in pending]  # Cancel pending tasks
    # print(results)
    print(scheduler.get_stats())

    # assert (
    #     sum(results[0]) <= 2
    # )  # Only 2 requests should be scheduled due to concurrency limit
    assert scheduler.domain_stats["example.com"]["active_requests"] <= 2


@pytest.mark.asyncio
async def test_get_request(scheduler):
    """Test getting requests from queue"""
    original_request = Request(url="http://example.com/page1")
    await scheduler.schedule_request(original_request)

    retrieved_request = await scheduler.get_request()
    assert retrieved_request.url == original_request.url


@pytest.mark.asyncio
async def test_request_completed(scheduler):
    """Test request completion handling"""
    request = Request(url="http://example.com/page1")
    await scheduler.schedule_request(request)
    initial_active = scheduler.domain_stats["example.com"]["active_requests"]

    scheduler.request_completed(request)
    assert (
        scheduler.domain_stats["example.com"]["active_requests"] == initial_active - 1
    )


@pytest.mark.asyncio
async def test_get_stats(scheduler):
    """Test statistics collection"""
    request = Request(url="http://example.com/page1")
    await scheduler.schedule_request(request)

    stats = scheduler.get_stats()
    assert isinstance(stats, dict)
    assert "total_scheduled" in stats
    assert "queue_size" in stats
    assert "domain_stats" in stats
    assert stats["total_scheduled"] == 1
    assert "example.com" in stats["domain_stats"]


@pytest.mark.asyncio
async def test_domain_delay(scheduler):
    """Test domain-specific delay"""
    request1 = Request(url="http://example.com/page1")
    request2 = Request(url="http://example.com/page2")

    start_time = asyncio.get_event_loop().time()
    await scheduler.schedule_request(request1)
    await scheduler.schedule_request(request2)
    end_time = asyncio.get_event_loop().time()

    # Should have at least waited for domain_delay
    assert end_time - start_time >= scheduler.domain_delay
