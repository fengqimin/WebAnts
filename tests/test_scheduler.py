import asyncio

import pytest

from webants.libs.request import Request
from webants.scheduler import Scheduler, ScheduleReason, ScheduleSlot


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
    assert scheduler.audit()["idle"] is True


@pytest.mark.asyncio
async def test_schedule_single_request(scheduler):
    """A successful schedule returns a slot and counts exactly one request"""
    request = Request(url="http://example.com/page1")
    result = await scheduler.schedule_request(request)

    assert result
    assert result.reason is ScheduleReason.SCHEDULED
    assert isinstance(result.slot, ScheduleSlot)
    assert result.slot.request is request
    assert "example.com" in scheduler.domain_stats
    assert scheduler.stats["total_scheduled"] == 1
    assert scheduler.stats["total_filtered"] == 0


@pytest.mark.asyncio
async def test_duplicate_request_filtering(scheduler):
    """Duplicate URLs are rejected without owning any resource"""
    request = Request(url="http://example.com/page1")
    first = await scheduler.schedule_request(request)
    second = await scheduler.schedule_request(request)

    assert first
    assert not second
    assert second.reason is ScheduleReason.DUPLICATED
    assert second.slot is None
    assert scheduler.stats["total_filtered"] == 1
    assert scheduler.stats["current_requests"] == 1


@pytest.mark.asyncio
async def test_max_requests_limit(scheduler):
    """Requests beyond the limit are rejected with an explicit reason"""
    scheduler.max_requests = 2
    req1 = Request(url="http://example.com/page1")
    req2 = Request(url="http://example.com/page2")
    req3 = Request(url="http://example.com/page3")

    assert await scheduler.schedule_request(req1)
    assert await scheduler.schedule_request(req2)
    rejected = await scheduler.schedule_request(req3)
    assert rejected.reason is ScheduleReason.LIMIT_REACHED
    assert scheduler.stats["total_scheduled"] == 2


@pytest.mark.asyncio
async def test_domain_concurrency(scheduler):
    """Domain concurrency caps scheduling and releases cleanly on cancel"""
    scheduler.max_domain_concurrent = 2
    requests = [Request(url=f"http://example.com/page{i}") for i in range(4)]

    tasks = [asyncio.create_task(scheduler.schedule_request(req)) for req in requests]
    done, pending = await asyncio.wait(tasks, timeout=5)

    assert sum(1 for task in done if task.result()) == 2
    assert scheduler.domain_stats["example.com"]["active_requests"] <= 2

    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    # Cancelling a waiter that never got a permit must leave nothing behind.
    audit = scheduler.audit()
    assert audit["balanced"] is True
    assert audit["domains"]["example.com"]["pending"] == 0

    await _drain_and_release(scheduler)
    assert scheduler.audit()["idle"] is True


async def _drain_and_release(scheduler):
    """Consume every queued slot and release it, like a shutdown would."""
    while True:
        slot = await scheduler.get_request()
        if slot is None:
            break
        slot.release()


@pytest.mark.asyncio
async def test_get_request(scheduler):
    """get_request hands back the slot that owns the queued request"""
    original_request = Request(url="http://example.com/page1")
    await scheduler.schedule_request(original_request)

    slot = await scheduler.get_request()

    assert isinstance(slot, ScheduleSlot)
    assert slot.request.url == original_request.url
    assert scheduler.domain_stats["example.com"]["active_requests"] == 1
    slot.release()
    assert scheduler.audit()["idle"] is True


@pytest.mark.asyncio
async def test_get_request_returns_none_on_empty_queue(scheduler):
    """An empty queue returns None instead of blocking the caller forever"""
    assert await scheduler.get_request() is None


@pytest.mark.asyncio
async def test_request_completed(scheduler):
    """The deprecated URL-based completion still releases accounting"""
    request = Request(url="http://example.com/page1")
    await scheduler.schedule_request(request)
    initial_active = scheduler.domain_stats["example.com"]["active_requests"]

    scheduler.request_completed(request)

    assert scheduler.domain_stats["example.com"]["active_requests"] == initial_active - 1
    assert scheduler.audit()["idle"] is True
    assert scheduler.stats["slot_mismatches"] == 0


@pytest.mark.asyncio
async def test_request_completed_for_unscheduled_request(scheduler):
    """P1-01: a never-scheduled request must not raise and must not corrupt"""
    scheduler.request_completed(Request(url="http://neverseen.example.com/x"))

    assert scheduler.stats["slot_mismatches"] == 1
    assert scheduler.stats["current_requests"] == 0
    assert scheduler.audit()["balanced"] is True


@pytest.mark.asyncio
async def test_double_slot_release_is_idempotent(scheduler):
    """E2: repeated release cannot push a domain past its concurrency ceiling"""
    request = Request(url="http://example.com/page1")
    result = await scheduler.schedule_request(request)
    max_concurrent = scheduler.max_domain_concurrent

    assert result.slot.release() is True
    assert result.slot.release() is False
    assert result.slot.release() is False

    assert scheduler.domain_semaphores["example.com"]._value == max_concurrent
    assert scheduler.domain_stats["example.com"]["active_requests"] == 0
    assert scheduler.stats["current_requests"] == 0
    assert scheduler.stats["slot_mismatches"] == 2
    assert scheduler.audit()["idle"] is True


@pytest.mark.asyncio
async def test_cancel_during_domain_delay_restores_permits():
    """E3: cancelling mid-delay leaves no counting or permit behind"""
    scheduler = Scheduler(
        max_domain_concurrent=2, domain_delay=5.0, max_queue_size=10, log_level=40
    )
    tasks = [
        asyncio.create_task(scheduler.schedule_request(Request(url=f"http://example.com/{index}")))
        for index in range(2)
    ]
    await asyncio.sleep(0.05)

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    domain_stats = scheduler.domain_stats["example.com"]
    assert domain_stats["pending_permits"] == 0
    assert domain_stats["active_requests"] == 0
    assert scheduler.seen_urls == set()
    assert scheduler.stats["total_scheduled"] == 0
    assert scheduler.domain_semaphores["example.com"]._value == scheduler.max_domain_concurrent
    assert scheduler.audit()["idle"] is True

    # A later request must still be schedulable.
    scheduler.domain_delay = 0
    assert await scheduler.schedule_request(Request(url="http://example.com/late"))


@pytest.mark.asyncio
async def test_queue_full_returns_reason_without_leaking():
    """E4: a full queue rejects immediately and keeps the URL re-schedulable"""
    scheduler = Scheduler(domain_delay=0, max_queue_size=1, log_level=40)

    first = await scheduler.schedule_request(Request(url="http://example.com/1"))
    second = await scheduler.schedule_request(Request(url="http://example.com/2"))

    assert first
    assert second.reason is ScheduleReason.QUEUE_FULL
    assert second.slot is None
    assert scheduler.stats["queue_full_count"] == 1
    # The rejected URL was never counted, so it can be scheduled again later.
    assert "http://example.com/2" not in scheduler.seen_urls
    assert scheduler.domain_stats["example.com"]["active_requests"] == 1
    assert scheduler.domain_semaphores["example.com"]._value == scheduler.max_domain_concurrent - 1
    assert scheduler.audit()["balanced"] is True


@pytest.mark.asyncio
async def test_pending_permit_keeps_audit_balanced():
    """Holding a permit during the domain delay is visible, not a mismatch"""
    scheduler = Scheduler(
        max_domain_concurrent=2, domain_delay=0.2, max_queue_size=10, log_level=40
    )

    task = asyncio.create_task(scheduler.schedule_request(Request(url="http://example.com/slow")))
    await asyncio.sleep(0.05)

    audit = scheduler.audit()
    assert audit["domains"]["example.com"]["pending"] == 1
    assert audit["balanced"] is True
    assert audit["idle"] is False

    assert await task
    assert scheduler.audit()["balanced"] is True
    await _drain_and_release(scheduler)
    assert scheduler.audit()["idle"] is True


@pytest.mark.asyncio
async def test_discard_pending_releases_queued_slots():
    """Shutdown drains the queue so queued requests cannot leak permits"""
    scheduler = Scheduler(domain_delay=0, max_queue_size=10, log_level=40)
    for index in range(3):
        await scheduler.schedule_request(Request(url=f"http://example.com/{index}"))

    discarded = scheduler.discard_pending()

    assert discarded == 3
    assert scheduler.stats["discarded_on_close"] == 3
    assert scheduler.request_queue.qsize() == 0
    assert scheduler.audit()["idle"] is True


@pytest.mark.asyncio
async def test_two_slots_for_same_url_are_independent():
    """dont_filter duplicates each own a slot that releases separately"""
    scheduler = Scheduler(domain_delay=0, max_domain_concurrent=5, max_queue_size=10, log_level=40)
    first = await scheduler.schedule_request(Request(url="http://example.com/dup"))
    second = await scheduler.schedule_request(
        Request(url="http://example.com/dup", dont_filter=True)
    )

    assert first.slot is not second.slot
    assert scheduler.domain_stats["example.com"]["active_requests"] == 2

    assert first.slot.release() is True
    assert scheduler.domain_stats["example.com"]["active_requests"] == 1
    assert second.slot.release() is True
    assert scheduler.audit()["idle"] is True


@pytest.mark.asyncio
async def test_concurrent_same_url_is_scheduled_once():
    """P1-04: the duplicate filter must hold across the domain delay await"""
    scheduler = Scheduler(
        domain_delay=0.05, max_domain_concurrent=10, max_queue_size=10, log_level=40
    )
    requests = [Request(url="http://example.com/dup") for _ in range(5)]

    results = await asyncio.gather(*(scheduler.schedule_request(request) for request in requests))

    assert sum(1 for result in results if result) == 1
    assert sum(1 for result in results if result.reason is ScheduleReason.DUPLICATED) == 4
    assert scheduler.request_queue.qsize() == 1
    assert scheduler.stats["total_scheduled"] == 1
    assert scheduler.audit()["reserved"] == 0
    await _drain_and_release(scheduler)
    assert scheduler.audit()["idle"] is True


@pytest.mark.asyncio
async def test_duplicate_filter_releases_reservation_after_rejection():
    """A rejected schedule must not leave the URL reserved forever"""
    scheduler = Scheduler(domain_delay=0, max_queue_size=1, log_level=40)

    await scheduler.schedule_request(Request(url="http://example.com/1"))
    rejected = await scheduler.schedule_request(Request(url="http://example.com/2"))

    assert rejected.reason is ScheduleReason.QUEUE_FULL
    assert scheduler.audit()["reserved"] == 0
    # The URL is free again, so it can be scheduled once the queue has room.
    scheduler.discard_pending()
    assert await scheduler.schedule_request(Request(url="http://example.com/2"))


@pytest.mark.asyncio
async def test_cancel_while_waiting_for_permit_keeps_accounting_balanced():
    """Cancellation during acquire must not reserve or count anything"""
    scheduler = Scheduler(domain_delay=0, max_domain_concurrent=1, max_queue_size=10, log_level=40)
    first = await scheduler.schedule_request(Request(url="http://example.com/1"))
    blocked = asyncio.create_task(scheduler.schedule_request(Request(url="http://example.com/2")))

    await asyncio.sleep(0.05)
    blocked.cancel()
    await asyncio.gather(blocked, return_exceptions=True)

    audit = scheduler.audit()
    assert audit["balanced"] is True
    assert audit["domains"]["example.com"]["pending"] == 0
    assert audit["reserved"] == 0
    assert "http://example.com/2" not in scheduler.seen_urls

    first.slot.release()
    assert scheduler.audit()["idle"] is True


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
