"""Scheduler Module
This module provides scheduling functionality with the following features:
- Priority-based request scheduling
- Duplicate URL filtering
- Resource limits management
- Request rate limiting
- Domain-specific scheduling policies
- Slot-ownership accounting for scheduled requests

Scheduling is the single owner of scheduling resources. A successful call to
``Scheduler.schedule_request`` returns a :class:`ScheduleSlot` that carries the
domain permit and the accounting entries acquired for that request. Whoever
holds the slot owns the obligation to release it exactly once; releasing is
idempotent and unpaired releases are counted instead of raising.
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Set
from urllib.parse import urlparse

from webants.libs.request import Request
from webants.utils.logger import get_logger


class ScheduleReason(Enum):
    """Why a scheduling attempt ended the way it did."""

    SCHEDULED = "scheduled"
    DUPLICATED = "duplicated"
    LIMIT_REACHED = "limit"
    QUEUE_FULL = "queue_full"


class ScheduleSlot:
    """Handle to the resources acquired by one successful enqueue.

    Only :class:`Scheduler` constructs slots. The slot identity, not the URL,
    identifies the reservation, so two in-flight requests for the same URL
    never collapse into a single bookkeeping entry.
    """

    __slots__ = ("_scheduler", "_request", "_domain", "_seq", "_released")

    def __init__(
        self, scheduler: "Scheduler", request: Request, domain: str, seq: int
    ) -> None:
        self._scheduler = scheduler
        self._request = request
        self._domain = domain
        self._seq = seq
        self._released = False

    @property
    def request(self) -> Request:
        """The request this slot was created for."""
        return self._request

    @property
    def domain(self) -> str:
        """Domain whose permit this slot holds."""
        return self._domain

    @property
    def sequence(self) -> int:
        """Monotonic scheduling sequence number."""
        return self._seq

    @property
    def released(self) -> bool:
        """Whether the resources have already been handed back."""
        return self._released

    def release(self) -> bool:
        """Return the permit and accounting entries held by this slot.

        Returns:
            bool: True if this call released the resources, False if the slot
            was already released or never belonged to the scheduler.
        """
        if self._released:
            self._scheduler._record_mismatch(
                f"slot already released: {self._domain} #{self._seq} {self._request.url}"
            )
            return False
        self._released = True
        return self._scheduler._release_slot(self)

    def __repr__(self) -> str:
        state = "released" if self._released else "held"
        return f"<ScheduleSlot {self._domain} #{self._seq} {self._request.url} {state}>"


@dataclass(frozen=True)
class ScheduleResult:
    """Outcome of :meth:`Scheduler.schedule_request`.

    ``slot`` is present only when ``reason`` is ``SCHEDULED``. Truthiness of
    the result mirrors "the request is now queued".
    """

    reason: ScheduleReason
    slot: Optional[ScheduleSlot] = None

    def __bool__(self) -> bool:
        return self.reason is ScheduleReason.SCHEDULED


class Scheduler:
    """Request scheduler with explicit resource ownership."""

    def __init__(
        self,
        *,
        max_requests: int = 0,
        request_delay: float = 0,
        domain_delay: float = 0,
        max_domain_concurrent: int = 10,
        log_level: int = logging.INFO,
        max_queue_size: int = 10000,
        poll_interval: float = 0.05,
    ):
        """Initialize the scheduler.

        Args:
            max_requests: Maximum number of requests to process (0 for unlimited)
            request_delay: Global delay between requests in seconds
            domain_delay: Delay between requests to the same domain
            max_domain_concurrent: Maximum concurrent requests per domain
            log_level: Logging level
            max_queue_size: Maximum size of request queue to prevent memory issues
            poll_interval: Pause applied by :meth:`get_request` when the queue is
                empty, so the main loop can poll instead of blocking forever
        """
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)

        # Request queue and filter. Queue entries are
        # ``(priority, sequence, request, slot)``: priority wins first, and the
        # monotonic sequence keeps the ordering total so scheduling never relies
        # on ``Request.__lt__``.
        self.request_queue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self.seen_urls: Set[str] = set()
        # URLs being scheduled right now. Written without awaiting so two
        # concurrent attempts for the same URL cannot both slip past the
        # duplicate check while the first one waits out its domain delay.
        self._reserved_urls: Set[str] = set()

        # Resource limits
        self.max_requests = max_requests
        self.request_delay = request_delay
        self.domain_delay = domain_delay
        self.max_domain_concurrent = max_domain_concurrent
        self.max_queue_size = max_queue_size
        self.poll_interval = poll_interval

        # Domain-level scheduling state
        self.domain_stats: Dict[str, dict] = {}
        self.domain_semaphores: Dict[str, asyncio.Semaphore] = {}

        # Slots handed out and not yet released, keyed by sequence number.
        self._outstanding: Dict[int, ScheduleSlot] = {}
        self._sequence = 0

        # Statistics
        self.stats = {
            "total_scheduled": 0,
            "total_filtered": 0,
            "active_domains": 0,
            "current_requests": 0,
            "queue_full_count": 0,
            "slot_mismatches": 0,
            "discarded_on_close": 0,
            "domain_delays": {},  # Track delays by domain
            "domain_concurrency": {},  # Track concurrent requests by domain
        }

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        domain = urlparse(url).netloc
        # Remove port number if present
        return domain.split(":")[0]

    def _ensure_domain_state(self, domain: str) -> None:
        """Create domain bookkeeping. Must run without awaiting."""
        if domain in self.domain_stats:
            return
        self.domain_stats[domain] = {
            "last_request": 0,
            "total_requests": 0,
            "active_requests": 0,
            "pending_permits": 0,
            "avg_delay": self.domain_delay,  # Initialize with default delay
            "min_delay": self.domain_delay,
            "max_delay": self.domain_delay,
        }
        self.domain_semaphores[domain] = asyncio.Semaphore(self.max_domain_concurrent)
        self.stats["active_domains"] += 1

    def _record_mismatch(self, message: str) -> None:
        """Record an unpaired slot release without breaking the crawl."""
        self.stats["slot_mismatches"] += 1
        self.logger.warning(f"Slot accounting mismatch: {message}")

    def _release_slot(self, slot: ScheduleSlot) -> bool:
        """Give back the permit and counters owned by ``slot``."""
        if self._outstanding.pop(slot.sequence, None) is None:
            self._record_mismatch(
                f"slot was never scheduled by this scheduler: {slot.domain} #{slot.sequence}"
            )
            return False

        domain_stats = self.domain_stats.get(slot.domain)
        if domain_stats is None:
            self._record_mismatch(f"unknown domain in slot: {slot.domain}")
            return False

        if domain_stats["active_requests"] <= 0:
            # Something else already gave this permit back. Releasing again
            # would push the domain above its concurrency ceiling, so refuse.
            self._record_mismatch(
                f"active_requests already zero for domain: {slot.domain}"
            )
            return False

        domain_stats["active_requests"] -= 1
        self.stats["current_requests"] = max(0, self.stats["current_requests"] - 1)
        self.stats["domain_concurrency"][slot.domain] = domain_stats["active_requests"]
        self.domain_semaphores[slot.domain].release()
        self.logger.debug(f"Slot released: {slot}")
        return True

    def _compute_domain_delay(self, domain: str, domain_stats: dict) -> float:
        """Update adaptive delay statistics and return the delay to apply.

        Purely synchronous: the enqueue path must not await between acquiring
        the permit and writing the accounting entries.
        """
        if domain_stats["total_requests"] > 0:
            # Use exponential moving average for delay
            alpha = 0.2  # Smoothing factor
            current_delay = time.time() - domain_stats["last_request"]
            domain_stats["avg_delay"] = (
                alpha * current_delay + (1 - alpha) * domain_stats["avg_delay"]
            )
            domain_stats["min_delay"] = min(domain_stats["min_delay"], current_delay)
            domain_stats["max_delay"] = max(domain_stats["max_delay"], current_delay)

            # Update global statistics
            self.stats["domain_delays"][domain] = {
                "avg": domain_stats["avg_delay"],
                "min": domain_stats["min_delay"],
                "max": domain_stats["max_delay"],
            }

        # Apply domain delay with jitter
        return max(
            self.domain_delay,
            domain_stats["avg_delay"] * (0.8 + 0.4 * random.random()),
        )

    async def schedule_request(self, request: Request) -> ScheduleResult:
        """Queue a request and hand back the slot that owns its resources.

        The URL is claimed before the first suspension point, so concurrent
        attempts for the same URL cannot both pass the duplicate filter. The
        permit is acquired first and the accounting entries are written in the
        same synchronous stretch as the enqueue, so a cancellation or a full
        queue can only ever give the permit back: counters and ``seen_urls`` are
        never left describing a request that is not queued.

        Args:
            request: Request object to schedule

        Returns:
            ScheduleResult: ``SCHEDULED`` with a slot, or a reason without one

        Raises:
            Exception: Unexpected errors propagate to the caller; expected
                rejections are reported through ``ScheduleResult.reason``
        """
        if self.max_requests and self.stats["total_scheduled"] >= self.max_requests:
            self.logger.warning("Maximum request limit reached")
            return ScheduleResult(ScheduleReason.LIMIT_REACHED)

        reserved = not request.dont_filter
        if reserved and (
            request.url in self.seen_urls or request.url in self._reserved_urls
        ):
            self.stats["total_filtered"] += 1
            return ScheduleResult(ScheduleReason.DUPLICATED)

        domain = self._get_domain(request.url)
        self._ensure_domain_state(domain)
        domain_stats = self.domain_stats[domain]

        # Claim the URL synchronously, before any suspension point, so a
        # concurrent attempt for the same URL is rejected as a duplicate.
        if reserved:
            self._reserved_urls.add(request.url)

        permit_held = False
        handed_off = False
        try:
            # The only backpressure point. Nothing is counted yet, so a
            # cancellation here leaves no trace beyond the permit below.
            await self.domain_semaphores[domain].acquire()
            permit_held = True
            domain_stats["pending_permits"] += 1
            self.logger.debug(f"{request}Acquired semaphore for domain: {domain}")

            effective_delay = self._compute_domain_delay(domain, domain_stats)
            if effective_delay > 0:
                await asyncio.sleep(effective_delay)

            self._sequence += 1
            slot = ScheduleSlot(self, request, domain, self._sequence)
            try:
                self.request_queue.put_nowait(
                    (request.priority, slot.sequence, request, slot)
                )
            except asyncio.QueueFull:
                self.stats["queue_full_count"] += 1
                self.logger.warning(
                    f"Queue full ({self.max_queue_size}), request not scheduled: "
                    f"{request.url}"
                )
                return ScheduleResult(ScheduleReason.QUEUE_FULL)

            # Enqueued: from here on the writes are plain assignments.
            self._outstanding[slot.sequence] = slot
            handed_off = True
            domain_stats["pending_permits"] -= 1
            self.seen_urls.add(request.url)
            self.stats["total_scheduled"] += 1
            domain_stats["total_requests"] += 1
            domain_stats["active_requests"] += 1
            domain_stats["last_request"] = time.time()
            self.stats["current_requests"] += 1
            self.stats["domain_concurrency"][domain] = domain_stats["active_requests"]
            return ScheduleResult(ScheduleReason.SCHEDULED, slot)

        except asyncio.CancelledError:
            # Cancellation is control flow, not a fault: propagate untouched.
            raise
        except Exception as exc:
            # Expected rejections use ScheduleReason; anything landing here is
            # a real error and must be visible before it bubbles up.
            self.logger.error(f"Error scheduling request {request.url}: {exc}")
            raise
        finally:
            if reserved:
                self._reserved_urls.discard(request.url)
            if permit_held and not handed_off:
                # Covers cancellation, queue-full and unexpected errors.
                self._rollback_permit(domain, domain_stats)

    def _rollback_permit(self, domain: str, domain_stats: dict) -> None:
        """Give back a permit that never became a queued slot."""
        if domain_stats["pending_permits"] > 0:
            domain_stats["pending_permits"] -= 1
            self.domain_semaphores[domain].release()

    async def get_request(self) -> Optional[ScheduleSlot]:
        """Take the next queued request, without blocking forever.

        Returns:
            Optional[ScheduleSlot]: The slot owning the next request, or None
            when the queue is empty. The caller now owns the slot and must
            release it, either through ``process_request`` or directly.
        """
        try:
            _priority, _seq, _request, slot = self.request_queue.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(self.poll_interval)
            return None

        if self.request_delay > 0:
            try:
                await asyncio.sleep(self.request_delay)
            except BaseException:
                # The slot was already taken out of the queue; hand it back
                # instead of stranding it.
                slot.release()
                raise

        return slot

    def discard_pending(self) -> int:
        """Release every slot still waiting in the queue.

        Used on shutdown so queued requests cannot leak domain permits.

        Returns:
            int: Number of requests dropped and released
        """
        discarded = 0
        while True:
            try:
                _priority, _seq, request, slot = self.request_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            slot.release()
            discarded += 1
            self.logger.debug(f"Dropped queued request on close: {request.url}")

        if discarded:
            self.stats["discarded_on_close"] += discarded
            self.logger.info(f"Discarded {discarded} queued requests on close")
        return discarded

    def request_completed(self, request: Request) -> None:
        """Release scheduling resources for ``request`` by URL lookup.

        .. deprecated::
            URL lookup cannot tell two in-flight requests for the same URL
            apart, and it silently tolerates unpaired calls. Use the
            :class:`ScheduleSlot` returned by :meth:`schedule_request`; this
            entry point is kept for one release cycle and counts every call as
            a slot mismatch when the bookkeeping does not line up.
        """
        domain = self._get_domain(request.url)
        domain_stats = self.domain_stats.get(domain)
        if domain_stats is None or domain_stats["active_requests"] <= 0:
            self._record_mismatch(
                f"request_completed for unscheduled request: {request.url}"
            )
            return

        # Consume the registered slot so a later slot.release() cannot decrement
        # the same accounting twice.
        for seq, slot in list(self._outstanding.items()):
            if slot.domain == domain and slot.request.url == request.url:
                slot._released = True
                del self._outstanding[seq]
                break

        domain_stats["active_requests"] -= 1
        self.stats["current_requests"] = max(0, self.stats["current_requests"] - 1)
        self.stats["domain_concurrency"][domain] = domain_stats["active_requests"]
        self.domain_semaphores[domain].release()
        self.logger.info(f"Request completed: {request.url}")

    def audit(self) -> Dict[str, Any]:
        """Report whether scheduling resources are accounted for consistently.

        Per domain ``active_requests + pending_permits`` must equal the number
        of permits taken from the semaphore, which gives an exact identity even
        while a request is waiting out its domain delay.
        """
        domains: Dict[str, Dict[str, Any]] = {}
        balanced = True
        for domain, domain_stats in self.domain_stats.items():
            available = self.domain_semaphores[domain]._value
            held = self.max_domain_concurrent - available
            active = domain_stats["active_requests"]
            pending = domain_stats["pending_permits"]
            domain_balanced = active + pending == held
            balanced = balanced and domain_balanced
            domains[domain] = {
                "active": active,
                "pending": pending,
                "available": available,
                "max": self.max_domain_concurrent,
                "mismatch": not domain_balanced,
            }

        current_matches = self.stats["current_requests"] == sum(
            stats["active_requests"] for stats in self.domain_stats.values()
        )
        outstanding_matches = len(self._outstanding) == sum(
            stats["active_requests"] for stats in self.domain_stats.values()
        )
        balanced = balanced and current_matches and outstanding_matches

        return {
            "domains": domains,
            "current_requests": self.stats["current_requests"],
            "outstanding": len(self._outstanding),
            "reserved": len(self._reserved_urls),
            "mismatch_count": self.stats["slot_mismatches"],
            "balanced": balanced,
            "idle": balanced
            and self.stats["current_requests"] == 0
            and not self._outstanding
            and all(state["pending"] == 0 for state in domains.values()),
        }

    def get_stats(self) -> dict:
        """Get scheduler statistics.

        Returns:
            Dictionary with scheduler statistics
        """
        return {
            **self.stats,
            "queue_size": self.request_queue.qsize(),
            "domain_stats": self.domain_stats,
        }

    def __repr__(self) -> str:
        return (
            f"<Scheduler queued={self.request_queue.qsize()} "
            f"active={self.stats['current_requests']} "
            f"domains={self.stats['active_domains']}>"
        )
