"""Scheduler Module
This module provides scheduling functionality with the following features:
- Priority-based request scheduling
- Duplicate URL filtering
- Resource limits management
- Request rate limiting
- Domain-specific scheduling policies
"""

import asyncio
import logging
import time
import random
from typing import Optional, Set, Dict
from urllib.parse import urlparse

from webants.libs.request import Request
from webants.utils.logger import get_logger


class Scheduler:
    """Request scheduler with advanced features."""

    def __init__(
        self,
        *,
        max_requests: int = 0,
        request_delay: float = 0,
        domain_delay: float = 0,
        max_domain_concurrent: int = 10,
        log_level: int = logging.INFO,
        max_queue_size: int = 10000,
    ):
        """Initialize the scheduler.

        Args:
            max_requests: Maximum number of requests to process (0 for unlimited)
            request_delay: Global delay between requests in seconds
            domain_delay: Delay between requests to the same domain
            max_domain_concurrent: Maximum concurrent requests per domain
            log_level: Logging level
            max_queue_size: Maximum size of request queue to prevent memory issues
        """
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)

        # Request queue and filter
        self.request_queue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self.seen_urls: Set[str] = set()

        # Resource limits
        self.max_requests = max_requests
        self.request_delay = request_delay
        self.domain_delay = domain_delay
        self.max_domain_concurrent = max_domain_concurrent
        self.max_queue_size = max_queue_size

        # Domain-level scheduling state
        self.domain_stats: Dict[str, dict] = {}
        self.domain_semaphores: Dict[str, asyncio.Semaphore] = {}

        # Statistics
        self.stats = {
            "total_scheduled": 0,
            "total_filtered": 0,
            "active_domains": 0,
            "current_requests": 0,
            "queue_full_count": 0,
            "domain_delays": {},  # Track delays by domain
            "domain_concurrency": {},  # Track concurrent requests by domain
        }

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        domain = urlparse(url).netloc
        # Remove port number if present
        return domain.split(":")[0]

    async def schedule_request(self, request: Request) -> bool:
        """Schedule a new request with priority and domain-based rate limiting.

        Args:
            request: Request object to schedule

        Returns:
            bool: True if request was scheduled, False if filtered

        Raises:
            asyncio.QueueFull: If request queue is full
        """
        try:
            if self.max_requests and self.stats["total_scheduled"] >= self.max_requests:
                self.logger.warning("Maximum request limit reached")
                return False

            if request.url in self.seen_urls and not request.dont_filter:
                self.stats["total_filtered"] += 1
                return False

            domain = self._get_domain(request.url)

            # Initialize domain state
            if domain not in self.domain_stats:
                self.domain_stats[domain] = {
                    "last_request": 0,
                    "total_requests": 0,
                    "active_requests": 0,
                    "avg_delay": self.domain_delay,  # Initialize with default delay
                    "min_delay": self.domain_delay,
                    "max_delay": self.domain_delay,
                }
                self.domain_semaphores[domain] = asyncio.Semaphore(
                    self.max_domain_concurrent
                )
                self.logger.debug(f"domain: {self.domain_semaphores[domain]}")
                self.stats["active_domains"] += 1

            # Check domain concurrency limit
            await self.domain_semaphores[domain].acquire()
            self.logger.debug(f"{request}Acquired semaphore for domain: {domain}")
            self.logger.debug(f"domain: {self.domain_semaphores[domain]}")

            # Calculate adaptive domain delay based on response times
            domain_stats = self.domain_stats[domain]
            if domain_stats["total_requests"] > 0:
                # Use exponential moving average for delay
                alpha = 0.2  # Smoothing factor
                current_delay = time.time() - domain_stats["last_request"]
                domain_stats["avg_delay"] = (
                    alpha * current_delay + (1 - alpha) * domain_stats["avg_delay"]
                )
                domain_stats["min_delay"] = min(
                    domain_stats["min_delay"], current_delay
                )
                domain_stats["max_delay"] = max(
                    domain_stats["max_delay"], current_delay
                )

                # Update global statistics
                self.stats["domain_delays"][domain] = {
                    "avg": domain_stats["avg_delay"],
                    "min": domain_stats["min_delay"],
                    "max": domain_stats["max_delay"],
                }

            # Apply domain delay with jitter
            effective_delay = max(
                self.domain_delay,
                domain_stats["avg_delay"] * (0.8 + 0.4 * random.random()),
            )
            await asyncio.sleep(effective_delay)

            # Update statistics
            self.seen_urls.add(request.url)
            self.stats["total_scheduled"] += 1
            domain_stats["total_requests"] += 1
            domain_stats["active_requests"] += 1
            domain_stats["last_request"] = time.time()
            self.stats["current_requests"] += 1
            self.stats["domain_concurrency"][domain] = domain_stats["active_requests"]

            # Add to request queue

            await self.request_queue.put((request.priority, request))
            return True

        except Exception as e:
            self.logger.error(f"Error scheduling request: {str(e)}")
            return False

    async def get_request(self) -> Optional[Request]:
        """Get next request from queue.

        Returns:
            Next request or None if queue is empty
        """
        priority, request = await self.request_queue.get()

        if self.request_delay > 0:
            await asyncio.sleep(self.request_delay)

        return request

    def request_completed(self, request: Request) -> None:
        """Mark request as completed and update stats.

        Args:
            request: Completed request
        """
        domain = self._get_domain(request.url)
        self.domain_stats[domain]["active_requests"] -= 1
        self.stats["current_requests"] -= 1

        # Release domain semaphore
        if domain in self.domain_semaphores:
            self.domain_semaphores[domain].release()
        self.logger.info(f"Request completed: {request.url}")

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
