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
        max_domain_concurrent: int = 8,
        log_level: int = logging.INFO
    ):
        """Initialize the scheduler.
        
        Args:
            max_requests: Maximum number of requests to process (0 for unlimited)
            request_delay: Global delay between requests in seconds
            domain_delay: Delay between requests to the same domain
            max_domain_concurrent: Maximum concurrent requests per domain
            log_level: Logging level
        """
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)
        
        # Request queue and filter
        self.request_queue = asyncio.PriorityQueue()
        self.seen_urls: Set[str] = set()
        
        # Resource limits
        self.max_requests = max_requests
        self.request_delay = request_delay
        self.domain_delay = domain_delay
        self.max_domain_concurrent = max_domain_concurrent
        
        # Domain-level scheduling state
        self.domain_stats: Dict[str, dict] = {}
        self.domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        
        # Statistics
        self.stats = {
            "total_scheduled": 0,
            "total_filtered": 0,
            "active_domains": 0,
            "current_requests": 0
        }

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        return urlparse(url).netloc

    async def schedule_request(self, request: Request) -> bool:
        """Schedule a new request with priority.
        
        Args:
            request: Request object to schedule
            
        Returns:
            bool: True if request was scheduled, False if filtered
        """
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
                "active_requests": 0
            }
            self.domain_semaphores[domain] = asyncio.Semaphore(self.max_domain_concurrent)
            self.stats["active_domains"] += 1
            
        # Check domain concurrency limit
        if not self.domain_semaphores[domain].locked():
            await self.domain_semaphores[domain].acquire()
            
        # Check domain delay
        if self.domain_delay > 0:
            last_req = self.domain_stats[domain]["last_request"]
            if last_req > 0:
                wait_time = self.domain_delay - (time.time() - last_req)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    
        # Update statistics
        self.seen_urls.add(request.url)
        self.stats["total_scheduled"] += 1
        self.domain_stats[domain]["total_requests"] += 1
        self.domain_stats[domain]["active_requests"] += 1
        self.domain_stats[domain]["last_request"] = time.time()
        self.stats["current_requests"] += 1
        
        # Add to request queue
        await self.request_queue.put((request.priority, request))
        return True

    async def get_request(self) -> Optional[Request]:
        """Get next request from queue.
        
        Returns:
            Next request or None if queue is empty
        """
        if self.request_queue.empty():
            return None
            
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

    def get_stats(self) -> dict:
        """Get scheduler statistics.
        
        Returns:
            Dictionary with scheduler statistics
        """
        return {
            **self.stats,
            "queue_size": self.request_queue.qsize(),
            "domain_stats": self.domain_stats
        }
