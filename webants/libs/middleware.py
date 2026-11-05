"""Middleware Module

This module provides request/response processing with:
- Circuit breaker pattern
- Rate limiting
- Request deduplication
- Response caching
- Request filtering
"""

import asyncio
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Type, Union
from urllib.parse import urlparse

from webants.libs.exceptions import CircuitBreakerError, RateLimitError, exception_tracker
from webants.libs.request import Request
from webants.libs.signals import signal_manager
from webants.utils.logger import get_logger


class Middleware(ABC):
    """Base middleware class."""

    def __init__(self):
        """Initialize middleware."""
        self.logger = get_logger(self.__class__.__name__)

    async def lookup(self, request: Request) -> Optional[Any]:
        """Return a cached response for ``request``, or None on a miss.

        Cache-like middleware implements this phase instead of returning a
        response from :meth:`process_request`: the request chain only accepts
        ``Request`` values.
        """
        return None

    @abstractmethod
    async def process_request(self, request: Request) -> Optional[Request]:
        """Process request before sending.

        Args:
            request: Request to process

        Returns:
            Processed request or None to drop
        """
        return request

    @abstractmethod
    async def process_response(self, response: Any, request: Request) -> Optional[Any]:
        """Process response after receiving.

        Args:
            response: Response to process
            request: Original request

        Returns:
            Processed response or None to drop
        """
        return response

    @abstractmethod
    async def process_exception(
        self, exception: Exception, request: Request
    ) -> Optional[Union[Request, Any]]:
        """Process request exception.

        Args:
            exception: Exception to handle
            request: Failed request

        Returns:
            New request to retry, response to return,
            or None to propagate exception
        """
        return None


class CircuitBreaker(Middleware):
    """Circuit breaker for failing domains."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        """Initialize circuit breaker.

        Args:
            failure_threshold: Failures before opening
            recovery_timeout: Seconds before recovery
        """
        super().__init__()
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        # Track domain failures
        self._failures: Dict[str, int] = defaultdict(int)
        self._last_failure: Dict[str, float] = {}

    async def process_request(self, request: Request) -> Optional[Request]:
        """Check circuit state before request.

        Args:
            request: Request to check

        Returns:
            Request if circuit closed, None if open

        Raises:
            CircuitBreakerError: If circuit is open
        """
        domain = urlparse(request.url).netloc

        # Check if circuit is open
        if domain in self._failures:
            failures = self._failures[domain]
            if failures >= self.failure_threshold:
                # Check recovery timeout
                last_failure = self._last_failure[domain]
                if time.time() - last_failure < self.recovery_timeout:
                    raise CircuitBreakerError(domain=domain, failures=failures)
                # Reset after timeout
                self._failures[domain] = 0

        return request

    async def process_response(self, response: Any, request: Request) -> Optional[Any]:
        """Reset failure count on success.

        Args:
            response: Response to process
            request: Original request

        Returns:
            Unmodified response
        """
        domain = urlparse(request.url).netloc
        if domain in self._failures:
            self._failures[domain] = 0
        return response

    async def process_exception(
        self, exception: Exception, request: Request
    ) -> Optional[Union[Request, Any]]:
        """Track domain failures.

        Args:
            exception: Exception to handle
            request: Failed request

        Returns:
            None to propagate exception
        """
        domain = urlparse(request.url).netloc
        self._failures[domain] += 1
        self._last_failure[domain] = time.time()
        return None


class RateLimiter(Middleware):
    """Rate limiting by domain."""

    def __init__(
        self,
        requests_per_second: Optional[float] = None,
        requests_per_minute: Optional[int] = None,
        domain_delay: float = 0.0,
    ):
        """Initialize rate limiter.

        Args:
            requests_per_second: Max requests per second
            requests_per_minute: Max requests per minute
            domain_delay: Minimum delay between requests
        """
        super().__init__()
        self.requests_per_second = requests_per_second
        self.requests_per_minute = requests_per_minute
        self.domain_delay = domain_delay

        # Track request timing
        self._last_request: Dict[str, float] = {}
        self._request_times: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=60)  # Store last minute
        )

    async def process_request(self, request: Request) -> Optional[Request]:
        """Apply rate limiting before request.

        Args:
            request: Request to check

        Returns:
            Request after rate limiting

        Raises:
            RateLimitError: If rate limit exceeded
        """
        domain = urlparse(request.url).netloc
        now = time.time()

        # Check domain delay
        if self.domain_delay > 0 and domain in self._last_request:
            delay = now - self._last_request[domain]
            if delay < self.domain_delay:
                await asyncio.sleep(self.domain_delay - delay)

        # Check rate limits
        if domain in self._request_times:
            times = self._request_times[domain]

            # Clean old requests
            while times and times[0] < now - 60:
                times.popleft()

            # Check requests per minute
            if self.requests_per_minute and len(times) >= self.requests_per_minute:
                raise RateLimitError(
                    domain=domain, limit=self.requests_per_minute, reset_after=60 - (now - times[0])
                )

            # Check requests per second
            if self.requests_per_second:
                recent = sum(1 for t in times if t > now - 1)
                if recent >= self.requests_per_second:
                    raise RateLimitError(
                        domain=domain, limit=self.requests_per_second, reset_after=1.0
                    )

        return request

    async def process_response(self, response: Any, request: Request) -> Optional[Any]:
        """Track successful request timing.

        Args:
            response: Response to process
            request: Original request

        Returns:
            Unmodified response
        """
        domain = urlparse(request.url).netloc
        now = time.time()
        self._last_request[domain] = now
        self._request_times[domain].append(now)
        return response

    async def process_exception(
        self, exception: Exception, request: Request
    ) -> Optional[Union[Request, Any]]:
        """Handle rate limit errors.

        Args:
            exception: Exception to handle
            request: Failed request

        Returns:
            None to propagate exception
        """
        if isinstance(exception, RateLimitError):
            # Could implement automatic retry after delay
            pass
        return None


class RequestDeduplicator(Middleware):
    """Filter duplicate requests."""

    def __init__(self, max_size: int = 10000):
        """Initialize deduplicator.

        Args:
            max_size: Maximum number of URLs to track
        """
        super().__init__()
        self.max_size = max_size
        self._seen_urls: Set[str] = set()

    async def process_request(self, request: Request) -> Optional[Request]:
        """Check for duplicate requests.

        Args:
            request: Request to check

        Returns:
            Request if new, None if duplicate
        """
        # Skip if at capacity
        if len(self._seen_urls) >= self.max_size:
            return request

        url = request.url
        if url in self._seen_urls:
            await signal_manager.send("request_dropped", self, request=request, reason="duplicate")
            return None

        self._seen_urls.add(url)
        return request

    async def process_response(self, response: Any, request: Request) -> Optional[Any]:
        """Pass through response unmodified."""
        return response

    async def process_exception(
        self, exception: Exception, request: Request
    ) -> Optional[Union[Request, Any]]:
        """Pass through exception unmodified."""
        return None


class ResponseCache(Middleware):
    """Cache responses by URL.

    Cache hits are exposed through :meth:`lookup`, not ``process_request``:
    a hit returns a response, which is not a valid request-chain value.
    """

    def __init__(self, max_size: int = 1000, ttl: Optional[float] = None):
        """Initialize response cache.

        Args:
            max_size: Maximum number of responses (0 disables caching)
            ttl: Cache TTL in seconds. TTL is sliding: a hit refreshes the
                access time.
        """
        super().__init__()
        self.max_size = max_size
        self.ttl = ttl

        # url -> (response, stored_at, last_access)
        self._cache: Dict[str, tuple[Any, float, float]] = {}
        self.stats = {
            "hits": 0,
            "misses": 0,
            "expired": 0,
            "evictions": 0,
        }

    async def lookup(self, request: Request) -> Optional[Any]:
        """Return a cached response for ``request``, or None on a miss.

        A hit refreshes ``last_access`` so the TTL slides forward.
        """
        url = request.url
        entry = self._cache.get(url)
        if entry is None:
            self.stats["misses"] += 1
            return None

        response, stored_at, last_access = entry
        now = time.time()
        if self.ttl is not None and now - last_access > self.ttl:
            del self._cache[url]
            self.stats["expired"] += 1
            self.stats["misses"] += 1
            return None

        self._cache[url] = (response, stored_at, now)
        self.stats["hits"] += 1
        return response

    async def process_request(self, request: Request) -> Optional[Request]:
        """Pass the request through; hits are handled by :meth:`lookup`."""
        return request

    async def process_response(self, response: Any, request: Request) -> Optional[Any]:
        """Cache a fresh response, or touch an existing cached one.

        Args:
            response: Response to cache
            request: Original request

        Returns:
            Unmodified response
        """
        if self.max_size <= 0:
            return response

        url = request.url
        now = time.time()
        entry = self._cache.get(url)
        if entry is not None and entry[0] is response:
            # The cached object is flowing through the response chain again;
            # refreshing its timestamp is enough (no duplicate write).
            self._cache[url] = (entry[0], entry[1], now)
            return response

        if len(self._cache) >= self.max_size and url not in self._cache:
            lru_url = min(self._cache, key=lambda key: self._cache[key][2])
            del self._cache[lru_url]
            self.stats["evictions"] += 1

        self._cache[url] = (response, now, now)
        return response

    async def process_exception(
        self, exception: Exception, request: Request
    ) -> Optional[Union[Request, Any]]:
        """Pass through exception unmodified."""
        return None


class MiddlewareManager:
    """Manages middleware execution chain."""

    def __init__(self):
        """Initialize middleware manager."""
        self.logger = get_logger(self.__class__.__name__)
        self._middleware: List[Middleware] = []

    def add_middleware(
        self, middleware: Union[Middleware, Type[Middleware]], **kwargs: Any
    ) -> None:
        """Add middleware to chain.

        Args:
            middleware: Middleware instance or class
            **kwargs: Constructor arguments
        """
        if isinstance(middleware, type):
            middleware = middleware(**kwargs)
        self._middleware.append(middleware)

    async def lookup(self, request: Request) -> Optional[Any]:
        """Return the first cached response produced by the chain.

        Middlewares are queried in registration order; the first non-None
        result wins and later caches are not consulted.

        Args:
            request: Request to look up

        Returns:
            Cached response, or None when no middleware has one
        """
        for middleware in self._middleware:
            response = await middleware.lookup(request)
            if response is not None:
                return response
        return None

    def get_stats(self) -> Dict[str, dict]:
        """Return the statistics dictionaries exposed by middlewares."""
        stats: Dict[str, dict] = {}
        for middleware in self._middleware:
            middleware_stats = getattr(middleware, "stats", None)
            if isinstance(middleware_stats, dict):
                stats[middleware.__class__.__name__] = dict(middleware_stats)
        return stats

    async def process_request(self, request: Request) -> Optional[Request]:
        """Process request through middleware chain.

        Args:
            request: Request to process

        Returns:
            Processed request or None if dropped
        """
        current_request = request
        for middleware in self._middleware:
            try:
                result = await middleware.process_request(current_request)
                if result is None:
                    return None
                if not isinstance(result, Request):
                    raise TypeError(
                        f"{middleware.__class__.__name__}.process_request must "
                        f"return Request or None, got {type(result).__name__}"
                    )
                current_request = result

            except Exception as e:
                self.logger.error(
                    f"Error in {middleware.__class__.__name__}.process_request: {str(e)}"
                )
                exception_tracker.track(e)
                raise

        return current_request

    async def process_response(self, response: Any, request: Request) -> Optional[Any]:
        """Process response through middleware chain.

        Args:
            response: Response to process
            request: Original request

        Returns:
            Processed response or None if dropped
        """
        current_response = response
        for middleware in reversed(self._middleware):
            try:
                result = await middleware.process_response(current_response, request)
                if result is None:
                    return None
                current_response = result

            except Exception as e:
                self.logger.error(
                    f"Error in {middleware.__class__.__name__}.process_response: {str(e)}"
                )
                exception_tracker.track(e)
                raise

        return current_response

    async def process_exception(
        self, exception: Exception, request: Request
    ) -> Optional[Union[Request, Any]]:
        """Process exception through middleware chain.

        Args:
            exception: Exception to handle
            request: Failed request

        Returns:
            New request, response, or None
        """
        for middleware in reversed(self._middleware):
            try:
                result = await middleware.process_exception(exception, request)
                if result is not None:
                    return result

            except Exception as e:
                self.logger.error(
                    f"Error in {middleware.__class__.__name__}.process_exception: {str(e)}"
                )
                exception_tracker.track(e)

        return None
