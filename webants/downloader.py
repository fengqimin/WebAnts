"""Downloader Module
This module provides downloading functionality with the following features:
- Asynchronous HTTP/HTTPS requests handling
- Local file system access
- Priority queue based request scheduling
- Concurrent download management
- Smart retry mechanism with exponential backoff
"""

import asyncio
import logging
import random
import time
from abc import abstractmethod
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional, Union

import httpx

from webants.libs.exceptions import RequestError
from webants.libs.request import Request
from webants.utils.logger import get_logger


class BaseDownloader:
    """Base class for all downloaders.

    Provides the basic interface and common functionality for downloading resources.
    All concrete downloaders should inherit from this class.
    """

    download_count: int = 0

    def __init__(
        self,
        request_queue: Optional[asyncio.PriorityQueue] = None,
        response_queue: Optional[asyncio.Queue] = None,
    ) -> None:
        """Initialize the base downloader.

        Args:
            request_queue: Queue for incoming requests
            response_queue: Queue for outgoing responses
        """
        if request_queue is not None:
            assert isinstance(request_queue, asyncio.PriorityQueue), (
                f"request_queue must be asyncio.PriorityQueue, not {type(request_queue)}"
            )
        if response_queue is not None:
            assert isinstance(response_queue, asyncio.Queue), (
                f"response_queue must be asyncio.Queue, not {type(response_queue)}"
            )
        self.request_queue = request_queue or asyncio.PriorityQueue()
        self.response_queue = response_queue or asyncio.Queue()

    async def _next_request(self) -> Request:
        """Get the next request from the queue.

        Returns:
            The next Request object
        """
        item = await self.request_queue.get()

        return item[1]

    @abstractmethod
    async def fetch(self, *args, **kwargs) -> Union[httpx.Response, Request, None]:
        """Abstract method to fetch resources."""
        pass

    @abstractmethod
    async def start_worker(self, *args, **kwargs) -> None:
        """Abstract method to start worker process."""
        pass

    @abstractmethod
    async def start_downloader(self, *args, **kwargs) -> None:
        """Abstract method to start downloader."""
        pass

    @abstractmethod
    async def close(self):
        """Abstract method to clean up resources."""
        pass


class Downloader(BaseDownloader):
    """Concrete implementation of downloader using httpx library."""

    # Status codes retried by default. 403/404/420 are deliberately excluded:
    # they are deterministic client/response states, not transient failures.
    DEFAULT_RETRY_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})

    def __init__(
        self,
        request_queue: Optional[asyncio.PriorityQueue] = None,
        response_queue: Optional[asyncio.Queue] = None,
        *,
        log_level: int = logging.INFO,
        concurrency: int = 10,
        loop: asyncio.AbstractEventLoop | None = None,
        **kwargs,
    ):
        """Initialize the downloader.

        Args:
            request_queue: Priority queue for requests
            response_queue: Queue for responses
            log_level: Logging level
            concurrency: Maximum number of concurrent downloads
            loop: Event loop to use
            **kwargs: Additional configuration parameters:
                - delay: Default delay between requests (seconds)
                - timeout: Request timeout dict or float (seconds)
                - retry_delay: Base delay for retry mechanism (seconds)
                - headers: Custom HTTP headers
                - cookies: Custom cookies
                - encoding: Default response encoding
                - limits: httpx.Limits configuration
                - proxies: Proxy configuration
                - follow_redirects: Whether to follow redirects
                - http2: Whether to enable HTTP/2
        """
        super().__init__(request_queue=request_queue, response_queue=response_queue)
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)

        # Statistics
        self.stats: dict[str, int | float] = {
            "total_requests": 0,
            "total_attempts": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "retry_requests": 0,
            "total_retries": 0,
            "total_time": 0.0,
            "min_response_time": float("inf"),
            "max_response_time": 0.0,
            "avg_response_time": 0.0,
        }

        # Configuration
        self.delay = kwargs.get("delay", 0)
        self.timeout = self._setup_timeout(kwargs.get("timeout", 30))
        self.default_encoding = kwargs.get("encoding", "utf-8")
        self.follow_redirects = kwargs.get("follow_redirects", True)

        # Concurrency control
        self.concurrency = concurrency
        self.sem = asyncio.Semaphore(concurrency)
        self.kwargs = kwargs

        # client setup
        self.headers = self._setup_headers(kwargs.get("headers", {}))
        self.cookies = kwargs.get("cookies", {})
        self.limits = self._setup_limits(kwargs.get("limits", {}))
        self.proxies = kwargs.get("proxies")
        self.http2 = kwargs.get("http2", False)

        self.client = self._setup_http_client()

        # retry mechanism
        self.retry_delay = kwargs.get("retry_delay", 1.0)
        self.retry_backoff_factor = kwargs.get("retry_backoff_factor", 2.0)
        self.retry_max_delay = kwargs.get("retry_max_delay", 60.0)
        self.retry_jitter = kwargs.get("retry_jitter", 0.25)
        retry_status_codes = kwargs.get("retry_status_codes")
        self.retry_status_codes = (
            frozenset(retry_status_codes)
            if retry_status_codes is not None
            else self.DEFAULT_RETRY_STATUS_CODES
        )

    def _setup_timeout(self, timeout: Union[float, dict]) -> httpx.Timeout:
        """Setup request timeout configuration."""
        if isinstance(timeout, (int, float)):
            return httpx.Timeout(timeout)
        return httpx.Timeout(**timeout)

    def _setup_limits(self, limits: dict) -> httpx.Limits:
        """Setup connection pool limits."""
        defaults = {
            "max_keepalive_connections": self.concurrency,
            "max_connections": self.concurrency * 2,
            "keepalive_expiry": 60.0,
        }
        return httpx.Limits(**{**defaults, **limits})

    def _setup_headers(self, headers: dict) -> dict:
        """Setup default headers with user agent if not provided."""
        headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/116.0",
        )
        return headers

    def _setup_http_client(self) -> httpx.AsyncClient:
        """Create and configure httpx client."""
        return httpx.AsyncClient(
            headers=self.headers,
            cookies=self.cookies,
            timeout=self.timeout,
            limits=self.limits,
            proxy=self.proxies,
            follow_redirects=self.follow_redirects,
            http2=self.http2,
        )

    async def _fetch(self, request: Request) -> httpx.Response:
        """Perform a single HTTP attempt.

        This method never calls ``request.callback`` and never swallows
        transport errors: retry decisions belong to :meth:`fetch`.

        Args:
            request: Request object containing URL and parameters

        Returns:
            The raw ``httpx.Response`` for this attempt.

        Raises:
            httpx.HTTPError: On transport-level failures.
        """
        start_time = time.perf_counter()
        headers = request.headers or self.headers

        request_kwargs: dict = {
            "method": request.method,
            "url": request.url,
            "headers": headers,
            "timeout": request.timeout or self.timeout,
        }
        if request.cookies:
            # httpx 0.28 deprecates per-request cookies=. Merge the client jar
            # with request-level cookies and send an explicit Cookie header so
            # cookies are actually transmitted without mutating client state.
            merged_cookies: dict[str, str] = {
                cookie.name: cookie.value for cookie in self.client.cookies.jar
            }
            merged_cookies.update(
                {str(name): str(value) for name, value in request.cookies.items()}
            )
            request_kwargs["headers"] = {
                **headers,
                "Cookie": "; ".join(
                    f"{name}={value}" for name, value in merged_cookies.items()
                ),
            }
        if request.body is not None:
            if isinstance(request.body, dict):
                request_kwargs["json"] = request.body
            else:
                request_kwargs["content"] = request.body
        if request.referer is not None:
            request_kwargs["headers"] = {
                **request_kwargs["headers"],
                "Referer": getattr(request.referer, "url", str(request.referer)),
            }

        response = await self.client.request(**request_kwargs)

        elapsed = time.perf_counter() - start_time
        self._update_stats(elapsed)
        self.logger.info(
            f"Request completed - {request}, "
            f"Status: {response.status_code}, "
            f"Time: {elapsed:.3f}s"
        )
        return response

    def _update_stats(self, elapsed: float) -> None:
        """Update response time statistics."""
        self.stats["total_time"] += elapsed
        self.stats["min_response_time"] = min(self.stats["min_response_time"], elapsed)
        self.stats["max_response_time"] = max(self.stats["max_response_time"], elapsed)
        total_attempts = self.stats["total_attempts"]
        if total_attempts > 0:
            self.stats["avg_response_time"] = self.stats["total_time"] / total_attempts

    async def _sleep(self, seconds: float) -> None:
        """Sleep between attempts.

        Kept as a separate method so tests can inject a no-op or fake clock
        without waiting for real backoff delays.
        """
        if seconds > 0:
            await asyncio.sleep(seconds)

    def _compute_backoff(self, retry_index: int) -> float:
        """Compute exponential backoff with optional jitter.

        Args:
            retry_index: Zero-based index of the retry (0 = first retry).
        """
        delay = self.retry_delay * (self.retry_backoff_factor**retry_index)
        delay = min(delay, self.retry_max_delay)
        if self.retry_jitter > 0:
            delay *= 1 + self.retry_jitter * random.random()
        return delay

    @staticmethod
    def _parse_retry_after(response: httpx.Response) -> Optional[float]:
        """Parse ``Retry-After`` as seconds or an HTTP date.

        Returns:
            Non-negative delay in seconds, or ``None`` if absent/invalid.
        """
        value = response.headers.get("Retry-After")
        if not value:
            return None

        value = value.strip()
        try:
            seconds = float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
            except (TypeError, ValueError, OverflowError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()

        return max(0.0, seconds)

    def _record_final_result(self, *, success: bool, retried: bool) -> None:
        """Record one logical request's final outcome exactly once."""
        if retried:
            self.stats["retry_requests"] += 1
        if success:
            self.stats["successful_requests"] += 1
        else:
            self.stats["failed_requests"] += 1

    async def fetch(self, request: Request) -> httpx.Response:
        """Fetch a resource, retrying transient failures.

        Retry ownership: this method retries transport errors and configured
        retryable status codes internally. It never invokes ``request.callback``;
        Spider is responsible for calling callbacks exactly once per logical
        request.

        Args:
            request: Request object containing URL and other parameters

        Returns:
            The final non-retryable ``httpx.Response``.

        Raises:
            RequestError: If transport errors or retryable status codes are
                exhausted.
        """
        max_attempts = max(1, request.retries + 1)
        self.stats["total_requests"] += 1

        pre_delay = request.delay or self.delay
        if pre_delay > 0:
            self.logger.debug(f"Fetching {request}, delay: {pre_delay}s")
            await self._sleep(pre_delay)

        retried = False
        last_error: Optional[httpx.HTTPError] = None
        last_response: Optional[httpx.Response] = None

        for attempt in range(max_attempts):
            if attempt > 0:
                retried = True
                self.stats["total_retries"] += 1
                delay = self._compute_backoff(attempt - 1)
                if last_response is not None:
                    retry_after = self._parse_retry_after(last_response)
                    if retry_after is not None:
                        delay = max(delay, retry_after)
                self.logger.warning(
                    f"Retrying request - {request}, "
                    f"attempt {attempt + 1}/{max_attempts}, delay: {delay:.2f}s"
                )
                await self._sleep(delay)

            self.stats["total_attempts"] += 1
            try:
                response = await self._fetch(request)
            except httpx.HTTPError as exc:
                last_error = exc
                last_response = None
                if attempt + 1 >= max_attempts:
                    self._record_final_result(success=False, retried=retried)
                    raise RequestError(
                        f"Fetch failed after {attempt + 1} attempt(s): {exc}",
                        code="FETCH_FAILED",
                        retryable=False,
                        metadata={
                            "url": request.url,
                            "attempts": attempt + 1,
                            "cause": type(exc).__name__,
                        },
                    ) from exc
                continue

            if response.status_code in self.retry_status_codes:
                last_response = response
                last_error = None
                if attempt + 1 >= max_attempts:
                    self._record_final_result(success=False, retried=retried)
                    raise RequestError(
                        f"Retryable status {response.status_code} after "
                        f"{attempt + 1} attempt(s)",
                        code="RETRY_EXHAUSTED",
                        retryable=False,
                        metadata={
                            "url": request.url,
                            "status_code": response.status_code,
                            "headers": dict(response.headers),
                            "attempts": attempt + 1,
                        },
                    )
                continue

            self._record_final_result(success=True, retried=retried)
            return response

        # Defensive fallback: the loop always returns or raises.
        self._record_final_result(success=False, retried=retried)
        raise RequestError(
            f"Fetch failed: {last_error}",
            code="FETCH_FAILED",
            retryable=False,
            metadata={"url": request.url, "attempts": max_attempts},
        )

    async def fetch_retry(self, request: Request) -> httpx.Response:
        """Deprecated compatibility alias for :meth:`fetch`."""
        return await self.fetch(request)

    async def start_worker(self) -> None:
        """Process queue items forever.

        Worker mode is a compatibility adapter: only successful responses are
        queued; final failures are logged. Spider does not use this path.
        """

        while True:
            async with self.sem:
                request = await self._next_request()
                try:
                    resp = await self.fetch(request)
                except RequestError as exc:
                    self.logger.error(f"Request failed permanently: {exc}")
                    resp = None

                if self.response_queue and resp:
                    self.response_queue.put_nowait(resp)

                # Notify the queue that the "work item" has been processed.
                self.request_queue.task_done()

    async def start_downloader(self, many: int = None) -> None:
        """Run {many} workers until all tasks finished."""
        many = many or self.concurrency
        self.logger.info(f"Start {self.__class__.__name__}...")

        self.logger.debug(f"Downloader started with {many} workers.")
        try:
            __ = [asyncio.create_task(self.start_worker()) for _ in range(many)]
            await self.request_queue.join()
            for task in asyncio.all_tasks():
                if task is not asyncio.current_task():
                    task.cancel()
        except asyncio.CancelledError:
            self.logger.info(f"{self.__class__.__name__} has been cancelled.")
        except Exception as e:
            raise e

    async def close(self) -> None:
        await self.client.aclose()

        self.logger.info(f"{self.__class__.__name__} has been closed.")

    def get_stats(self) -> dict[str, int | float]:
        """Get downloader statistics."""
        return self.stats.copy()
