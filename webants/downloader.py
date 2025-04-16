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
import time
from abc import abstractmethod
from typing import Optional, Union

import httpx

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

    # Define retry strategies for different error types
    RETRY_CODES = {
        403: {"max_retries": 5, "backoff_factor": 2},  # Forbidden
        404: {"max_retries": 5, "backoff_factor": 2},  # Not Found
        408: {"max_retries": 3, "backoff_factor": 2},  # Request Timeout
        420: {"max_retries": 3, "backoff_factor": 2},  # Bad Request
        429: {"max_retries": 3, "backoff_factor": 5},  # Too Many Requests
        500: {"max_retries": 3, "backoff_factor": 2},  # Server Error
        502: {"max_retries": 3, "backoff_factor": 2},  # Bad Gateway
        503: {"max_retries": 3, "backoff_factor": 2},  # Service Unavailable
        504: {"max_retries": 3, "backoff_factor": 2},  # Gateway Timeout
    }

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
        self.retry_delay = kwargs.get("retry_delay", 1)
        self.retry_requests = set()

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

    async def _fetch(self, request: Request) -> Union[httpx.Response, Request]:
        """Fetch a resource based on the request with advanced error handling.

        Args:
            request: Request object containing URL and parameters

        Returns:
            Either a successful response or the original request for retry
        """
        start_time = time.time()
        delay = request.delay or self.delay
        self.logger.debug(f"Fetching {request}, delay: {delay}s")
        await asyncio.sleep(delay)

        headers = request.headers or self.headers
        if request.url not in self.retry_requests:
            self.stats["total_requests"] += 1

        try:
            response = await self.client.request(
                method=request.method,
                url=request.url,
                headers=headers,
                timeout=request.timeout or self.timeout,
            )

            elapsed = time.time() - start_time
            self._update_stats(elapsed)

            # Log detailed request information
            self.logger.info(
                f"Request completed - {request}, "
                f"Status: {response.status_code}, "
                f"Time: {elapsed:.3f}s"
            )
            if response.status_code not in self.RETRY_CODES:
                self.stats["successful_requests"] += 1

            return response

        except httpx.HTTPError as e:
            elapsed = time.time() - start_time
            self._update_stats(elapsed)
            self.stats["failed_requests"] += 1

            self.logger.error(
                f"HTTP Error: {str(e)}, {request}, "
                f"Retries left: {request.retries - 1}, "
                f"Time: {elapsed:.2f}s"
            )
            return request

        except Exception as e:
            self.stats["failed_requests"] += 1
            self.logger.error(f"Unexpected error: {str(e)}, {request}")

            return request

    def _update_stats(self, elapsed: float) -> None:
        """Update response time statistics."""
        self.stats["total_time"] += elapsed
        self.stats["min_response_time"] = min(self.stats["min_response_time"], elapsed)
        self.stats["max_response_time"] = max(self.stats["max_response_time"], elapsed)
        total_requests = (
            self.stats["successful_requests"] + self.stats["failed_requests"]
        )
        if total_requests > 0:
            self.stats["avg_response_time"] = self.stats["total_time"] / total_requests

    def _retry_request(self, request: Request) -> Request:
        """Retry a request by re-queuing it with adjusted priority."""
        self.stats["total_retries"] += 1

        request.retries -= 1
        request.priority += 10  # Lower priority for retry requests

        if request.url not in self.retry_requests:
            self.stats["retry_requests"] += 1
            self.retry_requests.add(request.url)

        self.request_queue.put_nowait((request.priority, request))

        return request

    async def fetch_retry(self, request: Request) -> Optional[httpx.Response]:
        """Fetch resources using an adaptive retry mechanism with exponential backoff strategy.

        This method implements a smart retry mechanism with the following features:
        1. Conditional retry based on HTTP status codes
        2. Exponential backoff algorithm to avoid frequent retries
        3. Dynamic request priority adjustment
        4. Detailed retry status tracking

        Args:
            request: Request object containing URL and other parameters

        Returns:
            Response object on success, None during retry, failed response when retries exhausted
        """

        result = await self._fetch(request)

        if isinstance(result, httpx.Response):
            # Handle HTTP status code triggered retries
            if result.status_code in self.RETRY_CODES:
                if request.retries > 0:
                    retry_config = self.RETRY_CODES[result.status_code]
                    # Calculate exponential backoff delay
                    backoff_delay = self.retry_delay * (
                        retry_config["backoff_factor"]
                        ** (retry_config["max_retries"] - request.retries)
                    )

                    # Update request status
                    request.delay = backoff_delay

                    request = self._retry_request(request)

                    # Log retry status
                    self.logger.warning(
                        f"Retrying request - {request}, "
                        f"Status code: {result.status_code}, "
                        f"Retries remaining: {request.retries}, "
                        f"New priority: {request.priority}, "
                        f"Delay: {backoff_delay:.2f}s"
                    )

                return None

            # Handle successful response
            if request.callback is None:
                return result
            return await request.callback(result, request.cb_kwargs)

        # Handle request exception triggered retries
        elif isinstance(result, Request):
            if request.retries > 0:
                request = self._retry_request(request)

                self.logger.warning(
                    f"Request failed, retrying : {request}, "
                    f"Retries remaining: {request.retries}, "
                    f"New priority: {request.priority}"
                )
                return None
            else:
                # Retries exhausted, return failure response
                self.logger.error(
                    f"Request finally failed: {request}, Retries exhausted"
                )
                self.stats["failed_requests"] += 1
                return httpx.Response(
                    status_code=600,  # Custom status code indicating retry failure
                    extensions={"Request": request, "retry_exhausted": True},
                )

    async def fetch(self, request: Request) -> httpx.Response:
        """Fetch a resource based on the request.

        Args:
            request: Request object containing URL and other parameters

        Returns:
            Response object
        """

        result = await self._fetch(request)
        if isinstance(result, httpx.Response):
            if request.callback is None:
                return result
            return await request.callback(result, request.cb_kwargs)

        elif isinstance(result, Request):
            return httpx.Response(status_code=600, extensions={"Request": request})

    async def start_worker(self) -> None:
        """Process queue items forever."""

        while True:
            async with self.sem:
                request = await self._next_request()
                resp = await self.fetch_retry(request)

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

        except Exception as e:
            raise e

    async def close(self) -> None:
        await self.client.aclose()

        self.logger.info(f"{self.__class__.__name__} has been closed.")
