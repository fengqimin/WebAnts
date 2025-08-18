"""Spider Module
This module provides the base Spider class with the following features:
- Configurable request handling
- Middleware support
- Error recovery with circuit breaker
- Stats collection and monitoring
- Event hooks and signals
"""

import asyncio
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Union, Callable
from collections import defaultdict

from webants.downloader import Downloader
from webants.libs.request import Request
from webants.libs.result import Result
from webants.parser import Parser
from webants.scheduler import Scheduler
from webants.utils.logger import get_logger


class Signal:
    """Signal system for Spider events.
    Signals:
        - spider_started: Called when the spider starts running.
        - spider_stopped: Called when the spider stops running.
        - request_scheduled: Called when a request is scheduled.
        - request_finished: Called when a request is finished.
        - item_scraped: Called when an item is scraped.
        - response_received: Called when a response is received.
        - error: Called when an error occurs.
        - stats_collected: Called when stats are collected.
        - stats_updated: Called when stats are updated.
        - stats_reset: Called when stats are reset.
    """

    def __init__(self):
        self.handlers = defaultdict(list)

    def connect(self, signal: str, handler: Callable) -> None:
        """Connect a handler to a signal.
        Args:
            signal: Signal name
            handler: Callable to handle the signal
        Example:
            ```
            def on_response_received(sender, response):
                print(f"Received response: {response.status_code}")
            spider.signal.connect("response_received", on_response_received)
            spider.run()
            # Output: Received response: 200
            ```
        Returns:
            None
        """
        self.handlers[signal].append(handler)

    def send(self, signal: str, sender: Any, **kwargs) -> None:
        """Send a signal to all connected handlers."""
        for handler in self.handlers[signal]:
            try:
                handler(sender, **kwargs)
            except Exception as e:
                logger = get_logger("Signal")
                logger.error(f"Error in signal handler: {str(e)}")


class CircuitBreaker:
    """Circuit breaker for error handling.
    Circuit breaker states:
    - closed: No failures, requests allowed
    - open: Failures exceed threshold, requests blocked
    - half-open: Attempt to recover, requests allowed temporarily
    Example:
        ```
        circuit = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)
        while True:
            if circuit.allow_request():
                try:
                    response = requests.get(url)
                    if response.status_code != 200:
                        circuit.record_failure()
                        continue
                    circuit.record_success()
                    # Process response
                except Exception as e:
                    circuit.record_failure()
                    continue
            else:
                # Circuit is open, wait before trying again
                time.sleep(circuit.recovery_timeout)
        ```
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        """Initialize circuit breaker.
        Args:
            failure_threshold: Number of failures before circuit breaker opens
            recovery_timeout: Time before circuit breaker attempts recovery
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half-open

    def record_failure(self) -> None:
        """Record a failure and potentially open the circuit."""
        self.failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold:
            self.state = "open"

    def record_success(self) -> None:
        """Record a success and potentially close the circuit."""
        self.failures = 0
        self.state = "closed"

    def allow_request(self) -> bool:
        """Check if a request should be allowed."""
        if self.state == "closed":
            return True
        elif self.state == "open":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "half-open"
                return True
            return False
        else:  # half-open
            return True


class Spider:
    """Base Spider class with advanced features."""

    name: str = "base_spider"
    start_urls: List[str] = []
    custom_settings: Dict[str, Any] = {}

    def __init__(
        self,
        *,
        concurrent_requests: int = 10,
        request_timeout: float = 30,
        retry_times: int = 3,
        retry_delay: float = 1.0,
        log_level: int = logging.INFO,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        **kwargs,
    ):
        """Initialize spider with configuration.

        Args:
            concurrent_requests: Maximum concurrent requests
            request_timeout: Request timeout in seconds
            retry_times: Number of times to retry failed requests
            retry_delay: Delay between retries in seconds
            log_level: Logging level
            failure_threshold: Number of failures before circuit breaker opens
            recovery_timeout: Time before circuit breaker attempts recovery
            **kwargs: Additional configuration
        """
        self.logger = get_logger(self.name, log_level=log_level)

        # Component initialization with performance monitoring
        self.scheduler = Scheduler(
            max_domain_concurrent=concurrent_requests, log_level=log_level
        )
        self.downloader = Downloader(
            concurrency=concurrent_requests,
            log_level=log_level,
            timeout=request_timeout,
        )
        self.parser = Parser(log_level=log_level)

        # Configuration
        self.settings = {
            **self.custom_settings,
            "CONCURRENT_REQUESTS": concurrent_requests,
            "REQUEST_TIMEOUT": request_timeout,
            "RETRY_TIMES": retry_times,
            "RETRY_DELAY": retry_delay,
            **kwargs,
        }

        # State management
        self._running = False
        self._start_time = 0
        self._stop_future: Optional[asyncio.Future] = None
        self._domain_circuits: Dict[str, CircuitBreaker] = {}

        # Statistics and monitoring
        self.stats = {
            "start_time": None,
            "finish_time": None,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "items_scraped": 0,
            "retry_count": 0,
            "circuits_open": 0,
            "avg_request_time": 0.0,
            "domain_stats": defaultdict(dict),
        }

        # Error recovery
        self._failed_urls: Set[str] = set()
        self._processing: Set[str] = set()
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        # Signal system
        self.signals = Signal()
        self._setup_signals()

    def _setup_signals(self) -> None:
        """Setup default signal handlers."""
        self.signals.connect("spider_started", self._on_spider_started)
        self.signals.connect("spider_stopped", self._on_spider_stopped)
        self.signals.connect("request_scheduled", self._on_request_scheduled)
        self.signals.connect("request_finished", self._on_request_finished)
        self.signals.connect("item_scraped", self._on_item_scraped)

    def _on_spider_started(self, sender: Any) -> None:
        """Handle spider started signal."""
        self.logger.info(f"Spider {self.name} started")

    def _on_spider_stopped(self, sender: Any) -> None:
        """Handle spider stopped signal."""
        self.logger.info(f"Spider {self.name} stopped")

    def _on_request_scheduled(self, sender: Any, request: Request) -> None:
        """Handle request scheduled signal."""
        domain = self._get_domain(request.url)
        self.stats["domain_stats"][domain]["scheduled"] = (
            self.stats["domain_stats"][domain].get("scheduled", 0) + 1
        )

    def _on_request_finished(
        self, sender: Any, request: Request, success: bool
    ) -> None:
        """Handle request finished signal."""
        domain = self._get_domain(request.url)
        stats = self.stats["domain_stats"][domain]
        if success:
            stats["successful"] = stats.get("successful", 0) + 1
        else:
            stats["failed"] = stats.get("failed", 0) + 1

    def _on_item_scraped(self, sender: Any, item: Any) -> None:
        """Handle item scraped signal."""
        self.stats["items_scraped"] += 1

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        from urllib.parse import urlparse

        return urlparse(url).netloc

    def _get_circuit_breaker(self, domain: str) -> CircuitBreaker:
        """Get or create circuit breaker for domain."""
        if domain not in self._domain_circuits:
            self._domain_circuits[domain] = CircuitBreaker(
                self.failure_threshold, self.recovery_timeout
            )
        return self._domain_circuits[domain]

    async def start_requests(self) -> AsyncGenerator[Request, None]:
        """Generate initial requests.

        Yields:
            Request objects for initial URLs
        """
        for url in self.start_urls:
            yield Request(
                url=url,
                callback=self.parse,
                errback=self.handle_error,
                retries=self.settings["RETRY_TIMES"],
                timeout=self.settings["REQUEST_TIMEOUT"],
            )

    async def parse(
        self, response: Any, **kwargs
    ) -> AsyncGenerator[Union[Request, Result], None]:
        """Default parse method.

        Args:
            response: Response from downloader
            **kwargs: Additional arguments

        Yields:
            Request or Result objects
        """
        raise NotImplementedError

    async def handle_error(self, failure: Exception, request: Request) -> None:
        """Handle request failures with circuit breaker.

        Args:
            failure: Exception that occurred
            request: Failed request
        """
        self.stats["failed_requests"] += 1
        self._failed_urls.add(request.url)

        domain = self._get_domain(request.url)
        circuit = self._get_circuit_breaker(domain)
        circuit.record_failure()

        if circuit.state == "open":
            self.stats["circuits_open"] += 1
            self.logger.warning(f"Circuit breaker opened for domain: {domain}")

        self.logger.error(
            f"Request failed: {request.url}, error: {str(failure)}, "
            f"retries left: {request.retries}, circuit state: {circuit.state}"
        )

    async def retry_failed(self) -> None:
        """Retry failed requests with circuit breaker check."""
        if not self._failed_urls:
            return

        self.logger.info(f"Retrying {len(self._failed_urls)} failed requests")
        for url in self._failed_urls.copy():
            if url not in self._processing:
                domain = self._get_domain(url)
                circuit = self._get_circuit_breaker(domain)

                if circuit.allow_request():
                    await self.scheduler.schedule_request(
                        Request(
                            url=url,
                            callback=self.parse,
                            errback=self.handle_error,
                            retries=self.settings["RETRY_TIMES"],
                            priority=100,  # High priority retry
                        )
                    )
                    self._failed_urls.remove(url)
                else:
                    self.logger.warning(
                        f"Retry blocked by circuit breaker for domain: {domain}"
                    )

    async def process_request(self, request: Request) -> None:
        """Process a single request with monitoring.

        Args:
            request: Request to process
        """
        start_time = time.time()
        self.stats["total_requests"] += 1
        self._processing.add(request.url)

        domain = self._get_domain(request.url)
        circuit = self._get_circuit_breaker(domain)

        if not circuit.allow_request():
            self.logger.warning(f"Request blocked by circuit breaker: {request.url}")
            return

        try:
            self.signals.send("request_scheduled", self, request=request)
            response = await self.downloader.fetch(request)

            if request.callback:
                async for result in request.callback(response):
                    if isinstance(result, Request):
                        await self.scheduler.schedule_request(result)
                    elif isinstance(result, Result):
                        self.signals.send("item_scraped", self, item=result)

            circuit.record_success()
            self.stats["successful_requests"] += 1
            self.signals.send("request_finished", self, request=request, success=True)

        except Exception as e:
            if request.errback:
                await request.errback(e, request)
            self.signals.send("request_finished", self, request=request, success=False)

        finally:
            self._processing.remove(request.url)
            self.scheduler.request_completed(request)

            # Update timing stats
            elapsed = time.time() - start_time
            total_requests = self.stats["total_requests"]
            self.stats["avg_request_time"] = (
                self.stats["avg_request_time"] * (total_requests - 1) + elapsed
            ) / total_requests

    async def start(self) -> None:
        """Start the spider with monitoring."""
        if self._running:
            return

        self._running = True
        self._start_time = time.time()
        self.stats["start_time"] = self._start_time
        self.signals.send("spider_started", self)

        try:
            # Initialize requests
            async for request in self.start_requests():
                await self.scheduler.schedule_request(request)

            # Main loop
            while self._running:
                request = await self.scheduler.get_request()
                if not request:
                    if not self._processing:
                        break
                    await asyncio.sleep(0.1)
                    continue

                asyncio.create_task(self.process_request(request))

                # Periodically retry failed requests
                if self._failed_urls:
                    await self.retry_failed()

        except Exception as e:
            self.logger.error(f"Spider error: {str(e)}")
        finally:
            await self.close()

    async def close(self) -> None:
        """Clean up resources and save stats."""
        if not self._running:
            return

        self._running = False
        self.stats["finish_time"] = time.time()

        await self.downloader.close()

        duration = self.stats["finish_time"] - self.stats["start_time"]
        self.logger.info(
            f"Spider {self.name} closed: "
            f"crawled {self.stats['successful_requests']} pages in {duration:.2f}s"
        )

        self.signals.send("spider_stopped", self)

    def get_stats(self) -> dict:
        """Get comprehensive spider statistics.

        Returns:
            Dictionary with spider statistics
        """
        return {
            **self.stats,
            "scheduler_stats": self.scheduler.get_stats(),
            "parser_stats": self.parser.get_stats(),
            "downloader_stats": self.downloader.stats,
            "failed_urls": list(self._failed_urls),
            "processing": list(self._processing),
            "circuits": {
                domain: cb.state for domain, cb in self._domain_circuits.items()
            },
        }
