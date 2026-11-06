"""Spider Module
This module provides the base Spider class with the following features:
- Configurable request handling
- Middleware support
- Error recovery with circuit breaker
- Stats collection and monitoring
- Event hooks and signals
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, AsyncGenerator, Callable, Dict, List, Optional, Union

from webants.downloader import Downloader
from webants.libs.request import Request
from webants.libs.result import Result
from webants.parser import Parser
from webants.scheduler import Scheduler, ScheduleReason, ScheduleResult, ScheduleSlot
from webants.utils.logger import get_logger

if TYPE_CHECKING:
    from webants.libs.config import Config
    from webants.libs.middleware import MiddlewareManager
    from webants.libs.persister import PersisterManager


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

        Raises:
            TypeError: If ``handler`` is a coroutine function. This signal is
                synchronous; async handlers belong to
                ``webants.libs.signals.signal_manager``.
        """
        if inspect.iscoroutinefunction(handler):
            raise TypeError(
                "Spider.signals is synchronous and cannot accept a coroutine "
                "function; use webants.libs.signals.signal_manager for async "
                "handlers"
            )
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
        concurrent_requests: int | None = None,
        request_timeout: float | None = None,
        retry_times: int | None = None,
        retry_delay: float | None = None,
        log_level: int | str | None = None,
        failure_threshold: int | None = None,
        recovery_timeout: float | None = None,
        idle_grace: float = 0.5,
        middleware_max_retries: int = 3,
        config: Config | None = None,
        middleware: MiddlewareManager | None = None,
        persister: PersisterManager | None = None,
        **kwargs,
    ):
        """Initialize spider with configuration.

        Args:
            concurrent_requests: Maximum concurrent requests. Overrides the
                corresponding config value when provided.
            request_timeout: Request timeout in seconds
            retry_times: Number of times to retry failed requests
            retry_delay: Delay between retries in seconds
            log_level: Logging level (int or level name)
            failure_threshold: Number of failures before circuit breaker opens
            recovery_timeout: Time before circuit breaker attempts recovery
            idle_grace: Seconds the queue and the in-flight table must stay
                empty before the main loop exits. 0 exits as soon as both are
                empty (useful in tests).
            middleware_max_retries: Maximum times a middleware may replace a
                failed request through ``process_exception``.
            config: Optional explicit configuration. Values are used only when
                the matching constructor argument is not provided.
            middleware: Optional middleware chain. The spider calls its
                ``lookup``/``process_request``/``process_response``/
                ``process_exception`` phases.
            persister: Optional persister manager. Ownership transfers to the
                spider: ``close()`` flushes and closes it.
            **kwargs: Additional configuration added to ``settings``
        """

        def config_value(path: str, default: Any) -> Any:
            if config is None:
                return default
            return config.get(path, default)

        downloader_concurrency = (
            concurrent_requests
            if concurrent_requests is not None
            else config_value("downloader.concurrent_requests", 10)
        )
        if concurrent_requests is not None:
            scheduler_concurrency = concurrent_requests
        elif config is not None:
            scheduler_concurrency = config_value(
                "scheduler.max_domain_concurrent", downloader_concurrency
            )
        else:
            scheduler_concurrency = downloader_concurrency

        request_timeout = (
            request_timeout
            if request_timeout is not None
            else config_value("downloader.request_timeout", 30)
        )
        retry_times = (
            retry_times if retry_times is not None else config_value("downloader.retry_times", 3)
        )
        retry_delay = (
            retry_delay if retry_delay is not None else config_value("downloader.retry_delay", 1.0)
        )
        failure_threshold = (
            failure_threshold
            if failure_threshold is not None
            else config_value("spider.failure_threshold", 5)
        )
        recovery_timeout = (
            recovery_timeout
            if recovery_timeout is not None
            else config_value("spider.recovery_timeout", 60.0)
        )
        if log_level is None:
            log_level = self._coerce_log_level(config_value("logging.level", "INFO"))
        elif isinstance(log_level, str):
            log_level = self._coerce_log_level(log_level)

        self.config = config
        self.middleware = middleware
        self.persister = persister
        self.middleware_max_retries = middleware_max_retries

        self.logger = get_logger(self.name, log_level=log_level)

        # Component initialization with performance monitoring
        self.scheduler = Scheduler(
            max_requests=config_value("scheduler.max_requests", 0),
            request_delay=config_value("scheduler.request_delay", 0),
            domain_delay=config_value("scheduler.domain_delay", 0),
            max_domain_concurrent=scheduler_concurrency,
            log_level=log_level,
            max_queue_size=config_value("scheduler.max_queue_size", 10000),
        )
        self.downloader = Downloader(
            concurrency=downloader_concurrency,
            log_level=log_level,
            timeout=request_timeout,
            retry_delay=retry_delay,
        )
        self.parser = Parser(
            encoding=config_value("parser.default_encoding", "utf-8"),
            max_document_size=config_value("parser.max_document_size", 10 * 1024 * 1024),
            cache_size=config_value("parser.cache_size", 1000),
            log_level=log_level,
        )

        # Configuration
        self.settings = {
            **self.custom_settings,
            "CONCURRENT_REQUESTS": downloader_concurrency,
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

        # Lifecycle: shutdown state, close serialization and the activity clock
        # the main loop uses to decide when the crawl has gone quiet.
        self.idle_grace = idle_grace
        self._last_activity = 0.0
        self._closed = False
        self._close_lock = asyncio.Lock()

        # Statistics and monitoring
        self.stats = {
            "start_time": None,
            "finish_time": None,
            "total_requests": 0,
            "completed_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "cancelled_requests": 0,
            "unhandled_task_errors": 0,
            "items_scraped": 0,
            "retry_count": 0,
            "circuits_open": 0,
            "avg_request_time": 0.0,
            "total_request_time": 0.0,
            "filtered_requests": 0,
            "filtered_responses": 0,
            "middleware_retries": 0,
            "cache_hits": 0,
            "persist_failures": 0,
            "domain_stats": defaultdict(dict),
        }

        # Lifecycle and error recovery
        # In-flight requests keyed by the task processing them. Keying by task
        # instead of URL keeps two concurrent requests for the same URL distinct.
        self._inflight: Dict[asyncio.Task, Request] = {}
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        # Signal system
        self.signals = Signal()
        self._setup_signals()

    @staticmethod
    def _coerce_log_level(value: Any) -> int:
        """Convert a logging level name or number to an int."""
        if isinstance(value, int) and not isinstance(value, bool):
            return value

        name = str(value).upper()
        level = logging.getLevelName(name)
        if isinstance(level, int):
            return level

        logging.getLogger("Spider").warning(f"Unknown logging level {value!r}, using INFO")
        return logging.INFO

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

    def _on_request_finished(self, sender: Any, request: Request, success: bool) -> None:
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

    async def parse(self, response: Any, **kwargs) -> AsyncGenerator[Union[Request, Result], None]:
        """Default parse method.

        Args:
            response: Response from downloader
            **kwargs: Additional arguments

        Yields:
            Request or Result objects
        """
        raise NotImplementedError

    async def handle_error(self, failure: Exception, request: Request) -> None:
        """Default errback: log the failure.

        Failure counting and circuit-breaker updates are owned by
        ``process_request`` so custom errbacks cannot skew statistics.

        Args:
            failure: Exception that occurred
            request: Failed request
        """
        domain = self._get_domain(request.url)
        circuit = self._get_circuit_breaker(domain)
        self.logger.error(
            f"Request failed: {request.url}, error: {str(failure)}, "
            f"retries left: {request.retries}, circuit state: {circuit.state}"
        )

    async def _invoke_callback(self, request: Request, response: Any) -> None:
        """Invoke a request callback exactly once and dispatch its output.

        Canonical callbacks are async generator functions. Coroutine callbacks
        are accepted for compatibility and normalized here; Downloader never
        invokes callbacks.
        """
        callback = request.callback
        if callback is None:
            return

        result = callback(response, **request.cb_kwargs)

        if inspect.isasyncgen(result):
            try:
                async for item in result:
                    await self._dispatch_item(item)
            finally:
                await result.aclose()
            return

        if inspect.isawaitable(result):
            value = await result
            await self._consume_callback_value(request, value)
            return

        raise TypeError(
            f"callback for {request.url} did not return a coroutine or async "
            f"generator; got {type(result).__name__}"
        )

    async def _consume_callback_value(self, request: Request, value: Any) -> None:
        """Normalize the value returned by a coroutine callback."""
        if value is None:
            return

        if isinstance(value, (Request, Result)):
            await self._dispatch_item(value)
            return

        if hasattr(value, "__aiter__"):
            async for item in value:
                await self._dispatch_item(item)
            return

        if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray, dict)):
            for item in value:
                await self._dispatch_item(item)
            return

        raise TypeError(
            f"callback for {request.url} returned unsupported type "
            f"{type(value).__name__}; expected Request, Result, an iterable, "
            f"an async iterable, or None"
        )

    async def _schedule(self, request: Request) -> Optional[ScheduleResult]:
        """Hand a request to the scheduler and report the honest outcome.

        ``request_scheduled`` is emitted only when the request really entered
        the queue, so domain statistics and listeners describe actual scheduling
        instead of every request that reached ``process_request``.

        Returns:
            Optional[ScheduleResult]: None when the spider is already closed and
            the request was dropped without touching the scheduler.
        """
        if self._closed:
            self.logger.warning(f"Spider is closed, request dropped: {request.url}")
            return None

        result = await self.scheduler.schedule_request(request)
        if result:
            self._last_activity = time.monotonic()
            self.signals.send("request_scheduled", self, request=request)
        elif result.reason is ScheduleReason.QUEUE_FULL:
            self.logger.warning(f"Queue full, request dropped: {request.url}")
        elif result.reason is ScheduleReason.LIMIT_REACHED:
            self.logger.warning(f"Request limit reached, dropped: {request.url}")
        else:
            self.logger.debug(f"Request filtered ({result.reason.value}): {request.url}")
        return result

    async def _dispatch_item(self, item: Any) -> None:
        """Dispatch one callback output item."""
        if isinstance(item, Request):
            await self._schedule(item)
        elif isinstance(item, Result):
            self.signals.send("item_scraped", self, item=item)
            await self._persist_item(item)
        else:
            raise TypeError(
                f"callback yielded unsupported type {type(item).__name__}; "
                f"expected Request or Result"
            )

    async def _persist_item(self, item: Result) -> None:
        """Persist one result without turning success into request failure."""
        if self.persister is None:
            return

        try:
            results = await self.persister.save(item)
        except Exception as e:
            self.stats["persist_failures"] += 1
            self.logger.error(f"Error persisting result for {item.url}: {e}")
            return

        failed = [name for name, success in results.items() if not success]
        if failed:
            self.stats["persist_failures"] += len(failed)
            self.logger.error(f"Persisting result for {item.url} failed for: {', '.join(failed)}")

    async def _prepare_response(self, request: Request) -> Optional[tuple[Request, Any]]:
        """Run the middleware phases that precede the callback.

        Returns:
            ``(request, response)`` where ``request`` is the request actually
            used (middleware may replace it), or ``None`` when middleware
            filtered the request or the response.
        """
        if self.middleware is None:
            return request, await self.downloader.fetch(request)

        response = await self.middleware.lookup(request)
        if response is not None:
            self.stats["cache_hits"] += 1
        else:
            prepared = await self.middleware.process_request(request)
            if prepared is None:
                self.stats["filtered_requests"] += 1
                self.signals.send("request_dropped", self, request=request, reason="middleware")
                return None

            if self._get_domain(prepared.url) != self._get_domain(request.url):
                raise ValueError(
                    "middleware.process_request must not change the request "
                    f"domain: {request.url} -> {prepared.url}"
                )

            request = prepared
            response = await self.downloader.fetch(request)

        response = await self.middleware.process_response(response, request)
        if response is None:
            self.stats["filtered_responses"] += 1
            return None

        return request, response

    async def _apply_middleware_exception(
        self, exception: Exception, request: Request
    ) -> tuple[str, Any]:
        """Ask middleware how to handle a failed request.

        Returns:
            ``("none", None)`` to use the normal failure path,
            ``("retry", Request)`` to reschedule the replacement, or
            ``("response", response)`` to substitute a response.
        """
        if self.middleware is None:
            return "none", None

        try:
            replacement = await self.middleware.process_exception(exception, request)
        except Exception as middleware_error:
            self.logger.error(
                f"Error in middleware.process_exception for {request.url}: {middleware_error}"
            )
            return "none", None

        if replacement is None:
            return "none", None
        if not isinstance(replacement, Request):
            return "response", replacement

        meta = request.meta if isinstance(request.meta, dict) else {}
        try:
            retries = int(meta.get("_middleware_retry", 0))
        except (TypeError, ValueError):
            retries = 0
        if retries >= self.middleware_max_retries:
            self.logger.warning(
                f"Middleware retry limit reached for {request.url} ({self.middleware_max_retries})"
            )
            return "none", None

        replacement.meta = {
            **(replacement.meta if isinstance(replacement.meta, dict) else {}),
            "_middleware_retry": retries + 1,
        }
        return "retry", replacement

    async def process_request(
        self, request: Request, *, slot: Optional[ScheduleSlot] = None
    ) -> None:
        """Process a single request with monitoring.

        Args:
            request: Request to process
            slot: Scheduling slot owning the domain permit and accounting
                entries for this request. Passed by the main loop; omitted when
                a caller processes a request it never scheduled, in which case
                there is nothing to release.
        """
        start_time = time.time()
        self.stats["total_requests"] += 1

        # The main loop registers the task before it can yield, which keeps the
        # exit check race-free. Direct callers (tests, library use) register
        # themselves here so in-flight counting never depends on the loop.
        current_task = asyncio.current_task()
        tracked_externally = current_task in self._inflight
        if current_task is not None and not tracked_externally:
            self._inflight[current_task] = request

        domain = self._get_domain(request.url)
        circuit = self._get_circuit_breaker(domain)

        try:
            if not circuit.allow_request():
                self.logger.warning(f"Request blocked by circuit breaker: {request.url}")
                return

            prepared = await self._prepare_response(request)
            if prepared is None:
                return
            request, response = prepared

            await self._invoke_callback(request, response)

            circuit.record_success()
            self.stats["successful_requests"] += 1
            self.signals.send("request_finished", self, request=request, success=True)

        except asyncio.CancelledError:
            # Tasks registered by the main loop are counted by their done
            # callback. A directly awaited process_request() has no such
            # callback, so count its cancellation here to keep the statistic
            # truthful for both entry points.
            if not tracked_externally:
                self.stats["cancelled_requests"] += 1
            raise

        except Exception as e:
            action, replacement = await self._apply_middleware_exception(e, request)

            if action == "retry":
                try:
                    result = await self._schedule(replacement)
                except Exception as schedule_error:
                    self.logger.error(
                        f"Error scheduling middleware retry for {request.url}: {schedule_error}"
                    )
                    e = schedule_error
                else:
                    self.stats["middleware_retries"] += 1
                    if not result:
                        reason = result.reason.value if result else "closed"
                        self.logger.warning(
                            f"Middleware retry for {request.url} was not scheduled: {reason}"
                        )
                    return

            elif action == "response":
                try:
                    await self._invoke_callback(request, replacement)
                except Exception as callback_error:
                    e = callback_error
                else:
                    circuit.record_success()
                    self.stats["successful_requests"] += 1
                    self.signals.send("request_finished", self, request=request, success=True)
                    return

            was_open = circuit.state == "open"
            circuit.record_failure()
            if not was_open and circuit.state == "open":
                self.stats["circuits_open"] += 1
                self.logger.warning(f"Circuit breaker opened for domain: {domain}")

            self.stats["failed_requests"] += 1
            if request.errback:
                try:
                    await request.errback(e, request)
                except Exception as errback_error:
                    self.logger.error(f"Error in errback for {request.url}: {str(errback_error)}")
            self.signals.send("request_finished", self, request=request, success=False)

        finally:
            if current_task is not None and not tracked_externally:
                self._inflight.pop(current_task, None)
            if slot is not None:
                slot.release()

            # ``avg_request_time`` is the mean duration of completed requests;
            # in-flight requests never contribute to the denominator.
            self.stats["completed_requests"] += 1
            elapsed = time.time() - start_time
            self.stats["total_request_time"] += elapsed
            self.stats["avg_request_time"] = (
                self.stats["total_request_time"] / self.stats["completed_requests"]
            )

    def _on_task_done(self, task: asyncio.Task) -> None:
        """Retire a finished task and observe how it ended.

        ``asyncio`` invokes done callbacks synchronously, so this must stay a
        plain function. ``process_request`` already handles ``Exception``; what
        reaches here is either a cancellation or an escaped ``BaseException``.
        Consuming the exception keeps the event loop from printing
        ``Task exception was never retrieved`` when the task is collected.
        """
        self._inflight.pop(task, None)
        self._last_activity = time.monotonic()

        if task.cancelled():
            self.stats["cancelled_requests"] += 1
            return

        exc = task.exception()
        if exc is not None:
            self.stats["unhandled_task_errors"] += 1
            self.logger.error(f"Unhandled task error: {exc!r}", exc_info=exc)

    async def _close_persister(self) -> None:
        """Flush and close the persister without blocking shutdown."""
        if self.persister is None:
            return

        try:
            results = await self.persister.close()
        except Exception as e:
            self.logger.error(f"Error closing persister: {e}")
            return

        failed = [name for name, success in results.items() if not success]
        if failed:
            self.logger.error(f"Persister close failed for: {', '.join(failed)}")

    async def start(self) -> None:
        """Start the spider with monitoring.

        Raises:
            RuntimeError: when the spider has already been closed. Shutdown is
                terminal; construct a new instance to crawl again.
        """
        if self._closed:
            raise RuntimeError("Spider has been closed; create a new instance")
        if self._running:
            return

        self._running = True
        self._start_time = time.time()
        self.stats["start_time"] = self._start_time
        self._last_activity = time.monotonic()
        self.signals.send("spider_started", self)

        try:
            # Initialize requests
            async for request in self.start_requests():
                await self._schedule(request)

            # Main loop. The scheduler never blocks forever: an empty queue
            # yields None after a short poll, which is what lets the loop exit.
            while self._running:
                slot = await self.scheduler.get_request()
                if slot is None:
                    # An empty poll does not mean done: a background producer
                    # may enqueue moments later. Quiescence requires no
                    # in-flight task and no activity for ``idle_grace``.
                    if (
                        not self._inflight
                        and time.monotonic() - self._last_activity >= self.idle_grace
                    ):
                        break
                    continue

                self._last_activity = time.monotonic()
                request = slot.request
                try:
                    task = asyncio.create_task(self.process_request(request, slot=slot))
                except Exception:
                    # The task never started, so nobody else can release.
                    slot.release()
                    raise
                # Registered before yielding, so the exit check can never see
                # an empty in-flight table while this request is still running.
                self._inflight[task] = request
                self._last_activity = time.monotonic()
                task.add_done_callback(self._on_task_done)

        except Exception as e:
            self.logger.error(f"Spider error: {str(e)}")
        finally:
            await self.close()

    async def close(self) -> None:
        """Release every resource owned by the spider.

        Idempotent, re-entrant and valid before ``start()``. Order matters:
        stop producing, cancel in-flight work, drain the queue, then close the
        HTTP client, then announce — listeners observe a fully closed spider.
        """
        if self._closed:
            return

        async with self._close_lock:
            if self._closed:
                return

            # 1. The loop must not create tasks while we cancel them.
            self._running = False
            if self.stats["finish_time"] is None:
                self.stats["finish_time"] = time.time()

            # 2. Cancel in-flight work. Each task releases its slot in
            # ``process_request``'s finally block; done callbacks clear the
            # registry and count the cancellation.
            inflight = list(self._inflight)
            for task in inflight:
                task.cancel()
            if inflight:
                await asyncio.gather(*inflight, return_exceptions=True)
                for task in inflight:
                    self._inflight.pop(task, None)

            # 3. Hand back every queued permit nobody will consume.
            self.scheduler.discard_pending()

            # 4. Flush and close storage; a failure must not block shutdown.
            await self._close_persister()

            # 5. No task needs the network anymore.
            await self.downloader.close()

            # 6. Announce exactly once.
            self.signals.send("spider_stopped", self)

            # 7. Terminal state.
            self._closed = True

            if self._start_time:
                duration = self.stats["finish_time"] - self._start_time
                self.logger.info(
                    f"Spider {self.name} closed: "
                    f"crawled {self.stats['successful_requests']} pages in {duration:.2f}s"
                )
            else:
                self.logger.info(f"Spider {self.name} closed before it started")

    async def __aenter__(self) -> "Spider":
        """Enter the context manager without starting the crawl."""
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Guarantee shutdown on context exit."""
        await self.close()

    def get_stats(self) -> dict:
        """Get comprehensive spider statistics.

        Returns:
            Dictionary with spider statistics
        """
        stats = {
            **self.stats,
            "scheduler_stats": self.scheduler.get_stats(),
            "parser_stats": self.parser.get_stats(),
            "downloader_stats": self.downloader.stats,
            "processing": [request.url for request in self._inflight.values()],
            "inflight_count": len(self._inflight),
            "closed": self._closed,
            "circuits": {domain: cb.state for domain, cb in self._domain_circuits.items()},
        }
        if self.middleware is not None:
            stats["middleware_stats"] = self.middleware.get_stats()
        if self.persister is not None:
            stats["persister_stats"] = self.persister.get_stats()
        return stats
