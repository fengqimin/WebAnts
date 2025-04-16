"""Spider Module
This module provides the base Spider class with the following features:
- Configurable request handling
- Middleware support
- Error recovery
- Stats collection
- Event hooks
"""

import asyncio
import inspect
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Union

from webants.downloader import Downloader
from webants.libs.request import Request
from webants.libs.result import Result
from webants.parser import Parser
from webants.scheduler import Scheduler
from webants.utils.logger import get_logger


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
        retry_delay: float = 1,
        log_level: int = logging.INFO,
        **kwargs
    ):
        """Initialize spider with configuration.
        
        Args:
            concurrent_requests: Maximum concurrent requests
            request_timeout: Request timeout in seconds
            retry_times: Number of times to retry failed requests
            retry_delay: Delay between retries in seconds
            log_level: Logging level
            **kwargs: Additional configuration
        """
        self.logger = get_logger(self.name, log_level=log_level)
        
        # Component initialization
        self.scheduler = Scheduler(
            max_domain_concurrent=concurrent_requests,
            log_level=log_level
        )
        self.downloader = Downloader(
            concurrency=concurrent_requests,
            log_level=log_level,
            timeout=request_timeout
        )
        self.parser = Parser(log_level=log_level)
        
        # Configuration
        self.settings = {
            **self.custom_settings,
            "CONCURRENT_REQUESTS": concurrent_requests,
            "REQUEST_TIMEOUT": request_timeout,
            "RETRY_TIMES": retry_times,
            "RETRY_DELAY": retry_delay,
            **kwargs
        }
        
        # State management
        self._running = False
        self._start_time = 0
        self._stop_future: Optional[asyncio.Future] = None
        
        # Statistics
        self.stats = {
            "start_time": None,
            "finish_time": None,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "items_scraped": 0,
            "retry_count": 0
        }
        
        # Error recovery
        self._failed_urls: Set[str] = set()
        self._processing: Set[str] = set()

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
                timeout=self.settings["REQUEST_TIMEOUT"]
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
        """Handle request failures.
        
        Args:
            failure: Exception that occurred
            request: Failed request
        """
        self.stats["failed_requests"] += 1
        self._failed_urls.add(request.url)
        self.logger.error(
            f"Request failed: {request.url}, error: {str(failure)}, "
            f"retries left: {request.retries}"
        )

    async def retry_failed(self) -> None:
        """Retry failed requests."""
        if not self._failed_urls:
            return
            
        self.logger.info(f"Retrying {len(self._failed_urls)} failed requests")
        for url in self._failed_urls.copy():
            if url not in self._processing:
                await self.scheduler.schedule_request(
                    Request(
                        url=url,
                        callback=self.parse,
                        errback=self.handle_error,
                        retries=self.settings["RETRY_TIMES"],
                        priority=100  # High priority retry
                    )
                )
                self._failed_urls.remove(url)

    async def process_request(self, request: Request) -> None:
        """Process a single request.
        
        Args:
            request: Request to process
        """
        self.stats["total_requests"] += 1
        self._processing.add(request.url)
        
        try:
            response = await self.downloader.fetch(request)
            
            if request.callback:
                async for result in request.callback(response):
                    if isinstance(result, Request):
                        await self.scheduler.schedule_request(result)
                    elif isinstance(result, Result):
                        self.stats["items_scraped"] += 1
                        # Process result...
                        
            self.stats["successful_requests"] += 1
            
        except Exception as e:
            if request.errback:
                await request.errback(e, request)
        finally:
            self._processing.remove(request.url)
            self.scheduler.request_completed(request)

    async def start(self) -> None:
        """Start the spider."""
        if self._running:
            return
            
        self._running = True
        self._start_time = time.time()
        self.stats["start_time"] = self._start_time
        self.logger.info(f"Spider {self.name} starting...")
        
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
        """Clean up resources."""
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

    def get_stats(self) -> dict:
        """Get spider statistics.
        
        Returns:
            Dictionary with spider statistics
        """
        return {
            **self.stats,
            "scheduler_stats": self.scheduler.get_stats(),
            "parser_stats": self.parser.get_stats(),
            "failed_urls": list(self._failed_urls),
            "processing": list(self._processing)
        }
