"""Downloader Module
This module provides downloading functionality with the following features:
- Asynchronous HTTP/HTTPS requests handling
- Local file system access
- Priority queue based request scheduling
- Concurrent download management
- Automatic retry mechanism

Downloader从request_queue队列中获取request，进行获取，并根据获取的结果类型放入相应的队列中：

response类型，放入response_queue队列中；

request类型，重新放入request队列中，等待再次重试获取；

"""

import asyncio
import logging
from abc import abstractmethod
from inspect import iscoroutinefunction
from typing import Optional, Union

import httpx

from webants.libs import Request, Response
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
            **kwargs: Additional configuration parameters
        """
        super().__init__(request_queue=request_queue, response_queue=response_queue)
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)

        # Event loop setup
        self.loop = self._setup_event_loop(loop)

        # HTTP client configuration
        self.headers = self._setup_headers(kwargs.get("headers", {}))
        self.cookies = kwargs.get("cookies", {})
        self.client = self._setup_http_client()

        # Concurrency control
        self.concurrency = concurrency
        self.sem = asyncio.Semaphore(concurrency)

        # Additional settings
        self.delay = kwargs.get("delay", 0)
        self.default_encoding = kwargs.get("encoding", "utf-8")
        self.kwargs = kwargs

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
            http2=False,
        )

    def _setup_event_loop(
        self, loop: Optional[asyncio.AbstractEventLoop]
    ) -> asyncio.AbstractEventLoop:
        """Setup and return event loop."""
        if loop:
            self._close_loop = False
            return loop

        try:
            loop = asyncio.get_running_loop()
            self._close_loop = False
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._close_loop = True
        return loop

    async def _fetch(self, request: Request) -> Union[httpx.Response, Exception, None]:
        delay = request.delay or self.delay
        self.logger.debug(f"Fetching {request}, delay<{delay}>")
        await asyncio.sleep(delay)

        # if not request.headers:
        headers = request.headers or self.headers

        self.logger.debug(f"request headers: {headers}")

        try:
            # 发送请求，并包装成Response
            # httpx.AsyncClient.request()方法会自动处理重定向和cookies
            resp = await self.client.request(
                method=request.method,
                url=request.url,
                headers=headers,
                timeout=request.timeout,
            )
            return resp

        except httpx.TimeoutException as e:
            self.logger.error(
                f"Timeout, retries {request}<{request.retries}> again later."
            )
            return e
        except httpx.RequestError as e:
            self.logger.error(
                f"RequestError, retries {request}<{request.retries}> again later."
            )
            return e

    async def _retry(
        self, request: Request, exception: Exception
    ) -> Optional[httpx.Response]:
        request.retries -= 1
        # request.priority += 10

        self.logger.info(
            f"<Retry, url: {request.url}>, times: {request.retries}, reason: {exception}>"
        )
        if request.retries > 0:
            return await self.fetch(request)
        else:
            return httpx.Response(
                status_code=600,
                request=httpx.Request(request.method, request.url),
                # body=bytes(str(exception), encoding=self.default_encoding),
            )

    async def fetch(self, request: Request) -> Optional[httpx.Response]:
        """Fetch a resource based on the request.

        Args:
            request: Request object containing URL and other parameters

        Returns:
            Response object or None if request fails
        """

        try:
            async with self.sem:
                if request.url.startswith("http"):
                    result = await self._fetch(request)
                elif request.url.startswith("file"):
                    result = await self._fetch_local(request)
        except Exception as e:
            # result = None
            self.logger.error(f"<Error: {request.url} {e}>")
            return None

        if isinstance(result, httpx.Response):
            if request.callback is None:
                return result
            if iscoroutinefunction(request.callback):
                result = await request.callback(result, request.cb_kwargs)
            else:
                result = request.callback(result, request.cb_kwargs)
        elif isinstance(result, Request):
            if request.retries > 0:
                request.retries -= 1
                request.priority += 10
                self.request_queue.put_nowait((request.priority, request))
            result = None
        else:
            result = await self._retry(request, result)

        return result

    async def start_worker(self) -> None:
        """Process queue items forever."""

        while True:
            request = await self._next_request()
            # if request is None:
            #     continue
            resp = await self.fetch(request)

            if self.response_queue:
                self.response_queue.put_nowait(resp)

            # Notify the queue that the "work item" has been processed.
            self.request_queue.task_done()

    async def start_downloader(self, many: int = None) -> None:
        """Run {many} workers until all tasks finished."""
        many = many or self.concurrency
        self.logger.info(f"Start {self.__class__.__name__}...")
        try:
            # __ = [asyncio.create_task(self.worker())
            #       for _ in range(min(self.request_queue.qsize(), self.concurrency) or 1)]
            __ = [
                asyncio.create_task(self.start_worker())
                for _ in range(min(self.concurrency, many))
            ]
            await self.request_queue.join()
        except Exception as e:
            raise e

    def stop(self) -> None:
        self.logger.debug(f"{self.__class__.__name__} already stopped.")

    async def close(self) -> None:
        await self.client.aclose()

        self.logger.info(f"{self.__class__.__name__} has been closed.")


if __name__ == "__main__":

    async def main():
        pd = Downloader(asyncio.PriorityQueue())
        resp = await pd.fetch(
            Request(
                "http://www.google.com"
            )
        )
        print(resp)
        print(resp.request.headers)
        print(resp.text)
        # print(resp.json)
        await pd.close()

    asyncio.run(main())
