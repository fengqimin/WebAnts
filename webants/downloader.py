"""Downloader

    Downloader从request_queue队列中获取request，进行获取，并根据获取的结果类型放入相应的队列中：

    response类型，放入response_queue队列中；

    request类型，重新放入request队列中，等待再次重试获取；

"""
import asyncio
import logging
import queue
from abc import abstractmethod
from inspect import iscoroutinefunction
from pathlib import Path
from typing import Union

import aiohttp

from webants.libs.request import Request
from webants.libs.response import Response, get_encoding
from webants.utils.logger import get_logger


class BaseDownloader:
    """base class of Downloader"""

    download_count: int = 0

    def __init__(
        self,
        request_queue: asyncio.PriorityQueue,
        response_queue: queue.Queue | asyncio.Queue = None,
        **kwargs,
    ):
        self.request_queue = request_queue
        self.response_queue = response_queue

    async def _next_request(self) -> Request | None:
        if isinstance(self.request_queue, asyncio.PriorityQueue):
            priority, request = await self.request_queue.get()
        else:
            request = await self.request_queue.get()

        return request

    @abstractmethod
    async def fetch(self, *args, **kwargs) -> Union[Response, Request, None]:
        pass

    @abstractmethod
    async def start_worker(self, *args, **kwargs) -> None:
        """Process queue items forever."""
        pass

    @abstractmethod
    async def start_downloader(self, *args, **kwargs) -> None:
        """Run the worker until all finished."""
        pass

    @abstractmethod
    async def close(self):
        pass


class Downloader(BaseDownloader):
    """Downloader"""

    def __init__(
        self,
        request_queue: asyncio.PriorityQueue,
        response_queue: queue.Queue | asyncio.Queue = None,
        *,
        log_level: int = logging.INFO,
        concurrency: int = 10,
        loop: asyncio.AbstractEventLoop | None = None,
        **kwargs,
    ):
        """

        :param request_queue:
        :param response_queue
        :param log_level:
        :param concurrency: asyncio.Semaphore,控制并行下载的连接数
        :param loop:
        :param kwargs
        """
        super(Downloader, self).__init__(
            request_queue=request_queue, response_queue=response_queue
        )
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)

        if loop:
            self.loop = loop
            self._close_loop = False
        else:
            try:
                self.loop = asyncio.get_running_loop()
            except RuntimeError:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
                self._close_loop = True

        self._close_session = False
        self.cookies = kwargs.get("cookies", {})
        self.session = aiohttp.ClientSession(loop=self.loop, cookies=self.cookies)
        self._close_session = True

        self.request_queue = request_queue
        assert isinstance(self.request_queue, asyncio.PriorityQueue)
        self.response_queue = response_queue

        self.concurrency = concurrency
        self.sem = asyncio.Semaphore(concurrency)
        self.kwargs = kwargs or {}
        self.delay = kwargs.get("delay", 0)
        self.headers = kwargs.get("headers", {})
        self.headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/116.0",
        )
        # self.cookies = kwargs.get("cookies", {})
        self.default_encoding = kwargs.get("encoding", "utf-8")

    async def _fetch(self, request: Request) -> Union[Response, Exception, None]:
        delay = request.delay or self.delay
        self.logger.debug(f"Fetching {request}, delay<{delay}>")
        await asyncio.sleep(delay)

        # if not request.headers:
        request.headers = self.headers or request.headers

        self.logger.debug(f"request headers: {request.headers}")

        try:
            async with self.session.request(
                method=request.method,
                url=request.url,
                headers=request.headers,
                timeout=request.timeout,
                cookies=self.cookies,
            ) as r:
                resp = await Response.build(r, request)

                self.logger.debug(resp)
                self.download_count += 1
                return resp
        except asyncio.TimeoutError:
            self.logger.error(
                f"Timeout, retries {request}<{request.retries}> again later."
            )
            # return request
            return Exception("asyncio.TimeoutError")
        except aiohttp.ClientError:
            self.logger.error(
                f"ClientError, retries {request}<{request.retries}> again later."
            )
            return Exception("aiohttp.ClientError")

    async def _fetch_local(self, request: Request) -> Union[Response, None]:
        self.logger.debug(f"Fetching {request}, delay<{0.0}>")
        await asyncio.sleep(0.0)
        p = Path(request.url.removeprefix("file:///"))
        if not p.is_file():
            self.logger.error(f"'{p}' is not a file.")

        try:
            # 读取本地文件，并包装成Response
            content = p.read_bytes()
            resp = Response(
                "GET",
                str(request.url),
                status_code=200,
                reason="su",
                request=request,
                http_version=None,
                headers=None,
                cookies=None,
                body=content,
                encoding=get_encoding(content=content),
                history=None,
            )
            self.logger.debug(resp)
            self.download_count += 1
            return resp
        except OSError as e:
            self.logger.error(f"OSError: {e}")
            return None

    async def _retry(self, request: Request, exception: Exception) -> Response | None:
        request.retries -= 1
        # request.priority += 10

        self.logger.info(
            f"<Retry, url: {request.url}>, times: {request.retries}, reason: {exception}>"
        )
        if request.retries > 0:
            return await self.fetch(request)
        else:
            return Response(
                request.method.upper(),
                str(request.url),
                status_code=600,
                reason=str(exception),
                request=request,
                http_version=None,
                headers=None,
                cookies=None,
                body=bytes(str(exception), encoding=self.default_encoding),
                encoding=self.default_encoding,
                history=None,
            )

    async def fetch(self, request: Request) -> Union[Response, None]:
        """
        信号量会管理一个内部计数器，该计数器会随每次 acquire() 调用递减并随每次 release() 调用递增。
        计数器的值永远不会降到零以下；当 acquire() 发现其值为零时，它将保持阻塞直到有某个任务调用了 release()。
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

        if isinstance(result, Response):
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
        if self._close_session:
            await asyncio.gather(self.session.close())
            self._close_session = False

            # if self._close_loop:
            #     self.loop.run_until_complete(self.session.close())
            #     self.loop.close()

            self.logger.info(f"{self.__class__.__name__} has been closed.")


if __name__ == "__main__":

    async def main():
        pd = Downloader(asyncio.PriorityQueue())
        await pd.close()

    asyncio.run(main())
