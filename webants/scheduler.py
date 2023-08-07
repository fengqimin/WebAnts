import asyncio
from typing import Iterable, Callable

from webants.libs import Request
from webants.utils import get_logger


class Handler:
    def __init__(self):
        pass

    def handle(self, request: Request) -> Request:
        pass


class Scheduler(object):
    """调度器"""

    def __init__(
        self,
        raw_request_queue: asyncio.Queue,
        request_queue: asyncio.PriorityQueue = None,
        *,
        log_level=20,
        **kwargs,
    ):
        self.raw_request_queue = raw_request_queue
        self.request_queue = request_queue or asyncio.PriorityQueue()
        self.request_handlers = kwargs.get("handlers")
        self.seen_urls = set()  # 已经调度过的url集合
        self.logger = get_logger(self.__class__.__name__, log_level=log_level)
        self.running = False

    def _filter_request(self, request: Request) -> Request | None:
        """过滤请求，确保不重复

        :param request:
        :return:
        """
        if request.unique:
            if (fp := request.fingerprint()) not in self.seen_urls:
                # self.request_queue.put_nowait((request.priority, request))
                self.seen_urls.add(fp)
                return request
            # self.logger.debug(f"{request} has been seen")
            return None

        return request

    def enqueue_request(self, request: Request):
        """添加任务"""

        # 根据指纹进行去重
        # if (fp := fingerprint(request)) not in self.seen_urls:
        #     self.request_queue.put_nowait((request.priority, request))
        #     self.seen_urls.add(fp)
        if request := self._filter_request(request):
            self.logger.debug(f"Enqueue: {request}")
            self.request_queue.put_nowait((request.priority, request))

        # self.logger.debug(f"request_queue:{self.request_queue.qsize()}")
        # self.logger.debug(f"raw_request_queue: {self.raw_request_queue.qsize()}")

    async def _next_raw_request(self):
        """获取下一个请求"""
        return await self.raw_request_queue.get()

    async def close(self):
        if self.running:
            self.running = False
            self.logger.info(f"{self.__class__.__name__} has been closed.")

    def process_request(self, request: Request) -> Request:
        if self.request_handlers:
            if isinstance(self.request_handlers, Iterable):
                for handler in self.request_handlers:
                    if isinstance(handler, Callable):
                        request = handler(request)
            elif isinstance(self.request_handlers, Callable):
                request = self.request_handlers(request)

        return self.schedule_request(request)

    def schedule_request(self, request: Request) -> Request:
        if isinstance(request, Request):
            referer = request.referer
            if referer:
                request.priority = referer.priority + 1
                request.headers.setdefault("Referer", request.referer.url)

        return request

    async def start_worker(self):
        while True:
            raw_request = await self._next_raw_request()
            request = self.process_request(raw_request)
            self.enqueue_request(request)
            self.raw_request_queue.task_done()

            # if self.raw_request_queue.empty():
            #     self.logger.debug(f'Empty, exit')
            #     break

    def size(self) -> int:
        return self.request_queue.qsize()

    async def start_scheduler(self):
        self.logger.info(f"Start {self.__class__.__name__}...")
        self.running = True
        try:
            _ = [asyncio.create_task(self.start_worker())]
            await self.raw_request_queue.join()
        except Exception as e:
            raise e
