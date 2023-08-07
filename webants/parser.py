"""Parser

`Parser`从`response_queue`队列中获取`response`，进行解析，根据解析的结果按照类型放入相应的队列中：
   - `Link`类型， 封装为`Request`后， 放入`raw_request_queue`队列；
   - 其他类型， 封装为`Result`后， 放入`result_queue`队列（如果存在）


"""
import asyncio
import time
from typing import Iterable

from webants.libs import Response, Link, Request, Result, Field
from webants.libs.item import Item, ItemDescriptor
from webants.utils import get_logger


class ParserMeta(type):
    def __new__(mcs, name: str, bases: tuple, attrs: dict):
        __items__ = {}
        for attr, attr_value in attrs.items():
            # 将自定义的Item类存储到类属性中
            if isinstance(attr_value, (Item, ItemDescriptor)):
                __items__[attr] = attr_value
        attrs["__items__"] = __items__
        return type.__new__(mcs, name, bases, attrs)


class Parser(metaclass=ParserMeta):
    """Parser class

    对html页面中的元素进行解析，并获取所需的Item
    """

    def __init__(
        self,
        *,
        response_queue: asyncio.Queue = None,
        raw_request_queue: asyncio.Queue = None,
        result_queue=None,
        spider: str = "",
        log_level: int = 20,
        **kwargs,
    ):
        """ """
        # 响应队列，用于与Downloader组件进行通信
        self.response_queue = response_queue
        # 原生请求队列,用于与Scheduler组件进行通信
        self.raw_request_queue = raw_request_queue
        # 结果队列,用于与Spider组件进行通信
        self.result_queue = result_queue

        self.html = ""
        self.logger = get_logger(log_name=self.__class__.__name__, log_level=log_level)
        self.spider = spider
        self.parsed_response_count: int = 0
        self.running = False
        self.kwargs = kwargs

        self.work_dir = self.kwargs.get("work_dir", None)

    async def _next_response(self):
        """获取下一个请求"""
        if self.response_queue:
            self.parsed_response_count += 1
            return await self.response_queue.get()

    def _enqueue_raw_request(self, request: Request):
        if self.raw_request_queue:
            self.raw_request_queue.put_nowait(request)

    def _enqueue_result(self, result: Result):
        if self.result_queue:
            self.result_queue.put_nowait(result)

    def _parse(
        self,
        resp_or_str: Response | str,
    ) -> Iterable[tuple[str, list]]:
        """

        :param resp_or_str:
        :return:
        """
        return self.parse(resp_or_str)

    def parse(
        self,
        resp_or_str: Response | str,
    ) -> Iterable[tuple[str, list]]:
        """

        :param resp_or_str:
        :return:
        """
        if isinstance(resp_or_str, Response):
            self.logger.debug(f"parse {resp_or_str} {resp_or_str.mediatype}")
            self.html = resp_or_str.text
        else:
            self.html = resp_or_str

        items: dict = getattr(self, "__items__", None)
        assert items is not None and isinstance(items, dict), f"请定义item"
        for item_name, item_ins in items.items():
            if isinstance(item_ins, Item):
                item_ins.html = self.html
                # yield 属性名称和提取结果
                yield item_name, item_ins.field
            elif isinstance(item_ins, ItemDescriptor):
                setattr(self, item_name, self.html)
                yield item_name, getattr(self, item_name)

    def process_response(self, response: Response) -> Response:
        """处理响应

            TODO： 这里预留，后续考虑加入中间件支持
        :param response:
        :return:
        """
        return response

    async def start_worker(self):
        while True:
            response = self.process_response(await self._next_response())

            if response.text is None:
                item_name = response.request.cb_kwargs.get("field")
                field = Field()
                field[item_name] = response.body
                result = Result(
                    spider=self.spider,
                    field=field,
                    url=response.url,
                    mediatype=response.mediatype,
                    title=response.request.cb_kwargs.get("title"),
                    crawl_time=time.time(),
                )
                self.logger.debug(f"{item_name}: {result}")
                self._enqueue_result(result)
            else:
                for item_name, items in self._parse(response):
                    #
                    if not items:
                        continue

                    for item in items:
                        if isinstance(item, Link):
                            self.logger.debug(f"{item_name:>5}: {item}")
                            request = Request(
                                item.url,
                                referer=response.request,
                                cb_kwargs={"field": item_name, "title": response.title},
                            )
                            self._enqueue_raw_request(request)
                        else:
                            field = Field()
                            field[item_name] = item
                            result = Result(
                                spider=self.spider,
                                field=field,
                                url=response.url,
                                mediatype=response.mediatype,
                                title=response.title,
                                crawl_time=time.time(),
                            )
                            self.logger.debug(f"{item_name:>5}: {result}")
                            self._enqueue_result(result)

            self.response_queue.task_done()

    async def start_parser(self):
        self.logger.info(f"Start {self.__class__.__name__}...")
        self.running = True
        try:
            _ = [asyncio.create_task(self.start_worker())]
            await self.response_queue.join()
        except Exception as e:
            raise e

    async def close(self):
        if self.running and issubclass(self.__class__, Parser):
            self.logger.info(f"{self.__class__.__name__} has been closed.")
            self.running = False
