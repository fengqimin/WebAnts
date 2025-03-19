"""

update：
    2022-11-06, 调用Parser组件实现解析
"""
import asyncio
import time
from abc import abstractmethod
from pathlib import Path
from urllib.parse import urlparse, unquote_plus

from webants.downloader import Downloader, BaseDownloader
from webants.libs import Request, Result, InvalidParser, Response
from webants.parser import Parser
from webants.scheduler import Scheduler
from webants.utils import get_logger, args_to_list


class Spider:
    """Spider base class

    基础类，所有的爬虫都必须继承这个类.
    """

    start_urls: list[str] | str = []
    name: str = "Spider"

    def __init__(
        self,
        start_urls: list[str] | str = None,
        *,
        config_file: Path | str | None = None,
        concurrency: int = 10,
        run_forever: bool = False,
        **kwargs,
    ):
        # 爬虫配置文件
        self.config_file = config_file
        # 初始化队列
        # 原生请求队列,用于初始化请求以及Parser组件与Scheduler组件间的通信
        self.raw_request_queue = asyncio.Queue()
        # 请求队列,用于Scheduler组件与Downloader组件间的通信
        self.request_queue = asyncio.PriorityQueue()
        # 响应队列，用于Downloader组件与Parser组件间的通信
        self.response_queue = asyncio.Queue()
        # 结果队列，用于Parser组件与Spider组件间的通信
        self.result_queue = asyncio.Queue()
        self.concurrency = concurrency
        self.kwargs = kwargs or {}
        # 初始化日志记录器
        self.logger = get_logger(
            self.__class__.__name__, log_level=kwargs.get("spider_log_level", 20)
        )

        # 初始化事件循环
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self._close_loop = True
        self.logger.debug(self.loop)
        self.work_dir = self.kwargs.pop("work_dir", Path.cwd())

        # 初始化调度器(Scheduler)实例
        self.scheduler = self._init_scheduler(**self.kwargs)
        # 初始化下载器(Downloader)实例
        downloader_cls = self.kwargs.get("downloader", Downloader)
        assert issubclass(downloader_cls, BaseDownloader)
        self.downloader = self._init_downloader(downloader_cls, **self.kwargs)
        # 初始化解析器(Parser)实例
        self.parser = self._init_parser(**self.kwargs)

        self._run_forever = run_forever

        # self.__dict__.update(kwargs)
        if start_urls:
            self.start_urls = args_to_list(start_urls)
        else:
            assert self.__class__.start_urls, f"start_urls not found or empty."
            self.start_urls = args_to_list(self.__class__.start_urls)
        self.unique = self.kwargs.get("unique", True)

        # 所有队列为空状态的计数
        self.all_queue_empty_count = 0
        # 所有队列为空时的等待间隔时间
        self.wait_delay = self.kwargs.get("wait_delay", 2)
        # 所有队列为空时的等待次数
        self.wait_times = self.kwargs.get("wait_times", 10)

        self.running = False
        self._exit = False
        self.start_time = time.time_ns()

    def _enqueue_raw_request(self, request: Request):
        self.logger.debug(f"RAW: {request}")
        # request = self.process_request(request)
        self.raw_request_queue.put_nowait(request)

    def _init_downloader(self, download_cls: type(BaseDownloader), **kwargs):
        return download_cls(
            request_queue=self.request_queue,
            response_queue=self.response_queue,
            log_level=self.kwargs.get("downloader_log_level", 20),
            concurrency=self.concurrency,
            loop=self.loop,
            **kwargs,
        )

    def _init_parser(self, **kwargs) -> Parser:
        try:
            parser_cls = kwargs.get("parser", Parser.__subclasses__()[0])
        except IndexError:
            raise InvalidParser("Expected class<Parser>, got None.")

        self.logger.debug(parser_cls)
        if parser_cls and issubclass(parser_cls, Parser):
            return parser_cls(
                response_queue=self.response_queue,
                raw_request_queue=self.raw_request_queue,
                result_queue=self.result_queue,
                spider=self.__class__.__name__,
                log_level=kwargs.get("parser_log_level", 20),
                work_dir=self.work_dir,
            )
        else:
            raise InvalidParser("Expected class<Parser>, got None.")

    def _init_scheduler(self, **kwargs):
        s = Scheduler(
            raw_request_queue=self.raw_request_queue,
            request_queue=self.request_queue,
            log_level=self.kwargs.get("scheduler_log_level", 20),
            **kwargs,
        )
        # seen_urls_json_file = kwargs.get('seen_urls', 'seen_urls.pickle')
        # p = Path(seen_urls_json_file)
        # if p.is_file():
        #     with p.open('rb') as fp:
        #         s.seen_urls = pickle.load(fp)
        return s

    async def _next_result(self) -> Result:
        if self.result_queue:
            return await self.result_queue.get()

    def _read_config(self):
        pass

    def _start_request(self):
        for url in self.start_urls:
            scheme = urlparse(url).scheme
            assert scheme in (
                "http",
                "https",
                "file",
            ), f"Expected URL scheme ('http', 'https', 'file'), got {scheme}. "

            if scheme in ("http", "https"):
                self._enqueue_raw_request(Request(url, unique=self.unique))
            else:
                # file 协议
                url = unquote_plus(url)
                path = Path(url.removeprefix("file:///"))

                if path.is_file():
                    self._enqueue_raw_request(Request(url=url, unique=self.unique))
                elif path.is_dir():
                    # 遍历本地文件,包装成file URL后,放入request_queue队列中
                    for p in path.iterdir():
                        if p.is_file():
                            self.logger.debug(f"{p}")
                            request = Request(
                                url=unquote_plus(p.as_uri()), unique=self.unique
                            )
                            self._enqueue_raw_request(request)
                else:
                    raise FileNotFoundError(f"File Not Found: {str(path)}")

    def all_queue_empty(self):
        return (
            self.response_queue.empty()
            and self.request_queue.empty()
            and self.raw_request_queue.empty()
            and self.result_queue.empty()
        )

    async def close(self):
        if self.running:
            # 取消所有非当前运行的 Task 实例
            tasks = []
            for task in asyncio.all_tasks():
                if task is not asyncio.current_task():
                    tasks.append(task)
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            # 关闭各个组件
            tasks = [
                asyncio.create_task(self.scheduler.close()),
                asyncio.create_task(self.parser.close()),
                asyncio.create_task(self.downloader.close()),
            ]
            await asyncio.gather(*tasks)

            # p = Path('seen_urls.pickle')
            # with p.open('wb') as fp:
            #     pickle.dump(self.scheduler.seen_urls, fp)

            self.running = False

    @abstractmethod
    def process_result(self, result: Result) -> Result:
        """

        :param result:
        :return:
        """
        pass

    async def run_forever(self, many: int = 0):
        """
        以永久运行模式启动爬虫。

        此方法会持续运行爬虫，直到收到退出信号。它会初始化请求，启动所有必要的任务，并在退出时关闭爬虫。

        Args:
            many (int): 下载器一次下载的最大数量，默认为 0。
        """
        # 初始化请求队列，将起始 URL 封装成请求对象放入原生请求队列
        self._start_request()

        # 标记爬虫为运行状态
        self.running = True

        # 记录日志，表明爬虫开始运行
        self.logger.info(f"Start {self.__class__.__name__}...")

        try:
            # 进入无限循环，持续运行爬虫
            while True:
                # 检查是否收到退出信号
                if self._exit:
                    # 记录退出日志
                    self.logger.debug("EXIT")
                    # 关闭爬虫，取消所有任务并关闭各个组件
                    await self.close()
                    # 跳出循环，结束运行
                    break

                # 创建并启动多个异步任务
                tasks = [
                    # 启动调度器任务，负责管理请求队列
                    asyncio.create_task(self.scheduler.start_scheduler()),
                    # 启动下载器任务，负责下载页面内容
                    asyncio.create_task(self.downloader.start_downloader(many)),
                    # 启动爬虫主任务，处理结果队列
                    asyncio.create_task(self.start_spider()),
                    # 启动解析器任务，解析下载的页面内容
                    asyncio.create_task(self.parser.start_parser()),
                ]

                # 等待所有任务完成
                await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            # 捕获用户手动中断信号（Ctrl+C）
            print("KeyboardInterrupt")
            # 关闭爬虫，取消所有任务并关闭各个组件
            await self.close()

    async def run(self, many: int = 0):
        self._start_request()

        self.running = True

        self.logger.info(f"Start {self.__class__.__name__}...")
        self.logger.debug(f"{self.loop}")

        tasks = [
            asyncio.create_task(self.status_monitoring(), name="monitor"),
            asyncio.create_task(self.scheduler.start_scheduler(), name="Scheduler"),
            asyncio.create_task(
                self.downloader.start_downloader(many), name="Downloader"
            ),
            asyncio.create_task(self.start_spider(), name="Spider"),
            asyncio.create_task(self.parser.start_parser(), name="Parser"),
        ]

        await asyncio.gather(*tasks)

    @property
    def seen_count(self):
        return self.scheduler.seen_urls.__len__()

    def start(self, many: int = 0):
        """启动爬虫

        :return:
        """

        try:
            self.loop.run_until_complete(self.run(many))
        except KeyboardInterrupt:
            self.logger.exception("KeyboardInterrupt, closing...")
            self.loop.run_until_complete(self.close())
        except AssertionError:
            self.logger.exception("AssertionError, closing...")
            self.loop.run_until_complete(self.close())

    async def start_spider(self):
        await self.start_worker()
        await self.result_queue.join()

    async def start_worker(self):
        """读取结果队列（Result Queue），并处理结果（Result）

        :return:
        """
        while True:
            result = await self._next_result()
            self.process_result(result)

            self.result_queue.task_done()

    async def status_monitoring(self):
        def _check_task_status():
            all_tasks = asyncio.all_tasks()
            self.logger.debug(f"Task: {all_tasks}")
            self.logger.info(f"Task count: {len(all_tasks)}")

        while True:
            _check_task_status()

            delta_time = time.time_ns() - self.start_time
            if delta_time > 0:
                self.logger.info(
                    f"并发数量: {self.concurrency}, "
                    f"解析数量: {Request.count:5}, "
                    f"爬取数量: {Response.count:5}, "
                    f"爬取时间： {delta_time / 1000000000:.2f},"
                    f"爬取速度（URLs/s）: {(Response.count * 1000000000 / delta_time):.2f}"
                )

            self.logger.debug(
                f"raw_request_queue: {self.raw_request_queue.qsize()},"
                f"request_queue: {self.request_queue.qsize()},"
                f"response_queue: {self.response_queue.qsize()},"
                f"result_queue: {self.result_queue.qsize()}"
            )

            if self.all_queue_empty():
                self.logger.info(f"All queue is empty, wait<{self.wait_delay}>s")
                self.all_queue_empty_count += 1
                await asyncio.sleep(self.wait_delay)
                # 超过最大等待次数，raise KeyboardInterrupt错误，以关闭爬虫
                if self.all_queue_empty_count > self.wait_times:
                    raise KeyboardInterrupt

            await asyncio.sleep(self.wait_delay)
