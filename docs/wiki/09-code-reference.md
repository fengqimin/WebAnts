# 09 代码索引

按模块分组。`位置` 均为 `path:line`，对应 `master @ 23f8051`。`调用方` 只列仓库内已知调用点，`—` 表示无内部调用或仅由外部使用。

## 9.1 `webants/spider.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Signal` | class | `webants/spider.py:24` | 同步事件系统容器 | `Spider.__init__` |
| `Signal.connect` | method | `:42` | 注册同步 handler | `Spider._setup_signals` |
| `Signal.send` | method | `:60` | 同步调用所有 handler，异常吞掉并记日志 | `Spider.process_request` |
| `CircuitBreaker` | class | `:70` | per-domain 三态熔断 | `Spider._get_circuit_breaker` |
| `CircuitBreaker.record_failure` | method | `:109` | 累加失败，达阈值置 open | `Spider.handle_error` |
| `CircuitBreaker.record_success` | method | `:116` | 清零并关闭 | `Spider.process_request` |
| `CircuitBreaker.allow_request` | method | `:121` | open 超时转 half-open | `Spider.process_request`、`retry_failed` |
| `Spider` | class | `:134` | 爬虫基类 | 外部子类 |
| `Spider.__init__` | method | `:141` | 组装组件、统计、信号 | 外部 |
| `Spider._setup_signals` | method | `:218` | 连接五个默认 handler | `__init__` |
| `Spider.start_requests` | async gen | `:270` | 由 `start_urls` 生成 `Request` | `Spider.start` |
| `Spider.parse` | async method | `:285` | 默认 `raise NotImplementedError` | 子类覆盖 |
| `Spider.handle_error` | async method | `:299` | 计失败、记 URL、熔断 | `Request.errback` |
| `Spider.retry_failed` | async method | `:322` | 重新调度失败 URL | `Spider.start` |
| `Spider.process_request` | async method | `:349` | 单请求完整处理 | `Spider.start` 的 task |
| `Spider.start` | async method | `:397` | 生命周期主循环 | 外部 |
| `Spider.close` | async method | `:432` | 关闭 downloader、发信号 | `Spider.start` 的 finally |
| `Spider.get_stats` | method | `:450` | 聚合四组统计 | 外部 |

## 9.2 `webants/scheduler.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Scheduler` | class | `webants/scheduler.py:21` | 优先级调度器 | `Spider.__init__` |
| `Scheduler.__init__` | method | `:24` | 建队列、去重集合、域名状态 | `Spider.__init__` |
| `Scheduler._get_domain` | method | `:72` | 取 netloc 并去端口 | 内部 |
| `Scheduler.schedule_request` | async method | `:78` | 去重、限流、延迟、入队 | `Spider.start`、`process_request`、`retry_failed` |
| `Scheduler.get_request` | async method | `:170` | 出队并应用全局延迟 | `Spider.start` |
| `Scheduler.request_completed` | method | `:183` | 释放域名信号量、减计数 | `Spider.process_request` |
| `Scheduler.get_stats` | method | `:198` | 队列与域名统计 | `Spider.get_stats` |

## 9.3 `webants/downloader.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `BaseDownloader` | class | `webants/downloader.py:22` | 下载器抽象基类 | `Downloader` |
| `BaseDownloader._next_request` | async method | `:53` | 从 `request_queue` 取 `item[1]` | `start_worker` |
| `Downloader` | class | `:84` | httpx 实现 | `Spider.__init__` |
| `Downloader.RETRY_CODES` | class attr | `:88` | 状态码 → 重试策略 | `_fetch`、`fetch_retry` |
| `Downloader.__init__` | method | `:100` | 统计、超时、连接池、client | `Spider.__init__` |
| `Downloader._setup_timeout` | method | `:170` | float/dict → `httpx.Timeout` | `__init__` |
| `Downloader._setup_limits` | method | `:176` | 连接池默认值合并 | `__init__` |
| `Downloader._setup_headers` | method | `:185` | 注入默认 UA | `__init__` |
| `Downloader._setup_http_client` | method | `:193` | 创建 `AsyncClient` | `__init__` |
| `Downloader._fetch` | async method | `:205` | 单次请求；异常返回 `Request` | `fetch`、`fetch_retry` |
| `Downloader._update_stats` | method | `:263` | 响应时间统计 | `_fetch` |
| `Downloader._retry_request` | method | `:274` | 减 retries、提 priority、回队 | `fetch_retry` |
| `Downloader.fetch_retry` | async method | `:289` | 状态码/异常重试 | `start_worker`、测试 |
| `Downloader.fetch` | async method | `:361` | 单次请求 + 回调 | `Spider.process_request`、测试 |
| `Downloader.start_worker` | async method | `:380` | 永久消费队列 | `start_downloader` |
| `Downloader.start_downloader` | async method | `:394` | 启动 N worker 并 join | 外部 |
| `Downloader.close` | async method | `:411` | 关闭 client | `Spider.close` |
| `Downloader.get_stats` | method | `:416` | 统计副本 | `Spider.get_stats` |

## 9.4 `webants/parser.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Parser` | class | `webants/parser.py:26` | HTML 解析器 | `Spider.__init__` |
| `Parser.__init__` | method | `:29` | 配置 `HTMLParser`、缓存、统计 | `Spider.__init__` |
| `Parser._get_content_hash` | method | `:68` | SHA-1 内容哈希 | `parse` |
| `Parser._cached_parse` | method（lru_cache） | `:74` | 带缓存的 `fromstring` | `parse` |
| `Parser.parse` | method | `:107` | 解码 + 缓存解析 | 外部、`extract` |
| `Parser._get_cached_css` | method | `:166` | CSS 表达式缓存 | `css` |
| `Parser._get_cached_xpath` | method | `:172` | XPath 表达式缓存 | `xpath` |
| `Parser.css` | method | `:178` | CSS 查询 | 外部、`extract` |
| `Parser.xpath` | method | `:197` | XPath 查询 | 外部、`extract` |
| `Parser.extract` | method | `:216` | 解析 + 选择一步完成 | 外部 |
| `Parser.get_stats` | method | `:244` | 统计 + `cache_info` | `Spider.get_stats` |
| `Parser.clear_caches` | method | `:257` | 清空三类缓存 | 测试、外部 |

## 9.5 `webants/libs/request.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `DEFAULT_REQUEST_HEADERS` | constant | `webants/libs/request.py:18` | 默认请求头 | `Request.__init__` |
| `Request` | class | `:27` | 请求模型（`__slots__`） | 全链路 |
| `Request.METHODS` | class attr | `:30` | 允许的 HTTP 方法 | `_validate_method` |
| `Request.__init__` | method | `:55` | 校验、默认值、回调断言 | 全链路 |
| `Request._validate_method` | method | `:143` | 方法白名单 | `__init__` |
| `Request._validate_url` | method | `:155` | 校验 scheme + netloc | `__init__` |
| `Request.__lt__` | method | `:175` | 反转优先级比较 | `PriorityQueue` |
| `Request.__eq__` | method | `:179` | 按 fingerprint 相等 | 去重/集合 |
| `Request.fingerprint` | method | `:189` | method + 归一化 URL + body | `__eq__`、`__hash__` |
| `Request.copy` | method | `:227` | 复制并覆盖属性 | 外部 |
| `Request.replace` | method | `:263` | 原地替换属性 | 外部 |
| `Request.prepare` | async method | `:278` | 执行请求级中间件 | 无内部调用 |
| `Request.add_middleware` | method | `:291` | 追加中间件 | 外部 |

## 9.6 `webants/libs/result.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Field` | pydantic model | `webants/libs/result.py:11` | 单字段 + 元数据 | `Result` |
| `Result` | class | `:49` | 抓取结果容器 | 回调、Persister |
| `Result.__init__` | method | `:68` | 原始值转 `Field` | 外部 |
| `Result.add_field` | method | `:117` | 新增字段 | 外部 |
| `Result.get_field` | method | `:136` | 取 `Field` | 外部 |
| `Result.get_value` | method | `:147` | 取原始值 | 外部 |
| `Result.is_valid` | method | `:159` | 是否有校验错误 | 外部 |
| `Result.to_dict` | method | `:183` | 序列化为 dict | Persister、外部 |
| `Result.save` | method | `:203` | 空实现 | 无 |

## 9.7 `webants/libs/item.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `ExtractorDescriptor` | descriptor | `webants/libs/item.py:36` | 字段访问时执行提取 | `_Item.field` |
| `_Item` | class | `:77` | 持有 `html` 的基类 | 四个 Item |
| `AttrItem` | class | `:90` | 属性提取 | 外部 |
| `LinkItem` | class | `:106` | 链接提取 + 过滤 | 外部、示例 |
| `ElementItem` | class | `:155` | 元素提取 | 外部 |
| `TextItem` | class | `:171` | 文本提取 | 外部、测试 |
| `ItemDescriptor` | descriptor | `:194` | 类属性式提取 | 四个 Descriptor |
| `AttrItemDescriptor` | descriptor | `:221` | 属性字段声明 | 外部 |
| `LinkItemDescriptor` | descriptor | `:235` | 链接字段声明 | `example/use_item.py` |
| `ElementItemDescriptor` | descriptor | `:281` | 元素字段声明 | 外部 |
| `TextItemDescriptor` | descriptor | `:295` | 文本字段声明 | `example/use_item.py` |

## 9.8 `webants/libs/extractor.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Link` | class | `webants/libs/extractor.py:39` | URL + unique 标记 | 链接提取 |
| `iter_elements` | function | `:59` | 按 tags/attr 迭代元素 | `find_elements` |
| `find_elements` | function | `:90` | CSS/XPath/tags 统一查找 | 多个 extract 函数 |
| `extract_attrib` | function | `:139` | 批量取属性 | 外部 |
| `extract_links` | function | `:174` | 批量取 `Link` | `extract_and_filter_links` |
| `extract_and_filter_links` | function | `:224` | 模块级链接过滤 | 外部 |
| `extract_text` | function | `:360` | 批量取文本 | 外部 |
| `get_base_url` | function | `:402` | 取 `<base href>` | 外部 |
| `get_html_title` | function | `:409` | 取 `<title>` | 外部 |
| `ExtractorFactory` | class | `:416` | 注册表 + 工厂 | `item.py`、测试 |
| `ExtractorFactory.create_extractor` | classmethod | `:435` | 按前缀创建类 | `ElementItem`、`TextItem` |
| `_ExtractorMeta` | metaclass | `:444` | 自动注册提取器类 | 所有提取器 |
| `BaseExtractor` | class | `:462` | 提取器基类 | 所有提取器 |
| `_LxmlElementExtractor` | class | `:485` | lxml 查找 + 提取模板 | `AttribExtractor` 等 |
| `AttribExtractor` | class | `:617` | 取单个属性 | `AttrItem` |
| `LinkExtractor` | class | `:659` | 取 `Link` | `FilteringLinkExtractor` |
| `ElementExtractor` | class | `:708` | 返回元素 | `ElementItem` |
| `TextExtractor` | class | `:720` | 返回文本 | `TextItem` |
| `FilteringLinkExtractor` | class | `:759` | 链接过滤 | `LinkItem` |
| `RegexExtractor` | class | `:952` | 空实现（`pass`） | 无 |

## 9.9 `webants/libs/middleware.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Middleware` | ABC | `webants/libs/middleware.py:29` | 三阶段接口 | 四个实现 |
| `CircuitBreaker` | class | `:87` | 中间件版熔断 | 无内部调用 |
| `RateLimiter` | class | `:181` | 域名限速 | 无内部调用 |
| `RequestDeduplicator` | class | `:306` | 请求去重 | 无内部调用 |
| `ResponseCache` | class | `:365` | 响应缓存 | 无内部调用 |
| `MiddlewareManager` | class | `:454` | 中间件链 | 无内部调用 |

## 9.10 `webants/libs/persister.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `BasePersister` | ABC | `webants/libs/persister.py:22` | 存储接口 + 统计 | 三个后端 |
| `JsonPersister` | class | `:94` | JSON 数组存储 | 无内部调用 |
| `CsvPersister` | class | `:205` | CSV 存储 | 无内部调用 |
| `SqlitePersister` | class | `:324` | SQLite 存储 | 无内部调用 |
| `PersisterManager` | class | `:476` | 多后端管理与同步 | 无内部调用 |

## 9.11 `webants/libs/signals.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Signal` | class | `webants/libs/signals.py:27` | 异步信号 | 标准信号、`SignalManager` |
| `Signal.send` | async method | `:46` | 按优先级调用接收器 | `SignalManager.send` |
| `Signal.connect` | method | `:87` | 注册协程接收器 | `SignalManager.connect` |
| `SignalManager` | class | `:165` | 信号注册表 | `signal_manager` |
| `signal_manager` | instance | `:260` | 全局单例 | `middleware.py:25` |
| 标准信号 | constants | `:263-273` | `spider_opened` 等 11 个 | 注册循环 `:276-277` |

## 9.12 `webants/libs/config.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `DEFAULT_CONFIG` | constant | `webants/libs/config.py:18` | 全部默认配置 | `Config` |
| `Config` | class | `:77` | 配置管理 | 外部（未接入） |
| `Config.load_config` | method | `:101` | 读取 YAML | `__init__` |
| `Config._load_env_vars` | method | `:122` | 读取 `WEBANTS_*` | `__init__` |
| `Config._set_nested_value` | method | `:129` | 点分 key 写值 + 类型转换 | `set`、`_load_env_vars` |
| `Config._merge_config` | method | `:157` | 递归合并（有缺陷） | `load_config` |
| `Config._validate_config` | method | `:173` | 范围校验与路径绝对化 | `__init__`、`set` |
| `Config.get` | method | `:200` | 点分 key 读取 | 外部 |
| `Config.set` | method | `:219` | 写值并校验 | 外部 |
| `Config.as_dict` | method | `:229` | 浅拷贝配置 | 外部 |
| `Config.save` | method | `:237` | 写 YAML | 外部 |
| `config` | instance | `:260` | 全局单例 | 无内部调用 |

## 9.13 `webants/libs/exceptions.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `WebAntsException` | class | `webants/libs/exceptions.py:41` | 异常根类（code/retryable/metadata） | 全部异常 |
| `RequestError` | class | `:76` | 请求异常基类 | 请求相关 |
| `InvalidRequestMethod` | class | `:82` | 非法 method | `Request._validate_method` |
| `InvalidURL` | class | `:88` | 非法 URL | 未被使用 |
| `RequestTimeout` | class | `:94` | 超时 | 未被使用 |
| `TooManyRedirects` | class | `:113` | 重定向过多 | 未被使用 |
| `ConnectionError` | class | `:134` | 连接错误 | 未被使用 |
| `ParserError` | class | `:162` | 解析异常基类 | `Parser` |
| `InvalidExtractor` | class | `:174` | 提取器名不存在 | `ExtractorFactory` |
| `ExtractorError` | class | `:180` | 提取失败 | 未被使用 |
| `SpiderError` | class | `:208` | Spider 异常基类 | 未被使用 |
| `MiddlewareError` | class | `:233` | 中间件异常基类 | 未被使用 |
| `CircuitBreakerError` | class | `:239` | 熔断打开 | `middleware.CircuitBreaker` |
| `StorageError` | class | `:267` | 存储异常基类 | 未被使用 |
| `RateLimitError` | class | `:286` | 限速 | `middleware.RateLimiter` |
| `ExceptionTracker` | class | `:348` | 异常计数与速率 | `exception_tracker` |
| `exception_tracker` | instance | `:416` | 全局追踪器 | `middleware.py`、`signals.py` |

## 9.14 `webants/utils/`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `get_logger` | function | `webants/utils/logger.py:10` | 创建/复用 logger | 全仓库 |
| `lenient_host` | function | `webants/utils/url.py:47` | 宽松 host 归一化 | `extractor.py` |
| `normalize_url` | function | `:67` | 去默认端口/认证/片段，排序 query | `request.py`、`extractor.py` |
| `url_fingerprint` | function | `:159` | method + URL 哈希 | 未被使用 |
| `url_fp_to_filename` | function | `:201` | 哈希作为文件名 | 未被使用 |
| `url_to_path` | function | `:240` | URL 转路径 | 未被使用 |
| `url_with_host` | function | `:256` | 替换 host | 内部 |
| `valid_path` | function | `webants/utils/misc.py:19` | 文件名非法字符替换 | 未被使用 |
| `args_to_list` | function | `:23` | 单值转列表 | `extractor.py` |
| `copy_object` | function | `:32` | 反射复制对象 | 未被使用 |
| `TimeSeriesMetric` | class | `webants/utils/statistic.py:21` | 滑动窗口指标 | `StatsCollector` |
| `PerformanceMonitor` | class | `:94` | psutil 采样线程 | `StatsCollector` |
| `StatsCollector` | class | `:205` | 爬虫统计聚合 | 未被 Spider 使用 |
| `async_login_with_playwright` | async function | `webants/utils/login.py:17` | Playwright 异步登录 | 未被使用 |
| `sync_login_with_playwright` | function | `:76` | Playwright 同步登录 | 未被使用 |
| `load_cookies` | function | `:172` | 读取 cookies.json | 未被使用 |

## 9.15 测试与示例

| 文件 | 用例数 | 覆盖对象 | 备注 |
|------|--------|----------|------|
| `tests/test_downloader.py` | 10（含参数化） | `Downloader.fetch` / `fetch_retry` / 统计 / 信号量 | `test_normal_request` 访问 httpbin |
| `tests/test_extractor.py` | 20（含参数化） | 函数式提取、四类 Extractor、工厂、Link 变体 | 覆盖 `FilteringLinkExtractor` 默认 normalize |
| `tests/test_item.py` | 6 | 四类 Item + 四类 Descriptor | `test_invalid_html` 失败 |
| `tests/test_parser.py` | 18（含参数化） | 解析、选择器、缓存、错误 | 覆盖编码/超限/空内容 |
| `tests/test_result.py` | 13 | `Field` / `Result` | 覆盖校验与序列化 |
| `tests/test_scheduler.py` | 9 | 初始化、去重、上限、并发、完成、统计、延迟 | 域名并发用 `asyncio.wait` |
| `tests/conftest.py` | — | `http_client`、`sample_html` fixture | 将仓库根加入 `sys.path` |
| `example/use_item.py` | — | `LinkItemDescriptor` / `TextItemDescriptor` | import 即执行 |

合计 76 个测试用例（`uv run pytest -q` 报告 75 passed + 1 failed）。
