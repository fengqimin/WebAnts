# 09 代码索引

按模块分组。`位置` 均为 `path:line`，对应 2026-11-05 工作区（ADR-0003 实施后）。`调用方` 只列仓库内已知调用点，`—` 表示无内部调用或仅由外部使用。

## 9.1 `webants/spider.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Signal` | class | `webants/spider.py:26` | 同步事件系统容器 | `Spider.__init__` |
| `Signal.connect` | method | `:44` | 注册同步 handler | `Spider._setup_signals` |
| `Signal.send` | method | `:62` | 同步调用所有 handler，异常吞掉并记日志 | `Spider` 各处 |
| `CircuitBreaker` | class | `:72` | per-domain 三态熔断 | `Spider._get_circuit_breaker` |
| `CircuitBreaker.record_failure` | method | `:111` | 累加失败，达阈值置 open | `Spider.process_request` |
| `CircuitBreaker.record_success` | method | `:118` | 清零并关闭 | `Spider.process_request` |
| `CircuitBreaker.allow_request` | method | `:123` | open 超时转 half-open | `Spider.process_request` |
| `Spider` | class | `:136` | 爬虫基类 | 外部子类 |
| `Spider.__init__` | method | `:143` | 组装组件、统计、信号、关闭状态 | 外部 |
| `Spider._setup_signals` | method | `:235` | 连接五个默认 handler | `__init__` |
| `Spider.start_requests` | async gen | `:287` | 由 `start_urls` 生成 `Request` | `Spider.start` |
| `Spider._coerce_log_level` | static method | `:341` | 字符串/整数日志级别转换，非法值回退 INFO | `__init__` |
| `Spider.parse` | async method | `:423` | 默认 `raise NotImplementedError`；子类可用 async generator 覆盖 | 子类覆盖 |
| `Spider.handle_error` | async method | `:437` | 默认 errback：只记录日志，不计数/不改熔断 | `Request.errback` |
| `Spider._invoke_callback` | async method | `:454` | 调用回调一次并归一化 async generator/coroutine | `process_request` |
| `Spider._consume_callback_value` | async method | `:485` | 归一化协程返回值（None/单项/可迭代/异步可迭代） | `_invoke_callback` |
| `Spider._schedule` | async method | `:512` | 入队并返回 `ScheduleResult`；已关闭时返回 `None` | `start`、`_dispatch_item` |
| `Spider._dispatch_item` | async method | `:539` | Request → `_schedule`，Result → item_scraped + `_persist_item` | `_invoke_callback` |
| `Spider._persist_item` | async method | `:552` | 将 Result 交给 persister；失败只计 `persist_failures` | `_dispatch_item` |
| `Spider._prepare_response` | async method | `:571` | 执行 lookup/process_request/下载/process_response；过滤时返回 None | `process_request` |
| `Spider._apply_middleware_exception` | async method | `:612` | 询问 process_exception，返回 none/retry/response 并限制重试 | `process_request` |
| `Spider.process_request` | async method | `:657` | 单请求完整处理（中间件、下载、回调、持久化、`slot.release()`） | `Spider.start` 的 task、外部 |
| `Spider._on_task_done` | method | `:768` | 清登记、刷新活动时钟、计取消/逃逸异常 | `Task.add_done_callback` |
| `Spider.start` | async method | `:804` | 生命周期主循环；关闭后抛 `RuntimeError` | 外部 |
| `Spider.close` | async method | `:864` | 幂等关闭：取消在飞、排空队列、persister close、关 client、发信号一次 | 外部、`start` 的 finally、`__aexit__` |
| `Spider._close_persister` | async method | `:789` | flush + close 注入的 persister；失败只记录 | `Spider.close` |
| `Spider.__aenter__` | async method | `:918` | 返回 `self`，不自动 start | `async with` |
| `Spider.__aexit__` | async method | `:922` | 调用一次 `close()` | `async with` |
| `Spider.get_stats` | method | `:926` | 聚合四组统计与可选层统计，含 `closed`/`inflight_count` | 外部 |

## 9.2 `webants/scheduler.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Scheduler` | class | `webants/scheduler.py:21` | 优先级调度器 | `Spider.__init__` |
| `Scheduler.__init__` | method | `:24` | 建队列、去重集合、域名状态 | `Spider.__init__` |
| `Scheduler._get_domain` | method | `:72` | 取 netloc 并去端口 | 内部 |
| `Scheduler.schedule_request` | async method | `:265` | 去重、限流、延迟、入队；返回 `ScheduleResult` | `Spider._schedule` |
| `Scheduler.get_request` | async method | `:367` | 非阻塞轮询出队并应用全局延迟；空队列返回 `None` | `Spider.start` |
| `Scheduler.discard_pending` | method | `:392` | 关闭时释放全部排队 slot | `Spider.close` |
| `Scheduler.request_completed` | method | `:415` | 兼容入口：按 URL 释放凭据（已弃用） | 旧调用方 |
| `Scheduler.audit` | method | `:447` | 槽位对账与 `idle` 判定 | `Spider.close` 的验收断言 |
| `Scheduler.get_stats` | method | `:492` | 队列与域名统计 | `Spider.get_stats` |

## 9.3 `webants/downloader.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `BaseDownloader` | class | `webants/downloader.py:22` | 下载器抽象基类 | `Downloader` |
| `BaseDownloader._next_request` | async method | `:53` | 从 `request_queue` 取 `item[1]` | `start_worker` |
| `Downloader` | class | `:84` | httpx 实现 | `Spider.__init__` |
| `Downloader.DEFAULT_RETRY_STATUS_CODES` | class attr | `:93` | 默认可重试状态码集合 | `__init__` |
| `Downloader.__init__` | method | `:100` | 统计、超时、连接池、重试配置、client | `Spider.__init__` |
| `Downloader._setup_timeout` | method | `:174` | float/dict → `httpx.Timeout` | `__init__` |
| `Downloader._setup_limits` | method | `:180` | 连接池默认值合并 | `__init__` |
| `Downloader._setup_headers` | method | `:189` | 只保存 Downloader 级 headers，不注入默认值 | `__init__` |
| `Downloader.effective_headers` | method | `:197` | 合并「默认 < Downloader < Request」请求头 | `_fetch` |
| `Downloader._setup_http_client` | method | `:197` | 创建 `AsyncClient` | `__init__` |
| `Downloader._fetch` | async method | `:209` | 单次 HTTP 尝试；不调用回调，异常不吞 | `fetch` |
| `Downloader._update_stats` | method | `:271` | 响应时间统计（按 `total_attempts`） | `_fetch` |
| `Downloader._sleep` | async method | `:280` | 可注入的等待点 | `fetch` |
| `Downloader._compute_backoff` | method | `:289` | 指数退避 + 抖动 | `fetch` |
| `Downloader._parse_retry_after` | static method | `:302` | 解析秒数/HTTP-date | `fetch` |
| `Downloader._record_final_result` | method | `:326` | 每个逻辑请求只记一次最终结果 | `fetch` |
| `Downloader.fetch` | async method | `:335` | 下载 + 内部重试 + 失败异常；不调用回调 | `Spider.process_request` |
| `Downloader.fetch_retry` | async method | `:431` | 兼容别名，委托给 `fetch` | 旧调用方 |
| `Downloader.start_worker` | async method | `:435` | worker 兼容适配器 | `start_downloader` |
| `Downloader.start_downloader` | async method | `:457` | 启动 N worker，`join` 后只取消自建 worker | 外部 |
| `Downloader.close` | async method | `:475` | 关闭 client（幂等） | `Spider.close` |
| `Downloader.get_stats` | method | `:480` | 统计副本 | `Spider.get_stats` |

## 9.4 `webants/parser.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Parser` | class | `webants/parser.py:27` | HTML 解析器 | `Spider.__init__` |
| `Parser.__init__` | method | `:30` | 配置 `HTMLParser`、实例缓存、统计 | `Spider.__init__` |
| `Parser._get_content_hash` | method | `:76` | SHA-1 内容哈希 | `parse` |
| `Parser._parse_content` | method | `:82` | 无缓存 `fromstring` | `parse` |
| `Parser.parse` | method | `:106` | 解码 + 实例缓存（每次返回深拷贝） | 外部、`extract` |
| `Parser._get_cached_css` | method | `:186` | CSS 表达式缓存 | `css` |
| `Parser._get_cached_xpath` | method | `:195` | XPath 表达式缓存 | `xpath` |
| `Parser.css` | method | `:204` | CSS 查询 | 外部、`extract` |
| `Parser.xpath` | method | `:222` | XPath 查询 | 外部、`extract` |
| `Parser.extract` | method | `:240` | 解析 + 选择一步完成 | 外部 |
| `Parser.get_stats` | method | `:268` | 统计 + `cache_info` 投影 | `Spider.get_stats` |
| `Parser.clear_caches` | method | `:286` | 清空三类缓存 | 测试、外部 |

## 9.5 `webants/libs/request.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `DEFAULT_REQUEST_HEADERS` | constant | `webants/libs/request.py:18` | 默认请求头 | `Downloader.effective_headers` |
| `Request` | class | `:27` | 请求模型（`__slots__`） | 全链路 |
| `Request.METHODS` | class attr | `:30` | 允许的 HTTP 方法 | `_validate_method` |
| `Request.__init__` | method | `:55` | 校验、默认值、回调与 `cb_kwargs` 校验 | 全链路 |
| `Request._validate_method` | method | `:138` | 方法白名单 | `__init__` |
| `Request._validate_url` | method | `:150` | 校验 scheme + netloc | `__init__` |
| `Request._validate_callback` | static method | `:167` | callback 接受协程/async generator；errback 只接受协程 | `__init__` |
| `Request._validate_cb_kwargs` | static method | `:204` | 校验 `cb_kwargs` 为 dict、键为 str、禁止保留键 `response` | `__init__` |
| `Request.__lt__` | method | `:229` | 反转优先级比较 | `PriorityQueue` 同优先级比较 |
| `Request.__eq__` | method | `:233` | 按 fingerprint 相等 | 去重/集合 |
| `Request.__hash__` | method | `:239` | fingerprint 哈希 | 集合 |
| `Request.fingerprint` | method | `:243` | method + 归一化 URL + body | `__eq__`、`__hash__` |
| `Request.copy` | method | `:281` | 复制并覆盖属性 | 外部 |
| `Request.replace` | method | `:317` | 原地替换属性 | 外部 |
| `Request.prepare` | async method | `:332` | 执行请求级中间件 | 无内部调用 |
| `Request.add_middleware` | method | `:345` | 追加中间件 | 外部 |

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
| `_Item` | class | `:77` | 持有 `html` 的基类；setter 校验 `str`/`_Element`/`None` | 四个 Item |
| `AttrItem` | class | `:90` | 属性提取 | 外部 |
| `LinkItem` | class | `:106` | 链接提取 + 过滤 | 外部、示例 |
| `ElementItem` | class | `:155` | 元素提取 | 外部 |
| `TextItem` | class | `:187` | 文本提取 | 外部、测试 |
| `ItemDescriptor` | descriptor | `:210` | 类属性式提取 | 四个 Descriptor |
| `AttrItemDescriptor` | descriptor | `:237` | 属性字段声明 | 外部 |
| `LinkItemDescriptor` | descriptor | `:251` | 链接字段声明 | `example/use_item.py` |
| `ElementItemDescriptor` | descriptor | `:297` | 元素字段声明 | 外部 |
| `TextItemDescriptor` | descriptor | `:311` | 文本字段声明 | `example/use_item.py` |

## 9.8 `webants/libs/extractor.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Link` | class | `webants/libs/extractor.py:39` | URL + unique 标记 | 链接提取 |
| `iter_elements` | function | `:59` | 按 tags/attr 迭代元素 | `find_elements` |
| `find_elements` | function | `:90` | CSS/XPath/tags 统一查找 | 多个 extract 函数 |
| `extract_attrib` | function | `:139` | 批量取属性 | 外部 |
| `extract_links` | function | `:173` | 批量取 `Link` | `extract_and_filter_links` |
| `_LinkFilter` | class | `:239` | 共享过滤核心（输入恒为 URL 字符串） | 模块级函数、`FilteringLinkExtractor` |
| `extract_and_filter_links` | function | `:354` | 模块级链接过滤（新增 `lenient`） | 外部 |
| `extract_text` | function | `:360` | 批量取文本 | 外部 |
| `get_base_url` | function | `:402` | 取 `<base href>` | 外部 |
| `get_html_title` | function | `:409` | 取 `<title>` | 外部 |
| `ExtractorFactory` | class | `:416` | 注册表 + 工厂 | `item.py`、测试 |
| `ExtractorFactory.create_extractor` | classmethod | `:435` | 按前缀创建类 | `ElementItem`、`TextItem` |
| `_ExtractorMeta` | metaclass | `:444` | 自动注册提取器类 | 所有提取器 |
| `BaseExtractor` | class | `:462` | 提取器基类 | 所有提取器 |
| `_LxmlElementExtractor` | class | `:485` | lxml 查找 + 提取模板 | `AttribExtractor` 等 |
| `AttribExtractor` | class | `:617` | 取单个属性 | `AttrItem` |
| `LinkExtractor` | class | `:705` | 取 `Link` | `FilteringLinkExtractor` |
| `ElementExtractor` | class | `:708` | 返回元素 | `ElementItem` |
| `TextExtractor` | class | `:720` | 返回文本 | `TextItem` |
| `FilteringLinkExtractor` | class | `:805` | 链接过滤（委托 `_LinkFilter`） | `LinkItem` |
| `RegexExtractor` | class | `:952` | 空实现（`pass`） | 无 |

## 9.9 `webants/libs/middleware.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Middleware` | ABC | `webants/libs/middleware.py:29` | 四阶段接口：`lookup` + `process_request/response/exception` | 四个实现 |
| `Middleware.lookup` | async method | `:31` | 返回缓存响应或 None；默认实现返回 None | `MiddlewareManager.lookup` |
| `CircuitBreaker` | class | `:91` | 中间件版熔断 | 可选中间件 |
| `RateLimiter` | class | `:185` | 域名限速 | 可选中间件 |
| `RequestDeduplicator` | class | `:310` | 请求去重 | 可选中间件 |
| `ResponseCache` | class | `:369` | 响应缓存；命中走 `lookup`，TTL 滑动 | `Spider` 经 Manager |
| `MiddlewareManager` | class | `:474` | 中间件链；新增 `lookup`/`get_stats` 与请求类型守卫 | `Spider(middleware=...)` |

## 9.10 `webants/libs/persister.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `BasePersister` | ABC | `webants/libs/persister.py:26` | 存储接口 + 锁 + `flush/close` + 统计 | 三个后端 |
| `BasePersister.flush` | async method | `:110` | 强制写出当前批次，返回条数 | `PersisterManager.flush`、`close` |
| `BasePersister.close` | async method | `:125` | 幂等 flush + 标记关闭；关闭后写入抛 `PersistenceError` | `Spider._close_persister` |
| `JsonPersister` | class | `:171` | NDJSON 追加存储；旧数组可读并在下次写入原子迁移 | `PersisterManager` |
| `CsvPersister` | class | `:411` | CSV 存储 | `PersisterManager` |
| `SqlitePersister` | class | `:535` | SQLite 存储 | `PersisterManager` |
| `PersisterManager` | class | `:692` | 多后端管理、`flush/close` 与同步 | `Spider(persister=...)` |

## 9.11 `webants/libs/signals.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `Signal` | class | `webants/libs/signals.py:25` | 异步信号 | 标准信号、`SignalManager` |
| `Signal.send` | async method | `:46` | 按 `(-priority, seq)` 调用接收器 | `SignalManager.send` |
| `Signal.connect` | method | `:87` | 注册协程接收器并分配自增序号 | `SignalManager.connect` |
| `Signal._get_receivers` | method | `:130` | 合并具体 sender 与全局接收器（`None` 只取一次） | `send` |
| `SignalManager` | class | `:167` | 信号注册表 | `signal_manager` |
| `signal_manager` | instance | `:260` | 全局单例 | `middleware.py:25` |
| 标准信号 | constants | `:263-273` | `spider_opened` 等 11 个 | 注册循环 `:276-277` |

## 9.12 `webants/libs/config.py`

| 符号 | 类型 | 位置 | 用途 | 调用方 |
|------|------|------|------|--------|
| `DEFAULT_CONFIG` | constant | `webants/libs/config.py:18` | 全部默认配置 | `Config` |
| `Config` | class | `:78` | 配置管理 | 外部（未接入） |
| `Config.load_config` | method | `:102` | 读取 YAML | `__init__` |
| `Config._load_env_vars` | method | `:123` | 读取 `WEBANTS_*`（`__` 分层） | `__init__` |
| `Config._set_nested_value` | method | `:146` | 点分 key 写值 + 环境变量类型守卫 | `set`、`_load_env_vars` |
| `Config._coerce_scalar` | method | `:195` | 环境变量标量转换 | `_set_nested_value` |
| `Config._types_match` | method | `:211` | 环境变量类型兼容判断 | `_set_nested_value` |
| `Config._deep_merge` | method | `:221` | 递归深合并到当前层级 | `_merge_config` |
| `Config._merge_config` | method | `:229` | 深合并入口 | `load_config` |
| `Config._validate_config` | method | `:237` | 类型/范围校验与路径绝对化 | `__init__`、`set` |
| `Config.get` | method | `:281` | 点分 key 读取 | 外部 |
| `Config.set` | method | `:300` | 写值并校验 | 外部 |
| `Config.as_dict` | method | `:310` | 深拷贝配置 | 外部 |
| `Config.save` | method | `:318` | 写 YAML | 外部 |
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
| `PerformanceMonitor` | class | `:94` | psutil 采样线程（需 `webants[monitoring]`；缺依赖时构造抛 `RuntimeError`） | `StatsCollector` |
| `StatsCollector` | class | `:205` | 爬虫统计聚合 | 未被 Spider 使用 |
| `async_login_with_playwright` | async function | `webants/utils/login.py:17` | Playwright 异步登录 | 未被使用 |
| `sync_login_with_playwright` | function | `:76` | Playwright 同步登录 | 未被使用 |
| `load_cookies` | function | `:172` | 读取 cookies.json | 未被使用 |

## 9.15 测试与示例

| 文件 | 用例数 | 覆盖对象 | 备注 |
|------|--------|----------|------|
| `tests/test_downloader.py` | 14（含参数化） | `Downloader.fetch` 重试/退避/`Retry-After`/body/cookies/统计、worker 取消边界 | 全部使用 `httpx.MockTransport`，不访问网络 |
| `tests/test_request.py` | 9 | 回调双类型、errback、`cb_kwargs` 校验 | 覆盖 C1 契约 |
| `tests/test_spider.py` | 24 | 主链路、异步生成器/协程回调、errback、下载失败、关闭/取消/`idle_grace` | 覆盖 C3/C6 与 ADR-0003 E1–E5，全部离线 |
| `tests/test_config.py` | 10 | 深合并、默认值隔离、`__` 环境变量、类型守卫、`as_dict` 深拷贝 | ADR-0004 |
| `tests/test_extractor.py` | 30（含参数化） | 函数式提取、四类 Extractor、工厂、Link 变体、normalize/lenient 矩阵 | ADR-0004 |
| `tests/test_item.py` | 6 | 四类 Item + 四类 Descriptor；`html` 类型校验 | 非法类型抛 `TypeError` |
| `tests/test_parser.py` | 22（含参数化） | 解析、选择器、缓存命中/miss、返回副本、容量淘汰、错误 | 覆盖编码/超限/空内容 |
| `tests/test_signals.py` | 7 | 同优先级顺序、优先级方向、`sender=None`、禁用/断开 | ADR-0004 |
| `tests/test_result.py` | 13 | `Field` / `Result` | 覆盖校验与序列化 |
| `tests/test_scheduler.py` | 20 | 初始化、去重、上限、并发、完成、统计、延迟、slot 对账 | 域名并发用 `asyncio.wait` |
| `tests/conftest.py` | — | `http_client`、`sample_html` fixture | 将仓库根加入 `sys.path` |
| `example/use_item.py` | — | `LinkItemDescriptor` / `TextItemDescriptor` | import 即执行 |

阶段 5 完成后合计 243 个测试用例（`uv run pytest -q` 报告 `243 passed`，2026-11-06）；上表为阶段 4 时的文件级拆分，新增用例分布在 `test_request.py`、`test_downloader.py`、`test_extractor.py`、`test_persister.py`、`test_result.py`、`test_spider.py`、`test_url.py`、`test_statistic.py`、`test_login_tool.py`、`test_package.py`。
