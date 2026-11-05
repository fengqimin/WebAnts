# 03 爬虫主链路

本章按一次爬取的顺序拆解 `Spider → Scheduler → Downloader → 解析/提取 → 回灌`。行号对应 2026-11-05 工作区（ADR-0005 实施后）。

## 3.1 常量与默认值

| 名称 | 值 | 位置 |
|------|----|------|
| `DEFAULT_REQUEST_HEADERS` | Accept、Accept-Language `en`、Accept-Encoding `gzip, deflate`、Connection `keep-alive`、固定 Firefox UA；由 `Downloader.effective_headers()` 在每次请求时合并（`Request.headers` 只存显式头） | `webants/libs/request.py`、`webants/downloader.py` |
| `Downloader.DEFAULT_RETRY_STATUS_CODES` | 408/425/429/500/502/503/504（可由 `retry_status_codes` 覆盖） | `webants/downloader.py:93,163-171` |
| 重试退避 | `base=1s`、`factor=2`、`max=60s`、`jitter=25%`；429/503 参考 `Retry-After` | `webants/downloader.py:163-166,289-324` |
| HTTP 超时 | `httpx.Timeout(30)`（Spider 默认；`Config` 可覆盖） | `webants/spider.py:161-320`；`webants/downloader.py:144,170-175` |
| 连接池 | `max_keepalive_connections=concurrency`、`max_connections=concurrency*2`、`keepalive_expiry=60` | `webants/downloader.py:180-187` |
| 域名并发 | `max_domain_concurrent=10`（不传 `Config` 时） | `webants/scheduler.py:30`；`webants/spider.py:220-245` |
| 调度队列容量 | 10000 | `webants/scheduler.py:32,47` |
| 解析缓存 | 实例级 `OrderedDict(maxsize=1000)`，每次返回深拷贝 | `webants/parser.py:43-56,154-185` |
| 文档大小上限 | 10 MB | `webants/parser.py:33,51` |
| 默认 UA（Downloader） | Firefox 116 字符串 | `webants/downloader.py:189-195` |

## 3.2 阶段 1：构造与初始请求

1. `Spider.__init__` 解析生效参数：显式构造参数 > `Config` > Spider 内置默认值；不传 `Config` 时保持旧默认（10/30/3/1.0/INFO/5/60，`domain_delay=0`）。映射覆盖 Downloader、Scheduler、Parser 与熔断参数（`webants/spider.py:161-320`）。
2. 实例化三个核心组件：`Scheduler`、`Downloader`、`Parser`；三者之间**没有注入关系**，各自持有独立状态。
3. 可选层按显式注入保存：`self.middleware = middleware`、`self.persister = persister`、`self.config = config`；默认均为 `None`（`webants/spider.py:214-218`）。
4. 初始化 `stats`（含 `filtered_requests`/`filtered_responses`/`middleware_retries`/`cache_hits`/`persist_failures`）、`_inflight`（task → Request）、`_domain_circuits`、`idle_grace`/`_last_activity`/`_closed`/`_close_lock`，并注册五个默认同步信号处理器（`webants/spider.py:275-340`）。
5. `start_requests()` 遍历 `start_urls`，为每个 URL 构造 `Request(callback=self.parse, errback=self.handle_error, retries=..., timeout=...)`（`webants/spider.py:408-430`）。
6. `Request.__init__` 校验 method/URL；callback 接受 `iscoroutinefunction` **或** `isasyncgenfunction`，errback 只接受 coroutine function；不满足时抛 `TypeError`（`webants/libs/request.py:99-122,167-222`）。

> 回调契约由 [ADR-0001](../adr/0001-callback-contract-and-retry-ownership.md) 冻结：规范形态是 async generator，普通协程作为兼容形态由 Spider 归一化。`assert` 已替换为 `TypeError`，避免 `python -O` 下失效。

## 3.3 阶段 2：入队与去重

`Scheduler.schedule_request` 的完整顺序（`webants/scheduler.py:265-359`）：

1. `max_requests` 检查：达到上限返回 `ScheduleResult(LIMIT_REACHED)`。
2. 去重：未设置 `dont_filter` 且 URL 已在 `seen_urls`/`_reserved_urls` 中，返回 `DUPLICATED`。
3. 首次见到的域名初始化 `domain_stats` 与 `asyncio.Semaphore(max_domain_concurrent)`。
4. **在任何挂起点之前**把 URL 加入 `_reserved_urls`，避免并发调度同一 URL 同时穿透。
5. `await semaphore.acquire()`（唯一背压点），随后 `pending_permits += 1`。
6. 计算自适应延迟（EMA + 抖动）并 `await asyncio.sleep(effective_delay)`。
7. `put_nowait((priority, seq, request, slot))`；队列满时返回 `QUEUE_FULL`，由 `finally` 归还信号量。
8. 入队成功后在同步代码段内写 `_outstanding`、`seen_urls`、`total_scheduled` 与域名计数，返回 `ScheduleResult(SCHEDULED, slot)`。

关键事实：

- **所有权凭据是 `ScheduleSlot`**，不是 URL；同一 URL 的两个在飞请求各自持有槽位，互不覆盖（`ScheduleSlot.release()` 幂等）。
- 去重键是原始 `request.url`，不是 `Request.fingerprint()`（`:289-294`）。
- 取消、队列满或异常路径由 `finally` 统一回滚未交付的 permit；`audit()` 用 `active + pending == 持有的许可` 做恒等式检查。
- 适配器更新见 [ADR-0002](../adr/0002-schedule-slot-ownership-and-request-lifecycle.md)；关闭时的 `discard_pending()` 见 ADR-0003。

## 3.4 阶段 3：主循环与下载

`Spider.start`（`webants/spider.py:524-582`）：

1. 若 `_closed` 为真抛 `RuntimeError`（关闭是终态）；`_running` 为真则直接返回。
2. 设置 `start_time`、`stats["start_time"]`、`_last_activity = time.monotonic()`，发 `spider_started`。
3. 把所有初始请求 `await _schedule(...)`。
4. `while self._running:` → `await scheduler.get_request()`。返回 `None` 时，仅当 **`_inflight` 为空 ∧ `time.monotonic() - _last_activity >= idle_grace`** 时 `break`，否则继续轮询（`default idle_grace=0.5`，传 `0` 退化为立即退出）。
5. 取到 slot：刷新 `_last_activity`，`asyncio.create_task(process_request(request, slot=slot))`；在让出控制权前登记 `self._inflight[task] = request` 并挂 `_on_task_done`（`:563-577`）。
6. 正常退出或异常统一走 `finally: await close()`。

> ADR-0003 冻结了收敛契约：时间窗而非空转次数，避免轮询粒度改变退出语义；外部异步生产者可在 `idle_grace` 窗口内继续入队。异步信号桥接尚未启用（BL-001），因此当前退出条件不包含事件分发任务。P1-02 已关闭。

任务结束回调 `_on_task_done`（同步函数，`:503-522`）：

1. 从 `_inflight` 移除该 task，并刷新 `_last_activity`。
2. `task.cancelled()` → `cancelled_requests+1`，直接返回（取消不是失败）。
3. 否则 `task.exception()` **消费异常**（避免 GC 时 “Task exception was never retrieved”）；非空时 `unhandled_task_errors+1` 并带 traceback 记 error。`process_request` 已捕获 `Exception`，此处只会看到逃逸的 `BaseException`（如自定义 `BaseException`、`MemoryError`）。

`process_request`（`webants/spider.py:657-820`）：

1. 计时、`total_requests+1`；若当前 task 未被主循环登记（直接调用场景），在此登记 `_inflight[current_task]`。
2. 取域名熔断器；`allow_request()` 为假时记录日志并 `return`。判断位于 `try` 内，`finally` 仍会释放资源。
3. **可选中间件阶段**（`middleware` 为 `None` 时整段跳过）：
   - `lookup(request)` 命中：`cache_hits+1`，跳过下载；
   - 未命中：`process_request(request)`；返回 `None` → `filtered_requests+1`，释放槽位，不调 errback；
   - 返回新 `Request` 时校验域名未变，后续下载与回调都使用新请求。
4. `response = await self.downloader.fetch(request)`。
5. 可选 `process_response(response, request)`：返回 `None` → `filtered_responses+1`，跳过回调；返回新响应则继续。
6. **只调用一次** `_invoke_callback(request, response)`（`:511-558`）：
   - async generator 回调：`async for` 消费；
   - 普通协程回调：`await` 后按 `None` / 单个 `Request|Result` / 同步可迭代 / 异步可迭代归一化；
   - 其他返回类型：抛 `TypeError`，走 errback。
7. 分发（`:579-607`）：`Request` → `_schedule`；`Result` → 同步 `item_scraped` + 可选 `persister.save(item)`；其他类型 → `TypeError`。
8. 成功：`circuit.record_success()`、`successful_requests+1`、发 `request_finished(success=True)`。
9. `Exception`：先询问可选 `process_exception(exception, request)`：
   - 返回 `Request` → 计入 `middleware_retries`，通过 `_schedule` 重新入队（受 `middleware_max_retries` 限制）；
   - 返回响应 → 直接交给回调，按成功处理；
   - 返回 `None` / 未注入 → 走原失败路径：`circuit.record_failure()`、`failed_requests+1`、errback 一次、发 `request_finished(success=False)`。
10. `finally`：仅当本方法自行登记时 pop `_inflight`；`slot.release()` 幂等归还调度资源；更新平均耗时。

> 可选层的四阶段、缓存命中与持久化契约由 [ADR-0005](../adr/0005-optional-layer-wiring-and-persistence-lifecycle.md) 冻结；实现测试见 `tests/test_spider_optional.py`。

### 3.4.1 关闭与终态

`Spider.close`（`webants/spider.py:864-924`）是幂等、可在未 `start()` 时调用的终态操作：

1. 置 `_running = False`，主循环不再创建新任务；`finish_time` 仅在未设置时写入。
2. 快照 `list(self._inflight)`，逐个 `cancel()`，`await asyncio.gather(*snapshot, return_exceptions=True)`；被取消任务的 `finally` 归还 slot，`_on_task_done` 清登记并计 `cancelled_requests`。
3. `scheduler.discard_pending()`：排队请求逐条 `slot.release()`，累加 `discarded_on_close`。
4. `persister.close()`（若注入）：先 flush 尾批再关闭；单个后端失败只记录日志，不阻断后续关闭。
5. `await downloader.close()`（`aclose()` 幂等）。
6. `spider_stopped` **恰好一次**；随后置 `_closed = True`。

不变量（可用 `get_stats()` 与 `scheduler.audit()` 断言）：`_inflight` 为空、`request_queue.qsize() == 0`、`audit()["idle"]` 为真且 `mismatch_count == 0`、`downloader.client.is_closed` 为真、`closed is True`、注入的 persister 已关闭；关闭后 `_schedule()` 返回 `None`，`start()` 抛 `RuntimeError`。`async with Spider(...)` 只保证退出时 close，不会自动 start。契约见 [ADR-0003](../adr/0003-shutdown-cancellation-and-loop-quiescence.md) 与 [ADR-0005](../adr/0005-optional-layer-wiring-and-persistence-lifecycle.md)。

`Downloader.fetch`（`webants/downloader.py:335-432`）：

1. `max_attempts = max(1, retries + 1)`；`total_requests+1`；首次请求前只等待一次 `request.delay or self.delay`（`:354-361`）。
2. 循环尝试：第 `n` 次重试前计算指数退避，并按 429/503 的 `Retry-After` 取较大值（`:363-378`）；每次尝试 `total_attempts+1`。
3. `_fetch` 只做单次 HTTP：透传 method/url/合并后的 headers（默认 < Downloader < Request）、timeout、body（dict → `json`，其余 → `content`）、cookies（合并 client jar 后显式 `Cookie` 头）、referer；**不调用 callback**，异常不吞。
4. 状态码命中 `retry_status_codes`：未耗尽则继续；耗尽抛 `RequestError(code="RETRY_EXHAUSTED")`（`:395-414`）。
5. 传输异常：未耗尽则继续；耗尽抛 `RequestError(code="FETCH_FAILED")`（`:381-394`）。
6. 非可重试响应（含 2xx/3xx/4xx）：`successful_requests+1` 并返回 `httpx.Response`（`:416-417`）。

> ADR-0001 已解决：P0-01（回调重复调用/类型冲突）、P1-03（fetch 不重试）、P1-13（body/cookies 未透传）、P1-14（600 伪响应）、P1-19（重试投递到私有队列）。重试不再修改 `request.retries`/`request.delay`，也不再跨队列。

## 3.5 阶段 4：重试（Downloader 内部）

重试归属由 [ADR-0001](../adr/0001-callback-contract-and-retry-ownership.md) 冻结：**`Downloader.fetch` 内部完成，Scheduler 与 Spider 不重试**。`fetch_retry` 仅保留为兼容别名，行为与 `fetch` 相同。

- 总尝试次数：`max(1, request.retries + 1)`；`request.retries` 表示首次尝试之外的额外次数，重试过程中**不修改**该字段（`webants/downloader.py:354`）。
- 可重试状态码：408、425、429、500、502、503、504；403/404/420 不再重试（`:93,168-171`）。
- 退避：`delay = min(max_delay, base_delay * factor ** retry_index)`，再叠加最多 25% 抖动（`:289-300`）。
- `Retry-After`：429/503 时取“计算退避”与响应头的较大值；支持秒数与 HTTP-date（`:302-324`）。
- 最终结果：
  - 收到非可重试响应 → 返回 `httpx.Response`；
  - 传输异常耗尽 → `RequestError(code="FETCH_FAILED")`；
  - 可重试状态耗尽 → `RequestError(code="RETRY_EXHAUSTED")`，metadata 含 `status_code/headers/url/attempts`（`:381-414`）。
- 重试期间占用当前域名并发槽位；这是 v1 的显式取舍，未来若需“退避让出槽位”，应引入延迟队列而不是把重试散落到 Spider。
- worker 模式（`start_worker`）只是兼容适配器：调用 `fetch`，成功响应入 `response_queue`，失败记录日志；Spider 不走该路径（`:435-455`）。`start_downloader` 在 `try/finally` 中只取消并 `gather` 自己创建的 worker，不再触碰事件循环里的其他任务（`:457-472`，P1-20 已关闭）。

> P1-03、P1-19、P2-01 均已随本阶段关闭。重试不再使用 `max_retries - retries` 作为指数，也不再向 Downloader 私有队列投递。

## 3.6 阶段 5：解析与提取

`Parser.parse`（`webants/parser.py:106-184`）：

1. `total_parses+1`。
2. 若是 `bytes`：按 `encoding or self.encoding` 解码；`UnicodeDecodeError` 时用 `charset_normalizer.detect`，置信度 > 0.8 才采用，否则抛 `ParserError`（`:125-152`）。
3. 检查大小上限（`:154-159`）。
4. 计算 SHA-1 内容哈希，查询实例级 `OrderedDict`（`:161-175`）：命中则 `move_to_end`、`cache_hits+1`、返回深拷贝；未命中则解析、`cache_misses+1`、写入缓存并按 `cache_size` 淘汰，返回深拷贝（`:176-184`）。
5. `_parse_content` 执行 `fromstring`，失败包装为 `ParserError`（`:82-104`）。

缓存与统计（[ADR-0004](../adr/0004-data-correctness-link-filter-config-cache-signal.md)）：

- 缓存归 Parser 实例所有，key 仅内容哈希；不再用 `lru_cache` 修饰实例方法，实例可被回收，跨实例不共享树。
- 首次解析计入 `cache_misses`，只有真正命中才累加 `cache_hits`；`get_stats()["cache_info"]` 保留 `hits/misses/maxsize/currsize` 形状（`:268-284`）。
- `parse` 每次返回深拷贝，调用方修改返回树不会污染缓存。
- `css`/`xpath` 只在复用已编译对象时计 `*_cache_hits`，首次编译计 `*_cache_misses`（`:186-203`）。

提取层（可选，需调用方显式使用）：

1. `Item` 实例持有 `html`，字段描述符在 `__get__` 时调用 `extractor.extract(instance.html)`（`webants/libs/item.py:52-67`）。
2. `ExtractorFactory.create_extractor` 按类名前缀查找（`webants/libs/extractor.py:430-441`），类由元类自动注册（`:444-459`）。
3. `_LxmlElementExtractor.extract` 对 `str` 调用 `etree.HTML`，再按 selector → xpath → tags 顺序找元素（`:574-614`）。
4. `FilteringLinkExtractor` 先用 `LinkExtractor` 取 `Link` 列表，交给共享 `_LinkFilter` 判定（输入恒为 URL 字符串），再按 `normalize` 重建输出（`webants/libs/extractor.py:239-350,881-911`）。模块级 `extract_and_filter_links` 使用同一核心（`:354-410`）。

## 3.7 阶段 6：结果与中间产物

`Result` 的 `fields` 是 `Dict[str, Field]`，`Field` 记录 `value/source/extractor/timestamp/metadata`（`webants/libs/result.py:11-33`）。`to_dict()` 输出：

```json
{
  "spider": "my_spider",
  "fields": {
    "title": {
      "value": "Example",
      "source": "https://example.com/",
      "extractor": "my_spider.parse",
      "timestamp": 1762160000.0,
      "metadata": {}
    }
  },
  "status": 200,
  "url": "https://example.com/",
  "mediatype": null,
  "title": null,
  "crawl_time": 1762160000.0,
  "errors": [],
  "warnings": []
}
```

结构见 `webants/libs/result.py`。`crawl_time=None`（默认）取当前时间；显式传入 `0` 会保留（ADR-0006，P2-11 Closed）。

运行期中间产物：

| 产物 | 位置 | 生命周期 |
|------|------|----------|
| `seen_urls` | `Scheduler` 实例 | 进程内，无上限 |
| `domain_stats` / `domain_semaphores` | `Scheduler` 实例 | 每域名一份，不清理 |
| `_inflight` | `Spider` 实例 | task → Request；运行中非空，`close()` 后为空 |
| `_last_activity` / `idle_grace` / `_closed` | `Spider` 实例 | 收敛时钟、静默窗口与终态标志 |
| `_outstanding` / `_reserved_urls` | `Scheduler` 实例 | 未归还的 slot 与正在调度中的 URL；`close()` 后应为空 |
| `retry_requests` / `total_retries` / `total_attempts` | `Downloader.stats` | 进程内计数，不再保存 URL 集合 |
| `_cache` | Parser 实例缓存 | 最多 `cache_size` 条（默认 1000），key 仅内容哈希；返回深拷贝 |
| `_css_cache` / `_xpath_cache` | Parser 实例 dict | 无上限 |
| `response_queue` | Downloader 实例 | 默认创建但 Spider 不消费（仅 worker 模式） |

## 3.8 失败模式

| 场景 | 实际行为 | 证据 |
|------|----------|------|
| 队列为空但仍有处理中任务 | `get_request()` 轮询返回 `None`，`_inflight` 非空，主循环继续等待 | `webants/spider.py:549-561`、`webants/scheduler.py:367-390` |
| 队列为空且无处理中任务 | 静默 `idle_grace` 后 `break`；`idle_grace=0` 时立即退出 | `webants/spider.py:551-561` |
| 主循环运行中调用 `close()` | 停止建新任务、取消在飞、排空队列、flush/close persister、关 client；`start()` 的 `finally` 再次 close 为空操作 | `webants/spider.py:864-924` |
| 关闭后再 `start()` | 抛 `RuntimeError("Spider has been closed...")` | `webants/spider.py:531-532` |
| 回调是 async generator | 正常执行；`Request` 构造期接受 | `webants/libs/request.py:167-202`、`webants/spider.py:317-346` |
| 回调是普通协程 | 正常执行；返回值由 Spider 归一化 | `webants/spider.py:348-373` |
| 下载失败（HTTPError/可重试状态耗尽） | `fetch` 抛 `RequestError`，`process_request` 走 errback；不再构造 600 | `webants/downloader.py:381-414` |
| 请求未入队就调用 `process_request` | 不接触调度资源，`slot=None`，`finally` 无需释放；`_inflight` 自行登记/清理 | `webants/spider.py:657-820` |
| 队列满 | `put_nowait` 抛 `QueueFull`，返回 `QUEUE_FULL`；`finally` 归还 permit | `webants/scheduler.py:321-331` |
| 中间件过滤请求/响应 | 分别计 `filtered_requests`/`filtered_responses`，不调 errback，槽位照常释放 | `webants/spider.py:571-655` |
| 缓存命中 | `lookup` 返回响应，跳过下载；计 `cache_hits`，仍走 `process_response` 与回调 | `webants/spider.py:584-590` |
| 持久化失败 | 计 `persist_failures`，请求仍按成功处理，不回滚已抓取结果 | `webants/spider.py:552-568` |
| 文档为空字符串 | `fromstring` 抛错并包装为 `ParserError` | `webants/parser.py:82-104` |
| 链接过滤 `normalize=False` | 正常返回；判定统一用 canonical URL，`normalize` 只决定输出是否重建为归一化 URL（ADR-0004） | `webants/libs/extractor.py:239-350,881-911` |
| 站点返回 403/404 | 直接返回 Response，不重试；是否视为失败由 callback 决定 | `webants/downloader.py:93,416-417` |

## 3.9 入口与“CLI”

仓库没有命令行入口、没有打包 `console_scripts`。可直接运行的示例有两个：

- `example/use_item.py`：在模块级构造 HTML、定义 `P` 类、实例化并 `print`（`example/use_item.py:403-418`），因此 `import` 该模块也会产生输出；只演示 `LinkItemDescriptor` / `TextItemDescriptor`，不涉及网络。
- `example/optional_layers.py`：ADR-0005 的组装示例，`PYTHONPATH=. uv run python example/optional_layers.py` 可在离线 `MockTransport` 下运行，演示 `Config + RequestDeduplicator + ResponseCache + JsonPersister`，关闭时自动 flush。

## 3.10 可选层接线（ADR-0005）

| 层 | 注入方式 | 生效点 | 关闭语义 |
|----|----------|--------|----------|
| `Config` | `Spider(config=Config(...))` | 构造 Scheduler/Downloader/Parser 与熔断参数；显式构造参数优先 | 无资源 |
| `MiddlewareManager` | `Spider(middleware=mm)` | `lookup` → `process_request` → 下载 → `process_response`；失败时 `process_exception` | 无资源 |
| `PersisterManager` | `Spider(persister=pm)` | 每个 `Result` 分发后 `save`；失败只计 `persist_failures` | `close()` 时 flush + close，所有权移交 Spider |
| 异步 `SignalManager` | 不自动接入 | 仅可选层内部发送（如 `request_dropped`） | 实验性，桥接待办 BL-001 |

不变量：

- 不注入任何可选层时，`Spider()` 的构造参数、统计键与旧版一致。
- 中间件 `lookup` 先于 `process_request`；命中时跳过下载，但不会跳过响应链与回调。
- `process_request` 返回新请求时域名必须不变；跨域改写应作为新请求产出。
- `persister` 关闭后 `save` 抛 `PersistenceError(PERSISTER_CLOSED)`；Spider 关闭时自动 flush 尾批。
- 同步 `Spider.signals.connect` 拒绝协程函数；异步处理器使用 `webants.libs.signals.signal_manager`。

契约与验收见 [ADR-0005](../adr/0005-optional-layer-wiring-and-persistence-lifecycle.md)；测试见 `tests/test_spider_optional.py`、`tests/test_middleware.py`、`tests/test_persister.py`。
