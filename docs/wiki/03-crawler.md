# 03 爬虫主链路

本章按一次爬取的顺序拆解 `Spider → Scheduler → Downloader → 解析/提取 → 回灌`。行号对应 2026-11-04 工作区（ADR-0001 实施后），基线逻辑仍来自 `master @ 23f8051`。

## 3.1 常量与默认值

| 名称 | 值 | 位置 |
|------|----|------|
| `DEFAULT_REQUEST_HEADERS` | Accept、Accept-Language `en`、Accept-Encoding `gzip, deflate`、Connection `keep-alive`、固定 Firefox UA | `webants/libs/request.py:18-24` |
| `Downloader.DEFAULT_RETRY_STATUS_CODES` | 408/425/429/500/502/503/504（可由 `retry_status_codes` 覆盖） | `webants/downloader.py:93,163-171` |
| 重试退避 | `base=1s`、`factor=2`、`max=60s`、`jitter=25%`；429/503 参考 `Retry-After` | `webants/downloader.py:163-166,289-324` |
| HTTP 超时 | `httpx.Timeout(30)`（Spider 默认） | `webants/spider.py:143,175`；`webants/downloader.py:144,170-175` |
| 连接池 | `max_keepalive_connections=concurrency`、`max_connections=concurrency*2`、`keepalive_expiry=60` | `webants/downloader.py:180-187` |
| 域名并发 | `max_domain_concurrent=10` | `webants/scheduler.py:30`；`webants/spider.py:170-178` |
| 调度队列容量 | 10000 | `webants/scheduler.py:32,47` |
| 解析缓存 | `lru_cache(maxsize=1000)` | `webants/parser.py:23,74-76` |
| 文档大小上限 | 10 MB | `webants/parser.py:33,51` |
| 默认 UA（Downloader） | Firefox 116 字符串 | `webants/downloader.py:189-195` |

## 3.2 阶段 1：构造与初始请求

1. `Spider.__init__` 实例化三个组件：`Scheduler(max_domain_concurrent=concurrent_requests)`、`Downloader(concurrency=..., timeout=...)`、`Parser()`（`webants/spider.py:170-178`）。三者之间**没有注入关系**，各自持有独立状态。
2. 初始化 `stats`、`_processing`、`_domain_circuits`，并注册五个默认信号处理器（`webants/spider.py:197-225`）。
3. `start_requests()` 遍历 `start_urls`，为每个 URL 构造 `Request(callback=self.parse, errback=self.handle_error, retries=..., timeout=...)`（`webants/spider.py:271-284`）。
4. `Request.__init__` 校验 method/URL；callback 接受 `iscoroutinefunction` **或** `isasyncgenfunction`，errback 只接受 coroutine function；不满足时抛 `TypeError`（`webants/libs/request.py:99-122,167-222`）。

> 回调契约由 [ADR-0001](../adr/0001-callback-contract-and-retry-ownership.md) 冻结：规范形态是 async generator，普通协程作为兼容形态由 Spider 归一化。`assert` 已替换为 `TypeError`，避免 `python -O` 下失效。

## 3.3 阶段 2：入队与去重

`Scheduler.schedule_request` 的完整顺序（`webants/scheduler.py:78-168`）：

1. `max_requests` 检查：`total_scheduled >= max_requests` 时返回 `False`（`:91-93`）。
2. 去重：`request.url in seen_urls and not request.dont_filter` → `total_filtered+1`，返回 `False`（`:95-97`）。
3. 首次见到的域名初始化 `domain_stats` 与 `asyncio.Semaphore(max_domain_concurrent)`（`:99-115`）。
4. `await semaphore.acquire()`（`:118`）。
5. 计算自适应延迟：用 EMA（α=0.2）平滑“距上次请求的间隔”，再乘 `0.8 + 0.4*random()` 抖动（`:122-149`）。
6. `await asyncio.sleep(effective_delay)`（`:150`）。
7. 更新 `seen_urls`、`total_scheduled`、域名计数，`PriorityQueue.put((priority, request))`（`:152-163`）。

关键事实：

- **去重键是原始 `request.url`**，不是 `Request.fingerprint()`（`:95,153`）。`docs/architecture.md:226` 写的是“基于 method+url+body 的 fingerprint”，与代码不一致。
- 去重发生在 `sleep` **之前**判断、`sleep` **之后**写入；两个协程同时调度同一 URL 时都可能通过检查（`:95` 与 `:153` 之间没有原子性）。
- 信号量在 `put` 之前获取；若队列满导致 `put` 阻塞，信号量长期不释放；若 `put` 或其他步骤抛异常，`except` 只记录日志，不 `release`（`:117-118,163,166-168`）。
- 异常路径返回值是 `False`，但此前可能已经 `acquire` 过信号量，也可能已经增加了统计。

## 3.4 阶段 3：主循环与下载

`Spider.start`（`webants/spider.py:448-490`）：

1. 幂等检查 `_running`，设置 `start_time`，发 `spider_started`。
2. 把所有初始请求 `await schedule_request(...)`。
3. `while self._running:` → `await scheduler.get_request()`；队列空且无处理中请求时 `break`，否则 `sleep(0.1)` 后重试（`:462-470`）。
4. 取到请求后 `asyncio.create_task(process_request(request))`（`:472`）。

> 主循环仍没有超时等待，任务也未被跟踪（P1-02，属于第 2 阶段）。Spider 级重试已删除，`_failed_urls`/`retry_failed` 不再存在。

`process_request`（`webants/spider.py:387-446`）：

1. 计时、`total_requests+1`、`_processing.add(url)`（`:393-395`）。
2. 取域名熔断器；`allow_request()` 为假时记录日志并 `return`。判断位于 `try` 内，`finally` 仍会释放资源（`:400-404`）。
3. 发 `request_scheduled` 信号（`:406`）。
4. `response = await self.downloader.fetch(request)`（`:407`）。
5. **只调用一次** `_invoke_callback(request, response)`（`:317-346`）：
   - async generator 回调：`async for` 消费；
   - 普通协程回调：`await` 后按 `None` / 单个 `Request|Result` / 同步可迭代 / 异步可迭代归一化（`:348-373`）；
   - 其他返回类型：抛 `TypeError`，走 errback。
6. 分发（`:375-385`）：`Request` → `Scheduler.schedule_request`；`Result` → `item_scraped`；其他类型 → `TypeError`。
7. 成功：`circuit.record_success()`、`successful_requests+1`、发 `request_finished(success=True)`（`:413-416`）。
8. 异常：`circuit.record_failure()`（每个逻辑请求只记一次）、`failed_requests+1`、调用 errback 一次；errback 自身异常只记录日志（`:418-432`）。
9. `finally`：`_processing.discard(url)`；`scheduler.request_completed(request)` 的 `KeyError` 被捕获并告警；更新平均耗时（`:433-446`）。

`Downloader.fetch`（`webants/downloader.py:335-432`）：

1. `max_attempts = max(1, retries + 1)`；`total_requests+1`；首次请求前只等待一次 `request.delay or self.delay`（`:354-361`）。
2. 循环尝试：第 `n` 次重试前计算指数退避，并按 429/503 的 `Retry-After` 取较大值（`:363-378`）；每次尝试 `total_attempts+1`。
3. `_fetch`（`:209-269`）只做单次 HTTP：透传 method/url/headers/timeout、body（dict → `json`，其余 → `content`）、cookies（合并 client jar 后显式 `Cookie` 头）、referer；**不调用 callback**，异常不吞。
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
- worker 模式（`start_worker`）只是兼容适配器：调用 `fetch`，成功响应入 `response_queue`，失败记录日志；Spider 不走该路径（`:435-457`）。

> P1-03、P1-19、P2-01 均已随本阶段关闭。重试不再使用 `max_retries - retries` 作为指数，也不再向 Downloader 私有队列投递。

## 3.6 阶段 5：解析与提取

`Parser.parse`（`webants/parser.py:107-164`）：

1. `total_parses+1`。
2. 若是 `bytes`：按 `encoding or self.encoding` 解码；`UnicodeDecodeError` 时用 `charset_normalizer.detect`，置信度 > 0.8 才采用，否则抛 `ParserError`（`:124-153`）。
3. 计算 SHA-1 内容哈希（`:68-72`），调用 `_cached_parse(hash, content)`（`:156-158`）。
4. `_cached_parse` 检查大小上限，`fromstring(content, parser=self._parser)`（`:74-105`）。

缓存与统计：

- `_cached_parse` 是绑定在实例方法上的 `functools.lru_cache`，key 包含 `self`，会持有 Parser 实例（`:74-76`）。
- `parse` 每次调用都 `cache_hits+1`（`:159`），并不区分命中；实测首次解析后 `stats["cache_hits"]==1` 而 `cache_info().hits==0`。
- `css`/`xpath` 同样无条件累加 `css_cache_hits`/`xpath_cache_hits`（`:190-191,209-210`）。

提取层（可选，需调用方显式使用）：

1. `Item` 实例持有 `html`，字段描述符在 `__get__` 时调用 `extractor.extract(instance.html)`（`webants/libs/item.py:52-67`）。
2. `ExtractorFactory.create_extractor` 按类名前缀查找（`webants/libs/extractor.py:430-441`），类由元类自动注册（`:444-459`）。
3. `_LxmlElementExtractor.extract` 对 `str` 调用 `etree.HTML`，再按 selector → xpath → tags 顺序找元素（`:574-614`）。
4. `FilteringLinkExtractor` 先用 `LinkExtractor` 取 `Link` 列表，再逐条 `link_allowed` 过滤（`:932-949`）。

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

结构见 `webants/libs/result.py:183-201`。注意 `crawl_time` 参数默认 0.0，但构造时用 `crawl_time or time.time()`，显式传 0 也会被覆盖（`:107`）。

运行期中间产物：

| 产物 | 位置 | 生命周期 |
|------|------|----------|
| `seen_urls` | `Scheduler` 实例 | 进程内，无上限 |
| `domain_stats` / `domain_semaphores` | `Scheduler` 实例 | 每域名一份，不清理 |
| `_processing` | `Spider` 实例 | 进程内，当前处理中的 URL |
| `retry_requests` / `total_retries` / `total_attempts` | `Downloader.stats` | 进程内计数，不再保存 URL 集合 |
| `_cached_parse` LRU | Parser 类级缓存 | 最多 1000 条，实例被 key 持有 |
| `_css_cache` / `_xpath_cache` | Parser 实例 dict | 无上限 |
| `response_queue` | Downloader 实例 | 默认创建但 Spider 不消费（仅 worker 模式） |

## 3.8 失败模式

| 场景 | 实际行为 | 证据 |
|------|----------|------|
| 队列为空但仍有处理中任务 | `get_request()` 阻塞等待，主循环无法继续 | `webants/spider.py:462-470`、`webants/scheduler.py:170-181` |
| 队列为空且无处理中任务 | 本应 `break`，但 `get_request()` 先阻塞，永远到不了判断 | `webants/spider.py:463-469` |
| 回调是 async generator | 正常执行；`Request` 构造期接受 | `webants/libs/request.py:167-202`、`webants/spider.py:317-346` |
| 回调是普通协程 | 正常执行；返回值由 Spider 归一化 | `webants/spider.py:348-373` |
| 下载失败（HTTPError/可重试状态耗尽） | `fetch` 抛 `RequestError`，`process_request` 走 errback；不再构造 600 | `webants/downloader.py:381-414` |
| 请求未入队就调用 `process_request` | `finally` 捕获 `request_completed` 的 `KeyError` 并告警，不再掩盖原始异常 | `webants/spider.py:433-441` |
| 队列满 | `PriorityQueue.put` 挂起；持有域名信号量 | `webants/scheduler.py:118,163` |
| 文档为空字符串 | `fromstring` 抛错并包装为 `ParserError` | `webants/parser.py:90-105` |
| 链接过滤 `normalize=False` | `urlparse(Link)` → `AttributeError: 'Link' object has no attribute 'decode'` | 实测；`webants/libs/extractor.py:307-313,889-895` |
| 站点返回 403/404 | 直接返回 Response，不重试；是否视为失败由 callback 决定 | `webants/downloader.py:93,416-417` |

## 3.9 入口与“CLI”

仓库没有命令行入口、没有 `if __name__ == "__main__"`，也没有打包 `console_scripts`。唯一的可直接运行示例是 `example/use_item.py`：它在模块级构造 HTML、定义 `P` 类、实例化并 `print`（`example/use_item.py:403-418`），因此 `import` 该模块也会产生输出。该脚本只演示 `LinkItemDescriptor` / `TextItemDescriptor`，不涉及网络。
