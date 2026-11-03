# 03 爬虫主链路

本章按一次爬取的顺序拆解 `Spider → Scheduler → Downloader → 解析/提取 → 回灌`。所有行号对应 `master @ 23f8051`。

## 3.1 常量与默认值

| 名称 | 值 | 位置 |
|------|----|------|
| `DEFAULT_REQUEST_HEADERS` | Accept、Accept-Language `en`、Accept-Encoding `gzip, deflate`、Connection `keep-alive`、固定 Firefox UA | `webants/libs/request.py:18-24` |
| `Downloader.RETRY_CODES` | 403/404（5 次，factor 2）、408/420/429/500/502/503/504（3 次，429 factor 5，其余 2） | `webants/downloader.py:88-98` |
| HTTP 超时 | `httpx.Timeout(30)`（Spider 默认） | `webants/spider.py:145,174`；`webants/downloader.py:148,170-174` |
| 连接池 | `max_keepalive_connections=concurrency`、`max_connections=concurrency*2`、`keepalive_expiry=60` | `webants/downloader.py:176-183` |
| 域名并发 | `max_domain_concurrent=10` | `webants/scheduler.py:30`；`webants/spider.py:168-170` |
| 调度队列容量 | 10000 | `webants/scheduler.py:32,47` |
| 解析缓存 | `lru_cache(maxsize=1000)` | `webants/parser.py:23,74-76` |
| 文档大小上限 | 10 MB | `webants/parser.py:33,51` |
| 默认 UA（Downloader） | Firefox 116 字符串 | `webants/downloader.py:187-190` |

## 3.2 阶段 1：构造与初始请求

1. `Spider.__init__` 实例化三个组件：`Scheduler(max_domain_concurrent=concurrent_requests)`、`Downloader(concurrency=..., timeout=...)`、`Parser()`（`webants/spider.py:168-176`）。三者之间**没有注入关系**，各自持有独立状态。
2. 初始化 `stats`、`_failed_urls`、`_processing`、`_domain_circuits`，并注册五个默认信号处理器（`webants/spider.py:194-224`）。
3. `start_requests()` 遍历 `start_urls`，为每个 URL 构造 `Request(callback=self.parse, errback=self.handle_error, retries=..., timeout=...)`（`webants/spider.py:270-283`）。
4. `Request.__init__` 校验 method/URL，并断言 callback/errback 为 coroutine function（`webants/libs/request.py:99-118`）。

> 第 4 步是主链路的第一个硬约束：回调必须是普通协程函数，不能是 async generator。

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

`Spider.start`（`webants/spider.py:397-430`）：

1. 幂等检查 `_running`，设置 `start_time`，发 `spider_started`。
2. 把所有初始请求 `await schedule_request(...)`。
3. `while self._running:` → `await scheduler.get_request()`；队列空且无处理中请求时 `break`，否则 `sleep(0.1)` 后重试（`:413-419`）。
4. 取到请求后 `asyncio.create_task(process_request(request))`（`:421`）。
5. 若 `_failed_urls` 非空，`await retry_failed()`（`:423-425`）。

`process_request`（`webants/spider.py:349-395`）：

1. 计时、`total_requests+1`、`_processing.add(url)`（`:355-357`）。
2. 取域名熔断器；`allow_request()` 为假则直接 `return`（`:359-364`）。
3. 发 `request_scheduled` 信号（`:367`）。
4. `response = await self.downloader.fetch(request)`（`:368`）。
5. 若存在 callback，则 `async for result in request.callback(response):`（`:370-375`）：
   - `Request` → 回灌 Scheduler；
   - `Result` → 发 `item_scraped`。
6. 成功：`circuit.record_success()`、`successful_requests+1`、发 `request_finished(success=True)`（`:377-379`）。
7. 异常：调用 errback、发 `request_finished(success=False)`（`:381-384`）。
8. `finally`：`_processing.remove(url)`、`scheduler.request_completed(request)`、更新平均耗时（`:386-395`）。

`Downloader.fetch`（`webants/downloader.py:361-378`）：

1. `_fetch(request)`（`:205-262`）：按 `request.delay or self.delay` 等待；选 headers；`client.request(method, url, headers, timeout)`。
2. 成功返回 `httpx.Response`；`httpx.HTTPError` 与其他异常都记录并返回**原 `Request`**。
3. `fetch` 收到 `Response` 后，如果 `request.callback` 非空，`await request.callback(result, request.cb_kwargs)`（`:372-375`）。
4. 收到 `Request`（失败）时，返回 `httpx.Response(status_code=600, extensions={"Request": request})`（`:377-378`）。

> 缺陷集中区：`fetch` 已经调用过一次回调，`process_request` 又对 `request.callback(response)` 的返回值做 `async for`。实测第二次是对协程对象迭代，抛 `TypeError: 'async for' requires an object with __aiter__ method, got coroutine`，随后被 errback 捕获，统计为失败（[P0-01](08-issues-and-recommendations.md)）。另外 `fetch` 以位置参数传 `request.cb_kwargs`，与常见 `parse(self, response, **kwargs)` 签名不兼容，实测会先抛 “takes 2 positional arguments but 3 were given”。

## 3.5 阶段 4：重试（worker 模式）

`fetch_retry` 只在 `start_worker` 中调用（`webants/downloader.py:386`），Spider 不会走到这里。

状态码重试（`:307-332`）：

- 状态码命中 `RETRY_CODES` 且 `retries > 0`：读取该状态码的 `backoff_factor` 与 `max_retries`，计算
  `backoff_delay = retry_delay * (backoff_factor ** (max_retries - request.retries))`（`:313-316`），写入 `request.delay`；
- `_retry_request` 执行 `retries -= 1`、`priority += 10`、`put_nowait` 回 `request_queue`（`:274-287`）；
- 返回 `None`，本轮 worker 结束。

异常重试（`:340-359`）：`_fetch` 返回 `Request` 且 `retries > 0` → 同上；耗尽后返回 `status_code=600`，并再一次 `failed_requests+1`。

可疑点：

- 退避指数用配置里的 `max_retries` 减去当前 `retries`，而不是“已重试次数”；当 `Request.retries` 大于该状态码的 `max_retries` 时指数为负（`:313-316`）。
- `priority += 10` 的注释写 “Lower priority”，但 `Request.__lt__` 反转，数值越大越先出队，实际是**提高**优先级（`webants/libs/request.py:175-177`；`webants/downloader.py:279`）。
- `_retry_request` 写入的是 Downloader 自己的 `request_queue`；`fetch_retry` 的直接调用方（测试、手工）不会自动消费该队列。

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
| `_failed_urls` / `_processing` | `Spider` 实例 | 进程内 |
| `retry_requests` | `Downloader` 实例 | 进程内，记录重试过的 URL |
| `_cached_parse` LRU | Parser 类级缓存 | 最多 1000 条，实例被 key 持有 |
| `_css_cache` / `_xpath_cache` | Parser 实例 dict | 无上限 |
| `response_queue` | Downloader 实例 | 默认创建但 Spider 不消费 |

## 3.8 失败模式

| 场景 | 实际行为 | 证据 |
|------|----------|------|
| 队列为空但仍有处理中任务 | `get_request()` 阻塞等待，主循环无法继续，`retry_failed` 不会执行 | `webants/spider.py:413-425`、`webants/scheduler.py:170-181` |
| 队列为空且无处理中任务 | 本应 `break`，但 `get_request()` 先阻塞，永远到不了判断 | `webants/spider.py:414-419` |
| 回调是 async generator | `Request` 构造期 `AssertionError` | `webants/libs/request.py:110-114` |
| 回调是普通协程 | `fetch` 内调用一次，`process_request` 再 `async for` 协程 → `TypeError` | 实测；`webants/spider.py:370-371` |
| 下载失败（HTTPError） | `fetch` 返回 600 响应，`process_request` 把它当成功响应继续回调 | `webants/downloader.py:377-378` |
| 请求未入队就调用 `process_request` | `finally` 中 `request_completed` 抛 `KeyError` | 实测；`webants/scheduler.py:189-190` |
| 队列满 | `PriorityQueue.put` 挂起；持有域名信号量 | `webants/scheduler.py:118,163` |
| 文档为空字符串 | `fromstring` 抛错并包装为 `ParserError` | `webants/parser.py:90-105` |
| 链接过滤 `normalize=False` | `urlparse(Link)` → `AttributeError: 'Link' object has no attribute 'decode'` | 实测；`webants/libs/extractor.py:307-313,889-895` |
| 站点返回 403/404 | 在 `fetch_retry` 路径会重试 5 次；在 `fetch` 路径只返回响应 | `webants/downloader.py:88-98,361-378` |

## 3.9 入口与“CLI”

仓库没有命令行入口、没有 `if __name__ == "__main__"`，也没有打包 `console_scripts`。唯一的可直接运行示例是 `example/use_item.py`：它在模块级构造 HTML、定义 `P` 类、实例化并 `print`（`example/use_item.py:403-418`），因此 `import` 该模块也会产生输出。该脚本只演示 `LinkItemDescriptor` / `TextItemDescriptor`，不涉及网络。
