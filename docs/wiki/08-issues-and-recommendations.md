# 08 问题与建议

## 8.1 严重级别

| 级别 | 定义 |
|------|------|
| **P0** | 核心功能不可用、数据损坏、安全暴露 |
| **P1** | 常见路径行为错误，或严重的可维护性/可观测性问题 |
| **P2** | 体验、性能、一致性问题，不阻断使用 |

## 8.2 验证环境与证据

所有结论基于 `master @ 23f8051`，Python 3.12.11，`uv 0.13.6`。

| 验证项 | 命令 / 脚本 | 实际结果 |
|--------|-------------|----------|
| 测试套件 | `uv run pytest -q` | `1 failed, 75 passed in 15.08s`；失败为 `tests/test_item.py::test_invalid_html` |
| Lint | `uv run ruff check .` | `Found 17 errors`，全部 `F401` |
| 导入排序 | `uv run isort --check .` | 7 个文件不合格 |
| 格式检查 | `uv run black --check .` | `Failed to canonicalize script path`，无法运行 |
| 配置合并 | 构造 `Config()` 后 `_merge_config({"downloader": {"concurrent_requests": 42}})` | 顶层出现 `concurrent_requests`，`downloader.concurrent_requests` 仍为 10 |
| 链接过滤 | `FilteringLinkExtractor(base_url=..., normalize=False).extract('<a href="/p">x</a>')` | `AttributeError: 'Link' object has no attribute 'decode'` |
| 链接归一化 | `normalize=True` 过滤 `http://EXAMPLE.com:80/a?b=2&a=1#f` | 返回的 `Link.url` 仍是原串，未被归一化 |
| Parser 统计 | 解析一次后读取 `stats` 与 `cache_info()` | `cache_hits == 1`，`cache_info().hits == 0` |
| Spider 处理 | mock `_fetch` 返回 200，调用 `process_request` | 回调在 `fetch` 内调用 1 次，随后 `async for` 协程抛 `TypeError`，统计 `failed_requests == 1` |
| 可选依赖 | `import psutil` | `ModuleNotFoundError` |

## 8.3 问题总表

| ID | 级别 | 领域 | 摘要 | 位置 | 建议动作 |
|----|------|------|------|------|----------|
| P0-01 | P0 | 主链路 | 回调契约冲突：`fetch` 调用一次回调，`process_request` 又对协程做 `async for` | `webants/spider.py:370-371`、`webants/downloader.py:361-375`、`webants/libs/request.py:110-114` | 统一回调契约 |
| P1-01 | P1 | 生命周期 | 未入队请求在 `finally` 中触发 `KeyError`，掩盖原始异常 | `webants/spider.py:386-388`、`webants/scheduler.py:183-196` | 记录入队状态、容错释放 |
| P1-02 | P1 | 生命周期 | 主循环无超时等待；任务未跟踪；关闭不等待 | `webants/spider.py:413-425,432-448` | 超时 + Task 集合 |
| P1-03 | P1 | 重试 | `fetch` 不重试，`fetch_retry` 只在 worker 路径生效 | `webants/downloader.py:289-378` | 统一入口 |
| P1-04 | P1 | 调度 | 先占信号量再 sleep/put，异常泄漏；入队串行化 | `webants/scheduler.py:117-168` | 调整 acquire 位置 |
| P1-05 | P1 | 提取 | `normalize=False` 崩溃；`normalize=True` 对输出无效 | `webants/libs/extractor.py:307-313,889-930` | 用 URL 字符串做判断 |
| P1-06 | P1 | 配置 | 嵌套 YAML 被拍平到顶层，默认值不生效 | `webants/libs/config.py:157-171` | 重写递归合并 |
| P1-07 | P1 | 解析 | 缓存统计失真；实例被类级 LRU 持有 | `webants/parser.py:74-76,156-160` | 改模块级缓存 + 计算命中 |
| P1-08 | P1 | 事件 | 等优先级接收器排序会 TypeError；优先级方向与文档相反 | `webants/libs/signals.py:46-85` | 显式 `key` 排序 |
| P1-09 | P1 | 持久化 | 无 flush/close，未满批次数据丢失 | `webants/libs/persister.py:94-473` | 增加 `close()` |
| P1-10 | P1 | 中间件 | `ResponseCache` 命中时返回 response，破坏链式契约 | `webants/libs/middleware.py:386-414,489-505` | 定义缓存命中语义 |
| P1-11 | P1 | 架构 | 中间件/持久化/配置/异步信号均未接入 Spider | 全仓库 grep | 接入或移出承诺 |
| P1-12 | P1 | 测试 | 1 个用例失败；Spider 无任何测试 | `tests/test_item.py:62-65` | 修测试 + 补主链路用例 |
| P1-13 | P1 | 下载 | 请求 body/cookies 未传给 httpx | `webants/downloader.py:224-229` | 透传 body/cookies |
| P1-14 | P1 | 下载 | 失败被包装成 `status_code=600` 的“成功响应” | `webants/downloader.py:377-378` | 明确失败信号 |
| P2-01 | P2 | 重试 | 403/404 被重试；退避指数使用 `max_retries` 而非已重试次数 | `webants/downloader.py:88-98,313-316` | 调整策略 |
| P2-02 | P2 | 优先级 | `priority += 10` 注释说降低，实际提高 | `webants/downloader.py:279`、`webants/libs/request.py:175-177` | 修正注释或符号 |
| P2-03 | P2 | 提取 | `except AttribExtractor` 写错；`_scheme_denied` 用错条件；`Link` 恒真 | `webants/libs/extractor.py:301-305,653-656,212-220` | 修正类型与条件 |
| P2-04 | P2 | 仓库卫生 | readme 混入 AI 对话；死 `.pyc`；requirements 漂移 | `readme.md:25-347`、`requirements.txt` | 清理与对齐 |
| P2-05 | P2 | 质量 | 17 个 F401；7 个文件 isort 不合格；black 不可用 | 实测 | 提交前跑 lint |
| P2-06 | P2 | 依赖 | `psutil` 未声明，`statistic.py` 无法导入 | `webants/utils/statistic.py:12` | 加依赖或延迟导入 |
| P2-07 | P2 | 工具 | 登录工具硬编码论坛 IP；async 函数内 `time.sleep` | `webants/utils/login.py:50,53,110` | 参数化 + `asyncio.sleep` |
| P2-08 | P2 | 持久化 | `JsonPersister` 用标准库 json，其他处优先 ujson | `webants/libs/persister.py:163-164` | 统一序列化 |
| P2-09 | P2 | 安全 | SQLite 表名 f-string 拼接 | `webants/libs/persister.py:354,451-453` | 白名单校验 |
| P2-10 | P2 | 打包 | `webants/__init__.py` 为空，无公共导出 | `webants/__init__.py:1` | 定义 `__all__` |
| P2-11 | P2 | 数据 | `crawl_time` 显式传 0 被 `or` 覆盖 | `webants/libs/result.py:107` | 用 `is None` 判断 |
| P2-12 | P2 | 工具 | `url_to_path` 对根路径 IndexError；`url_with_host` 对无 host 的 URL 会 TypeError | `webants/utils/url.py:253,270-275` | 边界校验 |
| P2-13 | P2 | 打包 | `readme = "README.md"` 与 `readme.md` 大小写不一致 | `pyproject.toml:5`、`setup.py:14` | 统一命名 |
| P2-14 | P2 | 流程 | 无 CI、无 `[build-system]` | 仓库根目录 | 增加 CI 与构建声明 |

## 8.4 P0 详解

### P0-01 回调契约冲突导致 Spider 主链路不可用

**现象**：`Spider.process_request` 在 `await downloader.fetch(request)` 之后，执行 `async for result in request.callback(response)`（`webants/spider.py:370-371`）。但 `Downloader.fetch` 内部已经 `await request.callback(result, request.cb_kwargs)`（`webants/downloader.py:372-375`）。因此：

1. 回调被调用第一次（在 downloader 内）；
2. 回到 Spider 后，`request.callback(response)` 只是再次创建协程对象，`async for` 迭代协程对象立即抛 `TypeError`；
3. 异常被 `except` 捕获，走 errback，页面被统计为失败。

**实测**（mock `_fetch` 返回 200，回调为普通协程）：

```text
RuntimeWarning: coroutine 'S.parse' was never awaited
  async for result in request.callback(response):
ERROR Request failed: ... 'async for' requires an object with __aiter__ method, got coroutine
callback invocation count: 1
stats: {'total_requests': 1, 'successful_requests': 0, 'failed_requests': 1}
```

另外，`fetch` 以位置参数传 `cb_kwargs`，与 `async def parse(self, response, **kwargs)` 不兼容，实测先报 `S.parse() takes 2 positional arguments but 3 were given`。

**根因**：三处对回调的假设互相矛盾：

- `Request` 要求 callback 是 coroutine function（`webants/libs/request.py:110-114`），排除了 async generator；
- `Spider.parse` 的注解/文档说它 yield `Request`/`Result`（`webants/spider.py:285-297`）；
- `process_request` 用 `async for` 消费回调结果（`webants/spider.py:370-375`）；
- `Downloader.fetch` 又假定回调是“接收 (response, kwargs)、返回 response”的普通协程（`webants/downloader.py:335-337`）。

**修复建议**：只保留一个契约。推荐“Downloader 只负责下载，Spider 负责回调”：

```python
# webants/downloader.py
async def fetch(self, request: Request) -> httpx.Response:
    result = await self._fetch(request)
    if isinstance(result, Request):
        raise RequestError(f"fetch failed: {request.url}")
    return result

# webants/spider.py
callback_result = request.callback(response)
if inspect.isasyncgen(callback_result):
    async for item in callback_result:
        ...
else:
    item = await callback_result
    ...
```

同时把 `Request` 的回调校验改为接受 coroutine function 或 async generator function（`inspect.iscoroutinefunction(...) or inspect.isasyncgenfunction(...)`），并让 `fetch_retry` 不再调用业务回调。

**回归测试**：新增 `tests/test_spider.py`，mock `_fetch`，覆盖“协程回调返回 async generator”“回调返回单个 Result”“回调抛异常”三条路径。

## 8.5 P1 详解

### P1-01 未入队请求在 finally 中触发 KeyError

`process_request` 的 `finally` 无条件调用 `scheduler.request_completed(request)`（`webants/spider.py:388`）。`request_completed` 直接访问 `self.domain_stats[domain]["active_requests"]`（`webants/scheduler.py:190`），若该请求从未 `schedule_request`（例如直接调用 `process_request`、或调度在 `sleep` 阶段被取消），就会抛 `KeyError`，并把原始异常替换掉。

实测：未先调度时，`process_request` 最终抛 `KeyError: 'example.com'`，而不是回调错误。

**修复**：

```python
def request_completed(self, request: Request) -> None:
    domain = self._get_domain(request.url)
    stats = self.domain_stats.get(domain)
    if stats is None or stats["active_requests"] <= 0:
        self.logger.warning(f"request_completed for unscheduled request: {request.url}")
        return
    stats["active_requests"] -= 1
    self.stats["current_requests"] = max(0, self.stats["current_requests"] - 1)
    sem = self.domain_semaphores.get(domain)
    if sem is not None:
        sem.release()
```

并在 `process_request` 里记录 `scheduled` 标志，只有成功入队才释放。

### P1-02 主循环无超时等待、任务未跟踪

- `get_request()` 内部 `await self.request_queue.get()` 没有超时（`webants/scheduler.py:176`），主循环在队列空时无法执行 `if not request: break`（`webants/spider.py:414-419`），爬虫可能永远挂着。
- `asyncio.create_task(...)` 没有保存引用（`webants/spider.py:421`），任务可能被垃圾回收，且 `close()` 不等待它们（`:432-448`）。

**修复**：

```python
self._tasks: set[asyncio.Task] = set()
...
task = asyncio.create_task(self.process_request(request))
self._tasks.add(task)
task.add_done_callback(self._tasks.discard)
...
# 主循环改为
try:
    request = await asyncio.wait_for(self.scheduler.get_request(), timeout=0.2)
except asyncio.TimeoutError:
    if not self._processing and self.scheduler.request_queue.empty():
        break
    continue
...
# close 中
if self._tasks:
    await asyncio.gather(*self._tasks, return_exceptions=True)
```

### P1-03 fetch 不重试

`Spider` 用的是 `fetch`（`webants/spider.py:368`），而重试逻辑只在 `fetch_retry`（`webants/downloader.py:289-359`）。因此主链路遇到 500/429 等状态码只会返回响应，不会重试；`retry_times` 参数在 Spider 路径上实际无效。

**修复**：让 `Spider` 调用 `fetch_retry`，或让 `fetch` 内部复用 `fetch_retry`，并统一 `None`（等待重试）与 `600`（耗尽）的处理方式。

### P1-04 调度器信号量泄漏与串行化

`schedule_request` 的顺序是 acquire → 计算延迟 → `sleep` → `put`（`webants/scheduler.py:118-163`）。问题：

- `sleep` 期间持有信号量，降低同域名并发；
- 队列满时 `put` 永久等待，信号量不释放；
- 任意异常被 `except` 吞掉，仅返回 `False`，不 `release`（`:166-168`）；
- `seen_urls` 在 sleep 之后才写入，并发下可能重复入队。

**修复**：先在锁内完成去重与占位，`put_nowait` 成功后再 acquire；或把信号量移到 worker 消费侧，确保 `finally` 释放。

### P1-05 normalize=False 崩溃、normalize=True 不生效

`FilteringLinkExtractor.link_allowed`：

```python
if self.normalize:
    link = normalize_url(link.url)   # link 变成 str
parts = urlparse(link)               # normalize=False 时仍是 Link 对象
```

证据：`webants/libs/extractor.py:889-895`；模块级 `extract_and_filter_links` 同样写法（`:307-313`）。实测 `normalize=False` 抛 `AttributeError: 'Link' object has no attribute 'decode'`。而 `normalize=True` 时只把局部变量变成字符串用于判断，最终返回的 `links` 仍是原始 `Link`（`:942-949`），归一化对输出无效。

**修复**：

```python
url = normalize_url(link.url) if self.normalize else link.url
parts = urlparse(url)
...
if allowed and self.normalize:
    link = Link(url=url, unique=link.unique)
```

### P1-06 Config 嵌套合并污染顶层

`_merge_config` 递归调用时始终使用 `self._config` 作为目标（`webants/libs/config.py:157-171`），而不是当前层级的 dict：

```python
for key, value in config.items():
    if key in self._config and isinstance(self._config[key], dict) and isinstance(value, dict):
        self._merge_config(value)   # 仍然合并到顶层
    else:
        self._config[key] = value   # 顶层写入
```

实测 `_merge_config({"downloader": {"concurrent_requests": 42}})` 后，顶层多出 `concurrent_requests=42`，而 `downloader.concurrent_requests` 仍为 10。另外 `DEFAULT_CONFIG.copy()` 是浅拷贝（`:92`），嵌套默认值会被就地修改。

**修复**：

```python
def _deep_merge(self, base: dict, patch: dict) -> None:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            self._deep_merge(base[key], value)
        else:
            base[key] = value
```

并用 `copy.deepcopy(DEFAULT_CONFIG)` 初始化。

### P1-07 Parser 缓存统计失真与实例泄漏

- `@functools.lru_cache` 修饰 `_cached_parse(self, content_hash, content)`（`webants/parser.py:74-76`），缓存 key 包含 `self`，Parser 实例无法被回收，多个实例共享同一个类级缓存。
- `parse` 每次调用都 `cache_hits += 1`（`:159`），`css`/`xpath` 也无条件累加命中数（`:191,210`）。实测首次解析 `cache_hits=1`、`cache_info().hits=0`。

**修复**：把缓存改为模块级函数或 `dict`，key 只用内容哈希；命中统计用 `cache_info()` 前后差值，或显式 `if hash in cache`。

### P1-08 异步信号排序与优先级方向

`Signal.send` 使用 `sorted(receivers)`（`webants/libs/signals.py:68`），receivers 是 `(priority, handler)` 元组。两个接收器优先级相同（默认都是 0）时，Python 会比较函数对象，抛 `TypeError: '<' not supported between instances of 'function' and 'function'`。同时升序排序与文档“higher = earlier”相反（`:92-105`）。

**修复**：`for priority, receiver in sorted(receivers, key=lambda x: -x[0]):`，并为同优先级增加稳定序号。

### P1-09 Persister 无 flush/close

三个后端都只有 `save`/`load`/`_write_batch`，`_batch` 达到 `batch_size` 才写（`webants/libs/persister.py:139-140,253-254,389-390`），没有公开的 `flush()` 或 `close()`。进程正常退出时，尾部不足 100 条的数据全部丢失。

**修复**：在 `BasePersister` 增加：

```python
async def close(self) -> None:
    await self._write_batch()
```

并在 `PersisterManager` 与 Spider 的 `close()` 中调用。

### P1-10 ResponseCache 破坏中间件链

`ResponseCache.process_request` 命中缓存时 `return response`（`webants/libs/middleware.py:408`），但 `MiddlewareManager.process_request` 把返回值当作 `current_request` 继续传给下一个中间件（`:489-505`）。后续中间件会对 `httpx.Response` 调用 `process_request`，语义完全错位。

**修复**：缓存命中不应通过 request 链返回。可以抛一个 `CacheHit(response)` 控制流异常，或在 Manager 中定义独立的 `lookup(request)` 阶段。

### P1-11 可选层未接入

grep 结果显示，除 `libs/middleware.py:25` 引用 `libs/signals.py` 外，没有任何核心模块 import `persister`、`config`、`middleware` 或 `utils.statistic`。因此：

- 中间件不会执行；`Request.prepare()` 也从未被 Spider 调用（`webants/libs/request.py:278-289`）；
- 持久化不会发生；
- YAML/环境变量配置不影响爬虫；
- 异步信号不会触发。

**建议**：要么在 `Spider.__init__` 中注入 `MiddlewareManager`、`PersisterManager` 并在 `process_request`/`item_scraped` 处调用；要么在 README 明确标注“实验性、需手工组装”，避免使用者误以为开箱可用。

### P1-12 测试失败与主链路零覆盖

- `tests/test_item.py:62-65` 期望 `item.html = 123` 抛 `AssertionError`，但 `_Item.html` 是普通属性，没有描述符校验（`webants/libs/item.py:86-87`），实测 `DID NOT RAISE`。
- `tests/` 中没有 `test_spider.py`，主链路（P0-01、P1-01、P1-02）完全未被测试覆盖。
- `test_normal_request` 直接访问 `www.httpbin.org`（`tests/test_downloader.py:30`），离线环境会失败。

**建议**：删除或改写失败用例（若要保留校验，应在 `_Item` 上增加 `html` 描述符）；为 Spider 增加 mock 测试；把网络用例标记为 `integration` 并默认跳过。

### P1-13 body/cookies 未发送

`_fetch` 只传 `method/url/headers/timeout`（`webants/downloader.py:224-229`），`Request.body` 与 `Request.cookies` 完全没传给 httpx。POST/PUT 实际发送空体，需要登录态的请求也不会带 cookie。

**修复**：按 httpx 约定传 `content=`/`data=`/`json=` 与 `cookies=`，并在 `Request` 中明确 body 类型语义。

### P1-14 失败被伪装为 600 成功响应

`fetch` 在 `_fetch` 返回 `Request` 时构造 `httpx.Response(status_code=600, extensions={"Request": request})`（`webants/downloader.py:377-378`）。`process_request` 不检查状态码，直接把它交给回调，回调可能把空响应当正常页面解析；统计上也不会增加 `failed_requests`。

**建议**：让 `fetch` 在失败时抛专用异常或返回 `None`，由 Spider 明确走 errback；若保留 600，至少 `process_request` 要判断 `response.status_code >= 600`。

## 8.6 P2 详解（摘要）

| ID | 证据 | 修复建议 |
|----|------|----------|
| P2-01 | `RETRY_CODES` 含 403/404（`webants/downloader.py:88-90`）；退避指数 `max_retries - request.retries`（`:313-316`） | 移除 403/404 或改为可配置；用 `attempt` 计数计算指数 |
| P2-02 | 注释 “Lower priority” 与 `Request.__lt__` 反转矛盾（`webants/downloader.py:279`、`webants/libs/request.py:175-177`） | 改成 `priority -= 10` 或修正注释 |
| P2-03 | `except AttribExtractor`（`webants/libs/extractor.py:655`）、`_scheme_denied` 判断 `schemes_allow`（`:301-305`）、`if link := Link(...)` 恒真（`:212-220`） | 分别改为 `AttributeError`、`schemes_deny`、显式判空 |
| P2-04 | `readme.md:25-203`、`:203-347` 混入 AI 对话；`webants/libs/__pycache__/response.cpython-312.pyc` 与 `tests/__pycache__/test_url.cpython-311.pyc` 无对应源码；`requirements.txt` 缺 pyyaml/psutil | 清理 readme、删除陈旧 pyc（走回收站）、重建依赖清单 |
| P2-05 | `ruff` 17 个 F401；`isort` 7 个文件；`black` 不可用（见 8.2） | 在 dev 组加入 ruff/black，提交前统一执行 |
| P2-06 | `webants/utils/statistic.py:12` 顶层 import psutil；`uv.lock` 无 psutil | 加入依赖，或改为函数内延迟导入并降级 |
| P2-07 | 异步登录硬编码 `http://108.170.5.99/...`（`webants/utils/login.py:53`；注释 `:35,110` 出现同一地址）；async 函数内 `time.sleep(5)`（`:50`） | URL 参数化；改 `await asyncio.sleep(5)` |
| P2-08 | `JsonPersister.load` 用 `json.load`（`webants/libs/persister.py:163-164`），`misc.py` 优先 ujson（`webants/utils/misc.py:5-8`） | 统一从 `utils.misc` 导入 json |
| P2-09 | 表名 f-string（`webants/libs/persister.py:354,451-453`） | 用正则白名单校验表名，或固定枚举 |
| P2-10 | `webants/__init__.py:1` 为空 | 导出 `Spider`、`Request`、`Result` 等常用符号并定义 `__all__` |
| P2-11 | `crawl_time or time.time()`（`webants/libs/result.py:107`） | 改为 `time.time() if crawl_time is None else crawl_time` |
| P2-12 | `url_to_path` 对根路径 `split('/', 1)[1]` 会 IndexError（`webants/utils/url.py:253`）；`url_with_host` 在 `hostname` 为 None 时 `replace` 报错（`:270-275`） | 增加边界判断与空值回退 |
| P2-13 | `pyproject.toml:5` 写 `README.md`，实际为 `readme.md`；`setup.py:14` 同样 | 统一改成 `readme.md` |
| P2-14 | 无 `.github/workflows`、无 `[build-system]` | 增加最小 CI（pytest + ruff + isort）与构建后端声明 |

## 8.7 安全专项

| 风险 | 位置 | 评估 |
|------|------|------|
| SQL 标识符注入 | `webants/libs/persister.py:354,416,451-453` | 表名来自构造参数；若上层允许用户输入表名则存在注入，当前默认值安全 |
| SSRF（任意 URL 抓取） | `webants/downloader.py:205-229` | 框架不限制目标地址；若 URL 来自不可信输入，需要调用方自行做 host 白名单 |
| 硬编码内网地址 | `webants/utils/login.py:53,110` | 暴露内部论坛 IP；示例代码不应提交真实地址 |
| 凭据落盘 | `webants/utils/login.py:65-67,154-156` | `state.json`/`cookies.json` 明文保存，需限制目录权限 |
| TLS 校验 | `webants/downloader.py:195-203` | httpx 默认校验证书，未主动关闭；风险低 |
| 无认证/授权 | 全仓库 | 这是库而非服务，安全边界由调用方负责 |

## 8.8 技术债与测试缺口

- **测试覆盖**：`spider.py`、`scheduler.py` 的部分、`downloader.py` 的重试分支、`persister.py`、`middleware.py`、`signals.py`、`config.py`、`utils/` 大多没有测试；`test_url.py` 只有陈旧 `.pyc`，没有源码。
- **双实现**：两套 Signal、两套 CircuitBreaker，维护成本高。
- **无类型检查**：没有 mypy/pyright 配置；大量 `Any` 与 `# type: ignore` 缺失。
- **文档漂移**：`docs/architecture.md:234` 声称若干历史问题“已修正”，但本次实测仍存在回调契约、Config 合并等问题；`readme.md` 大段内容与代码无关。
- **无 CI**：lint 与测试未在提交前自动执行，导致 17 个 F401 与 1 个失败用例长期存在。

## 8.9 路线图

| 阶段 | 目标 | 验收标准 |
|------|------|----------|
| 短期（1 周） | 修 P0-01、P1-01、P1-02、P1-14；补 `tests/test_spider.py` | 端到端 mock 爬取 3 个页面成功；`uv run pytest` 全绿 |
| 短期 | 修 P1-05、P1-06、P1-07；加 `close()/flush()` | 复现脚本全部通过；无未处理异常 |
| 中期（2–4 周） | 统一重试入口；修正调度器信号量；接入或明确隔离可选层 | `fetch` 与 `fetch_retry` 行为一致；中间件/持久化有明确接入点与测试 |
| 中期 | 清理 lint 与依赖；加 CI | `ruff check`、`isort --check`、`pytest` 在 CI 全绿 |
| 长期 | 增加类型检查、持久化去重、可恢复队列 | mypy 基线通过；重启后不重复抓取；任务可断点续跑 |

## 8.10 验收清单

- [ ] `uv run pytest -q` 无失败，且包含 Spider 主链路用例。
- [ ] `uv run ruff check .` 无错误；`uv run isort --check .` 通过。
- [ ] 用 mock HTTP 完成一次“初始页 → 解析出 2 个链接 → 抓取子页 → 产出 Result”的端到端测试。
- [ ] `FilteringLinkExtractor(normalize=False)` 不再抛异常，且 `normalize=True` 的返回 URL 已归一化。
- [ ] `Config` 加载嵌套 YAML 后，顶层不出现子键，默认值保持不变。
- [ ] Persister 在进程退出前能 flush 全部缓存；SQLite 行数与 `save` 次数一致。
- [ ] README 与 `docs/architecture.md` 的能力描述与运行路径一致（不再声称未接入的层已生效）。
