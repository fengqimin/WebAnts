# WebAnts 代码 Wiki

| 项目 | 内容 |
|------|------|
| 代码版本 | 2026-11-05 工作区（ADR-0005 实施后） |
| 生成日期 | 2026-11-03；2026-11-05 同步 |
| 文档范围 | `webants/`（全部 Python 源码）、`tests/`、`pyproject.toml`、`README.md`、`readme.cn.md`、`.github/workflows/ci.yml`、`docs/architecture.md`、`example/` |
| 数据快照 | 仓库内无 SQLite 数据库文件；仅 `data/html/awesome-python.html`（约 152 KB 样例 HTML），未做行数统计 |
| 运行验证 | `uv run pytest -q` → 242 passed（2026-11-05，阶段 5 完成后）；`uv run ruff check .`、`uv run python -m isort --check .`、`uv run ruff format --check .` 全部通过；CI 见 `.github/workflows/ci.yml`（Python 3.12/3.14）；问题与修复状态见 [08](08-issues-and-recommendations.md) |

## 这个项目是什么

WebAnts 是一个用 `httpx` + `lxml` 实现的异步爬虫框架。它把爬取过程拆成四个可替换的角色：`Spider` 负责生命周期与统计，`Scheduler` 负责优先级队列、去重、域名并发与自适应延迟，`Downloader` 负责 HTTP 请求与按状态码重试，`Parser` 负责 HTML 解析与 CSS/XPath 提取。仓库同时提供了一套更细粒度的 Request/Result/Item/Extractor 抽象，以及中间件、持久化、异步信号、YAML 配置、异常追踪等可选层。

当前版本是 `0.1.1`。主链路的回调契约与重试已按 [ADR-0001](../adr/0001-callback-contract-and-retry-ownership.md) 接通；调度槽位与关闭/取消契约分别按 [ADR-0002](../adr/0002-schedule-slot-ownership-and-request-lifecycle.md)、[ADR-0003](../adr/0003-shutdown-cancellation-and-loop-quiescence.md) 落地；数据正确性按 [ADR-0004](../adr/0004-data-correctness-link-filter-config-cache-signal.md) 修复；可选层按 [ADR-0005](../adr/0005-optional-layer-wiring-and-persistence-lifecycle.md) 以显式注入方式接入（默认关闭）。异步信号桥接仍待办（[BL-001](../backlog.md#bl-001-异步信号桥接)），其余 P2 按 [08](08-issues-and-recommendations.md) 阶段 5 收口。

## 一页速览

| 维度 | 内容 |
|------|------|
| 语言 / 版本 | Python ≥ 3.12（`.python-version` 为 `3.12`） |
| 核心依赖 | `httpx` 0.28.x、`lxml` 5.3+、`pydantic` 2.11+、`pyyaml`、`ujson`、`charset-normalizer` |
| 包管理 | `uv`；`pyproject.toml` + `uv.lock` 是唯一事实源（`setup.py`、`requirements.txt` 已删除，ADR-0006） |
| 入口 | 无 CLI、无 HTTP 服务；继承 `webants.spider.Spider` 并调用 `await spider.start()` |
| 数据流 | `Spider → Scheduler → Downloader → Parser/回调 → Scheduler / Spider` |
| 数据落盘 | `libs/persister.py` 的 JSON(NDJSON) / CSV / SQLite；显式注入后由 Spider 分发并关闭时 flush |
| 测试 | `pytest` + `pytest-asyncio`（`asyncio_mode=auto`），13 个测试文件、212 个用例 |
| 代码规模 | 源码 + 测试共 9,396 行（`wc -l`） |

## 按角色阅读路径

- **新加入的开发者**：先读 [01 架构](01-architecture.md) → [03 爬虫主链路](03-crawler.md) → [09 代码索引](09-code-reference.md)，再按需看 [02 编程接口](02-backend-api.md)。
- **维护者 / 重构者**：读 [01](01-architecture.md) 的 ADR 与风险 → [08 问题清单](08-issues-and-recommendations.md) → [06 依赖与边界](06-dependencies.md)。
- **数据使用方**：读 [04 数据模型](04-data-model.md)（Result/Field 与 SQLite 表结构）→ [03](03-crawler.md) 的输出结构。
- **运维 / 集成方**：读 [07 运行与运维](07-operations.md) → [06](06-dependencies.md) → [08](08-issues-and-recommendations.md) 的 P0/P1。
- **代码评审者**：直接看 [08](08-issues-and-recommendations.md) 的验证证据与修复建议，再用 [09](09-code-reference.md) 定位。

## 章节索引

| 章节 | 内容 | 目标读者 |
|------|------|----------|
| [01 架构](01-architecture.md) | 定位、分层、运行时图、核心时序、技术选型与 ADR、架构风险 | 所有人 |
| [02 编程接口](02-backend-api.md) | 无 HTTP 服务；Spider/Scheduler/Downloader/Parser/Request/Result 的公开接口、错误处理、并发语义 | 框架使用者 |
| [03 爬虫主链路](03-crawler.md) | 从 `start_urls` 到 `Result` 的逐段流程、重试、缓存、提取器、失败模式 | 爬虫开发者 |
| [04 数据模型](04-data-model.md) | `Result`/`Field` 结构、SQLite DDL、JSON/CSV 形态、去重与生命周期、数据质量 | 数据使用方 |
| [05 前端](05-frontend.md) | 本仓库无前端；说明唯一的示例脚本入口 | 前端/使用者 |
| [06 依赖与边界](06-dependencies.md) | 运行时/开发依赖、使用点、模块依赖图、实测 lint 结果、耦合热点 | 维护者 |
| [07 运行与运维](07-operations.md) | 环境准备、安装、运行模式、环境变量、构建、故障排查 | 运维/集成方 |
| [08 问题与建议](08-issues-and-recommendations.md) | 修订版：44 条 P0/P1/P2 问题登记、证据标注、问题簇与依赖、根因、修复建议、路线图 | 维护者/评审者 |
| [09 代码索引](09-code-reference.md) | 按模块分组的 symbol → file:line → 用途 → 调用方 | 所有人 |

## 代码地图

| 路径 | 责任 |
|------|------|
| `webants/spider.py` | `Spider` 生命周期、同步 `Signal`、per-domain `CircuitBreaker`、统计、失败重试 |
| `webants/scheduler.py` | 优先级队列、URL 去重、域名信号量、EMA 自适应延迟 |
| `webants/downloader.py` | `BaseDownloader` 抽象 + `Downloader`（httpx、状态码重试、连接池） |
| `webants/parser.py` | lxml 解析、LRU 文档缓存、CSS/XPath 表达式缓存、编码检测 |
| `webants/libs/request.py` | `Request`（`__slots__`、回调校验、指纹、优先级） |
| `webants/libs/result.py` | `Result` + pydantic `Field` |
| `webants/libs/item.py` | 描述符式 `AttrItem` / `LinkItem` / `ElementItem` / `TextItem` |
| `webants/libs/extractor.py` | 提取器基类、工厂、元类注册、链接过滤 |
| `webants/libs/middleware.py` | `Middleware` ABC、熔断、限速、去重、响应缓存、`MiddlewareManager` |
| `webants/libs/persister.py` | JSON/CSV/SQLite 持久化与 `PersisterManager` |
| `webants/libs/signals.py` | 异步信号系统与全局 `signal_manager` |
| `webants/libs/config.py` | YAML + `WEBANTS_` 环境变量配置 |
| `webants/libs/exceptions.py` | 异常层级与 `ExceptionTracker` |
| `webants/utils/` | 日志、URL 归一化/指纹、ujson 回退、psutil 监控、playwright 登录 |
| `tests/` | pytest 用例与 `conftest.py` |
| `example/use_item.py` | Item/Descriptor 的独立演示脚本（导入即执行） |
| `example/optional_layers.py` | ADR-0005 的可选层组装示例（离线 MockTransport，关闭时 flush） |

## 快速开始

前置：Python 3.12+、`uv`（本机验证版本 0.13.6）。

```bash
# 安装依赖（本次未执行，标注为未验证）
uv sync

# 运行测试（已验证：242 passed）
uv run pytest -q

# 可选层组装示例（离线运行）
PYTHONPATH=. uv run python example/optional_layers.py

# 只跑某个测试文件
uv run pytest tests/test_parser.py
```

最小使用形态（根据代码推断，未做端到端运行验证）：

```python
from webants.libs.result import Result
from webants.spider import Spider


class MySpider(Spider):
    name = "my_spider"
    start_urls = ["https://example.com/"]

    async def parse(self, response):
        # 规范形态：async generator，逐个 yield Request 或 Result
        yield Result(
            spider=self.name,
            fields={"body": response.text},
            url=str(response.url),
        )


# async with MySpider() as spider:   # __aexit__ 保证 close
#     await spider.start()           # 关闭后不可重启，每次爬取新建实例
```

> 回调契约与重试已按 [ADR-0001](../adr/0001-callback-contract-and-retry-ownership.md) 修复；关闭、取消与主循环收敛已按 [ADR-0003](../adr/0003-shutdown-cancellation-and-loop-quiescence.md) 落地（P1-02、P1-20、P2-23 Closed）；可选层按 [ADR-0005](../adr/0005-optional-layer-wiring-and-persistence-lifecycle.md) 注入（P1-09/10/11、P2-19 Closed）。`start()` 在主循环收敛后自动 `close()`；也可以显式 `await spider.close()`。需要中间件/持久化时，构造 `Spider(config=..., middleware=..., persister=...)`，完整示例见 `example/optional_layers.py`。

## 维护约定

- 本 Wiki 对应 2026-11-05 工作区（ADR-0005 实施后）。代码变更后请同步更新受影响章节的行号引用，并在 `README.md` 顶部更新 commit 与“运行验证”列。
- 行号一律相对仓库根目录，格式为 `path:line`；跨章节引用使用 `.md` 相对链接。
- 问题编号（`P0-01`、`P1-03` 等）在 [08](08-issues-and-recommendations.md) 定义，其他章节只引用、不重复定义。
- 结构校验：`python C:\Users\fengq\.qwenworkcn\skills\code-wiki-generator\scripts\validate_wiki.py docs/wiki`。
