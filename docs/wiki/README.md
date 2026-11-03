# WebAnts 代码 Wiki

| 项目 | 内容 |
|------|------|
| 代码版本 | `master` @ `23f8051`（`fix: correct method name from get_stsats to get_stats in Downloader class`） |
| 生成日期 | 2026-11-03 |
| 文档范围 | `webants/`（全部 Python 源码）、`tests/`、`pyproject.toml`、`setup.py`、`requirements.txt`、`readme*.md`、`docs/architecture.md`、`example/use_item.py` |
| 数据快照 | 仓库内无 SQLite 数据库文件；仅 `data/html/awesome-python.html`（约 152 KB 样例 HTML），未做行数统计 |
| 运行验证 | `uv run pytest -q` → 1 failed, 75 passed（14.33s）；`uv run ruff check .` → 17 errors；`uv run isort --check .` → 11 个文件不合格、2 个跳过；`uv run black --check .` → 不可用；问题复核（2026-11-03）见 [08](08-issues-and-recommendations.md) |

## 这个项目是什么

WebAnts 是一个用 `httpx` + `lxml` 实现的异步爬虫框架。它把爬取过程拆成四个可替换的角色：`Spider` 负责生命周期与统计，`Scheduler` 负责优先级队列、去重、域名并发与自适应延迟，`Downloader` 负责 HTTP 请求与按状态码重试，`Parser` 负责 HTML 解析与 CSS/XPath 提取。仓库同时提供了一套更细粒度的 Request/Result/Item/Extractor 抽象，以及中间件、持久化、异步信号、YAML 配置、异常追踪等可选层。

当前版本是 `0.1.1`，定位为 Alpha 阶段的框架骨架：底层组件各自可用，但主链路尚未完全接通（详见 [08](08-issues-and-recommendations.md)）。

## 一页速览

| 维度 | 内容 |
|------|------|
| 语言 / 版本 | Python ≥ 3.12（`.python-version` 为 `3.12`） |
| 核心依赖 | `httpx` 0.28.x、`lxml` 5.3+、`pydantic` 2.11+、`pyyaml`、`ujson`、`charset-normalizer` |
| 包管理 | `uv`（`uv.lock`），同时保留 `setup.py` 与 `requirements.txt` |
| 入口 | 无 CLI、无 HTTP 服务；继承 `webants.spider.Spider` 并调用 `await spider.start()` |
| 数据流 | `Spider → Scheduler → Downloader → Parser/回调 → Scheduler / Spider` |
| 数据落盘 | `libs/persister.py` 的 JSON / CSV / SQLite（默认未接入 Spider） |
| 测试 | `pytest` + `pytest-asyncio`（`asyncio_mode=auto`），6 个测试文件、76 个用例 |
| 代码规模 | 源码 + 测试共 7,295 行（`wc -l`） |

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

## 快速开始

前置：Python 3.12+、`uv`（本机验证版本 0.13.6）。

```bash
# 安装依赖（本次未执行，标注为未验证）
uv sync

# 运行测试（已验证：1 failed, 75 passed）
uv run pytest -q

# 只跑某个测试文件
uv run pytest tests/test_parser.py
```

最小使用形态（根据代码推断，未做端到端运行验证）：

```python
from webants.spider import Spider

class MySpider(Spider):
    name = "my_spider"
    start_urls = ["https://example.com/"]

    async def parse(self, response):
        # 注意：当前回调契约存在冲突，见 08 的 P0-01
        yield None

# await MySpider().start()
```

> 注意：`Spider.start()` 在当前 `23f8051` 上存在回调契约与清理问题（[P0-01](08-issues-and-recommendations.md)、[P1-01](08-issues-and-recommendations.md)），上面的示例不代表已可稳定运行。

## 维护约定

- 本 Wiki 对应 `master` 的 `23f8051`。代码变更后请同步更新受影响章节的行号引用，并在 `README.md` 顶部更新 commit 与“运行验证”列。
- 行号一律相对仓库根目录，格式为 `path:line`；跨章节引用使用 `.md` 相对链接。
- 问题编号（`P0-01`、`P1-03` 等）在 [08](08-issues-and-recommendations.md) 定义，其他章节只引用、不重复定义。
- 结构校验：`python C:\Users\fengq\.qwenworkcn\skills\code-wiki-generator\scripts\validate_wiki.py docs/wiki`。
