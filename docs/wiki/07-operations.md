# 07 运行与运维

## 7.1 前置条件

| 项 | 要求 | 证据 / 实测 |
|----|------|-------------|
| Python | ≥ 3.12 | `pyproject.toml:6`；`.python-version` 为 `3.12` |
| 解释器（本机） | 3.12.11 | `.venv/Scripts/python.exe --version` |
| 包管理器 | `uv` | 本机 `uv 0.13.6` |
| 操作系统 | 源码可跨平台；测试/示例含 Windows 惯例 | `setup.py:33` 标注 Windows |
| 可选浏览器 | Playwright Chromium | `webants/utils/login.py:27,106`；仅在用登录工具时需要 |
| 网络 | 可访问目标站点 | `tests/test_downloader.py:30` 会请求 `www.httpbin.org` |

## 7.2 首次安装

```bash
cd E:\Repositories\Python\WebCrawler\WebAnts

# 方式 A：uv（推荐，仓库有 uv.lock）
uv sync

# 方式 B：已有虚拟环境
.venv\Scripts\python.exe -m pip install -r requirements.txt

# 可选：安装 Playwright 及浏览器
uv sync --extra playwright
uv run playwright install chromium
```

> `uv sync` 与 `playwright install` 本次**未执行**（避免改动环境），标记为未验证。已验证的是现有 `.venv` 可导入 `pytest/httpx/lxml/pydantic/yaml`，且 `psutil` 缺失。

## 7.3 验证安装

```bash
# 已验证：1 failed, 123 passed
uv run pytest -q

# 已验证：17 个 F401
uv run ruff check .

# 已验证：7 个文件排序不合格
uv run isort --check .
```

若只想确认核心依赖可用：

```bash
.venv\Scripts\python.exe -c "import httpx, lxml, pydantic, yaml; print('ok')"
```

## 7.4 运行模式

| 模式 | 入口 | 说明 | 验证状态 |
|------|------|------|----------|
| 库调用 | 继承 `Spider`，`await start()` | 唯一的正式用法；无 CLI | 主链路已按 ADR-0001 修复并由 `tests/test_spider.py` 覆盖；`start()` 主循环仍未加超时（P1-02） |
| 单元测试 | `uv run pytest` | 8 个测试文件、98 个用例（含新增 `test_request.py`、`test_spider.py`） | 已验证（1 失败：`test_item.py::test_invalid_html`，P1-12） |
| 示例脚本 | `python example/use_item.py` | 只演示 Item 描述符，不联网；import 即执行 | 未执行 |
| Worker 模式 | `Downloader.start_downloader(many)` | 兼容适配器，调用 `fetch`；`RequestError` 只记录日志 | 未执行；`start_downloader` 仍会取消事件循环内其他任务（`webants/downloader.py:457-472`，P1-20） |
| 中间件/持久化 | 手工组装 `MiddlewareManager`/`PersisterManager` | 框架不自动接线 | 静态阅读 |

## 7.5 环境变量

`Config` 使用前缀 `WEBANTS_`，把变量名去掉前缀、转小写、按 `.` 分割成嵌套 key（`webants/libs/config.py:83,122-128`）。但环境变量名不能包含 `.`，所以像 `downloader.concurrent_requests` 这样的嵌套路径实际上无法通过环境变量表达；只能写 `WEBANTS_DOWNLOADER` 之类的一级键，落地为一个字典值字符串。

| 变量形式 | 预期效果 | 实际可行性 |
|----------|----------|------------|
| `WEBANTS_DOWNLOADER=...` | 覆盖 `downloader` 整节 | 可行，但值是字符串，后续代码按下标访问会失败 |
| `WEBANTS_DOWNLOADER.CONCURRENT_REQUESTS=20` | 覆盖嵌套项 | 不可行：变量名含 `.` 在 shell 中非法 |
| `WEBANTS_LOG_LEVEL=DEBUG` | 设置 `log.level` | 实际生成一级 `log_level`，不是嵌套的 `logging.level` |

此外，`Config` 是模块级单例（`webants/libs/config.py:260`），但**没有被 Spider/Scheduler/Downloader/Parser 引用**，所以即使设置环境变量，默认爬虫行为也不会改变。修改配置只能显式 `from webants.libs.config import config; config.set(...)`，且需要自己把值传给组件构造参数。

`DEFAULT_CONFIG` 中的关键默认值（`webants/libs/config.py:18-74`）：

| 节 | 键 | 默认 |
|----|----|------|
| downloader | `concurrent_requests` / `request_timeout` / `retry_times` / `retry_delay` | 10 / 30 / 3 / 1 |
| scheduler | `max_requests` / `request_delay` / `domain_delay` / `max_domain_concurrent` / `max_queue_size` | 0 / 0 / 1 / 8 / 10000 |
| parser | `default_encoding` / `max_document_size` / `cache_size` | utf-8 / 10485760 / 1000 |
| spider | `failure_threshold` / `recovery_timeout` | 5 / 60.0 |
| storage | `base_path` / `default_format` / `batch_size` | `data` / `json` / 100 |

注意这些默认值与 `Spider` 构造函数默认值并不完全一致（例如 `domain_delay` 配置为 1，而 `Scheduler` 默认 0；`max_domain_concurrent` 配置 8，而 Spider 传 10）。

## 7.6 构建与打包

- `pyproject.toml` 只声明项目元数据与工具配置，没有 `[build-system]` 节；`setup.py` 提供 setuptools 配置（`setup.py:1-35`）。
- 本次未执行 `uv build` 或 `python setup.py sdist`，构建产物未验证。
- `setup.py` 用 `open("README.md", ...)` 读取长描述（`setup.py:14`），实际文件名是 `readme.md`；Windows 大小写不敏感可工作，Linux 上会失败（[P2-13](08-issues-and-recommendations.md)）。
- 没有 `console_scripts`，安装后不会产生命令行工具。

## 7.7 数据导入、备份与恢复

| 场景 | 操作 | 注意 |
|------|------|------|
| JSON | 直接复制 `*.json` | 写入是“读全文件 + 覆盖”，并发写会互相覆盖 |
| CSV | 直接复制 `*.csv` | 追加模式要求列一致 |
| SQLite | 复制 `.db` 文件，或用 `sqlite3 .backup` | 无 WAL/PRAGMA 配置 |
| 导入 | 只有 `Persister.load()`，无 CLI | 需自己写脚本组装 `PersisterManager` |
| 迁移 | 无 migration | 表结构变更需手工 `ALTER TABLE` 或重建 |

**重要**：所有 Persister 的 `_batch` 都没有 `close()/flush()` 公共方法（`webants/libs/persister.py:94-473`），进程结束前未达到 `batch_size` 的数据不会落盘。若接入持久化，必须在退出前手动触发一次写入，或自行扩展 `flush()`。

## 7.8 健康检查与观测

没有 HTTP 健康端点。可观测性依赖：

```python
stats = spider.get_stats()
# 含 spider/scheduler/parser/downloader 四组统计与 circuits、failed_urls
```

证据：`webants/spider.py:450-466`、`webants/scheduler.py:198-208`、`webants/parser.py:244-255`、`webants/downloader.py:416-418`。

日志由 `get_logger(name)` 创建，同名 logger 复用已有 handler（`webants/utils/logger.py:31-35`），默认同时输出到 stderr；文件日志需显式传 `log_file`，但各组件构造时都没有传，所以默认没有日志文件。

## 7.9 故障排查

| 现象 | 可能原因 | 处理 |
|------|----------|------|
| 爬虫“卡住”不结束 | `get_request()` 在空队列上阻塞，主循环无法跳出 | 见 P1-02；临时可向队列塞哨兵或加超时 |
| 回调抛 `TypeError` | 协程回调返回了非法类型（如字符串/dict） | 返回 `None`、`Request`、`Result` 或 Request/Result 组成的可迭代对象 |
| `AttributeError: 'Link' object has no attribute 'decode'` | `FilteringLinkExtractor(normalize=False)` | 使用 `normalize=True`（默认） |
| `ModuleNotFoundError: psutil` | `utils/statistic.py` 的未声明依赖 | `uv add psutil` 或不要 import 该模块 |
| `ModuleNotFoundError: playwright` | 未安装可选组 | `uv sync --extra playwright` |
| 429/5xx 反复重试 | 默认可重试状态码包含 408/425/429/500/502/503/504，并遵循 `Retry-After` | 通过 `Downloader(retry_status_codes=...)` 覆盖；403/404 已默认不重试 |
| `black --check` 失败 | black 未声明/未安装 | 安装 black 或从流程中移除 |

## 7.10 部署说明

- 本框架不监听端口，不需要反向代理、TLS 或 CORS 配置。
- 若部署为定时任务，建议每个进程独立运行，避免 `Downloader.start_downloader` 的全局任务取消影响其他协程（`webants/downloader.py:402-405`）。
- 出口 IP、代理与 UA：代理通过 `Downloader(proxies=...)` 传入（`webants/downloader.py:161,200`），UA 通过 `headers` 或默认值（`:158,187-190`）。
- 登录场景依赖 Playwright 浏览器与硬编码论坛地址（`webants/utils/login.py:53`，注释 `:35`、`:110` 出现同一地址），部署前需改成本地配置。
- 没有容器化文件（无 `Dockerfile`），也没有 CI 配置。
