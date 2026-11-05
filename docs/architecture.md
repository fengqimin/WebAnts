# WebAnts 爬虫架构分析

## 一、架构图

```mermaid
graph TD
    subgraph 顶层控制层["顶层控制层 (Spider)"]
        SP["Spider<br/>主控/生命周期/统计"]
        SIG1["Signal (同步)<br/>spider.py 内置信号"]
        CB1["CircuitBreaker<br/>per-domain 熔断器"]
    end

    subgraph 调度层["调度层 (Scheduler)"]
        SCH["Scheduler"]
        PQ["PriorityQueue<br/>优先级队列"]
        DEDUP["seen_urls<br/>URL 去重"]
        DOM_SEM["domain_semaphores<br/>域名并发信号量"]
        ADAPT["自适应延迟<br/>EMA + jitter"]
    end

    subgraph 下载层["下载层 (Downloader)"]
        DL["Downloader"]
        HTTP["httpx.AsyncClient<br/>HTTP/HTTPS"]
        RETRY["fetch_retry<br/>状态码重试 + 指数退避"]
        STATS_DL["响应时间统计"]
    end

    subgraph 解析层["解析层 (Parser)"]
        PS["Parser"]
        LRU["_cache<br/>实例级 LRU（返回副本）"]
        CSS["_css_cache"]
        XP["_xpath_cache"]
        ENC["编码检测<br/>charset_normalizer"]
    end

    subgraph 数据抽象层["数据抽象层 (libs)"]
        REQ["Request<br/>__slots__ + 指纹"]
        RES["Result / Field<br/>pydantic"]
        ITEM["Item 体系<br/>AttrItem/LinkItem/..."]
        EXT["Extractor 体系<br/>Attrib/Link/Element/Text"]
    end

    subgraph 中间件层["中间件层 (libs/middleware)"]
        MM["MiddlewareManager"]
        CB2["CircuitBreaker (中间件版)"]
        RL["RateLimiter"]
        RD["RequestDeduplicator"]
        RC["ResponseCache"]
    end

    subgraph 持久化层["持久化层 (libs/persister)"]
        PM["PersisterManager"]
        JSON["JsonPersister"]
        CSV["CsvPersister"]
        SQLITE["SqlitePersister"]
    end

    subgraph 事件配置层["事件/配置/异常层"]
        SIG2["SignalManager (异步)<br/>libs/signals.py"]
        CFG["Config<br/>YAML + WEBANTS_ 环境变量"]
        EXC["ExceptionTracker<br/>异常追踪"]
    end

    subgraph 工具层["工具层 (utils)"]
        LOG["logger.py"]
        URL["url.py<br/>normalize/fingerprint"]
        MISC["misc.py<br/>ujson fallback"]
        STAT["statistic.py<br/>psutil 监控"]
        LOGIN["login.py<br/>playwright"]
    end

    SP --> SCH
    SP --> DL
    SP --> PS
    SP --> SIG1
    SP --> CB1

    SCH --> PQ
    SCH --> DEDUP
    SCH --> DOM_SEM
    SCH --> ADAPT

    DL --> HTTP
    DL --> RETRY
    DL --> STATS_DL

    PS --> LRU
    PS --> CSS
    PS --> XP
    PS --> ENC

    DL --> REQ
    SCH --> REQ
    PS --> ITEM
    ITEM --> EXT

    MM --> CB2
    MM --> RL
    MM --> RD
    MM --> RC
    MM -. 可选接入 .-> SP

    PM --> JSON
    PM --> CSV
    PM --> SQLITE
    PM -. 可选接入 .-> SP

    SIG2 --> MM
    SIG2 --> PM
    CFG -. 配置 .-> SP
    EXC -. 关联 .-> SIG2

    SP --> LOG
    SCH --> URL
    PS --> MISC
```

## 二、数据流图

### 2.1 主数据流（Request/Response 生命周期）

```mermaid
flowchart LR
    A["start_urls"] --> B["Spider.start_requests<br/>生成初始 Request"]
    B --> C["Scheduler.schedule_request<br/>去重 + 域名信号量 + 自适应延迟"]
    C --> D{"URL 已见过?<br/>dont_filter?"}
    D -- 是 --> F["丢弃/统计 total_filtered"]
    D -- 否 --> E["PriorityQueue.put"]
    E --> G["Scheduler.get_request<br/>按优先级出队"]

    G --> H["Spider.process_request<br/>熔断器检查"]
    H --> I{"circuit<br/>allow?"}
    I -- 否 --> J["跳过/记录"]
    I -- 是 --> K["Downloader.fetch<br/>httpx.AsyncClient"]

    K --> L{"HTTP 状态码<br/>in RETRY_CODES?"}
    L -- 是且可重试 --> M["_retry_request<br/>priority+=10<br/>指数退避 delay"]
    M --> E
    L -- 否 --> N["回调 Request.callback(response)"]

    N --> O{"yield 结果类型"}
    O -- Request --> P["Scheduler.schedule_request<br/>回灌新请求"]
    O -- Result --> Q["信号 item_scraped<br/>→ 持久化/统计"]
    O -- 异常 --> R["Request.errback<br/>熔断器 record_failure<br/>加入 _failed_urls"]

    R --> S["Spider.retry_failed<br/>周期性重试失败 URL"]
    S --> C

    P --> E
    Q --> T["PersisterManager<br/>JSON/CSV/SQLite"]
    Q --> U["Spider.stats 统计"]
```

### 2.2 组件交互时序

```mermaid
sequenceDiagram
    participant SP as Spider
    participant SCH as Scheduler
    participant DL as Downloader
    participant CB as CircuitBreaker
    participant PS as Parser/Callback
    participant PM as Persister

    SP->>SCH: schedule_request(start_requests)
    SCH->>SCH: 去重 + 获取域名信号量 + 自适应延迟
    SCH->>SCH: PriorityQueue.put

    loop 主循环
        SP->>SCH: get_request()
        SCH-->>SP: Request

        SP->>CB: allow_request(domain)?
        CB-->>SP: True/False

        SP->>DL: fetch(request)
        DL->>DL: httpx.AsyncClient.request
        alt 状态码触发重试
            DL->>DL: _retry_request (priority+=10, 退避)
            DL->>SCH: 队列回灌
        else 成功
            DL-->>SP: httpx.Response
        end

        SP->>PS: request.callback(response)
        loop yield Request | Result
            alt Request
                PS->>SCH: schedule_request (回灌)
            else Result
                PS->>SP: item_scraped 信号
                SP->>PM: save (可选)
            end
        end

        SP->>SCH: request_completed(request)
        SCH->>SCH: 释放域名信号量
    end

    Note over SP: _failed_urls 非空时<br/>周期性 retry_failed
```

### 2.3 并发控制数据流

```mermaid
flowchart TD
    REQ["新 Request 到达"] --> CHK1{"超过 max_requests?"}
    CHK1 -- 是 --> DROP1["拒绝"]
    CHK1 -- 否 --> CHK2{"URL 在 seen_urls<br/>且 非 dont_filter?"}
    CHK2 -- 是 --> DROP2["过滤 total_filtered"]
    CHK2 -- 否 --> ACQ["acquire 域名信号量<br/>max_domain_concurrent"]

    ACQ --> DELAY["计算自适应延迟<br/>EMA(α=0.2) + jitter(0.8~1.2)"]
    DELAY --> PUT["PriorityQueue.put<br/>(priority, request)"]

    PUT --> WAIT["等待出队"]
    WAIT --> WORK["process_request 执行"]
    WORK --> REL["request_completed<br/>release 域名信号量"]
    REL --> DONE["active_requests -= 1"]
```

## 三、关键设计要点

| 维度 | 机制 | 位置 |
|---|---|---|
| **优先级** | `PriorityQueue`，`__lt__` 反转 → 数值越大越先出队 | `request.py:177` / `scheduler.py:163` |
| **去重** | `seen_urls: Set[str]`，`Request.fingerprint()` 基于 method+url+body | `scheduler.py:95` / `request.py:189` |
| **域名并发** | 每域名独立 `asyncio.Semaphore(max_domain_concurrent)` | `scheduler.py:111` |
| **自适应限速** | 指数移动平均(α=0.2)估算延迟 + 随机抖动 | `scheduler.py:124-149` |
| **重试** | 按 HTTP 状态码配置 `max_retries`/`backoff_factor`，指数退避 | `downloader.py:88-98,289-359` |
| **熔断** | 两套独立实现：Spider per-domain 状态机 + 中间件版 | `spider.py:70` / `middleware.py:87` |
| **解析缓存** | 实例级 `OrderedDict`（默认 1000 条）按内容 hash 缓存，每次返回深拷贝 + CSS/XPath 表达式缓存 | `parser.py:55,186,195` |
| **信号系统** | 同步(Spider 内部) + 异步(全局 `signal_manager`)，两套并存 | `spider.py:24` / `libs/signals.py:27` |

> ⚠️ **历史陷阱（已修正）**：`Downloader.get_stsats` 拼写错误已改为 `get_stats`；`exception_tracker` 全局实例已取消注释并强化 `track()` 以兼容普通 `Exception`；`pyyaml` 已加入 `pyproject.toml`/`setup.py` 依赖；`setup.py` 已对齐 `pyproject.toml`（httpx、Python>=3.12、版本 0.1.1）。剩余未声明依赖：`utils/statistic.py` 需要 `psutil`。详见 `AGENTS.md`。
