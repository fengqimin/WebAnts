# Overview

A simple asynchronous web crawler framework implemented using `httpx`.

## Architecture

```mermaid
graph TD
    A[Spider] --> B[Scheduler]
    B --> C[Downloader]
    C --> D[Parser]
    D --> B
    D --> A

 ```

## Main Data Flow

1. Spider initializes requests -> Scheduler
2. Scheduler dispatches requests -> Downloader
3. Downloader retrieves responses -> Parser
4. Parser processes data to generate new requests -> Scheduler
5. Parser processes data to generate results -> Spider

## 可选层

`Spider()` 默认保持旧行为。配置、中间件与持久化需要显式注入：

```python
from pathlib import Path

from webants.libs.config import Config
from webants.libs.middleware import MiddlewareManager, ResponseCache
from webants.libs.persister import JsonPersister, PersisterManager
from webants.spider import Spider

config = Config()
middleware = MiddlewareManager()
middleware.add_middleware(ResponseCache(ttl=60))
persister = PersisterManager(Path("data"))
persister.add_persister("json", JsonPersister(Path("data/results.json")))

spider = MySpider(config=config, middleware=middleware, persister=persister)
await spider.start()  # close() 会 flush 并关闭注入的 persister
```

可离线运行的完整组装示例见 `example/optional_layers.py`。异步 `signal_manager` 仍为实验性，暂未桥接 Spider 生命周期。
