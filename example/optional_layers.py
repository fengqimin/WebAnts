"""Optional layer assembly example (ADR-0005).

Run from the repository root:

    PYTHONPATH=. uv run python example/optional_layers.py

The example uses ``httpx.MockTransport`` so it runs offline. Replace the
mocked client with a real ``httpx.AsyncClient`` to crawl live sites.
"""

import asyncio
from pathlib import Path

import httpx

from webants.libs.config import Config
from webants.libs.middleware import (
    MiddlewareManager,
    RequestDeduplicator,
    ResponseCache,
)
from webants.libs.persister import JsonPersister, PersisterManager
from webants.libs.result import Result
from webants.spider import Spider

DEMO_HTML = "<html><head><title>Demo</title></head><body><p>hello from WebAnts</p></body></html>"


def demo_site(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, text=DEMO_HTML, request=request)


class DemoSpider(Spider):
    name = "optional_layers_demo"
    start_urls = ["http://example.com/"]

    async def parse(self, response, **kwargs):
        title = response.text.split("<title>")[1].split("</title>")[0]
        yield Result(
            spider=self.name,
            fields={"title": title},
            url=str(response.url),
        )


async def main() -> None:
    config = Config()
    config.set("scheduler.domain_delay", 0)

    middleware = MiddlewareManager()
    middleware.add_middleware(RequestDeduplicator())
    middleware.add_middleware(ResponseCache(ttl=60))

    persister = JsonPersister(Path("data/demo/results.json"), batch_size=10)
    persisters = PersisterManager(Path("data/demo"))
    persisters.add_persister("json", persister, is_default=True)

    spider = DemoSpider(
        config=config,
        middleware=middleware,
        persister=persisters,
    )

    # Offline demo: swap the real client for a mock transport.
    await spider.downloader.client.aclose()
    spider.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(demo_site))

    # start() closes the spider on exit, which flushes and closes the persister.
    await spider.start()

    stats = spider.get_stats()
    print("successful_requests:", stats["successful_requests"])
    print("cache_hits:", stats["cache_hits"])
    print("middleware_stats:", stats["middleware_stats"])
    print("persister_stats:", stats["persister_stats"])
    print("saved records:", await persister.load())


if __name__ == "__main__":
    asyncio.run(main())
