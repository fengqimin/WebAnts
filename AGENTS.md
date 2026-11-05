# WebAnts - Agent Instructions

## Project

Async web crawler framework built on `httpx` + `lxml`. Python >=3.12, managed with `uv`.

## Commands

```bash
uv run pytest                    # run all tests
uv run pytest tests/test_parser.py  # single test file
uv run pytest -m "not slow"     # skip slow-marked tests
uv run pytest --asyncio-mode=auto  # explicit (already default in pyproject.toml)
uv run ruff check .             # lint (VS Code has ruff enabled)
uv run python -m isort --check . # import sort check (Windows-safe entry point)
uv run ruff format --check .    # format check (line-length=100, target py312)
```

Test markers: `slow`, `integration`, `unit`.

## Architecture

Single package `webants/`. Data flow: `Spider -> Scheduler -> Downloader -> Parser -> Spider`.

| Module | Purpose |
|---|---|
| `spider.py` | Base `Spider` class, `Signal` system, `CircuitBreaker` (per-domain) |
| `scheduler.py` | Priority queue, duplicate URL filter, domain concurrency semaphores, adaptive delay |
| `downloader.py` | `httpx.AsyncClient`-based HTTP, retry with exponential backoff by status code |
| `parser.py` | `lxml` HTML parser with LRU cache, CSS/XPath selectors, encoding detection |
| `libs/request.py` | `Request` data class with `__slots__`, callback/errback must be async |
| `libs/result.py` | `Result` + `Field` (pydantic `BaseModel`) for scraped data |
| `libs/item.py` | Descriptor-based `Item` classes (`AttrItem`, `LinkItem`, `ElementItem`, `TextItem`) that compose extractors |
| `libs/extractor.py` | Extractor hierarchy: `AttribExtractor`, `LinkExtractor`, `FilteringLinkExtractor`, `ElementExtractor`, `TextExtractor`, `ExtractorFactory` |
| `libs/middleware.py` | `Middleware` ABC + implementations: `CircuitBreaker`, `RateLimiter`, `RequestDeduplicator`, `ResponseCache`; `MiddlewareManager` chains them |
| `libs/persister.py` | Storage backends: `JsonPersister`, `CsvPersister`, `SqlitePersister`; `PersisterManager` |
| `libs/signals.py` | Async signal system with priority receivers; global `signal_manager` with standard signals registered |
| `libs/config.py` | YAML config with `WEBANTS_` env var overrides; global `config` instance |
| `libs/exceptions.py` | Exception hierarchy rooted at `WebAntsException`; global `exception_tracker` (currently commented out) |
| `utils/` | `logger.py`, `url.py` (normalize/fingerprint), `misc.py` (ujson with json fallback), `statistic.py` (psutil-based), `login.py` (playwright, optional) |

## Key Gotchas

- **Two separate Signal systems**: `spider.py` has a synchronous `Signal` class used by `Spider.signals`. `libs/signals.py` has an async `Signal`/`SignalManager` used by middleware and globally. Don't confuse them.
- **Two CircuitBreaker implementations**: one in `spider.py` (standalone class), one in `libs/middleware.py` (middleware subclass). They are independent.
- **Request callbacks must be coroutine functions or async generator functions**: enforced with `inspect` in `Request.__init__` (raises `TypeError`).
- **Request priority**: lower `priority` value is dequeued first. The scheduler uses `(priority, sequence, ...)` tuples; `Request.__lt__` is a strict weak ordering and is not used by the scheduler.
- **Request.headers holds only explicit headers**: `Downloader.effective_headers()` merges `DEFAULT_REQUEST_HEADERS` < downloader headers < request headers.
- **Downloader.get_stats**: returns stats dict copy (renamed from typo `get_stsats`).
- **exception_tracker** is a global `ExceptionTracker` instance in `libs/exceptions.py`, imported by `libs/signals.py` and `libs/middleware.py`. Its `track()` accepts any `Exception` (falls back to class name when `code`/`metadata` attrs absent).
- **utils/statistic.py requires `psutil`** — declared as the optional `monitoring` extra. The module imports safely without it; constructing `PerformanceMonitor` raises `RuntimeError` with an install hint.
- **login.py requires `playwright`** — listed under `[project.optional-dependencies]`, not core deps.
- **JsonPersister serialization** goes through `webants.utils.misc.dumps/loads` (ujson when available, stdlib fallback); do not import `json` directly in the persister.

## Testing

- Uses `pytest-asyncio` with `asyncio_mode=auto` and `loop_scope=function`.
- `conftest.py` adds project root to `sys.path`; provides `http_client` (async httpx) and `sample_html` fixtures.
- Downloader tests mock HTTP via `unittest.mock.AsyncMock` / `patch`.
- CI runs `.github/workflows/ci.yml` on Python 3.12 and 3.14 with ruff, isort, ruff format and pytest; all tests must stay offline.

## Style

- `ruff format` for formatting (line-length=100, py312); run `uv run ruff format .`.
- isort with black profile (line_length=100, multi_line_output=3).
- Ruff linting with an explicit `select` (`E4`, `E7`, `E9`, `F`) in `pyproject.toml`.
- On Windows use `uv run python -m isort`; the `isort` console entry point can fail with `Failed to canonicalize script path`.
