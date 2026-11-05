"""Persistence lifecycle tests (ADR-0005, T1).

Covers flush/close/context management introduced for P1-09. The JSON format
itself is unchanged in T1; NDJSON migration belongs to T2.
"""

import asyncio
import json

import pytest

from webants.libs.exceptions import PersistenceError
from webants.libs.persister import (
    BasePersister,
    CsvPersister,
    JsonPersister,
    PersisterManager,
    SqlitePersister,
)


def _row(i: int) -> dict:
    return {
        "spider": "test",
        "url": f"http://example.com/{i}",
        "title": f"page {i}",
        "status": 200,
        "crawl_time": 1.0 + i,
        "fields": {"value": i},
    }


async def test_json_flush_writes_partial_batch(tmp_path):
    persister = JsonPersister(tmp_path / "results.json", batch_size=100)

    for i in range(3):
        assert await persister.save({"i": i}) is True

    assert not (tmp_path / "results.json").exists()
    assert await persister.flush() == 3
    assert len(await persister.load()) == 3
    assert persister.stats["total_written"] == 3
    assert persister.stats["flush_count"] == 1
    assert persister.stats["total_saved"] == 3


async def test_csv_flush_writes_partial_batch(tmp_path):
    persister = CsvPersister(tmp_path / "results.csv", batch_size=100)

    for i in range(3):
        await persister.save({"url": f"http://example.com/{i}", "value": i})

    assert not (tmp_path / "results.csv").exists()
    assert await persister.flush() == 3
    assert len(await persister.load()) == 3


async def test_sqlite_flush_writes_partial_batch(tmp_path):
    persister = SqlitePersister(tmp_path / "results.db", batch_size=100)

    for i in range(3):
        await persister.save(_row(i))

    assert await persister.flush() == 3
    assert len(await persister.load()) == 3


async def test_flush_empty_batch_returns_zero_without_creating_file(tmp_path):
    path = tmp_path / "empty.json"
    persister = JsonPersister(path)

    assert await persister.flush() == 0
    assert not path.exists()
    assert persister.stats["flush_count"] == 0


async def test_close_flushes_and_is_idempotent(tmp_path):
    persister = JsonPersister(tmp_path / "results.json", batch_size=100)
    await persister.save({"i": 1})
    await persister.save({"i": 2})

    await persister.close()
    assert persister.closed is True
    assert len(await persister.load()) == 2

    # Closing again must not raise or duplicate data.
    await persister.close()
    await persister.close()
    assert len(await persister.load()) == 2


async def test_save_after_close_raises_persistence_error(tmp_path):
    persister = JsonPersister(tmp_path / "results.json")
    await persister.close()

    with pytest.raises(PersistenceError) as excinfo:
        await persister.save({"i": 1})

    assert excinfo.value.code == "PERSISTER_CLOSED"


async def test_flush_after_close_is_noop(tmp_path):
    persister = JsonPersister(tmp_path / "results.json")
    await persister.close()

    assert await persister.flush() == 0


async def test_concurrent_saves_are_serialized(tmp_path):
    path = tmp_path / "results.json"
    persister = JsonPersister(path, batch_size=10)

    await asyncio.gather(*(persister.save({"i": i}) for i in range(50)))
    await persister.close()

    data = await persister.load()
    assert len(data) == 50
    assert {item["i"] for item in data} == set(range(50))


async def test_persister_async_context_manager(tmp_path):
    path = tmp_path / "results.json"

    async with JsonPersister(path, batch_size=100) as persister:
        await persister.save({"i": 1})

    assert persister.closed is True
    assert len(await persister.load()) == 1


async def test_manager_flush_and_close(tmp_path):
    manager = PersisterManager(tmp_path)
    json_persister = JsonPersister(tmp_path / "results.json", batch_size=100)
    csv_persister = CsvPersister(tmp_path / "results.csv", batch_size=100)
    manager.add_persister("json", json_persister)
    manager.add_persister("csv", csv_persister)

    await manager.save({"url": "http://example.com/1", "value": 1})

    assert await manager.flush() == {"json": 1, "csv": 1}
    assert await manager.close() == {"json": True, "csv": True}
    assert len(await json_persister.load()) == 1
    assert len(await csv_persister.load()) == 1


async def test_manager_close_continues_after_one_failure(tmp_path):
    class FailingPersister(BasePersister):
        async def save(self, data):
            return True

        async def load(self, query=None):
            return []

        async def _write_batch(self):
            raise RuntimeError("flush failed")

    manager = PersisterManager(tmp_path)
    manager.add_persister("bad", FailingPersister(tmp_path / "bad.json"))
    good = JsonPersister(tmp_path / "good.json", batch_size=100)
    manager.add_persister("good", good)
    await good.save({"i": 1})

    assert await manager.close() == {"bad": False, "good": True}
    assert len(await good.load()) == 1


async def test_manager_async_context_manager(tmp_path):
    manager = PersisterManager(tmp_path)
    persister = JsonPersister(tmp_path / "results.json", batch_size=100)
    manager.add_persister("json", persister)

    async with manager:
        await manager.save({"i": 1})

    assert persister.closed is True
    assert len(await persister.load()) == 1


# --- T2: NDJSON format, append-only writes and legacy migration --------------


async def test_json_writes_one_compact_record_per_line(tmp_path):
    path = tmp_path / "results.json"
    persister = JsonPersister(path, indent=4)

    await persister.save({"i": 1, "name": "中文"})
    await persister.save({"i": 2})
    await persister.flush()

    raw = path.read_text(encoding="utf-8")
    lines = raw.splitlines()
    assert raw.startswith("{")
    assert raw.endswith("\n")
    assert lines == ['{"i":1,"name":"中文"}', '{"i":2}']
    assert [json.loads(line) for line in lines] == [
        {"i": 1, "name": "中文"},
        {"i": 2},
    ]


async def test_json_append_does_not_reread_existing_file(tmp_path, monkeypatch):
    path = tmp_path / "results.json"
    reads = []
    real_load = json.load

    def counting_load(file):
        reads.append(file)
        return real_load(file)

    monkeypatch.setattr("webants.libs.persister.json.load", counting_load)
    persister = JsonPersister(path, batch_size=1)

    for i in range(3):
        assert await persister.save({"i": i}) is True

    assert reads == []
    assert len(await persister.load()) == 3


async def test_json_legacy_array_is_migrated_atomically(tmp_path):
    path = tmp_path / "results.json"
    path.write_text(
        json.dumps([{"i": 0}, {"i": 1}], ensure_ascii=False),
        encoding="utf-8",
    )
    persister = JsonPersister(path, batch_size=1)

    assert await persister.save({"i": 2}) is True
    raw = path.read_text(encoding="utf-8")
    assert raw.lstrip().startswith("{")
    assert [json.loads(line) for line in raw.splitlines()] == [
        {"i": 0},
        {"i": 1},
        {"i": 2},
    ]
    assert persister.stats["total_written"] == 1
    assert not list(tmp_path.glob("*.tmp"))


async def test_json_failed_migration_preserves_legacy_file(tmp_path):
    path = tmp_path / "results.json"
    path.write_text(json.dumps([{"i": 0}]), encoding="utf-8")
    persister = JsonPersister(path, batch_size=1)

    assert await persister.save({"bad": object()}) is False
    assert json.loads(path.read_text(encoding="utf-8")) == [{"i": 0}]
    assert not list(tmp_path.glob("*.tmp"))


async def test_json_load_skips_malformed_ndjson_line(tmp_path):
    path = tmp_path / "results.json"
    path.write_text('{"i":0}\n{"i":1}\n{"i":2', encoding="utf-8")
    persister = JsonPersister(path)

    data = await persister.load()

    assert [item["i"] for item in data] == [0, 1]
    assert persister.stats["load_errors"] == 1


async def test_json_append_repairs_missing_trailing_newline(tmp_path):
    path = tmp_path / "results.json"
    path.write_text('{"i":0}', encoding="utf-8")
    persister = JsonPersister(path, batch_size=1)

    assert await persister.save({"i": 1}) is True
    data = await persister.load()

    assert [item["i"] for item in data] == [0, 1]


async def test_json_failed_serialization_preserves_existing_records(tmp_path):
    path = tmp_path / "results.json"
    persister = JsonPersister(path, batch_size=1)
    assert await persister.save({"i": 0}) is True

    assert await persister.save({"bad": object()}) is False
    assert persister.stats["save_errors"] == 1

    data = await persister.load()
    assert [item["i"] for item in data] == [0]


async def test_json_ensure_ascii_flag(tmp_path):
    raw_path = tmp_path / "raw.json"
    escaped_path = tmp_path / "escaped.json"

    raw = JsonPersister(raw_path, ensure_ascii=False)
    escaped = JsonPersister(escaped_path, ensure_ascii=True)
    await raw.save({"name": "中文"})
    await escaped.save({"name": "中文"})
    await raw.flush()
    await escaped.flush()

    assert "中文" in raw_path.read_text(encoding="utf-8")
    assert "\\u4e2d\\u6587" in escaped_path.read_text(encoding="utf-8")


async def test_json_load_query_filters_ndjson(tmp_path):
    path = tmp_path / "results.json"
    persister = JsonPersister(path)
    await persister.save({"group": "a", "i": 1})
    await persister.save({"group": "b", "i": 2})
    await persister.flush()

    data = await persister.load({"group": "a"})

    assert [item["i"] for item in data] == [1]


async def test_json_empty_file_loads_as_empty_list(tmp_path):
    path = tmp_path / "results.json"
    path.write_text("", encoding="utf-8")

    assert await JsonPersister(path).load() == []


async def test_json_loads_legacy_array_before_migration(tmp_path):
    path = tmp_path / "results.json"
    path.write_text(
        json.dumps([{"i": 0}, {"i": 1}], ensure_ascii=False),
        encoding="utf-8",
    )

    data = await JsonPersister(path).load()

    assert data == [{"i": 0}, {"i": 1}]
