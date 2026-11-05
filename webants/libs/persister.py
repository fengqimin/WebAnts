"""Persister Module

This module provides storage backends for saving crawled data with:
- Multiple storage backend support (JSON, CSV, SQLite)
- Batched writing for performance
- Signal integration
- Validation and error handling
"""

import asyncio
import csv
import os
import re
import sqlite3
import tempfile
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from webants.libs.exceptions import PersistenceError
from webants.libs.result import Result
from webants.utils import misc
from webants.utils.logger import get_logger

TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


class BasePersister(ABC):
    """Base class for all persisters."""

    def __init__(self, path: Union[str, Path], **kwargs):
        """Initialize persister.

        Args:
            path: Storage path
            **kwargs: Additional configuration
        """
        self.path = Path(path)
        self.logger = get_logger(self.__class__.__name__)

        # Lifecycle: writes are serialized per instance and rejected after close.
        self._lock = asyncio.Lock()
        self._closed = False

        # Statistics
        self.stats = {
            "total_saved": 0,
            "total_loaded": 0,
            "save_errors": 0,
            "load_errors": 0,
            "last_save": None,
            "last_load": None,
            "storage_size": 0,
            "total_written": 0,
            "flush_count": 0,
        }

    @abstractmethod
    async def save(self, data: Union[Result, Dict, List]) -> bool:
        """Save data to storage.

        Args:
            data: Data to save

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    async def load(self, query: Optional[Dict] = None) -> List[Dict]:
        """Load data from storage.

        Args:
            query: Optional query filter

        Returns:
            List of loaded items
        """
        pass

    @abstractmethod
    async def _write_batch(self) -> int:
        """Flush the in-memory batch to storage.

        Callers must hold ``self._lock``. Implementations must keep the batch
        intact when writing fails and return the number of written items.
        """
        pass

    @property
    def closed(self) -> bool:
        """Whether the persister has been closed."""
        return self._closed

    def _ensure_open(self) -> None:
        """Reject writes after ``close()`` instead of losing them silently."""
        if self._closed:
            raise PersistenceError(
                f"{self.__class__.__name__} is closed",
                code="PERSISTER_CLOSED",
            )

    def _record_written(self, count: int) -> None:
        """Update write statistics after a successful batch write."""
        if count <= 0:
            return
        self.stats["total_written"] += count
        if self.path.exists():
            self.stats["storage_size"] = self.path.stat().st_size

    async def flush(self) -> int:
        """Write the pending batch regardless of ``batch_size``.

        Returns:
            int: Number of items written. Returns 0 when the batch is empty or
            the persister is already closed.
        """
        async with self._lock:
            if self._closed:
                return 0
            written = await self._write_batch()
            if written:
                self.stats["flush_count"] += 1
            return written

    async def close(self) -> None:
        """Flush the pending batch and mark the persister as closed.

        Idempotent. If flushing fails the exception propagates and the
        persister stays open, so the caller can retry without losing the batch.
        """
        if self._closed:
            return
        async with self._lock:
            if self._closed:
                return
            await self._write_batch()
            self._closed = True

    async def __aenter__(self) -> "BasePersister":
        """Enter the asynchronous context without opening any resource."""
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Close the persister on context exit."""
        await self.close()

    def update_stats(self, operation: str, success: bool = True) -> None:
        """Update persister statistics.

        Args:
            operation: Operation type ('save' or 'load')
            success: Whether operation succeeded
        """
        if operation == "save":
            if success:
                self.stats["total_saved"] += 1
                self.stats["last_save"] = time.time()
            else:
                self.stats["save_errors"] += 1
        elif operation == "load":
            if success:
                self.stats["total_loaded"] += 1
                self.stats["last_load"] = time.time()
            else:
                self.stats["load_errors"] += 1

        if self.path.exists():
            self.stats["storage_size"] = self.path.stat().st_size


class JsonPersister(BasePersister):
    """NDJSON storage backend (one JSON value per line).

    Legacy files containing a single JSON array are still readable and are
    converted atomically to NDJSON on the next write.

    A given path must have a single writer. ``asyncio.Lock`` only serializes
    operations on this instance; it does not coordinate other processes.
    """

    def __init__(
        self,
        path: Union[str, Path],
        *,
        indent: int = 2,
        ensure_ascii: bool = False,
        batch_size: int = 100,
        **kwargs,
    ):
        """Initialize JSON persister.

        Args:
            path: JSON file path
            indent: Retained for signature compatibility; NDJSON output is
                always compact and this value is ignored.
            ensure_ascii: Whether to escape non-ASCII characters
            batch_size: Number of items to write at once
            **kwargs: Additional configuration
        """
        super().__init__(path, **kwargs)
        self.indent = indent
        self.ensure_ascii = ensure_ascii
        self.batch_size = batch_size
        self._batch = []
        # None = not inspected yet; True = legacy JSON array; False = NDJSON.
        self._legacy_format: Optional[bool] = None

    async def save(self, data: Union[Result, Dict, List]) -> bool:
        """Save data to JSON file.

        Args:
            data: Data to save

        Returns:
            True if successful
        """
        async with self._lock:
            self._ensure_open()
            try:
                if isinstance(data, Result):
                    data = data.to_dict()

                if isinstance(data, list):
                    self._batch.extend(data)
                else:
                    self._batch.append(data)

                if len(self._batch) >= self.batch_size:
                    await self._write_batch()

                self.update_stats("save")
                return True

            except Exception as e:
                self.logger.error(f"Error saving to JSON: {str(e)}")
                self.update_stats("save", success=False)
                return False

    async def load(self, query: Optional[Dict] = None) -> List[Dict]:
        """Load data from an NDJSON file, or a legacy JSON array.

        Malformed NDJSON lines are counted in ``load_errors`` and skipped so a
        partially written trailing line cannot hide the records before it.

        Args:
            query: Optional filter query

        Returns:
            List of loaded items
        """
        try:
            if not self.path.exists():
                return []

            with open(self.path, "r", encoding="utf-8") as f:
                head = f.read(4096)
                f.seek(0)
                if self._looks_like_array(head):
                    data = self._read_legacy_array(f)
                else:
                    data = self._read_ndjson(f)

            data = self._filter(data, query)
            self.update_stats("load")
            return data

        except Exception as e:
            self.logger.error(f"Error loading from JSON: {str(e)}")
            self.update_stats("load", success=False)
            return []

    def _read_legacy_array(self, file) -> List:
        """Parse a legacy single-array JSON file."""
        text = file.read()
        if not text.strip():
            return []
        try:
            data = misc.loads(text)
        except ValueError as e:
            self.stats["load_errors"] += 1
            self.logger.warning(f"Skipping malformed legacy JSON array: {e}")
            return []
        if not isinstance(data, list):
            self.stats["load_errors"] += 1
            self.logger.warning("Legacy JSON file does not contain an array")
            return []
        return data

    def _read_ndjson(self, file) -> List:
        """Parse line-delimited JSON, skipping malformed lines."""
        data = []
        for lineno, line in enumerate(file, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data.append(misc.loads(line))
            except ValueError as e:
                self.stats["load_errors"] += 1
                self.logger.warning(f"Skipping malformed NDJSON line {lineno} in {self.path}: {e}")
        return data

    @staticmethod
    def _filter(data: List, query: Optional[Dict]) -> List:
        """Apply an equality query to loaded records."""
        if not query:
            return data
        return [
            item
            for item in data
            if isinstance(item, dict) and all(item.get(k) == v for k, v in query.items())
        ]

    @staticmethod
    def _looks_like_array(text: str) -> bool:
        """Whether text starts with a legacy JSON array."""
        return text.lstrip("\ufeff \t\r\n").startswith("[")

    def _serialize(self, item: Any) -> str:
        """Serialize one NDJSON line."""
        return misc.dumps(
            item,
            ensure_ascii=self.ensure_ascii,
            separators=(",", ":"),
        )

    def _is_legacy_array(self) -> bool:
        """Inspect the target file once and cache its format."""
        if self._legacy_format is not None:
            return self._legacy_format
        if not self.path.exists() or self.path.stat().st_size == 0:
            self._legacy_format = False
            return False

        with open(self.path, "r", encoding="utf-8") as f:
            head = f.read(4096)
        self._legacy_format = self._looks_like_array(head)
        return self._legacy_format

    def _append(self, items: List) -> None:
        """Append items as NDJSON without reading the existing file."""
        payload = "".join(f"{self._serialize(item)}\n" for item in items)
        # A partially written trailing line must not swallow the next record.
        separator = "" if not self.path.exists() or self._ends_with_newline() else "\n"
        with open(self.path, "a", encoding="utf-8", newline="\n") as f:
            f.write(separator)
            f.write(payload)
            f.flush()

    def _ends_with_newline(self) -> bool:
        """Check the last byte of the target file."""
        try:
            with open(self.path, "rb") as f:
                f.seek(0, os.SEEK_END)
                if f.tell() == 0:
                    return True
                f.seek(-1, os.SEEK_END)
                return f.read(1) == b"\n"
        except OSError:
            return True

    def _migrate_and_write(self, items: List) -> None:
        """Convert a legacy array to NDJSON and append the new items atomically."""
        with open(self.path, "r", encoding="utf-8") as f:
            legacy = misc.loads(f.read())
        if not isinstance(legacy, list):
            raise ValueError(f"Legacy JSON file is not an array: {self.path}")

        records = legacy + items
        fd, tmp_name = tempfile.mkstemp(
            dir=str(self.path.parent),
            prefix=f"{self.path.name}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
                f.write("".join(f"{self._serialize(item)}\n" for item in records))
            os.replace(tmp_name, self.path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    async def _write_batch(self) -> int:
        """Write batched data to file and return the number of items written."""
        if not self._batch:
            return 0

        try:
            written = len(self._batch)
            if self._is_legacy_array():
                self._migrate_and_write(list(self._batch))
                self._legacy_format = False
            else:
                self._append(self._batch)

            self._batch = []
            self._record_written(written)
            return written

        except Exception as e:
            self.logger.error(f"Error writing batch to JSON: {str(e)}")
            raise


class CsvPersister(BasePersister):
    """CSV file storage backend."""

    def __init__(
        self,
        path: Union[str, Path],
        *,
        fieldnames: Optional[List[str]] = None,
        delimiter: str = ",",
        batch_size: int = 100,
        **kwargs,
    ):
        """Initialize CSV persister.

        Args:
            path: CSV file path
            fieldnames: Column names
            delimiter: Field delimiter
            batch_size: Number of rows to write at once
            **kwargs: Additional configuration
        """
        super().__init__(path, **kwargs)
        self.fieldnames = fieldnames
        self.delimiter = delimiter
        self.batch_size = batch_size
        self._batch = []

    async def save(self, data: Union[Result, Dict, List]) -> bool:
        """Save data to CSV file.

        Args:
            data: Data to save

        Returns:
            True if successful
        """
        async with self._lock:
            self._ensure_open()
            try:
                if isinstance(data, Result):
                    data = {
                        k: str(v.value) if hasattr(v, "value") else str(v)
                        for k, v in data.fields.items()
                    }

                if isinstance(data, list):
                    self._batch.extend(data)
                else:
                    self._batch.append(data)

                if len(self._batch) >= self.batch_size:
                    await self._write_batch()

                self.update_stats("save")
                return True

            except Exception as e:
                self.logger.error(f"Error saving to CSV: {str(e)}")
                self.update_stats("save", success=False)
                return False

    async def load(self, query: Optional[Dict] = None) -> List[Dict]:
        """Load data from CSV file.

        Args:
            query: Optional filter query

        Returns:
            List of loaded rows
        """
        try:
            if not self.path.exists():
                return []

            with open(self.path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                data = list(reader)

            if query:
                data = [row for row in data if all(row.get(k) == v for k, v in query.items())]

            self.update_stats("load")
            return data

        except Exception as e:
            self.logger.error(f"Error loading from CSV: {str(e)}")
            self.update_stats("load", success=False)
            return []

    async def _write_batch(self) -> int:
        """Write batched rows to file and return the number of rows written."""
        if not self._batch:
            return 0

        try:
            mode = "a" if self.path.exists() else "w"
            fieldnames = self.fieldnames or list(self._batch[0].keys())
            written = len(self._batch)

            with open(self.path, mode, newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=self.delimiter)

                if mode == "w":
                    writer.writeheader()

                for row in self._batch:
                    # Ensure all values are strings
                    writer.writerow({k: str(v) for k, v in row.items()})

            self._batch = []
            self._record_written(written)
            return written

        except Exception as e:
            self.logger.error(f"Error writing batch to CSV: {str(e)}")
            raise


class SqlitePersister(BasePersister):
    """SQLite storage backend."""

    def __init__(
        self,
        path: Union[str, Path],
        *,
        table_name: str = "results",
        batch_size: int = 100,
        **kwargs,
    ):
        """Initialize SQLite persister.

        Args:
            path: Database file path
            table_name: Name of table to use
            batch_size: Number of rows to write at once
            **kwargs: Additional configuration
        """
        if not isinstance(table_name, str) or not TABLE_NAME_RE.match(table_name):
            raise ValueError(
                f"table_name must match ^[A-Za-z_][A-Za-z0-9_]{{0,63}}$ (got {table_name!r})"
            )
        super().__init__(path, **kwargs)
        self.table_name = table_name
        self.batch_size = batch_size
        self._batch = []
        self._setup_db()

    def _setup_db(self) -> None:
        """Create database and table if needed."""
        try:
            with sqlite3.connect(self.path) as conn:
                c = conn.cursor()
                c.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        spider TEXT,
                        url TEXT,
                        title TEXT,
                        status INTEGER,
                        crawl_time REAL,
                        data TEXT
                    )
                """)
                conn.commit()

        except Exception as e:
            self.logger.error(f"Error setting up SQLite database: {str(e)}")
            raise

    async def save(self, data: Union[Result, Dict, List]) -> bool:
        """Save data to SQLite database.

        Args:
            data: Data to save

        Returns:
            True if successful
        """
        async with self._lock:
            self._ensure_open()
            try:
                if isinstance(data, Result):
                    data = data.to_dict()

                if isinstance(data, list):
                    self._batch.extend(data)
                else:
                    self._batch.append(data)

                if len(self._batch) >= self.batch_size:
                    await self._write_batch()

                self.update_stats("save")
                return True

            except Exception as e:
                self.logger.error(f"Error saving to SQLite: {str(e)}")
                self.update_stats("save", success=False)
                return False

    async def load(self, query: Optional[Dict] = None) -> List[Dict]:
        """Load data from SQLite database.

        Args:
            query: Optional filter query

        Returns:
            List of loaded rows
        """
        try:
            with sqlite3.connect(self.path) as conn:
                c = conn.cursor()

                if query:
                    where = " AND ".join(f"{k}=?" for k in query)
                    params = tuple(query.values())
                    sql = f"SELECT * FROM {self.table_name} WHERE {where}"
                else:
                    sql = f"SELECT * FROM {self.table_name}"
                    params = ()

                c.execute(sql, params)
                columns = [col[0] for col in c.description]
                rows = c.fetchall()

                data = []
                for row in rows:
                    item = dict(zip(columns, row))
                    if "data" in item:
                        item["fields"] = misc.loads(item["data"])
                        del item["data"]
                    data.append(item)

                self.update_stats("load")
                return data

        except Exception as e:
            self.logger.error(f"Error loading from SQLite: {str(e)}")
            self.update_stats("load", success=False)
            return []

    async def _write_batch(self) -> int:
        """Write batched rows to database and return the number of rows written."""
        if not self._batch:
            return 0

        try:
            written = len(self._batch)
            with sqlite3.connect(self.path) as conn:
                c = conn.cursor()

                for item in self._batch:
                    c.execute(
                        f"""
                        INSERT INTO {self.table_name}
                        (spider, url, title, status, crawl_time, data)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            item.get("spider"),
                            item.get("url"),
                            item.get("title"),
                            item.get("status"),
                            item.get("crawl_time"),
                            misc.dumps(item.get("fields", {})),
                        ),
                    )

                conn.commit()

            self._batch = []
            self._record_written(written)
            return written

        except Exception as e:
            self.logger.error(f"Error writing batch to SQLite: {str(e)}")
            raise


class PersisterManager:
    """Manages multiple storage backends with synchronization."""

    def __init__(self, base_path: Union[str, Path]):
        """Initialize persister manager.

        Args:
            base_path: Base path for storage files
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger(self.__class__.__name__)

        self._persisters: Dict[str, BasePersister] = {}
        self._default_persister: Optional[str] = None

    def add_persister(self, name: str, persister: BasePersister, is_default: bool = False) -> None:
        """Add a persister backend.

        Args:
            name: Unique name for persister
            persister: Persister instance
            is_default: Whether this is the default persister
        """
        self._persisters[name] = persister
        if is_default or len(self._persisters) == 1:
            self._default_persister = name

    def remove_persister(self, name: str) -> None:
        """Remove a persister backend.

        Args:
            name: Name of persister to remove
        """
        if name in self._persisters:
            if name == self._default_persister:
                self._default_persister = next(iter(self._persisters.keys()), None)
            del self._persisters[name]

    async def save(
        self, data: Union[Result, Dict, List], persisters: Optional[List[str]] = None
    ) -> Dict[str, bool]:
        """Save data to specified persisters.

        Args:
            data: Data to save
            persisters: List of persister names to use, or None for all

        Returns:
            Dictionary mapping persister names to success status
        """
        results = {}
        persisters = persisters or list(self._persisters.keys())

        for name in persisters:
            if name not in self._persisters:
                self.logger.warning(f"Persister {name} not found")
                results[name] = False
                continue

            try:
                success = await self._persisters[name].save(data)
                results[name] = success
            except Exception as e:
                self.logger.error(f"Error saving to {name}: {str(e)}")
                results[name] = False

        return results

    async def load(
        self, query: Optional[Dict] = None, persister: Optional[str] = None
    ) -> List[Dict]:
        """Load data from a persister.

        Args:
            query: Optional filter query
            persister: Persister to load from, or None for default

        Returns:
            List of loaded items
        """
        persister = persister or self._default_persister
        if not persister:
            self.logger.error("No persister available")
            return []

        if persister not in self._persisters:
            self.logger.error(f"Persister {persister} not found")
            return []

        try:
            return await self._persisters[persister].load(query)
        except Exception as e:
            self.logger.error(f"Error loading from {persister}: {str(e)}")
            return []

    async def flush(self) -> Dict[str, int]:
        """Flush every backend and report how many items each wrote.

        A failure in one backend is logged and reported as 0 without preventing
        the other backends from flushing.
        """
        results: Dict[str, int] = {}
        for name, persister in self._persisters.items():
            try:
                results[name] = await persister.flush()
            except Exception as e:
                self.logger.error(f"Error flushing {name}: {str(e)}")
                results[name] = 0
        return results

    async def close(self) -> Dict[str, bool]:
        """Close every backend, flushing pending batches first.

        Idempotent. A failure in one backend is reported as ``False`` and does
        not stop the remaining backends from closing.
        """
        results: Dict[str, bool] = {}
        for name, persister in self._persisters.items():
            try:
                await persister.close()
                results[name] = True
            except Exception as e:
                self.logger.error(f"Error closing {name}: {str(e)}")
                results[name] = False
        return results

    async def __aenter__(self) -> "PersisterManager":
        """Enter the asynchronous context."""
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Close every backend on context exit."""
        await self.close()

    def get_stats(self) -> Dict[str, dict]:
        """Get statistics from all persisters.

        Returns:
            Dictionary mapping persister names to their stats
        """
        return {name: persister.stats for name, persister in self._persisters.items()}

    async def sync(self, source: str, target: str) -> bool:
        """Synchronize data between persisters.

        Args:
            source: Source persister name
            target: Target persister name

        Returns:
            True if sync successful
        """
        if source not in self._persisters:
            self.logger.error(f"Source persister {source} not found")
            return False

        if target not in self._persisters:
            self.logger.error(f"Target persister {target} not found")
            return False

        try:
            data = await self._persisters[source].load()
            return await self._persisters[target].save(data)
        except Exception as e:
            self.logger.error(f"Error syncing data: {str(e)}")
            return False
