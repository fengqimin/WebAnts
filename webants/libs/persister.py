"""Persister Module

This module provides storage backends for saving crawled data with:
- Multiple storage backend support (JSON, CSV, SQLite)
- Batched writing for performance
- Signal integration
- Validation and error handling
"""

import csv
import json
import sqlite3
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Union

from webants.libs.result import Result
from webants.utils.logger import get_logger


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

        # Statistics
        self.stats = {
            "total_saved": 0,
            "total_loaded": 0,
            "save_errors": 0,
            "load_errors": 0,
            "last_save": None,
            "last_load": None,
            "storage_size": 0,
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
    """JSON file storage backend."""

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
            indent: JSON indentation
            ensure_ascii: Whether to escape non-ASCII characters
            batch_size: Number of items to write at once
            **kwargs: Additional configuration
        """
        super().__init__(path, **kwargs)
        self.indent = indent
        self.ensure_ascii = ensure_ascii
        self.batch_size = batch_size
        self._batch = []

    async def save(self, data: Union[Result, Dict, List]) -> bool:
        """Save data to JSON file.

        Args:
            data: Data to save

        Returns:
            True if successful
        """
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
        """Load data from JSON file.

        Args:
            query: Optional filter query

        Returns:
            List of loaded items
        """
        try:
            if not self.path.exists():
                return []

            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if query:
                data = [
                    item
                    for item in data
                    if all(item.get(k) == v for k, v in query.items())
                ]

            self.update_stats("load")
            return data

        except Exception as e:
            self.logger.error(f"Error loading from JSON: {str(e)}")
            self.update_stats("load", success=False)
            return []

    async def _write_batch(self) -> None:
        """Write batched data to file."""
        if not self._batch:
            return

        try:
            if self.path.exists():
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = []

            data.extend(self._batch)

            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=self.indent, ensure_ascii=self.ensure_ascii)

            self._batch = []

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
                data = [
                    row
                    for row in data
                    if all(row.get(k) == v for k, v in query.items())
                ]

            self.update_stats("load")
            return data

        except Exception as e:
            self.logger.error(f"Error loading from CSV: {str(e)}")
            self.update_stats("load", success=False)
            return []

    async def _write_batch(self) -> None:
        """Write batched rows to file."""
        if not self._batch:
            return

        try:
            mode = "a" if self.path.exists() else "w"
            fieldnames = self.fieldnames or list(self._batch[0].keys())

            with open(self.path, mode, newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f, fieldnames=fieldnames, delimiter=self.delimiter
                )

                if mode == "w":
                    writer.writeheader()

                for row in self._batch:
                    # Ensure all values are strings
                    writer.writerow({k: str(v) for k, v in row.items()})

            self._batch = []

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
                        item["fields"] = json.loads(item["data"])
                        del item["data"]
                    data.append(item)

                self.update_stats("load")
                return data

        except Exception as e:
            self.logger.error(f"Error loading from SQLite: {str(e)}")
            self.update_stats("load", success=False)
            return []

    async def _write_batch(self) -> None:
        """Write batched rows to database."""
        if not self._batch:
            return

        try:
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
                            json.dumps(item.get("fields", {})),
                        ),
                    )

                conn.commit()

            self._batch = []

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

    def add_persister(
        self, name: str, persister: BasePersister, is_default: bool = False
    ) -> None:
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
