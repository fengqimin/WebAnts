"""Configuration Module

This module provides configuration management with:
- YAML-based configuration
- Environment variable support
- Dynamic settings validation
- Default configuration profiles
"""

import copy
import os
from pathlib import Path
from typing import Any, Optional, Union

import yaml

from webants.utils.logger import get_logger

DEFAULT_CONFIG = {
    "downloader": {
        "concurrent_requests": 10,
        "request_timeout": 30,
        "retry_times": 3,
        "retry_delay": 1,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/116.0",
        "default_headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en",
            "Accept-Encoding": "gzip, deflate",
        },
    },
    "scheduler": {
        "max_requests": 0,  # 0 for unlimited
        "request_delay": 0,
        "domain_delay": 1,
        "max_domain_concurrent": 8,
        "max_queue_size": 10000,
    },
    "parser": {
        "default_encoding": "utf-8",
        "max_document_size": 10485760,  # 10MB
        "cache_size": 1000,
    },
    "spider": {
        "failure_threshold": 5,
        "recovery_timeout": 60.0,
        "circuit_breaker_enabled": True,
        "retry_http_codes": [500, 502, 503, 504, 408, 429],
    },
    "storage": {
        "base_path": "data",
        "default_format": "json",
        "batch_size": 100,
        "formats": {
            "json": {"enabled": True, "indent": 2, "ensure_ascii": False},
            "csv": {
                "enabled": False,
                "delimiter": ",",
            },
            "sqlite": {"enabled": False, "table_name": "results"},
        },
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        "file": None,
    },
}


class Config:
    """Configuration management with validation."""

    def __init__(
        self, config_path: Optional[Union[str, Path]] = None, env_prefix: str = "WEBANTS_"
    ):
        """Initialize configuration.

        Args:
            config_path: Path to YAML config file
            env_prefix: Prefix for environment variables
        """
        self.logger = get_logger(self.__class__.__name__)
        self._config = copy.deepcopy(DEFAULT_CONFIG)
        self._env_prefix = env_prefix

        if config_path:
            self.load_config(config_path)

        self._load_env_vars()
        self._validate_config()

    def load_config(self, path: Union[str, Path]) -> None:
        """Load configuration from YAML file.

        Args:
            path: Path to config file
        """
        path = Path(path)
        if not path.exists():
            self.logger.warning(f"Config file not found: {path}")
            return

        try:
            with open(path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            if config:
                self._merge_config(config)

        except Exception as e:
            self.logger.error(f"Error loading config file: {str(e)}")

    def _load_env_vars(self) -> None:
        """Load configuration from environment variables.

        Nested keys use a double-underscore separator:
        ``WEBANTS_DOWNLOADER__CONCURRENT_REQUESTS`` sets
        ``downloader.concurrent_requests``. Single underscores are part of the
        key name.
        """
        for key, value in os.environ.items():
            if not key.startswith(self._env_prefix):
                continue

            config_key = key[len(self._env_prefix) :].lower()
            parts = config_key.split("__")
            if not config_key or any(not part for part in parts):
                self.logger.warning(f"Ignoring malformed environment override: {key}")
                continue
            if parts[0] not in self._config:
                self.logger.warning(f"Ignoring unknown environment override: {key}")
                continue

            self._set_nested_value(".".join(parts), value, from_env=True)

    def _set_nested_value(self, key: str, value: Any, *, from_env: bool = False) -> bool:
        """Set a nested configuration value from a flat key.

        Args:
            key: Dot-separated configuration key
            value: Configuration value
            from_env: Whether the value comes from an environment variable.
                Environment values are coerced and type-checked against the
                current value; mismatches are ignored with a warning.

        Returns:
            Whether the value was written.
        """
        parts = [part for part in key.split(".") if part]
        if not parts:
            return False

        current = self._config
        for part in parts[:-1]:
            node = current.get(part)
            if not isinstance(node, dict):
                if from_env:
                    self.logger.warning(
                        f"Ignoring environment override {key!r}: {part!r} is not a section"
                    )
                    return False
                node = {}
                current[part] = node
            current = node

        last = parts[-1]
        if from_env:
            if last not in current:
                self.logger.warning(f"Ignoring unknown environment override: {key}")
                return False
            coerced = self._coerce_scalar(value)
            if not self._types_match(current[last], coerced):
                self.logger.warning(
                    f"Ignoring environment override {key!r}: expected "
                    f"{type(current[last]).__name__}, got {type(coerced).__name__}"
                )
                return False
            current[last] = coerced
        else:
            current[last] = value
        return True

    @staticmethod
    def _coerce_scalar(value: str) -> Any:
        """Convert an environment string to bool/int/float when possible."""
        lowered = value.strip().lower()
        if lowered in ("true", "false"):
            return lowered == "true"
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        return value

    @staticmethod
    def _types_match(existing: Any, value: Any) -> bool:
        """Return whether ``value`` can replace ``existing`` without a type error."""
        if isinstance(existing, bool):
            return isinstance(value, bool)
        if isinstance(existing, (int, float)):
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if existing is None:
            return True
        return isinstance(value, type(existing))

    def _deep_merge(self, base: dict, patch: dict) -> None:
        """Recursively merge ``patch`` into the ``base`` mapping."""
        for key, value in patch.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = copy.deepcopy(value)

    def _merge_config(self, config: dict) -> None:
        """Recursively merge configuration dictionaries into the current config.

        Args:
            config: Configuration to merge
        """
        self._deep_merge(self._config, config)

    def _validate_config(self) -> None:
        """Validate configuration values."""
        self._validate_number(
            "downloader.concurrent_requests",
            1,
            DEFAULT_CONFIG["downloader"]["concurrent_requests"],
        )
        self._validate_number(
            "downloader.request_timeout",
            0,
            DEFAULT_CONFIG["downloader"]["request_timeout"],
        )
        self._validate_number(
            "scheduler.request_delay",
            0,
            DEFAULT_CONFIG["scheduler"]["request_delay"],
        )
        self._validate_number(
            "scheduler.domain_delay",
            0,
            DEFAULT_CONFIG["scheduler"]["domain_delay"],
        )

        # Validate storage path
        storage_path = self.get("storage.base_path")
        if not isinstance(storage_path, str):
            self.logger.warning("base_path must be a string, using default")
            storage_path = DEFAULT_CONFIG["storage"]["base_path"]
        path = Path(storage_path)
        if not path.is_absolute():
            path = Path.cwd() / path
        self._set_nested_value("storage.base_path", str(path))

    def _validate_number(self, key: str, minimum: float, default: Any) -> None:
        """Validate a numeric setting, falling back to ``default``."""
        value = self.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            self.logger.warning(f"{key} must be a number, using default")
            self._set_nested_value(key, default)
            return
        if value < minimum:
            self.logger.warning(f"{key} must be >= {minimum}, using default")
            self._set_nested_value(key, default)

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value.

        Args:
            key: Dot-separated configuration key
            default: Default value if key not found

        Returns:
            Configuration value
        """
        current = self._config
        for part in key.split("."):
            if not isinstance(current, dict):
                return default
            if part not in current:
                return default
            current = current[part]
        return current

    def set(self, key: str, value: Any) -> None:
        """Set configuration value.

        Args:
            key: Dot-separated configuration key
            value: Configuration value
        """
        self._set_nested_value(key, value)
        self._validate_config()

    def as_dict(self) -> dict:
        """Get complete configuration dictionary.

        Returns:
            Configuration dictionary
        """
        return copy.deepcopy(self._config)

    def save(self, path: Union[str, Path]) -> None:
        """Save configuration to YAML file.

        Args:
            path: Path to save config file
        """
        try:
            path = Path(path)
            path.parent.mkdir(parents=True, exist_ok=True)

            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(self._config, f, default_flow_style=False, sort_keys=False)

        except Exception as e:
            self.logger.error(f"Error saving config file: {str(e)}")


# Global configuration instance
config = Config()
