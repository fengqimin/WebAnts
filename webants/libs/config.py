"""Configuration Module

This module provides configuration management with:
- YAML-based configuration
- Environment variable support
- Dynamic settings validation
- Default configuration profiles
"""

import os
import yaml
from pathlib import Path
from typing import Any, Dict, Optional, Union

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
            "Accept-Encoding": "gzip, deflate"
        }
    },
    "scheduler": {
        "max_requests": 0,  # 0 for unlimited
        "request_delay": 0,
        "domain_delay": 1,
        "max_domain_concurrent": 8,
        "max_queue_size": 10000
    },
    "parser": {
        "default_encoding": "utf-8",
        "max_document_size": 10485760,  # 10MB
        "cache_size": 1000
    },
    "spider": {
        "failure_threshold": 5,
        "recovery_timeout": 60.0,
        "circuit_breaker_enabled": True,
        "retry_http_codes": [500, 502, 503, 504, 408, 429]
    },
    "storage": {
        "base_path": "data",
        "default_format": "json",
        "batch_size": 100,
        "formats": {
            "json": {
                "enabled": True,
                "indent": 2,
                "ensure_ascii": False
            },
            "csv": {
                "enabled": False,
                "delimiter": ",",
            },
            "sqlite": {
                "enabled": False,
                "table_name": "results"
            }
        }
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        "file": None
    }
}


class Config:
    """Configuration management with validation."""

    def __init__(
        self,
        config_path: Optional[Union[str, Path]] = None,
        env_prefix: str = "WEBANTS_"
    ):
        """Initialize configuration.
        
        Args:
            config_path: Path to YAML config file
            env_prefix: Prefix for environment variables
        """
        self.logger = get_logger(self.__class__.__name__)
        self._config = DEFAULT_CONFIG.copy()
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
        """Load configuration from environment variables."""
        for key, value in os.environ.items():
            if key.startswith(self._env_prefix):
                config_key = key[len(self._env_prefix):].lower()
                self._set_nested_value(config_key, value)
                
    def _set_nested_value(self, key: str, value: str) -> None:
        """Set nested configuration value from flat key.
        
        Args:
            key: Dot-separated configuration key
            value: Configuration value
        """
        parts = key.split(".")
        current = self._config
        
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
            
        # Convert value to appropriate type
        try:
            if value.lower() in ("true", "false"):
                value = value.lower() == "true"
            elif value.isdigit():
                value = int(value)
            elif value.replace(".", "").isdigit():
                value = float(value)
        except (ValueError, AttributeError):
            pass
            
        current[parts[-1]] = value
        
    def _merge_config(self, config: dict) -> None:
        """Recursively merge configuration dictionaries.
        
        Args:
            config: Configuration to merge
        """
        for key, value in config.items():
            if (
                key in self._config
                and isinstance(self._config[key], dict)
                and isinstance(value, dict)
            ):
                self._merge_config(value)
            else:
                self._config[key] = value
                
    def _validate_config(self) -> None:
        """Validate configuration values."""
        # Validate concurrent requests
        if self._config["downloader"]["concurrent_requests"] < 1:
            self.logger.warning("concurrent_requests must be >= 1, using default")
            self._config["downloader"]["concurrent_requests"] = DEFAULT_CONFIG["downloader"]["concurrent_requests"]
            
        # Validate timeouts
        if self._config["downloader"]["request_timeout"] < 0:
            self.logger.warning("request_timeout must be >= 0, using default")
            self._config["downloader"]["request_timeout"] = DEFAULT_CONFIG["downloader"]["request_timeout"]
            
        # Validate delays
        if self._config["scheduler"]["request_delay"] < 0:
            self.logger.warning("request_delay must be >= 0, using default")
            self._config["scheduler"]["request_delay"] = DEFAULT_CONFIG["scheduler"]["request_delay"]
            
        if self._config["scheduler"]["domain_delay"] < 0:
            self.logger.warning("domain_delay must be >= 0, using default")
            self._config["scheduler"]["domain_delay"] = DEFAULT_CONFIG["scheduler"]["domain_delay"]
            
        # Validate storage path
        storage_path = Path(self._config["storage"]["base_path"])
        if not storage_path.is_absolute():
            storage_path = Path.cwd() / storage_path
        self._config["storage"]["base_path"] = str(storage_path)
        
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
        return self._config.copy()
        
    def save(self, path: Union[str, Path]) -> None:
        """Save configuration to YAML file.
        
        Args:
            path: Path to save config file
        """
        try:
            path = Path(path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(
                    self._config,
                    f,
                    default_flow_style=False,
                    sort_keys=False
                )
                
        except Exception as e:
            self.logger.error(f"Error saving config file: {str(e)}")


# Global configuration instance
config = Config()