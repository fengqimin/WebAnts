import logging

import pytest

from webants.libs.config import DEFAULT_CONFIG, Config
from webants.spider import Spider


def test_nested_merge_stays_nested():
    """P1-06: nested keys must not be flattened into the top level."""
    config = Config()
    config._merge_config({"downloader": {"concurrent_requests": 42}})

    assert config.get("downloader.concurrent_requests") == 42
    assert "concurrent_requests" not in config.as_dict()


def test_instances_do_not_mutate_defaults():
    """P1-06: DEFAULT_CONFIG must be isolated from instance changes."""
    config = Config()
    config._config["downloader"]["concurrent_requests"] = 99

    assert DEFAULT_CONFIG["downloader"]["concurrent_requests"] == 10
    assert Config().get("downloader.concurrent_requests") == 10


def test_as_dict_is_a_deep_copy():
    config = Config()
    snapshot = config.as_dict()
    snapshot["downloader"]["concurrent_requests"] = 99

    assert config.get("downloader.concurrent_requests") == 10


def test_nested_yaml_merges_into_its_section(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("downloader:\n  concurrent_requests: 7\n", encoding="utf-8")

    config = Config(path)

    assert config.get("downloader.concurrent_requests") == 7
    assert "concurrent_requests" not in config.as_dict()


def test_env_double_underscore_overrides_nested_key(monkeypatch):
    """P2-16: WEBANTS_SECTION__KEY must reach the nested key."""
    monkeypatch.setenv("WEBANTS_DOWNLOADER__CONCURRENT_REQUESTS", "42")

    assert Config().get("downloader.concurrent_requests") == 42


def test_env_single_underscore_is_not_a_separator(monkeypatch):
    """P2-16: the legacy flat form must not create top-level junk keys."""
    monkeypatch.setenv("WEBANTS_DOWNLOADER_CONCURRENT_REQUESTS", "42")
    config = Config()

    assert config.get("downloader.concurrent_requests") == 10
    assert "downloader_concurrent_requests" not in config.as_dict()


def test_env_type_mismatch_falls_back_to_default(monkeypatch):
    """P2-16/E15: a non-numeric value must not crash validation."""
    monkeypatch.setenv("WEBANTS_DOWNLOADER__CONCURRENT_REQUESTS", "abc")

    assert Config().get("downloader.concurrent_requests") == 10


def test_unknown_env_top_level_is_ignored(monkeypatch):
    monkeypatch.setenv("WEBANTS_UNKNOWN__KEY", "value")
    config = Config()

    assert "unknown" not in config.as_dict()


def test_set_supports_dotted_keys():
    config = Config()
    config.set("downloader.concurrent_requests", 5)

    assert config.get("downloader.concurrent_requests") == 5


def test_boolean_and_float_env_coercion(monkeypatch):
    monkeypatch.setenv("WEBANTS_SPIDER__CIRCUIT_BREAKER_ENABLED", "false")
    monkeypatch.setenv("WEBANTS_SPIDER__RECOVERY_TIMEOUT", "1.5")

    config = Config()

    assert config.get("spider.circuit_breaker_enabled") is False
    assert config.get("spider.recovery_timeout") == pytest.approx(1.5)


def test_spider_applies_config_values():
    """ADR-0005 T4: every mapped config key reaches its component."""
    config = Config()
    config.set("downloader.concurrent_requests", 7)
    config.set("downloader.request_timeout", 12)
    config.set("downloader.retry_times", 2)
    config.set("downloader.retry_delay", 0.25)
    config.set("scheduler.max_domain_concurrent", 3)
    config.set("scheduler.domain_delay", 0)
    config.set("scheduler.request_delay", 0.5)
    config.set("scheduler.max_requests", 11)
    config.set("scheduler.max_queue_size", 22)
    config.set("parser.default_encoding", "gbk")
    config.set("parser.max_document_size", 1234)
    config.set("parser.cache_size", 5)
    config.set("spider.failure_threshold", 2)
    config.set("spider.recovery_timeout", 9.0)

    spider = Spider(config=config)

    assert spider.downloader.concurrency == 7
    assert spider.downloader.retry_delay == pytest.approx(0.25)
    assert spider.scheduler.max_domain_concurrent == 3
    assert spider.scheduler.domain_delay == 0
    assert spider.scheduler.request_delay == pytest.approx(0.5)
    assert spider.scheduler.max_requests == 11
    assert spider.scheduler.max_queue_size == 22
    assert spider.parser.encoding == "gbk"
    assert spider.parser.max_document_size == 1234
    assert spider.parser.cache_size == 5
    assert spider.failure_threshold == 2
    assert spider.recovery_timeout == pytest.approx(9.0)
    assert spider.settings["CONCURRENT_REQUESTS"] == 7
    assert spider.settings["REQUEST_TIMEOUT"] == 12
    assert spider.settings["RETRY_TIMES"] == 2
    assert spider.settings["RETRY_DELAY"] == pytest.approx(0.25)


def test_spider_config_defaults_only_apply_when_config_is_passed():
    """No config keeps the legacy defaults; an explicit Config uses its own."""
    bare = Spider()
    configured = Spider(config=Config())

    assert bare.downloader.concurrency == 10
    assert bare.scheduler.max_domain_concurrent == 10
    assert bare.scheduler.domain_delay == 0

    assert configured.downloader.concurrency == 10
    assert configured.scheduler.max_domain_concurrent == 8
    assert configured.scheduler.domain_delay == 1
    assert configured.parser.cache_size == 1000


def test_spider_explicit_arguments_override_config():
    spider = Spider(
        config=Config(),
        concurrent_requests=4,
        request_timeout=5,
        retry_times=1,
        retry_delay=0,
        failure_threshold=1,
        recovery_timeout=2.0,
    )

    assert spider.downloader.concurrency == 4
    assert spider.scheduler.max_domain_concurrent == 4
    assert spider.settings["REQUEST_TIMEOUT"] == 5
    assert spider.settings["RETRY_TIMES"] == 1
    assert spider.settings["RETRY_DELAY"] == 0
    assert spider.failure_threshold == 1
    assert spider.recovery_timeout == pytest.approx(2.0)


def test_spider_log_level_coercion():
    assert Spider._coerce_log_level("WARNING") == logging.WARNING
    assert Spider._coerce_log_level(logging.ERROR) == logging.ERROR
    assert Spider._coerce_log_level("not-a-level") == logging.INFO
