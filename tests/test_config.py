import pytest

from webants.libs.config import DEFAULT_CONFIG, Config


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
