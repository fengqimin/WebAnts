import pytest

from webants.utils import statistic


def test_statistic_module_imports_without_psutil():
    assert hasattr(statistic, "PerformanceMonitor")


def test_performance_monitor_requires_psutil(monkeypatch):
    monkeypatch.setattr(statistic, "psutil", None)
    with pytest.raises(RuntimeError, match="psutil"):
        statistic.PerformanceMonitor()
