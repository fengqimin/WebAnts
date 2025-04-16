"""Statistics Module

This module provides performance monitoring with:
- Real-time metrics collection
- Aggregated statistics
- Time-series data
- Memory profiling
"""

import gc
import time
import psutil
import threading
from collections import defaultdict, deque
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from webants.utils.logger import get_logger


class TimeSeriesMetric:
    """Time-series metric with rolling window."""
    
    def __init__(self, window_size: int = 60):
        """Initialize time series metric.
        
        Args:
            window_size: Number of points to keep
        """
        self.window_size = window_size
        self.values = deque(maxlen=window_size)
        self.timestamps = deque(maxlen=window_size)
        
    def add(self, value: float, timestamp: Optional[float] = None) -> None:
        """Add value to time series.
        
        Args:
            value: Metric value
            timestamp: Optional timestamp
        """
        self.values.append(value)
        self.timestamps.append(timestamp or time.time())
        
    def get_values(self, window: Optional[int] = None) -> List[float]:
        """Get recent values.
        
        Args:
            window: Optional window size
            
        Returns:
            List of recent values
        """
        if window is None or window >= len(self.values):
            return list(self.values)
        return list(self.values)[-window:]
        
    def get_average(self, window: Optional[int] = None) -> float:
        """Get average of recent values.
        
        Args:
            window: Optional window size
            
        Returns:
            Average value
        """
        values = self.get_values(window)
        return sum(values) / len(values) if values else 0.0
        
    def get_min(self, window: Optional[int] = None) -> float:
        """Get minimum of recent values.
        
        Args:
            window: Optional window size
            
        Returns:
            Minimum value
        """
        values = self.get_values(window)
        return min(values) if values else 0.0
        
    def get_max(self, window: Optional[int] = None) -> float:
        """Get maximum of recent values.
        
        Args:
            window: Optional window size
            
        Returns:
            Maximum value
        """
        values = self.get_values(window)
        return max(values) if values else 0.0


class PerformanceMonitor:
    """System performance monitoring."""
    
    def __init__(self, interval: float = 1.0):
        """Initialize performance monitor.
        
        Args:
            interval: Sampling interval in seconds
        """
        self.interval = interval
        self.logger = get_logger(self.__class__.__name__)
        
        self.process = psutil.Process()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        
        # Performance metrics
        self.cpu_percent = TimeSeriesMetric()
        self.memory_percent = TimeSeriesMetric()
        self.memory_rss = TimeSeriesMetric()
        self.thread_count = TimeSeriesMetric()
        self.open_files = TimeSeriesMetric()
        self.open_connections = TimeSeriesMetric()
        
    def start(self) -> None:
        """Start monitoring thread."""
        if self._running:
            return
            
        self._running = True
        self._thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True
        )
        self._thread.start()
        
    def stop(self) -> None:
        """Stop monitoring thread."""
        self._running = False
        if self._thread:
            self._thread.join()
            
    def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._running:
            try:
                # CPU usage
                self.cpu_percent.add(
                    self.process.cpu_percent()
                )
                
                # Memory usage
                memory = self.process.memory_info()
                self.memory_percent.add(
                    self.process.memory_percent()
                )
                self.memory_rss.add(
                    memory.rss / 1024 / 1024  # MB
                )
                
                # Thread count
                self.thread_count.add(
                    self.process.num_threads()
                )
                
                # Open resources
                self.open_files.add(
                    len(self.process.open_files())
                )
                self.open_connections.add(
                    len(self.process.connections())
                )
                
            except Exception as e:
                self.logger.error(f"Error in monitor loop: {str(e)}")
                
            time.sleep(self.interval)

    def get_stats(self) -> Dict[str, Any]:
        """Get current performance statistics.
        
        Returns:
            Dictionary with performance metrics
        """
        return {
            "cpu": {
                "current": self.cpu_percent.get_values(1)[0],
                "average": self.cpu_percent.get_average(10),
                "max": self.cpu_percent.get_max(60)
            },
            "memory": {
                "percent": {
                    "current": self.memory_percent.get_values(1)[0],
                    "average": self.memory_percent.get_average(10)
                },
                "rss_mb": {
                    "current": self.memory_rss.get_values(1)[0],
                    "average": self.memory_rss.get_average(10)
                }
            },
            "threads": {
                "current": self.thread_count.get_values(1)[0],
                "max": self.thread_count.get_max(60)
            },
            "resources": {
                "open_files": self.open_files.get_values(1)[0],
                "connections": self.open_connections.get_values(1)[0]
            }
        }


class StatsCollector:
    """Crawler statistics collection."""
    
    def __init__(self):
        """Initialize stats collector."""
        self.logger = get_logger(self.__class__.__name__)
        
        # General statistics
        self.stats: Dict[str, Any] = {
            "start_time": None,
            "finish_time": None,
            "elapsed_time": 0.0,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "retry_count": 0,
            "items_scraped": 0
        }
        
        # Domain-level statistics
        self.domain_stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "requests": 0,
                "successful": 0,
                "failed": 0,
                "items": 0,
                "avg_response_time": 0.0,
                "status_codes": defaultdict(int)
            }
        )
        
        # Time-series metrics
        self.requests_per_minute = TimeSeriesMetric()
        self.response_times = TimeSeriesMetric()
        self.errors_per_minute = TimeSeriesMetric()
        self.items_per_minute = TimeSeriesMetric()
        
        # Performance monitoring
        self.perf_monitor = PerformanceMonitor()
        
    def start(self) -> None:
        """Start statistics collection."""
        self.stats["start_time"] = time.time()
        self.perf_monitor.start()
        
    def finish(self) -> None:
        """Finish statistics collection."""
        self.stats["finish_time"] = time.time()
        self.stats["elapsed_time"] = (
            self.stats["finish_time"] - self.stats["start_time"]
        )
        self.perf_monitor.stop()
        
    def record_request(
        self,
        url: str,
        success: bool = True,
        response_time: float = 0.0,
        status_code: Optional[int] = None,
        retry: bool = False
    ) -> None:
        """Record request statistics.
        
        Args:
            url: Request URL
            success: Whether request succeeded
            response_time: Request duration
            status_code: HTTP status code
            retry: Whether this was a retry
        """
        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        
        # Update general stats
        self.stats["total_requests"] += 1
        if success:
            self.stats["successful_requests"] += 1
        else:
            self.stats["failed_requests"] += 1
        if retry:
            self.stats["retry_count"] += 1
            
        # Update domain stats
        domain_stat = self.domain_stats[domain]
        domain_stat["requests"] += 1
        if success:
            domain_stat["successful"] += 1
        else:
            domain_stat["failed"] += 1
        if status_code:
            domain_stat["status_codes"][status_code] += 1
            
        # Update response time metrics
        if response_time > 0:
            self.response_times.add(response_time)
            total_time = domain_stat["avg_response_time"] * (domain_stat["requests"] - 1)
            domain_stat["avg_response_time"] = (
                total_time + response_time
            ) / domain_stat["requests"]
            
        # Update time-series metrics
        self.requests_per_minute.add(1)
        if not success:
            self.errors_per_minute.add(1)
            
    def record_item(self, domain: str) -> None:
        """Record scraped item statistics.
        
        Args:
            domain: Domain item was scraped from
        """
        self.stats["items_scraped"] += 1
        self.domain_stats[domain]["items"] += 1
        self.items_per_minute.add(1)
        
    def get_current_stats(self) -> Dict[str, Any]:
        """Get current statistics.
        
        Returns:
            Dictionary with current statistics
        """
        current_time = time.time()
        elapsed = (
            current_time - self.stats["start_time"]
            if self.stats["start_time"]
            else 0
        )
        
        return {
            **self.stats,
            "current_time": current_time,
            "elapsed_time": elapsed,
            "requests_per_minute": self.requests_per_minute.get_average(1),
            "errors_per_minute": self.errors_per_minute.get_average(1),
            "items_per_minute": self.items_per_minute.get_average(1),
            "response_times": {
                "current": self.response_times.get_average(1),
                "average": self.response_times.get_average(),
                "min": self.response_times.get_min(),
                "max": self.response_times.get_max()
            },
            "domain_stats": dict(self.domain_stats),
            "performance": self.perf_monitor.get_stats()
        }
        
    def get_summary(self) -> str:
        """Get human-readable statistics summary.
        
        Returns:
            Formatted statistics summary
        """
        stats = self.get_current_stats()
        
        return (
            f"Crawler Statistics\n"
            f"-----------------\n"
            f"Runtime: {stats['elapsed_time']:.1f}s\n"
            f"Requests: {stats['total_requests']} total, "
            f"{stats['successful_requests']} successful, "
            f"{stats['failed_requests']} failed\n"
            f"Items: {stats['items_scraped']} scraped\n"
            f"Current Rate: {stats['requests_per_minute']:.1f} req/min, "
            f"{stats['items_per_minute']:.1f} items/min\n"
            f"Response Time: {stats['response_times']['average']:.3f}s avg, "
            f"{stats['response_times']['max']:.3f}s max\n"
            f"Memory Usage: {stats['performance']['memory']['rss_mb']['current']:.1f}MB\n"
            f"CPU Usage: {stats['performance']['cpu']['current']:.1f}%"
        )
