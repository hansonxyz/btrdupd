"""
Observability metrics for btrdupd

Provides structured metrics logging with optional Prometheus integration.
Metrics are tracked in-memory and can be exported via log messages or
HTTP endpoint.
"""

import time
import json
import logging
import threading
from collections import defaultdict
from typing import Dict, Optional, Any

logger = logging.getLogger('btrdupd.metrics')


class MetricsCollector:
    """Collects and exports metrics for btrdupd operations

    Tracks counters, gauges, and histograms for key operations:
    - files_scanned_total: Total files scanned during walks
    - files_hashed_total: Total files hashed
    - hash_failures_total: Hash operation failures
    - dedup_operations_total: Deduplication calls made
    - dedup_files_total: Files processed in dedup operations
    - potential_bytes_saved_total: Cumulative potential space savings
    - current_queue_depth: Current number of pending dedup candidates
    - last_scan_duration_seconds: Duration of last scan operation
    - snapshot_files_included_total: Snapshot files included in dedup
    """

    def __init__(self):
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._timestamps: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._session_start = time.time()

    def inc(self, name: str, amount: int = 1, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric

        Args:
            name: Metric name
            amount: Amount to increment (default 1)
            labels: Optional labels dict for metric dimensions
        """
        key = self._make_key(name, labels)
        with self._lock:
            self._counters[key] += amount

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric to a specific value

        Args:
            name: Metric name
            value: Value to set
            labels: Optional labels dict
        """
        key = self._make_key(name, labels)
        with self._lock:
            self._gauges[key] = value
            self._timestamps[key] = time.time()

    def observe_duration(self, name: str, start_time: float, labels: Optional[Dict[str, str]] = None):
        """Record a duration observation (for timing operations)

        Args:
            name: Metric name (e.g., 'scan_duration_seconds')
            start_time: Start timestamp from time.time()
            labels: Optional labels dict
        """
        duration = time.time() - start_time
        self.set_gauge(name, duration, labels)

    def _make_key(self, name: str, labels: Optional[Dict[str, str]] = None) -> str:
        """Create a unique key for a metric with labels"""
        if not labels:
            return name
        label_str = ','.join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f'{name}{{{label_str}}}'

    def get_counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> int:
        """Get current counter value"""
        key = self._make_key(name, labels)
        with self._lock:
            return self._counters.get(key, 0)

    def get_gauge(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        """Get current gauge value"""
        key = self._make_key(name, labels)
        with self._lock:
            return self._gauges.get(key, 0.0)

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics as a dictionary

        Returns:
            Dict with 'counters', 'gauges', and 'session_uptime_seconds' keys
        """
        with self._lock:
            return {
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'session_uptime_seconds': time.time() - self._session_start
            }

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus text format

        Returns:
            String in Prometheus exposition format
        """
        lines = []
        with self._lock:
            # Add session uptime
            lines.append('# HELP btrdupd_session_uptime_seconds Time since metrics collection started')
            lines.append('# TYPE btrdupd_session_uptime_seconds gauge')
            lines.append(f'btrdupd_session_uptime_seconds {time.time() - self._session_start:.2f}')
            lines.append('')

            # Export counters
            for key, value in sorted(self._counters.items()):
                metric_name = f'btrdupd_{key.split("{")[0]}'
                if '{' in key:
                    labels = key[key.index('{'):]
                    lines.append(f'{metric_name}{labels} {value}')
                else:
                    lines.append(f'{metric_name} {value}')

            lines.append('')

            # Export gauges
            for key, value in sorted(self._gauges.items()):
                metric_name = f'btrdupd_{key.split("{")[0]}'
                if '{' in key:
                    labels = key[key.index('{'):]
                    lines.append(f'{metric_name}{labels} {value:.6f}')
                else:
                    lines.append(f'{metric_name} {value:.6f}')

        return '\n'.join(lines)

    def log_metrics_summary(self, level: int = logging.INFO):
        """Log a summary of current metrics

        Args:
            level: Logging level to use
        """
        metrics = self.get_all_metrics()
        logger.log(level, "Metrics summary: %s", json.dumps(metrics, indent=2))

    def log_structured(self, event: str, **kwargs):
        """Log a structured event with metrics context

        Args:
            event: Event name/description
            **kwargs: Additional key-value pairs to include
        """
        data = {
            'event': event,
            'timestamp': time.time(),
            **kwargs
        }
        logger.info("METRIC: %s", json.dumps(data))

    def reset(self):
        """Reset all metrics (for testing or session restart)"""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timestamps.clear()
            self._session_start = time.time()


# Global metrics instance
_metrics: Optional[MetricsCollector] = None


def get_metrics() -> MetricsCollector:
    """Get the global metrics collector instance"""
    global _metrics
    if _metrics is None:
        _metrics = MetricsCollector()
    return _metrics


# Convenience functions that use global instance
def inc(name: str, amount: int = 1, labels: Optional[Dict[str, str]] = None):
    """Increment a counter metric"""
    get_metrics().inc(name, amount, labels)


def set_gauge(name: str, value: float, labels: Optional[Dict[str, str]] = None):
    """Set a gauge metric"""
    get_metrics().set_gauge(name, value, labels)


def observe_duration(name: str, start_time: float, labels: Optional[Dict[str, str]] = None):
    """Record a duration observation"""
    get_metrics().observe_duration(name, start_time, labels)


def log_structured(event: str, **kwargs):
    """Log a structured event"""
    get_metrics().log_structured(event, **kwargs)


def export_prometheus() -> str:
    """Export metrics in Prometheus format"""
    return get_metrics().export_prometheus()


class MetricsContext:
    """Context manager for timing operations

    Usage:
        with MetricsContext('scan_duration_seconds'):
            # perform scan
    """

    def __init__(self, metric_name: str, labels: Optional[Dict[str, str]] = None):
        self.metric_name = metric_name
        self.labels = labels
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        observe_duration(self.metric_name, self.start_time, self.labels)
        return False
