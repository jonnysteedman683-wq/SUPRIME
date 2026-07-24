"""Lightweight in-process metrics and structured logging.

A dependency-free :class:`MetricsRegistry` of monotonic **counters** and
point-in-time **gauges**, plus a :func:`prometheus_format` exporter so a node's
health can be scraped, printed or asserted on in tests. :class:`StructuredLogger`
emits one JSON object per event for machine-readable logs.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Callable, Dict, Optional


class MetricsRegistry:
    """A small registry of named counters and gauges."""

    def __init__(self) -> None:
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._gauge_fns: Dict[str, Callable[[], float]] = {}

    def inc(self, name: str, amount: float = 1.0) -> None:
        self._counters[name] = self._counters.get(name, 0.0) + amount

    def set_gauge(self, name: str, value: float) -> None:
        self._gauges[name] = value

    def gauge_from(self, name: str, fn: Callable[[], float]) -> None:
        """Register a gauge computed on demand at snapshot time."""
        self._gauge_fns[name] = fn

    def counter(self, name: str) -> float:
        return self._counters.get(name, 0.0)

    def gauge(self, name: str) -> Optional[float]:
        if name in self._gauge_fns:
            return self._gauge_fns[name]()
        return self._gauges.get(name)

    def snapshot(self) -> Dict[str, float]:
        out: Dict[str, float] = dict(self._counters)
        out.update(self._gauges)
        for name, fn in self._gauge_fns.items():
            try:
                out[name] = fn()
            except Exception:
                pass
        return out


def prometheus_format(snapshot: Dict[str, float], prefix: str = "suprime") -> str:
    """Render a metrics snapshot in Prometheus text exposition format."""
    lines = []
    for name, value in sorted(snapshot.items()):
        metric = f"{prefix}_{name}".replace(".", "_").replace("-", "_")
        lines.append(f"{metric} {value}")
    return "\n".join(lines) + "\n"


class StructuredLogger:
    """Emits one JSON log record per event to a stream."""

    def __init__(self, node_id: str, stream=None, enabled: bool = False) -> None:
        self._node_id = node_id
        self._stream = stream or sys.stderr
        self.enabled = enabled

    def log(self, event: str, **fields: Any) -> None:
        if not self.enabled:
            return
        record = {"ts": round(time.time(), 3), "node": self._node_id, "event": event}
        record.update(fields)
        self._stream.write(json.dumps(record, separators=(",", ":")) + "\n")
        self._stream.flush()
