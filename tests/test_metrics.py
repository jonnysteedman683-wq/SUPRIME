"""Tests for metrics and structured logging."""

from __future__ import annotations

import io

import pytest

from conftest import Cluster
from suprime.metrics import MetricsRegistry, StructuredLogger, prometheus_format


def test_metrics_registry_counters_and_gauges():
    m = MetricsRegistry()
    m.inc("hits")
    m.inc("hits", 4)
    m.set_gauge("temp", 20.5)
    m.gauge_from("computed", lambda: 42.0)
    assert m.counter("hits") == 5
    assert m.gauge("temp") == 20.5
    assert m.gauge("computed") == 42.0
    snap = m.snapshot()
    assert snap["hits"] == 5 and snap["computed"] == 42.0


def test_prometheus_format():
    text = prometheus_format({"messages.received": 3, "peers-alive": 2})
    assert "suprime_messages_received 3" in text
    assert "suprime_peers_alive 2" in text


def test_structured_logger_emits_json_when_enabled():
    buf = io.StringIO()
    log = StructuredLogger("n1", stream=buf, enabled=True)
    log.log("joined", peer="n2")
    out = buf.getvalue()
    assert '"event":"joined"' in out
    assert '"node":"n1"' in out
    assert '"peer":"n2"' in out


def test_structured_logger_silent_when_disabled():
    buf = io.StringIO()
    log = StructuredLogger("n1", stream=buf, enabled=False)
    log.log("x")
    assert buf.getvalue() == ""


@pytest.mark.asyncio
async def test_node_metrics_populate(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)
    nodes[0].store.set("k", "v")
    await cluster.settle(nodes, rounds=10)

    snap = nodes[0].metrics.snapshot()
    assert snap["ticks"] > 0
    assert snap["gossip_sent"] > 0
    assert snap["messages_received"] > 0
    assert snap["peers_alive"] == 2      # gauge reflects live membership
    assert snap["store_keys"] >= 1
