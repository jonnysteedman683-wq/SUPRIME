"""Tests for hardening: crash isolation, GC, and bounded caches."""

from __future__ import annotations

import pytest

from conftest import Cluster, flush
from suprime.message import Message
from suprime.store import DistributedStore


# -- crash isolation -------------------------------------------------------

@pytest.mark.asyncio
async def test_malformed_message_does_not_crash_node(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(2)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=10)

    # a GOSSIP whose membership entry is missing required fields would raise
    # inside apply_digest — hardening must catch it, not crash the node
    bad = Message(type="gossip", src="attacker", payload={"membership": [{"node_id": "x"}]})
    await nodes[0]._on_message(bad)
    # a totally unknown type with no handler is fine too
    await nodes[0]._on_message(Message(type="weird", src="x", payload={}))

    # node still works: normal replication proceeds
    assert nodes[0].metrics.counter("bad_messages") >= 1
    nodes[1].store.set("k", "v")
    await cluster.settle(nodes, rounds=15)
    assert nodes[0].store.get("k") == "v"


@pytest.mark.asyncio
async def test_throwing_app_handler_is_isolated(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(2)]
    good_got = []

    async def boom(_msg):
        raise RuntimeError("handler blew up")

    async def good(msg):
        good_got.append(msg.payload)

    # two handlers on the same type; the first throws, the second must still run
    nodes[1].on("evt", boom)
    nodes[1].on("evt", good)
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=10)

    await nodes[0].send(nodes[1].id, "evt", {"x": 1})
    await flush()
    assert good_got == [{"x": 1}]
    assert nodes[1].metrics.counter("handler_errors") >= 1


@pytest.mark.asyncio
async def test_throwing_tick_hook_is_isolated(cluster: Cluster):
    nodes = [cluster.node("n0", seed=0)]

    async def bad_hook():
        raise RuntimeError("hook failed")

    ran = []

    async def good_hook():
        ran.append(1)

    nodes[0].on_tick(bad_hook)
    nodes[0].on_tick(good_hook)
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=5)

    assert ran  # good hook kept running despite the bad one
    assert nodes[0].metrics.counter("hook_errors") >= 1


# -- bounded de-dup cache --------------------------------------------------

@pytest.mark.asyncio
async def test_seen_cache_is_bounded(cluster: Cluster):
    node = cluster.node("n0", seed=0)
    node._seen_cap = 100
    await node.start(auto=False)
    for i in range(500):
        await node._on_message(Message(type="x", src="s", payload={}, id=f"m{i}"))
    assert len(node._seen_messages) <= 100
    assert len(node._seen_order) <= 100


# -- store tombstone GC ----------------------------------------------------

def test_collect_garbage_purges_old_tombstones():
    t = {"v": 1000.0}
    s = DistributedStore("n1", clock=lambda: t["v"])
    s.set("keep", 1)
    s.set("gone", 2)
    s.delete("gone")           # tombstone written at t=1000
    assert s.tombstones() == 1

    t["v"] = 1000.0 + 50       # 50s later
    # too young to purge with a 100s floor
    assert s.collect_garbage(min_age=100) == 0
    # old enough with a 30s floor
    assert s.collect_garbage(min_age=30) == 1
    assert s.tombstones() == 0
    assert s.get("keep") == 1  # live data untouched
    assert s.get("gone") is None


@pytest.mark.asyncio
async def test_node_periodic_tombstone_gc(cluster: Cluster):
    t = {"v": 0.0}
    node = cluster.node("n0", seed=0, tombstone_gc_after=10.0, clock=lambda: t["v"])
    await node.start(auto=False)
    node.store.set("a", 1)
    node.store.delete("a")
    assert node.store.tombstones() == 1

    t["v"] = 100.0             # well past the 10s GC threshold
    for _ in range(50):        # GC runs every 50 ticks
        await node.tick()
    assert node.store.tombstones() == 0
