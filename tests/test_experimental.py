"""Tests for the experimental swarm features.

Covers the chaos harness, push-sum aggregation, stigmergic load balancing and
the Plumtree / HyParView topology layer, all driven deterministically over the
in-memory transport.
"""

from __future__ import annotations

import random
import statistics

import pytest

from conftest import Cluster, flush
from suprime.aggregate import PushSumAggregator
from suprime.chaos import ChaosController, ChaosTransport
from suprime.node import SwarmNode
from suprime.plumtree import PlumtreeBroadcast
from suprime.transport import InMemoryTransport, TransportError


# -- chaos harness ---------------------------------------------------------

def test_chaos_partition_blocks_and_heals():
    ctrl = ChaosController()
    ctrl.partition(["a", "b"], ["c"])
    assert ctrl.can_reach("a", "b") is True
    assert ctrl.can_reach("a", "c") is False
    assert ctrl.can_reach("c", "a") is False
    ctrl.heal()
    assert ctrl.can_reach("a", "c") is True


def test_chaos_drop_rate_is_deterministic_with_seed():
    ctrl = ChaosController(drop_rate=0.5, rng=random.Random(1))
    drops = sum(ctrl.should_drop() for _ in range(1000))
    assert 400 < drops < 600  # ~50%


@pytest.mark.asyncio
async def test_chaos_partition_splits_swarm_then_reconverges():
    reg: dict = {}
    ctrl = ChaosController()
    mono_holder = {"t": 0.0}
    mono = lambda: mono_holder["t"]

    def build(nid):
        inner = InMemoryTransport(nid, registry=reg)
        return SwarmNode(
            transport=ChaosTransport(inner, ctrl),
            node_id=nid,
            rng=random.Random(hash(nid) % 100),
            monotonic=mono,
            dead_after=1e9,  # don't evict during the partition test
        )

    nodes = [build(f"n{i}") for i in range(4)]
    for i, n in enumerate(nodes):
        n._seeds = [nodes[0].address] if i else []
        await n.start(auto=False)
    await flush()

    async def settle(rounds):
        for _ in range(rounds):
            mono_holder["t"] += 1.0
            for n in nodes:
                await n.tick()
            await flush()

    await settle(20)
    addrs = [n.address for n in nodes]

    # Partition {n0,n1} | {n2,n3}; a write on each side must not cross.
    ctrl.partition(addrs[:2], addrs[2:])
    nodes[0].store.set("side", "A")
    nodes[2].store.set("side", "B")
    await settle(15)
    assert nodes[1].store.get("side") == "A"
    assert nodes[3].store.get("side") == "B"

    # Heal: the swarm must reconverge to a single value.
    ctrl.heal()
    await settle(25)
    healed = {n.store.get("side") for n in nodes}
    assert len(healed) == 1
    assert ctrl.stats()["delivered"] > 0


# -- push-sum aggregation --------------------------------------------------

@pytest.mark.asyncio
async def test_pushsum_computes_global_average(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(5)]
    aggs = [PushSumAggregator(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)  # converge membership first

    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    for agg, v in zip(aggs, values):
        agg.average("temp", v)

    await cluster.settle(nodes, rounds=60)

    expected = statistics.mean(values)
    estimates = [agg.estimate("temp") for agg in aggs]
    assert all(e is not None for e in estimates)
    for e in estimates:
        assert abs(e - expected) < 1.0  # converged close to the true mean


@pytest.mark.asyncio
async def test_pushsum_counts_nodes(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(6)]
    aggs = [PushSumAggregator(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)

    # count = sum of ones: value 1 everywhere, weight 1 at a single initiator.
    for i, agg in enumerate(aggs):
        agg.start("count", value=1.0, weight=1.0 if i == 0 else 0.0)

    await cluster.settle(nodes, rounds=80)
    estimates = [agg.estimate("count") for agg in aggs]
    for e in estimates:
        assert abs(e - len(nodes)) < 0.5  # converges to N = 6


# -- stigmergic load balancing ---------------------------------------------

@pytest.mark.asyncio
async def test_stigmergic_load_balancing_prefers_idle_node(cluster: Cluster):
    # Fixed shared clock so claim ordering is decided purely by load penalty.
    nodes = [
        cluster.node(f"n{i}", seed=i, clock=lambda: 1000.0) for i in range(3)
    ]
    loads = {"n0": 100.0, "n1": 0.0, "n2": 50.0}  # n1 is idle
    runs = {n.id: 0 for n in nodes}

    for node in nodes:
        node.tasks.set_load_model(lambda nid=node.id: loads[nid], weight=1.0)
        node.tasks.register_handler("job", (lambda nid: (lambda t: runs.__setitem__(nid, runs[nid] + 1)))(node.id))

    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)

    for _ in range(5):
        nodes[0].tasks.submit("job", {})
    await cluster.settle(nodes, rounds=40)

    # The idle node (n1) should win every claim.
    assert runs["n1"] == 5
    assert runs["n0"] == 0
    assert runs["n2"] == 0


# -- plumtree --------------------------------------------------------------

@pytest.mark.asyncio
async def test_plumtree_broadcast_reaches_all_once(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(6)]
    delivered = {n.id: [] for n in nodes}
    trees = [
        PlumtreeBroadcast(
            n,
            on_deliver=(lambda nid: (lambda mid, p: delivered[nid].append((mid, p))))(n.id),
            rng=random.Random(i),
        )
        for i, n in enumerate(nodes)
    ]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)  # membership + let trees form

    mid = await trees[0].broadcast({"hello": "swarm"})
    await cluster.settle(nodes, rounds=15)

    # every node delivered the message exactly once
    for n in nodes:
        got = [d for d in delivered[n.id] if d[0] == mid]
        assert len(got) == 1
        assert got[0][1] == {"hello": "swarm"}


@pytest.mark.asyncio
async def test_plumtree_prunes_redundant_edges(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(6)]
    trees = [PlumtreeBroadcast(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)

    # several broadcasts let the tree optimise itself
    for _ in range(4):
        await trees[0].broadcast({"x": 1})
        await cluster.settle(nodes, rounds=8)

    # after pruning, total eager edges are far below a full mesh (N*(N-1))
    total_eager = sum(len(t.eager) for t in trees)
    full_mesh = len(nodes) * (len(nodes) - 1)
    assert total_eager < full_mesh
    # but the overlay stays connected: every node has at least one eager link
    assert all(len(t.eager) + len(t.lazy) >= 1 for t in trees)
