"""Tests for adaptive gossip: size-scaled fanout + churn boost."""

from __future__ import annotations

import pytest

from conftest import Cluster
from suprime.gossip import GossipService
from suprime.peers import PeerTable
from suprime.store import DistributedStore


def _gossip(n_peers, **kw):
    pt = PeerTable("self")
    for i in range(n_peers):
        pt.merge(f"p{i}", f"a{i}", 1)
    return GossipService("self", "addr", pt, DistributedStore("self"), **kw)


def test_fixed_fanout_when_not_adaptive():
    g = _gossip(100, fanout=3, adaptive=False)
    assert g.effective_fanout() == 3


def test_adaptive_fanout_scales_logarithmically():
    small = _gossip(2, fanout=3, adaptive=True, max_fanout=8)
    mid = _gossip(30, fanout=3, adaptive=True, max_fanout=8)
    big = _gossip(500, fanout=3, adaptive=True, max_fanout=8)
    assert small.effective_fanout() == 3           # never below the floor
    assert 3 < mid.effective_fanout() <= 8
    assert mid.effective_fanout() < big.effective_fanout() or big.effective_fanout() == 8
    assert big.effective_fanout() == 8             # capped at max_fanout


def test_churn_boost_raises_fanout_temporarily():
    g = _gossip(4, fanout=2, adaptive=True, max_fanout=8)
    assert g.effective_fanout() == 3               # small swarm, size-based
    g.boost(rounds=2)
    assert g.effective_fanout() == 8               # boosted to max
    # select_targets consumes the boost each round
    g.select_targets(); g.select_targets()
    assert g.effective_fanout() < 8                # boost expired → back to size-based


@pytest.mark.asyncio
async def test_adaptive_swarm_converges(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i, adaptive_gossip=True, max_fanout=8) for i in range(16)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=25)

    # full membership discovered, and fanout adapted above the default floor
    assert all(len(n.peers.alive()) == len(nodes) - 1 for n in nodes)
    assert nodes[0].gossip.effective_fanout() > 3

    # a write propagates to the whole swarm
    nodes[7].store.set("k", "v")
    await cluster.settle(nodes, rounds=20)
    assert all(n.store.get("k") == "v" for n in nodes)
