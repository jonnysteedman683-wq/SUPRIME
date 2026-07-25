"""Tests for the SwarmNode integration upgrades."""

from __future__ import annotations

import random

import pytest

from conftest import Cluster, flush
from suprime import crypto
from suprime.node import SwarmNode
from suprime.replicate import CRDTReplicator
from suprime.crdt import PNCounter
from suprime.security import secure_transport
from suprime.transport import InMemoryTransport


@pytest.mark.asyncio
async def test_persist_dir_restores_state_across_restarts(tmp_path):
    reg: dict = {}
    d = str(tmp_path / "node0")

    node = SwarmNode(transport=InMemoryTransport("a", registry=reg), node_id="a", persist_dir=d)
    await node.start(auto=False)
    node.store.set("k", "durable")
    await node.stop()

    # a brand-new node instance over the same dir recovers the state
    node2 = SwarmNode(transport=InMemoryTransport("a", registry=reg), node_id="a", persist_dir=d)
    await node2.start(auto=False)
    assert node2.store.get("k") == "durable"
    assert node2.persistence is not None
    await node2.stop()


@pytest.mark.asyncio
async def test_secure_transport_helper_sign_then_encrypt():
    reg: dict = {}
    key = b"z" * 32
    sk_a, pk_a = crypto.generate_keypair()
    sk_b, pk_b = crypto.generate_keypair()
    a_id, b_id = crypto.fingerprint(pk_a), crypto.fingerprint(pk_b)

    a = SwarmNode(
        transport=secure_transport(InMemoryTransport(a_id, registry=reg), sk=sk_a, pk=pk_a, cluster_key=key),
        node_id=a_id,
    )
    b = SwarmNode(
        transport=secure_transport(InMemoryTransport(b_id, registry=reg), sk=sk_b, pk=pk_b, cluster_key=key),
        node_id=b_id,
    )
    await a.start(auto=False)
    await b.start(auto=False)
    b._seeds = [a.address]
    await b._bootstrap()
    await flush()
    for _ in range(6):
        await a.tick(); await b.tick(); await flush()

    # signed + encrypted channel still forms membership end-to-end
    assert b_id in a.peers and a_id in b.peers
    await a.stop(); await b.stop()


@pytest.mark.asyncio
async def test_crdt_replicator_goes_quiet_when_converged(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    reps = [CRDTReplicator(n, rng=random.Random(i), full_sync_every=10_000) for i, n in enumerate(nodes)]
    counters = [r.register("c", PNCounter(n.id)) for r, n in zip(reps, nodes)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    counters[0].increment(5)
    await cluster.settle(nodes, rounds=25)
    assert all(c.value == 5 for c in counters)  # converged

    # once converged and stable, dirty-tracking should suppress further syncs
    before = sum(r.syncs_sent for r in reps)
    await cluster.settle(nodes, rounds=10)
    after = sum(r.syncs_sent for r in reps)
    # no CRDT changed, so no CRDT-sync messages should have been sent at all
    assert after == before


@pytest.mark.asyncio
async def test_bootstrap_ignores_dead_seeds():
    reg: dict = {}
    node = SwarmNode(transport=InMemoryTransport("a", registry=reg), node_id="a", seeds=["dead_seed", "alive_seed"])

    # Mock the transport send method to raise an exception for the dead seed
    original_send = node._transport.send
    async def mock_send(address, message):
        if address == "dead_seed":
            raise Exception("Simulated connection error")
        return await original_send(address, message)

    node._transport.send = mock_send

    # Verify that start (which calls _bootstrap) completes without raising the Exception
    await node.start(auto=False)

    # The node should be running
    assert node._running is True
    await node.stop()
