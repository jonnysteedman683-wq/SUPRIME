"""Tests for the distributed KV database."""

from __future__ import annotations

import pytest

from conftest import Cluster, flush
from suprime.kvstore import KVStore


@pytest.mark.asyncio
async def test_local_put_get_and_scan(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    kvs = [KVStore(n) for n in nodes]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    kvs[0].put("user:1", {"name": "ada"})
    kvs[0].put("user:2", {"name": "bob"})
    await cluster.settle(nodes, rounds=20)

    # eventual consistency: the write shows up on other nodes
    assert kvs[1].get("user:1") == {"name": "ada"}
    # namespaced scan works locally
    assert kvs[2].scan("user:") == [("user:1", {"name": "ada"}), ("user:2", {"name": "bob"})]


@pytest.mark.asyncio
async def test_quorum_put_acked_by_replicas(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    kvs = [KVStore(n) for n in nodes]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    acks = await kvs[0].quorum_put("k", "v", w=3)
    assert acks == 3  # self + 2 replicas acknowledged synchronously
    # the value is immediately present on the replicas that acked
    assert nodes[1].store.get("kv/k") == "v"
    assert nodes[2].store.get("kv/k") == "v"


@pytest.mark.asyncio
async def test_quorum_get_returns_newest_and_repairs(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    kvs = [KVStore(n) for n in nodes]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    # write only to node 2's local replica (no propagation yet)
    kvs[2].put("k", "fresh")
    await flush()

    # node 0 has no value yet locally...
    assert nodes[0].store.get("kv/k") is None
    # ...but a quorum read finds the newest version and repairs node 0
    value = await kvs[0].quorum_get("k", r=3)
    assert value == "fresh"
    assert nodes[0].store.get("kv/k") == "fresh"  # read-repaired locally


@pytest.mark.asyncio
async def test_quorum_read_your_writes(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    kvs = [KVStore(n) for n in nodes]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    # W + R > N (3 + 1 > 3) guarantees the read sees the write
    await kvs[0].quorum_put("x", 100, w=3)
    val = await kvs[1].quorum_get("x", r=1)
    assert val == 100
