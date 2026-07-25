"""Tests for KV database TTL expiry, range queries and secondary indexes."""

from __future__ import annotations

import pytest

from conftest import Cluster
from suprime.kvstore import KVStore


# -- TTL expiry ------------------------------------------------------------

@pytest.mark.asyncio
async def test_ttl_expires_on_access(cluster: Cluster):
    t = {"v": 1000.0}
    nodes = [cluster.node("n0", seed=0)]
    await cluster.start_chain(nodes)
    db = KVStore(nodes[0], clock=lambda: t["v"])

    db.put("session", "alice", ttl=30)
    assert db.get("session") == "alice"
    assert 0 < db.ttl("session") <= 30

    t["v"] += 31  # advance past expiry
    assert db.get("session") is None      # lazily reaped on read
    assert "session" not in db.keys()


@pytest.mark.asyncio
async def test_ttl_none_never_expires(cluster: Cluster):
    t = {"v": 0.0}
    nodes = [cluster.node("n0", seed=0)]
    await cluster.start_chain(nodes)
    db = KVStore(nodes[0], clock=lambda: t["v"])
    db.put("permanent", 42)
    t["v"] += 10_000
    assert db.get("permanent") == 42
    assert db.ttl("permanent") is None


@pytest.mark.asyncio
async def test_sweep_expired(cluster: Cluster):
    t = {"v": 0.0}
    nodes = [cluster.node("n0", seed=0)]
    await cluster.start_chain(nodes)
    db = KVStore(nodes[0], clock=lambda: t["v"])
    db.put("a", 1, ttl=5)
    db.put("b", 2, ttl=50)
    db.put("c", 3)  # no ttl
    t["v"] = 10
    assert db.sweep_expired() == 1  # only "a" expired
    assert sorted(db.keys()) == ["b", "c"]


@pytest.mark.asyncio
async def test_ttl_replicates_consistently(cluster: Cluster):
    # A shared clock stands in for loosely-synced wall clocks across nodes.
    t = {"v": 500.0}
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    await cluster.start_chain(nodes)
    dbs = [KVStore(n, clock=lambda: t["v"]) for n in nodes]
    await cluster.settle(nodes, rounds=15)

    dbs[0].put("token", "xyz", ttl=20)
    await cluster.settle(nodes, rounds=20)
    assert dbs[1].get("token") == "xyz"       # value + expiry replicated

    t["v"] += 25                               # everyone is now past expiry
    assert dbs[1].get("token") is None
    assert dbs[2].get("token") is None


# -- range queries ---------------------------------------------------------

@pytest.mark.asyncio
async def test_range_query(cluster: Cluster):
    nodes = [cluster.node("n0", seed=0)]
    await cluster.start_chain(nodes)
    db = KVStore(nodes[0])
    for k in ("apple", "banana", "cherry", "date", "elderberry"):
        db.put(k, len(k))
    assert [k for k, _ in db.range("banana", "date")] == ["banana", "cherry"]
    assert db.range("z", "zz") == []


# -- secondary indexes -----------------------------------------------------

@pytest.mark.asyncio
async def test_secondary_index_query_update_delete(cluster: Cluster):
    nodes = [cluster.node("n0", seed=0)]
    await cluster.start_chain(nodes)
    db = KVStore(nodes[0])
    db.create_index("by_city", lambda v: v["city"])

    db.put("u1", {"name": "ada", "city": "london"})
    db.put("u2", {"name": "bob", "city": "paris"})
    db.put("u3", {"name": "cara", "city": "london"})

    assert db.query_index("by_city", "london") == ["u1", "u3"]
    assert db.query_index("by_city", "paris") == ["u2"]

    # updating a value moves it between index buckets
    db.put("u1", {"name": "ada", "city": "paris"})
    assert db.query_index("by_city", "london") == ["u3"]
    assert sorted(db.query_index("by_city", "paris")) == ["u1", "u2"]

    # deleting removes it from the index
    db.delete("u2")
    assert db.query_index("by_city", "paris") == ["u1"]


@pytest.mark.asyncio
async def test_index_backfills_existing_values(cluster: Cluster):
    nodes = [cluster.node("n0", seed=0)]
    await cluster.start_chain(nodes)
    db = KVStore(nodes[0])
    db.put("p1", {"kind": "fruit"})
    db.put("p2", {"kind": "veg"})
    db.put("p3", {"kind": "fruit"})
    # index created *after* the data exists → must backfill
    db.create_index("by_kind", lambda v: v["kind"])
    assert db.query_index("by_kind", "fruit") == ["p1", "p3"]


@pytest.mark.asyncio
async def test_index_replicates_across_swarm(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    await cluster.start_chain(nodes)
    dbs = [KVStore(n) for n in nodes]
    dbs[0].create_index("by_role", lambda v: v["role"])
    dbs[0].put("x", {"role": "admin"})
    dbs[0].put("y", {"role": "user"})
    await cluster.settle(nodes, rounds=25)

    # index entries live in the replicated store, so another node can query them
    assert dbs[2].query_index("by_role", "admin") == ["x"]
