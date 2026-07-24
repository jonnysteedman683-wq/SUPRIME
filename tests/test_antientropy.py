"""Tests for Merkle-style anti-entropy delta reconciliation."""

from __future__ import annotations

import pytest

from conftest import Cluster
from suprime.antientropy import (
    AntiEntropy,
    bucket_hashes,
    diff_buckets,
    entries_for_buckets,
)
from suprime.store import DistributedStore


def test_bucket_hashes_match_for_identical_stores():
    a = DistributedStore("a")
    b = DistributedStore("b")
    for i in range(50):
        e = a.set(f"k{i}", i)
        b.merge_entry(f"k{i}", e)  # b has identical versions
    ha = bucket_hashes(a.digest(), 16)
    hb = bucket_hashes(b.digest(), 16)
    assert diff_buckets(ha, hb) == set()


def test_diff_isolates_changed_bucket():
    a = DistributedStore("a")
    b = DistributedStore("b")
    for i in range(50):
        e = a.set(f"k{i}", i)
        b.merge_entry(f"k{i}", e)
    a.set("k7", "changed")  # one key diverges

    n = 16
    ha = bucket_hashes(a.digest(), n)
    hb = bucket_hashes(b.digest(), n)
    mismatched = diff_buckets(ha, hb)
    assert len(mismatched) == 1  # only one bucket differs

    # reconciling just that bucket carries far fewer than all 50 entries
    delta = entries_for_buckets(a.digest(), mismatched, n)
    assert len(delta) < 50
    b.apply_digest(delta)
    assert b.get("k7") == "changed"


@pytest.mark.asyncio
async def test_antientropy_converges_with_small_transfer(cluster: Cluster):
    # gossip carries membership only; state is reconciled by anti-entropy.
    nodes = [cluster.node(f"n{i}", seed=i, gossip_store=False) for i in range(3)]
    aes = [AntiEntropy(n, n_buckets=32) for n in nodes]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    # seed a shared dataset on node 0 and let it reconcile everywhere
    for i in range(40):
        nodes[0].store.set(f"key{i}", i)
    await cluster.settle(nodes, rounds=40)
    for n in nodes:
        assert n.store.get("key0") == 0
        assert n.store.get("key39") == 39

    transferred_after_sync = sum(a.entries_received for a in aes)

    # now change exactly one key; reconciliation should move very little data
    baseline = sum(a.entries_received for a in aes)
    nodes[1].store.set("key20", "updated")
    await cluster.settle(nodes, rounds=25)
    for n in nodes:
        assert n.store.get("key20") == "updated"

    delta_transfer = sum(a.entries_received for a in aes) - baseline
    # far less than re-shipping the whole 40-key dataset to every node
    assert 0 < delta_transfer < 40
    assert transferred_after_sync > 0
