"""Tests for the CRDT toolkit and swarm replication of CRDTs."""

from __future__ import annotations

import itertools
import random

import pytest

from conftest import Cluster
from suprime.crdt import (
    GCounter,
    LWWMap,
    MVRegister,
    ORSet,
    PNCounter,
    VectorClock,
)
from suprime.replicate import CRDTReplicator


# -- vector clocks ---------------------------------------------------------

def test_vector_clock_causality():
    a = VectorClock().increment("x")
    b = VectorClock(dict(a.clock)).increment("x")
    assert a.compare(b) == "before"
    assert b.compare(a) == "after"

    c = VectorClock().increment("y")
    assert a.compare(c) == "concurrent"
    assert a.compare(VectorClock(dict(a.clock))) == "equal"


# -- counters --------------------------------------------------------------

def test_gcounter_merge_converges():
    a, b = GCounter("a"), GCounter("b")
    a.increment(3)
    b.increment(5)
    a.merge(b)
    b.merge(a)
    assert a.value == b.value == 8


def test_gcounter_merge_returns_false_if_no_change():
    a, b = GCounter("a"), GCounter("b")
    a.increment(3)
    b.increment(2)

    # Initial merge should return True
    assert a.merge(b) is True

    # Merging again with the same or lesser state should return False
    assert a.merge(b) is False

    # Merging an empty counter should return False
    assert a.merge(GCounter("c")) is False


def test_pncounter_inc_dec():
    a, b = PNCounter("a"), PNCounter("b")
    a.increment(10)
    a.decrement(3)
    b.increment(1)
    a.merge(b)
    b.merge(a)
    assert a.value == b.value == 8


def test_crdt_merge_is_order_independent():
    # three replicas each make a distinct increment; every merge order agrees
    results = set()
    for order in itertools.permutations(range(3)):
        reps = [GCounter(f"r{i}") for i in range(3)]
        for i, r in enumerate(reps):
            r.increment(i + 1)
        target = GCounter("t")
        for i in order:
            target.merge(reps[i])
        results.add(target.value)
    assert results == {6}


# -- OR-set ----------------------------------------------------------------

def test_orset_add_remove():
    s = ORSet("a")
    s.add("x")
    assert s.contains("x")
    s.remove("x")
    assert not s.contains("x")


def test_orset_concurrent_add_wins():
    a, b = ORSet("a"), ORSet("b")
    a.add("x")
    # b removes based on its (empty) observation, then a's add is merged in
    b.remove("x")
    a.merge(b)
    b.merge(a)
    # a's add token was never observed by b's remove, so the element survives
    assert a.contains("x")
    assert b.contains("x")
    assert a.elements() == b.elements() == {"x"}


# -- LWW map ---------------------------------------------------------------

def test_lwwmap_last_writer_wins():
    clock = [0.0]
    a = LWWMap("a", clock=lambda: clock[0])
    b = LWWMap("b", clock=lambda: clock[0])
    clock[0] = 1.0
    a.set("k", "old")
    clock[0] = 2.0
    b.set("k", "new")
    a.merge(b)
    b.merge(a)
    assert a.get("k") == b.get("k") == "new"


# -- multi-value register --------------------------------------------------

def test_mvregister_surfaces_concurrent_writes():
    a, b = MVRegister("a"), MVRegister("b")
    a.set("A")
    b.set("B")  # concurrent with a's write
    a.merge(b)
    assert set(a.values()) == {"A", "B"}  # both kept — a real conflict


def test_mvregister_causal_overwrite():
    a = MVRegister("a")
    a.set("first")
    a.set("second")  # causally after 'first'
    assert a.values() == ["second"]


def test_mvregister_digest_apply():
    a = MVRegister("a")
    a.set("first")
    b = MVRegister("b")
    b.set("second")
    a.merge(b)

    digest = a.digest()
    c = MVRegister("c")
    changed = c.apply_digest(digest)

    assert changed is True
    assert set(c.values()) == {"first", "second"}
    assert c.digest() == digest


# -- replication over the swarm --------------------------------------------

@pytest.mark.asyncio
async def test_crdt_replicates_across_swarm(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(4)]
    reps = [CRDTReplicator(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    counters = [r.register("votes", PNCounter(n.id)) for r, n in zip(reps, nodes)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    # each node casts a different number of votes
    counters[0].increment(2)
    counters[1].increment(5)
    counters[2].decrement(1)
    counters[3].increment(4)

    await cluster.settle(nodes, rounds=30)

    # the counter converges to the same swarm-wide total everywhere
    totals = {c.value for c in counters}
    assert totals == {2 + 5 - 1 + 4}


@pytest.mark.asyncio
async def test_orset_replicates_across_swarm(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    reps = [CRDTReplicator(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    sets = [r.register("members", ORSet(n.id)) for r, n in zip(reps, nodes)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    sets[0].add("alice")
    sets[1].add("bob")
    sets[2].add("carol")
    await cluster.settle(nodes, rounds=30)
    for s in sets:
        assert s.elements() == {"alice", "bob", "carol"}
