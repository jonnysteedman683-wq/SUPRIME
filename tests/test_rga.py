"""Tests for the RGA collaborative sequence CRDT."""

from __future__ import annotations

import random

import pytest

from conftest import Cluster
from suprime.rga import RGA
from suprime.replicate import CRDTReplicator


def test_rga_local_edits():
    doc = RGA("a")
    for ch in "hello":
        doc.append(ch)
    assert doc.to_string() == "hello"
    doc.delete(0)
    assert doc.to_string() == "ello"


def test_rga_concurrent_inserts_converge():
    a = RGA("a")
    b = RGA("b")
    for ch in "abc":
        a.append(ch)
    b.merge(a)  # b starts from "abc"

    # concurrent edits: a appends "X", b appends "Y" at the same tail
    a.append("X")
    b.append("Y")

    a.merge(b)
    b.merge(a)
    # both replicas converge to identical content and ordering
    assert a.to_string() == b.to_string()
    assert set(a.to_string()) == set("abcXY")


def test_rga_merge_is_idempotent():
    a = RGA("a")
    for ch in "hi":
        a.append(ch)
    b = RGA("b")
    b.merge(a)
    before = b.to_string()
    b.merge(a)  # applying the same state again changes nothing
    assert b.to_string() == before == "hi"


def test_rga_concurrent_delete_and_insert():
    a = RGA("a")
    for ch in "cat":
        a.append(ch)
    b = RGA("b")
    b.merge(a)
    a.delete(1)          # a removes 'a' -> "ct"
    b.insert(2, "z")     # b inserts before 't' region -> "caz"t area
    a.merge(b)
    b.merge(a)
    assert a.to_string() == b.to_string()


@pytest.mark.asyncio
async def test_rga_replicates_across_swarm(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    reps = [CRDTReplicator(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    docs = [r.register("doc", RGA(n.id)) for r, n in zip(reps, nodes)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    # three nodes each contribute concurrently
    docs[0].append("A")
    docs[1].append("B")
    docs[2].append("C")
    await cluster.settle(nodes, rounds=40)

    strings = {d.to_string() for d in docs}
    assert len(strings) == 1  # all replicas converged to identical text
    assert set(next(iter(strings))) == {"A", "B", "C"}
