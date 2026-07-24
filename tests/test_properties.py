"""Property-based tests (Hypothesis) for the CRDT merge laws.

A state-based CRDT must have a merge that is **commutative**, **associative**
and **idempotent**; those three laws are exactly what guarantees replicas
converge under arbitrary gossip ordering, loss and duplication. Rather than
trust a handful of examples, we let Hypothesis generate thousands of random
operation histories and assert the laws hold for every one.
"""

from __future__ import annotations

import copy

from hypothesis import given, settings
from hypothesis import strategies as st

from suprime.crdt import GCounter, LWWMap, ORSet, PNCounter
from suprime.rga import RGA


# Strategies for building up CRDT replicas from random operations.
_node_ids = st.sampled_from(["a", "b", "c", "d"])
_ops = st.lists(st.tuples(_node_ids, st.integers(min_value=1, max_value=5)), max_size=25)


def _build_pncounter(ops, incdec):
    c = PNCounter("seed")
    replicas = {}
    for node, amt in ops:
        r = replicas.setdefault(node, PNCounter(node))
        if incdec:
            r.increment(amt)
        else:
            r.decrement(amt)
    return list(replicas.values())


@settings(max_examples=200, deadline=None)
@given(ops_a=_ops, ops_b=_ops)
def test_pncounter_merge_is_commutative(ops_a, ops_b):
    def make(ops):
        c = PNCounter("x")
        for node, amt in ops:
            (c.increment if node in ("a", "b") else c.decrement)(amt)
        return c

    a1, b1 = make(ops_a), make(ops_b)
    a2, b2 = copy.deepcopy(a1), copy.deepcopy(b1)
    a1.merge(b1)          # a ∪ b
    b2.merge(a2)          # b ∪ a
    assert a1.value == b2.value


@settings(max_examples=200, deadline=None)
@given(ops=_ops)
def test_gcounter_merge_is_idempotent(ops):
    a = GCounter("x")
    for node, amt in ops:
        a.increment(amt)
    b = copy.deepcopy(a)
    before = b.value
    b.merge(a)            # merging the same state again
    b.merge(a)
    assert b.value == before


@settings(max_examples=150, deadline=None)
@given(
    a_ops=st.lists(_node_ids, max_size=10),
    b_ops=st.lists(_node_ids, max_size=10),
    c_ops=st.lists(_node_ids, max_size=10),
)
def test_gcounter_merge_is_associative(a_ops, b_ops, c_ops):
    def make(ops, name):
        g = GCounter(name)
        for _ in ops:
            g.increment()
        return g

    a, b, c = make(a_ops, "a"), make(b_ops, "b"), make(c_ops, "c")
    # (a ∪ b) ∪ c
    left = copy.deepcopy(a)
    left.merge(copy.deepcopy(b))
    left.merge(copy.deepcopy(c))
    # a ∪ (b ∪ c)
    bc = copy.deepcopy(b)
    bc.merge(copy.deepcopy(c))
    right = copy.deepcopy(a)
    right.merge(bc)
    assert left.value == right.value


@settings(max_examples=150, deadline=None)
@given(
    adds=st.lists(st.integers(min_value=0, max_value=9), max_size=15),
    removes=st.lists(st.integers(min_value=0, max_value=9), max_size=15),
)
def test_orset_merge_commutes_to_same_elements(adds, removes):
    a = ORSet("a")
    b = ORSet("b")
    for e in adds:
        a.add(e)
    for e in removes:
        if a.contains(e):
            a.remove(e)
    for e in adds:
        b.add(e)

    ab = copy.deepcopy(a)
    ab.merge(copy.deepcopy(b))
    ba = copy.deepcopy(b)
    ba.merge(copy.deepcopy(a))
    assert ab.elements() == ba.elements()


@settings(max_examples=150, deadline=None)
@given(
    ins_a=st.lists(st.characters(min_codepoint=97, max_codepoint=122), max_size=8),
    ins_b=st.lists(st.characters(min_codepoint=97, max_codepoint=122), max_size=8),
)
def test_rga_converges_regardless_of_merge_direction(ins_a, ins_b):
    base = RGA("base")
    for ch in "seed":
        base.append(ch)

    a = RGA("a")
    a.merge(base)
    b = RGA("b")
    b.merge(base)
    for ch in ins_a:
        a.append(ch)
    for ch in ins_b:
        b.append(ch)

    ab = RGA("ab")
    ab.merge(a)
    ab.merge(b)
    ba = RGA("ba")
    ba.merge(b)
    ba.merge(a)
    # both merge orders converge to the identical sequence
    assert ab.to_string() == ba.to_string()
