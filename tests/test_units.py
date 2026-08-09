"""Unit tests for the pure, I/O-free components of the swarm."""

from __future__ import annotations

import os
import pytest
import itertools
from suprime.aggregate import _Mass


from suprime.consensus import LeaderView, elect_leader
from suprime.message import Message, MessageType
from suprime.peers import PeerState, PeerTable
from suprime.store import DistributedStore, Entry, Version
from suprime.identity import NodeID


# -- aggregate -------------------------------------------------------------

def test_mass_half():
    original = _Mass(s=10.0, w=2.0)
    halved = original.half()
    assert halved.s == 5.0
    assert halved.w == 1.0
    assert original.s == 10.0
    assert original.w == 2.0


# -- message ---------------------------------------------------------------

def test_message_round_trip():
    msg = Message(type=MessageType.DIRECT, src="a", dst="b", payload={"x": 1})
    restored = Message.from_bytes(msg.to_bytes())
    assert restored.type == msg.type
    assert restored.src == "a"
    assert restored.dst == "b"
    assert restored.payload == {"x": 1}
    assert restored.id == msg.id


# -- store -----------------------------------------------------------------

def test_store_basic_set_get_delete():
    s = DistributedStore("n1")
    s.set("k", 1)
    assert s.get("k") == 1
    assert "k" in s
    s.delete("k")
    assert s.get("k") is None
    assert "k" not in s


def test_store_lww_higher_timestamp_wins():
    a = DistributedStore("a")
    b = DistributedStore("b")
    a.set("k", "old")
    b.set("k", "new")  # b's write has a later timestamp
    # merge a -> b keeps b's newer value; b -> a adopts b's value
    b.apply_digest(a.digest())
    a.apply_digest(b.digest())
    assert a.get("k") == "new"
    assert b.get("k") == "new"


def test_store_merge_is_convergent_regardless_of_order():
    values = {}
    for i, order in enumerate(itertools.permutations(range(3))):
        replicas = [DistributedStore(f"r{j}") for j in range(3)]
        # each replica makes a distinct write
        entries = []
        for j, r in enumerate(replicas):
            r.set("k", f"v{j}")
            entries.append(("k", r.digest()["k"]))
        # apply the three writes to a fresh replica in this permutation's order
        target = DistributedStore("t")
        for j in order:
            key, data = entries[j]
            target.merge_entry(key, Entry.from_dict(data))
        values[order] = target.get("k")
    # every ordering converges to the same winner
    assert len(set(values.values())) == 1


def test_version_tie_break_by_origin():
    v1 = Version(ts=1.0, origin="a")
    v2 = Version(ts=1.0, origin="b")
    assert v2 > v1  # same ts, "b" > "a"


def test_store_subscribe_notifies_on_change():
    s = DistributedStore("n1")
    seen = []
    s.subscribe(lambda k, v: seen.append((k, v)))
    s.set("k", 42)
    assert ("k", 42) in seen


# -- peers / failure detection --------------------------------------------

def test_peer_merge_and_heartbeat_progress():
    clock = [0.0]
    pt = PeerTable("self", clock=lambda: clock[0])
    assert pt.merge("p1", "addr1", 1) is True
    assert pt.merge("p1", "addr1", 1) is False  # no new info
    assert pt.merge("p1", "addr1", 2) is True  # newer heartbeat
    assert pt.get("p1").heartbeat == 2


def test_peer_ignores_self():
    pt = PeerTable("self")
    assert pt.merge("self", "addr", 5) is False
    assert len(pt) == 0


def test_failure_detection_transitions():
    clock = [0.0]
    pt = PeerTable("self", suspect_after=3.0, dead_after=6.0, clock=lambda: clock[0])
    pt.merge("p1", "addr", 1)
    assert pt.get("p1").state is PeerState.ALIVE

    clock[0] = 4.0
    pt.tick()
    assert pt.get("p1").state is PeerState.SUSPECT

    clock[0] = 7.0
    pt.tick()
    assert pt.get("p1") is None  # evicted as dead
    assert len(pt.alive()) == 0


def test_fresh_heartbeat_revives_suspect():
    clock = [0.0]
    pt = PeerTable("self", suspect_after=3.0, dead_after=6.0, clock=lambda: clock[0])
    pt.merge("p1", "addr", 1)
    clock[0] = 4.0
    pt.tick()
    assert pt.get("p1").state is PeerState.SUSPECT
    pt.merge("p1", "addr", 2)  # heartbeat advanced
    assert pt.get("p1").state is PeerState.ALIVE


def test_peer_table_apply_digest():
    pt1 = PeerTable("node1")
    pt1.merge("node2", "addr2", 1)
    pt1.merge("node3", "addr3", 5)

    pt2 = PeerTable("node2")

    # pt2 learns about node3; its own record (node2) is ignored
    changed = pt2.apply_digest(pt1.digest())
    assert changed is True
    assert len(pt2) == 1
    assert pt2.get("node3").heartbeat == 5
    assert pt2.get("node2") is None # ignored self

    # Manually add entries, including node1
    entries = [
        {"node_id": "node1", "address": "addr1", "heartbeat": 10},
        {"node_id": "node3", "address": "addr3", "heartbeat": 5}, # no change
        {"node_id": "node4", "address": "addr4", "heartbeat": 2},
    ]
    changed = pt2.apply_digest(entries)
    assert changed is True
    assert len(pt2) == 3
    assert pt2.get("node1").heartbeat == 10
    assert pt2.get("node4").heartbeat == 2

    # Applying the same entries should return False
    changed = pt2.apply_digest(entries)
    assert changed is False

    # Applying a newer heartbeat should return True
    entries = [
        {"node_id": "node1", "address": "addr1", "heartbeat": 11}
    ]
    changed = pt2.apply_digest(entries)
    assert changed is True
    assert pt2.get("node1").heartbeat == 11

    # Ignoring self directly
    entries = [
        {"node_id": "node2", "address": "addr2", "heartbeat": 99}
    ]
    changed = pt2.apply_digest(entries)
    assert changed is False


# -- consensus -------------------------------------------------------------

def test_elect_leader_smallest_id():
    assert elect_leader("b", ["c", "d"]) == "b"
    assert elect_leader("z", ["a", "m"]) == "a"
    assert elect_leader("solo", []) == "solo"


def test_leader_view_reports_transitions():
    lv = LeaderView("m")
    assert lv.update([]) == "m"  # first election
    assert lv.update([]) is None  # unchanged
    assert lv.update(["a"]) == "a"  # a smaller id joined -> new leader
    assert lv.is_leader() is False
    assert lv.update([]) == "m"  # a left -> leadership returns
    assert lv.is_leader() is True

# -- identity --------------------------------------------------------------

def test_nodeid_generate():
    """Verify NodeID generate creates valid, unique IDs."""
    node1 = NodeID.generate()
    node2 = NodeID.generate()

    # Must be instances of NodeID
    assert isinstance(node1, NodeID)

    # Must be unique
    assert node1 != node2

    # Default prefix and pid must be present
    assert node1.value.startswith(f"node-{os.getpid()}-")

    # Custom prefix support
    custom_node = NodeID.generate(prefix="test-prefix")
    assert custom_node.value.startswith(f"test-prefix-{os.getpid()}-")


def test_nodeid_validation():
    """Verify NodeID validates its input correctly."""
    # Empty string should fail
    with pytest.raises(ValueError, match="NodeID value must be a non-empty string"):
        NodeID("")

    # None should fail
    with pytest.raises(ValueError, match="NodeID value must be a non-empty string"):
        NodeID(None)  # type: ignore

def test_entry_to_dict():
    v1 = Version(ts=1.5, origin="node1")
    e1 = Entry(value="test_value", version=v1, deleted=True)
    assert e1.to_dict() == {
        "value": "test_value",
        "ts": 1.5,
        "origin": "node1",
        "deleted": True,
    }

    v2 = Version(ts=2.0, origin="node2")
    e2 = Entry(value=None, version=v2, deleted=False)
    assert e2.to_dict() == {
        "value": None,
        "ts": 2.0,
        "origin": "node2",
        "deleted": False,
    }
