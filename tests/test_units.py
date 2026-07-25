"""Unit tests for the pure, I/O-free components of the swarm."""

from __future__ import annotations

import itertools


from suprime.consensus import LeaderView, elect_leader
from suprime.message import Message, MessageType
from suprime.peers import PeerState, PeerTable
from suprime.store import DistributedStore, Entry, Version


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

def test_peer_table_evict():
    pt = PeerTable("self")
    pt.merge("p1", "addr1", 1)

    # Peer should exist
    assert pt.get("p1") is not None
    assert "p1" in pt

    # Evict peer
    pt.evict("p1")
    assert pt.get("p1") is None
    assert "p1" not in pt

    # Evicting an unknown peer should not raise errors
    pt.evict("p2")
