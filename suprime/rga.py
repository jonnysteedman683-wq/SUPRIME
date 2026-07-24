"""RGA — a Replicated Growable Array for collaborative sequences.

An RGA is a sequence CRDT: multiple nodes can insert and delete characters (or
any elements) concurrently and every replica converges to the same ordering,
which is what lets a swarm host a shared, editable document or append-mostly log
with no central authority.

How it converges
----------------
Every element gets a globally unique, totally-ordered id ``(counter, node)``.
An insert names the id of the element it follows. When two inserts target the
same predecessor concurrently, they are ordered deterministically by id (higher
id first), so all replicas agree. Deletes are tombstones, so they commute with
concurrent inserts around them. Merging is idempotent, so duplicated gossip is
harmless.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# An element id is (lamport_counter, node_id); ordering is by counter then node.
Id = Tuple[int, str]
ROOT: Id = (0, "")


@dataclass
class _Elem:
    id: Id
    value: Any
    after: Id          # id of the predecessor this was inserted after
    deleted: bool = False


class RGA:
    """A collaborative sequence CRDT owned by one node.

    Args:
        node_id: This replica's identity, used to mint unique element ids.
    """

    def __init__(self, node_id: str) -> None:
        self._node_id = node_id
        self._counter = 0
        # id -> element (ROOT is implicit)
        self._elems: Dict[Id, _Elem] = {}
        # predecessor id -> list of child ids, kept sorted (desc) for RGA order
        self._children: Dict[Id, List[Id]] = {ROOT: []}

    # -- local editing ------------------------------------------------------

    def _next_id(self) -> Id:
        self._counter += 1
        return (self._counter, self._node_id)

    def insert(self, index: int, value: Any) -> Id:
        """Insert ``value`` at visible position ``index``; returns its id."""
        visible = self._visible_ids()
        after = visible[index - 1] if index > 0 else ROOT
        return self._apply_insert(self._next_id(), value, after)

    def append(self, value: Any) -> Id:
        return self.insert(len(self._visible_ids()), value)

    def delete(self, index: int) -> Optional[Id]:
        """Tombstone the element at visible position ``index``."""
        visible = self._visible_ids()
        if index < 0 or index >= len(visible):
            return None
        eid = visible[index]
        self._elems[eid].deleted = True
        return eid

    # -- core apply / ordering ---------------------------------------------

    def _apply_insert(self, eid: Id, value: Any, after: Id) -> Id:
        if eid in self._elems:
            return eid
        # Bump our Lamport counter past anything we've seen for causality.
        self._counter = max(self._counter, eid[0])
        self._elems[eid] = _Elem(id=eid, value=value, after=after)
        siblings = self._children.setdefault(after, [])
        # RGA tie-break: among elements sharing a predecessor, higher id first.
        siblings.append(eid)
        siblings.sort(reverse=True)
        self._children.setdefault(eid, [])
        return eid

    def _visible_ids(self) -> List[Id]:
        order: List[Id] = []

        def walk(pred: Id) -> None:
            for child in self._children.get(pred, []):
                elem = self._elems[child]
                if not elem.deleted:
                    order.append(child)
                walk(child)

        walk(ROOT)
        return order

    # -- reads --------------------------------------------------------------

    def to_list(self) -> List[Any]:
        return [self._elems[i].value for i in self._visible_ids()]

    def to_string(self) -> str:
        return "".join(str(v) for v in self.to_list())

    def __len__(self) -> int:
        return len(self._visible_ids())

    # -- replication --------------------------------------------------------

    def merge(self, other: "RGA") -> bool:
        changed = False
        # Insert any elements we don't have, in id order so predecessors land
        # first and ordering is well defined.
        for eid in sorted(other._elems):
            oe = other._elems[eid]
            if eid not in self._elems:
                self._apply_insert(eid, oe.value, oe.after)
                changed = True
            if oe.deleted and not self._elems[eid].deleted:
                self._elems[eid].deleted = True
                changed = True
        return changed

    def digest(self) -> Dict[str, Any]:
        return {
            "elems": [
                [list(e.id), e.value, list(e.after), e.deleted]
                for e in self._elems.values()
            ]
        }

    def apply_digest(self, digest: Dict[str, Any]) -> bool:
        peer = RGA("")
        for raw in digest.get("elems", []):
            eid = (raw[0][0], raw[0][1])
            after = (raw[2][0], raw[2][1])
            peer._elems[eid] = _Elem(id=eid, value=raw[1], after=after, deleted=raw[3])
        return self.merge(peer)
