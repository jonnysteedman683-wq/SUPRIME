"""A toolkit of state-based CRDTs (CvRDTs) for the swarm.

Each type here is a *convergent replicated data type*: its :meth:`merge` is
commutative, associative and idempotent, so replicas that observe the same set
of operations — in any order, with any duplication — converge to the same state.
That is exactly the guarantee gossip needs, so these compose directly with the
epidemic layer via :class:`~suprime.replicate.CRDTReplicator`.

Provided types:

* :class:`GCounter` — grow-only counter.
* :class:`PNCounter` — increment/decrement counter.
* :class:`ORSet` — observed-remove set (add/remove that both converge).
* :class:`LWWMap` — last-writer-wins map with causal tie-breaking.
* :class:`VectorClock` — for *detecting* concurrency rather than hiding it.
* :class:`MVRegister` — multi-value register that surfaces concurrent writes.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


# -- vector clocks ---------------------------------------------------------

@dataclass
class VectorClock:
    """A vector clock: per-node logical counters capturing causality.

    Unlike a wall-clock timestamp, comparing two vector clocks tells you whether
    one event *happened-before* the other or whether they are truly concurrent.
    """

    clock: Dict[str, int] = field(default_factory=dict)

    def increment(self, node_id: str) -> "VectorClock":
        self.clock[node_id] = self.clock.get(node_id, 0) + 1
        return self

    def merge(self, other: "VectorClock") -> "VectorClock":
        merged = dict(self.clock)
        for k, v in other.clock.items():
            merged[k] = max(merged.get(k, 0), v)
        return VectorClock(merged)

    def compare(self, other: "VectorClock") -> str:
        """Return ``'before'``, ``'after'``, ``'equal'`` or ``'concurrent'``."""
        keys = set(self.clock) | set(other.clock)
        less = greater = False
        for k in keys:
            a, b = self.clock.get(k, 0), other.clock.get(k, 0)
            if a < b:
                less = True
            elif a > b:
                greater = True
        if less and greater:
            return "concurrent"
        if less:
            return "before"
        if greater:
            return "after"
        return "equal"

    def to_dict(self) -> Dict[str, int]:
        return dict(self.clock)

    @classmethod
    def from_dict(cls, data: Dict[str, int]) -> "VectorClock":
        return cls({k: int(v) for k, v in data.items()})


# -- counters --------------------------------------------------------------

class GCounter:
    """A grow-only counter: the swarm-wide value is the sum of per-node counts."""

    def __init__(self, node_id: str) -> None:
        self._node_id = node_id
        self._counts: Dict[str, int] = {}

    def increment(self, amount: int = 1) -> None:
        if amount < 0:
            raise ValueError("GCounter only increments")
        self._counts[self._node_id] = self._counts.get(self._node_id, 0) + amount

    @property
    def value(self) -> int:
        return sum(self._counts.values())

    def merge(self, other: "GCounter") -> bool:
        changed = False
        for node, count in other._counts.items():
            if count > self._counts.get(node, 0):
                self._counts[node] = count
                changed = True
        return changed

    def digest(self) -> Dict[str, int]:
        return dict(self._counts)

    def apply_digest(self, digest: Dict[str, int]) -> bool:
        peer = GCounter("")
        peer._counts = {k: int(v) for k, v in digest.items()}
        return self.merge(peer)


class PNCounter:
    """An increment/decrement counter built from two grow-only counters."""

    def __init__(self, node_id: str) -> None:
        self._p = GCounter(node_id)
        self._n = GCounter(node_id)

    def increment(self, amount: int = 1) -> None:
        self._p.increment(amount)

    def decrement(self, amount: int = 1) -> None:
        self._n.increment(amount)

    @property
    def value(self) -> int:
        return self._p.value - self._n.value

    def merge(self, other: "PNCounter") -> bool:
        a = self._p.merge(other._p)
        b = self._n.merge(other._n)
        return a or b

    def digest(self) -> Dict[str, Any]:
        return {"p": self._p.digest(), "n": self._n.digest()}

    def apply_digest(self, digest: Dict[str, Any]) -> bool:
        a = self._p.apply_digest(digest.get("p", {}))
        b = self._n.apply_digest(digest.get("n", {}))
        return a or b


# -- sets ------------------------------------------------------------------

class ORSet:
    """An observed-remove set.

    Each add tags the element with a unique token; a remove tombstones exactly
    the tokens it has observed. An element is present iff it has at least one
    live (non-removed) token. This makes concurrent add/remove converge with a
    bias toward *add-wins* for tokens the remover never saw.
    """

    def __init__(self, node_id: str) -> None:
        self._node_id = node_id
        self._counter = 0
        self._adds: Dict[Any, Set[str]] = {}
        self._removed: Set[str] = set()

    def _fresh_tag(self) -> str:
        self._counter += 1
        return f"{self._node_id}:{self._counter}"

    def add(self, element: Any) -> None:
        self._adds.setdefault(element, set()).add(self._fresh_tag())

    def remove(self, element: Any) -> None:
        for tag in self._adds.get(element, set()):
            self._removed.add(tag)

    def contains(self, element: Any) -> bool:
        return bool(self._adds.get(element, set()) - self._removed)

    def elements(self) -> Set[Any]:
        return {e for e, tags in self._adds.items() if tags - self._removed}

    def merge(self, other: "ORSet") -> bool:
        changed = False
        for element, tags in other._adds.items():
            cur = self._adds.setdefault(element, set())
            if not tags <= cur:
                cur |= tags
                changed = True
        if not other._removed <= self._removed:
            self._removed |= other._removed
            changed = True
        return changed

    def digest(self) -> Dict[str, Any]:
        return {
            "adds": {repr(e): [e, list(tags)] for e, tags in self._adds.items()},
            "removed": list(self._removed),
        }

    def apply_digest(self, digest: Dict[str, Any]) -> bool:
        peer = ORSet("")
        peer._adds = {v[0]: set(v[1]) for v in digest.get("adds", {}).values()}
        peer._removed = set(digest.get("removed", []))
        return self.merge(peer)


# -- maps ------------------------------------------------------------------

@dataclass
class _LWWEntry:
    value: Any
    ts: float
    origin: str

    def newer_than(self, other: "_LWWEntry") -> bool:
        return (self.ts, self.origin) > (other.ts, other.origin)


class LWWMap:
    """A last-writer-wins map, versioned by ``(timestamp, origin)``."""

    def __init__(self, node_id: str, clock=time.time) -> None:
        self._node_id = node_id
        self._clock = clock
        self._entries: Dict[str, _LWWEntry] = {}

    def set(self, key: str, value: Any) -> None:
        self._entries[key] = _LWWEntry(value, self._clock(), self._node_id)

    def get(self, key: str, default: Any = None) -> Any:
        entry = self._entries.get(key)
        return entry.value if entry else default

    def items(self) -> List[Tuple[str, Any]]:
        return [(k, e.value) for k, e in self._entries.items()]

    def merge(self, other: "LWWMap") -> bool:
        changed = False
        for key, entry in other._entries.items():
            cur = self._entries.get(key)
            if cur is None or entry.newer_than(cur):
                self._entries[key] = entry
                changed = True
        return changed

    def digest(self) -> Dict[str, Any]:
        return {k: [e.value, e.ts, e.origin] for k, e in self._entries.items()}

    def apply_digest(self, digest: Dict[str, Any]) -> bool:
        peer = LWWMap("")
        peer._entries = {
            k: _LWWEntry(v[0], v[1], v[2]) for k, v in digest.items()
        }
        return self.merge(peer)


# -- multi-value register --------------------------------------------------

class MVRegister:
    """A multi-value register that *keeps* concurrent writes.

    Each write carries a vector clock. Writes that causally dominate others
    replace them; concurrent writes are all retained, so :meth:`values` can
    surface a genuine conflict instead of silently discarding a write.
    """

    def __init__(self, node_id: str) -> None:
        self._node_id = node_id
        self._vc = VectorClock()
        # list of (value, VectorClock)
        self._versions: List[Tuple[Any, VectorClock]] = []

    def set(self, value: Any) -> None:
        self._vc.increment(self._node_id)
        self._versions = [(value, VectorClock(dict(self._vc.clock)))]

    def values(self) -> List[Any]:
        return [v for v, _ in self._versions]

    def merge(self, other: "MVRegister") -> bool:
        combined = self._versions + other._versions
        # Keep only versions not dominated by another (causal maxima).
        kept: List[Tuple[Any, VectorClock]] = []
        for i, (val, vc) in enumerate(combined):
            dominated = False
            for j, (_, other_vc) in enumerate(combined):
                if i != j and vc.compare(other_vc) == "before":
                    dominated = True
                    break
            if dominated:
                continue
            if not any(vc.compare(k_vc) == "equal" and val == k_val for k_val, k_vc in kept):
                kept.append((val, vc))
        changed = {id(v) for v in kept} != {id(v) for v in self._versions}
        self._versions = kept
        self._vc = self._vc.merge(other._vc)
        return changed or len(kept) != len(self._versions)

    def digest(self) -> Dict[str, Any]:
        return {
            "versions": [[v, vc.to_dict()] for v, vc in self._versions],
            "vc": self._vc.to_dict(),
        }

    def apply_digest(self, digest: Dict[str, Any]) -> bool:
        peer = MVRegister("")
        peer._versions = [
            (v[0], VectorClock.from_dict(v[1])) for v in digest.get("versions", [])
        ]
        peer._vc = VectorClock.from_dict(digest.get("vc", {}))
        return self.merge(peer)
