"""A replicated last-writer-wins key/value store.

The store is a simple state-based CRDT: every key maps to a value tagged with a
Lamport-style ``(timestamp, origin)`` version. Merging two replicas keeps, per
key, the entry with the greater version. Because the merge is commutative,
associative and idempotent, all replicas that see the same set of writes
converge to the same state regardless of message order or duplication — exactly
what a gossip network needs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Version:
    """A causality tag for a store entry.

    Ordering is by ``ts`` first, then by ``origin`` as a deterministic
    tie-breaker so concurrent writes resolve identically on every replica.
    """

    ts: float
    origin: str

    def __gt__(self, other: "Version") -> bool:
        return (self.ts, self.origin) > (other.ts, other.origin)


@dataclass
class Entry:
    value: Any
    version: Version
    #: Tombstones let deletions propagate through gossip like any other write.
    deleted: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "ts": self.version.ts,
            "origin": self.version.origin,
            "deleted": self.deleted,
        }

    @classmethod
    def from_dict(cls, key_data: Dict[str, Any]) -> "Entry":
        return cls(
            value=key_data.get("value"),
            version=Version(ts=key_data["ts"], origin=str(key_data["origin"])),
            deleted=bool(key_data.get("deleted", False)),
        )


class DistributedStore:
    """A gossip-replicated LWW key/value map.

    Args:
        node_id: The owning node, used as the ``origin`` on local writes.
        clock: Injectable time source for versioning; defaults to ``time.time``.
    """

    def __init__(self, node_id: str, clock: Callable[[], float] = time.time) -> None:
        self._node_id = node_id
        self._clock = clock
        self._data: Dict[str, Entry] = {}
        self._subscribers: List[Callable[[str, Any], None]] = []
        self._commit_subs: List[Callable[[str, Entry], None]] = []

    def _next_version(self) -> Version:
        # Ensure monotonicity even if the wall clock does not advance between
        # rapid writes by nudging past the highest version we've produced.
        ts = self._clock()
        highest = max(
            (e.version.ts for e in self._data.values() if e.version.origin == self._node_id),
            default=0.0,
        )
        if ts <= highest:
            ts = highest + 1e-6
        return Version(ts=ts, origin=self._node_id)

    def set(self, key: str, value: Any) -> Entry:
        """Write ``value`` at ``key`` with a fresh local version."""
        entry = Entry(value=value, version=self._next_version(), deleted=False)
        self._data[key] = entry
        self._notify(key, value)
        self._emit_commit(key, entry)
        return entry

    def delete(self, key: str) -> Optional[Entry]:
        """Tombstone ``key`` so the deletion replicates through gossip."""
        if key not in self._data:
            return None
        entry = Entry(value=None, version=self._next_version(), deleted=True)
        self._data[key] = entry
        self._notify(key, None)
        self._emit_commit(key, entry)
        return entry

    def get(self, key: str, default: Any = None) -> Any:
        entry = self._data.get(key)
        if entry is None or entry.deleted:
            return default
        return entry.value

    def entry(self, key: str) -> Optional[Entry]:
        """Return the raw versioned :class:`Entry` for ``key`` (or ``None``)."""
        return self._data.get(key)

    def __contains__(self, key: object) -> bool:
        entry = self._data.get(key)  # type: ignore[arg-type]
        return entry is not None and not entry.deleted

    def items(self) -> List[Tuple[str, Any]]:
        return [(k, e.value) for k, e in self._data.items() if not e.deleted]

    def keys(self) -> List[str]:
        return [k for k, e in self._data.items() if not e.deleted]

    # -- replication --------------------------------------------------------

    def merge_entry(self, key: str, entry: Entry) -> bool:
        """Merge a single remote entry; returns ``True`` if state changed."""
        current = self._data.get(key)
        if current is None or entry.version > current.version:
            self._data[key] = entry
            if not entry.deleted:
                self._notify(key, entry.value)
            self._emit_commit(key, entry)
            return True
        return False

    def merge(self, snapshot: Iterable[Tuple[str, Entry]]) -> bool:
        changed = False
        for key, entry in snapshot:
            if self.merge_entry(key, entry):
                changed = True
        return changed

    def digest(self) -> Dict[str, Dict[str, Any]]:
        """A serialisable snapshot of every entry (including tombstones)."""
        return {k: e.to_dict() for k, e in self._data.items()}

    def apply_digest(self, digest: Dict[str, Dict[str, Any]]) -> bool:
        return self.merge(
            (key, Entry.from_dict(data)) for key, data in digest.items()
        )

    # -- observation --------------------------------------------------------

    def subscribe(self, callback: Callable[[str, Any], None]) -> None:
        """Register a callback invoked as ``callback(key, value)`` on change."""
        self._subscribers.append(callback)

    def _notify(self, key: str, value: Any) -> None:
        for callback in self._subscribers:
            callback(key, value)

    def on_commit(self, callback: Callable[[str, "Entry"], None]) -> None:
        """Register a callback invoked with ``(key, entry)`` on every commit.

        Unlike :meth:`subscribe`, this passes the full versioned ``Entry``
        (including tombstones), which is what a durable write-ahead log needs.
        """
        self._commit_subs.append(callback)

    def _emit_commit(self, key: str, entry: "Entry") -> None:
        for callback in self._commit_subs:
            callback(key, entry)
