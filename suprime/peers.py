"""Membership and heartbeat-based failure detection.

The swarm tracks its members with a gossip-style heartbeat protocol. Each node
owns a monotonically increasing heartbeat counter which it bumps every tick and
disseminates through gossip. When a peer's heartbeat stops advancing it is first
marked ``SUSPECT`` and, if the silence persists, ``DEAD`` and evicted.

This module is pure state — it has no I/O — which keeps the failure detector
deterministic and unit-testable.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, List, Optional


class PeerState(str, Enum):
    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"


@dataclass
class Peer:
    """A remote node as seen from the local membership table."""

    node_id: str
    address: str
    heartbeat: int = 0
    state: PeerState = PeerState.ALIVE
    #: Local monotonic time at which we last saw the heartbeat advance.
    last_update: float = 0.0

    def to_dict(self) -> Dict[str, object]:
        return {
            "node_id": self.node_id,
            "address": self.address,
            "heartbeat": self.heartbeat,
        }


class PeerTable:
    """Local view of swarm membership with heartbeat failure detection.

    Args:
        self_id: The owning node's identity (never stored as a peer of itself).
        suspect_after: Seconds without a heartbeat advance before a peer is
            marked ``SUSPECT``.
        dead_after: Seconds without a heartbeat advance before a peer is marked
            ``DEAD`` and removed from the live set.
        clock: Injectable time source (seconds); defaults to ``time.monotonic``.
    """

    def __init__(
        self,
        self_id: str,
        suspect_after: float = 3.0,
        dead_after: float = 6.0,
        clock=time.monotonic,
    ) -> None:
        self._self_id = self_id
        self._suspect_after = suspect_after
        self._dead_after = dead_after
        self._clock = clock
        self._peers: Dict[str, Peer] = {}

    # -- mutation -----------------------------------------------------------

    def merge(self, node_id: str, address: str, heartbeat: int) -> bool:
        """Integrate a heartbeat observation about ``node_id``.

        Returns ``True`` if this call produced new information (a new peer or a
        newer heartbeat), which callers use to decide whether to re-gossip.
        """
        if node_id == self._self_id:
            return False
        now = self._clock()
        existing = self._peers.get(node_id)
        if existing is None:
            self._peers[node_id] = Peer(
                node_id=node_id,
                address=address,
                heartbeat=heartbeat,
                state=PeerState.ALIVE,
                last_update=now,
            )
            return True
        if heartbeat > existing.heartbeat:
            existing.heartbeat = heartbeat
            existing.address = address
            existing.last_update = now
            existing.state = PeerState.ALIVE
            return True
        return False

    def evict(self, node_id: str) -> None:
        self._peers.pop(node_id, None)

    def tick(self) -> None:
        """Advance failure-detection state based on elapsed time.

        Peers are demoted to ``SUSPECT`` and then ``DEAD`` as their heartbeats
        go stale. Dead peers are removed from the table.
        """
        now = self._clock()
        for peer in list(self._peers.values()):
            silence = now - peer.last_update
            if silence >= self._dead_after:
                peer.state = PeerState.DEAD
                self._peers.pop(peer.node_id, None)
            elif silence >= self._suspect_after:
                peer.state = PeerState.SUSPECT

    # -- queries ------------------------------------------------------------

    def get(self, node_id: str) -> Optional[Peer]:
        return self._peers.get(node_id)

    def all(self) -> List[Peer]:
        return list(self._peers.values())

    def alive(self) -> List[Peer]:
        return [p for p in self._peers.values() if p.state == PeerState.ALIVE]

    def addresses(self) -> List[str]:
        return [p.address for p in self._peers.values()]

    def digest(self) -> List[Dict[str, object]]:
        """A compact, serialisable snapshot for gossip piggybacking."""
        return [p.to_dict() for p in self._peers.values()]

    def known_ids(self, include_self: bool = False) -> List[str]:
        ids = list(self._peers.keys())
        if include_self:
            ids.append(self._self_id)
        return ids

    def __len__(self) -> int:
        return len(self._peers)

    def __contains__(self, node_id: object) -> bool:
        return node_id in self._peers

    def apply_digest(self, entries: Iterable[Dict[str, object]]) -> bool:
        """Merge a batch of peer descriptors from a gossip message."""
        changed = False
        for entry in entries:
            node_id = str(entry["node_id"])
            address = str(entry["address"])
            heartbeat = int(entry["heartbeat"])
            if self.merge(node_id, address, heartbeat):
                changed = True
        return changed
