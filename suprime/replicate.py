"""Replicate arbitrary CRDTs across the swarm over the gossip channel.

Any object exposing ``digest()`` / ``apply_digest(dict) -> bool`` (all the
types in :mod:`suprime.crdt`) can be registered under a name and kept in sync on
every node. Each tick a node pushes its named CRDT digests to a random peer;
receivers merge. Because the merges are CRDT merges, the replicas converge
regardless of ordering or loss — the same epidemic guarantee the core store has.
"""

from __future__ import annotations

import random
from typing import Any, Callable, Dict, Optional

from .message import Message

CRDT_SYNC = "__crdt_sync__"


class CRDTReplicator:
    """Keeps named CRDTs replicated across the swarm.

    Args:
        node: The :class:`~suprime.node.SwarmNode` to run on.
        fanout: How many peers to sync with each tick.
        rng: Injectable RNG for deterministic tests.
    """

    def __init__(
        self,
        node,
        fanout: int = 2,
        rng: Optional[random.Random] = None,
        full_sync_every: int = 20,
    ) -> None:
        self._node = node
        self._fanout = fanout
        self._rng = rng or random.Random()
        self._crdts: Dict[str, Any] = {}
        self._on_change: Dict[str, Callable[[Any], None]] = {}
        # Dirty-tracking: only ship a CRDT whose serialized state changed since
        # we last sent it, so a converged swarm goes quiet. A periodic full sync
        # every ``full_sync_every`` ticks still catches up newly-joined peers.
        self._last_sent: Dict[str, str] = {}
        self._full_sync_every = full_sync_every
        self._ticks = 0
        # Observability: CRDT-sync messages sent/received (for tests & dashboards).
        self.syncs_sent = 0
        self.syncs_received = 0
        node.on(CRDT_SYNC, self._on_sync)
        node.on_tick(self._tick)

    def register(self, name: str, crdt: Any, on_change: Optional[Callable[[Any], None]] = None) -> Any:
        """Register ``crdt`` under ``name`` for replication; returns the CRDT."""
        self._crdts[name] = crdt
        if on_change is not None:
            self._on_change[name] = on_change
        return crdt

    def get(self, name: str) -> Any:
        return self._crdts.get(name)

    async def _tick(self) -> None:
        if not self._crdts:
            return
        alive = [p.node_id for p in self._node.peers.alive()]
        if not alive:
            return
        self._ticks += 1
        full = self._ticks % self._full_sync_every == 0

        payload: Dict[str, Any] = {}
        for name, crdt in self._crdts.items():
            digest = crdt.digest()
            fingerprint = repr(digest)
            if full or self._last_sent.get(name) != fingerprint:
                payload[name] = digest
                self._last_sent[name] = fingerprint
        if not payload:
            return  # nothing changed since last round — stay quiet

        targets = alive if len(alive) <= self._fanout else self._rng.sample(alive, self._fanout)
        for target in targets:
            await self._node.send(target, CRDT_SYNC, payload)
            self.syncs_sent += 1

    async def _on_sync(self, message: Message) -> None:
        self.syncs_received += 1
        for name, digest in message.payload.items():
            crdt = self._crdts.get(name)
            if crdt is None:
                continue
            if crdt.apply_digest(digest) and name in self._on_change:
                self._on_change[name](crdt)
