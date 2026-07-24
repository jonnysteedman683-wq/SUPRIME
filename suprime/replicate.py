"""Replicate arbitrary CRDTs across the swarm over the gossip channel.

Any object exposing ``digest()`` / ``apply_digest(dict) -> bool`` (all the
types in :mod:`suprime.crdt`) can be registered under a name and kept in sync on
every node. Each tick a node pushes its named CRDT digests to a random peer;
receivers merge. Because the merges are CRDT merges, the replicas converge
regardless of ordering or loss — the same epidemic guarantee the core store has.
"""

from __future__ import annotations

import random
from typing import Any, Callable, Dict, List, Optional

from .message import Message

CRDT_SYNC = "__crdt_sync__"


class CRDTReplicator:
    """Keeps named CRDTs replicated across the swarm.

    Args:
        node: The :class:`~suprime.node.SwarmNode` to run on.
        fanout: How many peers to sync with each tick.
        rng: Injectable RNG for deterministic tests.
    """

    def __init__(self, node, fanout: int = 2, rng: Optional[random.Random] = None) -> None:
        self._node = node
        self._fanout = fanout
        self._rng = rng or random.Random()
        self._crdts: Dict[str, Any] = {}
        self._on_change: Dict[str, Callable[[Any], None]] = {}
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
        targets = alive if len(alive) <= self._fanout else self._rng.sample(alive, self._fanout)
        payload = {name: crdt.digest() for name, crdt in self._crdts.items()}
        for target in targets:
            await self._node.send(target, CRDT_SYNC, payload)

    async def _on_sync(self, message: Message) -> None:
        for name, digest in message.payload.items():
            crdt = self._crdts.get(name)
            if crdt is None:
                continue
            if crdt.apply_digest(digest) and name in self._on_change:
                self._on_change[name](crdt)
