"""Plumtree — epidemic broadcast trees.

Naive gossip broadcasts every message to a random fanout every round: robust,
but wasteful (each node receives the same message many times). Plumtree keeps
the robustness of gossip while paying the bandwidth of a tree.

Each link to a neighbour is either **eager** or **lazy**:

* Eager links form a spanning tree; full messages are pushed along them.
* Lazy links carry only compact ``IHAVE`` announcements (message ids).

When a full message arrives on a lazy link the receiver already had it, so it
``PRUNE``s that redundant edge (demotes it to lazy). When a node learns via
``IHAVE`` about a message it is missing (a tree branch broke), it ``GRAFT``s the
lazy link back into the tree and pulls the message. The tree thus self-optimises
to O(N) message copies and self-heals when nodes fail — no global coordination.
"""

from __future__ import annotations

import random
import uuid
from typing import Callable, Dict, List, Optional, Set

from .message import Message

PT_GOSSIP = "__pt_gossip__"
PT_IHAVE = "__pt_ihave__"
PT_PRUNE = "__pt_prune__"
PT_GRAFT = "__pt_graft__"

DeliverCallback = Callable[[str, dict], None]


class PlumtreeBroadcast:
    """A self-optimising, self-healing broadcast primitive over a node.

    Args:
        node: The :class:`~suprime.node.SwarmNode` to run on.
        neighbors: Zero-arg callable returning the current neighbour node ids.
            Defaults to the node's full peer set; pass a HyParView active view
            for bounded-degree operation in large swarms.
        on_deliver: Called ``on_deliver(message_id, payload)`` exactly once per
            distinct broadcast this node receives.
        graft_timeout_rounds: Rounds to wait after an ``IHAVE`` before grafting.
        rng: Injectable RNG for deterministic tests.
    """

    def __init__(
        self,
        node,
        neighbors: Optional[Callable[[], List[str]]] = None,
        on_deliver: Optional[DeliverCallback] = None,
        graft_timeout_rounds: int = 2,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._node = node
        self._neighbors = neighbors or (lambda: [p.node_id for p in node.peers.all()])
        self._on_deliver = on_deliver
        self._graft_timeout = graft_timeout_rounds
        self._rng = rng or random.Random()

        self.eager: Set[str] = set()
        self.lazy: Set[str] = set()
        self._received: Dict[str, dict] = {}
        self._missing: Dict[str, int] = {}  # msg_id -> rounds waited
        self._ihave_from: Dict[str, List[str]] = {}
        self._lazy_queue: List[tuple] = []  # (peer, msg_id)

        node.on(PT_GOSSIP, self._on_gossip)
        node.on(PT_IHAVE, self._on_ihave)
        node.on(PT_PRUNE, self._on_prune)
        node.on(PT_GRAFT, self._on_graft)
        node.on_tick(self._tick)

    @property
    def delivered_ids(self) -> Set[str]:
        return set(self._received)

    def has(self, msg_id: str) -> bool:
        return msg_id in self._received

    # -- broadcasting -------------------------------------------------------

    async def broadcast(self, payload: dict) -> str:
        """Broadcast ``payload`` to the whole swarm; returns the message id."""
        self._sync_neighbors()
        msg_id = uuid.uuid4().hex
        self._received[msg_id] = payload
        self._deliver(msg_id, payload)
        await self._eager_push(msg_id, payload, sender=None)
        self._lazy_push(msg_id, sender=None)
        return msg_id

    def _deliver(self, msg_id: str, payload: dict) -> None:
        if self._on_deliver is not None:
            self._on_deliver(msg_id, payload)

    # -- neighbour bookkeeping ---------------------------------------------

    def _sync_neighbors(self) -> None:
        current = set(self._neighbors())
        # New neighbours start eager (part of the tree until pruned).
        for n in current - (self.eager | self.lazy):
            self.eager.add(n)
        # Drop neighbours that left.
        self.eager &= current
        self.lazy &= current

    # -- push helpers -------------------------------------------------------

    async def _eager_push(self, msg_id: str, payload: dict, sender: Optional[str]) -> None:
        for peer in list(self.eager):
            if peer == sender:
                continue
            await self._node.send(peer, PT_GOSSIP, {"mid": msg_id, "payload": payload})

    def _lazy_push(self, msg_id: str, sender: Optional[str]) -> None:
        for peer in list(self.lazy):
            if peer == sender:
                continue
            self._lazy_queue.append((peer, msg_id))

    # -- message handlers ---------------------------------------------------

    async def _on_gossip(self, message: Message) -> None:
        mid = message.payload["mid"]
        payload = message.payload["payload"]
        src = message.src
        if mid not in self._received:
            self._received[mid] = payload
            self._missing.pop(mid, None)
            self._ihave_from.pop(mid, None)
            self.eager.add(src)
            self.lazy.discard(src)
            self._deliver(mid, payload)
            await self._eager_push(mid, payload, sender=src)
            self._lazy_push(mid, sender=src)
        else:
            # Redundant delivery: this edge is not needed in the tree.
            self.eager.discard(src)
            self.lazy.add(src)
            await self._node.send(src, PT_PRUNE, {})

    async def _on_prune(self, message: Message) -> None:
        self.eager.discard(message.src)
        self.lazy.add(message.src)

    async def _on_ihave(self, message: Message) -> None:
        for mid in message.payload.get("ids", []):
            if mid not in self._received:
                self._missing.setdefault(mid, 0)
                self._ihave_from.setdefault(mid, [])
                if message.src not in self._ihave_from[mid]:
                    self._ihave_from[mid].append(message.src)

    async def _on_graft(self, message: Message) -> None:
        self.eager.add(message.src)
        self.lazy.discard(message.src)
        mid = message.payload["mid"]
        if mid in self._received:
            await self._node.send(
                message.src, PT_GOSSIP, {"mid": mid, "payload": self._received[mid]}
            )

    # -- periodic maintenance ----------------------------------------------

    async def _tick(self) -> None:
        self._sync_neighbors()

        # Flush queued IHAVE announcements, batched per peer.
        if self._lazy_queue:
            by_peer: Dict[str, List[str]] = {}
            for peer, mid in self._lazy_queue:
                by_peer.setdefault(peer, []).append(mid)
            self._lazy_queue = []
            for peer, ids in by_peer.items():
                await self._node.send(peer, PT_IHAVE, {"ids": ids})

        # Repair broken tree branches: graft lazy links we heard IHAVE from.
        for mid in list(self._missing):
            if mid in self._received:
                self._missing.pop(mid, None)
                continue
            self._missing[mid] += 1
            if self._missing[mid] >= self._graft_timeout:
                candidates = self._ihave_from.get(mid) or []
                if candidates:
                    peer = candidates.pop(0)
                    self.eager.add(peer)
                    self.lazy.discard(peer)
                    self._missing[mid] = 0
                    await self._node.send(peer, PT_GRAFT, {"mid": mid})
