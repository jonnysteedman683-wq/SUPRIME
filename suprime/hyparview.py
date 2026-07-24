"""HyParView — a partial-view membership protocol for large swarms.

Full-membership gossip is O(N) state per node; that stops scaling. HyParView
instead gives every node two bounded views:

* a small **active view** (~log N) of symmetric links that together form a
  connected overlay — this is the set an epidemic broadcast (Plumtree) runs on;
* a larger **passive view** of backup peers, refreshed by periodic *shuffles*,
  used to repair the active view when a link fails.

New nodes join through any contact; a randomised *forward-join* walk spreads the
newcomer across the overlay. When an active link dies, the node promotes a
passive peer, so the overlay self-heals and stays connected without any node
ever holding the full membership.

This implementation keeps its own address book, so it does not depend on the
full-membership layer and can serve as the sole membership substrate.
"""

from __future__ import annotations

import random
from typing import Dict, List, Optional, Set, Tuple

from .message import Message

HPV_JOIN = "__hpv_join__"
HPV_FWDJOIN = "__hpv_fwdjoin__"
HPV_NEIGHBOR = "__hpv_neighbor__"
HPV_NEIGHBOR_REPLY = "__hpv_neighbor_reply__"
HPV_DISCONNECT = "__hpv_disconnect__"
HPV_SHUFFLE = "__hpv_shuffle__"
HPV_SHUFFLE_REPLY = "__hpv_shuffle_reply__"


class HyParView:
    """Bounded active/passive-view membership for one node.

    Args:
        node: The :class:`~suprime.node.SwarmNode` to run on.
        active_size: Maximum active-view size (the overlay degree).
        passive_size: Maximum passive-view size (the backup pool).
        arwl: Active random-walk length for forward-joins.
        prwl: Passive random-walk length (when a walk deposits into passive).
        shuffle_ttl: Time-to-live for shuffle walks.
        rng: Injectable RNG for deterministic tests.
    """

    def __init__(
        self,
        node,
        active_size: int = 4,
        passive_size: int = 8,
        arwl: int = 3,
        prwl: int = 2,
        shuffle_ttl: int = 2,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._node = node
        self._active_size = active_size
        self._passive_size = passive_size
        self._arwl = arwl
        self._prwl = prwl
        self._shuffle_ttl = shuffle_ttl
        self._rng = rng or random.Random()

        self.active: Set[str] = set()
        self.passive: Set[str] = set()
        #: node_id -> transport address for everyone we've ever heard of.
        self._book: Dict[str, str] = {}

        node.on(HPV_JOIN, self._on_join)
        node.on(HPV_FWDJOIN, self._on_forward_join)
        node.on(HPV_NEIGHBOR, self._on_neighbor)
        node.on(HPV_NEIGHBOR_REPLY, self._on_neighbor_reply)
        node.on(HPV_DISCONNECT, self._on_disconnect)
        node.on(HPV_SHUFFLE, self._on_shuffle)
        node.on(HPV_SHUFFLE_REPLY, self._on_shuffle_reply)
        node.on_tick(self._tick)

    # -- helpers ------------------------------------------------------------

    @property
    def _id(self) -> str:
        return self._node.id

    def _record(self, node_id: str, address: str) -> None:
        if node_id and address and node_id != self._id:
            self._book[node_id] = address

    def _addr(self, node_id: str) -> Optional[str]:
        return self._book.get(node_id)

    async def _send(self, node_id: str, msg_type: str, payload: dict) -> bool:
        address = self._addr(node_id)
        if address is None:
            return False
        payload = dict(payload)
        payload["_from_addr"] = self._node.address
        return await self._node.send_to(address, msg_type, payload)

    # -- joining ------------------------------------------------------------

    async def join(self, contact_address: str) -> None:
        """Join the overlay through an existing member at ``contact_address``."""
        await self._node.send_to(
            contact_address,
            HPV_JOIN,
            {"node_id": self._id, "address": self._node.address},
        )

    async def _add_active(self, node_id: str) -> None:
        if node_id == self._id or node_id in self.active:
            return
        if self._addr(node_id) is None:
            return
        self.passive.discard(node_id)
        if len(self.active) >= self._active_size:
            await self._drop_random_active()
        self.active.add(node_id)

    async def _drop_random_active(self) -> None:
        if not self.active:
            return
        victim = self._rng.choice(list(self.active))
        self.active.discard(victim)
        self._add_passive(victim)
        await self._send(victim, HPV_DISCONNECT, {"node_id": self._id})

    def _add_passive(self, node_id: str) -> None:
        if node_id == self._id or node_id in self.active or node_id in self.passive:
            return
        if self._addr(node_id) is None:
            return
        if len(self.passive) >= self._passive_size:
            self.passive.discard(self._rng.choice(list(self.passive)))
        self.passive.add(node_id)

    async def _on_join(self, message: Message) -> None:
        nid = message.payload["node_id"]
        self._record(nid, message.payload["address"])
        await self._add_active(nid)
        # Establish the reverse link so the joiner adds us too (symmetry).
        await self._send(
            nid,
            HPV_NEIGHBOR,
            {"node_id": self._id, "address": self._node.address, "priority": "high"},
        )
        # Spread the newcomer through the overlay via a random walk.
        for peer in self.active:
            if peer == nid:
                continue
            await self._send(
                peer,
                HPV_FWDJOIN,
                {
                    "node_id": nid,
                    "address": message.payload["address"],
                    "ttl": self._arwl,
                },
            )

    async def _on_forward_join(self, message: Message) -> None:
        nid = message.payload["node_id"]
        addr = message.payload["address"]
        ttl = int(message.payload["ttl"])
        self._record(nid, addr)
        if ttl == 0 or len(self.active) <= 1:
            await self._add_active(nid)
            # Establish the reverse link so the active view stays symmetric.
            await self._send(
                nid, HPV_NEIGHBOR, {"node_id": self._id, "address": self._node.address, "priority": "high"}
            )
            return
        if ttl == self._prwl:
            self._add_passive(nid)
        # Forward to a random active peer other than the sender.
        candidates = [p for p in self.active if p != message.src and p != nid]
        if candidates:
            nxt = self._rng.choice(candidates)
            await self._send(
                nxt, HPV_FWDJOIN, {"node_id": nid, "address": addr, "ttl": ttl - 1}
            )
        else:
            await self._add_active(nid)

    # -- neighbour (repair) protocol ---------------------------------------

    async def _on_neighbor(self, message: Message) -> None:
        nid = message.payload["node_id"]
        self._record(nid, message.payload["address"])
        priority = message.payload.get("priority", "low")
        accept = priority == "high" or len(self.active) < self._active_size
        if accept:
            await self._add_active(nid)
        await self._send(
            nid,
            HPV_NEIGHBOR_REPLY,
            {"node_id": self._id, "address": self._node.address, "accepted": accept},
        )

    async def _on_neighbor_reply(self, message: Message) -> None:
        nid = message.payload["node_id"]
        self._record(nid, message.payload["address"])
        if message.payload.get("accepted"):
            await self._add_active(nid)
        else:
            self._add_passive(nid)

    async def _on_disconnect(self, message: Message) -> None:
        nid = message.payload["node_id"]
        if nid in self.active:
            self.active.discard(nid)
            self._add_passive(nid)

    # -- shuffle (passive-view refresh) ------------------------------------

    async def _shuffle(self) -> None:
        if not self.active:
            return
        target = self._rng.choice(list(self.active))
        sample = self._sample(self.active | self.passive, 4)
        payload = {
            "origin": self._id,
            "origin_addr": self._node.address,
            "nodes": [(n, self._addr(n)) for n in sample if self._addr(n)],
            "ttl": self._shuffle_ttl,
        }
        await self._send(target, HPV_SHUFFLE, payload)

    async def _on_shuffle(self, message: Message) -> None:
        ttl = int(message.payload["ttl"]) - 1
        nodes: List[Tuple[str, str]] = [tuple(x) for x in message.payload["nodes"]]
        origin = message.payload["origin"]
        self._record(origin, message.payload["origin_addr"])
        if ttl > 0 and self.active:
            candidates = [p for p in self.active if p != message.src]
            if candidates:
                fwd = dict(message.payload)
                fwd["ttl"] = ttl
                await self._send(self._rng.choice(candidates), HPV_SHUFFLE, fwd)
                self._integrate(nodes)
                return
        # End of walk: reply with our own sample and integrate theirs.
        reply_sample = [(n, self._addr(n)) for n in self._sample(self.active | self.passive, 4) if self._addr(n)]
        await self._send(
            origin,
            HPV_SHUFFLE_REPLY,
            {"node_id": self._id, "address": self._node.address, "nodes": reply_sample},
        )
        self._integrate(nodes)

    async def _on_shuffle_reply(self, message: Message) -> None:
        self._record(message.payload["node_id"], message.payload["address"])
        self._integrate([tuple(x) for x in message.payload["nodes"]])

    def _integrate(self, nodes: List[Tuple[str, str]]) -> None:
        for nid, addr in nodes:
            if nid == self._id:
                continue
            self._record(nid, addr)
            self._add_passive(nid)

    def _sample(self, pool: Set[str], k: int) -> List[str]:
        items = [p for p in pool if p != self._id]
        if len(items) <= k:
            return items
        return self._rng.sample(items, k)

    # -- periodic maintenance ----------------------------------------------

    async def _tick(self) -> None:
        # Repair: if the active view is underfull, promote passive peers.
        while len(self.active) < self._active_size and self.passive:
            candidate = self._rng.choice(list(self.passive))
            self.passive.discard(candidate)
            priority = "high" if not self.active else "low"
            await self._send(
                candidate,
                HPV_NEIGHBOR,
                {"node_id": self._id, "address": self._node.address, "priority": priority},
            )
            # Optimistically add; a rejection reply will demote it again.
            self.active.add(candidate)
        await self._shuffle()

    def prune_dead(self, alive_ids: Set[str]) -> None:
        """Drop active/passive entries not in ``alive_ids`` (failure signal).

        Wire this to the node's failure detector to trigger overlay repair when
        peers die.
        """
        for nid in list(self.active):
            if nid not in alive_ids:
                self.active.discard(nid)
        for nid in list(self.passive):
            if nid not in alive_ids:
                self.passive.discard(nid)
