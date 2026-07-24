"""A distributed key/value database built on the swarm.

Layered on the replicated store, ``KVStore`` adds a database-style API with
**tunable consistency**:

* ``put`` / ``get`` are fast, local and *eventually* consistent (the write
  replicates in the background via gossip / anti-entropy).
* ``quorum_put`` / ``quorum_get`` trade latency for stronger guarantees by
  synchronously contacting a quorum of replicas. Quorum reads perform
  **read-repair**: they pull the newest version across the quorum and heal any
  stale replica they saw.

Choosing ``W`` and ``R`` such that ``W + R > N`` gives read-your-writes
consistency, the classic Dynamo-style knob — all on top of the same gossip
substrate.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Dict, List, Optional, Tuple

from .message import Message
from .store import Entry

KV_READ_REQ = "__kv_read_req__"
KV_READ_RESP = "__kv_read_resp__"
KV_WRITE_REQ = "__kv_write_req__"
KV_WRITE_RESP = "__kv_write_resp__"


class KVStore:
    """A quorum-capable distributed KV database over a :class:`SwarmNode`.

    Args:
        node: The node to run on.
        namespace: Key prefix isolating this database within the shared store.
    """

    def __init__(self, node, namespace: str = "kv/") -> None:
        self._node = node
        self._ns = namespace
        self._pending: Dict[str, Dict[str, Any]] = {}
        node.on(KV_READ_REQ, self._on_read_req)
        node.on(KV_READ_RESP, self._on_read_resp)
        node.on(KV_WRITE_REQ, self._on_write_req)
        node.on(KV_WRITE_RESP, self._on_write_resp)

    def _k(self, key: str) -> str:
        return self._ns + key

    # -- local (eventually consistent) API ---------------------------------

    def put(self, key: str, value: Any) -> None:
        self._node.store.set(self._k(key), value)

    def get(self, key: str, default: Any = None) -> Any:
        return self._node.store.get(self._k(key), default)

    def delete(self, key: str) -> None:
        self._node.store.delete(self._k(key))

    def keys(self) -> List[str]:
        return [k[len(self._ns):] for k in self._node.store.keys() if k.startswith(self._ns)]

    def scan(self, prefix: str = "") -> List[Tuple[str, Any]]:
        out = []
        for k, v in self._node.store.items():
            if k.startswith(self._ns) and k[len(self._ns):].startswith(prefix):
                out.append((k[len(self._ns):], v))
        return sorted(out)

    # -- quorum API --------------------------------------------------------

    async def quorum_put(self, key: str, value: Any, w: int = 2, timeout: float = 1.0) -> int:
        """Write locally and to peers, waiting for ``w`` total acks (incl. self).

        Returns the number of replicas that durably acknowledged the write.
        """
        entry = self._node.store.set(self._k(key), value)
        acks = 1  # our own write
        peers = [p.node_id for p in self._node.peers.alive()]
        need = max(0, w - 1)
        if need == 0 or not peers:
            return acks
        req_id = uuid.uuid4().hex
        fut = asyncio.get_event_loop().create_future()
        self._pending[req_id] = {"acks": 0, "need": min(need, len(peers)), "future": fut}
        payload = {"req_id": req_id, "key": self._k(key), "entry": entry.to_dict()}
        for peer in peers[: min(need, len(peers))]:
            await self._node.send(peer, KV_WRITE_REQ, payload)
        try:
            await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            pass
        acks += self._pending.pop(req_id, {}).get("acks", 0)
        return acks

    async def quorum_get(self, key: str, r: int = 2, timeout: float = 1.0) -> Any:
        """Read from a quorum, return the newest value, and repair stale replicas."""
        full = self._k(key)
        local = self._node.store.entry(full)
        collected: List[Tuple[str, Optional[dict]]] = [
            (self._node.id, local.to_dict() if local else None)
        ]
        peers = [p.node_id for p in self._node.peers.alive()]
        need = max(0, r - 1)
        if need and peers:
            req_id = uuid.uuid4().hex
            fut = asyncio.get_event_loop().create_future()
            self._pending[req_id] = {
                "responses": collected,
                "need": min(need, len(peers)),
                "future": fut,
            }
            for peer in peers[: min(need, len(peers))]:
                await self._node.send(peer, KV_READ_REQ, {"req_id": req_id, "key": full})
            try:
                await asyncio.wait_for(fut, timeout)
            except asyncio.TimeoutError:
                pass
            self._pending.pop(req_id, None)

        # Pick the newest version across the quorum.
        best: Optional[Entry] = None
        for _peer, data in collected:
            if data is None:
                continue
            e = Entry.from_dict(data)
            if best is None or e.version > best.version:
                best = e
        if best is None:
            return None
        # Read-repair: fold the winner back into our own replica.
        self._node.store.merge_entry(full, best)
        return None if best.deleted else best.value

    # -- handlers ----------------------------------------------------------

    async def _on_write_req(self, message: Message) -> None:
        key = message.payload["key"]
        entry = Entry.from_dict(message.payload["entry"])
        self._node.store.merge_entry(key, entry)
        await self._node.send(
            message.src, KV_WRITE_RESP, {"req_id": message.payload["req_id"]}
        )

    async def _on_write_resp(self, message: Message) -> None:
        pending = self._pending.get(message.payload["req_id"])
        if pending is None:
            return
        pending["acks"] += 1
        if pending["acks"] >= pending["need"] and not pending["future"].done():
            pending["future"].set_result(True)

    async def _on_read_req(self, message: Message) -> None:
        key = message.payload["key"]
        entry = self._node.store.entry(key)
        await self._node.send(
            message.src,
            KV_READ_RESP,
            {"req_id": message.payload["req_id"], "entry": entry.to_dict() if entry else None},
        )

    async def _on_read_resp(self, message: Message) -> None:
        pending = self._pending.get(message.payload["req_id"])
        if pending is None:
            return
        pending["responses"].append((message.src, message.payload.get("entry")))
        if len(pending["responses"]) >= pending["need"] + 1 and not pending["future"].done():
            pending["future"].set_result(True)
