"""Merkle-style anti-entropy: reconcile stores by exchanging only diffs.

Baseline gossip ships the *entire* store digest every round — O(state) bandwidth
even when two nodes are already almost identical. Anti-entropy fixes that: keys
are hashed into buckets, and each node summarises its store as one hash per
bucket (a shallow Merkle tree). Two nodes first compare bucket hashes — tiny —
and then exchange full entries only for the buckets that actually differ.

When stores are mostly in sync (the common case) this transfers a handful of
entries instead of the whole dataset, which is what lets the store scale.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Set

from .message import Message

AE_DIGEST = "__ae_digest__"
AE_PULL = "__ae_pull__"
AE_PUSH = "__ae_push__"


def _bucket_of(key: str, n_buckets: int) -> int:
    h = hashlib.sha1(key.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big") % n_buckets


def bucket_hashes(digest: Dict[str, Dict[str, Any]], n_buckets: int) -> Dict[int, str]:
    """Hash each bucket's ``(key, version)`` pairs into a single fingerprint."""
    buckets: Dict[int, List[str]] = {}
    for key, entry in digest.items():
        b = _bucket_of(key, n_buckets)
        # Version (ts, origin, deleted) identifies content under LWW semantics.
        buckets.setdefault(b, []).append(
            f"{key}|{entry['ts']}|{entry['origin']}|{entry.get('deleted', False)}"
        )
    out: Dict[int, str] = {}
    for b, items in buckets.items():
        joined = "\n".join(sorted(items)).encode("utf-8")
        out[b] = hashlib.sha1(joined).hexdigest()
    return out


def diff_buckets(local: Dict[int, str], remote: Dict[int, str]) -> Set[int]:
    """Bucket indices whose fingerprints differ (or exist on only one side)."""
    mismatched: Set[int] = set()
    for b in local.keys() | remote.keys():
        if local.get(b) != remote.get(b):
            mismatched.add(b)
    return mismatched


def entries_for_buckets(
    digest: Dict[str, Dict[str, Any]], buckets: Set[int], n_buckets: int
) -> Dict[str, Dict[str, Any]]:
    return {
        key: entry
        for key, entry in digest.items()
        if _bucket_of(key, n_buckets) in buckets
    }


class AntiEntropy:
    """Delta-reconciliation service for a node's :class:`DistributedStore`.

    Args:
        node: The :class:`~suprime.node.SwarmNode` whose store to reconcile.
        n_buckets: Number of Merkle buckets (more = finer diffs, bigger digest).
    """

    def __init__(self, node, n_buckets: int = 32) -> None:
        self._node = node
        self._store = node.store
        self._n = n_buckets
        # Observability: how many full entries this node has sent/received.
        self.entries_sent = 0
        self.entries_received = 0
        node.on(AE_DIGEST, self._on_digest)
        node.on(AE_PULL, self._on_pull)
        node.on(AE_PUSH, self._on_push)
        node.on_tick(self._tick)

    async def _tick(self) -> None:
        alive = self._node.peers.alive()
        if not alive:
            return
        # Reconcile with one random peer per round.
        peer = self._node.gossip._rng.choice(alive)
        await self._node.send(
            peer.node_id,
            AE_DIGEST,
            {"buckets": bucket_hashes(self._store.digest(), self._n), "n": self._n},
        )

    async def _on_digest(self, message: Message) -> None:
        remote = {int(k): v for k, v in message.payload["buckets"].items()}
        local = bucket_hashes(self._store.digest(), self._n)
        mismatched = diff_buckets(local, remote)
        if not mismatched:
            return
        # Push our entries for the differing buckets, and ask the peer for theirs.
        entries = entries_for_buckets(self._store.digest(), mismatched, self._n)
        self.entries_sent += len(entries)
        await self._node.send(message.src, AE_PUSH, {"entries": entries})
        await self._node.send(message.src, AE_PULL, {"buckets": list(mismatched)})

    async def _on_pull(self, message: Message) -> None:
        buckets = set(message.payload["buckets"])
        entries = entries_for_buckets(self._store.digest(), buckets, self._n)
        self.entries_sent += len(entries)
        await self._node.send(message.src, AE_PUSH, {"entries": entries})

    async def _on_push(self, message: Message) -> None:
        entries = message.payload["entries"]
        self.entries_received += len(entries)
        self._store.apply_digest(entries)
