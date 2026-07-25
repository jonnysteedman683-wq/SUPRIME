"""The epidemic dissemination layer.

Gossip is how the swarm stays coherent without any central broker. On every
round a node picks a small random subset of peers (the *fanout*) and pushes a
digest containing:

* its own heartbeat and address,
* its view of membership,
* its slice of the replicated store.

The receiver merges everything into its own state. Repeated over many rounds
this epidemic spread drives every replica toward the same view — membership,
key/value data and task coordination all ride the same channel.
"""

from __future__ import annotations

import math
import random
from typing import Any, Callable, Dict, List, Union

from .message import Message, MessageType
from .peers import PeerTable
from .store import DistributedStore


class GossipService:
    """Builds and applies gossip digests for one node.

    Args:
        self_id: The owning node's identity.
        address: The owning node's transport address, or a zero-arg callable
            returning it. A callable is preferred when the address is only
            known after the transport starts (e.g. an OS-assigned TCP port).
        peers: The membership table to disseminate and update.
        store: The replicated store to disseminate and update.
        fanout: How many peers to push to each round.
        rng: Injectable RNG for deterministic tests.
    """

    def __init__(
        self,
        self_id: str,
        address: Union[str, Callable[[], str]],
        peers: PeerTable,
        store: DistributedStore,
        fanout: int = 3,
        rng: random.Random | None = None,
        include_store: bool = True,
        adaptive: bool = False,
        max_fanout: int = 8,
    ) -> None:
        self._self_id = self_id
        self._address_provider: Callable[[], str] = (
            address if callable(address) else (lambda: address)
        )
        self._peers = peers
        self._store = store
        self._fanout = fanout
        self._rng = rng or random.Random()
        self._heartbeat = 0
        # When False, gossip carries only membership; state replication is left
        # to a dedicated layer (e.g. Merkle anti-entropy).
        self._include_store = include_store
        # Adaptive fanout: scale the per-round fanout with swarm size so
        # dissemination still finishes in ~O(log N) rounds as N grows, with a
        # temporary boost to max_fanout right after membership churn.
        self._adaptive = adaptive
        self._max_fanout = max_fanout
        self._boost = 0

    def boost(self, rounds: int = 3) -> None:
        """Temporarily gossip at max fanout for the next ``rounds`` (churn)."""
        self._boost = max(self._boost, rounds)

    def effective_fanout(self) -> int:
        """The fanout to use this round given swarm size and any churn boost."""
        if not self._adaptive:
            return self._fanout
        n = len(self._peers)
        # ~log2(N) neighbours per round keeps convergence logarithmic in N.
        base = max(self._fanout, math.ceil(math.log2(n + 2)))
        if self._boost > 0:
            base = self._max_fanout
        return min(self._max_fanout, base)

    @property
    def heartbeat(self) -> int:
        return self._heartbeat

    def bump_heartbeat(self) -> int:
        self._heartbeat += 1
        return self._heartbeat

    def select_targets(self) -> List[str]:
        """Choose up to the effective fanout of random peer addresses."""
        addresses = self._peers.addresses()
        fanout = self.effective_fanout()
        if self._boost > 0:
            self._boost -= 1
        if len(addresses) <= fanout:
            return addresses
        return self._rng.sample(addresses, fanout)

    def build_digest(self) -> Dict[str, Any]:
        """Assemble the payload pushed to peers this round."""
        membership = self._peers.digest()
        # Always include ourselves so peers learn/refresh our heartbeat.
        membership.append(
            {
                "node_id": self._self_id,
                "address": self._address_provider(),
                "heartbeat": self._heartbeat,
            }
        )
        store = self._store.digest() if self._include_store else {}
        return {"membership": membership, "store": store}

    def make_message(self, msg_type: str = MessageType.GOSSIP) -> Message:
        return Message(type=msg_type, src=self._self_id, payload=self.build_digest())

    def apply(self, message: Message) -> bool:
        """Merge an incoming gossip digest; return whether state changed."""
        payload = message.payload
        changed = False
        if self._peers.apply_digest(payload.get("membership", [])):
            changed = True
        if self._store.apply_digest(payload.get("store", {})):
            changed = True
        return changed
