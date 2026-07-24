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

    @property
    def heartbeat(self) -> int:
        return self._heartbeat

    def bump_heartbeat(self) -> int:
        self._heartbeat += 1
        return self._heartbeat

    def select_targets(self) -> List[str]:
        """Choose up to ``fanout`` random peer addresses to gossip to."""
        addresses = self._peers.addresses()
        if len(addresses) <= self._fanout:
            return addresses
        return self._rng.sample(addresses, self._fanout)

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
        return {"membership": membership, "store": self._store.digest()}

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
