"""Topic-based publish/subscribe over the Plumtree broadcast tree.

A thin, useful application layer: publishers send messages tagged with a topic,
and every node delivers each message only to its local subscribers for that
topic. Dissemination rides :class:`~suprime.plumtree.PlumtreeBroadcast`, so a
publish reaches the whole swarm efficiently (spanning-tree, O(N) copies) and
exactly once per node, while topic filtering happens locally on delivery.

This turns the swarm into a decentralised event bus with no broker.
"""

from __future__ import annotations

import random
from typing import Any, Callable, Dict, List, Optional

from .plumtree import PlumtreeBroadcast

Subscriber = Callable[[str, Any], None]  # (topic, payload)


class PubSub:
    """A decentralised topic event bus for one node.

    Args:
        node: The :class:`~suprime.node.SwarmNode` to run on.
        broadcast: An existing Plumtree instance to reuse; one is created if
            omitted.
        rng: Injectable RNG (only used when creating a Plumtree).
    """

    def __init__(
        self,
        node,
        broadcast: Optional[PlumtreeBroadcast] = None,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._node = node
        self._bcast = broadcast or PlumtreeBroadcast(node, rng=rng)
        self._bcast._on_deliver = self._deliver
        self._subs: Dict[str, List[Subscriber]] = {}

    def subscribe(self, topic: str, callback: Subscriber) -> None:
        """Register ``callback(topic, payload)`` for messages on ``topic``."""
        self._subs.setdefault(topic, []).append(callback)

    def unsubscribe(self, topic: str) -> None:
        self._subs.pop(topic, None)

    async def publish(self, topic: str, payload: Any) -> str:
        """Publish ``payload`` on ``topic`` to the whole swarm."""
        return await self._bcast.broadcast({"topic": topic, "data": payload})

    def _deliver(self, msg_id: str, envelope: dict) -> None:
        topic = envelope.get("topic")
        data = envelope.get("data")
        for callback in self._subs.get(topic, []):
            callback(topic, data)
