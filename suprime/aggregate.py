"""Decentralised aggregation via the push-sum (gossip aggregation) protocol.

Push-sum lets a swarm compute global aggregates — averages, sums, counts —
with no coordinator, converging exponentially fast. Each node holds a
mass pair ``(s, w)``. Every round it halves its mass, keeps one half and sends
the other to a random neighbour; incoming mass is summed in. The ratio
``s / w`` at every node converges to the same value:

* **average** of local values ``v``: initialise ``(v, 1)`` everywhere → ``s/w → mean(v)``.
* **sum** of local values: exactly one node starts with ``w = 1``, the rest
  ``w = 0`` (all keep ``s = v``) → ``s/w → Σ v``.
* **count** of nodes: as *sum* with every ``v = 1`` → ``s/w → N``.

Total mass is conserved by construction, which is what makes the estimate
unbiased even under message reordering.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Dict, List, Optional

from .message import Message

AGG_MSG = "__pushsum__"


@dataclass
class _Mass:
    s: float
    w: float

    def add(self, other: "_Mass") -> None:
        self.s += other.s
        self.w += other.w

    def half(self) -> "_Mass":
        self.s /= 2.0
        self.w /= 2.0
        return _Mass(self.s, self.w)


class PushSumAggregator:
    """Runs push-sum aggregations over a :class:`~suprime.node.SwarmNode`.

    Attach one to a node, register a couple of tick/message hooks, then start
    named aggregations. Read the current estimate any time with
    :meth:`estimate`; it converges toward the true global value each round.
    """

    def __init__(self, node, rng: Optional[random.Random] = None) -> None:
        self._node = node
        self._rng = rng or random.Random()
        # Per-aggregation mailbox of mass received (and kept) since last round.
        self._inbox: Dict[str, List[_Mass]] = {}
        self._estimate: Dict[str, float] = {}
        node.on(AGG_MSG, self._on_message)
        node.on_tick(self._round)

    # -- public API ---------------------------------------------------------

    def start(self, key: str, value: float, weight: float = 1.0) -> None:
        """Begin (or reseed) aggregation ``key`` with local mass ``(value, weight)``.

        Use ``weight=1`` on every node for an **average**; ``weight=1`` on a
        single initiator (``0`` elsewhere) for a **sum**/**count**.
        """
        self._inbox.setdefault(key, []).append(_Mass(float(value), float(weight)))

    def average(self, key: str, value: float) -> None:
        """Convenience: contribute ``value`` to an average aggregation."""
        self.start(key, value, weight=1.0)

    def estimate(self, key: str) -> Optional[float]:
        """Current local estimate of the global aggregate, or ``None`` if unknown."""
        return self._estimate.get(key)

    def keys(self) -> List[str]:
        return list(set(self._inbox) | set(self._estimate))

    # -- protocol -----------------------------------------------------------

    async def _round(self) -> None:
        for key, masses in list(self._inbox.items()):
            if not masses:
                continue
            total = _Mass(0.0, 0.0)
            for m in masses:
                total.add(m)
            self._inbox[key] = []
            if total.w > 0:
                self._estimate[key] = total.s / total.w
            # Keep half, send half to a random alive neighbour.
            keep = _Mass(total.s / 2.0, total.w / 2.0)
            send = _Mass(total.s - keep.s, total.w - keep.w)
            self._inbox[key].append(keep)
            target = self._random_peer()
            if target is None:
                # No peers: keep all mass so nothing is lost.
                self._inbox[key].append(send)
                continue
            await self._node.send(
                target, AGG_MSG, {"key": key, "s": send.s, "w": send.w}
            )

    async def _on_message(self, message: Message) -> None:
        key = message.payload["key"]
        self._inbox.setdefault(key, []).append(
            _Mass(float(message.payload["s"]), float(message.payload["w"]))
        )

    def _random_peer(self) -> Optional[str]:
        alive = [p.node_id for p in self._node.peers.alive()]
        if not alive:
            return None
        return self._rng.choice(alive)
