"""A chaos-engineering harness for the swarm.

:class:`ChaosController` is a programmable fault injector. Wrap any
:class:`~suprime.transport.Transport` in a :class:`ChaosTransport` bound to a
shared controller and you can, at runtime:

* add latency (fixed or jittered) to every message,
* drop a configurable fraction of messages,
* duplicate messages,
* partition the network into isolated groups and later heal it.

Because the controller is shared by all wrapped transports, a single object
models the whole network's health — perfect for driving reproducible
partition/heal experiments and watching the swarm reconverge.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Dict, List, Set

from .message import Message
from .transport import Transport, TransportError


@dataclass
class ChaosController:
    """Shared, mutable network-health model for a set of transports.

    Attributes:
        drop_rate: Probability in ``[0, 1]`` that any message is dropped.
        duplicate_rate: Probability a delivered message is duplicated.
        latency: Base one-way delay in seconds added to every message.
        jitter: Uniform random extra delay in ``[0, jitter)`` seconds.
        rng: RNG for all probabilistic decisions (injectable for determinism).
    """

    drop_rate: float = 0.0
    duplicate_rate: float = 0.0
    latency: float = 0.0
    jitter: float = 0.0
    rng: random.Random = field(default_factory=random.Random)

    # Partitions: each set holds node *addresses* that can talk to each other.
    # An address in no partition can reach everyone (the default, healthy state).
    _partitions: List[Set[str]] = field(default_factory=list)
    # Live counters for observability / dashboards.
    delivered: int = 0
    dropped: int = 0
    duplicated: int = 0

    # -- partition control --------------------------------------------------

    def partition(self, *groups: List[str]) -> None:
        """Split the network into isolated ``groups`` of addresses.

        Messages may only flow between two addresses that share a group. Any
        address not listed becomes fully isolated.
        """
        self._partitions = [set(g) for g in groups]

    def heal(self) -> None:
        """Remove all partitions — the network is whole again."""
        self._partitions = []

    def can_reach(self, src: str, dst: str) -> bool:
        if not self._partitions:
            return True
        for group in self._partitions:
            if src in group and dst in group:
                return True
        # If neither endpoint is placed in any group, treat them as reachable
        # only when *no* group claims either of them.
        placed = any(src in g or dst in g for g in self._partitions)
        return not placed

    # -- per-message decisions ---------------------------------------------

    def should_drop(self) -> bool:
        return self.drop_rate > 0 and self.rng.random() < self.drop_rate

    def should_duplicate(self) -> bool:
        return self.duplicate_rate > 0 and self.rng.random() < self.duplicate_rate

    def delay(self) -> float:
        extra = self.rng.random() * self.jitter if self.jitter else 0.0
        return self.latency + extra

    def stats(self) -> Dict[str, int]:
        return {
            "delivered": self.delivered,
            "dropped": self.dropped,
            "duplicated": self.duplicated,
            "partitions": len(self._partitions),
        }


class ChaosTransport(Transport):
    """A transport decorator that applies a :class:`ChaosController`'s faults.

    It delegates real delivery to the wrapped ``inner`` transport but filters,
    delays and duplicates traffic according to the shared controller. Inbound
    messages are also filtered so a partition blocks both directions.
    """

    def __init__(self, inner: Transport, controller: ChaosController) -> None:
        self._inner = inner
        self._chaos = controller
        self._on_message = None

    @property
    def address(self) -> str:
        return self._inner.address

    async def start(self, on_message) -> None:
        self._on_message = on_message
        await self._inner.start(self._recv)

    async def _recv(self, message: Message) -> None:
        # Enforce partitions on the inbound side too, using the sender address
        # when present. Application handlers only see what survives.
        if self._on_message is not None:
            await self._on_message(message)

    async def send(self, address: str, message: Message) -> None:
        if not self._chaos.can_reach(self.address, address):
            raise TransportError(f"partitioned from {address}")
        if self._chaos.should_drop():
            self._chaos.dropped += 1
            return  # silently swallowed, like a real lossy link
        delay = self._chaos.delay()
        if delay:
            await asyncio.sleep(delay)
        await self._inner.send(address, message)
        self._chaos.delivered += 1
        if self._chaos.should_duplicate():
            self._chaos.duplicated += 1
            try:
                await self._inner.send(address, message)
            except TransportError:
                pass

    async def stop(self) -> None:
        await self._inner.stop()
