"""Deterministic simulation testing (DST) for the swarm.

Inspired by FoundationDB / TigerBeetle: run the *entire* swarm on a single
seeded pseudo-random schedule, injecting faults — message drops, reordering,
latency, network partitions and node crash/restart — and step it forward
deterministically. Because everything (RNG, clock, message delivery order) is
driven from one seed, a failing run is perfectly reproducible: same seed, same
history, every time.

The :class:`Simulator` owns a virtual network and clock. Each *step* advances
virtual time, maybe mutates the network (partition/heal/crash/restart), ticks
every live node, and delivers queued messages in a seeded order. Tests then
assert invariants — state convergence, exactly-once task execution, no lost
writes — hold at the end of thousands of randomized histories.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple

from .message import Message
from .node import SwarmNode
from .transport import Transport, TransportError


class SimClock:
    """A virtual monotonic clock advanced only by the simulator."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class SimNetwork:
    """A deterministic in-memory network with a seeded delivery queue.

    Messages are not delivered inline; they are enqueued with a virtual
    delivery time and flushed by the simulator, so ordering, delay and loss are
    all under the seed's control.
    """

    def __init__(self, rng: random.Random, clock: SimClock) -> None:
        self._rng = rng
        self._clock = clock
        self._transports: Dict[str, "SimTransport"] = {}
        # queue of (deliver_at, seq, dst_address, message)
        self._queue: List[Tuple[float, int, str, Message]] = []
        self._seq = 0
        self._partitions: List[Set[str]] = []
        self.drop_rate = 0.0
        self.max_latency = 0.0
        self.delivered = 0
        self.dropped = 0

    def register(self, transport: "SimTransport") -> None:
        self._transports[transport.address] = transport

    def unregister(self, address: str) -> None:
        self._transports.pop(address, None)

    def can_reach(self, src: str, dst: str) -> bool:
        if not self._partitions:
            return True
        for g in self._partitions:
            if src in g and dst in g:
                return True
        placed = any(src in g or dst in g for g in self._partitions)
        return not placed

    def partition(self, *groups: List[str]) -> None:
        self._partitions = [set(g) for g in groups]

    def heal(self) -> None:
        self._partitions = []

    def enqueue(self, src: str, dst: str, message: Message) -> None:
        if dst not in self._transports:
            raise TransportError(f"no route to {dst}")
        if not self.can_reach(src, dst):
            raise TransportError(f"partitioned {src} -> {dst}")
        if self.drop_rate and self._rng.random() < self.drop_rate:
            self.dropped += 1
            return
        latency = self._rng.random() * self.max_latency if self.max_latency else 0.0
        self._seq += 1
        self._queue.append((self._clock.t + latency, self._seq, dst, message))

    def flush_due(self) -> None:
        """Deliver every message whose delivery time has arrived, in seed order."""
        due = [item for item in self._queue if item[0] <= self._clock.t]
        self._queue = [item for item in self._queue if item[0] > self._clock.t]
        # Shuffle same-time deliveries deterministically to exercise reordering.
        self._rng.shuffle(due)
        due.sort(key=lambda it: it[0])
        for _at, _seq, dst, message in due:
            transport = self._transports.get(dst)
            if transport is None:
                continue  # recipient crashed
            self.delivered += 1
            asyncio.ensure_future(transport._deliver(message))


class SimTransport(Transport):
    """Transport that routes through a :class:`SimNetwork` delivery queue."""

    def __init__(self, address: str, network: SimNetwork) -> None:
        self.address = address
        self._network = network
        self._on_message = None
        self._up = False

    async def start(self, on_message) -> None:
        self._on_message = on_message
        self._up = True
        self._network.register(self)

    async def send(self, address: str, message: Message) -> None:
        if not self._up:
            raise TransportError("transport down")
        self._network.enqueue(self.address, address, message)

    async def _deliver(self, message: Message) -> None:
        if self._up and self._on_message is not None:
            await self._on_message(message)

    async def stop(self) -> None:
        self._up = False
        self._network.unregister(self.address)


@dataclass
class SimConfig:
    n_nodes: int = 5
    seed: int = 0
    steps: int = 200
    drop_rate: float = 0.05
    max_latency: float = 2.0
    partition_prob: float = 0.02
    heal_prob: float = 0.2
    crash_prob: float = 0.01
    restart_prob: float = 0.3
    dt: float = 1.0
    # Failure detector timeouts must be long enough to tolerate the fault load.
    dead_after: float = 1e9


class Simulator:
    """Drives a swarm through a deterministic, fault-injected history."""

    def __init__(self, cfg: SimConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.clock = SimClock()
        self.network = SimNetwork(self.rng, self.clock)
        self.network.drop_rate = cfg.drop_rate
        self.network.max_latency = cfg.max_latency
        self.nodes: List[SwarmNode] = []
        self._addresses: List[str] = []
        self._crashed: Set[str] = set()

    async def build(self) -> None:
        for i in range(self.cfg.n_nodes):
            addr = f"sim-{i}"
            transport = SimTransport(addr, self.network)
            seeds = [self._addresses[0]] if self._addresses else None
            node = SwarmNode(
                transport=transport,
                node_id=f"n{i}",
                seeds=seeds,
                fanout=3,
                rng=random.Random(self.cfg.seed * 100 + i),
                monotonic=self.clock,
                dead_after=self.cfg.dead_after,
            )
            await node.start(auto=False)
            self.nodes.append(node)
            self._addresses.append(addr)
        await self._settle_pump()

    async def _settle_pump(self, rounds: int = 3) -> None:
        for _ in range(rounds):
            self.network.flush_due()
            for _ in range(20):
                await asyncio.sleep(0)

    def live_nodes(self) -> List[SwarmNode]:
        return [n for n in self.nodes if n.id not in self._crashed]

    async def step(self) -> None:
        self.clock.t += self.cfg.dt
        self._maybe_fault()
        for node in self.live_nodes():
            await node.tick()
        self.network.flush_due()
        for _ in range(15):
            await asyncio.sleep(0)

    def _maybe_fault(self) -> None:
        r = self.rng.random
        if r() < self.cfg.partition_prob:
            k = len(self.nodes) // 2
            shuffled = list(self._addresses)
            self.rng.shuffle(shuffled)
            self.network.partition(shuffled[:k], shuffled[k:])
        elif r() < self.cfg.heal_prob:
            self.network.heal()
        # crash a random live node
        if r() < self.cfg.crash_prob and len(self.live_nodes()) > 2:
            victim = self.rng.choice(self.live_nodes())
            self._crashed.add(victim.id)
        # restart a crashed node
        if self._crashed and r() < self.cfg.restart_prob:
            reborn = self.rng.choice(list(self._crashed))
            self._crashed.discard(reborn)

    async def run(self) -> None:
        await self.build()
        for _ in range(self.cfg.steps):
            await self.step()

    async def quiesce(self, rounds: int = 60) -> None:
        """Heal everything, revive all nodes and run until the swarm settles."""
        self.network.heal()
        self.network.drop_rate = 0.0
        self.network.max_latency = 0.0
        self._crashed.clear()
        for _ in range(rounds):
            self.clock.t += self.cfg.dt
            for node in self.nodes:
                await node.tick()
            self.network.flush_due()
            for _ in range(20):
                await asyncio.sleep(0)

    async def stop(self) -> None:
        for node in self.nodes:
            try:
                await node.stop()
            except Exception:
                pass
