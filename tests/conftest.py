"""Shared test fixtures and helpers for the SUPRIME swarm.

Tests run whole swarms inside a single event loop using the in-memory transport
and drive them deterministically via :meth:`SwarmNode.tick`, so there are no
sockets, wall-clock sleeps or flakiness. A :class:`ManualClock` is injected as
the failure-detector's time source and advanced explicitly by :meth:`Cluster.settle`
so heartbeat timeouts are exercised deterministically.
"""

from __future__ import annotations

import asyncio
import random
from typing import Dict, List, Optional

import pytest

from suprime.node import SwarmNode
from suprime.transport import InMemoryTransport


class ManualClock:
    """A monotonic clock advanced by hand, for deterministic timeouts."""

    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def __call__(self) -> float:
        return self._t

    def advance(self, dt: float) -> None:
        self._t += dt


async def flush(times: int = 40) -> None:
    """Yield control so queued in-memory deliveries are processed."""
    for _ in range(times):
        await asyncio.sleep(0)


class Cluster:
    """A private in-process swarm with a shared registry and manual clock."""

    def __init__(self) -> None:
        self.registry: Dict[str, InMemoryTransport] = {}
        self.mono = ManualClock()
        self.nodes: List[SwarmNode] = []

    def node(
        self,
        node_id: str,
        *,
        seeds: Optional[List[str]] = None,
        seed: int = 0,
        **kwargs,
    ) -> SwarmNode:
        transport = InMemoryTransport(node_id, registry=self.registry)
        node = SwarmNode(
            transport=transport,
            node_id=node_id,
            seeds=seeds,
            rng=random.Random(seed),
            monotonic=self.mono,
            **kwargs,
        )
        self.nodes.append(node)
        return node

    async def start_chain(self, nodes: List[SwarmNode]) -> None:
        """Start nodes so each (after the first) bootstraps from the first."""
        for i, node in enumerate(nodes):
            node._seeds = [nodes[0].address] if i else []
            await node.start(auto=False)
        await flush()

    async def settle(
        self,
        nodes: Optional[List[SwarmNode]] = None,
        rounds: int = 12,
        dt: float = 1.0,
    ) -> None:
        """Drive the swarm until it converges, advancing the clock each round."""
        targets = nodes if nodes is not None else self.nodes
        for _ in range(rounds):
            self.mono.advance(dt)
            for node in targets:
                await node.tick()
            await flush()

    async def stop_all(self) -> None:
        for node in self.nodes:
            try:
                await node.stop()
            except Exception:
                pass


@pytest.fixture
async def cluster() -> Cluster:
    c = Cluster()
    try:
        yield c
    finally:
        await c.stop_all()
