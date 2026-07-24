"""Tests for the HyParView partial-view membership overlay."""

from __future__ import annotations

import random
from collections import deque
from typing import Dict, List

import pytest

from conftest import flush
from suprime.hyparview import HyParView
from suprime.node import SwarmNode
from suprime.transport import InMemoryTransport


def _connected(views: Dict[str, HyParView]) -> bool:
    """True if the undirected overlay induced by active views is connected."""
    ids = list(views)
    if not ids:
        return True
    adj: Dict[str, set] = {i: set() for i in ids}
    for nid, hpv in views.items():
        for peer in hpv.active:
            if peer in adj:
                adj[nid].add(peer)
                adj[peer].add(nid)  # treat as undirected
    seen = {ids[0]}
    q = deque([ids[0]])
    while q:
        cur = q.popleft()
        for nb in adj[cur]:
            if nb not in seen:
                seen.add(nb)
                q.append(nb)
    return len(seen) == len(ids)


async def _build(n: int, active_size: int = 3, passive_size: int = 6):
    registry: dict = {}
    nodes: List[SwarmNode] = []
    views: Dict[str, HyParView] = {}
    for i in range(n):
        transport = InMemoryTransport(f"h{i}", registry=registry)
        node = SwarmNode(transport=transport, node_id=f"h{i}")
        await node.start(auto=False)
        hpv = HyParView(
            node,
            active_size=active_size,
            passive_size=passive_size,
            rng=random.Random(i),
        )
        nodes.append(node)
        views[node.id] = hpv
    return nodes, views


async def _drive(nodes, rounds=25):
    for _ in range(rounds):
        for node in nodes:
            await node.tick()
        await flush()


@pytest.mark.asyncio
async def test_hyparview_builds_bounded_connected_overlay():
    active_size = 3
    nodes, views = await _build(8, active_size=active_size)
    try:
        # everyone joins through the first node
        contact = nodes[0].address
        for node in nodes[1:]:
            await views[node.id].join(contact)
            await flush()

        await _drive(nodes, rounds=30)

        # active views respect the degree bound
        for hpv in views.values():
            assert len(hpv.active) <= active_size

        # the overlay is a single connected component
        assert _connected(views)

        # every node has at least one active link (nobody isolated)
        assert all(len(hpv.active) >= 1 for hpv in views.values())
    finally:
        for node in nodes:
            await node.stop()


@pytest.mark.asyncio
async def test_hyparview_shuffle_populates_passive_views():
    nodes, views = await _build(8, active_size=3, passive_size=6)
    try:
        contact = nodes[0].address
        for node in nodes[1:]:
            await views[node.id].join(contact)
            await flush()
        await _drive(nodes, rounds=40)

        # shuffles should have seeded passive views with backup peers
        total_passive = sum(len(hpv.passive) for hpv in views.values())
        assert total_passive > 0
    finally:
        for node in nodes:
            await node.stop()


@pytest.mark.asyncio
async def test_hyparview_repairs_after_node_failure():
    nodes, views = await _build(7, active_size=3, passive_size=6)
    try:
        contact = nodes[0].address
        for node in nodes[1:]:
            await views[node.id].join(contact)
            await flush()
        await _drive(nodes, rounds=40)
        assert _connected(views)

        # Kill a node and signal its failure to the overlay.
        victim = nodes[3]
        await victim.stop()
        survivors = [n for n in nodes if n.id != victim.id]
        survivor_views = {n.id: views[n.id] for n in survivors}
        alive_ids = set(survivor_views)
        for hpv in survivor_views.values():
            hpv.prune_dead(alive_ids)

        # the overlay repairs itself from passive views
        await _drive(survivors, rounds=40)
        assert victim.id not in {
            p for hpv in survivor_views.values() for p in hpv.active
        }
        assert _connected(survivor_views)
    finally:
        for node in nodes:
            if node.id in {n.id for n in nodes}:
                try:
                    await node.stop()
                except Exception:
                    pass
