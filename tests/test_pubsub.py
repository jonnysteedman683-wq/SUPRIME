"""Tests for topic pub/sub over Plumtree."""

from __future__ import annotations

import random

import pytest

from conftest import Cluster
from suprime.pubsub import PubSub


@pytest.mark.asyncio
async def test_pubsub_delivers_only_to_subscribers(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(5)]
    buses = [PubSub(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)

    inbox = {n.id: [] for n in nodes}
    # nodes 1,2,3 subscribe to "weather"; node 4 subscribes to "sports"
    for i in (1, 2, 3):
        buses[i].subscribe("weather", (lambda nid: (lambda t, d: inbox[nid].append((t, d))))(nodes[i].id))
    buses[4].subscribe("sports", (lambda t, d: inbox["n4"].append((t, d))))

    await buses[0].publish("weather", {"temp": 21})
    await cluster.settle(nodes, rounds=15)

    # weather subscribers got it; the sports-only subscriber and publisher did not
    for i in (1, 2, 3):
        assert inbox[nodes[i].id] == [("weather", {"temp": 21})]
    assert inbox["n4"] == []
    assert inbox["n0"] == []


@pytest.mark.asyncio
async def test_pubsub_multiple_topics_and_publishers(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(4)]
    buses = [PubSub(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)

    got = {n.id: [] for n in nodes}
    for i, node in enumerate(nodes):
        buses[i].subscribe("alerts", (lambda nid: (lambda t, d: got[nid].append(d)))(node.id))

    await buses[0].publish("alerts", "A")
    await cluster.settle(nodes, rounds=10)
    await buses[3].publish("alerts", "B")
    await cluster.settle(nodes, rounds=10)

    # every node received both alerts, each exactly once
    for node in nodes:
        assert sorted(got[node.id]) == ["A", "B"]
