"""Demonstrate SUPRIME's experimental features programmatically.

Covers, in one in-process run:

* the chaos harness partitioning and healing the network,
* push-sum aggregation computing a swarm-wide average,
* stigmergic load balancing steering work to idle nodes,
* Plumtree epidemic broadcast reaching every node.

Run with::

    python examples/experimental.py
"""

from __future__ import annotations

import asyncio
import random

from suprime import (
    ChaosController,
    ChaosTransport,
    PlumtreeBroadcast,
    PushSumAggregator,
    SwarmNode,
)
from suprime.transport import InMemoryTransport


async def build_swarm(n, chaos):
    registry: dict = {}
    nodes = []
    for i in range(n):
        inner = InMemoryTransport(f"node-{i}", registry=registry)
        transport = ChaosTransport(inner, chaos)
        seeds = [nodes[0].address] if nodes else None
        node = SwarmNode(
            transport=transport,
            node_id=f"node-{i}",
            seeds=seeds,
            gossip_interval=0.15,
            suspect_after=2.0,
            dead_after=4.0,
            rng=random.Random(i),
        )
        await node.start()
        nodes.append(node)
    await asyncio.sleep(2)
    return nodes


async def main() -> None:
    chaos = ChaosController(latency=0.003)
    nodes = await build_swarm(6, chaos)

    # --- push-sum aggregation: average of per-node values -------------------
    aggs = [PushSumAggregator(n, rng=random.Random(100 + i)) for i, n in enumerate(nodes)]
    values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
    for agg, v in zip(aggs, values):
        agg.average("load", v)
    await asyncio.sleep(3)
    print(f"push-sum average of {values} (true mean {sum(values)/len(values):.1f}):")
    for n, agg in zip(nodes, aggs):
        print(f"  {n.id}: {agg.estimate('load'):.2f}")

    # --- stigmergic load balancing -----------------------------------------
    load = {n.id: 0.0 for n in nodes}
    ran = {n.id: 0 for n in nodes}
    for n in nodes:
        n.tasks.set_load_model(lambda nid=n.id: load[nid], weight=0.01)

        def handler(task, nid=n.id):
            ran[nid] += 1
            load[nid] += 1.0  # getting busier makes future claims less likely
            return task.args["x"] * 2

        n.tasks.register_handler("double", handler)
    for i in range(12):
        nodes[i % 6].tasks.submit("double", {"x": i})
    await asyncio.sleep(4)
    print("\nstigmergic load balancing — tasks run per node:")
    for n in nodes:
        print(f"  {n.id}: {ran[n.id]}")

    # --- Plumtree broadcast ------------------------------------------------
    got = {n.id: 0 for n in nodes}
    trees = [
        PlumtreeBroadcast(n, on_deliver=(lambda nid: (lambda mid, p: got.__setitem__(nid, got[nid] + 1)))(n.id))
        for n in nodes
    ]
    await asyncio.sleep(1)
    await trees[0].broadcast({"announcement": "hello swarm"})
    await asyncio.sleep(2)
    print("\nPlumtree broadcast — deliveries per node (want 1 each):")
    for n in nodes:
        print(f"  {n.id}: {got[n.id]}")
    total_eager = sum(len(t.eager) for t in trees)
    print(f"  eager tree edges: {total_eager} (full mesh would be {len(nodes)*(len(nodes)-1)})")

    # --- chaos: partition then heal ----------------------------------------
    print("\nchaos: partitioning the swarm...")
    half = len(nodes) // 2
    chaos.partition([n.address for n in nodes[:half]], [n.address for n in nodes[half:]])
    nodes[0].store.set("beacon", "LEFT")
    nodes[-1].store.set("beacon", "RIGHT")
    await asyncio.sleep(3)
    print("  during partition:", {n.id: n.store.get("beacon") for n in nodes})
    chaos.heal()
    await asyncio.sleep(4)
    print("  after healing:   ", {n.id: n.store.get("beacon") for n in nodes})
    print(f"  chaos stats: {chaos.stats()}")

    for n in nodes:
        await n.stop()


if __name__ == "__main__":
    asyncio.run(main())
