"""Spin up a small swarm inside one process and watch it self-organise.

Run with::

    python examples/local_swarm.py

The example uses the in-memory transport so no sockets are involved. It shows
membership discovery, replicated state, leader election with failover and
distributed task execution — the full feature set on a single machine.
"""

from __future__ import annotations

import asyncio

from suprime import SwarmNode
from suprime.transport import InMemoryTransport


def word_count(task):
    return len(str(task.args.get("text", "")).split())


async def main() -> None:
    registry: dict = {}
    nodes = []
    for i in range(5):
        transport = InMemoryTransport(f"node-{i}", registry=registry)
        seeds = [nodes[0].address] if nodes else None
        node = SwarmNode(
            transport=transport,
            node_id=f"node-{i}",
            seeds=seeds,
            gossip_interval=0.2,
        )
        node.tasks.register_handler("wordcount", word_count)
        await node.start()
        nodes.append(node)

    # Let membership converge.
    await asyncio.sleep(2)
    leader = nodes[0].leader
    print(f"Swarm formed. Elected leader: {leader}")
    for n in nodes:
        print(f"  {n.id} sees {len(n.peers.alive())} peers, leader={n.leader}")

    # Replicate some state from one node.
    nodes[3].store.set("mission", "explore")
    await asyncio.sleep(2)
    print("\nReplicated state ('mission') as seen per node:")
    for n in nodes:
        print(f"  {n.id}: {n.store.get('mission')}")

    # Submit distributed tasks; the swarm decides who runs each one.
    print("\nSubmitting tasks to the swarm...")
    ids = [
        nodes[1].tasks.submit("wordcount", {"text": "the quick brown fox"}),
        nodes[2].tasks.submit("wordcount", {"text": "hello world"}),
        nodes[4].tasks.submit("wordcount", {"text": "a b c d e f g"}),
    ]
    await asyncio.sleep(3)
    for tid in ids:
        task = nodes[0].tasks.get_task(tid)
        print(f"  task {tid[:8]} -> result={task.result} run_by={task.owner}")

    # Simulate leader failure and watch failover.
    print(f"\nStopping current leader ({leader})...")
    for n in nodes:
        if n.id == leader:
            await n.stop()
    survivors = [n for n in nodes if n.id != leader]
    await asyncio.sleep(4)
    print(f"New leader after failover: {survivors[0].leader}")

    for n in survivors:
        await n.stop()


if __name__ == "__main__":
    asyncio.run(main())
