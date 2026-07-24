"""A decentralised collaborative text editor over the swarm.

Several nodes edit one shared document concurrently. Each holds an RGA (a
sequence CRDT) replicated across the swarm; concurrent inserts and deletes
converge to the same text on every node, with no server. This is the
"Google-Docs-without-a-server" demo.

Run with::

    python examples/collab_editor.py
"""

from __future__ import annotations

import asyncio
import random

from suprime import RGA, CRDTReplicator, SwarmNode
from suprime.transport import InMemoryTransport


async def build(n):
    registry: dict = {}
    nodes = []
    for i in range(n):
        seeds = [nodes[0].address] if nodes else None
        node = SwarmNode(
            transport=InMemoryTransport(f"editor-{i}", registry=registry),
            node_id=f"editor-{i}",
            seeds=seeds,
            gossip_interval=0.1,
            rng=random.Random(i),
        )
        await node.start()
        nodes.append(node)
    await asyncio.sleep(1.5)
    return nodes


async def main() -> None:
    nodes = await build(3)
    reps = [CRDTReplicator(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    docs = [r.register("document", RGA(n.id)) for r, n in zip(reps, nodes)]

    # Editor 0 types a sentence.
    for ch in "the swarm ":
        docs[0].append(ch)
    await asyncio.sleep(1.5)
    print("after editor-0 types:")
    for n, d in zip(nodes, docs):
        print(f"  {n.id}: {d.to_string()!r}")

    # Now all three edit concurrently before syncing.
    for ch in "converges":
        docs[1].append(ch)
    docs[2].append("!")
    # editor 0 fixes the start
    docs[0].insert(0, ">")
    await asyncio.sleep(3)

    print("\nafter concurrent edits from all three, converged document:")
    strings = {d.to_string() for d in docs}
    for n, d in zip(nodes, docs):
        print(f"  {n.id}: {d.to_string()!r}")
    print(f"\nall replicas identical: {len(strings) == 1}")

    for n in nodes:
        await n.stop()


if __name__ == "__main__":
    asyncio.run(main())
