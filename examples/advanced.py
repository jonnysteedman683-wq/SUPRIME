"""A tour of SUPRIME's advanced toolkit, all in one in-process run.

Covers CRDT replication, collaborative-text (RGA), authenticated transport,
Byzantine-tolerant agreement, decentralised federated learning and pub/sub.

Run with::

    python examples/advanced.py
"""

from __future__ import annotations

import asyncio
import random

from suprime import (
    ByzantineConsensus,
    CRDTReplicator,
    GossipLearner,
    LinearModel,
    PNCounter,
    PubSub,
    RGA,
    SwarmNode,
    mint_identity,
    verify_identity,
)
from suprime.transport import InMemoryTransport


async def build(n):
    registry: dict = {}
    nodes = []
    for i in range(n):
        seeds = [nodes[0].address] if nodes else None
        node = SwarmNode(
            transport=InMemoryTransport(f"node-{i}", registry=registry),
            node_id=f"node-{i}",
            seeds=seeds,
            gossip_interval=0.1,
            rng=random.Random(i),
        )
        await node.start()
        nodes.append(node)
    await asyncio.sleep(1.5)
    return nodes


async def main() -> None:
    nodes = await build(5)

    # --- replicated CRDT counter -------------------------------------------
    reps = [CRDTReplicator(n, rng=random.Random(i)) for i, n in enumerate(nodes)]
    counters = [r.register("likes", PNCounter(n.id)) for r, n in zip(reps, nodes)]
    counters[0].increment(3)
    counters[1].increment(4)
    counters[2].decrement(1)
    await asyncio.sleep(2)
    print("replicated PN-counter 'likes' (want 6):", [c.value for c in counters])

    # --- collaborative text (RGA) ------------------------------------------
    docs = [r.register("doc", RGA(n.id)) for r, n in zip(reps, nodes)]
    docs[0].append("H"); docs[0].append("i")
    docs[1].append("!")
    await asyncio.sleep(2)
    print("collaborative RGA doc converged to:", {d.to_string() for d in docs})

    # --- proof-of-work identity --------------------------------------------
    ident = mint_identity("newcomer", difficulty=12)
    print(f"minted PoW identity nonce={ident.nonce}, verifies={verify_identity(ident, 12)}")

    # --- Byzantine-tolerant agreement --------------------------------------
    byz = [ByzantineConsensus(n, quorum=0.5) for n in nodes]
    for i in range(4):
        byz[i].vote("leader_color", "blue")   # honest majority
    byz[4].vote("leader_color", "red")        # one liar
    await asyncio.sleep(2)
    print("Byzantine agreement (want 'blue'):", byz[0].accepted("leader_color"))
    print("  trust in liar vs honest:", round(byz[0].trust("node-4"), 2), "vs", round(byz[0].trust("node-1"), 2))

    # --- pub/sub -----------------------------------------------------------
    buses = [PubSub(n, rng=random.Random(10 + i)) for i, n in enumerate(nodes)]
    received = []
    for i in (1, 2, 3):
        buses[i].subscribe("news", lambda t, d: received.append(d))
    await asyncio.sleep(0.5)
    await buses[0].publish("news", "swarm launched")
    await asyncio.sleep(1.5)
    print(f"pub/sub: {len(received)} subscribers received the 'news' message")

    # --- federated learning ------------------------------------------------
    true_w, true_b = [1.5, -0.5], 0.25
    learners = []
    for i, node in enumerate(nodes):
        r = random.Random(200 + i)
        data = []
        for _ in range(30):
            x = [r.uniform(0, 1), r.uniform(0, 1)]
            y = true_w[0] * x[0] + true_w[1] * x[1] + true_b
            data.append((x, y))
        learners.append(GossipLearner(node, LinearModel(2), data, lr=0.3, steps_per_round=3, rng=random.Random(i)))
    await asyncio.sleep(6)
    m = learners[0].model
    print(f"federated model learned w={[round(x,2) for x in m.w]} b={round(m.b,2)} (true w={true_w} b={true_b})")

    for n in nodes:
        await n.stop()


if __name__ == "__main__":
    asyncio.run(main())
