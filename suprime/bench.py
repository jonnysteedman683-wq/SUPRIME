"""Scale and performance benchmarks for the swarm.

Runs whole swarms in-process (in-memory transport, manual ticks against a manual
clock) and *measures* the properties the design claims:

* **convergence time** — rounds for a write to reach every node, vs swarm size;
* **push-sum accuracy** — aggregate error decaying over rounds;
* **broadcast cost** — Plumtree spanning-tree messages vs naive flooding.

Results are printed as tables and rendered into a self-contained HTML report
with inline SVG charts (no plotting dependency).

Run with::

    python -m suprime bench --out bench_report.html
"""

from __future__ import annotations

import asyncio
import random
from typing import Dict, List, Optional, Tuple

from .aggregate import PushSumAggregator
from .message import Message
from .node import SwarmNode
from .plumtree import PlumtreeBroadcast
from .svg import html_report, line_chart
from .transport import InMemoryTransport, Transport


class _CountingTransport(Transport):
    """Wraps a transport and counts messages sent, into a shared dict."""

    def __init__(self, inner: Transport, counter: Dict[str, int]) -> None:
        self._inner = inner
        self._counter = counter

    @property
    def address(self) -> str:
        return self._inner.address

    async def start(self, on_message) -> None:
        await self._inner.start(on_message)

    async def send(self, address: str, message: Message) -> None:
        self._counter["sends"] = self._counter.get("sends", 0) + 1
        self._counter[message.type] = self._counter.get(message.type, 0) + 1
        await self._inner.send(address, message)

    async def stop(self) -> None:
        await self._inner.stop()


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


async def _flush(n: int = 40) -> None:
    for _ in range(n):
        await asyncio.sleep(0)


async def _build_swarm(
    n: int,
    fanout: int = 3,
    counter: Optional[Dict[str, int]] = None,
    clock: Optional[_Clock] = None,
) -> List[SwarmNode]:
    registry: Dict[str, InMemoryTransport] = {}
    clock = clock or _Clock()
    nodes: List[SwarmNode] = []
    for i in range(n):
        inner = InMemoryTransport(f"n{i}", registry=registry)
        transport = _CountingTransport(inner, counter) if counter is not None else inner
        seeds = [nodes[0].address] if nodes else None
        node = SwarmNode(
            transport=transport,
            node_id=f"n{i}",
            seeds=seeds,
            fanout=fanout,
            rng=random.Random(i),
            monotonic=clock,
            dead_after=1e9,
        )
        await node.start(auto=False)
        nodes.append(node)
    await _flush()
    return nodes, clock


async def _settle(nodes, clock, rounds) -> None:
    for _ in range(rounds):
        clock.t += 1.0
        for node in nodes:
            await node.tick()
        await _flush()


# -- benchmark 1: convergence time vs swarm size ---------------------------

async def bench_convergence(sizes=(5, 10, 20, 40), fanout=3, max_rounds=60):
    xs, ys = [], []
    for n in sizes:
        nodes, clock = await _build_swarm(n, fanout=fanout)
        await _settle(nodes, clock, rounds=n)  # let membership converge
        nodes[0].store.set("probe", "value")
        rounds = 0
        for r in range(1, max_rounds + 1):
            clock.t += 1.0
            for node in nodes:
                await node.tick()
            await _flush()
            if all(node.store.get("probe") == "value" for node in nodes):
                rounds = r
                break
        xs.append(n)
        ys.append(rounds or max_rounds)
        for node in nodes:
            await node.stop()
    return xs, ys


# -- benchmark 2: push-sum error over rounds -------------------------------

async def bench_pushsum(n=12, rounds=40):
    nodes, clock = await _build_swarm(n)
    aggs = [PushSumAggregator(node, rng=random.Random(500 + i)) for i, node in enumerate(nodes)]
    await _settle(nodes, clock, rounds=n)
    values = [float(i + 1) for i in range(n)]
    true_mean = sum(values) / n
    for agg, v in zip(aggs, values):
        agg.average("m", v)
    xs, ys = [], []
    for r in range(1, rounds + 1):
        clock.t += 1.0
        for node in nodes:
            await node.tick()
        await _flush()
        ests = [a.estimate("m") for a in aggs if a.estimate("m") is not None]
        if ests:
            err = sum(abs(e - true_mean) for e in ests) / len(ests)
            xs.append(r)
            ys.append(err)
    for node in nodes:
        await node.stop()
    return xs, ys


# -- benchmark 3: broadcast cost, Plumtree vs flooding ---------------------

async def bench_broadcast(sizes=(5, 10, 20, 30)):
    from .plumtree import PT_GOSSIP

    plum_x, plum_y, flood_x, flood_y = [], [], [], []
    for n in sizes:
        # --- Plumtree: count payload copies (PT_GOSSIP) after the tree forms.
        counter = {}
        nodes, clock = await _build_swarm(n, counter=counter)
        await _settle(nodes, clock, rounds=n + 5)
        trees = [PlumtreeBroadcast(node, rng=random.Random(i)) for i, node in enumerate(nodes)]
        # Warm up: several broadcasts prune the mesh down to a spanning tree.
        for w in range(4):
            await trees[0].broadcast({"warm": w})
            await _flush(120)
            await _settle(nodes, clock, rounds=2)
        base = counter.get(PT_GOSSIP, 0)
        await trees[0].broadcast({"m": 1})
        await _flush(200)
        plum_x.append(n)
        plum_y.append(counter.get(PT_GOSSIP, 0) - base)
        for node in nodes:
            await node.stop()

        # --- Naive flooding: every node forwards a new message to all peers.
        counter2 = {}
        nodes2, clock2 = await _build_swarm(n, counter=counter2)
        await _settle(nodes2, clock2, rounds=n + 5)
        seen = {node.id: set() for node in nodes2}

        async def flood(node, mid, payload):
            for peer in node.peers.all():
                await node.send(peer.node_id, "__flood__", {"mid": mid, "p": payload})

        for node in nodes2:
            async def handler(msg, me=node):
                mid = msg.payload["mid"]
                if mid in seen[me.id]:
                    return
                seen[me.id].add(mid)
                await flood(me, mid, msg.payload["p"])
            node.on("__flood__", handler)
        base2 = counter2.get("__flood__", 0)
        seen[nodes2[0].id].add("m1")
        await flood(nodes2[0], "m1", 1)
        await _flush(200)
        flood_x.append(n)
        flood_y.append(counter2.get("__flood__", 0) - base2)
        for node in nodes2:
            await node.stop()
    return (plum_x, plum_y), (flood_x, flood_y)


# -- runner ----------------------------------------------------------------

async def run_all(out_path: str = "bench_report.html") -> str:
    print("Running SUPRIME benchmarks (in-process)...\n")

    conv_x, conv_y = await bench_convergence()
    print("Convergence (rounds for a write to reach all nodes):")
    for x, y in zip(conv_x, conv_y):
        print(f"  {x:>3} nodes -> {y} rounds")

    ps_x, ps_y = await bench_pushsum()
    print("\nPush-sum mean absolute error over rounds:")
    print(f"  round {ps_x[0]}: {ps_y[0]:.3f}   ...   round {ps_x[-1]}: {ps_y[-1]:.4f}")

    (px, py), (fx, fy) = await bench_broadcast()
    print("\nBroadcast messages to reach all nodes (Plumtree vs flooding):")
    for i in range(len(px)):
        print(f"  {px[i]:>3} nodes -> plumtree={py[i]:>4}   flood={fy[i]:>5}")

    charts = [
        line_chart(
            [("gossip", conv_x, conv_y)],
            title="Convergence time vs swarm size",
            x_label="nodes",
            y_label="rounds to full convergence",
        ),
        line_chart(
            [("mean abs error", ps_x, ps_y)],
            title="Push-sum aggregation error",
            x_label="gossip round",
            y_label="|estimate - true mean|",
        ),
        line_chart(
            [("Plumtree", px, py), ("naive flooding", fx, fy)],
            title="Broadcast cost: Plumtree vs flooding",
            x_label="nodes",
            y_label="messages per broadcast",
        ),
    ]
    notes = (
        "Generated by <code>python -m suprime bench</code>. All swarms run "
        "in-process over the in-memory transport, driven deterministically. "
        "Convergence is gossip-based dissemination of a single write; push-sum "
        "shows the aggregate error decaying toward zero; the broadcast chart "
        "shows Plumtree's spanning tree using far fewer messages than flooding "
        "as the swarm grows."
    )
    html = html_report("SUPRIME benchmarks", charts, notes)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"\nWrote HTML report with charts to: {out_path}")
    return out_path


def main(out_path: str = "bench_report.html") -> None:
    asyncio.run(run_all(out_path))
