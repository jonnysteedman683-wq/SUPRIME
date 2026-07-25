"""A live terminal dashboard for watching a SUPRIME swarm self-organise.

Runs an entire swarm in one process over the in-memory transport (wrapped in a
:class:`~suprime.chaos.ChaosTransport`) and renders, in real time, membership,
the elected leader, replicated state, a push-sum aggregate and the chaos
controller's counters. It scripts a small chaos scenario — steady state, a
network partition, then healing — so you can watch the swarm split and
reconverge.

Run it with::

    python -m suprime dashboard              # default 6-node scenario
    python -m suprime dashboard --nodes 10   # bigger swarm

Pure standard library: rendering uses ANSI escape codes, no curses or
third-party TUI dependency. Press Ctrl-C to stop.
"""

from __future__ import annotations

import asyncio
import random
import shutil
from typing import Dict, List

from .aggregate import PushSumAggregator
from .chaos import ChaosController, ChaosTransport
from .node import SwarmNode
from .peers import PeerState
from .transport import InMemoryTransport

CLEAR = "\033[2J\033[H"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"


def _color_state(state: PeerState) -> str:
    return {
        PeerState.ALIVE: GREEN + "●" + RESET,
        PeerState.SUSPECT: YELLOW + "◐" + RESET,
        PeerState.DEAD: RED + "○" + RESET,
    }.get(state, "?")


class SwarmDashboard:
    """Builds, runs and renders an in-process chaos swarm."""

    def __init__(self, n_nodes: int = 6, seed: int = 7) -> None:
        self._n = n_nodes
        self._rng = random.Random(seed)
        self._registry: Dict[str, InMemoryTransport] = {}
        self._chaos = ChaosController(latency=0.005, rng=random.Random(seed))
        self._nodes: List[SwarmNode] = []
        self._aggs: List[PushSumAggregator] = []
        self._phase = "forming"
        self._tick = 0

    async def build(self) -> None:
        for i in range(self._n):
            inner = InMemoryTransport(f"node-{i}", registry=self._registry)
            transport = ChaosTransport(inner, self._chaos)
            seeds = [self._nodes[0].address] if self._nodes else None
            node = SwarmNode(
                transport=transport,
                node_id=f"node-{i}",
                seeds=seeds,
                gossip_interval=0.25,
                suspect_after=1.5,
                dead_after=3.0,
                rng=random.Random(i),
            )
            node.tasks.register_handler("work", lambda t: t.args.get("x", 0) ** 2)
            await node.start()
            agg = PushSumAggregator(node, rng=random.Random(100 + i))
            self._nodes.append(node)
            self._aggs.append(agg)

    def render(self) -> str:
        width = shutil.get_terminal_size((80, 24)).columns
        line = "─" * min(width, 78)
        out = [CLEAR]
        out.append(f"{BOLD}{CYAN}  SUPRIME swarm — live dashboard{RESET}")
        out.append(f"{DIM}  tick {self._tick}   phase: {BOLD}{self._phase}{RESET}")
        out.append(line)

        # Membership matrix: each node's view of every other node.
        header = "  node        leader   view of peers"
        out.append(f"{BOLD}{header}{RESET}")
        for node in self._nodes:
            leader = node.leader or "?"
            leader_mark = (MAGENTA + "★" + RESET) if node.is_leader() else " "
            cells = []
            for other in self._nodes:
                if other.id == node.id:
                    cells.append(CYAN + "◆" + RESET)  # self
                    continue
                peer = node.peers.get(other.id)
                cells.append(_color_state(peer.state) if peer else RED + "·" + RESET)
            row = " ".join(cells)
            out.append(f"  {node.id:<10} {leader_mark}{leader:<7} {row}")

        out.append(line)

        # Replicated state (show a shared key) and push-sum estimate.
        store_vals = {n.id: n.store.get("beacon", "—") for n in self._nodes}
        converged = len(set(store_vals.values())) == 1
        conv_mark = (GREEN + "converged" + RESET) if converged else (YELLOW + "diverged" + RESET)
        out.append(f"  {BOLD}replicated 'beacon':{RESET} {conv_mark}")
        out.append("    " + "  ".join(f"{k}={v}" for k, v in store_vals.items()))

        ests = [a.estimate("load") for a in self._aggs]
        shown = [f"{e:.1f}" if e is not None else "—" for e in ests]
        out.append(f"  {BOLD}push-sum avg('load'):{RESET} " + "  ".join(shown))

        out.append(line)

        # Chaos stats.
        s = self._chaos.stats()
        out.append(
            f"  {BOLD}chaos:{RESET} delivered={s['delivered']} "
            f"dropped={RED}{s['dropped']}{RESET} "
            f"duplicated={s['duplicated']} "
            f"partitions={YELLOW if s['partitions'] else ''}{s['partitions']}{RESET}"
        )
        out.append(f"{DIM}  legend: ◆ self  {GREEN}●{RESET}{DIM} alive  "
                   f"{YELLOW}◐{RESET}{DIM} suspect  {RED}·{RESET}{DIM} unknown   "
                   f"{MAGENTA}★{RESET}{DIM} leader{RESET}")
        out.append(f"{DIM}  Ctrl-C to quit{RESET}")
        return "\n".join(out)

    async def run(self, duration: float = 30.0) -> None:
        await self.build()
        # Seed some aggregation input and a beacon value.
        for i, (node, agg) in enumerate(zip(self._nodes, self._aggs)):
            agg.average("load", float(10 * (i + 1)))
        self._nodes[0].store.set("beacon", "A")

        import sys

        sys.stdout.write(HIDE_CURSOR)
        loop = asyncio.get_event_loop()
        start = loop.time()
        try:
            while loop.time() - start < duration:
                self._tick += 1
                elapsed = loop.time() - start

                # Scripted chaos scenario.
                if elapsed < duration * 0.35:
                    self._phase = "steady state"
                elif elapsed < duration * 0.65:
                    if self._phase != "PARTITIONED":
                        half = len(self._nodes) // 2
                        left = [n.address for n in self._nodes[:half]]
                        right = [n.address for n in self._nodes[half:]]
                        self._chaos.partition(left, right)
                        # write divergent beacons on each side
                        self._nodes[0].store.set("beacon", "LEFT")
                        self._nodes[-1].store.set("beacon", "RIGHT")
                    self._phase = "PARTITIONED"
                else:
                    if self._phase != "healed":
                        self._chaos.heal()
                    self._phase = "healed"

                sys.stdout.write(self.render())
                sys.stdout.flush()
                await asyncio.sleep(0.4)
        except asyncio.CancelledError:  # pragma: no cover
            pass
        finally:
            sys.stdout.write(SHOW_CURSOR + "\n")
            sys.stdout.flush()
            for node in self._nodes:
                await node.stop()


async def run_dashboard(n_nodes: int = 6, duration: float = 30.0) -> None:
    await SwarmDashboard(n_nodes=n_nodes).run(duration=duration)
