"""End-to-end tests over the real asyncio TCP transport.

These use actual sockets and the automatic gossip loop, so they exercise the
wire protocol, lazy port resolution and bidirectional connectivity that the
in-memory tests cannot. They poll for convergence with a timeout instead of
driving ticks manually.
"""

from __future__ import annotations

import asyncio

import pytest

from suprime import SwarmNode
from suprime.transport import TcpTransport


async def _wait_for(predicate, timeout: float = 5.0, interval: float = 0.05):
    """Poll ``predicate`` until it is truthy or ``timeout`` elapses."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def _tcp_node(node_id: str, seeds=None) -> SwarmNode:
    return SwarmNode(
        transport=TcpTransport("127.0.0.1", 0),
        node_id=node_id,
        seeds=seeds,
        gossip_interval=0.1,
        suspect_after=0.4,
        dead_after=0.9,
        claim_grace_rounds=3,
    )


@pytest.mark.asyncio
async def test_tcp_discovery_and_bidirectional_replication():
    a = _tcp_node("alpha")
    await a.start()
    c = _tcp_node("gamma", seeds=[a.address])
    await c.start()
    try:
        assert await _wait_for(lambda: "gamma" in a.peers and "alpha" in c.peers)

        # Regression guard: the seed must advertise its real (resolved) port so
        # peers can gossip *back* to it, not the :0 bind placeholder.
        a.store.set("from_a", "A")
        c.store.set("from_c", "C")
        assert await _wait_for(
            lambda: a.store.get("from_c") == "C" and c.store.get("from_a") == "A"
        )
    finally:
        await a.stop()
        await c.stop()


@pytest.mark.asyncio
async def test_tcp_distributed_task_execution():
    a = _tcp_node("alpha")
    await a.start()
    b = _tcp_node("beta", seeds=[a.address])
    c = _tcp_node("gamma", seeds=[a.address])
    await b.start()
    await c.start()

    for node in (a, b, c):
        node.tasks.register_handler("sum", lambda t: sum(t.args["values"]))

    try:
        assert await _wait_for(lambda: len(a.peers.alive()) == 2)
        tid = c.tasks.submit("sum", {"values": [10, 20, 30]})
        got = await _wait_for(
            lambda: (a.tasks.get_task(tid) or _pending()).state.value == "done",
            timeout=5.0,
        )
        assert got
        task = a.tasks.get_task(tid)
        assert task.result == 60
        assert task.owner in {"alpha", "beta", "gamma"}
    finally:
        await a.stop()
        await b.stop()
        await c.stop()


class _pending:
    """Sentinel with a pending state, so polling before propagation is safe."""

    class _S:
        value = "pending"

    state = _S()


@pytest.mark.asyncio
async def test_tcp_leader_failover():
    a = _tcp_node("n0")
    await a.start()
    b = _tcp_node("n1", seeds=[a.address])
    c = _tcp_node("n2", seeds=[a.address])
    await b.start()
    await c.start()
    try:
        assert await _wait_for(lambda: b.leader == "n0" and c.leader == "n0")
        await a.stop()
        # survivors must detect the dead leader and elect n1. Generous timeout:
        # this is a real-clock test and CI/full-suite CPU load can add jitter.
        assert await _wait_for(
            lambda: b.leader == "n1" and c.leader == "n1" and "n0" not in b.peers,
            timeout=15.0,
        )
    finally:
        await b.stop()
        await c.stop()
