"""Tests for SWIM-style failure detection (direct + indirect probing)."""

from __future__ import annotations

import random

import pytest

from conftest import flush
from suprime.node import SwarmNode
from suprime.peers import PeerState
from suprime.transport import InMemoryTransport


def _mk(reg, clk, nid, *, swim=True, dead_after=6.0, **kw):
    return SwarmNode(
        transport=InMemoryTransport(nid, registry=reg),
        node_id=nid,
        rng=random.Random(0),
        monotonic=clk,
        suspect_after=3.0,
        dead_after=dead_after,
        swim=swim,
        **kw,
    )


async def _connect(a, b):
    b._seeds = [a.address]
    await b._bootstrap()
    await flush()
    # Clear seeds so the bootstrap-retry doesn't re-add an intentionally-evicted
    # peer mid-test — we want to observe the failure detector in isolation.
    b._seeds = []


@pytest.mark.asyncio
async def test_swim_keeps_silent_but_alive_peer_alive():
    reg = {}
    t = {"v": 0.0}
    clk = lambda: t["v"]
    a = _mk(reg, clk, "a", swim=True)
    b = _mk(reg, clk, "b", swim=True)
    await a.start(auto=False)
    await b.start(auto=False)
    await _connect(a, b)
    for _ in range(3):
        t["v"] += 1
        await a.tick(); await b.tick(); await flush()
    assert "b" in a.peers

    # B goes silent — it stops gossiping entirely, so A never hears its heartbeat
    # again. It still answers pings (that's reactive), so SWIM should keep it alive.
    async def _silent():
        return
    b.gossip_once = _silent

    for _ in range(15):  # 15 > dead_after: pure staleness would evict B
        t["v"] += 1
        await a.tick(); await b.tick(); await flush()

    assert "b" in a.peers  # SWIM ping/ack kept it from being falsely evicted
    assert a.peers.get("b").state is PeerState.ALIVE
    assert a.metrics.counter("swim_acks") > 0
    await a.stop(); await b.stop()


@pytest.mark.asyncio
async def test_without_swim_silent_peer_is_evicted():
    reg = {}
    t = {"v": 0.0}
    clk = lambda: t["v"]
    a = _mk(reg, clk, "a", swim=False)
    b = _mk(reg, clk, "b", swim=False)
    await a.start(auto=False)
    await b.start(auto=False)
    await _connect(a, b)
    for _ in range(3):
        t["v"] += 1
        await a.tick(); await b.tick(); await flush()
    assert "b" in a.peers

    async def _silent():
        return
    b.gossip_once = _silent

    for _ in range(15):  # no probing → staleness evicts the silent peer
        t["v"] += 1
        await a.tick(); await b.tick(); await flush()

    assert "b" not in a.peers  # baseline: heartbeat-only FD gives a false positive
    await a.stop(); await b.stop()


@pytest.mark.asyncio
async def test_swim_still_evicts_a_truly_dead_node():
    reg = {}
    t = {"v": 0.0}
    clk = lambda: t["v"]
    a = _mk(reg, clk, "a", swim=True)
    b = _mk(reg, clk, "b", swim=True)
    await a.start(auto=False)
    await b.start(auto=False)
    await _connect(a, b)
    for _ in range(3):
        t["v"] += 1
        await a.tick(); await b.tick(); await flush()
    assert "b" in a.peers

    await b.stop()  # B is genuinely down — it will never ACK
    for _ in range(15):
        t["v"] += 1
        await a.tick(); await flush()

    assert "b" not in a.peers  # probes go unanswered → correctly evicted
    await a.stop()


@pytest.mark.asyncio
async def test_swim_escalates_to_indirect_probe():
    reg = {}
    t = {"v": 0.0}
    clk = lambda: t["v"]
    # dead_after huge so the stopped peer stays in the table and a keeps probing
    # (and escalating) it — we're testing escalation, not eviction, here.
    a = _mk(reg, clk, "a", swim=True, probe_timeout=1.0, dead_after=1e9)
    b = _mk(reg, clk, "b", swim=True)
    c = _mk(reg, clk, "c", swim=True)
    await a.start(auto=False)
    await b.start(auto=False)
    await c.start(auto=False)
    await _connect(a, b)
    await _connect(a, c)
    for _ in range(6):
        t["v"] += 1
        await a.tick(); await b.tick(); await c.tick(); await flush()
    assert "b" in a.peers and "c" in a.peers

    await b.stop()  # b stops answering; a's direct probe of b must escalate
    for _ in range(15):
        t["v"] += 1
        await a.tick(); await c.tick(); await flush()

    # a sent at least one indirect PING-REQ (through c) trying to reach b
    assert a.metrics.counter("swim_ping_reqs") > 0
    await a.stop(); await c.stop()
