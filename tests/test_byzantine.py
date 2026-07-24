"""Tests for Byzantine-tolerant, trust-weighted value agreement."""

from __future__ import annotations

import pytest

from conftest import Cluster
from suprime.byzantine import ByzantineConsensus, QuorumRegister, TrustLedger


def test_trust_ledger_reward_and_penalize():
    led = TrustLedger()
    assert led.score("n") == 1.0
    led.reward("n")
    assert led.score("n") > 1.0
    led.penalize("n", amount=10.0)
    assert led.score("n") == 0.0  # clamped at zero


def test_quorum_accepts_honest_majority():
    led = TrustLedger()
    reg = QuorumRegister(led, quorum=0.5)
    for n in ("a", "b", "c"):
        reg.record_vote("temp", n, 20)
    reg.record_vote("temp", "liar", 999)  # single Byzantine vote
    assert reg.tally("temp") == 20


def test_quorum_penalizes_liars_over_rounds():
    led = TrustLedger()
    reg = QuorumRegister(led, quorum=0.5)
    honest = ("a", "b", "c")
    for _ in range(5):
        for n in honest:
            reg.record_vote("k", n, "true")
        reg.record_vote("k", "liar", "false")
        assert reg.resolve("k") == "true"
    # the liar's reputation is driven down; honest nodes rewarded
    assert led.score("liar") < 1.0
    assert led.score("a") > 1.0


def test_quorum_no_decision_without_quorum():
    led = TrustLedger()
    reg = QuorumRegister(led, quorum=0.8)
    reg.record_vote("k", "a", 1)
    reg.record_vote("k", "b", 2)  # split, neither reaches 80%
    assert reg.tally("k") is None


@pytest.mark.asyncio
async def test_byzantine_consensus_over_swarm(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(5)]
    byz = [ByzantineConsensus(n, quorum=0.5) for n in nodes]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    # four honest nodes agree on 42; one liar reports 7
    for i in range(4):
        byz[i].vote("answer", 42)
    byz[4].vote("answer", 7)

    await cluster.settle(nodes, rounds=30)

    # every honest node accepts the true value
    for i in range(4):
        assert byz[i].accepted("answer") == 42
    # and the liar has lost trust in honest nodes' ledgers
    assert byz[0].trust("n4") < byz[0].trust("n1")
