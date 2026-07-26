"""Tests for the Agent layer — the collective-AI facade over the swarm."""

from __future__ import annotations

import pytest

from conftest import Cluster
from suprime.agent import build_agents


# -- pluggable brain + distributed work ------------------------------------

@pytest.mark.asyncio
async def test_agents_execute_work_with_their_brain(cluster: Cluster):
    def brain(task, agent):
        return task.args["x"] * 2

    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    agents = build_agents(nodes, brain=brain, kinds=("double",))
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    tid = agents[0].assign("double", x=21)
    await cluster.settle(nodes, rounds=25)
    # exactly one agent ran it; the result is visible to every agent
    assert agents[1].result(tid) == 42
    assert agents[2].result(tid) == 42


@pytest.mark.asyncio
async def test_async_brain_supported(cluster: Cluster):
    async def brain(task, agent):
        return sum(task.args["xs"])

    nodes = [cluster.node(f"n{i}", seed=i) for i in range(2)]
    agents = build_agents(nodes, brain=brain, kinds=("sum",))
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    tid = agents[0].assign("sum", xs=[1, 2, 3, 4])
    await cluster.settle(nodes, rounds=25)
    assert agents[0].result(tid) == 10


# -- shared blackboard memory ----------------------------------------------

@pytest.mark.asyncio
async def test_shared_blackboard_memory(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    agents = build_agents(nodes)
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    agents[0].remember("goal", "find the exit")
    await cluster.settle(nodes, rounds=20)
    assert agents[2].recall("goal") == "find the exit"


# -- brain uses shared memory ----------------------------------------------

@pytest.mark.asyncio
async def test_brain_can_use_agent_context(cluster: Cluster):
    # brain reads shared memory (a "bias") and writes a result note
    def brain(task, agent):
        bias = agent.recall("bias", 0)
        out = task.args["x"] + bias
        agent.remember(f"seen:{task.args['x']}", out)
        return out

    nodes = [cluster.node(f"n{i}", seed=i) for i in range(2)]
    agents = build_agents(nodes, brain=brain, kinds=("add",))
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)
    agents[0].remember("bias", 100)
    await cluster.settle(nodes, rounds=20)

    tid = agents[0].assign("add", x=5)
    await cluster.settle(nodes, rounds=25)
    assert agents[1].result(tid) == 105


# -- collective decision (ensemble Q&A) ------------------------------------

@pytest.mark.asyncio
async def test_collective_answer_outvotes_dissenter(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(5)]
    agents = build_agents(nodes)

    # four honest agents classify correctly; one is wrong
    def honest(question):
        return "spam" if "buy now" in question else "ham"

    def liar(_question):
        return "ham"

    for a in agents[:4]:
        a.answer_with(honest)
    agents[4].answer_with(liar)

    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    qid = await agents[0].ask("buy now cheap pills")
    await cluster.settle(nodes, rounds=30)

    # the collective converges on the correct answer despite the dissenter
    assert agents[0].consensus(qid) == "spam"
    assert agents[3].consensus(qid) == "spam"


@pytest.mark.asyncio
async def test_direct_vote_and_consensus(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(4)]
    agents = build_agents(nodes)
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    for a in agents[:3]:
        a.vote("leader_color", "blue")
    agents[3].vote("leader_color", "red")
    await cluster.settle(nodes, rounds=30)
    assert agents[0].consensus("leader_color") == "blue"
