"""A collective AI on SUPRIME: a swarm of agents that answer together.

Each node runs a pluggable "brain" (here a tiny rule-based classifier, but the
same interface accepts a local model or an LLM call). Agents share a blackboard,
divide work, and vote — so the collective's answer is an ensemble that outvotes
wrong or adversarial members. Swap `classify` for `lambda q: call_llm(q)` and
each node becomes an LLM agent with no other changes.

Run with::

    python examples/collective_ai.py
"""

from __future__ import annotations

import asyncio

from suprime import Agent, SwarmNode
from suprime.transport import InMemoryTransport


def classify(question: str) -> str:
    """A trivial 'brain' — swap for a model or LLM call."""
    q = question.lower()
    if any(w in q for w in ("buy now", "free", "winner", "cheap")):
        return "spam"
    return "ham"


async def build(n):
    registry: dict = {}
    nodes = []
    for i in range(n):
        seeds = [nodes[0].address] if nodes else None
        node = SwarmNode(
            transport=InMemoryTransport(f"agent-{i}", registry=registry),
            node_id=f"agent-{i}",
            seeds=seeds,
            gossip_interval=0.1,
        )
        await node.start()
        nodes.append(node)
    await asyncio.sleep(1.5)
    return nodes


async def main() -> None:
    nodes = await build(5)
    agents = [Agent(n) for n in nodes]

    # 4 agents classify well; 1 is faulty and always says "ham".
    for a in agents[:4]:
        a.answer_with(classify)
    agents[4].answer_with(lambda q: "ham")

    # --- collective classification (ensemble outvotes the faulty agent) ----
    print("Collective answers (5 agents, 1 faulty):")
    for text in ["buy now cheap pills", "meeting at noon tomorrow", "you are a winner"]:
        qid = await agents[0].ask(text)
        await asyncio.sleep(1.5)
        print(f"  {text!r:38} -> {agents[0].consensus(qid)}")

    # --- shared blackboard memory -----------------------------------------
    agents[2].remember("mission", "triage the inbox")
    await asyncio.sleep(1.5)
    print(f"\nShared memory as seen by agent-0: mission = {agents[0].recall('mission')!r}")

    # --- distributed work (a brain that summarises) ------------------------
    for a in agents:
        a.brain = lambda task, agent: len(str(task.args["text"]).split())
        a.node.tasks.register_handler("wordcount", a._invoke_brain)
    tid = agents[1].assign("wordcount", text="the swarm thinks together")
    await asyncio.sleep(2)
    t = agents[0].task(tid)
    print(f"\nDistributed task result: {t.result} (run by {t.owner})")

    for n in nodes:
        await n.stop()


if __name__ == "__main__":
    asyncio.run(main())
