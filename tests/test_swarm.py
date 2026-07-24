"""Integration tests: real ``SwarmNode`` instances converging over gossip."""

from __future__ import annotations

import pytest

from conftest import Cluster, flush


@pytest.mark.asyncio
async def test_two_nodes_discover_each_other(cluster: Cluster):
    a = cluster.node("a")
    b = cluster.node("b")
    await cluster.start_chain([a, b])

    await cluster.settle([a, b])
    assert "b" in a.peers
    assert "a" in b.peers


@pytest.mark.asyncio
async def test_state_replicates_across_swarm(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(5)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)

    # everyone knows everyone
    for node in nodes:
        assert len(node.peers.alive()) == len(nodes) - 1

    # a write on one node reaches all nodes
    nodes[2].store.set("motd", "hello swarm")
    await cluster.settle(nodes, rounds=20)
    for node in nodes:
        assert node.store.get("motd") == "hello swarm"


@pytest.mark.asyncio
async def test_deletion_tombstone_replicates(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    nodes[0].store.set("k", "v")
    await cluster.settle(nodes, rounds=15)
    assert all(n.store.get("k") == "v" for n in nodes)

    nodes[0].store.delete("k")
    await cluster.settle(nodes, rounds=15)
    assert all("k" not in n.store for n in nodes)


@pytest.mark.asyncio
async def test_leader_election_and_failover(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(4)]
    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)

    # smallest id ("n0") is leader everywhere
    for node in nodes:
        assert node.leader == "n0"
    assert nodes[0].is_leader()

    # kill the leader; the rest must fail it over to "n1"
    await nodes[0].stop()
    survivors = nodes[1:]
    await cluster.settle(survivors, rounds=30)

    for node in survivors:
        assert "n0" not in node.peers  # detected as dead and evicted
        assert node.leader == "n1"
    assert survivors[0].is_leader()


@pytest.mark.asyncio
async def test_distributed_task_executed_exactly_by_winner(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    executions = {n.id: [] for n in nodes}

    def make_handler(node_id):
        def handler(task):
            executions[node_id].append(task.id)
            return sum(task.args["values"])
        return handler

    for node in nodes:
        node.tasks.register_handler("sum", make_handler(node.id))

    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)

    tid = nodes[0].tasks.submit("sum", {"values": [1, 2, 3, 4]})
    await cluster.settle(nodes, rounds=30)

    # the task completed and the result replicated to every node
    for node in nodes:
        task = node.tasks.get_task(tid)
        assert task is not None
        assert task.state.value == "done"
        assert task.result == 10

    # exactly one node executed it (the deterministic winner)
    runners = [nid for nid, runs in executions.items() if tid in runs]
    assert len(runners) == 1
    assert runners[0] == nodes[0].tasks.get_task(tid).owner


@pytest.mark.asyncio
async def test_many_tasks_run_exactly_once_each(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    executions = {n.id: 0 for n in nodes}

    def make_handler(node_id):
        def handler(task):
            executions[node_id] += 1
            return task.args["x"] * 2
        return handler

    for node in nodes:
        node.tasks.register_handler("double", make_handler(node.id))

    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=20)

    ids = [nodes[i % 3].tasks.submit("double", {"x": i}) for i in range(12)]
    await cluster.settle(nodes, rounds=40)

    for i, tid in enumerate(ids):
        task = nodes[0].tasks.get_task(tid)
        assert task.state.value == "done"
        assert task.result == i * 2

    # every task ran exactly once across the whole swarm
    assert sum(executions.values()) == len(ids)


@pytest.mark.asyncio
async def test_failing_task_is_marked_failed(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(2)]

    def boom(task):
        raise ValueError("boom")

    for node in nodes:
        node.tasks.register_handler("explode", boom)

    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    tid = nodes[0].tasks.submit("explode", {})
    await cluster.settle(nodes, rounds=20)

    task = nodes[1].tasks.get_task(tid)
    assert task.state.value == "failed"
    assert "boom" in task.error


@pytest.mark.asyncio
async def test_async_handler_supported(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(2)]

    async def slow_double(task):
        return task.args["x"] * 2

    for node in nodes:
        node.tasks.register_handler("adouble", slow_double)

    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    tid = nodes[0].tasks.submit("adouble", {"x": 21})
    await cluster.settle(nodes, rounds=20)

    assert nodes[1].tasks.get_task(tid).result == 42


@pytest.mark.asyncio
async def test_direct_and_broadcast_messaging(cluster: Cluster):
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(3)]
    inbox = {n.id: [] for n in nodes}

    for node in nodes:
        async def handler(msg, nid=node.id):
            inbox[nid].append((msg.src, msg.payload["text"]))
        node.on("chat", handler)

    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=15)

    # direct message
    ok = await nodes[0].send("n1", "chat", {"text": "hi n1"})
    assert ok is True
    await flush()
    assert ("n0", "hi n1") in inbox["n1"]

    # broadcast reaches all known peers
    count = await nodes[0].broadcast("chat", {"text": "hello all"})
    assert count == 2
    await flush()
    assert ("n0", "hello all") in inbox["n1"]
    assert ("n0", "hello all") in inbox["n2"]


@pytest.mark.asyncio
async def test_send_to_unknown_node_returns_false(cluster: Cluster):
    a = cluster.node("a")
    await a.start(auto=False)
    assert await a.send("ghost", "chat", {"text": "hi"}) is False
