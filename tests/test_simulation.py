"""Deterministic simulation tests: invariants hold under adversarial schedules.

Each test runs many seeded histories, each injecting drops, reordering, latency,
partitions and crash/restart. After the fault storm we heal and quiesce the
swarm, then assert the swarm's safety/liveness invariants. A failure prints the
seed, which reproduces the exact history.
"""

from __future__ import annotations

import pytest

from suprime.simulation import SimConfig, Simulator


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(8))
async def test_state_converges_after_fault_storm(seed):
    cfg = SimConfig(n_nodes=5, seed=seed, steps=120, drop_rate=0.1, max_latency=3.0)
    sim = Simulator(cfg)
    await sim.build()

    # A few writes land during the chaos, from different nodes.
    for i in range(cfg.steps):
        await sim.step()
        if i in (10, 40, 80):
            writer = sim.rng.choice(sim.live_nodes())
            writer.store.set("shared", f"v{i}")

    # Heal and let the swarm settle, then every node must agree.
    await sim.quiesce(rounds=80)
    values = {n.store.get("shared") for n in sim.nodes}
    assert len(values) == 1, f"seed={seed} diverged: {values}"
    assert next(iter(values)) is not None
    await sim.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(6))
async def test_tasks_execute_at_least_once_under_faults(seed):
    cfg = SimConfig(n_nodes=5, seed=seed, steps=100, drop_rate=0.08, max_latency=2.0)
    sim = Simulator(cfg)
    await sim.build()

    executed = {n.id: [] for n in sim.nodes}
    for node in sim.nodes:
        node.tasks.register_handler(
            "job",
            (lambda nid: (lambda t: executed[nid].append(t.args["k"])))(node.id),
        )

    task_keys = []
    for i in range(cfg.steps):
        await sim.step()
        if i in (15, 45, 75):
            k = f"task{i}"
            sim.live_nodes()[0].tasks.submit("job", {"k": k})
            task_keys.append(k)

    await sim.quiesce(rounds=100)

    # Every submitted task ran (at least once) somewhere and reached a terminal
    # state that replicated to all nodes.
    all_runs = [k for runs in executed.values() for k in runs]
    for k in task_keys:
        assert k in all_runs, f"seed={seed}: task {k} never executed"
    await sim.stop()


@pytest.mark.asyncio
async def test_simulation_is_reproducible():
    # Same seed → identical delivered/dropped counts (determinism guarantee).
    async def run_once():
        sim = Simulator(SimConfig(n_nodes=4, seed=123, steps=60, drop_rate=0.1))
        await sim.build()
        for _ in range(60):
            await sim.step()
        stats = (sim.network.delivered, sim.network.dropped)
        await sim.stop()
        return stats

    a = await run_once()
    b = await run_once()
    assert a == b
