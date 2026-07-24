"""Tests for gossip-based (federated) learning."""

from __future__ import annotations

import random

import pytest

from conftest import Cluster
from suprime.learning import GossipLearner, LinearModel


def _make_data(rng, n, true_w, true_b):
    data = []
    for _ in range(n):
        x = [rng.uniform(0, 1) for _ in true_w]
        y = sum(wi * xi for wi, xi in zip(true_w, x)) + true_b
        data.append((x, y))
    return data


def test_linear_model_learns_locally():
    rng = random.Random(0)
    true_w, true_b = [2.0, -1.0], 0.5
    data = _make_data(rng, 200, true_w, true_b)
    model = LinearModel(2)
    for _ in range(2000):
        model.sgd_step(data, lr=0.2)
    assert abs(model.w[0] - 2.0) < 0.05
    assert abs(model.w[1] - (-1.0)) < 0.05
    assert abs(model.b - 0.5) < 0.05


@pytest.mark.asyncio
async def test_federated_learning_converges_across_swarm(cluster: Cluster):
    true_w, true_b = [1.5, -0.5], 0.25
    n_nodes = 4
    nodes = [cluster.node(f"n{i}", seed=i) for i in range(n_nodes)]

    learners = []
    for i, node in enumerate(nodes):
        # each node holds a private, disjoint slice of the data
        data = _make_data(random.Random(100 + i), 40, true_w, true_b)
        learner = GossipLearner(
            node,
            LinearModel(2),
            data,
            lr=0.3,
            steps_per_round=3,
            rng=random.Random(i),
        )
        learners.append(learner)

    await cluster.start_chain(nodes)
    await cluster.settle(nodes, rounds=400)

    # all nodes converged to essentially the same model (consensus)
    w0 = learners[0].model.w
    for learner in learners[1:]:
        assert abs(learner.model.w[0] - w0[0]) < 0.05
        assert abs(learner.model.w[1] - w0[1]) < 0.05

    # and that shared model fits the true global relationship
    test = _make_data(random.Random(999), 50, true_w, true_b)
    model = learners[0].model
    mse = sum((model.predict(x) - y) ** 2 for x, y in test) / len(test)
    assert mse < 0.05
