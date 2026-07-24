"""Decentralised federated learning by gossip (diffusion SGD).

There is no parameter server. Every node holds its own copy of a model and its
own slice of the data. Each round a node takes a local gradient step and then
*gossip-averages* its weights with a random peer. Local training pulls each
model toward fitting local data; pairwise averaging pulls all models toward a
common consensus. Together they converge on a shared model — as if the data had
been pooled — while raw data never leaves the node that owns it.

This rides the same swarm primitives as everything else: one message type for
weight exchange and one tick hook for the train/average step. It builds directly
on the intuition behind push-sum (gossip averaging of vectors).
"""

from __future__ import annotations

import random
from typing import Callable, List, Optional, Sequence, Tuple

from .message import Message

MODEL_SYNC = "__model_sync__"

Sample = Tuple[Sequence[float], float]


class LinearModel:
    """A plain linear model ``y = w · x + b`` trained by MSE gradient descent."""

    def __init__(self, dim: int) -> None:
        self.w: List[float] = [0.0] * dim
        self.b: float = 0.0

    def predict(self, x: Sequence[float]) -> float:
        return sum(wi * xi for wi, xi in zip(self.w, x)) + self.b

    def sgd_step(self, data: Sequence[Sample], lr: float) -> float:
        """One full-batch gradient step; returns the pre-step mean squared error."""
        n = len(data)
        if n == 0:
            return 0.0
        grad_w = [0.0] * len(self.w)
        grad_b = 0.0
        sse = 0.0
        for x, y in data:
            err = self.predict(x) - y
            sse += err * err
            for i, xi in enumerate(x):
                grad_w[i] += 2 * err * xi / n
            grad_b += 2 * err / n
        self.w = [wi - lr * gi for wi, gi in zip(self.w, grad_w)]
        self.b -= lr * grad_b
        return sse / n

    def average_with(self, other_w: List[float], other_b: float) -> None:
        self.w = [(a + b) / 2.0 for a, b in zip(self.w, other_w)]
        self.b = (self.b + other_b) / 2.0


class GossipLearner:
    """Runs diffusion SGD for one node over the swarm.

    Args:
        node: The :class:`~suprime.node.SwarmNode` to run on.
        model: The local :class:`LinearModel` (its weights converge in place).
        data: This node's private training samples.
        lr: Learning rate for the local gradient step.
        steps_per_round: Local SGD steps taken before each gossip average.
        rng: Injectable RNG for deterministic peer selection.
    """

    def __init__(
        self,
        node,
        model: LinearModel,
        data: Sequence[Sample],
        lr: float = 0.1,
        steps_per_round: int = 1,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._node = node
        self.model = model
        self._data = list(data)
        self._lr = lr
        self._steps = steps_per_round
        self._rng = rng or random.Random()
        self.last_loss = float("inf")
        node.on(MODEL_SYNC, self._on_sync)
        node.on_tick(self._tick)

    async def _tick(self) -> None:
        for _ in range(self._steps):
            self.last_loss = self.model.sgd_step(self._data, self._lr)
        alive = [p.node_id for p in self._node.peers.alive()]
        if alive:
            target = self._rng.choice(alive)
            await self._node.send(
                target, MODEL_SYNC, {"w": list(self.model.w), "b": self.model.b}
            )

    async def _on_sync(self, message: Message) -> None:
        self.model.average_with(message.payload["w"], float(message.payload["b"]))
