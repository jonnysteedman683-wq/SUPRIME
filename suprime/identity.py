"""Node identity primitives for the SUPRIME swarm.

Every participant in the swarm is uniquely identified by a :class:`NodeID`.
Identities are stable, sortable strings so that membership, leader election and
conflict resolution can rely on a deterministic total order across the network.
"""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass


def _short_token(n: int = 6) -> str:
    """Return a short, url-safe random token."""
    return secrets.token_hex(n // 2 + 1)[:n]


@dataclass(frozen=True, order=True)
class NodeID:
    """A stable, sortable identifier for a swarm node.

    The string ``value`` is the canonical identity used on the wire and as the
    tie-breaker for every deterministic decision in the swarm (leader election,
    last-writer-wins conflict resolution, task-claim arbitration).
    """

    value: str

    def __post_init__(self) -> None:
        if not self.value or not isinstance(self.value, str):
            raise ValueError("NodeID value must be a non-empty string")

    @classmethod
    def generate(cls, prefix: str = "node") -> "NodeID":
        """Create a fresh, process-unique identity."""
        return cls(f"{prefix}-{os.getpid()}-{_short_token()}")

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value
