"""Byzantine-tolerant value agreement with reputation weighting.

The core swarm assumes honest nodes. This layer tolerates a minority of *lying*
nodes for designated critical values. Nodes vote on the value of a key; a value
is accepted only when it commands a **quorum of trust-weighted votes**. Nodes
whose vote contradicts the accepted value lose reputation, so persistent liars
are progressively discounted and eventually ignored.

This is not a full BFT consensus protocol (no view-changes or signed message
chains), but it is a practical, gossip-friendly defence: as long as honest nodes
hold a trust-weighted quorum, the swarm converges on the truth and down-weights
attackers.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .message import Message

VOTE_MSG = "__byz_vote__"


class TrustLedger:
    """Per-node reputation in ``[0, max_score]``, starting neutral."""

    def __init__(self, initial: float = 1.0, max_score: float = 5.0) -> None:
        self._scores: Dict[str, float] = {}
        self._initial = initial
        self._max = max_score

    def score(self, node_id: str) -> float:
        return self._scores.get(node_id, self._initial)

    def weight(self, node_id: str) -> float:
        return max(0.0, self.score(node_id))

    def reward(self, node_id: str, amount: float = 0.2) -> None:
        self._scores[node_id] = min(self._max, self.score(node_id) + amount)

    def penalize(self, node_id: str, amount: float = 0.5) -> None:
        self._scores[node_id] = max(0.0, self.score(node_id) - amount)

    def snapshot(self) -> Dict[str, float]:
        return dict(self._scores)


class QuorumRegister:
    """Collects trust-weighted votes per key and decides accepted values.

    Args:
        ledger: The :class:`TrustLedger` supplying vote weights.
        quorum: Fraction of total participating weight a value needs to win.
    """

    def __init__(self, ledger: TrustLedger, quorum: float = 0.5) -> None:
        self._ledger = ledger
        self._quorum = quorum
        self._votes: Dict[str, Dict[str, Any]] = {}

    def record_vote(self, key: str, node_id: str, value: Any) -> None:
        self._votes.setdefault(key, {})[node_id] = value

    def tally(self, key: str) -> Optional[Any]:
        """Return the trust-weighted plurality value if it meets quorum."""
        votes = self._votes.get(key, {})
        if not votes:
            return None
        totals: Dict[Any, float] = {}
        total_weight = 0.0
        for node_id, value in votes.items():
            w = self._ledger.weight(node_id)
            totals[value] = totals.get(value, 0.0) + w
            total_weight += w
        if total_weight <= 0:
            return None
        best_value, best_weight = max(totals.items(), key=lambda kv: kv[1])
        if best_weight >= self._quorum * total_weight:
            return best_value
        return None

    def resolve(self, key: str) -> Optional[Any]:
        """Tally, then reward agreeing voters and penalise contradicting ones."""
        accepted = self.tally(key)
        if accepted is None:
            return None
        for node_id, value in self._votes.get(key, {}).items():
            if value == accepted:
                self._ledger.reward(node_id)
            else:
                self._ledger.penalize(node_id)
        return accepted


class ByzantineConsensus:
    """Gossips votes for critical keys and exposes trust-weighted decisions.

    Args:
        node: The :class:`~suprime.node.SwarmNode` to run on.
        quorum: Fraction of trust-weighted votes required to accept a value.
    """

    def __init__(self, node, quorum: float = 0.5) -> None:
        self._node = node
        self.ledger = TrustLedger()
        self.register = QuorumRegister(self.ledger, quorum=quorum)
        self._own_votes: Dict[str, Any] = {}
        node.on(VOTE_MSG, self._on_vote)
        node.on_tick(self._tick)

    def vote(self, key: str, value: Any) -> None:
        """Cast (or update) this node's vote for ``key``."""
        self._own_votes[key] = value
        self.register.record_vote(key, self._node.id, value)

    def accepted(self, key: str) -> Optional[Any]:
        return self.register.resolve(key)

    def trust(self, node_id: str) -> float:
        return self.ledger.score(node_id)

    async def _tick(self) -> None:
        if not self._own_votes:
            return
        for peer in self._node.peers.alive():
            await self._node.send(peer.node_id, VOTE_MSG, {"votes": self._own_votes})

    async def _on_vote(self, message: Message) -> None:
        for key, value in message.payload.get("votes", {}).items():
            self.register.record_vote(key, message.src, value)
