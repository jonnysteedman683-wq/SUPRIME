"""Deterministic leader election over the live membership set.

The swarm needs, from time to time, a single node to take a distinguished role
(seeding work, breaking ties, acting as a rendezvous point). Rather than run a
heavyweight consensus protocol, SUPRIME derives the leader as a pure function of
the currently-alive members: the node with the smallest id wins.

Because every node applies the same rule to its (eventually consistent) view of
membership, the swarm converges on one leader without any dedicated election
messages. When the leader fails, its heartbeat goes stale, it drops out of the
alive set, and the next-smallest id transparently takes over.
"""

from __future__ import annotations

from typing import Iterable, Optional


def elect_leader(self_id: str, alive_peer_ids: Iterable[str]) -> str:
    """Return the elected leader id given the local view of the swarm.

    Args:
        self_id: This node's identity (always a candidate).
        alive_peer_ids: Identities of peers currently believed alive.

    Returns:
        The id of the node that should act as leader. With a single node this
        is always ``self_id``.
    """
    candidates = set(alive_peer_ids)
    candidates.add(self_id)
    return min(candidates)


class LeaderView:
    """Tracks the elected leader and reports transitions.

    Useful for triggering side effects exactly when leadership changes rather
    than on every membership tick.
    """

    def __init__(self, self_id: str) -> None:
        self._self_id = self_id
        self._leader: Optional[str] = None

    @property
    def leader(self) -> Optional[str]:
        return self._leader

    def is_leader(self) -> bool:
        return self._leader == self._self_id

    def update(self, alive_peer_ids: Iterable[str]) -> Optional[str]:
        """Recompute the leader; return the new leader id if it changed."""
        new_leader = elect_leader(self._self_id, alive_peer_ids)
        if new_leader != self._leader:
            self._leader = new_leader
            return new_leader
        return None
