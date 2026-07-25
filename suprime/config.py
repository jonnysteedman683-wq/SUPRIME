"""Configuration loading for deploying a swarm node.

Turns environment variables (or an explicit mapping) into a :class:`NodeConfig`,
and builds a ready-to-run :class:`~suprime.node.SwarmNode` from it — including a
TCP transport, optional signed+encrypted wrapping and optional persistence. This
is what the Docker image and ``suprime serve`` use.

Recognised environment variables (all optional):

===========================  ================================================
``SUPRIME_HOST``             bind host (default ``0.0.0.0``)
``SUPRIME_PORT``             bind port (default ``7000``)
``SUPRIME_ID``               explicit node id (default: derived/random)
``SUPRIME_SEEDS``            comma-separated ``host:port`` seed list
``SUPRIME_GOSSIP_INTERVAL``  seconds between gossip rounds (default ``0.5``)
``SUPRIME_FANOUT``           gossip fanout (default ``3``)
``SUPRIME_PERSIST_DIR``      directory for WAL+snapshot durability
``SUPRIME_CLUSTER_KEY``      hex key → enables encrypted transport
===========================  ================================================
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class NodeConfig:
    host: str = "0.0.0.0"
    port: int = 7000
    node_id: Optional[str] = None
    seeds: List[str] = field(default_factory=list)
    gossip_interval: float = 0.5
    fanout: int = 3
    persist_dir: Optional[str] = None
    cluster_key_hex: Optional[str] = None

    @classmethod
    def from_env(cls, env: Optional[Dict[str, str]] = None) -> "NodeConfig":
        e = env if env is not None else os.environ
        seeds_raw = e.get("SUPRIME_SEEDS", "").strip()
        seeds = [s.strip() for s in seeds_raw.split(",") if s.strip()]
        return cls(
            host=e.get("SUPRIME_HOST", "0.0.0.0"),
            port=int(e.get("SUPRIME_PORT", "7000")),
            node_id=e.get("SUPRIME_ID") or None,
            seeds=seeds,
            gossip_interval=float(e.get("SUPRIME_GOSSIP_INTERVAL", "0.5")),
            fanout=int(e.get("SUPRIME_FANOUT", "3")),
            persist_dir=e.get("SUPRIME_PERSIST_DIR") or None,
            cluster_key_hex=e.get("SUPRIME_CLUSTER_KEY") or None,
        )

    def cluster_key(self) -> Optional[bytes]:
        if not self.cluster_key_hex:
            return None
        key = bytes.fromhex(self.cluster_key_hex)
        if len(key) != 32:
            raise ValueError("SUPRIME_CLUSTER_KEY must be 32 bytes (64 hex chars)")
        return key


def build_node(config: NodeConfig):
    """Construct a :class:`~suprime.node.SwarmNode` from ``config``."""
    from .node import SwarmNode
    from .transport import TcpTransport

    transport = TcpTransport(host=config.host, port=config.port)
    key = config.cluster_key()
    if key is not None:
        from .security import EncryptedTransport

        transport = EncryptedTransport(transport, key)

    return SwarmNode(
        transport=transport,
        node_id=config.node_id,
        seeds=config.seeds,
        gossip_interval=config.gossip_interval,
        fanout=config.fanout,
        persist_dir=config.persist_dir,
    )
