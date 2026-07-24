"""SUPRIME — a powerful experimental swarm network.

A decentralised, gossip-based network of autonomous nodes. There is no central
coordinator: membership, replicated state, distributed task execution and
leader election all emerge from a single epidemic gossip channel.

Quick start::

    import asyncio
    from suprime import SwarmNode

    async def main():
        a = await SwarmNode(node_id="a").start()
        b = await SwarmNode(node_id="b", seeds=[a.address]).start()
        a.store.set("motd", "hello swarm")
        await asyncio.sleep(1)
        print(b.store.get("motd"))  # -> "hello swarm"
        await a.stop(); await b.stop()

    asyncio.run(main())
"""

from .aggregate import PushSumAggregator
from .antientropy import AntiEntropy
from .byzantine import ByzantineConsensus, QuorumRegister, TrustLedger
from .chaos import ChaosController, ChaosTransport
from .consensus import LeaderView, elect_leader
from .kvstore import KVStore
from .metrics import MetricsRegistry, StructuredLogger, prometheus_format
from .persistence import PersistenceManager
from .simulation import SimConfig, Simulator
from .crdt import (
    GCounter,
    LWWMap,
    MVRegister,
    ORSet,
    PNCounter,
    VectorClock,
)
from .gossip import GossipService
from .hyparview import HyParView
from .identity import NodeID
from .learning import GossipLearner, LinearModel
from .message import Message, MessageType
from .security import (
    EncryptedTransport,
    Identity,
    SecureTransport,
    SignedTransport,
    mint_identity,
    verify_identity,
)
from .node import SwarmNode
from .peers import Peer, PeerState, PeerTable
from .plumtree import PlumtreeBroadcast
from .pubsub import PubSub
from .replicate import CRDTReplicator
from .rga import RGA
from .store import DistributedStore, Entry, Version
from .tasks import Task, TaskBoard, TaskState
from .transport import InMemoryTransport, TcpTransport, Transport, TransportError

__version__ = "0.1.0"

__all__ = [
    "SwarmNode",
    "NodeID",
    "Message",
    "MessageType",
    "Transport",
    "InMemoryTransport",
    "TcpTransport",
    "TransportError",
    "PeerTable",
    "Peer",
    "PeerState",
    "DistributedStore",
    "Entry",
    "Version",
    "TaskBoard",
    "Task",
    "TaskState",
    "GossipService",
    "LeaderView",
    "elect_leader",
    "ChaosController",
    "ChaosTransport",
    "PushSumAggregator",
    "PlumtreeBroadcast",
    "HyParView",
    "PubSub",
    "AntiEntropy",
    "CRDTReplicator",
    "GCounter",
    "PNCounter",
    "ORSet",
    "LWWMap",
    "MVRegister",
    "VectorClock",
    "RGA",
    "SecureTransport",
    "Identity",
    "mint_identity",
    "verify_identity",
    "TrustLedger",
    "QuorumRegister",
    "ByzantineConsensus",
    "GossipLearner",
    "LinearModel",
    "SignedTransport",
    "EncryptedTransport",
    "KVStore",
    "PersistenceManager",
    "MetricsRegistry",
    "StructuredLogger",
    "prometheus_format",
    "Simulator",
    "SimConfig",
    "__version__",
]
