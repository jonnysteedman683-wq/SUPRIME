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

from .consensus import LeaderView, elect_leader
from .gossip import GossipService
from .identity import NodeID
from .message import Message, MessageType
from .node import SwarmNode
from .peers import Peer, PeerState, PeerTable
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
    "__version__",
]
