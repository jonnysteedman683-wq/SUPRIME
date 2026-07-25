"""The wire format for all swarm communication.

Every byte exchanged between nodes is a :class:`Message`. Messages are encoded
as a single JSON object so the protocol is transport agnostic and trivially
inspectable on the wire.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


class MessageType:
    """Well-known message types used by the swarm protocols."""

    # Membership / failure detection
    JOIN = "join"
    WELCOME = "welcome"
    GOSSIP = "gossip"
    PING = "ping"
    PING_REQ = "ping_req"  # SWIM indirect probe request
    ACK = "ack"

    # Application level
    DIRECT = "direct"
    BROADCAST = "broadcast"


@dataclass
class Message:
    """A self-describing envelope exchanged between two nodes.

    Attributes:
        type: One of :class:`MessageType` (or an application-defined string).
        src: Identity of the sending node.
        payload: Arbitrary JSON-serialisable body.
        dst: Optional target node id. ``None`` means the message is not
            addressed to a specific node (e.g. a gossip push to a peer).
        id: Unique message id, used for de-duplication.
        ts: Wall-clock timestamp (seconds) when the message was created.
    """

    type: str
    src: str
    payload: Dict[str, Any] = field(default_factory=dict)
    dst: Optional[str] = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ts: float = field(default_factory=time.time)

    def to_bytes(self) -> bytes:
        """Serialise to a UTF-8 JSON byte string."""
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "src": self.src,
            "payload": self.payload,
            "dst": self.dst,
            "id": self.id,
            "ts": self.ts,
        }

    @classmethod
    def from_bytes(cls, raw: bytes) -> "Message":
        """Parse a message from its JSON byte representation."""
        data = json.loads(raw.decode("utf-8"))
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        return cls(
            type=data["type"],
            src=data["src"],
            payload=data.get("payload", {}),
            dst=data.get("dst"),
            id=data.get("id", uuid.uuid4().hex),
            ts=data.get("ts", time.time()),
        )
