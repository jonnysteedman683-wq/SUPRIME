"""Message authentication and Sybil-resistance for the swarm.

Two independent, stdlib-only mechanisms:

* :class:`SecureTransport` wraps any transport and attaches an HMAC-SHA256 tag
  (keyed by a shared cluster secret) to every frame, rejecting messages that are
  forged or tampered with. This gives integrity + admission control: only
  holders of the cluster key can inject gossip.

* :func:`mint_identity` / :func:`verify_identity` implement a hashcash-style
  **proof of work** so creating a node identity has a tunable computational
  cost. That raises the price of spinning up many fake identities (a Sybil
  attack) — a node can require a valid PoW before admitting a peer.

Public-key identities (Ed25519) are a natural extension but need a third-party
crypto library; the HMAC scheme here keeps SUPRIME dependency-free while still
being real, verifiable authentication within a trusted cluster.
"""

from __future__ import annotations

import hashlib
import hmac
import itertools
import time
from dataclasses import dataclass
from typing import Optional

from .message import Message
from .transport import Transport, TransportError


# -- HMAC-authenticated transport ------------------------------------------

class SecureTransport(Transport):
    """Transport decorator that authenticates every message with HMAC-SHA256.

    Outbound messages get a ``_sig`` tag over their canonical bytes; inbound
    messages missing a valid tag are dropped before the application ever sees
    them. Uses a constant-time comparison to avoid timing oracles.
    """

    def __init__(self, inner: Transport, cluster_key: bytes) -> None:
        self._inner = inner
        self._key = cluster_key
        self._on_message = None
        self.rejected = 0

    @property
    def address(self) -> str:
        return self._inner.address

    def _sign(self, message: Message) -> str:
        # Sign the message minus the signature field itself.
        data = message.to_dict()
        data.pop("_sig", None)
        canonical = repr(sorted(data.items())).encode("utf-8")
        return hmac.new(self._key, canonical, hashlib.sha256).hexdigest()

    async def start(self, on_message) -> None:
        self._on_message = on_message
        await self._inner.start(self._recv)

    async def _recv(self, message: Message) -> None:
        sig = message.payload.pop("_sig", None) if isinstance(message.payload, dict) else None
        expected = self._sign(message)
        if sig is None or not hmac.compare_digest(sig, expected):
            self.rejected += 1
            return  # unauthenticated: silently drop
        if self._on_message is not None:
            await self._on_message(message)

    async def send(self, address: str, message: Message) -> None:
        # Attach the tag inside the payload so it rides the existing envelope.
        message.payload = dict(message.payload)
        message.payload["_sig"] = self._sign(message)
        await self._inner.send(address, message)

    async def stop(self) -> None:
        await self._inner.stop()


# -- hashcash-style proof of work ------------------------------------------

@dataclass
class Identity:
    """A node identity backed by a proof of work."""

    node_id: str
    nonce: int
    difficulty: int

    def token(self) -> str:
        return f"{self.node_id}:{self.nonce}:{self.difficulty}"


def _pow_hash(node_id: str, nonce: int) -> str:
    return hashlib.sha256(f"{node_id}:{nonce}".encode("utf-8")).hexdigest()


def mint_identity(node_id: str, difficulty: int = 12, max_iters: int = 5_000_000) -> Identity:
    """Find a nonce whose hash has ``difficulty`` leading zero bits.

    The expected work is ``2**difficulty`` hashes, making identity creation
    deliberately costly. Raises ``RuntimeError`` if no nonce is found in the
    iteration budget (raise ``max_iters`` or lower ``difficulty``).
    """
    target_prefix_bits = difficulty
    for nonce in itertools.count():
        if nonce > max_iters:
            raise RuntimeError("proof-of-work budget exhausted")
        digest = _pow_hash(node_id, nonce)
        if _leading_zero_bits(digest) >= target_prefix_bits:
            return Identity(node_id=node_id, nonce=nonce, difficulty=difficulty)


def verify_identity(identity: Identity, min_difficulty: int = 12) -> bool:
    """Verify a proof of work meets ``min_difficulty`` (cheap: one hash)."""
    if identity.difficulty < min_difficulty:
        return False
    digest = _pow_hash(identity.node_id, identity.nonce)
    return _leading_zero_bits(digest) >= identity.difficulty


def _leading_zero_bits(hex_digest: str) -> int:
    bits = 0
    for ch in hex_digest:
        nibble = int(ch, 16)
        if nibble == 0:
            bits += 4
            continue
        # count leading zeros within this nibble (4 bits)
        bits += 4 - nibble.bit_length()
        break
    return bits
