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
import os
from dataclasses import dataclass
from typing import Optional

from . import crypto
from .message import Message
from .transport import Transport


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


# -- Ed25519-signed transport (public-key authentication) ------------------

class SignedTransport(Transport):
    """Transport decorator that signs every message with an Ed25519 key.

    Unlike :class:`SecureTransport` (shared secret), this gives true public-key
    authentication: each message carries the sender's public key and a
    signature, and the receiver rejects it unless the signature verifies *and*
    the sender's node id is the fingerprint of that key — so no node can forge
    another's identity, with no shared secret to distribute.

    The owning node's id should be :func:`suprime.crypto.fingerprint` of ``pk``.
    """

    def __init__(self, inner: Transport, sk: bytes, pk: bytes) -> None:
        self._inner = inner
        self._sk = sk
        self._pk = pk
        self._pk_hex = pk.hex()
        self._on_message = None
        self.rejected = 0

    @property
    def address(self) -> str:
        return self._inner.address

    def _canonical(self, message: Message) -> bytes:
        data = message.to_dict()
        payload = dict(data.get("payload", {}))
        payload.pop("_sig", None)
        payload.pop("_pk", None)
        data["payload"] = payload
        return repr(sorted(data.items())).encode("utf-8")

    async def start(self, on_message) -> None:
        self._on_message = on_message
        await self._inner.start(self._recv)

    async def _recv(self, message: Message) -> None:
        payload = message.payload if isinstance(message.payload, dict) else {}
        sig_hex = payload.pop("_sig", None)
        pk_hex = payload.pop("_pk", None)
        if not sig_hex or not pk_hex:
            self.rejected += 1
            return
        try:
            pk = bytes.fromhex(pk_hex)
            sig = bytes.fromhex(sig_hex)
        except ValueError:
            self.rejected += 1
            return
        # Identity binding: src must be the fingerprint of the presented key.
        if crypto.fingerprint(pk) != message.src:
            self.rejected += 1
            return
        if not crypto.verify(pk, self._canonical(message), sig):
            self.rejected += 1
            return
        if self._on_message is not None:
            await self._on_message(message)

    async def send(self, address: str, message: Message) -> None:
        message.payload = dict(message.payload)
        sig = crypto.sign(self._sk, self._pk, self._canonical(message))
        message.payload["_sig"] = sig.hex()
        message.payload["_pk"] = self._pk_hex
        await self._inner.send(address, message)

    async def stop(self) -> None:
        await self._inner.stop()


# -- ChaCha20 encrypted transport (confidentiality + integrity) ------------

class EncryptedTransport(Transport):
    """Transport decorator providing authenticated encryption of the wire.

    Each outbound message is serialised, encrypted with ChaCha20 under a shared
    key and tagged with HMAC-SHA256 (encrypt-then-MAC). On the wire only an
    opaque ciphertext envelope is visible; the receiver verifies the tag, then
    decrypts and reconstructs the original message. Tampered or wrongly-keyed
    frames are dropped.
    """

    ENVELOPE = "__enc__"

    def __init__(self, inner: Transport, key: bytes) -> None:
        if len(key) != 32:
            raise ValueError("key must be 32 bytes")
        self._inner = inner
        self._key = key
        self._on_message = None
        self.rejected = 0

    @property
    def address(self) -> str:
        return self._inner.address

    async def start(self, on_message) -> None:
        self._on_message = on_message
        await self._inner.start(self._recv)

    async def send(self, address: str, message: Message) -> None:
        plaintext = message.to_bytes()
        nonce = os.urandom(12)
        ct = crypto.chacha20(self._key, nonce, plaintext)
        mac = hmac.new(self._key, nonce + ct, hashlib.sha256).hexdigest()
        envelope = Message(
            type=self.ENVELOPE,
            src=message.src,
            payload={"n": nonce.hex(), "ct": ct.hex(), "mac": mac},
        )
        await self._inner.send(address, envelope)

    async def _recv(self, message: Message) -> None:
        if message.type != self.ENVELOPE:
            # Unencrypted traffic is not accepted on an encrypted channel.
            self.rejected += 1
            return
        try:
            nonce = bytes.fromhex(message.payload["n"])
            ct = bytes.fromhex(message.payload["ct"])
            mac = message.payload["mac"]
        except (KeyError, ValueError):
            self.rejected += 1
            return
        expected = hmac.new(self._key, nonce + ct, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(mac, expected):
            self.rejected += 1
            return
        plaintext = crypto.chacha20(self._key, nonce, ct)
        inner = Message.from_bytes(plaintext)
        if self._on_message is not None:
            await self._on_message(inner)

    async def stop(self) -> None:
        await self._inner.stop()


def secure_transport(
    inner: Transport,
    *,
    sk: Optional[bytes] = None,
    pk: Optional[bytes] = None,
    cluster_key: Optional[bytes] = None,
) -> Transport:
    """Compose a hardened transport stack over ``inner``.

    With both a keypair and a cluster key you get **sign-then-encrypt**: messages
    are Ed25519-signed then ChaCha20-encrypted, so the wire is opaque *and* every
    message is provably from the claimed node. Pass only one for just
    authentication or just confidentiality.
    """
    t = inner
    if cluster_key is not None:
        t = EncryptedTransport(t, cluster_key)
    if sk is not None and pk is not None:
        t = SignedTransport(t, sk, pk)
    return t


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
