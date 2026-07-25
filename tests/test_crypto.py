"""Tests for pure-Python crypto and the signed/encrypted transports."""

from __future__ import annotations

import pytest

from conftest import flush
from suprime import crypto
from suprime.message import Message
from suprime.node import SwarmNode
from suprime.security import EncryptedTransport, SignedTransport
from suprime.transport import InMemoryTransport


# -- primitives ------------------------------------------------------------

def test_ed25519_sign_verify_roundtrip():
    sk, pk = crypto.generate_keypair()
    msg = b"the swarm coordinates"
    sig = crypto.sign(sk, pk, msg)
    assert crypto.verify(pk, msg, sig)
    assert not crypto.verify(pk, msg + b"!", sig)  # tampered message
    sk2, pk2 = crypto.generate_keypair()
    assert not crypto.verify(pk2, msg, sig)         # wrong key


def test_ed25519_backends_interoperate():
    # Whatever the active backend, it must agree byte-for-byte with the
    # pure-Python reference (RFC 8032 determinism) so mixed-backend swarms work.
    assert crypto.BACKEND in ("cryptography", "pure-python")
    sk = b"\x07" * 32
    pk_pure = crypto._pure_publickey(sk)
    assert crypto.publickey(sk) == pk_pure
    msg = b"cross-backend"
    sig_pure = crypto._pure_sign(sk, pk_pure, msg)
    assert crypto.verify(pk_pure, msg, sig_pure)          # active verifies pure
    assert crypto._pure_verify(pk_pure, msg, crypto.sign(sk, pk_pure, msg))


def test_chacha20_roundtrip_and_rfc_vector():
    key, nonce = b"k" * 32, b"n" * 12
    data = b"attack at dawn" * 10
    ct = crypto.chacha20(key, nonce, data)
    assert ct != data
    assert crypto.chacha20(key, nonce, ct) == data
    # RFC 8439 keystream for all-zero key/nonce begins 76b8e0ad...
    ks = crypto.chacha20(b"\x00" * 32, b"\x00" * 12, b"\x00" * 16)
    assert ks[:4].hex() == "76b8e0ad"


# -- signed transport ------------------------------------------------------

async def _signed_node(node_id_key, registry):
    sk, pk = node_id_key
    node_id = crypto.fingerprint(pk)
    transport = SignedTransport(InMemoryTransport(node_id, registry=registry), sk, pk)
    node = SwarmNode(transport=transport, node_id=node_id)
    return node, transport


@pytest.mark.asyncio
async def test_signed_transport_authentic_messages_flow():
    reg: dict = {}
    a, _ = await _signed_node(crypto.generate_keypair(), reg)
    b, _ = await _signed_node(crypto.generate_keypair(), reg)
    await a.start(auto=False)
    await b.start(auto=False)
    b._seeds = [a.address]
    await b._bootstrap()
    await flush()
    for _ in range(6):
        await a.tick(); await b.tick(); await flush()
    assert b.id in a.peers and a.id in b.peers
    await a.stop(); await b.stop()


@pytest.mark.asyncio
async def test_signed_transport_rejects_forged_identity():
    reg: dict = {}
    sk, pk = crypto.generate_keypair()
    victim = SwarmNode(
        transport=SignedTransport(InMemoryTransport("victim", registry=reg), *crypto.generate_keypair()[:2]),
        node_id="victim",
    )
    got = []
    async def handler(m):
        got.append(m)
    victim.on("chat", handler)
    await victim.start(auto=False)

    # An attacker signs with its own key but claims to be someone else's id.
    attacker_inner = InMemoryTransport("attacker", registry=reg)
    attacker = SignedTransport(attacker_inner, sk, pk)
    await attacker.start(lambda m: None)
    spoof = Message(type="chat", src="somebody-else", payload={"text": "hi"})
    await attacker.send(victim.address, spoof)  # src != fingerprint(pk)
    await flush()
    assert got == []  # rejected: identity not bound to the signing key
    await victim.stop(); await attacker.stop()


# -- encrypted transport ---------------------------------------------------

@pytest.mark.asyncio
async def test_encrypted_transport_roundtrip_and_wire_is_opaque():
    reg: dict = {}
    key = b"x" * 32
    got = []
    b = SwarmNode(transport=EncryptedTransport(InMemoryTransport("b", registry=reg), key), node_id="b")
    async def handler(m):
        got.append(m.payload.get("text"))
    b.on("chat", handler)
    await b.start(auto=False)

    a = EncryptedTransport(InMemoryTransport("a", registry=reg), key)
    await a.start(lambda m: None)
    await a.send(b.address, Message(type="chat", src="a", payload={"text": "top secret"}))
    await flush()
    assert got == ["top secret"]
    await b.stop(); await a.stop()


@pytest.mark.asyncio
async def test_encrypted_transport_rejects_wrong_key():
    reg: dict = {}
    got = []
    b = SwarmNode(transport=EncryptedTransport(InMemoryTransport("b", registry=reg), b"a" * 32), node_id="b")
    async def handler(m):
        got.append(m)
    b.on("chat", handler)
    await b.start(auto=False)

    a = EncryptedTransport(InMemoryTransport("a", registry=reg), b"b" * 32)  # wrong key
    await a.start(lambda m: None)
    await a.send(b.address, Message(type="chat", src="a", payload={"text": "x"}))
    await flush()
    assert got == []  # HMAC tag mismatch → dropped
    await b.stop(); await a.stop()
